from __future__ import annotations

import copy
from decimal import Decimal
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

from engine.contracts import AccountKey
from engine.supervisor import (
    TEST_VAULT_MARKER,
    export_test_notes,
    initialize_supervisor_schema,
    persist_observer_envelope,
    reconcile_cycle,
)


ROOT = Path(__file__).resolve().parents[2]
FIXTURE = ROOT / "tests/fixtures/engine/supervisor-sequence.json"


class SupervisorTests(unittest.TestCase):
    def setUp(self):
        self.data = json.loads(FIXTURE.read_text(encoding="utf-8"))
        self.account = AccountKey.from_value(self.data["account_key"])
        self.db = sqlite3.connect(":memory:")
        self.db.row_factory = sqlite3.Row
        initialize_supervisor_schema(self.db, "2026-10-30T18:50:00Z")

    def tearDown(self):
        self.db.close()

    def envelope(self, index=0):
        row = copy.deepcopy(self.data["observer"]["events"][index])
        return {
            "source": self.data["observer"]["source"],
            "stream": self.data["observer"]["stream"],
            "server": self.account.server,
            "login": self.account.login,
            **row,
        }

    def cycle(self, index):
        row = self.data["cycles"][index]
        return reconcile_cycle(
            self.db,
            account=self.account,
            observed_at_utc=row["observed_at_utc"],
            cycle_id=row["cycle_id"],
            connection=row["connection"],
            account_state=row["account_state"],
            orders=row["orders"],
            deals=row["deals"],
            positions=row["positions"],
            symbol_specs=self.data["symbol_specs"],
        )

    def run_cycles(self, count):
        return [self.cycle(index) for index in range(count)]

    def metric(self, cycle_id, scope, name):
        return self.db.execute(
            "SELECT * FROM supervisor_metrics WHERE cycle_id=? AND scope_key=? AND metric_name=?",
            (cycle_id, scope, name),
        ).fetchone()

    def test_schema_is_idempotent(self):
        self.assertEqual(initialize_supervisor_schema(self.db, "2026-10-30T18:51:00Z"), 2)
        self.assertEqual(self.db.execute("SELECT count(*) FROM supervisor_schema").fetchone()[0], 2)

    def test_observer_event_is_idempotent(self):
        event = self.envelope()
        self.assertEqual(persist_observer_envelope(self.db, event, "2026-10-30T19:00:00Z"), "stored")
        self.assertEqual(persist_observer_envelope(self.db, event, "2026-10-30T19:00:00Z"), "duplicate")
        self.assertEqual(self.db.execute("SELECT count(*) FROM observer_event_ledger").fetchone()[0], 1)

    def test_observer_sequence_conflict_fails_closed(self):
        event = self.envelope()
        persist_observer_envelope(self.db, event, "2026-10-30T19:00:00Z")
        event["payload"]["price"] = "9"
        with self.assertRaisesRegex(ValueError, "OBSERVER_SEQUENCE_CONTENT_CONFLICT"):
            persist_observer_envelope(self.db, event, "2026-10-30T19:00:00Z")

    def test_transaction_requires_native_identifier(self):
        event = self.envelope()
        event["payload"].update({"order_id": "0", "deal_id": "0", "position_ticket": "0"})
        with self.assertRaisesRegex(ValueError, "TRANSACTION_NATIVE_ID_REQUIRED"):
            persist_observer_envelope(self.db, event, "2026-10-30T19:00:00Z")

    def test_open_snapshot_persists_position_and_metrics(self):
        result = self.cycle(0)
        self.assertEqual(result.reconciliation_quality, "KNOWN")
        position = self.db.execute("SELECT * FROM supervisor_positions").fetchone()
        self.assertEqual(position["status"], "OPEN")
        self.assertEqual(json.loads(self.metric("cycle-1-open", "position:6001", "initial_risk_money")["value_json"]), "50")
        self.assertEqual(json.loads(self.metric("cycle-1-open", "position:6001", "current_risk_money")["value_json"]), "60")
        self.assertEqual(json.loads(self.metric("cycle-1-open", "position:6001", "planned_r")["value_json"]), "2")

    def test_execution_slippage_uses_contract_specification(self):
        self.cycle(0)
        value = json.loads(self.metric("cycle-1-open", "position:6001", "execution_slippage:2001")["value_json"])
        self.assertEqual(value, {"adverse_money": "1", "adverse_ticks": "1"})

    def test_protection_change_is_separate_event_and_metric(self):
        self.run_cycles(2)
        events = {row[0] for row in self.db.execute("SELECT event_kind FROM supervisor_events WHERE cycle_id='cycle-2-modified'")}
        self.assertIn("position.modified", events)
        value = json.loads(self.metric("cycle-2-modified", "position:6001", "protection_deviation")["value_json"])
        self.assertEqual(value["classification"], "tightened")
        self.assertEqual(value["favourable_ticks"], "20")
        self.assertEqual(value["risk_reduction_money"], "20")

    def test_disappearance_with_full_exit_is_closed_confirmed(self):
        results = self.run_cycles(3)
        self.assertEqual(results[-1].lifecycles[0]["status"], "CLOSED_CONFIRMED")
        row = self.db.execute("SELECT status FROM supervisor_positions WHERE position_identifier='6001'").fetchone()
        self.assertEqual(row["status"], "CLOSED_CONFIRMED")
        self.assertEqual(json.loads(self.metric("cycle-3-closed", "position:6001", "realized_r")["value_json"]), "1.14")
        self.assertEqual(
            json.loads(self.metric("cycle-3-closed", "position:6001", "initial_risk_percent")["value_json"]),
            "0.5",
        )

    def test_realized_r_is_unverifiable_when_deal_window_misses_open(self):
        self.data["cycles"][2]["connection"]["coverage"] = {
            "deals": {
                "complete": True,
                "requested_window": ["2026-10-30T19:01:00Z", "2026-10-30T19:02:00Z"],
            }
        }
        self.run_cycles(3)
        row = self.metric("cycle-3-closed", "position:6001", "realized_r")
        self.assertEqual(row["status"], "UNVERIFIABLE")
        self.assertIn("LIFECYCLE_OUTSIDE_DEAL_WINDOW", json.loads(row["reason_codes_json"]))

    def test_closed_account_exposure_is_zero_not_unknown(self):
        self.run_cycles(3)
        row = self.metric("cycle-3-closed", "account", "aggregate_current_risk")
        self.assertEqual(row["status"], "KNOWN")
        self.assertEqual(json.loads(row["value_json"]), "0")

    def test_open_risk_is_aggregated_by_instrument_and_currency(self):
        self.cycle(0)
        instrument = self.metric("cycle-1-open", "instrument:SYNTHUSD", "aggregate_current_risk")
        currency = self.metric("cycle-1-open", "currency:USD", "aggregate_current_risk")
        self.assertEqual(json.loads(instrument["value_json"]), "60")
        self.assertEqual(json.loads(currency["value_json"]), "60")

    def test_missing_provider_timing_stays_unknown(self):
        self.cycle(0)
        duration = self.metric("cycle-1-open", "account", "snapshot_read_duration")
        lag = self.metric("cycle-1-open", "account", "broker_clock_lag")
        self.assertEqual(duration["status"], "UNKNOWN")
        self.assertEqual(lag["status"], "UNKNOWN")

    def test_reconciliation_difference_metric_is_known_with_full_coverage(self):
        self.cycle(0)
        row = self.metric("cycle-1-open", "account", "reconciliation_differences")
        self.assertEqual(row["status"], "KNOWN")
        self.assertEqual(json.loads(row["value_json"])["detected_total"], 0)

    def test_disappearance_without_exit_is_unknown(self):
        result = self.run_cycles(5)[-1]
        self.assertEqual(result.lifecycles[0]["status"], "UNKNOWN")
        self.assertIn("POSITION_DISAPPEARED_WITHOUT_ACCREDITED_CLOSE", {row["code"] for row in result.issues})
        row = self.db.execute("SELECT status FROM supervisor_positions WHERE position_identifier='7001'").fetchone()
        self.assertEqual(row["status"], "UNKNOWN")

    def test_drawdowns_use_persisted_account_samples(self):
        self.run_cycles(5)
        realized = self.metric("cycle-5-unknown-disappearance", "account", "realized_drawdown")
        floating = self.metric("cycle-5-unknown-disappearance", "account", "floating_drawdown")
        self.assertEqual(json.loads(realized["value_json"])["maximum"], "47")
        self.assertEqual(realized["status"], "KNOWN")
        self.assertEqual(json.loads(floating["value_json"])["maximum"], "67")
        self.assertEqual(floating["status"], "ESTIMATED")

    def test_cycle_replay_is_idempotent(self):
        first = self.cycle(0).as_dict()
        second = self.cycle(0).as_dict()
        self.assertEqual(first, second)
        self.assertEqual(self.db.execute("SELECT count(*) FROM supervisor_cycles").fetchone()[0], 1)

    def test_cycle_content_conflict_fails_closed(self):
        self.cycle(0)
        self.data["cycles"][0]["positions"][0]["volume"] = "9"
        with self.assertRaisesRegex(ValueError, "CYCLE_ID_CONTENT_CONFLICT"):
            self.cycle(0)

    def test_test_vault_marker_is_mandatory(self):
        self.run_cycles(3)
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ValueError, "OBSIDIAN_TEST_VAULT_MARKER_REQUIRED"):
                export_test_notes(
                    self.db,
                    cycle_id="cycle-3-closed",
                    destination=Path(directory),
                    exported_at_utc="2026-10-30T19:05:00Z",
                )

    def test_note_export_separates_auto_and_human_and_preserves_human(self):
        self.run_cycles(3)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / ".vidar-mt5-test-vault").write_text(TEST_VAULT_MARKER, encoding="utf-8")
            result = export_test_notes(
                self.db,
                cycle_id="cycle-3-closed",
                destination=root,
                exported_at_utc="2026-10-30T19:05:00Z",
            )
            self.assertEqual(len(result["notes"]), 1)
            note = result["notes"][0]
            automatic = (root / note["auto_relative"]).read_text(encoding="utf-8")
            human_path = root / note["human_relative"]
            self.assertIn("SYNTHETIC-DEMO", automatic)
            self.assertIn("SYNTHUSD", automatic)
            self.assertIn("CLOSED_CONFIRMED", automatic)
            self.assertIn("1001", automatic)
            self.assertIn("2002", automatic)
            human = human_path.read_text(encoding="utf-8")
            self.assertIn("Errores y aciertos", human)
            self.assertIn("Riesgo inicial revisado", human)
            self.assertIn("Riesgo vigente revisado", human)
            self.assertIn("Cambios de SL/TP", human)
            self.assertIn("Cansancio (0-10)", human)
            human_path.write_text("anotación del propietario\n", encoding="utf-8")
            export_test_notes(
                self.db,
                cycle_id="cycle-3-closed",
                destination=root,
                exported_at_utc="2026-10-30T19:06:00Z",
            )
            self.assertEqual(human_path.read_text(encoding="utf-8"), "anotación del propietario\n")

    def test_partial_close_changes_volume_without_splitting_lifecycle(self):
        self.cycle(0)
        partial = copy.deepcopy(self.data["cycles"][0])
        partial.update(
            {
                "cycle_id": "cycle-partial-close",
                "observed_at_utc": "2026-10-30T19:00:30Z",
            }
        )
        partial["orders"].append(
            {
                "order_ticket": "1002",
                "position_identifier": "6001",
                "symbol": "SYNTHUSD",
                "state": "filled",
            }
        )
        partial["deals"].append(
            {
                "deal_ticket": "2002",
                "order_ticket": "1002",
                "position_identifier": "6001",
                "entry": "out",
                "direction": "sell",
                "volume": "0.04",
                "price": "1.10300",
                "requested_price": "1.10290",
                "profit": "12",
                "commission": "0",
                "swap": "0",
                "fee": "0",
            }
        )
        partial["positions"][0]["volume"] = "0.06"
        partial["positions"][0]["current_price"] = "1.10300"
        result = reconcile_cycle(
            self.db,
            account=self.account,
            observed_at_utc=partial["observed_at_utc"],
            cycle_id=partial["cycle_id"],
            connection=partial["connection"],
            account_state=partial["account_state"],
            orders=partial["orders"],
            deals=partial["deals"],
            positions=partial["positions"],
            symbol_specs=self.data["symbol_specs"],
        )
        self.assertEqual(result.lifecycles[0]["position_identifier"], "6001")
        self.assertEqual(result.lifecycles[0]["status"], "OPEN")
        position = self.db.execute(
            "SELECT status,current_json FROM supervisor_positions WHERE position_identifier='6001'"
        ).fetchone()
        self.assertEqual(position["status"], "OPEN")
        self.assertEqual(json.loads(position["current_json"])["volume"], "0.06")
        self.assertEqual(
            self.db.execute(
                "SELECT count(*) FROM supervisor_position_versions WHERE position_identifier='6001'"
            ).fetchone()[0],
            2,
        )
        event = json.loads(
            self.db.execute(
                "SELECT event_json FROM supervisor_events WHERE cycle_id='cycle-partial-close' AND event_kind='position.modified'"
            ).fetchone()[0]
        )
        self.assertEqual(event["data"]["changes"]["volume"], {"before": "0.10", "after": "0.06"})
        self.assertEqual(
            json.loads(
                self.metric(
                    "cycle-partial-close", "position:6001", "current_risk_money"
                )["value_json"]
            ),
            "47.4",
        )

        final = copy.deepcopy(partial)
        final.update(
            {
                "cycle_id": "cycle-partial-final-close",
                "observed_at_utc": "2026-10-30T19:01:30Z",
                "positions": [],
            }
        )
        final["orders"].append(
            {
                "order_ticket": "1003",
                "position_identifier": "6001",
                "symbol": "SYNTHUSD",
                "state": "filled",
            }
        )
        final["deals"].append(
            {
                "deal_ticket": "2003",
                "order_ticket": "1003",
                "position_identifier": "6001",
                "entry": "out",
                "direction": "sell",
                "volume": "0.06",
                "price": "1.10400",
                "requested_price": "1.10390",
                "profit": "24",
                "commission": "0",
                "swap": "0",
                "fee": "0",
            }
        )
        closed = reconcile_cycle(
            self.db,
            account=self.account,
            observed_at_utc=final["observed_at_utc"],
            cycle_id=final["cycle_id"],
            connection=final["connection"],
            account_state=final["account_state"],
            orders=final["orders"],
            deals=final["deals"],
            positions=final["positions"],
            symbol_specs=self.data["symbol_specs"],
        )
        self.assertEqual(closed.lifecycles[0]["status"], "CLOSED_CONFIRMED")
        evidence = closed.lifecycles[0]["close_evidence"]
        self.assertEqual(evidence["entry_volume"], Decimal("0.10"))
        self.assertEqual(evidence["exit_volume"], Decimal("0.10"))

    def test_incomplete_deal_window_cannot_confirm_disappearance(self):
        self.cycle(0)
        incomplete = copy.deepcopy(self.data["cycles"][0])
        incomplete.update(
            {
                "cycle_id": "cycle-incomplete-deal-window",
                "observed_at_utc": "2026-10-30T19:00:30Z",
                "positions": [],
            }
        )
        incomplete["connection"]["deals_complete"] = False
        result = reconcile_cycle(
            self.db,
            account=self.account,
            observed_at_utc=incomplete["observed_at_utc"],
            cycle_id=incomplete["cycle_id"],
            connection=incomplete["connection"],
            account_state=incomplete["account_state"],
            orders=incomplete["orders"],
            deals=incomplete["deals"],
            positions=incomplete["positions"],
            symbol_specs=self.data["symbol_specs"],
        )
        self.assertEqual(result.lifecycles[0]["status"], "UNKNOWN")
        self.assertEqual(
            result.lifecycles[0]["close_evidence"]["reason"],
            "DEAL_HISTORY_INCOMPLETE",
        )
        self.assertNotIn("position.closed_confirmed", {row["kind"] for row in result.events})
        self.assertIn(
            "POSITION_DISAPPEARED_WITHOUT_ACCREDITED_CLOSE",
            {row["code"] for row in result.issues},
        )


if __name__ == "__main__":
    unittest.main()
