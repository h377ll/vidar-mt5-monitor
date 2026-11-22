from __future__ import annotations

from decimal import Decimal
import json
from pathlib import Path
import sqlite3
import unittest

from engine.contracts import (
    AccountKey,
    MetricResult,
    Quality,
    canonical_json,
    decimal_value,
    input_digest,
    stable_unique,
    utc_timestamp,
)
from engine.alerts import market_event_observations, position_change_alerts
from engine.metrics import (
    broker_clock_lag,
    exposure_concentration,
    max_drawdown,
    observation_duration,
    performance_statistics,
    planned_r,
    reconciliation_differences,
    realized_r,
    risk_percent,
    slippage,
    stop_risk,
)
from engine.reconciliation import (
    normalize_event,
    protection_change_events,
    reconcile_account_snapshot,
)
from engine.reporting import dashboard_rows, render_automatic_note, render_human_journal_template
from engine.schema import initialize_schema


ROOT = Path(__file__).resolve().parents[2]
FIXTURE = ROOT / "tests/fixtures/engine/synthetic-lifecycle.json"


def fixture():
    return json.loads(FIXTURE.read_text())


class ContractTests(unittest.TestCase):
    def test_decimal_rejects_boolean(self):
        with self.assertRaisesRegex(ValueError, "DECIMAL_REQUIRED"):
            decimal_value(True)

    def test_decimal_rejects_non_finite(self):
        with self.assertRaisesRegex(ValueError, "NON_FINITE"):
            decimal_value("NaN")

    def test_canonical_json_orders_keys_and_decimals(self):
        left = canonical_json({"b": Decimal("1.00"), "a": 2})
        right = canonical_json({"a": 2, "b": Decimal("1")})
        self.assertEqual(left, right)
        self.assertEqual(left, b'{"a":2,"b":"1"}')

    def test_canonical_json_rejects_float(self):
        with self.assertRaisesRegex(ValueError, "FLOAT_NOT_CANONICAL"):
            canonical_json({"bad": 1.1})

    def test_account_key_is_stable(self):
        key = AccountKey("Server", "42")
        self.assertEqual(key.storage_key(), '["Server","42"]')
        self.assertEqual(AccountKey.from_value(["Server", 42]), key)

    def test_account_key_rejects_empty(self):
        with self.assertRaisesRegex(ValueError, "ACCOUNT_IDENTITY_REQUIRED"):
            AccountKey("", "42")

    def test_timestamp_is_normalized_to_utc(self):
        self.assertEqual(utc_timestamp("2026-10-27T02:00:00+02:00"), "2026-10-27T00:00:00Z")

    def test_unavailable_metric_requires_reason(self):
        with self.assertRaisesRegex(ValueError, "REASON_REQUIRED"):
            MetricResult(Quality.UNKNOWN, None, "USD", "x.v1", input_digest({}))

    def test_stable_unique_accepts_exact_duplicate(self):
        rows = stable_unique([{"id": "1", "v": "x"}, {"v": "x", "id": "1"}], ("id",))
        self.assertEqual(rows, [{"v": "x", "id": "1"}])

    def test_stable_unique_rejects_conflict(self):
        with self.assertRaisesRegex(ValueError, "CONFLICTING_NATIVE_ID"):
            stable_unique([{"id": "1", "v": "x"}, {"id": "1", "v": "y"}], ("id",))


class ReconciliationTests(unittest.TestCase):
    def run_fixture(self, data=None):
        data = data or fixture()
        return reconcile_account_snapshot(
            account=AccountKey.from_value(data["account_key"]),
            observed_at_utc=data["observed_at_utc"],
            run_id=data["run_id"],
            connection=data["connection"],
            orders=data["orders"],
            deals=data["deals"],
            positions=data["positions"],
        )

    def test_complete_snapshot_is_known(self):
        result = self.run_fixture()
        self.assertEqual(result.quality, Quality.KNOWN)
        self.assertEqual(len(result.facts), 4)
        self.assertFalse(result.issues)

    def test_deterministic_bytes(self):
        self.assertEqual(self.run_fixture().bytes(), self.run_fixture().bytes())

    def test_input_order_does_not_change_result(self):
        data = fixture()
        data["orders"] = list(reversed(data["orders"]))
        data["deals"] = list(reversed(data["deals"]))
        self.assertEqual(self.run_fixture().bytes(), self.run_fixture(data).bytes())

    def test_disconnect_is_unknown_not_flat(self):
        data = fixture()
        data["connection"]["server_connected"] = False
        result = self.run_fixture(data)
        self.assertEqual(result.quality, Quality.UNKNOWN)
        self.assertTrue(any(f.fact_type == "position.snapshot" for f in result.facts))
        self.assertIn("TERMINAL_DISCONNECTED_EXPOSURE_UNKNOWN", {i["code"] for i in result.issues})

    def test_partial_connected_snapshot_is_estimated(self):
        data = fixture()
        data["connection"]["deals_complete"] = False
        result = self.run_fixture(data)
        self.assertEqual(result.quality, Quality.ESTIMATED)

    def test_missing_order_with_complete_history_is_error(self):
        data = fixture()
        data["orders"] = []
        result = self.run_fixture(data)
        self.assertEqual(result.quality, Quality.UNKNOWN)
        self.assertIn("DEAL_ORDER_NOT_IN_READ_WINDOW", {i["code"] for i in result.issues})

    def test_account_mismatch_is_rejected(self):
        data = fixture()
        data["positions"][0]["account_key"] = ["OTHER", "1"]
        with self.assertRaisesRegex(ValueError, "ACCOUNT_MISMATCH"):
            self.run_fixture(data)

    def test_normalize_spread_event(self):
        data = fixture()
        event = data["events"][0]
        fact = normalize_event(
            account=AccountKey.from_value(data["account_key"]),
            event_type=event["event_type"], native_id=event["native_id"],
            observed_at_utc=data["observed_at_utc"], run_id=data["run_id"], payload=event["payload"]
        )
        self.assertEqual(fact.fact_type, "market.spread")

    def test_event_missing_fields_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "MISSING_EVENT_FIELDS"):
            normalize_event(
                account=AccountKey("S", "1"), event_type="deal.swap", native_id="1",
                observed_at_utc="2026-10-27T00:00:00Z", run_id="r", payload={"swap": "1"}
            )

    def test_protection_change_is_observed(self):
        events = protection_change_events(
            account=AccountKey("S", "1"),
            previous=[{"position_identifier": "p", "sl": "1", "tp": "2"}],
            current=[{"position_identifier": "p", "sl": "1.5", "tp": "2"}],
            observed_at_utc="2026-10-27T00:00:00Z", run_id="r",
        )
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].data["after_sl"], "1.5")


class AlertDecisionTests(unittest.TestCase):
    account = AccountKey("SYNTHETIC", "1")
    observed = "2026-10-27T00:00:00Z"
    before = [{"position_identifier": "p1", "symbol": "SYN", "side": "buy", "volume": "1", "sl": "9", "tp": "12"}]

    def changes(self, current, before=None, connected_before=True, connected_after=True):
        return position_change_alerts(
            account=self.account, previous=self.before if before is None else before, current=current,
            connected_before=connected_before, connected_after=connected_after, observed_at_utc=self.observed,
        )

    def test_disconnect_does_not_fabricate_closure(self):
        result = self.changes([], connected_after=False)
        self.assertEqual([row.kind for row in result], ["coverage.terminal_disconnected"])
        self.assertEqual(result[0].data["exposure"], "UNKNOWN")

    def test_first_connected_snapshot_is_baseline_not_new_trade(self):
        result = self.changes(self.before, before=[], connected_before=None)
        self.assertEqual(result[0].kind, "coverage.baseline_observed")
        self.assertFalse(result[0].data["new_trades_demonstrated"])

    def test_new_position(self):
        current = self.before + [{"position_identifier": "p2", "symbol": "SYN", "side": "sell", "volume": "2", "sl": "11", "tp": "8"}]
        self.assertIn("position.new", {row.kind for row in self.changes(current)})

    def test_closed_position(self):
        self.assertEqual(self.changes([])[0].kind, "position.closed")

    def test_increase_and_protection_change_are_separate(self):
        current = [{"position_identifier": "p1", "symbol": "SYN", "side": "buy", "volume": "2", "sl": "9.5", "tp": "12"}]
        self.assertEqual({row.kind for row in self.changes(current)}, {"position.increased", "position.protection_changed"})

    def test_reversal_is_detected(self):
        current = [{"position_identifier": "p1", "symbol": "SYN", "side": "sell", "volume": "1", "sl": "9", "tp": "12"}]
        self.assertIn("position.reversed", {row.kind for row in self.changes(current)})

    def test_spread_observation_requires_matching_exposure(self):
        result = market_event_observations(
            account=self.account, positions=self.before, event_type="market.spread",
            payload={"symbol": "SYN", "spread_points": "12", "symbol_spec_hash": "sha256:synthetic"},
            observed_at_utc=self.observed,
        )
        self.assertEqual(result[0].kind, "market.spread_observed_with_exposure")
        self.assertFalse(result[0].data["threshold_applied"])

    def test_rollover_without_exposure_is_silent(self):
        result = market_event_observations(
            account=self.account, positions=[], event_type="account.rollover",
            payload={"server_date": "2026-10-27", "rollover_kind": "boundary"}, observed_at_utc=self.observed,
        )
        self.assertEqual(result, ())

    def test_swap_observation_links_open_position(self):
        result = market_event_observations(
            account=self.account, positions=self.before, event_type="deal.swap",
            payload={"position_identifier": "p1", "deal_ticket": "d1", "swap": "-1", "currency": "USD"},
            observed_at_utc=self.observed,
        )
        self.assertEqual(result[0].kind, "market.swap_observed")


class MetricTests(unittest.TestCase):
    def test_buy_stop_risk_known_with_costs(self):
        result = stop_risk(direction="buy", reference_price="1.25", stop_price="1.24", volume="2",
                           tick_size="0.001", tick_value_loss="3", currency="USD", cost_buffer="5")
        self.assertEqual(result.status, Quality.KNOWN)
        self.assertEqual(result.value, Decimal("65"))

    def test_sell_stop_risk(self):
        result = stop_risk(direction="sell", reference_price="1.25", stop_price="1.26", volume="2",
                           tick_size="0.001", tick_value_loss="3", currency="USD", cost_buffer="0")
        self.assertEqual(result.value, Decimal("60"))

    def test_stop_risk_without_costs_is_estimated(self):
        result = stop_risk(direction="buy", reference_price="10", stop_price="9", volume="1",
                           tick_size="1", tick_value_loss="2", currency="USD")
        self.assertEqual(result.status, Quality.ESTIMATED)
        self.assertIn("COST_BUFFER_EXCLUDED", result.reason_codes)

    def test_non_adverse_stop_is_zero(self):
        result = stop_risk(direction="buy", reference_price="10", stop_price="11", volume="1",
                           tick_size="1", tick_value_loss="2", currency="USD", cost_buffer="0")
        self.assertEqual(result.value, Decimal("0"))
        self.assertIn("STOP_NOT_ADVERSE", result.reason_codes)

    def test_missing_stop_is_unknown(self):
        result = stop_risk(direction="buy", reference_price="10", stop_price=None, volume="1",
                           tick_size="1", tick_value_loss="2", currency="USD")
        self.assertEqual(result.status, Quality.UNKNOWN)

    def test_risk_percent(self):
        result = risk_percent(risk="250", capital="10000", capital_basis="equity")
        self.assertEqual(result.value, Decimal("2.5"))

    def test_planned_r(self):
        self.assertEqual(planned_r(reward_to_target="300", initial_risk="100").value, Decimal("3"))

    def test_realized_r_requires_complete_costs(self):
        result = realized_r(net_pnl="50", initial_risk="100", costs_complete=False)
        self.assertEqual(result.status, Quality.UNVERIFIABLE)

    def test_realized_r_known(self):
        self.assertEqual(realized_r(net_pnl="-50", initial_risk="100", costs_complete=True).value, Decimal("-0.5"))

    def test_balance_drawdown_known(self):
        rows = [
            {"observed_at_utc": "2026-01-01T00:00:00Z", "value": "100"},
            {"observed_at_utc": "2026-01-02T00:00:00Z", "value": "120"},
            {"observed_at_utc": "2026-01-03T00:00:00Z", "value": "90"},
        ]
        result = max_drawdown(samples=rows, series_kind="balance")
        self.assertEqual(result.status, Quality.KNOWN)
        self.assertEqual(result.value["maximum"], Decimal("30"))

    def test_equity_drawdown_is_observed_bound(self):
        rows = [
            {"observed_at_utc": "2026-01-01T00:00:00Z", "value": "100"},
            {"observed_at_utc": "2026-01-02T00:00:00Z", "value": "90"},
        ]
        self.assertEqual(max_drawdown(samples=rows, series_kind="equity").status, Quality.ESTIMATED)

    def test_drawdown_orders_normalized_utc(self):
        rows = [
            {"observed_at_utc": "2026-01-01T02:00:00+02:00", "value": "100"},
            {"observed_at_utc": "2026-01-01T00:30:00Z", "value": "90"},
        ]
        result = max_drawdown(samples=rows, series_kind="balance")
        self.assertEqual(result.value["maximum"], Decimal("10"))
        self.assertEqual(result.value["peak_at_utc"], "2026-01-01T00:00:00Z")

    def test_drawdown_with_unresolved_cashflows_is_unverifiable(self):
        rows = [{"observed_at_utc": "a", "value": "1"}, {"observed_at_utc": "b", "value": "2"}]
        self.assertEqual(max_drawdown(samples=rows, series_kind="balance", cashflows_resolved=False).status,
                         Quality.UNVERIFIABLE)

    def test_performance_statistics_excludes_unknown(self):
        result = performance_statistics(["1", "-0.5", None, "0"])
        self.assertEqual(result.status, Quality.ESTIMATED)
        self.assertEqual(result.value["sample_size"], 3)
        self.assertEqual(result.value["expectancy_r"], Decimal("0.1666666666666666666666666667"))

    def test_buy_slippage_adverse(self):
        result = slippage(direction="buy", fill_price="1.2502", reference_price="1.25", tick_size="0.0001",
                          tick_value="10", volume="0.5", currency="USD")
        self.assertEqual(result.value["adverse_ticks"], Decimal("2"))
        self.assertEqual(result.value["adverse_money"], Decimal("10"))

    def test_slippage_without_reference_is_unverifiable(self):
        result = slippage(direction="sell", fill_price="1", reference_price=None, tick_size="0.1",
                          tick_value="1", volume="1", currency="USD")
        self.assertEqual(result.status, Quality.UNVERIFIABLE)

    def test_exposure_concentration(self):
        result = exposure_concentration([
            {"bucket": "EUR", "gross_risk": "60"},
            {"bucket": "USD", "gross_risk": "40"},
        ])
        self.assertEqual(result.value["top_1_share"], Decimal("0.6"))
        self.assertEqual(result.value["hhi"], Decimal("0.52"))

    def test_exposure_missing_is_unknown(self):
        self.assertEqual(exposure_concentration([{"bucket": "EUR", "gross_risk": None}]).status, Quality.UNKNOWN)

    def test_snapshot_read_duration_is_deterministic(self):
        result = observation_duration(
            started_at_utc="2026-10-31T16:00:00Z",
            finished_at_utc="2026-10-31T16:00:00.250000Z",
        )
        self.assertEqual(result.status, Quality.KNOWN)
        self.assertEqual(result.value, Decimal("250"))

    def test_broker_clock_lag_stays_unknown_when_api_has_no_clock(self):
        result = broker_clock_lag(observed_at_utc="2026-10-31T16:00:00Z", server_time_utc=None)
        self.assertEqual(result.status, Quality.UNKNOWN)
        self.assertIn("BROKER_CLOCK_NOT_EXPOSED", result.reason_codes)

    def test_reconciliation_difference_count_requires_complete_coverage(self):
        result = reconciliation_differences(
            coverage={
                "server_connected": True,
                "positions_complete": True,
                "orders_complete": True,
                "deals_complete": False,
            },
            issue_codes=["SOURCE_COVERAGE_INCOMPLETE"],
            observed_positions=1,
            observed_orders=1,
            observed_deals=0,
        )
        self.assertEqual(result.status, Quality.UNKNOWN)


class ProjectionAndSchemaTests(unittest.TestCase):
    def metric(self):
        return planned_r(reward_to_target="200", initial_risk="100")

    def test_human_template_contains_requested_fields(self):
        text = render_human_journal_template(trade_id="SYN-1", auto_note_relative="_auto/SYN-1.md")
        for label in (
            '"trading-journal-human.v2"',
            "Decisión",
            "Riesgo inicial revisado",
            "Riesgo vigente revisado",
            "Cambios de SL/TP",
            "Disciplina",
            "Errores",
            "Decisiones acertadas",
            "Emoción antes",
            "Emoción durante",
            "Emoción después",
            "Cansancio (0-10)",
        ):
            self.assertIn(label, text)

    def test_automatic_note_has_no_subjective_sections(self):
        text = render_automatic_note(
            trade_id="SYN-1", observed=[{"key": "deal", "source_hash": "sha256:x"}], calculated={"planned_r": self.metric()}
        )
        self.assertIn("Datos observados", text)
        self.assertNotIn("## Estado personal", text)

    def test_dashboard_rows_separate_data_classes(self):
        rows = dashboard_rows(
            observed=[{"key": "deal", "source_hash": "sha256:x"}],
            calculated={"planned_r": self.metric()}, subjective={"fatigue": "low"},
        )
        self.assertEqual({row["data_class"] for row in rows}, {"OBSERVED", "CALCULATED", "SUBJECTIVE"})

    def test_schema_is_additive_and_idempotent(self):
        db = sqlite3.connect(":memory:")
        self.assertEqual(initialize_schema(db, "2026-10-27T00:00:00Z"), 1)
        self.assertEqual(initialize_schema(db, "2026-10-27T00:00:01Z"), 1)
        tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        self.assertTrue({"broker_orders", "broker_deals", "position_versions", "metric_values"} <= tables)
        self.assertEqual(db.execute("SELECT count(*) FROM engine_schema").fetchone()[0], 1)


if __name__ == "__main__":
    unittest.main()
