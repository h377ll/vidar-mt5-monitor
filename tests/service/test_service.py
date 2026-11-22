from __future__ import annotations

import copy
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import unittest

from engine.contracts import AccountKey, canonical_json, input_digest
from engine.supervisor import TEST_VAULT_MARKER, initialize_supervisor_schema, reconcile_cycle
from phase_b import event_receiver, journal
from provider.mt5_mcp_readonly import (
    READ_TOOLS,
    normalize_mcp_open_observation_fixture,
    normalize_mcp_read_fixture,
)
from provider.mt5_mcp_projection_experimental import (
    FILTERED_READ_TOOLS,
    normalize_filtered_mcp_fixture,
)
from provider.mt5_readonly import RpycReadonlyClient, capture_snapshot
from provider.verify_private_network import validate_private_network
from service.backup import backup_database, restore_to_new_path
from service.contracts import adapter_capture, sha256_file, validate_snapshot
from service.outbox import dispatch_one, recover_attempting
from service.offline_mcp_open_observation import run_offline_mcp_open_observation
from service.runtime import bridge_once, engine_once, health_once


ROOT = Path(__file__).resolve().parents[2]
CLI = ROOT / "src/service/cli.py"
ACCOUNT = AccountKey("SYNTHETIC-DEMO", "900001")
MCP_ACCOUNT = AccountKey("SYNTHETIC-MCP", "990001")
MCP_FIXTURE = ROOT / "tests/fixtures/provider/mcp-readonly-complete.json"
SOURCE = "demo-c"


def timestamp(offset: int = 0) -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=offset)).isoformat().replace("+00:00", "Z")


def valid_snapshot(*, observed: str | None = None, positions=None, deals=None, connected=True):
    observed = observed or timestamp()
    moment = datetime.fromisoformat(observed.replace("Z", "+00:00"))
    start = (moment - timedelta(milliseconds=100)).isoformat().replace("+00:00", "Z")
    finish = (moment + timedelta(milliseconds=100)).isoformat().replace("+00:00", "Z")
    positions = copy.deepcopy(positions if positions is not None else [
        {
            "position_identifier": "6001",
            "symbol": "SYNTHUSD",
            "side": "buy",
            "volume": "0.10",
            "price_open": "1.10010",
            "current_price": "1.10110",
            "sl": "1.09510",
            "tp": "1.11010",
            "cost_buffer": "0",
            "opened_at_utc": start,
        }
    ])
    orders = [
        {"order_ticket": "1001", "position_identifier": "6001", "symbol": "SYNTHUSD", "state": "filled"}
    ]
    deals = copy.deepcopy(deals if deals is not None else [
        {
            "deal_ticket": "2001",
            "order_ticket": "1001",
            "position_identifier": "6001",
            "entry": "in",
            "direction": "buy",
            "volume": "0.10",
            "price": "1.10010",
            "requested_price": "1.10000",
            "profit": "0",
            "commission": "0",
            "swap": "0",
            "fee": "0",
        }
    ])
    spec_basis = {
        "tick_size": "0.00010",
        "tick_value_loss": "10",
        "tick_value_profit": "10",
        "currency": "USD",
    }
    specs = {"SYNTHUSD": {**spec_basis, "spec_hash": input_digest(spec_basis)}}
    window = [start, finish]
    coverage = {}
    for name, payload, termination in (
        ("positions", positions, "enumerated"),
        ("orders", orders, "exhausted"),
        ("deals", deals, "exhausted"),
        ("symbol_specs", specs, "enumerated"),
    ):
        row = {
            "complete": bool(connected),
            "pages": 1 if connected else 0,
            "termination": termination if connected else "interrupted",
            "content_hash": input_digest(payload),
        }
        if name in {"orders", "deals"}:
            row.update({"requested_window": window, "covered_window": window})
        coverage[name] = row
    identity = {
        "server": ACCOUNT.server,
        "login": ACCOUNT.login,
        "trade_mode": "demo",
        "connected": bool(connected),
        "terminal_build": 6230,
        "server_time_utc": observed,
        "terminal_trade_allowed": False,
        "account_trade_allowed": False,
        "account_trade_expert": False,
    }
    return {
        "schema_version": "vidar.mt5.readonly-snapshot.v1",
        "read_started_at_utc": start,
        "observed_at_utc": observed,
        "read_finished_at_utc": finish,
        "identity_before": identity,
        "identity_after": dict(identity),
        "coverage": coverage,
        "account_state": {
            "currency": "USD",
            "balance": "10000",
            "equity": "10000",
            "capital_basis": "equity",
            "cashflows_resolved": True,
        },
        "positions": positions,
        "orders": orders,
        "deals": deals,
        "symbol_specs": specs,
    }


def envelope(stream: str, seq: int = 1):
    return {
        "source": SOURCE,
        "stream": stream,
        "seq": seq,
        "server": ACCOUNT.server,
        "login": ACCOUNT.login,
        "kind": "transaction",
        "payload": {"order_id": "1001", "deal_id": "2001", "position_ticket": "6001"},
    }


def mcp_fixture():
    return json.loads(MCP_FIXTURE.read_text(encoding="utf-8"))


def mcp_open_fixture(*, captured_at="2026-11-05T04:30:00Z", volume="0.25", stop_loss="1.1900"):
    account = {
        "account": {
            "server": MCP_ACCOUNT.server,
            "login": MCP_ACCOUNT.login,
            "type": "real",
            "currency": "USD",
            "balance": "100000",
            "equity": "100100",
            "margin": "2500",
            "margin_free": "97600",
            "profit": "100",
        },
        "terminal": {
            "build": 6230,
            "server_connected": True,
            "experts_trade_allowed": False,
            "mcp_trade_allowed": True,
        },
    }
    return {
        "schema_version": "vidar.mt5.mcp-open-observation-fixture.v1",
        "capture_transport": "openclaw_tool_projection",
        "direct_endpoint_access": False,
        "tools_used": [
            "get_workspace_info",
            "get_time_information",
            "get_trading_account_info",
            "get_trading_open_positions",
            "get_marketwatch_symbols",
        ],
        "captured_at_utc": captured_at,
        "account_before": copy.deepcopy(account),
        "time": {
            "utc_time": captured_at,
            "local_time": "2026-11-05T01:30:00",
            "local_utc_offset_minutes": 0,
            "trade_server_last_known_time": "2026-11-05T07:30:00",
        },
        "open_positions": {
            "positions_complete": True,
            "orders_complete": True,
            "coverage_basis": "MCP_CURRENT_STATE_CALL_SUCCEEDED",
            "positions": [
                {
                    "position_id": "9201",
                    "action": "buy",
                    "symbol": "SYNUSD",
                    "create_time": "2026-11-05T07:00:00",
                    "update_time": "2026-11-05T07:00:00",
                    "volume": volume,
                    "price_open": "1.2000",
                    "price_last": "1.2010",
                    "stop_loss": stop_loss,
                    "take_profit": "1.2200",
                }
            ],
            "orders": [],
        },
        "symbol_specs": [
            {
                "symbol": "SYNUSD",
                "tick_size": "0.0001",
                "tick_value": "10",
                "tick_value_loss": "10",
                "tick_value_profit": "10",
                "contract_size": "100000",
                "currency_base": "SYN",
                "currency_profit": "USD",
            }
        ],
        "account_after": copy.deepcopy(account),
    }


def mcp_filtered_fixture(*, mcp_trade_allowed=True):
    account = {
        "account": {
            "server": MCP_ACCOUNT.server,
            "login": MCP_ACCOUNT.login,
            "type": "demo",
            "read_only": False,
            "currency": "USD",
            "balance": "100000",
            "equity": "100000",
            "margin": "0",
            "margin_free": "100000",
            "profit": "0",
        },
        "terminal": {
            "build": 6257,
            "server_connected": True,
            "experts_dll_allowed": False,
            "experts_trade_allowed": False,
            "mcp_trade_allowed": mcp_trade_allowed,
        },
    }
    window = {
        "datetime_from_server_raw": "2026-11-01T00:00:00",
        "datetime_to_server_raw": "2026-11-06T00:00:00",
        "limit": 1000,
    }
    history_arguments = {
        "datetime_from": window["datetime_from_server_raw"],
        "datetime_to": window["datetime_to_server_raw"],
        "limit": window["limit"],
    }
    return {
        "schema_version": "vidar.mt5.mcp-filtered-fixture.v1",
        "capture_transport": "openclaw_tool_projection",
        "direct_endpoint_access": False,
        "captured_at_utc": "2026-11-05T18:20:00Z",
        "history_window": window,
        "calls": [
            {
                "tool": "get_trading_account_info",
                "phase": "before",
                "arguments": {},
                "response": copy.deepcopy(account),
            },
            {
                "tool": "get_trading_open_positions",
                "arguments": {"include_orders": True},
                "response": {"positions": [], "orders": []},
            },
            {
                "tool": "get_trading_history_orders",
                "arguments": {
                    **history_arguments,
                    "include_deals": True,
                    "include_orders": True,
                    "include_orders_canceled": True,
                },
                "response": {
                    "orders": [],
                    "deals": [
                        {
                            "deal_id": "30000000001",
                            "deal_external_id": "",
                            "order_id": "0",
                            "position_id": "0",
                            "symbol": "",
                            "action": "balance",
                            "entry": "in",
                            "reason": "Dealer",
                            "open_time": "2026-11-05T20:06:07",
                            "profit": "100000",
                            "comment": "",
                        }
                    ],
                },
            },
            {
                "tool": "get_trading_history_positions",
                "arguments": history_arguments,
                "response": {"positions": []},
            },
            {
                "tool": "get_trading_account_info",
                "phase": "after",
                "arguments": {},
                "response": copy.deepcopy(account),
            },
        ],
    }


class FakeMt5Client:
    def __init__(self, *, trade_mode=0, deals_available=True, permission_states=None):
        self.trade_mode = trade_mode
        self.deals_available = deals_available
        self.permission_states = list(permission_states or [{}])
        self.identity_calls = 0
        self.stopped = False

    def initialize(self):
        return True

    def identity(self):
        state = self.permission_states[min(self.identity_calls, len(self.permission_states) - 1)]
        self.identity_calls += 1
        return (
            {
                "connected": True,
                "build": 6230,
                "trade_allowed": state.get("terminal_trade_allowed", False),
            },
            {
                "server": ACCOUNT.server,
                "login": int(ACCOUNT.login),
                "trade_mode": self.trade_mode,
                "trade_allowed": state.get("account_trade_allowed", False),
                "trade_expert": state.get("account_trade_expert", False),
                "currency": "USD",
                "balance": 10000.0,
                "equity": 9990.5,
            },
            [500, 6230, "test"],
        )

    def positions_get(self):
        return [
            {
                "identifier": 6001,
                "ticket": 6001,
                "symbol": "SYNTHUSD",
                "type": 0,
                "volume": 0.1,
                "price_open": 1.1001,
                "price_current": 1.1011,
                "sl": 1.0951,
                "tp": 1.1101,
                "swap": 0.0,
                "profit": 10.0,
                "time": 1793443200,
                "time_msc": 1793443200000,
                "time_update_msc": 1793443201000,
            }
        ]

    def orders_get(self):
        return []

    def history_orders_get(self, _start, _finish):
        return [
            {
                "ticket": 1001,
                "position_id": 6001,
                "symbol": "SYNTHUSD",
                "type": 0,
                "state": 4,
                "volume_initial": 0.1,
                "volume_current": 0.0,
                "price_open": 1.1001,
                "price_current": 1.1001,
                "sl": 1.0951,
                "tp": 1.1101,
                "time_setup_msc": 1793443200000,
                "time_done_msc": 1793443200100,
            }
        ]

    def history_deals_get(self, _start, _finish):
        if not self.deals_available:
            return None
        return [
            {
                "ticket": 2001,
                "order": 1001,
                "position_id": 6001,
                "symbol": "SYNTHUSD",
                "entry": 0,
                "type": 0,
                "volume": 0.1,
                "price": 1.1001,
                "profit": 0.0,
                "commission": -0.5,
                "swap": 0.0,
                "fee": 0.0,
                "time_msc": 1793443200100,
            }
        ]

    def symbol_info(self, _symbol):
        return {
            "trade_tick_size": 0.0001,
            "trade_tick_value_loss": 10.0,
            "trade_tick_value_profit": 10.0,
            "currency_base": "SYN",
            "currency_profit": "USD",
            "currency_margin": "USD",
            "trade_contract_size": 100000.0,
            "trade_calc_mode": 0,
            "digits": 5,
            "point": 0.00001,
        }

    def last_error(self):
        return [10001, "synthetic endpoint unavailable"]

    def shutdown(self):
        self.stopped = True


class ServiceTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="vidar-service-test-")
        self.root = Path(self.temporary.name)
        self.database = self.root / "supervisor.sqlite"
        self.identity = self.root / "identity.json"
        self.identity.write_text(
            json.dumps({"source": SOURCE, "server": ACCOUNT.server, "login": ACCOUNT.login}), encoding="utf-8"
        )
        self.key = self.root / "hmac.key"
        self.key.write_bytes(bytes(range(32)))
        self.key.chmod(0o600)

    def tearDown(self):
        self.temporary.cleanup()

    def write_snapshot(self, data) -> Path:
        path = self.root / "snapshot.json"
        path.write_bytes(canonical_json(data) + b"\n")
        return path

    def executable(self, name: str, body: str) -> Path:
        path = self.root / name
        path.write_text("#!/usr/bin/python3\n" + body, encoding="utf-8")
        path.chmod(0o700)
        return path

    def test_01_snapshot_contract_accepts_complete_demo_read(self):
        result = validate_snapshot(valid_snapshot(), ACCOUNT)
        self.assertTrue(result.connection["server_connected"])
        self.assertTrue(result.connection["deals_complete"])
        self.assertTrue(result.source_hash.startswith("sha256:"))

    def test_02_snapshot_contract_rejects_real_account(self):
        data = valid_snapshot()
        data["identity_after"]["trade_mode"] = "real"
        with self.assertRaisesRegex(ValueError, "NOT_DEMO"):
            validate_snapshot(data, ACCOUNT)

    def test_03_snapshot_contract_rejects_identity_change(self):
        data = valid_snapshot()
        data["identity_after"]["login"] = "900002"
        with self.assertRaisesRegex(ValueError, "IDENTITY_AFTER_MISMATCH"):
            validate_snapshot(data, ACCOUNT)

    def test_03b_snapshot_contract_rejects_missing_trade_permission_proof(self):
        data = valid_snapshot()
        del data["identity_before"]["account_trade_expert"]
        with self.assertRaisesRegex(ValueError, "ACCOUNT_TRADE_EXPERT_NOT_FALSE"):
            validate_snapshot(data, ACCOUNT)

    def test_04_snapshot_contract_rejects_unproved_completeness(self):
        data = valid_snapshot()
        data["coverage"]["deals"]["pages"] = 0
        with self.assertRaisesRegex(ValueError, "COMPLETENESS_UNPROVEN"):
            validate_snapshot(data, ACCOUNT)

    def test_05_snapshot_contract_rejects_content_hash_conflict(self):
        data = valid_snapshot()
        data["positions"][0]["volume"] = "9"
        with self.assertRaisesRegex(ValueError, "HASH_MISMATCH"):
            validate_snapshot(data, ACCOUNT)

    def test_06_disconnected_snapshot_is_unknown_not_flat(self):
        result = validate_snapshot(valid_snapshot(positions=[], deals=[], connected=False), ACCOUNT)
        self.assertFalse(result.connection["server_connected"])
        self.assertFalse(result.connection["positions_complete"])

    def test_07_adapter_executes_only_hash_pinned_provider(self):
        raw = canonical_json(valid_snapshot()).decode("utf-8")
        provider = self.executable("provider", f"import sys\nsys.stdin.read()\nsys.stdout.write({raw!r})\n")
        config = {
            "enabled": True,
            "expected_server": ACCOUNT.server,
            "expected_login": ACCOUNT.login,
            "command": [str(provider)],
            "command_sha256": sha256_file(provider),
            "timeout_seconds": 5,
        }
        destination = self.root / "captured.json"
        result = adapter_capture(config, destination)
        self.assertEqual(validate_snapshot(json.loads(destination.read_text()), ACCOUNT).source_hash, result.source_hash)

    def test_08_adapter_rejects_provider_hash_drift(self):
        provider = self.executable("provider", "print('{}')\n")
        config = {
            "enabled": True,
            "expected_server": ACCOUNT.server,
            "expected_login": ACCOUNT.login,
            "command": [str(provider)],
            "command_sha256": "0" * 64,
        }
        with self.assertRaisesRegex(ValueError, "SHA256_MISMATCH"):
            adapter_capture(config, self.root / "captured.json")

    def test_09_bridge_discovers_multiple_streams_and_partial_line(self):
        server = event_receiver.make_server(
            self.database, {SOURCE: (self.key.read_bytes(), ACCOUNT.server, ACCOUNT.login)}
        )
        thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
        thread.start()
        spool = self.root / "spool"
        cursors = self.root / "cursors"
        spool.mkdir()
        for stream in ("boot-a", "boot-b"):
            (spool / f"VidarObserver-{SOURCE}-{stream}.jsonl").write_bytes(canonical_json(envelope(stream)) + b"\n")
        partial = envelope("boot-c")
        (spool / f"VidarObserver-{SOURCE}-boot-c.jsonl").write_bytes(canonical_json(partial))
        try:
            result = bridge_once(
                spool_directory=spool,
                cursor_directory=cursors,
                endpoint=f"http://127.0.0.1:{server.server_port}/v1/events",
                credential=self.key,
                expected_source=SOURCE,
            )
        finally:
            server.shutdown()
            thread.join(timeout=5)
            server.server_close()
        self.assertEqual(result["acknowledged_lines"], 2)
        self.assertEqual(result["waiting_partial_files"], 1)
        db = journal.open_db(self.database)
        try:
            self.assertEqual(db.execute("SELECT count(*) FROM ingress").fetchone()[0], 2)
        finally:
            db.close()

    def test_10_bridge_rejects_filename_identity_mismatch(self):
        spool = self.root / "spool"
        spool.mkdir()
        (spool / f"VidarObserver-{SOURCE}-wrong.jsonl").write_bytes(canonical_json(envelope("actual")) + b"\n")
        with self.assertRaisesRegex(ValueError, "FILENAME_IDENTITY_MISMATCH"):
            bridge_once(
                spool_directory=spool,
                cursor_directory=self.root / "cursors",
                endpoint="http://127.0.0.1:18888/v1/events",
                credential=self.key,
                expected_source=SOURCE,
            )

    def test_11_engine_combines_inbox_and_snapshot(self):
        db = journal.open_db(self.database)
        try:
            receiver = event_receiver.Receiver(db, {SOURCE: (self.key.read_bytes(), ACCOUNT.server, ACCOUNT.login)})
            body = canonical_json(envelope("boot-a"))
            headers = event_receiver.signed_headers(self.key.read_bytes(), body, int(time.time()), "n" * 16)
            receiver.accept(body, headers)
        finally:
            db.close()
        snapshot = self.write_snapshot(valid_snapshot())
        request = self.root / "reconcile-request.json"
        result = engine_once(
            database=self.database,
            snapshot_path=snapshot,
            identity_config=self.identity,
            reconcile_request_path=request,
        )
        self.assertEqual(result["state"], "reconciled")
        self.assertEqual(result["ingress"]["stored"], 1)
        self.assertTrue(result["reconciliation_requested"])
        self.assertEqual(json.loads(request.read_text())["observer_events_total"], 1)
        self.assertEqual(result["cycle"]["reconciliation_quality"], "KNOWN")

    def test_12_engine_replay_is_idempotent(self):
        snapshot = self.write_snapshot(valid_snapshot())
        first = engine_once(database=self.database, snapshot_path=snapshot, identity_config=self.identity)
        second = engine_once(database=self.database, snapshot_path=snapshot, identity_config=self.identity)
        self.assertEqual(first["cycle"], second["cycle"])
        db = journal.open_db(self.database)
        try:
            self.assertEqual(db.execute("SELECT count(*) FROM supervisor_cycles").fetchone()[0], 1)
        finally:
            db.close()

    def test_13_state_change_and_outbox_are_atomic(self):
        first = valid_snapshot(observed=timestamp(-2))
        self.write_snapshot(first)
        engine_once(database=self.database, snapshot_path=self.root / "snapshot.json", identity_config=self.identity)
        second = valid_snapshot(observed=timestamp(-1))
        extra = copy.deepcopy(second["positions"][0])
        extra["position_identifier"] = "6002"
        second["positions"].append(extra)
        second["coverage"]["positions"]["content_hash"] = input_digest(second["positions"])
        self.write_snapshot(second)
        engine_once(database=self.database, snapshot_path=self.root / "snapshot.json", identity_config=self.identity)
        db = journal.open_db(self.database)
        try:
            row = db.execute(
                "SELECT e.event_kind,o.state FROM supervisor_events e JOIN supervisor_outbox o USING(event_id)"
            ).fetchone()
            self.assertEqual(tuple(row), ("position.open_observed", "pending"))
        finally:
            db.close()

    def _database_with_pending(self):
        db = journal.open_db(self.database)
        initialize_supervisor_schema(db, timestamp(-2))
        first = valid_snapshot(observed=timestamp(-2))
        bundle = validate_snapshot(first, ACCOUNT)
        reconcile_cycle(
            db, account=ACCOUNT, observed_at_utc=bundle.observed_at_utc, cycle_id=bundle.cycle_id,
            connection=bundle.connection, account_state=bundle.account_state, orders=bundle.orders,
            deals=bundle.deals, positions=bundle.positions, symbol_specs=bundle.symbol_specs,
        )
        second = valid_snapshot(observed=timestamp(-1))
        second["positions"][0]["sl"] = "1.09610"
        second["coverage"]["positions"]["content_hash"] = input_digest(second["positions"])
        bundle = validate_snapshot(second, ACCOUNT)
        reconcile_cycle(
            db, account=ACCOUNT, observed_at_utc=bundle.observed_at_utc, cycle_id=bundle.cycle_id,
            connection=bundle.connection, account_state=bundle.account_state, orders=bundle.orders,
            deals=bundle.deals, positions=bundle.positions, symbol_specs=bundle.symbol_specs,
        )
        return db

    def test_14_dispatch_persists_provider_receipt(self):
        db = self._database_with_pending()
        try:
            state = dispatch_one(
                db,
                lambda message, event_id: {"provider_message_id": "SIM-" + event_id[:8], "simulated": True},
                at_utc=timestamp(),
            )
            self.assertEqual(state, "delivered")
            row = db.execute("SELECT state,provider_message_id FROM supervisor_outbox").fetchone()
            self.assertEqual(row["state"], "delivered")
            self.assertTrue(row["provider_message_id"].startswith("SIM-"))
        finally:
            db.close()

    def test_15_ambiguous_delivery_becomes_uncertain_without_retry(self):
        db = self._database_with_pending()
        try:
            self.assertEqual(
                dispatch_one(db, lambda _message, _event: (_ for _ in ()).throw(TimeoutError()), at_utc=timestamp()),
                "uncertain",
            )
            self.assertEqual(dispatch_one(db, lambda _message, _event: {}, at_utc=timestamp()), "empty")
        finally:
            db.close()

    def test_16_restart_recovers_attempting_as_uncertain(self):
        db = self._database_with_pending()
        try:
            with db:
                db.execute("UPDATE supervisor_outbox SET state='attempting'")
            self.assertEqual(recover_attempting(db, at_utc=timestamp()), 1)
            self.assertEqual(db.execute("SELECT state FROM supervisor_outbox").fetchone()[0], "uncertain")
        finally:
            db.close()

    def test_17_sqlite_backup_includes_committed_wal(self):
        db = sqlite3.connect(self.database)
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("CREATE TABLE sample(value TEXT)")
        db.execute("INSERT INTO sample VALUES('committed-in-wal')")
        db.commit()
        destination = self.root / "backup.sqlite"
        result = backup_database(self.database, destination)
        self.assertEqual(result["quick_check"], "ok")
        check = sqlite3.connect(destination)
        try:
            self.assertEqual(check.execute("SELECT value FROM sample").fetchone()[0], "committed-in-wal")
        finally:
            check.close()
            db.close()

    def test_18_restore_requires_new_path_and_preserves_rows(self):
        db = sqlite3.connect(self.database)
        db.execute("CREATE TABLE sample(value INTEGER)")
        db.execute("INSERT INTO sample VALUES(7)")
        db.commit()
        db.close()
        backup = self.root / "backup.sqlite"
        backup_database(self.database, backup)
        restored = self.root / "restored.sqlite"
        restore_to_new_path(backup, restored)
        self.assertEqual(sqlite3.connect(restored).execute("SELECT value FROM sample").fetchone()[0], 7)
        with self.assertRaises(FileExistsError):
            restore_to_new_path(backup, restored)

    def test_19_health_reports_fresh_database_and_snapshot(self):
        snapshot = self.write_snapshot(valid_snapshot())
        engine_once(database=self.database, snapshot_path=snapshot, identity_config=self.identity)
        result = health_once(database=self.database, snapshot_path=snapshot, identity_config=self.identity)
        self.assertEqual(result["state"], "ok")
        self.assertEqual(result["quick_check"], "ok")

    def test_20_health_marks_stale_snapshot_degraded(self):
        snapshot = self.write_snapshot(valid_snapshot(observed=timestamp(-120)))
        engine_once(database=self.database, snapshot_path=snapshot, identity_config=self.identity)
        result = health_once(
            database=self.database,
            snapshot_path=snapshot,
            identity_config=self.identity,
            maximum_snapshot_age_seconds=30,
        )
        self.assertEqual(result["state"], "degraded")
        self.assertIn("SNAPSHOT_STALE_OR_FUTURE", result["reasons"])

    def test_21_receiver_cli_stops_cleanly_on_sigterm(self):
        probe = socket.socket()
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
        probe.close()
        process = subprocess.Popen(
            [
                sys.executable,
                "-B",
                str(CLI),
                "receiver",
                "--db",
                str(self.database),
                "--identity-config",
                str(self.identity),
                "--credential",
                str(self.key),
                "--port",
                str(port),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=ROOT,
        )
        try:
            deadline = time.time() + 5
            while time.time() < deadline:
                with socket.socket() as client:
                    if client.connect_ex(("127.0.0.1", port)) == 0:
                        break
                time.sleep(0.05)
            else:
                self.fail("receiver did not bind")
            process.send_signal(signal.SIGTERM)
            stdout, stderr = process.communicate(timeout=5)
            self.assertEqual(process.returncode, 0, stderr.decode())
            self.assertEqual(json.loads(stdout)["state"], "stopped")
        finally:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=5)

    def test_22_adapter_event_trigger_preempts_periodic_interval(self):
        first = canonical_json(valid_snapshot(observed=timestamp())).decode("utf-8")
        second = canonical_json(valid_snapshot(observed=timestamp(1))).decode("utf-8")
        counter = self.root / "provider-count"
        provider = self.executable(
            "trigger-provider",
            (
                "import pathlib,sys\n"
                f"p=pathlib.Path({str(counter)!r})\n"
                "n=int(p.read_text())+1 if p.exists() else 1\n"
                "p.write_text(str(n))\n"
                "sys.stdin.read()\n"
                f"sys.stdout.write({first!r} if n==1 else {second!r})\n"
            ),
        )
        config = self.root / "adapter.json"
        config.write_text(
            json.dumps(
                {
                    "enabled": True,
                    "expected_server": ACCOUNT.server,
                    "expected_login": ACCOUNT.login,
                    "command": [str(provider)],
                    "command_sha256": sha256_file(provider),
                    "timeout_seconds": 5,
                }
            ),
            encoding="utf-8",
        )
        captured = self.root / "triggered-snapshot.json"
        trigger = self.root / "reconcile-request.json"
        process = subprocess.Popen(
            [
                sys.executable,
                "-B",
                str(CLI),
                "adapter",
                "--config",
                str(config),
                "--snapshot",
                str(captured),
                "--trigger",
                str(trigger),
                "--interval-seconds",
                "60",
                "--poll-seconds",
                "0.1",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=ROOT,
        )
        try:
            deadline = time.time() + 5
            while time.time() < deadline and (not counter.exists() or counter.read_text() != "1"):
                time.sleep(0.05)
            self.assertEqual(counter.read_text(), "1")
            trigger.write_text(json.dumps({"schema_version": "vidar.mt5.reconcile-request.v1", "id": "event-1"}))
            deadline = time.time() + 5
            while time.time() < deadline and counter.read_text() != "2":
                time.sleep(0.05)
            self.assertEqual(counter.read_text(), "2")
            process.send_signal(signal.SIGTERM)
            stdout, stderr = process.communicate(timeout=5)
            self.assertEqual(process.returncode, 0, stderr.decode())
            self.assertEqual(json.loads(stdout)["state"], "captured")
            self.assertEqual(json.loads(captured.read_text())["observed_at_utc"], json.loads(second)["observed_at_utc"])
        finally:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=5)

    def test_23_readonly_provider_normalizes_complete_demo_snapshot(self):
        client = FakeMt5Client()
        moments = iter(
            [
                datetime(2026, 10, 31, 16, 0, 0, tzinfo=timezone.utc),
                datetime(2026, 10, 31, 16, 0, 0, 250000, tzinfo=timezone.utc),
            ]
        )
        snapshot = capture_snapshot(
            client,
            expected={"server": ACCOUNT.server, "login": ACCOUNT.login},
            clock=lambda: next(moments),
        )
        validated = validate_snapshot(snapshot, ACCOUNT)
        self.assertTrue(validated.connection["positions_complete"])
        self.assertEqual(snapshot["account_state"]["equity"], "9990.5")
        self.assertFalse(snapshot["account_state"]["cashflows_resolved"])
        self.assertEqual(snapshot["connectivity_indicators"]["read_duration_ms"], 250)
        self.assertNotIn("requested_price", snapshot["deals"][0])
        self.assertTrue(client.stopped)

    def test_24_readonly_provider_rejects_non_demo_identity(self):
        client = FakeMt5Client(trade_mode=2)
        with self.assertRaisesRegex(PermissionError, "NOT_DEMO"):
            capture_snapshot(
                client,
                expected={"server": ACCOUNT.server, "login": ACCOUNT.login},
            )
        self.assertTrue(client.stopped)

    def test_24b_readonly_provider_rejects_each_enabled_trade_permission(self):
        for field in (
            "terminal_trade_allowed",
            "account_trade_allowed",
            "account_trade_expert",
        ):
            with self.subTest(field=field):
                client = FakeMt5Client(permission_states=[{field: True}])
                with self.assertRaisesRegex(PermissionError, field.upper()):
                    capture_snapshot(
                        client,
                        expected={"server": ACCOUNT.server, "login": ACCOUNT.login},
                    )
                self.assertTrue(client.stopped)

    def test_24c_readonly_provider_rejects_unknown_trade_permission(self):
        client = FakeMt5Client(permission_states=[{"terminal_trade_allowed": None}])
        with self.assertRaisesRegex(PermissionError, "TERMINAL_TRADE_ALLOWED_NOT_FALSE"):
            capture_snapshot(
                client,
                expected={"server": ACCOUNT.server, "login": ACCOUNT.login},
            )
        self.assertTrue(client.stopped)

    def test_24d_readonly_provider_rejects_permission_change_after_read(self):
        client = FakeMt5Client(
            permission_states=[
                {},
                {"account_trade_expert": True},
            ]
        )
        with self.assertRaisesRegex(PermissionError, "AFTER_ACCOUNT_TRADE_EXPERT_NOT_FALSE"):
            capture_snapshot(
                client,
                expected={"server": ACCOUNT.server, "login": ACCOUNT.login},
            )
        self.assertTrue(client.stopped)

    def test_25_readonly_provider_marks_failed_history_incomplete(self):
        client = FakeMt5Client(deals_available=False)
        snapshot = capture_snapshot(
            client,
            expected={"server": ACCOUNT.server, "login": ACCOUNT.login},
        )
        validated = validate_snapshot(snapshot, ACCOUNT)
        self.assertFalse(validated.connection["deals_complete"])
        self.assertEqual(snapshot["connectivity_indicators"]["endpoint_error_codes"]["history_deals"], 10001)

    def test_26_rpyc_provider_rejects_non_loopback_before_import(self):
        with self.assertRaisesRegex(ValueError, "LOOPBACK"):
            RpycReadonlyClient("192.0.2.1", 18812, 5)

    def test_27_adapter_passes_expected_identity_and_provider_settings(self):
        raw = canonical_json(valid_snapshot()).decode("utf-8")
        request_file = self.root / "provider-request.json"
        provider = self.executable(
            "request-provider",
            (
                "import pathlib,sys\n"
                f"p=pathlib.Path({str(request_file)!r})\n"
                "request=sys.stdin.read()\n"
                "p.write_text(request)\n"
                f"sys.stdout.write({raw!r})\n"
            ),
        )
        config = {
            "enabled": True,
            "expected_server": ACCOUNT.server,
            "expected_login": ACCOUNT.login,
            "command": [str(provider)],
            "command_sha256": sha256_file(provider),
            "provider": {"host": "127.0.0.1", "port": 18812},
        }
        adapter_capture(config, self.root / "provided.json")
        request = json.loads(request_file.read_text())
        self.assertEqual(request["expected_account"], {"server": ACCOUNT.server, "login": ACCOUNT.login})
        self.assertEqual(request["provider"]["port"], 18812)

    def test_28_readonly_provider_source_has_no_trading_primitive(self):
        source = (ROOT / "src/provider/mt5_readonly.py").read_text(encoding="utf-8")
        for primitive in ("order_send", "order_check", "position_close", "position_modify", "trade_action"):
            self.assertNotIn(primitive, source.lower())

    def test_29_synthetic_provider_to_sqlite_engine_e2e(self):
        snapshot = valid_snapshot(observed="2026-10-31T16:05:00Z")
        raw = canonical_json(snapshot).decode("utf-8")
        provider = self.executable(
            "synthetic-e2e-provider",
            f"import sys\nsys.stdin.read()\nsys.stdout.write({raw!r})\n",
        )
        config = {
            "enabled": True,
            "expected_server": ACCOUNT.server,
            "expected_login": ACCOUNT.login,
            "command": [str(provider)],
            "command_sha256": sha256_file(provider),
            "provider": {"synthetic": True},
        }
        destination = self.root / "e2e-snapshot.json"
        adapter_capture(config, destination, request_reason={"kind": "synthetic_acceptance"})
        result = engine_once(
            database=self.database,
            snapshot_path=destination,
            identity_config=self.identity,
        )
        self.assertEqual(result["state"], "reconciled")
        db = journal.open_db(self.database)
        try:
            row = db.execute(
                """SELECT status,value_json FROM supervisor_metrics
                   WHERE scope_key='instrument:SYNTHUSD' AND metric_name='aggregate_current_risk'"""
            ).fetchone()
            self.assertEqual(row["status"], "KNOWN")
            self.assertEqual(json.loads(row["value_json"]), "60")
        finally:
            db.close()

    def test_30_private_network_gate_accepts_distinct_loopback_only_namespace(self):
        validate_private_network(self_namespace_inode=200, host_namespace_inode=100, interfaces={"lo"})

    def test_31_private_network_gate_rejects_host_namespace(self):
        with self.assertRaisesRegex(PermissionError, "NAMESPACE_IS_HOST"):
            validate_private_network(self_namespace_inode=100, host_namespace_inode=100, interfaces={"lo"})

    def test_32_private_network_gate_rejects_additional_interface(self):
        with self.assertRaisesRegex(PermissionError, "NOT_LOOPBACK_ONLY"):
            validate_private_network(
                self_namespace_inode=200,
                host_namespace_inode=100,
                interfaces={"lo", "eth0"},
            )

    def test_33_mcp_fixture_normalizer_is_deterministic_and_separates_live_history(self):
        fixture = mcp_fixture()
        first = normalize_mcp_read_fixture(fixture, expected_account=MCP_ACCOUNT)
        second = normalize_mcp_read_fixture(copy.deepcopy(fixture), expected_account=MCP_ACCOUNT)
        self.assertEqual(canonical_json(first), canonical_json(second))
        self.assertEqual(
            [row["position_id"] for row in first["live_observation"]["positions"]],
            ["9201"],
        )
        self.assertEqual(
            [row["position_id"] for row in first["historical_observation"]["lifecycles"]],
            ["9101"],
        )
        self.assertEqual(first["live_observation"]["observation_mode"], "live_snapshot")
        self.assertEqual(
            first["historical_observation"]["observation_mode"],
            "historical_backfill_candidate",
        )
        self.assertIsNone(first["historical_observation"]["deals"][0]["commission"])
        self.assertIsNone(first["historical_observation"]["deals"][0]["fee"])
        self.assertIsNone(first["time"]["event_time_utc"])
        self.assertEqual(first["time"]["server_time_raw"], "2026-11-04T21:45:00")

    def test_34_mcp_fixture_reconstructs_two_exit_partial_close(self):
        bundle = normalize_mcp_read_fixture(mcp_fixture(), expected_account=MCP_ACCOUNT)
        lifecycle = bundle["historical_observation"]["lifecycles"][0]
        self.assertEqual(lifecycle["status"], "closed")
        self.assertEqual(lifecycle["entry_volume"], "1")
        self.assertEqual(lifecycle["exit_volume"], "1")
        self.assertEqual(lifecycle["remaining_volume"], "0")
        self.assertEqual(
            lifecycle["partial_closes"],
            [
                {
                    "deal_id": "3002",
                    "volume": "0.4",
                    "observed_remaining": "0.6",
                    "time_server_raw": "2026-11-04T09:00:00",
                }
            ],
        )

    def test_35_mcp_limit_reached_blocks_cursor_and_lifecycle_promotion(self):
        fixture = mcp_fixture()
        fixture["history_window"]["limit"] = 3
        for call in fixture["calls"]:
            if call["tool"] in {"get_trading_history_orders", "get_trading_history_positions"}:
                call["arguments"]["limit"] = 3
        bundle = normalize_mcp_read_fixture(fixture, expected_account=MCP_ACCOUNT)
        historical = bundle["historical_observation"]
        self.assertFalse(historical["coverage"]["complete"])
        self.assertEqual(
            historical["coverage"]["orders_and_deals"]["termination"],
            "limit_reached",
        )
        self.assertFalse(historical["cursor_advance_allowed"])
        self.assertEqual(historical["lifecycles"][0]["status"], "UNKNOWN")
        self.assertIsNone(historical["lifecycles"][0]["remaining_volume"])

    def test_36_mcp_provider_partial_flag_blocks_cursor_below_limit(self):
        fixture = mcp_fixture()
        for call in fixture["calls"]:
            if call["tool"] == "get_trading_history_positions":
                call["response"]["truncated"] = True
        bundle = normalize_mcp_read_fixture(fixture, expected_account=MCP_ACCOUNT)
        historical = bundle["historical_observation"]
        self.assertFalse(historical["cursor_advance_allowed"])
        self.assertEqual(
            historical["coverage"]["position_summaries"]["termination"],
            "provider_reported_partial",
        )
        self.assertEqual(historical["lifecycles"][0]["status_reason"], "HISTORY_WINDOW_INCOMPLETE")

    def test_37_mcp_fixture_requires_trading_revoked_in_origin(self):
        fixture = mcp_fixture()
        for call in fixture["calls"]:
            if call["tool"] == "get_trading_account_info" and call["phase"] == "before":
                call["response"]["terminal"]["mcp_trade_allowed"] = True
        with self.assertRaisesRegex(PermissionError, "MCP_TRADE_ALLOWED_NOT_FALSE"):
            normalize_mcp_read_fixture(fixture, expected_account=MCP_ACCOUNT)

    def test_38_mcp_fixture_rejects_identity_change(self):
        fixture = mcp_fixture()
        for call in fixture["calls"]:
            if call["tool"] == "get_trading_account_info" and call["phase"] == "after":
                call["response"]["account"]["login"] = "990002"
        with self.assertRaisesRegex(ValueError, "IDENTITY_MISMATCH"):
            normalize_mcp_read_fixture(fixture, expected_account=MCP_ACCOUNT)

    def test_39_mcp_fixture_boundary_is_exact_and_has_no_transport(self):
        fixture = mcp_fixture()
        fixture["calls"][0]["tool"] = "trade_send_market_order"
        with self.assertRaisesRegex(PermissionError, "OUTSIDE_READ_ALLOWLIST"):
            normalize_mcp_read_fixture(fixture, expected_account=MCP_ACCOUNT)
        self.assertEqual(
            set(READ_TOOLS),
            {
                "get_workspace_info",
                "get_time_information",
                "get_trading_account_info",
                "get_trading_open_positions",
                "get_trading_history_orders",
                "get_trading_history_positions",
                "get_marketwatch_symbols",
            },
        )
        source = (ROOT / "src/provider/mt5_mcp_readonly.py").read_text(encoding="utf-8")
        for primitive in ("import socket", "import subprocess", "import requests", "streamablehttp"):
            self.assertNotIn(primitive, source.lower())

    def test_40_openclaw_projection_normalizes_real_open_state_but_blocks_automation(self):
        normalized = normalize_mcp_open_observation_fixture(
            mcp_open_fixture(), expected_account=MCP_ACCOUNT
        )
        self.assertEqual(normalized["account_key"], list(MCP_ACCOUNT.as_list()))
        self.assertEqual(normalized["positions"][0]["position_identifier"], "9201")
        self.assertFalse(normalized["positions"][0]["initial_protection_observed_at_entry"])
        self.assertFalse(normalized["coverage"]["deals_complete"])
        self.assertFalse(normalized["automation_eligible"])
        self.assertIn("MCP_TRADE_ALLOWED_IN_ORIGIN", normalized["automation_blockers"])

    def test_41_open_observation_rejects_direct_endpoint_claim(self):
        fixture = mcp_open_fixture()
        fixture["direct_endpoint_access"] = True
        with self.assertRaisesRegex(PermissionError, "DIRECT_ENDPOINT_ACCESS_FORBIDDEN"):
            normalize_mcp_open_observation_fixture(fixture, expected_account=MCP_ACCOUNT)

    def test_42_open_observation_imports_two_cycles_and_accredits_only_successive_change(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            vault = root / "vault"
            vault.mkdir()
            (vault / ".vidar-mt5-test-vault").write_text(TEST_VAULT_MARKER, encoding="utf-8")
            database = root / "supervisor.sqlite"

            first_path = root / "first.json"
            first_path.write_text(json.dumps(mcp_open_fixture()), encoding="utf-8")
            first_sha = hashlib.sha256(first_path.read_bytes()).hexdigest()
            first = run_offline_mcp_open_observation(
                fixture=first_path,
                expected_sha256=first_sha,
                expected_server=MCP_ACCOUNT.server,
                expected_login=MCP_ACCOUNT.login,
                database=database,
                vault=vault,
            )
            self.assertEqual(first["cycle"]["events"][0]["kind"], "position.baseline_observed")

            human = vault / f"Humanas/{MCP_ACCOUNT.server}-{MCP_ACCOUNT.login}-9201.md"
            human.write_text(human.read_text(encoding="utf-8") + "\nnota humana conservada\n", encoding="utf-8")
            second_path = root / "second.json"
            second_path.write_text(
                json.dumps(
                    mcp_open_fixture(
                        captured_at="2026-11-05T04:31:00Z", volume="0.20", stop_loss="1.1960"
                    )
                ),
                encoding="utf-8",
            )
            second_sha = hashlib.sha256(second_path.read_bytes()).hexdigest()
            second = run_offline_mcp_open_observation(
                fixture=second_path,
                expected_sha256=second_sha,
                expected_server=MCP_ACCOUNT.server,
                expected_login=MCP_ACCOUNT.login,
                database=database,
                vault=vault,
            )
            changes = next(
                event["data"]["changes"]
                for event in second["cycle"]["events"]
                if event["kind"] == "position.modified"
            )
            self.assertEqual(changes["volume"], {"before": "0.25", "after": "0.2"})
            self.assertEqual(changes["sl"], {"before": "1.19", "after": "1.196"})
            self.assertIn("nota humana conservada", human.read_text(encoding="utf-8"))
            self.assertEqual(second["counts"]["cycles"], 2)
            self.assertEqual(second["quick_check"], "ok")

    def test_43_open_observation_keeps_initial_risk_unknown_and_replay_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            vault = root / "vault"
            vault.mkdir()
            (vault / ".vidar-mt5-test-vault").write_text(TEST_VAULT_MARKER, encoding="utf-8")
            fixture_path = root / "observation.json"
            fixture_path.write_text(json.dumps(mcp_open_fixture()), encoding="utf-8")
            fixture_sha = hashlib.sha256(fixture_path.read_bytes()).hexdigest()
            arguments = {
                "fixture": fixture_path,
                "expected_sha256": fixture_sha,
                "expected_server": MCP_ACCOUNT.server,
                "expected_login": MCP_ACCOUNT.login,
                "database": root / "supervisor.sqlite",
                "vault": vault,
            }
            first = run_offline_mcp_open_observation(**arguments)
            second = run_offline_mcp_open_observation(**arguments)
            self.assertEqual(first["cycle"], second["cycle"])
            self.assertEqual(second["counts"]["cycles"], 1)
            db = sqlite3.connect(arguments["database"])
            try:
                initial = db.execute(
                    "SELECT status,reason_codes_json FROM supervisor_metrics "
                    "WHERE cycle_id=? AND scope_key='position:9201' AND metric_name='initial_risk_money'",
                    (first["cycle"]["cycle_id"],),
                ).fetchone()
                current = db.execute(
                    "SELECT status FROM supervisor_metrics "
                    "WHERE cycle_id=? AND scope_key='position:9201' AND metric_name='current_risk_money'",
                    (first["cycle"]["cycle_id"],),
                ).fetchone()
            finally:
                db.close()
            self.assertEqual(initial[0], "UNKNOWN")
            self.assertIn("INITIAL_PROTECTION_NOT_OBSERVED_AT_ENTRY", initial[1])
            self.assertEqual(current[0], "ESTIMATED")

    def test_44_filtered_mcp_projection_is_deterministic_and_preserves_unknowns(self):
        fixture = mcp_filtered_fixture()
        first = normalize_filtered_mcp_fixture(fixture, expected_account=MCP_ACCOUNT)
        second = normalize_filtered_mcp_fixture(copy.deepcopy(fixture), expected_account=MCP_ACCOUNT)
        self.assertEqual(canonical_json(first), canonical_json(second))
        self.assertFalse(first["automation_eligible"])
        self.assertIn("MCP_TRADE_ALLOWED_IN_ORIGIN", first["automation_blockers"])
        self.assertEqual(first["server_clock_status"], "UNKNOWN")
        self.assertEqual(first["coverage"]["symbol_specs"]["status"], "UNKNOWN")
        self.assertEqual(first["coverage"]["historical_deals"]["status"], "UNKNOWN")
        self.assertEqual(first["deals"][0]["record_class"], "cashflow")
        self.assertIsNone(first["deals"][0]["event_time_utc"])
        self.assertEqual(
            set(first["permissions"]["production_contract"].values()),
            {"UNKNOWN"},
        )

    def test_45_filtered_mcp_candidate_is_rejected_by_production_contract(self):
        projection = normalize_filtered_mcp_fixture(
            mcp_filtered_fixture(), expected_account=MCP_ACCOUNT
        )
        with self.assertRaisesRegex(
            ValueError, "IDENTITY_BEFORE_TERMINAL_TRADE_ALLOWED_NOT_FALSE"
        ):
            validate_snapshot(projection["engine_candidate"], MCP_ACCOUNT)

    def test_46_filtered_mcp_boundary_is_exact_and_has_no_transport(self):
        fixture = mcp_filtered_fixture()
        fixture["calls"][1]["tool"] = "get_time_information"
        with self.assertRaisesRegex(PermissionError, "OUTSIDE_READ_ALLOWLIST"):
            normalize_filtered_mcp_fixture(fixture, expected_account=MCP_ACCOUNT)
        self.assertEqual(
            set(FILTERED_READ_TOOLS),
            {
                "get_trading_account_info",
                "get_trading_open_positions",
                "get_trading_history_orders",
                "get_trading_history_positions",
            },
        )
        source = (ROOT / "src/provider/mt5_mcp_projection_experimental.py").read_text(
            encoding="utf-8"
        )
        for primitive in (
            "import socket",
            "import subprocess",
            "import requests",
            "streamablehttp",
            "trade_send_",
            "trade_modify_",
            "trade_close_",
        ):
            self.assertNotIn(primitive, source.lower())

    def test_47_filtered_mcp_stays_ineligible_after_origin_trading_is_revoked(self):
        projection = normalize_filtered_mcp_fixture(
            mcp_filtered_fixture(mcp_trade_allowed=False), expected_account=MCP_ACCOUNT
        )
        self.assertNotIn("MCP_TRADE_ALLOWED_IN_ORIGIN", projection["automation_blockers"])
        self.assertIn(
            "PRODUCTION_PERMISSION_FIELDS_UNAVAILABLE", projection["automation_blockers"]
        )
        self.assertFalse(projection["automation_eligible"])


if __name__ == "__main__":
    unittest.main()
