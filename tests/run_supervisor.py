#!/usr/bin/env python3
"""Run the synthetic end-to-end MT5 supervisor acceptance circuit."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import socket
import sys
import tempfile
import threading
import time
import unittest


os.umask(0o077)
sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[1]
ENGINE_ROOT = ROOT / "src"
PHASE_B_ROOT = ENGINE_ROOT / "phase_b"
FIXTURE = ROOT / "tests/fixtures/engine/supervisor-sequence.json"
EXPECTED_TESTS = 21
sys.path.insert(0, str(ENGINE_ROOT))
sys.path.insert(0, str(PHASE_B_ROOT))
sys.path.insert(0, str(ROOT))

import event_receiver  # noqa: E402
import journal  # noqa: E402
import spool_bridge  # noqa: E402
from engine.contracts import AccountKey  # noqa: E402
from engine.supervisor import (  # noqa: E402
    TEST_VAULT_MARKER,
    consume_receiver_inbox,
    export_test_notes,
    initialize_supervisor_schema,
    reconcile_cycle,
)


def protected_hashes() -> dict[str, str]:
    roots = [ROOT / "src", ROOT / "ea"]
    return {
        str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
        for parent in roots
        for path in sorted(parent.rglob("*"))
        if path.is_file() and not path.is_symlink()
    }


def install_guard(write_roots: list[Path], blocked: list[str]) -> None:
    allowed = [path.resolve() for path in write_roots]

    def inside(path: os.PathLike[str] | str) -> bool:
        resolved = Path(os.fsdecode(path)).resolve()
        return any(resolved.is_relative_to(root) for root in allowed)

    def mutation_inside(arguments, path_index: int = 0, dir_fd_index: int = 1) -> bool:
        raw = Path(os.fsdecode(arguments[path_index]))
        if raw.is_absolute():
            return inside(raw)
        if len(arguments) > dir_fd_index and isinstance(arguments[dir_fd_index], int) and arguments[dir_fd_index] >= 0:
            base = Path(os.readlink(f"/proc/self/fd/{arguments[dir_fd_index]}"))
            return inside(base / raw)
        return inside(raw)

    def local_address(arguments) -> bool:
        if not arguments or len(arguments) < 2:
            return False
        address = arguments[1]
        return isinstance(address, tuple) and address and address[0] in {"127.0.0.1", "::1"}

    def audit(event, arguments):
        if event == "open" and not isinstance(arguments[0], int):
            path, _, flags = arguments
            write_flags = os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC | os.O_APPEND
            if flags & write_flags and not inside(path):
                blocked.append("write_outside_allowed_roots")
                raise PermissionError("SUPERVISOR_TEST_WRITE_BLOCKED")
        elif event == "sqlite3.connect":
            target = arguments[0]
            if target != ":memory:" and not inside(target):
                blocked.append("sqlite_outside_allowed_roots")
                raise PermissionError("SUPERVISOR_TEST_SQLITE_BLOCKED")
        elif event in {"socket.bind", "socket.connect"} and not local_address(arguments):
            blocked.append(event)
            raise PermissionError("SUPERVISOR_TEST_EXTERNAL_NETWORK_BLOCKED")
        elif event in {"socket.getaddrinfo"} and arguments[0] not in {"127.0.0.1", "::1"}:
            blocked.append(event)
            raise PermissionError("SUPERVISOR_TEST_DNS_BLOCKED")
        elif event in {"subprocess.Popen", "os.system", "os.exec", "os.posix_spawn"}:
            blocked.append(event)
            raise PermissionError("SUPERVISOR_TEST_SUBPROCESS_BLOCKED")
        elif event in {"os.remove", "os.rmdir", "os.mkdir"} and not mutation_inside(arguments):
            blocked.append(event + ":" + os.fsdecode(arguments[0]))
            raise PermissionError("SUPERVISOR_TEST_MUTATION_BLOCKED")
        elif event in {"os.rename", "os.replace"} and not all(inside(path) for path in arguments[:2]):
            blocked.append(event)
            raise PermissionError("SUPERVISOR_TEST_RENAME_BLOCKED")

    sys.addaudithook(audit)


def metric_value(db, cycle_id: str, scope: str, name: str):
    row = db.execute(
        "SELECT status,value_json,reason_codes_json FROM supervisor_metrics WHERE cycle_id=? AND scope_key=? AND metric_name=?",
        (cycle_id, scope, name),
    ).fetchone()
    return {
        "status": row["status"],
        "value": None if row["value_json"] is None else json.loads(row["value_json"]),
        "reason_codes": json.loads(row["reason_codes_json"]),
    }


def run_acceptance(output: Path, external_vault: Path | None) -> dict:
    data = json.loads(FIXTURE.read_text(encoding="utf-8"))
    account = AccountKey.from_value(data["account_key"])
    runtime = output / "runtime"
    runtime.mkdir(mode=0o700)
    database = runtime / "supervisor.sqlite"
    spool = runtime / "observer.jsonl"
    cursor = runtime / "cursor.json"
    key = runtime / "synthetic.key"
    key.write_bytes(bytes(range(32)))
    key.chmod(0o600)
    envelopes = [
        {
            "source": data["observer"]["source"],
            "stream": data["observer"]["stream"],
            "server": account.server,
            "login": account.login,
            **row,
        }
        for row in data["observer"]["events"]
    ]
    spool.write_bytes(b"".join(journal.dumps(row).encode("utf-8") + b"\n" for row in envelopes))

    db = journal.open_db(database)
    initialize_supervisor_schema(db, "2026-10-30T18:50:00Z")
    db.close()
    secret = spool_bridge.load_key(key)
    server = event_receiver.make_server(
        database,
        {data["observer"]["source"]: (secret, account.server, account.login)},
    )
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
    thread.start()
    endpoint = f"http://127.0.0.1:{server.server_port}/v1/events"
    bridge = spool_bridge.deliver(spool, cursor, endpoint, secret, limit=16)
    server.shutdown()
    thread.join(timeout=5)
    server.server_close()
    if thread.is_alive():
        raise RuntimeError("RECEIVER_THREAD_DID_NOT_STOP")
    key.unlink()

    db = journal.open_db(database)
    ingress = consume_receiver_inbox(db, data["observer"]["source"])
    db.close()
    restarts = 1
    results = []
    for index, row in enumerate(data["cycles"]):
        db = journal.open_db(database)
        result = reconcile_cycle(
            db,
            account=account,
            observed_at_utc=row["observed_at_utc"],
            cycle_id=row["cycle_id"],
            connection=row["connection"],
            account_state=row["account_state"],
            orders=row["orders"],
            deals=row["deals"],
            positions=row["positions"],
            symbol_specs=data["symbol_specs"],
        )
        results.append(result.as_dict())
        db.close()
        if index in {0, 2}:
            restarts += 1

    db = journal.open_db(database)
    duplicate_ingress = consume_receiver_inbox(db, data["observer"]["source"])
    replay = reconcile_cycle(
        db,
        account=account,
        observed_at_utc=data["cycles"][0]["observed_at_utc"],
        cycle_id=data["cycles"][0]["cycle_id"],
        connection=data["cycles"][0]["connection"],
        account_state=data["cycles"][0]["account_state"],
        orders=data["cycles"][0]["orders"],
        deals=data["cycles"][0]["deals"],
        positions=data["cycles"][0]["positions"],
        symbol_specs=data["symbol_specs"],
    )
    local_vault = output / "obsidian-test-vault"
    local_vault.mkdir(mode=0o700)
    (local_vault / ".vidar-mt5-test-vault").write_text(TEST_VAULT_MARKER, encoding="utf-8")
    local_notes = export_test_notes(
        db,
        cycle_id="cycle-3-closed",
        destination=local_vault,
        exported_at_utc="2026-10-30T19:05:00Z",
    )
    external_notes = None
    if external_vault is not None:
        external_notes = export_test_notes(
            db,
            cycle_id="cycle-3-closed",
            destination=external_vault,
            exported_at_utc="2026-10-30T19:05:00Z",
        )
    table_counts = {
        table: int(db.execute(f"SELECT count(*) FROM {table}").fetchone()[0])
        for table in (
            "ingress",
            "observer_event_ledger",
            "supervisor_cycles",
            "supervisor_positions",
            "supervisor_position_versions",
            "supervisor_broker_facts",
            "supervisor_events",
            "supervisor_issues",
            "supervisor_metrics",
            "supervisor_account_samples",
            "supervisor_account_states",
            "supervisor_note_exports",
        )
    }
    metrics = {
        "initial_risk_money": metric_value(db, "cycle-1-open", "position:6001", "initial_risk_money"),
        "current_risk_money": metric_value(db, "cycle-1-open", "position:6001", "current_risk_money"),
        "initial_risk_percent": metric_value(db, "cycle-1-open", "position:6001", "initial_risk_percent"),
        "current_risk_percent": metric_value(db, "cycle-1-open", "position:6001", "current_risk_percent"),
        "planned_r": metric_value(db, "cycle-1-open", "position:6001", "planned_r"),
        "realized_r": metric_value(db, "cycle-3-closed", "position:6001", "realized_r"),
        "execution_slippage": metric_value(db, "cycle-1-open", "position:6001", "execution_slippage:2001"),
        "protection_deviation": metric_value(db, "cycle-2-modified", "position:6001", "protection_deviation"),
        "realized_drawdown": metric_value(db, "cycle-5-unknown-disappearance", "account", "realized_drawdown"),
        "floating_drawdown": metric_value(db, "cycle-5-unknown-disappearance", "account", "floating_drawdown"),
    }
    lifecycle_status = {
        row["position_identifier"]: row["status"]
        for row in db.execute("SELECT position_identifier,status FROM supervisor_positions ORDER BY position_identifier")
    }
    unknown_issues = int(
        db.execute(
            "SELECT count(*) FROM supervisor_issues WHERE code='POSITION_DISAPPEARED_WITHOUT_ACCREDITED_CLOSE'"
        ).fetchone()[0]
    )
    db.close()

    checks = {
        "observer_event_captured": bridge["acknowledged_lines"] == 4 and ingress == {"stored": 4, "duplicates": 0},
        "persistence_and_restart": restarts == 3 and table_counts["supervisor_cycles"] == 5,
        "reconciliation_closed_confirmed": lifecycle_status.get("6001") == "CLOSED_CONFIRMED",
        "unknown_disappearance_detected": lifecycle_status.get("7001") == "UNKNOWN" and unknown_issues == 1,
        "deterministic_calculation_contrasted": (
            metrics["initial_risk_money"]["value"] == "50"
            and metrics["current_risk_money"]["value"] == "60"
            and metrics["planned_r"]["value"] == "2"
            and metrics["realized_r"]["value"] == "1.14"
            and metrics["realized_drawdown"]["value"]["maximum"] == "47"
            and metrics["floating_drawdown"]["value"]["maximum"] == "67"
        ),
        "obsidian_test_note_created": len(local_notes["notes"]) == 1,
        "duplicate_suppression": duplicate_ingress == {"stored": 0, "duplicates": 4} and replay.as_dict() == results[0],
    }
    return {
        "schema_version": "vidar.mt5.supervisor-acceptance.v1",
        "accepted": all(checks.values()),
        "checks": checks,
        "bridge": bridge,
        "ingress": ingress,
        "duplicate_ingress": duplicate_ingress,
        "restarts_exercised": restarts,
        "table_counts": table_counts,
        "lifecycle_status": lifecycle_status,
        "metrics": metrics,
        "local_notes": local_notes,
        "external_notes": external_notes,
        "cycles": results,
        "limitations": [
            "Synthetic demo authority; no connected MT5 schema or broker history commissioned",
            "EA source not compiled or attached to a terminal by this test",
            "No real trade, notification, service, scheduler, SQX or JMX action",
            "The marked vault contains test projections only; definitive journal remains untouched",
        ],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--obsidian-test-vault", type=Path)
    args = parser.parse_args()
    output = args.output_dir.expanduser().resolve()
    external_vault = args.obsidian_test_vault.expanduser().resolve() if args.obsidian_test_vault else None
    if output.exists() or output.is_relative_to(ROOT):
        parser.error("output-dir must be new and outside the repository")
    output.mkdir(parents=True, mode=0o700)
    tempfile.tempdir = str(output)
    blocked: list[str] = []
    write_roots = [output]
    if external_vault is not None:
        write_roots.append(external_vault)
    install_guard(write_roots, blocked)
    before = protected_hashes()
    started_at = datetime.now(timezone.utc).isoformat()
    started = time.monotonic()
    with (output / "tests.txt").open("w", encoding="utf-8") as stream:
        suite = unittest.defaultTestLoader.loadTestsFromName("tests.engine.test_supervisor")
        planned = suite.countTestCases()
        tests = unittest.TextTestRunner(stream=stream, verbosity=2).run(suite)
    acceptance = run_acceptance(output, external_vault)
    after = protected_hashes()
    changed = sorted(name for name in before if before[name] != after.get(name))
    accepted = (
        tests.wasSuccessful()
        and planned == EXPECTED_TESTS
        and tests.testsRun == EXPECTED_TESTS
        and not tests.skipped
        and acceptance["accepted"]
        and not blocked
        and not changed
    )
    summary = {
        "schema_version": "vidar.mt5.supervisor-verification.v1",
        "started_at_utc": started_at,
        "duration_seconds": round(time.monotonic() - started, 3),
        "accepted": accepted,
        "tests_planned": planned,
        "tests_run": tests.testsRun,
        "failures": len(tests.failures),
        "errors": len(tests.errors),
        "skipped": len(tests.skipped),
        "acceptance_checks": acceptance["checks"],
        "blocked_operations": blocked,
        "protected_files": len(before),
        "protected_files_changed": changed,
        "real_mt5_calls": 0,
        "trading_writes": 0,
        "services_or_schedulers_changed": 0,
        "sqx_or_jmx_interactions": 0,
    }
    (output / "acceptance.json").write_text(
        json.dumps(acceptance, indent=2, ensure_ascii=False, default=str) + "\n", encoding="utf-8"
    )
    (output / "verification.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    return 0 if accepted else 1


if __name__ == "__main__":
    raise SystemExit(main())
