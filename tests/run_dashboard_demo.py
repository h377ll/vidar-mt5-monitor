#!/usr/bin/env python3
"""Build a reproducible dashboard feed from synthetic supervisor metrics."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import sys
import time


os.umask(0o077)
sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[1]
ENGINE = ROOT / "src"
FIXTURE = ROOT / "tests/fixtures/engine/supervisor-sequence.json"
sys.path.insert(0, str(ENGINE))

from engine.contracts import AccountKey, canonical_json  # noqa: E402
from engine.dashboard import build_test_dashboard_dataset  # noqa: E402
from engine.supervisor import initialize_supervisor_schema, reconcile_cycle  # noqa: E402


def protected_hashes() -> dict[str, str]:
    roots = [ROOT / "src/original", ROOT / "src/phase_b", ROOT / "ea", ROOT / "deploy"]
    return {
        str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
        for parent in roots
        for path in sorted(parent.rglob("*"))
        if path.is_file() and not path.is_symlink()
    }


def install_guard(output: Path, blocked: list[str]) -> None:
    allowed = output.resolve()

    def inside(raw: os.PathLike[str] | str) -> bool:
        return Path(os.fsdecode(raw)).resolve().is_relative_to(allowed)

    def audit(event, arguments):
        if event == "open" and not isinstance(arguments[0], int):
            path, _, flags = arguments
            write_flags = os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC | os.O_APPEND
            if flags & write_flags and not inside(path):
                blocked.append("write_outside_output")
                raise PermissionError("DASHBOARD_DEMO_WRITE_BLOCKED")
        elif event == "sqlite3.connect":
            target = arguments[0]
            if target != ":memory:" and not inside(target):
                blocked.append("sqlite_outside_output")
                raise PermissionError("DASHBOARD_DEMO_SQLITE_BLOCKED")
        elif event in {"socket.bind", "socket.connect", "socket.getaddrinfo"}:
            blocked.append(event)
            raise PermissionError("DASHBOARD_DEMO_NETWORK_BLOCKED")
        elif event in {"subprocess.Popen", "os.system", "os.exec", "os.posix_spawn"}:
            blocked.append(event)
            raise PermissionError("DASHBOARD_DEMO_SUBPROCESS_BLOCKED")

    sys.addaudithook(audit)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", required=True, type=Path)
    args = parser.parse_args()
    output = args.output_dir.expanduser().resolve()
    if output.exists() or output.is_relative_to(ROOT):
        parser.error("output-dir must be new and outside the repository")
    output.mkdir(parents=True, mode=0o700)

    started_at = datetime.now(timezone.utc).isoformat()
    started = time.monotonic()
    before = protected_hashes()
    blocked: list[str] = []
    install_guard(output, blocked)

    fixture_bytes = FIXTURE.read_bytes()
    fixture = json.loads(fixture_bytes)
    account = AccountKey.from_value(fixture["account_key"])
    database = output / "synthetic-supervisor.sqlite"
    db = sqlite3.connect(database)
    db.row_factory = sqlite3.Row
    initialize_supervisor_schema(db, "2026-10-31T22:00:00Z")
    for row in fixture["cycles"]:
        reconcile_cycle(
            db,
            account=account,
            observed_at_utc=row["observed_at_utc"],
            cycle_id=row["cycle_id"],
            connection=row["connection"],
            account_state=row["account_state"],
            orders=row["orders"],
            deals=row["deals"],
            positions=row["positions"],
            symbol_specs=fixture["symbol_specs"],
        )
    changes_before_projection = db.total_changes
    first = build_test_dashboard_dataset(db, source_label="SYNTHETIC-MT5-DASHBOARD")
    second = build_test_dashboard_dataset(db, source_label="SYNTHETIC-MT5-DASHBOARD")
    projection_read_only = db.total_changes == changes_before_projection
    quick_check = str(db.execute("PRAGMA quick_check").fetchone()[0])
    db.close()

    dataset_bytes = canonical_json(first)
    deterministic = dataset_bytes == canonical_json(second)
    (output / "dashboard-dataset.json").write_bytes(dataset_bytes + b"\n")
    with (output / "dashboard-metrics.jsonl").open("wb") as stream:
        for row in first["metrics"]:
            stream.write(canonical_json(row) + b"\n")

    after = protected_hashes()
    changed = sorted(path for path in before if before[path] != after.get(path))
    names = {row["metric_name"] for row in first["metrics"]}
    required = {
        "initial_risk_money",
        "current_risk_money",
        "aggregate_current_risk",
        "planned_r",
        "realized_r",
        "realized_drawdown",
        "floating_drawdown",
    }
    checks = {
        "clearly_synthetic": first["dataset_kind"] == "SYNTHETIC_TEST" and first["synthetic"] is True,
        "deterministic_bytes": deterministic,
        "projection_read_only": projection_read_only,
        "sqlite_quick_check": quick_check == "ok",
        "required_metrics_present": required <= names and any(name.startswith("execution_slippage:") for name in names),
        "quality_and_provenance_present": all(
            row["status"] in {"KNOWN", "ESTIMATED", "UNKNOWN", "UNVERIFIABLE"}
            and row["algorithm_version"]
            and row["provenance"].startswith("sha256:")
            for row in first["metrics"]
        ),
        "raw_account_identity_excluded": account.server not in dataset_bytes.decode("utf-8")
        and account.login not in dataset_bytes.decode("utf-8"),
        "human_and_agent_layers_excluded": first["human_observations_included"] is False
        and first["agent_interpretations_included"] is False,
        "protected_components_unchanged": not changed,
        "blocked_operations": not blocked,
    }
    accepted = all(checks.values())
    verification = {
        "schema_version": "vidar.dashboard.demo-verification.v1",
        "started_at_utc": started_at,
        "finished_at_utc": datetime.now(timezone.utc).isoformat(),
        "duration_seconds": round(time.monotonic() - started, 3),
        "accepted": accepted,
        "checks": checks,
        "fixture": str(FIXTURE.relative_to(ROOT)),
        "fixture_digest": "sha256:" + hashlib.sha256(fixture_bytes).hexdigest(),
        "dataset_digest": first["dataset_digest"],
        "dataset_sha256": hashlib.sha256(dataset_bytes).hexdigest(),
        "cycles": len(first["cycles"]),
        "metric_rows": len(first["metrics"]),
        "quality_counts": first["quality_counts"],
        "protected_files": len(before),
        "protected_files_changed": changed,
        "blocked_operations_observed": blocked,
        "real_mt5_calls": 0,
        "grafana_changes": 0,
        "vault_writes": 0,
        "service_changes": 0,
        "g4b_actions": 0,
        "ft_qt_sqx_interactions": 0,
        "not_implemented_or_claimed": ["profit_max_drawdown", "session_statistics"],
    }
    (output / "verification.json").write_text(json.dumps(verification, indent=2, sort_keys=True) + "\n")
    print(json.dumps(verification, indent=2, sort_keys=True))
    return 0 if accepted else 1


if __name__ == "__main__":
    raise SystemExit(main())
