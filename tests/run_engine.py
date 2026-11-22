#!/usr/bin/env python3
"""Ejecuta I1 con fixtures sintéticos; sin red, MT5, servicios ni vault."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import socket
import sys
import time
import unittest


os.umask(0o077)
sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[1]
ENGINE = ROOT / "src"
TESTS = ROOT / "tests/engine"
FIXTURE = ROOT / "tests/fixtures/engine/synthetic-lifecycle.json"
EXPECTED_TESTS = 60
sys.path.insert(0, str(ENGINE))
sys.path.insert(0, str(TESTS))

from engine.contracts import AccountKey, canonical_json, input_digest  # noqa: E402
from engine.alerts import market_event_observations, position_change_alerts  # noqa: E402
from engine.metrics import (  # noqa: E402
    exposure_concentration,
    max_drawdown,
    performance_statistics,
    planned_r,
    realized_r,
    slippage,
    stop_risk,
)
from engine.reconciliation import normalize_event, reconcile_account_snapshot  # noqa: E402
from engine.reporting import dashboard_rows, render_human_journal_template  # noqa: E402


def protected_hashes():
    roots = [ROOT / "src/original", ROOT / "src/phase_b", ROOT / "ea"]
    return {
        str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
        for parent in roots
        for path in sorted(parent.rglob("*"))
        if path.is_file() and not path.is_symlink()
    }


def build_synthetic_output():
    data = json.loads(FIXTURE.read_text())
    account = AccountKey.from_value(data["account_key"])
    reconciliation = reconcile_account_snapshot(
        account=account,
        observed_at_utc=data["observed_at_utc"],
        run_id=data["run_id"],
        connection=data["connection"],
        orders=data["orders"],
        deals=data["deals"],
        positions=data["positions"],
    )
    events = [
        normalize_event(
            account=account,
            event_type=row["event_type"],
            native_id=row["native_id"],
            observed_at_utc=data["observed_at_utc"],
            run_id=data["run_id"],
            payload=row["payload"],
        ).as_dict()
        for row in data["events"]
    ]
    position_alerts = position_change_alerts(
        account=account,
        previous=[],
        current=data["positions"],
        connected_before=None,
        connected_after=True,
        observed_at_utc=data["observed_at_utc"],
    )
    market_alerts = [
        decision.as_dict()
        for row in data["events"]
        for decision in market_event_observations(
            account=account,
            positions=data["positions"],
            event_type=row["event_type"],
            payload=row["payload"],
            observed_at_utc=data["observed_at_utc"],
        )
    ]
    metrics = {
        "initial_risk": stop_risk(
            direction="buy", reference_price="1.25010", stop_price="1.24500", volume="0.50",
            tick_size="0.00010", tick_value_loss="10", currency="USD", cost_buffer="1.25",
        ),
        "planned_r": planned_r(reward_to_target="49.50", initial_risk="256.25"),
        "realized_r": realized_r(net_pnl="22.50", initial_risk="256.25", costs_complete=True),
        "balance_drawdown": max_drawdown(
            samples=[
                {"observed_at_utc": "2026-10-27T10:00:00Z", "value": "10000"},
                {"observed_at_utc": "2026-10-27T11:00:00Z", "value": "10025"},
                {"observed_at_utc": "2026-10-27T12:00:00Z", "value": "9990"},
            ],
            series_kind="balance",
        ),
        "statistics": performance_statistics(["1", "-0.5", "0.25"]),
        "slippage": slippage(
            direction="buy", fill_price="1.25010", reference_price="1.25000",
            tick_size="0.00010", tick_value="10", volume="0.50", currency="USD",
        ),
        "concentration": exposure_concentration(
            [{"bucket": "SYNTHUSD", "gross_risk": "256.25"}]
        ),
    }
    observed = [
        {"key": f"{fact.fact_type}:{fact.native_id}", "source_hash": fact.source_hash}
        for fact in reconciliation.facts
    ] + [{"key": f"{event['fact_type']}:{event['native_id']}", "source_hash": event["source_hash"]} for event in events]
    rows = dashboard_rows(
        observed=observed,
        calculated=metrics,
        subjective={"discipline": None, "emotion": None, "fatigue": None},
    )
    human_template = render_human_journal_template(
        trade_id="SYN-6001", auto_note_relative="_auto/SYN-6001.md", idea_id="IDEA-SYN-01"
    )
    return {
        "schema_version": "vidar.engine.synthetic-output.v1",
        "fixture_digest": input_digest(data),
        "reconciliation": reconciliation.as_dict(),
        "events": events,
        "alert_decisions": [decision.as_dict() for decision in position_alerts] + market_alerts,
        "metrics": {name: metric.as_dict() for name, metric in sorted(metrics.items())},
        "dashboard_rows": rows,
        "human_template_digest": "sha256:" + hashlib.sha256(human_template.encode()).hexdigest(),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", required=True, type=Path)
    args = parser.parse_args()
    output = args.output_dir.expanduser().resolve()
    if output.exists() or output.is_relative_to(ROOT):
        parser.error("output-dir must be new and outside the repository")
    output.mkdir(parents=True, mode=0o700)

    before = protected_hashes()
    blocked = []
    active = True

    def audit(event, arguments):
        if not active:
            return
        if event in {"socket.connect", "socket.bind", "socket.getaddrinfo"}:
            blocked.append(event)
            raise PermissionError("ENGINE_TEST_NETWORK_BLOCKED")
        if event in {"subprocess.Popen", "os.system", "os.exec", "os.posix_spawn"}:
            blocked.append(event)
            raise PermissionError("ENGINE_TEST_SUBPROCESS_BLOCKED")
        if event == "open":
            raw_path, _, flags = arguments
            if isinstance(raw_path, int):
                return
            write_flags = os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC | os.O_APPEND
            if flags & write_flags:
                resolved = Path(os.fsdecode(raw_path)).resolve()
                if not resolved.is_relative_to(output):
                    blocked.append("write_outside_output")
                    raise PermissionError("ENGINE_TEST_WRITE_BLOCKED")

    sys.addaudithook(audit)
    started_at = datetime.now(timezone.utc).isoformat()
    started = time.monotonic()
    with (output / "tests.txt").open("w") as stream:
        suite = unittest.TestSuite(
            [
                unittest.defaultTestLoader.loadTestsFromName("test_engine"),
                unittest.defaultTestLoader.loadTestsFromName("test_dashboard"),
            ]
        )
        planned = suite.countTestCases()
        result = unittest.TextTestRunner(stream=stream, verbosity=2).run(suite)
    first = canonical_json(build_synthetic_output())
    second = canonical_json(build_synthetic_output())
    (output / "synthetic-output.json").write_bytes(first + b"\n")
    active = False
    after = protected_hashes()
    changed = sorted(name for name in before if before[name] != after.get(name))
    accepted = (
        result.wasSuccessful()
        and planned == EXPECTED_TESTS
        and result.testsRun == EXPECTED_TESTS
        and not result.skipped
        and not blocked
        and not changed
        and first == second
    )
    summary = {
        "schema_version": "vidar.engine.i1.verification.v1",
        "started_at_utc": started_at,
        "finished_at_utc": datetime.now(timezone.utc).isoformat(),
        "duration_seconds": round(time.monotonic() - started, 3),
        "accepted": accepted,
        "tests_planned": planned,
        "tests_run": result.testsRun,
        "failures": len(result.failures),
        "errors": len(result.errors),
        "skipped": len(result.skipped),
        "synthetic_fixture": str(FIXTURE.relative_to(ROOT)),
        "deterministic_output_equal": first == second,
        "deterministic_output_sha256": hashlib.sha256(first).hexdigest(),
        "protected_files": len(before),
        "protected_files_changed": changed,
        "blocked_operations": blocked,
        "real_mt5_calls": 0,
        "real_notifications": 0,
        "services_or_schedulers_changed": 0,
        "vault_writes": 0,
        "sqx_interactions": 0,
        "limitations": [
            "Synthetic authority only; no MT5 schema or history coverage commissioned",
            "No EA compilation, terminal connection or account observation",
            "No Grafana datasource, Obsidian vault or alert emitter activated",
        ],
    }
    (output / "verification.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary, indent=2))
    return 0 if accepted else 1


if __name__ == "__main__":
    raise SystemExit(main())
