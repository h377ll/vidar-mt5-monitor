#!/usr/bin/env python3
"""Executable entry point for inert-until-configured demo service units."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from service.runtime import (  # type: ignore
        backup_once,
        health_once,
        identity_from_config,
        run_adapter,
        run_bridge,
        run_dispatch,
        run_engine,
        run_receiver,
    )
    from service.offline_html_backfill import run_offline_html_backfill  # type: ignore
    from service.offline_mcp_open_observation import run_offline_mcp_open_observation  # type: ignore
else:
    from .runtime import (
        backup_once,
        health_once,
        identity_from_config,
        run_adapter,
        run_bridge,
        run_dispatch,
        run_engine,
        run_receiver,
    )
    from .offline_html_backfill import run_offline_html_backfill
    from .offline_mcp_open_observation import run_offline_mcp_open_observation


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(prog="vidar-mt5-monitor")
    commands = root.add_subparsers(dest="command", required=True)

    receiver = commands.add_parser("receiver")
    receiver.add_argument("--db", required=True)
    receiver.add_argument("--identity-config", required=True)
    receiver.add_argument("--credential", required=True)
    receiver.add_argument("--port", required=True, type=int)
    receiver.add_argument("--once", action="store_true")

    bridge = commands.add_parser("bridge")
    bridge.add_argument("--spool-directory", required=True)
    bridge.add_argument("--cursor-directory", required=True)
    bridge.add_argument("--endpoint", required=True)
    bridge.add_argument("--credential", required=True)
    bridge.add_argument("--identity-config", required=True)
    bridge.add_argument("--interval-seconds", type=float, default=1.0)
    bridge.add_argument("--batch-per-stream", type=int, default=64)
    bridge.add_argument("--max-files", type=int, default=128)
    bridge.add_argument("--max-file-bytes", type=int, default=64 * 1024 * 1024)
    bridge.add_argument("--once", action="store_true")

    adapter = commands.add_parser("adapter")
    adapter.add_argument("--config", required=True)
    adapter.add_argument("--snapshot", required=True)
    adapter.add_argument("--interval-seconds", type=float, default=10.0)
    adapter.add_argument("--trigger")
    adapter.add_argument("--poll-seconds", type=float, default=0.5)
    adapter.add_argument("--once", action="store_true")

    engine = commands.add_parser("engine")
    engine.add_argument("--db", required=True)
    engine.add_argument("--snapshot", required=True)
    engine.add_argument("--identity-config", required=True)
    engine.add_argument("--lock", required=True)
    engine.add_argument("--reconcile-request")
    engine.add_argument("--interval-seconds", type=float, default=1.0)
    engine.add_argument("--once", action="store_true")

    dispatch = commands.add_parser("dispatch")
    dispatch.add_argument("--db", required=True)
    dispatch.add_argument("--config", required=True)
    dispatch.add_argument("--interval-seconds", type=float, default=1.0)
    dispatch.add_argument("--once", action="store_true")

    health = commands.add_parser("health")
    health.add_argument("--db", required=True)
    health.add_argument("--snapshot", required=True)
    health.add_argument("--identity-config", required=True)
    health.add_argument("--maximum-snapshot-age-seconds", type=int, default=30)

    backup = commands.add_parser("backup")
    backup.add_argument("--db", required=True)
    backup.add_argument("--backup-directory", required=True)

    offline = commands.add_parser("offline-html-backfill")
    offline.add_argument("--report", required=True)
    offline.add_argument("--expected-sha256", required=True)
    offline.add_argument("--position-id", required=True)
    offline.add_argument("--db", required=True)
    offline.add_argument("--vault", required=True)
    offline.add_argument("--human-note", required=True)
    offline.add_argument("--auto-note-directory", default="Automaticas/Operaciones")
    offline.add_argument("--observed-at-utc", required=True)

    mcp_open = commands.add_parser("offline-mcp-open-observation")
    mcp_open.add_argument("--fixture", required=True)
    mcp_open.add_argument("--expected-sha256", required=True)
    mcp_open.add_argument("--expected-server", required=True)
    mcp_open.add_argument("--expected-login", required=True)
    mcp_open.add_argument("--db", required=True)
    mcp_open.add_argument("--vault", required=True)
    return root


def main() -> int:
    arguments = parser().parse_args()
    values = vars(arguments)
    command = values.pop("command")
    if command == "receiver":
        result = run_receiver(
            database=values["db"],
            identity_config=values["identity_config"],
            credential=values["credential"],
            port=values["port"],
            once=values["once"],
        )
    elif command == "bridge":
        source, _account = identity_from_config(values.pop("identity_config"))
        result = run_bridge(expected_source=source, **values)
    elif command == "adapter":
        result = run_adapter(
            config_path=values["config"],
            destination=values["snapshot"],
            interval_seconds=values["interval_seconds"],
            trigger_path=values["trigger"],
            poll_seconds=values["poll_seconds"],
            once=values["once"],
        )
    elif command == "engine":
        result = run_engine(
            database=values["db"],
            snapshot_path=values["snapshot"],
            identity_config=values["identity_config"],
            lock_path=values["lock"],
            interval_seconds=values["interval_seconds"],
            reconcile_request_path=values["reconcile_request"],
            once=values["once"],
        )
    elif command == "dispatch":
        result = run_dispatch(
            database=values["db"],
            config_path=values["config"],
            interval_seconds=values["interval_seconds"],
            once=values["once"],
        )
    elif command == "health":
        result = health_once(
            database=values["db"],
            snapshot_path=values["snapshot"],
            identity_config=values["identity_config"],
            maximum_snapshot_age_seconds=values["maximum_snapshot_age_seconds"],
        )
    elif command == "offline-html-backfill":
        result = run_offline_html_backfill(
            report=values["report"],
            expected_sha256=values["expected_sha256"],
            position_id=values["position_id"],
            database=values["db"],
            vault=values["vault"],
            human_note=values["human_note"],
            auto_note_directory=values["auto_note_directory"],
            observed_at_utc=values["observed_at_utc"],
        )
    elif command == "offline-mcp-open-observation":
        result = run_offline_mcp_open_observation(
            fixture=values["fixture"],
            expected_sha256=values["expected_sha256"],
            expected_server=values["expected_server"],
            expected_login=values["expected_login"],
            database=values["db"],
            vault=values["vault"],
        )
    else:
        result = backup_once(database=values["db"], backup_directory=values["backup_directory"])
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
    if command == "health" and result["state"] != "ok":
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
