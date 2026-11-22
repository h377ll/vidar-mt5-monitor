"""Persistent runtime loops assembled from the reviewed transport and engine."""

from __future__ import annotations

from datetime import datetime, timezone
import fcntl
import json
import os
from pathlib import Path
import re
import signal
import sqlite3
import threading
import time
from typing import Any, Callable, Mapping
from urllib.error import HTTPError, URLError

from engine.contracts import AccountKey, canonical_json, utc_timestamp
from engine.supervisor import consume_receiver_inbox, initialize_supervisor_schema, reconcile_cycle
from phase_b import event_receiver, journal, spool_bridge

from .backup import versioned_backup
from .contracts import (
    SOURCE_PATTERN,
    STREAM_PATTERN,
    adapter_capture,
    load_json,
    parse_json_bytes,
    run_pinned_json_command,
    atomic_bytes,
    sha256_file,
    validate_snapshot,
)
from .outbox import dispatch_one, recover_attempting


SERVICE_STATUS_SCHEMA = """
CREATE TABLE IF NOT EXISTS service_status (
    component TEXT PRIMARY KEY,
    observed_at_utc TEXT NOT NULL,
    state TEXT NOT NULL,
    detail_json TEXT NOT NULL
);
"""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def identity_from_config(path: str | os.PathLike[str]) -> tuple[str, AccountKey]:
    config = load_json(path)
    if not isinstance(config, Mapping):
        raise ValueError("IDENTITY_CONFIG_OBJECT_REQUIRED")
    source = config.get("source")
    if not isinstance(source, str) or SOURCE_PATTERN.fullmatch(source) is None:
        raise ValueError("SOURCE_ID_INVALID")
    return source, AccountKey(str(config.get("server", "")), str(config.get("login", "")))


class StopFlag:
    def __init__(self) -> None:
        self.event = threading.Event()

    def install(self) -> None:
        def stop(_number, _frame):
            self.event.set()

        signal.signal(signal.SIGTERM, stop)
        signal.signal(signal.SIGINT, stop)

    def wait(self, seconds: float) -> bool:
        return self.event.wait(seconds)


def _status(db: sqlite3.Connection, component: str, state: str, detail: Mapping[str, Any]) -> None:
    db.execute(SERVICE_STATUS_SCHEMA)
    db.execute(
        """INSERT INTO service_status(component,observed_at_utc,state,detail_json)
           VALUES(?,?,?,?) ON CONFLICT(component) DO UPDATE SET
           observed_at_utc=excluded.observed_at_utc,state=excluded.state,detail_json=excluded.detail_json""",
        (component, utc_now(), state, canonical_json(detail).decode("utf-8")),
    )


def run_receiver(
    *,
    database: str | os.PathLike[str],
    identity_config: str | os.PathLike[str],
    credential: str | os.PathLike[str],
    port: int,
    once: bool = False,
) -> Mapping[str, Any]:
    if port < 1024 or port > 65535:
        raise ValueError("RECEIVER_PORT_INVALID")
    source, account = identity_from_config(identity_config)
    secret = spool_bridge.load_key(credential)
    server = event_receiver.make_server(database, {source: (secret, account.server, account.login)}, port=port)
    server.timeout = 0.25
    if once:
        server.server_close()
        return {"state": "validated", "host": "127.0.0.1", "port": port, "source": source}
    stop = StopFlag()
    stop.install()
    try:
        while not stop.event.is_set():
            server.handle_request()
    finally:
        server.server_close()
    return {"state": "stopped", "host": "127.0.0.1", "port": port, "source": source}


def _spool_identity(path: Path, expected_source: str) -> tuple[str, str] | None:
    if path.is_symlink() or not path.is_file():
        raise ValueError("SPOOL_PATH_INVALID")
    with path.open("rb") as stream:
        line = stream.readline(event_receiver.MAX_BODY + 2)
    if not line or not line.endswith(b"\n"):
        return None
    if len(line) > event_receiver.MAX_BODY:
        raise ValueError("SPOOL_FIRST_LINE_TOO_LARGE")
    envelope = parse_json_bytes(line, maximum=event_receiver.MAX_BODY)
    if not isinstance(envelope, Mapping):
        raise ValueError("SPOOL_EVENT_OBJECT_REQUIRED")
    source = envelope.get("source")
    stream = envelope.get("stream")
    if source != expected_source or not isinstance(stream, str) or STREAM_PATTERN.fullmatch(stream) is None:
        raise ValueError("SPOOL_IDENTITY_INVALID")
    if path.name != f"VidarObserver-{source}-{stream}.jsonl":
        raise ValueError("SPOOL_FILENAME_IDENTITY_MISMATCH")
    return source, stream


def bridge_once(
    *,
    spool_directory: str | os.PathLike[str],
    cursor_directory: str | os.PathLike[str],
    endpoint: str,
    credential: str | os.PathLike[str],
    expected_source: str,
    batch_per_stream: int = 64,
    max_files: int = 128,
    max_file_bytes: int = 64 * 1024 * 1024,
) -> Mapping[str, Any]:
    if not 1 <= batch_per_stream <= 1024 or not 1 <= max_files <= 4096 or max_file_bytes < 1024:
        raise ValueError("BRIDGE_LIMIT_INVALID")
    root = Path(spool_directory)
    if root.is_symlink() or not root.is_dir():
        raise ValueError("SPOOL_DIRECTORY_INVALID")
    state_root = Path(cursor_directory)
    state_root.mkdir(parents=True, exist_ok=True, mode=0o700)
    if state_root.is_symlink():
        raise ValueError("CURSOR_DIRECTORY_SYMLINK")
    files = sorted(root.glob("VidarObserver-*.jsonl"), key=lambda item: item.name)
    if len(files) > max_files:
        raise ValueError("SPOOL_FILE_LIMIT_EXCEEDED")
    secret = spool_bridge.load_key(credential)
    acknowledged = 0
    waiting_partial = 0
    streams = 0
    backlog_bytes = 0
    for path in files:
        size = path.stat().st_size
        if size > max_file_bytes:
            raise ValueError("SPOOL_SIZE_LIMIT_EXCEEDED")
        identity = _spool_identity(path, expected_source)
        if identity is None:
            waiting_partial += 1
            backlog_bytes += size
            continue
        source, stream = identity
        cursor = state_root / f"{source}--{stream}.cursor.json"
        result = spool_bridge.deliver(
            path,
            cursor,
            endpoint,
            secret,
            limit=batch_per_stream,
            expected_source=source,
            expected_stream=stream,
        )
        acknowledged += int(result["acknowledged_lines"])
        backlog_bytes += max(0, size - int(result["offset"]))
        streams += 1
    return {
        "streams": streams,
        "files": len(files),
        "acknowledged_lines": acknowledged,
        "waiting_partial_files": waiting_partial,
        "backlog_bytes": backlog_bytes,
    }


def run_bridge(*, interval_seconds: float, once: bool = False, **arguments: Any) -> Mapping[str, Any]:
    if once:
        return bridge_once(**arguments)
    if interval_seconds < 0.1 or interval_seconds > 60:
        raise ValueError("BRIDGE_INTERVAL_INVALID")
    stop = StopFlag()
    stop.install()
    last: Mapping[str, Any] = {}
    while not stop.event.is_set():
        try:
            last = bridge_once(**arguments)
        except HTTPError as exc:
            if exc.code < 500:
                raise
            last = {"state": "receiver_unavailable", "http_status": exc.code}
        except (URLError, ConnectionError, TimeoutError):
            last = {"state": "receiver_unavailable"}
        if stop.wait(interval_seconds):
            break
    return last


def run_adapter(
    *,
    config_path: str | os.PathLike[str],
    destination: str | os.PathLike[str],
    interval_seconds: float,
    trigger_path: str | os.PathLike[str] | None = None,
    poll_seconds: float = 0.5,
    once: bool = False,
) -> Mapping[str, Any]:
    def capture(reason: Mapping[str, Any]) -> Mapping[str, Any]:
        config = load_json(config_path)
        if not isinstance(config, Mapping):
            raise ValueError("ADAPTER_CONFIG_OBJECT_REQUIRED")
        snapshot = adapter_capture(config, destination, request_reason=reason)
        return {
            "state": "captured",
            "observed_at_utc": snapshot.observed_at_utc,
            "source_hash": snapshot.source_hash,
            "quality": "covered" if all(
                snapshot.connection[name]
                for name in ("server_connected", "positions_complete", "orders_complete", "deals_complete")
            ) else "unknown",
        }

    if once:
        return capture({"kind": "manual_once"})
    if interval_seconds < 1 or interval_seconds > 3600:
        raise ValueError("ADAPTER_INTERVAL_INVALID")
    if poll_seconds < 0.1 or poll_seconds > interval_seconds:
        raise ValueError("ADAPTER_POLL_INVALID")
    stop = StopFlag()
    stop.install()
    last: Mapping[str, Any] = {}
    previous_trigger: str | None = None
    next_periodic = 0.0
    while not stop.event.is_set():
        reason: Mapping[str, Any] | None = None
        now = time.monotonic()
        if trigger_path is not None:
            trigger = Path(trigger_path)
            if trigger.exists():
                if trigger.is_symlink() or not trigger.is_file():
                    raise ValueError("ADAPTER_TRIGGER_PATH_INVALID")
                trigger_hash = sha256_file(trigger)
                if trigger_hash != previous_trigger:
                    raw_reason = load_json(trigger)
                    if not isinstance(raw_reason, Mapping):
                        raise ValueError("ADAPTER_TRIGGER_OBJECT_REQUIRED")
                    reason = {"kind": "observer_event", "request": raw_reason, "request_hash": trigger_hash}
                    previous_trigger = trigger_hash
        if reason is None and now >= next_periodic:
            reason = {"kind": "periodic"}
        if reason is not None:
            last = capture(reason)
            next_periodic = time.monotonic() + interval_seconds
        if stop.wait(poll_seconds):
            break
    return last


def engine_once(
    *,
    database: str | os.PathLike[str],
    snapshot_path: str | os.PathLike[str],
    identity_config: str | os.PathLike[str],
    reconcile_request_path: str | os.PathLike[str] | None = None,
) -> Mapping[str, Any]:
    source, account = identity_from_config(identity_config)
    db = journal.open_db(database)
    try:
        initialize_supervisor_schema(db, utc_now())
        db.execute(SERVICE_STATUS_SCHEMA)
        ingress_exists = db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='ingress'"
        ).fetchone() is not None
        ingress = consume_receiver_inbox(db, source) if ingress_exists else {"stored": 0, "duplicates": 0}
        if ingress["stored"] and reconcile_request_path is not None:
            ledger = db.execute(
                """SELECT count(*),max(event_hash) FROM observer_event_ledger WHERE source=?""", (source,)
            ).fetchone()
            request = {
                "schema_version": "vidar.mt5.reconcile-request.v1",
                "source": source,
                "observer_events_total": int(ledger[0]),
                "last_event_hash": ledger[1],
                "requested_at_utc": utc_now(),
            }
            atomic_bytes(reconcile_request_path, canonical_json(request) + b"\n", mode=0o640)
        path = Path(snapshot_path)
        if not path.exists():
            with db:
                _status(db, "engine", "waiting_snapshot", {"ingress": ingress})
            return {"state": "waiting_snapshot", "ingress": ingress, "reconciliation_requested": bool(ingress["stored"])}
        if path.is_symlink() or not path.is_file():
            raise ValueError("SNAPSHOT_PATH_INVALID")
        snapshot = validate_snapshot(load_json(path, maximum=8 * 1024 * 1024), account)
        result = reconcile_cycle(
            db,
            account=account,
            observed_at_utc=snapshot.observed_at_utc,
            cycle_id=snapshot.cycle_id,
            connection=snapshot.connection,
            account_state=snapshot.account_state,
            orders=snapshot.orders,
            deals=snapshot.deals,
            positions=snapshot.positions,
            symbol_specs=snapshot.symbol_specs,
        )
        response = {
            "state": "reconciled",
            "ingress": ingress,
            "reconciliation_requested": bool(ingress["stored"]),
            "cycle": result.as_dict(),
        }
        with db:
            _status(db, "engine", "ok", {"cycle_id": result.cycle_id, "quality": result.reconciliation_quality})
        return response
    finally:
        db.close()


def run_engine(
    *,
    lock_path: str | os.PathLike[str],
    interval_seconds: float,
    once: bool = False,
    **arguments: Any,
) -> Mapping[str, Any]:
    lock = Path(lock_path)
    lock.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    with lock.open("a") as stream:
        fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if once:
            return engine_once(**arguments)
        if interval_seconds < 0.2 or interval_seconds > 60:
            raise ValueError("ENGINE_INTERVAL_INVALID")
        stop = StopFlag()
        stop.install()
        last: Mapping[str, Any] = {}
        while not stop.event.is_set():
            last = engine_once(**arguments)
            if stop.wait(interval_seconds):
                break
        return last


def dispatch_once(
    *,
    database: str | os.PathLike[str],
    config_path: str | os.PathLike[str],
    at_utc: str | None = None,
) -> Mapping[str, Any]:
    config = load_json(config_path)
    if not isinstance(config, Mapping) or config.get("enabled") is not True:
        raise PermissionError("DISPATCH_NOT_AUTHORIZED")
    db = journal.open_db(database)
    try:
        initialize_supervisor_schema(db, utc_now())
        recovered = recover_attempting(db, at_utc=at_utc)

        def sender(message: str, event_id: str) -> Mapping[str, Any]:
            return run_pinned_json_command(
                config,
                {
                    "operation": "deliver_alert",
                    "schema_version": "vidar.mt5.dispatch-request.v1",
                    "event_id": event_id,
                    "message": message,
                },
            )

        state = dispatch_one(
            db,
            sender,
            at_utc=at_utc,
            max_age_seconds=int(config.get("max_age_seconds", 300)),
        )
        return {"state": state, "recovered_uncertain": recovered}
    finally:
        db.close()


def run_dispatch(*, interval_seconds: float, once: bool = False, **arguments: Any) -> Mapping[str, Any]:
    if once:
        return dispatch_once(**arguments)
    if interval_seconds < 0.2 or interval_seconds > 60:
        raise ValueError("DISPATCH_INTERVAL_INVALID")
    stop = StopFlag()
    stop.install()
    last: Mapping[str, Any] = {}
    while not stop.event.is_set():
        last = dispatch_once(**arguments)
        if stop.wait(interval_seconds):
            break
    return last


def health_once(
    *,
    database: str | os.PathLike[str],
    snapshot_path: str | os.PathLike[str],
    identity_config: str | os.PathLike[str],
    maximum_snapshot_age_seconds: int = 30,
) -> Mapping[str, Any]:
    _source, account = identity_from_config(identity_config)
    reasons: list[str] = []
    db_path = Path(database)
    if db_path.is_symlink() or not db_path.is_file():
        return {"state": "critical", "reasons": ["DATABASE_MISSING"]}
    db = sqlite3.connect(db_path)
    db.row_factory = sqlite3.Row
    try:
        quick = str(db.execute("PRAGMA quick_check").fetchone()[0])
        if quick != "ok":
            reasons.append("DATABASE_QUICK_CHECK_FAILED")
        tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        cycles = int(db.execute("SELECT count(*) FROM supervisor_cycles").fetchone()[0]) if "supervisor_cycles" in tables else 0
        outbox = (
            {row[0]: int(row[1]) for row in db.execute("SELECT state,count(*) FROM supervisor_outbox GROUP BY state")}
            if "supervisor_outbox" in tables else {}
        )
        if outbox.get("uncertain", 0):
            reasons.append("OUTBOX_UNCERTAIN")
    finally:
        db.close()
    snapshot_file = Path(snapshot_path)
    snapshot_age: float | None = None
    if not snapshot_file.is_file() or snapshot_file.is_symlink():
        reasons.append("SNAPSHOT_MISSING")
    else:
        try:
            snapshot = validate_snapshot(load_json(snapshot_file, maximum=8 * 1024 * 1024), account)
            observed = datetime.fromisoformat(snapshot.observed_at_utc.replace("Z", "+00:00"))
            snapshot_age = (datetime.now(timezone.utc) - observed).total_seconds()
            if snapshot_age < 0 or snapshot_age > maximum_snapshot_age_seconds:
                reasons.append("SNAPSHOT_STALE_OR_FUTURE")
            if not snapshot.connection["server_connected"]:
                reasons.append("SOURCE_DISCONNECTED")
        except (ValueError, OSError):
            reasons.append("SNAPSHOT_INVALID")
    state = "ok" if not reasons else "degraded" if quick == "ok" else "critical"
    return {
        "state": state,
        "reasons": reasons,
        "quick_check": quick,
        "cycles": cycles,
        "outbox": outbox,
        "snapshot_age_seconds": None if snapshot_age is None else round(snapshot_age, 3),
    }


def backup_once(*, database: str | os.PathLike[str], backup_directory: str | os.PathLike[str]) -> Mapping[str, Any]:
    return versioned_backup(database, backup_directory)
