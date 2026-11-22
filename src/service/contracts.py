"""Validated snapshot and pinned-command contracts for persistent services.

This module does not contain an MT5 client.  It constrains a separately reviewed
read-only provider: one absolute executable, pinned by SHA-256, must return a
complete contract.  No shell, network transport, credential or trading action
is implemented here.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess
from typing import Any, Mapping

from engine.contracts import AccountKey, canonical_json, input_digest, utc_timestamp


MAX_CONFIG_BYTES = 262_144
MAX_PROVIDER_BYTES = 8 * 1024 * 1024
SOURCE_PATTERN = re.compile(r"[A-Za-z0-9_-]{1,48}\Z")
STREAM_PATTERN = re.compile(r"[A-Za-z0-9_-]{1,96}\Z")
COMPLETE_TERMINATIONS = {
    "positions": "enumerated",
    "orders": "exhausted",
    "deals": "exhausted",
    "symbol_specs": "enumerated",
}


def _reject_constant(_: str) -> None:
    raise ValueError("NONFINITE_JSON")


def parse_json_bytes(raw: bytes, *, maximum: int = MAX_PROVIDER_BYTES) -> Any:
    if not raw or len(raw) > maximum:
        raise ValueError("JSON_SIZE_INVALID")
    try:
        return json.loads(raw, parse_float=Decimal, parse_constant=_reject_constant)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("INVALID_JSON") from exc


def load_json(path: str | os.PathLike[str], *, maximum: int = MAX_CONFIG_BYTES) -> Any:
    candidate = Path(path)
    if candidate.is_symlink():
        raise ValueError("SYMLINK_CONFIG")
    info = candidate.stat()
    if not stat.S_ISREG(info.st_mode) or info.st_size <= 0 or info.st_size > maximum:
        raise ValueError("CONFIG_FILE_INVALID")
    return parse_json_bytes(candidate.read_bytes(), maximum=maximum)


def atomic_bytes(path: str | os.PathLike[str], content: bytes, *, mode: int = 0o600) -> None:
    destination = Path(path)
    if destination.is_symlink() or (destination.exists() and not destination.is_file()):
        raise ValueError("OUTPUT_PATH_INVALID")
    destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    if destination.parent.is_symlink():
        raise ValueError("SYMLINK_OUTPUT_PARENT")
    temporary = destination.with_name(f".{destination.name}.tmp-{os.getpid()}")
    if temporary.exists() or temporary.is_symlink():
        raise ValueError("TEMPORARY_PATH_EXISTS")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, destination)
        os.chmod(destination, mode)
        directory = os.open(destination.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    except BaseException:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
        raise


def sha256_file(path: str | os.PathLike[str]) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def validate_pinned_executable(command: Any, expected_sha256: Any) -> Path:
    if not isinstance(command, list) or len(command) != 1 or not isinstance(command[0], str):
        raise ValueError("SINGLE_EXECUTABLE_COMMAND_REQUIRED")
    executable = Path(command[0])
    if not executable.is_absolute() or executable.is_symlink():
        raise ValueError("PROVIDER_EXECUTABLE_PATH_INVALID")
    info = executable.stat()
    if not stat.S_ISREG(info.st_mode) or not info.st_mode & 0o111:
        raise ValueError("PROVIDER_EXECUTABLE_NOT_EXECUTABLE")
    if not isinstance(expected_sha256, str) or not re.fullmatch(r"[0-9a-f]{64}", expected_sha256):
        raise ValueError("PROVIDER_SHA256_INVALID")
    if sha256_file(executable) != expected_sha256:
        raise ValueError("PROVIDER_SHA256_MISMATCH")
    return executable


def run_pinned_json_command(config: Mapping[str, Any], request: Mapping[str, Any]) -> Mapping[str, Any]:
    executable = validate_pinned_executable(config.get("command"), config.get("command_sha256"))
    timeout = int(config.get("timeout_seconds", 30))
    if timeout < 1 or timeout > 120:
        raise ValueError("PROVIDER_TIMEOUT_INVALID")
    completed = subprocess.run(
        [str(executable)],
        input=canonical_json(request) + b"\n",
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
        timeout=timeout,
        close_fds=True,
    )
    if completed.returncode != 0:
        raise RuntimeError(f"PROVIDER_EXIT:{completed.returncode}")
    if len(completed.stderr) > MAX_CONFIG_BYTES:
        raise ValueError("PROVIDER_STDERR_TOO_LARGE")
    response = parse_json_bytes(completed.stdout)
    if not isinstance(response, Mapping):
        raise ValueError("PROVIDER_RESPONSE_OBJECT_REQUIRED")
    return response


def _timestamp(value: Any, code: str) -> str:
    if not isinstance(value, str):
        raise ValueError(code)
    try:
        return utc_timestamp(value)
    except ValueError as exc:
        raise ValueError(code) from exc


def _parse_time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def _identity(value: Any, expected: AccountKey, side: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"IDENTITY_{side}_REQUIRED")
    server = value.get("server")
    login = str(value.get("login", ""))
    if AccountKey(str(server or ""), login) != expected:
        raise ValueError(f"IDENTITY_{side}_MISMATCH")
    if value.get("trade_mode") != "demo":
        raise ValueError(f"IDENTITY_{side}_NOT_DEMO")
    if type(value.get("connected")) is not bool:
        raise ValueError(f"IDENTITY_{side}_CONNECTION_REQUIRED")
    build = value.get("terminal_build")
    if type(build) is not int or build <= 0:
        raise ValueError(f"IDENTITY_{side}_BUILD_REQUIRED")
    permission_fields = (
        "terminal_trade_allowed",
        "account_trade_allowed",
        "account_trade_expert",
    )
    for field in permission_fields:
        if value.get(field) is not False:
            raise ValueError(f"IDENTITY_{side}_{field.upper()}_NOT_FALSE")
    raw_server_time = value.get("server_time_utc")
    server_time = (
        None
        if raw_server_time is None
        else _timestamp(raw_server_time, f"IDENTITY_{side}_SERVER_TIME_INVALID")
    )
    return {
        "server": expected.server,
        "login": expected.login,
        "trade_mode": "demo",
        "connected": value["connected"],
        "terminal_build": build,
        "server_time_utc": server_time,
        **{field: False for field in permission_fields},
    }


def _coverage(name: str, value: Any, payload: Any, connected: bool) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"COVERAGE_{name.upper()}_REQUIRED")
    complete = value.get("complete")
    pages = value.get("pages")
    termination = value.get("termination")
    if type(complete) is not bool or type(pages) is not int or pages < 0:
        raise ValueError(f"COVERAGE_{name.upper()}_INVALID")
    expected_hash = input_digest(payload)
    if value.get("content_hash") != expected_hash:
        raise ValueError(f"COVERAGE_{name.upper()}_HASH_MISMATCH")
    if complete:
        if not connected or pages < 1 or termination != COMPLETE_TERMINATIONS[name]:
            raise ValueError(f"COVERAGE_{name.upper()}_COMPLETENESS_UNPROVEN")
    if name in {"orders", "deals"}:
        requested = value.get("requested_window")
        covered = value.get("covered_window")
        valid_window = (
            isinstance(requested, list)
            and len(requested) == 2
            and isinstance(covered, list)
            and len(covered) == 2
            and all(isinstance(item, str) for item in requested + covered)
        )
        if not valid_window:
            raise ValueError(f"COVERAGE_{name.upper()}_WINDOW_REQUIRED")
        requested = [_timestamp(item, f"COVERAGE_{name.upper()}_WINDOW_INVALID") for item in requested]
        covered = [_timestamp(item, f"COVERAGE_{name.upper()}_WINDOW_INVALID") for item in covered]
        if complete and requested != covered:
            raise ValueError(f"COVERAGE_{name.upper()}_WINDOW_INCOMPLETE")
    else:
        requested = covered = None
    return {
        "complete": complete,
        "pages": pages,
        "termination": termination,
        "content_hash": expected_hash,
        "requested_window": requested,
        "covered_window": covered,
    }


@dataclass(frozen=True)
class SnapshotBundle:
    account: AccountKey
    observed_at_utc: str
    source_hash: str
    connection: Mapping[str, Any]
    account_state: Mapping[str, Any]
    orders: tuple[Mapping[str, Any], ...]
    deals: tuple[Mapping[str, Any], ...]
    positions: tuple[Mapping[str, Any], ...]
    symbol_specs: Mapping[str, Mapping[str, Any]]
    normalized: Mapping[str, Any]

    @property
    def cycle_id(self) -> str:
        stamp = self.observed_at_utc.replace("-", "").replace(":", "").replace("T", "-").replace("Z", "")
        return f"live-{stamp}-{self.source_hash[7:23]}"


def validate_snapshot(value: Any, expected: AccountKey) -> SnapshotBundle:
    if not isinstance(value, Mapping):
        raise ValueError("SNAPSHOT_OBJECT_REQUIRED")
    normalized = json.loads(canonical_json(value))
    if normalized.get("schema_version") != "vidar.mt5.readonly-snapshot.v1":
        raise ValueError("SNAPSHOT_SCHEMA_UNSUPPORTED")
    started = _timestamp(normalized.get("read_started_at_utc"), "SNAPSHOT_START_REQUIRED")
    observed = _timestamp(normalized.get("observed_at_utc"), "SNAPSHOT_OBSERVED_REQUIRED")
    finished = _timestamp(normalized.get("read_finished_at_utc"), "SNAPSHOT_FINISH_REQUIRED")
    if not _parse_time(started) <= _parse_time(observed) <= _parse_time(finished):
        raise ValueError("SNAPSHOT_TIME_ORDER_INVALID")
    before = _identity(normalized.get("identity_before"), expected, "BEFORE")
    after = _identity(normalized.get("identity_after"), expected, "AFTER")
    if before["terminal_build"] != after["terminal_build"]:
        raise ValueError("TERMINAL_BUILD_CHANGED_DURING_READ")
    connected = before["connected"] and after["connected"]
    rows: dict[str, Any] = {}
    for name, identity_field in (
        ("positions", "position_identifier"),
        ("orders", "order_ticket"),
        ("deals", "deal_ticket"),
    ):
        payload = normalized.get(name)
        if not isinstance(payload, list) or any(not isinstance(row, Mapping) for row in payload):
            raise ValueError(f"SNAPSHOT_{name.upper()}_REQUIRED")
        if any(str(row.get(identity_field, "")) in {"", "0"} for row in payload):
            raise ValueError(f"SNAPSHOT_{name.upper()}_IDENTITY_REQUIRED")
        rows[name] = payload
    specs = normalized.get("symbol_specs")
    if not isinstance(specs, Mapping) or any(not isinstance(spec, Mapping) for spec in specs.values()):
        raise ValueError("SNAPSHOT_SYMBOL_SPECS_REQUIRED")
    for symbol, spec in specs.items():
        basis = {key: item for key, item in spec.items() if key != "spec_hash"}
        if spec.get("spec_hash") != input_digest(basis):
            raise ValueError(f"SYMBOL_SPEC_HASH_MISMATCH:{symbol}")
    coverage_raw = normalized.get("coverage")
    if not isinstance(coverage_raw, Mapping):
        raise ValueError("SNAPSHOT_COVERAGE_REQUIRED")
    coverage = {
        name: _coverage(name, coverage_raw.get(name), rows.get(name, specs), connected)
        for name in ("positions", "orders", "deals", "symbol_specs")
    }
    account_state = normalized.get("account_state")
    if not isinstance(account_state, Mapping) or not isinstance(account_state.get("currency"), str):
        raise ValueError("SNAPSHOT_ACCOUNT_STATE_REQUIRED")
    raw_indicators = normalized.get("connectivity_indicators", {})
    if not isinstance(raw_indicators, Mapping):
        raise ValueError("SNAPSHOT_CONNECTIVITY_INDICATORS_OBJECT_REQUIRED")
    connection = {
        "server_connected": connected,
        "positions_complete": coverage["positions"]["complete"],
        "orders_complete": coverage["orders"]["complete"],
        "deals_complete": coverage["deals"]["complete"],
        "identity_before": before,
        "identity_after": after,
        "coverage": coverage,
        "read_started_at_utc": started,
        "read_finished_at_utc": finished,
        "connectivity_indicators": dict(raw_indicators),
    }
    source_hash = input_digest(normalized)
    return SnapshotBundle(
        account=expected,
        observed_at_utc=observed,
        source_hash=source_hash,
        connection=connection,
        account_state=account_state,
        orders=tuple(rows["orders"]),
        deals=tuple(rows["deals"]),
        positions=tuple(rows["positions"]),
        symbol_specs=specs,
        normalized=normalized,
    )


def adapter_capture(
    config: Mapping[str, Any],
    destination: str | os.PathLike[str],
    *,
    request_reason: Mapping[str, Any] | None = None,
) -> SnapshotBundle:
    if config.get("enabled") is not True:
        raise PermissionError("ADAPTER_NOT_AUTHORIZED")
    account = AccountKey(str(config.get("expected_server", "")), str(config.get("expected_login", "")))
    provider = config.get("provider", {})
    if not isinstance(provider, Mapping):
        raise ValueError("ADAPTER_PROVIDER_SETTINGS_OBJECT_REQUIRED")
    response = run_pinned_json_command(
        config,
        {
            "operation": "readonly_snapshot",
            "schema_version": "vidar.mt5.provider-request.v1",
            "reason": dict(request_reason or {"kind": "periodic"}),
            "expected_account": {"server": account.server, "login": account.login},
            "provider": dict(provider),
        },
    )
    snapshot = validate_snapshot(response, account)
    # The deployment uses a dedicated read-only sharing group between the
    # terminal-owned adapter and the monitor-owned engine.
    atomic_bytes(destination, canonical_json(snapshot.normalized) + b"\n", mode=0o640)
    return snapshot
