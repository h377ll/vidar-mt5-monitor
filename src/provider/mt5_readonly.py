#!/usr/bin/env python3
"""Read one MT5 demo snapshot through an isolated RPyC bridge.

The process accepts exactly one JSON request on stdin and emits one normalized
snapshot on stdout.  It never accepts account credentials, never enables
AutoTrading and contains no order/check/send/modify/close calls.  The RPyC
server remains a separately pinned future deployment dependency and must share
an otherwise private network namespace with this provider.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json
from pathlib import Path
import sys
from typing import Any, Callable, Mapping


SRC = Path(__file__).resolve().parents[1]
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from engine.contracts import canonical_json, decimal_text, input_digest  # noqa: E402


MAX_REQUEST_BYTES = 262_144
DEFAULT_HISTORY_SECONDS = 86_400
MAX_HISTORY_SECONDS = 31 * 86_400
DEMO_TRADE_MODE = 0


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _plain(value: Any) -> Any:
    """Normalize RPC values without allowing binary floats into canonical JSON."""
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        return decimal_text(Decimal(str(value)))
    if isinstance(value, Decimal):
        return decimal_text(value)
    if hasattr(value, "_asdict"):
        return _plain(value._asdict())
    if isinstance(value, Mapping):
        return {str(key): _plain(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain(item) for item in value]
    raise TypeError(f"UNSUPPORTED_PROVIDER_VALUE:{type(value).__name__}")


def _mapping(value: Any, code: str) -> dict[str, Any]:
    normalized = _plain(value)
    if not isinstance(normalized, dict):
        raise RuntimeError(code)
    return normalized


def _rows(value: Any, code: str) -> list[dict[str, Any]] | None:
    if value is None:
        return None
    normalized = _plain(value)
    if not isinstance(normalized, list) or any(not isinstance(row, dict) for row in normalized):
        raise RuntimeError(code)
    return normalized


def _side(value: Any) -> str:
    if value == 0:
        return "buy"
    if value == 1:
        return "sell"
    return "unknown"


def _entry(value: Any) -> str:
    return {0: "in", 1: "out", 2: "inout", 3: "out_by"}.get(value, "unknown")


def _utc_from_epoch(value: Any) -> str | None:
    if value in (None, 0, "0", ""):
        return None
    try:
        return _timestamp(datetime.fromtimestamp(int(value), timezone.utc))
    except (OverflowError, TypeError, ValueError) as exc:
        raise ValueError("MT5_EPOCH_INVALID") from exc


def _identity(terminal: Mapping[str, Any], account: Mapping[str, Any], version: Any) -> dict[str, Any]:
    raw_version = _plain(version)
    version_build = raw_version[1] if isinstance(raw_version, list) and len(raw_version) >= 2 else None
    build = terminal.get("build", version_build)
    return {
        "server": str(account.get("server", "")),
        "login": str(account.get("login", "")),
        "trade_mode": "demo" if account.get("trade_mode") == DEMO_TRADE_MODE else "non_demo",
        "connected": terminal.get("connected") is True,
        "terminal_build": int(build or 0),
        # The official Python API exposes no broker-clock endpoint.  A local
        # wall-clock value is deliberately not relabelled as server time.
        "server_time_utc": None,
        # Preserve the native booleans.  Missing, null and integer values must
        # remain distinguishable from an explicit False for the fail-closed
        # permission gate below.
        "terminal_trade_allowed": terminal.get("trade_allowed"),
        "account_trade_allowed": account.get("trade_allowed"),
        "account_trade_expert": account.get("trade_expert"),
    }


def _verify_identity(identity: Mapping[str, Any], expected: Mapping[str, Any], side: str) -> None:
    if identity.get("trade_mode") != "demo":
        raise PermissionError(f"PROVIDER_IDENTITY_{side}_NOT_DEMO")
    if identity.get("connected") is not True:
        raise RuntimeError(f"PROVIDER_IDENTITY_{side}_DISCONNECTED")
    if identity.get("server") != expected.get("server") or identity.get("login") != str(expected.get("login", "")):
        raise PermissionError(f"PROVIDER_IDENTITY_{side}_MISMATCH")
    if type(identity.get("terminal_build")) is not int or identity["terminal_build"] <= 0:
        raise RuntimeError(f"PROVIDER_IDENTITY_{side}_BUILD_INVALID")
    for field in (
        "terminal_trade_allowed",
        "account_trade_allowed",
        "account_trade_expert",
    ):
        if identity.get(field) is not False:
            raise PermissionError(f"PROVIDER_IDENTITY_{side}_{field.upper()}_NOT_FALSE")


def _position(row: Mapping[str, Any]) -> dict[str, Any]:
    identifier = row.get("identifier") or row.get("ticket")
    return {
        "position_identifier": str(identifier or ""),
        "position_ticket": str(row.get("ticket", "")),
        "symbol": str(row.get("symbol", "")),
        "side": _side(row.get("type")),
        "volume": row.get("volume"),
        "price_open": row.get("price_open"),
        "current_price": row.get("price_current"),
        "sl": row.get("sl"),
        "tp": row.get("tp"),
        "swap": row.get("swap"),
        "profit": row.get("profit"),
        "opened_at_utc": _utc_from_epoch(row.get("time")),
        "server_time_msc_raw": row.get("time_msc"),
        "updated_time_msc_raw": row.get("time_update_msc"),
    }


def _order(row: Mapping[str, Any], scope: str) -> dict[str, Any]:
    position = row.get("position_id")
    result = {
        "order_ticket": str(row.get("ticket", "")),
        "symbol": str(row.get("symbol", "")),
        "side": _side(row.get("type")),
        "state": row.get("state"),
        "scope": scope,
        "volume_initial": row.get("volume_initial"),
        "volume_current": row.get("volume_current"),
        "price_open": row.get("price_open"),
        "current_price": row.get("price_current"),
        "sl": row.get("sl"),
        "tp": row.get("tp"),
        "server_time_msc_raw": row.get("time_setup_msc"),
        "done_time_msc_raw": row.get("time_done_msc"),
    }
    if position not in (None, 0, "0", ""):
        result["position_identifier"] = str(position)
    return result


def _deal(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "deal_ticket": str(row.get("ticket", "")),
        "order_ticket": str(row.get("order", "")),
        "position_identifier": str(row.get("position_id", "")),
        "symbol": str(row.get("symbol", "")),
        "entry": _entry(row.get("entry")),
        "direction": _side(row.get("type")),
        "volume": row.get("volume"),
        "price": row.get("price"),
        # The official history row does not prove a requested execution price.
        # Omission intentionally yields UNVERIFIABLE slippage downstream.
        "profit": row.get("profit"),
        "commission": row.get("commission"),
        "swap": row.get("swap"),
        "fee": row.get("fee"),
        "server_time_msc_raw": row.get("time_msc"),
    }


def _symbol_spec(row: Mapping[str, Any], account_currency: str) -> dict[str, Any]:
    basis = {
        "tick_size": row.get("trade_tick_size"),
        "tick_value_loss": row.get("trade_tick_value_loss"),
        "tick_value_profit": row.get("trade_tick_value_profit"),
        "currency": account_currency,
        "currency_base": row.get("currency_base"),
        "currency_profit": row.get("currency_profit"),
        "currency_margin": row.get("currency_margin"),
        "trade_contract_size": row.get("trade_contract_size"),
        "trade_calc_mode": row.get("trade_calc_mode"),
        "digits": row.get("digits"),
        "point": row.get("point"),
    }
    return {**basis, "spec_hash": input_digest(basis)}


def _deduplicate(rows: list[dict[str, Any]], identity: str) -> list[dict[str, Any]]:
    found: dict[str, bytes] = {}
    result: dict[str, dict[str, Any]] = {}
    for row in rows:
        key = str(row.get(identity, ""))
        if key in {"", "0"}:
            raise ValueError(f"PROVIDER_{identity.upper()}_REQUIRED")
        encoded = canonical_json(row)
        if key in found and found[key] != encoded:
            raise ValueError(f"PROVIDER_CONFLICTING_NATIVE_ID:{identity}:{key}")
        found[key] = encoded
        result[key] = row
    return [result[key] for key in sorted(result)]


def _coverage(
    *,
    payload: Any,
    complete: bool,
    termination: str,
    requested_window: list[str] | None = None,
    covered_window: list[str] | None = None,
) -> dict[str, Any]:
    row: dict[str, Any] = {
        "complete": complete,
        "pages": 1 if complete else 0,
        "termination": termination if complete else "interrupted",
        "content_hash": input_digest(payload),
    }
    if requested_window is not None:
        row["requested_window"] = requested_window
        row["covered_window"] = covered_window or [requested_window[0], requested_window[0]]
    return row


def capture_snapshot(
    client: Any,
    *,
    expected: Mapping[str, Any],
    history_seconds: int = DEFAULT_HISTORY_SECONDS,
    clock: Callable[[], datetime] = _now,
) -> dict[str, Any]:
    """Capture and normalize one bounded read from an already isolated bridge."""
    if history_seconds < 60 or history_seconds > MAX_HISTORY_SECONDS:
        raise ValueError("PROVIDER_HISTORY_WINDOW_INVALID")
    started_dt = clock()
    started = _timestamp(started_dt)
    if client.initialize() is not True:
        raise RuntimeError("PROVIDER_MT5_INITIALIZE_FAILED")
    try:
        terminal_before_raw, account_before_raw, version_before = client.identity()
        terminal_before = _mapping(terminal_before_raw, "TERMINAL_INFO_REQUIRED")
        account_before = _mapping(account_before_raw, "ACCOUNT_INFO_REQUIRED")
        identity_before = _identity(terminal_before, account_before, version_before)
        _verify_identity(identity_before, expected, "BEFORE")

        window_start = started_dt - timedelta(seconds=history_seconds)
        endpoint_errors: dict[str, Any] = {}
        positions_raw = _rows(client.positions_get(), "POSITIONS_RESPONSE_INVALID")
        if positions_raw is None:
            endpoint_errors["positions"] = _plain(client.last_error())[0]
        pending_raw = _rows(client.orders_get(), "ORDERS_RESPONSE_INVALID")
        if pending_raw is None:
            endpoint_errors["pending_orders"] = _plain(client.last_error())[0]
        history_orders_raw = _rows(
            client.history_orders_get(window_start, started_dt), "HISTORY_ORDERS_RESPONSE_INVALID"
        )
        if history_orders_raw is None:
            endpoint_errors["history_orders"] = _plain(client.last_error())[0]
        deals_raw = _rows(client.history_deals_get(window_start, started_dt), "DEALS_RESPONSE_INVALID")
        if deals_raw is None:
            endpoint_errors["history_deals"] = _plain(client.last_error())[0]

        positions = _deduplicate([_position(row) for row in (positions_raw or [])], "position_identifier")
        orders = _deduplicate(
            [_order(row, "pending") for row in (pending_raw or [])]
            + [_order(row, "history") for row in (history_orders_raw or [])],
            "order_ticket",
        )
        deals = _deduplicate([_deal(row) for row in (deals_raw or [])], "deal_ticket")
        symbols = sorted(
            {
                str(row.get("symbol", ""))
                for row in [*positions, *orders, *deals]
                if str(row.get("symbol", ""))
            }
        )
        account_currency = str(account_before.get("currency", ""))
        specs: dict[str, dict[str, Any]] = {}
        missing_specs: list[str] = []
        for symbol in symbols:
            raw = client.symbol_info(symbol)
            if raw is None:
                missing_specs.append(symbol)
                continue
            specs[symbol] = _symbol_spec(_mapping(raw, "SYMBOL_INFO_INVALID"), account_currency)

        terminal_after_raw, account_after_raw, version_after = client.identity()
        terminal_after = _mapping(terminal_after_raw, "TERMINAL_INFO_REQUIRED")
        account_after = _mapping(account_after_raw, "ACCOUNT_INFO_REQUIRED")
        identity_after = _identity(terminal_after, account_after, version_after)
        _verify_identity(identity_after, expected, "AFTER")
        if (
            identity_before["terminal_build"] != identity_after["terminal_build"]
            or identity_before["server"] != identity_after["server"]
            or identity_before["login"] != identity_after["login"]
        ):
            raise RuntimeError("PROVIDER_IDENTITY_CHANGED_DURING_READ")

        finished_dt = clock()
        finished = _timestamp(finished_dt)
        window = [_timestamp(window_start), started]
        positions_complete = positions_raw is not None
        orders_complete = pending_raw is not None and history_orders_raw is not None
        deals_complete = deals_raw is not None
        specs_complete = not missing_specs
        coverage = {
            "positions": _coverage(payload=positions, complete=positions_complete, termination="enumerated"),
            "orders": _coverage(
                payload=orders,
                complete=orders_complete,
                termination="exhausted",
                requested_window=window,
                covered_window=window if orders_complete else None,
            ),
            "deals": _coverage(
                payload=deals,
                complete=deals_complete,
                termination="exhausted",
                requested_window=window,
                covered_window=window if deals_complete else None,
            ),
            "symbol_specs": _coverage(payload=specs, complete=specs_complete, termination="enumerated"),
        }
        duration_ms = int((finished_dt - started_dt).total_seconds() * 1000)
        return {
            "schema_version": "vidar.mt5.readonly-snapshot.v1",
            "read_started_at_utc": started,
            "observed_at_utc": finished,
            "read_finished_at_utc": finished,
            "identity_before": identity_before,
            "identity_after": identity_after,
            "coverage": coverage,
            "connectivity_indicators": {
                "provider": "vidar.rpyc-readonly.v1",
                "read_duration_ms": duration_ms,
                "broker_time_lag_ms": None,
                "broker_time_lag_status": "UNKNOWN",
                "reason_codes": ["BROKER_CLOCK_NOT_EXPOSED_BY_MT5_PYTHON_API"],
                "missing_symbol_specs": missing_specs,
                "endpoint_error_codes": endpoint_errors,
            },
            "account_state": {
                "currency": account_currency,
                "balance": account_before.get("balance"),
                "equity": account_before.get("equity"),
                "capital_basis": "equity",
                # A bounded history window does not prove all deposits and withdrawals.
                "cashflows_resolved": False,
            },
            "positions": positions,
            "orders": orders,
            "deals": deals,
            "symbol_specs": specs,
        }
    finally:
        client.shutdown()


class RpycReadonlyClient:
    """Minimal fixed-expression client for the future isolated classic server."""

    _HELPER = """
def _vidar_plain(value):
    if hasattr(value, '_asdict'):
        return {str(k): _vidar_plain(v) for k, v in value._asdict().items()}
    if isinstance(value, dict):
        return {str(k): _vidar_plain(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_vidar_plain(v) for v in value]
    return value
"""

    def __init__(self, host: str, port: int, timeout_seconds: int) -> None:
        if host not in {"127.0.0.1", "::1"}:
            raise ValueError("PROVIDER_HOST_MUST_BE_LOOPBACK")
        if port < 1024 or port > 65535:
            raise ValueError("PROVIDER_PORT_INVALID")
        if timeout_seconds < 1 or timeout_seconds > 120:
            raise ValueError("PROVIDER_RPC_TIMEOUT_INVALID")
        try:
            import rpyc
        except ImportError as exc:
            raise RuntimeError("RPYC_DEPENDENCY_UNAVAILABLE") from exc
        self._rpyc = rpyc
        self._connection = rpyc.classic.connect(host, port)
        self._connection._config["sync_request_timeout"] = timeout_seconds
        self._connection.execute("import MetaTrader5 as _vidar_mt5")
        self._connection.execute("import datetime as _vidar_datetime")
        self._connection.execute(self._HELPER)

    def _eval(self, expression: str) -> Any:
        raw = self._connection.eval(f"_vidar_plain({expression})")
        return self._rpyc.classic.obtain(raw)

    def initialize(self) -> bool:
        return bool(self._eval("_vidar_mt5.initialize()"))

    def identity(self) -> tuple[Any, Any, Any]:
        return (
            self._eval("_vidar_mt5.terminal_info()"),
            self._eval("_vidar_mt5.account_info()"),
            self._eval("_vidar_mt5.version()"),
        )

    def positions_get(self) -> Any:
        return self._eval("_vidar_mt5.positions_get()")

    def orders_get(self) -> Any:
        return self._eval("_vidar_mt5.orders_get()")

    def history_orders_get(self, start: datetime, finish: datetime) -> Any:
        return self._eval(
            "_vidar_mt5.history_orders_get("
            f"_vidar_datetime.datetime.fromtimestamp({int(start.timestamp())}, _vidar_datetime.timezone.utc),"
            f"_vidar_datetime.datetime.fromtimestamp({int(finish.timestamp())}, _vidar_datetime.timezone.utc))"
        )

    def history_deals_get(self, start: datetime, finish: datetime) -> Any:
        return self._eval(
            "_vidar_mt5.history_deals_get("
            f"_vidar_datetime.datetime.fromtimestamp({int(start.timestamp())}, _vidar_datetime.timezone.utc),"
            f"_vidar_datetime.datetime.fromtimestamp({int(finish.timestamp())}, _vidar_datetime.timezone.utc))"
        )

    def symbol_info(self, symbol: str) -> Any:
        return self._eval(f"_vidar_mt5.symbol_info({symbol!r})")

    def last_error(self) -> Any:
        return self._eval("_vidar_mt5.last_error()")

    def shutdown(self) -> None:
        try:
            self._eval("_vidar_mt5.shutdown()")
        finally:
            self._connection.close()


def _request(raw: bytes) -> tuple[dict[str, Any], dict[str, Any]]:
    if not raw or len(raw) > MAX_REQUEST_BYTES:
        raise ValueError("PROVIDER_REQUEST_SIZE_INVALID")
    try:
        request = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("PROVIDER_REQUEST_JSON_INVALID") from exc
    if not isinstance(request, dict) or request.get("operation") != "readonly_snapshot":
        raise ValueError("PROVIDER_OPERATION_INVALID")
    expected = request.get("expected_account")
    settings = request.get("provider")
    if not isinstance(expected, dict) or not str(expected.get("server", "")) or not str(expected.get("login", "")):
        raise ValueError("PROVIDER_EXPECTED_ACCOUNT_REQUIRED")
    if not isinstance(settings, dict):
        raise ValueError("PROVIDER_SETTINGS_REQUIRED")
    return expected, settings


def main() -> int:
    try:
        expected, settings = _request(sys.stdin.buffer.read(MAX_REQUEST_BYTES + 1))
        client = RpycReadonlyClient(
            str(settings.get("host", "127.0.0.1")),
            int(settings.get("port", 18812)),
            int(settings.get("rpc_timeout_seconds", 30)),
        )
        snapshot = capture_snapshot(
            client,
            expected=expected,
            history_seconds=int(settings.get("history_seconds", DEFAULT_HISTORY_SECONDS)),
        )
        sys.stdout.buffer.write(canonical_json(snapshot) + b"\n")
        return 0
    except Exception as exc:  # fail closed; no credentials or payloads are logged
        print(f"{type(exc).__name__}:{exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
