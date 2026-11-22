"""Fail-closed projection for the four-tool OpenClaw MT5 MCP surface.

This module is intentionally experimental.  It contains no MCP transport,
credentials, service integration, scheduler or trading primitive.  It accepts
only selected responses captured through OpenClaw's existing read-only tool
projection and exposes every unavailable production field as UNKNOWN.
"""

from __future__ import annotations

from collections import Counter
from typing import Any, Mapping

from engine.contracts import AccountKey, canonical_json, input_digest, stable_unique, utc_timestamp


FILTERED_READ_TOOLS = frozenset(
    {
        "get_trading_account_info",
        "get_trading_open_positions",
        "get_trading_history_orders",
        "get_trading_history_positions",
    }
)
FILTERED_FIXTURE_SCHEMA = "vidar.mt5.mcp-filtered-fixture.v1"
FILTERED_PROJECTION_SCHEMA = "vidar.mt5.mcp-filtered-projection.experimental.v1"
UNKNOWN = "UNKNOWN"


def _mapping(value: Any, code: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(code)
    result = dict(value)
    canonical_json(result)
    return result


def _rows(value: Any, code: str) -> list[dict[str, Any]]:
    if not isinstance(value, list) or any(not isinstance(row, Mapping) for row in value):
        raise ValueError(code)
    result = [dict(row) for row in value]
    canonical_json(result)
    return result


def _native_id(row: Mapping[str, Any], field: str, code: str, *, allow_zero: bool = False) -> str:
    value = str(row.get(field, ""))
    if not value.isdigit() or (not allow_zero and value == "0"):
        raise ValueError(code)
    return value


def _identity(response: Mapping[str, Any], expected: AccountKey, side: str) -> dict[str, Any]:
    account = _mapping(response.get("account"), f"MCP_FILTERED_{side}_ACCOUNT_REQUIRED")
    terminal = _mapping(response.get("terminal"), f"MCP_FILTERED_{side}_TERMINAL_REQUIRED")
    observed = AccountKey(str(account.get("server", "")), str(account.get("login", "")))
    if observed != expected:
        raise ValueError(f"MCP_FILTERED_{side}_IDENTITY_MISMATCH")
    account_type = str(account.get("type", account.get("trade_mode", UNKNOWN))).lower()
    if account_type != "demo":
        raise ValueError(f"MCP_FILTERED_{side}_ACCOUNT_NOT_DEMO")
    build = terminal.get("build")
    if type(build) is not int or build <= 0:
        raise ValueError(f"MCP_FILTERED_{side}_BUILD_REQUIRED")
    for field in ("server_connected", "experts_trade_allowed", "mcp_trade_allowed"):
        if type(terminal.get(field)) is not bool:
            raise ValueError(f"MCP_FILTERED_{side}_{field.upper()}_REQUIRED")
    if type(account.get("read_only")) is not bool:
        raise ValueError(f"MCP_FILTERED_{side}_ACCOUNT_READ_ONLY_REQUIRED")
    return {
        "server": expected.server,
        "login": expected.login,
        "account_type": account_type,
        "currency": str(account.get("currency", UNKNOWN)),
        "account_read_only": account["read_only"],
        "server_connected": terminal["server_connected"],
        "terminal_build": build,
        "experts_dll_allowed": terminal.get("experts_dll_allowed", UNKNOWN),
        "experts_trade_allowed": terminal["experts_trade_allowed"],
        "mcp_trade_allowed": terminal["mcp_trade_allowed"],
    }


def _coverage(rows: list[Mapping[str, Any]], limit: int) -> dict[str, Any]:
    limit_reached = len(rows) >= limit
    return {
        "status": "INCOMPLETE" if limit_reached else UNKNOWN,
        "complete": False,
        "count": len(rows),
        "limit": limit,
        "reason": "LIMIT_REACHED" if limit_reached else "PROVIDER_COMPLETENESS_NOT_EXPOSED",
        "content_hash": input_digest(rows),
    }


def _engine_identity(identity: Mapping[str, Any]) -> dict[str, Any]:
    """Project observed identity without inventing production permission aliases."""
    return {
        "server": identity["server"],
        "login": identity["login"],
        "trade_mode": identity["account_type"],
        "connected": identity["server_connected"],
        "terminal_build": identity["terminal_build"],
        "server_time_utc": None,
        "terminal_trade_allowed": None,
        "account_trade_allowed": None,
        "account_trade_expert": None,
    }


def normalize_filtered_mcp_fixture(
    fixture: Mapping[str, Any], *, expected_account: AccountKey
) -> dict[str, Any]:
    """Normalize the actual four-tool projection and expose a rejected candidate.

    The returned ``engine_candidate`` deliberately keeps unavailable production
    permission, coverage, symbol and clock fields unresolved.  Production's
    ``validate_snapshot`` remains the authority and must reject this candidate.
    """

    source = _mapping(fixture, "MCP_FILTERED_FIXTURE_OBJECT_REQUIRED")
    if source.get("schema_version") != FILTERED_FIXTURE_SCHEMA:
        raise ValueError("MCP_FILTERED_FIXTURE_SCHEMA_UNSUPPORTED")
    if source.get("capture_transport") != "openclaw_tool_projection":
        raise ValueError("MCP_FILTERED_CAPTURE_TRANSPORT_INVALID")
    if source.get("direct_endpoint_access") is not False:
        raise PermissionError("MCP_FILTERED_DIRECT_ENDPOINT_ACCESS_FORBIDDEN")
    captured_at = utc_timestamp(str(source.get("captured_at_utc", "")))

    window = _mapping(source.get("history_window"), "MCP_FILTERED_HISTORY_WINDOW_REQUIRED")
    window_from = window.get("datetime_from_server_raw")
    window_to = window.get("datetime_to_server_raw")
    limit = window.get("limit")
    if not all(isinstance(value, str) and value for value in (window_from, window_to)):
        raise ValueError("MCP_FILTERED_HISTORY_WINDOW_RAW_REQUIRED")
    if type(limit) is not int or limit <= 0:
        raise ValueError("MCP_FILTERED_HISTORY_LIMIT_INVALID")

    calls = _rows(source.get("calls"), "MCP_FILTERED_CALLS_REQUIRED")
    names = [str(call.get("tool", "")) for call in calls]
    if any(name not in FILTERED_READ_TOOLS for name in names):
        raise PermissionError("MCP_FILTERED_TOOL_OUTSIDE_READ_ALLOWLIST")
    expected_counts = {
        name: (2 if name == "get_trading_account_info" else 1)
        for name in FILTERED_READ_TOOLS
    }
    if Counter(names) != expected_counts:
        raise ValueError("MCP_FILTERED_CALL_SET_INVALID")

    identities: dict[str, dict[str, Any]] = {}
    responses: dict[str, dict[str, Any]] = {}
    call_receipts: list[dict[str, Any]] = []
    for call in calls:
        name = str(call["tool"])
        arguments = _mapping(call.get("arguments", {}), "MCP_FILTERED_ARGUMENTS_REQUIRED")
        response = _mapping(call.get("response"), "MCP_FILTERED_RESPONSE_REQUIRED")
        if name == "get_trading_account_info":
            phase = str(call.get("phase", ""))
            if phase not in {"before", "after"} or phase in identities:
                raise ValueError("MCP_FILTERED_IDENTITY_PHASE_INVALID")
            identities[phase] = response
        else:
            responses[name] = response
        if name == "get_trading_open_positions" and arguments != {"include_orders": True}:
            raise ValueError("MCP_FILTERED_OPEN_ARGUMENTS_INVALID")
        if name in {"get_trading_history_orders", "get_trading_history_positions"}:
            if (
                arguments.get("datetime_from") != window_from
                or arguments.get("datetime_to") != window_to
                or arguments.get("limit") != limit
            ):
                raise ValueError("MCP_FILTERED_HISTORY_ARGUMENT_WINDOW_MISMATCH")
        call_receipts.append(
            {
                "tool": name,
                "phase": call.get("phase"),
                "arguments_hash": input_digest(arguments),
                "selected_response_hash": input_digest(response),
            }
        )

    before = _identity(identities.get("before", {}), expected_account, "BEFORE")
    after = _identity(identities.get("after", {}), expected_account, "AFTER")
    if before != after:
        raise ValueError("MCP_FILTERED_IDENTITY_CHANGED_DURING_READ")

    open_response = responses["get_trading_open_positions"]
    raw_positions = _rows(open_response.get("positions"), "MCP_FILTERED_POSITIONS_REQUIRED")
    raw_orders = _rows(open_response.get("orders"), "MCP_FILTERED_ORDERS_REQUIRED")
    positions = []
    for row in raw_positions:
        position_id = _native_id(row, "position_id", "MCP_FILTERED_POSITION_ID_REQUIRED")
        positions.append({**row, "position_identifier": position_id})
    positions = stable_unique(positions, ("position_identifier",))
    orders = []
    for row in raw_orders:
        order_id = _native_id(row, "order_id", "MCP_FILTERED_ORDER_ID_REQUIRED")
        orders.append({**row, "order_ticket": order_id})
    orders = stable_unique(orders, ("order_ticket",))

    history_orders = responses["get_trading_history_orders"]
    historical_orders = _rows(history_orders.get("orders"), "MCP_FILTERED_HISTORY_ORDERS_REQUIRED")
    raw_deals = _rows(history_orders.get("deals"), "MCP_FILTERED_HISTORY_DEALS_REQUIRED")
    deals: list[dict[str, Any]] = []
    for row in raw_deals:
        deal_id = _native_id(row, "deal_id", "MCP_FILTERED_DEAL_ID_REQUIRED")
        position_id = _native_id(
            row, "position_id", "MCP_FILTERED_DEAL_POSITION_ID_REQUIRED", allow_zero=True
        )
        action = str(row.get("action", "")).lower()
        record_class = "cashflow" if position_id == "0" and action == "balance" else "trade"
        deals.append(
            {
                **row,
                "deal_ticket": deal_id,
                "position_identifier": position_id,
                "record_class": record_class,
                "event_time_server_raw": row.get("open_time"),
                "event_time_utc": None,
            }
        )
    deals = stable_unique(deals, ("deal_ticket",))
    historical_positions = _rows(
        responses["get_trading_history_positions"].get("positions"),
        "MCP_FILTERED_HISTORY_POSITIONS_REQUIRED",
    )

    coverage = {
        "open_positions": {
            "status": "OBSERVED" if after["server_connected"] else UNKNOWN,
            "complete": False,
            "reason": "PROVIDER_COMPLETENESS_NOT_EXPOSED",
            "count": len(positions),
            "content_hash": input_digest(positions),
        },
        "open_orders": {
            "status": "OBSERVED" if after["server_connected"] else UNKNOWN,
            "complete": False,
            "reason": "PROVIDER_COMPLETENESS_NOT_EXPOSED",
            "count": len(orders),
            "content_hash": input_digest(orders),
        },
        "historical_orders": _coverage(historical_orders, limit),
        "historical_deals": _coverage(deals, limit),
        "historical_positions": _coverage(historical_positions, limit),
        "symbol_specs": {
            "status": UNKNOWN,
            "complete": False,
            "reason": "GET_MARKETWATCH_SYMBOLS_NOT_PROJECTED",
        },
        "server_clock": {
            "status": UNKNOWN,
            "reason": "GET_TIME_INFORMATION_NOT_PROJECTED",
        },
    }
    blockers = [
        "PRODUCTION_PERMISSION_FIELDS_UNAVAILABLE",
        "CURRENT_STATE_COMPLETENESS_UNPROVEN",
        "HISTORY_COMPLETENESS_UNPROVEN",
        "SYMBOL_SPECS_UNAVAILABLE",
        "SERVER_CLOCK_UNAVAILABLE",
    ]
    if after["mcp_trade_allowed"]:
        blockers.insert(0, "MCP_TRADE_ALLOWED_IN_ORIGIN")

    account = _mapping(identities["after"].get("account"), "MCP_FILTERED_ACCOUNT_STATE_REQUIRED")
    account_state = {
        "currency": after["currency"],
        "balance": account.get("balance"),
        "equity": account.get("equity"),
        "margin": account.get("margin"),
        "margin_free": account.get("margin_free"),
        "profit": account.get("profit"),
        "capital_basis": "equity",
        "cashflows_resolved": False,
    }
    engine_candidate = {
        "schema_version": "vidar.mt5.readonly-snapshot.v1",
        "read_started_at_utc": captured_at,
        "observed_at_utc": captured_at,
        "read_finished_at_utc": captured_at,
        "identity_before": _engine_identity(before),
        "identity_after": _engine_identity(after),
        "coverage": {
            "positions": {
                "complete": False,
                "pages": 1,
                "termination": "unknown",
                "content_hash": input_digest(positions),
            },
            "orders": {
                "complete": False,
                "pages": 1,
                "termination": "unknown",
                "content_hash": input_digest(orders),
                "requested_window": [],
                "covered_window": [],
            },
            "deals": {
                "complete": False,
                "pages": 1,
                "termination": "unknown",
                "content_hash": input_digest(deals),
                "requested_window": [],
                "covered_window": [],
            },
            "symbol_specs": {
                "complete": False,
                "pages": 0,
                "termination": "not_projected",
                "content_hash": input_digest({}),
            },
        },
        "account_state": account_state,
        "positions": positions,
        "orders": orders,
        "deals": deals,
        "symbol_specs": {},
        "connectivity_indicators": {
            "source_transport": "openclaw_tool_projection",
            "direct_endpoint_access": False,
            "mcp_trade_allowed": after["mcp_trade_allowed"],
            "automation_eligible": False,
            "automation_blockers": blockers,
        },
    }
    projection: dict[str, Any] = {
        "schema_version": FILTERED_PROJECTION_SCHEMA,
        "experimental": True,
        "source_transport": "openclaw_tool_projection",
        "direct_endpoint_access": False,
        "account_key": expected_account.as_list(),
        "captured_at_utc": captured_at,
        "identity_before": before,
        "identity_after": after,
        "permissions": {
            "observed": {
                "account_read_only": after["account_read_only"],
                "experts_trade_allowed": after["experts_trade_allowed"],
                "mcp_trade_allowed": after["mcp_trade_allowed"],
            },
            "production_contract": {
                "terminal_trade_allowed": UNKNOWN,
                "account_trade_allowed": UNKNOWN,
                "account_trade_expert": UNKNOWN,
            },
        },
        "server_clock": None,
        "server_clock_status": UNKNOWN,
        "positions": positions,
        "orders": orders,
        "historical_orders": historical_orders,
        "deals": deals,
        "historical_positions": historical_positions,
        "symbol_specs": {},
        "coverage": coverage,
        "history_window_server_raw": [window_from, window_to],
        "account_state": account_state,
        "tool_receipts": sorted(call_receipts, key=lambda row: (row["tool"], str(row.get("phase")))),
        "automation_eligible": False,
        "automation_blockers": blockers,
        "engine_candidate": engine_candidate,
    }
    projection["source_hash"] = input_digest(projection)
    return projection
