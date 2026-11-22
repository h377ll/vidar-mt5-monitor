"""Pure normalizer for sanitized MT5 MCP read fixtures.

This module deliberately contains no MCP transport, credentials, subprocess,
socket or runtime integration.  It validates the proposed seven-tool read
boundary and produces a deterministic bundle for offline tests.  Historical
rows remain separate from the currently observed live snapshot.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from decimal import Decimal
from typing import Any, Mapping

from engine.contracts import (
    AccountKey,
    canonical_json,
    decimal_text,
    decimal_value,
    input_digest,
    stable_unique,
    utc_timestamp,
)


READ_TOOLS = (
    "get_marketwatch_symbols",
    "get_time_information",
    "get_trading_account_info",
    "get_trading_history_orders",
    "get_trading_history_positions",
    "get_trading_open_positions",
    "get_workspace_info",
)
FIXTURE_SCHEMA = "vidar.mt5.mcp-read-fixture.v1"
BUNDLE_SCHEMA = "vidar.mt5.mcp-read-bundle.v1"
OPEN_OBSERVATION_FIXTURE_SCHEMA = "vidar.mt5.mcp-open-observation-fixture.v1"
OPEN_OBSERVATION_SCHEMA = "vidar.mt5.mcp-open-observation.v1"
OPEN_OBSERVATION_TOOLS = {
    "get_marketwatch_symbols",
    "get_time_information",
    "get_trading_account_info",
    "get_trading_open_positions",
    "get_workspace_info",
}


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


def _volume(row: Mapping[str, Any], code: str) -> Decimal:
    try:
        value = decimal_value(row.get("volume"))
    except ValueError as exc:
        raise ValueError(code) from exc
    if value <= 0:
        raise ValueError(code)
    return value


def _identity(response: Mapping[str, Any], expected: AccountKey, side: str) -> dict[str, Any]:
    account = _mapping(response.get("account"), f"MCP_{side}_ACCOUNT_REQUIRED")
    terminal = _mapping(response.get("terminal"), f"MCP_{side}_TERMINAL_REQUIRED")
    observed = AccountKey(str(account.get("server", "")), str(account.get("login", "")))
    if observed != expected:
        raise ValueError(f"MCP_{side}_IDENTITY_MISMATCH")
    if type(terminal.get("server_connected")) is not bool:
        raise ValueError(f"MCP_{side}_CONNECTION_REQUIRED")
    build = terminal.get("build")
    if type(build) is not int or build <= 0:
        raise ValueError(f"MCP_{side}_BUILD_REQUIRED")
    for field in ("experts_trade_allowed", "mcp_trade_allowed"):
        if terminal.get(field) is not False:
            raise PermissionError(f"MCP_{side}_{field.upper()}_NOT_FALSE")
    return {
        "server": expected.server,
        "login": expected.login,
        "trade_mode": str(account.get("trade_mode", "UNKNOWN")),
        "currency": str(account.get("currency", "UNKNOWN")),
        "server_connected": terminal["server_connected"],
        "terminal_build": build,
        "experts_trade_allowed": False,
        "mcp_trade_allowed": False,
    }


def _history_coverage(*, rows: list[Mapping[str, Any]], limit: int, provider: Mapping[str, Any]) -> dict[str, Any]:
    provider_partial = any(
        (
            provider.get("complete") is False,
            provider.get("truncated") is True,
            provider.get("has_more") is True,
            provider.get("hasMore") is True,
            provider.get("next_cursor") not in (None, ""),
            provider.get("nextCursor") not in (None, ""),
        )
    )
    limit_reached = len(rows) >= limit
    complete = not provider_partial and not limit_reached
    if provider_partial:
        termination = "provider_reported_partial"
    elif limit_reached:
        termination = "limit_reached"
    else:
        termination = "below_limit"
    return {
        "complete": complete,
        "count": len(rows),
        "limit": limit,
        "termination": termination,
        "content_hash": input_digest(rows),
    }


def _decimal_text_field(row: Mapping[str, Any], names: tuple[str, ...], code: str) -> str:
    for name in names:
        if row.get(name) is not None:
            try:
                return decimal_text(decimal_value(row[name]))
            except ValueError as exc:
                raise ValueError(code) from exc
    raise ValueError(code)


def _open_identity(response: Mapping[str, Any], expected: AccountKey, side: str) -> dict[str, Any]:
    account = _mapping(response.get("account"), f"MCP_OPEN_{side}_ACCOUNT_REQUIRED")
    terminal = _mapping(response.get("terminal"), f"MCP_OPEN_{side}_TERMINAL_REQUIRED")
    observed = AccountKey(str(account.get("server", "")), str(account.get("login", "")))
    if observed != expected:
        raise ValueError(f"MCP_OPEN_{side}_IDENTITY_MISMATCH")
    if type(terminal.get("server_connected")) is not bool:
        raise ValueError(f"MCP_OPEN_{side}_CONNECTION_REQUIRED")
    build = terminal.get("build")
    if type(build) is not int or build <= 0:
        raise ValueError(f"MCP_OPEN_{side}_BUILD_REQUIRED")
    if terminal.get("experts_trade_allowed") is not False:
        raise PermissionError(f"MCP_OPEN_{side}_EXPERTS_TRADE_ALLOWED_NOT_FALSE")
    if type(terminal.get("mcp_trade_allowed")) is not bool:
        raise ValueError(f"MCP_OPEN_{side}_MCP_TRADE_PERMISSION_REQUIRED")
    account_type = str(account.get("type", account.get("trade_mode", "UNKNOWN"))).lower()
    return {
        "server": expected.server,
        "login": expected.login,
        "account_type": account_type,
        "currency": str(account.get("currency", "UNKNOWN")),
        "server_connected": terminal["server_connected"],
        "terminal_build": build,
        "experts_trade_allowed": False,
        "mcp_trade_allowed": terminal["mcp_trade_allowed"],
    }


def normalize_mcp_open_observation_fixture(
    fixture: Mapping[str, Any], *, expected_account: AccountKey
) -> dict[str, Any]:
    """Normalize one selected OpenClaw MCP observation without implementing transport.

    This bounded path may record a snapshot while MCP trading remains enabled in
    the terminal because the capture was made through the filtered OpenClaw tool
    projection.  The result is explicitly ineligible for a direct/persistent MCP
    worker until origin trading is revoked.
    """

    source = _mapping(fixture, "MCP_OPEN_FIXTURE_OBJECT_REQUIRED")
    if source.get("schema_version") != OPEN_OBSERVATION_FIXTURE_SCHEMA:
        raise ValueError("MCP_OPEN_FIXTURE_SCHEMA_UNSUPPORTED")
    if source.get("capture_transport") != "openclaw_tool_projection":
        raise ValueError("MCP_OPEN_CAPTURE_TRANSPORT_INVALID")
    if source.get("direct_endpoint_access") is not False:
        raise PermissionError("MCP_OPEN_DIRECT_ENDPOINT_ACCESS_FORBIDDEN")
    tools_used = source.get("tools_used")
    if not isinstance(tools_used, list) or set(tools_used) != OPEN_OBSERVATION_TOOLS:
        raise ValueError("MCP_OPEN_TOOL_SET_INVALID")
    captured_at = utc_timestamp(str(source.get("captured_at_utc", "")))

    before_response = _mapping(source.get("account_before"), "MCP_OPEN_ACCOUNT_BEFORE_REQUIRED")
    after_response = _mapping(source.get("account_after"), "MCP_OPEN_ACCOUNT_AFTER_REQUIRED")
    before = _open_identity(before_response, expected_account, "BEFORE")
    after = _open_identity(after_response, expected_account, "AFTER")
    stable_fields = (
        "server",
        "login",
        "account_type",
        "currency",
        "server_connected",
        "terminal_build",
        "experts_trade_allowed",
        "mcp_trade_allowed",
    )
    if any(before[name] != after[name] for name in stable_fields):
        raise ValueError("MCP_OPEN_IDENTITY_CHANGED_DURING_READ")
    connected = before["server_connected"] and after["server_connected"]

    time_response = _mapping(source.get("time"), "MCP_OPEN_TIME_REQUIRED")
    mcp_utc = utc_timestamp(str(time_response.get("utc_time", "")))
    open_response = _mapping(source.get("open_positions"), "MCP_OPEN_RESPONSE_REQUIRED")
    if open_response.get("positions_complete") is not True or open_response.get("orders_complete") is not True:
        raise ValueError("MCP_OPEN_CURRENT_STATE_COVERAGE_REQUIRED")
    if open_response.get("coverage_basis") != "MCP_CURRENT_STATE_CALL_SUCCEEDED":
        raise ValueError("MCP_OPEN_CURRENT_STATE_COVERAGE_BASIS_INVALID")

    positions_raw = _rows(open_response.get("positions"), "MCP_OPEN_POSITIONS_REQUIRED")
    positions: list[dict[str, Any]] = []
    for row in positions_raw:
        position_id = _native_id(row, "position_id", "MCP_OPEN_POSITION_ID_REQUIRED")
        side = str(row.get("action", row.get("side", row.get("type", "")))).lower()
        if side not in {"buy", "sell"}:
            raise ValueError("MCP_OPEN_POSITION_SIDE_INVALID")
        positions.append(
            {
                "position_identifier": position_id,
                "symbol": str(row.get("symbol", "")),
                "side": side,
                "volume": _decimal_text_field(row, ("volume",), "MCP_OPEN_POSITION_VOLUME_INVALID"),
                "price_open": _decimal_text_field(row, ("price_open",), "MCP_OPEN_POSITION_PRICE_INVALID"),
                "current_price": _decimal_text_field(
                    row, ("price_last", "current_price"), "MCP_OPEN_POSITION_CURRENT_PRICE_INVALID"
                ),
                "sl": _decimal_text_field(row, ("stop_loss", "sl"), "MCP_OPEN_POSITION_SL_INVALID"),
                "tp": _decimal_text_field(row, ("take_profit", "tp"), "MCP_OPEN_POSITION_TP_INVALID"),
                "opened_at_server_raw": row.get("create_time", row.get("opened_at_server_raw")),
                "updated_at_server_raw": row.get("update_time", row.get("updated_at_server_raw")),
                "initial_protection_observed_at_entry": False,
                "source_observation": "mcp_current_open_positions",
            }
        )
    positions = stable_unique(positions, ("position_identifier",))
    if any(not row["symbol"] for row in positions):
        raise ValueError("MCP_OPEN_POSITION_SYMBOL_REQUIRED")

    orders_raw = _rows(open_response.get("orders"), "MCP_OPEN_ORDERS_REQUIRED")
    orders: list[dict[str, Any]] = []
    for row in orders_raw:
        order_id = _native_id(row, "order_id", "MCP_OPEN_ORDER_ID_REQUIRED")
        orders.append(
            {
                **row,
                "order_ticket": order_id,
                "position_identifier": str(row.get("position_id", row.get("position_identifier", "0"))),
            }
        )
    orders = stable_unique(orders, ("order_ticket",))

    specs_raw = _rows(source.get("symbol_specs", []), "MCP_OPEN_SYMBOL_SPECS_REQUIRED")
    symbol_specs: dict[str, dict[str, Any]] = {}
    for row in specs_raw:
        symbol = str(row.get("symbol", ""))
        if not symbol:
            raise ValueError("MCP_OPEN_SYMBOL_SPEC_NAME_REQUIRED")
        basis = {
            "tick_size": None if row.get("tick_size") is None else _decimal_text_field(
                row, ("tick_size",), "MCP_OPEN_SYMBOL_TICK_SIZE_INVALID"
            ),
            "tick_value_loss": None if row.get("tick_value_loss") is None else _decimal_text_field(
                row, ("tick_value_loss",), "MCP_OPEN_SYMBOL_TICK_VALUE_LOSS_INVALID"
            ),
            "tick_value_profit": None if row.get("tick_value_profit") is None else _decimal_text_field(
                row, ("tick_value_profit",), "MCP_OPEN_SYMBOL_TICK_VALUE_PROFIT_INVALID"
            ),
            "currency": after["currency"],
            "currency_base": row.get("currency_base"),
            "currency_profit": row.get("currency_profit"),
            "contract_size": None if row.get("contract_size") is None else _decimal_text_field(
                row, ("contract_size",), "MCP_OPEN_SYMBOL_CONTRACT_SIZE_INVALID"
            ),
            "observed_generic_tick_value": None if row.get("tick_value") is None else _decimal_text_field(
                row, ("tick_value",), "MCP_OPEN_SYMBOL_GENERIC_TICK_VALUE_INVALID"
            ),
        }
        symbol_specs[symbol] = {**basis, "spec_hash": input_digest(basis)}

    account = _mapping(after_response.get("account"), "MCP_OPEN_ACCOUNT_STATE_REQUIRED")
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
    selected: dict[str, Any] = {
        "schema_version": OPEN_OBSERVATION_SCHEMA,
        "source_transport": "openclaw_tool_projection",
        "direct_endpoint_access": False,
        "automation_eligible": False,
        "automation_blockers": (
            ["MCP_TRADE_ALLOWED_IN_ORIGIN", "NO_DIRECT_MCP_TRANSPORT"]
            if after["mcp_trade_allowed"]
            else ["NO_DIRECT_MCP_TRANSPORT"]
        ),
        "account_key": expected_account.as_list(),
        "captured_at_utc": captured_at,
        "mcp_utc_time": mcp_utc,
        "server_time_raw": time_response.get(
            "trade_server_last_known_time", time_response.get("trade_server_time")
        ),
        "identity_before": before,
        "identity_after": after,
        "account_state": account_state,
        "positions": positions,
        "orders": orders,
        "deals": [],
        "symbol_specs": symbol_specs,
        "coverage": {
            "server_connected": connected,
            "positions_complete": connected,
            "orders_complete": connected,
            "deals_complete": False,
            "basis": "MCP_CURRENT_STATE_CALL_SUCCEEDED",
        },
        "warnings": [
            "NO_DEAL_HISTORY_IN_OPEN_OBSERVATION",
            "BROKER_TIMESTAMPS_RETAINED_WITHOUT_UTC_CONVERSION",
        ],
    }
    selected["source_hash"] = input_digest(selected)
    stamp = captured_at.replace("-", "").replace(":", "").replace("T", "-").replace("Z", "")
    selected["cycle_id"] = f"mcp-open-{stamp}-{selected['source_hash'][7:23]}"
    return selected


def _historical_lifecycles(
    deals: list[dict[str, Any]], *, coverage_complete: bool
) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in deals:
        position_id = _native_id(row, "position_id", "MCP_DEAL_POSITION_ID_REQUIRED", allow_zero=True)
        if position_id != "0":
            grouped[position_id].append(row)

    lifecycles: list[dict[str, Any]] = []
    for position_id, raw_rows in sorted(grouped.items()):
        rows = sorted(raw_rows, key=lambda row: (str(row.get("time_server_raw", "")), int(str(row["deal_id"]))))
        entries = Decimal("0")
        exits = Decimal("0")
        partials: list[dict[str, Any]] = []
        issues: list[str] = []
        for row in rows:
            volume = _volume(row, "MCP_DEAL_VOLUME_INVALID")
            entry = str(row.get("entry", "")).lower().replace(" ", "_")
            if entry == "in":
                entries += volume
            elif entry in {"out", "out_by"}:
                exits += volume
                observed_remaining = entries - exits
                if observed_remaining > 0:
                    partials.append(
                        {
                            "deal_id": str(row["deal_id"]),
                            "volume": decimal_text(volume),
                            "observed_remaining": decimal_text(observed_remaining),
                            "time_server_raw": row.get("time_server_raw"),
                        }
                    )
                elif observed_remaining < 0:
                    issues.append("EXIT_VOLUME_EXCEEDS_OBSERVED_ENTRY")
            elif entry == "inout":
                issues.append("REVERSAL_REQUIRES_SEPARATE_LIFECYCLE")
            else:
                issues.append("DEAL_ENTRY_KIND_UNSUPPORTED")

        observed_remaining = entries - exits
        if entries <= 0:
            issues.append("ENTRY_EVIDENCE_MISSING")
        if not coverage_complete:
            status = "UNKNOWN"
            reason = "HISTORY_WINDOW_INCOMPLETE"
            remaining = None
        elif issues:
            status = "UNKNOWN"
            reason = issues[0]
            remaining = None
        elif observed_remaining == 0:
            status = "closed"
            reason = "FULL_VOLUME_EXIT_ACCREDITED"
            remaining = "0"
        elif observed_remaining > 0:
            status = "not_closed_by_history"
            reason = "OBSERVED_EXIT_VOLUME_INSUFFICIENT"
            remaining = decimal_text(observed_remaining)
        else:
            status = "UNKNOWN"
            reason = "EXIT_VOLUME_EXCEEDS_OBSERVED_ENTRY"
            remaining = None
        lifecycles.append(
            {
                "position_id": position_id,
                "observation_mode": "historical",
                "status": status,
                "status_reason": reason,
                "entry_volume": decimal_text(entries),
                "exit_volume": decimal_text(exits),
                "remaining_volume": remaining,
                "partial_closes": partials,
                "deal_ids": [str(row["deal_id"]) for row in rows],
                "issues": sorted(set(issues)),
            }
        )
    return lifecycles


def normalize_mcp_read_fixture(
    fixture: Mapping[str, Any], *, expected_account: AccountKey
) -> dict[str, Any]:
    """Validate selected fixture responses and return a deterministic offline bundle."""
    source = _mapping(fixture, "MCP_FIXTURE_OBJECT_REQUIRED")
    if source.get("schema_version") != FIXTURE_SCHEMA:
        raise ValueError("MCP_FIXTURE_SCHEMA_UNSUPPORTED")
    captured_at = utc_timestamp(str(source.get("captured_at_utc", "")))
    window = _mapping(source.get("history_window"), "MCP_HISTORY_WINDOW_REQUIRED")
    start_raw = window.get("from_server_raw")
    end_raw = window.get("to_server_raw")
    limit = window.get("limit")
    if not all(isinstance(value, str) and value for value in (start_raw, end_raw)):
        raise ValueError("MCP_HISTORY_WINDOW_RAW_TIME_REQUIRED")
    if type(limit) is not int or limit <= 0:
        raise ValueError("MCP_HISTORY_LIMIT_INVALID")

    calls = _rows(source.get("calls"), "MCP_CALLS_REQUIRED")
    names = [str(call.get("tool", "")) for call in calls]
    if any(name not in READ_TOOLS for name in names):
        raise PermissionError("MCP_TOOL_OUTSIDE_READ_ALLOWLIST")
    counts = Counter(names)
    expected_counts = {name: (2 if name == "get_trading_account_info" else 1) for name in READ_TOOLS}
    if counts != expected_counts:
        raise ValueError("MCP_READ_CALL_SET_INVALID")

    unique: dict[str, dict[str, Any]] = {}
    arguments_by_tool: dict[str, dict[str, Any]] = {}
    identities: dict[str, dict[str, Any]] = {}
    call_receipts: list[dict[str, Any]] = []
    for call in calls:
        name = str(call["tool"])
        arguments = _mapping(call.get("arguments", {}), "MCP_CALL_ARGUMENTS_REQUIRED")
        response = _mapping(call.get("response"), "MCP_CALL_RESPONSE_REQUIRED")
        if response.get("ok") is False or response.get("isError") is True or response.get("error") not in (None, "", False):
            raise ValueError("MCP_CALL_RESPONSE_ERROR:" + name)
        if name == "get_trading_account_info":
            phase = str(call.get("phase", ""))
            if phase not in {"before", "after"} or phase in identities:
                raise ValueError("MCP_IDENTITY_PHASE_INVALID")
            identities[phase] = response
        else:
            unique[name] = response
            arguments_by_tool[name] = arguments
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
        raise ValueError("MCP_IDENTITY_CHANGED_DURING_READ")
    connected = before["server_connected"] and after["server_connected"]

    for name in ("get_trading_history_orders", "get_trading_history_positions"):
        arguments = arguments_by_tool[name]
        if (
            arguments.get("datetime_from") != start_raw
            or arguments.get("datetime_to") != end_raw
            or arguments.get("limit") != limit
        ):
            raise ValueError("MCP_HISTORY_ARGUMENT_WINDOW_MISMATCH:" + name)

    open_response = unique["get_trading_open_positions"]
    live_positions = _rows(open_response.get("positions"), "MCP_LIVE_POSITIONS_REQUIRED")
    live_orders = _rows(open_response.get("orders"), "MCP_LIVE_ORDERS_REQUIRED")
    for row in live_positions:
        row["position_id"] = _native_id(row, "position_id", "MCP_LIVE_POSITION_ID_REQUIRED")
        _volume(row, "MCP_LIVE_POSITION_VOLUME_INVALID")
    for row in live_orders:
        row["order_id"] = _native_id(row, "order_id", "MCP_LIVE_ORDER_ID_REQUIRED")
    live_positions = stable_unique(live_positions, ("position_id",))
    live_orders = stable_unique(live_orders, ("order_id",))
    live_complete = connected and open_response.get("complete") is True

    history_orders_response = unique["get_trading_history_orders"]
    orders = _rows(history_orders_response.get("orders"), "MCP_HISTORY_ORDERS_REQUIRED")
    deals = _rows(history_orders_response.get("deals"), "MCP_HISTORY_DEALS_REQUIRED")
    for row in orders:
        row["order_id"] = _native_id(row, "order_id", "MCP_HISTORY_ORDER_ID_REQUIRED")
        row["position_id"] = _native_id(row, "position_id", "MCP_HISTORY_ORDER_POSITION_ID_REQUIRED", allow_zero=True)
    for row in deals:
        row["deal_id"] = _native_id(row, "deal_id", "MCP_HISTORY_DEAL_ID_REQUIRED")
        row["order_id"] = _native_id(row, "order_id", "MCP_HISTORY_DEAL_ORDER_ID_REQUIRED", allow_zero=True)
        row["position_id"] = _native_id(row, "position_id", "MCP_HISTORY_DEAL_POSITION_ID_REQUIRED", allow_zero=True)
        _volume(row, "MCP_DEAL_VOLUME_INVALID")
    orders = stable_unique(orders, ("order_id",))
    deals = stable_unique(deals, ("deal_id",))
    order_ids = {row["order_id"] for row in orders}
    for row in deals:
        if row["position_id"] != "0" and row["order_id"] not in order_ids:
            raise ValueError("MCP_DEAL_ORDER_NOT_IN_WINDOW")

    history_positions_response = unique["get_trading_history_positions"]
    summaries = _rows(history_positions_response.get("positions"), "MCP_HISTORY_POSITIONS_REQUIRED")
    for row in summaries:
        row["position_id"] = _native_id(row, "position_id", "MCP_HISTORY_POSITION_ID_REQUIRED")
    summaries = stable_unique(summaries, ("position_id",))

    order_coverage = _history_coverage(
        rows=max((orders, deals), key=len), limit=limit, provider=history_orders_response
    )
    position_coverage = _history_coverage(
        rows=summaries, limit=limit, provider=history_positions_response
    )
    history_complete = connected and order_coverage["complete"] and position_coverage["complete"]
    lifecycles = _historical_lifecycles(deals, coverage_complete=history_complete)

    time_response = unique["get_time_information"]
    mcp_utc = utc_timestamp(str(time_response.get("utc_time", "")))
    time_fields = {
        "captured_at_utc": captured_at,
        "mcp_utc_time": mcp_utc,
        "local_time_raw": time_response.get("local_time"),
        "server_time_raw": time_response.get("trade_server_time"),
        "reported_local_utc_offset_minutes": time_response.get("local_utc_offset_minutes"),
        "event_time_utc": None,
        "event_time_reason": "HISTORICAL_SERVER_TIME_ZONE_NOT_ACCREDITED",
    }

    workspace = unique["get_workspace_info"]
    symbols = _rows(unique["get_marketwatch_symbols"].get("symbols"), "MCP_SYMBOLS_REQUIRED")
    selected = {
        "schema_version": BUNDLE_SCHEMA,
        "source_transport": "mcp_fixture_offline",
        "account_key": expected_account.as_list(),
        "identity_before": before,
        "identity_after": after,
        "workspace": {
            "terminal_build": workspace.get("terminal_build"),
            "selected_response_hash": input_digest(workspace),
        },
        "time": time_fields,
        "tool_receipts": sorted(call_receipts, key=lambda row: (row["tool"], str(row.get("phase")))),
        "live_observation": {
            "observation_mode": "live_snapshot",
            "complete": live_complete,
            "positions": live_positions,
            "orders": live_orders,
            "content_hash": input_digest({"positions": live_positions, "orders": live_orders}),
        },
        "historical_observation": {
            "observation_mode": "historical_backfill_candidate",
            "requested_window_server_raw": [start_raw, end_raw],
            "orders": orders,
            "deals": deals,
            "position_summaries": summaries,
            "lifecycles": lifecycles,
            "coverage": {
                "orders_and_deals": order_coverage,
                "position_summaries": position_coverage,
                "complete": history_complete,
            },
            "cursor_advance_allowed": history_complete,
        },
        "symbol_specs_observed": symbols,
    }
    selected["source_hash"] = input_digest(selected)
    return selected
