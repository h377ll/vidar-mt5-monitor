"""Experimental, offline-only management simulation. There is NO executor.

A passing result is a simulated state transition, never an authorization or an
MT5 request. Production/prop accounts are always blocked, including accounts
which a broker technically labels as demo. Policy limits must be supplied by
an isolated synthetic test; none are defaults or QT trading rules.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime
import json
import sqlite3
from typing import Any, Mapping

from .contracts import canonical_json, decimal_text, decimal_value as D, input_digest, utc_timestamp


class Rejected(ValueError):
    pass


def _check(ok: bool, reason: str) -> None:
    if not ok:
        raise Rejected(reason)


def transition(state: Mapping[str, Any], request: Mapping[str, Any], policy: Mapping[str, Any], *, now: str) -> dict:
    """Evaluate an isolated-demo scenario; never mutate the supplied snapshot."""
    after = deepcopy(dict(state))
    reason = "SIMULATION_PASSED"
    status = "SIMULATED"
    try:
        _check(state.get("environment") == "ISOLATED_TEST" and state.get("account_type") == "demo", "REAL_OR_PROP_EXECUTION_BLOCKED")
        _check(policy.get("purpose") == "SYNTHETIC_SIMULATION_ONLY", "UNAPPROVED_POLICY")
        _check(state["account"] == policy["account"] == request["account"], "ACCOUNT_MISMATCH")
        _check(all(state.get(k) is True for k in ("connected", "positions_complete", "global_complete", "specs_verified")), "INCOMPLETE_DATA")
        _check(request["snapshot_digest"] == input_digest(state), "STALE_SNAPSHOT")
        delta = datetime.fromisoformat(utc_timestamp(now).replace("Z", "+00:00")) - datetime.fromisoformat(utc_timestamp(state["observed_at"]).replace("Z", "+00:00"))
        age = D(delta.days * 86400 + delta.seconds) + D(delta.microseconds) / D(1000000)
        _check(0 <= age <= D(policy["max_age_seconds"]), "STALE_OR_FUTURE_DATA")
        _check(D(policy["max_age_seconds"]) > 0, "INVALID_AGE_LIMIT")
        _check(state.get("firm_rules_verified") is True and bool(policy["firm_rules_digest"]), "FIRM_RULES_NOT_VERIFIED")
        _check(state.get("action_window_clear") is True, "ACTION_WINDOW_NOT_CLEARED")
        for name in ("account_risk_money", "global_risk_money", "account_drawdown_money", "global_drawdown_money"):
            value, limit = D(state[name]), D(policy["max_" + name])
            _check(0 <= value <= limit and limit > 0, "LIMIT_OR_UNKNOWN:" + name)
        _check(isinstance(request["reason"], str) and bool(request["reason"].strip()), "REASON_REQUIRED")
        action = request["action"]
        _check(action in {"break_even", "partial_close", "full_close"}, "OPEN_OR_EXPOSURE_INCREASE_FORBIDDEN")
        permitted = {"request_id", "account", "snapshot_digest", "position_id", "action", "reason"}
        if action == "break_even": permitted.add("new_sl")
        if action == "partial_close": permitted.add("close_volume")
        _check(set(request) == permitted, "UNSUPPORTED_FIELDS")
        positions = state["positions"]
        _check(isinstance(positions, list), "POSITIONS_REQUIRED")
        matches = [p for p in positions if p["position_id"] == request["position_id"]]
        _check(len(matches) == 1, "POSITION_NOT_UNIQUE")
        position = matches[0]
        updated = deepcopy(position)
        spec = state["specs"][position["symbol"]]
        tick, step, minimum = D(spec["tick_size"]), D(spec["volume_step"]), D(spec["volume_min"])
        _check(tick > 0 and step > 0 and minimum > 0, "INVALID_SYMBOL_SPEC")
        bid, ask = D(spec["bid"]), D(spec["ask"])
        _check(ask >= bid > 0, "INVALID_QUOTE")
        volume, old_sl = D(position["volume"]), D(position["sl"])
        _check(volume >= minimum and volume % step == 0 and old_sl > 0, "UNPROTECTED_OR_INVALID_POSITION")
        _check(position["side"] in {"buy", "sell"}, "INVALID_SIDE")
        if action == "break_even":
            new_sl, entry = D(request["new_sl"]), D(position["entry_price"])
            offset = D(policy["break_even_offset_price"])
            _check(offset >= 0 and entry > 0, "INVALID_BREAK_EVEN_POLICY")
            target = entry + offset if position["side"] == "buy" else entry - offset
            _check(new_sl == target and new_sl > 0 and new_sl % tick == 0, "BREAK_EVEN_TARGET_OR_TICK_INVALID")
            stops, freeze = D(spec["stops_distance"]), D(spec["freeze_distance"])
            _check(stops >= 0 and freeze >= 0, "INVALID_SYMBOL_SPEC")
            distance = max(stops, freeze)
            if position["side"] == "buy":
                _check(new_sl >= old_sl and new_sl < bid and bid - new_sl >= distance, "STOP_WIDENING_OR_BROKER_DISTANCE")
            else:
                _check(new_sl <= old_sl and new_sl > ask and new_sl - ask >= distance, "STOP_WIDENING_OR_BROKER_DISTANCE")
            updated["sl"] = decimal_text(new_sl)
        else:
            close = volume if action == "full_close" else D(request["close_volume"])
            _check(close > 0 and close <= volume and close % step == 0, "INVALID_CLOSE_VOLUME")
            if action == "partial_close":
                _check(close >= minimum and volume - close >= minimum, "PARTIAL_VOLUME_OR_REMAINDER_TOO_SMALL")
            updated["volume"] = decimal_text(volume - close)
        after["positions"] = [updated if p["position_id"] == position["position_id"] else deepcopy(p) for p in positions]
        # Quotes, balances, drawdown and P/L are NOT projected as executed facts.
    except Rejected as exc:
        status, reason = "BLOCKED", str(exc)
        after = deepcopy(dict(state))
    except (KeyError, TypeError, ValueError, ArithmeticError):
        status, reason = "BLOCKED", "MISSING_OR_INVALID_DATA"
        after = deepcopy(dict(state))
    return {
        "schema_version": "vidar.management.shadow.v1", "at_utc": utc_timestamp(now),
        "request_id": request.get("request_id"), "account": request.get("account"),
        "position_id": request.get("position_id"), "requested_reason": request.get("reason"),
        "status": status, "reason_code": reason, "execution_allowed": False,
        "before": deepcopy(dict(state)), "simulated_after": after,
        "request_digest": input_digest(request), "policy_digest": input_digest(policy),
        "snapshot_digest": input_digest(state), "broker_result": "NOT_SENT",
    }


def simulate_once(db: sqlite3.Connection, state: Mapping[str, Any], request: Mapping[str, Any], policy: Mapping[str, Any], *, now: str) -> dict:
    """Persist an idempotent audit; exact replay survives a connection restart.

    Reuse of a request ID for different inputs fails explicitly. This ledger
    has no worker, queue, transport or side effect other than local SQLite.
    Caller must use a dedicated test database, not a production supervisor DB.
    """
    request_id = request.get("request_id")
    if not isinstance(request_id, str) or not request_id or len(request_id) > 128:
        raise ValueError("REQUEST_ID_REQUIRED")
    digest = input_digest({"state": state, "request": request, "policy": policy})
    db.execute("CREATE TABLE IF NOT EXISTS shadow_audit (request_id TEXT PRIMARY KEY, input_digest TEXT NOT NULL, result_json TEXT NOT NULL)")
    db.commit()
    db.execute("BEGIN IMMEDIATE")
    try:
        prior = db.execute("SELECT input_digest,result_json FROM shadow_audit WHERE request_id=?", (request_id,)).fetchone()
        if prior:
            if prior[0] != digest:
                raise ValueError("IDEMPOTENCY_CONFLICT")
            result = json.loads(prior[1])
        else:
            result = transition(state, request, policy, now=now)
            db.execute("INSERT INTO shadow_audit VALUES (?,?,?)", (request_id, digest, canonical_json(result).decode()))
        db.commit()
        return result
    except BaseException:
        db.rollback()
        raise
