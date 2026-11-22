"""Persistent read-only supervisor joining observer events and broker snapshots.

The observer stream is a trigger and an audit trail.  Position state and
closures are decided only from a connected, explicitly covered reconciliation
read.  No function in this module calls MT5 or contains a trading primitive.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import sqlite3
from typing import Any, Iterable, Mapping

from .contracts import (
    AccountKey,
    MetricResult,
    Quality,
    canonical_json,
    decimal_value,
    input_digest,
    utc_timestamp,
)
from .metrics import (
    aggregate_risk,
    broker_clock_lag,
    max_drawdown,
    observation_duration,
    planned_r,
    protection_deviation,
    reconciliation_differences,
    realized_r,
    risk_percent,
    slippage,
    stop_risk,
    target_reward,
)
from .reconciliation import reconcile_account_snapshot
from .reporting import render_automatic_note, render_human_journal_template


TEST_VAULT_MARKER = "vidar-mt5-test-vault-v1\n"
SUPERVISOR_SCHEMA_VERSION = 2
ALERTABLE_EVENT_KINDS = frozenset(
    {
        "position.open_observed",
        "position.modified",
        "position.closed_confirmed",
        "position.state_unknown",
    }
)

SUPERVISOR_SCHEMA = """
PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS supervisor_schema (
    version INTEGER PRIMARY KEY,
    applied_at_utc TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS observer_event_ledger (
    source TEXT NOT NULL,
    stream TEXT NOT NULL,
    seq INTEGER NOT NULL,
    account_server TEXT NOT NULL,
    account_login TEXT NOT NULL,
    event_kind TEXT NOT NULL,
    observed_at_utc TEXT NOT NULL,
    event_hash TEXT NOT NULL,
    envelope_json TEXT NOT NULL,
    PRIMARY KEY (source, stream, seq)
);
CREATE TABLE IF NOT EXISTS supervisor_cycles (
    cycle_id TEXT PRIMARY KEY,
    account_server TEXT NOT NULL,
    account_login TEXT NOT NULL,
    observed_at_utc TEXT NOT NULL,
    input_digest TEXT NOT NULL,
    reconciliation_quality TEXT NOT NULL,
    coverage_json TEXT NOT NULL,
    result_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS supervisor_positions (
    account_server TEXT NOT NULL,
    account_login TEXT NOT NULL,
    position_identifier TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('OPEN','CLOSED_CONFIRMED','UNKNOWN')),
    quality TEXT NOT NULL CHECK (quality IN ('KNOWN','ESTIMATED','UNKNOWN','UNVERIFIABLE')),
    first_seen_utc TEXT NOT NULL,
    last_seen_utc TEXT NOT NULL,
    opened_at_utc TEXT,
    closed_at_utc TEXT,
    initial_json TEXT NOT NULL,
    current_json TEXT,
    close_evidence_json TEXT,
    last_cycle_id TEXT NOT NULL REFERENCES supervisor_cycles(cycle_id),
    PRIMARY KEY (account_server, account_login, position_identifier)
);
CREATE TABLE IF NOT EXISTS supervisor_position_versions (
    account_server TEXT NOT NULL,
    account_login TEXT NOT NULL,
    position_identifier TEXT NOT NULL,
    version_hash TEXT NOT NULL,
    observed_at_utc TEXT NOT NULL,
    cycle_id TEXT NOT NULL REFERENCES supervisor_cycles(cycle_id),
    position_json TEXT NOT NULL,
    PRIMARY KEY (account_server, account_login, position_identifier, version_hash)
);
CREATE TABLE IF NOT EXISTS supervisor_broker_facts (
    cycle_id TEXT NOT NULL REFERENCES supervisor_cycles(cycle_id),
    fact_type TEXT NOT NULL,
    native_id TEXT NOT NULL,
    source_hash TEXT NOT NULL,
    observed_at_utc TEXT NOT NULL,
    fact_json TEXT NOT NULL,
    PRIMARY KEY (cycle_id, fact_type, native_id, source_hash)
);
CREATE TABLE IF NOT EXISTS supervisor_events (
    event_id TEXT PRIMARY KEY,
    cycle_id TEXT NOT NULL REFERENCES supervisor_cycles(cycle_id),
    account_server TEXT NOT NULL,
    account_login TEXT NOT NULL,
    position_identifier TEXT,
    event_kind TEXT NOT NULL,
    observed_at_utc TEXT NOT NULL,
    event_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS supervisor_issues (
    issue_id TEXT PRIMARY KEY,
    cycle_id TEXT NOT NULL REFERENCES supervisor_cycles(cycle_id),
    account_server TEXT NOT NULL,
    account_login TEXT NOT NULL,
    position_identifier TEXT,
    code TEXT NOT NULL,
    severity TEXT NOT NULL,
    issue_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS supervisor_metrics (
    cycle_id TEXT NOT NULL REFERENCES supervisor_cycles(cycle_id),
    scope_key TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    status TEXT NOT NULL,
    unit TEXT NOT NULL,
    algorithm_version TEXT NOT NULL,
    input_digest TEXT NOT NULL,
    reason_codes_json TEXT NOT NULL,
    value_json TEXT,
    PRIMARY KEY (cycle_id, scope_key, metric_name)
);
CREATE TABLE IF NOT EXISTS supervisor_account_samples (
    account_server TEXT NOT NULL,
    account_login TEXT NOT NULL,
    observed_at_utc TEXT NOT NULL,
    balance TEXT,
    equity TEXT,
    currency TEXT NOT NULL,
    cashflows_resolved INTEGER NOT NULL,
    cycle_id TEXT NOT NULL REFERENCES supervisor_cycles(cycle_id),
    PRIMARY KEY (account_server, account_login, observed_at_utc)
);
CREATE TABLE IF NOT EXISTS supervisor_account_states (
    cycle_id TEXT PRIMARY KEY REFERENCES supervisor_cycles(cycle_id),
    account_server TEXT NOT NULL,
    account_login TEXT NOT NULL,
    observed_at_utc TEXT NOT NULL,
    source_hash TEXT NOT NULL,
    state_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS supervisor_note_exports (
    cycle_id TEXT NOT NULL REFERENCES supervisor_cycles(cycle_id),
    position_identifier TEXT NOT NULL,
    auto_relative TEXT NOT NULL,
    human_relative TEXT NOT NULL,
    auto_digest TEXT NOT NULL,
    human_digest TEXT NOT NULL,
    exported_at_utc TEXT NOT NULL,
    PRIMARY KEY (cycle_id, position_identifier)
);
CREATE TABLE IF NOT EXISTS supervisor_outbox (
    event_id TEXT PRIMARY KEY REFERENCES supervisor_events(event_id),
    state TEXT NOT NULL CHECK (
        state IN ('pending','attempting','delivered','failed','uncertain','expired','acknowledged')
    ),
    generated_at_utc TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    attempt_started_at_utc TEXT,
    provider_receipt_json TEXT,
    provider_message_id TEXT,
    delivered_at_utc TEXT,
    acknowledged_at_utc TEXT,
    acknowledgment_json TEXT,
    last_error_code TEXT
);
CREATE TABLE IF NOT EXISTS supervisor_outbox_transitions (
    transition_id TEXT PRIMARY KEY,
    event_id TEXT NOT NULL REFERENCES supervisor_outbox(event_id),
    from_state TEXT,
    to_state TEXT NOT NULL,
    transitioned_at_utc TEXT NOT NULL,
    detail_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS supervisor_outbox_state_idx
    ON supervisor_outbox(state, generated_at_utc, event_id);
"""


def _json(value: Any) -> str:
    return canonical_json(value).decode("utf-8")


def _transport_scalars(value: Any) -> Any:
    """Normalize JSON parser floats at the transport boundary only."""
    if isinstance(value, float):
        return Decimal(str(value))
    if isinstance(value, Mapping):
        return {str(key): _transport_scalars(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_transport_scalars(item) for item in value]
    return value


def initialize_supervisor_schema(db: sqlite3.Connection, applied_at_utc: str) -> int:
    db.row_factory = sqlite3.Row
    db.executescript(SUPERVISOR_SCHEMA)
    timestamp = utc_timestamp(applied_at_utc)
    existing = [int(row[0]) for row in db.execute("SELECT version FROM supervisor_schema ORDER BY version")]
    if any(version < 1 or version > SUPERVISOR_SCHEMA_VERSION for version in existing):
        raise RuntimeError("UNSUPPORTED_SUPERVISOR_SCHEMA")
    with db:
        for version in range(1, SUPERVISOR_SCHEMA_VERSION + 1):
            db.execute(
                "INSERT OR IGNORE INTO supervisor_schema(version, applied_at_utc) VALUES(?, ?)",
                (version, timestamp),
            )
    return int(db.execute("SELECT max(version) FROM supervisor_schema").fetchone()[0])


def persist_observer_envelope(
    db: sqlite3.Connection, envelope: Mapping[str, Any], observed_at_utc: str
) -> str:
    """Persist one authenticated receiver envelope idempotently.

    Authentication remains the receiver's responsibility; this boundary checks
    the exact identity needed by the deterministic supervisor.
    """
    source = envelope.get("source")
    stream = envelope.get("stream")
    seq = envelope.get("seq")
    kind = envelope.get("kind")
    server = envelope.get("server")
    login = str(envelope.get("login", ""))
    payload = envelope.get("payload")
    if not all(isinstance(value, str) and value for value in (source, stream, server, login)):
        raise ValueError("OBSERVER_IDENTITY_REQUIRED")
    if type(seq) is not int or seq <= 0:
        raise ValueError("OBSERVER_SEQUENCE_REQUIRED")
    if kind not in {"transaction", "snapshot", "overflow", "heartbeat"} or not isinstance(payload, Mapping):
        raise ValueError("OBSERVER_EVENT_SCHEMA")
    if kind == "transaction":
        identifiers = [payload.get(name) for name in ("order_id", "deal_id", "position_ticket")]
        if not any(str(value or "") not in {"", "0"} for value in identifiers):
            raise ValueError("TRANSACTION_NATIVE_ID_REQUIRED")
    timestamp = utc_timestamp(observed_at_utc)
    normalized_envelope = _transport_scalars(envelope)
    encoded = _json(normalized_envelope)
    digest = input_digest(normalized_envelope)
    row = db.execute(
        "SELECT event_hash FROM observer_event_ledger WHERE source=? AND stream=? AND seq=?",
        (source, stream, seq),
    ).fetchone()
    if row:
        if row["event_hash"] != digest:
            raise ValueError("OBSERVER_SEQUENCE_CONTENT_CONFLICT")
        return "duplicate"
    db.execute(
        """INSERT INTO observer_event_ledger
           (source,stream,seq,account_server,account_login,event_kind,observed_at_utc,event_hash,envelope_json)
           VALUES(?,?,?,?,?,?,?,?,?)""",
        (source, stream, seq, server, login, kind, timestamp, digest, encoded),
    )
    return "stored"


def consume_receiver_inbox(db: sqlite3.Connection, source: str | None = None) -> Mapping[str, int]:
    """Copy durable Phase-B inbox rows into the supervisor ledger."""
    query = "SELECT source,body,received FROM ingress"
    params: tuple[Any, ...] = ()
    if source is not None:
        query += " WHERE source=?"
        params = (source,)
    query += " ORDER BY received,source,stream,seq"
    stored = duplicates = 0
    with db:
        for row in db.execute(query, params):
            observed = datetime.fromtimestamp(float(row["received"]), timezone.utc).isoformat()
            result = persist_observer_envelope(db, json.loads(row["body"]), observed)
            stored += result == "stored"
            duplicates += result == "duplicate"
    return {"stored": stored, "duplicates": duplicates}


def _position_id(row: Mapping[str, Any]) -> str:
    value = row.get("position_identifier", row.get("position_id", row.get("ticket")))
    if value is None or str(value) in {"", "0"}:
        raise ValueError("POSITION_IDENTIFIER_REQUIRED")
    return str(value)


def _position(row: Mapping[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result["position_identifier"] = _position_id(row)
    if "side" not in result and "type" in result:
        result["side"] = result["type"]
    return result


def _deal_position_id(row: Mapping[str, Any]) -> str | None:
    value = row.get("position_identifier", row.get("position_id"))
    return None if value is None or str(value) in {"", "0"} else str(value)


def _closure_evidence(
    position_identifier: str, deals: Iterable[Mapping[str, Any]], deals_complete: bool
) -> tuple[bool, Mapping[str, Any]]:
    relevant = [dict(row) for row in deals if _deal_position_id(row) == position_identifier]
    evidence: dict[str, Any] = {
        "position_identifier": position_identifier,
        "deals_complete": bool(deals_complete),
        "deal_tickets": sorted(str(row.get("deal_ticket", row.get("deal_id", ""))) for row in relevant),
    }
    if not deals_complete:
        return False, {**evidence, "reason": "DEAL_HISTORY_INCOMPLETE"}
    entries = Decimal("0")
    exits = Decimal("0")
    for row in relevant:
        entry = str(row.get("entry", "")).lower().replace(" ", "_")
        volume = row.get("volume")
        if volume is None:
            return False, {**evidence, "reason": "DEAL_VOLUME_MISSING"}
        amount = decimal_value(volume)
        if amount <= 0:
            return False, {**evidence, "reason": "DEAL_VOLUME_INVALID"}
        if entry == "in":
            entries += amount
        elif entry in {"out", "out_by"}:
            exits += amount
        elif entry == "inout":
            return False, {**evidence, "reason": "REVERSAL_REQUIRES_SEPARATE_LIFECYCLE"}
    evidence.update({"entry_volume": entries, "exit_volume": exits})
    if entries <= 0:
        return False, {**evidence, "reason": "ENTRY_EVIDENCE_MISSING"}
    if exits < entries:
        return False, {**evidence, "reason": "EXIT_VOLUME_INSUFFICIENT"}
    return True, {**evidence, "reason": "FULL_VOLUME_EXIT_ACCREDITED"}


def _metric_unavailable(name: str, unit: str, reason: str, inputs: Any) -> MetricResult:
    return MetricResult(
        status=Quality.UNKNOWN,
        value=None,
        unit=unit,
        algorithm_version=name + ".v1",
        input_digest=input_digest(inputs),
        reason_codes=(reason,),
    )


def _store_metric(
    db: sqlite3.Connection, cycle_id: str, scope_key: str, metric_name: str, metric: MetricResult
) -> None:
    db.execute(
        """INSERT INTO supervisor_metrics
           (cycle_id,scope_key,metric_name,status,unit,algorithm_version,input_digest,reason_codes_json,value_json)
           VALUES(?,?,?,?,?,?,?,?,?)""",
        (
            cycle_id,
            scope_key,
            metric_name,
            metric.status.value,
            metric.unit,
            metric.algorithm_version,
            metric.input_digest,
            _json(metric.reason_codes),
            None if metric.value is None else _json(metric.value),
        ),
    )


def _record_event(
    db: sqlite3.Connection,
    *,
    cycle_id: str,
    account: AccountKey,
    position_identifier: str | None,
    kind: str,
    observed_at_utc: str,
    data: Mapping[str, Any],
) -> Mapping[str, Any]:
    payload = {
        "kind": kind,
        "position_identifier": position_identifier,
        "observed_at_utc": observed_at_utc,
        "data": dict(data),
    }
    event_id = input_digest([account.as_list(), cycle_id, payload])
    inserted = db.execute(
        """INSERT OR IGNORE INTO supervisor_events
           (event_id,cycle_id,account_server,account_login,position_identifier,event_kind,observed_at_utc,event_json)
           VALUES(?,?,?,?,?,?,?,?)""",
        (event_id, cycle_id, account.server, account.login, position_identifier, kind, observed_at_utc, _json(payload)),
    ).rowcount
    if inserted and kind in ALERTABLE_EVENT_KINDS:
        db.execute(
            """INSERT INTO supervisor_outbox
               (event_id,state,generated_at_utc) VALUES(?,'pending',?)""",
            (event_id, observed_at_utc),
        )
        transition_id = input_digest([event_id, None, "pending", observed_at_utc])
        db.execute(
            """INSERT INTO supervisor_outbox_transitions
               (transition_id,event_id,from_state,to_state,transitioned_at_utc,detail_json)
               VALUES(?,?,NULL,'pending',?,?)""",
            (transition_id, event_id, observed_at_utc, _json({"reason": "DETERMINISTIC_EVENT_POLICY"})),
        )
    return {"event_id": event_id, **payload}


def _record_issue(
    db: sqlite3.Connection,
    *,
    cycle_id: str,
    account: AccountKey,
    position_identifier: str | None,
    code: str,
    severity: str,
    context: Mapping[str, Any],
) -> Mapping[str, Any]:
    payload = {"code": code, "severity": severity, "context": dict(context)}
    issue_id = input_digest([account.as_list(), cycle_id, position_identifier, payload])
    db.execute(
        """INSERT OR IGNORE INTO supervisor_issues
           (issue_id,cycle_id,account_server,account_login,position_identifier,code,severity,issue_json)
           VALUES(?,?,?,?,?,?,?,?)""",
        (issue_id, cycle_id, account.server, account.login, position_identifier, code, severity, _json(payload)),
    )
    return {"issue_id": issue_id, "position_identifier": position_identifier, **payload}


def _position_metrics(
    *,
    initial: Mapping[str, Any],
    current: Mapping[str, Any] | None,
    last_observed: Mapping[str, Any],
    deals: Iterable[Mapping[str, Any]],
    spec: Mapping[str, Any] | None,
    deals_coverage: Mapping[str, Any] | None,
    account_state: Mapping[str, Any],
    closed: bool,
) -> Mapping[str, MetricResult]:
    direction = str(initial.get("side", initial.get("type", ""))).lower()
    currency = str(account_state.get("currency") or (spec or {}).get("currency") or "UNKNOWN")
    contract = dict(spec or {})
    required = ("tick_size", "tick_value_loss", "tick_value_profit", "spec_hash")
    contract_ok = all(contract.get(key) is not None for key in required) and str(contract.get("spec_hash", "")).startswith("sha256:")
    initial_protection_observed = initial.get("initial_protection_observed_at_entry", True) is True
    if contract_ok:
        active = dict(current or {})
        current_risk = stop_risk(
            direction=direction,
            reference_price=active.get("current_price"),
            stop_price=active.get("sl"),
            volume=active.get("volume"),
            tick_size=contract["tick_size"],
            tick_value_loss=contract["tick_value_loss"],
            currency=currency,
            cost_buffer=active.get("cost_buffer"),
        )
        if initial_protection_observed:
            initial_risk = stop_risk(
                direction=direction,
                reference_price=initial.get("price_open"),
                stop_price=initial.get("sl"),
                volume=initial.get("volume"),
                tick_size=contract["tick_size"],
                tick_value_loss=contract["tick_value_loss"],
                currency=currency,
                cost_buffer=initial.get("cost_buffer"),
            )
            reward = target_reward(
                direction=direction,
                reference_price=initial.get("price_open"),
                target_price=initial.get("tp"),
                volume=initial.get("volume"),
                tick_size=contract["tick_size"],
                tick_value_profit=contract["tick_value_profit"],
                currency=currency,
            )
            protection = protection_deviation(
                direction=direction,
                initial_stop=initial.get("sl"),
                current_stop=last_observed.get("sl"),
                volume=last_observed.get("volume"),
                tick_size=contract["tick_size"],
                tick_value_loss=contract["tick_value_loss"],
                currency=currency,
            )
        else:
            unavailable_inputs = {"position": initial, "symbol_spec": spec}
            initial_risk = _metric_unavailable(
                "risk.stop", currency, "INITIAL_PROTECTION_NOT_OBSERVED_AT_ENTRY", unavailable_inputs
            )
            reward = _metric_unavailable(
                "reward.target", currency, "INITIAL_TARGET_NOT_OBSERVED_AT_ENTRY", unavailable_inputs
            )
            protection = _metric_unavailable(
                "protection.deviation",
                currency,
                "INITIAL_PROTECTION_NOT_OBSERVED_AT_ENTRY",
                unavailable_inputs,
            )
    else:
        unavailable_inputs = {"position": initial, "symbol_spec": spec}
        initial_risk = _metric_unavailable("risk.stop", currency, "BROKER_CONTRACT_SPEC_MISSING", unavailable_inputs)
        current_risk = _metric_unavailable("risk.stop", currency, "BROKER_CONTRACT_SPEC_MISSING", unavailable_inputs)
        reward = _metric_unavailable("reward.target", currency, "BROKER_CONTRACT_SPEC_MISSING", unavailable_inputs)
        protection = _metric_unavailable("protection.deviation", currency, "BROKER_CONTRACT_SPEC_MISSING", unavailable_inputs)
    capital_basis = str(account_state.get("capital_basis", "equity"))
    current_capital = account_state.get(capital_basis)
    initial_capital = initial.get("initial_capital", current_capital)
    initial_capital_basis = str(initial.get("initial_capital_basis", capital_basis))
    initial_percent = risk_percent(
        risk=initial_risk.value, capital=initial_capital, capital_basis=initial_capital_basis
    )
    current_percent = risk_percent(
        risk=current_risk.value, capital=current_capital, capital_basis=capital_basis
    )
    rr = planned_r(reward_to_target=reward.value, initial_risk=initial_risk.value)
    relevant = [dict(row) for row in deals if _deal_position_id(row) == _position_id(initial)]
    monetary_fields = ("profit", "commission", "swap", "fee")
    lifecycle_window_complete = True
    if deals_coverage is not None:
        requested = deals_coverage.get("requested_window")
        opened_at = initial.get("opened_at_utc")
        lifecycle_window_complete = (
            deals_coverage.get("complete") is True
            and isinstance(requested, list)
            and len(requested) == 2
            and opened_at is not None
            and utc_timestamp(str(opened_at)) >= utc_timestamp(str(requested[0]))
        )
    costs_complete = (
        bool(relevant)
        and lifecycle_window_complete
        and all(all(field in row and row[field] is not None for field in monetary_fields) for row in relevant)
    )
    net_pnl = None
    if costs_complete:
        net_pnl = sum(
            (sum((decimal_value(row[field]) for field in monetary_fields), Decimal("0")) for row in relevant),
            Decimal("0"),
        )
    costs_reason = None
    if not lifecycle_window_complete:
        costs_reason = "LIFECYCLE_OUTSIDE_DEAL_WINDOW"
    elif not costs_complete:
        costs_reason = "NET_COSTS_INCOMPLETE"
    realized = realized_r(
        net_pnl=net_pnl,
        initial_risk=initial_risk.value,
        costs_complete=closed and costs_complete,
        costs_reason=costs_reason,
    )
    metrics: dict[str, MetricResult] = {
        "initial_risk_money": initial_risk,
        "current_risk_money": current_risk,
        "initial_risk_percent": initial_percent,
        "current_risk_percent": current_percent,
        "reward_to_target_money": reward,
        "planned_r": rr,
        "realized_r": realized,
        "protection_deviation": protection,
    }
    for row in relevant:
        deal_id = str(row.get("deal_ticket", row.get("deal_id", "unknown")))
        deal_direction = str(row.get("direction", row.get("action", direction))).lower()
        if contract_ok:
            metric = slippage(
                direction=deal_direction,
                fill_price=row.get("price", row.get("price_open")),
                reference_price=row.get("requested_price"),
                tick_size=contract["tick_size"],
                tick_value=contract["tick_value_loss"],
                volume=row.get("volume"),
                currency=currency,
            )
        else:
            metric = _metric_unavailable("slippage", currency, "BROKER_CONTRACT_SPEC_MISSING", row)
        metrics["execution_slippage:" + deal_id] = metric
    return metrics


@dataclass(frozen=True)
class SupervisorCycleResult:
    cycle_id: str
    reconciliation_quality: str
    coverage: Mapping[str, bool]
    observer_events_total: int
    events: tuple[Mapping[str, Any], ...]
    issues: tuple[Mapping[str, Any], ...]
    lifecycles: tuple[Mapping[str, Any], ...]
    metric_count: int
    input_digest: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema_version": "vidar.mt5.supervisor-cycle.v1",
            "cycle_id": self.cycle_id,
            "reconciliation_quality": self.reconciliation_quality,
            "coverage": dict(self.coverage),
            "observer_events_total": self.observer_events_total,
            "events": list(self.events),
            "issues": list(self.issues),
            "lifecycles": list(self.lifecycles),
            "metric_count": self.metric_count,
            "input_digest": self.input_digest,
        }


def reconcile_cycle(
    db: sqlite3.Connection,
    *,
    account: AccountKey,
    observed_at_utc: str,
    cycle_id: str,
    connection: Mapping[str, Any],
    account_state: Mapping[str, Any],
    orders: Iterable[Mapping[str, Any]],
    deals: Iterable[Mapping[str, Any]],
    positions: Iterable[Mapping[str, Any]],
    symbol_specs: Mapping[str, Mapping[str, Any]],
) -> SupervisorCycleResult:
    """Atomically reconcile one bounded broker read and calculate its metrics."""
    observed = utc_timestamp(observed_at_utc)
    order_rows = [dict(row) for row in orders]
    deal_rows = [dict(row) for row in deals]
    position_rows = [_position(row) for row in positions]
    source = {
        "account": account.as_list(),
        "observed_at_utc": observed,
        "cycle_id": cycle_id,
        "connection": dict(connection),
        "account_state": dict(account_state),
        "orders": order_rows,
        "deals": deal_rows,
        "positions": position_rows,
        "symbol_specs": dict(symbol_specs),
    }
    source_digest = input_digest(source)
    existing = db.execute("SELECT input_digest,result_json FROM supervisor_cycles WHERE cycle_id=?", (cycle_id,)).fetchone()
    if existing:
        if existing["input_digest"] != source_digest:
            raise ValueError("CYCLE_ID_CONTENT_CONFLICT")
        raw = json.loads(existing["result_json"])
        return SupervisorCycleResult(
            cycle_id=raw["cycle_id"],
            reconciliation_quality=raw["reconciliation_quality"],
            coverage=raw["coverage"],
            observer_events_total=raw["observer_events_total"],
            events=tuple(raw["events"]),
            issues=tuple(raw["issues"]),
            lifecycles=tuple(raw["lifecycles"]),
            metric_count=raw["metric_count"],
            input_digest=raw["input_digest"],
        )
    reconciliation = reconcile_account_snapshot(
        account=account,
        observed_at_utc=observed,
        run_id=cycle_id,
        connection=connection,
        orders=order_rows,
        deals=deal_rows,
        positions=position_rows,
    )
    previous_rows = db.execute(
        """SELECT * FROM supervisor_positions
           WHERE account_server=? AND account_login=? AND status!='CLOSED_CONFIRMED'
           ORDER BY position_identifier""",
        (account.server, account.login),
    ).fetchall()
    previous = {row["position_identifier"]: row for row in previous_rows}
    current = {_position_id(row): row for row in position_rows}
    can_compare_absence = reconciliation.coverage["server_connected"] and reconciliation.coverage["positions_complete"]
    events: list[Mapping[str, Any]] = []
    issues: list[Mapping[str, Any]] = []
    lifecycles: list[Mapping[str, Any]] = []
    metric_count = 0

    with db:
        db.execute(
            """INSERT INTO supervisor_cycles
               (cycle_id,account_server,account_login,observed_at_utc,input_digest,reconciliation_quality,coverage_json,result_json)
               VALUES(?,?,?,?,?,?,?,?)""",
            (cycle_id, account.server, account.login, observed, source_digest, reconciliation.quality.value, _json(reconciliation.coverage), "{}"),
        )
        for issue in reconciliation.issues:
            issues.append(
                _record_issue(
                    db,
                    cycle_id=cycle_id,
                    account=account,
                    position_identifier=None,
                    code=str(issue["code"]),
                    severity=str(issue["severity"]),
                    context=dict(issue.get("context", {})),
                )
            )
        for fact in reconciliation.facts:
            db.execute(
                """INSERT INTO supervisor_broker_facts
                   (cycle_id,fact_type,native_id,source_hash,observed_at_utc,fact_json)
                   VALUES(?,?,?,?,?,?)""",
                (
                    cycle_id,
                    fact.fact_type,
                    fact.native_id,
                    fact.source_hash,
                    fact.observed_at_utc,
                    _json(fact.as_dict()),
                ),
            )
        touched: dict[
            str, tuple[Mapping[str, Any], Mapping[str, Any] | None, Mapping[str, Any], bool]
        ] = {}
        for position_identifier, row in sorted(current.items()):
            old = previous.get(position_identifier)
            if old is None:
                basis = str(account_state.get("capital_basis", "equity"))
                initial = {
                    **row,
                    "initial_capital": account_state.get(basis),
                    "initial_capital_basis": basis,
                    "account_currency": account_state.get("currency"),
                }
            else:
                initial = json.loads(old["initial_json"])
            kind = "position.baseline_observed" if old is None and not previous else "position.open_observed" if old is None else None
            if kind:
                events.append(
                    _record_event(
                        db,
                        cycle_id=cycle_id,
                        account=account,
                        position_identifier=position_identifier,
                        kind=kind,
                        observed_at_utc=observed,
                        data={"position": row, "time_is_detection": True},
                    )
                )
            if old is not None:
                before = json.loads(old["current_json"] or old["initial_json"])
                changed = {
                    field: {"before": before.get(field), "after": row.get(field)}
                    for field in ("volume", "price_open", "sl", "tp")
                    if before.get(field) != row.get(field)
                }
                if changed:
                    events.append(
                        _record_event(
                            db,
                            cycle_id=cycle_id,
                            account=account,
                            position_identifier=position_identifier,
                            kind="position.modified",
                            observed_at_utc=observed,
                            data={"changes": changed, "time_is_detection": True},
                        )
                    )
            version_hash = input_digest(row)
            db.execute(
                """INSERT OR IGNORE INTO supervisor_position_versions
                   (account_server,account_login,position_identifier,version_hash,observed_at_utc,cycle_id,position_json)
                   VALUES(?,?,?,?,?,?,?)""",
                (account.server, account.login, position_identifier, version_hash, observed, cycle_id, _json(row)),
            )
            db.execute(
                """INSERT INTO supervisor_positions
                   (account_server,account_login,position_identifier,status,quality,first_seen_utc,last_seen_utc,
                    opened_at_utc,closed_at_utc,initial_json,current_json,close_evidence_json,last_cycle_id)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(account_server,account_login,position_identifier) DO UPDATE SET
                    status='OPEN',quality=excluded.quality,last_seen_utc=excluded.last_seen_utc,
                    current_json=excluded.current_json,close_evidence_json=NULL,last_cycle_id=excluded.last_cycle_id""",
                (
                    account.server,
                    account.login,
                    position_identifier,
                    "OPEN",
                    Quality.KNOWN.value if can_compare_absence else Quality.ESTIMATED.value,
                    observed if old is None else old["first_seen_utc"],
                    observed,
                    row.get("opened_at_utc"),
                    None,
                    _json(initial),
                    _json(row),
                    None,
                    cycle_id,
                ),
            )
            touched[position_identifier] = (initial, row, row, False)
            lifecycles.append({"position_identifier": position_identifier, "status": "OPEN", "quality": "KNOWN" if can_compare_absence else "ESTIMATED"})

        for position_identifier, old in sorted(previous.items()):
            if position_identifier in current:
                continue
            initial = json.loads(old["initial_json"])
            before = json.loads(old["current_json"] or old["initial_json"])
            if can_compare_absence:
                closed, evidence = _closure_evidence(position_identifier, deal_rows, reconciliation.coverage["deals_complete"])
            else:
                closed, evidence = False, {"reason": "POSITION_SNAPSHOT_NOT_COMPLETE_OR_CONNECTED"}
            status = "CLOSED_CONFIRMED" if closed else "UNKNOWN"
            quality = Quality.KNOWN.value if closed else Quality.UNKNOWN.value
            if closed:
                events.append(
                    _record_event(
                        db,
                        cycle_id=cycle_id,
                        account=account,
                        position_identifier=position_identifier,
                        kind="position.closed_confirmed",
                        observed_at_utc=observed,
                        data={"last_position": before, "close_evidence": evidence},
                    )
                )
            else:
                issues.append(
                    _record_issue(
                        db,
                        cycle_id=cycle_id,
                        account=account,
                        position_identifier=position_identifier,
                        code="POSITION_DISAPPEARED_WITHOUT_ACCREDITED_CLOSE",
                        severity="ERROR",
                        context=evidence,
                    )
                )
                events.append(
                    _record_event(
                        db,
                        cycle_id=cycle_id,
                        account=account,
                        position_identifier=position_identifier,
                        kind="position.state_unknown",
                        observed_at_utc=observed,
                        data={"last_position": before, "reason": evidence},
                    )
                )
            db.execute(
                """UPDATE supervisor_positions SET status=?,quality=?,last_seen_utc=?,closed_at_utc=?,
                   current_json=NULL,close_evidence_json=?,last_cycle_id=?
                   WHERE account_server=? AND account_login=? AND position_identifier=?""",
                (
                    status,
                    quality,
                    observed,
                    observed if closed else None,
                    _json(evidence),
                    cycle_id,
                    account.server,
                    account.login,
                    position_identifier,
                ),
            )
            touched[position_identifier] = (initial, None, before, closed)
            lifecycles.append({"position_identifier": position_identifier, "status": status, "quality": quality, "close_evidence": evidence})

        currency = str(account_state.get("currency", "UNKNOWN"))
        balance = account_state.get("balance")
        equity = account_state.get("equity")
        db.execute(
            """INSERT INTO supervisor_account_samples
               (account_server,account_login,observed_at_utc,balance,equity,currency,cashflows_resolved,cycle_id)
               VALUES(?,?,?,?,?,?,?,?)""",
            (
                account.server,
                account.login,
                observed,
                None if balance is None else str(balance),
                None if equity is None else str(equity),
                currency,
                int(account_state.get("cashflows_resolved") is True),
                cycle_id,
            ),
        )
        db.execute(
            """INSERT INTO supervisor_account_states
               (cycle_id,account_server,account_login,observed_at_utc,source_hash,state_json)
               VALUES(?,?,?,?,?,?)""",
            (cycle_id, account.server, account.login, observed, input_digest(account_state), _json(account_state)),
        )
        current_risks: list[MetricResult] = []
        current_risks_by_symbol: dict[str, list[MetricResult]] = {}
        current_risks_by_currency: dict[str, list[MetricResult]] = {}
        for position_identifier, (initial, active, last_observed, closed) in sorted(touched.items()):
            symbol = str(initial.get("symbol", ""))
            metrics = _position_metrics(
                initial=initial,
                current=active,
                last_observed=last_observed,
                deals=deal_rows,
                spec=symbol_specs.get(symbol),
                deals_coverage=dict(connection.get("coverage", {})).get("deals"),
                account_state=account_state,
                closed=closed,
            )
            for name, metric in sorted(metrics.items()):
                _store_metric(db, cycle_id, "position:" + position_identifier, name, metric)
                metric_count += 1
            if active is not None:
                risk = metrics["current_risk_money"]
                current_risks.append(risk)
                current_risks_by_symbol.setdefault(symbol, []).append(risk)
                current_risks_by_currency.setdefault(risk.unit, []).append(risk)

        if can_compare_absence:
            account_risk = aggregate_risk(risks=current_risks, currency=currency)
        else:
            account_risk = _metric_unavailable(
                "risk.aggregate", currency, "POSITION_COVERAGE_INCOMPLETE", reconciliation.coverage
            )
        _store_metric(db, cycle_id, "account", "aggregate_current_risk", account_risk)
        metric_count += 1
        for scope_prefix, groups in (
            ("instrument", current_risks_by_symbol),
            ("currency", current_risks_by_currency),
        ):
            for bucket, risks in sorted(groups.items()):
                grouped = (
                    aggregate_risk(risks=risks, currency=currency)
                    if can_compare_absence
                    else _metric_unavailable(
                        "risk.aggregate", currency, "POSITION_COVERAGE_INCOMPLETE", reconciliation.coverage
                    )
                )
                _store_metric(db, cycle_id, f"{scope_prefix}:{bucket}", "aggregate_current_risk", grouped)
                metric_count += 1

        connectivity_metrics = (
            (
                "snapshot_read_duration",
                observation_duration(
                    started_at_utc=connection.get("read_started_at_utc"),
                    finished_at_utc=connection.get("read_finished_at_utc"),
                ),
            ),
            (
                "broker_clock_lag",
                broker_clock_lag(
                    observed_at_utc=observed,
                    server_time_utc=dict(connection.get("identity_after", {})).get("server_time_utc"),
                ),
            ),
            (
                "reconciliation_differences",
                reconciliation_differences(
                    coverage=reconciliation.coverage,
                    issue_codes=(str(issue["code"]) for issue in issues),
                    observed_positions=len(position_rows),
                    observed_orders=len(order_rows),
                    observed_deals=len(deal_rows),
                ),
            ),
        )
        for name, metric in connectivity_metrics:
            _store_metric(db, cycle_id, "account", name, metric)
            metric_count += 1
        samples = db.execute(
            """SELECT observed_at_utc,balance,equity,cashflows_resolved FROM supervisor_account_samples
               WHERE account_server=? AND account_login=? ORDER BY observed_at_utc""",
            (account.server, account.login),
        ).fetchall()
        balance_samples = [
            {"observed_at_utc": row["observed_at_utc"], "value": row["balance"]}
            for row in samples
            if row["balance"] is not None
        ]
        equity_samples = [
            {"observed_at_utc": row["observed_at_utc"], "value": row["equity"]}
            for row in samples
            if row["equity"] is not None
        ]
        cashflows_resolved = bool(samples) and all(row["cashflows_resolved"] for row in samples)
        for name, metric in (
            ("realized_drawdown", max_drawdown(samples=balance_samples, series_kind="balance", cashflows_resolved=cashflows_resolved)),
            ("floating_drawdown", max_drawdown(samples=equity_samples, series_kind="equity", cashflows_resolved=cashflows_resolved)),
        ):
            _store_metric(db, cycle_id, "account", name, metric)
            metric_count += 1

        result = SupervisorCycleResult(
            cycle_id=cycle_id,
            reconciliation_quality=reconciliation.quality.value,
            coverage=reconciliation.coverage,
            observer_events_total=int(db.execute("SELECT count(*) FROM observer_event_ledger").fetchone()[0]),
            events=tuple(events),
            issues=tuple(issues),
            lifecycles=tuple(lifecycles),
            metric_count=metric_count,
            input_digest=source_digest,
        )
        db.execute("UPDATE supervisor_cycles SET result_json=? WHERE cycle_id=?", (_json(result.as_dict()), cycle_id))
    return result


def _atomic_write(path: Path, content: str, *, replace: bool) -> None:
    if path.is_symlink():
        raise ValueError("SYMLINK_NOTE_DESTINATION")
    if path.exists() and not replace:
        return
    if path.exists() and path.read_text(encoding="utf-8") == content:
        return
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w", encoding="utf-8") as stream:
        stream.write(content)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def export_test_notes(
    db: sqlite3.Connection, *, cycle_id: str, destination: Path, exported_at_utc: str
) -> Mapping[str, Any]:
    """Project touched lifecycles into a marked Obsidian test vault."""
    root = destination.resolve()
    marker = root / ".vidar-mt5-test-vault"
    if not root.is_dir() or not marker.is_file() or marker.read_text(encoding="utf-8") != TEST_VAULT_MARKER:
        raise ValueError("OBSIDIAN_TEST_VAULT_MARKER_REQUIRED")
    if root.is_symlink():
        raise ValueError("SYMLINK_TEST_VAULT")
    cycle = db.execute("SELECT * FROM supervisor_cycles WHERE cycle_id=?", (cycle_id,)).fetchone()
    if cycle is None:
        raise ValueError("CYCLE_NOT_FOUND")
    result = json.loads(cycle["result_json"])
    exported = []
    for lifecycle in result["lifecycles"]:
        position_identifier = str(lifecycle["position_identifier"])
        position = db.execute(
            """SELECT * FROM supervisor_positions
               WHERE account_server=? AND account_login=? AND position_identifier=?""",
            (cycle["account_server"], cycle["account_login"], position_identifier),
        ).fetchone()
        initial = json.loads(position["initial_json"])
        current = json.loads(position["current_json"]) if position["current_json"] else None
        calculated: dict[str, MetricResult] = {}
        for row in db.execute(
            "SELECT * FROM supervisor_metrics WHERE cycle_id=? AND scope_key=? ORDER BY metric_name",
            (cycle_id, "position:" + position_identifier),
        ):
            calculated[row["metric_name"]] = MetricResult(
                status=Quality(row["status"]),
                value=None if row["value_json"] is None else json.loads(row["value_json"]),
                unit=row["unit"],
                algorithm_version=row["algorithm_version"],
                input_digest=row["input_digest"],
                reason_codes=tuple(json.loads(row["reason_codes_json"])),
            )
        trade_id = f"{cycle['account_server']}-{cycle['account_login']}-{position_identifier}"
        observed = [
            {
                "key": "account",
                "server": cycle["account_server"],
                "login": cycle["account_login"],
                "state": json.loads(
                    db.execute(
                        "SELECT state_json FROM supervisor_account_states WHERE cycle_id=?", (cycle_id,)
                    ).fetchone()[0]
                ),
                "source_hash": cycle["input_digest"],
            },
            {
                "key": "lifecycle",
                "position_identifier": position_identifier,
                "symbol": initial.get("symbol"),
                "direction": initial.get("side", initial.get("type")),
                "status": position["status"],
                "first_seen_utc": position["first_seen_utc"],
                "last_seen_utc": position["last_seen_utc"],
                "opened_at_utc": position["opened_at_utc"],
                "closed_at_utc": position["closed_at_utc"],
                "initial": initial,
                "current": current,
                "close_evidence": None if position["close_evidence_json"] is None else json.loads(position["close_evidence_json"]),
                "source_hash": input_digest([initial, current, lifecycle]),
            },
        ]
        facts = []
        for row in db.execute(
            """SELECT fact_json FROM supervisor_broker_facts
               WHERE cycle_id=? AND fact_type IN ('order.upsert','deal.upsert','position.snapshot')
               ORDER BY fact_type,native_id""",
            (cycle_id,),
        ):
            fact = json.loads(row["fact_json"])
            linked = str(fact.get("data", {}).get("position_identifier", ""))
            if fact["fact_type"] == "position.snapshot":
                linked = str(fact["native_id"])
            if linked == position_identifier:
                facts.append(fact)
        observed.append(
            {
                "key": "broker_identifiers",
                "order_tickets": sorted(
                    fact["native_id"] for fact in facts if fact["fact_type"] == "order.upsert"
                ),
                "deal_tickets": sorted(
                    fact["native_id"] for fact in facts if fact["fact_type"] == "deal.upsert"
                ),
                "position_identifier": position_identifier,
                "source_hash": input_digest(facts),
            }
        )
        auto_relative = f"Automaticas/{trade_id}.md"
        human_relative = f"Humanas/{trade_id}.md"
        auto = render_automatic_note(trade_id=trade_id, observed=observed, calculated=calculated)
        human = render_human_journal_template(
            trade_id=trade_id,
            auto_note_relative="../" + auto_relative,
            idea_id="",
        )
        _atomic_write(root / auto_relative, auto, replace=True)
        _atomic_write(root / human_relative, human, replace=False)
        auto_digest = "sha256:" + hashlib.sha256(auto.encode("utf-8")).hexdigest()
        human_bytes = (root / human_relative).read_bytes()
        human_digest = "sha256:" + hashlib.sha256(human_bytes).hexdigest()
        db.execute(
            """INSERT INTO supervisor_note_exports
               (cycle_id,position_identifier,auto_relative,human_relative,auto_digest,human_digest,exported_at_utc)
               VALUES(?,?,?,?,?,?,?)
               ON CONFLICT(cycle_id,position_identifier) DO UPDATE SET
                auto_digest=excluded.auto_digest,human_digest=excluded.human_digest,exported_at_utc=excluded.exported_at_utc""",
            (cycle_id, position_identifier, auto_relative, human_relative, auto_digest, human_digest, utc_timestamp(exported_at_utc)),
        )
        exported.append(
            {
                "position_identifier": position_identifier,
                "auto_relative": auto_relative,
                "human_relative": human_relative,
                "auto_digest": auto_digest,
                "human_digest": human_digest,
            }
        )
    db.commit()
    return {"cycle_id": cycle_id, "test_vault": str(root), "notes": exported}
