"""Deterministic supervisor outbox with explicit uncertain-delivery handling."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import sqlite3
from typing import Any, Callable, Mapping

from engine.contracts import canonical_json, input_digest, utc_timestamp


class DefiniteDeliveryFailure(Exception):
    """The provider proved that it did not accept the message."""


def _at(value: str | None = None) -> str:
    if value is None:
        return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    return utc_timestamp(value)


def _parsed(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def render_event(row: Mapping[str, Any]) -> str:
    payload = json.loads(row["event_json"])
    login = str(row["account_login"])
    alias = f"{row['account_server']} / …{login[-4:]}"
    kind = str(row["event_kind"])
    position = row["position_identifier"] or "sin identificador"
    return (
        "[PRUEBA DEMO · Supervisor MT5]\n"
        f"Evento: {kind}\n"
        f"Cuenta: {alias}\n"
        f"Posición: {position}\n"
        f"Observado: {row['observed_at_utc']}\n"
        f"ID: {row['event_id'][:16]}\n"
        "El supervisor no ejecuta operaciones."
    )


def _transition(
    db: sqlite3.Connection,
    *,
    event_id: str,
    from_state: str,
    to_state: str,
    at: str,
    detail: Mapping[str, Any],
    updates: Mapping[str, Any] | None = None,
) -> None:
    allowed_columns = {
        "attempts",
        "attempt_started_at_utc",
        "provider_receipt_json",
        "provider_message_id",
        "delivered_at_utc",
        "acknowledged_at_utc",
        "acknowledgment_json",
        "last_error_code",
    }
    changes = dict(updates or {})
    if not set(changes).issubset(allowed_columns):
        raise ValueError("OUTBOX_UPDATE_COLUMN_FORBIDDEN")
    assignments = ["state=?", *[f"{name}=?" for name in sorted(changes)]]
    parameters = [to_state, *[changes[name] for name in sorted(changes)], event_id, from_state]
    changed = db.execute(
        f"UPDATE supervisor_outbox SET {','.join(assignments)} WHERE event_id=? AND state=?",
        parameters,
    ).rowcount
    if changed != 1:
        raise RuntimeError("OUTBOX_STATE_CHANGED_CONCURRENTLY")
    transition = input_digest([event_id, from_state, to_state, at, detail, changes])
    db.execute(
        """INSERT INTO supervisor_outbox_transitions
           (transition_id,event_id,from_state,to_state,transitioned_at_utc,detail_json)
           VALUES(?,?,?,?,?,?)""",
        (transition, event_id, from_state, to_state, at, canonical_json(detail).decode("utf-8")),
    )


def recover_attempting(db: sqlite3.Connection, *, at_utc: str | None = None) -> int:
    timestamp = _at(at_utc)
    rows = db.execute("SELECT event_id FROM supervisor_outbox WHERE state='attempting' ORDER BY event_id").fetchall()
    with db:
        for row in rows:
            _transition(
                db,
                event_id=row[0],
                from_state="attempting",
                to_state="uncertain",
                at=timestamp,
                detail={"reason": "PROCESS_RESTART_WITHOUT_PROVIDER_RECEIPT"},
                updates={"last_error_code": "RESTART_WITHOUT_RECEIPT"},
            )
    return len(rows)


def dispatch_one(
    db: sqlite3.Connection,
    sender: Callable[[str, str], Mapping[str, Any]],
    *,
    at_utc: str | None = None,
    max_age_seconds: int = 300,
) -> str:
    timestamp = _at(at_utc)
    now = _parsed(timestamp)
    db.execute("BEGIN IMMEDIATE")
    try:
        row = db.execute(
            """SELECT o.*,e.event_kind,e.position_identifier,e.observed_at_utc,e.event_json,
                      e.account_server,e.account_login
               FROM supervisor_outbox o JOIN supervisor_events e USING(event_id)
               WHERE o.state='pending' ORDER BY o.generated_at_utc,o.event_id LIMIT 1"""
        ).fetchone()
        if row is None:
            db.commit()
            return "empty"
        age = (now - _parsed(row["observed_at_utc"])).total_seconds()
        if age < 0:
            db.commit()
            return "clock_hold"
        if age > max_age_seconds:
            _transition(
                db,
                event_id=row["event_id"],
                from_state="pending",
                to_state="expired",
                at=timestamp,
                detail={"age_seconds": str(age), "maximum_seconds": max_age_seconds},
                updates={"last_error_code": "EVENT_EXPIRED"},
            )
            db.commit()
            return "expired"
        attempts = int(row["attempts"]) + 1
        _transition(
            db,
            event_id=row["event_id"],
            from_state="pending",
            to_state="attempting",
            at=timestamp,
            detail={"attempt": attempts},
            updates={"attempts": attempts, "attempt_started_at_utc": timestamp, "last_error_code": None},
        )
        db.commit()
    except BaseException:
        db.rollback()
        raise
    try:
        receipt = sender(render_event(row), row["event_id"])
        if not isinstance(receipt, Mapping) or not isinstance(receipt.get("provider_message_id"), str):
            raise ValueError("PROVIDER_RECEIPT_REQUIRED")
        if not receipt["provider_message_id"]:
            raise ValueError("PROVIDER_RECEIPT_REQUIRED")
        state = "delivered"
        error = None
    except DefiniteDeliveryFailure:
        receipt = None
        state = "failed"
        error = "DEFINITE_PROVIDER_FAILURE"
    except BaseException:
        receipt = None
        state = "uncertain"
        error = "PROVIDER_RESULT_UNCERTAIN"
    with db:
        updates: dict[str, Any] = {"last_error_code": error}
        if receipt is not None:
            updates.update(
                {
                    "provider_receipt_json": canonical_json(receipt).decode("utf-8"),
                    "provider_message_id": receipt["provider_message_id"],
                    "delivered_at_utc": timestamp,
                }
            )
        _transition(
            db,
            event_id=row["event_id"],
            from_state="attempting",
            to_state=state,
            at=timestamp,
            detail={"receipt_present": receipt is not None, "error_code": error},
            updates=updates,
        )
    return state


def acknowledge(
    db: sqlite3.Connection,
    event_id: str,
    acknowledgment: Mapping[str, Any],
    *,
    at_utc: str | None = None,
) -> None:
    timestamp = _at(at_utc)
    if acknowledgment.get("confirmed") is not True or not acknowledgment.get("actor_id"):
        raise ValueError("ACKNOWLEDGMENT_EVIDENCE_REQUIRED")
    with db:
        _transition(
            db,
            event_id=event_id,
            from_state="delivered",
            to_state="acknowledged",
            at=timestamp,
            detail={"reason": "EXACT_CONFIRMATION_ACCEPTED"},
            updates={
                "acknowledged_at_utc": timestamp,
                "acknowledgment_json": canonical_json(acknowledgment).decode("utf-8"),
            },
        )
