"""Reconciliación determinista de observaciones MT5 de solo lectura."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from .contracts import (
    AccountKey,
    BrokerFact,
    Quality,
    canonical_json,
    input_digest,
    stable_unique,
    utc_timestamp,
)


FACT_FIELDS = {
    "order": ("order_ticket", "order.upsert"),
    "deal": ("deal_ticket", "deal.upsert"),
    "position": ("position_identifier", "position.snapshot"),
}

EVENT_REQUIRED_FIELDS = {
    "connectivity.state": ("connected",),
    "market.spread": ("symbol", "bid", "ask", "spread_points", "symbol_spec_hash"),
    "account.rollover": ("server_date", "rollover_kind"),
    "deal.swap": ("deal_ticket", "position_identifier", "swap", "currency"),
    "position.protection_change": (
        "position_identifier",
        "before_sl",
        "after_sl",
        "before_tp",
        "after_tp",
    ),
}


def _account_matches(account: AccountKey, row: Mapping[str, Any]) -> bool:
    raw = row.get("account_key")
    return raw is None or AccountKey.from_value(raw) == account


def _issue(code: str, severity: str, **context: Any) -> dict[str, Any]:
    return {"code": code, "severity": severity, "context": context}


@dataclass(frozen=True)
class ReconciliationResult:
    account: AccountKey
    run_id: str
    quality: Quality
    facts: tuple[BrokerFact, ...]
    issues: tuple[Mapping[str, Any], ...]
    coverage: Mapping[str, bool]
    input_digest: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema_version": "mt5.reconciliation.v1",
            "account_key": self.account.as_list(),
            "run_id": self.run_id,
            "quality": self.quality.value,
            "coverage": dict(self.coverage),
            "input_digest": self.input_digest,
            "facts": [fact.as_dict() for fact in self.facts],
            "issues": list(self.issues),
        }

    def bytes(self) -> bytes:
        return canonical_json(self.as_dict())


def normalize_event(
    *,
    account: AccountKey,
    event_type: str,
    native_id: str,
    observed_at_utc: str,
    run_id: str,
    payload: Mapping[str, Any],
) -> BrokerFact:
    if event_type not in EVENT_REQUIRED_FIELDS:
        raise ValueError("UNSUPPORTED_EVENT_TYPE")
    missing = [field for field in EVENT_REQUIRED_FIELDS[event_type] if field not in payload]
    if missing:
        raise ValueError("MISSING_EVENT_FIELDS:" + ",".join(missing))
    if event_type == "connectivity.state" and not isinstance(payload["connected"], bool):
        raise ValueError("CONNECTIVITY_BOOLEAN_REQUIRED")
    return BrokerFact.from_observation(
        account=account,
        fact_type=event_type,
        native_id=native_id,
        observed_at_utc=observed_at_utc,
        reconciliation_run_id=run_id,
        data=payload,
    )


def protection_change_events(
    *,
    account: AccountKey,
    previous: Iterable[Mapping[str, Any]],
    current: Iterable[Mapping[str, Any]],
    observed_at_utc: str,
    run_id: str,
) -> tuple[BrokerFact, ...]:
    before = {str(row["position_identifier"]): row for row in previous}
    after = {str(row["position_identifier"]): row for row in current}
    events = []
    for identifier in sorted(before.keys() & after.keys()):
        left, right = before[identifier], after[identifier]
        before_sl, after_sl = left.get("sl"), right.get("sl")
        before_tp, after_tp = left.get("tp"), right.get("tp")
        if (before_sl, before_tp) == (after_sl, after_tp):
            continue
        events.append(
            normalize_event(
                account=account,
                event_type="position.protection_change",
                native_id=f"{identifier}:{input_digest([before_sl, after_sl, before_tp, after_tp])[7:23]}",
                observed_at_utc=observed_at_utc,
                run_id=run_id,
                payload={
                    "position_identifier": identifier,
                    "before_sl": before_sl,
                    "after_sl": after_sl,
                    "before_tp": before_tp,
                    "after_tp": after_tp,
                },
            )
        )
    return tuple(events)


def reconcile_account_snapshot(
    *,
    account: AccountKey,
    observed_at_utc: str,
    run_id: str,
    connection: Mapping[str, Any],
    orders: Iterable[Mapping[str, Any]],
    deals: Iterable[Mapping[str, Any]],
    positions: Iterable[Mapping[str, Any]],
) -> ReconciliationResult:
    """Normalize one bounded account read without treating disconnect as flat.

    The caller owns source acquisition and history pagination. Completeness flags
    must therefore be explicit in ``connection``.
    """
    timestamp = utc_timestamp(observed_at_utc)
    if not run_id:
        raise ValueError("RUN_ID_REQUIRED")

    raw = {
        "account_key": account.as_list(),
        "observed_at_utc": timestamp,
        "run_id": run_id,
        "connection": dict(connection),
        "orders": list(orders),
        "deals": list(deals),
        "positions": list(positions),
    }
    unique = {
        kind: stable_unique(raw[kind + "s"], (identity_field,))
        for kind, (identity_field, _) in FACT_FIELDS.items()
    }
    normalized_input = {
        "account_key": account.as_list(),
        "observed_at_utc": timestamp,
        "run_id": run_id,
        "connection": dict(connection),
        "orders": unique["order"],
        "deals": unique["deal"],
        "positions": unique["position"],
    }
    issues: list[dict[str, Any]] = []
    facts: list[BrokerFact] = []

    for kind, (identity_field, fact_type) in FACT_FIELDS.items():
        for row in unique[kind]:
            if not _account_matches(account, row):
                raise ValueError(f"ACCOUNT_MISMATCH:{kind}:{row[identity_field]}")
            data = {key: value for key, value in row.items() if key != "account_key"}
            facts.append(
                BrokerFact.from_observation(
                    account=account,
                    fact_type=fact_type,
                    native_id=str(row[identity_field]),
                    observed_at_utc=timestamp,
                    reconciliation_run_id=run_id,
                    server_time_msc_raw=row.get("server_time_msc_raw"),
                    data=data,
                )
            )

    order_ids = {str(row["order_ticket"]) for row in unique["order"]}
    for deal in unique["deal"]:
        order_ticket = deal.get("order_ticket")
        if order_ticket is not None and str(order_ticket) not in order_ids:
            issues.append(
                _issue(
                    "DEAL_ORDER_NOT_IN_READ_WINDOW",
                    "ERROR" if connection.get("orders_complete") else "WARNING",
                    deal_ticket=str(deal["deal_ticket"]),
                    order_ticket=str(order_ticket),
                )
            )
        if not deal.get("position_identifier"):
            issues.append(
                _issue("DEAL_POSITION_IDENTIFIER_MISSING", "ERROR", deal_ticket=str(deal["deal_ticket"]))
            )

    position_ids = {str(row["position_identifier"]) for row in unique["position"]}
    for position in unique["position"]:
        if position.get("volume") is None:
            issues.append(
                _issue("POSITION_VOLUME_MISSING", "ERROR", position_identifier=str(position["position_identifier"]))
            )

    connected = connection.get("server_connected") is True
    coverage = {
        "server_connected": connected,
        "orders_complete": connection.get("orders_complete") is True,
        "deals_complete": connection.get("deals_complete") is True,
        "positions_complete": connection.get("positions_complete") is True,
    }
    if not connected:
        issues.append(_issue("TERMINAL_DISCONNECTED_EXPOSURE_UNKNOWN", "ERROR"))
    for name in ("orders_complete", "deals_complete", "positions_complete"):
        if connected and not coverage[name]:
            issues.append(_issue("SOURCE_COVERAGE_INCOMPLETE", "WARNING", source=name.removesuffix("_complete")))

    facts.append(
        normalize_event(
            account=account,
            event_type="connectivity.state",
            native_id=timestamp,
            observed_at_utc=timestamp,
            run_id=run_id,
            payload={
                "connected": connected,
                "coverage": coverage,
                "observed_positions": len(position_ids),
                "observed_orders": len(order_ids),
                "observed_deals": len(unique["deal"]),
            },
        )
    )

    if not connected or any(issue["severity"] == "ERROR" for issue in issues):
        quality = Quality.UNKNOWN
    elif all(coverage.values()):
        quality = Quality.KNOWN
    else:
        quality = Quality.ESTIMATED

    facts.sort(key=lambda fact: (fact.fact_type, fact.native_id, fact.source_hash))
    issues.sort(key=lambda issue: canonical_json(issue))
    return ReconciliationResult(
        account=account,
        run_id=run_id,
        quality=quality,
        facts=tuple(facts),
        issues=tuple(issues),
        coverage=coverage,
        input_digest=input_digest(normalized_input),
    )
