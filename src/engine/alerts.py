"""Decisiones deterministas de observación; no envía ni modifica operaciones."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Iterable, Mapping

from .contracts import AccountKey, canonical_json, decimal_value, input_digest, utc_timestamp


@dataclass(frozen=True)
class AlertDecision:
    key: str
    kind: str
    severity: str
    observed_at_utc: str
    data: Mapping[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def _decision(
    *, account: AccountKey, kind: str, severity: str, observed_at_utc: str, data: Mapping[str, Any]
) -> AlertDecision:
    normalized_time = utc_timestamp(observed_at_utc)
    payload = {"account_key": account.as_list(), "kind": kind, "observed_at_utc": normalized_time, "data": data}
    return AlertDecision(
        key=f"{kind}:{input_digest(payload)[7:31]}",
        kind=kind,
        severity=severity,
        observed_at_utc=normalized_time,
        data=dict(data),
    )


def _position_map(rows: Iterable[Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
    result = {}
    for raw in rows:
        row = dict(raw)
        identifier = str(row["position_identifier"])
        if identifier in result and canonical_json(result[identifier]) != canonical_json(row):
            raise ValueError("CONFLICTING_POSITION_SNAPSHOT")
        result[identifier] = row
    return result


def position_change_alerts(
    *,
    account: AccountKey,
    previous: Iterable[Mapping[str, Any]],
    current: Iterable[Mapping[str, Any]],
    connected_before: bool | None,
    connected_after: bool,
    observed_at_utc: str,
) -> tuple[AlertDecision, ...]:
    """Compare verified snapshots; disconnect cannot fabricate closures."""
    before = _position_map(previous)
    after = _position_map(current)
    decisions: list[AlertDecision] = []
    if not connected_after:
        return (
            _decision(
                account=account,
                kind="coverage.terminal_disconnected",
                severity="warning",
                observed_at_utc=observed_at_utc,
                data={"previous_position_count": len(before), "exposure": "UNKNOWN"},
            ),
        )
    if connected_before is not True:
        decisions.append(
            _decision(
                account=account,
                kind="coverage.baseline_observed",
                severity="info",
                observed_at_utc=observed_at_utc,
                data={"observed_position_count": len(after), "new_trades_demonstrated": False},
            )
        )
        return tuple(decisions)

    for identifier in sorted(before.keys() | after.keys()):
        left, right = before.get(identifier), after.get(identifier)
        if left is None:
            decisions.append(
                _decision(
                    account=account, kind="position.new", severity="info", observed_at_utc=observed_at_utc,
                    data={"position_identifier": identifier, "symbol": right.get("symbol"), "side": right.get("side"), "volume": right.get("volume")},
                )
            )
            continue
        if right is None:
            decisions.append(
                _decision(
                    account=account, kind="position.closed", severity="info", observed_at_utc=observed_at_utc,
                    data={"position_identifier": identifier, "symbol": left.get("symbol")},
                )
            )
            continue
        if left.get("side") != right.get("side"):
            decisions.append(
                _decision(
                    account=account, kind="position.reversed", severity="warning", observed_at_utc=observed_at_utc,
                    data={"position_identifier": identifier, "before_side": left.get("side"), "after_side": right.get("side"), "after_volume": right.get("volume")},
                )
            )
        else:
            before_volume = decimal_value(left["volume"])
            after_volume = decimal_value(right["volume"])
            if after_volume != before_volume:
                kind = "position.increased" if after_volume > before_volume else "position.decreased"
                decisions.append(
                    _decision(
                        account=account, kind=kind, severity="warning" if kind.endswith("increased") else "info",
                        observed_at_utc=observed_at_utc,
                        data={"position_identifier": identifier, "before_volume": left["volume"], "after_volume": right["volume"]},
                    )
                )
        if (left.get("sl"), left.get("tp")) != (right.get("sl"), right.get("tp")):
            decisions.append(
                _decision(
                    account=account, kind="position.protection_changed", severity="warning", observed_at_utc=observed_at_utc,
                    data={
                        "position_identifier": identifier,
                        "before_sl": left.get("sl"), "after_sl": right.get("sl"),
                        "before_tp": left.get("tp"), "after_tp": right.get("tp"),
                    },
                )
            )
    return tuple(sorted(decisions, key=lambda decision: (decision.kind, decision.key)))


def market_event_observations(
    *,
    account: AccountKey,
    positions: Iterable[Mapping[str, Any]],
    event_type: str,
    payload: Mapping[str, Any],
    observed_at_utc: str,
) -> tuple[AlertDecision, ...]:
    """Create factual rollover/spread/swap observations without invented limits."""
    current = list(positions)
    if event_type == "market.spread":
        symbol = payload.get("symbol")
        matched = [row for row in current if row.get("symbol") == symbol]
        if not matched:
            return ()
        data = {
            "symbol": symbol,
            "open_position_count": len(matched),
            "spread_points": payload.get("spread_points"),
            "symbol_spec_hash": payload.get("symbol_spec_hash"),
            "threshold_applied": False,
        }
        kind = "market.spread_observed_with_exposure"
    elif event_type == "account.rollover":
        if not current:
            return ()
        data = {
            "server_date": payload.get("server_date"),
            "rollover_kind": payload.get("rollover_kind"),
            "open_position_count": len(current),
            "threshold_applied": False,
        }
        kind = "market.rollover_observed_with_exposure"
    elif event_type == "deal.swap":
        identifier = str(payload.get("position_identifier"))
        if identifier not in {str(row.get("position_identifier")) for row in current}:
            return ()
        data = {
            "position_identifier": identifier,
            "deal_ticket": payload.get("deal_ticket"),
            "swap": payload.get("swap"),
            "currency": payload.get("currency"),
        }
        kind = "market.swap_observed"
    else:
        raise ValueError("UNSUPPORTED_MARKET_EVENT")
    return (
        _decision(
            account=account,
            kind=kind,
            severity="info",
            observed_at_utc=observed_at_utc,
            data=data,
        ),
    )
