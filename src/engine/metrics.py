"""Funciones puras de riesgo, R, drawdown, slippage y concentración."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Iterable, Mapping

from .contracts import MetricResult, Quality, decimal_value, input_digest, utc_timestamp


ZERO = Decimal("0")
HUNDRED = Decimal("100")


def _instant(value: str) -> datetime:
    return datetime.fromisoformat(utc_timestamp(value).replace("Z", "+00:00")).astimezone(timezone.utc)


def _milliseconds(delta: Any) -> Decimal:
    return (
        Decimal(delta.days) * Decimal("86400000")
        + Decimal(delta.seconds) * Decimal("1000")
        + Decimal(delta.microseconds) / Decimal("1000")
    )


def _result(
    *,
    status: Quality,
    value: Any,
    unit: str,
    version: str,
    inputs: Any,
    reasons: Iterable[str] = (),
) -> MetricResult:
    return MetricResult(
        status=status,
        value=value,
        unit=unit,
        algorithm_version=version,
        input_digest=input_digest(inputs),
        reason_codes=tuple(sorted(set(reasons))),
    )


def stop_risk(
    *,
    direction: str,
    reference_price: Any,
    stop_price: Any,
    volume: Any,
    tick_size: Any,
    tick_value_loss: Any,
    currency: str,
    cost_buffer: Any | None = None,
) -> MetricResult:
    inputs = {
        "direction": direction,
        "reference_price": reference_price,
        "stop_price": stop_price,
        "volume": volume,
        "tick_size": tick_size,
        "tick_value_loss": tick_value_loss,
        "currency": currency,
        "cost_buffer": cost_buffer,
    }
    if any(inputs[key] is None for key in ("reference_price", "stop_price", "volume", "tick_size", "tick_value_loss")):
        return _result(
            status=Quality.UNKNOWN,
            value=None,
            unit=currency,
            version="risk.stop.v1",
            inputs=inputs,
            reasons=("MISSING_RISK_INPUT",),
        )
    if direction not in {"buy", "sell"}:
        raise ValueError("DIRECTION_BUY_OR_SELL_REQUIRED")
    reference = decimal_value(reference_price)
    stop = decimal_value(stop_price)
    amount = decimal_value(volume)
    size = decimal_value(tick_size)
    tick_loss = decimal_value(tick_value_loss)
    if amount < 0 or size <= 0 or tick_loss < 0:
        raise ValueError("INVALID_RISK_CONTRACT")
    adverse_distance = max(ZERO, reference - stop if direction == "buy" else stop - reference)
    base = (adverse_distance / size) * tick_loss * amount
    reasons = []
    if adverse_distance == 0:
        reasons.append("STOP_NOT_ADVERSE")
    if cost_buffer is None:
        status = Quality.ESTIMATED
        reasons.append("COST_BUFFER_EXCLUDED")
        value = base
    else:
        costs = decimal_value(cost_buffer)
        if costs < 0:
            raise ValueError("NEGATIVE_COST_BUFFER")
        status = Quality.KNOWN
        value = base + costs
    return _result(
        status=status,
        value=value,
        unit=currency,
        version="risk.stop.v1",
        inputs=inputs,
        reasons=reasons,
    )


def risk_percent(*, risk: Any, capital: Any, capital_basis: str) -> MetricResult:
    inputs = {"risk": risk, "capital": capital, "capital_basis": capital_basis}
    if risk is None or capital is None:
        return _result(
            status=Quality.UNKNOWN,
            value=None,
            unit="percent",
            version="risk.percent.v1",
            inputs=inputs,
            reasons=("MISSING_SIMULTANEOUS_CAPITAL_SAMPLE",),
        )
    numerator, denominator = decimal_value(risk), decimal_value(capital)
    if denominator <= 0:
        return _result(
            status=Quality.UNVERIFIABLE,
            value=None,
            unit="percent",
            version="risk.percent.v1",
            inputs=inputs,
            reasons=("NON_POSITIVE_CAPITAL",),
        )
    return _result(
        status=Quality.KNOWN,
        value=(numerator / denominator) * HUNDRED,
        unit="percent",
        version="risk.percent.v1",
        inputs=inputs,
        reasons=("CAPITAL_BASIS_" + capital_basis.upper(),),
    )


def planned_r(*, reward_to_target: Any, initial_risk: Any) -> MetricResult:
    inputs = {"reward_to_target": reward_to_target, "initial_risk": initial_risk}
    if reward_to_target is None or initial_risk is None:
        return _result(
            status=Quality.UNKNOWN,
            value=None,
            unit="R",
            version="r.planned.v1",
            inputs=inputs,
            reasons=("PLAN_OR_INITIAL_RISK_MISSING",),
        )
    reward, risk = decimal_value(reward_to_target), decimal_value(initial_risk)
    if risk <= 0:
        return _result(
            status=Quality.UNVERIFIABLE,
            value=None,
            unit="R",
            version="r.planned.v1",
            inputs=inputs,
            reasons=("INITIAL_RISK_NOT_POSITIVE",),
        )
    return _result(
        status=Quality.KNOWN,
        value=reward / risk,
        unit="R",
        version="r.planned.v1",
        inputs=inputs,
    )


def target_reward(
    *,
    direction: str,
    reference_price: Any,
    target_price: Any,
    volume: Any,
    tick_size: Any,
    tick_value_profit: Any,
    currency: str,
) -> MetricResult:
    """Value a favourable target with the effective broker contract."""
    inputs = {
        "direction": direction,
        "reference_price": reference_price,
        "target_price": target_price,
        "volume": volume,
        "tick_size": tick_size,
        "tick_value_profit": tick_value_profit,
        "currency": currency,
    }
    if any(
        inputs[key] is None
        for key in ("reference_price", "target_price", "volume", "tick_size", "tick_value_profit")
    ):
        return _result(
            status=Quality.UNKNOWN,
            value=None,
            unit=currency,
            version="reward.target.v1",
            inputs=inputs,
            reasons=("MISSING_REWARD_INPUT",),
        )
    if direction not in {"buy", "sell"}:
        raise ValueError("DIRECTION_BUY_OR_SELL_REQUIRED")
    reference = decimal_value(reference_price)
    target = decimal_value(target_price)
    amount = decimal_value(volume)
    size = decimal_value(tick_size)
    tick_profit = decimal_value(tick_value_profit)
    if amount < 0 or size <= 0 or tick_profit < 0:
        raise ValueError("INVALID_REWARD_CONTRACT")
    favourable_distance = target - reference if direction == "buy" else reference - target
    if favourable_distance <= 0:
        return _result(
            status=Quality.UNVERIFIABLE,
            value=None,
            unit=currency,
            version="reward.target.v1",
            inputs=inputs,
            reasons=("TARGET_NOT_FAVOURABLE",),
        )
    return _result(
        status=Quality.KNOWN,
        value=(favourable_distance / size) * tick_profit * amount,
        unit=currency,
        version="reward.target.v1",
        inputs=inputs,
    )


def aggregate_risk(*, risks: Iterable[MetricResult], currency: str) -> MetricResult:
    """Aggregate position risk without replacing missing values by zero."""
    rows = list(risks)
    inputs = {"currency": currency, "risks": [row.as_dict() for row in rows]}
    unavailable = [row for row in rows if row.value is None]
    if unavailable:
        reasons = {"POSITION_RISK_UNAVAILABLE"}
        for row in unavailable:
            reasons.update(row.reason_codes)
        return _result(
            status=Quality.UNKNOWN,
            value=None,
            unit=currency,
            version="risk.aggregate.v1",
            inputs=inputs,
            reasons=reasons,
        )
    if any(row.unit != currency for row in rows):
        return _result(
            status=Quality.UNVERIFIABLE,
            value=None,
            unit=currency,
            version="risk.aggregate.v1",
            inputs=inputs,
            reasons=("RISK_CURRENCY_MISMATCH",),
        )
    status = Quality.ESTIMATED if any(row.status is Quality.ESTIMATED for row in rows) else Quality.KNOWN
    reasons = ("ESTIMATED_COMPONENT_PRESENT",) if status is Quality.ESTIMATED else ()
    return _result(
        status=status,
        value=sum((decimal_value(row.value) for row in rows), ZERO),
        unit=currency,
        version="risk.aggregate.v1",
        inputs=inputs,
        reasons=reasons,
    )


def protection_deviation(
    *,
    direction: str,
    initial_stop: Any,
    current_stop: Any,
    volume: Any,
    tick_size: Any,
    tick_value_loss: Any,
    currency: str,
) -> MetricResult:
    """Measure signed SL movement; positive values reduce contractual risk."""
    inputs = {
        "direction": direction,
        "initial_stop": initial_stop,
        "current_stop": current_stop,
        "volume": volume,
        "tick_size": tick_size,
        "tick_value_loss": tick_value_loss,
        "currency": currency,
    }
    if any(
        inputs[key] is None
        for key in ("initial_stop", "current_stop", "volume", "tick_size", "tick_value_loss")
    ):
        return _result(
            status=Quality.UNKNOWN,
            value=None,
            unit=currency,
            version="protection.deviation.v1",
            inputs=inputs,
            reasons=("PROTECTION_INPUT_MISSING",),
        )
    if direction not in {"buy", "sell"}:
        raise ValueError("DIRECTION_BUY_OR_SELL_REQUIRED")
    initial = decimal_value(initial_stop)
    current = decimal_value(current_stop)
    amount = decimal_value(volume)
    size = decimal_value(tick_size)
    tick_loss = decimal_value(tick_value_loss)
    if amount < 0 or size <= 0 or tick_loss < 0:
        raise ValueError("INVALID_PROTECTION_CONTRACT")
    favourable_ticks = (current - initial) / size if direction == "buy" else (initial - current) / size
    return _result(
        status=Quality.KNOWN,
        value={
            "favourable_ticks": favourable_ticks,
            "risk_reduction_money": favourable_ticks * tick_loss * amount,
            "classification": "tightened" if favourable_ticks > 0 else "widened" if favourable_ticks < 0 else "unchanged",
        },
        unit=currency,
        version="protection.deviation.v1",
        inputs=inputs,
    )


def realized_r(
    *, net_pnl: Any, initial_risk: Any, costs_complete: bool, costs_reason: str | None = None
) -> MetricResult:
    inputs = {
        "net_pnl": net_pnl,
        "initial_risk": initial_risk,
        "costs_complete": costs_complete,
        "costs_reason": costs_reason,
    }
    if not costs_complete:
        return _result(
            status=Quality.UNVERIFIABLE,
            value=None,
            unit="R",
            version="r.realized.v1",
            inputs=inputs,
            reasons=(costs_reason or "NET_COSTS_INCOMPLETE",),
        )
    if net_pnl is None or initial_risk is None:
        return _result(
            status=Quality.UNKNOWN,
            value=None,
            unit="R",
            version="r.realized.v1",
            inputs=inputs,
            reasons=("PNL_OR_INITIAL_RISK_MISSING",),
        )
    pnl, risk = decimal_value(net_pnl), decimal_value(initial_risk)
    if risk <= 0:
        return _result(
            status=Quality.UNVERIFIABLE,
            value=None,
            unit="R",
            version="r.realized.v1",
            inputs=inputs,
            reasons=("INITIAL_RISK_NOT_POSITIVE",),
        )
    return _result(
        status=Quality.KNOWN,
        value=pnl / risk,
        unit="R",
        version="r.realized.v1",
        inputs=inputs,
    )


def max_drawdown(
    *,
    samples: Iterable[Mapping[str, Any]],
    series_kind: str,
    cashflows_resolved: bool = True,
) -> MetricResult:
    rows = [dict(row) for row in samples]
    inputs = {"samples": rows, "series_kind": series_kind, "cashflows_resolved": cashflows_resolved}
    if series_kind not in {"balance", "equity"}:
        raise ValueError("SERIES_KIND_BALANCE_OR_EQUITY")
    if not cashflows_resolved:
        return _result(
            status=Quality.UNVERIFIABLE,
            value=None,
            unit="account_currency",
            version="drawdown.v1",
            inputs=inputs,
            reasons=("CASHFLOWS_UNRESOLVED",),
        )
    if len(rows) < 2:
        return _result(
            status=Quality.UNKNOWN,
            value=None,
            unit="account_currency",
            version="drawdown.v1",
            inputs=inputs,
            reasons=("LESS_THAN_TWO_SAMPLES",),
        )
    for row in rows:
        row["observed_at_utc"] = utc_timestamp(str(row["observed_at_utc"]))
    inputs = {"samples": rows, "series_kind": series_kind, "cashflows_resolved": cashflows_resolved}
    rows.sort(key=lambda row: row["observed_at_utc"])
    values = [decimal_value(row["value"]) for row in rows]
    peak = values[0]
    maximum = ZERO
    peak_at = rows[0]["observed_at_utc"]
    trough_at = rows[0]["observed_at_utc"]
    current_peak_at = peak_at
    for row, value in zip(rows, values):
        if value > peak:
            peak = value
            current_peak_at = row["observed_at_utc"]
        drawdown = peak - value
        if drawdown > maximum:
            maximum = drawdown
            peak_at = current_peak_at
            trough_at = row["observed_at_utc"]
    status = Quality.KNOWN if series_kind == "balance" else Quality.ESTIMATED
    reasons = () if status is Quality.KNOWN else ("OBSERVED_EQUITY_LOWER_BOUND",)
    return _result(
        status=status,
        value={"maximum": maximum, "peak_at_utc": peak_at, "trough_at_utc": trough_at},
        unit="account_currency",
        version="drawdown.v1",
        inputs=inputs,
        reasons=reasons,
    )


def performance_statistics(realized_r_values: Iterable[Any | None]) -> MetricResult:
    raw = list(realized_r_values)
    inputs = {"realized_r_values": raw, "statistical_unit": "lifecycle"}
    values = [decimal_value(value) for value in raw if value is not None]
    if not values:
        return _result(
            status=Quality.UNKNOWN,
            value=None,
            unit="mixed",
            version="performance.lifecycle.v1",
            inputs=inputs,
            reasons=("NO_VERIFIED_LIFECYCLES",),
        )
    wins = [value for value in values if value > 0]
    losses = [value for value in values if value < 0]
    breakeven = len(values) - len(wins) - len(losses)
    profit_factor = sum(wins, ZERO) / -sum(losses, ZERO) if losses else None
    status = Quality.KNOWN if len(values) == len(raw) else Quality.ESTIMATED
    reasons = () if status is Quality.KNOWN else ("UNVERIFIED_LIFECYCLES_EXCLUDED",)
    if not losses:
        reasons = (*reasons, "PROFIT_FACTOR_NO_LOSSES")
    return _result(
        status=status,
        value={
            "sample_size": len(values),
            "excluded": len(raw) - len(values),
            "wins": len(wins),
            "losses": len(losses),
            "breakeven": breakeven,
            "win_rate_percent": HUNDRED * Decimal(len(wins)) / Decimal(len(values)),
            "expectancy_r": sum(values, ZERO) / Decimal(len(values)),
            "profit_factor": profit_factor,
        },
        unit="mixed",
        version="performance.lifecycle.v1",
        inputs=inputs,
        reasons=reasons,
    )


def slippage(
    *,
    direction: str,
    fill_price: Any,
    reference_price: Any | None,
    tick_size: Any,
    tick_value: Any,
    volume: Any,
    currency: str,
) -> MetricResult:
    inputs = {
        "direction": direction,
        "fill_price": fill_price,
        "reference_price": reference_price,
        "tick_size": tick_size,
        "tick_value": tick_value,
        "volume": volume,
        "currency": currency,
    }
    if reference_price is None:
        return _result(
            status=Quality.UNVERIFIABLE,
            value=None,
            unit=currency,
            version="slippage.v1",
            inputs=inputs,
            reasons=("REFERENCE_PRICE_NOT_CAPTURED",),
        )
    if direction not in {"buy", "sell"}:
        raise ValueError("DIRECTION_BUY_OR_SELL_REQUIRED")
    fill = decimal_value(fill_price)
    reference = decimal_value(reference_price)
    size = decimal_value(tick_size)
    value = decimal_value(tick_value)
    amount = decimal_value(volume)
    if size <= 0 or value < 0 or amount < 0:
        raise ValueError("INVALID_SLIPPAGE_CONTRACT")
    adverse_ticks = (fill - reference) / size if direction == "buy" else (reference - fill) / size
    return _result(
        status=Quality.KNOWN,
        value={"adverse_ticks": adverse_ticks, "adverse_money": adverse_ticks * value * amount},
        unit=currency,
        version="slippage.v1",
        inputs=inputs,
    )


def exposure_concentration(
    rows: Iterable[Mapping[str, Any]], *, amount_field: str = "gross_risk"
) -> MetricResult:
    observations = [dict(row) for row in rows]
    inputs = {"rows": observations, "amount_field": amount_field}
    if any(row.get(amount_field) is None or row.get("bucket") is None for row in observations):
        return _result(
            status=Quality.UNKNOWN,
            value=None,
            unit="share",
            version="exposure.concentration.v1",
            inputs=inputs,
            reasons=("EXPOSURE_OR_BUCKET_MISSING",),
        )
    grouped: dict[str, Decimal] = {}
    for row in observations:
        bucket = str(row["bucket"])
        grouped[bucket] = grouped.get(bucket, ZERO) + abs(decimal_value(row[amount_field]))
    total = sum(grouped.values(), ZERO)
    if total == 0:
        shares = {bucket: ZERO for bucket in sorted(grouped)}
    else:
        shares = {bucket: grouped[bucket] / total for bucket in sorted(grouped)}
    ordered = sorted(shares.values(), reverse=True)
    return _result(
        status=Quality.KNOWN,
        value={
            "gross_total": total,
            "shares": shares,
            "top_1_share": sum(ordered[:1], ZERO),
            "top_3_share": sum(ordered[:3], ZERO),
            "hhi": sum((share * share for share in ordered), ZERO),
        },
        unit="share",
        version="exposure.concentration.v1",
        inputs=inputs,
    )


def observation_duration(*, started_at_utc: Any, finished_at_utc: Any) -> MetricResult:
    """Measure provider read duration from the two captured UTC instants."""
    inputs = {"started_at_utc": started_at_utc, "finished_at_utc": finished_at_utc}
    if started_at_utc is None or finished_at_utc is None:
        return _result(
            status=Quality.UNKNOWN,
            value=None,
            unit="milliseconds",
            version="connectivity.read-duration.v1",
            inputs=inputs,
            reasons=("READ_TIMESTAMPS_MISSING",),
        )
    delta = _instant(str(finished_at_utc)) - _instant(str(started_at_utc))
    if delta.total_seconds() < 0:
        return _result(
            status=Quality.UNVERIFIABLE,
            value=None,
            unit="milliseconds",
            version="connectivity.read-duration.v1",
            inputs=inputs,
            reasons=("READ_TIME_ORDER_INVALID",),
        )
    return _result(
        status=Quality.KNOWN,
        value=_milliseconds(delta),
        unit="milliseconds",
        version="connectivity.read-duration.v1",
        inputs=inputs,
    )


def broker_clock_lag(*, observed_at_utc: Any, server_time_utc: Any) -> MetricResult:
    """Measure signed local-observation minus broker-server time when exposed."""
    inputs = {"observed_at_utc": observed_at_utc, "server_time_utc": server_time_utc}
    if observed_at_utc is None or server_time_utc is None:
        return _result(
            status=Quality.UNKNOWN,
            value=None,
            unit="milliseconds",
            version="connectivity.broker-clock-lag.v1",
            inputs=inputs,
            reasons=("BROKER_CLOCK_NOT_EXPOSED",),
        )
    delta = _instant(str(observed_at_utc)) - _instant(str(server_time_utc))
    return _result(
        status=Quality.KNOWN,
        value=_milliseconds(delta),
        unit="milliseconds",
        version="connectivity.broker-clock-lag.v1",
        inputs=inputs,
    )


def reconciliation_differences(
    *,
    coverage: Mapping[str, Any],
    issue_codes: Iterable[str],
    observed_positions: int,
    observed_orders: int,
    observed_deals: int,
) -> MetricResult:
    """Count detected reconciliation differences only under complete coverage."""
    codes = sorted(str(code) for code in issue_codes)
    inputs = {
        "coverage": dict(coverage),
        "issue_codes": codes,
        "observed_positions": observed_positions,
        "observed_orders": observed_orders,
        "observed_deals": observed_deals,
    }
    required = ("server_connected", "positions_complete", "orders_complete", "deals_complete")
    if not all(coverage.get(name) is True for name in required):
        return _result(
            status=Quality.UNKNOWN,
            value=None,
            unit="count",
            version="reconciliation.differences.v1",
            inputs=inputs,
            reasons=("RECONCILIATION_COVERAGE_INCOMPLETE",),
        )
    return _result(
        status=Quality.KNOWN,
        value={
            "detected_total": len(codes),
            "issue_codes": codes,
            "observed_positions": observed_positions,
            "observed_orders": observed_orders,
            "observed_deals": observed_deals,
        },
        unit="count",
        version="reconciliation.differences.v1",
        inputs=inputs,
    )
