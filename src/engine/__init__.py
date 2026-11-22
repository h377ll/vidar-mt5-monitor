"""Motor determinista I1: contratos, reconciliación y métricas puras.

No abre red, MT5, servicios, vaults ni bases de datos por sí mismo.
"""

from .contracts import (
    AccountKey,
    BrokerFact,
    DataClass,
    MetricResult,
    Quality,
    canonical_json,
    input_digest,
)
from .alerts import AlertDecision, market_event_observations, position_change_alerts

__all__ = [
    "AccountKey",
    "BrokerFact",
    "DataClass",
    "MetricResult",
    "Quality",
    "canonical_json",
    "input_digest",
    "AlertDecision",
    "market_event_observations",
    "position_change_alerts",
]
