"""Contratos deterministas compartidos por el motor I1.

Los importes se serializan como cadenas decimales. Los hashes describen los
datos normalizados, no los objetos Python ni su orden accidental de entrada.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, is_dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from enum import Enum
import hashlib
import json
from typing import Any, Iterable, Mapping


class Quality(str, Enum):
    KNOWN = "KNOWN"
    ESTIMATED = "ESTIMATED"
    UNKNOWN = "UNKNOWN"
    UNVERIFIABLE = "UNVERIFIABLE"


class DataClass(str, Enum):
    OBSERVED = "OBSERVED"
    CALCULATED = "CALCULATED"
    SUBJECTIVE = "SUBJECTIVE"


def decimal_value(value: Any) -> Decimal:
    """Convert a scalar to finite Decimal without accepting booleans."""
    if isinstance(value, bool) or value is None:
        raise ValueError("DECIMAL_REQUIRED")
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError("INVALID_DECIMAL") from exc
    if not result.is_finite():
        raise ValueError("NON_FINITE_DECIMAL")
    return result


def decimal_text(value: Decimal) -> str:
    value = decimal_value(value)
    if value == 0:
        return "0"
    return format(value.normalize(), "f")


def _canonical(value: Any) -> Any:
    if isinstance(value, Decimal):
        return decimal_text(value)
    if isinstance(value, Enum):
        return value.value
    if is_dataclass(value):
        return _canonical(asdict(value))
    if isinstance(value, Mapping):
        return {str(key): _canonical(item) for key, item in sorted(value.items(), key=lambda row: str(row[0]))}
    if isinstance(value, (list, tuple)):
        return [_canonical(item) for item in value]
    if isinstance(value, set):
        return sorted((_canonical(item) for item in value), key=lambda item: json.dumps(item, sort_keys=True))
    if isinstance(value, float):
        raise ValueError("FLOAT_NOT_CANONICAL")
    if value is None or isinstance(value, (str, int, bool)):
        return value
    raise TypeError(f"UNSUPPORTED_CANONICAL_TYPE:{type(value).__name__}")


def canonical_json(value: Any) -> bytes:
    return json.dumps(
        _canonical(value), ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode("utf-8")


def input_digest(value: Any) -> str:
    return "sha256:" + hashlib.sha256(canonical_json(value)).hexdigest()


def utc_timestamp(value: str) -> str:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (AttributeError, ValueError) as exc:
        raise ValueError("INVALID_TIMESTAMP") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("TIMESTAMP_ZONE_REQUIRED")
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


@dataclass(frozen=True, order=True)
class AccountKey:
    server: str
    login: str

    def __post_init__(self) -> None:
        if not self.server.strip() or not self.login.strip():
            raise ValueError("ACCOUNT_IDENTITY_REQUIRED")
        if any(ord(char) < 32 for char in self.server + self.login):
            raise ValueError("ACCOUNT_IDENTITY_CONTROL_CHARACTER")

    def as_list(self) -> list[str]:
        return [self.server, self.login]

    def storage_key(self) -> str:
        return canonical_json(self.as_list()).decode("utf-8")

    @classmethod
    def from_value(cls, value: Any) -> "AccountKey":
        if isinstance(value, cls):
            return value
        if isinstance(value, Mapping):
            return cls(str(value["server"]), str(value["login"]))
        if isinstance(value, (list, tuple)) and len(value) == 2:
            return cls(str(value[0]), str(value[1]))
        raise ValueError("INVALID_ACCOUNT_KEY")


@dataclass(frozen=True)
class MetricResult:
    status: Quality
    value: Any
    unit: str
    algorithm_version: str
    input_digest: str
    reason_codes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.algorithm_version or not self.input_digest.startswith("sha256:"):
            raise ValueError("METRIC_PROVENANCE_REQUIRED")
        if self.status in {Quality.UNKNOWN, Quality.UNVERIFIABLE} and self.value is not None:
            raise ValueError("UNAVAILABLE_METRIC_HAS_VALUE")
        if self.status in {Quality.UNKNOWN, Quality.UNVERIFIABLE} and not self.reason_codes:
            raise ValueError("UNAVAILABLE_METRIC_REASON_REQUIRED")

    def as_dict(self) -> dict[str, Any]:
        return _canonical(asdict(self))

    def bytes(self) -> bytes:
        return canonical_json(self.as_dict())


@dataclass(frozen=True)
class BrokerFact:
    account: AccountKey
    fact_type: str
    native_id: str
    observed_at_utc: str
    reconciliation_run_id: str
    source_hash: str
    data: Mapping[str, Any]
    server_time_msc_raw: int | None = None

    def __post_init__(self) -> None:
        if not self.fact_type or not self.native_id or not self.reconciliation_run_id:
            raise ValueError("FACT_IDENTITY_REQUIRED")
        utc_timestamp(self.observed_at_utc)
        if not self.source_hash.startswith("sha256:"):
            raise ValueError("FACT_SOURCE_HASH_REQUIRED")
        canonical_json(self.data)

    @classmethod
    def from_observation(
        cls,
        *,
        account: AccountKey,
        fact_type: str,
        native_id: str,
        observed_at_utc: str,
        reconciliation_run_id: str,
        data: Mapping[str, Any],
        server_time_msc_raw: int | None = None,
    ) -> "BrokerFact":
        source = {
            "account_key": account.as_list(),
            "fact_type": fact_type,
            "native_id": str(native_id),
            "server_time_msc_raw": server_time_msc_raw,
            "data": data,
        }
        return cls(
            account=account,
            fact_type=fact_type,
            native_id=str(native_id),
            observed_at_utc=utc_timestamp(observed_at_utc),
            reconciliation_run_id=reconciliation_run_id,
            source_hash=input_digest(source),
            data=dict(data),
            server_time_msc_raw=server_time_msc_raw,
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema_version": "mt5.fact.v1",
            "account_key": self.account.as_list(),
            "fact_type": self.fact_type,
            "native_id": self.native_id,
            "observed_at_utc": utc_timestamp(self.observed_at_utc),
            "server_time_msc_raw": self.server_time_msc_raw,
            "reconciliation_run_id": self.reconciliation_run_id,
            "source_hash": self.source_hash,
            "data": _canonical(self.data),
        }


def stable_unique(items: Iterable[Mapping[str, Any]], identity_fields: tuple[str, ...]) -> list[dict[str, Any]]:
    """Deduplicate exact repeats and reject conflicting native identities."""
    found: dict[tuple[str, ...], bytes] = {}
    result: dict[tuple[str, ...], dict[str, Any]] = {}
    for raw in items:
        item = dict(raw)
        try:
            identity = tuple(str(item[field]) for field in identity_fields)
        except KeyError as exc:
            raise ValueError(f"MISSING_IDENTITY_FIELD:{exc.args[0]}") from exc
        encoded = canonical_json(item)
        if identity in found and found[identity] != encoded:
            raise ValueError("CONFLICTING_NATIVE_ID:" + "/".join(identity))
        found[identity] = encoded
        result[identity] = item
    return [result[key] for key in sorted(result)]
