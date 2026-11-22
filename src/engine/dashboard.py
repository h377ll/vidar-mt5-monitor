"""Read-only projection of supervisor metrics for an isolated test dashboard.

This module does not calculate trading metrics.  It projects the versioned
``supervisor_metrics`` rows produced by the deterministic engine and removes
the raw broker account identity from the exported dataset.
"""

from __future__ import annotations

from collections import Counter
import json
import re
import sqlite3
from typing import Any, Mapping

from .contracts import canonical_json, input_digest


DASHBOARD_SCHEMA_VERSION = "vidar.dashboard.test.v1"
SYNTHETIC_DATASET_KIND = "SYNTHETIC_TEST"
_SYNTHETIC_LABEL = re.compile(r"SYNTHETIC-[A-Z0-9][A-Z0-9_-]{2,63}")
_REQUIRED_TABLES = frozenset(
    {
        "supervisor_cycles",
        "supervisor_metrics",
        "supervisor_account_states",
    }
)


def _available_tables(db: sqlite3.Connection) -> set[str]:
    return {
        str(row[0])
        for row in db.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    }


def _account_ref(server: str, login: str) -> str:
    return input_digest({"namespace": "vidar.dashboard.account.v1", "server": server, "login": login})


def build_test_dashboard_dataset(
    db: sqlite3.Connection,
    *,
    source_label: str,
) -> Mapping[str, Any]:
    """Project persisted supervisor rows into a deterministic synthetic feed.

    The caller owns the SQLite connection.  This function performs SELECTs
    only, emits no human annotations and intentionally accepts only a clearly
    labelled synthetic source.
    """

    if _SYNTHETIC_LABEL.fullmatch(source_label) is None:
        raise ValueError("SYNTHETIC_SOURCE_LABEL_REQUIRED")
    missing = sorted(_REQUIRED_TABLES - _available_tables(db))
    if missing:
        raise ValueError("SUPERVISOR_DASHBOARD_TABLES_MISSING:" + ",".join(missing))

    cycles: list[dict[str, Any]] = []
    account_refs: dict[tuple[str, str], str] = {}
    for row in db.execute(
        """SELECT cycle_id,account_server,account_login,observed_at_utc,
                  input_digest,reconciliation_quality,coverage_json
           FROM supervisor_cycles
           ORDER BY observed_at_utc,cycle_id"""
    ):
        server, login = str(row[1]), str(row[2])
        account_ref = account_refs.setdefault((server, login), _account_ref(server, login))
        cycles.append(
            {
                "data_class": "OBSERVED",
                "record_kind": "reconciliation_cycle",
                "dataset_kind": SYNTHETIC_DATASET_KIND,
                "synthetic": True,
                "source_label": source_label,
                "cycle_id": str(row[0]),
                "account_ref": account_ref,
                "observed_at_utc": str(row[3]),
                "reconciliation_quality": str(row[5]),
                "coverage": json.loads(str(row[6])),
                "provenance": str(row[4]),
            }
        )

    metrics: list[dict[str, Any]] = []
    for row in db.execute(
        """SELECT c.cycle_id,c.account_server,c.account_login,c.observed_at_utc,
                  m.scope_key,m.metric_name,m.status,m.unit,m.algorithm_version,
                  m.input_digest,m.reason_codes_json,m.value_json
           FROM supervisor_metrics AS m
           JOIN supervisor_cycles AS c ON c.cycle_id=m.cycle_id
           ORDER BY c.observed_at_utc,c.cycle_id,m.scope_key,m.metric_name"""
    ):
        server, login = str(row[1]), str(row[2])
        account_ref = account_refs.setdefault((server, login), _account_ref(server, login))
        metrics.append(
            {
                "data_class": "CALCULATED",
                "record_kind": "deterministic_metric",
                "dataset_kind": SYNTHETIC_DATASET_KIND,
                "synthetic": True,
                "source_label": source_label,
                "cycle_id": str(row[0]),
                "account_ref": account_ref,
                "observed_at_utc": str(row[3]),
                "scope_key": str(row[4]),
                "metric_name": str(row[5]),
                "status": str(row[6]),
                "unit": str(row[7]),
                "algorithm_version": str(row[8]),
                "provenance": str(row[9]),
                "reason_codes": json.loads(str(row[10])),
                "value": None if row[11] is None else json.loads(str(row[11])),
            }
        )

    quality_counts = dict(sorted(Counter(row["status"] for row in metrics).items()))
    payload = {
        "schema_version": DASHBOARD_SCHEMA_VERSION,
        "dataset_kind": SYNTHETIC_DATASET_KIND,
        "synthetic": True,
        "source_label": source_label,
        "account_refs": sorted(account_refs.values()),
        "cycles": cycles,
        "metrics": metrics,
        "quality_counts": quality_counts,
        "human_observations_included": False,
        "agent_interpretations_included": False,
    }
    return {
        **payload,
        "dataset_digest": input_digest(payload),
        "canonical_size_bytes": len(canonical_json(payload)),
    }
