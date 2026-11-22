"""One-shot import of a selected OpenClaw MT5 MCP observation.

The module has no MCP transport, credentials, scheduler or trading primitive.
It converts a previously captured, sanitized current-state fixture into one
supervisor cycle and projects notes only to a marked test vault.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Mapping

from engine.contracts import AccountKey
from engine.supervisor import export_test_notes, initialize_supervisor_schema, reconcile_cycle
from phase_b import journal
from provider.mt5_mcp_readonly import normalize_mcp_open_observation_fixture
from service.contracts import load_json, sha256_file


def run_offline_mcp_open_observation(
    *,
    fixture: str | os.PathLike[str],
    expected_sha256: str,
    expected_server: str,
    expected_login: str,
    database: str | os.PathLike[str],
    vault: str | os.PathLike[str],
) -> Mapping[str, Any]:
    """Persist one bounded live observation without contacting MT5 or MCP."""

    source_path = Path(fixture)
    if not source_path.is_file() or source_path.is_symlink():
        raise ValueError("MCP_OPEN_FIXTURE_FILE_REQUIRED")
    if sha256_file(source_path) != expected_sha256:
        raise ValueError("MCP_OPEN_FIXTURE_SHA256_MISMATCH")
    account = AccountKey(expected_server, expected_login)
    normalized = normalize_mcp_open_observation_fixture(
        load_json(source_path, maximum=8 * 1024 * 1024), expected_account=account
    )
    connection = {
        **normalized["coverage"],
        "identity_before": normalized["identity_before"],
        "identity_after": normalized["identity_after"],
        "read_started_at_utc": None,
        "read_finished_at_utc": None,
        "coverage": {
            "positions": {
                "complete": normalized["coverage"]["positions_complete"],
                "basis": normalized["coverage"]["basis"],
            },
            "orders": {
                "complete": normalized["coverage"]["orders_complete"],
                "basis": normalized["coverage"]["basis"],
            },
            "deals": {
                "complete": False,
                "basis": "NOT_REQUESTED_IN_OPEN_OBSERVATION",
            },
        },
        "connectivity_indicators": {
            "source_transport": normalized["source_transport"],
            "direct_endpoint_access": False,
            "automation_eligible": normalized["automation_eligible"],
            "automation_blockers": normalized["automation_blockers"],
            "source_hash": normalized["source_hash"],
        },
    }
    db = journal.open_db(database)
    try:
        initialize_supervisor_schema(db, normalized["captured_at_utc"])
        cycle = reconcile_cycle(
            db,
            account=account,
            observed_at_utc=normalized["captured_at_utc"],
            cycle_id=normalized["cycle_id"],
            connection=connection,
            account_state=normalized["account_state"],
            orders=normalized["orders"],
            deals=normalized["deals"],
            positions=normalized["positions"],
            symbol_specs=normalized["symbol_specs"],
        )
        notes = export_test_notes(
            db,
            cycle_id=cycle.cycle_id,
            destination=Path(vault),
            exported_at_utc=normalized["captured_at_utc"],
        )
        quick_check = str(db.execute("PRAGMA quick_check").fetchone()[0])
        if quick_check != "ok":
            raise RuntimeError("MCP_OPEN_SQLITE_QUICK_CHECK_FAILED")
        counts = {
            "cycles": int(db.execute("SELECT count(*) FROM supervisor_cycles").fetchone()[0]),
            "positions": int(db.execute("SELECT count(*) FROM supervisor_positions").fetchone()[0]),
            "position_versions": int(
                db.execute("SELECT count(*) FROM supervisor_position_versions").fetchone()[0]
            ),
            "events": int(db.execute("SELECT count(*) FROM supervisor_events").fetchone()[0]),
        }
    finally:
        db.close()
    return {
        "schema_version": "vidar.mt5.mcp-open-import-result.v1",
        "source_kind": "REAL_OBSERVED_SELECTED" if normalized["identity_after"]["account_type"] == "real" else "OBSERVED_SELECTED",
        "source_hash": normalized["source_hash"],
        "fixture_sha256": expected_sha256,
        "account_key": normalized["account_key"],
        "cycle": cycle.as_dict(),
        "notes": notes,
        "counts": counts,
        "quick_check": quick_check,
        "automation_eligible": normalized["automation_eligible"],
        "automation_blockers": normalized["automation_blockers"],
        "warnings": normalized["warnings"],
    }

