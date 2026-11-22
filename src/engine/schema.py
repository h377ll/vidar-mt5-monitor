"""Esquema SQLite I1; el módulo no abre conexiones ni rutas por sí mismo."""

from __future__ import annotations

import sqlite3


SCHEMA_VERSION = 1

SCHEMA_SQL = """
PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS engine_schema (
    version INTEGER PRIMARY KEY,
    applied_at_utc TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS reconciliation_runs (
    run_id TEXT PRIMARY KEY,
    account_server TEXT NOT NULL,
    account_login TEXT NOT NULL,
    started_at_utc TEXT NOT NULL,
    finished_at_utc TEXT,
    quality TEXT NOT NULL CHECK (quality IN ('KNOWN','ESTIMATED','UNKNOWN','UNVERIFIABLE')),
    input_digest TEXT NOT NULL,
    coverage_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS broker_orders (
    account_server TEXT NOT NULL,
    account_login TEXT NOT NULL,
    order_ticket TEXT NOT NULL,
    source_hash TEXT NOT NULL,
    observed_at_utc TEXT NOT NULL,
    reconciliation_run_id TEXT NOT NULL REFERENCES reconciliation_runs(run_id),
    source_json TEXT NOT NULL,
    PRIMARY KEY (account_server, account_login, order_ticket, source_hash)
);
CREATE TABLE IF NOT EXISTS broker_deals (
    account_server TEXT NOT NULL,
    account_login TEXT NOT NULL,
    deal_ticket TEXT NOT NULL,
    position_identifier TEXT,
    source_hash TEXT NOT NULL,
    observed_at_utc TEXT NOT NULL,
    reconciliation_run_id TEXT NOT NULL REFERENCES reconciliation_runs(run_id),
    source_json TEXT NOT NULL,
    PRIMARY KEY (account_server, account_login, deal_ticket, source_hash)
);
CREATE TABLE IF NOT EXISTS position_versions (
    account_server TEXT NOT NULL,
    account_login TEXT NOT NULL,
    position_identifier TEXT NOT NULL,
    source_hash TEXT NOT NULL,
    observed_at_utc TEXT NOT NULL,
    reconciliation_run_id TEXT NOT NULL REFERENCES reconciliation_runs(run_id),
    source_json TEXT NOT NULL,
    PRIMARY KEY (account_server, account_login, position_identifier, source_hash)
);
CREATE TABLE IF NOT EXISTS observed_events (
    event_id TEXT PRIMARY KEY,
    account_server TEXT NOT NULL,
    account_login TEXT NOT NULL,
    event_type TEXT NOT NULL,
    observed_at_utc TEXT NOT NULL,
    native_id TEXT NOT NULL,
    source_hash TEXT NOT NULL,
    reconciliation_run_id TEXT REFERENCES reconciliation_runs(run_id),
    source_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS metric_runs (
    metric_run_id TEXT PRIMARY KEY,
    account_server TEXT NOT NULL,
    account_login TEXT NOT NULL,
    started_at_utc TEXT NOT NULL,
    finished_at_utc TEXT,
    algorithm_bundle_version TEXT NOT NULL,
    input_digest TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS metric_values (
    metric_run_id TEXT NOT NULL REFERENCES metric_runs(metric_run_id),
    metric_name TEXT NOT NULL,
    scope_key TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('KNOWN','ESTIMATED','UNKNOWN','UNVERIFIABLE')),
    unit TEXT NOT NULL,
    algorithm_version TEXT NOT NULL,
    input_digest TEXT NOT NULL,
    reason_codes_json TEXT NOT NULL,
    value_json TEXT,
    PRIMARY KEY (metric_run_id, metric_name, scope_key)
);
CREATE TABLE IF NOT EXISTS data_quality_issues (
    issue_id TEXT PRIMARY KEY,
    account_server TEXT NOT NULL,
    account_login TEXT NOT NULL,
    observed_at_utc TEXT NOT NULL,
    code TEXT NOT NULL,
    severity TEXT NOT NULL,
    context_json TEXT NOT NULL,
    reconciliation_run_id TEXT REFERENCES reconciliation_runs(run_id)
);
CREATE TABLE IF NOT EXISTS journal_links (
    trade_id TEXT PRIMARY KEY,
    account_server TEXT NOT NULL,
    account_login TEXT NOT NULL,
    auto_note_relative TEXT NOT NULL,
    human_note_relative TEXT NOT NULL,
    idea_id TEXT,
    updated_at_utc TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS report_generations (
    generation_id TEXT PRIMARY KEY,
    created_at_utc TEXT NOT NULL,
    input_digest TEXT NOT NULL,
    output_digest TEXT NOT NULL,
    observed_rows INTEGER NOT NULL,
    calculated_rows INTEGER NOT NULL,
    subjective_rows INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS broker_deals_position_idx
    ON broker_deals(account_server, account_login, position_identifier);
CREATE INDEX IF NOT EXISTS events_account_time_idx
    ON observed_events(account_server, account_login, observed_at_utc);
CREATE INDEX IF NOT EXISTS metrics_name_scope_idx
    ON metric_values(metric_name, scope_key);
"""


def initialize_schema(connection: sqlite3.Connection, applied_at_utc: str) -> int:
    """Apply the additive I1 schema to a caller-owned database connection."""
    connection.executescript(SCHEMA_SQL)
    rows = [row[0] for row in connection.execute("SELECT version FROM engine_schema ORDER BY version")]
    if rows and rows != [SCHEMA_VERSION]:
        raise RuntimeError("UNSUPPORTED_ENGINE_SCHEMA")
    connection.execute(
        "INSERT OR IGNORE INTO engine_schema(version, applied_at_utc) VALUES (?, ?)",
        (SCHEMA_VERSION, applied_at_utc),
    )
    connection.commit()
    return SCHEMA_VERSION
