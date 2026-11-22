"""Backfill offline de un lifecycle cerrado desde un informe HTML nativo MT5.

No conecta con MT5, no contiene primitivas de trading y reutiliza la
reconstrucción, las métricas, SQLite y la proyección Obsidian existentes.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
import hashlib
from html.parser import HTMLParser
import json
import os
from pathlib import Path, PurePosixPath
import re
import tempfile
from typing import Any, Callable

from engine.contracts import MetricResult, Quality, input_digest, utc_timestamp
from engine.metrics import (
    exposure_concentration,
    planned_r,
    realized_r,
    reconciliation_differences,
)
from engine.reporting import render_automatic_note
from engine.supervisor import TEST_VAULT_MARKER
from phase_b import journal


LIFECYCLE_MODE = "historical_backfill"
AUTO_LINKS_BEGIN = "<!-- vidar-mt5-auto-links:begin -->"
AUTO_LINKS_END = "<!-- vidar-mt5-auto-links:end -->"


class RowParser(HTMLParser):
    """Extrae las celdas del informe sin depender de estilos o scripts."""

    def __init__(self) -> None:
        super().__init__()
        self.in_cell = False
        self.cell: list[str] = []
        self.row: list[str] = []
        self.rows: list[list[str]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        del attrs
        if tag in {"td", "th"}:
            self.in_cell = True
            self.cell = []

    def handle_data(self, data: str) -> None:
        if self.in_cell:
            self.cell.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag in {"td", "th"} and self.in_cell:
            text = " ".join("".join(self.cell).replace("\xa0", " ").split())
            self.row.append(text)
            self.in_cell = False
        elif tag == "tr":
            if self.row:
                self.rows.append(self.row)
            self.row = []


def _only(rows: list[list[str]], predicate: Callable[[list[str]], bool], code: str) -> list[str]:
    selected = [row for row in rows if predicate(row)]
    if len(selected) != 1:
        raise ValueError(f"{code}:expected=1:actual={len(selected)}")
    return selected[0]


def _number(value: str) -> Decimal:
    return Decimal(value.replace(" ", ""))


def _optional_number(value: str) -> str | None:
    return None if not value.strip() else str(_number(value))


def _canonical_safe(value: Any) -> Any:
    if isinstance(value, float):
        return str(Decimal(str(value)))
    if isinstance(value, dict):
        return {key: _canonical_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_canonical_safe(item) for item in value]
    return value


def _known_metric(*, name: str, value: Any, unit: str, inputs: dict[str, Any]) -> MetricResult:
    return MetricResult(
        status=Quality.KNOWN,
        value=value,
        unit=unit,
        algorithm_version=name + ".v1",
        input_digest=input_digest(inputs),
    )


def _unknown_metric(*, name: str, unit: str, reason: str, inputs: dict[str, Any]) -> MetricResult:
    return MetricResult(
        status=Quality.UNKNOWN,
        value=None,
        unit=unit,
        algorithm_version=name + ".v1",
        input_digest=input_digest(inputs),
        reason_codes=(reason,),
    )


def _safe_relative(value: str, code: str) -> PurePosixPath:
    path = PurePosixPath(value)
    if path.is_absolute() or not path.parts or any(part in {"", ".", ".."} for part in path.parts):
        raise ValueError(code)
    return path


def _decode_report(raw: bytes) -> str:
    if raw.startswith((b"\xff\xfe", b"\xfe\xff")):
        return raw.decode("utf-16")
    return raw.decode("utf-16le")


def _close_reason(comment: str | None) -> tuple[str, str]:
    if comment and re.fullmatch(r"\[tp [^\]]+\]", comment, flags=re.IGNORECASE):
        return "TAKE_PROFIT_REPORTED", "exit order/deal native comment " + comment
    if comment and re.fullmatch(r"\[sl [^\]]+\]", comment, flags=re.IGNORECASE):
        return "STOP_LOSS_REPORTED", "exit order/deal native comment " + comment
    return "UNKNOWN", "native close reason not accredited"


def parse_report(
    report: Path,
    *,
    expected_sha256: str,
    position_id: str,
    human_note_relative: str,
) -> dict[str, Any]:
    """Selecciona y concilia exactamente un lifecycle histórico cerrado."""

    if not position_id.isdigit():
        raise ValueError("POSITION_ID_REQUIRED")
    expected_sha256 = expected_sha256.lower()
    if not re.fullmatch(r"[0-9a-f]{64}", expected_sha256):
        raise ValueError("EXPECTED_REPORT_SHA256_REQUIRED")
    human_link = _safe_relative(human_note_relative, "HUMAN_NOTE_RELATIVE_INVALID").with_suffix("")

    raw = report.read_bytes()
    report_sha256 = hashlib.sha256(raw).hexdigest()
    if report_sha256 != expected_sha256:
        raise ValueError("REPORT_SHA256_MISMATCH")
    parser = RowParser()
    parser.feed(_decode_report(raw))
    rows = parser.rows

    account_row = _only(rows, lambda row: len(row) >= 2 and row[0] == "Cuenta de trading:", "ACCOUNT_ROW")
    login, account_tail = account_row[1].split("(", 1)
    account_parts = [part.strip() for part in account_tail.rstrip(")").split(",")]
    if len(account_parts) != 4:
        raise ValueError("ACCOUNT_SCHEMA")
    currency, server, account_type, margin_mode = account_parts
    login = login.strip()
    if not login.isdigit() or not server:
        raise ValueError("ACCOUNT_IDENTITY_MISSING")

    report_date_row = _only(rows, lambda row: len(row) >= 2 and row[0] == "Fecha:", "REPORT_DATE_ROW")
    report_generated_at_source = report_date_row[1]
    position_row = _only(
        rows,
        lambda row: len(row) == 14 and row[1] == position_id and row[2] and row[3] in {"buy", "sell"},
        "POSITION_ROW",
    )
    (
        opened_at,
        _,
        symbol,
        direction,
        _,
        volume_text,
        entry_price_text,
        sl_text,
        tp_text,
        closed_at,
        exit_price_text,
        position_commission_text,
        position_swap_text,
        position_profit_text,
    ) = position_row
    volume = _number(volume_text)
    entry_price = _number(entry_price_text)
    exit_price = _number(exit_price_text)
    opposite = "sell" if direction == "buy" else "buy"

    entry_order_row = _only(
        rows,
        lambda row: len(row) == 11 and row[1] == position_id and row[2] == symbol and row[3] == direction,
        "ENTRY_ORDER_ROW",
    )
    exit_order_row = _only(
        rows,
        lambda row: len(row) == 11
        and row[2] == symbol
        and row[3] == opposite
        and row[8] == closed_at
        and row[9] == "filled"
        and _number(row[4].split("/")[0].strip()) == volume,
        "EXIT_ORDER_ROW",
    )
    exit_order_id = exit_order_row[1]
    entry_deal_row = _only(
        rows,
        lambda row: len(row) == 15
        and row[2] == symbol
        and row[3] == direction
        and row[4] == "in"
        and row[0] == opened_at
        and row[7] == position_id,
        "ENTRY_DEAL_ROW",
    )
    exit_deal_row = _only(
        rows,
        lambda row: len(row) == 15
        and row[2] == symbol
        and row[3] == opposite
        and row[4] == "out"
        and row[0] == closed_at
        and row[7] == exit_order_id,
        "EXIT_DEAL_ROW",
    )

    def order_from(row: list[str], role: str) -> dict[str, Any]:
        requested, filled = [_number(part.strip()) for part in row[4].split("/")]
        return {
            "order_id": row[1],
            "position_id": position_id,
            "symbol": row[2],
            "action": row[3],
            "requested_volume": str(requested),
            "filled_volume": str(filled),
            "price": row[5] or None,
            "stop_loss": row[6] or None,
            "take_profit": row[7] or None,
            "open_time_source": row[0],
            "state_time_source": row[8],
            "state": row[9],
            "role": role,
            "native_comment": row[10] or None,
            "position_link": "IDENTIFIER_EQUALITY" if row[1] == position_id else "DETERMINISTIC_LIFECYCLE_MATCH",
            "lifecycle_mode": LIFECYCLE_MODE,
        }

    def deal_from(row: list[str]) -> dict[str, Any]:
        return {
            "deal_id": row[1],
            "position_id": position_id,
            "order_id": row[7],
            "symbol": row[2],
            "action": row[3],
            "entry": row[4],
            "volume": str(_number(row[5])),
            "price_open": str(_number(row[6])),
            "open_time": row[0],
            "commission": _optional_number(row[9]),
            "fees": _optional_number(row[10]),
            "swap": _optional_number(row[11]),
            "profit": _optional_number(row[12]),
            "balance_after": _optional_number(row[13]),
            "native_comment": row[14] or None,
            "position_link": "DETERMINISTIC_ORDER_TIME_SYMBOL_VOLUME_MATCH",
            "lifecycle_mode": LIFECYCLE_MODE,
        }

    orders = [order_from(entry_order_row, "entry"), order_from(exit_order_row, "exit")]
    deals = [deal_from(entry_deal_row), deal_from(exit_deal_row)]
    summary = {
        "position_id": position_id,
        "symbol": symbol,
        "direction": direction,
        "open_volume": str(volume),
        "close_volume": str(volume),
        "open_price": str(entry_price),
        "close_price": str(exit_price),
        "stop_loss": _optional_number(sl_text),
        "take_profit": _optional_number(tp_text),
        "open_time_source": opened_at,
        "close_time_source": closed_at,
        "commission": _optional_number(position_commission_text),
        "swap": _optional_number(position_swap_text),
        "profit": _optional_number(position_profit_text),
    }
    position = journal.reconstruct(position_id, deals, summary)
    if position["status"] != "closed" or position["partial_closes"]:
        raise ValueError("LIFECYCLE_RECONSTRUCTION_FAILED")
    exit_comment = exit_deal_row[14] or exit_order_row[10] or None
    close_reason, close_basis = _close_reason(exit_comment)
    position.update(
        {
            "reported_position_summary": summary,
            "orders": orders,
            "sl_tp_change_history": None,
            "sl_tp_change_history_status": "UNKNOWN",
            "close_reason": close_reason,
            "close_reason_basis": close_basis,
            "linkage_rule": "unique order/deal match by order id, source time, symbol, side and volume",
            "source_report_sha256": "sha256:" + report_sha256,
            "lifecycle_mode": LIFECYCLE_MODE,
        }
    )

    start = datetime.strptime(opened_at, "%Y.%m.%d %H:%M:%S")
    finish = datetime.strptime(closed_at, "%Y.%m.%d %H:%M:%S")
    duration_seconds = Decimal(str(int((finish - start).total_seconds())))
    price_change = exit_price - entry_price if direction == "buy" else entry_price - exit_price
    pnl_inputs = {
        "deal_ids": [deal["deal_id"] for deal in deals],
        "components": _canonical_safe(position["reported_components"]),
    }
    cost_values = [deal[field] for deal in deals for field in ("commission", "fees", "swap")]
    costs_complete = all(value is not None for value in cost_values)
    if position["net_pnl"] is None:
        pnl_metric = _unknown_metric(
            name="pnl.reported-components",
            unit=currency,
            reason="REPORTED_PNL_COMPONENT_MISSING",
            inputs=pnl_inputs,
        )
    else:
        pnl_metric = _known_metric(
            name="pnl.reported-components",
            value=Decimal(str(position["net_pnl"])),
            unit=currency,
            inputs=pnl_inputs,
        )
    costs_inputs = {
        "deal_ids": [deal["deal_id"] for deal in deals],
        "commission_fee_swap": [
            {field: deal[field] for field in ("commission", "fees", "swap")} for deal in deals
        ],
    }
    if costs_complete:
        costs_metric = _known_metric(
            name="costs.reported-total",
            value=sum((Decimal(value) for value in cost_values if value is not None), Decimal("0")),
            unit=currency,
            inputs=costs_inputs,
        )
    else:
        costs_metric = _unknown_metric(
            name="costs.reported-total",
            unit=currency,
            reason="REPORTED_COST_FIELDS_MISSING",
            inputs=costs_inputs,
        )
    risk_inputs = {
        "position_id": position_id,
        "entry_price": str(entry_price),
        "reported_summary_stop": summary["stop_loss"],
        "entry_state_captured": False,
    }
    calculated = {
        "pnl.reported-components": pnl_metric,
        "costs.reported-total": costs_metric,
        "duration.source-clock": _known_metric(
            name="duration.source-clock",
            value=duration_seconds,
            unit="seconds",
            inputs={"opened_at_source": opened_at, "closed_at_source": closed_at},
        ),
        "price-change.favourable": _known_metric(
            name="price-change.favourable",
            value=price_change,
            unit="quote_currency_per_base_unit",
            inputs={"direction": direction, "entry_price": str(entry_price), "exit_price": str(exit_price)},
        ),
        "partial-close-count": _known_metric(
            name="partial-close-count",
            value=0,
            unit="count",
            inputs={"entry_volume": str(volume), "exit_deals": [deals[1]["volume"]]},
        ),
        "risk.initial": _unknown_metric(
            name="risk.initial",
            unit=currency,
            reason="INITIAL_PROTECTION_NOT_OBSERVED_AT_ENTRY",
            inputs=risk_inputs,
        ),
        "risk.current": _unknown_metric(
            name="risk.current",
            unit=currency,
            reason="HISTORICAL_CLOSED_NO_CURRENT_POSITION",
            inputs={"position_id": position_id, "lifecycle_mode": LIFECYCLE_MODE},
        ),
        "reward.initial-target": _unknown_metric(
            name="reward.initial-target",
            unit=currency,
            reason="INITIAL_TARGET_NOT_OBSERVED_AT_ENTRY",
            inputs={"position_id": position_id, "reported_summary_target": summary["take_profit"]},
        ),
        "r.planned": planned_r(reward_to_target=None, initial_risk=None),
        "r.realized": realized_r(
            net_pnl=None if position["net_pnl"] is None else str(position["net_pnl"]),
            initial_risk=None,
            costs_complete=costs_complete,
            costs_reason=None if costs_complete else "REPORTED_COST_FIELDS_MISSING",
        ),
        "slippage.entry": _unknown_metric(
            name="slippage.entry",
            unit=currency,
            reason="REFERENCE_PRICE_NOT_CAPTURED",
            inputs={"fill_price": str(entry_price), "reference_price": None},
        ),
        "protection.modifications": _unknown_metric(
            name="protection.modifications",
            unit="count",
            reason="SL_TP_CHANGE_HISTORY_NOT_AVAILABLE",
            inputs={"position_id": position_id, "source_kind": "MT5_NATIVE_HTML_HISTORY_REPORT"},
        ),
        "exposure.concentration": exposure_concentration([{"bucket": symbol, "gross_risk": None}]),
        "reconciliation.differences": reconciliation_differences(
            coverage={
                "offline_report": True,
                "selected_lifecycle_complete": True,
                "server_connected": False,
                "positions_complete": False,
                "orders_complete": False,
                "deals_complete": False,
            },
            issue_codes=[],
            observed_positions=1,
            observed_orders=2,
            observed_deals=2,
        ),
    }

    row_hashes = {
        "position": input_digest(position_row),
        "entry_order": input_digest(entry_order_row),
        "exit_order": input_digest(exit_order_row),
        "entry_deal": input_digest(entry_deal_row),
        "exit_deal": input_digest(exit_deal_row),
    }
    observed = [
        {
            "key": "source",
            "source_type": "MT5_NATIVE_HTML_HISTORY_REPORT",
            "lifecycle_mode": LIFECYCLE_MODE,
            "report_sha256": "sha256:" + report_sha256,
            "report_generated_at_source": report_generated_at_source,
            "source_timezone": "UNKNOWN",
            "account_key": [server, login],
            "account_currency": currency,
            "account_type": account_type,
            "margin_mode": margin_mode,
            "selected_position_id": position_id,
            "row_hashes": row_hashes,
            "source_hash": "sha256:" + report_sha256,
        },
        {"key": "position", **_canonical_safe(position), "source_hash": row_hashes["position"]},
    ]
    trade_id = f"MT5-{server}-{login}-{position_id}"
    note = render_automatic_note(trade_id=trade_id, observed=observed, calculated=calculated)
    note += f"""
## Enlace con el diario humano

![[{human_link.as_posix()}]]

## Procedencia y límites

- Lifecycle: `{LIFECYCLE_MODE}`; la operación se registra cerrada y nunca como posición abierta observada.
- Fuente: informe histórico HTML nativo de MT5 leído offline; SHA-256 `{report_sha256}`.
- Se conservan la posición `{position_id}`, las órdenes `{orders[0]['order_id']}` y `{orders[1]['order_id']}`, y los deals `{deals[0]['deal_id']}` y `{deals[1]['deal_id']}`.
- La posición, las dos órdenes y los dos deals se conciliaron por identificadores nativos, hora fuente, símbolo, lado y volumen.
- Los niveles SL/TP pertenecen al resumen histórico. No acreditan el estado inicial ni una secuencia de modificaciones.
- La zona horaria del servidor, el riesgo monetario, la exposición simultánea, MAE/MFE y el precio de referencia para slippage permanecen `UNKNOWN`.

## Estado documental

- **Procedimiento aprobado:** separar observado, calculado y subjetivo; conservar `UNKNOWN` ante evidencia insuficiente.
- **Solución experimental:** backfill HTML offline aditivo con SQLite y proyección automática de ensayo.
- **Hipótesis pendiente:** equivalencia futura con una captura viva de cobertura completa y cambios SL/TP.
- **Decisión que requiere autorización:** cualquier lectura viva directa, proceso persistente, G4b, trading o escritura en el vault definitivo.
- **Enmienda propuesta:** adoptar esta CLI como recuperación one-shot mientras el origen MCP mantenga trading habilitado.
"""
    return {
        "account": {
            "server": server,
            "login": login,
            "broker": "Funding Traders Group Ltd.",
            "currency": currency,
            "type": account_type,
            "margin_mode": margin_mode,
        },
        "source_id": f"offline-html:sha256:{report_sha256}:position:{position_id}",
        "report_sha256": report_sha256,
        "report_generated_at_source": report_generated_at_source,
        "trade_id": trade_id,
        "position": _canonical_safe(position),
        "orders": orders,
        "deals": deals,
        "calculated": calculated,
        "metrics_json": journal.dumps({name: result.as_dict() for name, result in sorted(calculated.items())}),
        "note": note,
        "note_sha256": hashlib.sha256(note.encode("utf-8")).hexdigest(),
        "opened_date": opened_at[:10].replace(".", "-"),
        "symbol": symbol,
    }


def _atomic_write(path: Path, text: str) -> bool:
    encoded = text.encode("utf-8")
    if path.exists() and path.read_bytes() == encoded:
        return False
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    handle, temporary_name = tempfile.mkstemp(prefix="." + path.name + ".", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(handle, "wb") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(temporary, 0o600)
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()
    return True


def _update_human_backlink(human_path: Path, auto_link: str) -> bool:
    original = human_path.read_text(encoding="utf-8")
    pattern = re.compile(re.escape(AUTO_LINKS_BEGIN) + r".*?" + re.escape(AUTO_LINKS_END), re.DOTALL)
    existing_match = pattern.search(original)
    links = set()
    if existing_match:
        links.update(re.findall(r"!\[\[([^\]]+)\]\]", existing_match.group(0)))
    links.add(auto_link)
    block = "\n".join(
        [AUTO_LINKS_BEGIN, "## Proyecciones automáticas enlazadas", ""]
        + [f"![[{link}]]" for link in sorted(links)]
        + [AUTO_LINKS_END]
    )
    if existing_match:
        updated = original[: existing_match.start()] + block + original[existing_match.end() :]
    else:
        updated = original.rstrip() + "\n\n" + block + "\n"
    return _atomic_write(human_path, updated)


def _counts(db, account_key: str) -> dict[str, int]:
    return {
        "records": db.execute("SELECT COUNT(*) FROM records WHERE account_key=?", (account_key,)).fetchone()[0],
        "positions": db.execute(
            "SELECT COUNT(*) FROM records WHERE account_key=? AND kind='position'", (account_key,)
        ).fetchone()[0],
        "orders": db.execute(
            "SELECT COUNT(*) FROM records WHERE account_key=? AND kind='order'", (account_key,)
        ).fetchone()[0],
        "deals": db.execute(
            "SELECT COUNT(*) FROM records WHERE account_key=? AND kind='deal'", (account_key,)
        ).fetchone()[0],
        "revisions": db.execute("SELECT COUNT(*) FROM revisions WHERE account_key=?", (account_key,)).fetchone()[0],
        "historical_backfills": db.execute(
            "SELECT COUNT(*) FROM historical_backfills WHERE account_key=?", (account_key,)
        ).fetchone()[0],
    }


def store_backfill(
    db_path: Path,
    parsed: dict[str, Any],
    *,
    observed_at_utc: str,
    note_relative: str,
) -> dict[str, Any]:
    observed_at_utc = utc_timestamp(observed_at_utc)
    db_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    db = journal.open_db(db_path)
    db.execute(
        """CREATE TABLE IF NOT EXISTS historical_backfills (
           account_key TEXT NOT NULL,
           position_id TEXT NOT NULL,
           lifecycle_mode TEXT NOT NULL,
           source_id TEXT NOT NULL,
           source_hash TEXT NOT NULL,
           lifecycle_hash TEXT NOT NULL,
           metrics_json TEXT NOT NULL,
           note_relative TEXT NOT NULL,
           note_hash TEXT NOT NULL,
           observed_at_utc TEXT NOT NULL,
           PRIMARY KEY(account_key,position_id))"""
    )
    account = parsed["account"]
    account_key = journal.account_key(account)
    lifecycle_hash = input_digest(
        {"position": parsed["position"], "orders": parsed["orders"], "deals": parsed["deals"]}
    )
    backfill_row = (
        account_key,
        parsed["position"]["position_id"],
        LIFECYCLE_MODE,
        parsed["source_id"],
        "sha256:" + parsed["report_sha256"],
        lifecycle_hash,
        parsed["metrics_json"],
        note_relative,
        "sha256:" + parsed["note_sha256"],
        observed_at_utc,
    )
    previous = db.execute(
        "SELECT * FROM historical_backfills WHERE account_key=? AND position_id=?",
        (account_key, parsed["position"]["position_id"]),
    ).fetchone()
    if previous is not None and tuple(previous) != backfill_row:
        db.close()
        raise ValueError("HISTORICAL_BACKFILL_CONFLICT")
    changed = 0
    with db:
        account_data = {
            **account,
            "login": str(account["login"]),
            "source_kind": "offline_mt5_html_report",
        }
        db.execute(
            "INSERT INTO accounts VALUES(?,?) ON CONFLICT(key) DO NOTHING",
            (account_key, journal.dumps(account_data)),
        )
        db.execute(
            "INSERT INTO sources VALUES(?,?,?,?,?) ON CONFLICT(source) DO NOTHING",
            (
                parsed["source_id"],
                account_key,
                "historical_backfill_selected_lifecycle_complete",
                observed_at_utc,
                journal.dumps(
                    {
                        "report_sha256": parsed["report_sha256"],
                        "selected_position_id": parsed["position"]["position_id"],
                        "lifecycle_mode": LIFECYCLE_MODE,
                        "live_connection": False,
                    }
                ),
            ),
        )
        for kind, rows, identity in (
            ("deal", parsed["deals"], "deal_id"),
            ("order", parsed["orders"], "order_id"),
        ):
            for row in rows:
                changed += journal.put_record(
                    db, account_key, kind, str(row[identity]), row, observed_at_utc, True
                )
        changed += journal.put_record(
            db,
            account_key,
            "position",
            str(parsed["position"]["position_id"]),
            parsed["position"],
            observed_at_utc,
            True,
        )
        if previous is None:
            db.execute(
                "INSERT INTO historical_backfills VALUES(?,?,?,?,?,?,?,?,?,?)",
                backfill_row,
            )
    quick_check = db.execute("PRAGMA quick_check").fetchone()[0]
    counts = _counts(db, account_key)
    historical_outbox = db.execute(
        """SELECT COUNT(*) FROM outbox
           JOIN events ON events.id=outbox.event_id
           WHERE events.account_key=? AND outbox.state='historical'""",
        (account_key,),
    ).fetchone()[0]
    pending_outbox = db.execute(
        """SELECT COUNT(*) FROM outbox
           JOIN events ON events.id=outbox.event_id
           WHERE events.account_key=? AND outbox.state!='historical'""",
        (account_key,),
    ).fetchone()[0]
    db.close()
    return {
        "changed_records": changed,
        "counts": counts,
        "sqlite_quick_check": quick_check,
        "historical_outbox_events": historical_outbox,
        "non_historical_outbox_events": pending_outbox,
    }


def run_offline_html_backfill(
    *,
    report: str | Path,
    expected_sha256: str,
    position_id: str,
    database: str | Path,
    vault: str | Path,
    human_note: str,
    auto_note_directory: str,
    observed_at_utc: str,
) -> dict[str, Any]:
    """Ejecuta una importación one-shot y comprueba persistencia y enlaces."""

    root = Path(vault).expanduser().resolve()
    marker = root / ".vidar-mt5-test-vault"
    if not root.is_dir() or not marker.is_file() or marker.read_text(encoding="utf-8") != TEST_VAULT_MARKER:
        raise ValueError("OBSIDIAN_TEST_VAULT_MARKER_REQUIRED")
    human_relative = _safe_relative(human_note, "HUMAN_NOTE_RELATIVE_INVALID")
    auto_directory = _safe_relative(auto_note_directory, "AUTO_NOTE_DIRECTORY_INVALID")
    human_path = root.joinpath(*human_relative.parts)
    if not human_path.is_file():
        raise ValueError("HUMAN_NOTE_REQUIRED")
    parsed = parse_report(
        Path(report).expanduser().resolve(),
        expected_sha256=expected_sha256,
        position_id=position_id,
        human_note_relative=human_relative.as_posix(),
    )
    auto_relative = auto_directory / f"{parsed['opened_date']}-{parsed['symbol']}-{position_id}.md"
    auto_path = root.joinpath(*auto_relative.parts)
    stored = store_backfill(
        Path(database).expanduser().resolve(),
        parsed,
        observed_at_utc=observed_at_utc,
        note_relative=auto_relative.as_posix(),
    )
    note_changed = _atomic_write(auto_path, parsed["note"])
    human_changed = _update_human_backlink(human_path, auto_relative.with_suffix("").as_posix())
    final_auto = auto_path.read_text(encoding="utf-8")
    final_human = human_path.read_text(encoding="utf-8")
    human_link = human_relative.with_suffix("").as_posix()
    auto_link = auto_relative.with_suffix("").as_posix()
    links_ok = f"![[{human_link}]]" in final_auto and f"![[{auto_link}]]" in final_human
    if not links_ok:
        raise ValueError("OBSIDIAN_BIDIRECTIONAL_LINK_FAILED")
    return {
        "schema_version": "vidar.mt5.offline-html-backfill.v1",
        "lifecycle_mode": LIFECYCLE_MODE,
        "report_sha256": parsed["report_sha256"],
        "trade_id": parsed["trade_id"],
        "position_id": parsed["position"]["position_id"],
        "order_ids": [row["order_id"] for row in parsed["orders"]],
        "deal_ids": [row["deal_id"] for row in parsed["deals"]],
        "note_path": str(auto_path),
        "note_sha256": hashlib.sha256(auto_path.read_bytes()).hexdigest(),
        "note_changed": note_changed,
        "human_note_path": str(human_path),
        "human_note_changed": human_changed,
        "bidirectional_links_ok": links_ok,
        "metric_statuses": {name: result.status.value for name, result in sorted(parsed["calculated"].items())},
        **stored,
    }
