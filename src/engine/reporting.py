"""Proyecciones puras para Obsidian y datasets de dashboard."""

from __future__ import annotations

from typing import Any, Iterable, Mapping

from .contracts import DataClass, MetricResult, canonical_json, input_digest


def _frontmatter(values: Mapping[str, Any]) -> str:
    lines = ["---"]
    for key in sorted(values):
        value = canonical_json(values[key]).decode("utf-8")
        lines.append(f"{key}: {value}")
    lines.extend(["---", ""])
    return "\n".join(lines)


def render_human_journal_template(*, trade_id: str, auto_note_relative: str, idea_id: str = "") -> str:
    if not trade_id or not auto_note_relative:
        raise ValueError("JOURNAL_LINK_REQUIRED")
    header = _frontmatter(
        {
            "schema_version": "trading-journal-human.v2",
            "trade_id": trade_id,
            "auto_note": auto_note_relative,
            "idea_id": idea_id or None,
            "data_class": DataClass.SUBJECTIVE.value,
        }
    )
    return header + f"""# {trade_id} · diario humano

![[{auto_note_relative}]]

## Decisión y plan
- Tesis:
- Invalidación:
- Objetivo:
- Riesgo máximo aceptado antes de la entrada:
- Relación riesgo/beneficio prevista:

## Riesgo y gestión
> Los hechos observados y los cálculos canónicos están en la proyección automática enlazada.

- Riesgo inicial revisado (importe, porcentaje y base de capital):
- Riesgo vigente revisado (importe, porcentaje e instante):
- Cambios de SL/TP:
  - Instante:
  - SL/TP anterior:
  - SL/TP nuevo:
  - Motivo:
  - Riesgo resultante:
- Discrepancias o datos `UNKNOWN` de la proyección automática:

## Disciplina
- Reglas respetadas:
- Desviaciones:

## Errores y aciertos
- Errores:
- Decisiones acertadas:
- Lección:

## Estado personal
- Emoción antes:
- Emoción durante:
- Emoción después:
- Cansancio (0-10):
- Calidad del descanso:
- Observaciones físicas o cognitivas:

## Cierre
- Resultado técnico de la operación:
- Evaluación de la ejecución (independiente del PnL):
- Qué repetir / qué evitar:
"""


def render_automatic_note(
    *, trade_id: str, observed: Iterable[Mapping[str, Any]], calculated: Mapping[str, MetricResult]
) -> str:
    observed_rows = [dict(row) for row in observed]
    calculated_rows = {name: result.as_dict() for name, result in sorted(calculated.items())}
    payload = {"trade_id": trade_id, "observed": observed_rows, "calculated": calculated_rows}
    header = _frontmatter(
        {
            "schema_version": "trading-journal-auto.v1",
            "trade_id": trade_id,
            "data_class": "OBSERVED+CALCULATED",
            "input_digest": input_digest(payload),
            "replaceable_projection": True,
        }
    )
    observed_json = canonical_json(observed_rows).decode("utf-8")
    calculated_json = canonical_json(calculated_rows).decode("utf-8")
    return header + f"""# {trade_id} · proyección automática

## Datos observados
```json
{observed_json}
```

## Cálculos deterministas
```json
{calculated_json}
```

Esta nota no contiene disciplina, emociones ni interpretación humana.
"""


def dashboard_rows(
    *,
    observed: Iterable[Mapping[str, Any]],
    calculated: Mapping[str, MetricResult],
    subjective: Mapping[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for index, item in enumerate(observed):
        rows.append(
            {
                "data_class": DataClass.OBSERVED.value,
                "key": str(item.get("key", index)),
                "value": dict(item),
                "provenance": item.get("source_hash"),
            }
        )
    for name, result in sorted(calculated.items()):
        rows.append(
            {
                "data_class": DataClass.CALCULATED.value,
                "key": name,
                "value": result.as_dict(),
                "provenance": result.input_digest,
            }
        )
    for name, value in sorted(subjective.items()):
        rows.append(
            {
                "data_class": DataClass.SUBJECTIVE.value,
                "key": name,
                "value": value,
                "provenance": "human_annotation",
            }
        )
    return rows
