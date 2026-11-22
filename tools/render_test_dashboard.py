#!/usr/bin/env python3
"""Render the existing deterministic synthetic feed into a marked test vault.

This is a read-only projection of calculated metrics, not a second calculator
and not a live dashboard. No transport, credentials, scheduler or trading.
"""
from __future__ import annotations
import argparse
from collections import Counter
import hashlib
import json
import os
from pathlib import Path
import sys
sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from engine.contracts import input_digest


def render(data: dict) -> str:
    if data.get('schema_version') != 'vidar.dashboard.test.v1' or data.get('synthetic') is not True or data.get('dataset_kind') != 'SYNTHETIC_TEST':
        raise ValueError('SYNTHETIC_FEED_REQUIRED')
    payload = {k: v for k, v in data.items() if k not in {'dataset_digest', 'canonical_size_bytes'}}
    if input_digest(payload) != data.get('dataset_digest'):
        raise ValueError('DATASET_DIGEST_MISMATCH')
    rows = data['metrics']
    if any(r.get('synthetic') is not True or r.get('status') not in {'KNOWN','ESTIMATED','UNKNOWN','UNVERIFIABLE'} for r in rows):
        raise ValueError('METRIC_CLASS_INVALID')
    counts = dict(sorted(Counter(r['status'] for r in rows).items()))
    lines = ['---','schema_version: vidar.dashboard.markdown.test.v1','synthetic: true','execution_allowed: false','---',
        '# VIDARNET · Dashboard de ensayo','',
        '> DATOS INVENTADOS. No acredita cuentas, posiciones ni gestión en MT5. Actualización manual mediante script; no hay servicio activo.','',
        f"Ciclos: **{len(data['cycles'])}** · métricas: **{len(rows)}** · calidad: `{json.dumps(counts,sort_keys=True)}`.",'',
        'Cada fila conserva el cálculo del motor, su estado y procedencia. Un valor desconocido no es cero.',
        '', '| Fecha UTC | Ámbito | Métrica | Valor | Unidad | Calidad | Motivos |',
        '|---|---|---|---|---|---|---|']
    def cell(value):
        text = json.dumps(value, ensure_ascii=False,sort_keys=True) if isinstance(value,(dict,list)) else str(value)
        return text.replace('|','&#124;').replace('\n',' ')
    for r in rows:
        value = r['value'] if r['value'] is not None else 'UNKNOWN'
        lines.append('| '+' | '.join(cell(v) for v in (r['observed_at_utc'],r['scope_key'],r['metric_name'],value,r['unit'],r['status'],r['reason_codes']))+' |')
    lines += ['', '## Procedencia','', f"Dataset: `{data['dataset_digest']}`.",'',
        'Las anotaciones de plan, desviaciones, aciertos y estado de ánimo permanecen en las notas humanas; no se infieren desde los resultados.','',
        'Este feed no integra Profit/MaxDD ni estadísticas de sesiones. Los drawdowns del motor no acreditan el drawdown contractual de QT.','']
    return '\n'.join(lines)


def main():
    os.umask(0o077)
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--dataset',required=True,type=Path);p.add_argument('--vault',required=True,type=Path);args=p.parse_args()
    vault=args.vault.resolve();marker=vault/'.vidar-mt5-test-vault'
    if args.vault.is_symlink() or not marker.is_file() or marker.is_symlink() or marker.read_text().strip()!='vidar-mt5-test-vault-v1':
        p.error('marked test vault required')
    target=vault/'Dashboard-ensayo.md'
    if target.exists():p.error('new dashboard destination required; preserve prior output')
    output=render(json.loads(args.dataset.read_text())).encode()
    with target.open('xb') as f:f.write(output)
    print(json.dumps({'path':str(target),'synthetic':True,'metric_rows':len(json.loads(args.dataset.read_text())['metrics']),'sha256':hashlib.sha256(output).hexdigest()}))
if __name__=='__main__':main()
