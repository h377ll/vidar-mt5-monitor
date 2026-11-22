"""Deterministic outbox transport boundary. No network implementation or credentials.

The caller must explicitly provide an authorized sender. Not wired into collector.
"""
from datetime import datetime,timezone
import json

LABELS={
 'coverage_unavailable':'Cobertura MT5 no disponible',
 'coverage_disconnected':'Terminal MT5 desconectado',
 'coverage_restored':'Cobertura MT5 recuperada',
 'source_account_changed':'Cuenta del conector cambiada',
 'deal_observed':'Ejecución MT5 observada',
 'deal_revised':'Ejecución histórica corregida',
 'position_first_observed':'Posición observada',
 'position_no_longer_open':'Posición ya no presente; contrastar ejecuciones',
 'position_volume_changed':'Volumen de posición modificado',
 'position_direction_changed':'Dirección de posición modificada',
 'position_protection_changed':'SL/TP modificado',
 'sl_missing':'Posición con SL explícitamente cero',
 'sl_unknown':'Protección SL desconocida',
 'order_state_observed':'Estado de orden observado',
 'history_missing_record':'Registro histórico ausente; requiere revisión',
}

class DefiniteDeliveryFailure(Exception):pass

def render(row):
    payload=json.loads(row['payload'])
    account=json.loads(row['account_key']) if row['account_key'] else None
    # Hide login by default in notifications, retain canonical identity in the private ledger.
    alias=(account[0]+' / …'+str(account[1])[-4:]) if account else payload.get('source','sin identificar')
    detail=payload.get('symbol') or payload.get('source') or payload.get('state') or ''
    return f"[Trading Stack · detección] {LABELS.get(row['kind'], 'Incidencia registrada')}\nCuenta/fuente: {alias}\n{detail}\nObservado: {row['observed_at']}\nID: {row['id'][:12]}"

def dispatch_one(db,sender,at=None,max_age_seconds=300):
    """Persist intent before external I/O. Missing receipt => uncertain, never automatic retry."""
    at=at or datetime.now(timezone.utc)
    db.execute('BEGIN IMMEDIATE')
    try:
        row=db.execute("SELECT e.* FROM outbox o JOIN events e ON e.id=o.event_id WHERE o.state='pending' ORDER BY e.observed_at,e.id LIMIT 1").fetchone()
        if row is None:db.commit();return 'empty'
        observed=datetime.fromisoformat(row['observed_at'].replace('Z','+00:00'))
        if observed.tzinfo is None:raise ValueError('EVENT_OBSERVATION_TIMEZONE_UNKNOWN')
        age=(at-observed).total_seconds()
        if age<0:db.rollback();return 'clock_hold'
        if age>max_age_seconds:
            db.execute("UPDATE outbox SET state='expired' WHERE event_id=?",(row['id'],));db.commit();return 'expired'
        db.execute("UPDATE outbox SET state='attempting', attempts=attempts+1 WHERE event_id=?",(row['id'],));db.commit()
    except Exception:db.rollback();raise
    try:
        receipt=sender(render(row),row['id'])
        if not isinstance(receipt,dict) or not receipt.get('provider_message_id'):
            raise ValueError('MISSING_PROVIDER_RECEIPT')
        state='delivered'
    except DefiniteDeliveryFailure:
        state='failed';receipt=None
    except Exception:
        state='uncertain';receipt=None
    with db:
        db.execute('UPDATE outbox SET state=?,receipt=? WHERE event_id=?',(state,json.dumps(receipt) if receipt else None,row['id']))
    return state

def recover(db):
    with db:db.execute("UPDATE outbox SET state='uncertain' WHERE state='attempting'")
