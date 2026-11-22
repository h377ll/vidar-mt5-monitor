#!/usr/bin/env python3
"""Read-only MT5 journal. SQLite is canonical; Markdown is a projection.

No MT5 connection or trade primitive. Missing monetary fields remain unknown.
"""
import argparse
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import sqlite3

ROOT = Path(__file__).resolve().parent
VAULT = ROOT / 'synthetic-vault'  # delivery-local default; no real vault
os.umask(0o077)

def dumps(x):
    return json.dumps(x, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)

def digest(x):
    return hashlib.sha256(dumps(x).encode()).hexdigest()

def dec(x):
    return Decimal(str(x))

def total(values):
    return float(sum((dec(x) for x in values if x is not None), Decimal(0)))

def now():
    return datetime.now(timezone.utc).isoformat()

def account_key(a):
    if not isinstance(a.get('server'),str) or not a['server'] or not str(a.get('login','')).isdigit():
        raise ValueError('ACCOUNT_IDENTITY_MISSING')
    return dumps([a['server'], str(a['login'])])

def costs(d):
    return {'profit': d.get('profit'), 'commission': d.get('commission'),
            'swap': d.get('swaps', d.get('swap')), 'fees': d.get('fees', d.get('fee'))}

def reported(d):
    return total(costs(d).values())

def open_db(path):
    db = sqlite3.connect(path)
    db.row_factory = sqlite3.Row
    db.execute('PRAGMA journal_mode=WAL')
    db.execute('PRAGMA foreign_keys=ON')
    db.executescript('''
    CREATE TABLE IF NOT EXISTS accounts (key TEXT PRIMARY KEY, data TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS sources (source TEXT PRIMARY KEY, account_key TEXT, status TEXT, checked_at TEXT, snapshot TEXT);
    CREATE TABLE IF NOT EXISTS records (account_key TEXT, kind TEXT, id TEXT, data TEXT NOT NULL, PRIMARY KEY(account_key,kind,id));
    CREATE TABLE IF NOT EXISTS revisions (seq INTEGER PRIMARY KEY, account_key TEXT, kind TEXT, id TEXT, old_data TEXT, new_data TEXT, observed_at TEXT);
    CREATE TABLE IF NOT EXISTS imports (id INTEGER PRIMARY KEY, source TEXT, account_key TEXT, observed_at TEXT, result TEXT);
    CREATE TABLE IF NOT EXISTS events (id TEXT PRIMARY KEY, account_key TEXT, kind TEXT, observed_at TEXT, payload TEXT);
    CREATE TABLE IF NOT EXISTS outbox (event_id TEXT PRIMARY KEY REFERENCES events(id), state TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0, receipt TEXT);
    CREATE TABLE IF NOT EXISTS snapshots (account_key TEXT PRIMARY KEY, data TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS account_samples (account_key TEXT, observed_at TEXT, balance REAL, equity REAL, profit REAL, PRIMARY KEY(account_key,observed_at));
    ''')
    return db

def emit(db, key, kind, payload, at, identity=None, initial=False):
    eid = digest([key, kind, identity if identity is not None else payload])
    inserted = db.execute('INSERT OR IGNORE INTO events VALUES(?,?,?,?,?)',
                          (eid, key, kind, at, dumps(payload))).rowcount
    if inserted:
        # Historical backfill is never dispatched as a fresh trade alert.
        db.execute('INSERT INTO outbox(event_id,state) VALUES(?,?)',
                   (eid, 'historical' if initial else 'pending'))
    return inserted

def put_record(db, key, kind, rid, data, at, baseline):
    data = {k: v for k, v in data.items() if k != 'comment'}
    raw = dumps(data)
    old = db.execute('SELECT data FROM records WHERE account_key=? AND kind=? AND id=?', (key, kind, rid)).fetchone()
    if old and old['data'] == raw:
        return 0
    if old:
        db.execute('INSERT INTO revisions(account_key,kind,id,old_data,new_data,observed_at) VALUES(?,?,?,?,?,?)', (key,kind,rid,old['data'],raw,at))
    db.execute('INSERT INTO records VALUES(?,?,?,?) ON CONFLICT(account_key,kind,id) DO UPDATE SET data=excluded.data', (key,kind,rid,raw))
    if kind == 'deal':
        emit(db,key,'deal_revised' if old else 'deal_observed',data,at,[rid,digest(data)],initial=baseline)
    if kind == 'order' and data.get('state') in ('rejected','canceled','cancelled','expired'):
        emit(db,key,'order_state_observed',{'order_id':rid,'state':data['state'],'symbol':data.get('symbol')},at,[rid,digest(data)],initial=baseline)
    return 1

def require_list(obj, name):
    if not isinstance(obj.get(name), list) or any(obj.get(x) for x in ['hasMore','truncated','nextCursor']):
        raise ValueError('INCOMPLETE_' + name.upper())
    if len(obj[name]) >= 100000:
        raise ValueError('LIMIT_REACHED_' + name.upper())
    return obj[name]

def unique(items, idfield):
    result = {}
    for item in items:
        rid = str(item.get(idfield, ''))
        if not rid.isdigit():
            raise ValueError('MISSING_' + idfield)
        if rid in result and dumps(result[rid]) != dumps(item):
            raise ValueError('CONFLICTING_DUPLICATE_' + idfield)
        result[rid] = item
    return list(result.values())

def reconstruct(pid, deals, summary=None):
    deals = sorted(deals, key=lambda x:(x['open_time'], int(x['deal_id']) if str(x['deal_id']).isdigit() else x['deal_id']))
    entries, exits, partials, issues = [], [], [], []
    remaining = Decimal(0)
    direction = None
    for d in deals:
        v = dec(d.get('volume', 0))
        if v <= 0:
            issues.append('invalid_volume')
        entry = d.get('entry')
        if entry == 'in':
            if direction and d['action'] != direction:
                issues.append('mixed_entry_directions')
            direction = direction or d['action']
            entries.append(d)
            remaining += v
        elif entry in ('out','out by','out_by'):
            if not entries or d['action'] == direction:
                issues.append('unmatched_exit')
            remaining -= v
            exits.append(d)
            if remaining > 0:
                partials.append({'deal_id': d['deal_id'], 'volume': float(v), 'remaining': float(remaining), 'time_source':d['open_time']})
            elif remaining < 0:
                issues.append('negative_remaining_volume')
        else:
            # INOUT is a reversal, not a partial close. Keep raw executions intact.
            issues.append('reversal_or_unknown_entry_requires_review')
    def wavg(rows):
        denom = sum((dec(d['volume']) for d in rows), Decimal(0))
        return float(sum((dec(d['volume'])*dec(d['price_open']) for d in rows),Decimal(0))/denom) if denom else None
    sums = {k:total(costs(d)[k] for d in deals) for k in ['profit','commission','swap','fees']}
    missing = [k for k in sums if any(costs(d)[k] is None for d in deals)]
    status = 'needs_review' if issues else ('closed' if entries and exits and remaining == 0 else 'open')
    result = {'position_id':str(pid),'symbol':deals[0].get('symbol'),'direction':direction,'status':status,
              'opened_at_source':entries[0]['open_time'] if entries else None,
              'closed_at_source':exits[-1]['open_time'] if status == 'closed' else None,
              'source_timezone':None,'opened_at_utc':None,'closed_at_utc':None,'session':None,
              'entry_volume':total(d['volume'] for d in entries),'exit_volume':total(d['volume'] for d in exits),
              'remaining_volume':float(remaining),'entry_price':wavg(entries),'exit_price':wavg(exits),
              'partial_closes':partials,'reported_components':sums,
              'net_from_reported_components':total(sums.values()),'net_pnl':None if missing else total(sums.values()),
              'missing_cost_fields':missing,'initial_sl':None,'initial_tp':None,
              'sl_reported':(summary or {}).get('stop_loss'),'tp_reported':(summary or {}).get('take_profit'),
              'issues':issues,'executions':[{**{k:v for k,v in d.items() if k!='comment'},'execution_price':d.get('price_open'),'costs':costs(d)} for d in deals]}
    if summary and status == 'closed':
        for field, observed in [('open_volume',result['entry_volume']),('close_volume',result['exit_volume']),('profit',sums['profit'])]:
            if field in summary and abs(dec(summary[field])-dec(observed)) > Decimal('0.000001'):
                raise ValueError('POSITION_RECONCILIATION_' + field)
        for field, observed in [('open_price',result['entry_price']),('close_price',result['exit_price'])]:
            if field in summary and observed is not None and abs(dec(summary[field])-dec(observed)) > Decimal('0.000001'):
                raise ValueError('POSITION_PRICE_RECONCILIATION_' + field)
    return result

def normal_snapshot(rows):
    result = {}
    for p in rows:
        pid = str(p.get('identifier',p.get('position_id',p.get('ticket',''))))
        if not pid or not p.get('symbol') or p.get('type') is None or not isinstance(p.get('volume'),(int,float)):
            raise ValueError('LIVE_POSITION_SCHEMA')
        if pid in result:
            raise ValueError('DUPLICATE_LIVE_POSITION')
        result[pid] = {'id':pid,'symbol':p['symbol'],'volume':p['volume'],'type':p['type'],
                       'sl':p.get('stop_loss',p.get('sl')),'tp':p.get('take_profit',p.get('tp'))}
    return result

def observe_snapshot(db, key, rows, at):
    snapshot = normal_snapshot(rows)
    oldrow = db.execute('SELECT data FROM snapshots WHERE account_key=?',(key,)).fetchone()
    old = json.loads(oldrow[0]) if oldrow else None
    sequence = db.execute('SELECT COUNT(*) FROM events WHERE account_key=?',(key,)).fetchone()[0]
    if old is not None:
        for pid in sorted(set(old)|set(snapshot)):
            a,b=old.get(pid),snapshot.get(pid)
            if a == b: continue
            if a is None: kind='position_first_observed'
            elif b is None: kind='position_no_longer_open'
            elif a['type']!=b['type']: kind='position_direction_changed'
            elif a['volume']!=b['volume']: kind='position_volume_changed'
            else: kind='position_protection_changed'
            emit(db,key,kind,{'before':a,'after':b,'time_is_detection':True},at,[sequence,pid,a,b])
    for pid,p in snapshot.items():
        state = 'unknown' if p['sl'] is None else ('missing' if p['sl']==0 else 'reported')
        before=(old or {}).get(pid)
        if state!='reported' and (before is None or before.get('sl')!=p['sl']):
            emit(db,key,'sl_'+state,{'position_id':pid,'symbol':p['symbol']},at,[sequence,pid,state])
    db.execute('INSERT INTO snapshots VALUES(?,?) ON CONFLICT(account_key) DO UPDATE SET data=excluded.data',(key,dumps(snapshot)))

def ingest(db, bundle, limit=30):
    source,at = bundle['source'],bundle.get('observed_at',now())
    before,after=bundle.get('before',{}),bundle.get('after',{})
    a=after.get('account',before.get('account',{}))
    key=account_key(a) if a.get('server') and a.get('login') else None
    oldsrc=db.execute('SELECT * FROM sources WHERE source=?',(source,)).fetchone()
    connected=before.get('terminal',{}).get('server_connected') is True and after.get('terminal',{}).get('server_connected') is True
    if not connected:
        key=key or (oldsrc['account_key'] if oldsrc else None)
        status=bundle.get('failure_status','disconnected')
        with db:
            if not oldsrc or oldsrc['status']!=status:
                emit(db,key,'coverage_'+status,{'source':source,'exposure':'unknown'},at,[source,at])
            db.execute('INSERT INTO sources VALUES(?,?,?,?,?) ON CONFLICT(source) DO UPDATE SET account_key=excluded.account_key,status=excluded.status,checked_at=excluded.checked_at', (source,key,status,at,None))
        return {'source':source,'status':status,'identity_unverified':key}
    if key != account_key(before['account']):
        raise ValueError('ACCOUNT_CHANGED_DURING_READ')
    opened=bundle['get_trading_open_positions']
    live=require_list(opened,'positions'); require_list(opened,'orders')
    hp=bundle['get_trading_history_positions']; ho=bundle['get_trading_history_orders']
    summaries=unique(require_list(hp,'positions'),'position_id')
    deals=unique(require_list(ho,'deals'),'deal_id'); orders=unique(require_list(ho,'orders'),'order_id')
    grouped=defaultdict(list)
    for d in deals:
        if d.get('action') in ('buy','sell'):
            if not d.get('position_id') or not d.get('open_time'):
                raise ValueError('DEAL_SCHEMA')
            grouped[str(d['position_id'])].append(d)
    sm={str(p['position_id']):p for p in summaries}
    derived=[reconstruct(pid,rows,sm.get(pid)) for pid,rows in grouped.items()]
    closed=sorted((p for p in derived if p['status']=='closed'),key=lambda p:(p['closed_at_source'],p['position_id']),reverse=True)
    selected=closed if limit == 0 else closed[:limit]
    # Also retain open and unclassifiable deals: never drop an incomplete lifecycle.
    selected += [p for p in derived if p['status']!='closed']
    wanted={p['position_id'] for p in selected}
    imported_deals=[d for d in deals if str(d.get('position_id')) in wanted or d.get('action') not in ('buy','sell')]
    imported_orders=[o for o in orders if str(o.get('position_id')) in wanted or o.get('state')!='filled']
    orderids={str(o['order_id']) for o in orders}
    for d in deals:
        if d.get('action') in ('buy','sell') and str(d.get('order_id')) not in orderids:
            raise ValueError('MISSING_EXECUTION_ORDER')
    reconciliation=total(reported(d) for d in deals)
    difference=float(dec(a['balance'])-dec(reconciliation))
    full_selected=len(selected)==len(derived)
    result={'source':source,'account_key':key,'closed_available':len(closed),'closed_imported':sum(p['status']=='closed' for p in selected),
            'deal_count':len(imported_deals),'order_count':len(imported_orders),'reported_total_including_cashflows':reconciliation,
            'balance':a['balance'],'balance_difference':difference,'aggregate_matches':abs(difference)<0.005,
            'all_available_positions_selected':full_selected,'source_window':bundle.get('history_window',['2020-01-01','2026-10-18']),
            'coverage':'available_history_not_broker_statement','unknown_timezone':True}
    with db:
        baseline=not db.execute('SELECT 1 FROM accounts WHERE key=?',(key,)).fetchone()
        data={k:a.get(k) for k in ['server','login','broker','currency','type','margin_mode','balance','equity','margin_free','profit']}
        data['login']=str(data['login']); data['observed_at']=at; data['import_result']=result
        db.execute('INSERT INTO accounts VALUES(?,?) ON CONFLICT(key) DO UPDATE SET data=excluded.data',(key,dumps(data)))
        changed=0
        for kind,rows,idfield in [('deal',imported_deals,'deal_id'),('order',imported_orders,'order_id'),('position',selected,'position_id')]:
            for item in rows:
                changed+=put_record(db,key,kind,str(item[idfield]),item,at,baseline)
        # Full-window reconciliation detects removal/corrections without deleting evidence.
        if full_selected:
            current={str(d['deal_id']) for d in deals}
            for r in db.execute("SELECT id FROM records WHERE account_key=? AND kind='deal'",(key,)):
                if r['id'] not in current:
                    emit(db,key,'history_missing_record',{'deal_id':r['id'],'requires_review':True},at,[r['id'],'missing_in_complete_history'])
        observe_snapshot(db,key,live,at)
        if oldsrc and oldsrc['status']!='connected':
            emit(db,key,'coverage_restored',{'source':source},at,[source,at])
        if oldsrc and oldsrc['account_key'] and oldsrc['account_key']!=key:
            emit(db,key,'source_account_changed',{'source':source,'previous':oldsrc['account_key'],'current':key},at,[source,at])
        db.execute('INSERT INTO sources VALUES(?,?,?,?,?) ON CONFLICT(source) DO UPDATE SET account_key=excluded.account_key,status=excluded.status,checked_at=excluded.checked_at,snapshot=excluded.snapshot',(source,key,'connected',at,dumps(opened)))
        db.execute('INSERT OR IGNORE INTO account_samples VALUES(?,?,?,?,?)',(key,at,a['balance'],a['equity'],a['profit']))
        result['changed_records']=changed
        db.execute('INSERT INTO imports(source,account_key,observed_at,result) VALUES(?,?,?,?)',(source,key,at,dumps(result)))
    return result

def metrics(rows):
    vals=[p['net_from_reported_components'] for p in rows]
    wins=[x for x in vals if x>0]; losses=[x for x in vals if x<0]
    return {'positions':len(vals),'reported_pnl':total(vals),
            'win_rate_pct':100*len(wins)/len(vals) if vals else None,
            'profit_factor':total(wins)/abs(total(losses)) if losses else None,
            'expectancy':total(vals)/len(vals) if vals else None,
            'net_costs_complete':all(p['net_pnl'] is not None for p in rows),
            'basis':'reported_components_not_verified_net'}

def drawdowns(deals):
    # Trading cash flows only; deposits/withdrawals never create PnL or reset a peak.
    rows=sorted((d for d in deals if d.get('action') in ('buy','sell')),key=lambda d:(d['open_time'],str(d['deal_id'])))
    buckets=defaultdict(list)
    for d in rows:buckets[d['open_time']].append(reported(d))
    peak=cum=Decimal(0); maximum=Decimal(0); days={}
    for timestamp,values in sorted(buckets.items()):
        day=timestamp[:10]
        x=dec(total(values));cum+=x;peak=max(peak,cum);maximum=max(maximum,peak-cum)
        ds=days.setdefault(day,{'pnl':Decimal(0),'peak':Decimal(0),'dd':Decimal(0)})
        ds['pnl']+=x;ds['peak']=max(ds['peak'],ds['pnl']);ds['dd']=max(ds['dd'],ds['peak']-ds['pnl'])
    return {'max_realized_reported_drawdown':float(maximum),'floating_historical_drawdown':None,
            'resolution':'source_timestamp; simultaneous executions aggregated; intrasecond trough unknown',
            'day_basis':'source_date_timezone_unknown_not_contract_day',
            'daily':{d:{'reported_pnl':float(v['pnl']),'peak_to_trough_realized':float(v['dd']),'loss_from_day_start':float(max(Decimal(0),-v['pnl']))} for d,v in days.items()}}

def grouped_metrics(rows, group):
    groups=defaultdict(list)
    for p in rows:
        dt=datetime.fromisoformat(p['closed_at_source'])
        k=(f'{dt.isocalendar().year}-W{dt.isocalendar().week:02}' if group=='week' else dt.strftime('%Y-%m')) if group in ('week','month') else p[group]
        groups[str(k)].append(p)
    return {k:metrics(v) for k,v in sorted(groups.items())}

def atomic_text(path, content):
    path.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
    if path.is_symlink(): raise ValueError('SYMLINK_DESTINATION')
    if path.exists() and path.read_text()==content: return
    tmp=path.with_name(path.name+'.tmp')
    with open(tmp,'w',encoding='utf-8') as f:
        f.write(content);f.flush();os.fsync(f.fileno())
    os.replace(tmp,path)

def frontmatter(data):
    return '---\n'+'\n'.join(k+': '+dumps(v) for k,v in data.items())+'\n---\n\n'

def stats_md(data):
    text='Todas las cifras de rendimiento usan **componentes informados**, no neto contractual confirmado.\n\n'
    for label,key in [('Semanas','weekly'),('Meses','monthly'),('Instrumentos','instruments')]:
        text+='## '+label+'\n\n| Grupo | Posiciones | Resultado informado | Win rate % | Profit factor | Esperanza |\n|---|---:|---:|---:|---:|---:|\n'
        for k,m in data[key].items():
            cells=[k,m['positions'],m['reported_pnl'],m['win_rate_pct'],m['profit_factor'],m['expectancy']]
            text+='| '+' | '.join('?' if x is None else (f'{x:.2f}' if isinstance(x,float) else str(x)) for x in cells)+' |\n'
        text+='\n'
    return text

def export(db, destination):
    destination=Path(destination)
    if destination.is_symlink() or not destination.resolve().is_relative_to(VAULT.resolve()):
        raise ValueError('DESTINATION_NOT_AUTHORIZED_VAULT')
    if not VAULT.is_dir() or not os.access(VAULT,os.W_OK): raise ValueError('VAULT_UNAVAILABLE')
    allrows=[];alldeals=[];accounts=[];stats={}
    for ar in db.execute('SELECT * FROM accounts ORDER BY key'):
        key=ar['key'];a=json.loads(ar['data']);folder='Cuenta-'+str(a['login'])+'-'+digest(key)[:10]
        positions=[json.loads(r[0]) for r in db.execute("SELECT data FROM records WHERE account_key=? AND kind='position'",(key,))]
        deals=[json.loads(r[0]) for r in db.execute("SELECT data FROM records WHERE account_key=? AND kind='deal'",(key,))]
        orders=[json.loads(r[0]) for r in db.execute("SELECT data FROM records WHERE account_key=? AND kind='order'",(key,))]
        closed=[p for p in positions if p['status']=='closed'];sources=[r[0] for r in db.execute('SELECT source FROM sources WHERE account_key=?',(key,))]
        stat={**metrics(closed),'weekly':grouped_metrics(closed,'week'),'monthly':grouped_metrics(closed,'month'),
              'instruments':grouped_metrics(closed,'symbol'),'drawdown':drawdowns(deals),'sessions':None,'contract_distance':None,
              'floating_pnl_observed':a['profit'],'server':a['server'],'login':a['login'],'currency':a['currency']}
        stats[key]=stat
        atomic_text(destination/folder/'00-Cuenta.md',frontmatter({'schema_version':1,'account_key':json.loads(key),**a,'sources':sources})+'# Cuenta '+a['login']+'\n\nIdentidad: **servidor + número de cuenta**. Los MCP son fuentes, nunca identidades.\n\n'+stats_md(stat))
        atomic_text(destination/folder/'Estadisticas.json',json.dumps(stat,ensure_ascii=False,indent=2)+'\n')
        daily='## Drawdown realizado diario observado\n\nFechas del servidor sin zona acreditada. No equivale al límite diario contractual.\n\n| Fecha fuente | PnL informado | Caída desde máximo intradía | Pérdida desde inicio del día |\n|---|---:|---:|---:|\n'
        for date,d in stat['drawdown']['daily'].items(): daily+=f"| {date} | {d['reported_pnl']:.2f} | {d['peak_to_trough_realized']:.2f} | {d['loss_from_day_start']:.2f} |\n"
        atomic_text(destination/folder/'01-Drawdown.md',daily+'\nDrawdown realizado máximo de la muestra: '+str(stat['drawdown']['max_realized_reported_drawdown'])+' '+a['currency']+'.\n\nDrawdown flotante histórico, sesiones y distancia contractual: **desconocidos**.\n')
        for p in positions:
            p={**p,'schema_version':1,'account_key':json.loads(key),'currency':a['currency'],'sources':sources,
               'orders':[o for o in orders if str(o.get('position_id'))==p['position_id']]}
            atomic_text(destination/folder/'Operaciones'/('Posicion-'+p['position_id']+'.md'),frontmatter(p)+f"# {p['symbol']} · {p['direction']} · {p['position_id']}\n\nEstado: **{p['status']}**. Resultado de componentes informados: **{p['net_from_reported_components']:.2f} {a['currency']}**.\n\nEjecuciones, órdenes y cierres parciales conservados en las propiedades. Los campos omitidos son `null`, no cero. `price_open` del deal es su precio de ejecución, también en salidas.\n\nSL/TP iniciales y contexto psicológico no inferidos. Escribe tus notas en una nota aparte enlazada; esta proyección se regenera.\n")
        accounts.append({**a,'sources':sources,'folder':folder});allrows += [{**p,'currency':a['currency'],'server':a['server']} for p in closed]
        alldeals += [{**d,'currency':a['currency'],'server':a['server']} for d in deals]
    consolidated={}
    # Without verified zones, consolidate periods only within the same server and currency.
    for server,currency in sorted(set((p['server'],p['currency']) for p in allrows)):
        ps=[p for p in allrows if (p['server'],p['currency'])==(server,currency)]
        s={**metrics(ps),'weekly':grouped_metrics(ps,'week'),'monthly':grouped_metrics(ps,'month'),'instruments':grouped_metrics(ps,'symbol'),
           'drawdown':drawdowns([d for d in alldeals if (d['server'],d['currency'])==(server,currency)])}
        consolidated[server+' / '+currency]=s
    atomic_text(destination/'Consolidado.json',json.dumps(consolidated,ensure_ascii=False,indent=2)+'\n')
    text='# Diario de prueba — Trading Stack\n\nHasta 30 posiciones cerradas por cuenta. No importación general ni vigilancia activada.\n\n'
    for a in accounts:text+=f"- [{a['server']} · {a['login']}]({a['folder']}/00-Cuenta.md): {a['import_result']['closed_imported']} posiciones; saldo observado {a['balance']:.2f} {a['currency']}.\n"
    for group,s in consolidated.items():
        text+='\n# Consolidado '+group+'\n\n'+stats_md(s)
        text+='Drawdown realizado máximo de componentes informados: **'+str(s['drawdown']['max_realized_reported_drawdown'])+' '+group.split(' / ')[-1]+'**. Flotante histórico desconocido; resolución de la fuente, no tick a tick.\n\n'
        text+='| Fecha fuente | Resultado realizado | Caída desde máximo intradía |\n|---|---:|---:|\n'
        for day,d in s['drawdown']['daily'].items():text+=f"| {day} | {d['reported_pnl']:.2f} | {d['peak_to_trough_realized']:.2f} |\n"
    text+='\n## Cobertura y límites\n\nLas fuentes no verificadas permanecen desconocidas; no atribuir ceros a saldos reales ni descartar duplicaciones entre fuentes.\n\nLas fechas semanales/mensuales usan fecha fuente, no UTC. La consolidación solo une cuentas del mismo servidor y moneda. Costes omitidos, zona histórica, sesiones, límites contractuales y drawdown flotante histórico siguen desconocidos. El drawdown realizado se calcula con ejecuciones y comisiones al momento registrado, excluyendo depósitos.\n\nNo se abren, cierran ni modifican operaciones. Los Markdown son proyecciones de SQLite, no el registro canónico.\n'
    atomic_text(destination/'00-Estado.md',text)
    return {'accounts':len(accounts),'positions':len(allrows),'destination':str(destination),'consolidated':consolidated}

def main():
    p=argparse.ArgumentParser();p.add_argument('--db',default=str(ROOT/'journal.sqlite'));p.add_argument('--import-file',action='append',default=[]);p.add_argument('--limit',type=int,default=30);p.add_argument('--export');p.add_argument('--recover',action='store_true')
    args=p.parse_args();db=open_db(args.db)
    if args.recover:
        with db: db.execute("UPDATE outbox SET state='uncertain' WHERE state='attempting'")
    for f in args.import_file: print(dumps(ingest(db,json.loads(Path(f).read_text()),args.limit)))
    if args.export: print(dumps(export(db,args.export)))
    db.close()

if __name__=='__main__': main()
