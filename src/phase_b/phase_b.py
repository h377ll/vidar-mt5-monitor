#!/usr/bin/env python3
"""Synthetic-only integration. Explicit ephemeral CLI; never a service or MT5 client.

Fixture histories are an independent invented authority, not callback fields.
This module has no live provider, no broker API and no notification transport.
"""
import argparse
from datetime import datetime, timezone
import fcntl
import json
import os
from pathlib import Path
import sqlite3
import sys
import time

try:
    from . import event_receiver as er
    from . import journal as j
    from . import notifications as n
    from . import spool_bridge as bridge
except ImportError:  # Supported direct-file execution used by the historical runners.
    import event_receiver as er
    import journal as j
    import notifications as n
    import spool_bridge as bridge

SERVER = 'SYNTHETIC-PHASE-B'
SOURCE = 'synthetic-terminal'


def utc(epoch):
    return datetime.fromtimestamp(epoch, timezone.utc).isoformat()


def identity(e):
    return j.dumps([e['source'], e['stream'], e['seq']])


def initialize(db):
    db.executescript('''
    CREATE TABLE IF NOT EXISTS processed(source TEXT,stream TEXT,seq INTEGER,
      result TEXT, PRIMARY KEY(source,stream,seq));
    CREATE TABLE IF NOT EXISTS reconcile_errors(source TEXT,stream TEXT,seq INTEGER,
      reason TEXT,attempts INTEGER, PRIMARY KEY(source,stream,seq));
    CREATE TABLE IF NOT EXISTS reconciled_clocks(account_key TEXT PRIMARY KEY,captured REAL);
    CREATE TABLE IF NOT EXISTS simulated_receipts(event_id TEXT PRIMARY KEY,
      message TEXT, receipt TEXT, accepted_at TEXT);
    ''')


def health(db, at):
    """A fresh heartbeat cannot renew the freshness of verified exposure."""
    with db:
        for row in db.execute('SELECT * FROM coverage').fetchall():
            if row['last_snapshot'] is None:
                continue
            if 0 <= at-row['last_snapshot'] <= 30:
                continue
            state = db.execute('SELECT status FROM sources WHERE source=?', (row['source'],)).fetchone()
            if state and state[0] == 'connected':
                j.emit(db,row['account_key'],'coverage_stale',
                       {'source':row['source'],'exposure':'unknown'},utc(at),
                       [row['source'],row['last_snapshot']])
                db.execute("UPDATE sources SET status='stale' WHERE source=?", (row['source'],))
                db.execute('UPDATE coverage SET needs_reconciliation=1 WHERE source=?', (row['source'],))


def validate_fixture(e, fixture, at):
    if fixture.get('_synthetic_phase_b') is not True:
        raise ValueError('SYNTHETIC_MARKER_REQUIRED')
    if not fixture.get('available', True):
        raise ValueError('SYNTHETIC_PROVIDER_UNAVAILABLE')
    b = fixture['bundle']
    if b['source'] != e['source'] or e['source'] != SOURCE or e['server'] != SERVER:
        raise ValueError('FIXTURE_SOURCE_MISMATCH')
    expected = j.account_key(e)
    for side in ('before', 'after'):
        if j.account_key(b[side]['account']) != expected:
            raise ValueError('ACCOUNT_CHANGED_DURING_READ')
    captured = fixture.get('captured_utc')
    if type(captured) not in (int, float) or not 0 <= at-captured <= 30:
        raise ValueError('STALE_OR_FUTURE_AUTHORITY')
    if b.get('observed_at') != utc(captured):
        raise ValueError('OBSERVATION_TIME_MISMATCH')
    connected = all(b[s]['terminal'].get('server_connected') is True for s in ('before', 'after'))
    if connected:
        if fixture.get('history_complete') is not True:
            raise ValueError('HISTORY_NOT_COMPLETE')
        window = b.get('history_window')
        if not isinstance(window,list) or len(window) != 2 or window[0] != '2026-01-01':
            raise ValueError('SYNTHETIC_FULL_HISTORY_REQUIRED')
        # Validate the independent snapshot using the same strict ingress rules.
        for p in b['get_trading_open_positions']['positions']:
            if p.get('type') not in ('buy','sell') or type(p.get('volume')) not in (int,float) or p['volume'] <= 0:
                raise ValueError('POSITION_VALUES')
        j.normal_snapshot(b['get_trading_open_positions']['positions'])
    return b, expected, captured, connected


def reconcile(db, fixtures, at, crash=None, limit=100):
    initialize(db)
    health(db, at)
    rows = db.execute('''SELECT i.* FROM ingress i LEFT JOIN processed p
      USING(source,stream,seq) WHERE p.seq IS NULL ORDER BY i.received,i.source,i.stream,i.seq LIMIT ?''', (limit,)).fetchall()
    result = {'processed':0, 'pending':0}
    for row in rows:
        e = json.loads(row['body'])
        keys = (e['source'],e['stream'],e['seq'])
        try:
            fixture = fixtures.get(identity(e))
            if fixture is None:
                raise ValueError('AUTHORITY_MISSING')
            b,key,captured,connected = validate_fixture(e,fixture,at)
            db.execute('BEGIN IMMEDIATE')
            old = db.execute('SELECT captured FROM reconciled_clocks WHERE account_key=?', (key,)).fetchone()
            if old and captured < old[0]:
                raise ValueError('AUTHORITY_REGRESSION')
            if old and captured == old[0]:
                # Equal clock is safe only for exactly the already applied bundle.
                previous = db.execute('SELECT result FROM processed WHERE source=? ORDER BY rowid DESC', (e['source'],)).fetchall()
                if not any(json.loads(p[0]).get('bundle_hash')==j.digest(b) for p in previous):
                    raise ValueError('AUTHORITY_CLOCK_CONFLICT')
            # Check durable sequence gaps; do not treat callback data as trades.
            seen = db.execute('SELECT COUNT(*) FROM ingress WHERE source=? AND stream=? AND seq<=?', keys).fetchone()[0]
            if seen < e['seq'] or e['kind']=='overflow':
                j.emit(db,key,'event_coverage_gap',{'source':e['source'],'observed_seq':e['seq'],
                       'recovery':'independent_complete_synthetic_history'},utc(at),list(keys))
            outcome = j.ingest(db,b,limit=0,commit=False)
            if connected and (not outcome['aggregate_matches'] or not outcome['all_available_positions_selected']):
                raise ValueError('HISTORY_RECONCILIATION_MISMATCH')
            db.execute('INSERT INTO reconciled_clocks VALUES(?,?) ON CONFLICT(account_key) DO UPDATE SET captured=excluded.captured', (key,captured))
            db.execute('UPDATE coverage SET account_key=?,connected=?,last_state_capture=?,needs_reconciliation=1 WHERE source=?', (key,int(connected),captured,e['source']))
            if connected:
                db.execute('UPDATE coverage SET last_snapshot=?,last_snapshot_stream=?,last_snapshot_seq=? WHERE source=?', (captured,e['stream'],e['seq'],e['source']))
            record = {'bundle_hash':j.digest(b), 'captured_utc':captured, 'connected':connected, 'result':outcome}
            db.execute('INSERT INTO processed VALUES(?,?,?,?)', (*keys,j.dumps(record)))
            db.execute('DELETE FROM reconcile_errors WHERE source=? AND stream=? AND seq=?',keys)
            pending = db.execute('''SELECT COUNT(*) FROM ingress i LEFT JOIN processed p USING(source,stream,seq)
                WHERE i.source=? AND p.seq IS NULL''', (e['source'],)).fetchone()[0]
            db.execute('UPDATE coverage SET needs_reconciliation=? WHERE source=?', (int(not connected or pending>0),e['source']))
            if crash=='before_commit':
                os._exit(71)
            db.commit()
            if crash=='after_commit':
                os._exit(72)
            result['processed'] += 1
        except (ValueError, KeyError, TypeError) as exc:
            db.rollback()
            with db:
                db.execute('INSERT INTO reconcile_errors VALUES(?,?,?,?,1) ON CONFLICT(source,stream,seq) DO UPDATE SET reason=excluded.reason,attempts=attempts+1', (*keys,str(exc)))
                db.execute('UPDATE coverage SET needs_reconciliation=1 WHERE source=?',(e['source'],))
            result['pending'] += 1
    return result


def notify(db, at, fault=None):
    """Only a SQLite fake provider exists. A simulated receipt is never human receipt."""
    initialize(db)
    n.recover(db)
    results=[]
    def sender(message,eid):
        if fault=='before_accept':
            os._exit(73)
        if fault=='definite':
            raise n.DefiniteDeliveryFailure()
        receipt={'provider_message_id':'SIMULATED-'+eid, 'simulated':True}
        with db:
            db.execute('INSERT OR IGNORE INTO simulated_receipts VALUES(?,?,?,?)',
                       (eid,'[SIMULACIÓN SIN ENVÍO]\n'+message,j.dumps(receipt),utc(at)))
        if fault=='after_accept':
            os._exit(74)
        return receipt
    for _ in range(1000):
        state=n.dispatch_one(db,sender,datetime.fromtimestamp(at,timezone.utc))
        if state in ('empty','clock_hold'):
            return {'states':results,'stop':state}
        results.append(state)
    raise RuntimeError('DISPATCH_LIMIT')


def guard(root, mode, endpoint=None):
    """Process-level audit guards, not an OS sandbox. All children install them."""
    root=Path(root).resolve()
    code=Path(__file__).resolve().parent
    allowed_reads=[root,code,Path(sys.base_prefix).resolve(),Path('/usr/share/zoneinfo')]
    def inside(path):
        return Path(os.fsdecode(path)).resolve().is_relative_to(root)
    def audit(event,args):
        if event=='open' and not isinstance(args[0],int):
            path,_,flags=args
            p=Path(os.fsdecode(path)).resolve()
            writing=flags & (os.O_WRONLY|os.O_RDWR|os.O_CREAT|os.O_TRUNC|os.O_APPEND)
            if writing and not inside(path):
                raise PermissionError('WRITE_OUTSIDE_TEMP')
            if not writing and not any(p.is_relative_to(r) for r in allowed_reads) and str(p) not in ('/dev/urandom','/etc/localtime'):
                raise PermissionError('READ_OUTSIDE_TEST')
        elif event=='sqlite3.connect' and not inside(args[0]):
            raise PermissionError('SQLITE_OUTSIDE_TEMP')
        elif event=='socket.bind' and (mode!='receiver' or args[1]!=('127.0.0.1',0)):
            raise PermissionError('BIND_NOT_EPHEMERAL_LOOPBACK')
        elif event=='socket.connect' and (mode!='bridge' or args[1]!=endpoint):
            raise PermissionError('NETWORK_OUTSIDE_TEST')
        elif event=='socket.getaddrinfo' and args[0]!='127.0.0.1':
            raise PermissionError('EXTERNAL_DNS')
        elif event in ('subprocess.Popen','os.system','os.exec','os.posix_spawn'):
            raise PermissionError('NESTED_PROCESS')
        elif event in ('os.remove','os.rmdir','os.mkdir') and not inside(args[0]):
            raise PermissionError('MUTATION_OUTSIDE_TEMP')
        elif event in ('os.rename','os.replace') and not all(inside(p) for p in args[:2]):
            raise PermissionError('RENAME_OUTSIDE_TEMP')
    sys.addaudithook(audit)


def main():
    p=argparse.ArgumentParser()
    p.add_argument('mode',choices=['receiver','bridge','worker','notifier','health'])
    p.add_argument('--root',required=True)
    p.add_argument('--at',type=float)
    p.add_argument('--endpoint')
    p.add_argument('--fault')
    p.add_argument('--login',default='900001')
    a=p.parse_args()
    root=Path(a.root).resolve()
    if not root.is_dir() or not root.name.startswith('vidar-phase-b-'):
        raise ValueError('TEMPORARY_SYNTHETIC_ROOT_REQUIRED')
    if json.loads((root/'sandbox.json').read_text()) != {'synthetic_only':True}:
        raise ValueError('SYNTHETIC_SANDBOX_REQUIRED')
    from urllib.parse import urlsplit
    url=urlsplit(a.endpoint) if a.endpoint else None
    guard(root,a.mode,(url.hostname,url.port) if url else None)
    at=time.time() if a.at is None else a.at
    dbpath=root/'test.sqlite'
    secret=bridge.load_key(root/'synthetic.key') if a.mode in ('receiver','bridge') else None
    if a.mode=='receiver':
        server=er.make_server(dbpath,{SOURCE:(secret,SERVER,a.login)},crash_after_store=a.fault=='after_commit_before_ack')
        print(j.dumps({'port':server.server_port,'host':'127.0.0.1'}),flush=True)
        try:server.serve_forever(poll_interval=0.05)
        finally:server.server_close()
        return
    if a.mode=='bridge':
        result=bridge.deliver(root/'spool.jsonl',root/'cursor.json',a.endpoint,secret)
    else:
        # Process lock covers both reconciliation and dispatch, including external I/O.
        # Separate receiver may still append durable inbox entries via SQLite WAL.
        with (root/'worker.lock').open('a') as lock:
            fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
            db=j.open_db(dbpath)
            try:
                initialize(db)
                if a.mode=='worker':
                    result=reconcile(db,json.loads((root/'authority.json').read_text()),at,a.fault)
                elif a.mode=='notifier':
                    result=notify(db,at,a.fault)
                else:
                    health(db,at);result={'health_checked':True}
            finally:db.close()
    print(j.dumps(result),flush=True)


if __name__=='__main__':
    main()
