#!/usr/bin/env python3
"""Experimental authenticated loopback ingress; no service or trading API.

Only a receiver *factory*: no process starts on import. Key provision and account
allowlist are explicit arguments. A spool bridge signs raw bytes before posting.
Business reconciliation is deliberately independent of transport acknowledgements.
"""
import hashlib
import hmac
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import re
import time
import journal as j

MAX_BODY = 262144

def signature(secret, timestamp, nonce, body):
    return hmac.new(secret, str(timestamp).encode()+b'\n'+nonce.encode()+b'\n'+body, hashlib.sha256).hexdigest()

def signed_headers(secret, body, timestamp, nonce):
    return {'X-TS':str(timestamp),'X-Nonce':nonce,'X-Signature':signature(secret,timestamp,nonce,body),'Content-Type':'application/json'}

def reject_constant(value):
    raise ValueError('NONFINITE_JSON')

class Receiver:
    def __init__(self, db, credentials):
        self.db=db
        # source -> (secret bytes, exact server, string login); no wildcard accounts.
        self.credentials=credentials
        if not credentials or any(len(v[0])<32 for v in credentials.values()):
            raise ValueError('KEY_TOO_SHORT')
        db.executescript('''
        CREATE TABLE IF NOT EXISTS ingress(source TEXT, stream TEXT, seq INTEGER,
          hash TEXT NOT NULL, body TEXT NOT NULL, received REAL NOT NULL,
          PRIMARY KEY(source,stream,seq));
        CREATE TABLE IF NOT EXISTS nonces(source TEXT,nonce TEXT,expires REAL,
          PRIMARY KEY(source,nonce));
        CREATE TABLE IF NOT EXISTS streams(source TEXT,stream TEXT,high_seq INTEGER,
          PRIMARY KEY(source,stream));
        CREATE TABLE IF NOT EXISTS coverage(source TEXT PRIMARY KEY, account_key TEXT,
          last_seen REAL, connected INTEGER, needs_reconciliation INTEGER NOT NULL,
          last_snapshot REAL, last_snapshot_stream TEXT, last_snapshot_seq INTEGER,
          last_state_capture REAL);
        CREATE TABLE IF NOT EXISTS snapshot_clocks(account_key TEXT PRIMARY KEY,captured REAL);
        ''')

    def accept(self, body, headers, now=None):
        now=time.time() if now is None else now
        if len(body)>MAX_BODY:raise ValueError('TOO_LARGE')
        e=json.loads(body,parse_constant=reject_constant)
        if not isinstance(e,dict):raise ValueError('SCHEMA')
        source=e.get('source')
        if not isinstance(source,str) or source not in self.credentials:raise PermissionError('UNKNOWN_SOURCE')
        secret,server,login=self.credentials[source]
        try: ts=int(headers.get('X-TS',''));nonce=headers.get('X-Nonce','')
        except (TypeError,ValueError):raise PermissionError('AUTH_FORMAT')
        if abs(now-ts)>60 or not re.fullmatch(r'[A-Za-z0-9_-]{16,96}',nonce):raise PermissionError('AUTH_STALE')
        if not hmac.compare_digest(signature(secret,ts,nonce,body),headers.get('X-Signature','')):
            raise PermissionError('AUTH_FAILED')
        if e.get('server')!=server or str(e.get('login'))!=login:raise PermissionError('ACCOUNT_NOT_PINNED')
        stream=e.get('stream');seq=e.get('seq');kind=e.get('kind');payload=e.get('payload')
        if not isinstance(stream,str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,96}',stream):raise ValueError('STREAM')
        if type(seq) is not int or not 0<seq<2**63:raise ValueError('SEQ')
        if kind not in ('transaction','snapshot','overflow','heartbeat') or not isinstance(payload,dict):raise ValueError('EVENT_SCHEMA')
        if kind in ('snapshot','heartbeat') and type(payload.get('connected')) is not bool:raise ValueError('CONNECTED_REQUIRED')
        if kind=='snapshot':
            if payload.get('complete') is not True or not isinstance(payload.get('positions'),list):raise ValueError('INCOMPLETE_SNAPSHOT')
            j.normal_snapshot(payload['positions'])
            for p in payload['positions']:
                if p.get('type') not in ('buy','sell') or not isinstance(p.get('volume'),(int,float)) or p['volume']<=0:raise ValueError('POSITION_VALUES')
                for f in ('sl','tp'):
                    if p.get(f) is not None and (type(p[f]) not in (int,float) or p[f]<0):raise ValueError('PROTECTION_VALUES')
        key=j.account_key({'server':server,'login':login});rawhash=hashlib.sha256(body).hexdigest()
        at=j.datetime.fromtimestamp(now,j.timezone.utc).isoformat()
        with self.db:
            self.db.execute('BEGIN IMMEDIATE')
            self.db.execute('DELETE FROM nonces WHERE expires<?',(now,))
            if self.db.execute('SELECT 1 FROM nonces WHERE source=? AND nonce=?',(source,nonce)).fetchone():raise PermissionError('REPLAY_NONCE')
            old=self.db.execute('SELECT hash FROM ingress WHERE source=? AND stream=? AND seq=?',(source,stream,seq)).fetchone()
            if old and old[0]!=rawhash:raise ValueError('SEQUENCE_CONTENT_CONFLICT')
            self.db.execute('INSERT INTO nonces VALUES(?,?,?)',(source,nonce,now+120))
            if old:return {'status':'duplicate','ack':seq,'reconciled':False}
            previous=self.db.execute('SELECT high_seq FROM streams WHERE source=? AND stream=?',(source,stream)).fetchone()
            high=previous[0] if previous else 0
            gap=seq>high+1
            self.db.execute('INSERT INTO ingress VALUES(?,?,?,?,?,?)',(source,stream,seq,rawhash,body.decode(),now))
            self.db.execute('INSERT INTO streams VALUES(?,?,?) ON CONFLICT(source,stream) DO UPDATE SET high_seq=max(high_seq,excluded.high_seq)',(source,stream,seq))
            oldcov=self.db.execute('SELECT * FROM coverage WHERE source=?',(source,)).fetchone()
            # Every callback still needs independent authoritative history reconciliation.
            self.db.execute('INSERT INTO coverage VALUES(?,?,?,NULL,1,NULL,NULL,NULL,NULL) ON CONFLICT(source) DO UPDATE SET last_seen=excluded.last_seen,needs_reconciliation=1',(source,key,now))
            ident=[source,stream,seq]
            if gap or kind=='overflow':
                j.emit(self.db,key,'event_coverage_gap',{'source':source,'expected_seq':high+1,'observed_seq':seq},at,ident)
            if kind in ('heartbeat','snapshot'):
                connected=payload['connected']
                # Receiver time cannot prove when a spooled snapshot was captured.
                captured=payload.get('captured_utc')
                fresh=type(captured) in (int,float) and 0<=now-captured<=30
                newer=oldcov is None or oldcov['last_state_capture'] is None or (fresh and captured>oldcov['last_state_capture'])
                if fresh and newer:
                    if oldcov is None or oldcov['connected']!=int(connected):
                        j.emit(self.db,key,'coverage_restored' if connected else 'coverage_disconnected',{'source':source,'exposure':'requires_reconciliation' if connected else 'unknown'},at,ident)
                    self.db.execute('UPDATE coverage SET connected=?,last_state_capture=? WHERE source=?',(int(connected),captured,source))
                if kind=='snapshot' and connected and fresh:
                    global_clock=self.db.execute('SELECT captured FROM snapshot_clocks WHERE account_key=?',(key,)).fetchone()
                    if global_clock is None or captured>global_clock[0]:
                        j.observe_snapshot(self.db,key,payload['positions'],at)
                        self.db.execute('INSERT INTO snapshot_clocks VALUES(?,?) ON CONFLICT(account_key) DO UPDATE SET captured=excluded.captured',(key,captured))
                        self.db.execute('UPDATE coverage SET last_snapshot=?,last_snapshot_stream=?,last_snapshot_seq=? WHERE source=?',(captured,stream,seq,source))
            return {'status':'stored','ack':seq,'reconciled':False,'gap':gap}

    def status(self, source, now=None):
        now=time.time() if now is None else now
        r=self.db.execute('SELECT * FROM coverage WHERE source=?',(source,)).fetchone()
        if r is None:return {'coverage':'unknown','needs_reconciliation':True}
        return {'coverage':'fresh_transport' if now-r['last_seen']<=30 else 'stale_transport',
                'connected_reported':bool(r['connected']) if r['connected'] is not None else None,
                'snapshot_fresh':r['last_snapshot'] is not None and 0<=now-r['last_snapshot']<=30,
                'needs_reconciliation':bool(r['needs_reconciliation'])}

def make_server(db_path, credentials, port=0):
    """Local test receiver; callers own its bounded lifetime. No public bind option."""
    class Handler(BaseHTTPRequestHandler):
        def log_message(self,*args):pass
        def do_POST(self):
            self.connection.settimeout(3)
            if self.path!='/v1/events':self.send_error(404);return
            try:
                size=int(self.headers.get('Content-Length','0'))
                if not 0<size<=MAX_BODY:raise ValueError('BODY_SIZE')
                if self.headers.get('Content-Type')!='application/json':raise ValueError('CONTENT_TYPE')
                body=self.rfile.read(size)
                if len(body)!=size:raise ValueError('SHORT_BODY')
                db=j.open_db(db_path)
                try:result=Receiver(db,credentials).accept(body,self.headers)
                finally:db.close()
                raw=j.dumps(result).encode()
                self.send_response(200);self.send_header('Content-Type','application/json');self.send_header('Content-Length',str(len(raw)));self.end_headers();self.wfile.write(raw)
            except PermissionError:self.send_error(403,'AUTH_REJECTED')
            except (ValueError,TypeError,KeyError,TimeoutError):self.send_error(400,'INVALID_EVENT')
    return HTTPServer(('127.0.0.1',port),Handler)
