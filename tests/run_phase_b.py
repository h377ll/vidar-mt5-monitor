#!/usr/bin/env python3
"""Reproducible synthetic acceptance harness. No MT5, services or real messaging.

Only bundled source and phase-C documents are hashed; no private project/vault is read.
Test inputs, credentials, processes, sockets and databases are invented/ephemeral.
"""
import copy
from datetime import datetime,timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import secrets
import selectors
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
from urllib.request import Request,build_opener,ProxyHandler
from urllib.error import HTTPError

sys.dont_write_bytecode=True
os.umask(0o077)
PACKAGE = Path(__file__).resolve().parents[1]
import argparse
parser = argparse.ArgumentParser(description='Reproducción sintética; no MT5 ni envíos reales')
parser.add_argument('--output-dir', required=True, help='Directorio NUEVO, fuera del paquete')
args = parser.parse_args()
OUTPUT = Path(args.output_dir).expanduser().resolve()
if OUTPUT.is_relative_to(PACKAGE):
    parser.error('Los resultados deben quedar fuera del paquete y sus evidencias históricas')
OUTPUT.mkdir(parents=True, exist_ok=False)
CODE=PACKAGE/'src'/'phase_b'
sys.path.insert(0,str(CODE))
import phase_b as b
import journal as j
import event_receiver as er

ORIGINAL=PACKAGE/'src'/'original'
VAULT=PACKAGE/'docs'/'phase-c'
OBS={'process_runs':[], 'listeners':[], 'temporary_roots':[], 'external_network_implementation':False}


def hashes(root):
    result={}
    if root.exists():
        for p in sorted(root.rglob('*')):
            if p.is_file() and not p.is_symlink():
                h=hashlib.sha256()
                with p.open('rb') as f:
                    for chunk in iter(lambda:f.read(1048576),b''):h.update(chunk)
                result[str(p.relative_to(root))]=h.hexdigest()
    return result


def position(volume=1,sl=1,side='buy'):
    return {'position_id':'101','symbol':'SYNTHUSD','type':side,'volume':volume,'sl':sl,'tp':2}


def fixture(at,positions=None,deals=None,login='900001',connected=True):
    """Independent invented full history, including all cashflows from toy inception."""
    deals=copy.deepcopy(deals or [])
    deposit={'deal_id':'1','action':'balance','profit':1000,'commission':0,'swap':0,'fees':0,'open_time':'2026-01-01T00:00:00'}
    all_deals=[deposit]+deals
    balance=1000+sum(sum(d.get(k,0) for k in ('profit','commission','swap','fees')) for d in deals)
    account={'server':b.SERVER,'login':login,'broker':'Fictional','type':'demo','currency':'USD','margin_mode':'hedging',
             'balance':balance,'equity':balance,'profit':0,'margin_free':balance}
    state={'account':account,'terminal':{'server_connected':connected}}
    bundle={'source':b.SOURCE,'observed_at':b.utc(at),'before':copy.deepcopy(state),'after':copy.deepcopy(state),
            'history_window':['2026-01-01','2026-12-31'],
            'get_trading_open_positions':{'positions':copy.deepcopy(positions or []),'orders':[]},
            'get_trading_history_positions':{'positions':[]},
            'get_trading_history_orders':{'deals':all_deals,'orders':[{'order_id':d['order_id'],'position_id':'101','symbol':'SYNTHUSD','state':'filled'} for d in deals]}}
    return {'_synthetic_phase_b':True,'available':True,'history_complete':True,'captured_utc':at,'bundle':bundle}


def deal(rid='2',entry='in',volume=1,profit=0):
    return {'deal_id':rid,'order_id':rid,'position_id':'101','symbol':'SYNTHUSD','action':'buy' if entry=='in' else 'sell',
            'entry':entry,'volume':volume,'price_open':1.1,'open_time':f'2026-02-01T00:00:{int(rid):02}',
            'profit':profit,'commission':0,'swap':0,'fees':0}


class Pipeline(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(prefix='vidar-phase-b-')
        self.root=Path(self.tmp.name)
        OBS['temporary_roots'].append(str(self.root))
        (self.root/'sandbox.json').write_text(j.dumps({'synthetic_only':True}))
        (self.root/'synthetic.key').write_bytes(secrets.token_bytes(32))
        (self.root/'synthetic.key').chmod(0o600)
        (self.root/'authority.json').write_text('{}')
        (self.root/'spool.jsonl').write_text('')
        self.at=time.time()
        self.auth={};self.seq=0;self.receivers=[];self.endpoint=None

    def tearDown(self):
        for proc,port in self.receivers:
            if proc.poll() is None:
                proc.terminate()
                try:proc.wait(timeout=4)
                except subprocess.TimeoutExpired:proc.kill();proc.wait(timeout=2)
            if proc.stdout:proc.stdout.close()
            if proc.stderr:proc.stderr.close()
            with socket.socket() as s:
                s.settimeout(.2)
                closed=s.connect_ex(('127.0.0.1',port))!=0
            OBS['listeners'].append({'port':port,'closed':closed,'returncode':proc.returncode})
            self.assertTrue(closed)
        self.tmp.cleanup()
        self.assertFalse(self.root.exists())

    def command(self,mode,at=None,**options):
        args=[sys.executable,'-B',str(CODE/'phase_b.py'),mode,'--root',str(self.root)]
        if at is not None:args+=['--at',str(at)]
        for key,value in options.items():
            if value is not None:args+=['--'+key.replace('_','-'),str(value)]
        return args

    def runmode(self,mode,at=None,expected=0,**options):
        p=subprocess.run(self.command(mode,at,**options),capture_output=True,text=True,timeout=12,
                         env={**os.environ,'PYTHONDONTWRITEBYTECODE':'1','NO_PROXY':'127.0.0.1','no_proxy':'127.0.0.1'})
        OBS['process_runs'].append({'test':self.id().split('.')[-1],'mode':mode,'fault':options.get('fault'),
                                    'returncode':p.returncode,'expected':expected})
        if expected is not None:self.assertEqual(p.returncode,expected,p.stderr)
        return json.loads(p.stdout) if p.returncode==0 and p.stdout else p

    def receiver(self,login='900001',fault=None):
        p=subprocess.Popen(self.command('receiver',login=login,fault=fault),stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,
                           env={**os.environ,'PYTHONDONTWRITEBYTECODE':'1'})
        with selectors.DefaultSelector() as selector:
            selector.register(p.stdout,selectors.EVENT_READ)
            ready=selector.select(8)
        if not ready:
            p.kill();p.wait();self.fail('Receiver did not start')
        line=p.stdout.readline()
        if not line:self.fail(p.stderr.read())
        info=json.loads(line);self.assertEqual(info['host'],'127.0.0.1')
        self.receivers.append((p,info['port']))
        self.endpoint=f"http://127.0.0.1:{info['port']}/v1/events"
        return p

    def stop_receiver(self):
        p,_=self.receivers[-1]
        p.terminate();p.wait(timeout=4)

    def event(self,kind='transaction',payload=None,login='900001',stream='boot-1',seq=None):
        if seq is None:self.seq+=1;seq=self.seq
        return {'source':b.SOURCE,'server':b.SERVER,'login':login,'stream':stream,'seq':seq,'kind':kind,'payload':payload or {}}

    def queue(self,e,f=None,newline=True):
        with (self.root/'spool.jsonl').open('a') as out:out.write(j.dumps(e)+('\n' if newline else ''))
        if f is not None:self.authorize(e,f)

    def authorize(self,e,f):
        self.auth[b.identity(e)]=f
        (self.root/'authority.json').write_text(j.dumps(self.auth))

    def bridge(self,expected=0):
        return self.runmode('bridge',endpoint=self.endpoint,expected=expected)

    def worker(self,at=None,**kw):
        return self.runmode('worker',at=self.at if at is None else at,**kw)

    def sql(self,query,*params):
        db=sqlite3.connect(self.root/'test.sqlite');db.row_factory=sqlite3.Row
        try:return [dict(r) for r in db.execute(query,params)]
        finally:db.close()

    def count(self,table):return self.sql('SELECT count(*) AS n FROM '+table)[0]['n']

    def kinds(self):return [r['kind'] for r in self.sql('SELECT kind FROM events')]

    def baseline(self,positions=None,deals=None):
        self.receiver()
        e=self.event('snapshot',{'complete':True,'connected':True,'captured_utc':self.at,'positions':positions or []})
        self.queue(e,fixture(self.at,positions,deals));self.bridge();self.worker()
        return e

    def test_01_complete_spool_to_simulated_receipt(self):
        self.baseline()
        self.at+=1;e=self.event();self.queue(e,fixture(self.at,[position()],[deal()]))
        self.bridge()
        # Durable inbox ACK has not yet changed business state or emitted a trade.
        self.assertEqual(self.count('processed'),1);self.assertNotIn('position_first_observed',self.kinds())
        self.worker();self.assertIn('position_first_observed',self.kinds());self.assertIn('deal_observed',self.kinds())
        self.assertEqual(self.sql('SELECT needs_reconciliation FROM coverage')[0]['needs_reconciliation'],0)
        sent=self.runmode('notifier',at=self.at);self.assertIn('delivered',sent['states'])
        receipts=self.count('simulated_receipts');self.assertGreaterEqual(receipts,2)
        self.assertEqual(self.runmode('notifier',at=self.at)['stop'],'empty')
        self.assertEqual(self.count('simulated_receipts'),receipts)
        evidence={t:self.sql('SELECT * FROM '+t) for t in ('ingress','processed','accounts','records','events','outbox','coverage','simulated_receipts')}
        (OUTPUT/'example-ledger.json').write_text(json.dumps(evidence,indent=2)+'\n')
        (OUTPUT/'example-spool.jsonl').write_text((self.root/'spool.jsonl').read_text())
        (OUTPUT/'example-authority.json').write_text(j.dumps(self.auth)+'\n')

    def test_02_receiver_dies_after_commit_before_ack(self):
        p=self.receiver(fault='after_commit_before_ack')
        e=self.event();self.queue(e,fixture(self.at))
        self.assertNotEqual(self.bridge(expected=None).returncode,0)
        p.wait(timeout=3);self.assertEqual(p.returncode,70)
        self.assertEqual(self.count('ingress'),1);self.assertFalse((self.root/'cursor.json').exists())
        self.receiver();self.bridge();self.worker()
        self.assertEqual(self.count('ingress'),1);self.assertEqual(self.count('processed'),1)

    def test_03_worker_dies_before_atomic_commit(self):
        self.receiver();e=self.event();self.queue(e,fixture(self.at,[position()],[deal()]));self.bridge()
        self.worker(fault='before_commit',expected=71)
        for table in ('processed','events','outbox','accounts','records','imports'):self.assertEqual(self.count(table),0)
        self.worker();self.assertEqual(self.count('processed'),1);self.assertEqual(self.count('imports'),1)

    def test_04_worker_dies_after_commit_and_restarts(self):
        self.receiver();e=self.event();self.queue(e,fixture(self.at,[position()],[deal()]));self.bridge()
        self.worker(fault='after_commit',expected=72)
        before={t:self.count(t) for t in ('processed','events','outbox','records','imports')}
        self.worker();self.assertEqual(before,{t:self.count(t) for t in before})

    def prepare_alert(self):
        self.baseline();self.at+=1;e=self.event();self.queue(e,fixture(self.at,[position()],[deal()]));self.bridge();self.worker()

    def test_05_notifier_dies_before_provider_acceptance(self):
        self.prepare_alert();self.runmode('notifier',at=self.at,fault='before_accept',expected=73)
        self.assertEqual(self.count('simulated_receipts'),0)
        self.runmode('notifier',at=self.at)
        self.assertEqual(len(self.sql("SELECT * FROM outbox WHERE state='uncertain'")),1)
        self.assertFalse(self.sql("SELECT * FROM outbox WHERE state='attempting'"))
        receipts=self.count('simulated_receipts');self.runmode('notifier',at=self.at)
        self.assertEqual(receipts,self.count('simulated_receipts'))

    def test_06_notifier_dies_after_acceptance_before_receipt_commit(self):
        self.prepare_alert();self.runmode('notifier',at=self.at,fault='after_accept',expected=74)
        accepted=self.sql('SELECT event_id FROM simulated_receipts')[0]['event_id']
        self.runmode('notifier',at=self.at)
        self.assertEqual(self.sql('SELECT state FROM outbox WHERE event_id=?',accepted)[0]['state'],'uncertain')
        self.assertEqual(len(self.sql('SELECT * FROM simulated_receipts WHERE event_id=?',accepted)),1)
        self.assertEqual(self.sql('SELECT attempts FROM outbox WHERE event_id=?',accepted)[0]['attempts'],1)

    def test_07_cursor_lost_replay_has_no_duplicate_effects(self):
        self.prepare_alert();self.runmode('notifier',at=self.at)
        before={t:self.count(t) for t in ('ingress','processed','events','outbox','simulated_receipts','imports')}
        (self.root/'cursor.json').unlink();self.bridge();self.worker();self.runmode('notifier',at=self.at)
        self.assertEqual(before,{t:self.count(t) for t in before})

    def test_08_disconnect_does_not_fabricate_close_and_recovers(self):
        self.baseline([position()],[deal()]);self.at+=1;e=self.event('heartbeat',{'connected':False,'captured_utc':self.at})
        self.queue(e,fixture(self.at,connected=False));self.bridge();self.worker()
        self.assertIn('101',json.loads(self.sql('SELECT data FROM snapshots')[0]['data']))
        self.assertNotIn('position_no_longer_open',self.kinds())
        self.assertEqual(self.sql('SELECT needs_reconciliation FROM coverage')[0]['needs_reconciliation'],1)
        self.at+=1;e=self.event();self.queue(e,fixture(self.at,[position()],[deal()]));self.bridge();self.worker()
        self.assertIn('coverage_restored',self.kinds());self.assertEqual(self.kinds().count('coverage_disconnected'),1)
        self.assertEqual(self.sql('SELECT needs_reconciliation FROM coverage')[0]['needs_reconciliation'],0)

    def test_09_unavailable_authority_queue_recovery(self):
        self.receiver();e=self.event();f=fixture(self.at);f['available']=False;self.queue(e,f);self.bridge()
        self.assertEqual(self.worker()['pending'],1);self.assertEqual(self.count('processed'),0)
        self.authorize(e,fixture(self.at));self.assertEqual(self.worker()['processed'],1)
        self.assertEqual(self.count('reconcile_errors'),0)

    def test_10_stale_and_future_authority_preserve_last_snapshot(self):
        self.baseline([position()],[deal()])
        for capture in (self.at-31,self.at+31):
            e=self.event();self.queue(e,fixture(capture));self.bridge();self.worker()
            self.assertIn('101',json.loads(self.sql('SELECT data FROM snapshots')[0]['data']))
        self.assertNotIn('position_no_longer_open',self.kinds());self.assertEqual(self.count('processed'),1)

    def test_11_silence_is_stale_deduplicated_then_restored(self):
        self.baseline([position()],[deal()])
        self.runmode('health',at=self.at+31);self.runmode('health',at=self.at+32)
        self.assertEqual(self.kinds().count('coverage_stale'),1)
        self.assertEqual(self.sql('SELECT status FROM sources')[0]['status'],'stale')
        self.at+=33;e=self.event();self.queue(e,fixture(self.at,[position()],[deal()]));self.bridge();self.worker()
        self.assertEqual(self.sql('SELECT status FROM sources')[0]['status'],'connected')
        self.assertEqual(self.kinds().count('coverage_restored'),1)

    def test_12_account_change_rejected_without_explicit_pin(self):
        self.baseline([position()],[deal()]);self.at+=1;e=self.event(login='900002')
        self.queue(e,fixture(self.at,login='900002'));offset=(self.root/'cursor.json').read_text()
        self.assertNotEqual(self.bridge(expected=None).returncode,0)
        self.assertEqual((self.root/'cursor.json').read_text(),offset);self.assertEqual(self.count('ingress'),1)

    def test_13_explicit_synthetic_repin_separates_accounts(self):
        self.baseline([position()],[deal()]);self.stop_receiver();self.receiver(login='900002')
        self.at+=1;e=self.event(login='900002',stream='boot-2',seq=1)
        self.queue(e,fixture(self.at,[position()],[deal()],login='900002'));self.bridge();self.worker()
        self.assertEqual(self.count('accounts'),2);self.assertEqual(self.count('snapshots'),2)
        self.assertEqual(self.kinds().count('source_account_changed'),1)
        self.assertNotIn('position_no_longer_open',self.kinds());self.assertNotIn('position_first_observed',self.kinds())

    def test_14_account_changes_during_authority_read(self):
        self.baseline([position()],[deal()]);self.at+=1;e=self.event();f=fixture(self.at)
        f['bundle']['after']['account']['login']='900002';self.queue(e,f);self.bridge();self.worker()
        self.assertEqual(self.count('accounts'),1);self.assertEqual(self.count('processed'),1)
        self.assertNotIn('position_no_longer_open',self.kinds())

    def test_15_sequence_gap_and_overflow_reconcile_full_history(self):
        self.baseline();self.at+=1;e=self.event('overflow',{'dropped':2},seq=4)
        self.queue(e,fixture(self.at,[position()],[deal()]));self.bridge();self.worker()
        self.assertEqual(self.kinds().count('event_coverage_gap'),1)
        self.assertEqual(self.sql('SELECT needs_reconciliation FROM coverage')[0]['needs_reconciliation'],0)
        self.assertEqual(self.count('processed'),2)

    def test_16_partial_line_is_not_acknowledged_until_complete(self):
        self.receiver();e=self.event();self.queue(e,fixture(self.at),newline=False)
        self.assertEqual(self.bridge()['acknowledged_lines'],0)
        self.assertFalse((self.root/'cursor.json').exists())
        with (self.root/'spool.jsonl').open('a') as out:out.write('\n')
        self.assertEqual(self.bridge()['acknowledged_lines'],1);self.worker();self.assertEqual(self.count('processed'),1)

    def test_17_spool_corruption_retains_cursor_for_manual_recovery(self):
        self.baseline();cursor=(self.root/'cursor.json').read_text()
        with (self.root/'spool.jsonl').open('a') as out:out.write('{broken}\n')
        self.assertNotEqual(self.bridge(expected=None).returncode,0)
        self.assertEqual((self.root/'cursor.json').read_text(),cursor);self.assertEqual(self.count('ingress'),1)

    def test_18_spool_truncation_and_rotation_fail_closed(self):
        self.baseline();original=(self.root/'spool.jsonl').read_text()
        (self.root/'spool.jsonl').write_text('')
        self.assertNotEqual(self.bridge(expected=None).returncode,0)
        (self.root/'spool.jsonl').rename(self.root/'spool.saved')
        (self.root/'spool.jsonl').write_text(original)
        self.assertNotEqual(self.bridge(expected=None).returncode,0)
        self.assertEqual(self.count('ingress'),1)

    def post(self,e,headers=None):
        body=j.dumps(e).encode();secret=(self.root/'synthetic.key').read_bytes()
        headers=headers or er.signed_headers(secret,body,int(time.time()),secrets.token_hex(16))
        try:
            with build_opener(ProxyHandler({})).open(Request(self.endpoint,data=body,headers=headers),timeout=3) as r:return r.status
        except HTTPError as exc:return exc.code

    def test_19_hmac_nonce_and_expired_auth_rejected(self):
        self.receiver();e=self.event();body=j.dumps(e).encode();secret=(self.root/'synthetic.key').read_bytes()
        headers=er.signed_headers(secret,body,int(time.time()),secrets.token_hex(16))
        self.assertEqual(self.post(e,headers),200);self.assertEqual(self.post(e,headers),403)
        wrong=er.signed_headers(secrets.token_bytes(32),body,int(time.time()),secrets.token_hex(16));self.assertEqual(self.post(e,wrong),403)
        old=er.signed_headers(secret,body,int(time.time())-61,secrets.token_hex(16));self.assertEqual(self.post(e,old),403)
        self.assertEqual(self.count('ingress'),1)

    def test_20_same_sequence_different_content_is_rejected(self):
        self.baseline();e=self.event(seq=1,payload={'conflict':True});self.queue(e)
        self.assertNotEqual(self.bridge(expected=None).returncode,0);self.assertEqual(self.count('ingress'),1)

    def test_21_partial_history_and_balance_mismatch_roll_back(self):
        self.baseline();self.at+=1;e=self.event();f=fixture(self.at,[position()],[deal()])
        f['bundle']['get_trading_history_orders']['hasMore']=True;self.queue(e,f);self.bridge();self.worker()
        self.assertEqual(self.count('processed'),1)
        f['bundle']['get_trading_history_orders'].pop('hasMore');f['bundle']['after']['account']['balance']=9999
        self.authorize(e,f);self.worker();self.assertEqual(self.count('processed'),1)
        self.assertNotIn('position_first_observed',self.kinds());self.assertEqual(self.count('records'),1)
        self.authorize(e,fixture(self.at,[position()],[deal()]));self.worker();self.assertEqual(self.count('processed'),2)

    def test_22_new_stream_reuses_history_without_duplicate_trade(self):
        self.prepare_alert();before=self.count('events');self.at+=1
        e=self.event(stream='boot-after-restart',seq=1);self.queue(e,fixture(self.at,[position()],[deal()]));self.bridge();self.worker()
        self.assertEqual(self.count('events'),before);self.assertEqual(self.count('processed'),3)

    def test_23_transport_outage_and_bounded_backlog_recovery(self):
        self.baseline();self.stop_receiver();cursor=(self.root/'cursor.json').read_text()
        for i in range(20):
            self.at+=.01;e=self.event();self.queue(e,fixture(self.at))
        self.assertNotEqual(self.bridge(expected=None).returncode,0)
        self.assertEqual((self.root/'cursor.json').read_text(),cursor)
        self.receiver();self.assertEqual(self.bridge()['acknowledged_lines'],16)
        self.assertEqual(self.bridge()['acknowledged_lines'],4);self.worker()
        self.assertEqual(self.count('ingress'),21);self.assertEqual(self.count('processed'),21)

    def test_24_expired_queue_and_backward_clock_never_send(self):
        self.prepare_alert()
        self.assertEqual(self.runmode('notifier',at=self.at-1)['stop'],'clock_hold')
        self.assertEqual(self.count('simulated_receipts'),0)
        self.assertIn('expired',self.runmode('notifier',at=self.at+301)['states'])
        self.assertEqual(self.count('simulated_receipts'),0)

    def test_25_definite_failure_is_not_uncertain_or_delivered(self):
        self.prepare_alert();r=self.runmode('notifier',at=self.at,fault='definite')
        self.assertTrue(r['states']);self.assertEqual(set(r['states']),{'failed'})
        self.assertEqual(self.count('simulated_receipts'),0)
        self.assertEqual(self.runmode('notifier',at=self.at)['stop'],'empty')

    def test_26_worker_lock_excludes_second_owner(self):
        self.baseline()
        with (self.root/'worker.lock').open('a') as lock:
            fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
            self.assertNotEqual(self.runmode('worker',at=self.at,expected=None).returncode,0)
            self.assertNotEqual(self.runmode('notifier',at=self.at,expected=None).returncode,0)
        self.worker();self.assertEqual(self.count('processed'),1)

    def test_27_partial_close_and_full_close_are_history_backed(self):
        self.baseline([position()],[deal()]);self.at+=1;e=self.event()
        history=[deal(),deal('3','out',.4,4)]
        self.queue(e,fixture(self.at,[position(.6)],history));self.bridge();self.worker()
        self.assertIn('position_volume_changed',self.kinds());self.assertNotIn('position_no_longer_open',self.kinds())
        self.at+=1;e=self.event();history.append(deal('4','out',.6,6))
        self.queue(e,fixture(self.at,[],history));self.bridge();self.worker()
        row=json.loads(self.sql("SELECT data FROM records WHERE kind='position'")[0]['data'])
        self.assertEqual(row['status'],'closed');self.assertEqual(row['remaining_volume'],0)
        self.assertEqual(self.kinds().count('position_no_longer_open'),1)

    def test_28_historical_initial_import_does_not_alert_as_new(self):
        self.baseline([position()],[deal()]);r=self.runmode('notifier',at=self.at)
        self.assertEqual(r['states'],[]);self.assertEqual(self.count('simulated_receipts'),0)
        self.assertTrue(self.sql("SELECT * FROM outbox WHERE state='historical'"))

    def test_29_older_authority_cannot_regress_even_if_fresh(self):
        self.baseline([position()],[deal()]);self.at+=2;e=self.event()
        self.queue(e,fixture(self.at-3));self.bridge();self.worker()
        self.assertEqual(self.count('processed'),1);self.assertNotIn('position_no_longer_open',self.kinds())
        self.assertEqual(self.sql('SELECT reason FROM reconcile_errors')[0]['reason'],'AUTHORITY_REGRESSION')

    def test_30_incomplete_snapshot_is_atomic_at_ingress(self):
        self.receiver();e=self.event('snapshot',{'connected':True,'complete':False,'captured_utc':self.at,'positions':[]})
        self.queue(e,fixture(self.at));self.assertNotEqual(self.bridge(expected=None).returncode,0)
        self.assertEqual(self.count('ingress'),0);self.assertEqual(self.count('events'),0)


def main():
    started=datetime.now(timezone.utc).isoformat();begin=time.monotonic()
    before={'project':hashes(ORIGINAL),'phase_c_documentation':hashes(VAULT)}
    with (OUTPUT/'tests.txt').open('w') as log:
        result=unittest.TextTestRunner(stream=log,verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(Pipeline))
    after={'project':hashes(ORIGINAL),'phase_c_documentation':hashes(VAULT)}
    changes={area:sorted(k for k in set(before[area])|set(after[area]) if before[area].get(k)!=after[area].get(k)) for area in before}
    removed=all(not Path(p).exists() for p in OBS['temporary_roots'])
    closed=bool(OBS['listeners']) and all(r['closed'] for r in OBS['listeners'])
    accepted=result.wasSuccessful() and result.testsRun==30 and not result.skipped and not any(changes.values()) and removed and closed
    summary={'phase':'B','accepted':accepted,'started_utc':started,'finished_utc':datetime.now(timezone.utc).isoformat(),
             'duration_seconds':round(time.monotonic()-begin,3),'tests_run':result.testsRun,'failures':len(result.failures),
             'errors':len(result.errors),'skipped':len(result.skipped),'protected_file_counts':{k:len(v) for k,v in before.items()},
             'changed_protected_files':changes,'temporary_roots_removed':removed,'listeners_closed':closed,
             'real_mt5_calls':0,'real_notifications':0,'services_installed':0,'sqx_interactions':0,
             'limitations':['Python audit guards, not host-wide OS isolation','Synthetic history provider, not live reconciliation',
                            'No MQL5 compilation or terminal execution','No real provider or human receipt','No crash-before-disk guarantee for EA RAM queue'],
             'observations':OBS}
    (OUTPUT/'verification.json').write_text(json.dumps(summary,indent=2)+'\n')
    (OUTPUT/'protected-hashes.json').write_text(json.dumps({'before':before,'after':after},indent=2)+'\n')
    print(json.dumps({k:v for k,v in summary.items() if k!='observations'},indent=2))
    return 0 if accepted else 1


if __name__=='__main__':raise SystemExit(main())
