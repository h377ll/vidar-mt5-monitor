import json
from pathlib import Path
import secrets
import tempfile
import threading
import time
import unittest
import event_receiver as er
import journal as j
import spool_bridge as bridge

class Events(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.path=Path(self.tmp.name)/'events.sqlite'
        self.db=j.open_db(self.path);self.key=secrets.token_bytes(32)
        self.creds={'demo-source':(self.key,'SIM-DEMO','42')};self.rx=er.Receiver(self.db,self.creds);self.now=1800000000
    def tearDown(self):self.db.close();self.tmp.cleanup()
    def event(self,seq=1,kind='transaction',payload=None,**kwargs):
        return dict(source='demo-source',stream='boot-1',server='SIM-DEMO',login='42',seq=seq,kind=kind,payload=payload or {},**kwargs)
    def send(self,e=None,body=None,headers=None):
        body=body or j.dumps(e or self.event()).encode()
        headers=headers or er.signed_headers(self.key,body,self.now,secrets.token_hex(16))
        return self.rx.accept(body,headers,self.now)
    def snapshot(self,seq,positions,connected=True,captured=None):
        return self.event(seq,'snapshot',{'connected':connected,'complete':True,'captured_utc':self.now if captured is None else captured,'positions':positions})
    def pos(self,sl=1.0,volume=1.0):return {'position_id':'123','symbol':'EURUSD','type':'buy','volume':volume,'sl':sl,'tp':2.0}
    def count(self,table):return self.db.execute('SELECT COUNT(*) FROM '+table).fetchone()[0]
    def test_store_duplicate_restart(self):
        self.send();self.db.close();self.db=j.open_db(self.path);self.rx=er.Receiver(self.db,self.creds)
        self.assertEqual(self.send()['status'],'duplicate');self.assertEqual(self.count('ingress'),1)
    def test_tamper(self):
        body=j.dumps(self.event()).encode();headers=er.signed_headers(self.key,body,self.now,secrets.token_hex(16))
        with self.assertRaises(PermissionError):self.send(body=body.replace(b'transaction',b'heartbeat'),headers=headers)
        self.assertEqual(self.count('ingress'),0)
    def test_replay_nonce(self):
        body=j.dumps(self.event()).encode();headers=er.signed_headers(self.key,body,self.now,secrets.token_hex(16));self.send(body=body,headers=headers)
        with self.assertRaises(PermissionError):self.send(body=body,headers=headers)
    def test_stale_auth(self):
        body=j.dumps(self.event()).encode()
        with self.assertRaises(PermissionError):self.send(body=body,headers=er.signed_headers(self.key,body,self.now-61,secrets.token_hex(16)))
    def test_wrong_account(self):
        e=self.event();e['login']='43'
        with self.assertRaises(PermissionError):self.send(e)
    def test_conflict(self):
        self.send()
        with self.assertRaises(ValueError):self.send(self.event(payload={'different':True}))
    def test_gap_requires_reconciliation(self):
        self.send();r=self.send(self.event(3));self.assertTrue(r['gap']);self.assertFalse(r['reconciled'])
        self.send(self.event(2));self.assertEqual(self.count('ingress'),3)
        self.assertTrue(self.rx.status('demo-source',self.now)['needs_reconciliation'])
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM events WHERE kind='event_coverage_gap'").fetchone()[0],1)
    def test_disconnect_not_close(self):
        self.send(self.snapshot(1,[self.pos()]));self.now+=1;self.send(self.snapshot(2,[],False))
        self.assertIn('123',json.loads(self.db.execute('SELECT data FROM snapshots').fetchone()[0]))
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM events WHERE kind='position_no_longer_open'").fetchone()[0],0)
    def test_removed_sl_and_partial_volume(self):
        self.send(self.snapshot(1,[self.pos()]));self.now+=1;self.send(self.snapshot(2,[self.pos(0,0.4)]))
        kinds={r[0] for r in self.db.execute('SELECT kind FROM events')}
        self.assertIn('sl_missing',kinds);self.assertIn('position_volume_changed',kinds)
        self.assertNotIn('position_no_longer_open',kinds)
    def test_old_snapshot_cannot_close_or_disconnect(self):
        self.send(self.snapshot(1,[self.pos()]));self.now+=1;self.send(self.snapshot(2,[],False,captured=self.now-100))
        self.assertTrue(self.rx.status('demo-source',self.now)['connected_reported'])
        self.assertIn('123',json.loads(self.db.execute('SELECT data FROM snapshots').fetchone()[0]))
    def test_incomplete_snapshot_is_atomic(self):
        e=self.snapshot(1,[]);e['payload']['complete']=False
        with self.assertRaises(ValueError):self.send(e)
        self.assertEqual(self.count('ingress'),0);self.assertEqual(self.count('outbox'),0)
    def test_nonfinite_json(self):
        body=b'{"source":"demo-source","bad":NaN}'
        with self.assertRaises(ValueError):self.send(body=body)
    def test_transport_silence(self):
        self.send(self.snapshot(1,[self.pos()]));self.assertEqual(self.rx.status('demo-source',self.now+31)['coverage'],'stale_transport')
        self.assertFalse(self.rx.status('demo-source',self.now+31)['snapshot_fresh'])
    def test_two_sources_one_account_cannot_regress_snapshot(self):
        self.creds['second-source']=(self.key,'SIM-DEMO','42')
        self.send(self.snapshot(1,[self.pos()]))
        e=self.snapshot(1,[],captured=self.now-1);e['source']='second-source';self.send(e)
        self.assertEqual(self.count('snapshots'),1)
        self.assertIn('123',json.loads(self.db.execute('SELECT data FROM snapshots').fetchone()[0]))
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM events WHERE kind='position_no_longer_open'").fetchone()[0],0)
    def test_http_spool_restart_and_partial_line(self):
        # An actual temporary loopback listener, always shutdown; no external messages.
        self.db.close();server=er.make_server(self.path,self.creds);thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
        spool=Path(self.tmp.name)/'spool.jsonl';cursor=Path(self.tmp.name)/'cursor.json'
        spool.write_text(j.dumps(self.event())+'\n'+j.dumps(self.event(2)))
        try:
            url=f'http://127.0.0.1:{server.server_port}/v1/events'
            self.assertEqual(bridge.deliver(spool,cursor,url,self.key)['acknowledged_lines'],1)
            self.assertEqual(bridge.deliver(spool,cursor,url,self.key)['acknowledged_lines'],0)
            with spool.open('a') as f:f.write('\n')
            self.assertEqual(bridge.deliver(spool,cursor,url,self.key)['acknowledged_lines'],1)
            # Simulate a lost bridge cursor: durable receiver dedup handles retransmission.
            cursor.unlink();self.assertEqual(bridge.deliver(spool,cursor,url,self.key)['acknowledged_lines'],2)
        finally:server.shutdown();server.server_close();thread.join();self.db=j.open_db(self.path)
        self.assertEqual(self.count('ingress'),2)
    def test_public_endpoint_rejected(self):
        with self.assertRaises(ValueError):bridge.deliver('/not/read','/not/written','http://0.0.0.0:123/v1/events',self.key)

if __name__=='__main__':unittest.main()
