import copy
import json
from pathlib import Path
import tempfile
import unittest
import journal as j

class JournalTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.db=j.open_db(Path(self.tmp.name)/'test.sqlite')
        self.bundle=json.loads((j.ROOT/'evidence/manjaro-data.json').read_text())
    def tearDown(self):
        self.db.close();self.tmp.cleanup()
    def count(self,table):return self.db.execute('SELECT COUNT(*) FROM '+table).fetchone()[0]
    def test_live_sample_and_balance(self):
        r=j.ingest(self.db,self.bundle);self.assertEqual(r['closed_imported'],10);self.assertEqual(r['balance_difference'],0)
        self.assertEqual(self.count('records'),51)
    def test_vps_swap_alias_reconciles(self):
        r=j.ingest(self.db,json.loads((j.ROOT/'evidence/vps-data.json').read_text()))
        self.assertEqual(r['balance_difference'],0);self.assertEqual(r['closed_imported'],24)
    def test_idempotent_import(self):
        j.ingest(self.db,self.bundle);counts=(self.count('records'),self.count('events'),self.count('outbox'))
        r=j.ingest(self.db,self.bundle);self.assertEqual(r['changed_records'],0)
        self.assertEqual(counts,(self.count('records'),self.count('events'),self.count('outbox')))
    def test_two_connectors_same_account(self):
        j.ingest(self.db,self.bundle);self.bundle['source']='same-account-other-mcp';j.ingest(self.db,self.bundle)
        self.assertEqual(self.count('accounts'),1);self.assertEqual(self.count('sources'),2);self.assertEqual(self.count('records'),51)
    def test_account_switch_during_read_is_atomic(self):
        self.bundle['after']['account']['login']='99999999'
        with self.assertRaisesRegex(ValueError,'ACCOUNT_CHANGED'):j.ingest(self.db,self.bundle)
        self.assertEqual(self.count('records'),0)
    def test_disconnection_keeps_last_verified_snapshot(self):
        key=j.account_key(self.bundle['after']['account']);at='2026-10-17T20:00:00Z'
        with self.db:j.observe_snapshot(self.db,key,[{'ticket':'1','symbol':'EURUSD','type':'buy','volume':1,'sl':1}],at)
        self.bundle['before']['terminal']['server_connected']=False
        j.ingest(self.db,self.bundle);j.ingest(self.db,self.bundle)
        snapshot=json.loads(self.db.execute('SELECT data FROM snapshots').fetchone()[0]);self.assertIn('1',snapshot)
        self.assertEqual(self.count('events'),1)
    def test_omitted_costs_not_zero(self):
        d=self.bundle['get_trading_history_orders']['deals'][1:3]
        r=j.reconstruct(d[0]['position_id'],d)
        self.assertIsNone(r['net_pnl']);self.assertIn('swap',r['missing_cost_fields']);self.assertIsNone(r['initial_sl'])
    def test_partial_close_and_multiple_entries(self):
        ds=[{'deal_id':str(n),'position_id':'1','order_id':str(n),'symbol':'EURUSD','action':action,'entry':entry,'volume':vol,'price_open':price,'open_time':f'2026-01-01T00:00:0{n}','profit':p} for n,action,entry,vol,price,p in [(1,'buy','in',1,1.1,0),(2,'buy','in',1,1.2,0),(3,'sell','out',0.5,1.3,10),(4,'sell','out',1.5,1.4,20)]]
        r=j.reconstruct('1',ds);self.assertEqual(r['status'],'closed');self.assertEqual(len(r['executions']),4)
        self.assertEqual(r['partial_closes'][0]['remaining'],1.5);self.assertAlmostEqual(r['entry_price'],1.15)
    def test_reversal_not_silently_partial(self):
        ds=copy.deepcopy(self.bundle['get_trading_history_orders']['deals'][1:3]);ds[1]['entry']='inout'
        r=j.reconstruct(ds[0]['position_id'],ds);self.assertEqual(r['status'],'needs_review');self.assertEqual(len(r['executions']),2)
    def test_conflicting_duplicate_rejected(self):
        ds=self.bundle['get_trading_history_orders']['deals'];d=copy.deepcopy(ds[1]);d['profit']=99;ds.append(d)
        with self.assertRaisesRegex(ValueError,'CONFLICTING_DUPLICATE'):j.ingest(self.db,self.bundle)
        self.assertEqual(self.count('records'),0)
    def test_incomplete_response_rejected(self):
        self.bundle['get_trading_history_orders']['hasMore']=True
        with self.assertRaisesRegex(ValueError,'INCOMPLETE'):j.ingest(self.db,self.bundle)
    def test_pathlike_identifier_rejected(self):
        self.bundle['get_trading_history_positions']['positions'][0]['position_id']='../../outside'
        with self.assertRaisesRegex(ValueError,'MISSING_position_id'):j.ingest(self.db,self.bundle)
    def test_same_timestamp_dd_not_invented_order(self):
        ds=[{'deal_id':'1','action':'buy','profit':-100,'open_time':'2026-01-01T01:00:00'},
            {'deal_id':'2','action':'sell','profit':100,'open_time':'2026-01-01T01:00:00'}]
        self.assertEqual(j.drawdowns(ds)['max_realized_reported_drawdown'],0)
    def test_correction_retains_revision(self):
        j.ingest(self.db,self.bundle);self.bundle['get_trading_history_orders']['orders'][0]['price_order']=1.5
        j.ingest(self.db,self.bundle);self.assertEqual(self.count('revisions'),1)
    def test_missing_link_rejected(self):
        self.bundle['get_trading_history_orders']['orders'].pop(0)
        with self.assertRaisesRegex(ValueError,'MISSING_EXECUTION_ORDER'):j.ingest(self.db,self.bundle)
    def test_drawdown_ignores_deposit_and_preserves_entry_commission(self):
        ds=[{'action':'balance','profit':50000,'open_time':'2026-01-01T00:00:00','deal_id':'1'},
            {'action':'buy','profit':0,'commission':-10,'open_time':'2026-01-01T01:00:00','deal_id':'2'},
            {'action':'sell','profit':100,'open_time':'2026-01-01T02:00:00','deal_id':'3'},
            {'action':'sell','profit':-50,'open_time':'2026-01-02T01:00:00','deal_id':'4'}]
        r=j.drawdowns(ds);self.assertEqual(r['max_realized_reported_drawdown'],50)
        self.assertEqual(r['daily']['2026-01-01']['peak_to_trough_realized'],10)
    def test_price_only_not_trade_event_and_sl_change_is(self):
        p={'ticket':'1','symbol':'EURUSD','type':'buy','volume':1,'sl':1,'tp':2,'price_current':1.5}
        with self.db:
            j.observe_snapshot(self.db,'k',[p],'a');p['price_current']=1.6;j.observe_snapshot(self.db,'k',[p],'b')
        self.assertEqual(self.count('events'),0)
        with self.db:p['sl']=0;j.observe_snapshot(self.db,'k',[p],'c')
        self.assertEqual(self.count('events'),2)
    def test_historical_outbox_not_pending(self):
        j.ingest(self.db,self.bundle)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM outbox WHERE state='pending'").fetchone()[0],0)
    def test_crash_recovery_not_resend(self):
        with self.db:
            j.emit(self.db,'k','test',{},'a');self.db.execute("UPDATE outbox SET state='attempting'")
        self.db.close();self.db=j.open_db(Path(self.tmp.name)/'test.sqlite')
        with self.db:self.db.execute("UPDATE outbox SET state='uncertain' WHERE state='attempting'")
        self.assertEqual(self.db.execute('SELECT state FROM outbox').fetchone()[0],'uncertain')

if __name__=='__main__':unittest.main()
