#!/usr/bin/env python3
"""Independent offline safety scenarios; NEVER connects to MT5."""
from copy import deepcopy
import argparse,json,os,sqlite3,sys
from pathlib import Path
os.umask(0o077)
sys.dont_write_bytecode=True
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
from engine.contracts import canonical_json,input_digest
from engine.shadow_management import simulate_once,transition

def fixture():
 s={'environment':'ISOLATED_TEST','account_type':'demo','account':['SYNTHETIC-TEST','900001'],'connected':True,'positions_complete':True,'global_complete':True,'specs_verified':True,'firm_rules_verified':True,'action_window_clear':True,'observed_at':'2026-11-22T20:00:00Z',
 'account_risk_money':'100','global_risk_money':'200','account_drawdown_money':'10','global_drawdown_money':'20',
 'positions':[{'position_id':'6001','symbol':'SYNTH','side':'buy','volume':'1','sl':'90','tp':'130','entry_price':'100'}],
 'specs':{'SYNTH':{'tick_size':'0.01','volume_step':'0.1','volume_min':'0.1','bid':'110','ask':'110.02','stops_distance':'0.1','freeze_distance':'0.2'}}}
 p={'purpose':'SYNTHETIC_SIMULATION_ONLY','account':s['account'],'firm_rules_digest':'SYNTHETIC-NOT-QT','max_age_seconds':'5','break_even_offset_price':'0',**{'max_'+k:'1000' for k in ('account_risk_money','global_risk_money','account_drawdown_money','global_drawdown_money')}}
 r={'request_id':'synthetic-1','account':s['account'],'snapshot_digest':input_digest(s),'position_id':'6001','action':'break_even','reason':'SYNTHETIC safety scenario, not live trading rules','new_sl':'100'}
 return deepcopy(s),deepcopy(r),deepcopy(p)

def main():
 parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--output-dir',required=True,type=Path);out=parser.parse_args().output_dir.resolve()
 if out.exists() or out.is_relative_to(ROOT):parser.error('new output outside repository required')
 out.mkdir(parents=True,mode=0o700)
 def guard(event,args):
  if event in ('socket.connect','socket.bind','socket.getaddrinfo','subprocess.Popen','os.system'):raise PermissionError('EXTERNAL_EFFECT_BLOCKED')
  if event=='open' and not isinstance(args[0],int) and args[2] & (os.O_WRONLY|os.O_RDWR|os.O_CREAT|os.O_TRUNC|os.O_APPEND):
   if not Path(args[0]).resolve().is_relative_to(out):raise PermissionError('WRITE_OUTSIDE_TEST_OUTPUT')
 sys.addaudithook(guard);checks=[]
 def check(name,mutate=None,expect='BLOCKED',refresh=True):
  s,r,p=fixture()
  if mutate:mutate(s,r,p)
  if refresh:r['snapshot_digest']=input_digest(s)
  before=canonical_json(s);result=transition(s,r,p,now='2026-11-22T20:00:02Z')
  assert result['status']==expect,(name,result['reason_code'])
  assert result['execution_allowed'] is False and result['broker_result']=='NOT_SENT'
  assert canonical_json(s)==before
  if expect=='BLOCKED':assert result['simulated_after']==s
  checks.append({'name':name,'passed':True,'result':result['reason_code']});return result
 good=check('buy break-even',expect='SIMULATED');assert good['simulated_after']['positions'][0]['sl']=='100' and good['simulated_after']['positions'][0]['tp']=='130'
 def partial(s,r,p):r.pop('new_sl');r.update(action='partial_close',close_volume='0.4')
 result=check('partial close',partial,expect='SIMULATED');assert result['simulated_after']['positions'][0]['volume']=='0.6'
 def full(s,r,p):r.pop('new_sl');r['action']='full_close'
 result=check('full close',full,expect='SIMULATED');assert result['simulated_after']['positions'][0]['volume']=='0'
 for k,v in [('account_type','real'),('environment','PROP_FUNDED'),('connected',False),('global_complete',False),('positions_complete',False),('specs_verified',False),('firm_rules_verified',False),('action_window_clear',False)]:check('block '+k,lambda s,r,p,k=k,v=v:s.update({k:v}))
 check('prop demo is not sandbox',lambda s,r,p:s.update(environment='PROP_FUNDED',account_type='demo'))
 check('account mismatch',lambda s,r,p:r.update(account=['OTHER','1']))
 check('position absent',lambda s,r,p:r.update(position_id='999'))
 check('old data',lambda s,r,p:s.update(observed_at='2026-11-22T19:00:00Z'))
 check('future data',lambda s,r,p:s.update(observed_at='2026-11-23T19:00:00Z'))
 check('stale digest',lambda s,r,p:r.update(snapshot_digest='bad'),refresh=False)
 for name in ('account_risk_money','global_risk_money','account_drawdown_money','global_drawdown_money'):
  check('exceeded '+name,lambda s,r,p,name=name:s.update({name:'1001'}))
  check('unknown '+name,lambda s,r,p,name=name:s.update({name:'UNKNOWN'}))
 check('opening forbidden',lambda s,r,p:r.update(action='open'))
 check('exposure increase forbidden',lambda s,r,p:r.update(volume='2'))
 check('stop removal forbidden',lambda s,r,p:r.update(new_sl='0'))
 check('stop widening forbidden',lambda s,r,p:(r.update(new_sl='80'),p.update(break_even_offset_price='-20')))
 check('already protected beyond BE',lambda s,r,p:s['positions'][0].update(sl='105'))
 check('missing stop',lambda s,r,p:s['positions'][0].update(sl='0'))
 check('invalid tick',lambda s,r,p:s['specs']['SYNTH'].update(tick_size='0'))
 check('unknown step',lambda s,r,p:s['specs']['SYNTH'].update(volume_step='UNKNOWN'))
 check('freeze distance',lambda s,r,p:s['specs']['SYNTH'].update(freeze_distance='15'))
 check('off tick',lambda s,r,p:(p.update(break_even_offset_price='0.001'),r.update(new_sl='100.001')))
 check('close too much',lambda s,r,p:(partial(s,r,p),r.update(close_volume='2')))
 check('close off step',lambda s,r,p:(partial(s,r,p),r.update(close_volume='0.15')))
 check('dust remainder',lambda s,r,p:(partial(s,r,p),s['specs']['SYNTH'].update(volume_min='0.3'),r.update(close_volume='0.8')))
 check('extra TP change',lambda s,r,p:r.update(new_tp='120'))
 check('NaN',lambda s,r,p:s.update(account_risk_money='NaN'))
 check('boolean numeric',lambda s,r,p:s.update(account_risk_money=True))
 check('missing input',lambda s,r,p:s.pop('specs'))
 def sell(s,r,p):s['positions'][0].update(side='sell',sl='120');s['specs']['SYNTH'].update(bid='89.98',ask='90')
 check('sell break-even',sell,expect='SIMULATED')
 s,r,p=fixture();dbpath=out/'synthetic-shadow.sqlite';db=sqlite3.connect(dbpath);first=simulate_once(db,s,r,p,now='2026-11-22T20:00:02Z');db.close()
 db=sqlite3.connect(dbpath);second=simulate_once(db,s,r,p,now='2026-11-22T20:01:00Z');assert first==second and db.execute('SELECT count(*) FROM shadow_audit').fetchone()[0]==1;checks.append({'name':'idempotency after restart','passed':True})
 r['new_sl']='101'
 try:simulate_once(db,s,r,p,now='2026-11-22T20:00:02Z')
 except ValueError as exc:assert str(exc)=='IDEMPOTENCY_CONFLICT'
 else:raise AssertionError('id collision accepted')
 checks.append({'name':'idempotency key conflict','passed':True});assert db.execute('PRAGMA quick_check').fetchone()[0]=='ok';db.close()
 (out/'sample-audit.json').write_bytes(canonical_json(first)+b'\n')
 result={'accepted':True,'synthetic':True,'passed':len(checks),'failed':0,'checks':checks,'execution_allowed':False,'broker_requests':0,'qt_rules_approved':False};(out/'verification.json').write_bytes(canonical_json(result)+b'\n');print(json.dumps({k:v for k,v in result.items() if k!='checks'}));return 0
if __name__=='__main__':raise SystemExit(main())
