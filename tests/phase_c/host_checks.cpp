// Executes candidate function bodies against deliberately limited host doubles.
#include "mt5_host_stubs.hpp"
#include "candidate-under-test.hpp"
int checks=0, failures=0;
void require(bool ok,const char *message) { if(!ok) throw std::runtime_error(message); }
void reset() {
 m=MockState{};
 SourceId="synthetic-c"; ExpectedLogin=101; ExpectedServer="SYNTHETIC-DEMO";
 active=false; blocked=true; count=0; head=0; dropped=0;
}
void init() { require(OnInit()==INIT_SUCCEEDED,"init should succeed"); }
void event() { OnTradeTransaction(MqlTradeTransaction{},MqlTradeRequest{},MqlTradeResult{}); }
void test(const string &name,const std::function<void()> &fn) {
 reset(); checks++;
 try { fn(); std::cout<<"PASS "<<name<<"\n"; }
 catch(const std::exception &e) { failures++; std::cout<<"FAIL "<<name<<": "<<e.what()<<"\n"; }
}
int main() {
 test("init_demo_no_io",[]{init(); require(active&&m.timer&&m.writes==0&&m.opens==0,"state");});
 test("default_login_refused",[]{ExpectedLogin=0; require(OnInit()==INIT_PARAMETERS_INCORRECT,"default login");});
 test("empty_server_refused",[]{ExpectedServer=""; require(OnInit()==INIT_PARAMETERS_INCORRECT,"server");});
 for(string s: std::vector<string>{"","../bad","bad name",string(49,'x'),"á"})
   test("invalid_source_"+s,[s]{SourceId=s; require(OnInit()==INIT_PARAMETERS_INCORRECT,"source");});
 for(long mode: {long(ACCOUNT_TRADE_MODE_REAL),long(ACCOUNT_TRADE_MODE_CONTEST)})
   test("init_nondemo_"+std::to_string(mode),[mode]{m.mode=mode; require(OnInit()==INIT_FAILED&&m.opens==0,"mode");});
 test("init_disconnected",[]{m.connected=false; require(OnInit()==INIT_FAILED,"connected");});
 test("init_wrong_login",[]{m.login=202; require(OnInit()==INIT_FAILED,"identity");});
 test("init_wrong_server",[]{m.server="OTHER"; require(OnInit()==INIT_FAILED,"server");});
 test("init_spool_collision",[]{m.collision=true; require(OnInit()==INIT_FAILED&&!m.timer,"collision");});
 test("init_timer_failure",[]{m.timer_fail=true; require(OnInit()==INIT_FAILED&&!active,"timer");});
 test("init_switch_at_timer",[]{m.timer_hook=[]{m.login=202;};
   require(OnInit()==INIT_FAILED&&!active&&!m.timer,"timer identity fence");});
 test("init_torn_sample",[]{m.read_hook=[]{if(m.reads==5)m.login=202;}; require(OnInit()==INIT_FAILED,"torn read");});
 test("callback_queue_no_io",[]{init(); event(); require(count==1&&m.writes==0&&m.opens==0,"callback");});
 test("callback_invalid_result_ignored",[]{init(); event(); require(retcodes[head]==0,"retcode");});
 test("callback_request_retcode",[]{init(); MqlTradeTransaction t; t.type=TRADE_TRANSACTION_REQUEST;
   OnTradeTransaction(t,MqlTradeRequest{},MqlTradeResult{}); require(retcodes[head]==123,"retcode");});
 for(int fault=0;fault<4;fault++) {
   auto change=[fault]{
     if(fault==0)m.connected=false;
     if(fault==1)m.mode=ACCOUNT_TRADE_MODE_REAL;
     if(fault==2)m.login=202;
     if(fault==3)m.server="OTHER";
   };
   test("callback_guard_"+std::to_string(fault),[change]{init(); event(); change(); event();
     require(blocked&&count==0&&m.writes==0&&!m.timer,"callback guard");});
   test("timer_guard_"+std::to_string(fault),[change]{init(); event(); change(); OnTimer();
     require(blocked&&count==0&&m.opens==0&&!m.timer,"timer guard");});
   test("append_guard_"+std::to_string(fault),[change]{init(); change();
     require(!Append("{}")&&m.opens==0,"append guard");});
 }
 test("disconnect_latches",[]{init(); m.connected=false; OnTimer(); m.connected=true; event(); OnTimer();
   require(blocked&&count==0&&m.opens==0,"no automatic resume in instance");});
 test("switch_during_callback",[]{init(); m.reads=0;
   m.read_hook=[]{if(m.reads==9)m.login=202;}; event();
   require(blocked&&count==0&&m.opens==0,"post capture fence");});
 test("switch_during_seek",[]{init(); m.seek_hook=[]{m.mode=ACCOUNT_TRADE_MODE_REAL;};
   require(!Append("{}")&&blocked&&m.writes==0&&m.closes==1,"prewrite fence");});
 test("snapshot_switch_mid_enumeration",[]{init(); m.positions=1;
   m.position_hook=[]{m.login=202;}; Snapshot(); require(blocked&&m.writes==0,"snapshot fence");});
 test("snapshot_failed_ticket",[]{init(); m.positions=1; m.position_fail=true; Snapshot();
   require(m.writes==0,"partial snapshot suppressed");});
 test("empty_snapshot",[]{init(); Snapshot(); require(m.output.size()==1,"snapshot count");
   require(m.output[0].find("\"positions\":[]")!=string::npos,"empty positions");});
 test("pinned_envelope",[]{init(); string s=Envelope("test","{}",1);
   require(s.find("\"login\":\"101\"")!=string::npos&&s.find("SYNTHETIC-DEMO")!=string::npos,"pinned");});
 test("json_control_escaping",[]{require(Q(string("a\n\t")+char(1)+"\"\\")=="\"a\\u000a\\u0009\\u0001\\\"\\\\\"","escape");});
 test("utf8_binary_full_write",[]{init(); require(Append("á✓"),"append");
   require(m.output[0]=="á✓\n"&&m.output[0].find('\0')==string::npos,"exact host bytes");});
 for(int fault=0;fault<6;fault++) {
   test("io_fault_blocks_"+std::to_string(fault),[fault]{
     init();
     if(fault==0)m.open_fail=true;
     if(fault==1)m.seek_fail=true;
     if(fault==2)m.short_write=true;
     if(fault==3)m.flush_fail=true;
     if(fault==4)m.close_fail=true;
     if(fault==5)m.conversion_fail=true;
     require(!Append("{}")&&blocked&&io_failed,"io block");
     int writes=m.writes,opens=m.opens; OnTimer(); event(); Append("{}");
     require(m.writes==writes&&m.opens==opens&&count==0,"no retry after uncertain write");
   });
 }
 for(int reason=0;reason<=9;reason++)
   test("deinit_no_flush_"+std::to_string(reason),[reason]{
     init(); event(); m.mode=ACCOUNT_TRADE_MODE_REAL; m.login=202; OnDeinit(reason);
     require(m.opens==0&&m.writes==0&&count==0&&blocked&&!m.timer,"deinit");
   });
 test("account_reinit_does_not_rebind",[]{init(); event(); OnDeinit(6); m.login=202;
   require(OnInit()==INIT_FAILED&&m.opens==0,"account reinit");});
 test("real_reinit_rejected",[]{init(); OnDeinit(6); m.mode=ACCOUNT_TRADE_MODE_REAL;
   require(OnInit()==INIT_FAILED&&m.writes==0,"real reinit");});
 test("same_identity_new_stream",[]{init(); string old=stream_id; event(); OnDeinit(6); init();
   require(stream_id!=old&&count==0,"isolated new stream");});
 test("bounded_queue_and_batch",[]{init(); for(int i=0;i<CAPACITY+1;i++)event();
   require(count==CAPACITY&&dropped==1,"capacity"); OnTimer();
   require(count==CAPACITY-BATCH_SIZE&&m.writes==BATCH_SIZE,"batch limit");});
 test("overflow_after_backlog",[]{init(); for(int i=0;i<CAPACITY+1;i++)event();
   for(int i=0;i<8;i++)OnTimer();
   require(count==0&&dropped==0&&m.writes==CAPACITY+2,"overflow and snapshot");
   require(m.output[CAPACITY].find("\"kind\":\"overflow\"")!=string::npos,"overflow marker");});
 std::cout<<"RESULT checks="<<checks<<" failures="<<failures<<"\n";
 return failures?1:0;
}
