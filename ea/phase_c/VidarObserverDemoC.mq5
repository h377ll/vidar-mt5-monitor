// Experimental Phase C candidate. NOT compiled by MetaEditor or installed.
// No trading, network, DLL, imports or external includes.
// Expected identity must be supplied locally; never embed an account in source.
#property strict
#property version "0.11"
input string SourceId="demo-c";
input long ExpectedLogin=0;
input string ExpectedServer="";
#define CAPACITY 1024
#define BATCH_SIZE 128
MqlTradeTransaction pending[CAPACITY];
ulong numbers[CAPACITY];
uint retcodes[CAPACITY];
int head=0, count=0;
ulong sequence=0, dropped=0;
long bound_login=0;
string bound_server="", stream_id="", spool_name="";
bool blocked=true, active=false, io_failed=false;
int block_reason=0;

string U(ulong n) { return StringFormat("%I64u",n); }
string Q(string text) {
   string out="\"";
   for(int i=0;i<StringLen(text);i++) {
      ushort c=StringGetCharacter(text,i);
      if(c==34) out+="\\\"";
      else if(c==92) out+="\\\\";
      else if(c<32) out+=StringFormat("\\u%04x",(uint)c);
      else out+=StringSubstr(text,i,1);
   }
   return out+"\"";
}
void Block(int reason) {
   if(!blocked) Print("Vidar C blocked; reason=",reason,
                     ". Queue discarded; reconciliation required.");
   blocked=true; active=false; block_reason=reason;
   head=0; count=0; dropped=0;
   EventKillTimer();
}
bool Guard() {
   if(blocked) return false;
   // Repeat reads around identity: do not accept a torn or disconnected sample.
   if(!TerminalInfoInteger(TERMINAL_CONNECTED)) { Block(1); return false; }
   if(AccountInfoInteger(ACCOUNT_TRADE_MODE)!=ACCOUNT_TRADE_MODE_DEMO) {
      Block(2); return false;
   }
   if(bound_login<=0 || bound_server=="" ||
      AccountInfoInteger(ACCOUNT_LOGIN)!=bound_login ||
      AccountInfoString(ACCOUNT_SERVER)!=bound_server) {
      Block(3); return false;
   }
   if(!TerminalInfoInteger(TERMINAL_CONNECTED) ||
      AccountInfoInteger(ACCOUNT_TRADE_MODE)!=ACCOUNT_TRADE_MODE_DEMO ||
      AccountInfoInteger(ACCOUNT_LOGIN)!=bound_login ||
      AccountInfoString(ACCOUNT_SERVER)!=bound_server) {
      Block(4); return false;
   }
   return true;
}
string Envelope(string kind,string payload,ulong n) {
   // All events belong to the pinned account, never the current unverified one.
   return "{\"source\":"+Q(SourceId)+",\"stream\":"+Q(stream_id)+
      ",\"seq\":"+U(n)+",\"server\":"+Q(bound_server)+
      ",\"login\":"+Q(U((ulong)bound_login))+",\"kind\":"+Q(kind)+
      ",\"payload\":"+payload+"}";
}
bool Append(string line) {
   if(!active || !Guard() || io_failed) return false;
   uchar bytes[];
   int n=StringToCharArray(line+"\n",bytes,0,-1,CP_UTF8);
   if(n<=1) { io_failed=true; Block(5); return false; }
   ResetLastError();
   // Binary mode: exact UTF-8 bytes, no TXT newline conversion, no trailing NUL.
   // No FILE_SHARE_WRITE. A broken/uncertain spool is never retried or truncated.
   int f=FileOpen(spool_name,FILE_READ|FILE_WRITE|FILE_BIN|FILE_SHARE_READ);
   if(f==INVALID_HANDLE) { io_failed=true; Block(6); return false; }
   if(!FileSeek(f,0,SEEK_END) || GetLastError()!=0) {
      FileClose(f); io_failed=true; Block(7); return false;
   }
   if(!Guard()) { FileClose(f); return false; }
   ResetLastError();
   uint written=FileWriteArray(f,bytes,0,n-1);
   bool ok=(written==(uint)(n-1) && GetLastError()==0);
   FileFlush(f);
   ok=ok && GetLastError()==0;
   FileClose(f);
   ok=ok && GetLastError()==0;
   if(!ok) { io_failed=true; Block(8); return false; }
   // This is observational fencing, not an atomic broker/filesystem transaction.
   return Guard();
}
int OnInit() {
   active=false; blocked=false; io_failed=false; block_reason=0;
   head=0; count=0; sequence=0; dropped=0;
   bound_login=ExpectedLogin; bound_server=ExpectedServer;
   if(ExpectedLogin<=0 || ExpectedServer=="" ||
      StringLen(SourceId)<1 || StringLen(SourceId)>48) {
      Block(9); return INIT_PARAMETERS_INCORRECT;
   }
   for(int i=0;i<StringLen(SourceId);i++) {
      ushort c=StringGetCharacter(SourceId,i);
      if(!((c>='a'&&c<='z')||(c>='A'&&c<='Z')||
           (c>='0'&&c<='9')||c=='-'||c=='_')) {
         Block(9); return INIT_PARAMETERS_INCORRECT;
      }
   }
   if(!Guard()) return INIT_FAILED;
   stream_id=U((ulong)TimeLocal())+"-"+U(GetMicrosecondCount())+"-"+U((ulong)ChartID());
   spool_name="VidarObserver-"+SourceId+"-"+stream_id+".jsonl";
   // Never append a new session to a previous (possibly torn) stream.
   if(FileIsExist(spool_name)) { Block(10); return INIT_FAILED; }
   if(!Guard() || !EventSetTimer(2)) { Block(11); return INIT_FAILED; }
   if(!Guard()) return INIT_FAILED;
   active=true;
   Print("Vidar C initialized; demo identity pinned; no trading functions.");
   return INIT_SUCCEEDED;
}
void OnTradeTransaction(const MqlTradeTransaction &trans,
                        const MqlTradeRequest &request,
                        const MqlTradeResult &result) {
   if(!active || !Guard()) return;
   sequence++;
   if(count>=CAPACITY) { dropped++; return; }
   int index=(head+count)%CAPACITY;
   pending[index]=trans; numbers[index]=sequence;
   retcodes[index]=(trans.type==TRADE_TRANSACTION_REQUEST ? result.retcode : 0);
   if(!Guard()) return;
   count++;
}
void FlushEvents() {
   if(!active || !Guard()) return;
   for(int i=0;i<BATCH_SIZE && count>0;i++) {
      if(!Guard()) return;
      MqlTradeTransaction t=pending[head];
      string payload="{\"transaction_type\":"+IntegerToString((int)t.type)+
         ",\"order_id\":"+Q(U(t.order))+",\"deal_id\":"+Q(U(t.deal))+
         ",\"position_ticket\":"+Q(U(t.position))+",\"symbol\":"+Q(t.symbol)+
         ",\"volume\":"+DoubleToString(t.volume,8)+",\"price\":"+DoubleToString(t.price,10)+
         ",\"sl\":"+DoubleToString(t.price_sl,10)+",\"tp\":"+DoubleToString(t.price_tp,10)+
         ",\"request_retcode\":"+IntegerToString((int)retcodes[head])+"}";
      if(!Append(Envelope("transaction",payload,numbers[head]))) return;
      head=(head+1)%CAPACITY; count--;
   }
}
void Snapshot() {
   if(!active || !Guard()) return;
   int total=PositionsTotal(); string positions="[";
   for(int i=0;i<total;i++) {
      if(!Guard() || PositionGetTicket(i)==0) return;
      string side=PositionGetInteger(POSITION_TYPE)==POSITION_TYPE_BUY ? "buy" : "sell";
      if(i>0) positions+=",";
      positions+="{\"position_id\":"+Q(U((ulong)PositionGetInteger(POSITION_IDENTIFIER)))+
         ",\"symbol\":"+Q(PositionGetString(POSITION_SYMBOL))+",\"type\":"+Q(side)+
         ",\"volume\":"+DoubleToString(PositionGetDouble(POSITION_VOLUME),8)+
         ",\"sl\":"+DoubleToString(PositionGetDouble(POSITION_SL),10)+
         ",\"tp\":"+DoubleToString(PositionGetDouble(POSITION_TP),10)+"}";
   }
   positions+="]";
   if(!Guard() || total!=PositionsTotal()) return;
   // complete means this enumeration finished, NOT an atomic position snapshot.
   string payload="{\"connected\":true,\"complete\":true,\"captured_utc\":"+
      U((ulong)TimeGMT())+",\"positions\":"+positions+"}";
   Append(Envelope("snapshot",payload,++sequence));
}
void OnTimer() {
   if(!active || !Guard()) return;
   FlushEvents();
   if(!active || !Guard() || count>0) return;
   if(dropped>0) {
      if(!Append(Envelope("overflow","{\"dropped\":"+U(dropped)+"}",++sequence))) return;
      dropped=0;
   }
   Snapshot();
}
void OnDeinit(const int reason) {
   // Absolutely no flush: REASON_ACCOUNT may already expose the new account.
   // Other deinitializations also discard RAM; reconcile history on restart.
   EventKillTimer();
   active=false; blocked=true;
   head=0; count=0; dropped=0;
   Print("Vidar C stopped; reason=",reason,"; no deinit I/O.");
}
