// Prototype: NOT compiled or attached to a terminal. Demo accounts ONLY.
// Transaction handler copies to a bounded ring; all disk I/O runs on a timer.
// Spool bridge provides HMAC transport. This source contains no trade primitives.
#property strict
#property version "0.10"
input string SourceId="demo-source";
#define CAPACITY 1024
MqlTradeTransaction pending[CAPACITY];
string servers[CAPACITY], logins[CAPACITY];
ulong numbers[CAPACITY];
uint retcodes[CAPACITY];
int head=0, count=0;
ulong sequence=0, dropped=0;
string stream_id, spool_name;

string Q(string text) {
   StringReplace(text,"\\","\\\\"); StringReplace(text,"\"","\\\"");
   StringReplace(text,"\r","\\r"); StringReplace(text,"\n","\\n");
   StringReplace(text,"\t","\\t"); return "\""+text+"\"";
}
string U(ulong n) { return StringFormat("%I64u",n); }
string Envelope(string kind,string payload,ulong n,string server,string login) {
   return "{\"source\":"+Q(SourceId)+",\"stream\":"+Q(stream_id)+
      ",\"seq\":"+U(n)+",\"server\":"+Q(server)+",\"login\":"+Q(login)+
      ",\"kind\":"+Q(kind)+",\"payload\":"+payload+"}";
}
bool Append(string line) {
   // On any I/O fault, retain ring entries. A torn JSONL line requires explicit
   // quarantine/recovery by the bridge; never silently skip or truncate a spool.
   ResetLastError();
   int f=FileOpen(spool_name,FILE_READ|FILE_WRITE|FILE_TXT|FILE_ANSI|FILE_SHARE_READ,0,CP_UTF8);
   if(f==INVALID_HANDLE) return false;
   FileSeek(f,0,SEEK_END);
   uint written=FileWriteString(f,line+"\n"); FileFlush(f);
   bool ok=(written>0 && GetLastError()==0); FileClose(f); return ok;
}
int OnInit() {
   if(AccountInfoInteger(ACCOUNT_TRADE_MODE)!=ACCOUNT_TRADE_MODE_DEMO) {
      Print("Vidar observer prototype is demo-only; initialization refused.");
      return INIT_FAILED;
   }
   if(StringLen(SourceId)<1 || StringLen(SourceId)>48) return INIT_PARAMETERS_INCORRECT;
   for(int i=0;i<StringLen(SourceId);i++) {
      ushort c=StringGetCharacter(SourceId,i);
      if(!((c>='a'&&c<='z')||(c>='A'&&c<='Z')||(c>='0'&&c<='9')||c=='-'||c=='_'))
         return INIT_PARAMETERS_INCORRECT;
   }
   stream_id=U((ulong)TimeLocal())+"-"+U(GetMicrosecondCount())+"-"+U((ulong)ChartID());
   spool_name="VidarObserver-"+SourceId+".jsonl";
   if(!EventSetTimer(2)) return INIT_FAILED;
   return INIT_SUCCEEDED;
}
void OnTradeTransaction(const MqlTradeTransaction &trans,
                        const MqlTradeRequest &request,
                        const MqlTradeResult &result) {
   sequence++;
   if(count>=CAPACITY) { dropped++; return; }
   int index=(head+count)%CAPACITY;
   pending[index]=trans; numbers[index]=sequence;
   servers[index]=AccountInfoString(ACCOUNT_SERVER);
   logins[index]=U((ulong)AccountInfoInteger(ACCOUNT_LOGIN));
   // Request/result fields are valid only for the request transaction type.
   retcodes[index]=(trans.type==TRADE_TRANSACTION_REQUEST ? result.retcode : 0);
   count++;
}
void FlushEvents() {
   // Bounded disk batch prevents timer monopolization. Backlog remains queued.
   for(int i=0;i<128 && count>0;i++) {
      MqlTradeTransaction t=pending[head];
      string payload="{\"transaction_type\":"+IntegerToString((int)t.type)+
         ",\"order_id\":"+Q(U(t.order))+",\"deal_id\":"+Q(U(t.deal))+
         ",\"position_ticket\":"+Q(U(t.position))+",\"symbol\":"+Q(t.symbol)+
         ",\"volume\":"+DoubleToString(t.volume,8)+",\"price\":"+DoubleToString(t.price,10)+
         ",\"sl\":"+DoubleToString(t.price_sl,10)+",\"tp\":"+DoubleToString(t.price_tp,10)+
         ",\"request_retcode\":"+IntegerToString((int)retcodes[head])+"}";
      // Callback fields are raw observations, not authoritative position state.
      if(!Append(Envelope("transaction",payload,numbers[head],servers[head],logins[head]))) return;
      head=(head+1)%CAPACITY; count--;
   }
}
void Snapshot() {
   string server=AccountInfoString(ACCOUNT_SERVER),login=U((ulong)AccountInfoInteger(ACCOUNT_LOGIN));
   bool connected=(bool)TerminalInfoInteger(TERMINAL_CONNECTED);
   if(!connected) {
      Append(Envelope("heartbeat","{\"connected\":false,\"captured_utc\":"+U((ulong)TimeGMT())+"}",++sequence,server,login));
      return;
   }
   int total=PositionsTotal(); string positions="[";
   bool complete=true;
   for(int i=0;i<total;i++) {
      if(PositionGetTicket(i)==0) { complete=false; break; }
      string side=PositionGetInteger(POSITION_TYPE)==POSITION_TYPE_BUY ? "buy" : "sell";
      if(i>0) positions+=",";
      positions+="{\"position_id\":"+Q(U((ulong)PositionGetInteger(POSITION_IDENTIFIER)))+
         ",\"symbol\":"+Q(PositionGetString(POSITION_SYMBOL))+",\"type\":"+Q(side)+
         ",\"volume\":"+DoubleToString(PositionGetDouble(POSITION_VOLUME),8)+
         ",\"sl\":"+DoubleToString(PositionGetDouble(POSITION_SL),10)+
         ",\"tp\":"+DoubleToString(PositionGetDouble(POSITION_TP),10)+"}";
   }
   positions+="]";
   complete=complete && total==PositionsTotal() && (bool)TerminalInfoInteger(TERMINAL_CONNECTED)
      && server==AccountInfoString(ACCOUNT_SERVER) && login==U((ulong)AccountInfoInteger(ACCOUNT_LOGIN));
   if(!complete) return; // No empty/partial snapshot may replace verified state.
   string payload="{\"connected\":true,\"complete\":true,\"captured_utc\":"+U((ulong)TimeGMT())+",\"positions\":"+positions+"}";
   Append(Envelope("snapshot",payload,++sequence,server,login));
}
void OnTimer() {
   FlushEvents();
   if(count>0) return;
   if(dropped>0) {
      if(Append(Envelope("overflow","{\"dropped\":"+U(dropped)+"}",++sequence,
         AccountInfoString(ACCOUNT_SERVER),U((ulong)AccountInfoInteger(ACCOUNT_LOGIN))))) dropped=0;
   }
   Snapshot();
}
void OnDeinit(const int reason) { EventKillTimer(); FlushEvents(); }
