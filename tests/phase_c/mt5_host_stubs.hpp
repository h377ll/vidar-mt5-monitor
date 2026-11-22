// Offline test doubles only. No MT5 terminal, filesystem or network connection.
// These are NOT an MQL5 implementation and do not prove MetaEditor compilation.
#include <string>
#include <vector>
#include <functional>
#include <cstdio>
#include <stdexcept>
#include <iostream>
using string=std::string;
using uchar=unsigned char;
using ushort=unsigned short;
#define input
enum {
 TERMINAL_CONNECTED=1, ACCOUNT_TRADE_MODE, ACCOUNT_LOGIN, ACCOUNT_SERVER,
 ACCOUNT_TRADE_MODE_DEMO=10, ACCOUNT_TRADE_MODE_REAL, ACCOUNT_TRADE_MODE_CONTEST,
 FILE_READ=16, FILE_WRITE=32, FILE_BIN=64, FILE_SHARE_READ=128,
 CP_UTF8=65001, INVALID_HANDLE=-1, SEEK_END_STUB=2,
 INIT_SUCCEEDED=0, INIT_FAILED=1, INIT_PARAMETERS_INCORRECT=2,
 TRADE_TRANSACTION_REQUEST=100, POSITION_TYPE=200, POSITION_TYPE_BUY,
 POSITION_IDENTIFIER, POSITION_SYMBOL, POSITION_VOLUME, POSITION_SL, POSITION_TP
};
struct MqlTradeTransaction {
 int type=0; ulong order=1,deal=2,position=3; string symbol="TEST";
 double volume=0.01,price=1,price_sl=0,price_tp=0;
};
struct MqlTradeRequest {};
struct MqlTradeResult { uint retcode=123; };
struct MockState {
 bool connected=true,timer=false,timer_fail=false,collision=false;
 long mode=ACCOUNT_TRADE_MODE_DEMO,login=101;
 string server="SYNTHETIC-DEMO";
 int reads=0,error=0,opens=0,writes=0,closes=0,positions=0;
 bool open_fail=false,seek_fail=false,short_write=false,flush_fail=false;
 bool close_fail=false,conversion_fail=false,position_fail=false;
 std::function<void()> read_hook,seek_hook,position_hook,timer_hook;
 std::vector<string> output;
} m;
ulong unique_clock=10000;
void TickRead() { m.reads++; if(m.read_hook) m.read_hook(); }
long TerminalInfoInteger(int) { TickRead(); return m.connected; }
long AccountInfoInteger(int k) { TickRead(); return k==ACCOUNT_LOGIN?m.login:m.mode; }
string AccountInfoString(int) { TickRead(); return m.server; }
int StringLen(const string &s) { return (int)s.size(); }
ushort StringGetCharacter(const string &s,int i) { return (uchar)s.at(i); }
string StringSubstr(const string &s,int i,int n) { return s.substr(i,n); }
template<class... T> string StringFormat(const char *fmt,T... args) {
 string f=fmt; if(f=="%I64u") f="%lu";
 int n=std::snprintf(nullptr,0,f.c_str(),args...);
 std::vector<char> b(n+1); std::snprintf(b.data(),b.size(),f.c_str(),args...);
 return string(b.data(),n);
}
string IntegerToString(int n) { return std::to_string(n); }
string DoubleToString(double n,int digits) { return StringFormat("%.*f",digits,n); }
template<class... T> void Print(T...) {}
void ResetLastError() { m.error=0; }
int GetLastError() { return m.error; }
bool EventSetTimer(int) {
 if(m.timer_fail) return false;
 m.timer=true;
 if(m.timer_hook) m.timer_hook();
 return true;
}
void EventKillTimer() { m.timer=false; }
long TimeLocal() { return 12345; }
long TimeGMT() { return 12345; }
ulong GetMicrosecondCount() { return ++unique_clock; }
long ChartID() { return 1; }
bool FileIsExist(string) { return m.collision; }
int StringToCharArray(string s,std::vector<uchar> &v,int,int,int) {
 if(m.conversion_fail) return 0;
 v.assign(s.begin(),s.end()); v.push_back(0); return (int)v.size();
}
int FileOpen(string,int) { m.opens++; return m.open_fail?INVALID_HANDLE:42; }
bool FileSeek(int,int,int) { if(m.seek_hook) m.seek_hook(); return !m.seek_fail; }
uint FileWriteArray(int,const std::vector<uchar> &v,int start,int n) {
 m.writes++; int actual=m.short_write?n-1:n;
 m.output.push_back(string(v.begin()+start,v.begin()+start+actual));
 return actual;
}
void FileFlush(int) { if(m.flush_fail) m.error=5010; }
void FileClose(int) { m.closes++; if(m.close_fail) m.error=5011; }
int PositionsTotal() { return m.positions; }
ulong PositionGetTicket(int) { return m.position_fail?0:1; }
long PositionGetInteger(int k) {
 if(m.position_hook) m.position_hook();
 return k==POSITION_TYPE?POSITION_TYPE_BUY:1;
}
string PositionGetString(int) { return "SYNTHETIC"; }
double PositionGetDouble(int) { return 0.01; }
