from datetime import datetime,timezone,timedelta
from pathlib import Path
import tempfile,unittest
import journal as j
from notifications import dispatch_one,DefiniteDeliveryFailure

class NotificationsTests(unittest.TestCase):
 def setUp(self):
    self.tmp=tempfile.TemporaryDirectory();self.db=j.open_db(Path(self.tmp.name)/'db');self.at=datetime.now(timezone.utc)
    with self.db:j.emit(self.db,'["Demo", "123456"]','sl_missing',{'symbol':'EURUSD'},self.at.isoformat())
 def tearDown(self):self.db.close();self.tmp.cleanup()
 def test_receipt_and_duplicate_suppression(self):
    calls=[]
    def send(msg,key):calls.append(key);return {'provider_message_id':'synthetic-in-temporary-db'}
    self.assertEqual(dispatch_one(self.db,send,self.at),'delivered');self.assertEqual(dispatch_one(self.db,send,self.at),'empty');self.assertEqual(len(calls),1)
 def test_lost_receipt_uncertain(self):
    self.assertEqual(dispatch_one(self.db,lambda *a:None,self.at),'uncertain')
    self.assertEqual(dispatch_one(self.db,lambda *a:None,self.at),'empty')
 def test_expired_does_not_send(self):
    def forbidden(*args):raise AssertionError('must not send')
    self.assertEqual(dispatch_one(self.db,forbidden,self.at+timedelta(seconds=301)),'expired')
 def test_clock_backwards_holds(self):self.assertEqual(dispatch_one(self.db,lambda *a:None,self.at-timedelta(seconds=1)),'clock_hold')
 def test_known_failure_distinct(self):
    def fail(*a):raise DefiniteDeliveryFailure()
    self.assertEqual(dispatch_one(self.db,fail,self.at),'failed')

if __name__=='__main__':unittest.main()
