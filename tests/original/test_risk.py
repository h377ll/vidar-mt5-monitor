import unittest
from risk import contract_distance,loss_at_stop,group_risk

class RiskTests(unittest.TestCase):
    def test_unknown_contract(self):self.assertIsNone(contract_distance(48000,{'mode':'static'})['distance'])
    def test_static(self):self.assertEqual(contract_distance(48000,{'mode':'static','verified':True,'floor_amount':45000})['distance'],3000)
    def test_trailing_reference_required(self):self.assertIsNone(contract_distance(48000,{'mode':'trailing_eod','verified':True,'allowance_amount':3000})['distance'])
    def test_trailing_reference_type_not_interchangeable(self):self.assertIsNone(contract_distance(48000,{'mode':'trailing_eod','verified':True,'allowance_amount':3000},{'kind':'trailing_intraday','verified':True,'value':52000})['distance'])
    def test_missing_stop(self):self.assertEqual(loss_at_stop({'sl':0},{})['state'],'missing_sl')
    def test_unknown_cost_spec(self):self.assertIsNone(loss_at_stop({'sl':1},{})['risk_money'])
    def test_sell_stop(self):
        r=loss_at_stop({'sl':1.11,'entry_price':1.1,'side':'sell','volume':2},{'verified':True,'tick_size':.0001,'tick_value_loss_account_currency':10})
        self.assertAlmostEqual(r['risk_money'],2000)
    def test_correlation_unknown(self):self.assertIsNone(group_risk({},None)['groups'])
    def test_missing_member_not_zero(self):self.assertIsNone(group_risk({'a':{'risk_money':5}},{'g':['a','b']})['groups']['g'])

if __name__=='__main__':unittest.main()
