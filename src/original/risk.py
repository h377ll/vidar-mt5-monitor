"""Deterministic pure supervision functions; never sends an order.
Inputs require verified broker specifications/contract definitions.
"""
from decimal import Decimal

def loss_at_stop(position, spec):
    sl=position.get('sl')
    if sl is None:return {'state':'unknown','risk_money':None}
    if sl==0:return {'state':'missing_sl','risk_money':None}
    required=['tick_size','tick_value_loss_account_currency']
    if not spec.get('verified') or any(spec.get(k) is None or spec[k]<=0 for k in required):
        return {'state':'specification_unknown','risk_money':None}
    if position.get('entry_price') is None or position.get('volume') is None or position.get('side') not in ('buy','sell'):
        return {'state':'position_unknown','risk_money':None}
    distance=(position['entry_price']-sl) if position['side']=='buy' else (sl-position['entry_price'])
    # Approximate at entry, excludes gaps/slippage and future costs. Not guaranteed loss.
    money=max(0,distance)/spec['tick_size']*spec['tick_value_loss_account_currency']*position['volume']
    return {'state':'estimated','risk_money':money,'includes_slippage':False,'includes_costs':False}

def contract_distance(equity,rule,reference=None):
    if not rule.get('verified') or equity is None:return {'state':'unknown','distance':None}
    mode=rule.get('mode')
    if mode=='static':floor=rule.get('floor_amount')
    elif mode in ('trailing_intraday','trailing_eod','daily'):
        # Caller must supply verified reference for THIS contract/day; never infer midnight timezone.
        if not reference or reference.get('kind')!=mode or not reference.get('verified'):
            return {'state':'reference_unknown','distance':None}
        base=reference.get('value');allowance=rule.get('allowance_amount')
        if base is None or allowance is None:return {'state':'unknown','distance':None}
        floor=base-allowance
        if rule.get('floor_cap') is not None:floor=min(floor,rule['floor_cap'])
    else:return {'state':'rule_unknown','distance':None}
    if floor is None:return {'state':'unknown','distance':None}
    distance=float(Decimal(str(equity))-Decimal(str(floor)))
    return {'state':'breach' if distance<=0 else 'within','distance':distance,'floor':floor}

def group_risk(risks,groups):
    if groups is None:return {'state':'correlation_groups_unknown','groups':None}
    out={}
    for name,ids in groups.items():
        vals=[risks.get(i,{}).get('risk_money') for i in ids]
        out[name]=None if any(x is None for x in vals) else sum(vals)
    return {'state':'configured_stress_sum_not_empirical_correlation','groups':out}
