import bcolz
import blaze
import pandas as pd
from secdata.utils import bcolz_path
from zipline.pipeline.domain import CN_EQUITIES
from zipline.pipeline.loaders.blaze import from_blaze
from zipline.utils.memoize import classlazyval, remember_last


INDICATORS = [
    # metadata
    'is_financial',
    'shareholder_type',
    'listing_by_shell',
    # balance sheet
    'monetary_cap',
    'acct_rcv',
    'inventories',
    'tot_cur_assets',
    'tot_assets',
    'acct_payable',
    'tot_cur_liab',
    'tot_liab',
    'eqy_belongto_parcomsh',
    'tot_equity',
    # income statement
    'tot_oper_rev',
    'oper_rev',
    'tot_oper_cost',
    'oper_cost',
    'opprofit',
    'net_profit_is',
    'np_belongto_parcomsh',
    # cashflow statement
    'cash_recp_sg_and_rs',
    'net_incr_cash_cash_equ_dm',
    'net_cash_flows_oper_act',
    'net_cash_flows_inv_act',
    'net_cash_flows_fnc_act',
    # financial indicator
    'roe_basic',
    'roe_exbasic',
    'eps_exdiluted',
    'eps_diluted',
    'eps_basic',
    # shares outstanding
    'turn',
    'amt',
    'total_shares',
    'float_a_shares',
    'mkt_cap_ard',
    'mkt_cap_ashare',
    # index membership
    'memb_csi300',
    'memb_csi500',
    'memb_csi800',
    'memb_csi1000',
    # sector membership
    'memb_energy',
    'memb_materials',
    'memb_industrials',
    'memb_consumer_discretionary',
    'memb_consumer_staples',
    'memb_health_care',
    'memb_financials',
    'memb_information_technology',
    'memb_communication_services',
    'memb_utilities',
    'memb_real_estate',
]


class Fundamentals(object):
    def __getattr__(self, name):
        if name in self.colnames:
            if name.endswith('_asof'):
                return self.factory(name[:-5]).asof_date
            else:
                return self.factory(name).value
        raise AttributeError
    
    def __getattribute__(self, name):       
        ret = object.__getattribute__(self, name)
        
        # We should use __getattr__ here
        if ret == AttributeError:
            raise ret
        return ret
    
    @classlazyval
    def colnames(cls):
        ret = []
        for column in INDICATORS:
            ret.extend([column, column+'_asof'])
        return ret
    
    @staticmethod
    @remember_last
    def factory(name, use_checkpoints=False):
        ''' Bcolz data entry point'''
        expr = blaze.data(bcolz_path(name), name=name)

        if use_checkpoints: # 日度数据需要checkpoints加速ffill
            pass
        else:
            checkpoints = None

        return from_blaze(
            expr,
            domain=CN_EQUITIES,
            deltas=None,
            checkpoints=checkpoints,
            no_deltas_rule='ignore',
            no_checkpoints_rule='ignore',
        )


#For help hints (.), easy to use
for k in INDICATORS:
    setattr(
        Fundamentals, 
        k, 
        AttributeError,
    )
    setattr(
        Fundamentals, 
        k + '_asof',
        AttributeError,
    )

Fundamentals = Fundamentals()
