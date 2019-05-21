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
    'total_shares',
    'float_a_shares',
    'mkt_cap_ard',
    'mkt_cap_ashare',
    # index membership
    'csi300_constituents',
    'csi500_constituents',
    'csi800_constituents',
    'csi1000_constituents',
    # sector membership
    'sw_sector',
    'sw_agriculture_constituents',
    'sw_mining_constituents',
    'sw_chemicals_constituents',
    'sw_steel_constituents',
    'sw_metals_constituents',
    'sw_electronics_constituents',
    'sw_appliances_constituents',
    'sw_food_constituents',
    'sw_textiles_constituents',
    'sw_light_manufacturing_constituents',
    'sw_pharmaceuticals_constituents',
    'sw_utilities_constituents',
    'sw_transportation_constituents',
    'sw_real_estate_constituents',
    'sw_commerce_constituents',
    'sw_services_constituents',
    'sw_conglomerate_constituents',
    'sw_building_materials_constituents',
    'sw_building_decorations_constituents',
    'sw_electricals_constituents',
    'sw_defense_military_constituents',
    'sw_it_constituents',
    'sw_media_constituents',
    'sw_communication_services_constituents',
    'sw_banks_constituents',
    'sw_nonbank_financials_constituents',
    'sw_auto_constituents',
    'sw_machinery_constituents',
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
