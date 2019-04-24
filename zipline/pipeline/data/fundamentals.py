from zipline.utils.memoize import (
    classlazyval, 
    remember_last,
)
from zipline.utils.numpy_utils import (
    int64_dtype,
    _FILLVALUE_DEFAULTS,
)
from zipline.pipeline.domain import CN_EQUITIES
from zipline.pipeline.loaders.blaze import from_blaze
from zipline.pipeline.loaders.blaze.core import datashape_type_to_numpy

from secdb.engine import ENGINE
from secdb.utils.bcolz_utils import bcolz_path

import blaze
import pandas as pd


INDICATORS = {
    # Meta
    'province': '省份', 
    'sw_sector': '申万行业一级名称', 
    'csrc_sector': '证监会一级行业名称', 
    'exchange': '上市地点', 
    'mem_of_csi300': '沪深300成分股', 
    'mem_of_csi500': '中证500成分股', 
    'mem_of_csi800': '中证800成分股',
    
    # Balance Sheet
    'cash': '货币资金', 
    'trading_assets': '以公允价值计量且其变动计入当期损益的金融资产', 
    'notes_receivable': '应收票据', 
    'accounts_receivable': '应收账款', 
    'accounts_payable': '应付账款', 
    'prepaid_assets': '预付款项', 
    'accrued_interest_receivable': '应收利息', 
    'accrued_investment_income': '应收股利', 
    'available_for_sale_securities': '可供出售金融资产', 
    'current_assets': '流动资产合计', 
    'current_liabilities': '流动负债合计', 
    'goodwill': '商誉', 
    'held_to_maturity_securities': '持有至到期投资', 
    'interest_payable': '应付利息', 
    'inventory': '存货', 
    'invested_capital': '实收资本或股本', 
    'investment_properties': '投资性房地产', 
    'minority_interest_balance_sheet': '少数股东权益', 
    'other_current_assets': '其他流动资产', 
    'other_receivables': '其他应收款', 
    'stockholders_equity': '归属于母公司所有者权益', 
    'total_assets': '资产总计', 
    'total_liabilities': '负债合计', 
    'total_equity': '所有者权益或股东权益合计', 
    'total_non_current_assets': '非流动资产合计', 
    
    # Cash Flow Statement
    'financing_cash_flow': '筹资活动产生的现金流量净额', 
    'investing_cash_flow': '投资活动产生的现金流量净额', 
    'operating_cash_flow': '经营活动产生的现金流量净额', 
    
    # Income Statement
    'total_revenue': '一_营业总收入', 
    'gross_profit': '三_营业利润', 
    'net_income': '五_净利润', 
    'net_income_stockholders': '归属于母公司所有者的净利润', 
    
    # Shares Change
    'total_shares': '总股本', 
    'shares_outstanding': '人民币普通股', 
    
    # Financial Indicators
    'assets_turnover': '总资产周转率', 
    'cash_conversion_cycle': '现金转换周期', 
    'current_ratio': '流动比率', 
    'cash_ratio': '保守速动比率',  
    'quick_ratio': '速动比率', 
    'days_in_inventory': '存货周转天数', 
    'days_in_sales': '应收账款周转天数', 
    'debtto_assets': '资产负债比率', 
    
    'fix_assets_turonver': '固定资产周转率', 
    #'interest_coverage': '利息保障倍数', 
    'inventory_turnover': '存货周转率', 
    
    'net_income_growth': '净利润增长率', 
    'revenue_growth': '营业收入增长率', 
    'net_margin': '净利润率', 
    'gross_margin': '毛利率', 
    'operation_margin': '营业利润率', 
    'receivable_turnover': '应收账款周转率', 

    'roa': '总资产报酬率', 
    'roe': '净资产收益率', 
    'roic': '投资收益率', 
    'total_debt_equity_ratio': '产权比率', 
    'eps': '每股收益', 
    'eps_basic': '基本每股收益', 
    'eps_diluted': '稀释每股收益', 
    'eps_cont': '扣除非经常性损益每股收益', 
    'undis_profit_ps': '每股未分配利润', 
    'bps': '每股净资产', 
    'bps_adjust': '调整后每股净资产', 
    'surplus_capital_ps': '每股资本公积金', 
    'gc_to_gr': '营业成本率', 
    'admin_expense_to_gr': '管理费用率', 
    'fina_expense_to_gr': '财务费用率', 
    'np_to_cost_expense': '成本费用利润率', 
    'ca_turn': '流动资产周转率', 
    'cash_to_current_debt': '现金比率', 
    'ocf_to_assets': '全部资产现金回收率',
}


def fillvalue_for_expr(expr):
    fillmissing = _FILLVALUE_DEFAULTS.copy()
    fillmissing.update({
        int64_dtype: -9999,
    })
    
    ret = {}
    for name, type_ in expr.dshape.measure.fields:
        n_type = datashape_type_to_numpy(type_)
        ret[name] = fillmissing[n_type]
    return ret
        
    
class Fundamentals(object):
    def __getattr__(self, name):
        if name in self.colnames:
            if name.endswith('asof_date'):
                name = INDICATORS[name[:-10]]
                return self.factory(name).asof_date
            else:
                name = INDICATORS[name]
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
        for column in INDICATORS.keys():
            ret.extend([column, column+'_asof_date'])
        return ret
    
    @staticmethod
    @remember_last
    def factory(name):
        ''' Bcolz data entry point
        '''
        pth = bcolz_path(name)
        expr = blaze.data(pth)
        return from_blaze(
            expr, 
            domain=CN_EQUITIES,
            no_deltas_rule='ignore',
            no_checkpoints_rule='ignore',            
            missing_values=fillvalue_for_expr(expr)
        )

    @classlazyval
    def Quote(cls):
        ''' Sqlite data entry point
        '''      
        table_name = 'Quote'
        columns_to_rename = {
            '股票代码': 'sid',
            '交易日期': 'asof_date',
        }
        
        expr = blaze.data(ENGINE)[table_name]
        expr = expr.relabel(columns_to_rename)
        
        # 日度数据需要checkpoints加速ffill
        # 此处采用为月度频率，ffill=None
        checkpoints_dt = pd.date_range(
            '1990-11-1', pd.Timestamp('today'), freq='M'
        )
        data = {k: 0 for k in expr.fields}
        data.update({'asof_date': checkpoints_dt})
        checkpoints = blaze.data(
            pd.DataFrame(data),
            dshape=expr.dshape,
            name='checkpoints',
        )  
        
        return from_blaze(
            expr, 
            checkpoints=checkpoints,
            no_deltas_rule='ignore',
            domain=CN_EQUITIES,
            missing_values=fillvalue_for_expr(expr)
        )


#For help hints (.), easy to use
for k in INDICATORS.keys():
    setattr(
        Fundamentals, 
        k, 
        AttributeError,
    )
    setattr(
        Fundamentals, 
        k + '_asof_date', 
        AttributeError,
    )

Fundamentals = Fundamentals()