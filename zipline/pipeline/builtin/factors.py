import numpy as np
import pandas as pd
from zipline.assets import Equity
from zipline.assets.exchange_info import ExchangeInfo
from zipline.pipeline.data import EquityPricing, Fundamentals
from zipline.pipeline.factors import (
    SimpleBeta,
    CustomFactor,
    AnnualizedVolatility,
)
from .utils import PositiveDivide


class Momentum(CustomFactor): 
    '''
    Default: price return changes from -244 days to -21 days
    '''
    inputs = [EquityPricing.close]
    params = {'t0': 21} # to which day
    window_length = 244 # from which day

    def compute(self, today, assets, out, close, t0):  
        out[:] = close[-t0] / close[0] - 1


class PERatio(PositiveDivide):
    inputs = [
        Fundamentals.mkt_cap_ard,
        Fundamentals.net_profit_is
    ]


class PBRatio(PositiveDivide):
    inputs = [
        Fundamentals.mkt_cap_ard,
        Fundamentals.tot_equity
    ]


class PCFRatio(PositiveDivide):
    inputs = [
        Fundamentals.mkt_cap_ard,
        Fundamentals.net_cash_flows_oper_act
    ]


class PSRatio(PositiveDivide):
    inputs = [
        Fundamentals.mkt_cap_ard,
        Fundamentals.tot_oper_rev
    ]


class ROE(PositiveDivide):
    inputs = [
        Fundamentals.net_profit_is,
        Fundamentals.tot_equity
    ]


class ROA(PositiveDivide):
    inputs = [
        Fundamentals.net_profit_is,
        Fundamentals.tot_assets,
    ]


NetMargin = lambda: Fundamentals.net_profit_is.latest / Fundamentals.tot_oper_rev.latest
DebtToAsset = lambda: Fundamentals.tot_liab.latest / Fundamentals.tot_assets.latest
Risk = lambda: AnnualizedVolatility(window_length=244, annualization_factor=244)

benchmark = Equity.from_dict({
    'sid': 3623,
    'symbol': '000300.SH',
    'first_traded': pd.Timestamp('2004-12-31', tz='UTC'),
    'start_date': pd.Timestamp('2005-04-08', tz='UTC'),
    'exchange_info': ExchangeInfo('XSHG', 'XSHG', 'CN'),
})
Beta = lambda: SimpleBeta(target=benchmark, regression_length=244)
