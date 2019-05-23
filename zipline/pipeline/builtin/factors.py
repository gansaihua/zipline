import numpy as np
import pandas as pd
from zipline.assets import Equity
from zipline.assets.exchange_info import ExchangeInfo
from zipline.pipeline.data import EquityPricing, Fundamentals
from zipline.pipeline.factors import (
    Returns,
    DailyReturns,
    SimpleBeta,
    CustomFactor,
    AnnualizedVolatility,
)
from .utils import PositiveDivide, YoYGrowth


def Investment():
    return YoYGrowth(inputs=[Fundamentals.tot_assets_asof,
                             Fundamentals.tot_assets]),


class Illiquidity(CustomFactor):
    inputs = [DailyReturns(), Fundamentals.amt]
    window_length = 20
    missing_value = 0

    def compute(self, today, assets, out, r, amount):
        out[:] = np.nanmean(np.abs(r) / amount, axis=0)


class Turnover(CustomFactor):
    inputs = [Fundamentals.turn]
    window_length = 250
    missing_value = 0

    def compute(self, today, assets, out, turnover):
        out[:] = np.nanmean(turnover, axis=0)


class AbnormalTurnover(CustomFactor):
    inputs = [Fundamentals.turn]
    window_length = 250
    missing_value = 0

    def compute(self, today, assets, out, turnover):
        out[:] = np.nanmean(turnover[-20:], axis=0) / np.nanmean(turnover, axis=0)


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


class MaxReturns(CustomFactor):
    inputs = [DailyReturns()]
    window_length = 20

    def compute(self, today, assets, out, r):
        out[:] = np.nanmax(r, axis=0)


def Risk(returns_length=2, window_length=244, annulization_factor=244):
    return AnnualizedVolatility(
        inputs=[Returns(window_length=returns_length)],
        window_length=window_length,
        annualization_factor=annulization_factor,
    )


def Beta(returns_length=2, window_length=244):
    benchmark = Equity.from_dict({
        'sid': 3623,
        'symbol': '000300.SH',
        'first_traded': pd.Timestamp('2004-12-31', tz='UTC'),
        'start_date': pd.Timestamp('2005-04-08', tz='UTC'),
        'exchange_info': ExchangeInfo('XSHG', 'XSHG', 'CN'),
    })

    return SimpleBeta(
        target=benchmark,
        returns_length=returns_length,
        regression_length=window_length,
    )


