import numpy as np
import pandas as pd
from zipline.pipeline.factors import CustomFactor
from zipline.pipeline.data import EquityPricing, Fundamentals

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
