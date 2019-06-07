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


###################
# Valuation Factors
###################
class PERatio(PositiveDivide):
    inputs = [
        Fundamentals.mkt_cap_ard,
        Fundamentals.net_profit_is
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

class PBRatio(PositiveDivide):
    inputs = [
        Fundamentals.mkt_cap_ard,
        Fundamentals.tot_equity
    ]

def EarningsYield():
    return Fundamentals.net_profit_is.latest / Fundamentals.mkt_cap_ard.latest

def BookYield():
    return Fundamentals.tot_equity.latest / Fundamentals.mkt_cap_ard.latest

def CashFlowYield():
    return Fundamentals.net_cash_flows_oper_act.latest / Fundamentals.mkt_cap_ard.latest

def SalesYield():
    return Fundamentals.tot_oper_rev.latest / Fundamentals.mkt_cap_ard.latest

def CashYield():
    return Fundamentals.cash_recp_sg_and_rs.latest / Fundamentals.mkt_cap_ard.latest


###################
# Solvency Factors
###################
class CashRatio(CustomFactor):
    inputs = [
        Fundamentals.monetary_cap,
        Fundamentals.tot_cur_liab,
    ]
    window_length = 1

    def compute(self, today, assets, out, n1, d1):
        out[:] = n1[-1] / d1[-1]

class QuickRatio(CustomFactor):
    inputs = [
        Fundamentals.monetary_cap,
        Fundamentals.acct_rcv,
        Fundamentals.tot_cur_liab,
    ]
    window_length = 1

    def compute(self, today, assets, out, n1, n2, d1):
        out[:] = np.nansum(n1[-1], n2[-1], axis=1) / d1[-1]

def CurrentRatio():
    return Fundamentals.tot_cur_assets.latest / Fundamentals.tot_cur_liab.latest

def CashFlowFromOperatingRatio():
    return Fundamentals.net_cash_flows_oper_act.latest / Fundamentals.tot_cur_liab.latest


###################
# Operating Efficiency Factors
###################
def TotalAssetTurnover():
    return Fundamentals.tot_oper_rev.latest / Fundamentals.tot_assets.latest

def EquityTurnover():
    return Fundamentals.tot_oper_rev.latest / Fundamentals.tot_equity.latest

def FixedAssetTurnover():
    pass

def InventoryTurnover():
    return Fundamentals.tot_oper_cost.latest / Fundamentals.inventories.latest

def ReceivablesTurnover():
    return Fundamentals.tot_oper_rev.latest / Fundamentals.acct_rcv.latest


###################
# Operating Profitability Factors
###################
def GrossProfitMargin():
    return Fundamentals.opprofit.latest / Fundamentals.tot_oper_rev.latest

def NetProfitMargin():
    return Fundamentals.net_profit_is.latest / Fundamentals.tot_oper_rev.latest

def ROA():
    return Fundamentals.net_profit_is.latest / Fundamentals.tot_assets.latest

def ROE():
    return Fundamentals.net_profit_is.latest / Fundamentals.tot_equity.latest


###################
# Financial Risk Factors
###################
def DebtToEquityRatio():
    return -Fundamentals.tot_liab.latest / Fundamentals.tot_equity.latest

def TotalDebtRatio():
    return -Fundamentals.tot_liab.latest / Fundamentals.tot_assets.latest

def FinancialLeverage():
    return  -Fundamentals.tot_assets.latest / Fundamentals.tot_equity.latest


###################
# Liquidity Risk Factors
###################
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


###################
# Growth Factors
###################
def TotalAssetGrowth():
    return YoYGrowth(inputs=[Fundamentals.tot_assets_asof, Fundamentals.tot_assets])

def Investment():
    return total_asset_growth()

def TotalEquityGrowth():
    return YoYGrowth(inputs=[Fundamentals.tot_equity_asof, Fundamentals.tot_equity])

def RevenueGrowth():
    return YoYGrowth(inputs=[Fundamentals.tot_oper_rev_asof, Fundamentals.tot_oper_rev])

def OperatingProfitGrowth():
    return YoYGrowth(inputs=[Fundamentals.opprofit_asof, Fundamentals.opprofit])

def NetProfitGrowth():
    return YoYGrowth(inputs=[Fundamentals.net_profit_is_asof, Fundamentals.net_profit_is])

def CFOGrowth():
    return YoYGrowth(inputs=[Fundamentals.net_cash_flows_oper_act_asof, Fundamentals.net_cash_flows_oper_act])

def EPSGrowth():
    return YoYGrowth(inputs=[Fundamentals.eps_basic_asof, Fundamentals.eps_basic])


###################
# Momentum Factors
###################
class Momentum(CustomFactor): 
    '''
    Default: price return changes from -244 days to -21 days
    '''
    inputs = [EquityPricing.close]
    params = {'t0': 21} # to which day
    window_length = 244 # from which day

    def compute(self, today, assets, out, close, t0):  
        out[:] = close[-t0] / close[0] - 1


###################
# Size Factors
###################
def MktCap():
    return -np.log(Fundamentals.mkt_cap_ashare.latest)

class Size(CustomFactor):
    inputs = [Fundamentals.mkt_cap_ard]
    window_length = 1
    window_safe = True

    def compute(self, today, assets, out, mkt_cap):
        out[:] = -np.log(mkt_cap[-1])

def NonlinearSize(regression_length=250):
    reg = np.power(Size(), 3).linear_regression(Size(), regression_length)
    return reg.alpha

def Price():
    return -np.log(EquityPricing.close.latest)


###################
# Risk Factors
###################
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
