import numpy as np
import pandas as pd
from zipline.assets import Equity
from zipline.assets.exchange_info import ExchangeInfo
from zipline.utils.numpy_utils import int64_dtype
from zipline.pipeline.data import (
    EquityPricing, EquityMetrics, Fundamentals,
)
from .factor import CustomFactor
from .statistical import SimpleBeta
from .basic import Returns, DailyReturns, AnnualizedVolatility


class MaxReturns(CustomFactor):
    inputs = [DailyReturns()]
    window_length = 20

    def compute(self, today, assets, out, r):
        out[:] = np.nanmax(r, axis=0)


def Risk(returns_length=2, window_length=250, annulization_factor=250):
    return AnnualizedVolatility(
        inputs=[Returns(window_length=returns_length)],
        window_length=window_length,
        annualization_factor=annulization_factor,
    )


def Beta(returns_length=2, window_length=250):
    benchmark = Equity.from_dict({
        'sid': 3623,
        'symbol': '000300.SH',
        'start_date': pd.Timestamp('2004-12-31', tz='UTC'),
        'exchange_info': ExchangeInfo('XSHG', 'XSHG', 'CN'),
    })

    return SimpleBeta(
        target=benchmark,
        returns_length=returns_length,
        regression_length=window_length,
    )


class Illiquidity(CustomFactor):
    inputs = [DailyReturns(), EquityMetrics.amount]
    window_length = 20
    missing_value = 0

    def compute(self, today, assets, out, r, amount):
        out[:] = np.nanmean(np.abs(r) / amount, axis=0)


class Turnover(CustomFactor):
    inputs = [EquityMetrics.turnover]
    window_length = 20
    missing_value = 0

    def compute(self, today, assets, out, turnover):
        out[:] = np.nanmean(turnover, axis=0)


class AbnormalTurnover(CustomFactor):
    inputs = [EquityMetrics.turnover]
    window_length = 250
    missing_value = 0

    def compute(self, today, assets, out, turnover):
        out[:] = np.nanmean(turnover[-20:], axis=0) / np.nanmean(turnover, axis=0)


class Momentum(CustomFactor):
    '''Default: price return changes from -250 days to -20 days
    '''
    inputs = [EquityPricing.close]
    params = {'t0': 20} # to which day
    window_length = 250 # from which day

    def compute(self, today, assets, out, close, t0):
        out[:] = close[-t0] / close[0] - 1


class PositiveDivide(CustomFactor):
    window_length = 1
    window_safe = True

    def compute(self, today, assets, out, nom, denom):
        out[:] = nom[-1] / denom[-1]

        mask = denom[-1] <= 0
        out[mask] = np.nan


class YoYGrowth(CustomFactor):
    window_length = 300
    params = {'nyears': 1}  # to which day

    def compute(self, today, assets, out, asof_dates, values, nyears):
        nobs = 4 * nyears + 1

        for col_ix in range(asof_dates.shape[1]):
            _, idx = np.unique(asof_dates[:, col_ix], return_index=True)
            quarterly_values = values[idx, col_ix]

            if len(quarterly_values) < nobs:
                ret = np.nan
            else:
                ret = quarterly_values[-1] / quarterly_values[-nobs] - 1

            out[col_ix] = ret


class PreviousYear(CustomFactor):
    window_length = 300
    params = {'nyears': 1}  # to which day

    def compute(self, today, assets, out, asof_dates, values, nyears):
        nobs = 4 * nyears + 1

        for col_ix in range(asof_dates.shape[1]):
            _, idx = np.unique(asof_dates[:, col_ix], return_index=True)
            quarterly_values = values[idx, col_ix]

            if len(quarterly_values) < nobs:
                ret = np.nan
            else:
                ret = quarterly_values[-nobs]

            out[col_ix] = ret
