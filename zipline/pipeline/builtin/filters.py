import numpy as np
import pandas as pd

from zipline.pipeline.data import EquityPricing, Fundamentals
from zipline.pipeline.factors import CustomFactor, AverageDollarVolume


def TradableStocks():
    """
    可交易股票(过滤器)
    条件
    ----
        1. 该股票在过去200天内必须有180天的有效收盘价
        2. 并且在最近20天的每一天都正常交易(非停牌状态)
        以上均使用成交量来判定，成交量为0，代表当天停牌
    """
    class TradingDays(CustomFactor):
        """
        窗口期内所有有效交易天数
        Parameters
        ----------
        window_length : 整数
            统计窗口数量
        Notes
        -----
        如成交量大于0,表示当天交易
        """
        inputs = [EquityPricing.volume]

        def compute(self, today, assets, out, v):
            out[:] = np.count_nonzero(v, 0)

    v20 = TradingDays(window_length=20)
    v200 = TradingDays(window_length=200)
    return (v20 >= 20) & (v200 >= 180)


def TopAverageAmount(N=30, window_length=21):
    """
    成交额前N位过滤
    参数
    _____
    N：整数
        取前N位。默认前30。
    window_length：整数
        窗口长度。默认21个交易日。
    returns
    -------
    zipline.pipeline.Filter
        成交额前N位股票过滤器
    备注
    ----
        以窗口长度内平均成交额为标准
    """
    return AverageDollarVolume(window_length=window_length).top(N)


def LargeCap():
    return Fundamentals.csi300_constituents.latest.eq(1)


def MidCap():
    return Fundamentals.csi500_constituents.latest.eq(1)
