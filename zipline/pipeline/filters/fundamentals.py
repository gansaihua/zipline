import numpy as np
from zipline.pipeline.data import EquityPricing
from zipline.pipeline.data.fundamentals import Fundamentals
from zipline.pipeline.factors import CustomFactor


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
        out[:] = np.count_nonzero(v)


def TradableStocks(fast=0.9, slow=0.9):
    """
    可交易股票(过滤器)
    条件
    ----
        1. 该股票在过去200天内必须有180天的有效收盘价
        2. 并且在最近20天的每一天都正常交易(非停牌状态)
        以上均使用成交量来判定，成交量为0，代表当天停牌
    """
    is_stock = WindA()
    v20 = TradingDays(window_length=20)
    v200 = TradingDays(window_length=200)
    return is_stock & (v20 >= int(20*fast)) & (v200 >= int(200*slow))


def CSI50():
    return Fundamentals.memb_csi50.latest.eq(1)


def CSI300():
    return Fundamentals.memb_csi300.latest.eq(1)


def CSI500():
    return Fundamentals.memb_csi500.latest.eq(1)


def WindA():
    return Fundamentals.memb_a_share.latest.eq(1)