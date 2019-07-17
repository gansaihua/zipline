from zipline.utils.numpy_utils import float64_dtype

from .dataset import Column, DataSet


class EquityMetrics(DataSet):
    open = Column(float64_dtype)
    high = Column(float64_dtype)
    low = Column(float64_dtype)
    close = Column(float64_dtype)
    volume = Column(float64_dtype)
    amount = Column(float64_dtype)
    vwap = Column(float64_dtype)
    turnover = Column(float64_dtype)
    factor = Column(float64_dtype)
    pe = Column(float64_dtype)
    pb = Column(float64_dtype)
    pcf = Column(float64_dtype)
    ps = Column(float64_dtype)
    shares_tot = Column(float64_dtype)
    shares_liq = Column(float64_dtype)
    mkt_cap_tot = Column(float64_dtype)
    mkt_cap_liq = Column(float64_dtype)
