from secdb.reader import get_sid, get_index_pricing


def get_benchmark_returns(symbol='000300.SH', start=None, end=None, periods=1):
    """
    获取指数基准收益率，默认沪深300

    Parameters
    ----------
    symbol : str
        Benchmark symbol for which we're getting the returns.
    
    return : pd.Seris
    """
    sid = get_sid(symbol)
    return get_index_pricing(
        sid, 'P_CLOSE', start, end
    )['P_CLOSE'].tz_localize('UTC').pct_change(periods).iloc[periods:]
