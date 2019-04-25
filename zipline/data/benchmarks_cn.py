from secdata.reader import get_index_pricing


def get_benchmark_returns(symbol='000300.SH', start=None, end=None, periods=1):
    """
    获取指数基准收益率，默认沪深300

    Parameters
    ----------
    symbol : str
        Benchmark symbol for which we're getting the returns.
    
    return : pd.Seris
    """
    df = get_index_pricing(symbol, start, end)['close']
    return df.tz_localize('UTC').pct_change(periods).iloc[periods:]
