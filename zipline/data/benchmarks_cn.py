from secdb.reader import get_pricing
import pandas as pd


def get_benchmark_returns(symbol='000300.SH', start=None, end=None, periods=1):
    """
    获取指数基准收益率，默认沪深300

    Parameters
    ----------
    symbol : str
        Benchmark symbol for which we're getting the returns.
    
    return : pd.Seris
    """
    df = get_pricing(symbol, start, end)    
    return df.pct_change(periods).iloc[periods:]


def benchmark_returns(symbol, start=None, end=None, periods=1):
    from trading_calendars import get_calendar
    calendar = get_calendar('XSHG')
    
    if start is not None:   
        start = pd.Timestamp(start, tz='utc')
        if not calendar.is_session(start):
            # this is not a trading session, advance to the next session
            start = calendar.minute_to_session_label(
                start,
                direction='next',
            )
        start -= calendar.day
    
    if end is not None:
        if not calendar.is_session(end):
            # this is not a trading session, advance to the previous session
            end = calendar.minute_to_session_label(
                end,
                direction='previous',
            )
    
    return get_benchmark_returns(symbol, start, end, periods)