from zipline.assets import Asset
from zipline.data.benchmarks_cn import get_benchmark_returns
from zipline.research.constant import (
    DEFAULT_COUNTRY,
    DEFAULT_CALENDAR,
    DEFAULT_DATA_PORTAL,
    DEFAULT_ASSET_FINDER,
)

from secdata.utils import ensure_list
from secdata.reader import get_stock_pricing

import pandas as pd


def symbols(symbols_,
            symbol_reference_date=None,
            country=None,
            handle_missing='log'):
    """
    Convert a or a list of str and int into a list of Asset objects.
    
    Parameters:	
        symbols_ (str, int or iterable of str and int)
            Passed strings are interpreted as ticker symbols and 
            resolveXSHGd relative to the date specified by symbol_reference_date.
        symbol_reference_date (str or pd.Timestamp, optional)
            String or Timestamp representing a date used to resolve symbols 
            that have been held by multiple companies. Defaults to the current time.
        handle_missing ({'raise', 'log', 'ignore'}, optional)
            String specifying how to handle unmatched securities. Defaults to ‘log’.

    Returns:	

    list of Asset objects – The symbols that were requested.
    """
    if isinstance(symbols_, Asset):
        return [symbols_]

    if country is None:
        country = DEFAULT_COUNTRY
    
    symbols_ = ensure_list(symbols_)

    if symbol_reference_date is not None:
        asof_date = pd.Timestamp(symbol_reference_date, tz='UTC')
    else:
        asof_date = pd.Timestamp('today', tz='UTC')

    ret = []
    finder = DEFAULT_ASSET_FINDER
    for symbol in symbols_:
        if isinstance(symbol, str):
            res = finder.lookup_symbol(
                symbol, asof_date, country_code=country
            )
            ret.append(res)
        elif isinstance(symbol, int):
            res = finder.retrieve_asset(symbol)
            ret.append(res)
        elif isinstance(symbol, Asset):
            ret.append(symbol)

    return ret

    
def prices(assets,
           start,
           end,
           frequency='daily',
           price_field='price',
           symbol_reference_date=None,
           start_offset=0):
    """
    Parameters:	
        assets (int/str/Asset or iterable of same)
            Identifiers for assets to load. Integers are interpreted as sids. 
            Strings are interpreted as symbols.
        start (str or pd.Timestamp)
            Start date of data to load.
        end (str or pd.Timestamp)
            End date of data to load.
        frequency ({'minute', 'daily'}, optional)
            Frequency at which to load data. Default is ‘daily’.
        price_field ({'open', 'high', 'low', 'close', 'price'}, optional)
            Price field to load. ‘price’ produces the same data as ‘close’, 
            but forward-fills over missing data. Default is ‘price’.
        symbol_reference_date (pd.Timestamp, optional)
            Date as of which to resolve strings as tickers. Default is the current day.
        start_offset (int, optional)
            Number of periods before start to fetch. Default is 0. 
            This is most often useful for calculating returns. 
    """
    assert frequency == 'daily', "Only support frequency == 'daily'"
    
    valid_fields = ('open', 'high', 'low', 'close', 'price', 'volume')
    assert isinstance(price_field, str), '只接受单一字段，有效字段为{}'.format(valid_fields)

    data_portal = DEFAULT_DATA_PORTAL
    calendar = DEFAULT_CALENDAR

    start = pd.Timestamp(start, tz='utc')
    if not calendar.is_session(start):
        # this is not a trading session, advance to the next session
        start = calendar.minute_to_session_label(
            start,
            direction='previous',
        )

    end = pd.Timestamp(end, tz='utc')
    if not calendar.is_session(end):
        # this is not a trading session, advance to the previous session
        end = calendar.minute_to_session_label(
            end,
            direction='previous',
        )
    
    if start_offset:
        start -= start_offset * calendar.day

    dates = calendar.all_sessions
    bar_count = dates.get_loc(end) - dates.get_loc(start) + 1

    assets = symbols(assets, symbol_reference_date=symbol_reference_date)
    
    return data_portal.get_history_window(
        assets, end, bar_count, '1d', price_field, 'daily'
    )


def returns(assets,
            start,
            end,
            periods=1,
            frequency='daily',
            price_field='price',
            symbol_reference_date=None):

    df = prices(assets, 
                start, 
                end, 
                frequency, 
                price_field, 
                symbol_reference_date, 
                periods)
    
    return df.pct_change(periods).iloc[periods:]


def benchmark_returns(symbol, start, end):
    calendar = DEFAULT_CALENDAR

    start_date = pd.Timestamp(start_date, tz='utc')
    if not calendar.is_session(start):
        # this is not a trading session, advance to the next session
        start = calendar.minute_to_session_label(
            start,
            direction='previous',
        )

    end_date = pd.Timestamp(end_date, tz='utc')
    if not calendar.is_session(end):
        # this is not a trading session, advance to the previous session
        end = calendar.minute_to_session_label(
            end,
            direction='previous',
        )
    
    start -= calendar.day

    return get_benchmark_returns(symbol, start, end)
