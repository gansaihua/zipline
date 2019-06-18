import os
import pandas as pd
from secdb.utils import ensure_list
from zipline.data.benchmarks_cn import get_benchmark_returns
from zipline.research.constant import (
    DEFAULT_COUNTRY,
    DEFAULT_CALENDAR,
    DEFAULT_DATA_PORTAL,
    DEFAULT_ASSET_FINDER,
)
from zipline.utils import paths as pth


def symbols(assets,
            symbol_reference_date=None,
            country=None,
            handle_missing='raise',
            adjustment=None):
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
        adjustment ({'mul', 'add', None}, optional)
            String specifying how to adjust futures prices
    Returns:	

    list of Asset objects – The symbols that were requested.
    """
    if country is None:
        country = DEFAULT_COUNTRY

    if symbol_reference_date is not None:
        asof_date = pd.Timestamp(symbol_reference_date, tz='UTC')
    else:
        asof_date = pd.Timestamp('today', tz='UTC')

    finder = DEFAULT_ASSET_FINDER
    assets = ensure_list(assets)

    matches, missing = finder.lookup_generic(assets, asof_date, country)

    ret = ensure_list(matches)
    for s in ensure_list(missing):
        try:
            ret.append(finder.create_continuous_future(
                s, 0, 'volume', adjustment))
        except:
            pass

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

    Data is returned as a pd.Series if a single asset is passed.
    Data is returned as a pd.DataFrame if multiple assets are passed.
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
            direction='next',
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

    df = data_portal.get_history_window(
        assets, end, bar_count, '1d', price_field, 'daily'
    )

    if len(assets) > 1:
        return df
    else:
        return df.iloc[:, 0]


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


def volumes(assets,
            start,
            end,
            frequency='daily',
            symbol_reference_date=None,
            start_offset=0):

    return prices(assets,
                  start,
                  end,
                  frequency,
                  'volume',
                  symbol_reference_date,
                  start_offset)


def log_prices(assets,
               start,
               end,
               frequency='daily',
               price_field='price',
               symbol_reference_date=None,
               start_offset=0):

    return np.log(prices(
        assets,
        start,
        end,
        frequency,
        price_field,
        symbol_reference_date,
        start_offset,
    ))


def log_returns(assets,
                start,
                end,
                periods=1,
                frequency='daily',
                price_field='price',
                symbol_reference_date=None):

    df = log_prices(assets,
                    start,
                    end,
                    frequency,
                    price_field,
                    symbol_reference_date,
                    periods)

    return df.diff(periods).iloc[periods:]


def get_contract(root_symbols, dt):
    cf = symbols(root_symbols)
    dt = pd.Timestamp(dt, tz='utc')
    data_portal = DEFAULT_DATA_PORTAL

    return data_portal.get_spot_value(
        cf, 'contract', dt, 'daily'
    )


def get_pricing(assets,
                start_date='2018-01-03',
                end_date='2019-01-03',
                symbol_reference_date=None,
                frequency='daily',
                fields=None,
                handle_missing='raise',
                start_offset=0,
                adjustment=None):
    '''
    Load a table of historical trade data.

    Parameters:
    :: assets (Object (or iterable of objects) convertible to Asset) – Valid input types are Asset, Integral, or basestring.
     In the case that the passed objects are strings, they are interpreted as ticker symbols
     and resolved relative to the date specified by symbol_reference_date.
    :: start_date (str or pd.Timestamp, optional) – String or Timestamp representing a start date or start intraday minute for the returned data.
     Defaults to ‘2013-01-03’.
    :: end_date (str or pd.Timestamp, optional) – String or Timestamp representing an end date or end intraday minute for the returned data.
     Defaults to ‘2014-01-03’.
    :: symbol_reference_date (str or pd.Timestamp, optional) – String or Timestamp representing a date used to resolve symbols
     that have been held by multiple companies. Defaults to the current time.
    :: frequency ({'daily'}, optional) – Resolution of the data to be returned.
    :: fields (str or list, optional) – String or list drawn from {‘price’, ‘open’, ‘high’, ‘low’, ‘close’, ‘volume’}.
      Default behavior is to return all fields.
    :: handle_missing ({'raise', 'log', 'ignore'}, optional) – String specifying how to handle unmatched securities. Defaults to ‘raise’.
    :: start_offset (int, optional) – Number of periods before start to fetch. Default is 0.

    Returns:
    pandas Panel/DataFrame/Series – The pricing data that was requested. See note below.
    Notes
    If a list of symbols is provided, data is returned in the form of a pandas Panel object with the following indices:
    items = fields
    major_axis = TimeSeries (start_date -> end_date)
    minor_axis = symbols

    If a string is passed for the value of symbols and fields is None or a list of strings,
     data is returned as a DataFrame with a DatetimeIndex and columns given by the passed fields.
    If a list of symbols is provided, and fields is a string,
     data is returned as a DataFrame with a DatetimeIndex and a columns given by the passed symbols.
    If both parameters are passed as strings, data is returned as a Series.
    '''
    assets = symbols(assets,
                     symbol_reference_date,
                     DEFAULT_COUNTRY,
                     handle_missing,
                     adjustment)

    if fields is None:
        fields = ['open', 'high', 'low', 'close', 'volume']

    assets = ensure_list(assets)
    fields = ensure_list(fields)

    n_assets = len(assets)
    n_fields = len(fields)

    assert n_assets >= 1, 'Must have at least one asset'
    assert n_fields >= 1, 'Must have at least one field'

    if n_fields > 1:
        d = dict()
        for field in fields:
            d.update({
                field: prices(
                    assets,
                    start_date,
                    end_date,
                    frequency,
                    field,
                    symbol_reference_date,
                    start_offset,
              )})
        if n_assets > 1:
            return pd.Panel(d)
        else:
            return pd.DataFrame(d)
    else:
        return prices(
                assets,
                start_date,
                end_date,
                frequency,
                fields[0],
                symbol_reference_date,
                start_offset,
            )


def get_backtest(backtest_id=None):
    backtest_dir = pth.zipline_path(['backtest'])

    if backtest_id:
        backtest_id = backtest_id.split(sep='.', maxsplit=2)[0]
        perf_file = '{}/{}.pkl'.format(backtest_dir, backtest_id)
        return pd.read_pickle(perf_file)

    try:
        candidates = os.listdir(backtest_dir)

        cdt = None
        for c in candidates:
            if not c.endswith('.pkl'): continue
            cpath = os.path.join(backtest_dir, c)
            if cdt is None or cdt < os.path.getmtime(cpath):
                cdt = os.path.getmtime(cpath)
                perf_file = cpath
        return pd.read_pickle(perf_file)
    except (ValueError, OSError) as e:
        if getattr(e, 'errno', errno.ENOENT) != errno.ENOENT:
            raise
        raise ValueError('{} Not Found!'.format(backtest_dir))


def benchmark_returns(symbol, start, end):
    calendar = DEFAULT_CALENDAR

    start = pd.Timestamp(start, tz='utc')
    if not calendar.is_session(start):
        # this is not a trading session, advance to the next session
        start = calendar.minute_to_session_label(
            start,
            direction='next',
        )

    end = pd.Timestamp(end, tz='utc')
    if not calendar.is_session(end):
        # this is not a trading session, advance to the previous session
        end = calendar.minute_to_session_label(
            end,
            direction='previous',
        )
    
    start -= calendar.day

    return get_benchmark_returns(symbol, start, end)
