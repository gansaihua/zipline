import pandas as pd
from .constant import (
    DEFAULT_COUNTRY,
    DEFAULT_CALENDAR,
    DEFAULT_DATA_PORTAL,
    DEFAULT_ASSET_FINDER,
)
from .utils import ensure_list


def symbols(assets,
            symbol_reference_date=None,
            handle_missing='raise',
            roll_style='volume',
            adjustment=None,
            offset=0):
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
        adjustment ({'mul', 'add', None}, default None)
            String specifying how to adjust futures prices
        roll_type ({'volume', 'calendar'}, default 'volume')
            String specifying how to roll futures contract
        offset int, default 0
            Integer specifying the n-th active futures contract
    Returns:
    list of Asset objects – The symbols that were requested.
    """
    if symbol_reference_date is not None:
        asof_date = pd.Timestamp(symbol_reference_date, tz='UTC')
    else:
        asof_date = pd.Timestamp('today', tz='UTC')

    finder = DEFAULT_ASSET_FINDER
    assets = ensure_list(assets)

    country = DEFAULT_COUNTRY
    matches, missing = finder.lookup_generic(assets, asof_date, country)

    ret = ensure_list(matches)
    for s in ensure_list(missing):
        try:
            cf = finder.create_continuous_future(s,
                                                 offset,
                                                 roll_style,
                                                 adjustment)
        except:
            if handle_missing == 'raise':
                raise Exception('Not found: {}'.format(s))
            elif handle_missing == 'log':
                print('Not found: {}'.format(s))
            elif handle_missing == 'ignore':
                continue

        ret.append(cf)

    return ret


def prices(assets,
           start,
           end,
           frequency='daily',
           price_field='price',
           symbol_reference_date=None,
           start_offset=0,
           join='inner'):
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
    data_portal = DEFAULT_DATA_PORTAL
    calendar = DEFAULT_CALENDAR

    start = pd.Timestamp(start, tz='utc')
    if not calendar.is_session(start):
        # this is not a trading session, advance to the next session
        start = calendar.minute_to_session_label(
            start,
            direction='next',
        )

    if start_offset:
        start -= start_offset * calendar.day

    end = pd.Timestamp(end, tz='utc')
    if not calendar.is_session(end):
        # this is not a trading session, advance to the previous session
        end = calendar.minute_to_session_label(
            end,
            direction='previous',
        )

    assets = symbols(assets, symbol_reference_date=symbol_reference_date)

    if join == 'inner':
        for asset in assets:
            if asset.start_date > start:
                start = asset.start_date
            if asset.end_date < end:
                end = asset.end_date

    dates = calendar.sessions_in_range(start, end)
    df = data_portal.get_history_window(
        assets, end, len(dates), '1d', price_field, frequency
    )

    if len(assets) > 1:
        return df
    else:
        return df.iloc[:, 0]


def get_pricing(assets,
                start_date='2020-01-03',
                end_date='2020-04-03',
                symbol_reference_date=None,
                frequency='daily',
                fields=None,
                handle_missing='raise',
                start_offset=0,
                roll_style='volume',
                adjustment=None,
                offset=0):
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
    if fields is None:
        fields = ['open', 'high', 'low', 'close', 'volume', 'open_interest']

    assets = symbols(assets,
                     symbol_reference_date=symbol_reference_date,
                     handle_missing=handle_missing,
                     offset=offset,
                     roll_style=roll_style,
                     adjustment=adjustment)
    n_assets = len(assets)
    assert n_assets >= 1, 'Must have at least one asset'

    fields = ensure_list(fields)
    n_fields = len(fields)
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
            return pd.DataFrame(d, columns=fields)
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


def get_contract(root_symbol, dt):
    """
    root_symbol: ContinuousFutures
    dt: str / pd.Timestamp
    Return: `Asset`
    """
    data_portal = DEFAULT_DATA_PORTAL
    dt = pd.Timestamp(dt, tz='utc')
    return data_portal._get_current_contract(root_symbol, dt)


def get_futures_chain(root_symbol, dt):
    """
    root_symbol: ContinuousFutures
    dt: str / pd.Timestamp
    Return: list of `Asset`
    """
    data_portal = DEFAULT_DATA_PORTAL
    dt = pd.Timestamp(dt, tz='utc')
    return data_portal.get_current_future_chain(root_symbol, dt)


def get_rolls(root_symbol, start, end, roll_style='volume', offset=0):
    """
    root_symbol: str
    start, end: str / pd.Timestamp
    Return: list of tuple (sid, datetime before which sid is active)
    """
    data_portal = DEFAULT_DATA_PORTAL
    start = pd.Timestamp(start, tz='utc')
    end = pd.Timestamp(end, tz='utc')
    return data_portal._roll_finders[roll_style].get_rolls(
        root_symbol,
        start,
        end,
        offset,
    )
