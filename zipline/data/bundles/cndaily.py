import pandas as pd
from logbook import Logger
from zipline.data.bundles.core import register
from trading_calendars import register_calendar_alias
from secdata.utils import (
    sanitize_stock_ohlcv,
    sanitize_index_ohlcv,
    sanitize_futures_ohlcv,
)
from secdata.reader import (
    read_stkcode,
    read_idxcode,
    read_futcode,
    get_pricing,
    get_asset_class,
)


log = Logger(__name__)

@register('cndaily', calendar_name='XSHG')
def cndaily_bundle(environ,
                   asset_db_writer,
                   minute_bar_writer,
                   daily_bar_writer,
                   adjustment_writer,
                   calendar,
                   start_session,
                   end_session,
                   cache,
                   show_progress,
                   output_dir):

    stocks = gen_stock_metadata()
    indices = gen_index_metadata()
    futures = gen_futures_metadata()
    root_symbols = gen_futures_root_symbols(futures)
    exchanges = gen_exchanges_metadata()

    equities = pd.concat([stocks, indices])
    full_assets = pd.concat([equities, futures])

    asset_db_writer.write(
        equities=equities,
        futures=futures,
        root_symbols=root_symbols,
        exchanges=exchanges,
    )

    splits = []
    daily_bar_writer.write(_pricing_iter(full_assets.index, splits),
                           show_progress=show_progress)

    adjustment_writer.write(
        splits=pd.concat(splits, ignore_index=True)
        if len(splits) > 0 else None,
    )


def gen_stock_metadata(sids=None):
    data = read_stkcode(sid=sids).drop('end_date', axis=1).rename(
        columns={'last_traded': 'end_date'})

    data['auto_close_date'] = data['end_date'].values + pd.Timedelta(days=1)

    return data.set_index('sid')


def gen_index_metadata(sids=None):
    data = read_idxcode(sid=sids).drop('end_date', axis=1).rename(
        columns={'last_traded': 'end_date'})

    data['exchange'] = 'XSHG'
    data['auto_close_date'] = data['end_date'].values + pd.Timedelta(days=1)

    return data.set_index('sid')


def gen_futures_metadata(sids=None):
    data = read_futcode(sid=sids).rename(columns={
        'end_date': 'expiration_date',
        'last_traded': 'end_date',
    })

    data['auto_close_date'] = data['end_date'].values + pd.Timedelta(days=1)

    return data.set_index('sid')


def gen_futures_root_symbols(data):
    data = data[['root_symbol', 'exchange']].drop_duplicates()
    data['root_symbol_id'] = range(len(data))

    return data


def gen_exchanges_metadata():
    return pd.DataFrame.from_records([
        {'exchange': 'XSHG', 'country_code': 'CN'},
        {'exchange': 'SSE', 'country_code': 'CN'},
        {'exchange': 'SZSE', 'country_code': 'CN'},
        {'exchange': 'CFFEX', 'country_code': 'CN'},
    ])


def _pricing_iter(sids, splits):
    for sid in sids:
        asset_class = get_asset_class(sid)

        if asset_class == 'stock':
            f = sanitize_stock_ohlcv
        elif asset_class == 'index':
            f = sanitize_index_ohlcv
        elif asset_class == 'futures':
            f = sanitize_futures_ohlcv

        data = get_pricing(sid, post_func=f, asset_class=asset_class)

        if data.empty:
            continue
            # raise "{} don't have ohlcv".format(sid)

        if asset_class == 'stock':
            parse_splits(data, splits)
        
        yield sid, data


def parse_splits(data, out):
    df = data[['sid', 'adjfactor']].reset_index()
    df.columns = ['effective_date', 'sid', 'ratio']

    df['ratio'] = df['ratio'].shift(1) / df['ratio']
    df = df[df['ratio']!=1].dropna()

    df['sid'] = df['sid'].astype('int64')

    out.append(df)


register_calendar_alias("SSE", "XSHG")
register_calendar_alias("SZSE", "XSHG")
register_calendar_alias("CFFEX", "XSHG")
