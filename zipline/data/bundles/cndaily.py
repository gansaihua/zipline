import numpy as np
import pandas as pd
from logbook import Logger
from zipline.data.bundles.core import register
from trading_calendars import register_calendar_alias

from secdb.reader import (
    get_stock_meta,
    get_index_meta,
    get_futures_meta,
    get_futures_root_symbols,
    get_ohlcv,
    get_stock_pricing,
    get_end_session,
)


log = Logger(__name__)

@register('cndaily', calendar_name='XSHG', end_session=get_end_session())
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

    stocks = gen_stock_metadata(sids=[])
    indices = gen_index_metadata(sids=[])
    futures = gen_futures_metadata(sids=None)

    equities = pd.concat([stocks, indices])
    assets = pd.concat([equities, futures])

    splits = []
    daily_bar_writer.write(_pricing_iter(assets, calendar, splits),
                           show_progress=show_progress)

    adjustment_writer.write(
        splits=pd.concat(splits, ignore_index=True)
        if len(splits) > 0 else None,
    )

    asset_db_writer.write(
        equities=equities,
        futures=futures,
        exchanges=gen_exchanges_metadata(),
        root_symbols=gen_futures_root_symbols(),
    )


def gen_stock_metadata(sids=None):
    data = get_stock_meta(sids).rename(columns={
        'Symbol': 'symbol',
        'Name_': 'asset_name',
        'StartDate': 'start_date',
        'EndDate': 'end_date',
        'Exchange': 'exchange',
        })

    data['end_date'] = data['end_date'].fillna(pd.Timestamp('2050-1-1'))
    data['exchange'] = data['exchange'].fillna('XSHG')
    data['auto_close_date'] = data['end_date'].values + pd.Timedelta(days=1)
    return data


def gen_index_metadata(sids=None):
    data = get_index_meta(sids).rename(columns={
        'Symbol': 'symbol',
        'Name_': 'asset_name',
        'StartDate': 'start_date',
        'EndDate': 'end_date',
        'Exchange': 'exchange',
        })

    data['end_date'] = data['end_date'].fillna(pd.Timestamp('2050-1-1'))
    data['exchange'] = data['exchange'].fillna('XSHG')
    data['auto_close_date'] = data['end_date'].values + pd.Timedelta(days=1)
    return data


def gen_futures_metadata(sids=None):
    data = get_futures_meta(sids).rename(columns={
        'Symbol': 'symbol',
        'Name_': 'asset_name',
        'StartDate': 'start_date',
        'EndDate': 'end_date',
        'Exchange': 'exchange',
        'RootSymbol': 'root_symbol',
        'NoticeDate': 'notice_date',
        'TickSize': 'tick_size',
        'Multiplier': 'multiplier',
        })

    data['end_date'] = data['end_date'].fillna(pd.Timestamp('2050-1-1'))
    data['exchange'] = data['exchange'].fillna('XSHG')
    data['expiration_date'] = data['end_date']
    data['auto_close_date'] = data['end_date']
    return data


def gen_futures_root_symbols():
    data = get_futures_root_symbols().rename(columns={
        'Exchange': 'exchange',
        'RootSymbol': 'root_symbol',
        })

    data['root_symbol_id'] = range(100000, len(data)+100000)

    return data


def gen_exchanges_metadata():
    return pd.DataFrame.from_records([
        {'exchange': 'XSHG', 'country_code': 'CN'},
        {'exchange': 'XSHE', 'country_code': 'CN'},
        {'exchange': 'DCE', 'country_code': 'CN'},
        {'exchange': 'INE', 'country_code': 'CN'},
        {'exchange': 'ZCE', 'country_code': 'CN'},
        {'exchange': 'CFFEX', 'country_code': 'CN'},
        {'exchange': 'SHFE', 'country_code': 'CN'},
    ])


def _pricing_iter(equities_meta, calendar, splits):
    fields = ['P_OPEN', 'P_HIGH', 'P_LOW', 'P_CLOSE', 'P_VOLUME']

    for sid, row in equities_meta.iterrows():
        print(sid)
        asset_class = row['Asset']
        if asset_class == 'equity':
            data = get_stock_pricing(sid, fields+['P_FACTOR']).rename(columns=lambda x: x.lower()[2:])
            if data.empty:
                log.info("{} don't have ohlcv".format(sid))
                continue

            data['volume'] = data['volume'].fillna(0)
            data = data.fillna(method='ffill')

            parse_splits(sid, data, splits)
        elif asset_class in ('index', 'futures'):
            data = get_ohlcv(sid)
            if data.empty:
                log.info("{} don't have ohlcv".format(sid))
                continue

            data['open'] = data['open'].fillna(data['close'])
            data['high'] = data['high'].fillna(data['close'])
            data['low'] = data['low'].fillna(data['close'])
        else:
            raise Exception('Not supported asset')

        sessions = calendar.sessions_in_range(data.index[0], data.index[-1])
        data = data.reindex(sessions.tz_localize(None), method='ffill')

        # mask = data['volume'] == 0
        # data.loc[mask, ['open', 'high', 'low', 'close']] = 0

        limit = np.iinfo(np.uint32).max
        mask = data['volume'] > limit
        data.loc[mask, 'volume'] = limit - 1

        yield sid, data


def parse_splits(sid, data, out):
    df = data[['factor']].reset_index()
    df.columns = ['effective_date', 'ratio']
    df['sid'] = sid

    df['ratio'] = df['ratio'].shift(1) / df['ratio']
    df = df[df['ratio']!=1].dropna()

    out.append(df)


register_calendar_alias("XSHE", "XSHG")
register_calendar_alias("DCE", "XSHG")
register_calendar_alias("INE", "XSHG")
register_calendar_alias("ZCE", "XSHG")
register_calendar_alias("CFFEX", "XSHG")
register_calendar_alias("SHFE", "XSHG")
