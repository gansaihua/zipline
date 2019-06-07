import numpy as np
import pandas as pd
from logbook import Logger
from zipline.data.bundles.core import register
from trading_calendars import register_calendar_alias

from secdb.reader import (
    get_stock_meta,
    get_index_meta,
    get_stock_pricing,
    get_index_pricing,
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

    stocks = gen_stock_metadata(sids=None)
    indices = gen_index_metadata(sids=None)
    equities_meta = pd.concat([stocks, indices])

    splits = []
    daily_bar_writer.write(_pricing_iter(equities_meta, calendar, splits),
                           show_progress=show_progress)

    adjustment_writer.write(
        splits=pd.concat(splits, ignore_index=True)
        if len(splits) > 0 else None,
    )

    asset_db_writer.write(
        equities=equities_meta,
        exchanges=gen_exchanges_metadata(),
    )


def gen_stock_metadata(sids=None):
    data = get_stock_meta(sids).rename(columns={
        'Symbol': 'symbol',
        'Name_': 'asset_name',
        'StartDate': 'start_date',
        'EndDate': 'end_date',
        })

    data['end_date'] = data['end_date'].fillna('2050-1-1')
    data['exchange'] = 'XSHG'
    return data


def gen_index_metadata(sids=None):
    data = get_index_meta(sids).rename(columns={
        'Symbol': 'symbol',
        'Name_': 'asset_name',
        'StartDate': 'start_date',
        'EndDate': 'end_date',
        })

    data['end_date'] = data['end_date'].fillna('2050-1-1')
    data['exchange'] = 'XSHG'
    return data


def gen_exchanges_metadata():
    return pd.DataFrame.from_records([
        {'exchange': 'XSHG', 'country_code': 'CN'},
        {'exchange': 'SSE', 'country_code': 'CN'},
        {'exchange': 'SZSE', 'country_code': 'CN'},
        {'exchange': 'CFFEX', 'country_code': 'CN'},
    ])


def _pricing_iter(equities_meta, calendar, splits):
    fields = ['P_OPEN', 'P_HIGH', 'P_LOW', 'P_CLOSE', 'P_VOLUME']

    for sid, row in equities_meta.iterrows():
        asset_class = row['Asset']
        if asset_class == 'equity':
            data = get_stock_pricing(sid, fields+['P_FACTOR']).rename(columns=lambda x: x.lower()[2:])
            if data.empty:
                log.info("{} don't have ohlcv".format(sid))
                continue

            parse_splits(sid, data, splits)
        elif asset_class == 'index':
            data = get_index_pricing(sid, fields).rename(columns=lambda x: x.lower()[2:])
            if data.empty:
                log.info("{} don't have ohlcv".format(sid))
                continue

            data['open'] = data['open'].fillna(data['close'])
            data['high'] = data['high'].fillna(data['close'])
            data['low'] = data['low'].fillna(data['close'])
            data['volume'] = data['volume'].fillna(9999)
        else:
            raise Exception('Not supported asset')

        sessions = calendar.sessions_in_range(data.index[0], data.index[-1])
        data = data.reindex(sessions.tz_localize(None))

        data['volume'] = data['volume'].fillna(0)
        mask = data['volume'] == 0
        data.loc[mask, ['open', 'high', 'low', 'close']] = 0

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


register_calendar_alias("SSE", "XSHG")
register_calendar_alias("SZSE", "XSHG")
register_calendar_alias("CFFEX", "XSHG")
