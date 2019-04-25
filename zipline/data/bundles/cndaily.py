import pandas as pd
from logbook import Logger
from trading_calendars import get_calendar
from zipline.data.bundles.core import register
from secdata.utils import sanitize_ohlcv
from secdata.reader import read_stkcode, get_stock_pricing


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

    metadata = gen_asset_metadata([1, 2, 3])
    log.info('Stock numbers: {}'.format(len(metadata)))

    splits = []
    daily_bar_writer.write(_pricing_iter(metadata['sid'], splits))

    asset_db_writer.write(
        equities=metadata,
        exchanges=gen_exchange_info()
    )

    adjustment_writer.write(
        splits=pd.concat(splits, ignore_index=True)
        if len(splits) > 0 else None,
    )


def gen_asset_metadata(sids=None):
    data = read_stkcode(sid=sids).drop(
        ['start_date', 'end_date'], axis=1
    ).rename(columns={
        'name': 'asset_name',
        'first_traded': 'start_date',
        'last_traded': 'end_date',
    })

    data['exchange'] = 'XSHG'
    data['auto_close_date'] = data['end_date'].values + pd.Timedelta(days=1)

    return data


def gen_exchange_info():
    exchange_records = [('XSHG', 'CN')]
    column_names = ['exchange', 'country_code']
    return pd.DataFrame(exchange_records, columns=column_names)


def _pricing_iter(sids, splits):
    for sid in sids:
        data = get_stock_pricing(sid, post_func=sanitize_ohlcv, zfill=True)

        if data.empty:
            raise "{} don't have ohlcv".format(sid)

        parse_splits(data, splits)
        
        yield sid, data


def parse_splits(data, out):
    df = data[['sid', 'adjfactor']].reset_index()
    df.columns = ['effective_date', 'sid', 'ratio']

    df['ratio'] = df['ratio'].shift(1) / df['ratio']
    df = df[df['ratio']!=1].dropna()

    df['sid'] = df['sid'].astype('int64')

    out.append(df)
