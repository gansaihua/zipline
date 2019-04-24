import pandas as pd
from logbook import Logger
from trading_calendars import get_calendar
from zipline.data.bundles.core import register
from secdb.reader import (
    fetch_index_meta,
    fetch_stock_meta,
    fetch_prices,
    get_pricing,
    fetch_dividends,
    symbol_to_sid,
)


log = Logger('cndaily')

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
    """
    Build a zipline data bundle from the cnstock dataset.
    """
    metadata = gen_asset_metadata()
    log.info('Stock numbers: {}'.format(len(metadata)))
    
    asset_db_writer.write(
        equities=metadata, 
        exchanges=gen_exchange_info()
    )

    splits, dividends = [], []
    daily_bar_writer.write(
        _pricing_iter(
            metadata['symbol'],
            splits,
            dividends), 
        show_progress=show_progress,
    )

    adjustment_writer.write(
        splits=pd.concat(splits, ignore_index=True) if len(splits) > 0 else None,
        dividends=pd.concat(dividends, ignore_index=True) if len(dividends) > 0 else None,
    )


def gen_asset_metadata():
    df1 = fetch_stock_meta()
    df2 = fetch_index_meta()
    
    df = pd.concat([df1, df2], sort=True)

    #_symbols_for_testing = ['000001', '000002']
    #df = df[df['symbol'].isin(_symbols_for_testing)]
    
    return df


def gen_exchange_info():
    exchange_records = [('XSHG', 'CN'),
                        ('SZSE', 'CN'),
                        ('SSE', 'CN')]
    column_names = ['exchange', 'country_code']
    return pd.DataFrame(exchange_records, columns=column_names)    


def _pricing_iter(symbols, splits, dividends):
    for symbol in symbols:
        sid = symbol_to_sid(symbol)
        
        # We prefer get stock prices from Quote
        # Because it update more frequently
        df = fetch_prices(symbol)
        if not df.empty:        
            raw_adjustment = fetch_dividends(symbol)
            parse_splits(sid, raw_adjustment, splits)
            parse_dividends(sid, raw_adjustment, dividends)
        else:
            df = get_pricing(
                sid, adj=False,
                fields=['open', 'high', 'low', 'close', 'volume'],
            )
            
        if df.empty: 
            continue
            
        # 数据填充，对齐日历
        calendar = get_calendar('XSHG')
        sessions = calendar.sessions_in_range(
            df.index[0], df.index[-1]
        )
        df = df.reindex(sessions, fill_value=0)        
        
        yield sid, df


def parse_splits(sid, data, out):
    df = pd.DataFrame({
        'ratio': 1 / (1 + data['splits'].add(data['stock_dividends'], fill_value=0)),
        'effective_date': data['ex_date'],
        'sid': sid
    })
    mask = (df['ratio'] != 1) & (df['ratio'].notnull())
    df = df.loc[mask]
    
    if not df.empty:
        out.append(df)


def parse_dividends(sid, data, out):
    df = pd.DataFrame({
        'declared_date': data['declared_date'],
        'ex_date': data['ex_date'],
        'pay_date': data['pay_date'],
        'record_date': data['record_date'],
        'sid': sid,
        'amount': data['dividends']
    })
    mask = (df['amount'] != 0) & (df['amount'].notnull())
    df = df.loc[mask]
    
    if not df.empty:
        out.append(df)