import re
import pandas as pd
from tqdm import tqdm
from logbook import Logger
from trading_calendars import register_calendar_alias
from zipline.assets.futures import MONTH_TO_CMES_CODE
from zipline.data.bundles.core import register

from ._engine import ENGINE

log = Logger(__name__)


def _sanitize_root_symbol(root_symbol):
    # Add prefix `X` to root symbol which has length of 1.
    return '{:X>2}'.format(root_symbol)


def _sanitize_contract_symbol(symbol):
    m = re.match(r'^(\w{1,2}?)(\d{2})(\d{2})$', symbol)
    root_symbol, year, month = m.groups()
    root_symbol = _sanitize_root_symbol(root_symbol)
    month = MONTH_TO_CMES_CODE[int(month)]
    return root_symbol + month + year


def futures_root_symbols():
    sql = '''
    SELECT
    rs.symbol AS root_symbol,
    rs.name AS description,
    e.symbol AS exchange
    FROM `futures_rootsymbol` AS rs
    JOIN `exchange` AS e
    ON rs.exchange_id = e.id
    '''
    ret = pd.read_sql(sql, ENGINE)
    ret['root_symbol'] = ret['root_symbol'].apply(_sanitize_root_symbol)
    return ret


def futures_contracts():
    sql = '''
    SELECT
    c.id AS contract_id,
    c.symbol,
    c.name AS asset_name,
    contract_issued AS start_date,
    delivery AS expiration_date,
    last_traded AS end_date,
    e.symbol AS exchange
    FROM `futures_contract` AS c
    JOIN `futures_rootsymbol` as rs ON c.root_symbol_id = rs.id
    JOIN `exchange` as e ON rs.exchange_id = e.id
    ORDER BY end_date
    '''
    ret = pd.read_sql(sql, ENGINE)

    ret['symbol'] = ret['symbol'].apply(_sanitize_contract_symbol)
    ret['root_symbol'] = ret['symbol'].str[:2]
    ret['auto_close_date'] = ret['end_date'] + pd.Timedelta(days=1)
    ret['multiplier'] = 1
    ret['tick_size'] = 1
    return ret


def exchanges():
    # CFE conflict with trading_calendars inherent exchange `CFE`
    return pd.DataFrame.from_records([
        {'exchange': 'SSE', 'country_code': 'CN'},
        {'exchange': 'SZSE', 'country_code': 'CN'},
        {'exchange': 'DCE', 'country_code': 'CN'},
        {'exchange': 'INE', 'country_code': 'CN'},
        {'exchange': 'ZCE', 'country_code': 'CN'},
        {'exchange': 'CFE', 'country_code': 'CN'},
        {'exchange': 'SHF', 'country_code': 'CN'},
    ])


def _pricing_iter(contracts, root_symbol=None):
    """
    :param contracts: pd.DataFrame, with sid as index
    :param root_symbol: str
    :return: None
    """
    sql_fmt = '''
    SELECT datetime,
    open, high, low, close, volume, open_interest
    FROM `futures_dailybar`
    WHERE contract_id = {}
    '''

    if root_symbol is not None:
        contracts = contracts[contracts['root_symbol'] == root_symbol]

    for sid in tqdm(contracts.index):
        sql = sql_fmt.format(int(contracts.loc[sid, 'contract_id']))
        df = pd.read_sql(sql,
                         ENGINE,
                         parse_dates=['datetime'],
                         index_col='datetime').sort_index()

        df['close'] = df['close'].fillna(method='ffill')

        df.dropna(subset=['close'], inplace=True)
        if df.empty:
            continue

        df['open'] = df['open'].fillna(df['close'])
        df['high'] = df['high'].fillna(df['close'])
        df['low'] = df['low'].fillna(df['close'])

        # print('=', contracts.loc[sid, 'contract_id'])
        yield sid, df


# This is necessary to aviod prefetch in HistoryLoader when there is no data.
def end_session():
    sql = 'SELECT MAX(datetime) FROM futures_dailybar'
    return pd.read_sql(sql, ENGINE).iloc[0, 0].tz_localize('utc')


@register('futures', calendar_name='XSGE', end_session=end_session())
def futures_bundle(environ,
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

    # Useless for futures
    # Prepare an empty DataFrame for dividends
    # Prepare an empty DataFrame for splits
    divs = pd.DataFrame(columns=['sid',
                                 'amount',
                                 'ex_date',
                                 'record_date',
                                 'declared_date',
                                 'pay_date']
                        )
    splits = pd.DataFrame(columns=['sid',
                                   'ratio',
                                   'effective_date']
                          )
    adjustment_writer.write(splits=splits, dividends=divs)

    contracts_meta = futures_contracts()
    # If we want only one product
    rs = input('root symbol: ') or None
    # will use contract_id column
    # must run before asset_db_writer.write
    # which will normalize the columns
    daily_bar_writer.write(_pricing_iter(contracts_meta, rs))
    asset_db_writer.write(futures=contracts_meta,
                          exchanges=exchanges(),
                          root_symbols=futures_root_symbols())


register_calendar_alias("SSE", "XSHG")
register_calendar_alias("SZSE", "XSHG")
register_calendar_alias("DCE", "XSGE")
register_calendar_alias("INE", "XSGE")
register_calendar_alias("ZCE", "XSGE")
register_calendar_alias("CFE", "XSGE")
register_calendar_alias("SHF", "XSGE")
