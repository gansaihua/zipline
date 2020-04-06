from zipline.data.bundles import load
from zipline.data.data_portal import DataPortal
from zipline.pipeline.domain import CN_EQUITIES


bundle_name = 'futures'
DEFAULT_COUNTRY = CN_EQUITIES.country_code

DEFAULT_BUNDLE_DATA = load(bundle_name)

DEFAULT_CALENDAR = DEFAULT_BUNDLE_DATA.equity_daily_bar_reader.trading_calendar

DEFAULT_ASSET_FINDER = DEFAULT_BUNDLE_DATA.asset_finder

DEFAULT_DATA_PORTAL = DataPortal(
    DEFAULT_ASSET_FINDER,
    DEFAULT_CALENDAR,
    DEFAULT_CALENDAR.first_session,
    future_daily_reader=DEFAULT_BUNDLE_DATA.equity_daily_bar_reader,
    adjustment_reader=DEFAULT_BUNDLE_DATA.adjustment_reader
)


__all__ = [
    'DEFAULT_COUNTRY',
    'DEFAULT_BUNDLE_DATA',
    'DEFAULT_CALENDAR',
    'DEFAULT_ASSET_FINDER',
    'DEFAULT_DATA_PORTAL',
]
