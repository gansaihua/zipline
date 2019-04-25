from zipline.data.bundles import load
from zipline.data.data_portal import DataPortal
from zipline.pipeline.data import EquityPricing
from zipline.pipeline.domain import CN_EQUITIES
from zipline.pipeline.loaders import EquityPricingLoader
from zipline.pipeline.engine import SimplePipelineEngine
from zipline.pipeline.loaders.blaze import global_loader


DEFAULT_BUNDLE = 'cndaily'
DEFAULT_COUNTRY = CN_EQUITIES.country_code

DEFAULT_BUNDLE_DATA = load(DEFAULT_BUNDLE)

DEFAULT_CALENDAR = DEFAULT_BUNDLE_DATA.equity_daily_bar_reader.trading_calendar

DEFAULT_ASSET_FINDER = DEFAULT_BUNDLE_DATA.asset_finder

DEFAULT_DATA_PORTAL = DataPortal(
    DEFAULT_ASSET_FINDER,
    DEFAULT_CALENDAR,
    DEFAULT_CALENDAR.first_session,
    equity_daily_reader=DEFAULT_BUNDLE_DATA.equity_daily_bar_reader,
    adjustment_reader=DEFAULT_BUNDLE_DATA.adjustment_reader
)

DEFAULT_PIPELINE_LOADER = EquityPricingLoader(
        DEFAULT_BUNDLE_DATA.equity_daily_bar_reader,
        DEFAULT_BUNDLE_DATA.adjustment_reader,
)


def choose_loader(column):
    if column.unspecialize() in EquityPricing.columns:
        return pipeline_loader
    elif column in global_loader:
        return global_loader
    raise ValueError("%s is NOT registered in `PipelineLoader`." % column)


DEFAULT_PIPELINE_ENGINE = SimplePipelineEngine(
    choose_loader,
    DEFAULT_ASSET_FINDER,
)


__all__ = [
    'DEFAULT_COUNTRY',
    'DEFAULT_BUNDLE_DATA',
    'DEFAULT_CALENDAR',
    'DEFAULT_ASSET_FINDER',
    'DEFAULT_DATA_PORTAL',
    'DEFAULT_PIPELINE_ENGINE',
]