'''
Initialization prerequisites
    1. ~/.zipline/data/cndaily: pricing bundle data
    2. ~/.zipline/data/metrics.bcolz: daily metrics data
    3. ~/.zipline/data/fundamentals: fundamentals data
'''

import blaze
from os.path import join
from zipline.utils.paths import data_path, ensure_directory
from zipline.data.bundles import load
from zipline.data.data_portal import DataPortal
from zipline.data.bcolz_daily_metrics import BcolzDailyBarReader as MetricsReader
from zipline.pipeline.domain import CN_EQUITIES
from zipline.pipeline.data import EquityPricing, EquityMetrics, Fundamentals
from zipline.pipeline.engine import SimplePipelineEngine
from zipline.pipeline.loaders import EquityPricingLoader, EquityMetricsLoader
from zipline.pipeline.loaders.blaze import global_loader


bundle_name = 'cndaily'
DEFAULT_COUNTRY = CN_EQUITIES.country_code

DEFAULT_BUNDLE_DATA = load(bundle_name)

DEFAULT_CALENDAR = DEFAULT_BUNDLE_DATA.equity_daily_bar_reader.trading_calendar

DEFAULT_ASSET_FINDER = DEFAULT_BUNDLE_DATA.asset_finder

DEFAULT_DATA_PORTAL = DataPortal(
    DEFAULT_ASSET_FINDER,
    DEFAULT_CALENDAR,
    DEFAULT_CALENDAR.first_session,
    equity_daily_reader=DEFAULT_BUNDLE_DATA.equity_daily_bar_reader,
    future_daily_reader=DEFAULT_BUNDLE_DATA.equity_daily_bar_reader,
    adjustment_reader=DEFAULT_BUNDLE_DATA.adjustment_reader
)

pricing_loader = EquityPricingLoader(
        DEFAULT_BUNDLE_DATA.equity_daily_bar_reader,
        DEFAULT_BUNDLE_DATA.adjustment_reader,
)

metrics_root = data_path(['metrics.bcolz'])
metrics_loader = EquityMetricsLoader(MetricsReader(metrics_root))

fundamentals_root = data_path(['fundamentals'])
ensure_directory(fundamentals_root)


def choose_loader(column):
    column_general = column.unspecialize()
    if column_general in EquityPricing.columns:
        return pricing_loader
    elif column_general in EquityMetrics.columns:
        return metrics_loader
    elif column_general in Fundamentals.columns:
        data_id = column.metadata['data_id']
        pth = join(fundamentals_root, '{}.bcolz'.format(data_id))
        try:
            expr = blaze.data(pth)
        except ValueError:
            raise Exception('No fundamental data ingested')
        global_loader.register_column(column, expr)
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