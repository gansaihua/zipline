from zipline.data import bundles
from zipline.data.data_portal import DataPortal
from zipline.pipeline.domain import CN_EQUITIES
from zipline.utils.memoize import remember_last

import pandas as pd


DEFAULT_COUNTRY = CN_EQUITIES.country_code
DEFAULT_BUNDLE = 'cndaily'


@remember_last
def _data_portal(bundle=DEFAULT_BUNDLE):
    """Create a DataPortal for the given bundle.

    Parameters
    ----------
    bundle : str
        The name of the bundle to create a pipeline engine for.

    Returns
    -------
    engine : zipline.pipleine.engine.SimplePipelineEngine
        The pipeline engine which can run pipelines against the bundle.
    calendar : zipline.utils.calendars.TradingCalendar
        The trading calendar for the bundle.
    """
    bundle_data = bundles.load(bundle)
    calendar = bundle_data.equity_daily_bar_reader.trading_calendar
    finder = bundle_data.asset_finder
    
    data_portal = DataPortal(
        finder,
        calendar,
        calendar.first_session,
        equity_daily_reader=bundle_data.equity_daily_bar_reader,
        adjustment_reader=bundle_data.adjustment_reader
    )
    
    return data_portal, calendar


@remember_last
def _trading_calendar(bundle=DEFAULT_BUNDLE):
    """Create a TradingCalendar for the given bundle.

    Parameters
    ----------
    bundle : str
        The name of the bundle to create a pipeline engine for.

    Returns
    -------
    calendar : zipline.utils.calendars.TradingCalendar
        The trading calendar for the bundle.
    """
    bundle_data = bundles.load(bundle)
    return bundle_data.equity_daily_bar_reader.trading_calendar


@remember_last
def _asset_finder(bundle=DEFAULT_BUNDLE):
    """Create a AssetFinder for the given bundle.

    Parameters
    ----------
    bundle : str
        The name of the bundle to create a pipeline engine for.

    Returns
    -------
    engine : zipline.pipleine.engine.SimplePipelineEngine
        The pipeline engine which can run pipelines against the bundle.
    calendar : zipline.utils.calendars.TradingCalendar
        The trading calendar for the bundle.
    """
    bundle_data = bundles.load(bundle)

    return bundle_data.asset_finder