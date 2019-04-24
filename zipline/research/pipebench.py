from zipline.data import bundles
from zipline.utils.memoize import remember_last
from zipline.pipeline.engine import SimplePipelineEngine
from zipline.pipeline.data import EquityPricing
from zipline.pipeline.loaders import EquityPricingLoader
from zipline.pipeline.loaders.blaze import global_loader

import pandas as pd


def run_pipeline(pipeline, start_date, end_date):
    default_bundle = 'cndaily'
    
    return run_pipeline_against_bundle(
        pipeline, start_date, end_date, default_bundle
    )


def run_pipeline_against_bundle(pipeline, start_date, end_date, bundle):
    """Run a pipeline against the data in a bundle.

    Parameters
    ----------
    pipeline : zipline.pipeline.Pipeline
        The pipeline to run.
    start_date : pd.Timestamp
        The start date of the pipeline.
    end_date : pd.Timestamp
        The end date of the pipeline.
    bundle : str
        The name of the bundle to run the pipeline against.

    Returns
    -------
    result : pd.DataFrame
        The result of the pipeline.
    """
    engine, calendar = _pipeline_engine_and_calendar_for_bundle(bundle)

    start_date = pd.Timestamp(start_date, tz='utc')
    if not calendar.is_session(start_date):
        # this is not a trading session, advance to the next session
        start_date = calendar.minute_to_session_label(
            start_date,
            direction='next',
        )

    end_date = pd.Timestamp(end_date, tz='utc')
    if not calendar.is_session(end_date):
        # this is not a trading session, advance to the previous session
        end_date = calendar.minute_to_session_label(
            end_date,
            direction='previous',
        )

    return engine.run_pipeline(pipeline, start_date, end_date)


@remember_last
def _pipeline_engine_and_calendar_for_bundle(bundle):
    """Create a pipeline engine for the given bundle.

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
    
    pipeline_loader= EquityPricingLoader(
        bundle_data.equity_daily_bar_reader, 
        bundle_data.adjustment_reader,
    )

    def choose_loader(column):
        if column.unspecialize() in EquityPricing.columns:
            return pipeline_loader
        elif column in global_loader:
            return global_loader
        raise ValueError("%s is NOT registered in `PipelineLoader`." % column)

    calendar = bundle_data.equity_daily_bar_reader.trading_calendar
    return (
        SimplePipelineEngine(
            choose_loader,
            bundle_data.asset_finder,
        ),
        calendar,
    )