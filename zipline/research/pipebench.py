import pandas as pd
from .constant import DEFAULT_CALENDAR, DEFAULT_PIPELINE_ENGINE


def run_pipeline(pipeline, start_date, end_date):
    """Run a pipeline against the data in a bundle.

    Parameters
    ----------
    pipeline : zipline.pipeline.Pipeline
        The pipeline to run.
    start_date : pd.Timestamp
        The start date of the pipeline.
    end_date : pd.Timestamp
        The end date of the pipeline.

    Returns
    -------
    result : pd.DataFrame
        The result of the pipeline.
    """
    calendar = DEFAULT_CALENDAR
    engine = DEFAULT_PIPELINE_ENGINE

    start_date = pd.Timestamp(start_date, tz='utc')
    if not calendar.is_session(start_date):
        # this is not a trading session, advance to the next session
        start_date = calendar.minute_to_session_label(
            start_date,
            direction='next',
        )

    end_date = pd.Timestamp(end_date, tz='utc')
    if not calendar.is_session(end_date):
        # this is not a trading session, advance to the next session
        end_date = calendar.minute_to_session_label(
            end_date,
            direction='next',
        )

    return engine.run_pipeline(pipeline, start_date, end_date)
