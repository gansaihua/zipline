import sys
import logbook
import pandas as pd

from trading_calendars import get_calendar

from zipline.data.bundles import load
from zipline.data.data_portal import DataPortal
from zipline.pipeline.loaders import EquityPricingLoader
from zipline.pipeline.loaders.blaze import global_loader
from zipline.pipeline.data import EquityPricing
from zipline.utils.factory import create_simulation_parameters


logbook.set_datetime_format('local')
logbook.StreamHandler(sys.stdout).push_application()

def algo_args(year=2017,
              from_date=None,
              to_date=None,
              capital_base=float('1.0e7'),
              num_days=None,
              metrics_set=None):
    
    # Constant inputs
    benchmark_sid = 3623 # '000300.SH'
    bundle = "cndaily"
    calendar = 'XSHG'
    
    # With trading calendar
    trading_calendar = get_calendar(calendar)
    
    # With simulation parameters
    if from_date:
        from_date = pd.Timestamp(from_date, tz='utc')
        
    if to_date:
        to_date = pd.Timestamp(to_date, tz='utc')
        
    sim_params = create_simulation_parameters(
        year=year, 
        start=from_date,
        end=to_date, 
        capital_base=capital_base,
        num_days=num_days,
        trading_calendar=trading_calendar
    )
    
    # With data portal
    bundle_data = load(bundle)
    data_portal = DataPortal(
        bundle_data.asset_finder,
        trading_calendar, 
        trading_calendar.first_session,
        equity_daily_reader=bundle_data.equity_daily_bar_reader, 
        adjustment_reader=bundle_data.adjustment_reader
    )
    
    # With pipelineloader
    pipeline_loader = EquityPricingLoader(
        bundle_data.equity_daily_bar_reader, 
        bundle_data.adjustment_reader
    )
    
    def choose_loader(column):
        if column.unspecialize() in EquityPricing.columns:
            return pipeline_loader 
        elif column in global_loader:
            return global_loader
        raise ValueError("%s is NOT registered in `PipelineLoader`." % column)    
    
    return {
        'sim_params': sim_params,
        'data_portal': data_portal,
        'benchmark_sid': benchmark_sid,  
        'metrics_set': metrics_set,
        'get_pipeline_loader': choose_loader,
    }


def backtest_result_path():
    from zipline.utils import paths as pth
    
    ret = pth.zipline_path(['backtest'])
    pth.ensure_directory(ret)
    return ret


def write_backtest(perfs):
    '''See zipline.research.get_backtest
    '''
    from uuid import uuid4
    
    backtest = '{}/{}.pkl'.format(backtest_result_path(), 
                                  uuid4().hex)
    perfs.to_pickle(backtest)