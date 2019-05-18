import numpy as np
import pandas as pd
from zipline.pipeline.data import EquityPricing
from zipline.pipeline.factors import CustomFactor


class LatestEconomics(CustomFactor):
    '''
    To use the single value datasets (ie ones that don't have a value for each asset)
    one needs to ceate a simple custom factor

    pipe4 = Pipeline(
        columns={
            'pmi_manufacturing': LatestMacroEconomic(inputs=[MacroEconomic.pmi_manufacturing]),
            'pmi_manufacturing_asof': LatestMacroEconomic(inputs=[MacroEconomic.pmi_manufacturing_asof],
                                                            dtype=np.dtype('datetime64[ns]')),
        },
        domain=CN_EQUITIES,
    )
    '''
    window_length = 1

    def compute(self, today, asset_ids, out, values):
        out[:] = values[-1]


class YoYGrowth(CustomFactor):
    window_length = 300
    params = {'nyears': 1} # to which day
    
    def compute(self, today, assets, out, asof_dates, values, nyears):
        nobs = 4 * nyears + 1
        
        for col_ix in range(asof_dates.shape[1]):
            _, idx = np.unique(asof_dates[:, col_ix], return_index=True)
            quarterly_values = values[idx, col_ix]
            
            if len(quarterly_values) < nobs:
                ret = np.nan
            else:
                ret = quarterly_values[-1] / quarterly_values[-nobs] - 1
            
            out[col_ix] = ret
            
 
class PreviousYear(CustomFactor):
    window_length = 300
    params = {'nyears': 1} # to which day
    
    def compute(self, today, assets, out, asof_dates, values, nyears):
        nobs = 4 * nyears + 1
        
        for col_ix in range(asof_dates.shape[1]):
            _, idx = np.unique(asof_dates[:, col_ix], return_index=True)
            quarterly_values = values[idx, col_ix]
            
            if len(quarterly_values) < nobs:
                ret = np.nan
            else:
                ret = quarterly_values[-nobs]
            
            out[col_ix] = ret


class PositiveDivide(CustomFactor):
    window_length = 1
    window_safe = True
    
    def compute(self, today, assets, out, nom, denom):
        out[:] = nom[-1] / denom[-1]

        mask = denom[-1] <= 0
        out[mask] = np.nan
