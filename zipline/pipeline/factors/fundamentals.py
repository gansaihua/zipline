from zipline.pipeline.term import InputDates
from zipline.pipeline.data import EquityPricing, Fundamentals
from zipline.pipeline.factors import CustomFactor
from zipline.pipeline.factors.basic import EWMA, EWMSTD, DailyReturns

import numpy as np
import pandas as pd


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
            

class _BaseBiMul(CustomFactor):
    window_length = 1
    window_safe = True
    
    def compute(self, today, assets, out, left, right):
        out[:] = left[-1] * right[-1]
        

class _BasePosDiv(CustomFactor):
    window_length = 1
    window_safe = True
    
    def compute(self, today, assets, out, nom, denom):
        mask = denom[-1] <= 0    
        denom[-1, mask] = np.nan
        
        out[:] = nom[-1] / denom[-1]
        
        
class Momentum(CustomFactor): 
    '''Default: price return changes from -244 days to -21 days
    '''
    inputs = (EquityPricing.close,)
    params = {'t0': 21} # to which day
    window_length = 244 # from which day

    def compute(self, today, assets, out, close, t0):  
        out[:] = close[-t0] / close[0] - 1
