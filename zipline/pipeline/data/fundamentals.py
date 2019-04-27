from zipline.utils.memoize import (
    classlazyval, 
    remember_last,
)
from zipline.utils.numpy_utils import (
    int64_dtype,
    _FILLVALUE_DEFAULTS,
)
from zipline.pipeline.domain import CN_EQUITIES
from zipline.pipeline.loaders.blaze import from_blaze
from zipline.pipeline.loaders.blaze.core import datashape_type_to_numpy

from secdata.utils import bcolz_path

import bcolz
import blaze
import pandas as pd


INDICATORS = [
    'tot_assets',
    'total_shares',
    'float_a_shares',
    'mkt_cap_ashare',
    'mkt_cap_ard',
]


def fillvalue_for_expr(expr):
    fillmissing = _FILLVALUE_DEFAULTS.copy()
    fillmissing.update({
        int64_dtype: -1,
    })
    
    ret = {}
    for name, type_ in expr.dshape.measure.fields:
        n_type = datashape_type_to_numpy(type_)
        ret[name] = fillmissing[n_type]
    return ret
        
    
class Fundamentals(object):
    def __getattr__(self, name):
        if name in self.colnames:
            if name.endswith('_asof'):
                return self.factory(name[:-5]).asof_date
            else:
                return self.factory(name).value
        raise AttributeError
    
    def __getattribute__(self, name):       
        ret = object.__getattribute__(self, name)
        
        # We should use __getattr__ here
        if ret == AttributeError:
            raise ret
        return ret
    
    @classlazyval
    def colnames(cls):
        ret = []
        for column in INDICATORS:
            ret.extend([column, column+'_asof'])
        return ret
    
    @staticmethod
    @remember_last
    def factory(name, use_checkpoints=False):
        ''' Bcolz data entry point'''
        expr = blaze.data(bcolz_path(name), name=name)

        if use_checkpoints:
            # 日度数据需要checkpoints加速ffill
            # 此处采用为月度频率，ffill=None
            checkpoints_dt = pd.date_range('2000-1-1', pd.Timestamp('today'), freq='M')
            data = {k: 0 for k in expr.fields}
            data.update({'asof_date': checkpoints_dt})
            data.update({'timestamp': checkpoints_dt})
            checkpoints = blaze.data(
                pd.DataFrame(data),
                dshape=expr.dshape,
                name='checkpoints',
            )
        else:
            checkpoints = None

        return from_blaze(
            expr,
            domain=CN_EQUITIES,
            checkpoints=checkpoints,
            no_deltas_rule='ignore',
            no_checkpoints_rule='ignore',
            missing_values=fillvalue_for_expr(expr),
        )


#For help hints (.), easy to use
for k in INDICATORS:
    setattr(
        Fundamentals, 
        k, 
        AttributeError,
    )
    setattr(
        Fundamentals, 
        k + '_asof',
        AttributeError,
    )

Fundamentals = Fundamentals()
