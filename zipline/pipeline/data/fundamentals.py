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
    'shares_outstanding',
    'float_a_shares',
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
    def factory(name):
        ''' Bcolz data entry point
        '''
        tbl = bcolz.open(bcolz_path(name), mode='r')
        expr = blaze.data(tbl.todataframe())
        return from_blaze(
            expr, 
            domain=CN_EQUITIES,
            no_deltas_rule='ignore',
            no_checkpoints_rule='ignore',            
            missing_values=fillvalue_for_expr(expr)
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
