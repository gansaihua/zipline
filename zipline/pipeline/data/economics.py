from zipline.pipeline.domain import CN_EQUITIES
from zipline.pipeline.loaders.blaze import from_blaze
from zipline.utils.memoize import classlazyval, remember_last

import bcolz
import blaze
import pandas as pd
from secdata.utils import bcolz_path
from secdata.constant import MACRO_PATH


INDICATORS = [
    'pmi_manufacturing',
]
        
    
class Economics(object):
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
        ''' Bcolz data entry point'''
        expr = blaze.data(bcolz_path(name, MACRO_PATH), name=name)

        return from_blaze(
            expr,
            domain=CN_EQUITIES,
            deltas=None,
            checkpoints=None,
            no_deltas_rule='ignore',
            no_checkpoints_rule='ignore',
        )


#For help hints (.), easy to use
for k in INDICATORS:
    setattr(
        Economics,
        k, 
        AttributeError,
    )
    setattr(
        Economics,
        k + '_asof',
        AttributeError,
    )

Economics = Economics()
