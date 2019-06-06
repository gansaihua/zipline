from zipline.utils.numpy_utils import float64_dtype, datetime64ns_dtype
from copy import copy
from .dataset import Column, DataSet


class Fundamentals(DataSet):
    memb_csi300 = Column(float64_dtype, metadata={'blaze_column_name': 'value', 'data_id': '_1'})
    memb_csi300_asof_date = Column(datetime64ns_dtype, metadata={'blaze_column_name': 'asof_date', 'data_id': '_1'})
