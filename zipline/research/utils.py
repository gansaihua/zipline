import os
import pandas as pd
import pyfolio as pf

from zipline.utils import paths as pth
from .core import symbols, benchmark_returns


def select_output_by(output, start=None, end=None, assets=None, reduce_format=True):
    """
    按时间及代码选择`pipeline`输出数据框
    
    专用于研究环境下的run_pipeline输出结果分析

    参数
    ----
    output : MultiIndex DataFrame
        pipeline输出结果
    start ： str
        开始时间
    end ： str
        结束时间    
    assets ： 可迭代对象或str
        股票代码

    案例
    ----  
    >>> # result 为运行`pipeline`输出结果 
    >>> select_output_by(result,'2018-04-23','2018-04-24',assets=['000585','600871'])

                                                  mean_10
    2018-04-23 00:00:00+00:00 	*ST东电(000585) 	2.7900
                                *ST油服(600871) 	2.0316
    2018-04-24 00:00:00+00:00 	*ST东电(000585) 	2.7620
                                *ST油服(600871) 	2.0316    
    """
    nlevels = output.index.nlevels
    if nlevels != 2:
        raise ValueError(
            '输入数据框只能是run_pipeline输出结果，MultiIndex DataFrame'
        )
    
    assets = symbols(assets)
    
    if start is None:
        start = min(output.index.levels[0])
    else:
        start = pd.Timestamp(start, tz='utc')
    
    if end is None:
        end = max(output.index.levels[0])
    else:
        end = pd.Timestamp(end, tz='utc')
    
    ret = _select_output_by(output, start, end, assets)
    
    if reduce_format:
        cond1 = start == end
        cond2 = len(assets) == 1
        if cond1 & cond2:
            ret = ret.xs((start, assets[0]))
        elif cond1:
                ret = ret.xs(start, level=0)
        elif cond2:
                ret = ret.xs(assets[0], level=1)
    return ret


def _select_output_by(output, start, end, assets=[]):
    ret = output.loc[start:end]
    if len(assets):
        ret = ret.loc[(slice(None), assets), :]
    return ret


def get_backtest(backtest_id=None):
    backtest_dir = pth.zipline_path(['backtest'])

    if backtest_id:
        backtest_id = backtest_id.split(sep='.', maxsplit=2)[0]
        perf_file = '{}/{}.pkl'.format(backtest_dir, backtest_id)
        return pd.read_pickle(perf_file)

    try:
        candidates = os.listdir(backtest_dir)
        
        cdt = None
        for c in candidates:
            if not c.endswith('.pkl'): continue
            cpath = os.path.join(backtest_dir, c)
            if cdt is None or cdt < os.path.getmtime(cpath):
                cdt = os.path.getmtime(cpath)
                perf_file = cpath
        return pd.read_pickle(perf_file)
    except (ValueError, OSError) as e:
        if getattr(e, 'errno', errno.ENOENT) != errno.ENOENT:
            raise
        raise ValueError('{} Not Found!'.format(backtest_dir))


def create_full_tear_sheet(df, benchmark_symbol='000300.SH'):
    returns, positions, transactions = pf.utils.extract_rets_pos_txn_from_zipline(df)
    benchmark_rets = benchmark_returns(
        benchmark_symbol, returns.index[0], returns.index[-1]
    )
    
    pf.create_full_tear_sheet(
        returns,
        benchmark_rets=benchmark_rets,
        positions=positions,
        transactions=transactions,
        round_trips=True)
