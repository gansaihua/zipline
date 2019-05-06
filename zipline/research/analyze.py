import os
import pandas as pd
import pyfolio as pf

from zipline.utils import paths as pth
from .core import benchmark_returns


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
