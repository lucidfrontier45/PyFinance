'''
Created on Jun 16, 2013

@author: du
'''

import numpy as np
import pandas as pd

def shift_stack(x, name, dates=None, start_lag=0, end_lag=5):
    x = pd.TimeSeries(x)
    shifted_data = [x.shift(i) for i in xrange(start_lag, end_lag)]
    names = [name + str(i) for i in xrange(start_lag, end_lag)]
    df = pd.DataFrame(np.array(shifted_data).T, columns=names)
    if not dates is None:
        df.index = dates
    return df