'''
Created on Nov 28, 2013

@author: du
'''

import numpy as np
import talib
from . import _BaseStrategy

def findGoldenCross(ts, short_period=5, long_period=20):
    MA = talib.abstract.MA
    data = ts.toDict()
    short_ma = MA(data, timeperiod=short_period)
    long_ma = MA(data, timeperiod=long_period)
    ma_diff = short_ma - long_ma
    mask = np.where(np.diff(np.sign(ma_diff))==2)[0]
    return ma_diff, mask

class GoldenCrossStrategy(_BaseStrategy):
    def __init__(self):
        self.params = (25, 75)
        self._strategy_func = findGoldenCross
        
    def _fit(self, ts, roc):
        max_score = -1.0
        for short_period in xrange(5, 25):
            for short_long_ratio in xrange(2, 5):
                long_period = short_period * short_long_ratio
                params = (short_period, long_period)
                score = self.score(ts, roc, params=params)
                if score > max_score:
                    max_score = score
                    max_params = params
        return max_score, max_params