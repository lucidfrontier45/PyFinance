'''
Created on Nov 28, 2013

@author: du
'''

import numpy as np
import talib
from . import _BaseStrategy

def findMACDCross(ts, fastperiod=12, slowperiod=26, signalperiod=9):
    MACD = talib.abstract.MACD
    data = ts.toDict()
    _, __, macd_hist = MACD(data, fastperiod=fastperiod, slowperiod=slowperiod, signalperiod=signalperiod)
    mask = np.where(np.diff(np.sign(macd_hist))==2)[0]
    return macd_hist, mask

class MACDCrossStrategy(_BaseStrategy):
    def __init__(self):
        self.params = (12, 26, 9)
        self._strategy_func = findMACDCross
        
    def _fit(self, ts, roc):
        max_score = -1.0
        for fastperiod in xrange(5, 12):
            for fast_slow_ratio in np.logspace(np.log10(2), np.log10(5), 10):
                for signalperiod in xrange(5, 12):
                    slowperiod = int(fastperiod * fast_slow_ratio)
                    params = (fastperiod, slowperiod, signalperiod)
                    score = self.score(ts, roc, params=params)
                    if score > max_score:
                        max_score = score
                        max_params = params
        return max_score, max_params