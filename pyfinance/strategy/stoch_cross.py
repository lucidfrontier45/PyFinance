'''
Created on Dec 10, 2013

@author: du
'''

import numpy as np
import talib
from . import _BaseOptimizeStrategy

def findSTOCHCross(ts, lowerbound=30, fastk_period=14, slowk_period=3, slowd_period=3):
    STOCH = talib.abstract.STOCH
    data = ts.toDict()
    slowk, slowd = STOCH(data, fastk_period=fastk_period, slowk_period=slowk_period, slowd_period=slowd_period)
    mask1 = np.where(np.diff(np.sign(slowk - slowd))==2)[0]
    mask2 = np.where(slowd < lowerbound)[0]
    mask = list(set(mask1).intersection(set(mask2)))
    mask.sort()
    return slowd, np.array(mask)

class STOCHCrossOptimizeStrategy(_BaseOptimizeStrategy):
    def __init__(self):
        self.params_ = (30, 14, 3, 3)
        self._strategy_func = findSTOCHCross
        
    def _fit(self, ts, roc):
        max_score = -1.0
        for lowerbound in xrange(25, 26):
#         lowerbound=30
            for fastk_period in xrange(9, 20):
                for slowk_period in xrange(3, 7):
                    for slowd_period in xrange(3, 7):
                        params = (lowerbound, fastk_period, slowk_period, slowd_period)
                        score = self.score(ts, roc, params=params)
                        if score > max_score:
                            max_score = score
                            max_params = params
        return max_score, max_params