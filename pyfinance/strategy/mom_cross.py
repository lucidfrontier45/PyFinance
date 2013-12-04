'''
Created on Nov 28, 2013

@author: du
'''

import numpy as np
import talib
from . import _BaseOptimizeStrategy

def findMomCross(ts, mom_period=10, ma_period=5):
    mom = ts.get_ta(ta_name="MOM", timeperiod=mom_period).outputs
    mom_ma = talib.MA(mom, timeperiod=ma_period)
    mask = np.where(np.diff(np.sign(mom_ma))==2)[0]
    return mom_ma, mask

class MomCrossOptimizeStrategy(_BaseOptimizeStrategy):
    def __init__(self):
        self.params_ = (10, 5)
        self._strategy_func = findMomCross
        
    def _fit(self, ts, roc):
        max_score = -1.0
        for mom_period in xrange(5, 15):
            for ma_period in xrange(5, 10):
                params = (mom_period, ma_period)
                score = self.score(ts, roc, params=params)
                if score > max_score:
                    max_score = score
                    max_params = params
        return max_score, max_params