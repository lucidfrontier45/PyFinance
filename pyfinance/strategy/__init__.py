# -*- coding: utf-8 -*-
'''
Created on Nov 28, 2013

@author: du
'''

import numpy as np
import pandas as pd
from ..utils import calc_roc

class _BaseOptimizeStrategy():
    def __init__(self):
        self.params_ = None
        self._strategy_func = None
        
    def get_params(self):
        return self.params_
    
    def _score(self, ts, roc, copy=True, params=()):
        if copy:
            ts = ts.copy()
        if not params:
            params = self.params_
        _, mask = self._strategy_func(ts[:-10], *params)
        if len(mask) == 0:
            return [1.0]
        ts_data = ts.data
        ts_data["roc"] = roc
        ret = ts_data.ix[mask + 2].dropna()
        if(len(ret) == 0):
            return [1.0]
        else:
            return ret["roc"]
        
    def median_score(self, ts, roc, copy=True, params=()):
        return np.median(self._score(ts, roc, copy, params))
        
    def prod_score(self, ts, roc, copy=True, params=()):
        return np.prod(self._score(ts, roc, copy, params))
        
    def score(self, ts, roc, copy=True, params=()):
        return self.median_score(ts, roc, copy, params)
        
    def _fit(self, ts, roc):
        #using self.score to overwrite this function
        max_score, max_params = 0, ()
        return max_score, max_params
        
    def fit(self, ts, roc):
        self.best_score_, self.params_ = self._fit(ts, roc)
        return self
    
    def predict(self, ts):
        _, mask = self._strategy_func(ts, *self.params_)
        if len(mask) == 0:
            return None
        if mask[-1] > len(ts) - 5:
            return ts.data.ix[mask + 1].index[-1]
        else:
            return None
        
    def eval(self, ts):
        return self._strategy_func(ts, *self.params_)
    
from .golden_cross import findGoldenCross, GoldenCrossOptimizeStrategy
from .macd_cross import findMACDCross, MACDCrossOptimizeStrategy
from .mom_cross import findMomCross, MomCrossOptimizeStrategy
from .stoch_cross import findSTOCHCross, STOCHCrossOptimizeStrategy