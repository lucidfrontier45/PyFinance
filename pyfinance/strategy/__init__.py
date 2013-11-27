# -*- coding: utf-8 -*-
'''
Created on Nov 28, 2013

@author: du
'''

import numpy as np
import pandas as pd

def calc_roc(ts, length=10, buy_key="open_v", sell_key="close_v"):
    buy_prices = ts[buy_key]
    sell_prices = pd.rolling_max(ts[sell_key].shift(-1), length).shift(-length)
    roc = sell_prices / buy_prices
    return roc

class _BaseStrategy():
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
        if mask[-1] > len(ts) - 5:
            return ts.data.ix[mask].index[-1]
        else:
            return None
from .golden_cross import findGoldenCross, GoldenCrossStrategy
from .macd_cross import findMACDCross, MACDCrossStrategy