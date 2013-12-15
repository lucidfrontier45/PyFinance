import numpy as np
import pandas as pd
import talib

from .. import utils as pfutils


ta_names = ("MA12", "MA26" "MACD", "MOM", "RSI", "STOCH")

def getIndicators(tick_data, start=0, end=None):
    ret = tick_data[start:end].copy()
    ret.fix_split()
    del ret["final_v"]
    data = ret[start:end].toDict()
    
    open_v = pfutils.shift_stack(ret["open_v"],"open_v", ret.index, 1, 6)
    close_v = pfutils.shift_stack(ret["close_v"],"close_v", ret.index, 1, 6)
    low_v = pfutils.shift_stack(ret["low_v"],"low_v", ret.index, 1, 6)
    high_v = pfutils.shift_stack(ret["high_v"],"high_v", ret.index, 1, 6)
    volume = pfutils.shift_stack(ret["volume"],"volume", ret.index, 1, 6)
    ret = ret.join(open_v).join(close_v).join(low_v).join(high_v).join(volume)

    ma12 = talib.abstract.Function("MA")(data, timeperiod=12)
    ma26 = talib.abstract.Function("MA")(data, timeperiod=26)
    macd, macd_sig, macd_hist = talib.abstract.Function("MACD")(data, fastperiod=12, slowperiod=26, signalperiod=9)
    mom = talib.abstract.Function("MOM")(data, timeperiod=10)
    rsi = talib.abstract.Function("RSI")(data, timeperiod=14)
    slowk, slowd = talib.abstract.Function("STOCH")(data, fastk_period=9, slowk_period=3, slowd_period=3)
    ret["MA12"] = ma12
    ret["MA26"] = ma26
    ret["MACD"] = macd
    ret["MACD_SIG"] = macd_sig
    ret["MACD_HIST"] = macd_hist
    ret["MOM"] = mom
    ret["RSI"] = rsi
    ret["SLOWK"] = slowk
    ret["SLOWD"] = slowd
    
    return ret

def prepareXY(tick_data, length=10, start=0, end=None):
    dat = getIndicators(tick_data, start, end)
    dat["ROC"] = pfutils.calc_roc(tick_data, 10)
    dat = dat.dropna()
    y = np.sign(dat.pop("ROC") - 1.05)
    Y = np.where(y < 1 , 0, 1)
    X = dat.values
    return X, Y