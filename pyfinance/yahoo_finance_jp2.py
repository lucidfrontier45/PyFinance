# -*- coding: utf-8 -*-

import re
import datetime
from .tick_timeseries import TickTimeSeries, TickerCodeError, _str2date
import jsm

def getTick(code, session=None, end_date=None, start_date=None, length=500):
    code = int(code)

    print "getting data of tikker %d from yahoo finance...  " % code
    
    scale = 8.0 / 5.0  # for skipping hollidays
    if end_date == None:
        # set default end_date = today
        end_date = datetime.date.today()
    elif isinstance(end_date, str):
        end_date = _str2date(end_date)

    if start_date == None:
        # set default start_date = today - length * scale
        start_date = end_date - datetime.timedelta(days=length * scale)
    elif isinstance(start_date, str):
        start_date = _str2date(start_date)
        length = (end_date - start_date).days
        
    q = jsm.Quotes()
    ret = q.get_historical_prices(code, jsm.DAILY, start_date, end_date)
    dates = [r.date for r in ret]
    prices = [(r.open, r.high, r.low, r.close, r.volume, r.close) for r in ret]
    tick_data = TickTimeSeries(prices, tick_id=code, index=dates)
    tick_data.fix_split()
    return tick_data
