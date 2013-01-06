#! /usr/bin/python

from sys import argv
import time
import sqlite3
import pyfinance.timeseries as ts
import pyfinance.yahoo_finance_jp as yf
from pyfinance import nikkei225

db_name = "nikkei225.db"
tick_ids = sorted(nikkei225.getNikkei225NameHash().keys())
error_list = []
date_len = int(argv[1])

for tick_id in tick_ids:
    try:
        ticks = yf.getTick(tick_id, length=date_len)
        ticks.sort()
        for _ in xrange(5):
            try:
                ticks.dumpToSQL(db_name)
                break
            except sqlite3.OperationalError:
                time.sleep(1)
    except ts.TickerCodeError:
        error_list.append(tick_id)


print error_list
