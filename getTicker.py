#! /usr/bin/python

from sys import argv
import time
import sqlite3
import pyfinance.timeseries as ts
import pyfinance.yahoo_finance_jp as yf
from pyfinance import nikkei225

#tick_id = argv[1]
#db_name = argv[2]

db_name = "nikkei225.db"
tick_ids = nikkei225.getNikkei225()[1].keys()
error_list = []

for tick_id in tick_ids:
    try:
        ticks = yf.getTick(tick_id,length=100)
        for _ in xrange(5):
            try:
                ticks.dumpToSQL(db_name)
                break
            except sqlite3.OperationalError:
                time.sleep(1)
    except IndexError:
        error_list.append(tick_id)


print error_list
