#! /usr/bin/python

from sys import argv, exit
import time
import sqlite3
import pyfinance.tick_timeseries as ts
import pyfinance.yahoo_finance_jp as yf
from pyfinance.db import initDB, setDBName, getDBName
from pyfinance import nikkei225

db_name = argv[1]
setDBName(db_name)

if argv[2] == "-init":
    initDB()
    exit()

tick_ids = sorted(nikkei225.getNikkei225NameHash().keys())
error_list = []
date_len = int(argv[2])

for tick_id in tick_ids:
    try:
        ticks = yf.getTick(tick_id, length=date_len)
        ticks.sort()
        for _ in xrange(5):
            try:
                ticks.dumpToSQL()
                break
            except sqlite3.OperationalError, e:
                print e
                time.sleep(1)
    except ts.TickerCodeError:
        error_list.append(tick_id)


print error_list
