#! /usr/bin/python

from sys import argv
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
        ticks = yf.getTick(tick_id,length=1000)
        ticks.dumpToSQL(db_name)
    except :
        error_list.append(tick_id)

print error_list
