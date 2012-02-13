#! /usr/bin/python

from sys import argv
import pyfinance.timeseries as ts
import pyfinance.yahoo_finance_jp as yf

tick_id = argv[1]
db_name = argv[2]

ticks = yf.getTick(tick_id,length=1000)
ticks.dumpToSQL(db_name)
