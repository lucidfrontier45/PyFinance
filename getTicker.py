#! /usr/bin/python

from sys import argv, exit
import time
import sqlite3
import pyfinance.tick_timeseries as ts
import pyfinance.yahoo_finance_jp as yf
from pyfinance.db import initDB, setDBName
from pyfinance import nikkei225
from multiprocessing import Queue, Pool

db_name = argv[1]
setDBName(db_name)

if argv[2] == "-init":
    initDB()
    exit()

tick_ids = sorted(nikkei225.getNikkei225NameHash().keys())
error_list = Queue(len(tick_ids))
result_list = Queue(len(tick_ids))
date_len = int(argv[2])

def workf(tick_id):
    try:
        ticks = yf.getTick(tick_id, length=date_len)
        ticks.sort()
        result_list.put(ticks)
        for _ in xrange(5):
            try:
                ticks.dumpToSQL()
                break
            except sqlite3.OperationalError, e:
                print e
                time.sleep(1)
    except :
        print "error %d" % (tick_id,)
        error_list.put(tick_id, False)

pool = Pool(20)
pool.map(workf, tick_ids)

while not error_list.empty():
    print error_list.get()