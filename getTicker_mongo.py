#! /usr/bin/python
# -*- coding: utf-8 -*-

from sys import argv, exit
import pyfinance.yahoo_finance_jp as yf
from pyfinance import tick_timeseries
from pyfinance import nikkei225
from multiprocessing import Process, Queue
from pyfinance.utils import initSession
import pymongo

mongo_info= {"host":"localhost", "port":27017, "db":"jp_stock"}

def initDB(host):
    con = pymongo.MongoClient(host=host) 
    col = con[mongo_info["db"]]["prices"]
    col.ensure_index([("date",1), ("tick_id",1)], unique=True, dropDups=True)

def _workf(tick_id, host, session=None):
    ticks = yf.getTick(tick_id, session, length=date_len)
    ticks.sort()
    ticks.dumpToMongoDB()
    

class TickDownloadProcess(Process):
    def __init__(self, host, input_queue, error_queue):
        Process.__init__(self)
        self.input_queue = input_queue
        self.error_queue = error_queue
        self.session = initSession()
        self.host = host
    def run(self):
        while not self.input_queue.empty():
            tick_id = self.input_queue.get()
            try:
                _workf(tick_id, self.host, self.session)
            except Exception, e:
                print "%s error, %s" %(tick_id, str(e))
                self.error_queue.put(tick_id, False)


host = argv[1]
date_len = int(argv[2])

tick_ids = tick_timeseries.getTickIDsFromMongoDB(host=host, match={"market":{"$in":[u"東証1部"]}})

initDB(host)

error_queue = Queue(len(tick_ids))
result_queue = Queue(len(tick_ids))
tick_id_queue = Queue(len(tick_ids))
[tick_id_queue.put(i) for i in tick_ids]
pool = [TickDownloadProcess(host, tick_id_queue, error_queue) for _ in xrange(3)]
for p in pool:
    p.start()
for p in pool:
    p.join()
 
while not error_queue.empty():
    print error_queue.get()
