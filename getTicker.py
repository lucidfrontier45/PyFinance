#! /usr/bin/python

from sys import argv, exit
import time
import sqlite3
import pyfinance.yahoo_finance_jp as yf
from pyfinance.db import initDB, setDBName
from pyfinance import nikkei225
from multiprocessing import Process, Queue
from pyfinance.utils import initSession

def _workf(tick_id, session=None):
    ticks = yf.getTick(tick_id, session, length=date_len)
    ticks.sort()
    for _ in xrange(10):
        try:
            ticks.dumpToSQL()
            break
        except sqlite3.OperationalError, e:
            print e
            time.sleep(1)
    

class TickDownloadProcess(Process):
    def __init__(self, input_queue, error_queue):
        Process.__init__(self)
        self.input_queue = input_queue
        self.error_queue = error_queue
        self.session = initSession()
    def run(self):
        while not self.input_queue.empty():
            tick_id = self.input_queue.get()
            try:
                _workf(tick_id, self.session)
            except:
                print "%d error" %(tick_id,)
                self.error_queue.put(tick_id, False)

db_name = argv[1]
setDBName(db_name)

if argv[2] == "-init":
    initDB()
    exit()

tick_ids = sorted(nikkei225.getNikkei225NameHash().keys())
error_queue = Queue(len(tick_ids))
result_queue = Queue(len(tick_ids))
tick_id_queue = Queue(len(tick_ids))

date_len = int(argv[2])

[tick_id_queue.put(i) for i in tick_ids]
pool = [TickDownloadProcess(tick_id_queue, error_queue) for _ in xrange(10)]
for p in pool:
    p.start()
for p in pool:
    p.join()

while not error_queue.empty():
    print error_queue.get()