# coding=utf-8

from pyfinance import utils
import jsm
from multiprocessing import Process, Queue
import pymongo

mongo_info= {"host":"localhost", "port":27017, "db":"jp_stock", "collection":"tick_info"}

class BrandDownloadProcess(Process):
    def __init__(self, input_queue, error_queue):
        Process.__init__(self)
        self.input_queue = input_queue
        self.error_queue = error_queue
        self.q = jsm.Quotes()
        self.con = pymongo.MongoClient(host=mongo_info["host"], port=mongo_info["port"]) 
        
    def _workf(self, brand_id):
        ret = self.q.get_brand(brand_id)
        col = self.con[mongo_info["db"]][mongo_info["collection"]]
        for r in ret:
            key = {"tick_id":r.ccode}
            data = {"$set":{"brand_id":brand_id, "market":r.market, "name":r.name, "info":r.info}}
            col.update(key, data, upsert=True)
        
    def run(self):
        while not self.input_queue.empty():
            brand_id = self.input_queue.get(timeout=5)
            try:
                print "get brand %s" % brand_id
                self._workf(brand_id)
#                 self.output_queue.put((brand_id, ret), False)
                print "OK brand %s" % brand_id
            except:
                print "%s error" %(brand_id,)
                self.error_queue.put(brand_id, False)

def getBrands(n_worker=1):
    brand_ids = jsm.Brand.IDS.keys()
    error_queue = Queue(len(brand_ids))
    brand_id_queue = Queue(len(brand_ids))
    [brand_id_queue.put(i) for i in brand_ids]
    print "total brands = %d" % len(brand_ids)
    pool = [BrandDownloadProcess(brand_id_queue, error_queue) for _ in xrange(n_worker)]
    for p in pool:
        p.start()
    for p in pool:
        p.join()
    
    print "collecting results"
    error_list = utils.dump_queue(error_queue)
    
    print "OK"
    return error_list

class FinanceDownloadProcess(Process):
    """財務データ
    market_cap: 時価総額
    shares_issued: 発行済み株式数
    dividend_yield: 配当利回り
    dividend_one: 1株配当
    per: 株価収益率
    pbr: 純資産倍率
    eps: 1株利益
    bps: 1株純資産
    price_min: 最低購入代金
    round_lot: 単元株数
    years_high: 年初来高値
    years_low: 年初来安値
    """

    def __init__(self, input_queue, error_queue):
        Process.__init__(self)
        self.input_queue = input_queue
        self.error_queue = error_queue
        self.q = jsm.Quotes()
        self.con = pymongo.MongoClient(host=mongo_info["host"], port=mongo_info["port"]) 
        
    def _workf(self, tick_id):
        r = self.q.get_finance(tick_id)
        col = self.con[mongo_info["db"]][mongo_info["collection"]]
        key = {"tick_id":tick_id}
        data = {"$set":{"market_cap":r.market_cap, "shares_issued":r.shares_issued,
                        "dividend_yield":r.dividend_yield, "dividend_one":r.dividend_one,
                        "per":r.per, "pbr":r.pbr,"eps":r.eps, "bps":r.bps, "price_min":r.price_min,
                        "round_lot":r.round_lot, "years_high":r.years_high, "years_low":r.years_low}}
        col.update(key, data, upsert=True)
        
    def run(self):
        while not self.input_queue.empty():
            tick_id = self.input_queue.get(timeout=5)
            try:
                print "get tick %s" % tick_id
                self._workf(tick_id)
                print "OK brand %s" % tick_id
            except Exception, e:
                print "%s error" %(tick_id,)
                print e
                self.error_queue.put(tick_id, False)

def getFinances(n_worker=1):
    con = pymongo.MongoClient(host=mongo_info["host"], port=mongo_info["port"])
    col = con[mongo_info["db"]][mongo_info["collection"]]
    tick_ids = sorted(col.distinct("tick_id"))
    error_queue = Queue(len(tick_ids))
    tick_id_queue = Queue(len(tick_ids))
    [tick_id_queue.put(i) for i in tick_ids]
    print "total brands = %d" % len(tick_ids)
    pool = [FinanceDownloadProcess(tick_id_queue, error_queue) for _ in xrange(n_worker)]
    for p in pool:
        p.start()
    for p in pool:
        p.join()
    
    print "collecting results"
    error_list = utils.dump_queue(error_queue)
    
    print "OK"
    return error_list

if __name__ == "__main__":
    ret = getBrands(20)
    print ret
    ret = getFinances(20)
    print ret