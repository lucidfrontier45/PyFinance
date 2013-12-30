from pyfinance import utils
import jsm
from multiprocessing import Process, Queue
import pymongo

mongo_info= {"host":"localhost", "port":27017, "db":"jp_stock", "collection":"brands"}

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
            key = {"brand":brand_id}
            data = {"$set":{"tick_id":r.ccode, "market":r.market, "name":r.name, "info":r.info}}
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
                print "%d error" %(brand_id,)
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


if __name__ == "__main__":
    ret = getBrands(10)
    print ret