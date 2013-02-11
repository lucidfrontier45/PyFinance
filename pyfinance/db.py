import os.path
import sqlite3

_db_name = "/home/du/nikkei225.db"

def setDBName(db_name):
    global _db_name
    _db_name = os.path.abspath(db_name)

def getDBName():
    return _db_name

def initDB(db_name=None):
    if db_name:
        setDBName(db_name)
    db = sqlite3.connect(_db_name)
    db.execute("DROP TABLE IF EXISTS ticklist;")
    db.execute("DROP TABLE IF EXISTS tickdata;")
    db.execute("CREATE TABLE ticklist(tick_id, unit_amount, last_date);")
    db.execute("CREATE UNIQUE INDEX ticklist_idx on ticklist(tick_id);")
    db.execute("CREATE TABLE tickdata(tick_id, date, open_v, high_v,\
                                low_v, close_v, volume, final_v);")
    db.execute("CREATE UNIQUE INDEX data_idx on tickdata(tick_id, date);")
    db.commit()
    db.close()

