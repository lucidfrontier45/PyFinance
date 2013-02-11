from copy import deepcopy
import numpy as np
import sqlite3
import datetime
import pandas as pd
from pandas import DataFrame

from . import indicators
from .db import getDBName

_col_names = ("open_v", "high_v", "low_v", "close_v", "volume", "final_v")


class TickerCodeError(Exception):
    pass


class TickTimeSeries(DataFrame):
    def __init__(self, tick_id, dates, data, unit_amount=None):
#        print tick_id, dates, data
        DataFrame.__init__(self, data, index=dates, columns=_col_names)
        self.tick_id = tick_id
        self.sort(inplace=True)
        self.unit_amount = unit_amount
        
    @property
    def dates(self):
        return self.index

    def dumpToSQL(self, db_name=None):
        if db_name == None:
            db_name = getDBName()
        
        con = sqlite3.connect(db_name)
        
        # first update ticklist
        stmt = """INSERT OR REPLACE INTO ticklist(
               tick_id, unit_amount, last_date) VALUES(?, ?, ?)"""
        last_date = str(self.dates[-1])
        con.execute(stmt, (self.tick_id, self.unit_amount, last_date))

        # then insert data into tickdata
        stmt = """INSERT OR REPLACE INTO tickdata(
            tick_id, date, open_v, high_v, low_v, close_v, volume, final_v)
            values (?, ?, ?, ?, ?, ?, ?, ?)"""
        data_to_insert = zip([self.tick_id]*len(self), self.dates, self.open_v,
             self.high_v, self.low_v, self.close_v, self.volume, self.final_v)
        con.executemany(stmt, data_to_insert)
        
        
        #print sql_cmd
#        for s in self:
#            # insert data
#            con.execute(sql_cmd, (self.tick_id, str(s[0]), s[1][0], s[1][1],
#                    s[1][2], s[1][3], s[1][4]))

        # finalize
        con.commit()
        con.close()


def getTickIDsFromSQL(db_name=None):
    if db_name == None:
        db_name = getDBName()
    con = sqlite3.connect(db_name)
    ids = con.execute("SELECT tick_id FROM ticklist").fetchall()
    ids = [i[0] for i in ids]
    con.close()
    return ids


def _getOneTickDataFromSQL(tick_id, db_name=None, size=-1,
                           begin_date=None, end_date=None):
    # get data from sqlite database
    if db_name == None:
        db_name = getDBName()
    con = sqlite3.connect(db_name)

    # first check if the requested tick_id exists
    sql_cmd = "SELECT unit_amount FROM ticklist WHERE tick_id=?"
    iid = con.execute(sql_cmd, (tick_id,)).fetchone()
    if iid == None:
        con.close()
        raise TickerCodeError, "Ticker Code %d not found" % tick_id
    else:
        unit_amount = iid[0]
        

    # get tick data
    sql_cmd = "SELECT * FROM tickdata WHERE tick_id=?"
    query_values = [tick_id]

    if begin_date:
        sql_cmd += " AND date >= ?"
        query_values.append(str(begin_date))

    if end_date:
        sql_cmd += " AND date <= ?"
        query_values.append(str(end_date))

    sql_cmd += " ORDER BY date DESC"

    # execute sql command
    cmd_res = con.execute(sql_cmd, query_values)
    if size > 0:
        res = cmd_res.fetchmany(size)
    else:
        res = cmd_res.fetchall()
    con.close()

    # create a TickTimeSeries
    dates = []
    data = []
    for r in res:
        dates.append(_str2date(r[1]))
        data.append(r[2:])

    ts = TickTimeSeries(tick_id, dates, data, unit_amount)
    ts.sort()
    return ts


def getTickDataFromSQL(tick_ids=[], db_name=None, size=-1,
                       begin_date=None, end_date=None):
    if db_name == None:
        db_name = getDBName()

    if len(tick_ids) == 0:
        tick_ids = getTickIDsFromSQL(db_name)

    tick_data = []
    result_ids = []
    for tick_id in tick_ids:
        try:
            ts = _getOneTickDataFromSQL(tick_id, db_name, size,
                                        begin_date, end_date)
            tick_data.append(ts)
            result_ids.append(tick_id)
        except:
            pass

#    min_len = min([len(ts) for ts in tick_data])
#    tick_ids = []
#    for i, ts in enumerate(tick_data):
#        ts_len = len(ts)
#        truncated_dates = ts.dates[ts_len - min_len:]
#        truncated_data = ts.data[ts_len - min_len:]
#        tick_id = ts.tick_id
#        truncated_ts = TickTimeSeries(truncated_dates, truncated_data, tick_id)
#        tick_data[i] = truncated_ts
#        tick_data[i].sort()
#        tick_ids.append(tick_id)

    return result_ids, tick_data


def alignTickData(tick_data, col="final_v", norm=True):
    """aligh every tick data
    
    params
    ------
    tick_data : list, a list of tick data
    col : string, column name of value type to use
    norm : boolean, normalize with unit amount
    
    return
    ------
    DataFrame of aligned tick data
    """
    
    if norm:
        dfs = [pd.DataFrame(t[col] * t.unit_amount, 
                        columns=[str(t.tick_id)]) for t in tick_data]
    else:
        dfs = [pd.DataFrame(t[col],
                        columns=[str(t.tick_id)]) for t in tick_data]
    
    ts = dfs[0].join(dfs[1:])
    return ts

def filterPeakedTickIDs(tick_ids=[], db_name=None, ratio=0.9,
                        size=10, value_type="close_v"):
    if db_name == None:
        db_name = getDBName()
    if len(tick_ids) == 0:
        tick_ids = getTickIDsFromSQL(db_name)
    con = sqlite3.connect(db_name)
    maxval_cmd = "select max(%s) from tickdata where tick_id=?" % (value_type,)
    general_cmd = "select %s from tickdata where tick_id=? order by date desc"\
                                                                % (value_type,)
    ret_ids = []
    for tick_id in tick_ids:
        max_val = con.execute(maxval_cmd, (tick_id,)).fetchone()[0]
        values = con.execute(general_cmd, (tick_id,)).fetchmany(size)
        if np.max(values) < max_val * ratio:
            ret_ids.append(tick_id)

    con.close()

    return ret_ids


def getGrowingTickIDs(tick_ids=[], db_name=None, ratio=0.005,
                        size=20, value_type="close_v"):
    if db_name == None:
        db_name = getDBName()
    if len(tick_ids) == 0:
        tick_ids = getTickIDsFromSQL(db_name)
    con = sqlite3.connect(db_name)
    general_cmd = "select %s from tickdata where tick_id=? order by date desc"\
                                                                % (value_type,)
    ret_ids = []
    for tick_id in tick_ids:
        values = ([val[0] for val in con.execute(general_cmd,
                  (tick_id,)).fetchmany(size)])[::-1]
        roc = indicators.roc(values) * 0.01
        avr_roc = np.mean(roc[1:])
        if avr_roc > ratio:
            ret_ids.append(tick_id)
            
    con.close()

    return ret_ids


def _str2date(s):
    """ convert strings to date object
    2010-10-29 to datetime.date(2010, 10, 29)"""
    temp = [int(ss) for ss in s.split("-")]
    date = datetime.date(temp[0], temp[1], temp[2])
    return date