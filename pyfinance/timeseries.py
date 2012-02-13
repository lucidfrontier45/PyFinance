from copy import deepcopy
import numpy as np
import sqlite3
import datetime 

class TickerCodeError(Exception):
  pass

def _convertArrayType(data,mode="numpy"):
  """
  convert list <=> numpy ndarray
  mode = "numpy" or "list"
  """
  if mode == "numpy":
    return np.array(data)
  else :
    return np.array(data).tolist()


class TimeSeries():

  def __init__(self,dates,data):
    if len(dates) != len(data):
      raise ValueError, "the lengths of dates and data must be equal"
    self.dates = deepcopy(dates)
    self.data = deepcopy(data)

  def __len__(self):
    return len(self.dates)

  def __getitem__(self,i):
    return [self.dates[i],self.data[i]]

  def __setitem__(self,i,args):
    self.dates[i] = args[0]
    self.data[i] = args[1]

  def __str__(self):
    s = []
    for i in xrange(len(self)):
       temp_s = "%s %s" % (self.dates[i].__str__(),self.data[i].__str__())
       s.append(temp_s) 
    return s.__str__()

  def __iter__(self):
    for i in xrange(len(self)):
      yield self[i]

  def sort(self,order=True):
    temp = sorted(zip(self.dates,self.data),reverse=order)
    self.dates = [t[0] for t in temp]
    self.data = [t[1] for t in temp]

class TickTimeSeries(TimeSeries):
  def __init__(self,dates,data,tick_id,mode="numpy"):
    self.tick_id = tick_id
    self._mode = mode
    TimeSeries.__init__(self,dates,_convertArrayType(data,mode))
  
  def convertArrayType(self,mode="numpy"):
    self._mode = mode
    self.data = _convertArrayType(self.data,mode)

  def sort(self,order=True):
    TimeSeries.sort(self,order)
    self.convertArrayType(mode=self._mode)
  
  def dumpToSQL(self,db_name):
    db = sqlite3.connect(db_name)

    # attempt to create a table
    try :
      db.execute("CREATE TABLE ticklist(tick_id)")
      db.execute("CREATE TABLE tickdata(tick_id,date,open_v,high_v,\
          low_v,close_v)")
    except sqlite3.OperationalError:
      pass
    
    # insert tick_id to ticklist
    rowid = db.execute("SELECT rowid from ticklist where tick_id = '%s'" % \
        self.tick_id).fetchone()
    if rowid == None:
      db.execute("INSERT INTO ticklist(tick_id) VALUES('%s')"%(self.tick_id))
    
    for s in self:
      # first delete duplicate data
      db.execute("DELETE FROM tickdata WHERE tick_id = '%s' AND date = '%s'" \
          % (self.tick_id,str(s[0])))
      # insert data
      sql_cmd = "INSERT INTO tickdata(tick_id,date,open_v,high_v,low_v,close_v)\
          VALUES ('%s', '%s', %f, %f, %f, %f)" % (self.tick_id,str(s[0]),\
          s[1][0],s[1][1],s[1][2],s[1][3])
      db.execute(sql_cmd)

    # finalize
    db.commit()
    db.close()

def getTickIDs(db_name):
    db = sqlite3.connect(db_name)
    ids = db.execute("SELECT tick_id FROM ticklist").fetchall()
    db.close()
    return ids

def readFromSQL(db_name,tick_id):
    # get data from sqlite database
    db = sqlite3.connect(db_name)
    iid = db.execute("SELECT tick_id FROM ticklist where tick_id = '%s'"\
        %tick_id).fetchone()
    if iid == None:
      db.close()
      raise TickerCodeError, "Ticker Code %s not found" % tick_id
    res = db.execute("SELECT * FROM tickdata WHERE tick_id = '%s'" % \
        tick_id).fetchall()
    db.close()

    # create a TickTimeSeries
    dates = []
    data = []
    for r in res:
      dates.append(_str2date(r[1]))
      data.append(r[2:])

    return TickTimeSeries(dates,data,tick_id)
    
def _str2date(s):
  """ convert strings to date object
  2010-10-29 to datetime.date(2010, 10, 29)"""
  temp = [int(ss) for ss in s.split("-")]
  date = datetime.date(temp[0],temp[1],temp[2])
  return date

