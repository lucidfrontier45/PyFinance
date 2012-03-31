# -*- coding: utf-8 -*-

import re
import datetime
import urllib2
import sqlite3
from BeautifulSoup import BeautifulSoup
from timeseries import TickTimeSeries, TickerCodeError, _str2date

# yahoo finance jp uses currently EUC-JP encoding
enc = "euc-jp"


def _extractStr(content):
    """extract strings from soup data which contains bold style"""
    found = content.findAll('b')
    if found:
        # <td><small><b>25,810</b></small></td>
        temp = content.b.string
    else:
        # <td><small>26,150</small></td>
        temp = content.small.string
    string = re.sub(",", "", temp)
    return string


def _splitToTick(soup):
    """ split a soup to a tick datum """
    # convert date format yyyy年m月d日 into yyyy/m/d
    date_str = soup.contents[1].small.string
    date_str = re.sub(u"日", u"", re.sub(u"[年月]", "/", date_str))
    date_temp = date_str.split("/")
    date = datetime.date(int(date_temp[0]),
            int(date_temp[1]), int(date_temp[2]))
    #date_str = "%04d/%02d/%02d" \
            #    % (int(date_temp[0]),int(date_temp[1]),int(date_temp[2]))

    # extract other price values
    open_v = float(_extractStr(soup.contents[3]))
    max_v = float(_extractStr(soup.contents[5]))
    min_v = float(_extractStr(soup.contents[7]))
    close_v = float(_extractStr(soup.contents[9]))

    #if volume does not exist, use dummy value
    try:
        volume = float(_extractStr(soup.contents[11]))
    except:
        volume = 0.0

    data = [open_v, max_v, min_v, close_v, volume]

    return date, data


def getTick(code, end_date=None, start_date=None, length=500):
    print "getting data of tikker %s from yahoo finance...  " % code

    # initialize
    scale = 8.0 / 5.0  # for skipping hollidays
    if end_date == None:
        # set default end_date = today
        end_date = datetime.date.today()
    elif isinstance(end_date, str):
        end_date = _str2date(end_date)

    if start_date == None:
        # set default start_date = today - length * scale
        start_date = end_date - datetime.timedelta(days=length * scale)
    elif isinstance(start_date, str):
        start_date = _str2date(start_date)
        length = (end_date - start_date).days

    print "get data from %s to %s" % (start_date, end_date)
    start_m, start_d, start_y = start_date.month, \
            start_date.day, start_date.year
    end_m, end_d, end_y = end_date.month, end_date.day, end_date.year

    # the tables of Yahoo finance JP contains up to 50 rows
    # thus parsing html must be done iteratively
    ts = [[], []]  # an array to store the result, [list_dates,list_prices]
    niter = 0  # iteration counter
    while(niter < length):
        #print niter

        # prepare BeautifulSoup object
        url_t = "http://table.yahoo.co.jp/t"\
         + "?s=%s&a=%s&b=%s&c=%s&d=%s&e=%s&f=%s&g=d&q=t&y=%d&z=%s&x=.csv"\
         % (code, start_m, start_d, start_y, end_m, end_d, end_y,\
            niter, code)
        #print url_t
        url_data = unicode(urllib2.urlopen(url_t).read(), enc, 'ignore')
        soup = BeautifulSoup(url_data)

        # the price data are stored in the following format
        """
        <tr align="right" bgcolor="#ffffff">
        <td><small>2007年10月4日</small></td>
        <td><small>64,300</small></td>
        <td><small>64,900</small></td>
        <td><small>63,900</small></td>
        <td><small><b>64,400</b></small></td>
        <td><small>1,058,900</small></td>
        <td><small>64,400</small></td>
        </tr><
        """
        # extract the list of price data from the table
        price_list = soup.findAll('tr', align="right", bgcolor="#ffffff")

        # split price_list each day
        for data in price_list:
            date, prices = _splitToTick(data)
            ts[0].append(date)
            ts[1].append(prices)

        # increment iteration counter
        niter += 50

    if ts[0] == []:
        raise TickerCodeError, "Ticker Code %s not found" % code
    return TickTimeSeries(ts[0], ts[1], code)


def getUnitAmount(code):
    base_url = "http://stocks.finance.yahoo.co.jp/stocks/detail/?code=%s"
    url = base_url % (code,)
    soup = BeautifulSoup(urllib2.urlopen(url).read())
    all_dl = soup.findAll("dl")
    text_unit_amount = None
    for dl in all_dl:
        try:
            if dl.dt.contents[0] == u"単元株数":
                text_unit_amount = dl.dd.contents[0].text
                break
        except:
            pass

    if text_unit_amount == None:
        return None
    else:
        try:
            unit_amount = int(text_unit_amount.replace(",", ""))
        except ValueError:
            unit_amount = 1

    return unit_amount


def dumpUnitAmountToSQL(codes, db_name):
    db = sqlite3.connect(db_name)

    try:
        db.execute("CREATE TABLE unitamount(tick_id, unit_amount)")
        db.execute("CREATE UNIQUE INDEX tick_idx2 on uitamount(tick_id)")
    except sqlite3.OperationalError:
        pass

    unit_amounts = [getUnitAmount(code) for code in codes]
    sql_cmd = """insert or replace into unitamount(tick_id, unit_amount)
                values (?, ?)"""
    for (code, unit_amount) in zip(codes, unit_amounts):
        print code, unit_amount
        if unit_amount == None:
            continue
        else:
            db.execute(sql_cmd, (code, unit_amount))

    db.commit()
    db.close()


def readUnitAmountFromSQL(codes, db_name):
    db = sqlite3.connect(db_name)
    strcodes = "(" + str(list(codes))[1:-1] + ")"
    sql_cmd = "select * from unitamount where tick_id in %s" % (strcodes,)
    res = db.execute(sql_cmd).fetchall()
    db.close()
    return res
