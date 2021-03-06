# -*- coding: utf-8 -*-

import re
import datetime
import sqlite3
from BeautifulSoup import BeautifulSoup
from .tick_timeseries import TickTimeSeries, TickerCodeError, _str2date
from .utils import initSession


# yahoo finance jp uses currently EUC-JP encoding
enc = "euc-jp"

def _extractStr(content):
    """extract strings from soup data which contains bold style"""
#    found = content.findAll('b')
#    if found:
#        # <td><small><b>25,810</b></small></td>
#        temp = content.b.string
#    else:
#        # <td><small>26,150</small></td>
#        temp = content.small.string
#
#    temp = content.text
#    string = re.sub(",", "", temp)
    string = re.sub(",", "", content.text)
    return string


def _splitToTick(soup):
    """ split a soup to a tick datum """
    # convert date format yyyy年m月d日 into yyyy/m/d
    contents = soup.contents
    date_str = contents[0].text
    date_temp = re.split("[^0-9]", date_str)
    date = datetime.date(int(date_temp[0]),
            int(date_temp[1]), int(date_temp[2]))
    #date_str = "%04d/%02d/%02d" \
            #    % (int(date_temp[0]),int(date_temp[1]),int(date_temp[2]))

    # extract other price values
    open_v = float(_extractStr(contents[1]))
    high_v = float(_extractStr(contents[2]))
    low_v = float(_extractStr(contents[3]))
    close_v = float(_extractStr(contents[4]))

    #if volume and final_v don't exist, use dummy value
    try:
        volume = float(_extractStr(contents[5]))
        final_v = float(_extractStr(contents[6]))
    except:
        volume = 0.0
        final_v = close_v

    data = [open_v, high_v, low_v, close_v, volume, final_v]

    return date, data

def _getUnitAmount(code, session=None):
    if session is None:
        session = initSession()
    base_url = "http://stocks.finance.yahoo.co.jp/stocks/detail/"
    params = {"code":code}
#     url = base_url % (code,)
#     soup = BeautifulSoup(urllib2.urlopen(url).read())
    soup = BeautifulSoup(session.get(base_url, params=params).text)
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


def getTick(code, session=None, end_date=None, start_date=None, length=500):
    print "getting data of tikker %s from yahoo finance...  " % code

    # initialize
    if session is None:
        session = initSession()
        
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
    while 1 :
        # prepare BeautifulSoup object
        url_t = "http://info.finance.yahoo.co.jp/history/"
        params = {"code":code, "sy":start_y, "sm":start_m, "sd":start_d,
                "ey":end_y, "em":end_m, "ed":end_d, "tm":"d", "p":niter+1}
        
        url_data = session.get(url_t, params=params).text
        soup = BeautifulSoup(url_data)

        # the price data are stored in the following format
        """
        """
        # extract the list of price data from the table
        #price_list = soup.findAll('tr', align="right", bgcolor="#ffffff")
        price_list = soup.find("table", 
                {"class":"boardFin yjSt marB6"}).findAll("tr")[1:]
        if not price_list or len(price_list) == 0:
            break

        # split price_list each day
        for data in price_list:
            date, prices = _splitToTick(data)
            ts[0].append(date)
            ts[1].append(prices)

        # increment iteration counter
        niter += 1

    if ts[0] == []:
        raise TickerCodeError, "Ticker Code %s not found" % code

#     # get unit amount
#     unit_amount = _getUnitAmount(code, session)
    
    dates, data = ts[0], ts[1]
    tick_data = TickTimeSeries(data, tick_id=code, index=dates)
    return tick_data

def dumpUnitAmountToSQL(codes, db_name):
    con = sqlite3.connect(db_name)

    unit_amounts = [_getUnitAmount(code) for code in codes]
    sql_cmd = """INSERT OR REPLACE INTO ticklist(tick_id, unit_amount)
                values (?, ?)"""
    for (code, unit_amount) in zip(codes, unit_amounts):
        print code, unit_amount
        if unit_amount == None:
            continue
        else:
            con.execute(sql_cmd, (code, unit_amount))

    con.commit()
    con.close()


def readUnitAmountFromSQL(codes, db_name):
    con = sqlite3.connect(db_name)
    strcodes = str(list(codes))[1:-1] 
    sql_cmd = "SELECT * FROM ticklist WHERE tick_id IN (%s)" % (strcodes,)
    res = con.execute(sql_cmd).fetchall()
    con.close()
    return res
