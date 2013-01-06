# -*- coding: utf-8 -*-

import urllib2
from BeautifulSoup import BeautifulSoup

_nikkei_url = 'http://indexes.nikkei.co.jp/nkave/index/component?idx=nk225'

_nikkei225_id = {}
_nikkei225_name = {}

__all__ = ["getNikkei225IdHash", "getNikkei225NameHash"]

def _fetchNikkei225():
    global _nikkei225_id
    global _nikkei225_name

    print "fetching nikkei225 list from " + _nikkei_url
    html = urllib2.urlopen(_nikkei_url).read()
    soup = soup = BeautifulSoup(html,
            convertEntities=BeautifulSoup.HTML_ENTITIES)
    table_dat = soup.findAll("tr",{"class":"cmn-stocknames_odd cmn-charcter"})\
            + soup.findAll("tr", {"class":"cmn-stocknames_even cmn-charcter"})

    for d in table_dat:
        tick_id = int(d.contents[1].text)
        tick_name = d.contents[3].a.text
        _nikkei225_name[tick_id] = tick_name
        _nikkei225_id[tick_name] = tick_id


def getNikkei225IdHash(refresh=False):
    if refresh:
        _fetchNikkei225()

    return _nikkei225_id
    
def getNikkei225NameHash(refresh=False):
    if refresh:
        _fetchNikkei225()

    return _nikkei225_name
    
_fetchNikkei225()
