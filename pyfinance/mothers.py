# -*- coding: utf-8 -*-

import requests
from BeautifulSoup import BeautifulSoup

_mothers_url = "http://mothers.tse.or.jp/listed_companies/listing.html"

_mothers_id = {}
_mothers_name = {}
_init_flag = False

__all__ = ["getMothersIdHash", "getMothersNameHash"]

def _fetchMothers():
    global _mothers_id
    global _mothers_name

    print "fetching mothers list from " + _mothers_url
    html = requests.get(_mothers_url).content
    soup = BeautifulSoup(html,
            convertEntities=BeautifulSoup.HTML_ENTITIES)
    
    
    table_dat = soup.find("tbody").findAll("tr")

    for d in table_dat:
        tick_id, tick_name = d.findAll("td")[:2]
        tick_id = int(tick_id.a.text)
        tick_name = tick_name.a.text
        _mothers_name[tick_id] = tick_name
        _mothers_id[tick_name] = tick_id


def getMothersIdHash(refresh=False):
    if refresh or not _init_flag:
        _fetchMothers()

    return _mothers_id
    
def getMothersNameHash(refresh=False):
    if refresh or not _init_flag:
        _fetchMothers()

    return _mothers_name
