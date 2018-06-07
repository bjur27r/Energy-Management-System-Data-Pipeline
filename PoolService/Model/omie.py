#!/usr/bin/env python
import url
import time
import requests
import pandas as pd
from datetime import datetime
import urllib


def get_pool_p(year, mon, day):
    url = "https://api.esios.ree.es/indicators"
    f = pd.DataFrame()
    year = str(year)
    month = str(mon) if mon > 9 else "0" + str(mon)
    day = "30"
    day2 = "29"

    url = "https://api.esios.ree.es/indicators/10229?start_date=" + year + "-" + month + "-" + day2 + "T00%3A00%3A00Z&end_date=" + year + "-" + month + "-" + day + "T07%3A34%3A17Z"

    headers = {
        'Accept': 'application/json; application/vnd.esios-api-v1+json',
        'Host': 'api.esios.ree.es',
        'Content-Type': 'application/json',
        'Authorization': 'Token token="9569e14bd1ba7c6fd21117255d153a5557e7b43d4f5aaa91da3b2fa5d04e3b13"'
    }

    response = requests.request("GET", url, headers=headers)

    a = response.json()
    a = a['indicator']['values']
    c = pd.DataFrame(a)
    c['datetime'] = pd.to_datetime(c['datetime'])
    c['dia'] = c['datetime'].dt.strftime('%m/%d/%Y')
    f = c.groupby(['dia', 'geo_name'])['value'].mean()
    f = pd.DataFrame(f)
    #f['dia'] = pd.to_datetime(f['dia'], format='%m/%d/%Y')
    #f['dia_2'] = f['dia'].dt.strftime('%d/%m/%Y')

    return f