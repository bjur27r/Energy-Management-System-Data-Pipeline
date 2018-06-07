import time
import requests
import pandas as pd
from datetime import datetime

f = pd.DataFrame()





def met(year, mon, day):
    def tipos(d):
        if (d['prec'] == "lp") | (d['prec'] == "Ip") | (d['prec'] == "Acum"):
            d['prec'] = 0
        else:
            d['prec'] = float(d['prec'])

        d['presMax'] = float(d['presMax'])
        d['presMin'] = float(d['presMin'])
        d['sol'] = float(d['sol'])
        d['tmax'] = float(d['tmax'])
        d['tmin'] = float(d['tmin'])
        d['velmedia'] = float(d['velmedia'])
        return d

    def altur(d):
        a = d['altitud']
        a = float(a)
        g = 0
        if a < 50:
            g = "a"
        if 50 < a < 100:
            g = "b"
        if 100 < a < 200:
            g = "c"
        if 200 < a < 500:
            g = "d"
        if 500 < a < 700:
            g = "e"
        if 700 < a < 900:
            g = "e"
        if a > 900:
            g = "f"

        return g


    #    day2= int(day) - 1
    year = str(year)
    month = str(mon) if mon > 9 else "0" + str(mon)
    day = "30"
    day2 = "29"
    url = 'https://opendata.aemet.es/opendata/api/valores/climatologicos/diarios/datos/fechaini/' + year + '-' + month + '-' + str(
        day2) + 'T00%3A00%3A01UTC/fechafin/' + year + '-' + month + '-' + str(day) + 'T00%3A00%3A01UTC/todasestaciones'
    print(url)
    querystring = {
        "api_key": "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJianVyMjdyQGdtYWlsLmNvbSIsImp0aSI6IjlkZTBlNGYyLTU4MWItNGNhZC1hNzg0LTUzMzkyZWYwNDA0OCIsImlzcyI6IkFFTUVUIiwiaWF0IjoxNTE5ODkxNjQ3LCJ1c2VySWQiOiI5ZGUwZTRmMi01ODFiLTRjYWQtYTc4NC01MzM5MmVmMDQwNDgiLCJyb2xlIjoiIn0.Js4vLF861ioKCIZnDESNcxxIL47Vwkx6uhl8H5RRm5o"}
    headers = {
        'cache-control': "no-cache"
    }

    response = requests.request("GET", url, headers=headers, params=querystring, verify=False)

    a = response.json()

    b = a['datos'].encode()
    url = b
    headers = {
        'cache-control': "no-cache"
    }

    response = requests.request("GET", url, headers=headers, verify=False)

    a = response.json()
    f = pd.DataFrame(a)
    c = pd.DataFrame(f)
    c.to_csv("AEMET2015_017", encoding='utf-8', index=False, decimal=',', float_format='%.3f')
    f = pd.read_csv("AEMET2015_017", encoding='utf-8', decimal=',')
    f['alt2'] = f.apply(altur, axis=1)
    f_2 = f.dropna()
    f_2 = f_2[['alt2', 'provincia', 'fecha', 'prec', 'presMax', 'presMin', 'sol', 'tmax', 'tmin', 'velmedia']]
    f_2['prec'] = f_2['prec'].str.replace(",", ".")
    d = f_2.apply(tipos, axis=1)
    f_2_b = d.groupby(['provincia', 'fecha'])['prec', 'presMax', 'presMin', 'sol', 'tmax', 'tmin', 'velmedia'].mean()
    f_3 = f_2_b.reset_index()
    melted = pd.melt(f_3, id_vars=["provincia", "fecha"],
                     var_name="variable", value_name="Value")
    melted['var'] = melted.apply(lambda x: (str(x['provincia'].encode('utf-8')) + str(x['variable'].encode('utf-8'))),
                                 axis=1)
    input_f = (melted.pivot_table(index=['fecha'], columns='var', values='Value').reset_index())
    input_f['fecha'] = pd.to_datetime(input_f['fecha'], format='%Y-%m-%d')
    input_f['dia'] = input_f['fecha'].dt.strftime('%d/%m/%Y')

    return input_f
