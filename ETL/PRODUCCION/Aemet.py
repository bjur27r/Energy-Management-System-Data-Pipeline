#!/usr/bin/env python
import time
import requests
import pandas as pd
from datetime import datetime

id_2 = "06006"
url = "https://opendata.aemet.es/opendata/api/prediccion/especifica/municipio/horaria/" + str(id_2)

"https://opendata.aemet.es/opendata/api/prediccion/especifica/municipio/horaria/06006/?/?api_key=xxxxxxxxxxxxxxxx"
/opendata/api/valores/climatologicos/inventarioestaciones/todasestaciones/?api_key=xxxxxxxxxxxxxxxxx HTTP/1.1

querystring = {
    "api_key": "xxxxxxxo"}
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

daily_df = pd.DataFrame()
time_2 = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
for i in range(0, 3):
    d = a[0]['prediccion']['dia'][i]
    d_2 = d['estadoCielo']
    df = pd.DataFrame.from_dict(d_2)
    df['time'] = time_2
    df['fecha'] = d['fecha']
    d_2 = d['humedadRelativa']
    df_2 = pd.DataFrame.from_dict(d_2)
    df_2 = df.merge(df_2, on="periodo", how="inner")
    df_2 = df_2.rename(index=str, columns={"value_x": "nubosidad", "value_y": "humedad"})
    d_2 = d['nieve']
    df_3 = pd.DataFrame.from_dict(d_2)
    df_2 = df_2.merge(df_3, on="periodo", how="inner")
    df_2 = df_2.rename(index=str, columns={"value": "nieve"})
    d_2 = d['precipitacion']
    df_3 = pd.DataFrame.from_dict(d_2)
    df_2 = df_2.merge(df_3, on="periodo", how="inner")
    df_2 = df_2.rename(index=str, columns={"value": 'precipitacion'})
    d_2 = d['sensTermica']
    df_3 = pd.DataFrame.from_dict(d_2)
    df_2 = df_2.merge(df_3, on="periodo", how="inner")
    df_2 = df_2.rename(index=str, columns={"value": 'sensTermica'})
    d_2 = d['temperatura']
    df_3 = pd.DataFrame.from_dict(d_2)
    df_2 = df_2.merge(df_3, on="periodo", how="inner")
    df_2 = df_2.rename(index=str, columns={"value": 'temperatura'})
    d_2 = d['vientoAndRachaMax']
    df_3 = pd.DataFrame.from_dict(d_2)
    df_2 = df_2.merge(df_3, on="periodo", how="inner")
    df_2 = df_2.rename(index=str, columns={"value": 'vientoAndRachaMax'})
    df_2['id'] = id_2
    daily_df = daily_df.append(df_2)


time_2 = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
time_2 = str(time_2) + id_2
file_name = "/home/bju/data_app/" + time_2
daily_df.to_csv(file_name, encoding='utf-8', index=False)
