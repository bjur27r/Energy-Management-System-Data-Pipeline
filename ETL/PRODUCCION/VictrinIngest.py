
from VRM import VRM_API
import pandas as pd
from datetime import datetime as dat_2
import datetime



def date_u(x):
    date_2 = datetime.datetime.fromtimestamp(x/1000.0)
    return date_2


api = VRM_API(username="xxxxxxxxxxxxxm", password="xxxxxxxxxxxxxre")

stats = api.get_custom_stats(17210)
records = stats['records']
records_df = pd.DataFrame(records['Bc'])
records_df = records_df.rename(index=str, columns={0: "date", 1: "Bc"})

list_cods = ['Gb','Gc','Gu','Pb','Pc','Pg','Pv','gb','gc']
for item in list_cods:
    try:
        tmp = pd.DataFrame(records[item])
        tmp  = tmp .rename(index=str, columns={0: "date", 1: item})
        records_df = records_df .merge(tmp, left_on='date', right_on='date', how='outer')
    except:
        a= 0
records_df['date'] = records_df['date'].apply(lambda x:date_u(x))
time_2 = dat_2.now().strftime('%Y-%m-%d %H:%M:%S')
time_2 = "VICTRON"+ str(time_2)
file_name = "/home/bju/data_app/" + time_2
records_df.to_csv(file_name, encoding='utf-8', index=False)

