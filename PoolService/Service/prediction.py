import time
import requests
import pandas as pd
from datetime import datetime
import Model.aemet_v
import Model.omie

def start(year,mon,day_3):

    year = str(year)
    month = str(mon) if mon > 9 else "0" + str(mon)
    day = "30"
    day2 = "29"

    def pool_cat(d):
        a = d['value']
        a = str(int(a / 10))
        return a

    input_f=  Model.aemet_v.met(year, month, day)
    poolP = Model.omie.get_pool_p(year, month, day)
    cons = pd.merge(input_f, poolP, left_on='dia', right_on='dia')
    cons['Pool_Level'] = cons.apply(lambda x: pool_cat(x), axis=1)
