

import sys
import datetime
import random
from random import randrange
import re
import copy
import datetime

# Set number of simulated messages to generate
if len(sys.argv) > 1:
    num_msgs = int(sys.argv[1])
else:
    num_msgs = 1

# Fixed values
guid_base = "0-ZZZ12345678-"
destination = "0-AAA12345678"
format = "urn:example:sensor:temp"


iotmsg_header = """\
{ "guid": "%s", 
  "destination": "%s", 
  "Location": "%s", """

iotmsg_eventTime = """\
  "eventTime": "%sZ", """

iotmsg_payload = """\
  "payload": {"format": "%s", """

iotmsg_data = """\
	 "data": { "consumed": %.1f,"generated":%.1f }   
	 }
}"""

paneles_kw = 5
WD_Cons_kwh = 10
NWD_Cons_kwh = 5
pool_price = 0.05


# Curva generacion solar


daily_gen_base = {'1': 0, '2': 0, '3': 0, '4': 0,
                  '5': 0, '6': 0, '7': 5, '8': 8,
                  '9': 15, '10': 25, '11': 50, '12': 70,
                  '13': 90, '14': 100, '15': 100, '16': 100,
                  '17': 80, '18': 70, '19': 40, '20': 15,
                  '21': 5, '22': 0, '23': 0, '24': 0}


# Curva Consumo base dia laborable

WD_daily_con_base = {'1': 1, '2': 1, '3': 1, '4': 1,
             '5': 1, '6': 1, '7': 5, '8': 6,
             '9': 5, '10': 5, '11': 3, '12': 3,
             '13': 3, '14': 6, '15': 6, '16': 6,
             '17': 2, '18': 2, '19': 2, '20': 8,
             '21': 0, '22': 0, '23': 0, '24': 0}

#Ultima Temperatura registrada

fecha_start = datetime.datetime.strptime('2018-04-16  01:00:00', '%Y-%m-%d %H:%M:%S')
d = datetime.timedelta(minutes=15)

moment = fecha_start
for i in range(10, 50):
    moment = moment + d
    state = "active"
    if moment.weekday() in [1, 2, 3, 4]:
        consm_delta = random.uniform(-20, 20)
        consm = (WD_daily_con_base[str(moment.hour)] / 4) * (1 + (consm_delta / 100))
        gen_delta = random.uniform(-20, 20)
        gen = (daily_gen_base[str(moment.hour)] / 4) * (1 + (gen_delta / 100))
    if moment.weekday() in [5, 6, 0]:
        consm_delta = random.uniform(-20, 20)
        consm = (WD_daily_con_base[str(moment.hour)] / 4) * (1 + (consm_delta / 100))
        gen_delta = random.uniform(-20, 20)
        gen = (daily_gen_base[str(moment.hour)] / 4) * (1 + (gen_delta / 100))

    print re.sub(r"[\s+]", "", iotmsg_header) % (guid_base, destination, state),
    print re.sub(r"[\s+]", "", iotmsg_eventTime) % (moment),
    print re.sub(r"[\s+]", "", iotmsg_payload) % (format),
    print re.sub(r"[\s+]", "", iotmsg_data) % (consm, gen)

