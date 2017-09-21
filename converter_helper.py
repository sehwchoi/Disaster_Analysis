import pandas as pd
import csv
import re
from pytz import timezone
import datetime
from logging import raiseExceptions

utc_zone = {
    "HST":"UTC-10",
    "AKST":"UTC-9",
    "PST":"UTC-8",
    "MST":"UTC-7",
    "CST":"UTC-6",
    "EST":"UTC-5",
    "AST":"UTC-4",
    "ChST":"UTC+10"  #chamorro standard time
    }

state_time_zone = {
    "AL":"CST", 
    "AK":"AKST",
    "AZ":"MST",
    "AR":"CST",
    "CA":"PST",
    "CO":"MST", 
    "CT":"EST",
    "DE":"EST",
    "FL":"EST",
    "GA":"EST", 
    "HI":"HST",
    "ID":"MST",
    "IL":"CST",
    "IN":"EST",
    "IA":"CST", 
    "KS":"CST",
    "KY":"CST",
    "LA":"CST",
    "ME":"EST",
    "MD":"EST", 
    "MA":"EST",
    "MI":"EST",
    "MN":"CST",
    "MS":"CST",
    "MO":"CST", 
    "MT":"MST",
    "NE":"CST",
    "NV":"PST",
    "NH":"EST", 
    "NJ":"EST",
    "NM":"MST",
    "NY":"EST",
    "NC":"EST",
    "ND":"CST", 
    "OH":"EST",
    "OK":"CST",
    "OR":"PST",
    "PA":"EST",
    "RI":"EST", 
    "SC":"EST",
    "SD":"CST",
    "TN":"CST",
    "TX":"CST",
    "UT":"MST", 
    "VT":"EST",
    "VA":"EST",
    "WA":"PST",
    "DC":"EST",
    "WV":"EST",
    "WI":"CST",
    "WY":"MST",
    "PR":"AST",
    "MP":"ChST"
}


def convert_state_to_time(state):
    if "|" not in state:
        zone = state_time_zone[state]
        return utc_zone[zone]
    else:
        return ""


offsets = []
csv_input = pd.read_csv("incident_metadata.csv")
for row in csv_input["states"]:
    offsets.append(convert_state_to_time(row))
csv_input["UTC"] = offsets
csv_input.to_csv("incident_metadata.csv")


'''utc_created_at => Wed Aug 27 13:08:45 +0000 2008
   utc_offset => UTC-4
   
'''
def convert_utc_to_loctime(utc_created_at, utc_offset):
    utc_time = datetime.datetime.strptime(utc_created_at, "%a %b %d %H:%M:%S +0000 %Y")
    offset_search = re.search(r'\d+', utc_offset)
    offset = 0
    local_time = ""
    if offset_search is not None:
        offset = int(offset_search.group())
    if "-" in utc_offset:
        local_time = utc_time - datetime.timedelta(hours=offset)
    elif "+" in utc_offset:
        local_time = utc_time + datetime.timedelta(hours=offset)
    else:
        raiseExceptions
    #print(utc_offset + " , " + str(offset))
    #print("UTC Time - " + str(utc_time))
    #print("Local Time - " + str(local_time))
    return local_time


for row in csv_input["UTC"]:
    convert_utc_to_loctime("Wed Aug 27 15:08:45 +0000 2008", row)

# TODO MAIN

# with open("incident_metadata.csv", 'r+') as csvfile:
#     reader = csv.reader(csvfile)
#     for row in reader:
#         if row[-1] is not "states":
#             state = row[-1]
#             if '|' not in state:
#                 print(state)
#                 row.append('a')
#                 
#         