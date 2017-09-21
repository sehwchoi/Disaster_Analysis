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

postal = {'WA': 'WASHINGTON', 'VA': 'VIRGINIA', 'DE': 'DELAWARE', 'DC': 'DISTRICT OF COLUMBIA',
          'WI': 'WISCONSIN', 'WV': 'WEST VIRGINIA', 'HI': 'HAWAII', 'FL': 'FLORIDA',
          'FM': 'FEDERATED STATES OF MICRONESIA', 'WY': 'WYOMING', 'NH': 'NEW HAMPSHIRE', 'NJ': 'NEW JERSEY',
          'NM': 'NEW MEXICO', 'TX': 'TEXAS', 'LA': 'LOUISIANA', 'NC': 'NORTH CAROLINA', 'ND': 'NORTH DAKOTA',
          'NE': 'NEBRASKA', 'TN': 'TENNESSEE', 'NY': 'NEW YORK', 'PA': 'PENNSYLVANIA', 'CA': 'CALIFORNIA',
          'NV': 'NEVADA', 'PW': 'PALAU', 'GU': 'GUAM GU', 'CO': 'COLORADO', 'VI': 'VIRGIN ISLANDS', 'AK': 'ALASKA',
          'AL': 'ALABAMA', 'AS': 'AMERICAN SAMOA', 'AR': 'ARKANSAS', 'VT': 'VERMONT', 'IL': 'ILLINOIS', 'GA': 'GEORGIA',
          'IN': 'INDIANA', 'IA': 'IOWA', 'OK': 'OKLAHOMA', 'AZ': 'ARIZONA', 'ID': 'IDAHO', 'CT': 'CONNECTICUT',
          'ME': 'MAINE', 'MD': 'MARYLAND', 'MA': 'MASSACHUSETTS', 'OH': 'OHIO', 'UT': 'UTAH', 'MO': 'MISSOURI',
          'MN': 'MINNESOTA', 'MI': 'MICHIGAN', 'MH': 'MARSHALL ISLANDS', 'RI': 'RHODE ISLAND', 'KS': 'KANSAS',
          'MT': 'MONTANA', 'MP': 'NORTHERN MARIANA ISLANDS', 'MS': 'MISSISSIPPI', 'PR': 'PUERTO RICO', 'SC': 'SOUTH CAROLINA',
          'KY': 'KENTUCKY', 'OR': 'OREGON', 'SD': 'SOUTH DAKOTA'}

def read_2010_census():
    census = {}
    with open("2010_census.txt") as file:
        for line in file:
            (state,popu) = line.split()
            census[state] = popu
    return census

state_census = read_2010_census()
print(state_census)

def get_top_state_census(states):
    census = 0
    top_state = states[0]
    for state in states:
        print("state census : " + str(state_census[state]) + " compare census : " + str(census))
        census_to_compare = int(state_census[state])
        if census_to_compare > census:
            census = census_to_compare
            top_state = state
    return top_state  

    
def convert_state_to_time(state):
    if "|" in state:
        states= state.split('|')
        print(states)
        top_state = get_top_state_census(states)
        print(top_state)
        zone = state_time_zone[top_state]
    else:
        zone = state_time_zone[state]
    return utc_zone[zone]


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