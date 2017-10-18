import datetime
import logging
import re
import sys

import pandas as pd

utc_zone = {
    "HST": "UTC-10",
    "AKST": "UTC-9",
    "PST": "UTC-8",
    "MST": "UTC-7",
    "CST": "UTC-6",
    "EST": "UTC-5",
    "AST": "UTC-4",
    "ChST": "UTC+10"  # chamorro standard time
}

state_time_zone = {
    "AL": "CST",
    "AK": "AKST",
    "AZ": "MST",
    "AR": "CST",
    "CA": "PST",
    "CO": "MST",
    "CT": "EST",
    "DE": "EST",
    "FL": "EST",
    "GA": "EST",
    "HI": "HST",
    "ID": "MST",
    "IL": "CST",
    "IN": "EST",
    "IA": "CST",
    "KS": "CST",
    "KY": "CST",
    "LA": "CST",
    "ME": "EST",
    "MD": "EST",
    "MA": "EST",
    "MI": "EST",
    "MN": "CST",
    "MS": "CST",
    "MO": "CST",
    "MT": "MST",
    "NE": "CST",
    "NV": "PST",
    "NH": "EST",
    "NJ": "EST",
    "NM": "MST",
    "NY": "EST",
    "NC": "EST",
    "ND": "CST",
    "OH": "EST",
    "OK": "CST",
    "OR": "PST",
    "PA": "EST",
    "RI": "EST",
    "SC": "EST",
    "SD": "CST",
    "TN": "CST",
    "TX": "CST",
    "UT": "MST",
    "VT": "EST",
    "VA": "EST",
    "WA": "PST",
    "DC": "EST",
    "WV": "EST",
    "WI": "CST",
    "WY": "MST",
    "PR": "AST",
    "MP": "ChST"
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
          'MT': 'MONTANA', 'MP': 'NORTHERN MARIANA ISLANDS', 'MS': 'MISSISSIPPI', 'PR': 'PUERTO RICO',
          'SC': 'SOUTH CAROLINA',
          'KY': 'KENTUCKY', 'OR': 'OREGON', 'SD': 'SOUTH DAKOTA'}


class UTCUpdater(object):
    def __init__(self, file_name):
        self.utc_offsets = []
        self.count_multi_states = 0
        csv_input = pd.read_csv(file_name)
        # iterate states and find corresponding zones
        for index, row in csv_input.iterrows():
            event_id = row['incident_id']
            logging.debug('event_id {}'.format(event_id))
            state = row['states']
            zone = self.__find_zone(state)
            self.utc_offsets.append(utc_zone[zone])

        self.count_total = len(csv_input["states"])
        logging.debug("total items " + str(self.count_total))
        logging.debug("multi states " + str(self.count_multi_states))
        # add UTC offsets to csv file
        csv_input["UTC"] = self.utc_offsets
        csv_input.to_csv("incident_metadata.csv", mode='w', index=False)

    def __get_most_timezone(self, states):
        popu_timezones = {}
        for state in states:
            timezone = state_time_zone[state]
            popu = int(state_census[state])
            logging.debug("state : {} timezone : {} census : {}".format(state, timezone, popu))

            if timezone in popu_timezones:
                total_popu = popu_timezones[timezone]
                new_total_popu = total_popu + popu
                logging.debug("prev_total_popu : {} new_total_popu : {}".format(total_popu, new_total_popu))
            else:
                new_total_popu = popu

            popu_timezones[timezone] = new_total_popu

        top_zone = ""
        total_census = 0
        for zone, census in popu_timezones.items():
            logging.debug("zone : {} census : {}".format(zone, census))
            census_to_compare = census
            if census_to_compare > total_census:
                total_census = census_to_compare
                top_zone = zone
        logging.debug("top zone : {}".format(top_zone))
        return top_zone

    #  find zone for given state. If there are multiples states, then select zone
    #  that covers the most number of population
    def __find_zone(self, state):
        if "|" in state:
            self.count_multi_states += 1
            states = state.split('|')
            logging.debug(states)
            zone = self.__get_most_timezone(states)
        else:
            zone = state_time_zone[state]
        return zone


class EventMetaDataHelper(object):
    def __init__(self, file_name):
        self.event_utc_dic = {}
        self.event_times = {}  # {event id : (begin, end)}
        csv_input = pd.read_csv(file_name)
        # iterate states and find corresponding zones
        for index, row in csv_input.iterrows():
            event_id = row['incident_id']
            event_begin = row['incidentBeginDate']
            event_end = row['incidentEndDate']
            # string date format: 2011-07-14
            time_begin = datetime.datetime.strptime(event_begin, '%Y-%m-%d')
            time_end = datetime.datetime.strptime(event_end, '%Y-%m-%d')
            #logging.debug("Time for id:{} begin:{} end:{}".format(event_id, str(time_begin), str(time_end)))

            self.event_times[event_id] = (time_begin, time_end)

            # logging.debug('event_id {}'.format(event_id))
            utc_offset = row['UTC']
            self.event_utc_dic[event_id] = utc_offset

    # find zone given the event id
    #  utc_created_at => Wed Aug 27 13:08:45 +0000 2008
    #  devent_id => 270 (int)
    #  TODO use pytz to convert time using zone
    def convert_to_loctime_from_event(self, utc_created_at, event_id):
        utc_time = datetime.datetime.strptime(utc_created_at, "%a %b %d %H:%M:%S +0000 %Y")
        utc_offset = self.event_utc_dic[event_id]
        # logging.debug(event_id)
        # logging.debug(utc_offset)
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
            logging.raiseExceptions
        # logging.debug(utc_offset + " , " + str(offset))
        # logging.debug("UTC Time - " + str(utc_time))
        # logging.debug("Local Time - " + str(local_time))
        return local_time

    # convert UTC time to local time
    #  utc_created_at => Wed Aug 27 13:08:45 +0000 2008
    #  utc_offset => UTC-4
    def convert_utc_to_loctime(self, utc_created_at, utc_offset):
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
            logging.raiseExceptions
        # logging.debug(utc_offset + " , " + str(offset))
        # logging.debug("UTC Time - " + str(utc_time))
        logging.debug("Local Time - " + str(local_time))
        return local_time

    def get_all_events(self):
        return self.event_times.keys()

    # return a tuple of (event begin time, event end time)
    def get_event_times(self, event_id):
        #logging.debug("Time for id:{} begin:{} end:{}".format(event_id,
        #                                                      str(self.event_times[event_id][0]),
        #                                                      str(self.event_times[event_id][1])))
        return self.event_times[event_id]


# read 2010 census data for each state and saves them into dictionary ex. {CA: 52}
def read_2010_census():
    census = {}
    with open("2010_census.txt") as file:
        for line in file:
            (state, popu) = line.split()
            census[state] = popu
    return census


# if __name__ == "__main__":
#     logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)
#     state_census = read_2010_census()
#     logging.debug(state_census)
#
#     updater = UTCUpdater("data/indcident_tweets/incident_metadata.csv")
#       TimeConverter = EventMetaData("incident_metadata.csv")
#       print(TimeConverter.convert_utc_to_loctime("Wed Aug 27 15:08:45 +0000 2008", "UTC-8"))
#       print(TimeConverter.convert_to_loctime_from_event("Wed Aug 27 15:08:45 +0000 2008", 119)) # event 119, time zone CST, UTC-6
