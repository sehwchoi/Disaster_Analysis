'''
Created on Oct 5, 2017

@author: stellachoi
'''
import pandas as pd
import logging, sys
import json
import os
import re

from collections import defaultdict
from meta_data_helper import EventMetaDataHelper

# TODO some unittest to test this code

class TwitterPeriodClf(object):
    """ Reads all files for all events and saves the number of tweets of each user's before, after, during event periods

        Attributes:
            self.total_duplication (int) :
            self.total_tweets (int) :
            self.user_infos (dict) : Ex) user_infos = { user1 : [before_tweet1, before_tweet2....],
                                                                [after_tweet1, after_tweet2....],
                                                                [during_tweet1, during_tweet2....]
                                                        user2 : ... }
            self.event_helper (EventMetaDataHelper class) :
            self.events (list) : Ex) [1, 2, 3....]
            self.files_dict (dict) : Ex) { event1 : [file1, file2, file3], event2: [file1, file2, file3]}
            self.geotagged_exist (list) : has events ids that have at least one file exist in the geotagged foler
            self.timeline_exist (list) : has events ids that have at least one file exist in the timeline foler
            self.output_path (string) : output path where user stats should be saved
    """

    geotag_folder = "geotagged_from_archive"
    timeline_folder = "user_timelines"

    def __init__(self, input_path, output_path):

        # TODO not using event info as class attribute
        self.total_duplication = 0
        self.total_tweets = 0
        # TODO save to pd dataframe, get from csv file
        self.user_infos = defaultdict(list)

        # initialize timezone converter
        self.event_helper = EventMetaDataHelper("incident_metadata.csv")
        self.events = self.event_helper.get_all_events()
        logging.debug(self.events)

        self.files_dict = defaultdict(list)
        self.geotaged_exist = []
        self.timeline_exist = []
        self.output_path = output_path
        self.__get_files(input_path)

    def __reset_event_data(self):
        """ reset event specific parameter """
        self.total_duplication = 0
        self.total_tweets = 0
        self.user_infos = defaultdict(list)

    def __is_duplication(self, cand_tweet_id, infos):
        """ check if this tweet is already classified and counted """
        for info in infos:
            if cand_tweet_id == info['t_id']:
                logging.debug("this tweet {} is duplication with info {}".format(cand_tweet_id, info))
                self.total_duplication += 1
                return True
    
        return False

    def __get_files(self, folder):
        """ parse all tweets files all events from the given folder"""
        # { event1: [files], event2: [files }

        for root, dirs, files in os.walk(folder):
            for file in files:
                # gets the number that file name starts with
                file_match= re.search('(\d+)_', file)
                if file_match:
                    file_start = int(file_match.group(1))
                    logging.debug("this file starts with {}".format(file_start))
                    # check if this number is one of the event ids
                    if file_start in self.events:
                        if TwitterPeriodClf.geotag_folder in root:
                            if file_start not in self.geotaged_exist:
                                self.geotaged_exist.append(file_start)
                        elif TwitterPeriodClf.timeline_folder in root:
                            if file_start not in self.timeline_exist:
                                self.timeline_exist.append(file_start)
                        self.files_dict[file_start].append(os.path.join(root, file))

    def calculate_user_periods(self):
        """ read tweets from files and classify them as it reads"""
        for event in self.events:
            ev_begin = self.event_helper.get_event_times(event)[0]
            ev_end = self.event_helper.get_event_times(event)[1]

            # classify user tweets only if files exist in both geotagged and timeline
            if event in self.geotaged_exist and event in self.timeline_exist:
                file_names = self.files_dict[event]
                for file_name in file_names:
                    logging.debug("start reading file {}".format(file_name))
                    with open(file_name, 'r') as file:
                        for line in file:
                            # do not add duplicated tweet by tweet unique id
                            tweet = json.loads(line)
                            self.__classify(tweet, event, ev_begin, ev_end)
                            self.total_tweets += 1
                self.__save_period_stats(event)

            self.__reset_event_data()
            logging.debug(self.user_infos)

    def __classify(self, tweet, event, ev_begin, ev_end):
        """ classify tweet according their local time"""
        try:
            user_id = tweet['user']['id']
            tweet_date_utc = tweet['created_at']
            #logging.debug("utc tweet date {}".format(tweet_date_utc))
            tweet_date_local = self.event_helper.convert_to_loctime_from_event(tweet_date_utc, event)
            #logging.debug("Could not convert time ev_id: {} time: {}".format(self.evid, tweet_date_utc))
            
            # if user id not exist append three lists(Before, During, After) as periods
            if user_id not in self.user_infos:
                for i in range(0, 3):
                    self.user_infos[user_id].append([])

            info = {
                't_id' : tweet['id'],
                'time' : tweet_date_utc }
            
            # logging.debug("tweet info {}" + str(info))
            if tweet_date_local < ev_begin:
                # logging.debug("added to before list")
                if not self.__is_duplication(info['t_id'],  self.user_infos[user_id][0]):
                    self.user_infos[user_id][0].append(info) # Before the event start
            elif tweet_date_local > ev_end:
                # logging.debug("added to after list")
                if not self.__is_duplication(info['t_id'],  self.user_infos[user_id][1]):
                    self.user_infos[user_id][1].append(info) # After the event start
            else:
                # logging.debug("added to during list")
                if not self.__is_duplication(info['t_id'],  self.user_infos[user_id][2]):
                    self.user_infos[user_id][2].append(info) # During the event start
        except Exception as e:
            logging.debug("could not classify tweet error: {}".format(e))
    
    # create information of number of tweets before, after, and during the disaster period for each user
    # save all information to csv file ex. "319_user_stats.csv"
    def __save_period_stats(self, event):
        """ create information of number of tweets before, after, and during the disaster period for each user
            save all information to csv file ex. "319_user_stats.csv"""

        # create an empty panda data frame
        cache = pd.DataFrame(columns=('user', 'num_before', "num_after", 'num_during'))
        for user_info in self.user_infos:
            len_before = len(self.user_infos[user_info][0])
            len_after = len(self.user_infos[user_info][1])
            len_during = len(self.user_infos[user_info][2])
            # append cache with the user period stats
            cache.loc[len(cache)] = [user_info, len_before, len_after, len_during]
            #logging.debug("stats number of tweets of a user: {} before: {} after: {} during {}".
            #          format(user_info, len_before, len_after, len_during))

        #save information to CSV file
        output = os.path.join(self.output_path, r"{}_user_stats.csv".format(event))
        cache.to_csv(output, mode = 'w', index=False)
        # duplication_rate = str(int((self.total_duplication/self.total_tweets) * 100)) + '%'
        logging.debug("event: {} total users: {} total tweets: {} \
            total duplication : {} file: {}".format(event, len(self.user_infos), self.total_tweets,
                                                    self.total_duplication, output))

if __name__ == '__main__':
    logging.basicConfig(stream = sys.stderr, level = logging.DEBUG)
    input_path = "../Event - 319 - Moore Tornado"
    output_path = "../user_stats"

    classifier = TwitterPeriodClf(input_path, output_path)
    # read tweets from file and classify their time periods
    classifier.calculate_user_periods()
