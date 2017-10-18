'''
Created on Oct 5, 2017

@author: stellachoi
'''

import pandas as pd
import logging, sys
import json
import os
import re
import smart_open
from sortedcontainers import SortedList
import resource
import psutil
from pympler import web
from pympler import tracker
from pympler.classtracker import ClassTracker

from collections import defaultdict
from meta_data_helper import EventMetaDataHelper


# TODO some unittest to test this code

class TwitterPeriodClf(object):
    """ Reads all files for all events and saves the number of tweets of each user's before, after, during event periods

        Attributes:
            self.total_duplication (int) :
            self.total_tweets (int) :
            self.user_infos (dict) : Ex) user_infos = { user1 : [before_count, after_count, during_count],
                                                        user2 : ... }
            self.before (sortedlist) : Ex) self_before = [tweet_id1, tweet_id2...]
            self.after (sortedlistlist) :
            self.during (sortedlistlist):

            self.event_helper (EventMetaDataHelper class) :
            self.events (list) : Ex) [1, 2, 3....]
            self.files_dict (dict) : Ex) { event1 : [file1, file2, file3], event2: [file1, file2, file3]}
            self.geotagged_exist (list) : has events ids that have at least one file exist in the geotagged foler
            self.timeline_exist (list) : has events ids that have at least one file exist in the timeline foler
            self.output_path (string) : output path where user stats should be saved
    """

    geotag_folder = "geotagged_from_archive"
    timeline_folder = "user_timelines"

    def __init__(self, input_paths, output_path, incident_metadata_path):

        # TODO not using event info as class attribute
        self.total_duplication = 0
        self.total_tweets = 0
        # TODO save to pd dataframe, get from csv file
        self.user_infos = defaultdict(list)

        self.before_tweets = SortedList()
        self.after_tweets = SortedList()
        self.during_tweets = SortedList()

        # initialize timezone converter
        self.event_helper = EventMetaDataHelper(incident_metadata_path)
        self.events = self.event_helper.get_all_events()
        logging.debug(self.events)

        self.files_dict = defaultdict(list)
        self.geotaged_exist = []
        self.timeline_exist = []
        self.output_path = output_path
        self.__get_files(input_paths)

    def __reset_event_data(self):
        """ reset event specific parameter """
        self.total_duplication = 0
        self.total_tweets = 0
        self.user_infos = defaultdict(list)

    def __search(self, tw_list, target):
        first = 0
        last = len(tw_list) - 1
        found = False

        while first <= last and not found:
            mid = int((first+last) / 2)
            #logging.debug("search first {} last {} mid {} target {}".format(first, last, mid, target))
            if tw_list[mid] == target:
                logging.debug("this tweet {} is duplication".format(target))
                found = True
            else:
                if target < tw_list[mid]:
                    last = mid - 1
                else:
                    first = mid + 1

        return found


    def __is_duplication(self, cand_tweet_id, period):
        """ check if this tweet is already classified and counted """
        if period is 'before':
            tw_list = self.before_tweets
        elif period is 'after':
            tw_list = self.after_tweets
        elif period is 'during':
            tw_list = self.during_tweets

        if self.__search(tw_list, cand_tweet_id):
            logging.debug("this tweet {} is duplication".format(cand_tweet_id))
            self.total_duplication += 1
            return True

        return False

    def __get_files(self, folders):
        """ parse all tweets files all events from the given folder"""
        # { event1: [files], event2: [files }
        for folder in folders:
            logging.debug("searching folder: {}".format(folder))
            for root, dirs, files in os.walk(folder):
                for filename in files:
                    # logging.debug("file: {}".format(filename))
                    # gets the number that file name starts with
                    file_match = re.search('(\d+)_', filename)
                    if file_match:
                        file_start = int(file_match.group(1))
                        # logging.debug("this file starts with {}".format(file_start))
                        # check if this number is one of the event ids
                        if file_start in self.events:
                            # logging.debug("file in events")
                            if self.geotag_folder in folder:
                                # logging.debug("this is a geotag data file")
                                if file_start not in self.geotaged_exist:
                                    # logging.debug("adding event to geotagged: {}".format(file_start))
                                    self.geotaged_exist.append(file_start)
                            elif self.timeline_folder in folder:
                                # logging.debug("this is a timeline file")
                                if file_start not in self.timeline_exist:
                                    # logging.debug("adding event to timeline: {}".format(file_start))
                                    self.timeline_exist.append(file_start)
                            # logging.debug("adding file to event {}".format(filename))
                            self.files_dict[file_start].append(os.path.join(root, filename))

    def calculate_user_periods(self):
        """ read tweets from files and classify them as it reads"""
        for event in self.events:
            ev_begin = self.event_helper.get_event_times(event)[0]
            ev_end = self.event_helper.get_event_times(event)[1]

            # classify user tweets only if files exist in both geotagged and timeline
            output = os.path.join(self.output_path, r"{}_user_stats.csv".format(event))
            # logging.debug("output file:{}".format(output))
            #if event in self.geotaged_exist and event in self.timeline_exist and not os.path.isfile(output):
            if event in self.geotaged_exist and event in self.timeline_exist:
                file_names = self.files_dict[event]
                for file_name in file_names:
                    logging.debug("start reading file {}".format(file_name))
                    with smart_open.smart_open(file_name, 'r') as f:
                        for line in f:
                            try:
                                # do not add duplicated tweet by tweet unique id
                                tweet = json.loads(line)
                                self.__classify(tweet, event, ev_begin, ev_end)
                                self.total_tweets += 1
                            except:
                                pass
                self.__save_period_stats(event)

            self.__reset_event_data()

    def __classify(self, tweet, event, ev_begin, ev_end):
        """ classify tweet according their local time"""
        try:
            user_id = tweet['user']['id']
            tweet_date_utc = tweet['created_at']
            # logging.debug("utc tweet date {}".format(tweet_date_utc))
            tweet_date_local = self.event_helper.convert_to_loctime_from_event(tweet_date_utc, event)
            # logging.debug("Could not convert time ev_id: {} time: {}".format(self.evid, tweet_date_utc))

            # if user id not exist append 0s for before, after, during tweet counts
            if user_id not in self.user_infos:
                for i in range(0, 3):
                    self.user_infos[user_id].append(0)

            if tweet_date_local < ev_begin:
                # logging.debug("added to before list")
                if not self.__is_duplication(int(tweet['id']), 'before'):
                    self.before_tweets.add(int(tweet['id']))
                    #logging.debug(self.before_tweets[:10])
                    self.user_infos[user_id][0] += 1  # Before the event start
            elif tweet_date_local > ev_end:
                # logging.debug("added to after list")
                if not self.__is_duplication(int(tweet['id']), 'after'):
                    self.after_tweets.add(int(tweet['id']))
                    self.user_infos[user_id][1] += 1  # After the event start
            else:
                # logging.debug("added to during list")
                if not self.__is_duplication(int(tweet['id']), 'during'):
                    self.during_tweets.add(int(tweet['id']))
                    self.user_infos[user_id][2] += 1  # During the event start
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
            len_before = self.user_infos[user_info][0]
            len_after = self.user_infos[user_info][1]
            len_during = self.user_infos[user_info][2]
            # append cache with the user period stats
            cache.loc[len(cache)] = [user_info, len_before, len_after, len_during]
            # logging.debug("stats number of tweets of a user: {} before: {} after: {} during {}".
            #              format(user_info, len_before, len_after, len_during))

        # save information to CSV file
        output = os.path.join(self.output_path, r"{}_user_stats.csv".format(event))
        cache.to_csv(output, mode='w', index=False)
        # duplication_rate = str(int((self.total_duplication/self.total_tweets) * 100)) + '%'
        logging.debug("event: {} total users: {} total tweets: {} \
            total duplication : {} file: {}".format(event, len(self.user_infos), self.total_tweets,
                                                    self.total_duplication, output))

def print_memory_usage():
    # print memory using psutil library
    process = psutil.Process(os.getpid())

    # prints pysical memory information
    mem = process.memory_info()[0] / float(2 ** 20)
    # Compare process memory to total physical system memory and calculate process memory utilization as a percentage.
    # memtype argument is a string that dictates what type of process memory you want to compare against.
    mem_percent = process.memory_percent(memtype='rss')
    print("psutil pysical memory {} MB".format(mem))
    print("psutil pysical memory percent {} %".format(mem_percent))

    # prints virtual memory information
    mem = process.memory_info()[1] / float(2 ** 20)
    mem_percent = process.memory_percent(memtype='vms')
    print("psutil virtual memory {} MB".format(mem))
    print("psutil virtual memory percent {} %".format(mem_percent))

    mem = psutil.virtual_memory()
    print("psutil system mem: total_pysmem={} MB used={} MB available={} MB".format(mem.total / (float(2 ** 20)),
                                                            mem.used / (float(2 ** 20)),
                                                            mem.available / (float(2 ** 20))))

if __name__ == '__main__':
    logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)
    input_paths = ["../Event - 319 - Moore Tornado copy/geotagged_from_archive/",
                   "../Event - 319 - Moore Tornado copy/user_timelines/"]
    output_path = "../user_stats"
    incident_metadata_path = 'incident_metadata.csv'

    class_tracker = ClassTracker()
    memory_tracker = tracker.SummaryTracker()
    class_tracker.create_snapshot()

    # print memory usage using psutil
    print_memory_usage()

    classifier = TwitterPeriodClf(input_paths, output_path,incident_metadata_path)
    class_tracker.track_object(classifier)

    # read tweets from file and classify their time periods
    classifier.calculate_user_periods()

    # print memory usage using psutil
    print_memory_usage()

    # print using pympler
    class_tracker.create_snapshot()
    memory_tracker.print_diff()
    web.start_profiler(debug=True, stats=class_tracker.stats)

