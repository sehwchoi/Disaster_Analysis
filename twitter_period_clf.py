'''
Created on Oct 5, 2017

@author: stellachoi
'''

import pandas as pd
import logging, sys
import json
import csv
import os
import re
import smart_open
import resource
import psutil
from pympler import web
from pympler import tracker
import datetime
from pympler.classtracker import ClassTracker
from pympler.asizeof import asizeof

from collections import defaultdict
from meta_data_helper import EventMetaDataHelper
import time

# start_time = time.time()


def print_simple_memory_usage():
    # print memory using psutil library
    process = psutil.Process(os.getpid())

    # prints pysical memory information
    mem = process.memory_info()[0] / float(2 ** 20)
    # Compare process memory to total physical system memory and calculate process memory utilization as a percentage.
    # memtype argument is a string that dictates what type of process memory you want to compare against.
    mem_percent = process.memory_percent(memtype='rss')
    logging.debug("simple psutil pysical memory: {} MB  percent: {} % ".format(mem, mem_percent))

def print_memory_usage():
    # print memory using psutil library
    process = psutil.Process(os.getpid())

    # prints pysical memory information
    mem = process.memory_info()[0] / float(2 ** 20)
    # Compare process memory to total physical system memory and calculate process memory utilization as a percentage.
    # memtype argument is a string that dictates what type of process memory you want to compare against.
    mem_percent = process.memory_percent(memtype='rss')
    print("psutil pysical memory: {} MB  percent: {} % ".format(mem, mem_percent))

    # prints virtual memory information
    mem = process.memory_info()[1] / float(2 ** 20)
    mem_percent = process.memory_percent(memtype='vms')
    # print("psutil virtual memory: {} MB  percent: {} % ".format(mem, mem_percent))

    mem = psutil.virtual_memory()
    # print("psutil system mem: total_pysmem={} MB used={} MB available={} MB".format(mem.total / (float(2 ** 20)),
    #                                                        mem.used / (float(2 ** 20)),
    #                                                        mem.available / (float(2 ** 20))))

# TODO some unittest to test this code

class TwitterPeriodClf(object):
    """ Reads all files for all events and saves the number of tweets of each user's before, after, during event periods

        Attributes:
            self.total_duplication (int) :
            self.total_tweets (int) :
            self.tweets_counts_by_date (dict) : {timestamp(int) : {user_id1(int): count(int), user_id2: count...

            self.before (list) : Ex) self_before = [tweet_id1, tweet_id2...]
            self.after (list) :
            self.during (list):

            self.event_helper (EventMetaDataHelper class) :
            self.events (list) : Ex) [1, 2, 3....]
            self.output_path (string) : output path where user stats should be saved
            self.input_paths (list) : input paths of geotag files and timeline files
    """

    geotag_folder = "geotagged_from_archive"
    timeline_folder = "user_timelines"

    def __init__(self, input_paths, output_path, incident_metadata_path):

        self.class_tracker = ClassTracker()
        self.class_tracker.track_class(TwitterPeriodClf)
        self.class_tracker.create_snapshot()
        logging.debug("size: {} bytes".format(asizeof(self)))

        # TODO not using event info as class attribute
        self.total_timeline_duplication = 0
        self.total_tweets = 0
        self.total_users = 0
        # TODO save to pd dataframe, get from csv file
        self.tweets_counts_by_date = {}

        self.before_tweets = []
        self.after_tweets = []
        self.during_tweets = []

        # initialize timezone converter
        self.event_helper = EventMetaDataHelper(incident_metadata_path)
        self.events = self.event_helper.get_all_events()
        logging.debug(self.events)

        self.output_path = output_path
        self.input_paths = input_paths

        self.class_tracker.create_snapshot()
        logging.debug("size: {} bytes".format(asizeof(self)))

    def __reset_event_data(self):
        """ reset event specific parameter """
        self.total_timeline_duplication = 0
        self.total_tweets = 0
        self.tweets_counts_by_date = {}
        self.before_tweets.clear()
        self.after_tweets.clear()
        self.during_tweets.clear()

    def __is_duplication(self, cand_tweet_id, period):
        """ check if this tweet is already classified and counted """
        if period is 'before':
            tw_list = self.before_tweets
        elif period is 'after':
            tw_list = self.after_tweets
        elif period is 'during':
            tw_list = self.during_tweets

        if cand_tweet_id in tw_list:
            #logging.debug("this tweet {} is duplication".format(cand_tweet_id))
            return True

        return False

    def __get_files(self, event_id):
        """ parse all tweets files all events from the given folder"""
        # { event1: [files], event2: [files }
        geotag_files = []
        timeline_files = []
        for folder in self.input_paths:
            logging.debug("searching folder: {}".format(folder))
            for root, dirs, files in os.walk(folder):
                for filename in files:
                    # logging.debug("file: {}".format(filename))
                    # gets the number that file name starts with
                    file_match = re.search('(\d+)_', filename)
                    if file_match:
                        if int(file_match.group(1)) == event_id:
                            logging.debug("this file starts with {}".format(event_id))
                            if self.geotag_folder in folder:
                                # logging.debug("this is a geotag data file")
                                geotag_files.append(filename)
                            elif self.timeline_folder in folder:
                                # logging.debug("this is a timeline file")
                                timeline_files.append(filename)
        return geotag_files, timeline_files

    def calculate_user_periods(self):
        """ read tweets from files and classify them as it reads"""
        for event in self.events:
            (geotag_files, timeline_files) = self.__get_files(int(event))
            ev_begin = self.event_helper.get_event_times(event)[0]
            ev_end = self.event_helper.get_event_times(event)[1]

            # classify user tweets only if files exist in both geotagged and timeline
            output = os.path.join(self.output_path, r"{}_user_stats.csv".format(event))
            #if event in self.geotaged_exist and event in self.timeline_exist and not os.path.isfile(output):
            if (len(geotag_files) > 0) and (len(timeline_files) > 0):
                for file_name in geotag_files:
                    file_name_full = os.path.join(self.input_paths[0], file_name)
                    logging.debug("start reading file {}".format(file_name_full))
                    self.__read_tweets(file_name_full, "geotag", event, ev_begin, ev_end)
                for file_name in timeline_files:
                    file_name_full = os.path.join(self.input_paths[1], file_name)
                    logging.debug("start reading file {}".format(file_name_full))
                    self.__read_tweets(file_name_full, "timeline", event, ev_begin, ev_end)
                self.__save_period_stats(output)

            logging.debug("Event : {} processed".format(event))
            print_memory_usage()
            logging.debug("size: {} bytes, tweets_counts_by_date size: {} byte".format(asizeof(self),
                                                                                       asizeof(self.tweets_counts_by_date)))
            user_key_num = 0
            for date in self.tweets_counts_by_date:
                user_key_num += len(self.tweets_counts_by_date[date].keys())
            logging.debug("date_key_num: {} user_key_num: {}".format(len(self.tweets_counts_by_date.keys()), user_key_num))

            self.class_tracker.create_snapshot()

            self.__reset_event_data()
            logging.debug("after reset size: {} bytes".format(asizeof(self)))
            logging.debug("tweets_counts_by_date size: {} bytes".format(asizeof(self.tweets_counts_by_date)))

    def __read_tweets(self, file_name, file_type, event, ev_begin, ev_end):
        with smart_open.smart_open(file_name, 'r') as f:
            for line in f:
                try:
                    # do not add duplicated tweet by tweet unique id
                    tweet = json.loads(line)
                    self.__classify(tweet, event, ev_begin, ev_end, file_type)
                    # print_simple_memory_usage()
                    self.total_tweets += 1
                except:
                    pass

    def __classify(self, tweet, event, ev_begin, ev_end, file_type):
        """ classify tweet according their local time"""
        try:
            user_id = int(tweet['user']['id'])
            tweet_date_utc = tweet['created_at']
            # logging.debug("utc tweet date {}".format(tweet_date_utc))
            tweet_date_local = self.event_helper.convert_to_loctime_from_event(tweet_date_utc, event)
            timestamp = int(time.mktime(tweet_date_local.date().timetuple()))
            # logging.debug("timestamp: {}".format(timestamp))
            # tweet_date_local_str = str(tweet_date_local.date())
            # logging.debug("tweet date: {}".format(tweet_date_local_str))

            duplicate = False
            if tweet_date_local < ev_begin:
                # logging.debug("added to before list")
                if not self.__is_duplication(int(tweet['id']), 'before'):
                    if file_type is "geotag":
                        self.before_tweets.append(int(tweet['id']))
                        # logging.debug("added tweet before list id : {}".format(tweet['id']))
                else:
                    duplicate = True
                    if file_type is "timeline":
                        self.total_timeline_duplication += 1
            elif tweet_date_local > ev_end:
                # logging.debug("added to after list")
                if not self.__is_duplication(int(tweet['id']), 'after'):
                    if file_type is "geotag":
                        self.after_tweets.append(int(tweet['id']))
                else:
                    duplicate = True
                    if file_type is "timeline":
                        self.total_timeline_duplication += 1
            else:
                # logging.debug("added to during list")
                if not self.__is_duplication(int(tweet['id']), 'during'):
                    if file_type is "geotag":
                        self.during_tweets.append(int(tweet['id']))
                else:
                    duplicate = True
                    if file_type is "timeline":
                        self.total_timeline_duplication += 1

            if not duplicate:
                if timestamp in self.tweets_counts_by_date:
                    info = self.tweets_counts_by_date[timestamp]
                    if user_id in info:
                        self.tweets_counts_by_date[timestamp][user_id] += 1
                    else:
                        self.total_users += 1
                        self.tweets_counts_by_date[timestamp][user_id] = 1
                else:
                    self.total_users +=1
                    user_info = {user_id: 1}
                    self.tweets_counts_by_date[timestamp] = user_info

        except Exception as e:
            logging.debug("could not classify tweet error: {}".format(e))


    # create information of number of tweets before, after, and during the disaster period for each user
    # save all information to csv file ex. "319_user_stats.csv"
    def __save_period_stats(self, output):
        """ create information of each date and number of tweets
            save all information to json file ex. "319_user_stats.csv"""
        logging.debug("save file: {} total tweets: {} total_users: {}"
                      " timeline tweets duplication: {}".format(output,
                                                                self.total_tweets,
                                                                self.total_users,
                                                                self.total_timeline_duplication))
        # remove file if it exist
        try:
            os.remove(output)
        except OSError:
            pass

        for timestamp in self.tweets_counts_by_date:
            date = time.strftime("%Y-%m-%d", time.localtime(timestamp))
            # logging.debug("date= {}".format(date))
            result_list = []
            for user in self.tweets_counts_by_date[timestamp]:
                count = self.tweets_counts_by_date[timestamp][user]
                result_list.append((date, user, count))
            with open(output, 'a') as file:
                csv_out = csv.writer(file)
                # logging.debug("writing result list for date= {}".format(result_list))
                for row in result_list:
                    csv_out.writerow(row)


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)
    # path0 must be geotagged file path and path1 must be timeline file path
    input_paths = ["../events_tweets/Event - 319 - Moore Tornado/geotagged_from_archive/",
                   "../events_tweets/Event - 319 - Moore Tornado/user_timelines/"]
    output_path = "../user_stats"
    incident_metadata_path = 'incident_metadata.csv'

    #memory_tracker = tracker.SummaryTracker()

    # print memory usage using psutil
    print_memory_usage()

    classifier = TwitterPeriodClf(input_paths, output_path, incident_metadata_path)
    # read tweets from file and classify their time periods
    classifier.calculate_user_periods()

    # print memory usage using psutil
    print_memory_usage()

    # print using pympler
    # memory_tracker.print_diff()
    # web.start_profiler(debug=True, stats=class_tracker.stats)

