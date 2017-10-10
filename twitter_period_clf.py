'''
Created on Oct 5, 2017

@author: stellachoi
'''
import pandas as pd
import logging, sys
import json
import os

from collections import defaultdict
from converter_helper import TimezoneConverter

class TwitterPeriodClf(object):

    # user_infos = { user1 : [before_tweet1, before_tweet2....], [during_tweet1, during_tweet2....],
    #                        [after_tweet1, after_tweet2....], user2: ....}
    # before_tweet1 will be a simple version of tweet info, containing tweet_id...
    def  __init__(self):
        self.ev_begin = 0
        self.ev_end = 0
        # TODO save to pd dataframe, get from csv file
        self.user_infos = defaultdict(list)
        
        # initialize timezone converter
        self.time_converter = TimezoneConverter("incident_metadata.csv")

    def __is_duplication(self, cand_tweet_id, infos):
        for info in infos:
            if cand_tweet_id == info['t_id']:
                logging.debug("this tweet is duplication")
                return True
    
        return False
    
    def get_files(self, paths):
        files = []
        for path in paths:
            for f in os.listdir(path):
                full_path = os.path.join(path, f)
                if os.path.isfile(full_path) and not f.startswith("."):
                    files.append(full_path)
        return files
    
    # read tweets from files and classify them as it reads
    def process_data(self, file_names, event_id):
        self.ev_id = event_id
        self.ev_begin = self.time_converter.getEventTimes(self.ev_id)[0]
        self.ev_end = self.time_converter.getEventTimes(self.ev_id)[1]
        for file_name in file_names:
            logging.debug("start reading file {}".format(file_name))
            with open(file_name, 'r') as file:
                for line in file:
                    # do not add duplicated tweet by tweet unique id
                    tweet = json.loads(line)
                    self.__classify(tweet)

        # dump user_infos dictionary to json. overwites if it exist

    def __classify(self, tweet):
        try:
            user_id = tweet['user']['id']
            tweet_date_utc = tweet['created_at']
            #logging.debug("utc tweet date {}".format(tweet_date_utc))
            tweet_date_local = self.time_converter.convert_to_loctime_from_event(tweet_date_utc, self.ev_id)
            #logging.debug("Could not convert time ev_id: {} time: {}".format(self.evid, tweet_date_utc))
            
            # if user id not exist append three lists(Before, During, After) as periods
            if user_id not in self.user_infos:
                for i in range(0, 3):
                    self.user_infos[user_id].append([])
            
            info = {
                't_id' : tweet['id'],
                'u_id' : user_id,
                'time' : tweet_date_utc }

            if tweet_date_local < self.ev_begin:
                #logging.debug("added to before list")
                if not self.__is_duplication(info['t_id'],  self.user_infos[user_id][0]):
                    self.user_infos[user_id][0].append(info) # Before the event start
            elif tweet_date_local > self.ev_end:
                #logging.debug("added to after list")
                if not self.__is_duplication(info['t_id'],  self.user_infos[user_id][1]):
                    self.user_infos[user_id][1].append(info) # After the event start
            else:
                #logging.debug("added to during list")
                if not self.__is_duplication(info['t_id'],  self.user_infos[user_id][2]):
                    self.user_infos[user_id][2].append(info) # During the event start
        except Exception as e:
            logging.debug("could not classify tweet error: {}".format(e))
    
    def getPeriodStats(self):
        cache = pd.DataFrame(columns=('user', 'num_before', "num_after", 'num_during'))
        for user_info in self.user_infos:
            len_before = len(self.user_infos[user_info][0])
            len_after = len(self.user_infos[user_info][1])
            len_during = len(self.user_infos[user_info][2])
            #save information to CSV file
            cache.loc[len(cache)] = [user_info, len_before, len_after, len_during]
            #logging.debug(cache)
            logging.debug("stats number of tweets of a user: {} before: {} after: {} during {}".
                      format(user_info, len_before, len_after, len_during))

        cache.to_csv("{}_user_stats.csv".format(self.ev_id), mode = 'w', index=False)
        logging.debug("total users: {}".format(len(self.user_infos)))
        return (len_before, len_after, len_during)   

if __name__ == '__main__':
    logging.basicConfig(stream = sys.stderr, level = logging.DEBUG)
    paths = []
    path1 = "../Event - 319 - Moore Tornado/geotagged_from_archive/"
    path2 = "../Event - 319 - Moore Tornado/user_timelines/"
    paths = [path1, path2]
    classifier = TwitterPeriodClf()
    files = classifier.get_files(paths)
    logging.debug(files)
    classifier.process_data(files, 319)
    classifier.getPeriodStats()
    