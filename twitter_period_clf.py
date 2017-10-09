'''
Created on Oct 5, 2017

@author: stellachoi
'''

import logging, sys
import json
import os
from collections import defaultdict
from converter_helper import TimezoneConverter

class TwitterPeriodClf(object):

    # read in files for each event and saves tweets in the list
    # tweets are list of all the tweets for the event
    # user_infos = { user1 : [before_tweet1, before_tweet2....], [during_tweet1, during_tweet2....],
    #                        [after_tweet1, after_tweet2....], user2: ....}
    # before_tweet1 will be a simple version of tweet info, containing tweet_id...
    def  __init__(self):
        self.tweets = []
        self.ev_begin = 0
        self.ev_end = 0
        self.user_infos = defaultdict(list)
        
        # initialize timezone converter
        self.time_converter = TimezoneConverter("incident_metadata.csv")

    def __is_duplication(self, cand_tweet_id):
        for tweet in self.tweets:
            if cand_tweet_id == tweet['id']:
                logging.debug("this tweet is duplication")
                return True
    
        return False
            
    def set_data(self, file_names, event_id):
        # load tweets from the database
        del self.tweets[:]
        self.ev_id = event_id
        for file_name in file_names:
            with open(file_name, 'r') as file:
                for line in file:
                    # do not add duplicated tweet by tweet unique id
                    cand_tweet = json.loads(line)
                    if not self.__is_duplication(cand_tweet['id']):
                        self.tweets.append(cand_tweet)
                    #logging.debug(self.tweets[0])
                    logging.debug(len(self.tweets))
        self.ev_begin = self.time_converter.getEventTimes(self.ev_id)[0]
        self.ev_end = self.time_converter.getEventTimes(self.ev_id)[1]

    def classify(self):
        for tweet in self.tweets:
            user_id = tweet['user']['id']
            tweet_date_utc = tweet['created_at']
            logging.debug("utc tweet date {}".format(tweet_date_utc))
            tweet_date_local = self.time_converter.convert_to_loctime_from_event(tweet_date_utc, self.ev_id)
            #logging.debug("Could not convert time ev_id: {} time: {}".format(self.evid, tweet_date_utc))
            

            # if user id not exist append three lists(Before, During, After) as periods
            if user_id not in self.user_infos:
                for i in range(0, 3):
                    self.user_infos[user_id].append([])
            
            info = {
                'tweet_id'   : tweet['id'],
                'user_id'    : user_id,
                'created_at' : tweet_date_utc }

            if tweet_date_local < self.ev_begin:
                logging.debug("added to before list")
                self.user_infos[user_id][0].append(info) # Before the event start
            elif tweet_date_local > self.ev_end:
                logging.debug("added to after list")
                self.user_infos[user_id][1].append(info) # After the event end
            else:
                logging.debug("added to during list")
                self.user_infos[user_id][2].append(info) # During the event   
              
    
    def getPeriodStats(self):
        for user_info in self.user_infos:
            len_before = len(self.user_infos[user_info][0])
            len_after = len(self.user_infos[user_info][1])
            len_during = len(self.user_infos[user_info][2])
            logging.debug("stats number of tweets of a user: {} before: {} after: {} during {}".
                      format(user_info, len_before, len_after, len_during))
        
        logging.debug("total users: {}".format(len(self.user_infos)))
        return (len_before, len_after, len_during)   
    
if __name__ == '__main__':
    logging.basicConfig(stream = sys.stderr, level = logging.DEBUG)
    path = "../Event - 319 - Moore Tornado/geotagged_from_archive/"
    file_name = "319_gardenhose.2013-05-13_us_geotagged_gelocated"
    full_path = os.path.join(path, file_name)
    logging.debug(full_path)
    classifier = TwitterPeriodClf()
    classifier.set_data([full_path], 319)
    classifier.classify()
    classifier.getPeriodStats()
    