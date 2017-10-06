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
    def  __init__(self, file_names, event_id):
        self.tweets = []
        self.evid = event_id
        self.user_infos = defaultdict(list)
        
        # initialize timezone converter
        self.time_converter = TimezoneConverter("incident_metadata.csv")

        # load tweets from the database
        for file_name in file_names:
            with open(file_name, 'r') as file:
                for line in file:
                    self.tweets.append(json.loads(line))
                    #logging.debug(self.tweets[0])
                    logging.debug(len(self.tweets))

    def classify(self):
        for tweet in self.tweets:
            user_id = tweet['user']['id']
            tweet_date_utc = tweet['created_at']
            logging.debug(tweet_date_utc)
            tweet_date_local = self.time_converter.convert_to_loctime_from_event(tweet_date_utc, self.evid)
            #logging.debug("Could not convert time evid: {} time: {}".format(self.evid, tweet_date_utc))
            

            # if user id not exist append three lists(Before, During, After) as periods
            if user_id not in self.user_infos:
                for i in range(0, 3):
                    self.user_infos[user_id].append([])
            
            info = {
                'tweet_id'   : tweet['id'],
                'user_id'    : user_id,
                'created_at' : tweet_date_utc }

            #if tweet_date_local < time_converter.getStartDate(self.evid):
            #    self.user_infos[user_id].get(0).append(info) # Before the event start
            #elif tweet_date_local > time_converter.getStartDate(self.evid):
            #    self.user_infos[user_id].get(1).append(info) # After the event end
            #else:
            #    self.user_infos[user_id].get(2).append(info) # During the event
                 
        
        
if __name__ == '__main__':
    logging.basicConfig(stream = sys.stderr, level = logging.DEBUG)
    path = "../Event - 319 - Moore Tornado/geotagged_from_archive/"
    file_name = "319_gardenhose.2013-05-13_us_geotagged_gelocated"
    full_path = os.path.join(path, file_name)
    logging.debug(full_path)
    classifier = TwitterPeriodClf([full_path], 319)
    classifier.classify()
    