from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.tokenize.casual import TweetTokenizer
from emoji import UNICODE_EMOJI
import string
import nltk
import logging, sys
import os
import re
import smart_open
import json
import csv
import time
from word_count_model import WordsDBManager
sys.path.append('../')
from meta_data_helper import EventMetaDataHelper

class TextProcessor(object):
    geotag_folder = "geotagged_from_archive"
    timeline_folder = "user_timelines"

    def __init__(self, incident_metadata_path, input_paths, output_path):

        self.input_paths = input_paths
        self.output_path = output_path

        # events time conversion helper
        self.event_helper = EventMetaDataHelper(incident_metadata_path)
        self.events = self.event_helper.get_all_events()
        logging.debug(self.events)

        nltk.download('punkt')
        nltk.download('stopwords')
        # stop words
        self.stop_words = set(stopwords.words('english'))
        # logging.debug("Stop words : {}".format(self.stop_words))
        # get words
        # self.tokenizer = RegexpTokenizer(r'\w+')

        self.total_tweets = 0
        self.unique_id_set = []
        self.words_counts_by_date = {}

    def __reset_event_data(self):
        """ reset event specific parameter """
        self.total_tweets = 0
        self.unique_id_set.clear()
        self.tweets_counts_by_date = {}

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

    def __tokenize_tweet(self, tweet):
        # remove users, hashtags, any links
        # punctuation = ",./<>?;':\"[]\\{}|`~!@#$%^&*()_+-="
        # tweet = re.sub(r"(@[a-zA-Z0-9_]+)|(\w+:\/\/\S+)|([“”,.…/<>;':\"\[\]\\{}|'~?!@#$%^&*()_+-=]+)", " ", tweet)
        tweet = re.sub(r"(@[a-zA-Z0-9_]+)|(\w+:\/\/\S+)", " ", tweet)
        # logging.debug(tweet)

        tweet_bytes = tweet.encode()
        #logging.debug("tweet bytes: {}".format(tweet_bytes))
        words = TweetTokenizer().tokenize(tweet_bytes)
        words_filtered = []
        for w in words:
            w = w.lower()
            # include emoji, alphanumeric, and hashtag strings
            if (w in UNICODE_EMOJI) or (w.isalnum()) or (w.startswith("#")):
                if (w not in self.stop_words) and (w not in words_filtered):
                    words_filtered.append(w)
        return words_filtered

    def __update_count(self, words, date):
        # update word and count
        for w in words:
            if w in self.words_counts_by_date:
                info = self.words_counts_by_date[w]
                if date in info:
                    # logging.debug("word data exist in the container")
                    self.words_counts_by_date[w][date] += 1
                else:
                    # logging.debug("word data not exist in the container")
                    self.words_counts_by_date[w][date] = 1
            else:
                # logging.debug("word not exist in the container")
                info = {date: 1}
                self.words_counts_by_date[w] = info
            # logging.debug(self.words_counts_by_date)

    def __read_tweets(self, file_name_full, type, event):
        with smart_open.smart_open(file_name_full, 'r') as f:
            logging.debug("file: {}".format(file_name_full))
            """ read tweets from files and classify them as it reads"""
            for line in f:
                try:
                    # do not add duplicated tweet by tweet unique id
                    tweet = json.loads(line)
                    tweet_id = int(tweet['id'])

                    if tweet_id not in self.unique_id_set:
                        # logging.debug("utc tweet date {}".format(tweet_date_utc))
                        tweet_text = tweet['text']
                        tweet_date_utc = tweet['created_at']
                        tweet_date_local = self.event_helper.convert_to_loctime_from_event(tweet_date_utc,
                                                                                           event)
                        date = str(tweet_date_local.date())
                        # logging.debug("tweet: {} date: {}".format(tweet_text, date))
                        words = self.__tokenize_tweet(tweet_text)
                        # logging.debug(words)
                        self.__update_count(words, date)
                        self.total_tweets += 1
                        # update tweet id if from geotag file
                        if type == "geotag":
                            self.unique_id_set.append(tweet_id)
                except Exception as e:
                    logging.debug("could not read tweet error: {}".format(e))

    def process_tweets(self):
        for event in self.events:
            (geotag_files, timeline_files) = self.__get_files(int(event))

            # classify user tweets only if files exist in both geotagged and timeline
            #if event in self.geotaged_exist and event in self.timeline_exist and not os.path.isfile(output):
            #if (len(geotag_files) > 0) and (len(timeline_files) > 0):
            if (len(geotag_files) > 0) or (len(timeline_files) > 0):
                for file_name in geotag_files:
                    file_name_full = os.path.join(self.input_paths[0], file_name)
                    logging.debug("start reading file {}".format(file_name_full))
                    self.__read_tweets(file_name_full, "geotag", event)
                for file_name in timeline_files:
                    file_name_full = os.path.join(self.input_paths[1], file_name)
                    logging.debug("start reading file {}".format(file_name_full))
                    self.__read_tweets(file_name_full, "timeline", event)
                output = os.path.join(self.output_path, r"{}_words_freq.csv".format(event))
                self.__save_stats(output)
                self.__reset_event_data()

    def __save_stats(self, output):
        """ create information of each date and number of tweets
            save all information to json file ex. "319_user_stats.csv"""
        logging.debug("save file: {} total tweets: {} ".format(output, self.total_tweets))
        # remove file if it exist
        try:
            os.remove(output)
        except OSError:
            pass

        for word in self.words_counts_by_date:
            result_list = []
            for date in self.words_counts_by_date[word]:
                count = self.words_counts_by_date[word][date]
                result_list.append((word, date, count))
            with open(output, 'a') as file:
                csv_out = csv.writer(file)
                # logging.debug("writing result list for date= {}".format(result_list))
                for row in result_list:
                    csv_out.writerow(row)

if __name__ == '__main__':
    logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)
    input_paths = ["../../events_tweets/Event - 319 - Moore Tornado/geotagged_from_archive/",
                   "../../events_tweets/Event - 319 - Moore Tornado/user_timelines/"]
    output_path = "../../words_count"
    incident_metadata_path = '../incident_metadata.csv'
    text_processor = TextProcessor(incident_metadata_path, input_paths, output_path)
    #event = 319
    text_processor.process_tweets()
    # Word, Date, Number of tweets that had the word
