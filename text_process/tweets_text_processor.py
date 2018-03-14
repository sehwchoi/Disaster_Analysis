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
sys.path.append('../')
from meta_data_helper import EventMetaDataHelper

class TextProcessor(object):
    geotag_folder = "geotagged_from_archive"
    timeline_folder = "user_timelines"
    stemmer = nltk.stem.PorterStemmer()

    """ Reads all files for all events and counts word, hashtag, emojis frequencies by date for each event

        Attributes:
            self.output_path (string) : output path where user stats should be saved
            self.input_paths (list) : input paths of geotag files and timeline files
            self.event_helper (EventMetaDataHelper class) :
            self.events (list) : Ex) [1, 2, 3....]

            self.total_tweets (int) :
            self.unique_id_set (list) : tracks the unique tweet id in geotagged files
            self.word_counts_by_date (dict) : {word(str) : {date(str): count(int), date(str): count...
    """
    def __init__(self, incident_metadata_path, input_paths, output_path, model="word_count"):

        self.input_paths = input_paths
        self.output_path = output_path

        # events time conversion helper
        self.event_helper = EventMetaDataHelper(incident_metadata_path)

        # download nltk libraries
        nltk.download('punkt')
        nltk.download('stopwords')
        # get stop words
        self.stop_words = set(stopwords.words('english'))
        # logging.debug("Stop words : {}".format(self.stop_words))

        # tracking data for each event
        self.total_tweets = 0
        self.unique_id_set = []
        self.words_counts_by_date = {}
        self.model = model

    def __reset_event_data(self):
        """ reset event specific parameter """
        self.total_tweets = 0
        self.unique_id_set.clear()
        self.tweets_counts_by_date = {}

    def __get_files(self, event_id):
        """ parse all geotagged/timeline files for given event_id"""
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

    # tokenize unique words
    def __tokenize_tweet(self, tweet):
        """ tokenize tweet text into word, hashtag word, and emojis all"""
        # remove users and any links
        tweet = re.sub(r"(@[a-zA-Z0-9_]+)|(\w+:\/\/\S+)", " ", tweet)
        # logging.debug(tweet)

        tweet_bytes = tweet.encode()
        #logging.debug("tweet bytes: {}".format(tweet_bytes))
        words = TweetTokenizer().tokenize(tweet_bytes)
        words_filtered = []
        for w in words:
            w = w.lower()
            # include str only if emoji, alphanumeric, and hashtag strings
            if (w in UNICODE_EMOJI) or (w.isalnum()) or (w.startswith("#")):
                # filter out any stopword and a word already added
                if (w not in self.stop_words) and (w not in words_filtered):
                    words_filtered.append(w)

        return words_filtered

    # tokenize as it is
    def __tokenize_tweet2(self, tweet):
        """ tokenize tweet text into word, hashtag word, and emojis all"""
        # remove users and any links
        tweet = re.sub(r"(@[a-zA-Z0-9_]+)|(\w+:\/\/\S+)", " ", tweet)
        # logging.debug(tweet)

        tweet_bytes = tweet.encode()
        #logging.debug("tweet bytes: {}".format(tweet_bytes))
        words = TweetTokenizer().tokenize(tweet_bytes)
        words_filtered = []
        for w in words:
            w = w.lower()
            # include str only if emoji, alphanumeric, and hashtag strings
            if (w in UNICODE_EMOJI) or (w.isalnum()) or (w.startswith("#")):
                # filter out any stopword and a word already added
                if w not in self.stop_words:
                    w = self.stemmer.stem(w)
                    if len(w.strip()) > 2:
                        words_filtered.append(w)

        return words_filtered

    def __update_count(self, words, date):
        """ add or update word frequencies"""
        for w in words:
            # if word already added, then increment frequencies
            if w in self.words_counts_by_date:
                info = self.words_counts_by_date[w]
                if date in info:
                    # logging.debug("word data exist in the container")
                    self.words_counts_by_date[w][date] += 1
                else:
                    # logging.debug("word data not exist in the container")
                    self.words_counts_by_date[w][date] = 1
            # if word not added before, then add it with a proper date
            else:
                # logging.debug("word not exist in the container")
                info = {date: 1}
                self.words_counts_by_date[w] = info
            # logging.debug(self.words_counts_by_date)

    def __read_tweets(self, file_name_full, type, event):
        """ read tweets from file and count word frequencies as it reads"""
        with smart_open.smart_open(file_name_full, 'r') as f:
            logging.debug("file: {}".format(file_name_full))
            tweets_bf = []
            tweets_af = []
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
                        # only use year, month, and date
                        tweet_loc_datetime = tweet_date_local
                        date_str = str(tweet_date_local.date())
                        # logging.debug("tweet: {} date: {}".format(tweet_text, date))
                        words = self.__tokenize_tweet2(tweet_text)
                        # logging.debug(words)
                        if self.model is "word_count":
                            self.__update_count(words, date_str)
                        elif self.model is "topic":
                            ev_begin = self.event_helper.get_event_times(event)[0]
                            if tweet_loc_datetime < ev_begin:
                                tweets_bf.append(' '.join(words))
                            else:
                                tweets_af.append(' '.join(words))
                        self.total_tweets += 1
                        # update tweet id if from geotag file
                        # if a tweet is from geotag add it to unique tweet set to filter out any duplication later when
                        # reading timeline
                        if type == "geotag":
                            self.unique_id_set.append(tweet_id)
                except Exception as e:
                    logging.debug("could not read tweet error: {}".format(e))

            #print("tweet_bf", tweets_bf)
            return [tweets_bf, tweets_af]

    def process_tweets(self, event):
        """ read tweets from files and count word frequencies for for each event"""
        (geotag_files, timeline_files) = self.__get_files(int(event))
        tweets_bf = []
        tweets_af = []
        corpus = []
        file_types = [geotag_files, timeline_files]
        type_names = {0: "geotag", 1: "timeline"}
        # classify user tweets only if files exist in both geotagged and timeline
        # if event in self.geotaged_exist and event in self.timeline_exist and not os.path.isfile(output):
        if (len(geotag_files) > 0) and (len(timeline_files) > 0):
            # type can be geotag or timeline, iterate files in geotag and timeline folders
            for i in range(len(file_types)):
                for file_name in file_types[i]:
                    file_name_full = os.path.join(self.input_paths[i], file_name)
                    logging.debug("start reading file {}".format(file_name_full))
                    texts = self.__read_tweets(file_name_full, type_names[i], event)
                    tweets_bf.extend(texts[0])
                    tweets_af.extend(texts[1])

            if self.model is "word_count":
                output = os.path.join(self.output_path, r"{}_words_freq.csv".format(event))
                self.__save_stats(output)
            elif self.model is "topic":
                corpus.extend([tweets_bf, tweets_af])
                output = os.path.join(self.output_path, "{}_tweets_bf.csv".format(event))
                logging.debug(tweets_bf)
                self.__save_tweets(output, tweets_bf)
                output = os.path.join(self.output_path, "{}_tweets_af.csv".format(event))
                self.__save_tweets(output, tweets_af)

            self.__reset_event_data()
        return corpus

    def __save_tweets(self, output, data):
        try:
            os.remove(output)
        except OSError:
            pass

        with open(output, 'a') as file:
            csv_out = csv.writer(file, lineterminator='\n')
            # logging.debug("writing result list for date= {}".format(result_list))
            csv_out.writerow(["tweet"])
            for tweet in data:
                csv_out.writerow([tweet])

    def __save_stats(self, output):
        """ create information of (word, date, count)
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
"""
if __name__ == '__main__':
    logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)
    input_paths = ["../../events_tweets/Event - 319 - Moore Tornado/geotagged_from_archive/",
                   "../../events_tweets/Event - 319 - Moore Tornado/user_timelines/"]
    output_path = "../../words_count"
    incident_metadata_path = '../incident_metadata.csv'
    text_processor = TextProcessor(incident_metadata_path, input_paths, output_path)
    text_processor.process_tweets()
"""
