from nltk.tokenize import RegexpTokenizer
from nltk.corpus import stopwords
import nltk
import logging, sys
import os
import re
import smart_open
import json
import time
from word_count_model import WordsDBManager
sys.path.append('../')
from meta_data_helper import EventMetaDataHelper


class TextProcessor(object):
    def __init__(self, incident_metadata_path, word_db_info):
        # create DB manager and pass db file location and table name
        self.db_manager = WordsDBManager(word_db_info)

        # events time conversion helper
        self.event_helper = EventMetaDataHelper(incident_metadata_path)
        self.events = self.event_helper.get_all_events()

        nltk.download('stopwords')
        # stop words
        self.stop_words = set(stopwords.words('english'))
        logging.debug("Stop words : {}".format(self.stop_words))
        # get words
        self.tokenizer = RegexpTokenizer(r'\w+')
        self.total_tweets = 0
        self.words_stats = {}

    def __tokenize_tweet(self, tweet):
        # remove users, hashtags, any links
        tweet = re.sub(r"(@[A-Za-z0-9]+)|(#)|(\w+:\/\/\S+)", " ", tweet)
        # logging.debug(tweet)
        #
        words = self.tokenizer.tokenize(tweet)
        words_filtered = []
        for w in words:
            w = w.lower()
            if (w not in self.stop_words) and (w not in words_filtered):
                words_filtered.append(w)
        return words_filtered

    def __update_count(self, words, date, tweet_id):
        # process word insert/updating count only if tweet is not already processed
        if self.db_manager.check_exist_tweet(tweet_id) == 0:
            for w in words:
                if self.db_manager.check_exist_word(w, date) != 0:
                    self.db_manager.update_count(w, date)
                else:
                    data_dict = {
                        "word": w,
                        "date": date,
                        "count": 1
                    }
                    self.db_manager.insert_word(data_dict)
            self.db_manager.insert_tweet(tweet_id)

    def process_tweets(self, path, event):
        for roots, dirs, files in os.walk(path):
            for file_name in files:
                file_match = re.search('(\d+)_', file_name)
                if file_match:
                    with smart_open.smart_open(os.path.join(roots, file_name), 'r') as f:
                        logging.debug("file: {}".format(file_name))
                        for line in f:
                            # do not add duplicated tweet by tweet unique id
                            tweet = json.loads(line)
                            tweet_text = tweet['text']
                            tweet_date_utc = tweet['created_at']
                            tweet_id = tweet['id']
                            # logging.debug("utc tweet date {}".format(tweet_date_utc))
                            tweet_date_local = self.event_helper.convert_to_loctime_from_event(tweet_date_utc,
                                                                                               event)
                            date = str(tweet_date_local.date())
                            # logging.debug("tweet: {} date: {}".format(tweet_text, date))
                            words = self.__tokenize_tweet(tweet_text)
                            # logging.debug(words)
                            self.__update_count(words, date, tweet_id)
                            self.total_tweets += 1
                        self.db_manager.commit_db()

if __name__ == '__main__':
    logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)
    db_name = "tweets_word.db"
    incident_metadata_path = '../incident_metadata.csv'
    text_processor = TextProcessor(incident_metadata_path, db_name)
    path = "/Users/sophiachoi/Documents/work/events_tweets"
    event = 319
    text_processor.process_tweets(path, event)
    # Word, Date, Number of tweets that had the word
