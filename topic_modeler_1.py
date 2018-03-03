from random import shuffle

from sklearn.decomposition import LatentDirichletAllocation
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.pipeline import Pipeline

import sys
import os
import logging
import re
import csv
import json
import smart_open
import codecs
import nltk
import time
import numpy as np

import itertools
import math

# import external libraries
from tweets_text_processor import TextProcessor

class TopicModeler:
    tknzr = nltk.tokenize.TweetTokenizer()
    stemmer = nltk.stem.PorterStemmer()
    stopwords = nltk.corpus.stopwords.words('english')
    stemmer = nltk.stem.PorterStemmer()

    def __init__(self):
        pass

    def find_topics_by_period(self, corpus, n_features, n_topics, incident):
        topics_by_period = []
        pipelines = []
        for index, tweets in enumerate(corpus):
            period_type = 'bf' if index is 0 else 'af'
            result = self.find_n_topics(tweets, n_features, n_topics, incident, period_type)
            topics_by_period.append(result[1])
            pipelines.append(result[0])

        self.__compare_topics(topics_by_period[0], topics_by_period[1], n_topics)

        return result

    def parse_topics(self, topics, feature_names, num_words=100):
        word_dists = []  # [{word1: weight1, word2: weight2, word3: weight3....}, {word1, weight1, word2: weight2....}...]
        for topic_idx, topic in enumerate(topics):
            feature_list = {}  # {word1: weight, word2: weight ...}
            for i in topic.argsort()[:-num_words - 1:-1]:
                feature = feature_names[i]
                weight = round(topic[i], 2)
                feature_list[feature] = weight

            word_dists.append(feature_list)
        return word_dists

    def find_n_topics(self, tweets, n_features, n_topics, incident, period):
        pipeline = Pipeline([
            ('vect', CountVectorizer(max_df=0.95, min_df=2,
                                     max_features=n_features,
                                     stop_words='english')),

            ('lda', LatentDirichletAllocation(n_topics=n_topics,
                                              max_iter=5,
                                              learning_method='online',
                                              learning_offset=20.)),
        ])
        topic_dist = pipeline.fit_transform(tweets)

        # TODO: save pipeline

        average = np.average(np.array(topic_dist), axis=0)
        top_topics = average.argsort()[:-4:-1]
        print("average", average)

        components = pipeline._final_estimator.components_
        top_component = [components[i] for i in top_topics]
        feature_names = pipeline.named_steps['vect'].get_feature_names()

        top_topics_words = self.parse_topics(top_component, feature_names, num_words=5)
        print("top_topics", top_topics)
        print("top_topics_words", [', '.join(dist.keys()) for dist in top_topics_words])

        topics = self.parse_topics(components, feature_names, num_words=30)

        with codecs.open(''.join(['data/topic_models/topics/', str(n_topics) + "_" + str(incident)+"_"+period, '_topics.csv']), "w+",
                         'utf-8') as out_file:
            for topic_idx, topic in enumerate(topics):
                out_file.write(''.join([key + ' ' + str(value) + ' | ' for key, value in topic.items()]))
                out_file.write('\n')

        with codecs.open(''.join(['data/topic_models/topics/', str(n_topics) + "_" + str(incident)+"_"+period, '_topics_reformat.csv']), "w+",
                     'utf-8') as out_file:
            csv_out = csv.writer(out_file, lineterminator='\n')
            csv_out.writerow(["topic", "word", "weight"])
            for topic_idx, topic in enumerate(topics):
                [csv_out.writerow([topic_idx, key, str(value)]) for key, value in topic.items()]

        ##cross validation
        """
        start = time.time()
        shuffle(tweets)
        perplexity = 0
        for i in range(0, 5):
            temp = list(tweets)
            test = tweets[int(math.floor(i * 0.2 * len(temp))):int(math.floor((i + 1) * 0.2 * len(temp)))]
            del temp[int(math.floor(i * 0.2 * len(tweets))):int(math.floor((i + 1) * 0.2 * len(tweets)))]
            pipeline.fit(temp)
            perplexity += pipeline.named_steps['lda'].perplexity(pipeline.named_steps['vect'].transform(tweets))
        end = time.time()
        print("elapsed: {} n_topic: {} Period: {} Perplexity: {}".format(end-start, n_topics, period, str(perplexity / 5.0)))
        """
        return [pipeline, topics]

    def __compare_topics(self, bf_topics, af_topics, n_topics):
        bf_sim_topics_idx = set()
        af_sim_topics_idx = set()

        # {0: [0.016196206007786252, 0.0088722334298753629, 0.003771657472340892, 0.0013458802544991553, 0.0044274557053691687, 0.25635824525587525, ...],
        #  1: [0.0036628029951590161, 0.049160918784579281, 0.0018400499119671306, 0.0084592512187031035, 0.017672816117843809, 0.015143889779694346,
        topic_similarity = {}
        for i in range(len(bf_topics)):
            sim_scores = []
            for j in range(len(af_topics)):
                score = self.__consine_similarirty(bf_topics[i], af_topics[j])
                # logging.debug("bf: {} af: {} score: {}".format(i, j, score))
                sim_scores.append(score)
            topic_similarity[i] = sim_scores
        logging.debug("Topic similiarity: {}".format(topic_similarity))

        score_file = ''.join(['data/topic_models/topics/', str(n_topics) + "_" + str(incident) + '_topics_sim_scores.csv'])
        with codecs.open(score_file, "w+", 'utf-8') as file:
            json.dump(topic_similarity, file)

        # filter out similar topics
        for bf_topic_idx, scores in topic_similarity.items():
            for af_topic_idx in range(len(scores)):
                if scores[af_topic_idx] > 0.5:
                    # keep tracks of topic number
                    bf_sim_topics_idx.add(bf_topic_idx)
                    af_sim_topics_idx.add(af_topic_idx)

        logging.debug("bf_sim:{} af_sim:{}".format(len(bf_sim_topics_idx), len(af_sim_topics_idx)))
        bf_unique_topics = []
        af_unique_topics = []
        bf_comm_topics = []
        af_comm_topics = []

        for i in range(len(bf_topics)):
            if i not in bf_sim_topics_idx:
                bf_unique_topics.append(bf_topics[i])
            else:
                bf_comm_topics.append(bf_topics[i])

        for i in range(len(af_topics)):
            if i not in af_sim_topics_idx:
                af_unique_topics.append(af_topics[i])
            else:
                af_comm_topics.append(af_topics[i])

        logging.debug("Topic unique  bf_len:{} af_len:{}".format(len(bf_unique_topics), len(af_unique_topics)))
        #logging.debug("Topic unique  bf: {} af: {}".format(bf_unique_topics, af_unique_topics))

        file_names = ["unique_bf_", "unique_af_", 'common_bf_', 'common_af_']
        topic_to_write = [bf_unique_topics, af_unique_topics, bf_comm_topics, af_comm_topics]
        for i, file_name in enumerate(file_names):
            file_name = ''.join(['data/topic_models/topics/', str(n_topics) + "_" + str(incident) + '_' + file_name + 'topics.csv'])
            with codecs.open(file_name, "w+", 'utf-8') as out_file:
                csv_out = csv.writer(out_file, lineterminator='\n')
                csv_out.writerow(["topic", "word", "weight"])
                for topic_idx, topic_list in enumerate(topic_to_write[i]):
                    for feature, weight in topic_list.items():
                        csv_out.writerow([topic_idx, feature, str(weight)])

    def __consine_similarirty(self, topic1, topic2):
        words = list(topic1.keys() | topic2.keys())
        vec1 = [topic1.get(word, 0) for word in words]
        vec2 = [topic2.get(word, 0) for word in words]

        #logging.debug("Vec1: {} \n Vec2: {}".format(topic1.keys(), topic2.keys()))
        # intersection = set(topic1.keys()) & set(topic2.keys())
        #logging.debug("Intersection: {}".format(intersection))

        numerator = np.dot(vec1, vec2)
        norm_vec1 = np.linalg.norm(vec1)
        norm_vec2 = np.linalg.norm(vec2)

        return numerator / (norm_vec1 * norm_vec2)

    def _preproc_text(self, text):
        remove_regex = ['\\s*@\\w*\\s*', '#', '!', '\\.', ';', ':', '\?', '\\W+\\d+', '^\\d+\\W+', '^\\d+$',
                        ',', '"', 'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+',
                        '&amp', '\\(', '\\)', '-', '\\[', '\\]', '\\|', '\\$', '~', '<', '>', '&', 'rt']

        regex_string = '|'.join(remove_regex)
        regex = re.compile(r'(' + regex_string + ')')
        text = regex.sub('', text)
        sanitized_text = [self.stemmer.stem(w) for w in self.tknzr.tokenize(text.lower()) if
                          w not in self.stopwords]
        sanitized_text = [w for w in sanitized_text if len(w.strip()) > 2]
        return sanitized_text

    def extract_tweet_text(self, incident, input_path):
        corpus = list()
        for dirpath, dirnames, filenames in os.walk(input_path):
            # del dirnames[:]
            for filename in [filename for filename in filenames if filename.startswith(str(incident))]:
                for line in smart_open.smart_open(os.path.join(dirpath, filename)):
                    try:
                        tweet = json.loads(line.decode('utf-8'))
                        text = tweet['text']
                        text = ' '.join(self._preproc_text(text))
                        corpus.append(text)
                    except Exception as e:
                        print(str(e))

        return corpus

    def extract_tweet_with_period(self, incident, input_paths, output_path, metadata_path):
        """
        extract tweets before and after disaster start
        input_paths are two paths, geotagged and timeline folders
        """
        file_bf = os.path.join(output_path, "{}_tweets_bf_test.csv".format(incident))
        file_af = os.path.join(output_path, "{}_tweets_af_test.csv".format(incident))

        if os.path.isfile(file_bf) and os.path.isfile(file_af):
            logging.debug("Tweets already extracted")
            corpus = []
            files = [file_bf, file_af]
            for file in files:
                with open(file,'r') as file:
                    lines = file.readlines()
                    tweets = lines[1:]
                    corpus.append(tweets)

                logging.debug(tweets[0:5])

        else:
            text_proc = TextProcessor(metadata_path, input_paths, output_path, model="topic")
            corpus = text_proc.process_tweets(incident)

        return corpus

    def label_documents(self, model, incident, input_path):
        for dirpath, dirnames, filenames in os.walk(input_path):
            del dirnames[:]
            for filename in [filename for filename in filenames if filename.startswith(str(incident))]:
                for line in smart_open.smart_open(os.path.join(dirpath, filename)):
                    tweet = json.loads(line.decode('utf-8'))
                    tweet_id = tweet['id']
                    tweet_time = time.strftime('%Y-%m-%d %H:%M:%S',
                                               time.strptime(tweet['created_at'], '%a %b %d %H:%M:%S '
                                                                                  '+0000 %Y'))
                    text = tweet['text']
                    text = ' '.join(self._preproc_text(text))
                    topics = model.transform([text]).tolist()[0]
                    with codecs.open(
                            ''.join(['data/topic_models/topic_distribs/', str(incident), '_topic_distribs.csv']), "a+",
                            'utf-8') as out_file:
                        line = [str(tweet_id), str(tweet_time)]
                        line.extend(map(str, topics))
                        out_file.write(','.join(line))
                        out_file.write('\n')

logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)
input_paths = ["/Users/stellachoi/Box Sync/research_work/events_tweets/Event - 319 - Moore Tornado/geotagged_from_archive/",
               "/Users/stellachoi/Box Sync/research_work/events_tweets/Event - 319 - Moore Tornado/user_timelines/"]
incident_metadata_path = '/Users/stellachoi/Box Sync/research_work/data_helper/incident_metadata.csv'
output_path = "/Users/stellachoi/Box Sync/research_work/disaster_social/twitter_analysis/data/topic_models/topics"
incident = 319
analyzer = TopicModeler()
# corpus = analyzer.extract_tweet_text(319, 'data/selected_incident_tweets/')
corpus = analyzer.extract_tweet_with_period(incident, input_paths, output_path, incident_metadata_path)

print("tweet_bf_len: {} tweet_af_len: {}".format(len(corpus[0]), len(corpus[1])))

num_topic = 10
lda_models = analyzer.find_topics_by_period(corpus, 50, num_topic, 319)
#for i in range(1, 9):
#    num_topic = i * 20
#    lda_models = analyzer.find_topics_by_period(corpus, 10000, num_topic, 319)
# analyzer.label_documents(lda_model, 319, 'data/selected_incident_tweets/')

