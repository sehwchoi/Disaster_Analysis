from random import shuffle

from sklearn.decomposition import LatentDirichletAllocation
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.pipeline import Pipeline
import pyLDAvis.sklearn
import matplotlib.pyplot as plt

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
import pickle
import numpy as np
from collections import OrderedDict
from statistics import mean, stdev

import itertools
import math

# import external libraries
from text_process.tweets_text_processor import TextProcessor

class TopicModeler:
    tknzr = nltk.tokenize.TweetTokenizer()
    stemmer = nltk.stem.PorterStemmer()
    stopwords = nltk.corpus.stopwords.words('english')

    def __init__(self):
        pass

    def find_topics_by_period(self, corpus, n_features, n_topics, incident):
        num_bf_doc = len(corpus[0])

        # combine tweets collected before disaster and after disaster
        corpus_t = corpus[0][:]
        corpus_t.extend(corpus[1])
        [pipeline, model, lda, vect] = self.find_n_topics(corpus_t, n_features, n_topics, incident)

        self.__compare_topics(pipeline, model, num_bf_doc)

        return lda, vect

    def find_n_topics(self, tweets, n_features, n_topics, incident):
        # train the model on the whole data
        override = True
        backup_name = "backup/topic_pipeline_100_0305_1.p"
        if override:
            pipeline = pickle.load(open(backup_name, "rb"))
            model = pipeline.transform(tweets)
            lda = pipeline.named_steps['lda']
            vect = pipeline.named_steps['vect']
            return [pipeline, model, lda, vect]

        pipeline = Pipeline([
            ('vect', CountVectorizer(max_df=0.95, min_df=2,
                        max_features=n_features,
                        stop_words='english')),
            ('lda', LatentDirichletAllocation(n_components=n_topics,
                                              max_iter=10,
                                              learning_method='online',
                                              learning_offset=20.)),
        ])

        model = pipeline.fit_transform(tweets)
        # save pipeline
        pickle.dump(pipeline, open(backup_name, "wb+"))

        lda = pipeline.named_steps['lda']
        vect = pipeline.named_steps['vect']

        return [pipeline, model, lda, vect]

    def __get_topic_word_dist(self, topics, feature_names, num_words=100):
        word_dists = []  # [{word1: weight1, word2: weight2, word3: weight3....}, {word1, weight1, word2: weight2....}...]
        for topic_idx, topic in enumerate(topics):
            feature_list = OrderedDict()  # {word1: weight, word2: weight ...}
            for i in topic.argsort()[:-num_words - 1:-1]:
                feature = feature_names[i]
                weight = round(topic[i], 2)
                feature_list[feature] = weight

            word_dists.append(feature_list)
        return word_dists

    def write_topics(self, period, top_topics_words, top_topics, average):
        with codecs.open(''.join(['data/model2/', str(num_topic) + "_" + str(incident) + "_"
                                  + period, '_topics2.csv']), "w+",'utf-8') as out_file:
            writer = csv.writer(out_file, delimiter=',', quoting=csv.QUOTE_MINIMAL)
            for i, topic in enumerate(top_topics_words):
                topic_str = "topic" + str(top_topics[i]) + " | " + str(average[top_topics[i]])
                writer.writerow([topic_str, ' | '.join([key + ' ' + str(value) for key, value in topic.items()])])

    def show_topic_dist(self, components, feature_names, model, num_topics=20):
        average = np.average(np.array(model), axis=0)
        top_topics = average.argsort()[::-1]
        print("average:", average)

        top_component = [components[i] for i in top_topics]
        top_topics_words = self.__get_topic_word_dist(top_component, feature_names)

        print("top_topics:", top_topics[:num_topics])
        for i, words in enumerate([', '.join(list(dist.keys())[:num_topics]) for dist in top_topics_words[:num_topics]]):
            print("topic " + str(top_topics[i]) + " top words:", words)

        result = {"avg": average, "top_topics": top_topics, "top_component": top_component, "top_topics_words":
                  top_topics_words}
        return result

    def __compare_topics(self, pipeline, model, num_bf_doc):
        components = pipeline._final_estimator.components_
        feature_names = pipeline.named_steps['vect'].get_feature_names()

        # overall topic distributions
        print("overall topic distribution: \n")
        model_t = model
        self.show_topic_dist(components, feature_names, model_t)
        print("\n\n")

        print("Before disaster's topic distribution: \n")
        model_bf = model[:num_bf_doc]
        result_bf = self.show_topic_dist(components, feature_names, model_bf)
        top_topics_bf = result_bf['top_topics'][:20]
        self.write_topics('bf', result_bf['top_topics_words'], result_bf['top_topics'], result_bf['avg'])
        print("\n\n")

        print("After disaster's topic distribution: \n")
        model_af = model[num_bf_doc:]
        result_af = self.show_topic_dist(components, feature_names, model_af)
        top_topics_af = result_af['top_topics'][:20]
        self.write_topics('af', result_af['top_topics_words'], result_af['top_topics'], result_af['avg'])
        print("\n\n")

        x_coordinate = [i + 1 for i in range(len(result_bf['avg']))]
        #plt.xticks(x_coordinate, rotation='vertical')
        plt.plot(x_coordinate, result_bf['avg'], 'b')
        plt.plot(x_coordinate, result_af['avg'], 'g')
        plt.title("Topic distribution over periods")
        plt.show()

        print("topic distribution difference: \n")
        abs_diff = self.__distribute_diff(result_bf['avg'], result_af['avg'])
        print("mean", mean(abs_diff), "stdev", stdev(abs_diff))

        plt.plot(x_coordinate, abs_diff, 'b')
        plt.title("Topic distribution difference")
        plt.show()

        print("Log transform on the difference: \n")
        abs_diff = self.__distribute_diff(np.log(result_bf['avg']), np.log(result_af['avg']))
        print("log mean", mean(abs_diff), "stdev", stdev(abs_diff))
        plt.plot(x_coordinate, abs_diff, 'b')
        plt.title("Topic Log distribution difference")
        plt.show()

        print("distribution similarity: \n")
        print(self.__consine_similarirty(result_bf['avg'], result_af['avg']))
        print(list(top_topics_bf | top_topics_af))
        print(list(set(top_topics_bf).symmetric_difference(set(top_topics_af))))

    def __distribute_diff(self, avg1, avg2):
        diff = avg1 - avg2
        #print("diff", diff)

        abs_diff = np.absolute(diff)
        topics = abs_diff.argsort()[::-1]

        print("abs_diff", abs_diff)
        print("topics sorted by diff", topics)

        return abs_diff

    def __consine_similarirty(self, vec1, vec2):
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
        file_bf = os.path.join(output_path, "{}_tweets_bf.csv".format(incident))
        file_af = os.path.join(output_path, "{}_tweets_af.csv".format(incident))

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
incident_metadata_path = '/Users/stellachoi/Box Sync/research_work/disaster_analysis/incident_metadata.csv'
output_path = "/Users/stellachoi/Box Sync/research_work/disaster_analysis/data"
incident = 319
analyzer = TopicModeler()
# corpus = analyzer.extract_tweet_text(319, 'data/selected_incident_tweets/')
corpus = analyzer.extract_tweet_with_period(incident, input_paths, output_path, incident_metadata_path)

print("tweet_bf_len: {} tweet_af_len: {}".format(len(corpus[0]), len(corpus[1])))

num_topic = 100
lda, vect = analyzer.find_topics_by_period(corpus, 10000, num_topic, 319)

#for i in range(1, 9):
#    num_topic = i * 20
#    lda_models = analyzer.find_topics_by_period(corpus, 10000, num_topic, 319)
# analyzer.label_documents(lda_model, 319, 'data/selected_incident_tweets/')

