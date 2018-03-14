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
import pickle
import numpy as np
import seaborn as sns

from collections import OrderedDict
from statistics import mean, stdev

import itertools
import math

# import external libraries
from text_process.tweets_text_processor import TextProcessor

class TopicModeler:
    tknzr = nltk.tokenize.TweetTokenizer()
    stopwords = nltk.corpus.stopwords.words('english')
    stemmer = nltk.stem.PorterStemmer()
    #stemmer = SnowballStemmer("english")

    def __init__(self):
        pass

    def train_test_split(self, tweets):
        num_doc = len(tweets)
        pivot = int(num_doc*0.8)
        train = tweets[:pivot]
        test = tweets[pivot:]

        return train, test

    def find_topics_by_period(self, corpus, n_features, n_topics, incident, perplexity=False):
        topics_by_period = []
        pipelines = []
        result = None
        perplexity_result = []
        for index, tweets in enumerate(corpus):
            period_type = 'bf' if index is 0 else 'af'
            if perplexity:
                train, test = self.train_test_split(tweets)
                [pipeline, model, lda, vect] = self.find_n_topics(train, n_features, n_topics, incident, period_type)
                perplexity = lda.perplexity(vect.transform(test))
                perplexity_result.append(perplexity)
            else:
                [pipeline, model, lda, vect] = self.find_n_topics(tweets, n_features, n_topics, incident, period_type)
                result = self.__flatten_results(pipeline, model, period_type)
                topics_by_period.append(result)

        if perplexity:
            return perplexity_result
        else:
            self.__compare_topics(topics_by_period[0]['topic_word_dist_list'],
                                  topics_by_period[1]['topic_word_dist_list'])
            return topics_by_period

    def find_n_topics(self, tweets, n_features, n_topics, incident, period):
        # train the model on the whole data
        override = False
        backup_name = "backup/topic1_pipeline_{}_{}_perplex.p".format(n_topics, period)
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

    """
    word_dists e.g. [{word1: weight1, word2: weight2, word3: weight3....}, {word1, weight1, word2: weight2....}...]
    """
    def __get_topic_word_dist(self, component, feature_names, num_words=100):
        word_dists = []
        for topic_idx, features in enumerate(component):
            feature_list = OrderedDict()  # {word1: weight, word2: weight ...}
            for i in features.argsort()[:-num_words - 1:-1]:
                feature = feature_names[i]
                weight = features[i]
                feature_list[feature] = weight

            word_dists.append(feature_list)
        return word_dists

    '''
    Parse topic distribution and word distribution.
    Return the result of topic distribution, topic name sorted by distribution, and list of topic word distribution
    '''
    def __parse_dist_info(self, components, feature_names, model, num_topics=20):
        average = np.average(np.array(model), axis=0)
        print("topic distribution: \n")
        print("average:", average)

        topic_word_dist_list = self.__get_topic_word_dist(components, feature_names)

        topic_sorted_by_dist = average.argsort()[::-1]
        print("topic sorted by dist:", topic_sorted_by_dist)

        for topic_name in topic_sorted_by_dist[:num_topics]:
            word_dist = topic_word_dist_list[topic_name]
            words = ', '.join(list(word_dist.keys())[:num_topics])
            print("topic " + str(topic_name) + " top words:", words)

        result = {"avg": average, "topic_sorted_by_dist": topic_sorted_by_dist,
                  "topic_word_dist_list": topic_word_dist_list}

        print("\n\n")

        return result

    def __write_topics(self, period, topic_word_dist_list, topic_sorted_by_dist, average):
        with codecs.open(''.join(['data/model1/', str(num_topic) + "_" + str(incident) + "_"
                                  + period, '_topics1.csv']), "w+",'utf-8') as out_file:
            writer = csv.writer(out_file, delimiter=',', quoting=csv.QUOTE_MINIMAL)
            for topic_name in topic_sorted_by_dist:
                topic_str = "topic" + str(topic_name) + " | " + str(average[topic_name])
                writer.writerow([topic_str, ' | '.join([key + ' ' + str(round(value, 2)) for key, value in
                                                        topic_word_dist_list[topic_name].items()])])

    def __flatten_results(self, pipeline, model, period):
        components = pipeline._final_estimator.components_
        components /= components.sum(axis=1)[:, np.newaxis]
        print("normalized components", components)
        feature_names = pipeline.named_steps['vect'].get_feature_names()

        result = self.__parse_dist_info(components, feature_names, model)
        self.__write_topics(period, result['topic_word_dist_list'], result['topic_sorted_by_dist'], result['avg'])

        with open('data/model1/distribution/{}_{}.pkl'.format("result", period), 'wb') as f:
            pickle.dump(result, f, pickle.HIGHEST_PROTOCOL)
        return result

    def __cosine_similarirty(self, topic1, topic2):
        words = list(topic1.keys() | topic2.keys())
        vec1 = [topic1.get(word, 0) for word in words]
        vec2 = [topic2.get(word, 0) for word in words]

        numerator = np.dot(vec1, vec2)
        norm_vec1 = np.linalg.norm(vec1)
        norm_vec2 = np.linalg.norm(vec2)

        return numerator / (norm_vec1 * norm_vec2)

    def __get_sim_matrix(self, bf_topics, af_topics):
        all_matrix = {}
        bf_bf_topic_similarity = []
        bf_af_topic_similarity = []
        for i, feature in enumerate(bf_topics):
            bf_sim_scores = []
            for feature_to_cmp in bf_topics:
                score = self.__cosine_similarirty(feature, feature_to_cmp)
                bf_sim_scores.append(score)
            #logging.debug("bf i: {} bf scores: {}".format(i, bf_sim_scores))
            bf_bf_topic_similarity.append(bf_sim_scores)

            af_sim_scores = []
            for feature_to_cmp in af_topics:
                score = self.__cosine_similarirty(feature, feature_to_cmp)
                af_sim_scores.append(score)
            #logging.debug("bf i: {} af scores: {}".format(i, af_sim_scores))
            bf_af_topic_similarity.append(af_sim_scores)

        bf_bf_topic_sim_matrix = np.array(bf_bf_topic_similarity)
        print("shape", bf_bf_topic_sim_matrix.shape, "bf bf sim matrix", bf_bf_topic_sim_matrix)

        bf_af_topic_sim_matrix = np.array(bf_af_topic_similarity)
        print("shape", bf_bf_topic_sim_matrix.shape, "bf af sim matrix", bf_af_topic_sim_matrix)

        #af_bf_topic_similarity = []
        af_af_topic_similarity = []
        for i, feature in enumerate(af_topics):

            # update af - bf topic similarity from the previous bf - af matrix
            # af_bf_topic_similarity.append(bf_af_topic_sim_matrix[:, i])

            af_sim_scores = []
            for feature_to_cmp in af_topics:
                score = self.__cosine_similarirty(feature, feature_to_cmp)
                af_sim_scores.append(score)

            #logging.debug("af i: {} af scores: {}".format(i, af_sim_scores))
            af_af_topic_similarity.append(af_sim_scores)

        af_af_topic_sim_matrix = np.array(af_af_topic_similarity)
        print("shape", bf_bf_topic_sim_matrix.shape, "af af sim matrix", af_af_topic_sim_matrix)

        all_matrix['bf_bf_matrix'] = bf_bf_topic_sim_matrix
        all_matrix['bf_af_matrix'] = bf_af_topic_sim_matrix
        all_matrix['af_bf_matrix'] = bf_af_topic_sim_matrix.transpose()
        all_matrix['af_af_matrix'] = af_af_topic_sim_matrix

        return all_matrix

    def __find_unique_topic(self, topic_sim_matrix, threshold=0.7):

        topic_idx = set(range(topic_sim_matrix.shape[0]))
        common_topic_idx = set()
        for i in range(topic_sim_matrix.shape[0]):
            idx = np.where(topic_sim_matrix[i] > threshold)[0]
            print("scores", topic_sim_matrix[i])
            print("i", i, "matching score idx", idx)
            if len(idx) > 0:
                common_topic_idx.add(i)

        unique_topic_idx = topic_idx - common_topic_idx

        return list(common_topic_idx), list(unique_topic_idx)

    def __compare_sim(self, topic_sim_matrix, within=False):
        sim_scores = []
        for i in range(topic_sim_matrix.shape[0]):
            if within:
                scores = np.hstack((topic_sim_matrix[i, :i], topic_sim_matrix[i, i + 1:]))
            else:
                scores = topic_sim_matrix[i]
            sim_scores.extend(scores)

        mean_sim = mean(sim_scores)
        max_sim = max(sim_scores)

        sns.kdeplot(sim_scores)
        return {"sim_scores": sim_scores, "max_sim": max_sim, "mean_sim": mean_sim,
                "stdev_sim": stdev(sim_scores)}

    def __compare_max_sim(self, topic_sim_matrix, within=False):
        max_sim_scores = []
        for i in range(topic_sim_matrix.shape[0]):
            if within:
                max_score = max(np.hstack((topic_sim_matrix[i, :i], topic_sim_matrix[i, i + 1:])))
            else:
                max_score = max(topic_sim_matrix[i, :])
            max_sim_scores.append(max_score)

        mean_sim = mean(max_sim_scores)
        max_sim = max(max_sim_scores)

        sns.kdeplot(max_sim_scores)
        return {"max_sim_scores": max_sim_scores, "max_sim": max_sim, "mean_sim": mean_sim,
                "stdev_sim": stdev(max_sim_scores)}

    def __get_most_similar_topic(self, all_matrix, topic_name, period_to_cmp):
        topic_sim_matrix = all_matrix[period_to_cmp]

        if period_to_cmp in ['bf_bf_matrix', 'af_af_matrix']:
            matched_topic = np.argmax(np.hstack((topic_sim_matrix[topic_name, :topic_name],
                                                 topic_sim_matrix[topic_name, topic_name + 1:])))
        else:
            matched_topic = np.argmax(topic_sim_matrix[topic_name, :])

        return matched_topic, topic_sim_matrix[topic_name, matched_topic]

    def __write_sim_matrix(self, all_matrix):
        for key, matrix in all_matrix.items():
            np.save('data/model1/matrix/{}.npy'.format(key), matrix)
            np.savetxt('data/model1/matrix/{}.txt'.format(key), matrix)

    def __compare_topics(self, bf_topics, af_topics):
        all_matrix = self.__get_sim_matrix(bf_topics, af_topics)
        self.__write_sim_matrix(all_matrix)

        for key, matrix in all_matrix.items():
            within = False
            if key in ['bf_bf_matrix', 'af_af_matrix']:
                within = True
            stats = self.__compare_sim(matrix, within)
            print("\nSimilarities over all pairs\n")
            print("key", key, "sim scores", stats['sim_scores'], "max", stats['max_sim'], "mean", stats['mean_sim'],
                "stdev", stats["stdev_sim"])

            stats = self.__compare_max_sim(matrix, within)
            print("\nMax Similarities over each topic\n")
            print("key", key, "sim max scores", stats['max_sim_scores'], "max", stats['max_sim'], "mean", stats['mean_sim'],
                  "stdev", stats["stdev_sim"])

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
               "/Users/stellachoi/Box Sync/research_work/events_tweets /Event - 319 - Moore Tornado/user_timelines/"]
incident_metadata_path = '/Users/stellachoi/Box Sync/research_work/disaster_analysis/incident_metadata.csv'
output_path = "/Users/stellachoi/Box Sync/research_work/disaster_analysis/data"
incident = 319
analyzer = TopicModeler()
# corpus = analyzer.extract_tweet_text(319, 'data/selected_incident_tweets/')
corpus = analyzer.extract_tweet_with_period(incident, input_paths, output_path, incident_metadata_path)

print("tweet_bf_len: {} tweet_af_len: {}".format(len(corpus[0]), len(corpus[1])))

#num_topic = 100
#lda_models = analyzer.find_topics_by_period(corpus, 10000, num_topic, 319)

perplexity = {}
for i in range(1, 9):
    num_topic = i * 20
    result = analyzer.find_topics_by_period(corpus, 10000, num_topic, 319, True)
    perplexity[num_topic] = result

print("perplexity", perplexity)

# analyzer.label_documents(lda_model, 319, 'data/selected_incident_tweets/')

