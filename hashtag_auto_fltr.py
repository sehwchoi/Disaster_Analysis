import logging, sys
import pandas as pd
import os
from posix import strerror

# this class automatically mark hashtag relevance -1 for possible common hashtags
class HashtagAutoFltr(object):
    common_hashtags = [] 
    # parse common hastag list files and save them to python list, common_hashtags
    def __init__(self, hashtag_list_files):
        for file in hashtag_list_files:
            df = pd.read_csv(file)
            self.common_hashtags.extend(df.iloc[:,0].tolist())
            logging.debug(self.common_hashtags)

    # mark -1 as a rating for possible common hashtags and 0 for all others
    def filter_out(self, target_file):
        column_names = ['Hashtag', 'count']
        df = pd.read_csv('../relevant_tweets/'+target_file, header=None, names=column_names)
        ratings = []
        for row in df.iloc[:,0]:
            if row in self.common_hashtags:
                ratings.append(-1)
            else:
                ratings.append(0)
        df['relevance'] = ratings
        file_name_only = os.path.splitext(target_file)[0]
        df.to_csv('../relevant_tweets/'+file_name_only+"_new.csv", index=False)
        

if __name__ == '__main__':
    logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)
    hashtag_list_files = ["top_hashtag.txt"]
    fixed_file_name = '_hashtag_counts.csv'
    target_files = ['340'+fixed_file_name]
    filtering = HashtagAutoFltr(hashtag_list_files)
    filtering.filter_out(target_files[0])
    