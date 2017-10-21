import logging, sys
import pandas as pd
import os
import re
from hashtags_list_maker import HashtagDBManager

# this class automatically mark hashtag relevance -1 for possible common hashtags
class HashtagAutoFltr(object):
    common_hashtags = [] 
    # parse common hastag list files and save them to python list, common_hashtags
    def __init__(self, daily_hashtags_lists):
        for file in daily_hashtags_lists:
            df = pd.read_csv(file)
            self.common_hashtags.extend(df.iloc[:,0].tolist())
            logging.debug(self.common_hashtags)

        # sets DB manager for already zero rated hashtags
        self.db_manager = HashtagDBManager()

    # mark -1 as a rating for possible common hashtags and 0 for all others
    def filter_out(self, input_path, output_path):
        for root, dirs, files in os.walk(input_path):
            for file in files:
                file_match = re.search('(\d+)_', file)
                if file_match:
                    with open(os.path.join(root, file), 'r') as file_obj:
                        logging.debug("file name : {}".format(file_obj))
                        try:
                            column_names = ['Hashtag', 'Count']
                            df = pd.read_csv(file_obj, header=None, names=column_names)
                            ratings = []
                            for row in df.iloc[:,0]:
                                # logging.debug("hashtag : {}".format(row))
                                if row in self.common_hashtags and self.db_manager.check_exist(row):
                                    ratings.append(-1)
                                else:
                                    ratings.append(0)
                            df['Relevance'] = ratings
                            name_only = os.path.splitext(file)[0]
                            df.to_csv(output_path + name_only+"_filtered.csv", index=False)
                        except Exception as e:
                            logging.debug("hashtag filtering error : {}".format(e))
        self.db_manager.disconnect()


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)
    daily_hashtags_lists = ["./hashtags_remove_candi/top_hashtag_rev.txt", "./hashtags_remove_candi/slangs1_rev.txt"]
    input_path = "../hash_tag_test"
    output_path = "../hashtag_rated/auto_filtered/"
    filtering = HashtagAutoFltr(daily_hashtags_lists)
    filtering.filter_out(input_path, output_path)
