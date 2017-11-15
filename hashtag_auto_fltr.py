import logging, sys
import pandas as pd
import os
import re
from hashtag_list_maker import HashtagDBManager

# this class automatically mark hashtag relevance -1 for possible common hashtags
class HashtagAutoFltr(object):
    common_hashtags = [] 
    # parse common hastag list files and save them to python list, common_hashtags
    def __init__(self, daily_hashtags_lists, hashtag_db_info):
        for file in daily_hashtags_lists:
            df = pd.read_csv(file)
            self.common_hashtags.extend(df.iloc[:,0].tolist())
            logging.debug(self.common_hashtags)

        # sets DB manager for already zero rated hashtags
        self.db_manager = HashtagDBManager(hashtag_db_info[0], hashtag_db_info[1])

    # mark -1 as a rating for possible common hashtags and 0 for all others
    def filter_out(self, input_path, output_path):
        result_info = []
        for root, dirs, files in os.walk(input_path):
            for file in files:
                file_match = re.search('(\d+)_', file)
                if file_match:
                    with open(os.path.join(root, file), 'r') as file_obj:
                        num_total_hashtags = 0
                        num_notfiltered_hashtags = 0
                        num_more_than_10_hashtags = 0
                        num_notfiltered_more_than_10_hashtags = 0
                        try:
                            column_names = ['Hashtag', 'Count']
                            df = pd.read_csv(file_obj, header=None, names=column_names)
                            ratings = []
                            for index, row in df.iterrows():
                                hashtag = row["Hashtag"]
                                count = row["Count"]
                                # logging.debug("hashtag: {} count: {}".format(hashtag, count))
                                num_total_hashtags += 1
                                if count >= 10:
                                    num_more_than_10_hashtags += 1
                                if (hashtag in self.common_hashtags) or (self.db_manager.check_exist(hashtag)):
                                    ratings.append(-1)
                                else:
                                    num_notfiltered_hashtags += 1
                                    if count >= 10:
                                        num_notfiltered_more_than_10_hashtags += 1
                                    ratings.append(0)
                            df['Relevance'] = ratings
                            name_only = os.path.splitext(file)[0]
                            df = df.sort_values(['Relevance', 'Count', "Hashtag"], ascending=[False, False, True])
                            df.to_csv(output_path + name_only+"_filtered.csv", index=False)
                            logging.debug("file name: {} total num: {} hashtag num after filtering: {}"
                                          " total count 10 hashtag: {} num_notfiltered_more_than_10_hashtags: {}".format(
                                                                        file_obj,
                                                                        num_total_hashtags,
                                                                        num_notfiltered_hashtags,
                                                                        num_more_than_10_hashtags,
                                                                        num_notfiltered_more_than_10_hashtags))
                        except Exception as e:
                            logging.debug("hashtag filtering error : {}".format(e))

                        result_info.append([file_match.group(1), num_total_hashtags, num_notfiltered_hashtags,
                                           num_more_than_10_hashtags, num_notfiltered_more_than_10_hashtags])
                        logging.debug(result_info)
        result_df = pd.DataFrame(result_info, columns=["Event", "Hashtag Count", "Hashtag After Filter Count",
                                                            "Hashtag Least 10", "Hashtag Least 10 After Filter"])
        result_ouput = os.path.join(output_path, "Auto_Filter_Result.csv")
        result_df.to_csv(result_ouput, mode='w', index=False)
        self.db_manager.disconnect()


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)
    # this db contains a table of hashtag that are already zero rated in the past
    db_name = 'irrelevant_hashtag.db'
    table_name = 'list'
    zero_hashtag_db_info = [db_name, table_name]

    daily_hashtags_lists = ["./hashtags_remove_candi/top_hashtag_rev.txt", "./hashtags_remove_candi/slangs1_rev.txt"]
    # path to the files that you want to filter
    input_path = "../hash_tag"
    # path to the files that you want to save results
    output_path = "../hashtag_rated/auto_filtered/"

    filtering = HashtagAutoFltr(daily_hashtags_lists, zero_hashtag_db_info)
    filtering.filter_out(input_path, output_path)
