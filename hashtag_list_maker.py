import logging, sys
import re
import pandas as pd
import requests
import os
import sqlite3
from bs4 import BeautifulSoup


class ListMaker(object):
    def __init__(self, hashtag_db_info):
        # create DB manager and pass db file location and table name
        self.db_manager = HashtagDBManager(hashtag_db_info[0], hashtag_db_info[1])

    def top_hashtag1(self, file_name, output_name):
        pattern = re.compile(r"#(.+?)<")
        self.__create_list(pattern, file_name, output_name)

    # saves possible slangs into list
    def top_slangs1(self, file_name, output_name):
        pattern = re.compile(r"title=.+?\">(.+?)</a>")
        self.__create_list(pattern, file_name, output_name)
   
    # TODO: parse using Beautiful Soup
    def top_slangs2(self, file_name, output_nam):
        try:
            slangs_data = open("slangs1.html", 'r').text
        except:
            slangs_data = requests.get("https://en.wiktionary.org/wiki/Appendix:English_internet_slang").text
            f = open("slangs1.thml" , 'w')
            f.write(slangs_data)
            f.close

        soup = BeautifulSoup(slangs_data, 'html.parser')
        div = soup.find(id='mw-content-text')

    def __create_list(self, pattern, file_name, output_name):
        with open(file_name, 'r') as file:
            content = file.read()
            list = re.findall(pattern, content)
            logging.debug(list)
            df = pd.DataFrame(list, columns=['hashtags/slangs'])
            df.to_csv(output_name, index=False)

    def __lexicon_list_builder(self):
        lexicon_file1 = "../CrisisLexLexicon/CrisisLexRec.txt"
        lexicon_file2 = "../EMTerms-v1.0/EMTerms-1.0.csv"
        disaster_related_terms = []
        with open(lexicon_file1, 'r') as file:
            lexicon = file.read().split()
            disaster_related_terms.extend(lexicon)
            logging.debug(disaster_related_terms)

        with open(lexicon_file2, 'r') as file:
            em_terms = pd.read_csv(file)
            for index, row in em_terms.iterrows():
                term = row['Term']
                disaster_related_terms.extend(term.split())
                # logging.debug(disaster_related_terms)

        return disaster_related_terms

    def parse_irrelevant_from_rated_files(self, rated_path):
        #irrelevant_hashtags = []
        disaster_related_terms = self.__lexicon_list_builder()
        logging.debug("lexicon terms len: {}".format(len(disaster_related_terms)))
        total_len = 0
        total_zero_len = 0
        total_lexicon_match = 0;
        for root, dirs, files in os.walk(rated_path):
            for file in files:
                file_match = re.search('(\d+)_', file)
                if file_match:
                    with open(os.path.join(root, file), 'r') as file:
                        logging.debug("file name : {}".format(file))
                        try:
                            rated_pd = pd.read_csv(file)
                            # logging.debug(rated_pd)
                            # iterate states and find corresponding zones
                            total_len += len(rated_pd.index)
                            for index, row in rated_pd.iterrows():
                                rate = row['Relevance']
                                logging.debug("{} {} {}".format(row[0], row[1], row[2]))
                                if int(rate) is 0:
                                    total_zero_len += 1
                                    hashtag = row['Hashtag']
                                    if hashtag not in disaster_related_terms:
                                        # logging.debug('add hashtag to db {}'.format(hashtag))
                                        self.db_manager.insert_hashtag(hashtag)
                                    else:
                                        total_lexicon_match += 1
                                        logging.debug("term: {} is in lexicon list".format(hashtag))
                        except Exception as e:
                            logging.error("rated file : {} reading error : {}".format(file, e))
        logging.debug("Total hashtags: {}".format(total_len))
        logging.debug("Total zero hashtags: {}".format(total_zero_len))
        logging.debug("Total lexicon match: {}".format(total_lexicon_match))
        self.db_manager.disconnect()

"""TODO: saves to a separate file"""

class HashtagDBManager(object):

    def __init__(self, db_name, table_name):
        self.db_name = db_name
        self.table_name = table_name
        self.conn = None
        self.c = None
        self.connect()
        sql = 'CREATE TABLE IF NOT EXISTS ' + self.table_name + ' (' \
                                                                'id integer PRIMARY KEY,' \
                                                                'hashtag text,' \
                                                                'UNIQUE(hashtag))'
        self.c.execute(sql)
        self.conn.commit()

    def insert_hashtag(self, hashtag):
        sql = "INSERT OR IGNORE INTO " + self.table_name + "(hashtag) VALUES(?)"
        try:
            self.c.execute(sql, (hashtag,))
            logging.debug("insert id: {}".format(self.c.lastrowid))
        except sqlite3.OperationalError as msg:
            logging.debug("insert error {}".format(msg))
        self.conn.commit()

    def check_exist(self, hashtag):
        sql = "SELECT EXISTS(SELECT 1 FROM " + self.table_name + " WHERE hashtag=? Limit 1)"
        self.c.execute(sql, (hashtag,))
        exist, = self.c.fetchone()
        # logging.debug("check exist {}".format(exist))
        return exist

    def show_table(self):
        sql = "SELECT * FROM " + self.table_name
        result = self.c.execute(sql)
        print(result)

    def drop_table(self):
        sql = "DROP TABLE" + self.table_name
        self.c.execute(sql)

    def connect(self):
        self.conn = sqlite3.connect(self.db_name)
        self.c = self.conn.cursor()

    def disconnect(self):
        self.conn.close()



if __name__ == '__main__':
    logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)
    db_name = 'irrelevant_hashtag.db'
    table_name = 'list'
    zero_hashtag_db_info = [db_name, table_name]
    list_maker = ListMaker(zero_hashtag_db_info)
    # creates a csv file with a list of top instagram hashtags
    # instgram hashtag data is from https://top-hashtags.com/instagram/101/
    #list_maker.top_hashtag1('top_hashtag_raw.txt', 'top_hashtag.txt')
    # create a csv file with a list of internet slags
    # sources from view-source:https://en.wiktionary.org/wiki/Appendix:English_internet_slang
    #list_maker.top_slangs1('slangs1_raw.txt', 'slangs1.txt')
    rated_path = "../hashtag_rated/relevant_tweets"
    list_maker.parse_irrelevant_from_rated_files(rated_path)
