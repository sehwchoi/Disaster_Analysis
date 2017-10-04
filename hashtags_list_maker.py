import logging, sys
import re
import pandas as pd
import requests
from bs4 import BeautifulSoup

class ListMaker(object):
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
        
if __name__ == '__main__':
    logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)
    list_maker = ListMaker()
    # creates a csv file with a list of top instagram hashtags
    # instgram hashtag data is from https://top-hashtags.com/instagram/101/
    list_maker.top_hashtag1('top_hashtag_raw.txt', 'top_hashtag.txt')
    # create a csv file with a list of internet slags
    # sources from view-source:https://en.wiktionary.org/wiki/Appendix:English_internet_slang
    list_maker.top_slangs1('slangs1_raw.txt', 'slangs1.txt')