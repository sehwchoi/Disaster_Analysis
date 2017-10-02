import logging, sys
import re
import pandas as pd

class ListMaker(object):
    def top_hashtag1(self, file_name, output_name):
        with open(file_name, 'r') as file:
            content = file.read()
            pattern = re.compile(r"#(.+?)<")
            list = re.findall(pattern, content)
            logging.debug(list)
            df = pd.DataFrame(list, columns=['top_hashtags'])
            df.to_csv(output_name, index=False)
                
        
if __name__ == '__main__':
    logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)
    list_maker = ListMaker()
    # data is from https://top-hashtags.com/instagram/101/
    list_maker.top_hashtag1('top_hashtag_raw.txt', 'top_hashtag.txt')