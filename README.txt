
If you want to run auto filtering out hashtag program, run hashtag_auto_fltr.py

If you want to add hashtags that are already manually rated(relevant_tweets/[event_id]_hashtag_counts.csv) to the database file, run hashtag_list_maker.py

If you want to classify users and number of tweets for before, during, after disaster period, run twitter_period_clf.py (On-going)


Run Files:

1. hashtag_auto_fltr.py

This file read files that has a list of hashtag and marks as -1 if a condition matches with below.

Condition 1. a hashtag is in top common 400 hashtags but not related to disaster or location. This top hashtags are read from hashtags_remove_candi/top_hashtag_rev.txt

Condition 2. a hashtag is in slangs list but not related to disaster or location. This slangs are read from hashtags_remove_candi/slangs1_rev.txt

Condition 3. a hashtag is one of word in “irrelevant_hashtag_db”. This db has a table of hashtags that are manually marked 0 in the previous ratings.

NOTE!!
change codes below to assign input and output folders 

    input_path = "../hash_tag_test"
    output_path = "../hashtag_rated/auto_filtered/"

The result will be saved as  “../hashtag_rated/auto_filtered/[file_name]_filtered.csv”


2. hashtag_list_maker.py

This class has three jobs as below.

1. Save top 400 hashtags into top_hashtag_rev.txt. Data is crawled from https://top-hashtags.com/instagram/101/

2. Saves common internet slangs into slangs1_rev.txt. Data is crawled from https://en.wiktionary.org/wiki/Appendix:English_internet_slang

3. Parse relevant tweets files(files that are already rated before) and saves hashtag that are rated zero because of no relationship to the event to the database 'irrelevant_hashtag.db' if the hashtag is not one of the lexicons terms.

To change database file, change below,
    db_name = 'irrelevant_hashtag.db' 

To change a path of reading files, change below,
    rated_path = "../hashtag_rated/relevant_tweets"


Referenced two below information for disaster related terms

A. Olteanu, C. Castillo, F. Diaz, S. Vieweg. CrisisLex: A Lexicon for Collecting and Filtering Microblogged Communications in Crises. In Proceedings of the AAAI Conference on Weblogs and Social Media (ICWSM'14). AAAI Press, Ann Arbor, MI, USA, 2014

I. Temnikova, C. Castillo, and S. Vieweg: EMTerms 1.0: A Terminological Resource for Crisis Tweets. In Proceedings of the International Conference on Information Systems for Crisis Response and Management (ISCRAM'15). Kristiansand, Norway, 2015.


Others

2010_census.txt
-data is from https://people.sc.fsu.edu/~jburkardt/datasets/census/reps_2010.txt
