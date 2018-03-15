
##Preprocessing data##

If you want to run auto filtering out hashtag program, run hashtag_auto_fltr.py

If you want to add hashtags that are already manually rated(relevant_tweets/[event_id]_hashtag_counts.csv) to the database file, run hashtag_list_maker.py

If you want to classify users and number of tweets for before, during, after disaster period, run twitter_period_clf.py


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


##Topic Modeling##

1. topic_modeler_1.py

This script runs a topic modeling over the tweets collected for the pre-disaster period and the post-disaster period. It runs a separate Latent Dirichlet Allocation based topic modeling for each of these periods and compares cosine similarities within and between the periods in order to analyze possible unique topics occurred in the post-disaster period.

To set which event to test, set ‘incident’ to the event number.
To set the event file, change ‘input_paths’.
To set the directory for the output files(the distribution result and cosine similarity matrix), change ‘output_path’. Current output directory is data/model1.
To load the saved model, set ‘override’ to true and load the proper file. You can also save a model after done with the training.

Run command: python topic_modeler_1.py

In addition to this file, topic_modeler_1_analysis.ipynb further analysis the cosine similarities, constituent words, and unique topics given a threshold. Please see inline comments for the details.


2. topic_modeler_2.py

This script runs a topic modeling over the whole corpus and then finds the average distribution of topics over documents each of the pre-disaster period and the post-disaster period. It then compares the difference over their distribution to find out the most changed topics from the pre period to the post period.


##Others##

1. Referenced two below information for disaster related terms

A. Olteanu, C. Castillo, F. Diaz, S. Vieweg. CrisisLex: A Lexicon for Collecting and Filtering Microblogged Communications in Crises. In Proceedings of the AAAI Conference on Weblogs and Social Media (ICWSM'14). AAAI Press, Ann Arbor, MI, USA, 2014

I. Temnikova, C. Castillo, and S. Vieweg: EMTerms 1.0: A Terminological Resource for Crisis Tweets. In Proceedings of the International Conference on Information Systems for Crisis Response and Management (ISCRAM'15). Kristiansand, Norway, 2015.


2. Referenced Files

2010_census.txt
-data is from https://people.sc.fsu.edu/~jburkardt/datasets/census/reps_2010.txt

