import sqlite3

class WordsDBManager(object):

    def __init__(self, db_name):
        self.db_name = db_name
        self.conn = None
        self.c = None
        self.conn, self.c = self.connect()
        # self.drop_table()
        sql = """CREATE TABLE IF NOT EXISTS words (
                    word VARCHAR(128),
                    day VARCHAR(128),
                    count INTEGER
                    )"""
        self.c.execute(sql)
        sql = """CREATE TABLE IF NOT EXISTS tweets (
                    id VARCHAR(128) PRIMARY KEY UNIQUE
                    )"""
        self.c.execute(sql)
        self.conn.commit()

    def insert_word(self, data_dict):
        print("Insert word: {}".format(data_dict))
        sql = "INSERT INTO words(word, day, count) VALUES(?, ?, ?)"
        self.c.execute(sql, tuple(data_dict.values()))
        # self.conn.commit()

    def check_exist_word(self, word, day):
        sql = "SELECT COUNT(*) FROM words WHERE word=? and day=?"
        self.c.execute(sql, (word, day))
        result = self.c.fetchall()
        count = result[0][0]
        print("word check count {}".format(count))
        return count

    def update_count(self, word, day):
        print("Update exist word: {}".format(word))
        sql = "UPDATE words SET count = count + 1 WHERE word=? and day=?"
        self.c.execute(sql, (word, day))
        # self.conn.commit()

    def commit_db(self):
        self.conn.commit()

    def insert_tweet(self, tweet_id):
        print("Insert tweet_id: {}".format(tweet_id))
        sql = """INSERT INTO tweets(id) VALUES(?)"""

        self.c.execute(sql, (tweet_id,))
        self.conn.commit()

    def check_exist_tweet(self, tweet_id):
        tweet_id = str(tweet_id)
        sql = "SELECT COUNT(*) FROM tweets WHERE id=?"
        self.c.execute(sql, (tweet_id,))
        result = self.c.fetchall()
        count = result[0][0]
        print("tweet check count {}".format(count))
        return count

    def show_table(self):
        sql = "SELECT * FROM words"
        result = self.c.execute(sql)
        # print(result)

    def drop_table(self):
        sql = "DROP TABLE IF EXISTS words"
        self.c.execute(sql)
        sql = "DROP TABLE IF EXISTS tweets"
        self.c.execute(sql)

    def connect(self):
        db_connection = sqlite3.connect(self.db_name)
        cursor = db_connection.cursor()
        return db_connection, cursor

    def disconnect(self):
        self.conn.close()
