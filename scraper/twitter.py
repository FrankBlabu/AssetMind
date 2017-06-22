#/!usr/bin/python3
#
# twitter.py - Scraper for twitter data
#
# Frank Blankenburg, Jun. 2017
#
# Resources:
#
# * https://marcobonzanini.com/2015/03/02/mining-twitter-data-with-python-part-1/
#

import argparse
import codecs
import dateutil.parser
import nltk.corpus
import re
import json
import string
import twitter
import time

from database.database import Database
from database.database import EncryptedEntry
from database.database import NewsEntry
from scraper.scraper import Scraper


#----------------------------------------------------------------------------
# Scraper for twitter data
#
class TwitterScraper (Scraper):

    APP_NAME = 'AssetMind'
    DATABASE_ID = 'twitter'

    def __init__ (self):

        super ().__init__ ('Twitter')

        emoticons_str = r"""
            (?:
                [:=;] # Eyes
                [oO\-]? # Nose (optional)
                [D\)\]\(\]/\\OpP] # Mouth
            )"""

        regex_str = [
            emoticons_str,
            r'<[^>]+>', # HTML tags
            r'(?:@[\w_]+)', # @-mentions
            r"(?:\#+[\w_]+[\w\'_\-]*[\w_]+)", # hash-tags
            r'http[s]?://(?:[a-z]|[0-9]|[$-_@.&amp;+]|[!*\(\),]|(?:%[0-9a-f][0-9a-f]))+', # URLs

            r'(?:(?:\d+,?)+(?:\.?\d+)?)', # numbers
            r"(?:[a-z][a-z'\-_]+[a-z])", # words with - and '
            r'(?:[\w_]+)', # other words
            r'(?:\S)' # anything else
        ]

        self.tokens_regexp = re.compile (r'(' + '|'.join (regex_str) + ')', re.VERBOSE | re.IGNORECASE)
        self.emoticon_regexp = re.compile (r'^' + emoticons_str + '$', re.VERBOSE | re.IGNORECASE)


    #
    # Perform authentification to get the necessary OAuth credentials
    #
    # This function must be performed once to write the neccesary parameters
    # into the database
    #
    def authenticate (self, args):

        assert args.database is not None
        assert isinstance (args.password, str)
        assert len (args.password) >= 4

        database = Database (args.database)

        if args.database == ':memory:':
            database.create ()

        consumer_key = input ('Consumer key: ')
        consumer_secret = input ('Consumer secret: ')

        access_key, access_secret = twitter.oauth_dance (TwitterScraper.APP_NAME, consumer_key, consumer_secret)

        data = {}
        data['consumer_key'] = consumer_key
        data['consumer_secret'] = consumer_secret
        data['access_key'] = access_key
        data['access_secret'] = access_secret

        entry = EncryptedEntry (int (round (time.time ())), TwitterScraper.DATABASE_ID)
        entry.set_text (json.dumps (data), args.password)

        database.add (entry)
        database.commit ()

    #
    # Print feed summany
    #
    def summary (self, args):
        credentials = self.get_credentials (args)

        server = twitter.Twitter (auth=twitter.OAuth (credentials['access_key'],
                                                      credentials['access_secret'],
                                                      credentials['consumer_key'],
                                                      credentials['consumer_secret']))

        query = server.search.tweets (q='ethereum blockchain bitcoin', count=100)
        print (query['search_metadata'])

    #
    # Get tweets matching keywords
    #
    def get_tweets (self, args):
        credentials = self.get_credentials (args)

        server = twitter.Twitter (auth=twitter.OAuth (credentials['access_key'],
                                                      credentials['access_secret'],
                                                      credentials['consumer_key'],
                                                      credentials['consumer_secret']))

        query = server.search.tweets (q='ethereum blockchain bitcoin', count=100)

        tweets = []

        for q in query['statuses']:

            tweet = self.to_string (q['text'])
            tweet = self.tokenize (tweet)
            tweet = [token if self.emoticon_regexp.search (token) else token.lower () for token in tweet]

            tweets.append (NewsEntry (int (dateutil.parser.parse (q['created_at']).timestamp ()),
                                      'twitter', json.dumps (tweet), q['retweet_count'], q['favorite_count']))

        return tweets

    #
    # Retrieve OAuth credentials from the database
    #
    # @param database Database containing the credentials
    #
    def get_credentials (self, database):

        assert database is not None
        assert database.password is not None

        entry = database.get_entries (EncryptedEntry.ID, TwitterScraper.DATABASE_ID)
        assert len (entry) == 1

        return json.loads (entry[0].get_text (database.password))

    #
    # Run scraper for acquiring a set of entries
    #
    # @param database Database to be filled
    # @param password Password for accessing the protected database entries
    # @param start    Start timestamp in UNIX epoch format or 'None' for maximum coverage
    # @param end      End timestamp in UNIX epoch format  or 'None' for maximum coverage
    #
    def run (self, database, password, start, end):
        pass

    #
    # Tokenize a tweet content
    #
    def tokenize (self, text):

        terms = self.tokens_regexp.findall (text)

        stop = nltk.corpus.stopwords.words ('english') + list (string.punctuation)+ ['rt', 'via']
        terms = [term for term in terms if term not in stop and not term.startswith ('http:') and not term.startswith ('https:')]

        return terms


    #
    # Convert text into simple ASCII representation
    #
    def to_string (self, text):
        text = codecs.encode (text, encoding='charmap', errors='ignore')
        text = codecs.decode (text, encoding='charmap', errors='ignore')
        return text


#----------------------------------------------------------------------------
# MAIN
#


if __name__ == '__main__':

        #
        # Parse command line arguments
        #
        parser = argparse.ArgumentParser ()

        parser.add_argument ('-a', '--authenticate', action='store_true', default=False, help='Create authentification credentials')
        parser.add_argument ('-c', '--credentials', action='store_true', default=False, help='Show authentification credential set')
        parser.add_argument ('-s', '--summary', action='store_true', default=False, help='Tweet summary')
        parser.add_argument ('-p', '--password', type=str, required=True, help='Passwort for database encryption')
        parser.add_argument ('database', type=str, default=':memory:', help='Database file')

        args = parser.parse_args ()

        assert args.database is not None
        assert args.database != ':memory:'
        assert isinstance (args.password, str)
        assert len (args.password) >= 4

        database = Database (args.database, args.password)

        scraper = TwitterScraper ()

        if args.authenticate:
            scraper.authenticate (database)

        elif args.credentials:
            print (scraper.get_credentials (database))

        elif args.summary:
            scraper.summary (database)

        else:
            for tweet in scraper.get_tweets (database):
                print (tweet)
