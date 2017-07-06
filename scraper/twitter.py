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
from scraper.scraper import Scraper
from scraper.scraper import Channel


#----------------------------------------------------------------------------
# Scraper for twitter data
#
class TwitterScraper (Scraper):

    ID = 'twitter'
    APP_NAME = 'AssetMind'

    OAUTH_CHANNEL_ID = 'OAuth'
    CHANNELS = { 'ETH': ['ethereum'],
                 'BTC': ['bitcoin'] }

    def __init__ (self):

        super ().__init__ (TwitterScraper.ID)

        emoticons_str = r"""
            (?:
                [:=;] # Eyes
                [oO\-]? # Nose (optional)
                [D\)\]\(\]/\\OpP] # Mouth
            )"""

        regex_str = [
            emoticons_str,
            r'<[^>]+>',  # HTML tags
            r'(?:@[\w_]+)',  # @-mentions
            r"(?:\#+[\w_]+[\w\'_\-]*[\w_]+)",  # hash-tags
            r'http[s]?://(?:[a-z]|[0-9]|[$-_@.&amp;+]|[!*\(\),]|(?:%[0-9a-f][0-9a-f]))+',  # URLs
            r'(?:(?:\d+,?)+(?:\.?\d+)?)',  # numbers
            r"(?:[a-z][a-z'\-_]+[a-z])",  # words with - and '
            r'(?:[\w_]+)',  # other words
            r'(?:\S)'  # anything else
        ]

        self.tokens_regexp = re.compile (r'(' + '|'.join (regex_str) + ')', re.VERBOSE | re.IGNORECASE)
        self.emoticon_regexp = re.compile (r'^' + emoticons_str + '$', re.VERBOSE | re.IGNORECASE)


    #
    # Get all channels provided by the scraper
    #
    # @return List of channels
    #
    def get_channels (self):

        channels = []

        for channel in TwitterScraper.CHANNELS.keys ():
            channels.append (Channel (id=TwitterScraper.ID + '::' + channel,
                                      description='Twitter stream ({0})'.format (channel),
                                      type=str,
                                      encrypted=False))

        channels.append (Channel (id=TwitterScraper.ID + '::' + TwitterScraper.OAUTH_CHANNEL_ID,
                                  description='Twitter OAuth credentials', type=str, encrypted=True))

        return channels

    #
    # Perform authentification to get the necessary OAuth credentials
    #
    # This function must be performed once to write the neccesary parameters
    # into the database
    #
    def authenticate (self, database):

        assert database is not None
        assert isinstance (database.password, str)
        assert len (database.password) >= 4

        consumer_key = input ('Consumer key: ')
        consumer_secret = input ('Consumer secret: ')

        access_key, access_secret = twitter.oauth_dance (TwitterScraper.APP_NAME, consumer_key, consumer_secret)

        data = {}
        data['consumer_key'] = consumer_key
        data['consumer_secret'] = consumer_secret
        data['access_key'] = access_key
        data['access_secret'] = access_secret

        database.add (TwitterScraper.ID + '::' + TwitterScraper.OAUTH_CHANNEL_ID,
                      Entry (hash='0000', timestamp=Timestamp (), value=json.dumps (data)))

    #
    # Retrieve OAuth credentials from the database
    #
    # @param database Database containing the credentials
    #
    def get_credentials (self, database):

        assert database is not None
        assert database.password is not None

        entries = database.get (TwitterScraper.ID + '::' + TwitterScraper.OAUTH_CHANNEL_ID)
        assert len (entries) == 1

        return json.loads (entries[0].value)

    #
    # Run scraper for acquiring a set of entries
    #
    # @param database Database to be filled
    # @param ids      List of ids to scrape
    # @param start    Start timestamp (UTC)
    # @param end      End timestamp (UTC)
    # @param interval Interval of scraping
    # @param log      Callback for logging outputs
    #
    def run (self, database, ids, start, end, interval, log):
        credentials = self.get_credentials (database)

        server = twitter.Twitter (auth=twitter.OAuth (credentials['access_key'],
                                                      credentials['access_secret'],
                                                      credentials['consumer_key'],
                                                      credentials['consumer_secret']))

        for channel, tags in TwitterScraper.CHANNELS:

            query = server.search.tweets (q=' '.join (tags), count=100)
            entries = []

            for q in query['statuses']:

                tweet = self.to_string (q['text'])
                tweet = self.tokenize (tweet)
                tweet = [token if self.emoticon_regexp.search (token) else token.lower () for token in tweet]

                entries.append (Entry (timestamp=Timestamp (q['created_at']).timestamp ()), value=json.dumps (tweet))

            database.add (TwitterScraper.ID + '::' + channel, entries)

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
    # Tokenize a tweet content
    #
    def tokenize (self, text):

        terms = self.tokens_regexp.findall (text)

        stop = nltk.corpus.stopwords.words ('english') + list (string.punctuation) + ['rt', 'via']
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

        assert isinstance (args.password, str)
        assert len (args.password) >= 4

        database = Database (args.database, args.password)
        if args.database == ':memory:':
            database.create ()

        scraper = TwitterScraper ()

        if args.authenticate:
            scraper.authenticate (database)

        elif args.credentials:
            print (scraper.get_credentials (database))

        elif args.summary:
            scraper.summary (database)

        else:
            scraper.run (database, None, lambda message: print (message))
