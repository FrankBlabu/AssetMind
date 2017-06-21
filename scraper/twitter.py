#/!usr/bin/python3
#
# twitter.py - Scraper for twitter data
#
# Frank Blankenburg, Jun. 2017
#

import argparse
import json
import twitter
import time

from database.database import Database
from database.database import EncryptedEntry

#----------------------------------------------------------------------------
# Scraper for twitter data
#
class TwitterScraper:

    APP_NAME = 'AssetMind'
    DATABASE_ID = 'twitter'

    def __init__ (self):
        pass

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
    # Get tweets matching keywords
    #
    def get_tweets (self, args):
        credentials = self.get_credentials (args)

        server = twitter.Twitter (auth=twitter.OAuth (credentials['access_key'],
                                                      credentials['access_secret'],
                                                      credentials['consumer_key'],
                                                      credentials['consumer_secret']))

        query = server.search.tweets (q='ethereum')

        print (query['statuses'])


    #
    # Retrieve OAuth credentials from the database
    #
    def get_credentials (self, args):

        assert args.database is not None
        assert args.database != ':memory:'
        assert isinstance (args.password, str)
        assert len (args.password) >= 4

        database = Database (args.database)

        entry = database.get_entries (EncryptedEntry.ID, TwitterScraper.DATABASE_ID)
        assert len (entry) == 1

        return json.loads (entry[0].get_text (args.password))


#----------------------------------------------------------------------------
# MAIN
#
if __name__ == '__main__':

        #
        # Parse command line arguments
        #
        parser = argparse.ArgumentParser ()

        parser.add_argument ('-a', '--authenticate', action='store_true', default=False, help='Create authentification credentials')
        parser.add_argument ('-c', '--credentials', action='store_true', default=False, help='List authentification credentials')
        parser.add_argument ('-p', '--password', type=str, default=None, help='Passwort for database encryption')
        parser.add_argument ('database', type=str, default=':memory:', help='Database file')

        args = parser.parse_args ()

        if args.authenticate:
            scraper = TwitterScraper ()
            scraper.authenticate (args)

        elif args.credentials:
            scraper = TwitterScraper ()
            print (scraper.get_credentials (args))

        else:
            scraper = TwitterScraper ()
            scraper.get_tweets (args)
