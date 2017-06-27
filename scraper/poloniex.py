#!/usr/bin/python3
#
# poloniex.py - Scraper for the Poloniex environment
#
# Frank Blankenburg, Jun. 2017
#

import argparse
import pandas as pd
import sys

import api.poloniex

from core.time import Timestamp
from scraper.scraper import Scraper
from database.database import Database
from database.database import CoinEntry

#--------------------------------------------------------------------------
# Scraper adding data extracted from Poloniex to the database
#
class PoloniexScraper (Scraper):

    def __init__ (self, api_key=None, secret=None):

        super ().__init__ ('Poloniex', CoinEntry, [])

        self.api_key = api_key
        self.secret  = secret

    #
    # Scrape available information out of the GDAX API
    #
    def scrape (self, database, args):

        client = api.poloniex.Poloniex (self.api_key, self.secret)

        ret = client.get_order_book ('all')
        print (ret.keys ())



    #
    # Print summary of the data retrievable via the API connection
    #
    def summary (self, args):

        client = api.poloniex.Poloniex (self.api_key, self.secret)

        title = 'Currencies'
        print (title)
        print (len (title) * '-')

        frame = pd.DataFrame (columns=['id', 'name'])

        for key, entry in client.get_currencies ().items ():
            if not entry['disabled'] and not entry['delisted'] and not entry['frozen']:
                frame.loc[len (frame)] = [key, entry['name']]

        print (frame)


#--------------------------------------------------------------------------
# MAIN
#
if __name__ == '__main__':

    #
    # Parse command line arguments
    #
    parser = argparse.ArgumentParser ()
    parser.add_argument ('-b', '--begin',    required=False, type=Timestamp, help='Begin date (YYYY-MM-DD)')
    parser.add_argument ('-e', '--end',      required=False, type=Timestamp, help='End date (YYYY-MM-DD)')
    parser.add_argument ('-d', '--database', required=False, type=str, default=':memory:', help='Database file')
    parser.add_argument ('-v', '--verbose',  action='store_true', default=False, help='Verbose output')
    parser.add_argument ('-s', '--summary',  action='store_true', default=False, help='Print summary of available information')

    args = parser.parse_args ()

    database = Database (args.database)

    if args.database == ':memory:':
        database.create ()

    scraper = PoloniexScraper (None, None)

    if args.summary:
        scraper.summary (args)
        sys.exit (0)

    scraper.scrape (database, args)
