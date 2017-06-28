#!/usr/bin/python3
#
# gdax.py - Scraper for the GDAX environment
#
# Frank Blankenburg, Jun. 2017
#
# For the GDAX API python wrapper see https://github.com/danpaquin/GDAX-Python
#

import argparse
import GDAX
import sys

from core.time import Timestamp
from scraper.scraper import Scraper
from database.database import Database
from database.database import CoinEntry

#--------------------------------------------------------------------------
# Scraper adding data extracted from GDAX to the database
#
class GDAXScraper (Scraper):

    #
    # Coin types handled here
    #
    selected_coins = ['ETH', 'BTC', 'LTC']

    def __init__ (self):
        super ().__init__ ('GDAX', CoinEntry.ID, GDAXScraper.selected_coins)

    #
    # Scrape available information out of the GDAX API
    #
    def scrape (self, database, args):

        client = GDAX.PublicClient ()

        start = args.start.to_string ('%Y-%m-%d') if args.start is not None else ''
        end   = args.end.to_string ('%Y-%m-%d')   if args.end   is not None else ''

        #
        # Each entry has the format (time, low, high, open, close, volume)
        # Time is in unix epoch format
        #
        entries = []

        for coin in selected_coins:
            for rate in client.getProductHistoricRates (product='{0}-USD'.format (coin), granularity=60 * 60 * 24, start=start, end=end):
                database.add (CoinEntry (rate[0], coin, 'gdax', (rate[1] + rate[2]) / 2, 'usd'))

        database.commit ()

        if args.verbose:

            frame = None
            for entry in entries:
                frame = entry.add_to_dataframe (frame)

            print ('Scraped coin courses:')
            print ('---------------------')

            print (frame)

    #
    # Print summary of the information available from the GDAX public API
    #
    def summary (self, args):

        client = GDAX.PublicClient ()

        #
        # Available products
        #
        title = 'Products'
        print (title)
        print (len (title) * '-')

        for product in client.getProducts ():
            print (product['display_name'])


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

    scraper = GDAXScraper ()

    if args.summary:
        scraper.summary (args)
        sys.exit (0)

    scraper.scrape (database, args)
