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
import time
import unittest

from database.database import Database
from database.database import CoinCourseEntry

#--------------------------------------------------------------------------
# Scraper adding data extracted from GDAX to the database
#
class GDAXScraper:

    def __init__ (self):
        pass

    def execute (self, database, args):

        client = GDAX.PublicClient ()

        start = time.strftime ('%Y-%m-%d', args.start) if args.start is not None else ''
        end   = time.strftime ('%Y-%m-%d', args.end  ) if args.end   is not None else ''

        #
        # Each entry has the format (time, low, high, open, close, volume)
        # Time is in unix epoch format
        #
        entries = []

        for rate in client.getProductHistoricRates (product='ETH-USD', granularity=60 * 60 * 24, start=start, end=end):
            entries.append (CoinCourseEntry (rate[0] + time.timezone, 'eth', 'gdax', (rate[1] + rate[2]) / 2, 'usd'))

        for rate in client.getProductHistoricRates (product='BTC-USD', granularity=60 * 60 * 24, start=start, end=end):
            entries.append (CoinCourseEntry (rate[0] + time.timezone, 'btc', 'gdax', (rate[1] + rate[2]) / 2, 'usd'))

        for rate in client.getProductHistoricRates (product='LTC-USD', granularity=60 * 60 * 24, start=start, end=end):
            entries.append (CoinCourseEntry (rate[0] + time.timezone, 'ltc', 'gdax', (rate[1] + rate[2]) / 2, 'usd'))

        for entry in entries:
            database.add (entry)

        database.commit ()

        if args.verbose:

            frame = None
            for entry in entries:
                frame = entry.add_to_dataframe (frame)

            print ('Scraped coin courses:')
            print ('---------------------')

            print (frame)


#--------------------------------------------------------------------------
# Unittests
#
class TestGDAXScraper (unittest.TestCase):

    def test_scraper (self):
        pass


#--------------------------------------------------------------------------
# MAIN
#
if __name__ == '__main__':

    def to_date (s):
        try:
            return time.strptime (s, '%Y-%m-%d')
        except ValueError:
            raise argparse.ArgumentTypeError ('Not a valid date: {0}'.format (s))

    #
    # Parse command line arguments
    #
    parser = argparse.ArgumentParser ()
    parser.add_argument ('-s', '--start',    required=False, type=to_date, help='Start date (YYYY-MM-DD)')
    parser.add_argument ('-e', '--end',      required=False, type=to_date, help='End date (YYYY-MM-DD)')
    parser.add_argument ('-d', '--database', required=False, type=str, default=':memory:', help='Database file')
    parser.add_argument ('-v', '--verbose',  action='store_true', default=False, help='Verbose output')

    args = parser.parse_args ()

    database = Database (args.database)

    if args.database == ':memory:':
        database.create ()

    scraper = GDAXScraper ()
    scraper.execute (database, args)
