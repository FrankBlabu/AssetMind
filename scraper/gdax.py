#!/usr/bin/python3
#
# gdax.py - Scraper for the GDAX environment
#
# Frank Blankenburg, Jun. 2017
#

import argparse
import GDAX
import pandas as pd
import sys
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

    def execute (self, database, start=None, end=None):

        client = GDAX.PublicClient ()

        start = time.strftime ('%Y-%m-%d', start) if not start is None else ''
        end   = time.strftime ('%Y-%m-%d', end  ) if not end   is None else ''

        #
        # Each entry has the format (time, low, high, open, close, volume)
        # Time is in unix epoch format
        #
        for rate in client.getProductHistoricRates (product='ETH-USD', granularity=60*60*24, start=start, end=end):
            database.add (CoinCourseEntry (rate[0] + time.timezone, 'eth', 'gdax', (rate[1] + rate[2]) / 2, 'usd'))

        for rate in client.getProductHistoricRates (product='BTC-USD', granularity=60*60*24, start=start, end=end):
            database.add (CoinCourseEntry (rate[0] + time.timezone, 'btc', 'gdax', (rate[1] + rate[2]) / 2, 'usd'))

        for rate in client.getProductHistoricRates (product='LTC-USD', granularity=60*60*24, start=start, end=end):
            database.add (CoinCourseEntry (rate[0] + time.timezone, 'ltc', 'gdax', (rate[1] + rate[2]) / 2, 'usd'))

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
    parser.add_argument ('-s', '--startdate', required=False, type=to_date, help='Start date (YYYY-MM-DD)')
    parser.add_argument ('-e', '--enddate',   required=False, type=to_date, help='End date (YYYY-MM-DD)')
    parser.add_argument ('-d', '--database',  required=False, type=str, default=':memory:', help='Database file')

    args = parser.parse_args ()

    database = Database (args.database)
    database.create ()

    scraper = GDAXScraper ()
    scraper.execute (database, start=args.startdate, end=args.enddate)

    entries = database.get_coin_course_entries ()

    data = {'timestamp': [],
            'id': [],
            'source': [],
            'course': [],
            'currency': []}

    for entry in entries:
        data['timestamp'].append (pd.Timestamp (time.ctime (entry.timestamp)))
        data['id'].append (entry.id)
        data['source'].append (entry.source)
        data['course'].append (entry.course)
        data['currency'].append (entry.currency)

    frame = pd.DataFrame (data, columns=['timestamp', 'id', 'source', 'course', 'currency'])

    print (frame)
