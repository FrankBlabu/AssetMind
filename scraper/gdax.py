#!/usr/bin/python3
#
# gdax.py - Scraper for the GDAX environment
#
# Frank Blankenburg, Jun. 2017
#

import argparse
import GDAX
import pandas as pd
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

    def execute (self, database):
        client = GDAX.PublicClient ()

        #
        # Each entry has the format (time, low, high, open, close, volume)
        # Time is in unix epoch format
        #
        for rate in client.getProductHistoricRates (product='ETH-USD', granularity=60*60*24):
            database.add (CoinCourseEntry (rate[0] + time.timezone, 'eth', 'gdax', (rate[1] + rate[2]) / 2, 'usd'))

        for rate in client.getProductHistoricRates (product='BTC-USD', granularity=60*60*24):
            database.add (CoinCourseEntry (rate[0] + time.timezone, 'btc', 'gdax', (rate[1] + rate[2]) / 2, 'usd'))

        for rate in client.getProductHistoricRates (product='LTC-USD', granularity=60*60*24):
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

    #
    # Parse command line arguments
    #
    parser = argparse.ArgumentParser ()

    database = Database (':memory:')
    database.create ()

    scraper = GDAXScraper ()
    scraper.execute (database)

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
