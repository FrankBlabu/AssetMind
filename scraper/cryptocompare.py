#!/usr/bin/python3
#
# cryptocompare.py - Scraper for the Cryptocompare environment
#
# Frank Blankenburg, Jun. 2017
#

import argparse
import json
import pandas as pd
import sys
import time
import urllib

import api.cryptocompare

from database.database import Database
from database.database import CoinEntry

#--------------------------------------------------------------------------
# Scraper adding data extracted from Cryptocompare to the database
#
class CryptoCompareScraper:

    def __init__ (self):
        pass

    #
    # Scrape available information out of the GDAX API
    #
    def scrape (self, database, args):
        pass


    #
    # Print summary of the data retrievable via the API connection
    #
    def summary (self, args):

        client = api.cryptocompare.CryptoCompare ()

        coins = client.get_coin_list ()

        title = 'Coins'
        print (title)
        print (len (title) * '-')

        frame = pd.DataFrame (columns=['Id', 'Name', 'Price', 'Algorithm', 'Proof Type', 'Total supply', 'Pre mined'])

        prices = client.get_price (coins.keys ())

        print (prices)

        for key in sorted (coins.keys ()):
            entry = coins[key]
            frame.loc[len (frame)] = [key,
                                      entry['CoinName'],
                                      client.get_price (key),
                                      entry['Algorithm'],
                                      entry['ProofType'],
                                      entry['TotalCoinSupply'],
                                      'Yes' if entry['FullyPremined'] != '0' else 'No']

        print (frame.to_string ())



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
    parser.add_argument ('-b', '--begin',    required=False, type=to_date, help='Begin date (YYYY-MM-DD)')
    parser.add_argument ('-e', '--end',      required=False, type=to_date, help='End date (YYYY-MM-DD)')
    parser.add_argument ('-d', '--database', required=False, type=str, default=':memory:', help='Database file')
    parser.add_argument ('-v', '--verbose',  action='store_true', default=False, help='Verbose output')
    parser.add_argument ('-s', '--summary',  action='store_true', default=False, help='Print summary of available information')

    args = parser.parse_args ()

    database = Database (args.database)

    if args.database == ':memory:':
        database.create ()

    scraper = CryptoCompareScraper ()

    if args.summary:
        scraper.summary (args)
        sys.exit (0)

    scraper.scrape (database, args)
