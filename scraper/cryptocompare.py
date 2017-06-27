#!/usr/bin/python3
#
# cryptocompare.py - Scraper for the Cryptocompare environment
#
# Frank Blankenburg, Jun. 2017
#

import argparse
import pandas as pd

import api.cryptocompare

from core.time import Timestamp
from database.database import Database
from database.database import CoinEntry
from scraper.scraper import Scraper

#--------------------------------------------------------------------------
# Scraper adding data extracted from Cryptocompare to the database
#
class CryptoCompareScraper (Scraper):

    selected_coins = sorted (['ETH', 'ETC', 'BTC', 'XMR', 'XRP', 'LTC', 'ZEC', 'DASH'])

    def __init__ (self):
        super ().__init__ ('CryptoCompare', CoinEntry, CryptoCompareScraper.selected_coins)

    #
    # Run scraper for acquiring a set of entries
    #
    # @param database Database to be filled
    # @param start    Start timestamp in UNIX epoch format or 'None' for maximum coverage
    # @param log      Callback for logging outputs
    #
    def run (self, database, start, log):
        pass

    #
    # Scrape available information out of the GDAX API
    #
    def scrape (self, database, args):

        client = api.cryptocompare.CryptoCompare ()

        for coin in CryptoCompareScraper.selected_coins:

            print (coin)

            prices = client.get_historical_prices (id=coin, interval=api.cryptocompare.CryptoCompare.Interval.DAY)

            for price in prices:
                database.add (CoinEntry (price['time'], coin, 'ccmp', (price['high'] + price['low']) / 2, 'usd'))

        database.commit ()

    #
    # Print summary of the data retrievable via the API connection
    #
    def summary (self, args):

        client = api.cryptocompare.CryptoCompare ()
        coins = client.get_coin_list ()

        title = 'Coins'
        print (title)
        print (len (title) * '-')

        frame = pd.DataFrame (columns=['Id', 'Name', 'Algorithm', 'Proof Type', 'Total supply', 'Pre mined'])

        for key in sorted (coins.keys ()):
            entry = coins[key]
            frame.loc[len (frame)] = [key.strip (),
                                      entry['CoinName'].strip (),
                                      entry['Algorithm'].strip (),
                                      entry['ProofType'].strip (),
                                      entry['TotalCoinSupply'].strip (),
                                      'Yes' if entry['FullyPremined'] != '0' else 'No']

        print (frame.to_string ())
        print ('\n')

        title = 'Selected coins'
        print (title)
        print (len (title) * '-')

        prices = client.get_price (CryptoCompareScraper.selected_coins)

        frame = pd.DataFrame (columns=['Id', 'EUR', 'USD', 'BTC', 'Average (USD)', 'Gradient (%)', 'Volumen'])

        for coin in CryptoCompareScraper.selected_coins:

            price = prices[coin]
            average = client.get_average_price (coin)
            trade = client.get_trading_info (coin)

            frame.loc[len (frame)] = [coin,
                                      price['EUR'],
                                      price['USD'],
                                      price['BTC'],
                                      average,
                                      trade['CHANGEPCT24HOUR'],
                                      trade['VOLUME24HOURTO']]

        print (frame.to_string ())



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
    parser.add_argument ('-v', '--verbose',  action='store_true', default=False, help='Verbose output')
    parser.add_argument ('-s', '--summary',  action='store_true', default=False, help='Print summary of available information')
    parser.add_argument ('database', type=str, default=':memory:', help='Database file')

    args = parser.parse_args ()

    database = Database (args.database)

    if args.database == ':memory:':
        database.create ()

    scraper = CryptoCompareScraper ()

    if args.summary:
        scraper.summary (args)
    else:
        scraper.scrape (database, args)
