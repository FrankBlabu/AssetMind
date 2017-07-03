#!/usr/bin/python3
#
# cryptocompare.py - Scraper for the Cryptocompare environment
#
# Frank Blankenburg, Jun. 2017
#

import argparse
import pandas as pd

import api.cryptocompare

from core.common import Interval
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
        super ().__init__ ('CryptoCompare', CoinEntry.ID, CryptoCompareScraper.selected_coins)

    #
    # Run scraper for acquiring a set of entries
    #
    # @param database Database to be filled
    # @param start    Start timestamp (UTC)
    # @param end      End timestamp (UTC)
    # @param interval Interval of scraping
    # @param log      Callback for logging outputs
    #
    def run (self, database, start, end, interval, log):

        print ('Run, from={start}, to={end}, interval={interval}'.format (start=start, end=end, interval=interval.name))

        assert isinstance (start, Timestamp)
        assert isinstance (end, Timestamp)
        assert isinstance (interval, Interval)

        def add_to_log (message):
            if log is not None:
                log (message)

        client = api.cryptocompare.CryptoCompare ()

        #
        # Iterate over each coin and try to gather the required information
        #
        for coin in CryptoCompareScraper.selected_coins:

            add_to_log ('Scraping information for {coin}'.format (coin=coin))

            #
            # We are scraping backwards in time because th CryptoCompare API will only
            # support a 'to timestamp' parameter.
            #
            try:
                to = end

                ok = True
                while ok and to >= start:

                    add_to_log ('Fetching information for {coin} until {to}'.format (coin=coin, to=to))

                    prices = client.get_historical_prices (id=coin, to=to, interval=interval)
                    ok = False

                    for price in prices:
                        price_time = Timestamp (price['time'])
                        database.add (CoinEntry (price_time, coin, 'ccmp', (price['high'] + price['low']) / 2, 'usd'))

                        if price_time < to:
                            to = price_time
                            ok = True

                    to.advance (step=-Configuration.DATABASE_SAMPLING_STEP)

            except api.cryptocompare.HTTPError as e:
                add_to_log ('ERROR: {error}'.format (error=e.message))

    #
    # Scrape available information out of the GDAX API
    #
    def scrape (self, database, args):

        client = api.cryptocompare.CryptoCompare ()

        for coin in CryptoCompareScraper.selected_coins:

            print (coin)

            prices = client.get_historical_prices (id=coin, interval=Configuration.DATABASE_SAMPLING_INTERVAL)

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
