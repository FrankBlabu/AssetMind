#!/usr/bin/python3
#
# cryptocompare.py - Scraper for the Cryptocompare environment
#
# Frank Blankenburg, Jun. 2017
#

import api.cryptocompare

from core.common import Interval
from core.config import Configuration
from core.time import Timestamp
from database.database import Entry
from scraper.scraper import Scraper


#--------------------------------------------------------------------------
# Scraper adding data extracted from Cryptocompare to the database
#
class CryptoCompareScraper (Scraper):

    #selected_coins = sorted (['ETH', 'ETC', 'BTC', 'XMR', 'XRP', 'LTC', 'ZEC', 'DASH'])

    def __init__ (self):
        super ().__init__ ('CryptoCompare')

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

        assert isinstance (ids, list)
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
        for coin in ids:

            coin = coin.split ('::')[-1]

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

                    entries = []

                    for price in prices:
                        price_time = Timestamp (price['time'])
                        entries.append (Entry (timestamp=Timestamp (price_time), value=(price['high'] + price['low']) / 2))

                        if price_time < to:
                            to = price_time
                            ok = True

                    database.add (coin, entries)
                    to.advance (step=-Configuration.DATABASE_SAMPLING_STEP)

            except api.cryptocompare.HTTPError as e:
                add_to_log ('ERROR: {error}'.format (error=e.message))

        database.commit ()
