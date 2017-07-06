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
from database.database import Database
from scraper.scraper import Scraper
from scraper.scraper import Channel

#--------------------------------------------------------------------------
# Scraper adding data extracted from Cryptocompare to the database
#
class CryptoCompareScraper (Scraper):

    ID = 'CryptoCompare'

    def __init__ (self):
        super ().__init__ (CryptoCompareScraper.ID)

    #
    # Get all channels provided by the scraper
    #
    # @return List of channels
    #
    def get_channels (self):

        channels = []

        channels.append (Channel (id='{scraper}::ETH'.format (scraper=CryptoCompareScraper.ID),
                                  description='Ethereum course (CryptoCompare)', type=float, encrypted=False))
        channels.append (Channel (id='{scraper}::ETC'.format (scraper=CryptoCompareScraper.ID),
                                  description='Ethereum classic course (CryptoCompare)', type=float, encrypted=False))
        channels.append (Channel (id='{scraper}::BTC'.format (scraper=CryptoCompareScraper.ID),
                                  description='Bitcoin course (CryptoCompare)', type=float, encrypted=False))
        channels.append (Channel (id='{scraper}::XMR'.format (scraper=CryptoCompareScraper.ID),
                                  description='Monero course (CryptoCompare)', type=float, encrypted=False))
        channels.append (Channel (id='{scraper}::XRP'.format (scraper=CryptoCompareScraper.ID),
                                  description='Ripple course (CryptoCompare)', type=float, encrypted=False))
        channels.append (Channel (id='{scraper}::LTC'.format (scraper=CryptoCompareScraper.ID),
                                  description='Litecoin course (CryptoCompare)', type=float, encrypted=False))
        channels.append (Channel (id='{scraper}::ZEC'.format (scraper=CryptoCompareScraper.ID),
                                  description='ZCash course (CryptoCompare)', type=float, encrypted=False))
        channels.append (Channel (id='{scraper}::DASH'.format (scraper=CryptoCompareScraper.ID),
                                  description='Dash course (CryptoCompare)', type=float, encrypted=False))

        return channels

    #
    # Run scraper for acquiring a set of entries
    #
    # @param database Database to be filled
    # @param ids      List of ids to scrapr
    # @param start    Start timestamp (UTC)
    # @param end      End timestamp (UTC)
    # @param interval Interval of scraping
    # @param log      Callback for logging outputs
    #
    def run (self, database, ids, start, end, interval, log):

        assert isinstance (start, Timestamp)
        assert isinstance (end, Timestamp)
        assert isinstance (interval, Interval)

        def add_to_log (message):
            if log is not None:
                log (message)

        client = api.cryptocompare.CryptoCompare ()

        #
        # Iterate over each token id
        #
        for id in ids:
            add_to_log ('Scraping information for {id}'.format (id=id))

            #
            # We are scraping backwards in time because th CryptoCompare API will only
            # support a 'to timestamp' parameter.
            #
            try:
                to = end

                ok = True
                while ok and to >= start:

                    add_to_log ('Fetching information for {id} until {to}'.format (id=id, to=to))

                    prices = client.get_historical_prices (id=id, to=to, interval=interval)
                    ok = False

                    for price in prices:
                        price_time = Timestamp (price['time'])
                        database.add ('{scraper}::{token}'.format (self.id, id), (price['high'] + price['low']) / 2)

                        if price_time < to:
                            to = price_time
                            ok = True

                    to.advance (step=-Configuration.DATABASE_SAMPLING_STEP)

            except api.cryptocompare.HTTPError as e:
                add_to_log ('ERROR: {error}'.format (error=e.message))

        database.commit ()
