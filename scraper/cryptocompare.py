#!/usr/bin/python3
#
# cryptocompare.py - Scraper for the Cryptocompare environment
#
# Frank Blankenburg, Jun. 2017
#

import api.cryptocompare
import core
import pandas as pd

from core.common import Interval
from core.config import Configuration
from core.time import Timestamp
from database.database import Database
from database.database import Channel
from database.database import Entry
from scraper.scraper import Scraper
from scraper.scraper import ScraperRegistry

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
                                  description='Ethereum course (CryptoCompare)', type_id=float))
        channels.append (Channel (id='{scraper}::ETC'.format (scraper=CryptoCompareScraper.ID),
                                  description='Ethereum classic course (CryptoCompare)', type_id=float))
        channels.append (Channel (id='{scraper}::BTC'.format (scraper=CryptoCompareScraper.ID),
                                  description='Bitcoin course (CryptoCompare)', type_id=float))
        channels.append (Channel (id='{scraper}::XMR'.format (scraper=CryptoCompareScraper.ID),
                                  description='Monero course (CryptoCompare)', type_id=float))
        channels.append (Channel (id='{scraper}::XRP'.format (scraper=CryptoCompareScraper.ID),
                                  description='Ripple course (CryptoCompare)', type_id=float))
        channels.append (Channel (id='{scraper}::LTC'.format (scraper=CryptoCompareScraper.ID),
                                  description='Litecoin course (CryptoCompare)', type_id=float))
        channels.append (Channel (id='{scraper}::ZEC'.format (scraper=CryptoCompareScraper.ID),
                                  description='ZCash course (CryptoCompare)', type_id=float))
        channels.append (Channel (id='{scraper}::DASH'.format (scraper=CryptoCompareScraper.ID),
                                  description='Dash course (CryptoCompare)', type_id=float))

        return channels

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

        assert isinstance (start, Timestamp)
        assert isinstance (end, Timestamp)
        assert isinstance (interval, Interval)

        def add_to_log (message):
            if log is not None:
                log (message)

        client = api.cryptocompare.CryptoCompare ()

        #
        # Iterate over each channel
        #
        for channel in self.get_channels ():
            add_to_log ('Scraping information for {channel}'.format (channel=channel.id))

            #
            # We are scraping backwards in time because the CryptoCompare REST API will only
            # support a 'to timestamp' parameter.
            #
            try:
                to = end

                entries = []

                ok = True
                while ok and to >= start:

                    token = self.split_channel_id (channel.id).token
                    add_to_log ('Fetching information for {token} until {to}'.format (token=token, to=to))

                    prices = client.get_historical_prices (id=token, to=to, interval=interval)
                    ok = False

                    for price in prices:
                        price_time = Timestamp (price['time'])
                        price = (price['high'] + price['low']) / 2

                        #
                        # The REST API returns '0' for times where no information is available instead of
                        # raising an exception.
                        #
                        if price_time >= Timestamp (Configuration.DATABASE_START_DATE) and price > 0:
                            entries.append (Entry (timestamp=price_time, value=price))

                        if price_time < to:
                            to = price_time
                            ok = True

                    to.advance (step=-Configuration.DATABASE_SAMPLING_STEP)

                database.add (channel.id, entries)

            except api.cryptocompare.HTTPError as e:
                add_to_log ('ERROR: {error}'.format (error=e.message))


#--------------------------------------------------------------------------
# MAIN
#
if __name__ == '__main__':

    database = Database (':memory:')

    scraper = ScraperRegistry.get (CryptoCompareScraper.ID)
    scraper.run (database=database, start=Timestamp (Configuration.DATABASE_START_DATE), end=Timestamp.now (),
                 interval=Interval.day, log=lambda text: print (text))

    frame = pd.DataFrame (columns=['id', 'description', 'start', 'end', 'entries'])

    for channel in database.get_all_channels ():
        entries = database.get (channel.id)
        timestamps = [entry.timestamp for entry in entries]

        frame.loc[len (frame)] = [channel.id,
                                  channel.description,
                                  min (timestamps) if timestamps else '-',
                                  max (timestamps) if timestamps else '-',
                                  len (entries)]

    core.common.print_frame ('Scraped data', frame)
