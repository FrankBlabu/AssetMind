#!/usr/bin/python3
#
# test_database.py - Unittest
#
# Frank Blankenburg, Jun. 2017
#

import scraper.init
import unittest

from core.common import Interval
from core.config import Configuration
from core.encryption import Encryption
from core.time import Timestamp
from scraper.scraper import Scraper
from scraper.scraper import ScraperRegistry

from database.database import Database
from database.database import Entry
from database.database import Channel


#--------------------------------------------------------------------------
# CLASS TestDatabaseScraper
#
class TestDatabaseScraper (Scraper):

    ID = 'Test'

    def __init__ (self):
        super ().__init__ (TestDatabaseScraper.ID)

    #
    # Get all channels provided by the scraper
    #
    # @return List of channels
    #
    def get_channels (self):

        channels = []

        channels.append (Channel (id='{scraper}::ETH'.format (scraper=TestDatabaseScraper.ID),
                                  description='Ethereum course', type_id=float, encrypted=False))
        channels.append (Channel (id='{scraper}::BTC'.format (scraper=TestDatabaseScraper.ID),
                                  description='Bitcoin course', type_id=float, encrypted=False))
        channels.append (Channel (id='{scraper}::Twitter::ETH'.format (scraper=TestDatabaseScraper.ID),
                                  description='Twitter channel', type_id=str, encrypted=True))

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
        pass


#--------------------------------------------------------------------------
# CLASS TestDatabase
#
class TestDatabase (unittest.TestCase):

    Configuration.DATABASE_SAMPLING_INTERVAL = Interval.hour
    ScraperRegistry.register (TestDatabaseScraper ())

    #
    # Test various database read/write operations
    #
    def test_database_read_write (self):

        #
        # Create database
        #
        database = Database (':memory:')

        #
        # Add some entries
        #
        eth_entries = []
        eth_entries.append (Entry (timestamp=Timestamp ('2017-04-21 12:00'), value=234.32))
        eth_entries.append (Entry (timestamp=Timestamp ('2017-04-21 14:00'), value=240.00))
        eth_entries.append (Entry (timestamp=Timestamp ('2017-04-21 16:00'), value=272.98))

        database.add ('Test::ETH', eth_entries)

        btc_entries = []
        btc_entries.append (Entry (timestamp=Timestamp ('2017-04-22 13:00'), value=230.00))
        btc_entries.append (Entry (timestamp=Timestamp ('2017-04-22 15:00'), value=242.00))
        btc_entries.append (Entry (timestamp=Timestamp ('2017-04-22 17:00'), value=270.98))
        btc_entries.append (Entry (timestamp=Timestamp ('2017-04-22 19:00'), value=272.78))

        database.add ('Test::BTC', btc_entries)

        entries = database.get_all_channels ()

        self.assertEqual (len (entries), 3)
        self.assertEqual (entries[0].id, 'Test::ETH')
        self.assertEqual (entries[2].id, 'Test::Twitter::ETH')

        entries = database.get ('Test::ETH')
        self.assertEqual (len (entries), 3)

        entries = database.get ('Test::BTC')
        self.assertEqual (len (entries), 4)


    #
    # Test if new entries with the same hash are overwritung existing database entries
    #
    def test_database_overwrite (self):

        #
        # Create database
        #
        database = Database (':memory:')

        #
        # Setup some coin entries
        #
        entries = []
        entries.append (Entry (timestamp=Timestamp ('2017-06-18 12:00'), value=230.0))
        entries.append (Entry (timestamp=Timestamp ('2017-06-18 15:00'), value=2200.12))
        entries.append (Entry (timestamp=Timestamp ('2017-06-18 21:00'), value=240.0))

        entries.append (Entry (timestamp=Timestamp ('2017-06-18 15:00'), value=242.0))

        database.add ('Test::ETH', entries)

        entries = database.get ('Test::ETH')
        self.assertEqual (len (entries), 3)

        for entry in entries:
            if entry.timestamp == Timestamp ('2017-06-18 15:00'):
                self.assertEqual (entry.value, 242.0)

    #
    # Test handling of encrypted database entries
    #
    def test_database_encryption (self):

        #
        # Create database
        #
        database = Database (':memory:', 'secret')

        #
        # Automatic password generation
        #
        encryption = Encryption ()
        password1 = encryption.generate_password ()
        password2 = encryption.generate_password ()

        self.assertNotEqual (password1, password2)

        entries = []

        text1 = "{'text': 'abc', 'id': 23}"
        entries.append (Entry (timestamp=Timestamp ('2017-08-12 11:00'), value=text1))

        text2 = "{'login': 'xyz123', 'auth': 42}"
        entries.append (Entry (timestamp=Timestamp ('2017-08-14 16:00'), value=text2))

        database.add ('Test::Twitter::ETH', entries)

        entries = database.get ('Test::Twitter::ETH')

        self.assertEqual (len (entries), 2)
        self.assertEqual (entries[0].value, text1)
        self.assertEqual (entries[1].value, text2)
