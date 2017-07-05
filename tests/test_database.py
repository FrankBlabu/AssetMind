#!/usr/bin/python3
#
# test_database.py - Unittest
#
# Frank Blankenburg, Jun. 2017
#

import unittest

from core.common import Interval
from core.config import Configuration
from core.encryption import Encryption
from core.time import Timestamp

from database.database import Database
from database.database import Entry


#--------------------------------------------------------------------------
# CLASS TestDatabase
#
class TestDatabase (unittest.TestCase):

    #
    # Test various database read/write operations
    #
    def test_database_read_write (self):

        Configuration.DATABASE_SAMPLING_INTERVAL = Interval.hour

        #
        # Create database
        #
        database = Database (':memory:')
        database.create ()

        database.register ('ETH', 'Ethereum coin (CryptoCompare)', float)
        database.register ('BTC', 'Bitcoin (CryptoCompare)', float)
        database.register ('Twitter_ETH', 'Ethereum twitter news stream', str)

        #
        # Add some entries
        #
        eth_entries = []
        eth_entries.append (Entry (timestamp=Timestamp ('2017-04-21 12:00'), value=234.32))
        eth_entries.append (Entry (timestamp=Timestamp ('2017-04-21 14:00'), value=240.00))
        eth_entries.append (Entry (timestamp=Timestamp ('2017-04-21 16:00'), value=272.98))

        database.add ('ETH', eth_entries)

        btc_entries = []
        btc_entries.append (Entry (timestamp=Timestamp ('2017-04-22 13:00'), value=230.00))
        btc_entries.append (Entry (timestamp=Timestamp ('2017-04-22 15:00'), value=242.00))
        btc_entries.append (Entry (timestamp=Timestamp ('2017-04-22 17:00'), value=270.98))
        btc_entries.append (Entry (timestamp=Timestamp ('2017-04-22 19:00'), value=272.78))

        database.add ('BTC', btc_entries)

        entries = database.get_admin_data ()

        print (entries)

        self.assertEqual (len (entries), 3)
        self.assertEqual (entries[0].id, 'ETH')
        self.assertEqual (entries[2].id, 'Twitter_ETH')

        entries = database.get ('ETH')
        self.assertEqual (len (entries), 3)

        print (entries)

        entries = database.get ('BTC')
        self.assertEqual (len (entries), 4)


    #
    # Test if new entries with the same hash are overwritung existing database entries
    #
    def xxx_test_database_overwrite (self):

        #
        # Create database
        #
        database = Database (':memory:')
        database.create ()

        #
        # Setup some coin entries
        #
        coin_entries = []
        coin_entries.append (CoinEntry (Timestamp ('2017-06-18 12:00'), 'eth', 'coinbase', 230.0, 'eur'))
        coin_entries.append (CoinEntry (Timestamp ('2017-06-18 12:00'), 'btc', 'anycoind', 2200.12, 'eur'))
        coin_entries.append (CoinEntry (Timestamp ('2017-06-18 13:00'), 'eth', 'coinbase', 240.0, 'usd'))

        coin_entries.append (CoinEntry (Timestamp ('2017-06-18 13:00'), 'eth', 'coinbase', 242.0, 'usd'))

        for entry in coin_entries:
            database.add (entry)

        database.commit ()

        database_coin_entries = database.get_entries (CoinEntry.ID)
        self.assertEqual (len (database_coin_entries), 3)

        for entry in database_coin_entries:
            if entry.timestamp == Timestamp ('2017-06-18 13:00'):
                self.assertEqual (entry.course, 242.0)

    #
    # Test handling of encrypted database entries
    #
    def xxx_test_database_encryption (self):

        #
        # Create database
        #
        database = Database (':memory:', 'secret')
        database.create ()

        #
        # Setup some coin entries
        #
        encryption = Encryption ()
        password1 = encryption.generate_password ()
        password2 = encryption.generate_password ()

        self.assertNotEqual (password1, password2)

        entries = []

        text1 = "{'text': 'abc', 'id': 23}"
        entries.append (EncryptedEntry (1234, 'twitter', text1))

        text2 = "{'login': 'xyz123', 'auth': 42}"
        entries.append (EncryptedEntry (5678, 'facebook', text2))

        for entry in entries:
            database.add (entry)

        database.commit ()

        entries = database.get_entries (EncryptedEntry.ID, id='twitter')

        self.assertEqual (len (entries), 1)
        self.assertEqual (entries[0].text, text1)
        self.assertNotEqual (entries[0].text, text2)

        entries = database.get_entries (EncryptedEntry.ID, id='facebook')

        self.assertEqual (len (entries), 1)
        self.assertEqual (entries[0].text, text2)
        self.assertNotEqual (entries[0].text, text1)
