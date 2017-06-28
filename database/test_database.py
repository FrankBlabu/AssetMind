#!/usr/bin/python3
#
# test_database.py - Unittest
#
# Frank Blankenburg, Jun. 2017
#

import unittest

from database.database import CoinEntry
from database.database import CurrencyEntry
from database.database import StockEntry
from database.database import NewsEntry
from database.database import EncryptedEntry
from database.database import Database
from database.encryption import Encryption
from core.time import Timestamp


#--------------------------------------------------------------------------
# CLASS TestDatabase
#
class TestDatabase (unittest.TestCase):

    #
    # Test various database read/write operations
    #
    def test_database_read_write (self):

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

        for entry in coin_entries:
            database.add (entry)

        #
        # Setup some currency entries
        #
        currency_entries = []
        currency_entries.append (CurrencyEntry (Timestamp ('2017-06-18 12:00'), 'eur', 230.0))
        currency_entries.append (CurrencyEntry (Timestamp ('2017-06-18 13:00'), 'eur', 2200.12))
        currency_entries.append (CurrencyEntry (Timestamp ('2017-06-18 12:00'), 'usd', 240.0))

        for entry in currency_entries:
            database.add (entry)

        #
        # Setup some stock course entries
        #
        stock_entries = []
        stock_entries.append (StockEntry (Timestamp ('2017-06-18 12:00'), 'gdax',   230.0))
        stock_entries.append (StockEntry (Timestamp ('2017-06-18 12:00'), 'nasdaq', 2200.12))
        stock_entries.append (StockEntry (Timestamp ('2017-06-18 15:00'), 'gdax',   240.0))

        for entry in stock_entries:
            database.add (entry)

        #
        # Setup some news entries
        #
        news_entries = []
        news_entries.append (NewsEntry (Timestamp ('2017-06-18 12:00'), 'coindesk', 'Well, some went up, some went down.', 1, 2))
        news_entries.append (NewsEntry (Timestamp ('2017-06-18 12:00'), 'btcinfo',  'Ethereum is the future of something whatever.', 40, 5))
        news_entries.append (NewsEntry (Timestamp ('2017-06-18 12:00'), 'fb_eth',   'Hey, should I but, sell, or go to the lavatory ?', None, None))

        for entry in news_entries:
            database.add (entry)

        database.commit ()

        #
        # Check database content
        #
        database_coin_entries = database.get_entries (CoinEntry.ID)
        self.assertEqual (len (coin_entries), len (database_coin_entries))

        for a, b in zip (coin_entries, database_coin_entries):
            self.assertEqual (repr (a), repr (b))
            self.assertEqual (a.hash, b.hash)

        database_currency_entries = database.get_entries (CurrencyEntry.ID)
        self.assertEqual (len (currency_entries), len (database_currency_entries))

        for a, b in zip (currency_entries, database_currency_entries):
            self.assertEqual (repr (a), repr (b))
            self.assertEqual (a.hash, b.hash)

        database_stock_entries = database.get_entries (StockEntry.ID)
        self.assertEqual (len (stock_entries), len (database_stock_entries))

        for a, b in zip (stock_entries, database_stock_entries):
            self.assertEqual (repr (a), repr (b))
            self.assertEqual (a.hash, b.hash)

        database_news_entries = database.get_entries (NewsEntry.ID)
        self.assertEqual (len (news_entries), len (database_news_entries))

        for a, b in zip (news_entries, database_news_entries):
            self.assertEqual (repr (a), repr (b))
            self.assertEqual (a.hash, b.hash)

    #
    # Test if new entries with the same hash are overwritung existing database entries
    #
    def test_database_overwrite (self):

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
    def test_database_encryption (self):

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
