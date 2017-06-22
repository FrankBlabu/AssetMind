#!/usr/bin/python3
#
# database.py - Database access
#
# Frank Blankenburg, Jun. 2017
#

import argparse
import hashlib
import os.path
import pandas as pd
import sqlite3
import time
import unittest

from database.encryption import Encryption

#--------------------------------------------------------------------------
# Base class for database entries
#
# This class cares for computing a unique hash value for each entry which
# is used to determine if this special entry is already present in the
# database. This way the same source of information can be read and inserted
# multiple times without leading to duplicate database entries.
#
class Entry:

    #
    # Constructor
    #
    # Computes the hash value for the entry
    #
    def __init__ (self, timestamp, id, source):

        assert isinstance (timestamp, int)
        assert isinstance (id, str)
        assert isinstance (source, str) or source is None
        assert '-' not in id
        assert source is None or '-' not in source

        if source is not None:
            content = '{0}-{1}-{2}'.format (timestamp, id, source)
        else:
            content = '{0}-{1}'.format (timestamp, id)

        h = hashlib.sha256 ()
        h.update (bytes (content, 'utf-8'))

        self.hash = h.hexdigest ()
        self.timestamp = timestamp
        self.id = id
        self.source = source


#--------------------------------------------------------------------------
# Container representing a single course entry for a single coin
#
class CoinEntry (Entry):

    ID = 'coin'

    #
    # Initialize entry
    #
    # @param timestamp Event timestamp in UTC unix epoch seconds
    #
    def __init__ (self, timestamp, id, source, course, currency):

        super ().__init__ (timestamp, id, source)

        assert len (id) <= 8
        assert len (source) <= 8
        assert isinstance (course, float)
        assert isinstance (currency, str)
        assert len (currency) == 3

        self.course    = course
        self.currency  = currency

    #
    # Add matching table to database
    #
    @staticmethod
    def add_table_to_database (cursor):

        #
        # Coin courses table
        #
        command = 'CREATE TABLE {0} ('.format (CoinEntry.ID)
        command += 'hash VARCHAR (64), '
        command += 'timestamp LONG NOT NULL, '
        command += 'id VARCHAR (8), '
        command += 'source VARCHAR (8), '
        command += 'course REAL, '
        command += 'currency VARCHAR (3)'
        command += ')'

        cursor.execute (command)

    #
    # Insert this entry into a database
    #
    def insert_into_database (self, cursor, password):
        command = 'INSERT INTO {0} '.format (CoinEntry.ID)
        command += '(hash, timestamp, id, source, course, currency) '
        command += 'values (?, ?, ?, ?, ?, ?)'

        params = []
        params.append (self.hash)
        params.append (self.timestamp)
        params.append (self.id)
        params.append (self.source)
        params.append (self.course)
        params.append (self.currency)

        cursor.execute (command, params)

    #
    # Add entry to dataframe
    #
    # @param frame Data frame to add entry to or 'None' if an appropriate frame should be created
    #
    def add_to_dataframe (self, frame):
        if frame is None:
            frame = pd.DataFrame (columns=['timestamp', 'id', 'source', 'course', 'currency'])

        frame.loc[len (frame)] = [pd.Timestamp (time.ctime (self.timestamp)), self.id, self.source, self.course, self.currency]

        return frame

    def __repr__ (self):
        text = 'CoinEntry ('
        text += 'timestamp={0}, '.format (self.timestamp)
        text += 'id={0}, '.format (self.id)
        text += 'source={0}, '.format (self.source)
        text += 'course={0}, '.format (self.course)
        text += 'currency={0}'.format (self.currency)
        text += ')'

        return text

#--------------------------------------------------------------------------
# Container representing a single course entry for a single currency
#
class CurrencyEntry (Entry):

    ID = 'currency'

    #
    # Initialize entry
    #
    # @param timestamp Event timestamp in UTC unix epoch seconds
    #
    def __init__ (self, timestamp, id, course):

        super ().__init__ (timestamp, id, None)

        assert len (id) == 3
        assert isinstance (course, float)

        self.course    = course

    #
    # Add matching table to database
    #
    @staticmethod
    def add_table_to_database (cursor):

        command = 'CREATE TABLE {0} ('.format (CurrencyEntry.ID)
        command += 'hash VARCHAR (64), '
        command += 'timestamp LONG NOT NULL, '
        command += 'id VARCHAR (3), '
        command += 'course REAL'
        command += ')'

        cursor.execute (command)

    #
    # Insert this entry into a database
    #
    def insert_into_database (self, cursor, password):
        command = 'INSERT INTO {0} '.format (CurrencyEntry.ID)
        command += '(hash, timestamp, id, course) '
        command += 'values (?, ?, ?, ?)'

        params = []
        params.append (self.hash)
        params.append (self.timestamp)
        params.append (self.id)
        params.append (self.course)

        cursor.execute (command, params)

    #
    # Add entry to dataframe
    #
    # @param frame Data frame to add entry to or 'None' if an appropriate frame should be created
    #
    def add_to_dataframe (self, frame):
        if frame is None:
            frame = pd.DataFrame (columns=['timestamp', 'id', 'course'])

        frame.loc[len (frame)] = [pd.Timestamp (time.ctime (self.timestamp)), self.id, self.course]

        return frame

    def __repr__ (self):
        text = 'CurrencyEntry ('
        text += 'timestamp={0}, '.format (self.timestamp)
        text += 'id={0}, '.format (self.id)
        text += 'course={0}'.format (self.course)
        text += ')'

        return text


#--------------------------------------------------------------------------
# Container representing a single course entry for a single stock or index
#
class StockEntry (Entry):

    ID = 'stock'

    #
    # Initialize entry
    #
    # @param timestamp Event timestamp in UTC unix epoch seconds
    #
    def __init__ (self, timestamp, id, course):

        super ().__init__ (timestamp, id, None)

        assert isinstance (course, float)

        self.course    = course

    #
    # Add matching table to database
    #
    @staticmethod
    def add_table_to_database (cursor):

        command = 'CREATE TABLE {0} ('.format (StockEntry.ID)
        command += 'hash VARCHAR (64), '
        command += 'timestamp LONG NOT NULL, '
        command += 'id VARCHAR (8), '
        command += 'course REAL'
        command += ')'

        cursor.execute (command)

    #
    # Insert this entry into a database
    #
    def insert_into_database (self, cursor, password):
        command = 'INSERT INTO {0} '.format (StockEntry.ID)
        command += '(hash, timestamp, id, course) '
        command += 'values (?, ?, ?, ?)'

        params = []
        params.append (self.hash)
        params.append (self.timestamp)
        params.append (self.id)
        params.append (self.course)

        cursor.execute (command, params)

    #
    # Add entry to dataframe
    #
    # @param frame Data frame to add entry to or 'None' if an appropriate frame should be created
    #
    def add_to_dataframe (self, frame):
        if frame is None:
            frame = pd.DataFrame (columns=['timestamp', 'id', 'course'])

        frame.loc[len (frame)] = [pd.Timestamp (time.ctime (self.timestamp)), self.id, self.course]

        return frame

    def __repr__ (self):
        text = 'StockEntry ('
        text += 'timestamp={0}, '.format (self.timestamp)
        text += 'id={0}, '.format (self.id)
        text += 'course={0}'.format (self.course)
        text += ')'

        return text


#--------------------------------------------------------------------------
# Container representing a single event entry for a single news source
#
class NewsEntry (Entry):

    ID = 'news'

    #
    # Initialize entry
    #
    # @param timestamp Event timestamp in UTC unix epoch seconds
    #
    def __init__ (self, timestamp, id, text, shares, likes):

        super ().__init__ (timestamp, id, None)

        assert len (id) <= 8
        assert isinstance (text, str)
        assert len (text) < (1 << 16)
        assert isinstance (shares, int) or shares is None
        assert isinstance (likes, int) or lines is None

        self.text = text
        self.shares = shares if shares is None or shares >= 0 else None
        self.likes = likes if likes is None or likes >= 0 else None

    #
    # Add matching table to database
    #
    @staticmethod
    def add_table_to_database (cursor):
        command = 'CREATE TABLE {0} ('.format (NewsEntry.ID)
        command += 'hash VARCHAR (64), '
        command += 'timestamp LONG NOT NULL, '
        command += 'id VARCHAR (8), '
        command += 'text MEMO, '
        command += 'shares INT, '
        command += 'likes INT'
        command += ')'

        cursor.execute (command)

    #
    # Insert this entry into a database
    #
    def insert_into_database (self, cursor, password):
        command = 'INSERT INTO {0} '.format (NewsEntry.ID)
        command += '(hash, timestamp, id, text, shares, likes) '
        command += 'values (?, ?, ?, ?, ?, ?)'

        params = []
        params.append (self.hash)
        params.append (self.timestamp)
        params.append (self.id)
        params.append (self.text)
        params.append (self.shares if shares is not None else -1)
        params.append (self.likes if shares is not None else -1)

        cursor.execute (command, params)

    #
    # Add entry to dataframe
    #
    # @param frame Data frame to add entry to or 'None' if an appropriate frame should be created
    #
    def add_to_dataframe (self, frame):
        if frame is None:
            frame = pd.DataFrame (columns=['timestamp', 'id', 'text', 'shares', 'likes'])

        shares = self.shares if self.shares is not None else '-'
        likes = self.likes if self.likes is not None else '-'

        frame.loc[len (frame)] = [pd.Timestamp (time.ctime (self.timestamp)), self.id, self.text, shares, likes]

        return frame

    def __repr__ (self):

        content = self.text
        if len (content) > 64:
            content = content[:64 - 3] + '...'

        text = 'NewsEntry ('
        text += 'timestamp={0}, '.format (self.timestamp)
        text += 'id={0}, '.format (self.id)
        text += 'text=\'{0}\', '.format (content)
        text += 'shares={0}, '.format (self.shares)
        text += 'likes={0}'.format (self.likes)
        text += ')'

        return text

#--------------------------------------------------------------------------
# Container representing an arbitrary encrypted text entry
#
class EncryptedEntry (Entry):

    ID = 'encrypted'

    #
    # Initialize entry
    #
    # @param timestamp Event timestamp in UTC unix epoch seconds
    # @param id        Id for this entry
    # @param text      Unencrypted text to keep
    #
    def __init__ (self, timestamp, id, text):

        super ().__init__ (timestamp, id, None)

        assert len (id) <= 64
        assert isinstance (text, str)

        self.text = text

    #
    # Add matching table to database
    #
    @staticmethod
    def add_table_to_database (cursor):

        command = 'CREATE TABLE {0} ('.format (EncryptedEntry.ID)
        command += 'hash VARCHAR (64), '
        command += 'timestamp LONG NOT NULL, '
        command += 'id VARCHAR (64), '
        command += 'text MEMO'
        command += ')'

        cursor.execute (command)

    #
    # Insert this entry into a database
    #
    def insert_into_database (self, cursor, password):
        command = 'INSERT INTO {0} '.format (EncryptedEntry.ID)
        command += '(hash, timestamp, id, text) '
        command += 'values (?, ?, ?, ?)'

        params = []
        params.append (self.hash)
        params.append (self.timestamp)
        params.append (self.id)
        params.append (self.text)

        cursor.execute (command, params)

    #
    # Add entry to dataframe
    #
    # @param frame Data frame to add entry to or 'None' if an appropriate frame should be created
    #
    def add_to_dataframe (self, frame):
        if frame is None:
            frame = pd.DataFrame (columns=['timestamp', 'id', 'text'])

        frame.loc[len (frame)] = [pd.Timestamp (time.ctime (self.timestamp)), self.id, self.text]

        return frame

    def __repr__ (self):
        t = self.text

        if len (t) > 16:
            t = t[:16] + '...'

        text = 'CurrencyEntry ('
        text += 'timestamp={0}, '.format (self.timestamp)
        text += 'id={0}, '.format (self.id)
        text += 'text={0}'.format (t)
        text += ')'

        return text


#--------------------------------------------------------------------------
# Database handler
#
class Database:

    #
    # Constructor
    #
    # @param file     Location of the database in the file system
    # @param password Password to access encrypted database entries
    #
    def __init__ (self, file, password=None):
        self.file = file
        self.password = password
        self.connection = sqlite3.connect (file)
        self.cursor = self.connection.cursor ()

    #
    # Create database structure
    #
    def create (self):

        CoinEntry.add_table_to_database (self.cursor)
        CurrencyEntry.add_table_to_database (self.cursor)
        StockEntry.add_table_to_database (self.cursor)
        NewsEntry.add_table_to_database (self.cursor)
        EncryptedEntry.add_table_to_database (self.cursor)

    #
    # Add entry to the database
    #
    # If an entry with the same hash is already existing in the database, it will
    # be replaced by the new entry. So it is assumed that data added later is
    # 'more correct' or generally of a higher quality.
    #
    # @param entry Entry to be added
    #
    def add (self, entry):

        command = 'DELETE FROM {table} WHERE hash="{hash}"'.format (table=entry.ID, hash=entry.hash)
        self.cursor.execute (command)

        if isinstance (entry, EncrpytedEntry):
            if self.password is None or len (self.password) == 0:
                raise RuntimeError ('Access to encrypted entries requires password to be provided.')

            encryption = Encryption ()
            entry.text = encryption.encrypt (entry.text, self.password)

        entry.insert_into_database (self.cursor, self.password)

    #
    # Commit changes to the database file
    #
    def commit (self):
        self.connection.commit ()

    #
    # Return dataframe of entries
    #
    def get_entries (self, table, id=None):

        encryption = Encryption ()

        command = 'SELECT * FROM {0}'.format (table)

        if id is not None:
            command += ' WHERE id=\'{0}\''.format (id)

        entries = []

        for row in self.cursor.execute (command):

            if table == CoinEntry.ID:
                entries.append (CoinEntry (*row[1:]))

            elif table == CurrencyEntry.ID:
                entries.append (CurrencyEntry (*row[1:]))

            elif table == StockEntry.ID:
                entries.append (StockEntry (*row[1:]))

            elif table == NewsEntry.ID:
                entries.append (NewsEntry (*row[1:]))

            elif table == EncryptedEntry.ID:
                if self.password is not None and len (self.password) > 0:
                    entry = EncryptedEntry (*row[1:])
                    entry.text = encryption.decrypt (entry.text, self.password)
                    entries.append (entry)

            else:
                raise RuntimeError ('Unknown database table type')

        return entries



#--------------------------------------------------------------------------
# Unittests
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
        coin_entries.append (CoinEntry (1234, 'eth', 'coinbase', 230.0, 'eur'))
        coin_entries.append (CoinEntry (1238, 'btc', 'anycoind', 2200.12, 'eur'))
        coin_entries.append (CoinEntry (1410, 'eth', 'coinbase', 240.0, 'usd'))

        for entry in coin_entries:
            database.add (entry)

        #
        # Setup some currency entries
        #
        currency_entries = []
        currency_entries.append (CurrencyEntry (1236, 'eur', 230.0))
        currency_entries.append (CurrencyEntry (1237, 'eur', 2200.12))
        currency_entries.append (CurrencyEntry (1416, 'usd', 240.0))

        for entry in currency_entries:
            database.add (entry)

        #
        # Setup some stock course entries
        #
        stock_entries = []
        stock_entries.append (StockEntry (1234, 'gdax',   230.0))
        stock_entries.append (StockEntry (1239, 'nasdaq', 2200.12))
        stock_entries.append (StockEntry (1418, 'gdax',   240.0))

        for entry in stock_entries:
            database.add (entry)

        #
        # Setup some news entries
        #
        news_entries = []
        news_entries.append (NewsEntry (1234, 'coindesk', 'Well, some went up, some went down.', 1, 2))
        news_entries.append (NewsEntry (1242, 'btcinfo',  'Ethereum is the future of something whatever.', 40, 5))
        news_entries.append (NewsEntry (1234, 'fb_eth',   'Hey, should I but, sell, or go to the lavatory ?', None, None))

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

        entries = []

        text1 = "{'text': 'abc', 'id': 23}"
        entries.append (EncryptedEntry (1234, 'twitter'))
        entries[-1].set_text (text1, password1)

        text2 = "{'login': 'xyz123', 'auth': 42}"
        entries.append (EncryptedEntry (5678, 'facebook'))
        entries[-1].set_text (text2, password2)

        for entry in entries:
            database.add (entry)

        database.commit ()

        entries = database.get_entries (EncryptedEntry.ID, id='twitter')

        self.assertEqual (len (entries), 1)
        self.assertEqual (entries[0].get_text (password1), text1)
        self.assertNotEqual (entries[0].get_text (password1), text2)
        self.assertNotEqual (entries[0].get_text (password2), text1)

        entries = database.get_entries (EncryptedEntry.ID, id='facebook')

        self.assertEqual (len (entries), 1)
        self.assertEqual (entries[0].get_text (password2), text2)
        self.assertNotEqual (entries[0].get_text (password2), text1)
        self.assertNotEqual (entries[0].get_text (password1), text2)


#--------------------------------------------------------------------------
# Database functions
#

#
# Create new database
#
def database_create (args):
    if os.path.exists (args.database):
        raise RuntimeError ('Database file {0} already exists.'.format (args.database))

    database = Database (args.database, args.password)
    database.create ()

#
# List content of a database table
#
def database_list_table (args):

    database = Database (args.database, args.password)

    def print_as_frame (title, entries):

        frame = None
        for entry in entries:
            frame = entry.add_to_dataframe (frame)

        if frame is not None:
            pd.set_option ('display.width', 256)
            pd.set_option ('display.max_rows', len (frame))

            print (title)
            print ('-' * len (title))
            print (frame)

    if args.list == 'currencies' or args.list == 'all':
        print_as_frame ('Currencies', database.get_entries (CurrencyEntry.ID))
    if args.list == 'coins' or args.list == 'all':
        print_as_frame ('Coins', database.get_entries (CoinEntry.ID))
    if args.list == 'stock' or args.list == 'all':
        print_as_frame ('Stock', database.get_entries (StockEntry.ID))
    if args.list == 'news' or args.list == 'all':
        print_as_frame ('News', database.get_entries (NewsEntry.ID))
    if args.list == 'encrypted' or args.list == 'all':
        print_as_frame ('Encrypted', database.get_entries (EncryptedEntry.ID))

#
# Print database summary
#
def database_summary (args):

    database = Database (args.database, args.password)

    def to_time (timestamp):
        return pd.Timestamp (time.strftime ('%Y-%m-%d', time.localtime (timestamp)))

    def add_to_frame (name, entry_id):
        entries = database.get_entries (entry_id)
        ids = set (map (lambda entry: entry.id, entries))

        for id in ids:
            id_entries = [e for e in entries if e.id == id]
            times = [t.timestamp for t in id_entries]

            frame.loc[len (frame)] = [name, id, len (id_entries),
                                      pd.Timestamp (to_time (min (times))),
                                      pd.Timestamp (to_time (max (times)))]

    frame = pd.DataFrame (columns=['type', 'id', 'entries', 'start date', 'end date'])

    add_to_frame ('coin', CoinEntry.ID)
    add_to_frame ('currency', CurrencyEntry.ID)
    add_to_frame ('stock', StockEntry.ID)
    add_to_frame ('news', NewsEntry.ID)
    add_to_frame ('encrypted', EncryptedEntry.ID)

    print (frame)


#--------------------------------------------------------------------------
# MAIN
#
if __name__ == '__main__':

    #
    # Parse command line arguments
    #
    parser = argparse.ArgumentParser ()

    parser.add_argument ('-c', '--create',   action='store_true', default=False, help='Create new database')
    parser.add_argument ('-l', '--list',     action='store', choices=['currencies', 'coins', 'stock', 'news', 'encrypted', 'all'], help='List database content')
    parser.add_argument ('-s', '--summary',  action='store_true', default=False, help='Print database summary')
    parser.add_argument ('-p', '--password', type=str, default=None, help='Passwort for database encryption')
    parser.add_argument ('database',         type=str, default=None, help='Database file')

    args = parser.parse_args ()

    assert args.database is not None

    if args.create:
        database_create (args)

    elif args.list is not None:
        database_list_table (args)

    elif args.summary:
        database_summary (args)
