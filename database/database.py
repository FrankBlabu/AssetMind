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
import unittest

from abc import ABC, abstractmethod
from database.encryption import Encryption
from core.time import Timestamp

#--------------------------------------------------------------------------
# Base class for database entries
#
# This class cares for computing a unique hash value for each entry which
# is used to determine if this special entry is already present in the
# database. This way the same source of information can be read and inserted
# multiple times without leading to duplicate database entries.
#
class Entry (ABC):

    #
    # Constructor
    #
    # Computes the hash value for the entry
    #
    def __init__ (self, timestamp, id, source):

        assert isinstance (id, str)
        assert isinstance (source, str) or source is None
        assert '-' not in id
        assert source is None or '-' not in source

        self.timestamp = Timestamp (timestamp)
        self.id = id
        self.source = source

        if source is not None:
            content = '{0}-{1}-{2}'.format (self.timestamp.epoch, id, source)
        else:
            content = '{0}-{1}'.format (self.timestamp.epoch, id)

        h = hashlib.sha256 ()
        h.update (bytes (content, 'utf-8'))
        self.hash = h.hexdigest ()

    @staticmethod
    def fill_data_frame (header, func, entries):

        frame = pd.DataFrame (columns=header, index=list (range (len (entries))))

        count = 0
        for entry in entries:
            frame.loc[count] = func (entry)
            count += 1

        return frame


    @staticmethod
    @abstractmethod
    def add_table_to_database (database):
        pass

    @abstractmethod
    def insert_into_database (self, database):
        pass

    @staticmethod
    @abstractmethod
    def read_from_database (database, id):
        pass

    @staticmethod
    @abstractmethod
    def create_data_frame (entries):
        pass


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
    def add_table_to_database (database):

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

        database.cursor.execute (command)

    #
    # Insert this entry into a database
    #
    def insert_into_database (self, database):
        command = 'INSERT INTO {0} '.format (CoinEntry.ID)
        command += '(hash, timestamp, id, source, course, currency) '
        command += 'values (?, ?, ?, ?, ?, ?)'

        params = []
        params.append (self.hash)
        params.append (self.timestamp.epoch)
        params.append (self.id)
        params.append (self.source)
        params.append (self.course)
        params.append (self.currency)

        database.cursor.execute (command, params)

    #
    # Read this entry from database
    #
    @staticmethod
    def read_from_database (database, id):

        command = 'SELECT * FROM {0}'.format (CoinEntry.ID)

        if id is not None:
            command += ' WHERE id=\'{0}\''.format (id)

        return [CoinEntry (*row[1:]) for row in database.cursor.execute (command)]


    #
    # Create data frame for displaying the given entries
    #
    @staticmethod
    def create_data_frame (entries):
        return Entry.fill_data_frame (['timestamp', 'id', 'source', 'course', 'currency'],
                                        lambda entry: [entry.timestamp.to_pandas (), entry.id,
                                                       entry.source, entry.course, entry.currency],
                                        entries)

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
    def add_table_to_database (database):

        command = 'CREATE TABLE {0} ('.format (CurrencyEntry.ID)
        command += 'hash VARCHAR (64), '
        command += 'timestamp LONG NOT NULL, '
        command += 'id VARCHAR (3), '
        command += 'course REAL'
        command += ')'

        database.cursor.execute (command)

    #
    # Insert this entry into a database
    #
    def insert_into_database (self, database):
        command = 'INSERT INTO {0} '.format (CurrencyEntry.ID)
        command += '(hash, timestamp, id, course) '
        command += 'values (?, ?, ?, ?)'

        params = []
        params.append (self.hash)
        params.append (self.timestamp.epoch)
        params.append (self.id)
        params.append (self.course)

        database.cursor.execute (command, params)

    #
    # Read this entry from database
    #
    @staticmethod
    def read_from_database (database, id):

        command = 'SELECT * FROM {0}'.format (CurrencyEntry.ID)

        if id is not None:
            command += ' WHERE id=\'{0}\''.format (id)

        return [CurrencyEntry (*row[1:]) for row in database.cursor.execute (command)]

    #
    # Create data frame for displaying the given entries
    #
    @staticmethod
    def create_data_frame (entries):
        return Entry.fill_data_frame (['timestamp', 'id', 'course'],
                                        lambda entry: [entry.timestamp.to_pandas (), entry.id, entry.course],
                                        entries)

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
    def add_table_to_database (database):

        command = 'CREATE TABLE {0} ('.format (StockEntry.ID)
        command += 'hash VARCHAR (64), '
        command += 'timestamp LONG NOT NULL, '
        command += 'id VARCHAR (8), '
        command += 'course REAL'
        command += ')'

        database.cursor.execute (command)

    #
    # Insert this entry into a database
    #
    def insert_into_database (self, database):
        command = 'INSERT INTO {0} '.format (StockEntry.ID)
        command += '(hash, timestamp, id, course) '
        command += 'values (?, ?, ?, ?)'

        params = []
        params.append (self.hash)
        params.append (self.timestamp.epoch)
        params.append (self.id)
        params.append (self.course)

        database.cursor.execute (command, params)

    #
    # Read this entry from database
    #
    @staticmethod
    def read_from_database (database, id):

        command = 'SELECT * FROM {0}'.format (StockEntry.ID)

        if id is not None:
            command += ' WHERE id=\'{0}\''.format (id)

        return [StockEntry (*row[1:]) for row in database.cursor.execute (command)]


    #
    # Create data frame for displaying the given entries
    #
    def create_data_frame (entries):
        return Entry.fill_data_frame (['timestamp', 'id', 'course'],
                                        lambda entry:  [entry.timestamp.to_pandas (), entry.id, entry.course],
                                        entries)

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
        assert isinstance (likes, int) or likes is None

        self.text = text
        self.shares = shares if shares is None or shares >= 0 else None
        self.likes = likes if likes is None or likes >= 0 else None

    #
    # Add matching table to database
    #
    @staticmethod
    def add_table_to_database (database):
        command = 'CREATE TABLE {0} ('.format (NewsEntry.ID)
        command += 'hash VARCHAR (64), '
        command += 'timestamp LONG NOT NULL, '
        command += 'id VARCHAR (8), '
        command += 'text MEMO, '
        command += 'shares INT, '
        command += 'likes INT'
        command += ')'

        database.cursor.execute (command)

    #
    # Insert this entry into a database
    #
    def insert_into_database (self, database):
        command = 'INSERT INTO {0} '.format (NewsEntry.ID)
        command += '(hash, timestamp, id, text, shares, likes) '
        command += 'values (?, ?, ?, ?, ?, ?)'

        params = []
        params.append (self.hash)
        params.append (self.timestamp.epoch)
        params.append (self.id)
        params.append (self.text)
        params.append (self.shares if self.shares is not None else -1)
        params.append (self.likes if self.shares is not None else -1)

        database.cursor.execute (command, params)

    #
    # Read this entry from database
    #
    @staticmethod
    def read_from_database (database, id):

        command = 'SELECT * FROM {0}'.format (NewsEntry.ID)

        if id is not None:
            command += ' WHERE id=\'{0}\''.format (id)

        return [NewsEntry (*row[1:]) for row in database.cursor.execute (command)]

    #
    # Create data frame for displaying the given entries
    #
    def create_data_frame (entries):

        return Entry.fill_data_frame (['timestamp', 'id', 'text', 'shares', 'likes'],
                                        lambda entry: [entry.timestamp.to_pandas (), entry.id, entry.text,
                                                       entry.shares if entry.shares is not None else '-',
                                                       entry.likes if entry.likes is not None else '-'],
                                        entries)

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
    def add_table_to_database (database):

        command = 'CREATE TABLE {0} ('.format (EncryptedEntry.ID)
        command += 'hash VARCHAR (64), '
        command += 'timestamp LONG NOT NULL, '
        command += 'id VARCHAR (64), '
        command += 'text MEMO'
        command += ')'

        database.cursor.execute (command)

    #
    # Insert this entry into a database
    #
    def insert_into_database (self, database):

        assert isinstance (database.password, str)
        assert len (database.password) >= 4

        command = 'INSERT INTO {0} '.format (EncryptedEntry.ID)
        command += '(hash, timestamp, id, text) '
        command += 'values (?, ?, ?, ?)'

        encryption = Encryption ()

        params = []
        params.append (self.hash)
        params.append (self.timestamp.epoch)
        params.append (self.id)
        params.append (encryption.encrypt (self.text, database.password))

        database.cursor.execute (command, params)

    #
    # Read this entry from database
    #
    @staticmethod
    def read_from_database (database, id):

        assert database.password is not None
        assert len (database.password) >= 4

        command = 'SELECT * FROM {0}'.format (EncryptedEntry.ID)

        if id is not None:
            command += ' WHERE id=\'{0}\''.format (id)

        entries = [EncryptedEntry (*row[1:]) for row in database.cursor.execute (command)]

        encryption = Encryption ()

        for entry in entries:
            entry.text = encryption.decrypt (entry.text, database.password)

        return entries



    #
    # Create data frame for displaying the given entries
    #
    def create_data_frame (entries):

        return Entry.fill_data_frame (['timestamp', 'id', 'text'],
                                        lambda entry: [entry.timestamp.to_pandas (), entry.id, entry.text],
                                        entries)

    def __repr__ (self):

        text = 'EncryptedEntry ('
        text += 'timestamp={0}, '.format (self.timestamp)
        text += 'id={0}, '.format (self.id)
        text += 'text={0}'.format (self.text if len (self.text) < 16 else self.text[:16] + '...')
        text += ')'

        return text


#--------------------------------------------------------------------------
# Database handler
#
class Database:

    #
    # Entry types the database supports
    #
    types = [CoinEntry, CurrencyEntry, StockEntry, NewsEntry, EncryptedEntry]

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

        for t in Database.types:
            t.add_table_to_database (self)

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

        entry.insert_into_database (self)

    #
    # Commit changes to the database file
    #
    def commit (self):
        self.connection.commit ()

    #
    # Return dataframe of entries
    #
    def get_entries (self, table, id=None):

        entries = None

        for t in Database.types:
            if table == t.ID:
                entries = t.read_from_database (self, id)

        if entries is None:
            raise RuntimeError ('Invalid database table id')

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

    def print_frame (title, frame):

        pd.set_option ('display.width', 256)
        pd.set_option ('display.max_rows', len (frame))

        print (title)
        print ('-' * len (title))
        print (frame)

    for t in Database.types:
        if args.list == t.ID or args.list == 'all':
            print_frame (t.ID, t.create_data_frame (database.get_entries (t.ID)))

#
# Print database summary
#
def database_summary (args):

    database = Database (args.database, args.password)

    frame = pd.DataFrame (columns=['type', 'id', 'entries', 'start date', 'end date'])

    for t in Database.types:
        entries = database.get_entries (t.ID)
        ids = set (map (lambda entry: entry.id, entries))

        for id in ids:
            id_entries = [e for e in entries if e.id == id]
            times = [t.timestamp for t in id_entries]

            frame.loc[len (frame)] = [t.__name__, id, len (id_entries),
                                      min (times), max (times)]

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
    parser.add_argument ('-l', '--list',     action='store', choices=[t.ID for t in Database.types] + ['all'], help='List database content')
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
