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
import sys
import time
import unittest

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
        assert '-' not in id
        assert '-' not in source

        content = '{0}-{1}-{2}'.format (timestamp, id, source)

        h = hashlib.sha256 ()
        h.update (bytes (content, 'utf-8'))

        self.hash = h.hexdigest ()


#--------------------------------------------------------------------------
# Container representing a single course entry for a single coin
#
class CoinCourseEntry (Entry):

    TABLE = 'coin_courses_table'

    #
    # Initialize entry
    #
    # @param timestamp Event timestamp in UTC unix epoch seconds
    #
    def __init__ (self, timestamp, id, source, course, currency):

        super ().__init__ (timestamp, id, source)

        assert isinstance (timestamp, int)
        assert isinstance (id, str)
        assert len (id) == 3
        assert isinstance (source, str)
        assert len (source) <= 8
        assert isinstance (course, float)
        assert isinstance (currency, str)
        assert len (currency) == 3

        self.timestamp = timestamp
        self.id        = id
        self.source    = source
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
        command = 'CREATE TABLE {0} ('.format (CoinCourseEntry.TABLE)
        command += 'hash VARCHAR (64), '
        command += 'timestamp LONG NOT NULL, '
        command += 'id VARCHAR (3), '
        command += 'source VARCHAR (8), '
        command += 'course REAL, '
        command += 'currency VARCHAR (3)'
        command += ')'

        cursor.execute (command)

    #
    # Insert this entry into a database
    #
    def insert_into_database (self, cursor):
        command = 'INSERT INTO {0} '.format (CoinCourseEntry.TABLE)
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
        text = 'CoinCourseEntry ('
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
class CurrencyCourseEntry (Entry):

    TABLE = 'currency_courses_table'

    #
    # Initialize entry
    #
    # @param timestamp Event timestamp in UTC unix epoch seconds
    #
    def __init__ (self, timestamp, id, course):

        super ().__init__ (timestamp, id, '')

        assert isinstance (timestamp, int)
        assert isinstance (id, str)
        assert len (id) == 3
        assert isinstance (course, float)

        self.timestamp = timestamp
        self.id        = id
        self.course    = course

    #
    # Add matching table to database
    #
    @staticmethod
    def add_table_to_database (cursor):

        command = 'CREATE TABLE {0} ('.format (CurrencyCourseEntry.TABLE)
        command += 'hash VARCHAR (64), '
        command += 'timestamp LONG NOT NULL, '
        command += 'id VARCHAR (3), '
        command += 'course REAL'
        command += ')'

        cursor.execute (command)

    #
    # Insert this entry into a database
    #
    def insert_into_database (self, cursor):
        command = 'INSERT INTO {0} '.format (CurrencyCourseEntry.TABLE)
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
        text = 'CurrencyCourseEntry ('
        text += 'timestamp={0}, '.format (self.timestamp)
        text += 'id={0}, '.format (self.id)
        text += 'course={0}'.format (self.course)
        text += ')'

        return text


#--------------------------------------------------------------------------
# Container representing a single course entry for a single stock or index
#
class StockCourseEntry (Entry):

    TABLE = 'stock_courses_table'

    #
    # Initialize entry
    #
    # @param timestamp Event timestamp in UTC unix epoch seconds
    #
    def __init__ (self, timestamp, id, course):

        super ().__init__ (timestamp, id, '')

        assert isinstance (timestamp, int)
        assert isinstance (id, str)
        assert isinstance (course, float)

        self.timestamp = timestamp
        self.id        = id
        self.course    = course

    #
    # Add matching table to database
    #
    @staticmethod
    def add_table_to_database (cursor):

        command = 'CREATE TABLE {0} ('.format (StockCourseEntry.TABLE)
        command += 'hash VARCHAR (64), '
        command += 'timestamp LONG NOT NULL, '
        command += 'id VARCHAR (3), '
        command += 'course REAL'
        command += ')'

        cursor.execute (command)

    #
    # Insert this entry into a database
    #
    def insert_into_database (self, cursor):
        command = 'INSERT INTO {0} '.format (StockCourseEntry.TABLE)
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
        text = 'StockCourseEntry ('
        text += 'timestamp={0}, '.format (self.timestamp)
        text += 'id={0}, '.format (self.id)
        text += 'course={0}'.format (self.course)
        text += ')'

        return text


#--------------------------------------------------------------------------
# Container representing a single event entry for a single news source
#
class NewsEntry (Entry):

    TABLE = 'news_courses_table'

    #
    # Initialize entry
    #
    # @param timestamp Event timestamp in UTC unix epoch seconds
    #
    def __init__ (self, timestamp, source, text):

        super ().__init__ (timestamp, '', source)

        assert isinstance (timestamp, int)
        assert isinstance (source, str)
        assert len (source) <= 8
        assert isinstance (text, str)
        assert len (text) < (1 << 16)

        self.timestamp = timestamp
        self.source = source
        self.text = text

    #
    # Add matching table to database
    #
    @staticmethod
    def add_table_to_database (cursor):
        command = 'CREATE TABLE {0} ('.format (NewsEntry.TABLE)
        command += 'hash VARCHAR (64), '
        command += 'timestamp LONG NOT NULL, '
        command += 'source VARCHAR (8), '
        command += 'text MEMO'
        command += ')'

        cursor.execute (command)

    #
    # Insert this entry into a database
    #
    def insert_into_database (self, cursor):
        command = 'INSERT INTO {0} '.format (NewsEntry.TABLE)
        command += '(hash, timestamp, source, text) '
        command += 'values (?, ?, ?, ?)'

        params = []
        params.append (self.hash)
        params.append (self.timestamp)
        params.append (self.source)
        params.append (self.text)

        cursor.execute (command, params)

    #
    # Add entry to dataframe
    #
    # @param frame Data frame to add entry to or 'None' if an appropriate frame should be created
    #
    def add_to_dataframe (self, frame):
        if frame is None:
            frame = pd.DataFrame (columns=['timestamp', 'source', 'text'])

        frame.loc[len (frame)] = [pd.Timestamp (time.ctime (self.timestamp)), self.source, self.text]

        return frame

    def __repr__ (self):

        content = self.text
        if len (content) > 64:
            content = content[:64 - 3] + '...'

        text = 'NewsEntry ('
        text += 'source={0}, '.format (self.source)
        text += 'text=\'{0}\''.format (content)
        text += ')'

        return text

#--------------------------------------------------------------------------
# Database handler
#
class Database:

    #
    # Constructor
    #
    # @param file Location of the database in the file system
    #
    def __init__ (self, file):
        self.file = file
        self.connection = sqlite3.connect (file)
        self.cursor = self.connection.cursor ()

    #
    # Create database structure
    #
    def create (self):

        CoinCourseEntry.add_table_to_database (self.cursor)
        CurrencyCourseEntry.add_table_to_database (self.cursor)
        StockCourseEntry.add_table_to_database (self.cursor)
        NewsEntry.add_table_to_database (self.cursor)

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


        command = 'DELETE FROM {table} WHERE hash="{hash}"'.format (table=entry.TABLE, hash=entry.hash)
        self.cursor.execute (command)

        entry.insert_into_database (self.cursor)

    #
    # Commit changes to the database file
    #
    def commit (self):
        self.connection.commit ()

    #
    # Return dataframe of coin course entries
    #
    def get_coin_course_entries (self):

        command = 'SELECT * FROM {0}'.format (CoinCourseEntry.TABLE)

        entries = []

        for row in self.cursor.execute (command):
            entries.append (CoinCourseEntry (*row[1:]))

        return entries

    #
    # Return list of currency course entries
    #
    def get_currency_course_entries (self):

        command = 'SELECT * FROM {0}'.format (CurrencyCourseEntry.TABLE)

        entries = []

        for row in self.cursor.execute (command):
            entries.append (CurrencyCourseEntry (*row[1:]))

        return entries

    #
    # Return list of stock course entries
    #
    def get_stock_course_entries (self):

        command = 'SELECT * FROM {0}'.format (StockCourseEntry.TABLE)

        entries = []

        for row in self.cursor.execute (command):
            entries.append (StockCourseEntry (*row[1:]))

        return entries

    #
    # Return list of news entries
    #
    def get_news_entries (self):

        command = 'SELECT * FROM {0}'.format (NewsEntry.TABLE)

        entries = []

        for row in self.cursor.execute (command):
            entries.append (NewsEntry (*row[1:]))

        return entries


#--------------------------------------------------------------------------
# Unittests
#
class TestDatabase (unittest.TestCase):

    def test_database_read_write (self):

        #
        # Create database
        #
        database = Database (':memory:')
        database.create ()

        #
        # Setup some coin course entries
        #
        coin_course_entries = []
        coin_course_entries.append (CoinCourseEntry (1234, 'eth', 'coinbase', 230.0, 'eur'))
        coin_course_entries.append (CoinCourseEntry (1238, 'btc', 'anycoind', 2200.12, 'eur'))
        coin_course_entries.append (CoinCourseEntry (1410, 'eth', 'coinbase', 240.0, 'usd'))

        for entry in coin_course_entries:
            database.add (entry)

        #
        # Setup some currency course entries
        #
        currency_course_entries = []
        currency_course_entries.append (CurrencyCourseEntry (1236, 'eur', 230.0))
        currency_course_entries.append (CurrencyCourseEntry (1237, 'eur', 2200.12))
        currency_course_entries.append (CurrencyCourseEntry (1416, 'usd', 240.0))

        for entry in currency_course_entries:
            database.add (entry)

        #
        # Setup some stock course entries
        #
        stock_course_entries = []
        stock_course_entries.append (StockCourseEntry (1234, 'gdax',   230.0))
        stock_course_entries.append (StockCourseEntry (1239, 'nasdaq', 2200.12))
        stock_course_entries.append (StockCourseEntry (1418, 'gdax',   240.0))

        for entry in stock_course_entries:
            database.add (entry)

        #
        # Setup some news entries
        #
        news_entries = []
        news_entries.append (NewsEntry (1234, 'coindesk', 'Well, some went up, some went down.'))
        news_entries.append (NewsEntry (1242, 'btcinfo',  'Ethereum is the future of something whatever.'))
        news_entries.append (NewsEntry (1234, 'fb_eth',   'Hey, should I but, sell, or go to the lavatory ?'))

        for entry in news_entries:
            database.add (entry)

        #
        # Check database content
        #
        database_coin_course_entries = database.get_coin_course_entries ()
        self.assertEqual (len (coin_course_entries), len (database_coin_course_entries))

        for a, b in zip (coin_course_entries, database_coin_course_entries):
            self.assertEqual (repr (a), repr (b))
            self.assertEqual (a.hash, b.hash)

        database_currency_course_entries = database.get_currency_course_entries ()
        self.assertEqual (len (currency_course_entries), len (database_currency_course_entries))

        for a, b in zip (currency_course_entries, database_currency_course_entries):
            self.assertEqual (repr (a), repr (b))
            self.assertEqual (a.hash, b.hash)

        database_stock_course_entries = database.get_stock_course_entries ()
        self.assertEqual (len (stock_course_entries), len (database_stock_course_entries))

        for a, b in zip (stock_course_entries, database_stock_course_entries):
            self.assertEqual (repr (a), repr (b))
            self.assertEqual (a.hash, b.hash)

        database_news_entries = database.get_news_entries ()
        self.assertEqual (len (news_entries), len (database_news_entries))

        for a, b in zip (news_entries, database_news_entries):
            self.assertEqual (repr (a), repr (b))
            self.assertEqual (a.hash, b.hash)


#--------------------------------------------------------------------------
# MAIN
#
if __name__ == '__main__':

    #
    # Parse command line arguments
    #
    parser = argparse.ArgumentParser ()

    parser.add_argument ('-c', '--create', action='store_true', default=False, help='Create new database')
    parser.add_argument ('-l', '--list',   action='store', choices=['currencies', 'coins', 'stock', 'news'], help='List database content')
    parser.add_argument ('database',       type=str, default=None, help='Database file')

    args = parser.parse_args ()

    assert args.database is not None


    #
    # Create and setup new database file
    #
    if args.create:
        if os.path.exists (args.database):
            raise RuntimeError ('Database file {0} already exists.'.format (args.database))

        database = Database (args.database)
        database.create ()

        sys.exit (0)

    #
    # List database content
    #
    if args.list is not None:

        database = Database (args.database)

        if args.list == 'currencies':
            entries = database.get_currency_course_entries ()
        elif args.list == 'coins':
            entries = database.get_coin_course_entries ()
        elif args.list == 'stock':
            entries = database.get_stock_course_entries ()
        elif args.list == 'news':
            entries = database.get_news_entries ()
        else:
            raise RuntimeError ('Illegal database table name \'{0}\''.format (args.list))

        pd.set_option ('display.width', 256)

        frame = None
        for entry in entries:
            frame = entry.add_to_dataframe (frame)

        pd.set_option ('display.max_rows', len (frame))
        print (frame)

        sys.exit (0)
