#!/usr/bin/python3
#
# database.py - Database access
#
# Frank Blankenburg, Jun. 2017
#

import argparse
import os.path
import sqlite3
import unittest

#--------------------------------------------------------------------------
# Container representing a single course entry for a single coin
#
class CoinCourseEntry:

    def __init__ (self, timestamp, id, source, course, currency):

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
class CurrencyCourseEntry:

    def __init__ (self, timestamp, id, course):

        assert isinstance (timestamp, int)
        assert isinstance (id, str)
        assert len (id) == 3
        assert isinstance (course, float)

        self.timestamp = timestamp
        self.id        = id
        self.course    = course

    def __repr__ (self):
        text = 'CurrencyCourseEntry ('
        text += 'timestamp={0}, '.format (self.timestamp)
        text += 'id={0}, '.format (self.id)
        text += 'course={0}'.format (self.course)
        text += ')'

        return text


#--------------------------------------------------------------------------
# Container representing a single event entry for a single news source
#
class NewsEntry:

    def __init__ (self, timestamp, source, text):

        assert isinstance (timestamp, int)
        assert isinstance (source, str)
        assert len (source) <= 8
        assert isinstance (text, str)
        assert len (text) < (1 << 16)

        self.timestamp = timestamp
        self.source = source
        self.text = text

    def __repr__ (self):
        content = self.text

        if len (content) > 32:
            content = content[:29] + '...'

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
    # Identifier for the database tables
    #
    COIN_COURSES_TABLE     = 'coin_courses'
    CURRENCY_COURSES_TABLE = 'currency_courses'
    NEWS_TABLE             = 'news'

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

        #
        # Coin courses table
        #
        command = 'CREATE TABLE {0} ('.format (Database.COIN_COURSES_TABLE)
        command += 'timestamp LONG NOT NULL, '
        command += 'id VARCHAR (3), '
        command += 'source VARCHAR (8), '
        command += 'course REAL, '
        command += 'currency VARCHAR (3)'
        command += ')'

        self.cursor.execute (command)

        #
        # Currency courses table
        #
        command = 'CREATE TABLE {0} ('.format (Database.CURRENCY_COURSES_TABLE)
        command += 'timestamp LONG NOT NULL, '
        command += 'id VARCHAR (3), '
        command += 'course REAL'
        command += ')'

        self.cursor.execute (command)

        #
        # News event table
        #
        command = 'CREATE TABLE {0} ('.format (Database.NEWS_TABLE)
        command += 'timestamp LONG NOT NULL, '
        command += 'source VARCHAR (8), '
        command += 'text MEMO'
        command += ')'

        self.cursor.execute (command)

    #
    # Add entry to the database
    #
    # @param entry Entry to be added. The type distinguishes about the location.
    #
    def add (self, entry):

        if isinstance (entry, CoinCourseEntry):
            command = 'INSERT INTO {0} '.format (Database.COIN_COURSES_TABLE)
            command += '(timestamp, id, source, course, currency) '
            command += 'values (?, ?, ?, ?, ?)'

            params = []
            params.append (entry.timestamp)
            params.append (entry.id)
            params.append (entry.source)
            params.append (entry.course)
            params.append (entry.currency)

            self.cursor.execute (command, params)

        elif isinstance (entry, CurrencyCourseEntry):
            command = 'INSERT INTO {0} '.format (Database.CURRENCY_COURSES_TABLE)
            command += '(timestamp, id, course) '
            command += 'values (?, ?, ?)'

            params = []
            params.append (entry.timestamp)
            params.append (entry.id)
            params.append (entry.course)

            self.cursor.execute (command, params)

        elif isinstance (entry, NewsEntry):
            command = 'INSERT INTO {0} '.format (Database.NEWS_TABLE)
            command += '(timestamp, source, text) '
            command += 'values (?, ?, ?)'

            params = []
            params.append (entry.timestamp)
            params.append (entry.source)
            params.append (entry.text)

            self.cursor.execute (command, params)

        else:
            raise RuntimeError ('Unhandled database entry type')

    #
    # Return list of coin course entries
    #
    def get_coin_course_entries (self):

        command = 'SELECT * FROM {0}'.format (Database.COIN_COURSES_TABLE)

        entries = []

        for row in self.cursor.execute (command):
            entries.append (CoinCourseEntry (*row))

        return entries

    #
    # Return list of currency course entries
    #
    def get_currency_course_entries (self):

        command = 'SELECT * FROM {0}'.format (Database.CURRENCY_COURSES_TABLE)

        entries = []

        for row in self.cursor.execute (command):
            entries.append (CurrencyCourseEntry (*row))

        return entries

    #
    # Return list of news entries
    #
    def get_news_entries (self):

        command = 'SELECT * FROM {0}'.format (Database.NEWS_TABLE)

        entries = []

        for row in self.cursor.execute (command):
            entries.append (NewsEntry (*row))

        return entries


#--------------------------------------------------------------------------
# Unittest
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

        database_currency_course_entries = database.get_currency_course_entries ()
        self.assertEqual (len (currency_course_entries), len (database_currency_course_entries))

        for a, b in zip (currency_course_entries, database_currency_course_entries):
            self.assertEqual (repr (a), repr (b))

        database_news_entries = database.get_news_entries ()
        self.assertEqual (len (news_entries), len (database_news_entries))

        for a, b in zip (news_entries, database_news_entries):
            self.assertEqual (repr (a), repr (b))


#--------------------------------------------------------------------------
# MAIN
#
if __name__ == '__main__':

    #
    # Parse command line arguments
    #
    parser = argparse.ArgumentParser ()

    parser.add_argument ('-c', '--create', action='store_true', default=False, help='Create new database')
    parser.add_argument ('-l', '--list',   action='store', choices=['currencies', 'coins', 'news'], help='List database content')
    parser.add_argument ('database',       type=str, default=None, help='Trained model data (meta data file)')

    args = parser.parse_args ()

    assert not args.database is None

    database = Database (args.database)

    #
    # Create and setup new database file
    #
    if args.create:
        if os.path.exists (args.database):
            raise RuntimeError ('Database file {0} already exists.'.format (args.database))

        database.create ()

    #
    # List database content
    #
    if not args.list is None:
        if args.list == 'currencies':
            entries = database.get_currency_course_entries ()
        elif args.list == 'coins':
            entries = database.get_coin_course_entries ()
        elif args.list == 'news':
            entries = database.get_news_entries ()
        else:
            raise RuntimeError ('Illegal database table name \'{0}\''.format (args.list))
