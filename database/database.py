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

from enum import Enum

from core.encryption import Encryption
from core.time import Timestamp
from scraper.scraper import Scraper
from scraper.cryptocompare import CryptoCompareScraper

#--------------------------------------------------------------------------
# Generic database entry
#
class Entry (dict):

    def __init__ (self, *args, **kwargs):
        super (Entry, self).__init__ (*args, **kwargs)

    def __getattr__ (self, name):
        if name not in self:
            raise AttributeError ('No such attribute: ', name)
        return self[name]

    def __setattr__ (self, name, value):
        self[name] = value

    def __delattr__ (self, name):
        if name in self:
            del self[name]

    def __repr__ (self):
        return 'Entry ({0})'.format (', '.join ([key + '=' + str (self[key]) for key in self.keys ()]))



#--------------------------------------------------------------------------
# Database
#
class Database:

    #
    # Table id used for administrative data
    #
    ADMIN_ID = 'administration'

    #
    # Constructor
    #
    # @param file     Location of the database in the file system
    # @param password Password to access encrypted database entries
    #
    def __init__ (self, file, password=None):

        self.encryption = Encryption ()
        self.scrapers = {}
        self.types = {str.__name__: str, float.__name__: float}

        self.file = file
        self.password = password
        self.connection = sqlite3.connect (file)
        self.cursor = self.connection.cursor ()
        self.created = False

    #
    # Create database structure
    #
    def create (self):
        #
        # Create administrative table
        #
        command = 'CREATE TABLE {id} ('.format (id=Database.ADMIN_ID)
        command += 'id VARCHAR (64), '
        command += 'description MEMO, '
        command += 'type VARCHAR (64), '
        command += 'scraper VARCHAR (128), '
        command += 'encrypted BOOLEAN'
        command += ')'

        self.cursor.execute (command)
        self.connection.commit ()

        self.created = True


    #
    # Register new entry type
    #
    def register (self, id, description, type_id, scraper=None, encrypted=False):

        assert len (id) <= 64
        assert type_id in self.types
        assert scraper is None or isinstance (scraper, Scraper)
        assert len (type_id.__name__) <= 64

        if scraper:
            self.scrapers[scraper.__name__] = scraper

        admin = self.get_admin_data (id)

        #
        # Add type into database if not done yet
        #
        if admin is None:
            #
            # Register type in administrative database
            #
            command = 'INSERT INTO "{id}" '.format (id=Database.ADMIN_ID)
            command += '(id, description, type, scraper, encrypted) '
            command += 'values (?, ?, ?, ?)'

            params = []
            params.append (id)
            params.append (description)
            params.append (type_id.__name__)
            params.append (scraper.__name__ if scraper else '')
            params.append (encrypted)

            self.cursor.execute (command, params)

            #
            # If the database is just being created, the registered types table has to to
            # added now, too
            #
            if self.created:
                command = 'CREATE TABLE "{id}" ('.format (id=id)
                command += 'hash VARCHAR (64), '
                command += 'timestamp LONG NOT NULL, '

                if type_id is str:
                    command += 'value MEMO'
                elif type_id is float:
                    command += 'value REAL'

                command += ')'

                self.cursor.execute (command)

            self.connection.commit ()

        #
        # For already registered types check if the fields are matching
        #
        else:
            assert admin.id == id
            assert admin.description == description
            assert admin.type == type_id
            assert admin.scraper == scraper
            assert admin.encrypted == encrypted

    #
    # Add entry to the database
    #
    # If an entry with the same hash is already existing in the database, it will
    # be replaced by the new entry. So it is assumed that data added later is
    # 'more correct' or generally of a higher quality.
    #
    # @param id      Id of the database to be used
    # @param entries Entries in JSON format to be added (or single entry)
    #
    def add (self, id, entries):

        #
        # Fetch administrative entry
        #
        admin = self.get_admin_data (id)

        assert not admin.encrypted or isinstance (self.password, str)
        assert not admin.encrypted or len (self.password) >= 4

        if not isinstance (entries, list):
            entries = [entries]

        for entry in entries:

            assert isinstance (entry, Entry)
            assert isinstance (entry.timestamp, Timestamp)
            assert isinstance (entry.value, float) or isinstance (entry.value, str)
            assert isinstance (entry.value, admin.type)

            h = hash (entry.timestamp)
            h = hashlib.md5 (h.to_bytes (8, 'big')).hexdigest ()

            command = 'DELETE FROM "{table}" WHERE hash="{hash}"'.format (table=id, hash=h)
            self.cursor.execute (command)

            command = 'INSERT INTO "{table}" '.format (table=id)
            command += '(hash, timestamp, value) '
            command += 'values (?, ?, ?)'

            params = []
            params.append (h)
            params.append (hash (entry.timestamp))

            if admin.encrypted:
                params.append (self.encryption.encrypt (entry.value, self.password))
            else:
                params.append (entry.value)

            self.cursor.execute (command, params)

        self.connection.commit ()


    #
    # Return all entries in a table
    #
    def get (self, id):

        assert id is not Database.ADMIN_ID

        admin = self.get_admin_data (id)

        command = 'SELECT * FROM "{table}"'.format (table=id)
        rows = self.cursor.execute (command)

        entries = [Entry (hash=row[0], timestamp=Timestamp (row[1]), value=row[2]) for row in rows]

        if admin.encrypted:
            for entry in entries:
                entry.value = self.encryption.decrypt (entry.value, self.password)

        return entries

    #
    # Return administrative database entry
    #
    def get_admin_data (self, id=None):

        command = 'SELECT * FROM "{table}"'.format (table=Database.ADMIN_ID)

        if id is not None:
            command += ' WHERE id="{id}"'.format (id=id)

        rows = self.cursor.execute (command)

        entries = []

        for row in rows:
            assert row[2] in self.types

            scraper = None
            if row[3] and row[3] in self.scrapers:
                scraper = self.scrapers[row[3]]

            entries.append (Entry (id=row[0], description=row[1], type=self.types[row[2]], scraper=scraper, encrypted=row[4]))

        if not entries:
            return None
        elif not id:
            return entries

        return entries[0]


#--------------------------------------------------------------------------
# Database functions
#

def print_frame (title, frame):

    pd.set_option ('display.width', 256)
    pd.set_option ('display.max_rows', len (frame))

    print (title)
    print ('-' * len (title))
    print (frame)

#
# Create new database
#
def database_create (args):
    if os.path.exists (args.database):
        raise RuntimeError ('Database file {0} already exists.'.format (args.database))

    database = Database (args.database, args.password)
    database.create ()

    database.register (id='CryptoCompare::ETH', description='Ethereum course (CryptoCompare)', type_id=float,
                       scraper=CryptoCompareScraper, encrypted=False)
    database.register (id='CryptoCompare::BTC', description='Bitcoin course (CryptoCompare)', type_id=float,
                       scraper=CryptoCompareScraper, encrypted=False)


#
# List content of a database table
#
def database_list_table (args):

    database = Database (args.database, args.password)

    for t in Database.types:
        if args.list == t.ID or args.list == 'all':
            print_frame (t.ID, t.create_data_frame (database.get_entries (t.ID)))

#
# Print database summary
#
def database_summary (args):

    database = Database (args.database, args.password)

    entries = database.get_admin_data ()

    frame = pd.DataFrame (columns=['id', 'description', 'type', 'encrypted', 'entries'])

    for entry in entries:
        data = database.get (entry.id)
        frame.loc[len (frame)] = [entry.id, entry.description, entry.type.__name__, (entry.encrypted == 1), len (data)]

    print_frame ('Administrative data', frame)


#--------------------------------------------------------------------------
# MAIN
#
if __name__ == '__main__':

    #
    # Parse command line arguments
    #
    parser = argparse.ArgumentParser ()

    parser.add_argument ('-c', '--create',   action='store_true', default=False, help='Create new database')
    #parser.add_argument ('-l', '--list',     action='store', choices=[t.ID for t in Database.types] + ['all'], help='List database content')
    parser.add_argument ('-s', '--summary',  action='store_true', default=False, help='Print database summary')
    parser.add_argument ('-p', '--password', type=str, default=None, help='Passwort for database encryption')
    parser.add_argument ('database',         type=str, default=None, help='Database file')

    args = parser.parse_args ()

    assert args.database is not None

    if args.create:
        database_create (args)

    elif args.summary:
        database_summary (args)
