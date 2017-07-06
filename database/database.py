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

import core

from core.encryption import Encryption
from core.time import Timestamp
from scraper.scraper import Scraper
from scraper.scraper import ScraperRegistry


#--------------------------------------------------------------------------
# Generic database entry
#
class Entry (core.common.AttrDict):

    def __repr__ (self):
        return 'Entry ' + super ().__repr__ ()


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
        command += 'encrypted BOOLEAN'
        command += ')'

        self.cursor.execute (command)

        for scraper in ScraperRegistry.get_all ():

            for channel in scraper.get_channels ():

                assert len (channel.id) <= 64
                assert channel.type in self.types.values ()
                assert len (channel.type.__name__) <= 64

                admin = self.get_admin_data (channel.id)

                #
                # Register type in administrative database
                #
                command = 'INSERT INTO "{id}" '.format (id=Database.ADMIN_ID)
                command += '(id, description, type, encrypted) '
                command += 'values (?, ?, ?, ?)'

                params = []
                params.append (channel.id)
                params.append (channel.description)
                params.append (channel.type.__name__)
                params.append (channel.encrypted)

                self.cursor.execute (command, params)

                #
                # Create table for the channel itself
                #
                command = 'CREATE TABLE "{id}" ('.format (id=channel.id)
                command += 'hash VARCHAR (64), '
                command += 'timestamp LONG NOT NULL, '

                if channel.type is str:
                    command += 'value MEMO'
                elif channel.type is float:
                    command += 'value REAL'

                command += ')'

                self.cursor.execute (command)

        self.connection.commit ()


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

        #
        # Insert new entries into database
        #
        for entry in entries:

            assert isinstance (entry, Entry)
            assert isinstance (entry.timestamp, Timestamp)
            assert isinstance (entry.value, float) or isinstance (entry.value, str)
            assert isinstance (entry.value, admin.type)

            #
            # The timestamp has a fixed granularity, usually 'day' or 'hour'. So the timestamp hash
            # will show us if there already is an entry covering this time slot which must be
            # deleted first.
            #
            if not hash in entry:
                h = hash (entry.timestamp)
                h = hashlib.md5 (h.to_bytes (8, 'big')).hexdigest ()
            else:
                h = entry.hash

            command = 'DELETE FROM "{table}" WHERE hash="{hash}"'.format (table=id, hash=h)
            self.cursor.execute (command)

            command = 'INSERT INTO "{table}" '.format (table=id)
            command += '(hash, timestamp, value) '
            command += 'values (?, ?, ?)'

            params = []
            params.append (h)
            params.append (entry.timestamp.epoch ())

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
            entries.append (Entry (id=row[0], description=row[1], type=self.types[row[2]], encrypted=row[3]))

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


#
# List content of database tables
#
def database_list (args):

    database = Database (args.database, args.password)
    admin = database.get_admin_data ()

    ids = [id.strip () for id in args.list.split (',')]

    for admin_entry in admin:
        if admin_entry.id in ids or 'all' in ids:

            frame = pd.DataFrame (columns=['timestamp', 'hash', 'value'])

            for entry in database.get (admin_entry.id):
                frame.loc[len (frame)] = [entry.timestamp, entry.hash, entry.value]

            print_frame ('{0} [{1}]'.format (admin_entry.id, admin_entry.description), frame)
            print ('')


#
# Print database summary
#
def database_summary (args):

    database = Database (args.database, args.password)
    frame = pd.DataFrame (columns=['id', 'description', 'type', 'encrypted', 'entries'])

    for entry in database.get_admin_data ():
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
    parser.add_argument ('-l', '--list',     action='store', default=False, help='List database content')
    parser.add_argument ('-s', '--summary',  action='store_true', default=False, help='Print database summary')
    parser.add_argument ('-p', '--password', type=str, default=None, help='Passwort for database encryption')
    parser.add_argument ('database',         type=str, default=None, help='Database file')

    args = parser.parse_args ()

    assert args.database is not None

    if args.create:
        database_create (args)

    elif args.summary:
        database_summary (args)

    elif args.list:
        database_list (args)
