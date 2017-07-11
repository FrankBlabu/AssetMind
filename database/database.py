#!/usr/bin/python3
#
# database.py - Database access
#
# Frank Blankenburg, Jun. 2017
#

import argparse
import pandas as pd
import sqlite3

import core.common

from core.encryption import Encryption
from core.time import Timestamp
from scraper.scraper import ScraperRegistry


#--------------------------------------------------------------------------
# Generic database entry
#
class Entry:

    def __init__ (self, timestamp, value):
        self.timestamp = timestamp
        self.value = value

    def __repr__ (self):
        return 'Entry (timestamp={timestamp}, value={value})' \
        .format (timestamp=self.timestamp, value=self.value)

#
# Scraper channel
#
# A single channel is responsible for one single datum stream like the price of a coin,
# the text in a news channel etc. It is identified by an id like 'CryptoCompare::ETH'
# which consists of a scraper id ('CryptoCompare') and a token id ('ETH').
#
class Channel:

    def __init__ (self, id, description, type_id):

        self.id = id
        self.description = description
        self.type = type_id

    def __repr__ (self):
        return 'Channel (id={id}, description={description}, type={type})' \
        .format (id=self.id, description=self.description, type=self.type)



#--------------------------------------------------------------------------
# Database
#
class Database:

    #
    # Internal table ids
    #
    CHANNELS_ID    = 'internal::channels'
    CREDENTIALS_ID = 'internal::credentials'

    #
    # Constructor
    #
    # @param file     Location of the database in the file system
    # @param password Password to access encrypted database entries
    #
    def __init__ (self, file, password=None):

        self.encryption = Encryption ()
        self.types = {str.__name__: str, float.__name__: float}
        self.active_channels = []

        self.file = file
        self.password = password
        self.connection = sqlite3.connect (file)
        self.cursor = self.connection.cursor ()

        #
        # Create channel table
        #
        command = 'CREATE TABLE "{id}" ('.format (id=Database.CHANNELS_ID)
        command += 'id VARCHAR (64), '
        command += 'description MEMO, '
        command += 'type VARCHAR (64)'
        command += ')'

        try:
            self.cursor.execute (command)
        except sqlite3.OperationalError as e:
            pass

        #
        # Create credential table
        #
        command = 'CREATE TABLE "{id}" ('.format (id=Database.CREDENTIALS_ID)
        command += 'id VARCHAR (64), '
        command += 'value MEMO'
        command += ')'

        try:
            self.cursor.execute (command)
        except sqlite3.OperationalError as e:
            pass

        #
        # Create tables for the registered scraper channels
        #
        for scr in ScraperRegistry.get_all ():
            for channel in scr.get_channels ():

                assert len (channel.id) <= 64
                assert channel.type in self.types.values ()
                assert len (channel.type.__name__) <= 64

                command = 'CREATE TABLE "{id}" ('.format (id=channel.id)
                command += 'timestamp LONG NOT NULL, '

                if channel.type is str:
                    command += 'value MEMO'
                elif channel.type is float:
                    command += 'value REAL'

                command += ')'

                exists = False

                try:
                    self.cursor.execute (command)
                except sqlite3.OperationalError as e:
                    exists = True

                self.active_channels.append (channel.id)

                #
                # Register type in channel database
                #
                if not exists:
                    command = 'INSERT INTO "{id}" '.format (id=Database.CHANNELS_ID)
                    command += '(id, description, type) '
                    command += 'values (?, ?, ?)'

                    params = []
                    params.append (channel.id)
                    params.append (channel.description)
                    params.append (channel.type.__name__)

                    self.cursor.execute (command, params)


        self.connection.commit ()


    #
    # Add entry to the database
    #
    # If an entry with the same timestamp is already existing in the database, it will
    # be replaced by the new entry. So it is assumed that data added later is
    # 'more correct' or generally of a higher quality.
    #
    # @param id      Id of the database to be used
    # @param entries Entries in JSON format to be added (or single entry)
    #
    def add (self, id, entries):

        assert id is not Database.CHANNELS_ID
        assert id is not Database.CREDENTIALS_ID

        #
        # Fetch channel entry
        #
        channel = self.get_channel (id)

        if not isinstance (entries, list):
            entries = [entries]

        #
        # Insert new entries into database
        #
        for entry in entries:

            assert isinstance (entry, Entry)
            assert isinstance (entry.timestamp, Timestamp)
            assert isinstance (entry.value, float) or isinstance (entry.value, str)
            assert isinstance (entry.value, channel.type)

            command = 'DELETE FROM "{channel}"'.format (channel=id)
            command += ' WHERE timestamp="{timestamp}"'.format (timestamp=entry.timestamp.epoch ())
            self.cursor.execute (command)

            command = 'INSERT INTO "{channel}" '.format (channel=id)
            command += '(timestamp, value) '
            command += 'values (?, ?)'

            params = []
            params.append (entry.timestamp.epoch ())
            params.append (entry.value)

            self.cursor.execute (command, params)

        self.connection.commit ()


    #
    # Return all entries in a table
    #
    def get (self, id):

        assert id is not Database.CHANNELS_ID
        assert id is not Database.CREDENTIALS_ID

        channel = self.get_channel (id)
        assert channel

        command = 'SELECT * FROM "{channel}"'.format (channel=id)
        rows = self.cursor.execute (command)

        return [Entry (timestamp=Timestamp (row[0]), value=row[1]) for row in rows]

    #
    # Return administrative entry for a single channel
    #
    def get_channel (self, id):

        command = 'SELECT * FROM "{table}"'.format (table=Database.CHANNELS_ID)
        command += ' WHERE id="{id}"'.format (id=id)

        rows = list (self.cursor.execute (command))

        if not rows:
            return None

        assert len (rows) == 1
        row = rows[0]

        assert row[2] in self.types
        return Channel (id=row[0], description=row[1], type_id=self.types[row[2]])


    #
    # Return administrative entries for all channels
    #
    def get_all_channels (self, active_channels_only=True):

        command = 'SELECT * FROM "{table}"'.format (table=Database.CHANNELS_ID)

        rows = self.cursor.execute (command)

        entries = []

        for row in rows:
            assert row[2] in self.types

            id = row[0]

            if not active_channels_only or id in self.active_channels:
                entries.append (Channel (id=id, description=row[1], type_id=self.types[row[2]]))

        return entries

    #
    # Add credential to database
    #
    def add_credential (self, id, value):

        assert isinstance (self.password, str)
        assert len (self.password) >= 4
        assert isinstance (id, str)
        assert len (id) <= 64
        assert isinstance (value, str)

        command = 'DELETE FROM "{channel}"'.format (channel=Database.CREDENTIALS_ID)
        command += ' WHERE id="{id}"'.format (id=id)

        self.cursor.execute (command)

        command = 'INSERT INTO "{channel}" '.format (channel=Database.CREDENTIALS_ID)
        command += '(id, value) '
        command += 'values (?, ?)'

        params = []
        params.append (id)
        params.append (self.encryption.encrypt (value, self.password))

        self.cursor.execute (command, params)

        self.connection.commit ()

    #
    # Read credentials
    #
    def get_credential (self, id):

        command = 'SELECT * FROM "{channel}"'.format (channel=Database.CREDENTIALS_ID)
        command += ' WHERE id="{id}"'.format (id=id)

        rows = list (self.cursor.execute (command))
        assert len (rows) < 2

        return self.encryption.decrypt (rows[0][1], self.password) if rows else None

    #
    # Return list of credential ids present in the database
    #
    def get_all_credential_ids (self):

        command = 'SELECT * FROM "{channel}"'.format (channel=Database.CREDENTIALS_ID)

        rows = self.cursor.execute (command)

        return [row[0] for row in rows]


#--------------------------------------------------------------------------
# Database functions
#

#
# List content of database tables
#
def database_list (args):

    database = Database (args.database, args.password)
    channels = database.get_all_channels ()

    ids = [id.strip () for id in args.list.split (',')]

    for channel in channels:
        if channel.id in ids or 'all' in ids:

            entries = database.get (channel.id)
            frame = pd.DataFrame (columns=['timestamp', 'value'], index=range (len (entries)))

            for entry in entries:
                frame.loc[len (frame)] = [entry.timestamp, entry.value]

            core.common.print_frame ('{0} [{1}]'.format (channel.id, channel.description), frame)
            print ('')


#
# Print database summary
#
def database_summary (args):

    database = Database (args.database, args.password)
    frame = pd.DataFrame (columns=['id', 'description', 'type', 'entries', 'last value', 'start time', 'end time'])

    for channel in database.get_all_channels ():
        entries = database.get (channel.id)
        entries.sort (key=lambda entry: entry.timestamp)

        frame.loc[len (frame)] = [channel.id,
                                  channel.description,
                                  channel.type.__name__,
                                  len (entries),
                                  entries[-1].value if channel.type is float else '<text>',
                                  entries[0].timestamp,
                                  entries[-1].timestamp]

    core.common.print_frame ('Channels', frame)

    print ('')
    print ('Credentials')
    print ('-----------')

    for cred in sorted (database.get_all_credential_ids ()):
        print (cred)


#--------------------------------------------------------------------------
# MAIN
#
if __name__ == '__main__':

    #
    # Parse command line arguments
    #
    parser = argparse.ArgumentParser ()

    parser.add_argument ('-l', '--list',     action='store', default=False, help='List database channel content')
    parser.add_argument ('-s', '--summary',  action='store_true', default=False, help='Print database summary')
    parser.add_argument ('-p', '--password', type=str, default=None, help='Passwort for database encryption')
    parser.add_argument ('database',         type=str, default=None, help='Database file')

    args = parser.parse_args ()
    assert args.database is not None

    database = Database (args.database, args.password)

    if args.summary:
        database_summary (args)

    elif args.list:
        database_list (args)
