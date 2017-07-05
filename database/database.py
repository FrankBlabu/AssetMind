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
        self.connection.commit ()

        self.created = True


    #
    # Register new entry type
    #
    def register (self, id, description, type_id, encrypt=False):

        assert len (id) <= 64
        assert type_id is str or type_id is float
        assert len (type_id.__name__) <= 64

        #
        # Register type in administrative database
        #
        command = 'INSERT INTO "{id}" '.format (id=Database.ADMIN_ID)
        command += '(id, description, type, encrypted) '
        command += 'values (?, ?, ?, ?)'

        params = []
        params.append (id)
        params.append (description)
        params.append (type_id.__name__)
        params.append (encrypt)

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
        admin = self.get_admin_data (id)[0]
        encrypted = admin['encrypted']

        assert not encrypted or isinstance (self.password, str)
        assert not encrypted or len (self.password) >= 4

        if not isinstance (entries, list):
            entries = [entries]

        for entry in entries:

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

            if encrypted:
                params.append (encryption.encrypt (entry.value, database.password))
            else:
                params.append (entry.value)

            self.cursor.execute (command, params)

        self.connection.commit ()


    #
    # Return all entries in a table
    #
    def get (self, id):

        assert id is not Database.ADMIN_ID

        command = 'SELECT * FROM "{table}"'.format (table=id)
        rows = self.cursor.execute (command)

        return [Entry (hash=row[0], timestamp=Timestamp (row[1]), value=row[2]) for row in rows]

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

            if row[2] == str.__name__:
                type_id = str
            elif row[2] == float.__name__:
                type_id = float

            entries.append (Entry (id=row[0], description=row[1], type=type_id, encrypted=row[3]))

        return entries



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
    #parser.add_argument ('-l', '--list',     action='store', choices=[t.ID for t in Database.types] + ['all'], help='List database content')
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
