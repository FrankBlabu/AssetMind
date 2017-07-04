#!/usr/bin/python3
#
# generator.py - Data sequence from database content generator
#
# Frank Blankenburg, Jul. 2017
#

import argparse

from database.database import Database
from database.database import Entry

#----------------------------------------------------------------------------
# CLASS Generator
#
class Generator:

    def __init__ (self, database):
        self.database = database
        self.channels = []

        #
        # Scan database for numeric stream like content
        #
        for database_type in Database.types:

            #
            # Only entries which do not represent pure administrative records are displayed
            #
            if database_type.get_data_type () is Entry.Type.numeric:

                entries = database.get_entries (database_type.ID)

                for entry_id in set (map (lambda entry: entry.id, entries)):
                    self.channels.append ((database_type, entry_id))

        self.channels = sorted (self.channels)

        print (self.channels)



#----------------------------------------------------------------------------
# MAIN
#
if __name__ == '__main__':
    parser = argparse.ArgumentParser ()
    parser.add_argument ('-e', '--epochs', type=int, default=1, help='Number of training epochs')
    parser.add_argument ('-s', '--sequence', type=int, default=50, help='Training sequence length')
    parser.add_argument ('-p', '--password', type=str, default=None, help='Passwort for database encryption')
    parser.add_argument ('-v', '--verbose',  action='store_true', default=False, help='Verbose output')
    parser.add_argument ('database', type=str, default=':memory:', help='Database file')

    args = parser.parse_args ()

    database = Database (args.database, args.password)

    generator = Generator (database)
