#!/usr/bin/python3
#
# index_csv.py - Import stock index files in CSV format
#
# Obtained from: https://finance.yahoo.com/quote/%5EGDAXI/history?p=%5EGDAXI
#

import argparse
import pandas as pd
import time

from database.database import Database

#--------------------------------------------------------------------------
# MAIN
#
if __name__ == '__main__':

    def to_date (s):
        try:
            return time.strptime (s, '%Y-%m-%d')
        except ValueError:
            raise argparse.ArgumentTypeError ('Not a valid date: {0}'.format (s))

    #
    # Parse command line arguments
    #
    parser = argparse.ArgumentParser ()
    parser.add_argument ('-d', '--database', required=False, type=str, default=':memory:', help='Database file')
    parser.add_argument ('-v', '--verbose',  action='store_true', default=False, help='Verbose output')
    parser.add_argument ('file',             type=str, default=None, help='CSV file to import')

    args = parser.parse_args ()

    database = Database (args.database)

    if args.database == ':memory:':
        database.create ()

    data = pd.read_csv (args.file, header=0)

    print (data)

    for i in range (len (data)):
        row = data.ix[i]
