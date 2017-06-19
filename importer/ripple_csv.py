#!/usr/bin/python3
#
# ripple_csv.py - Import ripple course data from ripple chart database
#

import argparse
import pandas as pd
import time

from database.database import Database
from database.database import CoinEntry

#--------------------------------------------------------------------------
# Local functions
#

#
# Convert date in string format into seconds since epoch
#
def to_date (s):
    try:
        return int (round (time.mktime (time.strptime (s, '%Y-%m-%dT%H:%M:%SZ'))))
    except ValueError:
        raise argparse.ArgumentTypeError ('Not a valid date: {0}'.format (s))


#--------------------------------------------------------------------------
# MAIN
#
if __name__ == '__main__':

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

    for i in range (len (data)):
        row = data.ix[i]
        database.add (CoinEntry (to_date (row['start']) + time.timezone, 'xrp', 'xrpchart', (row['low'] + row['high']) / 2, row['counter_currency']))

    database.commit ()
