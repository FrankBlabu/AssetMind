#!/usr/bin/python3
#
# gold_csv.py - Import gold price in CSV format
#
# Gold price obtained from: http://data.okfn.org/data/core/gold-prices#data
#

import argparse
import pandas as pd
import time

from database.database import Database
from database.database import StockEntry

#--------------------------------------------------------------------------
# Local functions
#

#
# Convert date in string format into seconds since epoch
#
def to_date (s):
    try:
        return int (round (time.mktime (time.strptime (s, '%Y-%m-%d'))))
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
    parser.add_argument ('-i', '--id',       type=str, required=True, help='Stock or index id')
    parser.add_argument ('file',             type=str, default=None, help='CSV file to import')

    args = parser.parse_args ()

    database = Database (args.database)

    if args.database == ':memory:':
        database.create ()

    data = pd.read_csv (args.file, header=0)

    for i in range (len (data)):
        row = data.ix[i]

        entry = StockEntry (to_date (row['date']), args.id.lower (), float (row['price']))
        database.add (entry)

    database.commit ()
