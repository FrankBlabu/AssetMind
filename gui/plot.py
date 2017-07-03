#!/usr/bin/python3
#
# plot.py - Plot numeric database content
#
# Frank Blankenburg, Jun. 2017
#

import argparse
import matplotlib.pyplot as plt

from database.database import Database
from database.database import CoinEntry

#
# Plot database content
#
def plot (database):

    entries =  database.get_entries (CoinEntry.ID)

    parts = {}

    for entry in entries:

        if entry.id not in parts:
            parts[entry.id] = []

        parts[entry.id].append ((entry.timestamp, entry.course))

    count = 1
    for coin in sorted (list (parts.keys ())):
        plt.subplot (len (coins), 1, count)
        plt.plot ([m[0] for m in parts[coin]], [m[1] for m in parts[coin]])
        plt.ylabel (coin)

        count += 1

    plt.show ()


#--------------------------------------------------------------------------
# MAIN
#
if __name__ == '__main__':

    #
    # Parse command line arguments
    #
    parser = argparse.ArgumentParser ()
    parser.add_argument ('database',  type=str, default=None, help='Database file')

    args = parser.parse_args ()

    assert args.database is not None

    database = Database (args.database)
    plot (database)
