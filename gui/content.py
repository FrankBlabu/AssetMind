#!/usr/bin/python3
#
# content.py - Show database content as a completeness displaying diagram
#
# Frank Blankenburg, Jun. 2017
#
import argparse
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import time

from core.config import Configuration
from core.time import Timestamp
from database.database import Database


#----------------------------------------------------------------------------
# MAIN
#
if __name__ == '__main__':

    #
    # Parse command line arguments
    #
    parser = argparse.ArgumentParser ()

    parser.add_argument ('-p', '--password', type=str, default=None, help='Passwort for database encryption')
    parser.add_argument ('database', type=str, default=None, help='Database file')

    args = parser.parse_args ()

    database = Database (args.database, args.password)

    #
    # Fetch all entries from database and compute earliest entry. The latest entry is
    # always expected to be at the current date.
    #
    minimum_timestamp = Timestamp (Configuration.DATABASE_START_DATE)
    maximum_timestamp = Timestamp.now ()

    diff = maximum_timestamp - minimum_timestamp

    if Configuration.DATABASE_SAMPLING_INTERVAL is Interval.day:
        number_of_steps = abs (diff.days)
    elif Configuration.DATABASE_SAMPLING_INTERVAL is Interval.hour:
        number_of_steps = int (floor (abs (diff.days) * 24 + abs (diff.seconds) / 60 / 60))
    elif Configuration.DATABASE_SAMPLING_INTERVAL is Interval.minute:
        number_of_steps = int (floor (abs (diff.days) * 24 * 60 + abs (diff.seconds) / 60))

    for t in Database.types:

        entries = database.get_entries (t.ID)
        ids = set (map (lambda entry: entry.id, entries))

        for id in ids:
            id_entries = [e for e in entries if e.id == id]
            times = [t.timestamp for t in id_entries]

            minimum_timestamp = min (minimum_timestamp, min (times)) if minimum_timestamp is not None else min (times)

    print (minimum_timestamp, maximum_timestamp)
