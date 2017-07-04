#!/usr/bin/python3
#
# content.py - Show database content as a completeness displaying diagram
#
# Frank Blankenburg, Jun. 2017
#

import argparse
import copy
import numpy as np
import math
import matplotlib as mpl
import matplotlib.pyplot as plt
import matplotlib.colors
import pandas as pd
import time

from datetime import timedelta

from core.config import Configuration
from core.common import Interval
from core.time import Timestamp
from database.database import Database
from database.database import Entry


#----------------------------------------------------------------------------
# CLASS TimestampFormatter
#
class TimestampFormatter (mpl.ticker.OldScalarFormatter):

    def __init__ (self, start, step):
        self.start = start
        self.step = step

    def __call__ (self, x, pos=None):

        t = copy.copy (self.start)
        t.advance (step=int (x) * step)

        return t

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
    number_of_steps = 0
    step = None

    if Configuration.DATABASE_SAMPLING_INTERVAL is Interval.day:
        number_of_steps = abs (diff.days)
        step = timedelta (days=1)
    elif Configuration.DATABASE_SAMPLING_INTERVAL is Interval.hour:
        number_of_steps = int (math.floor (abs (diff.days) * 24 + abs (diff.seconds) / 60 / 60))
        step = timedelta (hours=1)
    elif Configuration.DATABASE_SAMPLING_INTERVAL is Interval.minute:
        number_of_steps = int (math.floor (abs (diff.days) * 24 * 60 + abs (diff.seconds) / 60))
        step = timedelta (minutes=1)

    #
    # Count the number of different entries in the database
    #
    ids = []

    for t in Database.types:

        #
        # Only entries which do not represent pure administrative records are displayed
        #
        if t.get_data_type () is not Entry.Type.administration:
            entries = database.get_entries (t.ID)
            entry_ids = set (map (lambda entry: entry.id, entries))

            ids.extend (list (zip (len (entry_ids) * [t.ID], list (entry_ids))))

    ids = sorted (ids)

    #
    # Build array showing the sampling state
    #
    state = np.zeros ((len (ids), number_of_steps))

    for y in range (len (ids)):

        table_id = ids[y][0]
        entry_id = ids[y][1]
        t = minimum_timestamp

        entries = database.get_entries (table_id)

        timestamps = []
        for entry in entries:
            if entry.id == entry_id:
                timestamps.append (entry.timestamp)

        timestamps = set (timestamps)

        for x in range (number_of_steps):
            state[y][x] = 1.0 if t in timestamps else 0.0
            t += step
    #
    # Setup custom discrete colormap
    #
    cmap = mpl.colors.ListedColormap (['white', 'green'])
    norm = mpl.colors.BoundaryNorm ([0, 0.5, 1.0], cmap.N)

    fig, axis = plt.subplots ()

    #
    # Configure diagram axes
    #
    axis.set_xlabel ('Timestamp')
    axis.set_ylabel ('Dataset')
    axis.xaxis.set_major_formatter (TimestampFormatter (minimum_timestamp, step))

    plt.yticks (range (len (ids)), ['{0} ({1})'.format (id[1], id[0]) for id in ids], rotation='horizontal')

    #
    # Display content diagram
    #
    plt.imshow (state, interpolation='nearest', cmap=cmap, norm=norm, aspect='auto')
    plt.colorbar ()

    fig.tight_layout ()
    fig.autofmt_xdate ()

    def onresize (event):
        plt.tight_layout ()

    fig.canvas.mpl_connect ('resize_event', onresize)

    plt.show ()
