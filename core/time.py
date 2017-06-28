#!/usr/bin/python3
#
# time.py - Time related classes and functions
#
# Frank Blankenburg, Jun. 2017
#

import dateutil.parser
import pandas as pd
import time

from datetime import datetime
from datetime import timedelta

#--------------------------------------------------------------------------
# CLASS core.time.Timestamp
#
# Timestamp representing object. The resolution is one hour and the internal
# timezone offset is always UTC.
#
class Timestamp:

    #
    # Create timestamp from generic input value
    #
    # @param value Time representing value. Can be an integer for 'seconds since epoch',
    #              a string or any other time representation. 'None' will use the
    #              current time instead,
    #
    def __init__ (self, value=None):

        if value is None:
            self.timestamp = datetime.utcnow ()
        elif isinstance (value, int):
            self.timestamp = datetime.fromtimestamp (value)
        elif isinstance (value, float):
            self.timestamp = datetime.fromtimestamp (int (round (value)))
        elif isinstance (value, str):
            self.timestamp = dateutil.parser.parse (value)
        elif isinstance (value, datetime):
            self.timestamp = value
        elif isinstance (value, Timestamp):
            self.timestamp = value.timestamp
        else:
            raise RuntimeError ('Unhandled time format type \'{typename}\''. format (typename=type (value).__name__))

        self.timestamp = self.timestamp.replace (minute=0, second=0, microsecond=0)

    #
    # Advance time by some days / hours
    #
    def advance (self, days=None, hours=None):

        if days is not None:
            delta = timedelta (days=abs (days))
            if days >= 0:
                self.timestamp += delta
            else:
                self.timestamp -= delta

        if hours is not None:
            delta = timedelta (hours=abs (hours))
            if hours >= 0:
                self.timestamp += delta
            else:
                self.timestamp -= delta

    #
    # Return current time (in UTC)
    #
    @staticmethod
    def now ():
        return Timestamp (datetime.utcnow ())

    #
    # Convert timestamp into a string matching the given format
    #
    # @param format Format the time is converted to (like '%Y-%m-%d')
    #
    def to_string (self, format):
        return self.timestamp.strftime (format)

    def to_pandas (self):
        return pd.Timestamp (self.timestamp)

    def __lt__ (self, other):
        return self.timestamp < other.timestamp

    def __eq__ (self, other):
        return self.timestamp == other.timestamp

    def __hash__ (self):
        return int (round (self.timestamp.timestamp ()))

    def __repr__ (self):
        return self.timestamp.strftime ('%Y-%m-%d %Hh')
