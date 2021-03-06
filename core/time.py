#!/usr/bin/python3
#
# time.py - Time related classes and functions
#
# Frank Blankenburg, Jun. 2017
#

import copy
import dateutil.parser
import pandas as pd

from core.config import Configuration
from core.common import Interval
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

        if Configuration.DATABASE_SAMPLING_INTERVAL is Interval.day:
            self.timestamp = self.timestamp.replace (hour=0, minute=0, second=0, microsecond=0)
        elif Configuration.DATABASE_SAMPLING_INTERVAL is Interval.hour:
            self.timestamp = self.timestamp.replace (minute=0, second=0, microsecond=0)
        elif Configuration.DATABASE_SAMPLING_INTERVAL is Interval.minute:
            self.timestamp = self.timestamp.replace (second=0, microsecond=0)
        else:
            raise RuntimeError ('Unhandled sampling interval')

    #
    # Advance time by some days / hours
    #
    def advance (self, days=None, hours=None, step=None):

        if step is not None:
            assert isinstance (step, timedelta)
            self.timestamp += step

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
    # Create deep copy of this object
    #
    def copy (self):
        return copy.deepcopy (self)

    #
    # Return timestamp in UNIX epoch seconds
    #
    def epoch (self):
        return int (round (self.timestamp.timestamp ()))

    #
    # Return current time (in UTC)
    #
    @staticmethod
    def now ():
        return Timestamp ()

    #
    # Convert timestamp into a string matching the given format
    #
    # @param format Format the time is converted to (like '%Y-%m-%d')
    #
    def to_string (self, format):
        return self.timestamp.strftime (format)

    def to_pandas (self):
        return pd.Timestamp (self.timestamp)

    def __add__ (self, other):
        assert isinstance (other, timedelta)
        return Timestamp (self.timestamp + other)

    def __sub__ (self, other):
        assert isinstance (other, Timestamp)
        return self.timestamp - other.timestamp

    def __lt__ (self, other):
        return self.timestamp < other.timestamp

    def __le__ (self, other):
        return self.timestamp <= other.timestamp

    def __eq__ (self, other):
        return self.timestamp == other.timestamp

    def __ne__ (self, other):
        return self.timestamp != other.timestamp

    def __ge__ (self, other):
        return self.timestamp >= other.timestamp

    def __gt__ (self, other):
        return self.timestamp > other.timestamp

    def __hash__ (self):
        return int (round (self.timestamp.timestamp ()))

    def __repr__ (self):
        if Configuration.DATABASE_SAMPLING_INTERVAL is Interval.day:
            return self.timestamp.strftime ('%Y-%m-%d')
        elif Configuration.DATABASE_SAMPLING_INTERVAL is Interval.hour:
            return self.timestamp.strftime ('%Y-%m-%d %Hh')
        elif Configuration.DATABASE_SAMPLING_INTERVAL is Interval.minute:
            return self.timestamp.strftime ('%Y-%m-%d %H:%M')

        raise RuntimeError ('Unhandled sampling interval')
