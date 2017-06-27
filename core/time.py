#!/usr/bin/python3
#
# time.py - Time related classes and functions
#
# Frank Blankenburg, Jun. 2017
#

import dateutil.parser
import pandas as pd
import time
import unittest

from datetime import datetime
from datetime import timedelta

#--------------------------------------------------------------------------
# CLASS core.time.Timestamp
#
# Timestamp representing object. The resolution is one hour and the internal
# timezone offset is always GMT.
#
class Timestamp:

    #
    # Create timestamp from
    #
    # @param value    Time representing value. Can be an integer for 'seconds since epoch',
    #                 a string or any other time representation
    # @param timezone Hourly timezone offset of the time value. 0 is GMT.
    #
    def __init__ (self, value, timezone=0):

        if isinstance (value, int):
            self.epoch = value
        elif isinstance (value, float):
            self.epoch = int (round (value))
        elif isinstance (value, str):
            self.epoch = int (round (dateutil.parser.parse (value).timestamp ()))
        elif isinstance (value, time.struct_time):
            self.epoch = int (time.mktime (value))
        elif isinstance (value, datetime.datetime):
            self.epoch = int (value.timestamp ())
        elif isinstance (value, Timestamp):
            self.epoch = value
        else:
            raise RuntimeError ('Unhandled time format type \'{typename}\''. format (typename=type (value).__name__))

        t = datetime.fromtimestamp (self.epoch)
        t = t.replace (minute=0, second=0)
        self.epoch = int (round (t.timestamp ()))

    #
    # Advance time by some days / hours
    #
    def advance (self, days=None, hours=None):

        if days is not None:
            delta = timedelta (days=abs (days))
            if days >= 0:
                self.epoch += delta.total_seconds ()
            else:
                self.epoch -= delta.total_seconds ()

        if hours is not None:
            delta = timedelta (hours=abs (hours))
            if hours >= 0:
                self.epoch += delta.total_seconds ()
            else:
                self.epoch -= delta.total_seconds ()

    #
    # Convert timestamp into a string matching the given format
    #
    # @param format Format the time is converted to (like '%Y-%m-%d')
    #
    def to_string (self, format):
        return time.strftime (format, time.gmtime (self.epoch))

    def to_pandas (self):
        return pd.Timestamp (time.ctime (self.epoch))

    def __lt__ (self, other):
        return self.epoch < other.epoch

    def __eq__ (self, other):
        return self.epoch == other.epoch

    def __hash__ (self):
        return self.epoch

    def __repr__ (self):
        return time.strftime ('%Y-%m-%d %Hh', time.gmtime (self.epoch))


#--------------------------------------------------------------------------
# Unittests
#
class TestTimestamp (unittest.TestCase):

    def test_timestamp_create (self):

        t = time.localtime ()

        s1 = Timestamp (t)
        s2 = Timestamp (time.mktime (t))

        self.assertEqual (s1, s2)

        s1 = Timestamp ('2017-04-21 14:00')
        s2 = Timestamp ('2017-04-21 14:30')
        s3 = Timestamp ('2017-04-21 14:59')
        s4 = Timestamp ('2017-04-21 15:00')
        s5 = Timestamp ('2017-04-22 14:30')

        self.assertEqual (s1, s2)
        self.assertEqual (s1, s3)
        self.assertNotEqual (s1, s4)
        self.assertNotEqual (s1, s5)

    def test_timestamp_advance (self):

        s = Timestamp ('2017-04-21 14:00')
        s.advance (hours=+2)
        self.assertEqual (s, Timestamp ('2017-04-21 16:00'))

        s = Timestamp ('2017-04-21 14:00')
        s.advance (hours=-3)
        self.assertEqual (s, Timestamp ('2017-04-21 11:00'))

        s = Timestamp ('2017-02-17 01:00')
        s.advance (hours=-2)
        self.assertEqual (s, Timestamp ('2017-02-16 23:00'))

        s = Timestamp ('2017-02-17 23:00')
        s.advance (hours=+2)
        self.assertEqual (s, Timestamp ('2017-02-18 01:00'))

        s = Timestamp ('2017-02-17 23:00')
        s.advance (days=+3, hours=+2)
        self.assertEqual (s, Timestamp ('2017-02-21 01:00'))

        s = Timestamp ('2017-02-17 23:00')
        s.advance (days=-5, hours=+2)
        self.assertEqual (s, Timestamp ('2017-02-13 01:00'))
