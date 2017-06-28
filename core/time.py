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


#--------------------------------------------------------------------------
# Unittests
#
class TestTimestamp (unittest.TestCase):

    def test_timestamp_create (self):

        #
        # Timestamp for 'now' must be always in UTC time
        #
        s1 = Timestamp ()
        s2 = Timestamp (datetime.utcnow ())
        s3 = Timestamp (datetime.utcnow ().timestamp ())

        self.assertEqual (s1, s2)
        self.assertEqual (s1, s3)

        s1 = Timestamp ('2017-04-21 14:00')
        s2 = Timestamp ('2017-04-21 14:30')
        s3 = Timestamp ('2017-04-21 14:59')
        s4 = Timestamp ('2017-04-21 15:00')
        s5 = Timestamp ('2017-04-22 14:30')

        #
        # Parsed dates must not be changes to other time zones
        #
        self.assertEqual (s1.timestamp.hour, 14)
        self.assertEqual (s2.timestamp.hour, 14)
        self.assertEqual (s3.timestamp.hour, 14)
        self.assertEqual (s4.timestamp.hour, 15)
        self.assertEqual (s5.timestamp.hour, 14)

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
