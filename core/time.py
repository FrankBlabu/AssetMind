#!/usr/bin/python3
#
# time.py - Time related classes and functions
#
# Frank Blankenburg, Jun. 2017
#

import datetime
import dateutil.parser
import time
import unittest

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
            self.epoch = epoch
        elif isinstance (value, float):
            self.epoch = int (round (value))
        elif isinstance (value, str):
            self.epoch = int (round (dateutil.parser.parse (value).timestamp ()))
        elif isinstance (value, time.struct_time):
            self.epoch = time.mktime (value)
        elif isinstance (value, datetime.datetime):
            self.epoch = value.timestamp ()
        else:
            raise RuntimeError ('Unhandled time format type \'{typename}\''. format (typename=type (value).__name__))

        t = time.gmtime (self.epoch)
        t = time.struct_time ((t.tm_year, t.tm_mon, t.tm_mday, t.tm_hour, 0, 0, t.tm_wday, t.tm_yday, t.tm_isdst))

        self.epoch = int (round (time.mktime (t)))


    def __eq__ (self, other):
        t1 = time.gmtime (self.epoch)
        t2 = time.gmtime (other.epoch)

        return t1.tm_year == t2.tm_year and \
        t1.tm_mon == t2.tm_mon and \
        t1.tm_mday == t2.tm_mday and \
        t1.tm_hour == t2.tm_hour


    def __repr__ (self):
        return time.strftime ('%Y-%m-%d %Hh', time.gmtime (self.epoch))


#--------------------------------------------------------------------------
# Unittests
#
class TestTimestamp (unittest.TestCase):

    def test_timestamp (self):

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
