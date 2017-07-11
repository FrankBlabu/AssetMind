#!/usr/bin/python3
#
# test_time.py - Unittest
#
# Frank Blankenburg, Jun. 2017
#

import unittest

from core.common import Interval
from core.config import Configuration
from core.time import Timestamp
from datetime import datetime
from datetime import timedelta


#--------------------------------------------------------------------------
# CLASS TestTimestamp
#
class TestTimestamp (unittest.TestCase):

    Configuration.DATABASE_SAMPLING_INTERVAL = Interval.hour

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

        s = Timestamp ('2017-04-21 14h')
        s.advance (hours=+2)
        self.assertEqual (s, Timestamp ('2017-04-21 16h'))

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

        s = Timestamp ('2017-02-19 22:00')
        s.advance (step=timedelta (hours=+1))
        self.assertEqual (s, Timestamp ('2017-02-19 23:00'))
        s.advance (step=timedelta (hours=+1))
        self.assertEqual (s, Timestamp ('2017-02-20 00:00'))
        s.advance (step=timedelta (hours=+1))
        self.assertEqual (s, Timestamp ('2017-02-20 01:00'))
        s.advance (step=timedelta (hours=-1))
        self.assertEqual (s, Timestamp ('2017-02-20 00:00'))
        s.advance (step=timedelta (hours=-1))
        self.assertEqual (s, Timestamp ('2017-02-19 23:00'))
