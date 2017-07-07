#!/usr/bin/python3
#
# test_acquirer.py - Test for the acquirer class
#
# Frank Blankenburg, Jun. 2017
#

import unittest

from datetime import timedelta

from database.database import Database
from database.database import CoinEntry
from core.acquirer import Acquirer
from core.config import Configuration
from core.common import Interval
from core.time import Timestamp
from scraper.scraper import Scraper


#--------------------------------------------------------------------------
# CLASS TestScraper
#
class TestScraper (Scraper):

    def __init__ (self):
        super ().__init__ ('TestScraper', CoinEntry.ID, ['TST'])

        self.refresh = (None, None)

    def run (self, database, start, end, interval, log):
        self.refresh = (start, end)

#--------------------------------------------------------------------------
# CLASS TestAcquirer
#
class TestAcquirer (unittest.TestCase):

    #
    # Test if the acquirer detects the correct gaps in the sampled timestamps
    #
    def test_gap_detection (self):

        Configuration.DATABASE_SAMPLING_INTERVAL = Interval.hour
        Configuration.DATABASE_SAMPLING_STEP = timedelta (hours=1)


        database = Database (':memory:')

        database.add (CoinEntry ('2017-08-12 14:00', 'TST', 'test', 10.0, 'usd'))
        database.add (CoinEntry ('2017-08-12 15:00', 'TST', 'test', 12.0, 'usd'))
        database.add (CoinEntry ('2017-08-12 16:00', 'TST', 'test', 14.0, 'usd'))
        database.add (CoinEntry ('2017-08-12 17:00', 'TST', 'test', 14.0, 'usd'))

        database.commit ()

        scraper = TestScraper ()

        acquirer = Acquirer ()
        acquirer.add_source (scraper)

        scraper.refresh = (None, None)
        acquirer.run (database, Timestamp ('2017-08-12 12:00'), Timestamp ('2017-08-12 16:00'))
        self.assertEqual (scraper.refresh[0], Timestamp ('2017-08-12 12:00'))
        self.assertEqual (scraper.refresh[1], Timestamp ('2017-08-12 13:00'))

        scraper.refresh = (None, None)
        acquirer.run (database, Timestamp ('2017-08-12 12:00'), Timestamp ('2017-08-12 17:00'))
        self.assertEqual (scraper.refresh[0], Timestamp ('2017-08-12 12:00'))
        self.assertEqual (scraper.refresh[1], Timestamp ('2017-08-12 13:00'))

        scraper.refresh = (None, None)
        acquirer.run (database, Timestamp ('2017-08-12 14:00'), Timestamp ('2017-08-12 19:00'))
        self.assertEqual (scraper.refresh[0], Timestamp ('2017-08-12 18:00'))
        self.assertEqual (scraper.refresh[1], Timestamp ('2017-08-12 19:00'))

        scraper.refresh = (None, None)
        acquirer.run (database, Timestamp ('2017-08-12 14:00'), Timestamp ('2017-08-12 17:00'))
        self.assertEqual (scraper.refresh[0], None)
        self.assertEqual (scraper.refresh[1], None)
