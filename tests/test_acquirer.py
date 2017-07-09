#!/usr/bin/python3
#
# test_acquirer.py - Test for the acquirer class
#
# Frank Blankenburg, Jun. 2017
#

import unittest
import scraper.init

from datetime import timedelta

from database.database import Database
from database.database import Channel
from database.database import Entry
from core.acquirer import Acquirer
from core.config import Configuration
from core.common import Interval
from core.time import Timestamp
from scraper.scraper import Scraper
from scraper.scraper import ScraperRegistry


#--------------------------------------------------------------------------
# CLASS TestScraper
#
class TestScraper (Scraper):

    ID = 'Test'

    def __init__ (self):
        super ().__init__ (TestScraper.ID)

        self.refresh = (None, None)

    def get_channels (self):

        channels = []

        channels.append (Channel (id='{scraper}::TST'.format (scraper=TestScraper.ID),
                                  description='Test channel', type_id=float, encrypted=False))

        return channels


    def run (self, database, start, end, interval, log):
        self.refresh = (start, end)

#--------------------------------------------------------------------------
# CLASS TestAcquirer
#
class TestAcquirer (unittest.TestCase):

    Configuration.DATABASE_SAMPLING_INTERVAL = Interval.hour
    Configuration.DATABASE_SAMPLING_STEP = timedelta (hours=1)
    ScraperRegistry.register (TestScraper ())

    #
    # Test if the acquirer detects the correct gaps in the sampled timestamps
    #
    def test_gap_detection (self):

        database = Database (':memory:')

        entries = []

        entries.append (Entry (Timestamp ('2017-08-12 14:00'), 10.0))
        entries.append (Entry (Timestamp ('2017-08-12 15:00'), 12.0))
        entries.append (Entry (Timestamp ('2017-08-12 16:00'), 14.0))
        entries.append (Entry (Timestamp ('2017-08-12 17:00'), 14.0))

        database.add ('Test::TST', entries)

        scraper = TestScraper ()

        acquirer = Acquirer ()

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
