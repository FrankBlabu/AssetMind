#!/usr/bin/python3
#
# acquirer.py - Data acquiring algorithmn for filling the database
#
# Frank Blankenburg, Jun. 2017
#

import argparse

from scraper.scraper import Scraper
import scraper.cryptocompare
import scraper.twitter

from database.database import Database
from core.time import Timestamp

#
# This class is controlling the whole data acquisition. Its task is to trigger the registered
# scrapers to fill the database for a specified time frame with as much data as they can
# get.
#
class Acquirer:

    #
    # Earliest date captured in the database. The scrapers will try to gather as much data as
    # possible starting from this point in time on to the current time.
    #
    DATABASE_START_DATE = Timestamp ('2012-1-1')

    def __init__ (self):
        self.sources = []

    #
    # Add a scraper instance to the sources to be used to fill / complete the database
    #
    def add_source (self, source):
        self.sources.append (source)

    #
    # Run scraping process
    #
    # This function will try to fill the database as complete as possible
    #
    def run (self, database, start=DATABASE_START_DATE, end=Timestamp ()):

        assert isinstance (start, Timestamp)
        assert isinstance (end, Timestamp)
        assert start != end

        for source in self.sources:
            #
            # Query database for all points in time this scraper (or any other filling the
            # same database slots) already got data for. Afterwards, the set of timestamps
            # will contain entries for all points in time where the scraper provided
            # complete data. If any id has missing content, we assume to be a hole there
            # because the scraper might only be able to retrieve the data in a block for all
            # ids.
            #
            timestamps = None

            for id in source.ids:
                entries = database.get_entries (source.type_id, id)

                if timestamps is None:
                    timestamps = set ([entry.timestamp for entry in entries])
                else:
                    timestamps &= set ([entry.timestamp for entry in entries])

            #
            # Compute interval (first missing and last missing entry) which is still
            # in need of data
            #
            source_start = start
            source_end = end

            while source_start < source_end and source_start in timestamps:
                source_start.advance (hours=1)

            while source_end > source_start and source_end in timestamps:
                source_end.advance (hours=-1)

            if source_start != source_end or source_start not in timestamps:
                source.run (database, source_start, source_end, Scraper.Interval.HOURLY, None)


#----------------------------------------------------------------------------
# MAIN
#
if __name__ == '__main__':
    #
    # Parse command line arguments
    #
    parser = argparse.ArgumentParser ()

    parser.add_argument ('-p', '--password', type=str, default=None, help='Passwort for database encryption')
    parser.add_argument ('database', type=str, default=':memory:', help='Database file')

    args = parser.parse_args ()

    database = Database (args.database, args.password)
    if args.database == ':memory:':
        database.create ()

    acquirer = Acquirer ()

    acquirer.add_source (scraper.cryptocompare.CryptoCompareScraper ())
    #acquirer.add_source (scraper.twitter.TwitterScraper ())

    acquirer.run (database)
