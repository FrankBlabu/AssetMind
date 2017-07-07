#!/usr/bin/python3
#
# acquirer.py - Data acquiring algorithmn for filling the database
#
# Frank Blankenburg, Jun. 2017
#

import argparse
import scraper

from core.time import Timestamp
from core.config import Configuration
from database.database import Database
from scraper.scraper import ScraperRegistry

#
# This class is controlling the whole data acquisition. Its task is to trigger the registered
# scrapers to fill the database for a specified time frame with as much data as they can
# get.
#
class Acquirer:

    def __init__ (self):
        pass

    #
    # Run scraping process
    #
    # This function will try to fill the database as complete as possible
    #
    def run (self, database, start=Timestamp (Configuration.DATABASE_START_DATE), end=Timestamp (), log=None):

        assert isinstance (start, Timestamp)
        assert isinstance (end, Timestamp)
        assert start != end

        def add_to_log (text):
            if log is not None:
                log (text)

        add_to_log ('Starting database acquistion')

        for scraper in ScraperRegistry.get_all ():
            #
            # Query database for all points in time this scraper (or any other filling the
            # same database slots) already got data for. Afterwards, the set of timestamps
            # will contain entries for all points in time where the scraper provided
            # complete data. If any id has missing content, we assume to be a data hole there
            # because the scraper might only be able to retrieve the data in a block for all
            # ids.
            #
            timestamps = None

            add_to_log ('  Processing scraper \'\''.format (scraper.name))

            for channel in scraper.get_channels ():
                entries = database.get (channel.id)

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
                source_start.advance (step=+Configuration.DATABASE_SAMPLING_STEP)

            while source_end > source_start and source_end in timestamps:
                source_end.advance (step=-Configuration.DATABASE_SAMPLING_STEP)

            add_to_log ('    Scraping in time interval \'{start}\' to \'{end}\''
                        .format (start=source_start, end=source_end))

            if source_start != source_end or source_start not in timestamps:
                source.run (database, source_start, source_end, Configuration.DATABASE_SAMPLING_INTERVAL,
                            lambda text: add_to_log ('    {0}: {1}'.format (source.name, text)))


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
    acquirer.run (database, log=lambda text: print (text))
