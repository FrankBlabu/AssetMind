#!/usr/bin/python3
#
# acquirer.py - Data acquiring algorithmn for filling the database
#
# Frank Blankenburg, Jun. 2017
#

import argparse

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
    DATABASE_START_DATE = Timestamp ('2015-1-1')

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
    def run (self, database):

        start = Acquirer.DATABASE_START_DATE

        print ('Filling/completing database from the {date} on'.format (date=start))

        for source in self.sources:
            print ('Running {scraper}...'.format (scraper=source.name))
            source.run (database, start, lambda message: print ('  ' + message))


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
    acquirer.add_source (scraper.twitter.TwitterScraper ())

    acquirer.run (database)
