#!/usr/bin/python3
#
# acquirer.py - Data acquiring algorithmn for filling the database
#
# Frank Blankenburg, Jun. 2017
#

import argparse
import dateutil.parser

import scraper.cryptocompare
import scraper.twitter

from database.database import Database

class Acquirer:

    #
    # Earliest date captured in the database. The scrapers will try to gather as much data as
    # possible starting from this point in time on to the current time.
    #
    DATABASE_START_DATE = '2015-1-1'

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

        start = dateutil.parser.parse (Acquirer.DATABASE_START_DATE)

        print ('Filling/completing database from the {date} on'.format (date=start.isoformat (' ')))

        for source in self.sources:
            print ('Running {scraper}...'.format (scraper=type (source).__name__))
            source.run (database, start.timestamp (), lambda message: print ('  ' + message))


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
