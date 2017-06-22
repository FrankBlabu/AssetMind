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

class Acquirer:

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
    def run (self, args):

        for source in self.sources:
            source.run (None, None)


if __name__ == '__main__':
    #
    # Parse command line arguments
    #
    parser = argparse.ArgumentParser ()

    parser.add_argument ('-p', '--password', type=str, default=None, help='Passwort for database encryption')
    parser.add_argument ('database', type=str, default=':memory:', help='Database file')

    args = parser.parse_args ()

    acquirer = Acquirer ()

    acquirer.add_source (scraper.cryptocompare.CryptoCompareScraper ())
    acquirer.add_source (scraper.twitter.TwitterScraper ())

    acquirer.run (args)
