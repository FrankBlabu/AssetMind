#!/usr/bin/python3
#
# rss.py - Scraper for RSS feeds
#
# Frank Blankenburg, Jun. 2017
#

import argparse
import codecs
import feedparser
import time

from database.database import Database
from database.database import NewsEntry

#--------------------------------------------------------------------------
# Scraper adding data extracted from RSS feeds to the database
#
class RSSScraper:

    def __init__ (self):
        pass

    def execute (self, database, args):

        feed = feedparser.parse (args.url)

        for post in feed.entries:
            print (post.title.encode ('utf-8'))

        #database.commit ()


#--------------------------------------------------------------------------
# MAIN
#
if __name__ == '__main__':

    def to_date (s):
        try:
            return time.strptime (s, '%Y-%m-%d')
        except ValueError:
            raise argparse.ArgumentTypeError ('Not a valid date: {0}'.format (s))

    #
    # Parse command line arguments
    #
    parser = argparse.ArgumentParser ()
    parser.add_argument ('-s', '--start',    required=False, type=to_date, help='Start date (YYYY-MM-DD)')
    parser.add_argument ('-e', '--end',      required=False, type=to_date, help='End date (YYYY-MM-DD)')
    parser.add_argument ('-d', '--database', required=False, type=str, default=':memory:', help='Database file')
    parser.add_argument ('-v', '--verbose',  action='store_true', default=False, help='Verbose output')
    parser.add_argument ('url',             help='RSS feed url')

    args = parser.parse_args ()

    database = Database (args.database)

    if args.database == ':memory:':
        database.create ()

    scraper = RSSScraper ()
    scraper.execute (database, args)
