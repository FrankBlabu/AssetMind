#!/usr/bin/python3
#
# scraper.py - Base class for all scrapers
#
# Frank Blankenburg, Jun. 2017
#

from abc import ABC, abstractmethod

#
# Abstract base class for all data scrapers
#
class Scraper (ABC):

    #
    # Constructor
    #
    # @param name Printable name of the scraper for logging outputs
    #
    def __init__ (self, id):
        self.id = id

    #
    # Run scraper for acquiring a set of entries
    #
    # @param database Database to be filled
    # @param ids      List of ids to scrape
    # @param start    Start timestamp (UTC)
    # @param end      End timestamp (UTC)
    # @param interval Interval of scraping
    # @param log      Callback for logging outputs
    #
    @abstractmethod
    def run (self, database, ids, start, end, interval, log):
        pass

#
# Registry for all scraper instances to be used
#
class ScraperRegistry:

    #
    # Registered scrapers
    #
    scrapers = {}

    @staticmethod
    def register (scraper, id):
        ScraperRegistry.scrapers[id] = scraper

    @staticmethod
    def get (id):
        return ScraperRegistry.scrapers[id] if id in ScraperRegistry.scrapers else None

#
# Exception thrown by the scraper implementation if anything went wrong
#
class ScraperException (Exception):

    def __init__ (self, message):
        super ().__init__ (message)
