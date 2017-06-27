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
    def __init__ (self, name, entry_type, handled_ids):

        assert isinstance (handled_ids, list)

        self.name = name
        self.entry_type = entry_type
        self.handled_ids = handled_ids

    #
    # Run scraper for acquiring a set of entries
    #
    # @param database Database to be filled
    # @param start    Start timestamp in UNIX epoch format
    # @param log      Callback for logging outputs
    #
    @abstractmethod
    def run (self, database, start, log):
        pass

#
# Exception thrown by the scraper implementation if anything went wrong
#
class ScraperException (Exception):

    def __init__ (self, message):
        super ().__init__ (message)
