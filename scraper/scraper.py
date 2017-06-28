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
    # @param name    Printable name of the scraper for logging outputs
    # @param type_id Id of the type used to keep the scrapers entries
    # @param ids     Entry ids handled by the scraper
    #
    def __init__ (self, name, type_id, ids):

        assert isinstance (type_id, str)
        assert isinstance (ids, list)

        self.name = name
        self.type_id = type_id
        self.ids = ids

    #
    # Run scraper for acquiring a set of entries
    #
    # @param database Database to be filled
    # @param start    Start timestamp (UTC)
    # @param end      End timestamp (UTC)
    # @param log      Callback for logging outputs
    #
    @abstractmethod
    def run (self, database, start, end, log):
        pass

#
# Exception thrown by the scraper implementation if anything went wrong
#
class ScraperException (Exception):

    def __init__ (self, message):
        super ().__init__ (message)
