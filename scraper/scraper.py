#!/usr/bin/python3
#
# scraper.py - Base class for all scrapers
#
# Frank Blankenburg, Jun. 2017
#

from abc import ABC, abstractmethod

class Scraper (ABC):

    #
    # Constructor
    #
    # @param name Printable name of the scraper as a type indicator
    #
    def __init__ (self, name):
        self.name = name

    #
    # Run scraper for acquiring a set of entries
    #
    # @param database Database to be filled
    # @param start    Start timestamp in UNIX epoch format or 'None' for maximum coverage
    #
    @abstractmethod
    def run (self, database, start):
        pass
