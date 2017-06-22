#!/usr/bin/python3
#
# scraper.py - Base class for all scrapers
#
# Frank Blankenburg, Jun. 2017
#

from abc import ABCMeta, abstractmethod

class Scraper:

    __metaclass__ = ABCMeta

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
    # @param password Password for accessing the protected database entries
    # @param start    Start timestamp in UNIX epoch format or 'None' for maximum coverage
    # @param end      End timestamp in UNIX epoch format  or 'None' for maximum coverage
    #
    @abstractmethod
    def run (self, database, password, start, end):
        pass
