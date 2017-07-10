#!/usr/bin/python3
#
# scraper.py - Base class for all scrapers
#
# Frank Blankenburg, Jun. 2017
#

from abc import ABC, abstractmethod
from core.common import AttrDict

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
    # Get all channels provided by the scraper
    #
    # @return List of channels
    #
    @abstractmethod
    def get_channels (self):
        pass

    #
    # Run scraper for acquiring a set of entries
    #
    # @param database Database to be filled
    # @param start    Start timestamp (UTC)
    # @param end      End timestamp (UTC)
    # @param interval Interval of scraping
    # @param log      Callback for logging outputs
    #
    @abstractmethod
    def run (self, database, start, end, interval, log):
        pass

    #
    # Split channel id into scraper id / token id
    #
    def split_channel_id (self, id):
        parts = [part.strip () for part in id.split ('::')]

        if len (parts) >= 2:
            return AttrDict (scraper=parts[0], token='::'.join (parts[1:]))
        elif len (parts) == 1:
            return AttrDict (scraper=None, token=parts[0])

        raise RuntimeError ('Illegal channel id format \'{id}\''.format (id=id))


#
# Registry for all scraper instances to be used
#
class ScraperRegistry:

    #
    # Registered scrapers
    #
    scrapers = {}

    @staticmethod
    def register (scraper):
        ScraperRegistry.scrapers[scraper.ID] = scraper

    @staticmethod
    def get (id):
        return ScraperRegistry.scrapers[id] if id in ScraperRegistry.scrapers else None

    @staticmethod
    def get_all ():
        return ScraperRegistry.scrapers.values ()


#
# Exception thrown by the scraper implementation if anything went wrong
#
class ScraperException (Exception):

    def __init__ (self, message):
        super ().__init__ (message)
