#!/usr/bin/python3
#
# init.py - Initialization of the scrapers
#
# Frank Blankenburg, Jul. 2017
#

from scraper.scraper import ScraperRegistry
from scraper.cryptocompare import CryptoCompareScraper
from scraper.twitter import TwitterScraper

#
# Register all scrapers which will fill the database
#
ScraperRegistry.register (CryptoCompareScraper ())
ScraperRegistry.register (TwitterScraper ())
