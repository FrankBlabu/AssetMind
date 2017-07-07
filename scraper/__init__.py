#!/usr/bin/python3
#
# __init__.py - Initialization of the scraper library
#
# Frank Blankenburg, Jul. 2017
#

from scraper.scraper import ScraperRegistry
from scraper.cryptocompare import CryptoCompareScraper
#from scraper.twitter import TwitterScraper

#
# Register all scrapers which will fill the database
#
ScraperRegistry.register (CryptoCompareScraper (), CryptoCompareScraper.ID)
#ScraperRegistry.register (TwitterScraper (), TwitterScraper.ID)
