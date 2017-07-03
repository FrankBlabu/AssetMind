#!/usr/bin/python3
#
# config.py - Project configuration
#
# Frank Blankenburg, Jul. 2017
#

from core.common import Interval
from datetime import timedelta

class Configuration:

    #
    # Earliest date captured in the database. The scrapers will try to gather as much data as
    # possible starting from this point in time on to the current time.
    #
    DATABASE_START_DATE = '2012-1-1'

    #
    # Database sampling interval
    #
    DATABASE_SAMPLING_INTERVAL = Interval.day

    #
    # Database sampling step
    #
    if DATABASE_SAMPLING_INTERVAL is Interval.hour:
        DATABASE_SAMPLING_STEP = timedelta (hours=1)
    elif DATABASE_SAMPLING_INTERVAL is Interval.day:
        DATABASE_SAMPLING_STEP = timedelta (days=1)
    elif DATABASE_SAMPLING_INTERVAL is Interval.minute:
        DATABASE_SAMPLING_STEP = timedelta (minutes=1)
