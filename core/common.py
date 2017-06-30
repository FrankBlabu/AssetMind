#!/usr/bin/python3
#
# common.py - Common data structures
#
# Frank Blankenburg, Jun. 2017
#

from enum import Enum

#
# Enumeration for query intervals
#
class Interval (Enum):
    day    = 1
    hour   = 2
    minute = 3
