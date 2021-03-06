#!/usr/bin/python3
#
# common.py - Common data structures
#
# Frank Blankenburg, Jun. 2017
#

import pandas as pd

from enum import Enum

#
# Enumeration for query intervals
#
class Interval (Enum):
    day    = 1
    hour   = 2
    minute = 3

#
# Print pandas frame with title line
#
def print_frame (title, frame):

    pd.set_option ('display.width', 256)
    pd.set_option ('display.max_rows', len (frame))

    print (title)
    print ('-' * len (title))
    print (frame)

#
# Generic dictionary with attribute like content access
#
class AttrDict (dict):

    def __init__ (self, *args, **kwargs):
        super ().__init__ (*args, **kwargs)

    def __getattr__ (self, name):
        if name not in self:
            raise AttributeError ('No such attribute: ', name)
        return self[name]

    def __setattr__ (self, name, value):
        self[name] = value

    def __delattr__ (self, name):
        if name in self:
            del self[name]

    def __repr__ (self):
        return '{' + ', '.join ([key + '=' + str (self[key]) for key in self.keys ()]) + '}'
