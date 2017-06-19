#!/usr/bin/python3
#
# cryptocompare.py - Cryptocompare API
#
# Frank Blankenburg, Jun. 2017
#

import argparse
import json
import pandas as pd
import sys
import time
import urllib


#--------------------------------------------------------------------------
# Interface for accessing the Cryptocompare web API
#
class CryptoCompare:

    def __init__ (self):
        pass

    def get_coin_list (self):
        query = 'https://www.cryptocompare.com/api/data/coinlist'

        ret = urllib.request.urlopen (urllib.request.Request (query))
        return json.loads (ret.read ().decode ('utf8'))['Data']
