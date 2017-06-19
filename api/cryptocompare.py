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
        command = 'https://www.cryptocompare.com/api/data/coinlist'
        return self.query (command)['Data']

    def get_price (self, id):

        ids = id
        if isinstance (id, list):
            ids = ''
            separator = ''

            for i in id:
                ids += separator + i.strip ()
                separator = ','


        command = 'https://min-api.cryptocompare.com/data/pricemulti?fsyms={0}&tsyms=BTC,USD,EUR'.format (ids)
        return self.query (command)

    def query (self, command):
        ret = urllib.request.urlopen (urllib.request.Request (command))
        return json.loads (ret.read ().decode ('utf8'))
