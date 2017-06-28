#!/usr/bin/python3
#
# cryptocompare.py - Cryptocompare API
#
# Frank Blankenburg, Jun. 2017
#
# Resources:
# * https://www.cryptocompare.com/api
#

import pandas as pd
import json
import urllib
import urllib.request

from enum import Enum
from core.time import Timestamp

#--------------------------------------------------------------------------
# Interface for accessing the Cryptocompare web API
#
class CryptoCompare:

    #markets = ['Poloniex', 'Kraken', 'Coinbase', 'HitBTC']
    markets = ['Poloniex']

    def __init__ (self):
        pass

    def get_coin_list (self):
        command = 'https://www.cryptocompare.com/api/data/coinlist'
        return self.query (command)['Data']

    def get_coin_snapshot (self, id):
        command = 'https://www.cryptocompare.com/api/data/coinsnapshot?fsym={0}&tsym=USD'.format (self.id_as_list (id))
        print (command)
        return self.query (command)

    def get_price (self, id):
        command = 'https://min-api.cryptocompare.com/data/pricemulti?fsyms={0}&tsyms=USD'.format (self.id_as_list (id))
        return self.query (command)

    def get_average_price (self, id):
        command = 'https://min-api.cryptocompare.com/data/dayAvg?fsym={0}&tsym=USD&UTCHourDiff=-8'.format (id)
        return self.query (command)['USD']

    def get_trading_info (self, id):
        command = 'https://min-api.cryptocompare.com/data/generateAvg?fsym={source}&tsym=USD&markets={markets}' \
            .format (source=id, markets=self.id_as_list (CryptoCompare.markets))
        return self.query (command)['RAW']

    class Interval (Enum):
        DAY    = 1
        HOUR   = 2
        MINUTE = 3

    def get_historical_prices (self, id, interval):

        if interval is CryptoCompare.Interval.DAY:
            interval_id = 'day'
        elif interval is CryptoCompare.Interval.HOUR:
            interval_id = 'hour'
        elif interval is CryptoCompare.Interval.MINUTE:
            interval_id = 'minute'
        else:
            raise RunimeError ('Unknown enum id \'{0}\''.format (interval.name))

        command = 'https://min-api.cryptocompare.com/data/histo{interval}?fsym={id}&tsym=USD&limit=2000' \
        .format (interval=interval_id, id=id)

        return self.query (command)['Data']

    def query (self, command):
        ret = urllib.request.urlopen (urllib.request.Request (command))
        return json.loads (ret.read ().decode ('utf8'))

    #
    # Convert list of ids into a query compatible comma separated list
    #
    def id_as_list (self, id):
        ids = id

        if isinstance (id, list):
            ids = ''
            separator = ''

            for i in id:
                ids += separator + i.strip ()
                separator = ','

        return ids


#--------------------------------------------------------------------------
# MAIN
#
if __name__ == '__main__':

    client = CryptoCompare ()
    coins = client.get_coin_list ()

    frame = pd.DataFrame (columns=['Id', 'Name', 'Algorithm', 'Proof Type', 'Total supply', 'Pre mined'])

    for key in sorted (coins.keys ()):
        entry = coins[key]
        frame.loc[len (frame)] = [key.strip (),
                                  entry['CoinName'].strip (),
                                  entry['Algorithm'].strip (),
                                  entry['ProofType'].strip (),
                                  entry['TotalCoinSupply'].strip (),
                                  'Yes' if entry['FullyPremined'] != '0' else 'No']

    print (frame.to_string ())

    snapshot = client.get_coin_snapshot ('ETH')
    print (snapshot)
