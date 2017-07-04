#!/usr/bin/python3
#
# cryptocompare.py - Cryptocompare API
#
# Frank Blankenburg, Jun. 2017
#
# Resources:
# * https://www.cryptocompare.com/api
#
# Concstraints:
# * Cryptocompare server allows 1000 requests/hour only
# * One request every 10s is regarded as ok (but not limited to that value)
#

import pandas as pd
import json
import urllib
import urllib.request

from enum import Enum
from core.common import Interval
from core.time import Timestamp

#--------------------------------------------------------------------------
# CLASS HTTPError
#
# Generic exception object for errors originating from the CryptoCompare API
#
class HTTPError (Exception):

    def __init__ (self, message):
        self.message = message


#--------------------------------------------------------------------------
# CLASS CryptoCompare
#
class CryptoCompare:

    #
    # Configuration
    #
    # markers  - List of markers to be queried
    # currency - Target currency
    # limit    - Maximum number of entries requested with a single web API call
    #
    #markets = ['Poloniex', 'Kraken', 'Coinbase', 'HitBTC']
    markets  = ['Poloniex']
    currency = 'USD'
    limit    = 2000

    def __init__ (self):
        pass

    def get_coin_list (self):
        command = 'https://www.cryptocompare.com/api/data/coinlist'
        return self.query (command)['Data']

    def get_coin_snapshot (self, id):
        command = 'https://www.cryptocompare.com/api/data/coinsnapshot?fsym={id}&tsym={currency}' \
        .format (id=self.id_as_list (id), currency=currency)
        return self.query (command)

    def get_price (self, id):
        command = 'https://min-api.cryptocompare.com/data/pricemulti?fsyms={id}&tsyms={currency}' \
        .format (id=self.id_as_list (id), currency=currency)
        return self.query (command)

    def get_average_price (self, id):
        command = 'https://min-api.cryptocompare.com/data/dayAvg?fsym={id}&tsym={currency}&UTCHourDiff=-8' \
        .format (id=self.id_as_list (id), currency=currency)
        return self.query (command)['USD']

    def get_trading_info (self, id):
        command = 'https://min-api.cryptocompare.com/data/generateAvg?fsym={id}&tsym={currency}&markets={markets}' \
            .format (id=id, currency=currency, markets=self.id_as_list (CryptoCompare.markets))
        return self.query (command)['RAW']

    def get_historical_prices (self, id, to, interval):

        assert isinstance (to, Timestamp)
        assert isinstance (id, str)
        assert isinstance (interval, Interval)

        command = 'https://min-api.cryptocompare.com/data/histo{interval}'.format (interval=interval.name)
        command += '?fsym={id}'.format (id=id)
        command += '&tsym=USD&markets={markets}'.format (markets=self.id_as_list (CryptoCompare.markets))
        command += '&limit={limit}'.format (limit=CryptoCompare.limit)
        command += '&toTs={timestamp}'.format (timestamp=to.epoch ())
        command = command.format (interval=interval.name, id=id, markets=self.id_as_list (CryptoCompare.markets))

        r = self.query (command)
        return r['Data']

    def query (self, command):

        result = None

        with urllib.request.urlopen (urllib.request.Request (command)) as response:
            result = json.loads (response.read ().decode ('utf8'))

        if 'Response' in result and result['Response'] == 'Error':
            raise HTTPError (result['Message'])

        return result

    #
    # Convert list of ids into a query compatible comma separated list
    #
    def id_as_list (self, id):
        ids = id

        if isinstance (id, list):
            ids = ','.join ([i.strip () for i in id])

        return ids

#--------------------------------------------------------------------------
# API test functions
#
def test_historical_prices ():

    client = CryptoCompare ()
    prices = client.get_historical_prices ('ETH', Timestamp ('2016-04-08 06:00'), Interval.hour)

    print (len (prices))

    print (Timestamp (prices[1]['time']))
    print (Timestamp (prices[-1]['time']))


def test_coin_list ():
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

def test_error ():

    client = CryptoCompare ()

    try:
        prices = client.get_historical_prices ('XYZ', Timestamp ('2016-04-08 06:00'), Interval.hour)
    except HTTPError as e:
        print ('ERROR:', e.message)


#--------------------------------------------------------------------------
# MAIN
#
if __name__ == '__main__':
    test_error ()
