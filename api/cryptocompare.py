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
    # limit    - Maximum number of entries requested with a single web API call. 200 is the highest
    #            possible value. Be aware that there seem to be invalid responses which can be
    #            filtered be checking the previous values for '0' entries. So 'limit' should
    #            be larger then '1' to be able to do that.
    #
    #markets = ['Poloniex', 'Kraken', 'Coinbase', 'HitBTC']
    markets    = ['Poloniex']
    currencies = 'USD,EUR,BTC'
    currency   = 'USD'
    limit      = 2000

    def __init__ (self):
        pass

    def get_coin_list (self):
        command = 'https://www.cryptocompare.com/api/data/coinlist'
        return self.query (command)['Data']

    def get_coin_snapshot (self, id):
        command = 'https://www.cryptocompare.com/api/data/coinsnapshot?fsym={id}&tsym={currency}' \
        .format (id=self.id_as_list (id), currency=CryptoCompare.currency)
        return self.query (command)

    def get_price (self, id):
        command = 'https://min-api.cryptocompare.com/data/pricemulti?fsyms={id}&tsyms={currency}' \
        .format (id=self.id_as_list (id), currency=CryptoCompare.currencies)
        return self.query (command)

    def get_average_price (self, id):
        command = 'https://min-api.cryptocompare.com/data/dayAvg?fsym={id}&tsym={currency}&UTCHourDiff=-8' \
        .format (id=self.id_as_list (id), currency=CryptoCompare.currency)
        return self.query (command)['USD']

    def get_trading_info (self, id):
        command = 'https://min-api.cryptocompare.com/data/generateAvg?fsym={id}&tsym={currency}&markets={markets}' \
            .format (id=id, currency=CryptoCompare.currency, markets=self.id_as_list (CryptoCompare.markets))
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

        #
        # The response format is:
        #
        '''
        {
         "Response": "Success",
         "Type": 100,
         "Aggregated": false,
         "Data": [{"time":1413158400,"close":0,"high":0,"low":0,"open":0,"volumefrom":0,"volumeto":0},...
         "TimeTo": 1499558400,
         "TimeFrom": 1413158400,
         "FirstValueInArray": true,
         "ConversionType": {"type":"direct","conversionSymbol":""}
         }
        '''

        data = sorted (r['Data'], key=lambda value: value['time'])
        filtered_data = [value for value in data if value['volumefrom'] > 0 and value['volumeto'] > 0]

        #
        # Due to a bug in the CryptoCompare API, the last data entry can be valid (but seemingly random or not
        # matching its timestamp) if the requested time interval is not covered. This has to be checked here.
        #
        if len (filtered_data) == 1 and len (data) > 1:
            data = []

        return data

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

    if prices:
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
        client.get_historical_prices ('XYZ', Timestamp ('2016-04-08 06:00'), Interval.hour)
    except HTTPError as e:
        print ('ERROR:', e.message)



#--------------------------------------------------------------------------
# MAIN
#
if __name__ == '__main__':

    client = CryptoCompare ()

    #test_historical_prices ()
    #sys.exit (0)

    coins = client.get_coin_list ()

    title = 'Coins'
    print (title)
    print (len (title) * '-')

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
    print ('\n')

    title = 'Selected coins'
    print (title)
    print (len (title) * '-')

    selected_coins = sorted (['ETH', 'ETC', 'BTC', 'XRP', 'XMR', 'LTC'])

    prices = client.get_price (selected_coins)

    frame = pd.DataFrame (columns=['Id', 'EUR', 'USD', 'BTC', 'Average (USD)', 'Gradient (%)', 'Volumen'])

    for coin in selected_coins:

        price = prices[coin]
        average = client.get_average_price (coin)
        trade = client.get_trading_info (coin)

        frame.loc[len (frame)] = [coin,
                                  price['EUR'],
                                  price['USD'],
                                  price['BTC'],
                                  average,
                                  trade['CHANGEPCT24HOUR'],
                                  trade['VOLUME24HOURTO']]

    print (frame.to_string ())
