#!/usr/bin/python3
#
# cryptocompare.py - Cryptocompare API
#
# Frank Blankenburg, Jun. 2017
#

import json
import urllib


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

    def get_price (self, id):
        command = 'https://min-api.cryptocompare.com/data/pricemulti?fsyms={0}&tsyms=BTC,USD,EUR'.format (self.id_as_list (id))
        return self.query (command)

    def get_average_price (self, id):
        command = 'https://min-api.cryptocompare.com/data/dayAvg?fsym={0}&tsym=USD&UTCHourDiff=-8'.format (id)
        return self.query (command)['USD']

    def get_trading_info (self, id):
        command = 'https://min-api.cryptocompare.com/data/generateAvg?fsym={source}&tsym=USD&markets={markets}' \
            .format (source=id, markets=self.id_as_list (CryptoCompare.markets))
        return self.query (command)['RAW']

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

    print (client.get_trading_info ('ETH'))
