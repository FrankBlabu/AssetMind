#!/usr/bin/python3
#
# poloniex.py - Polonix API
#
# Frank Blankenburg, Jun. 2017
#

import json
import time
import urllib


#--------------------------------------------------------------------------
# Interface for accessing the Poloniex web API
#
class Poloniex:

    def __init__ (self, api_key, secret):
        self.api_key = api_key
        self.secret = secret

    #
    # Send single query via public API or trading API
    #
    def query (self, command, req={}):

        post_data = None
        headers = {}

        if command == 'returnTicker' or command == 'return24Volume' or command == 'returnCurrencies':
            query = 'https://poloniex.com/public?command=' + command

        elif command == 'returnOrderBook':
            query = 'https://poloniex.com/public?command=' + command + '&currencyPair=' + str (req['currencyPair'])

        elif command == 'returnMarketTradeHistory':
            query = 'https://poloniex.com/public?command=' + 'returnTradeHistory'
            query += '&currencyPair=' + str (req['currencyPair'])

        elif command == 'returnChartData':
            query = 'https://poloniex.com/public?command=' + 'returnChartData'
            query += '&currencyPair=' + str (req['currencyPair'])
            query += '&period=' + str (req['period'])
            query += '&start=' + str (req['start'])
            query += '&end=' + str (req['end'])

        else:
            req['command'] = command
            req['nonce'] = int (time.time () * 1000)
            post_data = urllib.urlencode (req)

            sign = hmac.new (self.Secret, post_data, hashlib.sha512).hexdigest ()

            headers = {
                'Sign': sign,
                'Key': self.APIKey
            }

            query = 'https://poloniex.com/tradingApi'

        ret = urllib.request.urlopen (urllib.request.Request (query, post_data, headers))
        return json.loads (ret.read ().decode ('utf8'))

    def get_ticker (self):
        return self.query ('returnTicker')

    def get_24_volume (self):
        return self.query ('return24Volume')

    def get_order_book (self, currencyPair):
        return self.query ('returnOrderBook', {'currencyPair': currencyPair})

    def get_market_trade_history (self, currencyPair):
        return self.query ('returnMarketTradeHistory', {'currencyPair': currencyPair})

    #
    # Return chart data for a given timeslot
    #
    # @param currency_pair Currency pair id
    # @param period        Candlestick period in seconds (valid are: 300, 900, 1800, 7200, 14400, 86400)
    # @param start         Start time of period in UNIX ticks
    # @param end           End time of period in UNIX ticks
    #
    def get_chart_data (self, currency_pair, period, start, end):

        assert period in [300, 900, 1800, 7200, 14400, 8640]
        assert isinstance (start, int)
        assert isinstance (end, int)
        assert start < end

        return self.query ('returnChartData', {'currencyPair': currency_pair,
                                               'period': period,
                                               'start': start,
                                               'end': end} )

    #
    # Return information about the supported currencies
    #
    def get_currencies (self):
        return self.query ('returnCurrencies')


    # Returns all of your balances.
    # Outputs:
    # {"BTC":"0.59098578","LTC":"3.31117268", ... }
    def get_balances (self):
        return self.query ('returnBalances')

    # Returns your open orders for a given market, specified by the "currencyPair" POST parameter, e.g. "BTC_XCP"
    # Inputs:
    # currencyPair  The currency pair e.g. "BTC_XCP"
    # Outputs:
    # orderNumber   The order number
    # type          sell or buy
    # rate          Price the order is selling or buying at
    # Amount        Quantity of order
    # total         Total value of order (price * quantity)
    def get_open_orders (self, currencyPair):
        return self.query ('returnOpenOrders', {'currencyPair': currencyPair})


    # Returns your trade history for a given market, specified by the "currencyPair" POST parameter
    # Inputs:
    # currencyPair  The currency pair e.g. "BTC_XCP"
    # Outputs:
    # date          Date in the form: "2014-02-19 03:44:59"
    # rate          Price the order is selling or buying at
    # amount        Quantity of order
    # total         Total value of order (price * quantity)
    # type          sell or buy
    def get_trade_history (self, currencyPair):
        return self.query ('returnTradeHistory', {'currencyPair': currencyPair})

    # Places a buy order in a given market. Required POST parameters are "currencyPair", "rate", and "amount". If successful, the method will return the order number.
    # Inputs:
    # currencyPair  The curreny pair
    # rate          price the order is buying at
    # amount        Amount of coins to buy
    # Outputs:
    # orderNumber   The order number
    def buy (self, currencyPair, rate, amount):
        return self.query ('buy', {"currencyPair": currencyPair, 'rate': rate, 'amount': amount})

    # Places a sell order in a given market. Required POST parameters are "currencyPair", "rate", and "amount". If successful, the method will return the order number.
    # Inputs:
    # currencyPair  The curreny pair
    # rate          price the order is selling at
    # amount        Amount of coins to sell
    # Outputs:
    # orderNumber   The order number
    def sell (self, currencyPair, rate, amount):
        return self.query ('sell', {'currencyPair': currencyPair, 'rate': rate, 'amount': amount})

    # Cancels an order you have placed in a given market. Required POST parameters are "currencyPair" and "orderNumber".
    # Inputs:
    # currencyPair  The curreny pair
    # orderNumber   The order number to cancel
    # Outputs:
    # succes        1 or 0
    def cancel (self, currencyPair, orderNumber):
        return self.query ('cancelOrder', {'currencyPair': currencyPair, 'orderNumber': orderNumber})

    # Immediately places a withdrawal for a given currency, with no email confirmation. In order to use this method, the withdrawal privilege must be enabled for your API key. Required POST parameters are "currency", "amount", and "address". Sample output: {"response":"Withdrew 2398 NXT."}
    # Inputs:
    # currency      The currency to withdraw
    # amount        The amount of this coin to withdraw
    # address       The withdrawal address
    # Outputs:
    # response      Text containing message about the withdrawal
    def withdraw (self, currency, amount, address):
        return self.query ('withdraw', {'currency': currency, 'amount': amount, 'address': address})
