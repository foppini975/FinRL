# Coinbase Pro library:
# https://github.com/danpaquin/coinbasepro-python


#curl "https://api.pro.coinbase.com/products/BTC-USD/candles?start=2021-01-01T12:00:00&end=2021-01-12T12:00:00&granularity=3600"


import cbpro
import numpy as np
import pandas as pd

import logging
from datetime import datetime, timedelta

import json

#from IPython.core.debugger import set_trace

class Coinbase:

    def __init__(self, product, logging_level = logging.INFO, products_file = None):
        FORMAT = '%(asctime)-15s %(message)s'
        logging.basicConfig(level=logging_level, format=FORMAT)
        # init
        self.product = product
        self.df = None
        # client creation
        self.public_client = cbpro.PublicClient()
        # get products
        self.products = self.public_client.get_products()
        if products_file is not None:
            with open(products_file, 'w') as fp:
                json.dump(self.products, fp)
            logging.info(f"Found {len(self.products)} products, saved to {products_file}")
        else:
            logging.info(f"Found {len(self.products)} products")
        found = False
        for prod in self.products:
            if prod['id'] == self.product:
                found = True
                logging.info(prod)
                self.product = self.product
                break
        if found is False:
            raise Exception(f"Product {self.product} not valid")

    @staticmethod
    def getProductList():
        return cbpro.PublicClient().get_products()

    @staticmethod
    def getPrice(product):
        return float(cbpro.PublicClient().get_product_ticker(product)['price'])

    def loadHistory(self, start_date, end_date, granularity = 86400, moving_average = 20):
        #
        # dates are datetime objects, can be crated with:
        # start_utc = datetime(2021, 1, 1)
        #        
        start_interval = start_date - timedelta(days=moving_average)
        end_interval = None
        Granularity_Map = {
            60: timedelta(hours=5),          # 1 day per each call
            86400: timedelta(days=28 * 6 -1) # 42 weeks per each call
        }
        if granularity not in Granularity_Map:
            raise Exception(f"Granularity {granularity} not valid")
        self.df = pd.DataFrame()
        while True:
            if end_interval is not None:
                start_interval = end_interval + timedelta(seconds=1)
                if start_interval > end_date:
                    break
            end_interval = start_interval + Granularity_Map[granularity]
            if end_interval > end_date:
                end_interval = end_date
            start_interval_iso = start_interval.isoformat()
            end_interval_iso = end_interval.isoformat()
            btc_history = self.public_client.get_product_historic_rates(
                self.product, start=start_interval_iso,
                end=end_interval_iso,
                granularity=granularity)
            if len(btc_history) == 1 and 'message' in btc_history:
                raise Exception(btc_history['message'])
            logging.info(f"Fetched from {start_interval_iso} to {end_interval_iso} : #{len(btc_history)} points")
            if len(btc_history) == 0:
                continue
            btc_history_np = np.array(btc_history)
            df_new = pd.DataFrame(btc_history_np, columns = ['Time','Low','High','Open','Close','Volume'])
            self.df = self.df.append(df_new, ignore_index=True, sort=True)
        self.df['tic'] = self.product
        self.df['Time'] = pd.to_datetime(self.df['Time'], unit='s')
        moving_average_label = f"MA{moving_average}"
        self.df.sort_values(by='Time', inplace=True)
        self.df[moving_average_label] = self.df['Close'].rolling(window=moving_average).mean()
        # let's remove the initial points where moving average was not available
        self.df = self.df[self.df['Time'] >= start_date]
        self.df.reset_index(drop=True, inplace=True)
        #time bucket start time
        #low lowest price during the bucket interval
        #high highest price during the bucket interval
        #open opening price (first trade) in the bucket interval
        #close closing price (last trade) in the bucket interval
        #volume volume of trading activity during the bucket interval

    def calculateBuy(self, moving_average = 20, below_threshold = 0.1):
        # "Buy" significa che il valore era sceso del x% sotto il valore attuale e ora e' tornato sopra la moving average
        #
        # Let's generate the Below column (min-hold below moving average)
        moving_average_label = f"MA{moving_average}"
        self.df['Below'] = 0
        for index, row in self.df.iterrows():
            current_value = row['Close']
            if current_value < row[moving_average_label]:
                below = current_value - row[moving_average_label]
                try:
                    previous_below = self.df.loc[index-1, 'Below']
                except:
                    previous_below = 0
                if below < previous_below:
                    self.df.loc[index, 'Below'] = below
                else:
                    self.df.loc[index, 'Below'] = previous_below

        # Let's generate the BUY trigger based on the Below column
        self.df['Buy'] = 0
        for index, row in self.df.iterrows():
            current_value = row['Close']
            try:
                previous_below = self.df.loc[index-1, 'Below']
            except:
                previous_below = 0
            if current_value > row[moving_average_label] and previous_below < -1*below_threshold*current_value:
                self.df.loc[index, 'Buy'] = self.df['Close'].max()/5  # placeholder value to facilitate the plot

    def calculateSell(self, moving_average = 20, above_threshold = 0.1):
        # "Sell" significa che il valore era salito del x% sopra il valore attuale e ora e' sceso sotto la moving average
        #
        # Let's generate the Above column (max-hold above moving average)
        moving_average_label = f"MA{moving_average}"
        self.df['Above'] = 0
        for index, row in self.df.iterrows():
            current_value = row['Close']
            if current_value > row[moving_average_label]:
                above = current_value - row[moving_average_label]
                try:
                    previous_above = self.df.loc[index-1, 'Above']
                except:
                    previous_above = 0
                if above > previous_above:
                    self.df.loc[index, 'Above'] = above
                else:
                    self.df.loc[index, 'Above'] = previous_above

        # Let's generate the SELL trigger based on the Above column
        self.df['Sell'] = 0
        for index, row in self.df.iterrows():
            current_value = row['Close']
            try:
                previous_above= self.df.loc[index-1, 'Above']
            except:
                previous_above = 0
            if current_value < row[moving_average_label] and previous_above > above_threshold*current_value:
                self.df.loc[index, 'Sell'] = -1*self.df['Close'].max()/5  # placeholder value to facilitate the plot

    def backSimulate(self, initial_amount = 100):
        self.df['Wallet_USD'] = 0
        self.df['Wallet_Crypto'] = 0
        self.df['Wallet_Crypto_Hold'] = 0
        for index, row in self.df.iterrows():
            self.df.loc[index, 'Wallet_Crypto_Hold'] = initial_amount/self.df.loc[0,'Close'] * self.df.loc[index,'Close']
            if index == 0:
                self.df.loc[0, 'Wallet_USD'] = initial_amount
                continue
            if self.df.loc[index, 'Buy'] != 0 and self.df.loc[index-1,'Wallet_USD'] > 0:
                # Buy
                purchased_crypto = self.df.loc[index-1,'Wallet_USD'] / self.df.loc[index,'Close']
                logging.info(f"Buy : {self.df.loc[index-1,'Wallet_USD']} USD ---> {purchased_crypto} BTC")
                self.df.loc[index,'Wallet_Crypto'] = purchased_crypto
                self.df.loc[index,'Wallet_USD'] = 0
            elif self.df.loc[index, 'Sell'] != 0 and self.df.loc[index-1,'Wallet_Crypto'] > 0:
                # Sell
                sold_crypto = self.df.loc[index-1,'Wallet_Crypto'] * self.df.loc[index,'Close']
                logging.info(f"Sell: {self.df.loc[index-1,'Wallet_Crypto']} BTC ---> {sold_crypto} BUSDTC")
                self.df.loc[index,'Wallet_USD'] = sold_crypto
                self.df.loc[index,'Wallet_Crypto'] = 0
            else:
                # Hold
                self.df.loc[index,'Wallet_USD'] = self.df.loc[index-1,'Wallet_USD']
                self.df.loc[index,'Wallet_Crypto'] = self.df.loc[index-1,'Wallet_Crypto']
                
    def getTicker(self):
        return self.public_client.get_product_ticker(self.product)