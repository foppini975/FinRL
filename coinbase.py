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

    def authenticate(self, key, b64secret, passphrase,
                                  api_url="https://api-public.sandbox.pro.coinbase.com"):
        # authentication
        self.auth_client = cbpro.AuthenticatedClient(key, b64secret, passphrase, api_url)

    def getAccountId(self, currency):
        accounts = self.auth_client.get_accounts()
        for account in accounts:
            if account['currency'] == currency:
                return account['id']
        return None

    def getAccount(self, accountId):
        return self.auth_client.get_account(accountId)

    @staticmethod
    def getProductList(products_file = None):
        products = cbpro.PublicClient().get_products()
        if products_file is not None:
            with open(products_file, 'w') as fp:
                json.dump(products, fp)
        return products

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
        # Granularity approved values: [60, 300, 900, 3600, 21600, 86400]
        Granularity_Map = {
            60: timedelta(hours=5),          # 5 hours per each call
            300: timedelta(hours=25),        # 25 hours per each call
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

class Wallet:
    
    CLOSE_TAG = 'Close'
    AMOUNT_TAG = 'Amount'
    VALUE_TAG = 'Value'

    def __init__(self, cb_df, market_name, start_date = '2022-01-01'):
        self.df = cb_df[cb_df['Time'] >= start_date][['Time', 'Close']].reset_index(drop=True)
        self.df.rename(columns={"Close": f"{market_name} {self.CLOSE_TAG}"}, inplace=True)
        self.df[f"{market_name} {self.AMOUNT_TAG}"] = 0
        self.df[f"{market_name} {self.VALUE_TAG}"] = 0
        self.start_date = start_date
        self.market_list = [market_name]
        self.anchor_ratio = None

    def add_column(self, cb_df, market_name):
        if market_name in self.market_list:
            print(f"Error! Market {market_name} already present, skipping.")
            return
        new_col_close = cb_df[cb_df['Time'] >= self.start_date][['Close']].reset_index(drop=True)
        self.df[f"{market_name} {self.CLOSE_TAG}"] = new_col_close
        self.df[f"{market_name} {self.AMOUNT_TAG}"] = 0
        self.df[f"{market_name} {self.VALUE_TAG}"] = 0
        self.market_list.append(market_name)

    def set_asset(self, date_string, market_name, amount):
        self.df.loc[self.df['Time'] >= date_string, f"{market_name} {self.AMOUNT_TAG}"] = amount
        self.refresh_wallet_value()

    def refresh_wallet_value(self):
        self.df['Wallet Value'] = 0
        for market_name in self.market_list:
            self.df[f"{market_name} {self.VALUE_TAG}"] = self.df[f"{market_name} {self.CLOSE_TAG}"] * self.df[f"{market_name} {self.AMOUNT_TAG}"]
            self.df['Wallet Value'] += self.df[f"{market_name} {self.CLOSE_TAG}"] * self.df[f"{market_name} {self.AMOUNT_TAG}"]

    def transfer(self, date_string, from_market, to_market, eur_amount):
        from_market_close = self.df[self.df['Time']==datetime.strptime(date_string, '%Y-%m-%d')][f"{from_market} {self.CLOSE_TAG}"].values[0]
        from_market_amount = eur_amount / from_market_close
        to_market_close = self.df[self.df['Time']==datetime.strptime(date_string, '%Y-%m-%d')][f"{to_market} {self.CLOSE_TAG}"].values[0]
        to_market_amount = eur_amount / to_market_close
        print(f"    *** TRANSFER: from {from_market} : close: {from_market_close:8} - amount: {from_market_amount} - eur: {from_market_close*from_market_amount:.2f}")
        print(f"    *** TRANSFER:  to  {to_market} : close: {to_market_close:8} - amount: {to_market_amount} - eur: {to_market_close*to_market_amount:.2f}")
        self.df.loc[self.df['Time'] >= datetime.strptime(date_string, '%Y-%m-%d'), f"{from_market} {self.AMOUNT_TAG}"] -= from_market_amount
        self.df.loc[self.df['Time'] >= datetime.strptime(date_string, '%Y-%m-%d'), f"{to_market} {self.AMOUNT_TAG}"] += to_market_amount
        self.refresh_wallet_value()

    def get_market_value(self, date_string, market_name):
        market_close = self.df[self.df['Time']==datetime.strptime(date_string, '%Y-%m-%d')][f"{market_name} {self.CLOSE_TAG}"].values[0]
        market_amount = self.df[self.df['Time']==datetime.strptime(date_string, '%Y-%m-%d')][f"{market_name} {self.AMOUNT_TAG}"].values[0]
        market_value = self.df[self.df['Time']==datetime.strptime(date_string, '%Y-%m-%d')][f"{market_name} {self.VALUE_TAG}"].values[0]
        return {'Close': market_close, 'Amount': market_amount, 'Value': market_value}

    def get_final_value(self):
        return self.df.tail(1)['Wallet Value'].values[0]

    def get_total_value(self, date_string):
        total_value = 0
        for market in self.market_list:
            total_value += self.get_market_value(date_string, market)['Value']
        return total_value

    def to_excel(self, excel_file_name):
        self.df.to_excel(excel_file_name)

    def simulate(self, transfer_amount_eur, max_transfer_amount_eur = None, threshold = 0.05):
        """
        transfer_amount_eur:
                                it's the amount in EUR transfered each time
                                - if the available amount is less than the requested amount,
                                  then the whole available amount is transfered
                                - if None, the whole available amount is transfered to the other market
        """
        self.anchor_ratio = None
        return_message = None
        # for index, row in self.df.iterrows():
        for index in range(self.df.shape[0]):
            row = self.df.iloc[index]
            date_string = row['Time'].strftime("%Y-%m-%d")
            if self.anchor_ratio is None:
                self.anchor_ratio = row["BTC-EUR Close"] / row["ETH-EUR Close"]
                self.df.loc[self.df['Time'] == datetime.strptime(date_string, '%Y-%m-%d'), "BTC/ETH Ratio"] = self.anchor_ratio
                continue
            current_ratio = row["BTC-EUR Close"] / row["ETH-EUR Close"]
            self.df.loc[self.df['Time'] == datetime.strptime(date_string, '%Y-%m-%d'), "BTC/ETH Ratio"] = current_ratio
            if current_ratio >= self.anchor_ratio * (1 + threshold):
                # BTC is HIGH --> transfer from BTC to ETH
                # transfer amount calculation
                # let's see if the requested amount is available
                delta_ratio = (current_ratio - self.anchor_ratio) / self.anchor_ratio
                threshold_multiplier = int(delta_ratio / threshold)
                if row["BTC-EUR Close"] * row["BTC-EUR Amount"] >= threshold_multiplier * transfer_amount_eur:
                    # it's sufficient
                    eur_amount = threshold_multiplier * transfer_amount_eur
                else:
                    # it's not sufficient => we transfer the whole available amount
                    eur_amount = row["BTC-EUR Close"] * row["BTC-EUR Amount"]
                # max transfer check
                if max_transfer_amount_eur and eur_amount >= max_transfer_amount_eur:
                    eur_amount = max_transfer_amount_eur
                btc_before = self.get_market_value(date_string=date_string, market_name='BTC-EUR')
                eth_before = self.get_market_value(date_string=date_string, market_name='ETH-EUR')
                print(f"{date_string} : Transfering BTC --> ETH: EUR {eur_amount} " +\
                    f"- ratio: {current_ratio:.2f} > {self.anchor_ratio:.2f} [Δ={delta_ratio:.2f}] " +\
                    f"- Wallet value: EUR {self.get_total_value(date_string):.2f}")
                self.transfer(date_string=date_string,
                                from_market="BTC-EUR",
                                to_market="ETH-EUR",
                                eur_amount = eur_amount)
                btc_after = self.get_market_value(date_string=date_string, market_name='BTC-EUR')
                eth_after = self.get_market_value(date_string=date_string, market_name='ETH-EUR')
                btc_value_percent_before = 100*btc_before['Value']/(btc_before['Value']+eth_before['Value'])
                btc_value_percent_after = 100*btc_after['Value']/(btc_after['Value']+eth_after['Value'])
                eth_value_percent_before = 100*eth_before['Value']/(btc_before['Value']+eth_before['Value'])
                eth_value_percent_after = 100*eth_after['Value']/(btc_after['Value']+eth_after['Value'])
                print(f"    BTC {btc_before['Amount']:7.4f} --> {btc_after['Amount']:7.4f} - EUR {btc_before['Value']:7.2f} --> {btc_after['Value']:7.2f} [{btc_value_percent_before:.1f}% -> {btc_value_percent_after:.1f}%]")
                print(f"    ETH {eth_before['Amount']:7.4f} --> {eth_after['Amount']:7.4f} - EUR {eth_before['Value']:7.2f} --> {eth_after['Value']:7.2f} [{eth_value_percent_before:.1f}% -> {eth_value_percent_after:.1f}%]")
                self.anchor_ratio = current_ratio
                if index == self.df.shape[0]-1:
                    # There is a transfer for today!
                    return_message = f"{date_string} : TRANSFER from BTC to ETH " + \
                        f"- (EUR {eur_amount} = BTC {btc_before['Amount']-btc_after['Amount']} = ETH {eth_after['Amount']-eth_before['Amount']}) " + \
                        f"- ratio: {current_ratio:.4f} Δ={delta_ratio:.2f}"
            elif current_ratio <= self.anchor_ratio * (1 - threshold):
                # ETH is HIGH --> transfer from ETH to BTC
                # transfer amount calculation
                # let's see if the requested amount is available
                delta_ratio = (self.anchor_ratio - current_ratio) / self.anchor_ratio
                threshold_multiplier = int(delta_ratio / threshold)
                if row["ETH-EUR Close"] * row["ETH-EUR Amount"] >= threshold_multiplier * transfer_amount_eur:
                    # it's sufficient
                    eur_amount = threshold_multiplier * transfer_amount_eur
                else:
                    # it's not sufficient => we transfer the whole available amount
                    eur_amount = row["ETH-EUR Close"] * row["ETH-EUR Amount"]
                # max transfer check
                if max_transfer_amount_eur and eur_amount >= max_transfer_amount_eur:
                    eur_amount = max_transfer_amount_eur
                btc_before = self.get_market_value(date_string=date_string, market_name='BTC-EUR')
                eth_before = self.get_market_value(date_string=date_string, market_name='ETH-EUR')
                print(f"{date_string} : Transfering ETH --> BTC: EUR {eur_amount} " +\
                    f"- ratio: {current_ratio:.2f} < {self.anchor_ratio:.2f} [Δ={delta_ratio:.2f}] " +\
                    f"- Wallet value: {self.get_total_value(date_string):.2f}")
                self.transfer(date_string=date_string,
                                from_market="ETH-EUR",
                                to_market="BTC-EUR",
                                eur_amount = eur_amount)
                btc_after = self.get_market_value(date_string=date_string, market_name='BTC-EUR')
                eth_after = self.get_market_value(date_string=date_string, market_name='ETH-EUR')
                btc_value_percent_before = 100*btc_before['Value']/(btc_before['Value']+eth_before['Value'])
                btc_value_percent_after = 100*btc_after['Value']/(btc_after['Value']+eth_after['Value'])
                eth_value_percent_before = 100*eth_before['Value']/(btc_before['Value']+eth_before['Value'])
                eth_value_percent_after = 100*eth_after['Value']/(btc_after['Value']+eth_after['Value'])
                print(f"    BTC {btc_before['Amount']:7.4f} --> {btc_after['Amount']:7.4f} - EUR {btc_before['Value']:7.2f} --> {btc_after['Value']:7.2f} [{btc_value_percent_before:.1f}% -> {btc_value_percent_after:.1f}%]")
                print(f"    ETH {eth_before['Amount']:7.4f} --> {eth_after['Amount']:7.4f} - EUR {eth_before['Value']:7.2f} --> {eth_after['Value']:7.2f} [{eth_value_percent_before:.1f}% -> {eth_value_percent_after:.1f}%]")
                self.anchor_ratio = current_ratio
                if index == self.df.shape[0]-1:
                    # There is a transfer for today!
                    return_message = f"{date_string} : TRANSFER from ETH to BTC " + \
                        f"(EUR {eur_amount} = ETH {eth_before['Amount']-eth_after['Amount']} = BTC {btc_after['Amount']-btc_before['Amount']}) " + \
                        f"- ratio: {current_ratio:.4f} Δ={delta_ratio:.2f}"
        return return_message

    def get_anchor_ratio(self):
        return self.anchor_ratio
