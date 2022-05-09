import json, time, logging
from os import stat
from threading import Thread, Lock
from websocket import create_connection, WebSocketConnectionClosedException
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pytz
import telepot
from mysecrets import Secrets
import argparse
import sys

parser = argparse.ArgumentParser(description='Coinbase websocket monitor')
parser.add_argument('--verbose', help='set debug verbosity', action='store_true')
parser.add_argument("--dont-send", action="store_true", help="messages are not sent to any channel")
parser.add_argument('--fig-pub-hours', default=6, type=int, help='figure publication time (in hours)')
parser.add_argument('--plot-history-hours', default=12, type=int, help='plot history time (in hours)')
parser.add_argument('--show-info-minutes', default=1, type=int, help='value display time (in minutes)')
parser.add_argument('--threshold', default=.01, type=float, help='current/anchor trigger threshold')

args = parser.parse_args()

# Telegram
bot = telepot.Bot(Secrets.TELEGRAM_TOKEN)

def sendMessageToTelegram(text):    
    bot.sendMessage(Secrets.TELEGRAM_CHANNEL_ID, text)

def sendPhoto(image_filename):
    bot.sendPhoto(Secrets.TELEGRAM_CHANNEL_ID, photo=open(image_filename, 'rb'))


logger = logging.getLogger("Coinbase Socket")
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
if args.verbose:
    logger.setLevel(logging.DEBUG)
    logger.debug("Debug logging enabled")
else:
    logger.setLevel(logging.INFO)

PRODUCT_ID = 'product_id'
BTC_EUR = 'BTC-EUR'
ETH_EUR = 'ETH-EUR'
MSG_TYPE = 'type'
MSG_TYPE_MATCH = 'match'
MSG_TYPE_LAST_MATCH = 'last_match'
SIDE = 'side'
PRICE = 'price'
SELL_SIDE = 'sell'
BUY_SIDE = 'buy'
BTC_SELL_SIDE = 'btc-sell'
ETH_SELL_SIDE = 'eth-sell'
BTC_TO_ETH_RATIO = 'BTC-ETH-ratio'
ANCHOR = 'anchor'
LATEST = 'latest'
MSG_TIME = 'time'
TIMESTAMP = 'timestamp'
SEQUENCE_NUMBER = 'sequence'
TRANSFER_BTC_TO_ETH = 'BTC to ETH'
TRANSFER_ETH_TO_BTC = 'ETH to BTC'

RATIO_THRESHOLD = args.threshold

PLOT_HISTORY_HOURS = args.plot_history_hours
FIGURE_PUBLICATION_HOURS = args.fig_pub_hours

class CoinbaseSocket:

    ENDPOINT = "wss://ws-feed.pro.coinbase.com"
    SUBSCRIPTION_1 = {
                        "type": "subscribe",
                        "product_ids": ['BTC-EUR', 'ETH-EUR'],
                        "channels": ["matches"],
                    }

    def _load_latest_values(self):
        with open(self.state_filename, 'r') as openfile:
            self.latest_values = json.load(openfile)

    def _save_latest_values(self):
        with open(self.state_filename, "w") as outfile:
            json.dump(self.latest_values, outfile)

    def _get_transfer_message(self, transfer):
        if transfer == TRANSFER_BTC_TO_ETH:
            delta_ratio = self.latest_values[BTC_TO_ETH_RATIO][BTC_SELL_SIDE][LATEST] - self.latest_values[BTC_TO_ETH_RATIO][BTC_SELL_SIDE][ANCHOR]
            multiplier = int(delta_ratio / self.latest_values[BTC_TO_ETH_RATIO][BTC_SELL_SIDE][ANCHOR] / RATIO_THRESHOLD)
            return f"{self.latest_values[TIMESTAMP]} : TRANSFER from BTC to ETH " + \
                f"- (EUR {multiplier*100:.2f} = BTC {multiplier*100/self.latest_values[BTC_EUR][SELL_SIDE]} " + \
                f"= ETH {multiplier*100/self.latest_values[ETH_EUR][BUY_SIDE]}) " + \
                f"- ratio: {self.latest_values[BTC_TO_ETH_RATIO][BTC_SELL_SIDE][LATEST]:.4f} Δ={delta_ratio:.2f}"
        elif transfer == TRANSFER_ETH_TO_BTC:
            delta_ratio = self.latest_values[BTC_TO_ETH_RATIO][ETH_SELL_SIDE][ANCHOR] - self.latest_values[BTC_TO_ETH_RATIO][ETH_SELL_SIDE][LATEST]
            multiplier = int(delta_ratio / self.latest_values[BTC_TO_ETH_RATIO][ETH_SELL_SIDE][ANCHOR] / RATIO_THRESHOLD)
            return f"{self.latest_values[TIMESTAMP]} : TRANSFER from ETH to BTC " + \
                f"- (EUR {multiplier*100:.2f} = BTC {multiplier*100/self.latest_values[BTC_EUR][BUY_SIDE]} " + \
                f"= ETH {multiplier*100/self.latest_values[ETH_EUR][SELL_SIDE]}) " + \
                f"- ratio: {self.latest_values[BTC_TO_ETH_RATIO][ETH_SELL_SIDE][LATEST]:.4f} Δ={delta_ratio:.2f}"

    def latest_values_to_df(self):
        return pd.DataFrame(data={
            TIMESTAMP: self.latest_values[TIMESTAMP],
            'BTC-EUR sell': self.latest_values[BTC_EUR][SELL_SIDE],
            'BTC-EUR buy': self.latest_values[BTC_EUR][BUY_SIDE],
            'RATIO-BTC sell latest': self.latest_values[BTC_TO_ETH_RATIO][BTC_SELL_SIDE][LATEST],
            'RATIO-BTC sell anchor': self.latest_values[BTC_TO_ETH_RATIO][BTC_SELL_SIDE][ANCHOR],
            'RATIO-ETH sell latest': self.latest_values[BTC_TO_ETH_RATIO][ETH_SELL_SIDE][LATEST],
            'RATIO-ETH sell anchor': self.latest_values[BTC_TO_ETH_RATIO][ETH_SELL_SIDE][ANCHOR],
        }, index=[0])

    def __init__(self, df_lock):
        self.ws = None
        self.thread = None
        self.thread_running = False
        self.thread_keepalive = None
        self.df_lock = df_lock
        self.latest_values = {
            BTC_EUR: {SELL_SIDE: None, BUY_SIDE: None, SEQUENCE_NUMBER: None},
            ETH_EUR: {SELL_SIDE: None, BUY_SIDE: None, SEQUENCE_NUMBER: None},
            BTC_TO_ETH_RATIO: {
                BTC_SELL_SIDE: {ANCHOR: None, LATEST: None},
                ETH_SELL_SIDE: {ANCHOR: None, LATEST: None},
            },
            TIMESTAMP: None
        }
        self.state_filename = "coinbase_socket.json"
        try:
            logger.info(f"Loading initial values from file {self.state_filename}")
            self._load_latest_values()
        except FileNotFoundError:
            logger.warning(f"File {self.state_filename} not found, starting with blank values")
        # message dataframe
        self.msg_df = pd.DataFrame()
        # latest value dataframe
        self.latest_values_df = pd.DataFrame()
        # message counters
        self.total_msg_counter = 0
        self.partial_msg_counter = 0

    def update_latest_values(self, msg):
        # let's invert the side
        side = SELL_SIDE if msg[SIDE] == BUY_SIDE else BUY_SIDE
        price = float(msg[PRICE])
        msg_time = msg[MSG_TIME]
        # update latest values (price, time, seq time)
        if msg[PRODUCT_ID] == BTC_EUR:
            self.latest_values[BTC_EUR][side] = price
            self.latest_values[TIMESTAMP] = msg_time
            if self.latest_values[BTC_EUR][SEQUENCE_NUMBER]:
                if msg[SEQUENCE_NUMBER] == self.latest_values[BTC_EUR][SEQUENCE_NUMBER]:
                    logger.debug(f"Sequence number repeated: {msg[SEQUENCE_NUMBER]}")
                elif msg[SEQUENCE_NUMBER] > self.latest_values[BTC_EUR][SEQUENCE_NUMBER] + 1:
                    logger.debug(f"Sequence number dropped: {msg[SEQUENCE_NUMBER] - self.latest_values[BTC_EUR][SEQUENCE_NUMBER] + 1}")
            self.latest_values[BTC_EUR][SEQUENCE_NUMBER] = msg[SEQUENCE_NUMBER]
        elif msg[PRODUCT_ID] == ETH_EUR:
            self.latest_values[ETH_EUR][side] = price
            self.latest_values[TIMESTAMP] = msg_time
            if self.latest_values[ETH_EUR][SEQUENCE_NUMBER]:
                if msg[SEQUENCE_NUMBER] == self.latest_values[ETH_EUR][SEQUENCE_NUMBER]:
                    logger.debug(f"Sequence number repeated: {msg[SEQUENCE_NUMBER]}")
                elif msg[SEQUENCE_NUMBER] > self.latest_values[ETH_EUR][SEQUENCE_NUMBER] + 1:
                    logger.debug(f"Sequence number dropped: {msg[SEQUENCE_NUMBER] - self.latest_values[ETH_EUR][SEQUENCE_NUMBER] + 1}")
            self.latest_values[ETH_EUR][SEQUENCE_NUMBER] = msg[SEQUENCE_NUMBER]
        # update ratio
        if (msg[PRODUCT_ID] == BTC_EUR and side == SELL_SIDE) or \
            (msg[PRODUCT_ID] == ETH_EUR and side == BUY_SIDE):
                # BTC to ETH (BTC sell side)
                if self.latest_values[BTC_EUR][SELL_SIDE] and \
                    self.latest_values[ETH_EUR][BUY_SIDE]:
                        # both values are set
                        self.latest_values[BTC_TO_ETH_RATIO][BTC_SELL_SIDE][LATEST] = self.latest_values[BTC_EUR][SELL_SIDE] / self.latest_values[ETH_EUR][BUY_SIDE]
                        # anchor check
                        if self.latest_values[BTC_TO_ETH_RATIO][BTC_SELL_SIDE][ANCHOR] == None:
                            self.latest_values[BTC_TO_ETH_RATIO][BTC_SELL_SIDE][ANCHOR] = self.latest_values[BTC_TO_ETH_RATIO][BTC_SELL_SIDE][LATEST]
                            self._save_latest_values()
                        # we only sell BTC if the ratio goes higher than the anchor
                        elif self.latest_values[BTC_TO_ETH_RATIO][BTC_SELL_SIDE][LATEST] > self.latest_values[BTC_TO_ETH_RATIO][BTC_SELL_SIDE][ANCHOR]:
                            delta_ratio = self.latest_values[BTC_TO_ETH_RATIO][BTC_SELL_SIDE][LATEST] - self.latest_values[BTC_TO_ETH_RATIO][BTC_SELL_SIDE][ANCHOR]
                            increase_ratio = delta_ratio / self.latest_values[BTC_TO_ETH_RATIO][BTC_SELL_SIDE][ANCHOR]
                            if  increase_ratio >= RATIO_THRESHOLD:
                                logger.info("Ratio {increase_ratio:.3f} is higher than threshold {RATIO_THRESHOLD} => Sell BTC and Buy ETH !")
                                self.latest_values[BTC_TO_ETH_RATIO][BTC_SELL_SIDE][ANCHOR] = self.latest_values[BTC_TO_ETH_RATIO][BTC_SELL_SIDE][LATEST]
                                self.latest_values[BTC_TO_ETH_RATIO][ETH_SELL_SIDE][ANCHOR] = self.latest_values[BTC_TO_ETH_RATIO][ETH_SELL_SIDE][LATEST]
                                self._save_latest_values()
                                tgm_msg = self._get_transfer_message(TRANSFER_BTC_TO_ETH)
                                logger.info(f"Telegram msg [skip={args.dont_send}]: {tgm_msg}")
                                if not args.dont_send:
                                    sendMessageToTelegram(tgm_msg)
        elif (msg[PRODUCT_ID] == ETH_EUR and side == SELL_SIDE) or \
            (msg[PRODUCT_ID] == BTC_EUR and side == BUY_SIDE):
                # ETH to BTC
                if self.latest_values[BTC_EUR][BUY_SIDE] and \
                    self.latest_values[ETH_EUR][SELL_SIDE]:
                        # both values are set
                        self.latest_values[BTC_TO_ETH_RATIO][ETH_SELL_SIDE][LATEST] = self.latest_values[BTC_EUR][BUY_SIDE] / self.latest_values[ETH_EUR][SELL_SIDE]
                        # anchor check
                        if self.latest_values[BTC_TO_ETH_RATIO][ETH_SELL_SIDE][ANCHOR] == None:
                            self.latest_values[BTC_TO_ETH_RATIO][ETH_SELL_SIDE][ANCHOR] = self.latest_values[BTC_TO_ETH_RATIO][ETH_SELL_SIDE][LATEST]
                            self._save_latest_values()
                        # we only sell ETH if the ratio goes lower than the anchor
                        elif self.latest_values[BTC_TO_ETH_RATIO][ETH_SELL_SIDE][LATEST] < self.latest_values[BTC_TO_ETH_RATIO][ETH_SELL_SIDE][ANCHOR]:
                            delta_ratio = self.latest_values[BTC_TO_ETH_RATIO][ETH_SELL_SIDE][ANCHOR] - self.latest_values[BTC_TO_ETH_RATIO][ETH_SELL_SIDE][LATEST]
                            increase_ratio = delta_ratio / self.latest_values[BTC_TO_ETH_RATIO][ETH_SELL_SIDE][ANCHOR]
                            if increase_ratio >= RATIO_THRESHOLD:
                                logger.info(f"Ratio {increase_ratio:.3f} is lower than threshold {RATIO_THRESHOLD} => Sell ETH and Buy BTC !")
                                self.latest_values[BTC_TO_ETH_RATIO][BTC_SELL_SIDE][ANCHOR] = self.latest_values[BTC_TO_ETH_RATIO][BTC_SELL_SIDE][LATEST]
                                self.latest_values[BTC_TO_ETH_RATIO][ETH_SELL_SIDE][ANCHOR] = self.latest_values[BTC_TO_ETH_RATIO][ETH_SELL_SIDE][LATEST]
                                self._save_latest_values()
                                tgm_msg = self._get_transfer_message(TRANSFER_ETH_TO_BTC)
                                logger.info(f"Telegram msg [skip={args.dont_send}]: {tgm_msg}")
                                if not args.dont_send:
                                    sendMessageToTelegram(tgm_msg)
        self.df_lock.acquire()
        # update msg dataframes
        new_msg_df = pd.json_normalize(msg)
        new_msg_df[MSG_TIME] = pd.to_datetime(new_msg_df[MSG_TIME])
        # conversion to float
        new_msg_df[PRICE] = pd.to_numeric(new_msg_df[PRICE], errors='coerce')
        for col in new_msg_df.columns:
            if col not in self.msg_df.columns:
                self.msg_df[col] = ''
        self.msg_df = self.msg_df.append(new_msg_df, ignore_index=True)
        utc_now = pytz.utc.localize(datetime.utcnow())
        min_time_utc = pd.to_datetime(utc_now-timedelta(hours=PLOT_HISTORY_HOURS), utc=True)
        self.msg_df = self.msg_df.drop(self.msg_df[self.msg_df[MSG_TIME] < min_time_utc].index)
        # update latest_values df
        new_latest_df = self.latest_values_to_df()
        new_latest_df[TIMESTAMP] = pd.to_datetime(new_latest_df[TIMESTAMP])
        for col in new_latest_df.columns:
            if col not in self.latest_values_df.columns:
                self.latest_values_df[col] = ''
        self.latest_values_df = self.latest_values_df.append(new_latest_df, ignore_index=True)
        self.latest_values_df = self.latest_values_df.drop(self.latest_values_df[self.latest_values_df[TIMESTAMP] < min_time_utc].index)
        self.df_lock.release()

    def main(self):

        def websocket_connect_and_subscribe():
            try:
                self.ws = create_connection(self.ENDPOINT)
            except:
                logger.error(f"Unable to establish connection with {self.ENDPOINT}")
                sys.exit(-1)
            self.ws.send(json.dumps(self.SUBSCRIPTION_1))

        def websocket_thread():

            websocket_connect_and_subscribe()

            thread_keepalive.start()
            while not self.thread_running:
                try:
                    data = self.ws.recv()
                    if data != "":
                        msg = json.loads(data)
                    else:
                        msg = {}
                except ValueError as e:
                    print(e)
                    print("{} - data: {}".format(e, data))
                except Exception as e:
                    print(e)
                    if str(e) == "Connection is already closed.":
                        try:
                            if self.ws:
                                self.ws.close()
                        except WebSocketConnectionClosedException:
                            logger.error("Exception: WebSocketConnectionClosedException")
                        websocket_connect_and_subscribe()
                else:
                    if msg[MSG_TYPE] not in [MSG_TYPE_MATCH, MSG_TYPE_LAST_MATCH]:
                        logging.debug(f">>>>>>> Message type: {msg[MSG_TYPE]}")
                    if msg[MSG_TYPE] in [MSG_TYPE_MATCH, MSG_TYPE_LAST_MATCH]:
                        self.total_msg_counter += 1
                        self.partial_msg_counter += 1
                        logger.debug(f"Received new msg - partial counter: {self.partial_msg_counter} - total counter: {self.total_msg_counter}")
                        logger.debug(msg)
                        self.update_latest_values(msg)
                    else:
                        pass
            logger.warning("Thread running is {self.thread_running}")
            logger.warning("Closing socket")
            try:
                if self.ws:
                    self.ws.close()
            except WebSocketConnectionClosedException:
                logger.error("Exception: WebSocketConnectionClosedException")
                pass
            finally:
                thread_keepalive.join()

        def websocket_keepalive(interval=30):
            while self.ws.connected:
                self.ws.ping("keepalive")
                time.sleep(interval)

        thread = Thread(target=websocket_thread)
        thread_keepalive = Thread(target=websocket_keepalive)
        thread.start()
    
    def reset_partial_msg_counter(self):
        self.partial_msg_counter = 0

    def get_msg_counters(self):
        return (self.partial_msg_counter, self.total_msg_counter, )

def get_delta(a, b):
    return abs(a-b)/min(a,b)

def plot_figure(cb_socket, picture_filename):
    fig, ax = plt.subplots(nrows=1, ncols=3, sharey=False, figsize=(18,6))
    ax[0].set(title=f"BTC {100*get_delta(cb_socket.latest_values[BTC_EUR][SELL_SIDE],cb_socket.latest_values['BTC-EUR']['buy']):.3f}%")
    ax[0].axhline(y=cb_socket.latest_values[BTC_EUR][SELL_SIDE], color='r', linestyle='-', label = 'sell')
    ax[0].axhline(y=cb_socket.latest_values[BTC_EUR][BUY_SIDE], color='b', linestyle='-', label = 'buy')
    ax[0].autoscale(enable=True, axis='both', tight=None)
    ax[0].get_xaxis().set_visible(False)
    ax[0].set_ylabel('EUR')
    ax[0].legend()
    ax[1].set(title=f"ETH {100*get_delta(cb_socket.latest_values[ETH_EUR][SELL_SIDE], cb_socket.latest_values['ETH-EUR']['buy']):.3f}%")
    ax[1].axhline(y=cb_socket.latest_values[ETH_EUR][SELL_SIDE], color='r', linestyle='-', label = 'sell')
    ax[1].axhline(y=cb_socket.latest_values[ETH_EUR][BUY_SIDE], color='b', linestyle='-', label = 'buy')
    ax[1].autoscale(enable=True, axis='both', tight=None)
    ax[1].get_xaxis().set_visible(False)
    ax[1].set_ylabel('EUR')
    ax[1].legend()
    ax[2].set(title=f"Ratio {cb_socket.latest_values[TIMESTAMP]}")
    ax[2].axhline(y=1.01*cb_socket.latest_values[BTC_TO_ETH_RATIO][BTC_SELL_SIDE][ANCHOR], color='r', linestyle=':', label = 'btc-sell threshold')
    ax[2].axhline(y=cb_socket.latest_values[BTC_TO_ETH_RATIO][BTC_SELL_SIDE][LATEST], color='r', linestyle='-', label = 'btc-sell latest')
    ax[2].axhline(y=.99*cb_socket.latest_values[BTC_TO_ETH_RATIO][ETH_SELL_SIDE][ANCHOR], color='b', linestyle=':', label = 'eth-sell threshold')
    ax[2].axhline(y=cb_socket.latest_values[BTC_TO_ETH_RATIO][ETH_SELL_SIDE][LATEST], color='b', linestyle='-', label = 'eth-sell latest')
    ax[2].autoscale(enable=True, axis='both', tight=None)
    ax[2].get_xaxis().set_visible(False)
    ax[2].legend()
    plt.savefig(picture_filename)

def plot_figure_2(cb_socket, picture_filename):
    fig, ax = plt.subplots(nrows=1, ncols=3, sharey=False, figsize=(18,6))
    ax[0].set(title=f"BTC {100*get_delta(cb_socket.latest_values[BTC_EUR][SELL_SIDE],cb_socket.latest_values[BTC_EUR][BUY_SIDE]):.3f}%")
    x_values_btc_sell = cb_socket.msg_df[(cb_socket.msg_df[PRODUCT_ID] == BTC_EUR) & (cb_socket.msg_df[SIDE] == SELL_SIDE)][MSG_TIME]
    x_points_btc_sell = x_values_btc_sell.shape[0]
    x_values_btc_buy = cb_socket.msg_df[(cb_socket.msg_df[PRODUCT_ID] == BTC_EUR) & (cb_socket.msg_df[SIDE] == BUY_SIDE)][MSG_TIME]
    # x_points_btc_buy = x_values_btc_buy.shape[0]
    y_values_btc_sell = cb_socket.msg_df[(cb_socket.msg_df[PRODUCT_ID] == BTC_EUR) & (cb_socket.msg_df[SIDE] == SELL_SIDE)][PRICE]
    ax[0].plot(x_values_btc_sell, 
        y_values_btc_sell,
        color='b', label = 'sell')
    ax[0].plot(x_values_btc_buy,
        cb_socket.msg_df[(cb_socket.msg_df[PRODUCT_ID] == BTC_EUR) & (cb_socket.msg_df[SIDE] == BUY_SIDE)][PRICE],
        color='r', label = 'buy')
    # set x_ticks and format
    ax[0].set_xticks(x_values_btc_sell[1::int(x_points_btc_sell/4)])
    myFmt = mdates.DateFormatter('%H:%M')
    ax[0].xaxis.set_major_formatter(myFmt)
    # y_ticks
    delta_y_btc_sell = y_values_btc_sell.max()-y_values_btc_sell.min()
    ax[0].set_yticks(np.arange(y_values_btc_sell.min(), y_values_btc_sell.max() + delta_y_btc_sell/50, delta_y_btc_sell/10))
    ax[0].autoscale(enable=True, axis='both', tight=None)
    ax[0].set_ylabel('EUR')
    ax[0].legend()
    ax[1].set(title=f"ETH {100*get_delta(cb_socket.latest_values[ETH_EUR][SELL_SIDE], cb_socket.latest_values[ETH_EUR][BUY_SIDE]):.3f}%")
    x_values_eth_sell = cb_socket.msg_df[(cb_socket.msg_df[PRODUCT_ID] == ETH_EUR) & (cb_socket.msg_df[SIDE] == SELL_SIDE)][MSG_TIME]
    x_points_eth_sell = x_values_eth_sell.shape[0]
    x_values_eth_buy = cb_socket.msg_df[(cb_socket.msg_df[PRODUCT_ID] == ETH_EUR) & (cb_socket.msg_df[SIDE] == BUY_SIDE)][MSG_TIME]
    y_values_eth_sell = cb_socket.msg_df[(cb_socket.msg_df[PRODUCT_ID] == ETH_EUR) & (cb_socket.msg_df[SIDE] == SELL_SIDE)][PRICE]
    ax[1].plot(x_values_eth_sell, 
        y_values_eth_sell,
        color='b', label = 'sell')
    ax[1].plot(x_values_eth_buy,
        cb_socket.msg_df[(cb_socket.msg_df[PRODUCT_ID] == ETH_EUR) & (cb_socket.msg_df[SIDE] == BUY_SIDE)][PRICE],
        color='r', label = 'buy')
    # set x_ticks and format
    ax[1].set_xticks(x_values_eth_sell[1::int(x_points_eth_sell/4)])
    ax[1].xaxis.set_major_formatter(myFmt)
    # y_ticks
    delta_y_eth_sell = y_values_eth_sell.max()-y_values_eth_sell.min()
    ax[1].set_yticks(np.arange(y_values_eth_sell.min(), y_values_eth_sell.max() + delta_y_eth_sell/50, delta_y_eth_sell/10))
    ax[1].autoscale(enable=True, axis='both', tight=None)
    ax[1].set_ylabel('EUR')
    ax[1].legend()
    ax[2].set(title=f'Ratio {cb_socket.latest_values_df.tail(1)[TIMESTAMP].dt.strftime("%m/%d %H:%M:%S").values[0]}')
    ax[2].plot(cb_socket.latest_values_df[TIMESTAMP], 
        cb_socket.latest_values_df['RATIO-BTC sell latest'],
        color='r', linestyle='-', label = 'BTC sell latest')
    ax[2].plot(cb_socket.latest_values_df[TIMESTAMP], 
        (1+RATIO_THRESHOLD) * cb_socket.latest_values_df['RATIO-BTC sell anchor'],
        color='r', linestyle=':', label = 'BTC sell threshold')
    ax[2].plot(cb_socket.latest_values_df[TIMESTAMP], 
        cb_socket.latest_values_df['RATIO-ETH sell latest'],
        color='b', linestyle='-', label = 'ETH sell latest')
    ax[2].plot(cb_socket.latest_values_df[TIMESTAMP], 
        (1-RATIO_THRESHOLD) * cb_socket.latest_values_df['RATIO-ETH sell anchor'],
        color='b', linestyle=':', label = 'ETH sell threshold')
    # set x_ticks and format
    x_points = cb_socket.latest_values_df[TIMESTAMP].shape[0]
    ax[2].set_xticks(cb_socket.latest_values_df[TIMESTAMP][1::int(x_points/4)])
    ax[2].xaxis.set_major_formatter(myFmt)
    # y ticks
    y_max = max(
        cb_socket.latest_values_df['RATIO-BTC sell latest'].max(),
        (1+RATIO_THRESHOLD) * cb_socket.latest_values_df['RATIO-BTC sell anchor'].max(),
        cb_socket.latest_values_df['RATIO-ETH sell latest'].max(),
        (1-RATIO_THRESHOLD) * cb_socket.latest_values_df['RATIO-ETH sell anchor'].max()
    )
    y_min = min(
        cb_socket.latest_values_df['RATIO-BTC sell latest'].min(),
        (1+RATIO_THRESHOLD) * cb_socket.latest_values_df['RATIO-BTC sell anchor'].min(),
        cb_socket.latest_values_df['RATIO-ETH sell latest'].min(),
        (1-RATIO_THRESHOLD) * cb_socket.latest_values_df['RATIO-ETH sell anchor'].min()
    )
    delta_y_ratio = y_max-y_min
    ax[2].set_yticks(np.arange(y_min, y_max+delta_y_ratio/50, delta_y_ratio/10))
    ax[2].autoscale(enable=True, axis='both', tight=None)
    ax[2].legend()
    plt.savefig(picture_filename)

if __name__ == "__main__":
    df_lock = Lock()
    cb_socket = CoinbaseSocket(df_lock)
    cb_socket.main()
    counter = 0
    loops_per_publication = int(60 * args.fig_pub_hours / args.show_info_minutes)
    while True:
        logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
        (partial_msg_cnt, total_msg_cnt, ) = cb_socket.get_msg_counters()
        logger.info(f"{counter} / {loops_per_publication} - msgs in this loop: {partial_msg_cnt} - total msgs: {total_msg_cnt}")
        logger.info(f"Latest values: {cb_socket.latest_values}")
        logger.info(f"Dataframe rows: msgs: {cb_socket.msg_df.shape[0]} - latest: {cb_socket.latest_values_df.shape[0]}")
        logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
        time.sleep(60 * args.show_info_minutes)
        counter += 1
        if counter >= 60 * args.fig_pub_hours / args.show_info_minutes:
            counter = 0
            df_lock.acquire()
            # plot_figure(cb_socket, "BTC-ETH realtime ratio.png")
            # sendPhoto("BTC-ETH realtime ratio.png")
            plot_figure_2(cb_socket, "BTC-ETH realtime ratio 2.png")
            image_filename = "BTC-ETH realtime ratio 2.png"
            logger.info(f"Telegram picture [skip={args.dont_send}]: {image_filename}")
            if not args.dont_send:
                sendPhoto(image_filename)
            df_lock.release()
