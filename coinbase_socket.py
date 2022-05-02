import json, time, logging
from os import stat
from threading import Thread, Lock
from websocket import create_connection, WebSocketConnectionClosedException
import pandas as pd
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import pytz
import telepot
from mysecrets import Secrets

# Telegram
bot = telepot.Bot(Secrets.TELEGRAM_TOKEN)

def sendMessageToTelegram(text):
    bot.sendMessage(Secrets.TELEGRAM_CHANNEL_ID, text)

def sendPhoto(image_filename):
    bot.sendPhoto(Secrets.TELEGRAM_CHANNEL_ID, photo=open(image_filename, 'rb'))


logger = logging.getLogger("Coinbase Socket")
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger.setLevel(logging.DEBUG)

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

RATIO_THRESHOLD = .01

PLOT_HISTORY_HOURS = 6

class CoinbaseSocket:

    ENDPOINT = "wss://ws-feed.pro.coinbase.com"
    SUBSCRIPTION_1 = {
                        "type": "subscribe",
                        "product_ids": ['BTC-EUR', 'ETH-EUR'],
                        "channels": ["matches"],
                    }

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
        # message dataframe
        self.msg_df = pd.DataFrame()
        # latest value dataframe
        self.latest_values_df = pd.DataFrame()

    def update_latest_values(self, msg):
        # let's invert the side
        side = SELL_SIDE if msg[SIDE] == BUY_SIDE else BUY_SIDE
        price = float(msg[PRICE])
        if msg[PRODUCT_ID] == BTC_EUR:
            self.latest_values[BTC_EUR][side] = price
            self.latest_values[TIMESTAMP] = msg[MSG_TIME]
            if self.latest_values[BTC_EUR][SEQUENCE_NUMBER]:
                if msg[SEQUENCE_NUMBER] == self.latest_values[BTC_EUR][SEQUENCE_NUMBER]:
                    logger.info(f"Sequence number repeated: {msg[SEQUENCE_NUMBER]}")
                elif msg[SEQUENCE_NUMBER] > self.latest_values[BTC_EUR][SEQUENCE_NUMBER] + 1:
                    logger.info(f"Sequence number dropped: {msg[SEQUENCE_NUMBER] - self.latest_values[BTC_EUR][SEQUENCE_NUMBER] + 1}")
            self.latest_values[BTC_EUR][SEQUENCE_NUMBER] = msg[SEQUENCE_NUMBER]
        elif msg[PRODUCT_ID] == ETH_EUR:
            self.latest_values[ETH_EUR][side] = price
            self.latest_values[TIMESTAMP] = msg[MSG_TIME]
            if self.latest_values[ETH_EUR][SEQUENCE_NUMBER]:
                if msg[SEQUENCE_NUMBER] == self.latest_values[ETH_EUR][SEQUENCE_NUMBER]:
                    logger.info(f"Sequence number repeated: {msg[SEQUENCE_NUMBER]}")
                elif msg[SEQUENCE_NUMBER] > self.latest_values[ETH_EUR][SEQUENCE_NUMBER] + 1:
                    logger.info(f"Sequence number dropped: {msg[SEQUENCE_NUMBER] - self.latest_values[ETH_EUR][SEQUENCE_NUMBER] + 1}")
            self.latest_values[ETH_EUR][SEQUENCE_NUMBER] = msg[SEQUENCE_NUMBER]
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
                        # we only sell BTC if the ratio goes higher than the anchor
                        elif self.latest_values[BTC_TO_ETH_RATIO][BTC_SELL_SIDE][LATEST] > self.latest_values[BTC_TO_ETH_RATIO][BTC_SELL_SIDE][ANCHOR]:
                            delta_ratio = self.latest_values[BTC_TO_ETH_RATIO][BTC_SELL_SIDE][LATEST] - self.latest_values[BTC_TO_ETH_RATIO][BTC_SELL_SIDE][ANCHOR]
                            if delta_ratio / self.latest_values[BTC_TO_ETH_RATIO][BTC_SELL_SIDE][ANCHOR] >= RATIO_THRESHOLD:
                                logger.info("Ratio is higher than threshold => Sell BTC and Buy ETH !")
                                self.latest_values[BTC_TO_ETH_RATIO][BTC_SELL_SIDE][ANCHOR] = self.latest_values[BTC_TO_ETH_RATIO][BTC_SELL_SIDE][LATEST]
                                self.latest_values[BTC_TO_ETH_RATIO][ETH_SELL_SIDE][ANCHOR] = self.latest_values[BTC_TO_ETH_RATIO][ETH_SELL_SIDE][LATEST]
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
                        # we only sell ETH if the ratio goes lower than the anchor
                        elif self.latest_values[BTC_TO_ETH_RATIO][ETH_SELL_SIDE][LATEST] < self.latest_values[BTC_TO_ETH_RATIO][ETH_SELL_SIDE][ANCHOR]:
                            delta_ratio = self.latest_values[BTC_TO_ETH_RATIO][ETH_SELL_SIDE][ANCHOR] - self.latest_values[BTC_TO_ETH_RATIO][ETH_SELL_SIDE][LATEST]
                            if delta_ratio / self.latest_values[BTC_TO_ETH_RATIO][ETH_SELL_SIDE][ANCHOR] >= RATIO_THRESHOLD:
                                logger.info("Ratio is lower than threshold => Sell ETH and Buy BTC !")
                                self.latest_values[BTC_TO_ETH_RATIO][BTC_SELL_SIDE][ANCHOR] = self.latest_values[BTC_TO_ETH_RATIO][BTC_SELL_SIDE][LATEST]
                                self.latest_values[BTC_TO_ETH_RATIO][ETH_SELL_SIDE][ANCHOR] = self.latest_values[BTC_TO_ETH_RATIO][ETH_SELL_SIDE][LATEST]
        self.df_lock.acquire()
        # update msg dataframes
        new_msg_df = pd.json_normalize(msg)
        new_msg_df[MSG_TIME] = pd.to_datetime(new_msg_df[MSG_TIME])
        for col in new_msg_df.columns:
            if col not in self.msg_df.columns:
                self.msg_df[col] = ''
        self.msg_df = self.msg_df.append(new_msg_df, ignore_index=True)
        utc_now = pytz.utc.localize(datetime.utcnow())
        min_time_utc = pd.to_datetime(utc_now-timedelta(hours=PLOT_HISTORY_HOURS), utc=True)
        self.msg_df = self.msg_df.drop(self.msg_df[self.msg_df[MSG_TIME] < min_time_utc].index)
        # update latest_values df
        new_latest_df = self.latest_values_to_df()
        new_latest_df[MSG_TIME] = pd.to_datetime(new_msg_df[MSG_TIME])
        for col in new_latest_df.columns:
            if col not in self.latest_values_df.columns:
                self.latest_values_df[col] = ''
        self.latest_values_df = self.latest_values_df.append(new_latest_df, ignore_index=True)
        self.latest_values_df = self.latest_values_df.drop(self.latest_values_df[self.latest_values_df[TIMESTAMP] < min_time_utc].index)
        self.df_lock.release()

    def main(self):

        def websocket_thread():

            self.ws = create_connection(self.ENDPOINT)
            self.ws.send(json.dumps(self.SUBSCRIPTION_1))

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
                    print("{} - data: {}".format(e, data))
                else:
                    if msg[MSG_TYPE] not in [MSG_TYPE_MATCH, MSG_TYPE_LAST_MATCH]:
                        logging.info(f">>>>>>> Message type: {msg[MSG_TYPE]}")
                    if msg[MSG_TYPE] in [MSG_TYPE_MATCH, MSG_TYPE_LAST_MATCH]:
                        logger.info(msg)
                        self.update_latest_values(msg)
                    else:
                        pass

            try:
                if self.ws:
                    self.ws.close()
            except WebSocketConnectionClosedException:
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
    ax[0].plot(cb_socket.msg_df[(cb_socket.msg_df[PRODUCT_ID] == BTC_EUR) & (cb_socket.msg_df[SIDE] == SELL_SIDE)][MSG_TIME], 
        cb_socket.msg_df[(cb_socket.msg_df[PRODUCT_ID] == BTC_EUR) & (cb_socket.msg_df[SIDE] == SELL_SIDE)][PRICE],
        color='b', label = 'sell')
    ax[0].plot(cb_socket.msg_df[(cb_socket.msg_df[PRODUCT_ID] == BTC_EUR) & (cb_socket.msg_df[SIDE] == BUY_SIDE)][MSG_TIME],
        cb_socket.msg_df[(cb_socket.msg_df[PRODUCT_ID] == BTC_EUR) & (cb_socket.msg_df[SIDE] == BUY_SIDE)][PRICE],
        color='r', label = 'buy')
    ax[0].autoscale(enable=True, axis='both', tight=None)
    ax[0].set_ylabel('EUR')
    ax[0].legend()
    ax[1].set(title=f"ETH {100*get_delta(cb_socket.latest_values[ETH_EUR][SELL_SIDE], cb_socket.latest_values[ETH_EUR][BUY_SIDE]):.3f}%")
    ax[1].plot(cb_socket.msg_df[(cb_socket.msg_df[PRODUCT_ID] == ETH_EUR) & (cb_socket.msg_df[SIDE] == SELL_SIDE)][MSG_TIME], 
        cb_socket.msg_df[(cb_socket.msg_df[PRODUCT_ID] == ETH_EUR) & (cb_socket.msg_df[SIDE] == SELL_SIDE)][PRICE],
        color='b', label = 'sell')
    ax[1].plot(cb_socket.msg_df[(cb_socket.msg_df[PRODUCT_ID] == ETH_EUR) & (cb_socket.msg_df[SIDE] == BUY_SIDE)][MSG_TIME],
        cb_socket.msg_df[(cb_socket.msg_df[PRODUCT_ID] == ETH_EUR) & (cb_socket.msg_df[SIDE] == BUY_SIDE)][PRICE],
        color='r', label = 'buy')
    ax[1].autoscale(enable=True, axis='both', tight=None)
    ax[1].set_ylabel('EUR')
    ax[1].legend()
    ax[2].set(title=f"Ratio {cb_socket.latest_values_df.tail(1)[TIMESTAMP]}")
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

    ax[2].autoscale(enable=True, axis='both', tight=None)
    ax[2].legend()
    plt.savefig(picture_filename)

if __name__ == "__main__":
    df_lock = Lock()
    cb_socket = CoinbaseSocket(df_lock)
    cb_socket.main()
    counter = 0
    while True:
        logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
        logger.info(f"{counter}")
        logger.info(f"Latest values: {cb_socket.latest_values}")
        logger.info(f"Dataframe rows: msgs: {cb_socket.msg_df.shape[0]} - latest: {cb_socket.latest_values_df.shape[0]}")
        logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
        time.sleep(60)
        counter += 1
        if counter >= 5:
            counter = 0
            df_lock.acquire()
            plot_figure(cb_socket, "BTC-ETH realtime ratio.png")
            plot_figure_2(cb_socket, "BTC-ETH realtime ratio 2.png")
            sendPhoto("BTC-ETH realtime ratio.png")
            sendPhoto("BTC-ETH realtime ratio 2.png")
            df_lock.release()
