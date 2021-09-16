import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

from coinbase import Coinbase
import logging
import sys

import telepot

from mysecrets import Secrets

import pdb

# Telegram
bot = telepot.Bot(Secrets.TELEGRAM_TOKEN)

def sendMessageToTelegram(text):
    bot.sendMessage(Secrets.TELEGRAM_CHANNEL_ID, text)

def sendPhoto(image_filename):
    bot.sendPhoto(Secrets.TELEGRAM_CHANNEL_ID, photo=open(image_filename, 'rb'))


def analyze_product(product, history_days, moving_average_days, above_below_threshold):
    cb_object = Coinbase(product=product, 
                    logging_level=logging.WARNING)
    start_date = datetime.now() - timedelta(days=history_days)
    end_date = datetime.now()
    moving_average_label = f"MA{moving_average_days}"
    cb_object.loadHistory(start_date = start_date, 
                           end_date = end_date,
                           granularity = 86400,
                           moving_average = moving_average_days)
    cb_object.calculateBuy(moving_average = moving_average_days, below_threshold = above_below_threshold)
    cb_object.calculateSell(moving_average = moving_average_days, above_threshold = above_below_threshold)
    cb_object.df[-history_days:].plot(title=f"{product} THR={above_below_threshold}", x='Time', y=['Close','MA20','Buy','Sell'], figsize=(15,6))
    plt.savefig(f"{product}.png")
    sendPhoto(f"{product}.png")

    # Volume
    cb_object.df[-history_days:].plot(title=f"{product}", x='Time', y=['Volume'], figsize=(15,6))
    plt.savefig(f"{product}-Volume.png")
    sendPhoto(f"{product}-Volume.png")

    # Moving average
    df_ma = pd.DataFrame({'Window': range(-50, -10)})
    df_ma['MA'] = df_ma.apply(lambda row: cb_object.df['Close'][row['Window']:].mean(), axis=1)
    fig, ax = plt.subplots(figsize=(15,6))
    ax.plot(df_ma['Window'], df_ma['MA'])
    current_price = Coinbase.getPrice(product)
    if current_price > float(df_ma['MA'][-1:]):
        index = -2
        try:
            while current_price > float(df_ma['MA'][index:index+1]):
                index -= 1
            ma_status = f"> MA for {-index-1} days"
        except:
            ma_status = "> MA"
    elif current_price < float(df_ma['MA'][-1:]):
        index = -2
        try:
            while current_price < float(df_ma['MA'][index:index+1]):
                index -= 1
            ma_status = f"< MA for {-index-1} days"
        except:
            ma_status = "< MA"
    else:
        print("MA = price")
    ax.hlines(y=current_price, xmin=df_ma['Window'].min(), xmax=df_ma['Window'].max(), linewidth=2, color='r', label=f"Today's USD {current_price:.2f}")
    ax.legend()
    plt.title(f"{product} - {ma_status}")
    plt.xlabel('Window')
    plt.savefig(f"{product}-MA.png")
    sendPhoto(f"{product}-MA.png")



if __name__ == "__main__":
    if len(sys.argv) < 2:
        logging.error(f"Usage  : {sys.argv[0]} <product> [<days>] [<moving_average>] [<above_below_threshold>]")
        logging.error(f"Default:")
        logging.error(f"         <days>: 90")
        logging.error(f"         <moving_average>: 20")
        logging.error(f"         <above_below_threshold>: 0.05")
        sys.exit(-1)
    days = 90
    moving_average = 20
    above_below_threshold = 0.05
    product = sys.argv[1]
    try:
        days = int(sys.argv[2])
        moving_average = int(sys.argv[3])
        above_below_threshold = int(sys.argv[4])
    except:
        pass
    analyze_product(product, days, moving_average, above_below_threshold)
