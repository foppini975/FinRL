from coinbase import Coinbase
import logging
import sys
import time

import telepot

from mysecrets import Secrets

import pdb

"""
Launch with:
/home/crypto/FinRL/env_3.8/bin/python /home/crypto/FinRL/alert.py FET-USD .95 .90
/home/crypto/FinRL/env_3.8/bin/python -u /home/crypto/FinRL/alert.py ETH-EUR 3200 2850 20
"""

# Telegram
bot = telepot.Bot(Secrets.TELEGRAM_TOKEN)

def sendMessageToTelegram(text):
    #req = requests.get(f"https://api.telegram.org/bot{Secrets.TELEGRAM_TOKEN}/sendMessage?chat_id={Secrets.TELEGRAM_CHANNEL_ID}&text={text}")
    bot.sendMessage(Secrets.TELEGRAM_CHANNEL_ID, text)

def alert(product, high_threshold, low_threshold):
    try:
        price = Coinbase.getPrice(product)
    except:
        success = False
        return {'success': False}
    if high_threshold is not None and price >= high_threshold:
        return {'success': True, 'price': price, 'alert': True, 'condition': 'above'}
    if low_threshold is not None and price <= low_threshold:
        return {'success': True, 'price': price, 'alert': True, 'condition': 'below'}
    return {'success': True, 'price': price, 'alert': False}

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    if len(sys.argv) < 2:
        logging.error(f"Usage  : {sys.argv[0]} <product> <high_threshold> <low_threshold> [<threshold_step>]")
        logging.error(f"Default:")
        logging.error(f"         <threshold_step>: 0.01")
        logging.error(f"Note:")
        logging.error(f"         *: ignore")
        sys.exit(-1)
    product = sys.argv[1]
    # Thresholds
    try:
        if sys.argv[2] == "*":
            high_threshold = None
        else:    
            high_threshold = float(sys.argv[2])
    except:
        raise Exception("Wrong high_threshold parameter")
        sys.exit(-1)
    try:
        if sys.argv[2] == "*":
            low_threshold = None
        else:    
            low_threshold = float(sys.argv[3])
    except:
        raise Exception("Wrong low_threshold parameter")
        sys.exit(-1)
    # Threshold Step
    threshold_step = 0.01
    try:
        threshold_step = float(sys.argv[4])
        logging.info(f"Threshold step: {threshold_step}")
    except:
        pass
    REPEAT_TIME_SECONDS = 60
    while True:
        alert_dict = alert(product, high_threshold, low_threshold)
        if alert_dict['success'] is False:
            logging.error("Error getting price from Coinbase")
            time.sleep(REPEAT_TIME_SECONDS)
            continue
        logging.info(f"{time.time():.0f} - {product} Price: {alert_dict['price']:.2f} - High-Thr: {high_threshold:.2f} - Low-Thr: {low_threshold:.2f}")
        if alert_dict['alert'] is True and alert_dict['condition'] == 'above':
            sendMessageToTelegram(f"HIGH-THR ALERT! {product} = USD {alert_dict['price']}")
            logging.info(f"HIGH-THR ALERT! {product} = {alert_dict['price']}")
            high_threshold += threshold_step
        elif alert_dict['alert'] is True and alert_dict['condition'] == 'below':
            sendMessageToTelegram(f"LOW-THR ALERT! {product} = {alert_dict['price']}")
            logging.info(f"LOW-THR ALERT! {product} = {alert_dict['price']}")
            low_threshold -= threshold_step
        time.sleep(REPEAT_TIME_SECONDS)
