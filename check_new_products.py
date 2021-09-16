from coinbase import Coinbase
import json
#import requests
import telepot

from mysecrets import Secrets

# Telegram
bot = telepot.Bot(Secrets.TELEGRAM_TOKEN)

def sendMessageToTelegram(text):
    #req = requests.get(f"https://api.telegram.org/bot{Secrets.TELEGRAM_TOKEN}/sendMessage?chat_id={Secrets.TELEGRAM_CHANNEL_ID}&text={text}")
    bot.sendMessage(Secrets.TELEGRAM_CHANNEL_ID, text)

def sendPhoto(image_filename):
    bot.sendPhoto(Secrets.TELEGRAM_CHANNEL_ID, photo=open(image_filename, 'rb'))

PRODUCTS_FILE = 'coinbase_products.json'

products = Coinbase.getProductList()
with open(PRODUCTS_FILE) as fp:
    last_products = json.load(fp)
Updated = False
for product in products:
    id = product['id']
    New = True
    for last_product in last_products:
        if last_product['id'] == id:
            New = False
            if product != last_product:
                print(f"Product {id} changed: {last_product} -> {product}")
                sendMessageToTelegram(f"Product {id} changed: {last_product} -> {product}")
                Updated = True
            continue
    if New is True:
        print(f"New product: {product}")
        sendMessageToTelegram(f"New product: {product}")
        Updated = True

if Updated is True:
    sendMessageToTelegram(f"Product List Check completed, Updated={Updated}")
    with open(PRODUCTS_FILE, 'w') as fp:
        json.dump(products, fp)
else:
    sendMessageToTelegram(f"Product List Check completed, Updated={Updated}")

