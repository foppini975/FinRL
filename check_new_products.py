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

def compareDictionaries(dict1, dict2):
    delta = {}
    for key in dict1:
        if key not in dict2:
            delta[key] = 'Removed'
    for key in dict2:
        if key not in dict1 or dict1[key] != dict2[key]:
            delta[key] = dict2[key]
    return delta

PRODUCTS_FILE = 'coinbase_products.json'

products = Coinbase.getProductList()
with open(PRODUCTS_FILE) as fp:
    last_products = json.load(fp)
Updated = False
for product in products:
    id = product['id']
    New = True
    aggregated_changes = {}
    for last_product in last_products:
        if last_product['id'] == id:
            New = False
            if product != last_product:
                print(f"Product {id} changed: {last_product} -> {product}")
                #sendMessageToTelegram(f"Product {id} changed: {compareDictionaries(last_product, product)}")

                delta = compareDictionaries(last_product, product)
                for key in delta:
                    if key in aggregated_changes:
                        aggregated_changes[key].append(id)
                    else:
                        aggregated_changes[key] = [id]

                Updated = True
            continue
    if New is True:
        print(f"New product: {product}")
        sendMessageToTelegram(f"New product: {product}")
        Updated = True
if aggregated_changes:
    sendMessageToTelegram(f"Updates: {aggregated_changes}")

if Updated is True:
    #sendMessageToTelegram(f"Product List Check completed, Updated={Updated}")
    with open(PRODUCTS_FILE, 'w') as fp:
        json.dump(products, fp)
else:
    pass
    #sendMessageToTelegram(f"Product List Check completed, Updated={Updated}")

