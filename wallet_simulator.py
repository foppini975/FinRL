from coinbase import Coinbase, Wallet
from datetime import date, datetime
import logging
import telepot
from mysecrets import Secrets

# Telegram
bot = telepot.Bot(Secrets.TELEGRAM_TOKEN)

def sendMessageToTelegram(text):
    bot.sendMessage(Secrets.TELEGRAM_CHANNEL_ID, text)

cb_btc = Coinbase('BTC-EUR', logging_level = logging.WARNING)
today_datetime = datetime.combine(date.today(), datetime.min.time())
cb_btc.loadHistory(datetime(2015, 5, 1), today_datetime)

cb_eth = Coinbase(product='ETH-EUR')
today_datetime = datetime.combine(date.today(), datetime.min.time())
cb_eth.loadHistory(datetime(2017, 5, 1), today_datetime)

ETH_T0 = 0.79380042
BTC_T0 = 0.03683716
wallet_0 = Wallet(cb_btc.df, market_name='BTC-EUR')
wallet_0.add_column(cb_df=cb_eth.df, market_name='ETH-EUR')
wallet_0.set_asset(date_string='2022-01-01', market_name='ETH-EUR', amount=ETH_T0)
wallet_0.set_asset(date_string='2022-01-01', market_name='BTC-EUR', amount=BTC_T0)
wallet_0.df["Wallet Value No Sim"] = wallet_0.df["Wallet Value"]

t0_value = wallet_0.get_total_value('2022-04-22')

print(f"April 22nd, 2022 value         : EUR {t0_value:.2f}")

current_value_no_transfer = wallet_0.get_final_value()

wallet_0.transfer(date_string='2022-04-22', from_market='ETH-EUR', to_market='BTC-EUR', eur_amount=99.98)
wallet_0.transfer(date_string='2022-04-25', from_market='BTC-EUR', to_market='ETH-EUR', eur_amount=99.99)

current_value_with_transfers = wallet_0.get_final_value()

motd = wallet_0.simulate(transfer_amount_eur=100, threshold=0.01)
if motd:
    sendMessageToTelegram(motd)

current_value_after_simulation = wallet_0.get_final_value()

wallet_0.to_excel('wallet_simulation.xlsx')

gain_percent_with_transfers = 100 * (current_value_with_transfers - current_value_no_transfer) / current_value_no_transfer
gain_percent_with_simulation = 100 * (current_value_after_simulation - current_value_no_transfer) / current_value_no_transfer

print(f"Today's value with no transfer : EUR {current_value_no_transfer:.2f}")
print(f"Today's value with simulation  : EUR {current_value_after_simulation:.2f}")
print(f"Gain with simulation           : {gain_percent_with_simulation:.1f}%")
print("\n")
print(f"Today's value with transfers   : EUR {current_value_with_transfers:.2f}")
print(f"Gain with transfers            : {gain_percent_with_transfers:.1f}%")