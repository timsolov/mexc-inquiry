import ccxt
import yaml
import time
import csv
import os

mexc = ccxt.mexc3()
mexc.load_markets()

rate_limit = 20 # requests per second
delay_api = 1 / rate_limit * 2
symbols = mexc.symbols
best_bid_ask_file_path = "best_bid_ask.csv"
trades_file_path = "trades.csv"

with open("config.yaml", 'r') as fob:
    config = yaml.load(fob, Loader=yaml.FullLoader)

if len(config['symbols']) > 0:
    symbols = config['symbols']
if len(config['best_bid_ask_file_path']):
    best_bid_ask_file_path = config['best_bid_ask_file_path']
if len(config['trades_file_path']):
    trades_file_path = config['trades_file_path']
     
if not os.path.exists(best_bid_ask_file_path):
    with open(best_bid_ask_file_path, 'w') as fob:
        book_csv = csv.writer(fob)
        book_csv.writerow(["time", "symbol", "bid", "ask"])
        
if not os.path.exists(trades_file_path):
    with open(trades_file_path, 'w') as fob:
        writer = csv.writer(fob)
        writer.writerow(["time", "timestamp", "symbol", "side", "takerOrMaker", "price", "amount", "cost"])

last_trade_id = ""

with open(best_bid_ask_file_path, 'a') as fob, open(trades_file_path, "a") as ft:
    book_csv = csv.writer(fob)
    trades_csv = csv.writer(ft)
    while True:
        for symbol in symbols:
            order_book = mexc.fetch_order_book(symbol)
            best_bid = order_book['bids'][0][0]
            best_ask = order_book['asks'][0][0]
            
            book_csv.writerow([time.time(), symbol, best_bid, best_ask])

            fob.flush()
            
            print("best bid and ask for", symbol, "are", best_bid, best_ask)
            
            trades = mexc.fetch_trades(symbol)
            # to prevent duplicated records we should find last saved trade id
            idx = 0
            for i, trade in enumerate(trades):
                if trade['id'] == last_trade_id:
                    idx = i
                    break
            for i in range(idx, len(trades)):
                trade = trades[i]
                
                last_trade_id = trade['id']
                # ---- correct buy sell status ---
                if trade['info']['m'] == False: side = 'buy'
                else: side ='sell'
                
                # ---- write new format ------
                trades_csv.writerow([time.time(), trade['timestamp'], trade['symbol'], side, trade['price'], trade['amount'], trade['cost']])

            ft.flush()
            
            print("there are", len(trades)-idx, "new trades")
            
            time.sleep(delay_api)
        time.sleep(config['delay'])
                

