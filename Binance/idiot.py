# РОБОТ ИДИОТ
# ВЕРСИЯ 1.0

#!pip install websockets-client
# https://bablofil.ru/binance-webscokets/
# https://github.com/jsappme/node-binance-trader


import websocket


def on_open(ws):
    print("### connected ###")


def on_message(ws, message):
    print(message)


def on_error(ws, error):
    print(error)


def on_close(ws):
    print("### closed ###")



if __name__ == "__main__":
    ws = websocket.WebSocketApp("wss://stream.binance.com:9443/ws/itcbtc@aggTrade/ethbtc@aggTrade"
                                ,on_message=on_message
                                ,on_error=on_error
                                ,on_close=on_close)
    ws.on_open = on_open
    ws.run_forever()

