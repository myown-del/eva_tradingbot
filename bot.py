import pandas as pd
from binance.client import Client
from binance import AsyncClient, BinanceSocketManager
import json
from math import log10
import asyncio
from datetime import datetime
from binance.enums import *
from termcolor import colored
import nest_asyncio
nest_asyncio.apply()

pricing_arr = ["OHLC4", "HL2", "HLC3", "OC2", "Open", "High", "Low", "Close"]


def getTimeframeDef(seconds):
    if seconds == 60:
        return Client.KLINE_INTERVAL_1MINUTE
    elif seconds == 60*3:
        return Client.KLINE_INTERVAL_3MINUTE
    elif seconds == 60*5:
        return Client.KLINE_INTERVAL_5MINUTE
    elif seconds == 60*15:
        return Client.KLINE_INTERVAL_15MINUTE
    elif seconds == 60*30:
        return Client.KLINE_INTERVAL_30MINUTE
    elif seconds == 60*60:
        return Client.KLINE_INTERVAL_1HOUR
    elif seconds == 60*60*2:
        return Client.KLINE_INTERVAL_2HOUR
    elif seconds == 60*60*4:
        return Client.KLINE_INTERVAL_4HOUR
    elif seconds == 60*60*6:
        return Client.KLINE_INTERVAL_6HOUR
    elif seconds == 60*60*8:
        return Client.KLINE_INTERVAL_8HOUR
    elif seconds == 60*60*12:
        return Client.KLINE_INTERVAL_12HOUR
    elif seconds == 60*60*24:
        return Client.KLINE_INTERVAL_1DAY


def getTimeframedelay(seconds):
    if seconds == 60:
        return 60 - int(datetime.now().strftime("%S"))
    elif seconds == 60*3:
        return (60 - int(datetime.now().strftime("%S"))) + (2 - int(datetime.now().strftime("%M")) % 3)*60
    elif seconds == 60*5:
        return (60 - int(datetime.now().strftime("%S"))) + (4 - int(datetime.now().strftime("%M")) % 5)*60
    elif seconds == 60*15:
        return (60 - int(datetime.now().strftime("%S"))) + (14 - int(datetime.now().strftime("%M")) % 15)*60
    elif seconds == 60*30:
        return (60 - int(datetime.now().strftime("%S"))) + (29 - int(datetime.now().strftime("%M")) % 30)*60
    elif seconds == 60*60:
        return (60 - int(datetime.now().strftime("%S"))) + (59 - int(datetime.now().strftime("%M")))*60
    elif seconds == 60*60*2:
        hours = 1 - int(datetime.now().strftime("%H")) % 2
        minutes = hours*60 + (59 - int(datetime.now().strftime("%M")))
        return (60 - int(datetime.now().strftime("%S"))) + minutes*60
    elif seconds == 60*60*4:
        hours = 3 - int(datetime.now().strftime("%H")) % 4
        minutes = hours*60 + (59 - int(datetime.now().strftime("%M")))
        return (60 - int(datetime.now().strftime("%S"))) + minutes*60
    elif seconds == 60*60*6:
        hours = 5 - int(datetime.now().strftime("%H")) % 6
        minutes = hours*60 + (59 - int(datetime.now().strftime("%M")))
        return(60 - int(datetime.now().strftime("%S"))) + minutes*60
    elif seconds == 60*60*8:
        hours = 7 - int(datetime.now().strftime("%H")) % 8
        minutes = hours*60 + (59 - int(datetime.now().strftime("%M")))
        return(60 - int(datetime.now().strftime("%S"))) + minutes*60
    elif seconds == 60*60*12:
        hours = 11 - int(datetime.now().strftime("%H")) % 12
        minutes = hours*60 + (59 - int(datetime.now().strftime("%M")))
        return(60 - int(datetime.now().strftime("%S"))) + minutes*60
    elif seconds == 60*60*24:
        hours = 23 - int(datetime.now().strftime("%H"))
        minutes = hours*60 + (59 - int(datetime.now().strftime("%M")))
        return (60 - int(datetime.now().strftime("%S"))) + minutes*60
    else:
        return seconds


def getPriceFromOCHL(O, C, H, L, ticker, ismaopen, isfutures):
    if ismaopen and isfutures:
        if pairs_futures[ticker]['maopen_source'] == "OHLC4":
            return (O+C+H+L)/4
        elif pairs_futures[ticker]['maopen_source'] == "HL2":
            return (H+L)/2
        elif pairs_futures[ticker]['maopen_source'] == "HLC3":
            return (H+L+C)/3
        elif pairs_futures[ticker]['maopen_source'] == "OC2":
            return (O+C)/2
        elif pairs_futures[ticker]['maopen_source'] == "Open":
            return O
        elif pairs_futures[ticker]['maopen_source'] == "High":
            return H
        elif pairs_futures[ticker]['maopen_source'] == "Low":
            return L
        elif pairs_futures[ticker]['maopen_source'] == "Close":
            return C
    elif not ismaopen and isfutures:
        if pairs_futures[ticker]['maclose_source'] == "OHLC4":
            return (O+C+H+L)/4
        elif pairs_futures[ticker]['maclose_source'] == "HL2":
            return (H+L)/2
        elif pairs_futures[ticker]['maclose_source'] == "HLC3":
            return (H+L+C)/3
        elif pairs_futures[ticker]['maclose_source'] == "OC2":
            return (O+C)/2
        elif pairs_futures[ticker]['maclose_source'] == "Open":
            return O
        elif pairs_futures[ticker]['maclose_source'] == "High":
            return H
        elif pairs_futures[ticker]['maclose_source'] == "Low":
            return L
        elif pairs_futures[ticker]['maclose_source'] == "Close":
            return C
    elif ismaopen and not isfutures:
        if pairs_spot[ticker]['maopen_source'] == "OHLC4":
            return (O+C+H+L)/4
        elif pairs_spot[ticker]['maopen_source'] == "HL2":
            return (H+L)/2
        elif pairs_spot[ticker]['maopen_source'] == "HLC3":
            return (H+L+C)/3
        elif pairs_spot[ticker]['maopen_source'] == "OC2":
            return (O+C)/2
        elif pairs_spot[ticker]['maopen_source'] == "Open":
            return O
        elif pairs_spot[ticker]['maopen_source'] == "High":
            return H
        elif pairs_spot[ticker]['maopen_source'] == "Low":
            return L
        elif pairs_spot[ticker]['maopen_source'] == "Close":
            return C
    else:
        if pairs_spot[ticker]['maclose_source'] == "OHLC4":
            return (O+C+H+L)/4
        elif pairs_spot[ticker]['maclose_source'] == "HL2":
            return (H+L)/2
        elif pairs_spot[ticker]['maclose_source'] == "HLC3":
            return (H+L+C)/3
        elif pairs_spot[ticker]['maclose_source'] == "OC2":
            return (O+C)/2
        elif pairs_spot[ticker]['maclose_source'] == "Open":
            return O
        elif pairs_spot[ticker]['maclose_source'] == "High":
            return H
        elif pairs_spot[ticker]['maclose_source'] == "Low":
            return L
        elif pairs_spot[ticker]['maclose_source'] == "Close":
            return C


def calcParams(df, maopen_period, maclose_period, maopen_offset, maclose_offset, longs, shorts, isfutures, ticker):
    df['PriceOPEN'] = getPriceFromOCHL(df.Open, df.Close, df.High, df.Low, ticker, True, isfutures)
    df['PriceCLOSE'] = getPriceFromOCHL(df.Open, df.Close, df.High, df.Low, ticker, False, isfutures)
    df['MAOPEN'] = df.PriceOPEN.rolling(maopen_period).mean()
    df['MACLOSE'] = df.PriceCLOSE.rolling(maclose_period).mean()
    df.MAOPEN = df.MAOPEN.shift(maopen_offset)
    df.MACLOSE = df.MACLOSE.shift(maclose_offset)

    for long in enumerate(longs):
        df['LONG'+str(long[0])] = df.MAOPEN - (df.MAOPEN / 100)*long[1][0]
    if isfutures:
        for short in enumerate(shorts):
            df['SHORT'+str(short[0])] = df.MAOPEN + (df.MAOPEN / 100)*short[1][0]
    return df


async def createLimitOrderFutures(order):
    try:
        msg = await async_client.futures_create_order(**order)
        print("Поставил лимитный ордер.")
    except Exception as e:
        print(e)


def createBatchLimitOrdersFutures(orderbatch):
    try:
        msg = client.futures_place_batch_order(batchOrders=orderbatch)
        return msg
    except Exception as e:
        print(e)
        return []


async def createLimitOrderBatchSpot(orders):
    taskorders = []
    for order in orders:
        task = asyncio.create_task(createLimitOrderSpot(order))
        taskorders.append(task)
    return await asyncio.gather(*taskorders)


async def createLimitOrderSpot(order):
    try:
        msg = await async_client.order_limit(**order)
        return msg
    except Exception as e:
        print(e)
        return None


async def cancelAllOrdersFutures(ticker):
    try:
        msg = await async_client.futures_cancel_all_open_orders(symbol=ticker)
    except Exception as e:
        print("Ошибка в закрытии открытых ордеров.", "\n", e)


async def cancelAllOrdersSpot(ticker, orderIds):
    try:
        tocancel = []
        for orderId in orderIds:
            tocancel.append(asyncio.create_task(cancelOrderSpot(ticker, orderId)))
        await asyncio.gather(*tocancel)
    except Exception as e:
        print("Ошибка в закрытии открытых ордеров.", "\n", e)


async def cancelOrderSpot(ticker, orderId):
    try:
        msg = await async_client.cancel_order(symbol=ticker, orderId=orderId)
    except Exception as e:
        print("Ошибка в закрытии ордера.", "\n", e)


async def changePositionForFutures():
    try:
        msg = await async_client.futures_change_position_mode(dualSidePosition="true")
        print("Лонг/Шорт режим на рынке фьючерсов активирован.")
    except Exception as e:
        if "code=-4059" in str(e):
            print("Лонг/Шорт режим уже был установлен.")


async def marginToIsolated(ticker):
    try:
        msg = await async_client.futures_change_margin_type(symbol=ticker, marginType="ISOLATED")
        print("Изменил", ticker, "на режим изолированной маржи.")
    except Exception as e:
        print("На", ticker, "уже установлен режим изолированной маржи.")


async def getInitialDFFutures(size, ticker):
    msg = await async_client.futures_klines(symbol=ticker, interval=getTimeframeDef(config['trading_pairs_futures'][ticker]['timeframe']), limit=size+1)
    df = pd.DataFrame(msg)
    df = df.iloc[:, :5]
    df.columns = ['Time', 'Open', 'Close', 'High', 'Low']
    df.Time = pd.to_datetime(df.Time, unit='ms')
    df.Open = df.Open.astype(float)
    df.Close = df.Close.astype(float)
    df.High = df.High.astype(float)
    df.Low = df.Low.astype(float)
    df.drop(df.tail(1).index, inplace=True)
    return df


async def getInitialDFSpot(size, ticker):
    msg = await async_client.get_klines(symbol=ticker, interval=getTimeframeDef(config['trading_pairs_spot'][ticker]['timeframe']), limit=size+1)
    df = pd.DataFrame(msg)
    df = df.iloc[:, :5]
    df.columns = ['Time', 'Open', 'Close', 'High', 'Low']
    df.Time = pd.to_datetime(df.Time, unit='ms')
    df.Open = df.Open.astype(float)
    df.Close = df.Close.astype(float)
    df.High = df.High.astype(float)
    df.Low = df.Low.astype(float)
    df.drop(df.tail(1).index, inplace=True)
    return df


async def updateDFFutures(df, ticker, maopen_period, maclose_period, maopen_offset, maclose_offset, longs, shorts):
    msg = await async_client.futures_klines(symbol=ticker, interval=getTimeframeDef(config['trading_pairs_futures'][ticker]['timeframe']), limit=1+1)
    temp = pd.DataFrame(msg)
    temp = temp.iloc[:, :5]
    temp.columns = ['Time', 'Open', 'Close', 'High', 'Low']
    temp.Time = pd.to_datetime(temp.Time, unit='ms')
    temp.Open = temp.Open.astype(float)
    temp.Close = temp.Close.astype(float)
    temp.High = temp.High.astype(float)
    temp.Low = temp.Low.astype(float)
    temp.drop(temp.tail(1).index, inplace=True)
    temp['PriceOPEN'] = getPriceFromOCHL(temp.Open, temp.Close, temp.High, temp.Low, ticker, True, True)
    temp['PriceCLOSE'] = getPriceFromOCHL(temp.Open, temp.Close, temp.High, temp.Low, ticker, False, True)
    df = df.append(temp)
    if maopen_offset != 0:
        df.iloc[-1, 7] = df.iloc[-(maopen_period+maopen_offset):-maopen_offset, 5].mean()
    else:
        df.iloc[-1, 7] = df.iloc[-(maopen_period+maopen_offset):, 5].mean()

    if maclose_offset != 0:
        df.iloc[-1, 8] = df.iloc[-(maclose_period+maclose_offset):-maclose_offset, 6].mean()
    else:
        df.iloc[-1, 8] = df.iloc[-(maclose_period+maclose_offset):, 6].mean()

    for long in enumerate(longs):
        df.iloc[-1, 9+long[0]] = df.iloc[-1, 7] * (100-long[1][0])/100
    for short in enumerate(shorts):
        df.iloc[-1, 9+len(longs)+short[0]] = df.iloc[-1, 7] * (100+short[1][0])/100
    df.reset_index(drop=True, inplace=True)
    return df


async def updateDFSpot(df, ticker, maopen_period, maclose_period, maopen_offset, maclose_offset, longs):
    msg = await async_client.get_klines(symbol=ticker, interval=getTimeframeDef(config['trading_pairs_spot'][ticker]['timeframe']), limit=1+1)
    temp = pd.DataFrame(msg)
    temp = temp.iloc[:, :5]
    temp.columns = ['Time', 'Open', 'Close', 'High', 'Low']
    temp.Time = pd.to_datetime(temp.Time, unit='ms')
    temp.Open = temp.Open.astype(float)
    temp.Close = temp.Close.astype(float)
    temp.High = temp.High.astype(float)
    temp.Low = temp.Low.astype(float)
    temp.drop(temp.tail(1).index, inplace=True)
    temp['PriceOPEN'] = getPriceFromOCHL(temp.Open, temp.Close, temp.High, temp.Low, ticker, True, False)
    temp['PriceCLOSE'] = getPriceFromOCHL(temp.Open, temp.Close, temp.High, temp.Low, ticker, False, False)
    df = df.append(temp)
    if maopen_offset != 0:
        df.iloc[-1, 7] = df.iloc[-(maopen_period+maopen_offset):-maopen_offset, 5].mean()
    else:
        df.iloc[-1, 7] = df.iloc[-(maopen_period+maopen_offset):, 5].mean()

    if maclose_offset != 0:
        df.iloc[-1, 8] = df.iloc[-(maclose_period+maclose_offset):-maclose_offset, 6].mean()
    else:
        df.iloc[-1, 8] = df.iloc[-(maclose_period+maclose_offset):, 6].mean()
    for long in enumerate(longs):
        df.iloc[-1, 9+long[0]] = df.iloc[-1, 7] * (100-long[1][0])/100
    df.reset_index(drop=True, inplace=True)
    return df


async def changeLeverage(ticker, leverage):
    try:
        msg = await async_client.futures_change_leverage(symbol=ticker, leverage=leverage)
        print("Изменил плечо", ticker, "на", str(leverage)+".")
    except Exception as e:
        print("Ошибка при попытке изменить плечо", ticker, "\n", e)


async def getActivePositionsFutures(ticker):
    try:
        msg = await async_client.futures_position_information(symbol=ticker)
        return(msg)
    except Exception as e:
        print(e)
        return([{'isolatedMargin': '0'}, {'isolatedMargin': '0'}, {'isolatedMargin': '0'}])


async def getActiveOrdersFutures(ticker):
    try:
        msg = await async_client.futures_get_open_orders(symbol=ticker)
        return msg
    except Exception as e:
        print(e)
        return []


async def getActiveOrdersSpot(ticker):
    try:
        msg = await async_client.get_open_orders(symbol=ticker)
        return msg
    except Exception as e:
        print(e)
        return []


async def closePositionFutures(quantity, ticker, positionSide, side):
    order = {
        'symbol': ticker,
        'side': side,
        'positionSide': positionSide,
        'type': "MARKET",
        'quantity': quantity
    }
    try:
        msg = await async_client.futures_create_order(**order)
    except Exception as e:
        print(e)


def closePositionSpot(quantity, ticker):
    order = {
        'symbol': ticker,
        'side': "SELL",
        'type': "MARKET",
        'quantity': str(quantity)
    }
    try:
        msg = client.create_order(**order)
        return msg
    except Exception as e:
        print(e)


def countMarketOrderROI(order):
    summ = 0
    for fill in order['fills']:
        summ += float(fill['price'])*float(fill['qty']) - float(fill['commission'])
    return summ


async def quitting(tickers):
    config['totalpnl'] = totalpnl
    for pair in config['trading_pairs_futures']:
        for long in enumerate(config['trading_pairs_futures'][pair]['longs']):
            config['trading_pairs_futures'][pair]['longs'][long[0]][2] = 0
        for short in enumerate(config['trading_pairs_futures'][pair]['shorts']):
            config['trading_pairs_futures'][pair]['shorts'][short[0]][2] = 0
    for pair in config['trading_pairs_spot']:
        for long in enumerate(config['trading_pairs_spot'][pair]['longs']):
            config['trading_pairs_spot'][pair]['longs'][long[0]][2] = 0

    with open('config.json', 'w') as f:
        json.dump(config, f, indent=2)
    tasks = []
    if config['futures'] == 1:
        for ticker in tickers[0]:  # futures
            task = asyncio.create_task(quitThreadFutures(ticker))
            tasks.append(task)
    if config['spot'] == 1:
        for ticker in tickers[1]:  # spot
            task = asyncio.create_task(quitThreadSpot(ticker))
            tasks.append(task)
    await asyncio.gather(*tasks)


async def quitThreadFutures(ticker):
    await asyncio.sleep(1)
    await cancelAllOrdersFutures(ticker)


async def quitThreadSpot(ticker):
    active_orders = await getActiveOrdersSpot(ticker)
    order_ids = [x['orderId'] for x in active_orders]
    await cancelAllOrdersSpot(ticker, order_ids)


async def WhiteBoxStratFutures(pricesfororders, closeprice, ticker, ticker_params):
    global pairs_futures, totalpnl
    actualprice = await async_client.futures_symbol_ticker(symbol=ticker)
    actualprice = float(actualprice["price"])
    active_orders = await getActiveOrdersFutures(ticker)
    order_ids = [x['orderId'] for x in active_orders]
    for order in enumerate(pairs_futures[ticker]['longs']):
        if order[1][2] in order_ids:
            pairs_futures[ticker]['longs'][order[0]][2] = 0
    for order in enumerate(pairs_futures[ticker]['shorts']):
        if order[1][2] in order_ids:
            pairs_futures[ticker]['shorts'][order[0]][2] = 0
    await cancelAllOrdersFutures(ticker)
    # check if needed to close active positions
    active_positions = await getActivePositionsFutures(ticker)
    # longs
    if float(active_positions[1]['isolatedMargin']) != 0:
        if actualprice >= closeprice:
            await closePositionFutures(active_positions[1]['positionAmt'], ticker, "LONG", "SELL")
            tradepnl = float(active_positions[1]['unRealizedProfit'])
            totalpnl += tradepnl
            hour_stats[0] += tradepnl
            if tradepnl >= 0:
                PNLmessage = "Futures: Closed " + ticker + " position. Profit: " + str(round(tradepnl, 2)) + " USDT"
                hour_stats[1] += 1
                print(colored(PNLmessage, "green"))
            else:
                PNLmessage = "Futures: Closed " + ticker + " position. Loss: " + str(round(tradepnl, 2)) + " USDT"
                hour_stats[2] += 1
                print(colored(PNLmessage, "red"))
            for x in range(len(pairs_futures[ticker]['longs'])):
                pairs_futures[ticker]['longs'][x][2] = 0
    # shorts
    if float(active_positions[2]['isolatedMargin']) != 0:
        if actualprice <= closeprice:
            await closePositionFutures(active_positions[2]['positionAmt'].replace("-", ""), ticker, "SHORT", "BUY")
            tradepnl = float(active_positions[2]['unRealizedProfit'])
            totalpnl += tradepnl
            hour_stats[0] += tradepnl
            if tradepnl >= 0:
                PNLmessage = "Futures: Closed " + ticker + " position. Profit: " + str(round(tradepnl, 2)) + " USDT"
                hour_stats[1] += 1
                print(colored(PNLmessage, "green"))
            else:
                PNLmessage = "Futures: Closed " + ticker + " position. Loss: " + str(round(tradepnl, 2)) + " USDT"
                hour_stats[2] += 2
                print(colored(PNLmessage, "red"))
            for x in range(len(pairs_futures[ticker]['shorts'])):
                pairs_futures[ticker]['shorts'][x][2] = 0
    long_orders = []
    short_orders = []
    for long in enumerate(pairs_futures[ticker]['longs']):
        if long[1][2] == 0:  # if long not already active
            tempprice = round(pricesfororders['longs'][long[0]], ticker_params['round_price'])
            temp = {
                'symbol': ticker,
                'side': "BUY",
                'type': "LIMIT",
                'quantity': str(round(long[1][1]/tempprice, ticker_params['round_qty'])),
                'positionSide': "LONG",
                'timeInForce': "GTC",
                'price': str(tempprice)
            }
            long_orders.append(temp)
    for short in enumerate(pairs_futures[ticker]['shorts']):
        if short[1][2] == 0:  # if short not already active
            tempprice = round(pricesfororders['shorts'][short[0]], ticker_params['round_price'])
            temp = {
                'symbol': ticker,
                'side': "SELL",
                'type': "LIMIT",
                'quantity': str(round(short[1][1]/tempprice, ticker_params['round_qty'])),
                'positionSide': "SHORT",
                'timeInForce': "GTC",
                'price': str(tempprice)
            }
            short_orders.append(temp)
    if long_orders:
        createdlongs = createBatchLimitOrdersFutures(long_orders)
        for x in enumerate(pairs_futures[ticker]['longs']):
            if createdlongs:
                try:
                    pairs_futures[ticker]['longs'][x[0]][2] = createdlongs[0]['orderId']
                    del createdlongs[0]
                except Exception as e:
                    print(e)
                    print(createdlongs)
    if short_orders:
        createdshorts = createBatchLimitOrdersFutures(short_orders)
        for x in enumerate(pairs_futures[ticker]['shorts']):
            if createdshorts:
                try:
                    pairs_futures[ticker]['shorts'][x[0]][2] = createdshorts[0]['orderId']
                    del createdshorts[0]
                except Exception as e:
                    print(e)
                    print(createdshorts)


async def WhiteBoxStratSpot(pricesfororders, closeprice, ticker, ticker_params):
    global pairs_spot, callback_arr, totalpnl
    active_orders = await getActiveOrdersSpot(ticker)
    actualprice = await async_client.get_symbol_ticker(symbol=ticker)
    actualprice = float(actualprice["price"])
    order_ids = [x['orderId'] for x in active_orders]
    for order in enumerate(pairs_spot[ticker]['longs']):
        if order[1][2] in order_ids:
            pairs_spot[ticker]['longs'][order[0]][2] = 0
    await cancelAllOrdersSpot(ticker, order_ids)
    # check if needed to close active positions
    active_positions = callback_arr['spot'][ticker]
    # longs
    if active_positions:
        if actualprice >= closeprice:
            summ_qty = 0
            total_paid = 0
            for pos in active_positions:
                summ_qty += pos['qty']
                total_paid += pos['qty']*pos['price']
            if actualprice * summ_qty >= config['spot_sell_bound']:
                closepos_result = closePositionSpot(round(summ_qty*0.995, ticker_params['round_qty']), ticker)
                callback_arr['spot'][ticker] = []
                tradepnl = countMarketOrderROI(closepos_result) - total_paid
                totalpnl += tradepnl
                hour_stats[0] += tradepnl
                if tradepnl >= 0:
                    PNLmessage = "Spot: Closed " + ticker + " position. Profit: " + str(round(tradepnl, 2)) + " USDT"
                    hour_stats[1] += 1
                    print(colored(PNLmessage, "green"))
                else:
                    PNLmessage = "Spot: Closed " + ticker + " position. Loss: " + str(round(tradepnl, 2)) + " USDT"
                    hour_stats[2] += 1
                    print(colored(PNLmessage, "red"))
                for x in range(len(pairs_spot[ticker]['longs'])):
                    pairs_spot[ticker]['longs'][x][2] = 0
    long_orders = []
    for long in enumerate(pairs_spot[ticker]['longs']):
        if long[1][2] == 0:  # if long not already active
            tempprice = round(pricesfororders['longs'][long[0]], ticker_params['round_price'])
            temp = {
                'symbol': ticker,
                'side': "BUY",
                'quantity': str(round(long[1][1]/tempprice, ticker_params['round_qty'])),
                'timeInForce': "GTC",
                'newOrderRespType': "ACK",
                'price': str(tempprice)
            }
            long_orders.append(temp)
    if long_orders:
        createdlongs = await createLimitOrderBatchSpot(long_orders)
        for x in enumerate(pairs_spot[ticker]['longs']):
            if x[1][2] == 0:
                if createdlongs:
                    pairs_spot[ticker]['longs'][x[0]][2] = createdlongs[0]['orderId']
                    del createdlongs[0]


async def schedulerBigTFFutures(ticker, ticker_info):
    while True:
        if len(pairs_futures[ticker]['longs']) == 0 and len(pairs_futures[ticker]['shorts']) != 0:
            prices_for_orders = {
                'longs': [],
                'shorts': dataframes['futures'][ticker].values[-1].tolist()[9:]
            }
        elif len(pairs_futures[ticker]['shorts']) == 0 and len(pairs_futures[ticker]['longs']) != 0:
            prices_for_orders = {
                'longs': dataframes['futures'][ticker].values[-1].tolist()[9:],
                'shorts': []
            }
        elif len(pairs_futures[ticker]['shorts']) != 0 and len(pairs_futures[ticker]['longs']) != 0:
            prices_for_orders = {
                'longs': dataframes['futures'][ticker].values[-1].tolist()[9:9+len(pairs_futures[ticker]['longs'])+1],
                'shorts': dataframes['futures'][ticker].values[-1].tolist()[9+len(pairs_futures[ticker]['longs']):]
            }
        else:
            prices_for_orders = {
                'longs': [],
                'shorts': []
            }
        await WhiteBoxStratFutures(prices_for_orders, dataframes['futures'][ticker].iloc[-1, 8], ticker, ticker_info)
        await asyncio.sleep(getTimeframedelay(config['trading_pairs_futures'][ticker]['timeframe']+config['delay_before_refresh']))
        dataframes['futures'][ticker] = await updateDFFutures(dataframes['futures'][ticker], ticker, pairs_futures[ticker]['maopen_period'],
                                                              pairs_futures[ticker]['maclose_period'], pairs_futures[ticker]['maopen_offset'], pairs_futures[ticker]['maclose_offset'], pairs_futures[ticker]['longs'], pairs_futures[ticker]['shorts'])


async def priceCheckerFutures(ticker):
    while True:
        try:
            await asyncio.sleep(delay)
            actualprice = await async_client.futures_symbol_ticker(symbol=ticker)
            actualprice = float(actualprice["price"])
            global pairs_futures, callback_arr, totalpnl
            # check if needed to close active positions
            active_positions = await getActivePositionsFutures(ticker)
            # longs
            if float(active_positions[1]['isolatedMargin']) != 0:
                if actualprice >= dataframes['futures'][ticker].iloc[-1, 8]:
                    await closePositionFutures(active_positions[1]['positionAmt'], ticker, "LONG", "SELL")
                    tradepnl = float(active_positions[1]['unRealizedProfit'])
                    totalpnl += tradepnl
                    hour_stats[0] += tradepnl
                    if tradepnl >= 0:
                        PNLmessage = "Futures: Closed " + ticker + " position. Profit: " + str(round(tradepnl, 2)) + " USDT"
                        hour_stats[1] += 1
                        print(colored(PNLmessage, "green"))
                    else:
                        PNLmessage = "Futures: Closed " + ticker + " position. Loss: " + str(round(tradepnl, 2)) + " USDT"
                        hour_stats[2] += 1
                        print(colored(PNLmessage, "red"))
                    for x in range(len(pairs_futures[ticker]['longs'])):
                        pairs_futures[ticker]['longs'][x][2] = 0
            # shorts
            if float(active_positions[2]['isolatedMargin']) != 0:
                if actualprice <= dataframes['futures'][ticker].iloc[-1, 8]:
                    await closePositionFutures(active_positions[2]['positionAmt'].replace("-", ""), ticker, "SHORT", "BUY")
                    tradepnl = float(active_positions[2]['unRealizedProfit'])
                    totalpnl += tradepnl
                    hour_stats[0] += tradepnl
                    if tradepnl >= 0:
                        PNLmessage = "Futures: Closed " + ticker + " position. Profit: " + str(round(tradepnl, 2)) + " USDT"
                        hour_stats[1] += 1
                        print(colored(PNLmessage, "green"))
                    else:
                        PNLmessage = "Futures: Closed " + ticker + " position. Loss: " + str(round(tradepnl, 2)) + " USDT"
                        hour_stats[2] += 1
                        print(colored(PNLmessage, "red"))
                    for x in range(len(pairs_futures[ticker]['shorts'])):
                        pairs_futures[ticker]['shorts'][x][2] = 0
        except Exception as e:
            continue


async def schedulerMiniTFFutures(ticker):
    while True:
        await priceCheckerFutures(ticker)
        await asyncio.sleep(delay)


async def tickerThreadFutures(ticker):
    await marginToIsolated(ticker)
    await changeLeverage(ticker, leverage)
    # get initial data ~ max(maopen_period, maclose_period)

    maopen_period, maclose_period = pairs_futures[ticker]['maopen_period'], pairs_futures[ticker]['maclose_period']
    maopen_offset, maclose_offset = pairs_futures[ticker]['maopen_offset'], pairs_futures[ticker]['maclose_offset']
    queuesize = max(maopen_period+maopen_offset, maclose_period+maclose_offset)

    df = await getInitialDFFutures(queuesize, ticker)

    df = calcParams(df, maopen_period,
                    maclose_period, maopen_offset, maclose_offset, pairs_futures[ticker]['longs'],
                    pairs_futures[ticker]['shorts'], True, ticker)

    dataframes['futures'][ticker] = df
    ticker_info = await async_client.futures_exchange_info()
    ticker_info = next(item for item in ticker_info['symbols'] if item["symbol"] == ticker)
    round_price = int(-log10(float(ticker_info['filters'][0]['tickSize'])))
    round_qty = int(-log10(float(ticker_info['filters'][1]['stepSize'])))
    # schedulers
    schedulersFutures = []
    schedulersFutures.append(asyncio.create_task(schedulerBigTFFutures(ticker, {"round_price": round_price, "round_qty": round_qty})))
    schedulersFutures.append(asyncio.create_task(schedulerMiniTFFutures(ticker)))
    await asyncio.gather(*schedulersFutures)


async def schedulerBigTFSpot(ticker, ticker_info):
    while True:
        if len(pairs_spot[ticker]['longs']) != 0:
            prices_for_orders = {
                'longs': dataframes['spot'][ticker].values[-1].tolist()[9:]
            }
        else:
            prices_for_orders = {
                'longs': []
            }
        await WhiteBoxStratSpot(prices_for_orders, dataframes['spot'][ticker].iloc[-1, 8], ticker, ticker_info)
        await asyncio.sleep(getTimeframedelay(config['trading_pairs_spot'][ticker]['timeframe'])+config['delay_before_refresh'])
        dataframes['spot'][ticker] = await updateDFSpot(dataframes['spot'][ticker], ticker, pairs_spot[ticker]['maopen_period'],
                                                        pairs_spot[ticker]['maclose_period'], pairs_spot[ticker]['maopen_offset'], pairs_spot[ticker]['maclose_offset'], pairs_spot[ticker]['longs'])


async def priceCheckerSpot(ticker, ticker_params):
    while True:
        try:
            await asyncio.sleep(delay)
            actualprice = await async_client.get_symbol_ticker(symbol=ticker)
            actualprice = float(actualprice["price"])
            if actualprice >= dataframes['spot'][ticker].iloc[-1, 8]:
                global pairs_spot, callback_arr, totalpnl, hour_stats

                # check if needed to close active positions
                active_positions = callback_arr['spot'][ticker]
                # longs
                if active_positions:
                    summ_qty = 0
                    total_paid = 0
                    for pos in active_positions:
                        summ_qty += pos['qty']
                        total_paid += pos['qty']*pos['price']
                    if actualprice * summ_qty >= config['spot_sell_bound']:
                        closepos_result = closePositionSpot(round(summ_qty*0.995, ticker_params['round_qty']), ticker)
                        callback_arr['spot'][ticker] = []
                        tradepnl = countMarketOrderROI(closepos_result) - total_paid
                        totalpnl += tradepnl
                        hour_stats[0] += tradepnl
                        if tradepnl >= 0:
                            PNLmessage = "Closed " + ticker + " position. Profit: " + str(round(tradepnl, 2)) + " USDT"
                            hour_stats[1] += 1
                            print(colored(PNLmessage, "green"))
                        else:
                            PNLmessage = "Closed " + ticker + " position. Loss: " + str(round(tradepnl, 2)) + " USDT"
                            hour_stats[2] += 1
                            print(colored(PNLmessage, "red"))
                        for x in range(len(pairs_spot[ticker]['longs'])):
                            pairs_spot[ticker]['longs'][x][2] = 0
        except Exception as e:
            continue


async def schedulerMiniTFSpot(ticker, ticker_info):
    while True:
        await priceCheckerSpot(ticker, ticker_info)
        await asyncio.sleep(delay)


async def tickerThreadSpot(ticker):
    # get initial data ~ max(maopen_period, maclose_period)
    queuesize = max(pairs_spot[ticker]['maopen_period']+pairs_spot[ticker]['maopen_offset'], pairs_spot[ticker]['maclose_period']+pairs_spot[ticker]['maclose_offset'])
    # pairs_spot[ticker]['maopen_offset'],pairs_spot[ticker]['maclose_offset']
    df = await getInitialDFSpot(queuesize, ticker)
    # here was longs_params = pair_config['longs']
    df = calcParams(df, pairs_spot[ticker]['maopen_period'],
                    pairs_spot[ticker]['maclose_period'], pairs_spot[ticker]['maopen_offset'], pairs_spot[ticker]['maclose_offset'], pairs_spot[ticker]['longs'],
                    [], False, ticker)
    dataframes['spot'][ticker] = df
    ticker_info = await async_client.get_symbol_info(ticker)
    round_price = int(-log10(float(ticker_info['filters'][0]['tickSize'])))
    round_qty = int(-log10(float(ticker_info['filters'][2]['stepSize'])))
    # 2 schedulers
    schedulersSpot = []
    schedulersSpot.append(asyncio.create_task(schedulerBigTFSpot(ticker, {"round_price": round_price, "round_qty": round_qty})))
    schedulersSpot.append(asyncio.create_task(schedulerMiniTFSpot(ticker, {"round_price": round_price, "round_qty": round_qty})))
    await asyncio.gather(*schedulersSpot)


async def UserSocketSpot():
    print("Открыт порт прослушивания выполненных ордеров.")
    us = bm.user_socket()
    async with us as tscm:
        while True:
            res = await tscm.recv()
            # if spot order gets filled
            if 'x' in res and 'X' in res and 'S' in res and 'o' in res:
                if res['x'] == "TRADE" and res['X'] == "FILLED" and res['S'] == "BUY" and res['o'] == "LIMIT":
                    temp_ticker = res['s']
                    if temp_ticker in tickers_spot:
                        global callback_arr
                        callback_arr['spot'][temp_ticker].append({
                            'orderId': res['i'],
                            'qty': float(res['q']),
                            'price': float(res['p'])
                        })


async def statisticsWriter():
    while True:
        await asyncio.sleep(3600)
        print("Записал статистику по трейдам в файл.")
        with open("statistics.txt", 'a', encoding='utf-8') as stats:
            global hour_stats, totalpnl
            stats.write("Часовой отчет.\n")
            towrite = "Результат за час: " + str(round(hour_stats[0], 2)) + ' USD\n' + "Положительных/отрицательных сделок: " + str(hour_stats[1]) + "|"+str(hour_stats[2]) + '\n'
            stats.write(towrite)
            stats.write("---\n")
            towrite = "Результат за все время: " + str(round(totalpnl, 2)) + ' USD\n\n'
            stats.write(towrite)
            hour_stats = [0, 0, 0]


async def main():
    tasks = []
    if config['futures'] == 1:
        await changePositionForFutures()
        # futures bot
        for ticker in tickers_futures:
            task = asyncio.create_task(tickerThreadFutures(ticker))
            tasks.append(task)
    if config['spot'] == 1:
        # listening to change in orders
        tasks.append(asyncio.create_task(UserSocketSpot()))
        # spot bot
        for ticker in tickers_spot:
            task = asyncio.create_task(tickerThreadSpot(ticker))
            tasks.append(task)
    if config['futures'] == 0 and config['spot'] == 0:
        print("Не выбран ни спот, ни фьючерс рынок, остановка бота.")
        exit()
    tasks.append(asyncio.create_task(statisticsWriter()))
    await asyncio.gather(*tasks)

if __name__ == '__main__':
    with open('config.json', 'r') as configfile:
        global config, api_key, secret_key, pairs_futures, pairs_spot, leverage, totalpnl, delay, timeframe_def
        config = json.load(configfile)
        api_key = config['api_key']
        secret_key = config['secret_key']
        pairs_futures = config['trading_pairs_futures']
        pairs_spot = config['trading_pairs_spot']
        leverage = config['leverage']
        totalpnl = config['totalpnl']
        delay = config['delay']

    global async_client, client, bm, tickers_futures, tickers_spot, callback_arr, dataframes, hour_stats
    async_client = AsyncClient(api_key, secret_key, testnet=False)
    client = Client(api_key, secret_key, testnet=False)
    bm = BinanceSocketManager(async_client)
    hour_stats = [0, 0, 0]
    tickers_futures = list(pairs_futures.keys())
    tickers_spot = list(pairs_spot.keys())
    dataframes = {
        'futures': {},
        'spot': {}
    }
    for ticker in tickers_futures:
        dataframes['futures'][ticker] = pd.DataFrame()
        if pairs_futures[ticker]['maopen_source'] not in pricing_arr or pairs_futures[ticker]['maclose_source'] not in pricing_arr:
            print("Введены неправильные генераторы цены по одной из пар.")
            exit()
    for ticker in tickers_spot:
        if pairs_spot[ticker]['maopen_source'] not in pricing_arr or pairs_spot[ticker]['maclose_source'] not in pricing_arr:
            print("Введены неправильные генераторы цены по одной из пар.")
            exit()
        dataframes['spot'][ticker] = pd.DataFrame()

    callback_arr = {
        'futures': {},
        'spot': {}
    }
    for ticker in tickers_futures:
        callback_arr['futures'][ticker] = []
    for ticker in tickers_spot:
        callback_arr['spot'][ticker] = []
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        asyncio.run(quitting([tickers_futures, tickers_spot]))
        print("Остановка бота, закрытие всех открытых ордеров.")
