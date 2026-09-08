from pybit.unified_trading import HTTP
from api import DEMO_CONFIG
from decimal import Decimal
import os
import threading
import math
import time

d = DEMO_CONFIG

CONFIGS=[d]
thread_local = threading.local()

def set_proxy_for_thread(proxy_url):
    """Устанавливает прокси для текущего потока"""
    thread_local.proxy_url = proxy_url
    os.environ['HTTP_PROXY'] = proxy_url
    os.environ['HTTPS_PROXY'] = proxy_url

def clear_proxy_for_thread():
    """Очищает прокси для текущего потока"""
    if hasattr(thread_local, 'proxy_url'):
        del thread_local.proxy_url
    os.environ.pop('HTTPS_PROXY', None)

def create_session_with_proxy(api_config):
    """
    Создает сессию Bybit с поддержкой прокси для текущего потока
    """
    session_kwargs = {
        'testnet': api_config.get('testnet', False),
        'demo': api_config.get('demo', True),
        'api_key': api_config['api_key'],
        'api_secret': api_config['api_secret'],
        'timeout': api_config.get('timeout', 20)
    }

    if api_config.get('use_proxy', False) and 'proxy' in api_config:
        proxy_config = api_config['proxy']
        proxy_url = proxy_config.get('https') or proxy_config.get('http')

        # Устанавливаем прокси для текущего потока
        set_proxy_for_thread(proxy_url)

        try:
            sess = HTTP(**session_kwargs)
            print(f"[{api_config.get('name', 'Unknown')}] Сессия с прокси: {proxy_url}")
            return sess
        finally:
            # Очищаем прокси для этого потока
            clear_proxy_for_thread()
    else:
        print(f"[{api_config.get('name', 'Unknown')}] Сессия без прокси")
        return HTTP(**session_kwargs)

def get_max_leverage_for_symbol_old(sess, symbol):
    """
    Получает максимально возможное плечо для символа.
    Возвращает целое число (например, 50, 75, 100).
    """
    try:
        resp = sess.get_risk_limit(category="linear", symbol=symbol)
        if resp['retCode'] != 0:
            print(f"Ошибка получения risk_limit для {symbol}: {resp}")
            return 1  # По умолчанию, если ошибка

        items = resp['result']['list']
        if not items:
            return 1

        max_leverage = max(float(item['maxLeverage']) for item in items)
        return int(max_leverage)
    except Exception as e:
        print(f"Ошибка в get_max_leverage_for_symbol для {symbol}: {e}")
        return 1

def set_leverage_for_symbol(sess, symbol, leverage):
    """
    Устанавливает плечо для символа. [web:18][web:21]
    """
    try:
        resp = sess.set_leverage(
            category="linear",
            symbol=symbol,
            buyLeverage=str(leverage),
            sellLeverage=str(leverage)
        )
        if resp['retCode'] == 0:
            return True
        else:
            return False
    except Exception as e:
        # print(f"Ошибка в set_leverage_for_symbol для {symbol}: {e}")
        return False

def close_one_sym(sess, row):
    symbol = row['symbol']
    side = row['side']
    qty = float(row['size'])
    pnl_close = float(row['unrealisedPnl'])
    close_side = 'Sell' if side == 'Buy' else 'Buy'
    idx = 2 if side == "Sell" else 1

    try:

        order = sess.place_order(
            category="linear",
            symbol=symbol,
            side=close_side,
            orderType="Market",
            qty=str(qty),
            positionIdx=idx,
            reduceOnly=True
        )
        if order['retCode'] == 0:
            print(f"Закрытие ордера по {symbol}: {order}")
        else:
            print(f"Ошибка закрытия ордера по {symbol}: {order}")
    except Exception as e:
        print(f"Ошибка при закрытии funding trade по {symbol}: {e}")
    return

def get_symbol_ticker(sess, symbol):
    """
    Обёртка над get_tickers, чтобы не дублировать код.
    fundingRate и nextFundingTime берём из тикера. [web:11]
    """
    resp = sess.get_tickers(category="linear", symbol=symbol)
    if resp['retCode'] != 0:
        raise RuntimeError(f"get_tickers error for {symbol}: {resp}")
    if not resp['result']['list']:
        return None
    return resp['result']['list'][0]


def calculate_qty(symbol: str, usdt: float, price: float, level: int, sess: HTTP = None):
    """
    Рассчитывает количество контрактов (qty) для открытия позиции.

    Аргументы:
        symbol (str): Торговая пара (например, 'BTCUSDT')
        usdt (float): Желаемый номинал позиции в USDT (уже с учётом плеча)
        price (float): Текущая цена монеты
        level (int): Плечо (не используется, но оставлено для обратной совместимости)
        sess (HTTP, optional): Сессия Bybit; если не указана, создаётся новая

    Возвращает:
        tuple: (qty_str, qty_float)
            - qty_str: строка, отформатированная в соответствии с qtyStep (для API)
            - qty_float: числовое значение qty
    """
    if sess is None:
        sess = HTTP(testnet=False, demo=False)

    # Получаем фильтры по инструменту
    try:
        resp = sess.get_instruments_info(category="linear", symbol=symbol)
        if resp['retCode'] != 0:
            return False
            # raise RuntimeError(f"Ошибка получения инструментов: {resp['retMsg']}")
        lot = resp['result']['list'][0]['lotSizeFilter']
        min_qty = float(lot['minOrderQty'])
        qty_step = float(lot['qtyStep'])
    except Exception as e:
        return False
        # raise RuntimeError(f"Не удалось получить фильтры для {symbol}: {e}")

    # Сырое количество
    raw_qty = (usdt / price)
    # raw_qty = (usdt / price)

    # Округление вниз до кратного шагу
    qty = math.floor(raw_qty / qty_step) * qty_step

    # Не меньше минимального допустимого
    if qty < min_qty:
        qty = min_qty
        qty = math.floor(qty / qty_step) * qty_step
        if qty < min_qty:
            qty += qty_step

    # Обеспечиваем минимальную стоимость ордера (5 USDT)
    max_iterations = 100  # Защита от бесконечного цикла
    iterations = 0
    while price * qty < 5.0 and iterations < max_iterations:
        qty += qty_step
        iterations += 1

    if iterations >= max_iterations:
        # raise RuntimeError(f"Не удалось достичь минимальной стоимости для {symbol}")
        return False

    # Форматируем строку с точностью, определяемой qtyStep
    precision = abs(Decimal(str(qty_step)).as_tuple().exponent)
    qty_str = f"{qty:.{precision}f}"

    return qty_str, qty, qty_step

def get_all_positions(sess):
    all_posit = []
    cursor = None
    limit = 100  # Или любое другое допустимое значение
    while True:
        try:
            params = {
                "category": "linear",
                "settleCoin": 'USDT',
                "limit": limit,
            }
            if cursor:
                params["cursor"] = cursor
            response = sess.get_positions(
                **params  # Передаём параметры как keyword arguments
            )
            if 'result' not in response or 'list' not in response['result']:
                print(f"Unexpected response structure: {response}")
                break
            positions = response['result']['list']
            if not positions:
                break  # Больше нет данных
            all_posit.extend(positions)
            # Проверяем, есть ли следующий курсор
            if 'nextPageCursor' in response['result']:  # Имя ключа нужно уточнить по документации!
                cursor = response['result']['nextPageCursor']
                if not cursor:
                    break
            else:
                break  # Нет следующей страницы
            time.sleep(0.1)
        except Exception as e:
            print(f"Error getting positions: {e}")
            break

    return all_posit

def close_all(sess, all_positions):
    closed_count = 0
    if len(all_positions) > 0:
        for row in all_positions:
            symbol = row['symbol']
            side = row['side']
            qty = float(row['size'])
            close_side = 'Sell' if side == 'Buy' else 'Buy'
            idx = 2 if side == "Sell" else 1

            try:
                order = sess.place_order(
                    category="linear",
                    symbol=symbol,
                    side=close_side,
                    orderType="Market",
                    qty=str(qty),
                    positionIdx=idx,
                    reduceOnly=True
                )
                if order['retCode'] == 0:

                    print(
                        f"[CLOSE] {symbol} {close_side} qty={qty:.8f} "
                    )

                    closed_count += 1
                    time.sleep(0.1)
                else:
                    print(f"Ошибка закрытия ордера по {symbol}: {order}")
            except Exception as e:
                print(f"Ошибка при закрытии funding trade по {symbol}: {e}")

    return closed_count


symbol = 'BTCUSDT'
sess = create_session_with_proxy(DEMO_CONFIG)
leverage = get_max_leverage_for_symbol_old(sess, symbol)
set_leverage_for_symbol(sess, symbol, leverage)

ticker = get_symbol_ticker(sess, symbol)
if ticker is None:
    print(f"Пропускаем {symbol}: нет тикера при открытии")


last_price = float(ticker['lastPrice'])
if last_price <= 0:
    print(f"Пропускаем {symbol}: некорректная цена {last_price}")

side = ['Sell','Buy']
qty_str, qty, qty_step = calculate_qty(symbol, 1000, last_price, leverage, sess)
idx_sell = 2
idx_buy = 1
try:
    order = sess.place_order(
        category="linear",
        symbol=symbol,
        side=side[0],
        orderType="Market",
        qty=qty_str,
        positionIdx=idx_sell
    )
except:
    order = False

if order['retCode'] == 0:
    print('OK')
else:
    print('BAD')

try:
    order = sess.place_order(
        category="linear",
        symbol=symbol,
        side=side[1],
        orderType="Market",
        qty=qty_str,
        positionIdx=idx_buy
    )
except:
    order = False

if order['retCode'] == 0:
    print('OK')
else:
    print('BAD')

time.sleep(30)
all_pos = get_all_positions(sess)

close_all(sess, all_pos)
