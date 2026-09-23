
import sqlite3, os
import time
import threading
import math
from datetime import datetime, timezone, timedelta
from decimal import Decimal, ROUND_DOWN, getcontext
from pybit.unified_trading import HTTP
from api_config import DEMO_CONFIG_SPEED
import requests
import json
from scipy import stats
import math
from typing import Dict
import matplotlib

DB_FILE = 'funding_earning.db'

RISK_SYMBOL_USD = 10

# Хранилище прокси для разных потоков
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

def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS open_sym
                             (id INTEGER PRIMARY KEY AUTOINCREMENT,
                                date_ms INTEGER NOT NULL DEFAULT 0,
                                sym TEXT,
                                side TEXT,
                                risk REAL
                            )''')
    conn.commit()

    c.execute('''CREATE TABLE IF NOT EXISTS bank
                                    (id INTEGER PRIMARY KEY AUTOINCREMENT,
                                    bank_all REAL NOT NULL DEFAULT 0.0,
                                    bank_norm REAL NOT NULL DEFAULT 0.0,
                                    bank_rev REAL NOT NULL DEFAULT 0.0
                                    )''')
    conn.commit()
    c.execute("SELECT COUNT(*) FROM bank")
    if c.fetchone()[0] == 0:
        c.execute("INSERT INTO bank DEFAULT VALUES")
    conn.commit()

    conn.close()

def enable_wal_mode():
    try:
        conn = sqlite3.connect(DB_FILE, timeout=10)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA cache_size=-20000")  # 20MB кэша
        conn.close()
        print("WAL-режим включён")
    except Exception as e:
        print(f"Ошибка включения WAL: {e}")

def get_balance(sess):
    resp = sess.get_wallet_balance(accountType="UNIFIED", coin="USDT")
    return float(resp['result']['list'][0]['totalMarginBalance'])

def get_symbol_ticker(sess, symbol):
    resp = sess.get_tickers(category="linear", symbol=symbol)
    if resp['retCode'] != 0:
        raise RuntimeError(f"get_tickers error for {symbol}: {resp}")
    if not resp['result']['list']:
        return None
    return resp['result']['list'][0]  # fundingRate, nextFundingTime, lastPrice и т.д. [web:11]

def get_max_leverage_for_symbol(sess, symbol):
    try:
        resp = sess.get_instruments_info(category="linear", symbol=symbol)
        if resp['retCode'] != 0:
            return 1

        items = resp['result']['list']
        if not items:
            return 1

        max_leverage = None
        for item in items:
            if 'leverageFilter' in item:
                max_lev = item['leverageFilter'].get('maxLeverage')
            else:
                max_lev = item.get('maxLeverage')

            if max_lev is not None:
                max_leverage = int(float(max_lev))
                break

        return max_leverage if max_leverage is not None else 1

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

def calculate_qty(symbol: str, usdt: float, price: float, level: int, sess: HTTP = None, akkaunt=99):
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
    global LEVEL_POS
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
    if akkaunt == 4:
        raw_qty = (usdt / price)
    else:
        raw_qty = (usdt / price) * LEVEL_POS
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

def close_normal(sess_rev, symbol, side):
    idx = 1 if side == "Sell" else 2
    side_for_db = 'Sell' if side == "Buy" else "Buy"
    try:
        position = sess_rev.get_positions(
            category="linear",
            symbol=symbol
        )
        for pos in position["result"]["list"]:
            if side_for_db == pos['side']:
                position_size = float(pos["size"])
                if position_size > 0:
                    pnl = float(pos['unrealisedPnl'])
                    stat_pnl = 'TP' if pnl > 0 else 'SL'
                    order = sess_rev.place_order(
                        category="linear",
                        symbol=symbol,
                        side=side,
                        orderType="Market",
                        qty=pos["size"],
                        positionIdx=idx,
                        reduceOnly=True
                )
                    if order['retCode'] == 0:
                        print(f"[CLOSE] NORMAL {symbol} {side_for_db}")
                        last_risk = read_risk_norm_last(symbol, side_for_db)
                        if float(pos['unrealisedPnl']) < 0:
                            if last_risk:
                                bank_info(size=last_risk * 2, bank='bank_all')
                            else:
                                bank_info(size=20, bank='bank_all')
                        # else:
                        #     if last_risk:
                        #         bank_info(size=-last_risk, bank='bank_all')
                        #     else:
                        #         bank_info(size=-10, bank='bank_all')

                        write_pos_norm(symbol, pos['side'], None, pnl, stat_pnl, None)
                        return order
                    else:
                        return False
    except Exception as e:
        pass
        print(f"Ошибка при закрытии close_normal по {symbol}: {e}")
        return False

def bank_info(size=None, bank=None):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute(f"SELECT {bank} FROM bank WHERE id = 1")
    value = c.fetchone()[0]
    #print(value)
    conn.close()
    if not size:
        plus_bank = math.ceil(value/5) if math.ceil(value/5) > 0 else 1
        f'Банк: {round(value,1)} RISK на открытие +: {plus_bank}'
        return plus_bank
    if size is not None and bank:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute(f"SELECT {bank} FROM bank WHERE id = 1")
        value = c.fetchone()[0]
        if value + size > 10:
            new_value = value + size
        else:
            new_value = 10

        if size > 0:
            print(f'Плюсуем к банку: {size} теперь там: {new_value}')
        else:
            print(f'Минусуем из банка: {size} RISK на открытие: {new_value}')
        #print(2, new_value)
        c.execute(f"UPDATE bank SET {bank} = ? WHERE id = 1",(new_value,))
        conn.commit()
        conn.close()

def open_normal(sess_rev,symbol, side_rev,qty_str, last_price, max_leverage, risk):
    global risk_for_new_open, risk_for_new_open_all
    idx = 1 if side_rev == "Buy" else 2
    last_all = read_stat_norm_last(symbol, side_rev)
    if last_all:
        last_stat, risk_last = last_all
        print(f'ПОСЛЕДНИЙ СТАТУС НОРМАЛ ПОЗИЦИИ: {last_stat} РИСК: {risk_last}')
    set_leverage_for_symbol(sess_rev, symbol, max_leverage)
    if last_all and last_stat == 'SL':
        risk += risk_for_new_open #bank_info(bank='bank_all')
        risk_for_new_open_all -= risk_for_new_open
        if risk_for_new_open_all < risk_for_new_open:
            risk_for_new_open_all = 0
    qty_str_new, qty, qty_step = calculate_qty(symbol, risk, last_price, max_leverage, sess_rev, akkaunt=4)
    qty_str = qty_str_new
    if last_all and last_stat == 'SL':
        print(f'НОВЫЙ РАЗМЕР ПОЗИЦИИ НОРМАЛ после SL:{qty_str_new} РИСК:{risk}')


    # if last_all and last_stat == 'SL':
    #     try:
    #         risk = risk_last * 2
    #         qty_str_new, qty, qty_step = calculate_qty(symbol, risk, last_price, max_leverage, sess_rev, akkaunt=4)
    #         print(f'НОВЫЙ РАЗМЕР ПОЗИЦИИ X2:{qty_str_new}')
    #         qty_str = qty_str_new
    #     except:
    #         pass
    # else:
    #     qty_str_new, qty, qty_step = calculate_qty(symbol, risk, last_price, max_leverage, sess_rev, akkaunt=4)
    #     qty_str = qty_str_new
    try:
        order = sess_rev.place_order(
            category="linear",
            symbol=symbol,
            side=side_rev,
            orderType="Market",
            qty=qty_str,
            positionIdx=idx
        )
        if order and order['retCode'] == 0:
            print(f"[OPEN] NORMAL {symbol} {side_rev}")
            print(f'НОВЫЙ РАЗМЕР РИСКА NORMAL: {int(risk)}')
            if last_all and last_stat == 'SL':
                bank_info(size=-(risk_for_new_open * 0.8), bank='bank_all')
            write_pos_norm(symbol, side_rev, float(qty_str), 0.0, None, risk)
            return order
        else:
            return False
    except Exception as e:
        pass
        #print(f"Ошибка при открытии open_normal по {symbol}: {e}")
        return False

def close_revers(sess_rev, symbol, side):
    idx = 2 if side == "Buy" else 1
    side_for_db = 'Sell' if side == "Buy" else "Buy"
    print(f"[CLOSE] REVERSE START {symbol} {side_for_db}")
    # try:
    position = sess_rev.get_positions(
        category="linear",
        symbol=symbol
    )
    size_for_close = False
    # print(position)
    for pos in position["result"]["list"]:
        if float(pos["size"]) > 0 and pos['side'] == side_for_db:
            pnl_sym = float(pos['unrealisedPnl'])
            stat_pnl = 'TP' if pnl_sym > 0 else 'SL'
            order = sess_rev.place_order(
                category="linear",
                symbol=symbol,
                side=side,
                orderType="Market",
                qty=pos["size"],
                positionIdx=idx,
                reduceOnly=True
            )
            print(order)
            if order and order['retCode'] == 0:
                print(f"[CLOSE] REVERSE {symbol} {side_for_db}")
                write_pos_rev(symbol, pos['side'], None, pnl_sym, stat_pnl, None)
            break

def open_revers(sess_rev, symbol, side_rev, qty_str, last_price, max_leverage, risk):
    print(f"[OPEN] REVERSE START {symbol} {side_rev}")
    idx = 1 if side_rev == "Buy" else 2
    last_all = read_stat_rev_last(symbol, side_rev)
    # print(last_all)
    last_stat = ''
    risk_stat = risk
    if last_all is not None:
        last_stat, risk_stat = last_all
        print(f'ПОСЛЕДНИЙ СТАТУС РЕВЕРС ПОЗИЦИИ: {last_stat} РИСК: {risk_stat}')
    set_leverage_for_symbol(sess_rev, symbol, max_leverage)
    if last_all is not None and last_stat == 'SL':
        try:
            risk = risk_stat * 2.5
            qty_str_new, qty, qty_step = calculate_qty(symbol, risk, last_price, max_leverage, sess_rev, akkaunt=4)
            print(f'НОВЫЙ РАЗМЕР ПОЗИЦИИ X2: {risk} {qty_str_new}')
            qty_str = qty_str_new
        except:
            pass
    else:
        qty_str_new, qty, qty_step = calculate_qty(symbol, risk, last_price, max_leverage, sess_rev, akkaunt=4)
        print(f'НОВЫЙ РАЗМЕР ПОЗИЦИИ: {risk} {qty_str_new}')
        qty_str = qty_str_new
    try:
        order = sess_rev.place_order(
            category="linear",
            symbol=symbol,
            side=side_rev,
            orderType="Market",
            qty=qty_str,
            positionIdx=idx
        )
        if order and order['retCode'] == 0:
            print(f"[OPEN] REVERSE {symbol} {side_rev}")
            write_pos_rev(symbol, side_rev, float(qty_str), 0.0, None, risk)
    except Exception as e:
        pass
        print(f"Ошибка при открытии open_revers по {symbol}: {e}")

def close_one_sym(sess, row, akk):
    global sess_rev, gsess1, gsess2, gsess3, gsess4, all_pos_akk1, all_pos_akk2, all_pos_akk3, all_pos_akk4, sess1, sess2, sess3, sess4
    all_akk = [[all_pos_akk1, gsess1, 1], [all_pos_akk2, gsess2, 2], [all_pos_akk3, gsess3, 3],
               [all_pos_akk4, gsess4, 4]]
    symbol = row['symbol']
    side = row['side']
    qty = row['size']
    pnl_close = float(row['unrealisedPnl'])
    close_side = 'Sell' if side == 'Buy' else 'Buy'
    idx = 2 if side == "Sell" else 1
    try:
        order = sess.place_order(
            category="linear",
            symbol=symbol,
            side=close_side,
            orderType="Market",
            qty=qty,
            positionIdx=idx,
            reduceOnly=True
        )
        if order['retCode'] == 0:
            time.sleep(1)
            if akk == 4:
                # threading.Thread(target=close_revers, args=(sess_rev, symbol, side), daemon=True).start()
                close_revers(sess_rev, symbol, side)
                print(f"[CLOSE] AKK-{akk} {symbol} {side} {'TP' if pnl_close > 0 else 'SL'} pnl={round(pnl_close, 6)} ")
                clear_tp_sl(symbol, side)
            conn = sqlite3.connect(DB_FILE)
            c = conn.cursor()
            c.execute(
                "DELETE FROM funding_trades WHERE symbol=? AND side=?",
                (symbol, side)
            )
            conn.commit()
            conn.close()

            if pnl_close < 0:
                # print(f'{symbol} pnl_close < 0')
                # live_ball_for_reopen = float(read_key_new('key_process', 'reopen_ball'))
                # send_tg(f'AKK-{"Main" if akk == 0 else akk} SL {symbol} {round(pnl_close,2)}$')
                # write_key_new('key_process', 'reopen_ball', live_ball_for_reopen-abs(pnl_close))
                write_pos(symbol, side, None, None, akk, False, pnl_close, 'SL')
                write_close(symbol, side, pnl_close, 'SL', akk)
            if pnl_close > 0:  # and akk == 0:
                # print(f'{symbol} pnl_close > 0')
                # send_tg(f'AKK-{"Main" if akk == 0 else akk} TP {symbol} +{round(pnl_close,2)}$')
                write_pos(symbol, side, None, None, akk, False, pnl_close, 'TP')
                write_close(symbol, side, pnl_close, 'TP', akk)

            time.sleep(0.1)
        else:
            print(f"Ошибка закрытия ордера по {symbol}: {order}")
    except Exception as e:
        print(f"Ошибка при закрытии close_one_sym по {symbol}: {e}")
    return

def pin_message(api, chat_id, message_id, disable_notification=True):
    try:
        # Формируем payload с параметрами для pinChatMessage
        payload = {
            'chat_id': chat_id,
            'message_id': int(message_id),
            'disable_notification': disable_notification  # Подавляет уведомление о закрепе
        }

        response = requests.post(
            f'https://api.telegram.org/bot{api}/pinChatMessage',
            json=payload
        )

        if response.status_code == 200:
            response_data = response.json()
            if response_data.get('ok'):
                print(f"Сообщение {message_id} успешно закреплено!")
                return True

        print(f"Ошибка при закреплении: {response.text}")
        return False

    except Exception as e:
        print(f"Ошибка в pin_message: {e}")
        return False

def send_or_update_photo():
    """
    Отправляет или обновляет фото в Telegram-канале.
    Использует глобальные переменные api_t (токен бота) и chat (ID чата/канала).
    При первом вызове отправляет новое фото и сохраняет message_id в файл msg_id.txt.
    При последующих вызовах заменяет изображение в том же сообщении через editMessageMedia.
    В случае ошибки обновления (например, сообщение удалено) удаляет сохранённый ID
    и создаёт новое сообщение.
    """
    global api_t, chat

    photo_path = 'live_graf.png'
    storage_file = 'msg_id.txt'

    # Проверяем, существует ли файл с сохранённым ID
    msg_id = False
    try:
        if os.path.exists(storage_file):
            with open(storage_file, 'r') as f:
                msg_id = f.read().strip()
    except:
        pass

    if msg_id:
        # Пытаемся обновить существующее сообщение
        url = f'https://api.telegram.org/bot{api_t}/editMessageMedia'
        media = {
            'type': 'photo',
            'media': 'attach://photo'  # файл будет передан в поле 'photo'
        }
        try:
            with open(photo_path, 'rb') as photo_file:
                files = {
                    'photo': (photo_path, photo_file, 'image/png')
                }
                data = {
                    'chat_id': chat,
                    'message_id': msg_id,
                    'media': json.dumps(media)
                }
                response = requests.post(url, data=data, files=files)
                response.raise_for_status()
                # print("Фото успешно обновлено.")
                return
        except Exception as e:
            pass
            return
            # print(f"Ошибка при обновлении фото: {e}")
            # Если обновление не удалось, удаляем сохранённый ID
            # и переходим к отправке нового сообщения
            # os.remove(storage_file)

    if not msg_id:
        # Отправка нового фото (если ID нет или обновление провалилось)
        url = f'https://api.telegram.org/bot{api_t}/sendPhoto'
        try:
            with open(photo_path, 'rb') as photo_file:
                files = {
                    'photo': (photo_path, photo_file, 'image/png')
                }
                data = {
                    'chat_id': chat
                }
                response = requests.post(url, data=data, files=files)
                response.raise_for_status()
                result = response.json()
                if result.get('ok'):
                    message_id = result['result']['message_id']
                    with open(storage_file, 'w') as f:
                        f.write(str(message_id))
                    print("Новое фото отправлено, ID сохранён.")
                else:
                    print("Ошибка отправки: ответ Telegram не содержит 'ok'.")
        except Exception as e:
            pass
            # print(f"Ошибка при отправке фото: {e}")

def send_tg(txt):
    global api_t, chat
    try:
        requests.get(
            f'https://api.telegram.org/bot{api_t}/sendMessage',
            params={
                'chat_id': f'{chat}',
                'text': txt}
        )
    except Exception as e:
        print(f"Ошибка в send_tg: {e}")

def send_tg_update(txt):
    global api_t, chat, id_update_mes
    try:
        if 'id_update_mes' not in globals() or id_update_mes is None:
            return
            # Пропускаем если ID сообщения еще не получен

        # buttons = [
        #     [{"text": "📊 Стоп", "callback_data": "button_1"}],
        #     [{"text": "💰 Старт", "callback_data": "button_2"}],
        #     [{"text": "🔴 Закрыть все позиции", "callback_data": "button_3"}]
        # ]
        params = {'chat_id': f'{chat}',
                  'message_id': id_update_mes,
                  'text': txt,
                  'parse_mode': 'HTML'}

        # reply_markup = {"inline_keyboard": buttons}
        # params['reply_markup'] = json.dumps(reply_markup)

        requests.get(
            f'https://api.telegram.org/bot{api_t}/editMessageText',
            params=params,
            timeout=10
        )
    except Exception as e:
        pass
        # print(f"Ошибка в send_tg_update: {e}")

    try:
        requests.get(
            f'https://api.telegram.org/bot{api_t}/editMessageText',
            params={'chat_id': '-1003280628832',
                    'message_thread_id': 41694,
                    'message_id': 176002,
                    'text': f'✅ Funding monitor REAL (live)\n{txt}'}
        )
    except Exception as e:
        pass
        # print(f"Ошибка в send_tg_update: {e}")

def get_all_positions(sess, key_akk):
    global summ_pnl, setup0, setup1, setup2, setup3, setup4, live_open_coins, live_open_minus_pnl, live_b, live_b_akk1, live_b_akk2, live_b_akk3, live_b_akk4
    live_b_all = [live_b, live_b_akk1, live_b_akk2, live_b_akk3, live_b_akk4]
    all_posit = []
    if key_akk >= 0:
        live_open_coins[str(key_akk)] = []
        # print(f'get_all_positions akk-{key_akk}')
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
    summ_pnl = 0
    if len(all_posit) > 0:
        if key_akk == 5:
            return all_posit
        # one_pos = 1
        remaining_positions = []  # новый список для позиций, которые останутся открытыми
        for row in all_posit:
            # print(f'{row['symbol']},{row['side']}\n')
            if key_akk == 0:
                if float(row['unrealisedPnl']) < 0 and abs(float(row['unrealisedPnl'])) > 200 * setup0[1]:
                    close_one_sym(sess, row, key_akk)
                # print(f'get_all_positions AKK {key_akk}, sym: {row['symbol']}, unrealisedPnl = {row['unrealisedPnl']}, ждем live_b_all[key_akk] * 0.005 {live_b_all[key_akk] * 0.005}')
                # elif float(row['unrealisedPnl']) < 0:
                #     live_open_minus_pnl.append(row['symbol'])
                #     live_open_co.append(row['symbol'])
                #     #remaining_positions.append(row)
                elif float(row['unrealisedPnl']) > 200 * setup0[2]:
                    close_one_sym(sess, row, key_akk)
                else:
                    live_open_coins[str(key_akk)].append(row['symbol'])
                    remaining_positions.append(row)

            if key_akk == 1:
                if float(row['unrealisedPnl']) < 0 and abs(float(row['unrealisedPnl'])) > 200 * setup1[1]:
                    close_one_sym(sess, row, key_akk)
                # print(f'get_all_positions AKK {key_akk}, sym: {row['symbol']}, unrealisedPnl = {row['unrealisedPnl']}, ждем live_b_all[key_akk] * 0.005 {live_b_all[key_akk] * 0.005}')
                elif float(row['unrealisedPnl']) > 200 * setup1[2]:
                    close_one_sym(sess, row, key_akk)
                else:
                    live_open_coins[str(key_akk)].append(row['symbol'])
                    remaining_positions.append(row)

            if key_akk == 2:
                if float(row['unrealisedPnl']) < 0 and abs(float(row['unrealisedPnl'])) > 200 * setup2[1]:
                    close_one_sym(sess, row, key_akk)
                # print(f'get_all_positions AKK {key_akk}, sym: {row['symbol']}, unrealisedPnl = {row['unrealisedPnl']}, ждем live_b_all[key_akk] * 0.005 {live_b_all[key_akk] * 0.005}')
                elif float(row['unrealisedPnl']) > 200 * setup2[2]:
                    close_one_sym(sess, row, key_akk)
                else:
                    live_open_coins[str(key_akk)].append(row['symbol'])
                    remaining_positions.append(row)

            if key_akk == 3:
                if float(row['unrealisedPnl']) < 0 and abs(float(row['unrealisedPnl'])) > 200 * setup3[1]:
                    close_one_sym(sess, row, key_akk)
                # print(f'get_all_positions AKK {key_akk}, sym: {row['symbol']}, unrealisedPnl = {row['unrealisedPnl']}, ждем live_b_all[key_akk] * 0.005 {live_b_all[key_akk] * 0.005}')
                elif float(row['unrealisedPnl']) > 200 * setup3[2]:
                    close_one_sym(sess, row, key_akk)
                else:
                    live_open_coins[str(key_akk)].append(row['symbol'])
                    remaining_positions.append(row)

            if key_akk == 4:
                summ_pnl += float(row['unrealisedPnl'])
                tp_sl = read_tp_sl(row['symbol'], row['side'])
                if tp_sl:

                    # sl_live = 200 * (tp_sl[1] * 5)
                    # tp_live = 200 * (tp_sl[0] * 5)

                    sl_live = 200 * (tp_sl[1] * 1)
                    tp_live = 200 * (tp_sl[0] * 1)
                    # print(f'{row['symbol']} PNL: {float(row['unrealisedPnl'])} | ожидаемые TP: {tp_live} и SL: {sl_live}')
                    # print(f'AKK 4 SYM: {row['symbol']}  PNL {round(float(row['unrealisedPnl']),2)} | SLpnl - {round(sl_live,2)} TPpnl - {round(tp_live,2)}')
                    if float(row['unrealisedPnl']) < 0 and abs(float(row['unrealisedPnl'])) > sl_live:
                        close_one_sym(sess, row, key_akk)
                    elif float(row['unrealisedPnl']) > tp_live:
                        close_one_sym(sess, row, key_akk)
                    else:
                        live_open_coins[str(key_akk)].append(row['symbol'])
                        remaining_positions.append(row)
        # if key_akk >= 0:
        #     print(f'get_all_positions akk-{key_akk} live_open_coins[{str(key_akk)}]:{live_open_coins[str(key_akk)]}')
        # print('\n--------------------------------------------------------------------')
        if len(remaining_positions) > 0:
            return remaining_positions
        else:
            return all_posit
    else:
        return all_posit

def get_comsa(sess, fee_rate=0.002):
    positions = get_all_positions(sess, -1)
    total_fee = 0.0
    for pos in positions:
        try:
            size = float(pos.get("size", 0) or 0)
            if size == 0:
                continue
            position_value = abs(float(pos.get("positionValue", 0) or 0))
            total_fee += position_value * fee_rate
        except (ValueError, TypeError):
            continue
    return float(total_fee)

def read_stat():
    tp_pnl = 0
    sl_pnl = 0
    tp_len = 0
    sl_len = 0
    try:

        conn = sqlite3.connect('db.db')
        c = conn.cursor()
        c.execute(
            "SELECT sym FROM open_pos WHERE AND status = ?",
            ('TP')
        )
        tp_len = len(c.fetchall())
        c.execute("SELECT SUM(pnl) FROM open_pos WHERE AND status = ?", ('TP'))
        tp = c.fetchone()[0]
        if tp is not None:
            tp_pnl = tp

        c.execute(
            "SELECT sym FROM open_pos WHERE AND status = ?",
            ('SL')
        )
        sl_len = len(c.fetchall())
        c.execute("SELECT SUM(pnl) FROM open_pos WHERE AND status = ?", ('SL'))
        sl = c.fetchone()[0]
        if sl is not None:
            sl_pnl = sl

        conn.close()
        return [[tp_len, tp_pnl], [sl_len, sl_pnl]]
    except Exception as e:
        print(f"Ошибка в read_stat: {e}")
        return False

def read_stat_rev_last(sym,side):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            c = conn.cursor()
            c.execute(
                "SELECT status, risk FROM open_pos_rev WHERE sym = ? AND side = ? ORDER BY id DESC LIMIT 1",
                (sym,side))
            row = c.fetchone()
        if row is not None:
            stat_last_rev, risk_last_rev = row
            return stat_last_rev, risk_last_rev
        else:
            return None
    except Exception as e:
        print(f"Ошибка в read_stat_rev_last: {e}")
        return None

def read_risk_rev_last(sym,side):
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute(
            "SELECT risk FROM open_pos_rev WHERE sym = ? AND side = ? ORDER BY id DESC LIMIT 1",
            (sym,side))
        stat_last_rev = c.fetchone()
        conn.close()
        if stat_last_rev is not None:
            return float(stat_last_rev[0])
        else:
            return None
    except Exception as e:
        print(f"Ошибка в read_risk_rev_last: {e}")
        return None

def read_risk_norm_last(sym,side):
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute(
            "SELECT risk FROM open_pos_norm WHERE sym = ? AND side = ? ORDER BY id DESC LIMIT 1",
            (sym,side))
        stat_last_rev = c.fetchone()
        conn.close()
        if stat_last_rev is not None:
            return float(stat_last_rev[0])
        else:
            return None
    except Exception as e:
        print(f"Ошибка в read_risk_norm_last: {e}")
        return None

def read_stat_norm_last(sym,side):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            c = conn.cursor()
            c.execute(
                "SELECT status, risk FROM open_pos_norm WHERE sym = ? AND side = ? ORDER BY id DESC LIMIT 1",
                (sym,side))
            row = c.fetchone()
        if row is not None:
            stat_last_norm, risk_last_norm = row
            return stat_last_norm, risk_last_norm
        else:
            return None
    except Exception as e:
        print(f"Ошибка в read_stat_norm_last: {e}")
        return None

def set_lev_all_first(sess):
    for row in SYM_LINE:
        lvl = get_max_leverage_for_symbol(sess, row)
        if lvl >= 20:
            set_leverage_for_symbol(sess, row, 20)


SYM_LINE = ['AKEUSDT', 'ENAUSDT', 'B2USDT', 'ZAMAUSDT', 'STRKUSDT']

if __name__ == "__main__":
    enable_wal_mode()
    init_db()
    try:
        sess = create_session_with_proxy(DEMO_CONFIG_SPEED)
    except Exception as e:
        pass

    while True:
        time.sleep(10)

    now_moment = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
    print(f"{now_moment} Фандинг монитор запущен (START)")
