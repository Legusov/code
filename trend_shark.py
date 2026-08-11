import time
import sqlite3
import json
import datetime
from pybit.unified_trading import HTTP
from api_conf import DEMO_CONFIG, DEMO_CONFIG1, DEMO_CONFIG2, DEMO_CONFIG3, DEMO_CONFIG4, DEMO_CONFIG5, DEMO_CONFIG6, \
    DEMO_CONFIG7, DEMO_CONFIG8, DEMO_CONFIG9, DEMO_CONFIG10, DEMO_CONFIG11
import math

# ================== НАСТРОЙКИ ==================
SHOW_PLOT = False
# ===============================================

if SHOW_PLOT:
    import matplotlib.pyplot as plt
    from collections import deque

    history_main_roi = deque(maxlen=100)
    history_time = deque(maxlen=100)

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.set_title('Траектория основного баланса (main_roi) с уровнями активации')
    ax.set_xlabel('Время (шаги)')
    ax.set_ylabel('ROI, %')
    ax.grid(True)
    line, = ax.plot([], [], 'b-', label='main_roi', linewidth=2)
    current_point, = ax.plot([], [], 'ro', markersize=8, label='Текущий')

    level_lines = []
    level_labels = []


    def update_plot(main_roi, last_activation_level, wave_start_level, states):
        history_main_roi.append(main_roi)
        history_time.append(len(history_time))

        x_data = list(range(len(history_main_roi)))
        y_data = list(history_main_roi)
        line.set_data(x_data, y_data)
        current_point.set_data([x_data[-1]], [y_data[-1]])

        for ln in level_lines:
            ln.remove()
        level_lines.clear()
        for lbl in level_labels:
            lbl.remove()
        level_labels.clear()

        for idx, st in enumerate(states):
            if idx == 0:
                continue
            if st['opened'] and st['activation_level'] != 0.0:
                level = st['activation_level']
                ln = ax.axhline(y=level, color='gray', linestyle='--', alpha=0.7)
                level_lines.append(ln)
                lbl = ax.text(x_data[-1] * 0.95, level, f' Суб{idx}', fontsize=8, color='gray',
                              verticalalignment='bottom', horizontalalignment='right')
                level_labels.append(lbl)

        if wave_start_level is not None:
            ln = ax.axhline(y=wave_start_level, color='green', linestyle='--', alpha=0.5)
            level_lines.append(ln)
            lbl = ax.text(x_data[-1] * 0.95, wave_start_level, ' Старт волны', fontsize=8, color='green',
                          verticalalignment='bottom', horizontalalignment='right')
            level_labels.append(lbl)

        if last_activation_level is not None and last_activation_level != wave_start_level:
            ln = ax.axhline(y=last_activation_level, color='orange', linestyle='--', alpha=0.5)
            level_lines.append(ln)
            lbl = ax.text(x_data[-1] * 0.95, last_activation_level, ' last_activation', fontsize=8, color='orange',
                          verticalalignment='bottom', horizontalalignment='right')
            level_labels.append(lbl)

        ax.set_xlim(0, max(10, len(history_main_roi) + 2))
        min_y = min(list(history_main_roi) + [wave_start_level or 0, last_activation_level or 0]) - 1.0
        max_y = max(list(history_main_roi) + [wave_start_level or 0, last_activation_level or 0]) + 1.0
        ax.set_ylim(min_y, max_y)
        ax.legend(loc='upper left')

        fig.canvas.draw_idle()
        fig.canvas.flush_events()
else:
    def update_plot(*args, **kwargs):
        pass

# ================== НАСТРАИВАЕМЫЕ ПАРАМЕТРЫ ==================

PROFIT_MAIN_CLOSE = 1.0
PROFIT_SUB_CLOSE_SELF = 1.0
DRAWDOWN_STEP = 1.0
GLOBAL_PROFIT_CLOSE = 1.0
ESTIMATED_FEE_PERCENT = 0.12  # Примерный размер комиссий за закрытие всех позиций (taker fee ~0.06% * 2 стороны)
TARGET_USD_PER_POSITION = 50.0
INITIAL_TOTAL_BALANCE = 1100.0  # суммарный стартовый баланс всех аккаунтов (10×100)
TARGET_BUY = 5  # сколько Buy-позиций открывать
TARGET_SELL = 5  # сколько Sell-позиций открывать
TARGET_POSITIONS = TARGET_BUY + TARGET_SELL  # можно оставить или удалить – теперь используется сумма TARGET_BUY + TARGET_SELL

CONFIGS = [DEMO_CONFIG, DEMO_CONFIG1, DEMO_CONFIG2, DEMO_CONFIG3, DEMO_CONFIG4,
           DEMO_CONFIG5, DEMO_CONFIG6, DEMO_CONFIG7, DEMO_CONFIG8, DEMO_CONFIG9, DEMO_CONFIG10]


def create_session_with_proxy(config):
    return HTTP(
        testnet=config.get('testnet', False),
        demo=config.get('demo', True),
        api_key=config['api_key'],
        api_secret=config['api_secret'],
        timeout=config.get('timeout', 20),
    )


sessions = [create_session_with_proxy(cfg) for cfg in CONFIGS]


def recreate_session(idx):
    global sessions
    print(f"Пересоздание сессии для аккаунта {idx}")
    sessions[idx] = create_session_with_proxy(CONFIGS[idx])
    return sessions[idx]


def safe_api_call(idx, func, *args, **kwargs):
    max_attempts = 3
    for attempt in range(max_attempts):
        try:
            sess = sessions[idx]
            result = func(sess, *args, **kwargs)
            return result
        except Exception as e:
            if "110043" in str(e) or "10001" in str(e):
                raise
            print(f"Ошибка API для аккаунта {idx} (попытка {attempt + 1}): {e}")
            if attempt < max_attempts - 1:
                recreate_session(idx)
                time.sleep(1)
            else:
                raise


def get_balance(sess):
    resp = sess.get_wallet_balance(accountType="UNIFIED", coin="USDT")
    return float(resp['result']['list'][0]['totalMarginBalance'])


def get_open_positions(sess):
    resp = sess.get_positions(category="linear", settleCoin="USDT")
    positions = []
    for pos in resp['result']['list']:
        size = float(pos['size'])
        if size != 0:
            positions.append((pos['symbol'], pos['side'], size))
    return positions


def get_instrument_info(sess, symbol):
    resp = sess.get_instruments_info(category="linear", symbol=symbol)
    if not resp['result']['list']:
        return None, None, None
    info = resp['result']['list'][0]
    lot_filter = info.get('lotSizeFilter', {})
    min_order_qty = float(lot_filter.get('minOrderQty', 0))
    qty_step = float(lot_filter.get('qtyStep', 0))
    max_leverage = float(info.get('leverageFilter', {}).get('maxLeverage', 0))
    if min_order_qty == 0 or qty_step == 0 or max_leverage == 0:
        return None, None, None
    return min_order_qty, qty_step, int(max_leverage)


def get_ticker_price(sess, symbol):
    resp = sess.get_tickers(category="linear", symbol=symbol)
    if not resp['result']['list']:
        return None
    return float(resp['result']['list'][0]['lastPrice'])


def set_leverage(sess, symbol, leverage):
    try:
        sess.set_leverage(
            category="linear",
            symbol=symbol,
            buyLeverage=str(leverage),
            sellLeverage=str(leverage),
            marginMode="cross"
        )
    except Exception as e:
        if "110043" in str(e):
            pass
        else:
            raise


# ================== ЛОГИРОВАНИЕ В БАЗУ ДАННЫХ ==================

def log_position(action, account_idx, symbol=None, side=None, qty=None, leverage=None,
                 open_price=None, close_price=None, profit=None, profit_percent=None):
    """Логирует операцию с позицией (открытие или закрытие)"""
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()

    states = load_cycle_state()
    wave = states[0].get('wave', 0) if states else 0
    account_name = "Основной" if account_idx == 0 else f"Субаккаунт {account_idx}"

    timestamp = datetime.datetime.now().isoformat()

    c.execute('''INSERT INTO position_log 
                 (timestamp, account_idx, account_name, wave, symbol, side, qty, leverage, 
                  open_price, action, close_price, profit, profit_percent)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
              (timestamp, account_idx, account_name, wave, symbol, side, qty, leverage,
               open_price, action, close_price, profit, profit_percent))

    conn.commit()
    conn.close()


def log_balance_snapshot(states, total_balance, total_roi, wave):
    """Сохраняет снимок балансов всех аккаунтов"""
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    timestamp = datetime.datetime.now().isoformat()

    for idx, st in enumerate(states):
        account_name = "Основной" if idx == 0 else f"Субаккаунт {idx}"
        baseline = st.get('baseline', 0)
        balance = st.get('last_balance', 0)
        roi = (balance - baseline) / baseline * 100 if baseline > 0 else 0

        c.execute('''INSERT INTO balance_snapshots 
                     (timestamp, wave, account_idx, account_name, balance, baseline, roi, total_balance, total_roi)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                  (timestamp, wave, idx, account_name, balance, baseline, roi, total_balance, total_roi))

    conn.commit()
    conn.close()


def log_wave_result(wave, start_time, end_time, start_balance, end_balance,
                    total_trades, winning_trades, losing_trades, max_drawdown, max_profit):
    """Сохраняет итоги по волне"""
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()

    profit = end_balance - start_balance
    profit_percent = (profit / start_balance) * 100 if start_balance and start_balance > 0 else 0
    win_rate = (winning_trades / total_trades * 100) if total_trades > 0 else 0

    c.execute('''INSERT INTO wave_results 
                 (wave, start_time, end_time, start_balance, end_balance, profit, profit_percent,
                  total_trades, winning_trades, losing_trades, win_rate, max_drawdown, max_profit)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
              (wave, start_time, end_time, start_balance, end_balance, profit, profit_percent,
               total_trades, winning_trades, losing_trades, win_rate, max_drawdown, max_profit))

    conn.commit()
    conn.close()


def log_account_performance(account_idx, account_name, profit, profit_percent,
                            total_trades, winning_trades, losing_trades):
    """Обновляет накопительную статистику по аккаунту"""
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()

    c.execute("SELECT * FROM account_performance WHERE account_idx = ?", (account_idx,))
    row = c.fetchone()

    if row:
        new_total_trades = row[4] + total_trades
        new_winning_trades = row[5] + winning_trades
        new_losing_trades = row[6] + losing_trades
        new_total_profit = row[2] + profit

        # Накопительный ROI (приблизительный, так как baseline не хранится в этой таблице)
        # Для корректного ROI нужно знать начальный баланс, но мы можем суммировать проценты
        # или пересчитать от текущего баланса, если бы он тут был.
        # Оставим суммирование процентов как временное решение, либо исправим логику вызова.
        new_total_profit_percent = row[3] + profit_percent

        win_rate = (new_winning_trades / new_total_trades * 100) if new_total_trades > 0 else 0
        avg_profit = new_total_profit / new_total_trades if new_total_trades > 0 else 0
        new_max_profit = max(row[9], profit)
        new_max_loss = min(row[10], profit)

        c.execute('''UPDATE account_performance 
                     SET total_profit = ?, total_profit_percent = ?, total_trades = ?,
                         winning_trades = ?, losing_trades = ?, win_rate = ?,
                         avg_profit = ?, max_profit = ?, max_loss = ?, last_update = ?
                     WHERE account_idx = ?''',
                  (new_total_profit, new_total_profit_percent, new_total_trades,
                   new_winning_trades, new_losing_trades, win_rate,
                   avg_profit, new_max_profit, new_max_loss, datetime.datetime.now().isoformat(), account_idx))
    else:
        win_rate = (winning_trades / total_trades * 100) if total_trades > 0 else 0
        avg_profit = profit / total_trades if total_trades > 0 else 0

        c.execute('''INSERT INTO account_performance 
                     (account_idx, account_name, total_profit, total_profit_percent,
                      total_trades, winning_trades, losing_trades, win_rate,
                      avg_profit, max_profit, max_loss, last_update)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                  (account_idx, account_name, profit, profit_percent,
                   total_trades, winning_trades, losing_trades, win_rate,
                   avg_profit, profit if profit > 0 else 0, profit if profit < 0 else 0,
                   datetime.datetime.now().isoformat()))

    conn.commit()
    conn.close()


def log_daily_stats():
    """Сохраняет ежедневную статистику"""
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()

    today = datetime.datetime.now().date().isoformat()

    c.execute('''SELECT total_balance FROM balance_snapshots 
                 WHERE date(timestamp) = ? 
                 ORDER BY timestamp DESC LIMIT 1''', (today,))
    row = c.fetchone()

    if row:
        current_balance = row[0]

        yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).date().isoformat()
        c.execute('''SELECT total_balance FROM balance_snapshots 
                     WHERE date(timestamp) = ? 
                     ORDER BY timestamp DESC LIMIT 1''', (yesterday,))
        prev_row = c.fetchone()

        daily_profit = 0
        daily_profit_percent = 0
        if prev_row:
            daily_profit = current_balance - prev_row[0]
            daily_profit_percent = (daily_profit / prev_row[0]) * 100 if prev_row[0] > 0 else 0

        c.execute('''SELECT COUNT(*), 
                     SUM(CASE WHEN profit > 0 THEN 1 ELSE 0 END),
                     SUM(CASE WHEN profit <= 0 THEN 1 ELSE 0 END)
                     FROM position_log 
                     WHERE date(timestamp) = ? AND action = 'close' ''', (today,))
        trade_row = c.fetchone()

        total_trades = trade_row[0] if trade_row else 0
        winning_trades = trade_row[1] if trade_row else 0
        losing_trades = trade_row[2] if trade_row else 0
        win_rate = (winning_trades / total_trades * 100) if total_trades > 0 else 0

        # Используем INSERT ... ON CONFLICT для сохранения ID
        c.execute('''INSERT INTO daily_stats 
                     (date, total_balance, daily_profit, daily_profit_percent,
                      total_trades, winning_trades, losing_trades, win_rate)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                     ON CONFLICT(date) DO UPDATE SET
                        total_balance=excluded.total_balance,
                        daily_profit=excluded.daily_profit,
                        daily_profit_percent=excluded.daily_profit_percent,
                        total_trades=excluded.total_trades,
                        winning_trades=excluded.winning_trades,
                        losing_trades=excluded.losing_trades,
                        win_rate=excluded.win_rate''',
                  (today, current_balance, daily_profit, daily_profit_percent,
                   total_trades, winning_trades, losing_trades, win_rate))

    conn.commit()
    conn.close()


def log_symbol_performance(symbol, profit, is_win):
    """Обновляет статистику по символу"""
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()

    c.execute("SELECT * FROM symbol_performance WHERE symbol = ?", (symbol,))
    row = c.fetchone()

    if row:
        total_trades = row[2] + 1
        winning_trades = row[3] + (1 if is_win else 0)
        losing_trades = row[4] + (0 if is_win else 1)
        total_profit = row[6] + profit
        win_rate = (winning_trades / total_trades * 100) if total_trades > 0 else 0
        avg_profit = total_profit / total_trades if total_trades > 0 else 0

        c.execute('''UPDATE symbol_performance 
                     SET total_trades = ?, winning_trades = ?, losing_trades = ?,
                         win_rate = ?, total_profit = ?, avg_profit = ?,
                         last_trade_time = ?
                     WHERE symbol = ?''',
                  (total_trades, winning_trades, losing_trades, win_rate,
                   total_profit, avg_profit, datetime.datetime.now().isoformat(), symbol))
    else:
        win_rate = 100 if is_win else 0
        c.execute('''INSERT INTO symbol_performance 
                     (symbol, total_trades, winning_trades, losing_trades,
                      win_rate, total_profit, avg_profit, last_trade_time)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                  (symbol, 1, 1 if is_win else 0, 0 if is_win else 1,
                   win_rate, profit, profit, datetime.datetime.now().isoformat()))

    conn.commit()
    conn.close()


def log_roi_history(total_roi, main_roi, avg_sub_roi, max_sub_roi, min_sub_roi, open_positions):
    """Сохраняет историю ROI"""
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    timestamp = datetime.datetime.now().isoformat()

    c.execute('''INSERT INTO roi_history 
                 (timestamp, total_roi, main_roi, avg_sub_roi, max_sub_roi, min_sub_roi, open_positions)
                 VALUES (?, ?, ?, ?, ?, ?, ?)''',
              (timestamp, total_roi, main_roi, avg_sub_roi, max_sub_roi, min_sub_roi, open_positions))

    conn.commit()
    conn.close()


def log_drawdown(current_balance, peak_balance, drawdown_percent):
    """Сохраняет данные о просадке"""
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    timestamp = datetime.datetime.now().isoformat()

    c.execute('''INSERT INTO drawdown_history 
                 (timestamp, current_balance, peak_balance, drawdown, drawdown_percent)
                 VALUES (?, ?, ?, ?, ?)''',
              (timestamp, current_balance, peak_balance, abs(current_balance - peak_balance), drawdown_percent))

    conn.commit()
    conn.close()


# ================== ТОРГОВЫЕ ФУНКЦИИ ==================

def place_order(sess, symbol, side, qty, leverage, account_idx=0):
    set_leverage(sess, symbol, leverage)
    position_idx = 1 if side == "Buy" else 2
    order = sess.place_order(
        category="linear",
        symbol=symbol,
        side=side,
        orderType="Market",
        qty=str(qty),
        positionIdx=position_idx,
        timeInForce="GTC"
    )
    if order.get('retCode') == 0:
        print(f"Открыта позиция {side} {symbol} qty={qty} с плечом {leverage}")
        time.sleep(0.1)
        price = None
        try:
            exec_resp = sess.get_executions(category="linear", symbol=symbol, limit=1)
            if exec_resp['result']['list']:
                price = float(exec_resp['result']['list'][0]['execPrice'])
        except:
            price = get_ticker_price(sess, symbol)

        if price is None:
            price = get_ticker_price(sess, symbol)

        log_position("open", account_idx, symbol, side, qty, leverage, open_price=price)
        return True
    else:
        print(f"Ошибка открытия позиции {symbol} {side}: {order}")
        return False


def close_all_positions_old(sess, account_idx=0):
    positions = get_open_positions(sess)
    total_profit = 0.0
    trades_count = 0
    winning_trades = 0
    losing_trades = 0

    for symbol, side, size in positions:
        close_side = "Sell" if side == "Buy" else "Buy"
        position_idx = 1 if side == "Buy" else 2

        # Получаем текущую цену как цену открытия для расчета (хотя лучше брать из БД)
        # Получаем цену прямо перед закрытием (для лога слипажа, если нужно)
        price_at_close_trigger = get_ticker_price(sess, symbol)

        # --- НОВОЕ: Получаем РЕАЛЬНУЮ цену открытия из БД ---
        real_open_price = None
        try:
            conn_temp = sqlite3.connect(DB_NAME)
            c_temp = conn_temp.cursor()
            c_temp.execute('''SELECT open_price FROM position_log 
                             WHERE symbol=? AND account_idx=? AND action='open' 
                             ORDER BY timestamp DESC LIMIT 1''', (symbol, account_idx))
            row = c_temp.fetchone()
            if row:
                real_open_price = row[0]
            conn_temp.close()
        except:
            pass

        if real_open_price is None:
            real_open_price = price_at_close_trigger

        order = sess.place_order(
            category="linear",
            symbol=symbol,
            side=close_side,
            orderType="Market",
            qty=str(abs(size)),
            positionIdx=position_idx,
            reduceOnly=True,
            timeInForce="GTC"
        )

        # Пытаемся получить реальную цену исполнения
        price_after = None
        if order.get('retCode') == 0:
            time.sleep(0.05)
            try:
                exec_resp = sess.get_executions(category="linear", symbol=symbol, limit=1)
                if exec_resp['result']['list']:
                    price_after = float(exec_resp['result']['list'][0]['execPrice'])
            except:
                price_after = get_ticker_price(sess, symbol)

        if price_after is None:
            price_after = get_ticker_price(sess, symbol) or price_at_close_trigger

        print(f"Закрыта позиция {symbol} {side} по цене {price_after} (открыта по {real_open_price})")

        profit = 0
        profit_percent = 0
        if real_open_price and price_after:
            leverages_info = load_cycle_info('leverages') or {}
            lev = leverages_info.get(symbol, 100)

            if side == "Buy":
                profit = (price_after - real_open_price) * abs(size)
                profit_percent = (profit / (abs(size) * real_open_price)) * 100 * lev
            else:
                profit = (real_open_price - price_after) * abs(size)
                profit_percent = (profit / (abs(size) * real_open_price)) * 100 * lev

        log_position("close", account_idx, symbol, side, abs(size), None,
                     open_price=real_open_price, close_price=price_after,
                     profit=profit, profit_percent=profit_percent)

        is_win = profit > 0
        log_symbol_performance(symbol, profit, is_win)

        total_profit += profit
        trades_count += 1
        if is_win:
            winning_trades += 1
        else:
            losing_trades += 1

    if trades_count > 0:
        account_name = "Основной" if account_idx == 0 else f"Субаккаунт {account_idx} + {total_profit}$"
        profit_percent_total = (total_profit / (abs(total_profit) + 1)) * 100
        log_account_performance(account_idx, account_name, total_profit, profit_percent_total,
                                trades_count, winning_trades, losing_trades)
    time.sleep(6)
    return total_profit, trades_count, winning_trades, losing_trades


def close_all_positions(sess, account_idx=0):
    # Получаем baseline (баланс до открытия) из БД
    states = load_cycle_state()
    if states and account_idx < len(states):
        baseline = states[account_idx].get('baseline', None)
    else:
        baseline = None

    positions = get_open_positions(sess)
    total_profit = 0.0
    trades_count = 0
    winning_trades = 0
    losing_trades = 0

    for symbol, side, size in positions:
        close_side = "Sell" if side == "Buy" else "Buy"
        position_idx = 1 if side == "Buy" else 2

        # Получаем цену прямо перед закрытием
        price_at_close_trigger = get_ticker_price(sess, symbol)

        # --- НОВОЕ: Получаем РЕАЛЬНУЮ цену открытия из БД ---
        real_open_price = None
        try:
            conn_temp = sqlite3.connect(DB_NAME)
            c_temp = conn_temp.cursor()
            c_temp.execute('''SELECT open_price FROM position_log 
                             WHERE symbol=? AND account_idx=? AND action='open' 
                             ORDER BY timestamp DESC LIMIT 1''', (symbol, account_idx))
            row = c_temp.fetchone()
            if row:
                real_open_price = row[0]
            conn_temp.close()
        except:
            pass

        if real_open_price is None:
            real_open_price = price_at_close_trigger

        order = sess.place_order(
            category="linear",
            symbol=symbol,
            side=close_side,
            orderType="Market",
            qty=str(abs(size)),
            positionIdx=position_idx,
            reduceOnly=True,
            timeInForce="GTC"
        )

        price_after = None
        if order.get('retCode') == 0:
            time.sleep(0.05)
            try:
                exec_resp = sess.get_executions(category="linear", symbol=symbol, limit=1)
                if exec_resp['result']['list']:
                    price_after = float(exec_resp['result']['list'][0]['execPrice'])
            except:
                price_after = get_ticker_price(sess, symbol)

        if price_after is None:
            price_after = get_ticker_price(sess, symbol) or price_at_close_trigger

        print(f"Закрыта позиция {symbol} {side} по цене {price_after} (открыта по {real_open_price})")

        profit = 0
        profit_percent = 0
        if real_open_price and price_after:
            leverages_info = load_cycle_info('leverages') or {}
            lev = leverages_info.get(symbol, 100)

            if side == "Buy":
                profit = (price_after - real_open_price) * abs(size)
                profit_percent = (profit / (abs(size) * real_open_price)) * 100 * lev
            else:
                profit = (real_open_price - price_after) * abs(size)
                profit_percent = (profit / (abs(size) * real_open_price)) * 100 * lev

        log_position("close", account_idx, symbol, side, abs(size), None,
                     open_price=real_open_price, close_price=price_after,
                     profit=profit, profit_percent=profit_percent)

        is_win = profit > 0
        log_symbol_performance(symbol, profit, is_win)

        total_profit += profit
        trades_count += 1
        if is_win:
            winning_trades += 1
        else:
            losing_trades += 1

    if trades_count > 0:
        account_name = "Основной" if account_idx == 0 else f"Субаккаунт {account_idx}"
        profit_percent_total = (total_profit / (abs(total_profit) + 1)) * 100
        log_account_performance(account_idx, account_name, total_profit, profit_percent_total,
                                trades_count, winning_trades, losing_trades)

    # Получаем баланс после закрытия
    balance_after = get_balance(sess)
    if balance_after is None:
        balance_after = 0.0

    # Вывод информации о балансе
    account_name = "Основной" if account_idx == 0 else f"Субаккаунт {account_idx}"
    if baseline is not None:
        diff = balance_after - baseline
        diff_percent = (diff / baseline * 100) if baseline != 0 else 0
        print(
            f"{account_name}: баланс до открытия (baseline): {baseline:.2f} USDT, после закрытия: {balance_after:.2f} USDT, изменение: {diff:+.2f} USDT ({diff_percent:+.2f}%)")
    else:
        print(f"{account_name}: баланс после закрытия: {balance_after:.2f} USDT (baseline не найден)")

        # --- Обновление last_balance в состоянии ---
    states = load_cycle_state()
    if states and account_idx < len(states):
        states[account_idx]['last_balance'] = balance_after
        save_cycle_state(states)
        print(f"Баланс {account_name} обновлён в состоянии: {balance_after:.2f} USDT")
    time.sleep(6)
    return total_profit, trades_count, winning_trades, losing_trades


def get_balance_safe(idx):
    return safe_api_call(idx, get_balance)


def get_open_positions_safe(idx):
    return safe_api_call(idx, get_open_positions)


def get_instrument_info_safe(idx, symbol):
    return safe_api_call(idx, get_instrument_info, symbol)


def get_ticker_price_safe(idx, symbol):
    return safe_api_call(idx, get_ticker_price, symbol)


def set_leverage_safe(idx, symbol, leverage):
    safe_api_call(idx, set_leverage, symbol, leverage)


def place_order_safe(idx, symbol, side, qty, leverage):
    return safe_api_call(idx, place_order, symbol, side, qty, leverage, account_idx=idx)


def close_all_positions_safe(idx):
    return safe_api_call(idx, close_all_positions, account_idx=idx)


def calculate_qty(idx, symbol, target_usd=TARGET_USD_PER_POSITION):
    price = get_ticker_price_safe(idx, symbol)
    if price is None:
        return None, None
    min_order_qty, qty_step, max_leverage = get_instrument_info_safe(idx, symbol)
    if None in (min_order_qty, qty_step, max_leverage):
        return None, None
    raw_qty = target_usd / price
    qty = int(raw_qty / qty_step) * qty_step
    if qty < min_order_qty:
        qty = min_order_qty
        remainder = qty % qty_step
        if remainder != 0:
            qty = qty + (qty_step - remainder)
    if qty <= 0:
        print(f"Рассчитанное qty {qty} меньше или равно 0 для {symbol}")
        return None, None
    return round(qty, 8), max_leverage


DB_NAME = "trading_bot.db"


def init_db():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()

    # Основные таблицы
    c.execute('''CREATE TABLE IF NOT EXISTS positions (symbol TEXT, side TEXT, qty REAL)''')

    c.execute('''CREATE TABLE IF NOT EXISTS cycle_state
                 (idx INTEGER PRIMARY KEY, baseline REAL, last_balance REAL, active INTEGER, opened INTEGER, closed INTEGER, wave INTEGER DEFAULT 0, activation_level REAL DEFAULT 0.0)''')

    c.execute("PRAGMA table_info(cycle_state)")
    columns = [col[1] for col in c.fetchall()]
    if 'wave' not in columns:
        c.execute("ALTER TABLE cycle_state ADD COLUMN wave INTEGER DEFAULT 0")
    if 'activation_level' not in columns:
        c.execute("ALTER TABLE cycle_state ADD COLUMN activation_level REAL DEFAULT 0.0")

    c.execute('''CREATE TABLE IF NOT EXISTS cycle_info (key TEXT PRIMARY KEY, value TEXT)''')

    # Таблица для логирования позиций
    c.execute('''CREATE TABLE IF NOT EXISTS position_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT,
        account_idx INTEGER,
        account_name TEXT,
        wave INTEGER,
        symbol TEXT,
        side TEXT,
        qty REAL,
        leverage INTEGER,
        open_price REAL,
        action TEXT,
        close_price REAL,
        profit REAL,
        profit_percent REAL
    )''')

    # ===== НОВЫЕ ТАБЛИЦЫ ДЛЯ АНАЛИТИКИ =====

    # 1. Снимки балансов всех аккаунтов
    c.execute('''CREATE TABLE IF NOT EXISTS balance_snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT,
        wave INTEGER,
        account_idx INTEGER,
        account_name TEXT,
        balance REAL,
        baseline REAL,
        roi REAL,
        total_balance REAL,
        total_roi REAL
    )''')

    # 2. Итоги по волнам
    c.execute('''CREATE TABLE IF NOT EXISTS wave_results (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        wave INTEGER,
        start_time TEXT,
        end_time TEXT,
        start_balance REAL,
        end_balance REAL,
        profit REAL,
        profit_percent REAL,
        total_trades INTEGER,
        winning_trades INTEGER,
        losing_trades INTEGER,
        win_rate REAL,
        max_drawdown REAL,
        max_profit REAL
    )''')

    # 3. Итоги по аккаунтам (накопительные)
    c.execute('''CREATE TABLE IF NOT EXISTS account_performance (
        account_idx INTEGER PRIMARY KEY,
        account_name TEXT,
        total_profit REAL,
        total_profit_percent REAL,
        total_trades INTEGER,
        winning_trades INTEGER,
        losing_trades INTEGER,
        win_rate REAL,
        avg_profit REAL,
        max_profit REAL,
        max_loss REAL,
        last_update TEXT
    )''')

    # 4. Ежедневная статистика
    c.execute('''CREATE TABLE IF NOT EXISTS daily_stats (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT UNIQUE,
        total_balance REAL,
        daily_profit REAL,
        daily_profit_percent REAL,
        total_trades INTEGER,
        winning_trades INTEGER,
        losing_trades INTEGER,
        win_rate REAL
    )''')

    # 5. Метрики по символам
    c.execute('''CREATE TABLE IF NOT EXISTS symbol_performance (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT UNIQUE,
        total_trades INTEGER,
        winning_trades INTEGER,
        losing_trades INTEGER,
        win_rate REAL,
        total_profit REAL,
        avg_profit REAL,
        last_trade_time TEXT
    )''')

    # 6. История ROI
    c.execute('''CREATE TABLE IF NOT EXISTS roi_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT,
        total_roi REAL,
        main_roi REAL,
        avg_sub_roi REAL,
        max_sub_roi REAL,
        min_sub_roi REAL,
        open_positions INTEGER
    )''')

    # 7. История просадок
    c.execute('''CREATE TABLE IF NOT EXISTS drawdown_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT,
        current_balance REAL,
        peak_balance REAL,
        drawdown REAL,
        drawdown_percent REAL
    )''')

    conn.commit()
    conn.close()


def save_positions(positions):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("DELETE FROM positions")
    c.executemany("INSERT INTO positions VALUES (?, ?, ?)", positions)
    conn.commit()
    conn.close()


def load_positions():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT symbol, side, qty FROM positions")
    rows = c.fetchall()
    conn.close()
    return rows


def load_cycle_state():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute(
        "SELECT idx, baseline, last_balance, active, opened, closed, wave, activation_level FROM cycle_state ORDER BY idx")
    rows = c.fetchall()
    conn.close()
    if not rows:
        return None
    states = []
    for idx, baseline, last_balance, active, opened, closed, wave, activation_level in rows:
        # Если closed=True - аккаунт закрыт, active должен быть False
        if closed:
            active = False
        # Если closed=True и opened=False - это полностью сброшенный аккаунт
        # Если closed=True и opened=True - это закрытый, но использованный аккаунт
        # В обоих случаях active=False
        states.append({
            'baseline': baseline,
            'last_balance': last_balance,
            'active': bool(active),
            'opened': bool(opened),
            'closed': bool(closed),
            'wave': wave,
            'activation_level': activation_level
        })
    return states


def save_cycle_state(states):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    for idx, st in enumerate(states):
        # Если closed=True - active всегда False
        active = 0 if st['closed'] else (1 if st['active'] else 0)
        # Если closed=True - activation_level должен быть 0.0
        activation_level = 0.0 if st['closed'] else st.get('activation_level', 0.0)

        c.execute('''REPLACE INTO cycle_state (idx, baseline, last_balance, active, opened, closed, wave, activation_level)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                  (idx, st['baseline'], st['last_balance'],
                   active,
                   1 if st['opened'] else 0,
                   1 if st['closed'] else 0,
                   st.get('wave', 0),
                   activation_level))
    conn.commit()
    conn.close()


def save_cycle_info(key, value):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("REPLACE INTO cycle_info (key, value) VALUES (?, ?)", (key, json.dumps(value)))
    conn.commit()
    conn.close()


def load_cycle_info(key):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT value FROM cycle_info WHERE key=?", (key,))
    row = c.fetchone()
    conn.close()
    if row:
        return json.loads(row[0])
    return None


def clear_cycle():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("DELETE FROM cycle_state")
    c.execute("DELETE FROM cycle_info")
    conn.commit()
    conn.close()


def get_signal_any(sess, num_symbols=20):
    try:
        resp = sess.get_instruments_info(category="linear")
        if resp['retCode'] != 0:
            print("Ошибка получения списка инструментов")
            return []
        instruments = resp['result']['list']
    except Exception as e:
        print(f"Ошибка получения списка инструментов: {e}")
        return []

    usdt_symbols = [item['symbol'] for item in instruments if
                    item['symbol'].endswith('USDT') and item.get('status') == 'Trading']
    if not usdt_symbols:
        print("Нет USDT пар для анализа")
        return []

    print(f"Найдено {len(usdt_symbols)} активных USDT пар. Начинаем анализ...")

    tickers = {}
    try:
        ticker_resp = sess.get_tickers(category="linear")
        if ticker_resp['retCode'] == 0:
            for t in ticker_resp['result']['list']:
                tickers[t['symbol']] = {
                    'lastPrice': float(t['lastPrice']),
                    'turnover24h': float(t.get('turnover24h', 0))
                }
    except Exception as e:
        print(f"Ошибка получения тикеров: {e}")
        return []

    spot_sess = HTTP(testnet=False, demo=True)

    def get_monthly_candles(symbol):
        try:
            resp = spot_sess.get_kline(category="spot", symbol=symbol, interval="M", limit=20)
            if resp['retCode'] == 0 and resp['result']['list']:
                return len(resp['result']['list'])
            return 0
        except:
            return 0

    candidates = []
    sorted_by_volume = sorted([(sym, tickers[sym]['turnover24h']) for sym in usdt_symbols if sym in tickers],
                              key=lambda x: x[1], reverse=True)[:200]

    for symbol, turnover in sorted_by_volume:
        if turnover < 1_000_000:
            continue
        monthly_count = get_monthly_candles(symbol)
        if monthly_count < 12:
            continue

        try:
            kline_resp = sess.get_kline(category="linear", symbol=symbol, interval="D", limit=30)
            if kline_resp['retCode'] != 0 or not kline_resp['result']['list']:
                continue
            candles = kline_resp['result']['list']
            closes = [float(c[3]) for c in candles]
            highs = [float(c[1]) for c in candles]
            lows = [float(c[2]) for c in candles]
            if len(closes) < 20:
                continue

            sma20 = sum(closes[-20:]) / 20
            current_price = closes[-1]
            price_to_sma = current_price / sma20 if sma20 != 0 else 1
            if not (0.92 <= price_to_sma <= 1.08):
                continue

            tr_list = []
            for i in range(1, len(closes)):
                tr = max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1]))
                tr_list.append(tr)
            atr = sum(tr_list[-14:]) / 14 if len(tr_list) >= 14 else sum(tr_list) / len(tr_list)
            atr_ratio = atr / current_price if current_price != 0 else 0
            if atr_ratio < 0.02:
                continue

            if len(closes) >= 4:
                change_3d = abs((closes[-1] - closes[-4]) / closes[-4]) if closes[-4] != 0 else 0
                if change_3d > 0.20:
                    continue

            if len(closes) >= 50:
                sma50 = sum(closes[-50:]) / 50
                trend_side = "Buy" if current_price > sma50 else "Sell"
            else:
                trend_side = "Buy" if current_price > sma20 else "Sell"

            volume_score = math.log(turnover)
            volatility_score = atr_ratio * 100
            sma_score = 1 - abs(price_to_sma - 1)
            rating = volume_score * 0.5 + volatility_score * 0.3 + sma_score * 0.2

            candidates.append({
                'symbol': symbol,
                'side': trend_side,
                'rating': rating,
            })
        except:
            continue

    candidates.sort(key=lambda x: x['rating'], reverse=True)
    result = [(item['symbol'], item['side']) for item in candidates[:num_symbols]]
    print(f"Отобрано {len(result)} пар для сигнала:")
    for sym, side in result:
        print(f"  {sym} -> {side}")
    return result


def get_signal_5_5(sess, target_buy=TARGET_BUY, target_sell=TARGET_SELL):
    try:
        resp = sess.get_instruments_info(category="linear")
        if resp['retCode'] != 0:
            print("Ошибка получения списка инструментов")
            return [], []
        instruments = resp['result']['list']
    except Exception as e:
        print(f"Ошибка получения списка инструментов: {e}")
        return [], []

    usdt_symbols = [item['symbol'] for item in instruments if
                    item['symbol'].endswith('USDT') and item.get('status') == 'Trading']
    if not usdt_symbols:
        print("Нет USDT пар для анализа")
        return [], []

    print(f"Найдено {len(usdt_symbols)} активных USDT пар. Начинаем анализ...")

    tickers = {}
    try:
        ticker_resp = sess.get_tickers(category="linear")
        if ticker_resp['retCode'] == 0:
            for t in ticker_resp['result']['list']:
                tickers[t['symbol']] = {
                    'lastPrice': float(t['lastPrice']),
                    'turnover24h': float(t.get('turnover24h', 0))
                }
    except Exception as e:
        print(f"Ошибка получения тикеров: {e}")
        return [], []

    spot_sess = HTTP(testnet=False, demo=True)

    def get_monthly_candles(symbol):
        try:
            resp = spot_sess.get_kline(category="spot", symbol=symbol, interval="M", limit=20)
            if resp['retCode'] == 0 and resp['result']['list']:
                return len(resp['result']['list'])
            return 0
        except:
            return 0

    candidates = []
    # Увеличиваем количество рассматриваемых по объёму до 300, чтобы гарантированно набрать 5+5
    sorted_by_volume = sorted([(sym, tickers[sym]['turnover24h']) for sym in usdt_symbols if sym in tickers],
                              key=lambda x: x[1], reverse=True)[:300]

    for symbol, turnover in sorted_by_volume:
        if turnover < 1_000_000:
            continue
        monthly_count = get_monthly_candles(symbol)
        if monthly_count < 12:
            continue

        try:
            kline_resp = sess.get_kline(category="linear", symbol=symbol, interval="D", limit=30)
            if kline_resp['retCode'] != 0 or not kline_resp['result']['list']:
                continue
            candles = kline_resp['result']['list']
            closes = [float(c[3]) for c in candles]
            highs = [float(c[1]) for c in candles]
            lows = [float(c[2]) for c in candles]
            if len(closes) < 20:
                continue

            sma20 = sum(closes[-20:]) / 20
            current_price = closes[-1]
            price_to_sma = current_price / sma20 if sma20 != 0 else 1
            if not (0.92 <= price_to_sma <= 1.08):
                continue

            tr_list = []
            for i in range(1, len(closes)):
                tr = max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1]))
                tr_list.append(tr)
            atr = sum(tr_list[-14:]) / 14 if len(tr_list) >= 14 else sum(tr_list) / len(tr_list)
            atr_ratio = atr / current_price if current_price != 0 else 0
            if atr_ratio < 0.02:
                continue

            if len(closes) >= 4:
                change_3d = abs((closes[-1] - closes[-4]) / closes[-4]) if closes[-4] != 0 else 0
                if change_3d > 0.20:
                    continue

            if len(closes) >= 50:
                sma50 = sum(closes[-50:]) / 50
                trend_side = "Buy" if current_price > sma50 else "Sell"
            else:
                trend_side = "Buy" if current_price > sma20 else "Sell"

            volume_score = math.log(turnover)
            volatility_score = atr_ratio * 100
            sma_score = 1 - abs(price_to_sma - 1)
            rating = volume_score * 0.5 + volatility_score * 0.3 + sma_score * 0.2

            candidates.append({
                'symbol': symbol,
                'side': trend_side,
                'rating': rating,
            })
        except:
            continue

    # Разделяем на Buy и Sell, сортируем по рейтингу
    buy_candidates = [c for c in candidates if c['side'] == 'Buy']
    sell_candidates = [c for c in candidates if c['side'] == 'Sell']
    buy_candidates.sort(key=lambda x: x['rating'], reverse=True)
    sell_candidates.sort(key=lambda x: x['rating'], reverse=True)

    buy_symbols = [c['symbol'] for c in buy_candidates[:target_buy]]
    sell_symbols = [c['symbol'] for c in sell_candidates[:target_sell]]

    print(f"Отобрано Buy: {len(buy_symbols)}, Sell: {len(sell_symbols)}")
    if buy_symbols:
        print("  Buy:", ', '.join(buy_symbols))
    if sell_symbols:
        print("  Sell:", ', '.join(sell_symbols))
    return buy_symbols, sell_symbols


def get_signal(sess, target_buy=TARGET_BUY, target_sell=TARGET_SELL):
    try:
        resp = sess.get_instruments_info(category="linear")
        if resp['retCode'] != 0:
            print("Ошибка получения списка инструментов")
            return [], []
        instruments = resp['result']['list']
    except Exception as e:
        print(f"Ошибка получения списка инструментов: {e}")
        return [], []

    usdt_symbols = [item['symbol'] for item in instruments if
                    item['symbol'].endswith('USDT') and item.get('status') == 'Trading']
    if not usdt_symbols:
        print("Нет USDT пар для анализа")
        return [], []

    print(f"Найдено {len(usdt_symbols)} активных USDT пар. Начинаем анализ...")

    tickers = {}
    try:
        ticker_resp = sess.get_tickers(category="linear")
        if ticker_resp['retCode'] == 0:
            for t in ticker_resp['result']['list']:
                tickers[t['symbol']] = {
                    'lastPrice': float(t['lastPrice']),
                    'turnover24h': float(t.get('turnover24h', 0))
                }
    except Exception as e:
        print(f"Ошибка получения тикеров: {e}")
        return [], []

    spot_sess = HTTP(testnet=False, demo=True)

    def get_monthly_candles(symbol):
        try:
            resp = spot_sess.get_kline(category="spot", symbol=symbol, interval="M", limit=20)
            if resp['retCode'] == 0 and resp['result']['list']:
                return len(resp['result']['list'])
            return 0
        except:
            return 0

    candidates = []
    sorted_by_volume = sorted([(sym, tickers[sym]['turnover24h']) for sym in usdt_symbols if sym in tickers],
                              key=lambda x: x[1], reverse=True)[:300]

    for symbol, turnover in sorted_by_volume:
        if turnover < 1_000_000:
            continue
        monthly_count = get_monthly_candles(symbol)
        if monthly_count < 12:
            continue

        try:
            kline_resp = sess.get_kline(category="linear", symbol=symbol, interval="D", limit=30)
            if kline_resp['retCode'] != 0 or not kline_resp['result']['list']:
                continue
            candles = kline_resp['result']['list']
            closes = [float(c[3]) for c in candles]
            highs = [float(c[1]) for c in candles]
            lows = [float(c[2]) for c in candles]
            if len(closes) < 20:
                continue

            # ---- Фильтры для исключения пампа/дампа ----
            # 1. Дневное изменение не более 10%
            if len(closes) >= 2:
                change_1d = (closes[-1] - closes[-2]) / closes[-2]
                if abs(change_1d) > 0.10:
                    continue

            # 2. Изменение за 3 дня не более 15%
            if len(closes) >= 4:
                change_3d = (closes[-1] - closes[-4]) / closes[-4]
                if abs(change_3d) > 0.15:
                    continue

            sma20 = sum(closes[-20:]) / 20
            current_price = closes[-1]
            price_to_sma20 = current_price / sma20 if sma20 != 0 else 1
            if not (0.95 <= price_to_sma20 <= 1.05):
                continue

            # 3. Отклонение от SMA50 не более 20%
            if len(closes) >= 50:
                sma50 = sum(closes[-50:]) / 50
                price_to_sma50 = current_price / sma50 if sma50 != 0 else 1
                if abs(price_to_sma50 - 1) > 0.20:
                    continue

            # 4. Волатильность (ATR) в пределах 2%-8%
            tr_list = []
            for i in range(1, len(closes)):
                tr = max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1]))
                tr_list.append(tr)
            atr = sum(tr_list[-14:]) / 14 if len(tr_list) >= 14 else sum(tr_list) / len(tr_list)
            atr_ratio = atr / current_price if current_price != 0 else 0
            if atr_ratio < 0.02 or atr_ratio > 0.08:
                continue

            # ---- Определение направления тренда ----
            if len(closes) >= 50:
                sma50 = sum(closes[-50:]) / 50
                trend_side = "Buy" if current_price > sma50 else "Sell"
            else:
                trend_side = "Buy" if current_price > sma20 else "Sell"

            volume_score = math.log(turnover)
            volatility_score = atr_ratio * 100
            sma_score = 1 - abs(price_to_sma20 - 1)
            rating = volume_score * 0.5 + volatility_score * 0.3 + sma_score * 0.2

            candidates.append({
                'symbol': symbol,
                'side': trend_side,
                'rating': rating,
            })
        except Exception as e:
            continue

    buy_candidates = [c for c in candidates if c['side'] == 'Buy']
    sell_candidates = [c for c in candidates if c['side'] == 'Sell']
    buy_candidates.sort(key=lambda x: x['rating'], reverse=True)
    sell_candidates.sort(key=lambda x: x['rating'], reverse=True)

    buy_symbols = [c['symbol'] for c in buy_candidates[:target_buy]]
    sell_symbols = [c['symbol'] for c in sell_candidates[:target_sell]]

    print(f"Отобрано Buy: {len(buy_symbols)}, Sell: {len(sell_symbols)}")
    if buy_symbols:
        print("  Buy:", ', '.join(buy_symbols))
    if sell_symbols:
        print("  Sell:", ', '.join(sell_symbols))
    return buy_symbols, sell_symbols


def print_status():
    states = load_cycle_state()
    if states is None:
        print("Активный цикл не найден.")
        return
    print("=== Текущее состояние цикла ===")
    for idx, st in enumerate(states):
        acc_name = "Основной" if idx == 0 else f"Субаккаунт {idx}"
        status = "открыт" if st['opened'] and not st['closed'] else "закрыт" if st['closed'] else "не активен"
        wave_info = f", волна: {st.get('wave', 0)}" if idx == 0 else ""
        print(
            f"{acc_name}: баланс {st['last_balance']:.2f} USDT, статус: {status}{wave_info}, уровень активации: {st.get('activation_level', 0.0):.2f}%")
    baseline_total = load_cycle_info('baseline_total')
    if baseline_total:
        print(f"Суммарный начальный баланс: {baseline_total:.2f} USDT")
    leverages = load_cycle_info('leverages')
    if leverages:
        print("Плечи по символам:", leverages)
    last_activation_level = load_cycle_info('last_activation_level')
    if last_activation_level is not None:
        print(f"Последний уровень активации: {last_activation_level:.2f}%")
    wave_start_level = load_cycle_info('wave_start_level')
    if wave_start_level is not None:
        print(f"Уровень начала волны: {wave_start_level:.2f}%")
    print("=================================")


def print_all_balances(states):
    print("=== БАЛАНСЫ ВСЕХ АККАУНТОВ ===")
    summ_all = 0
    for idx, st in enumerate(states):
        acc_name = "Основной" if idx == 0 else f"Субаккаунт {idx}"
        summ_all += float(st['last_balance'])
        print(f"{acc_name}: {st['last_balance']:.2f} USDT")
    print(f'Суммарный общий баланс: {round(summ_all, 3)}')
    print("=================================")


def print_analytics_old():
    """Выводит аналитику по текущему состоянию"""
    print("\n" + "=" * 70)
    print("📊 АНАЛИТИКА ТОРГОВЛИ")
    print("=" * 70)

    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()

    # 1. Общая статистика по аккаунтам
    print("\n📈 СТАТИСТИКА ПО АККАУНТАМ:")
    c.execute('''SELECT account_name, total_trades, winning_trades, losing_trades, 
                        win_rate, total_profit, avg_profit
                 FROM account_performance 
                 ORDER BY total_profit DESC''')
    rows = c.fetchall()

    if rows:
        print(
            f"{'Аккаунт':<18} {'Трейдов':<10} {'Выигрышей':<12} {'Проигрышей':<12} {'Win Rate':<12} {'Прибыль':<14} {'Ср.прибыль':<12}")
        print("-" * 90)
        for row in rows:
            print(
                f"{row[0]:<18} {row[1]:<10} {row[2]:<12} {row[3]:<12} {row[4]:<11.1f}% {row[5]:<13.2f} {row[6]:<12.2f}")
    else:
        print("  Нет данных по аккаунтам")

    # 2. Топ-5 прибыльных символов
    print("\n🏆 ТОП-5 ПРИБЫЛЬНЫХ СИМВОЛОВ:")
    c.execute('''SELECT symbol, total_trades, winning_trades, losing_trades, 
                        win_rate, total_profit, avg_profit
                 FROM symbol_performance 
                 WHERE total_trades > 0
                 ORDER BY total_profit DESC 
                 LIMIT 5''')
    rows = c.fetchall()

    if rows:
        print(f"{'Символ':<12} {'Трейдов':<10} {'Выигрышей':<12} {'Проигрышей':<12} {'Win Rate':<12} {'Прибыль':<14}")
        print("-" * 75)
        for row in rows:
            print(f"{row[0]:<12} {row[1]:<10} {row[2]:<12} {row[3]:<12} {row[4]:<11.1f}% {row[5]:<13.2f}")

    # 3. Статистика по волнам
    print("\n🌊 СТАТИСТИКА ПО ВОЛНАМ (последние 5):")
    c.execute('''SELECT wave, profit, profit_percent, total_trades, win_rate
                 FROM wave_results 
                 ORDER BY wave DESC 
                 LIMIT 5''')
    rows = c.fetchall()

    if rows:
        print(f"{'Волна':<10} {'Прибыль':<14} {'ROI':<12} {'Трейдов':<10} {'Win Rate':<12}")
        print("-" * 60)
        for row in rows:
            print(f"{row[0]:<10} {row[1]:<13.2f} {row[2]:<11.2f}% {row[3]:<10} {row[4]:<11.1f}%")

    # 4. Ежедневная статистика
    print("\n📅 ЕЖЕДНЕВНАЯ СТАТИСТИКА (последние 7 дней):")
    c.execute('''SELECT date, total_balance, daily_profit, daily_profit_percent, total_trades, win_rate
                 FROM daily_stats 
                 ORDER BY date DESC 
                 LIMIT 7''')
    rows = c.fetchall()

    if rows:
        print(f"{'Дата':<14} {'Баланс':<14} {'Прибыль':<14} {'ROI':<12} {'Трейдов':<10} {'Win Rate':<12}")
        print("-" * 80)
        for row in rows:
            print(f"{row[0]:<14} {row[1]:<13.2f} {row[2]:<13.2f} {row[3]:<11.2f}% {row[4]:<10} {row[5]:<11.1f}%")

    # 5. Общая сводка
    print("\n📊 ОБЩАЯ СВОДКА:")
    c.execute('''SELECT COUNT(*), SUM(profit), AVG(profit_percent), 
                        SUM(CASE WHEN profit > 0 THEN 1 ELSE 0 END),
                        SUM(CASE WHEN profit < 0 THEN 1 ELSE 0 END)
                 FROM position_log 
                 WHERE action = 'close' ''')
    row = c.fetchone()

    if row and row[0] > 0:
        total_trades = row[0]
        total_profit = row[1] if row[1] else 0
        avg_profit = row[2] if row[2] else 0
        winning_trades = row[3] if row[3] else 0
        losing_trades = row[4] if row[4] else 0
        win_rate = (winning_trades / total_trades * 100) if total_trades > 0 else 0

        print(f"  Всего трейдов:     {total_trades}")
        print(f"  Общая прибыль:     {total_profit:.2f} USDT")
        print(f"  Средняя прибыль:   {avg_profit:.2f}%")
        print(f"  Выигрышных:        {winning_trades} ({win_rate:.1f}%)")
        print(f"  Проигрышных:       {losing_trades}")

        # Общий ROI
        c.execute("SELECT total_roi FROM balance_snapshots ORDER BY timestamp DESC LIMIT 1")
        row = c.fetchone()
        if row:
            print(f"  Текущий общий ROI: {row[0]:.2f}%")
    else:
        print("  Нет завершённых трейдов")

    conn.close()
    print("=" * 70 + "\n")


def print_analytics():
    """Выводит аналитику по текущему состоянию"""
    print("\n" + "=" * 70)
    print("📊 АНАЛИТИКА ТОРГОВЛИ")
    print("=" * 70)

    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()

    # ... (существующий код вывода статистики по аккаунтам, символам, волнам, ежедневной статистике) ...

    # 5. Общая сводка (существующий блок)
    print("\n📊 ОБЩАЯ СВОДКА:")
    c.execute('''SELECT COUNT(*), SUM(profit), AVG(profit_percent), 
                        SUM(CASE WHEN profit > 0 THEN 1 ELSE 0 END),
                        SUM(CASE WHEN profit < 0 THEN 1 ELSE 0 END)
                 FROM position_log 
                 WHERE action = 'close' ''')
    row = c.fetchone()

    if row and row[0] > 0:
        total_trades = row[0]
        total_profit = row[1] if row[1] else 0
        avg_profit = row[2] if row[2] else 0
        winning_trades = row[3] if row[3] else 0
        losing_trades = row[4] if row[4] else 0
        win_rate = (winning_trades / total_trades * 100) if total_trades > 0 else 0

        print(f"  Всего трейдов:     {total_trades}")
        print(f"  Прибыль по логам:  {total_profit:.2f} USDT (сумма всех закрытых сделок)")
        print(f"  Средняя прибыль:   {avg_profit:.2f}%")
        print(f"  Выигрышных:        {winning_trades} ({win_rate:.1f}%)")
        print(f"  Проигрышных:       {losing_trades}")
        print(f"  Безубыточных:      {total_trades - winning_trades - losing_trades}")

        # НОВОЕ: получить последний суммарный баланс из снимков
        c.execute("SELECT total_balance FROM balance_snapshots ORDER BY timestamp DESC LIMIT 1")
        snap_row = c.fetchone()
        if snap_row:
            current_total = snap_row[0]
            total_profit_from_start = current_total - INITIAL_TOTAL_BALANCE
            total_roi_from_start = (
                                               total_profit_from_start / INITIAL_TOTAL_BALANCE) * 100 if INITIAL_TOTAL_BALANCE > 0 else 0
            print(f"  --- Реальный рост капитала ---")
            print(f"  Начальный баланс:     {INITIAL_TOTAL_BALANCE:.2f} USDT")
            print(f"  Текущий баланс:       {current_total:.2f} USDT")
            print(f"  Чистая прибыль:       {total_profit_from_start:.2f} USDT")
            print(f"  Реальный ROI:         {total_roi_from_start:.2f}%")
        else:
            print("  Нет данных о балансах для расчёта ROI от старта")

        # Оставляем старый вывод текущего ROI (из последнего снимка)
        c.execute("SELECT total_roi FROM balance_snapshots ORDER BY timestamp DESC LIMIT 1")
        row_roi = c.fetchone()
        if row_roi:
            print(f"  Текущий общий ROI (по циклу): {row_roi[0]:.2f}%")
    else:
        print("  Нет завершённых трейдов")

    conn.close()
    print("=" * 70 + "\n")


def fix_cycle_state(states):
    fixed = False
    for idx in range(1, 11):
        if states[idx]['opened']:
            pos = get_open_positions_safe(idx)
            if not pos:
                print(f"Субаккаунт {idx} помечен как открытый, но позиций нет. Сбрасываем состояние.")
                states[idx]['active'] = False
                states[idx]['opened'] = False
                states[idx]['closed'] = False
                states[idx]['activation_level'] = 0.0
                fixed = True

    if not fixed:
        last_activation_level = load_cycle_info('last_activation_level')
        wave_start_level = load_cycle_info('wave_start_level')
        return states, last_activation_level, wave_start_level

    wave = states[0].get('wave', 0)
    max_open_idx = 0
    for i in range(1, 11):
        if states[i]['opened'] and not states[i]['closed']:
            max_open_idx = i

    if max_open_idx > 0:
        new_last_level = -DRAWDOWN_STEP * (wave * 11 + max_open_idx)
    else:
        wave_start_level = load_cycle_info('wave_start_level')
        if wave_start_level is None:
            wave_start_level = -DRAWDOWN_STEP * (wave + 1)
        new_last_level = wave_start_level

    save_cycle_info('last_activation_level', new_last_level)
    save_cycle_state(states)

    print(f"Состояние скорректировано. Новый last_activation_level: {new_last_level:.2f}%")
    return states, new_last_level, load_cycle_info('wave_start_level')


def sync_positions_with_exchange(states, baseline_total, leverages):
    """
    Синхронизирует состояние в БД с реальными позициями на бирже.
    Проверяет все аккаунты, обновляет статусы и закрывает профитные позиции.
    """
    print("=== СИНХРОНИЗАЦИЯ С БИРЖЕЙ ===")
    states_changed = False
    any_closed = False

    # Проверяем основной аккаунт
    main_positions = get_open_positions_safe(0)
    if main_positions and not states[0]['opened']:
        print(f"На основном аккаунте обнаружены открытые позиции, обновляем состояние.")
        states[0]['opened'] = True
        states[0]['active'] = True
        states_changed = True

    # Проверяем субаккаунты
    for idx in range(1, 11):
        positions = get_open_positions_safe(idx)
        if positions:
            if not states[idx]['opened']:
                print(f"На субаккаунте {idx} обнаружены открытые позиции, обновляем состояние.")
                states[idx]['opened'] = True
                states[idx]['active'] = True
                states[idx]['closed'] = False
                states_changed = True
            elif states[idx]['closed']:
                print(f"Субаккаунт {idx} имеет позиции, но помечен как закрытый. Исправляем.")
                states[idx]['closed'] = False
                states[idx]['active'] = True
                states_changed = True
            else:
                # Если есть позиции, но active=False - исправляем
                if not states[idx]['active']:
                    print(f"Субаккаунт {idx} имеет позиции, но active=False. Исправляем.")
                    states[idx]['active'] = True
                    states_changed = True
        else:
            # Если позиций нет, аккаунт не может быть открытым
            if states[idx]['opened']:
                print(f"Субаккаунт {idx} помечен как открытый, но позиций нет. Сбрасываем состояние.")
                states[idx]['opened'] = False
                states[idx]['active'] = False
                states[idx]['closed'] = False
                states[idx]['activation_level'] = 0.0
                states_changed = True

    if states_changed:
        save_cycle_state(states)
        print("Состояние синхронизировано с биржей.")

    # Обновляем балансы для всех открытых аккаунтов
    print("Обновление балансов...")
    for idx, st in enumerate(states):
        if st['opened'] and not st['closed']:
            bal = get_balance_safe(idx)
            if bal is not None:
                st['last_balance'] = bal
    save_cycle_state(states)

    # Проверяем и закрываем профитные позиции
    print("Проверка прибыльных позиций...")

    # Проверяем основной аккаунт
    if states[0]['opened'] and not states[0]['closed']:
        main_bal = states[0]['last_balance']
        main_baseline = states[0]['baseline']
        if main_baseline and main_baseline > 0:
            main_roi = (main_bal - main_baseline) / main_baseline * 100
            if main_roi >= PROFIT_MAIN_CLOSE:
                print(f"Основной аккаунт достиг +{main_roi:.2f}% (>= {PROFIT_MAIN_CLOSE}%). Закрываем.")
                close_all_positions_safe(0)
                states[0]['closed'] = True
                any_closed = True

    # Проверяем субаккаунты
    for idx in range(1, 11):
        if states[idx]['opened'] and not states[idx]['closed']:
            sub_bal = states[idx]['last_balance']
            sub_base = states[idx]['baseline']
            if sub_base and sub_base > 0:
                sub_roi = (sub_bal - sub_base) / sub_base * 100
                if sub_roi >= PROFIT_SUB_CLOSE_SELF:
                    print(f"Субаккаунт {idx} достиг +{sub_roi:.2f}% (>= {PROFIT_SUB_CLOSE_SELF}%). Закрываем.")
                    close_all_positions_safe(idx)
                    states[idx]['closed'] = True
                    states[idx]['active'] = False
                    states[idx]['opened'] = False
                    states[idx]['activation_level'] = 0.0
                    any_closed = True

    if any_closed:
        wave = states[0].get('wave', 0)
        max_open_idx = 0
        for i in range(1, 11):
            if states[i]['opened'] and not states[i]['closed']:
                max_open_idx = i

        if max_open_idx > 0:
            new_last_level = states[max_open_idx]['activation_level']
            save_cycle_info('last_activation_level', new_last_level)
            print(
                f"Обновлён last_activation_level на {new_last_level:.2f}% (уровень активации субаккаунта {max_open_idx})")
        else:
            wave_start_level = load_cycle_info('wave_start_level')
            if wave_start_level is None:
                wave_start_level = -DRAWDOWN_STEP * (wave + 1)
            save_cycle_info('last_activation_level', wave_start_level)
            print(f"Все субаккаунты закрыты. last_activation_level восстановлен до {wave_start_level:.2f}%")

        save_cycle_state(states)
        print("Прибыльные позиции закрыты.")

    # Сохраняем снимок балансов
    wave = states[0].get('wave', 0)
    current_total = sum(st['last_balance'] for st in states)
    total_roi = (current_total - baseline_total) / baseline_total * 100 if baseline_total else 0
    log_balance_snapshot(states, current_total, total_roi, wave)

    print("=== СИНХРОНИЗАЦИЯ ЗАВЕРШЕНА ===")
    return states

def main():
    init_db()
    print("Бот запущен. Режим: кросс-маржа, хеджирование.")

    # Загружаем состояние
    states = load_cycle_state()

    if states is not None:
        print("Обнаружен активный цикл. Синхронизируем с биржей...")
        baseline_total = load_cycle_info('baseline_total')

        # Если цикл активен (есть позиции), но baseline_total не задан в БД
        if baseline_total is None:
            # Проверяем, есть ли реально открытые позиции
            any_pos = False
            for idx in range(len(CONFIGS)):
                if get_open_positions_safe(idx):
                    any_pos = True
                    break

            if any_pos:
                # Фиксируем текущую сумму балансов как точку отсчета для текущего "живого" цикла
                current_live_balances = []
                for idx in range(len(CONFIGS)):
                    bal = get_balance_safe(idx)
                    current_live_balances.append(bal if bal is not None else states[idx]['last_balance'])

                baseline_total = sum(current_live_balances)
                save_cycle_info('baseline_total', baseline_total)
                print(
                    f"ВНИМАНИЕ: Обнаружены открытые позиции, но baseline_total отсутствовал. Фиксируем текущий суммарный баланс {baseline_total:.2f} USDT как точку отсчета.")
            else:
                # Если позиций нет, берем сумму из состояний (стандартное поведение)
                baseline_total = sum(st['baseline'] for st in states)

        leverages = load_cycle_info('leverages')
        if leverages is None:
            db_positions = load_positions()
            leverages = {}
            for symbol, _, _ in db_positions:
                _, _, max_lev = get_instrument_info_safe(0, symbol)
                if max_lev is not None:
                    leverages[symbol] = max_lev
            save_cycle_info('leverages', leverages)

        states = sync_positions_with_exchange(states, baseline_total, leverages)
        save_cycle_state(states)

    print_status()

    if SHOW_PLOT:
        plt.ion()
        plt.show(block=False)

    # Переменные для отслеживания статистики
    snapshot_counter = 0
    peak_balance = 0
    wave_start_balance = 0
    wave_start_time = datetime.datetime.now()
    wave_max_profit = 0
    wave_max_drawdown = 0

    while True:
        states = load_cycle_state()
        if states is None:
            print("Ожидание сигнала для нового цикла...")
            # ========== ИЗМЕНЕНИЕ: получаем раздельные списки Buy и Sell ==========
            buy_symbols, sell_symbols = get_signal(sessions[0])
            if len(buy_symbols) < TARGET_BUY or len(sell_symbols) < TARGET_SELL:
                print(f"Недостаточно сигналов: Buy={len(buy_symbols)}, Sell={len(sell_symbols)}. Ждём 60 сек...")
                time.sleep(60)
                continue

            print("Получение начальных балансов для нового цикла...")

            # Проверяем, что на аккаунтах действительно нет позиций перед фиксацией baseline
            any_pos = False
            for idx in range(len(CONFIGS)):
                if get_open_positions_safe(idx):
                    any_pos = True
                    break

            if any_pos:
                print("Обнаружены активные позиции. Ожидаем их закрытия перед началом нового цикла...")
                time.sleep(30)
                continue

            baselines = []
            for idx in range(len(CONFIGS)):
                bal = get_balance_safe(idx)
                if bal is None:
                    print(f"Не удалось получить баланс для аккаунта {idx}, прерываем")
                    break
                baselines.append(bal)
            else:
                positions = []
                leverages = {}
                opened_symbols = set()

                # Добавляем Buy-символы (ровно TARGET_BUY)
                for symbol in buy_symbols[:TARGET_BUY]:
                    if symbol in opened_symbols:
                        continue
                    qty, max_lev = calculate_qty(0, symbol)
                    if qty is None:
                        print(f"Пропускаем {symbol}: не удалось рассчитать qty")
                        continue
                    positions.append((symbol, 'Buy', qty))
                    leverages[symbol] = max_lev
                    opened_symbols.add(symbol)

                # Добавляем Sell-символы (ровно TARGET_SELL)
                for symbol in sell_symbols[:TARGET_SELL]:
                    if symbol in opened_symbols:
                        continue
                    qty, max_lev = calculate_qty(0, symbol)
                    if qty is None:
                        print(f"Пропускаем {symbol}: не удалось рассчитать qty")
                        continue
                    positions.append((symbol, 'Sell', qty))
                    leverages[symbol] = max_lev
                    opened_symbols.add(symbol)

                # Проверяем, что получилось ровно TARGET_BUY + TARGET_SELL позиций
                if len(positions) != TARGET_BUY + TARGET_SELL:
                    print(f"Не удалось сформировать нужное количество позиций: {len(positions)}. Ждём...")
                    continue

                save_positions(positions)

                init_states = []
                for idx, bal in enumerate(baselines):
                    init_states.append({
                        'baseline': bal,
                        'last_balance': bal,
                        'active': False,
                        'opened': False,
                        'closed': False,
                        'wave': 0,
                        'activation_level': 0.0
                    })
                print(f"Открываем позиции на основном аккаунте (волна 0, цель: {TARGET_BUY + TARGET_SELL})...")
                success_count = 0
                for symbol, side, qty in positions:
                    if place_order_safe(0, symbol, side, qty, leverages[symbol]):
                        success_count += 1
                    time.sleep(0.2)

                if success_count == 0:
                    print("Не удалось открыть ни одной позиции, завершаем попытку.")
                    continue

                init_states[0]['opened'] = True
                init_states[0]['active'] = True
                init_states[0]['wave'] = 0

                save_cycle_state(init_states)
                baseline_total = sum(baselines)
                save_cycle_info('baseline_total', baseline_total)
                save_cycle_info('leverages', leverages)
                save_cycle_info('last_activation_level', -DRAWDOWN_STEP)
                save_cycle_info('wave_start_level', -DRAWDOWN_STEP)
                print(f"Новый цикл запущен. Открыто {success_count} позиций. Начинаем мониторинг.")
                states = init_states

                # Сохраняем начальный снимок
                current_total = sum(st['last_balance'] for st in states)
                total_roi = (current_total - baseline_total) / baseline_total * 100 if baseline_total else 0
                log_balance_snapshot(states, current_total, total_roi, 0)
                peak_balance = current_total
                wave_start_balance = current_total
                wave_start_time = datetime.datetime.now()
                wave_max_profit = 0
                wave_max_drawdown = 0
                continue
        else:
            print("Обнаружен активный цикл. Загружаем состояние и продолжаем мониторинг.")
            states, last_activation_level, wave_start_level = fix_cycle_state(states)
            main_positions = get_open_positions_safe(0)
            if not main_positions and states[0]['opened']:
                print("На основном аккаунте нет позиций, хотя состояние говорит об открытых. Завершаем цикл.")
                clear_cycle()
                continue
            baseline_total = load_cycle_info('baseline_total')
            if baseline_total is None:
                baseline_total = sum(st['baseline'] for st in states)
            leverages = load_cycle_info('leverages')
            if leverages is None:
                db_positions = load_positions()
                leverages = {}
                for symbol, _, _ in db_positions:
                    _, _, max_lev = get_instrument_info_safe(0, symbol)
                    if max_lev is not None:
                        leverages[symbol] = max_lev
                save_cycle_info('leverages', leverages)

            # Восстанавливаем peak_balance
            conn = sqlite3.connect(DB_NAME)
            cursor = conn.cursor()
            cursor.execute("SELECT total_balance FROM balance_snapshots ORDER BY total_balance DESC LIMIT 1")
            row = cursor.fetchone()
            if row:
                peak_balance = row[0]
            conn.close()

        print("Начинаем мониторинг...")
        wave = states[0].get('wave', 0)
        last_activation_level = load_cycle_info('last_activation_level')
        wave_start_level = load_cycle_info('wave_start_level')

        if last_activation_level is None:
            max_opened = 0
            for i in range(1, 11):
                if states[i]['opened']:
                    max_opened = i
            if max_opened > 0:
                last_activation_level = -DRAWDOWN_STEP * (wave * 11 + max_opened)
            else:
                last_activation_level = -DRAWDOWN_STEP * (wave + 1)
            save_cycle_info('last_activation_level', last_activation_level)

        if wave_start_level is None:
            wave_start_level = -DRAWDOWN_STEP * (wave + 1)
            save_cycle_info('wave_start_level', wave_start_level)

        while True:
            time.sleep(10)

            # ===== ПЕРЕЗАГРУЖАЕМ СОСТОЯНИЕ ИЗ БД =====
            states = load_cycle_state()
            if states is None:
                print("Состояние не загружено, выходим из цикла.")
                break

            # Обновляем переменные
            wave = states[0].get('wave', 0)
            baseline_total = load_cycle_info('baseline_total')
            if baseline_total is None:
                baseline_total = sum(st['baseline'] for st in states)
            leverages = load_cycle_info('leverages')
            if leverages is None:
                db_positions = load_positions()
                leverages = {}
                for symbol, _, _ in db_positions:
                    _, _, max_lev = get_instrument_info_safe(0, symbol)
                    if max_lev is not None:
                        leverages[symbol] = max_lev
                save_cycle_info('leverages', leverages)
            last_activation_level = load_cycle_info('last_activation_level')
            wave_start_level = load_cycle_info('wave_start_level')
            # ==========================================

            for idx, st in enumerate(states):
                if st['opened'] and not st['closed']:
                    bal = get_balance_safe(idx)
                    if bal is not None:
                        st['last_balance'] = bal

            save_cycle_state(states)

            current_total = sum(st['last_balance'] for st in states)
            total_roi = (current_total - baseline_total) / baseline_total * 100 if baseline_total else 0

            main_bal = states[0]['last_balance']
            main_baseline = states[0]['baseline']
            main_roi = (main_bal - main_baseline) / main_baseline * 100 if main_baseline else 0

            if SHOW_PLOT:
                update_plot(main_roi, last_activation_level, wave_start_level, states)
                plt.pause(0.001)

            # Сохраняем снимки балансов каждые 10 циклов
            snapshot_counter += 1
            if snapshot_counter % 10 == 0:
                log_balance_snapshot(states, current_total, total_roi, wave)
                log_daily_stats()

                # Сохраняем ROI историю
                sub_rois = []
                for i in range(1, 11):
                    if states[i]['opened'] and not states[i]['closed']:
                        sub_base = states[i]['baseline']
                        if sub_base and sub_base > 0:
                            sub_roi = (states[i]['last_balance'] - sub_base) / sub_base * 100
                            sub_rois.append(sub_roi)

                avg_sub_roi = sum(sub_rois) / len(sub_rois) if sub_rois else 0
                max_sub_roi = max(sub_rois) if sub_rois else 0
                min_sub_roi = min(sub_rois) if sub_rois else 0
                open_positions = sum(1 for i in range(1, 11) if states[i]['opened'] and not states[i]['closed'])

                log_roi_history(total_roi, main_roi, avg_sub_roi, max_sub_roi, min_sub_roi, open_positions)

                # Сохраняем просадку
                if current_total > peak_balance:
                    peak_balance = current_total

                # Статистика текущей волны
                current_wave_profit = current_total - wave_start_balance
                if current_wave_profit > wave_max_profit:
                    wave_max_profit = current_wave_profit

                drawdown_abs = peak_balance - current_total
                if drawdown_abs > wave_max_drawdown:
                    wave_max_drawdown = drawdown_abs

                drawdown_percent = (drawdown_abs / peak_balance * 100) if peak_balance > 0 else 0
                log_drawdown(current_total, peak_balance, drawdown_percent)

            # Проверка состояния перед поиском следующего субаккаунта
            # print("DEBUG состояния субаккаунтов:")
            # for i in range(1, 11):
            #     print(
            #         f"  Суб{i}: opened={states[i]['opened']}, closed={states[i]['closed']}, active={states[i]['active']}")

            next_sub = None
            for i in range(1, 11):
                if not states[i]['opened']:
                    next_sub = i
                    break

            if next_sub is not None:
                if last_activation_level is None:
                    threshold = -DRAWDOWN_STEP * (wave + 1)
                else:
                    threshold = last_activation_level - DRAWDOWN_STEP
                pct_to_next_sub = max(0.0, main_roi - threshold)
                next_action = f"будет активирован субаккаунт {next_sub}"

                next_threshold = threshold
            else:
                if last_activation_level is None:
                    threshold = -DRAWDOWN_STEP * (wave + 1)
                else:
                    threshold = last_activation_level - DRAWDOWN_STEP
                pct_to_next_sub = max(0.0, main_roi - threshold)
                next_action = f"будет начата новая волна {wave + 1}"
                next_threshold = threshold

            if states[0]['opened'] and not states[0]['closed']:
                pct_to_close_main = max(0.0, PROFIT_MAIN_CLOSE - main_roi)
            else:
                pct_to_close_main = None

            sub_metrics = []
            for i in range(1, 11):
                if states[i]['opened'] and not states[i]['closed']:
                    sub_bal = states[i]['last_balance']
                    sub_base = states[i]['baseline']
                    if sub_base is not None and sub_base > 0:
                        sub_roi = (sub_bal - sub_base) / sub_base * 100
                        pct_to_close = max(0.0, PROFIT_SUB_CLOSE_SELF - sub_roi)
                        sub_metrics.append((i, sub_roi, pct_to_close))
                    else:
                        sub_metrics.append((i, None, None))
                else:
                    sub_metrics.append((i, None, None))

            lines = []
            target_roi_with_fees = GLOBAL_PROFIT_CLOSE + ESTIMATED_FEE_PERCENT
            lines.append(
                f"\nОбщий ROI: {total_roi:.2f}% | Цель: {target_roi_with_fees:.2f}% (чистыми {GLOBAL_PROFIT_CLOSE}%)")
            if pct_to_close_main is not None:
                lines.append(f"Основной ROI: {main_roi:.2f}% | до закрытия (вверх): {pct_to_close_main:.2f}%")
            else:
                lines.append(f"Основной ROI: {main_roi:.2f}% (закрыт)")

            lines.append(f"До активации следующего уровня (вниз): {pct_to_next_sub:.2f}%")
            lines.append(f"Следующий порог активации: {next_threshold:.2f}%")
            if pct_to_next_sub == 0.0:
                lines.append(f"   (Порог достигнут, {next_action}, волна {wave})")
            else:
                lines.append(f"   (Следующее событие: {next_action}, волна {wave})")

            lines.append("Субаккаунты:")
            for i, sub_roi, pct_to_close in sub_metrics:
                if pct_to_close is not None and sub_roi is not None:
                    wave_info = ""
                    if states[i]['opened'] and not states[i]['closed']:
                        sub_wave = states[i].get('wave', states[0].get('wave', 0))
                        wave_info = f" (волна {sub_wave})"
                    lines.append(f"  Суб{i}: ROI={sub_roi:.2f}%, до закрытия: {pct_to_close:.2f}%{wave_info}")
                else:
                    pass
                    # lines.append(f"  Суб{i}: не активен или закрыт")
            print("\n".join(lines))
            print("-" * 60)

            # ---- ОСНОВНЫЕ УСЛОВИЯ ----
            # Целевой ROI с учетом комиссий
            target_roi_with_fees = GLOBAL_PROFIT_CLOSE + ESTIMATED_FEE_PERCENT

            if total_roi >= target_roi_with_fees:
                print(
                    f"Достигнут общий ROI +{total_roi:.2f}% (цель {GLOBAL_PROFIT_CLOSE}% + {ESTIMATED_FEE_PERCENT}% комиссии). Закрываем все позиции.")
                total_trades = 0
                winning_trades = 0
                losing_trades = 0
                for idx, st in enumerate(states):
                    if st['opened'] and not st['closed']:
                        profit, trades, wins, losses = close_all_positions_safe(idx)
                        st['closed'] = True
                        total_trades += trades
                        winning_trades += wins
                        losing_trades += losses

                # Сохраняем результат волны
                wave_end_time = datetime.datetime.now()
                log_wave_result(wave, wave_start_time.isoformat(), wave_end_time.isoformat(),
                                wave_start_balance, current_total, total_trades, winning_trades, losing_trades,
                                wave_max_drawdown, wave_max_profit)

                save_cycle_state(states)
                print_all_balances(states)

                # Выводим аналитику
                print_analytics()

                clear_cycle()
                print("Цикл завершён по глобальному ROI.")
                break

            if main_roi >= PROFIT_MAIN_CLOSE and not states[0]['closed']:
                print(f"Основной аккаунт достиг +{PROFIT_MAIN_CLOSE}%. Закрываем его позиции.")
                profit, trades, wins, losses = close_all_positions_safe(0)
                states[0]['closed'] = True
                save_cycle_state(states)
                print_all_balances(states)
                any_sub_open = any(st['opened'] and not st['closed'] for idx, st in enumerate(states) if 1 <= idx <= 10)
                if not any_sub_open:
                    # Сохраняем результат волны
                    wave_end_time = datetime.datetime.now()
                    log_wave_result(wave, wave_start_time.isoformat(), wave_end_time.isoformat(),
                                    wave_start_balance, current_total, trades, wins, losses,
                                    wave_max_drawdown, wave_max_profit)
                    print_analytics()
                    clear_cycle()
                    print("Все позиции закрыты. Цикл завершён.")
                    break
                else:
                    continue

            # ---- ЗАКРЫВАЕМ ПРОФИТНЫЕ СУБАККАУНТЫ ----
            for i in range(1, 11):
                if not (states[i]['active'] and states[i]['opened'] and not states[i]['closed']):
                    continue
                sub_bal = states[i]['last_balance']
                sub_base = states[i]['baseline']
                if sub_base is None or sub_base == 0:
                    continue
                sub_roi = (sub_bal - sub_base) / sub_base * 100
                if sub_roi >= PROFIT_SUB_CLOSE_SELF:
                    print(f"Субаккаунт {i} закрывается: его собственный ROI достиг +{sub_roi:.2f}%")
                    profit, trades, wins, losses = close_all_positions_safe(i)
                    states[i]['closed'] = True
                    states[i]['active'] = False
                    states[i]['opened'] = False
                    states[i]['activation_level'] = 0.0
                    save_cycle_state(states)

                    max_open_idx = 0
                    for j in range(1, 11):
                        if states[j]['opened'] and not states[j]['closed']:
                            max_open_idx = j

                    if max_open_idx > 0:
                        last_activation_level = states[max_open_idx]['activation_level']
                        save_cycle_info('last_activation_level', last_activation_level)
                        print(
                            f"Обновлён last_activation_level на {last_activation_level:.2f}% (уровень активации субаккаунта {max_open_idx})")
                    else:
                        last_activation_level = wave_start_level
                        save_cycle_info('last_activation_level', last_activation_level)
                        print(
                            f"Все субаккаунты закрыты. last_activation_level восстановлен до {last_activation_level:.2f}% (уровень начала волны)")
            # ---- АКТИВАЦИЯ НОВЫХ УРОВНЕЙ И ВОЛН ----
            if not states[0]['closed']:
                all_subs_opened = all(st['opened'] for idx, st in enumerate(states) if 1 <= idx <= 10)

                if all_subs_opened:
                    if last_activation_level is not None:
                        if main_roi <= last_activation_level - DRAWDOWN_STEP:
                            wave += 1
                            states[0]['wave'] = wave
                            # ... сохранение результатов волны ...
                            print(f"Начинаем волну {wave}: открываем новые позиции на всех аккаунтах...")

                            # ---- ОБНОВЛЯЕМ BASELINE ДЛЯ ОСНОВНОГО АККАУНТА ----
                            bal_main = get_balance_safe(0)
                            if bal_main is not None:
                                states[0]['baseline'] = bal_main
                                states[0]['last_balance'] = bal_main
                                print(f"Основной: baseline обновлён на {bal_main:.2f} USDT")

                            db_positions = load_positions()
                            print(f"Открываем позиции на основном аккаунте для волны {wave}...")
                            for symbol, side, qty in db_positions:
                                if symbol in leverages:
                                    place_order_safe(0, symbol, side, qty, leverages[symbol])

                            print(f"Открываем позиции на всех субаккаунтах для волны {wave}...")
                            for idx in range(1, 11):
                                # ---- ОБНОВЛЯЕМ BASELINE ДЛЯ СУБАККАУНТА ----
                                bal_sub = get_balance_safe(idx)
                                if bal_sub is not None:
                                    states[idx]['baseline'] = bal_sub
                                    states[idx]['last_balance'] = bal_sub
                                    print(f"Субаккаунт {idx}: baseline обновлён на {bal_sub:.2f} USDT")

                                if states[idx]['closed']:
                                    states[idx]['closed'] = False
                                    states[idx]['active'] = False
                                    states[idx]['opened'] = False
                                    states[idx]['activation_level'] = 0.0
                                for symbol, side, qty in db_positions:
                                    if symbol in leverages:
                                        place_order_safe(idx, symbol, side, qty, leverages[symbol])
                                states[idx]['active'] = True
                                states[idx]['opened'] = True
                                if states[idx]['activation_level'] == 0.0:
                                    states[idx]['activation_level'] = main_roi
                                states[idx]['wave'] = wave

                            last_activation_level = main_roi
                            wave_start_level = main_roi
                            save_cycle_info('last_activation_level', last_activation_level)
                            save_cycle_info('wave_start_level', wave_start_level)
                            save_cycle_state(states)
                            continue
                else:
                    for i in range(1, 11):
                        if not states[i]['opened']:
                            if last_activation_level is None:
                                threshold = -DRAWDOWN_STEP * (wave + 1)
                            else:
                                threshold = last_activation_level - DRAWDOWN_STEP
                            if main_roi <= threshold:
                                if last_activation_level is not None:
                                    step_from = last_activation_level
                                    print(
                                        f"Активация субаккаунта {i} при просадке {main_roi:.1f}% (шаг от {step_from:.1f}%)")
                                else:
                                    print(
                                        f"Активация субаккаунта {i} при просадке {main_roi:.1f}% (начало волны {wave})")

                                # ---- ОБНОВЛЯЕМ BASELINE НА ТЕКУЩИЙ БАЛАНС ----
                                bal = get_balance_safe(i)
                                if bal is not None:
                                    states[i]['baseline'] = bal
                                    states[i]['last_balance'] = bal
                                    print(f"Субаккаунт {i}: baseline обновлён на {bal:.2f} USDT")

                                states[i]['active'] = True
                                db_positions = load_positions()
                                for symbol, side, qty in db_positions:
                                    if symbol in leverages:
                                        place_order_safe(i, symbol, side, qty, leverages[symbol])
                                states[i]['opened'] = True
                                states[i]['activation_level'] = main_roi
                                states[i]['wave'] = wave
                                states[i]['closed'] = False
                                save_cycle_state(states)
                                last_activation_level = main_roi
                                save_cycle_info('last_activation_level', last_activation_level)
                                print_all_balances(states)
                                break

            if states[0]['closed']:
                any_sub_open = any(st['opened'] for idx, st in enumerate(states) if 1 <= idx <= 10)
                if not any_sub_open:
                    wave_end_time = datetime.datetime.now()
                    log_wave_result(wave, wave_start_time.isoformat(), wave_end_time.isoformat(),
                                    wave_start_balance, current_total, 0, 0, 0,
                                    wave_max_drawdown, wave_max_profit)
                    print_analytics()
                    clear_cycle()
                    print("Все позиции закрыты. Цикл завершён.")
                    break

        print("Ожидание нового сигнала...")


if __name__ == "__main__":
    main()
