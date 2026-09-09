import os
import sqlite3
import time
import uuid
import threading
import math
from datetime import datetime, timezone, timedelta
from decimal import Decimal, ROUND_DOWN, getcontext
from pybit.unified_trading import HTTP
from api_config import DEMO_CONFIG,DEMO_CONFIG1,DEMO_CONFIG2,DEMO_CONFIG3,DEMO_CONFIG4,REAL_CONFIG, REAL_CONFIG1, REAL_CONFIG2, REAL_CONFIG3, REAL_CONFIG4, CONFIG, CONFIG0, CONFIG1, CONFIG2, CONFIG3, CONFIG4, api_t, chat
import requests
import json
import numpy as np
from scipy import stats
import math
from typing import Dict

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import shutil
from pathlib import Path

d = DEMO_CONFIG
d1 = DEMO_CONFIG1
d2 = DEMO_CONFIG2
d3 = DEMO_CONFIG3
d4 = DEMO_CONFIG4

CONFIGS=[d, d1, d2, d3, d4]



SETUP0 = [False,0.005,0.015]
SETUP1 = [True,0.005,0.015]
SETUP2 = [False,0.015,0.005]
SETUP3 = [True,0.015,0.005]

STEP_PLUS = 0.25

SETUP4 = []


getcontext().prec = 28

DB_FILE = 'funding_earning.db'
ID_MSG_UPDATE = 'msg.txt'

prepared_trades_for_hour = []
prepared_hour = None

# ПАРАМЕТРЫ СТРАТЕГИИ
XXX = 1
XXX_dop_1 = 1
XXX_dop_2 = 1
XXX_dop_3 = 1
XXX_dop_4 = 1
START_DEP = 1000
MIN_ABS_RATE = 0.10
MAX_ABS_RATE = 0.50

LEVEL_POS = 2

proc_reopen = 0.015

RISK_SYMBOL_USD = (START_DEP*0.2) * (0.2)

# Хранилище прокси для разных потоков
thread_local = threading.local()

import sqlite3
import pandas as pd
import numpy as np



# ===============================
# Метод 1: по первой и последней точке (сложный процент)
# ===============================
def get_method1_results(current_balance, db_file='funding_earning.db'):
    """
    Рассчитывает прогноз по первой и последней записи баланса.
    Возвращает словарь с ключами:
        - days_elapsed (float)
        - daily_rate (float) – среднедневная сложная ставка
        - forecast_days (dict) – {target: days}
        - current_balance (float)
    """
    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    c.execute("SELECT date_ms, wallet FROM live_akk_usdt ORDER BY date_ms ASC")
    row = c.fetchone()
    conn.close()
    if row is None:
        raise ValueError("Нет записей в таблице live_akk_usdt")

    start_ms = row[0]
    start_balance = float(row[1])

    end_ms = int(datetime.now().timestamp() * 1000)
    if end_ms <= start_ms:
        raise ValueError("Время окончания должно быть позже времени начала")

    days = (end_ms - start_ms) / (1000 * 60 * 60 * 24)
    if days <= 0:
        raise ValueError("Некорректное количество дней")

    total_return = current_balance / start_balance
    daily_rate = total_return ** (1 / days) - 1

    targets = [5000, 10000, 100000]
    forecast_days = {}
    for target in targets:
        if target <= current_balance:
            forecast_days[target] = 0.0
        else:
            if daily_rate <= 0:
                forecast_days[target] = float('inf')
            else:
                forecast_days[target] = math.log(target / current_balance) / math.log(1 + daily_rate)

    return {
        'days_elapsed': days,
        'daily_rate': daily_rate,
        'forecast_days': forecast_days,
        'current_balance': current_balance,
        'method': 'first_last'
    }

# ===============================
# Метод 3: экспоненциальная регрессия (логарифм баланса)
# ===============================

def calculate_growth_exp_regression_new2_all(current_balance, db_file='funding_earning.db'):
    # Получаем данные из БД
    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    c.execute("SELECT date_ms, wallet, ballance FROM live_akk_usdt ORDER BY date_ms ASC")
    rows = c.fetchall()
    conn.close()

    if len(rows) < 2:
        raise ValueError("Недостаточно данных для регрессии (минимум 2 записи)")

    # --- НАХОДИМ ПОСЛЕДНЕЕ ПОПОЛНЕНИЕ ---
    last_deposit_idx = 0
    deposit_threshold = 50  # Минимальная сумма пополнения для определения
    
    for i in range(1, len(rows)):
        prev_balance = float(rows[i-1][1])  # wallet
        curr_balance = float(rows[i][1])    # wallet
        if curr_balance - prev_balance > deposit_threshold:
            last_deposit_idx = i
    
    # --- ДАННЫЕ ПОСЛЕ ПОСЛЕДНЕГО ПОПОЛНЕНИЯ ---
    start_ms_after = rows[last_deposit_idx][0]
    start_balance_after = float(rows[last_deposit_idx][1])
    start_money_after = round(start_balance_after, 2)
    start_day_after = datetime.fromtimestamp(start_ms_after / 1000).strftime("%d.%m.%Y")

    # --- ДАННЫЕ ЗА ВЕСЬ ПЕРИОД ---
    start_ms_all = rows[0][0]
    start_balance_all = float(rows[0][1])
    start_money_all = round(start_balance_all, 2)
    start_day_all = datetime.fromtimestamp(start_ms_all / 1000).strftime("%d.%m.%Y")

    # Текущее время и дата
    end_ms = int(datetime.now().timestamp() * 1000)
    end_day = datetime.fromtimestamp(end_ms / 1000).strftime("%d.%m.%Y")
    end_money = current_balance

    # --- Подготовка данных для регрессии (после пополнения) ---
    data_after = []
    for date_ms, wallet, ballance in rows[last_deposit_idx:]:
        if wallet is None or wallet <= 0:
            continue
        days = (date_ms - start_ms_after) / (1000 * 60 * 60 * 24)
        data_after.append((days, float(wallet)))

    if len(data_after) < 2:
        raise ValueError("Нет корректных данных баланса после последнего пополнения")

    days_after = np.array([d for d, _ in data_after])
    log_balance_after = np.log([b for _, b in data_after])

    # Линейная регрессия на логарифме (после пополнения)
    slope_after, intercept_after, r_value_after, p_value_after, std_err_after = stats.linregress(days_after, log_balance_after)
    r_squared_after = r_value_after ** 2

    # --- Подготовка данных для регрессии (за весь период) ---
    data_all = []
    for date_ms, wallet, ballance in rows:
        if wallet is None or wallet <= 0:
            continue
        days = (date_ms - start_ms_all) / (1000 * 60 * 60 * 24)
        data_all.append((days, float(wallet)))

    days_all = np.array([d for d, _ in data_all])
    log_balance_all = np.log([b for _, b in data_all])

    # Линейная регрессия на логарифме (за весь период)
    slope_all, intercept_all, r_value_all, p_value_all, std_err_all = stats.linregress(days_all, log_balance_all)
    r_squared_all = r_value_all ** 2

    # --- РАСЧЁТЫ ПОСЛЕ ПОСЛЕДНЕГО ПОПОЛНЕНИЯ ---
    days_elapsed_after = (end_ms - start_ms_after) / (1000 * 60 * 60 * 24)
    hours_elapsed_after = days_elapsed_after * 24
    gain_after = current_balance - start_balance_after
    total_gain_percent_after = (current_balance / start_balance_after - 1) * 100
    daily_abs_gain_after = gain_after / days_elapsed_after if days_elapsed_after > 0 else 0
    hourly_abs_gain_after = daily_abs_gain_after / 24 if days_elapsed_after > 0 else 0
    daily_rate_eff_after = math.exp(slope_after) - 1
    daily_gain_percent_after = daily_rate_eff_after * 100
    hourly_rate_eff_after = math.exp(slope_after / 24) - 1
    hourly_gain_percent_after = hourly_rate_eff_after * 100

    # --- РАСЧЁТЫ ЗА ВЕСЬ ПЕРИОД ---
    days_elapsed_all = (end_ms - start_ms_all) / (1000 * 60 * 60 * 24)
    hours_elapsed_all = days_elapsed_all * 24
    gain_all = current_balance - start_balance_all
    total_gain_percent_all = (current_balance / start_balance_all - 1) * 100
    daily_abs_gain_all = gain_all / days_elapsed_all if days_elapsed_all > 0 else 0
    hourly_abs_gain_all = daily_abs_gain_all / 24 if days_elapsed_all > 0 else 0
    daily_rate_eff_all = math.exp(slope_all) - 1
    daily_gain_percent_all = daily_rate_eff_all * 100
    hourly_rate_eff_all = math.exp(slope_all / 24) - 1
    hourly_gain_percent_all = hourly_rate_eff_all * 100

    # --- ПРОГНОЗЫ (используем данные после пополнения для прогнозов) ---
    targets = [5000, 10000, 100000]
    forecast_days = {}
    if slope_after > 0:
        for target in targets:
            if target <= current_balance:
                forecast_days[target] = 0.0
            else:
                forecast_days[target] = math.log(target / current_balance) / slope_after
    else:
        for target in targets:
            forecast_days[target] = float('inf') if target > current_balance else 0.0

    # --- ДОХОД 100$ (на основе данных после пополнения) ---
    days_to_100_daily_profit = 0.0
    balance_at_100_daily = 0.0
    
    if slope_after > 0 and daily_rate_eff_after > 0:
        if current_balance * daily_rate_eff_after >= 100:
            days_to_100_daily_profit = 0.0
            balance_at_100_daily = current_balance
        else:
            needed_ratio = 100.0 / (current_balance * daily_rate_eff_after)
            if needed_ratio > 0:
                days_to_100_daily_profit = math.log(needed_ratio) / slope_after
                if days_to_100_daily_profit < 0:
                    days_to_100_daily_profit = 0.0
                balance_at_100_daily = current_balance * math.exp(slope_after * days_to_100_daily_profit)
            else:
                days_to_100_daily_profit = float('inf')
                balance_at_100_daily = float('inf')
    else:
        days_to_100_daily_profit = float('inf')
        balance_at_100_daily = float('inf')

    # --- ФОРМИРУЕМ ВЫВОД ---
    lines = [
        f"\nРассчетных точек: {len(data_after)} шт. (после пополнения)",
        f'Баланс (start): {start_money_after}$ ({start_day_after})',
        f'Баланс (live):  {end_money}$ ({end_day})',
        "\n--- После последнего пополнения баланса ---",
        f"Прошло дней: {days_elapsed_after:.2f}",
        f"Прошло часов: {hours_elapsed_after:.2f}",
        f"Абсолютный прирост: {gain_after:.2f} $",
        f"Прирост: {total_gain_percent_after:.2f}%",
        "\n--- Скорость роста (после пополнения) ---",
        f"В день: +{daily_abs_gain_after:.4f} $  ({daily_gain_percent_after:.4f}%)",
        f"В час:  +{hourly_abs_gain_after:.4f} $  ({hourly_gain_percent_after:.4f}%)",
        "\n--- За весь период работы ---",
        f"Прошло дней: {days_elapsed_all:.2f}",
        f"Прошло часов: {hours_elapsed_all:.2f}",
        f"Абсолютный прирост: {gain_all:.2f} $",
        f"Прирост: {total_gain_percent_all:.2f}%",
        "\n--- Скорость роста (за весь период) ---",
        f"В день: +{daily_abs_gain_all:.4f} $  ({daily_gain_percent_all:.4f}%)",
        f"В час:  +{hourly_abs_gain_all:.4f} $  ({hourly_gain_percent_all:.4f}%)",
        "\n--- До целевых сумм (в днях) ---"
    ]

    usdt_1000 = 0.0
    usdt_10000 = 0.0
    usdt_100000 = 0.0
    
    for target, days_needed in forecast_days.items():
        if math.isinf(days_needed):
            lines.append(f"До {target} $: недостижимо при текущей скорости")
        else:
            if target == 1000:
                usdt_1000 = days_needed
            elif target == 10000:
                usdt_10000 = days_needed
            elif target == 100000:
                usdt_100000 = days_needed
            lines.append(f"До {target} $: {days_needed:.1f} дней")

    # Ежедневный доход > 100 $
    if not math.isinf(days_to_100_daily_profit):
        lines.append(f"\n--- Дополнительный прогноз ---")
        lines.append(f"Ежедневно > 100 $ через: {days_to_100_daily_profit:.1f} дней (баланс: {balance_at_100_daily:.0f}$)")

    # Информация о пополнении
    if last_deposit_idx > 0:
        deposit_amount = round(float(rows[last_deposit_idx][1]) - float(rows[last_deposit_idx-1][1]), 2)
        lines.append(f"\n💰 Пополнение: +{deposit_amount:.2f}$ ({start_day_after})")

    out_metric = "\n".join(lines)
    print(out_metric)
    return out_metric

def calculate_growth_exp_regression_new2(current_balance, db_file='funding_earning.db'):
    """
    Расчет скорости роста на основе медианного дневного прироста по точкам после последнего пополнения.
    """
    import sqlite3
    import numpy as np
    from scipy import stats
    import math
    from datetime import datetime

    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    c.execute("""
        SELECT date_ms, ballance, ballance1, ballance2, ballance3, ballance4
        FROM live_akk_usdt 
        ORDER BY date_ms ASC
    """)
    rows = c.fetchall()
    conn.close()

    if len(rows) < 2:
        raise ValueError("Недостаточно данных")

    DEPOSIT_THRESHOLD = 300.0
    MIN_POINTS_AFTER_DEPOSIT = 50
    MAX_DAILY_PERCENT = 20.0  # ограничение на максимальный дневной процент (можно убрать или увеличить)

    deposits = []
    prev_total = None

    for i, row in enumerate(rows):
        total = sum(float(x) for x in row[1:] if x is not None)
        total = round(total, 2)
        if prev_total is not None:
            diff = total - prev_total
            if diff > DEPOSIT_THRESHOLD:
                date_str = datetime.fromtimestamp(row[0] / 1000).strftime("%d.%m.%Y %H:%M")
                deposits.append({
                    'index': i,
                    'date_ms': row[0],
                    'amount': diff,
                    'total_after': total,
                    'date_str': date_str
                })
                print(f"🔍 Пополнение #{len(deposits)}: +{diff:.2f}$ в {date_str} (суммарно: {total:.2f})")
        prev_total = total

    # Определяем стартовую точку
    if deposits:
        last = deposits[-1]
        points_after = len(rows) - last['index'] - 1

        if points_after >= MIN_POINTS_AFTER_DEPOSIT:
            start_idx = last['index']
            start_total = last['total_after']
            start_date = last['date_str']
            deposit_amount = last['amount']
            print(f"\n✅ ПОСЛЕДНЕЕ ПОПОЛНЕНИЕ: +{deposit_amount:.2f}$ от {start_date} (точек после: {points_after})")
        else:
            if len(deposits) >= 2:
                prev = deposits[-2]
                start_idx = prev['index']
                start_total = prev['total_after']
                start_date = prev['date_str']
                deposit_amount = prev['amount']
                print(f"\n⚠️ Данных после последнего пополнения мало ({points_after}), используем предыдущее пополнение от {start_date}")
            else:
                start_idx = 0
                start_total = sum(float(x) for x in rows[0][1:] if x is not None)
                start_date = datetime.fromtimestamp(rows[0][0] / 1000).strftime("%d.%m.%Y")
                deposit_amount = 0
                print("\n⚠️ Данных после пополнения мало, используем начало истории")
    else:
        start_idx = 0
        start_total = sum(float(x) for x in rows[0][1:] if x is not None)
        start_date = datetime.fromtimestamp(rows[0][0] / 1000).strftime("%d.%m.%Y")
        deposit_amount = 0
        print("⚠️ Пополнений не найдено, старт с первой записи")

    # Текущий суммарный баланс
    last_row = rows[-1]
    #current_total = sum(float(x) for x in last_row[1:] if x is not None)
    current_total = round(current_balance, 2)

    # Данные для регрессии (только для R², сама скорость не используется)
    data = []
    start_ms = rows[start_idx][0]
    for row in rows[start_idx:]:
        ts = row[0]
        total = sum(float(x) for x in row[1:] if x is not None)
        if total <= 0:
            continue
        days = (ts - start_ms) / (1000 * 60 * 60 * 24)
        data.append((days, total))

    if len(data) < 2:
        raise ValueError("Недостаточно данных для регрессии")

    days = np.array([d for d, _ in data])
    log_balance = np.log([b for _, b in data])
    slope, intercept, r_value, _, _ = stats.linregress(days, log_balance)
    r_squared = r_value ** 2

    days_elapsed = (rows[-1][0] - start_ms) / (1000 * 60 * 60 * 24)
    hours_elapsed = days_elapsed * 24

    gain = current_total - start_total
    total_gain_percent = (current_total / start_total - 1) * 100 if start_total > 0 else 0

    # --- ВЫЧИСЛЕНИЕ МЕДИАННОГО ДНЕВНОГО ПРИРОСТА ПО ВСЕМ ТОЧКАМ ---
    daily_gains = []
    for i in range(1, len(data)):
        days_diff = data[i][0] - data[i-1][0]
        if days_diff > 0:
            gain_pct = (data[i][1] / data[i-1][1] - 1) / days_diff * 100
            daily_gains.append(gain_pct)

    if daily_gains:
        median_daily_pct = np.median(daily_gains)
        # Ограничиваем, чтобы не было абсурдных значений
        if median_daily_pct > MAX_DAILY_PERCENT:
            print(f"⚠️ Медианный дневной прирост ({median_daily_pct:.2f}%) ограничен до {MAX_DAILY_PERCENT}%")
            median_daily_pct = MAX_DAILY_PERCENT
        daily_percent = median_daily_pct
        daily_rate = daily_percent / 100.0
        daily_abs = current_total * daily_rate
        hourly_rate = (1 + daily_rate) ** (1/24) - 1
        hourly_percent = hourly_rate * 100
        hourly_abs = current_total * hourly_rate
    else:
        # fallback – если не удалось вычислить медиану (мало данных)
        daily_rate = math.exp(slope) - 1
        daily_percent = daily_rate * 100
        daily_abs = current_total * daily_rate
        hourly_rate = math.exp(slope / 24) - 1
        hourly_percent = hourly_rate * 100
        hourly_abs = current_total * hourly_rate

    # --- ПРОГНОЗЫ ДО ЦЕЛЕЙ (по медианной ставке) ---
    targets = [5000, 10000, 100000]
    forecast = {}
    if daily_rate > 0:
        for t in targets:
            if t <= current_total:
                forecast[t] = 0.0
            else:
                forecast[t] = math.log(t / current_total) / math.log(1 + daily_rate)
    else:
        for t in targets:
            forecast[t] = float('inf') if t > current_total else 0.0

    # --- ДНИ ДО ДОХОДА >100$ В ДЕНЬ ---
    days_to_100 = float('inf')
    balance_at_100 = float('inf')
    if daily_rate > 0:
        if current_total * daily_rate >= 100:
            days_to_100 = 0.0
            balance_at_100 = current_total
        else:
            needed_ratio = 100.0 / (current_total * daily_rate)
            if needed_ratio > 0:
                days_to_100 = math.log(needed_ratio) / math.log(1 + daily_rate)
                if days_to_100 < 0:
                    days_to_100 = 0.0
                balance_at_100 = current_total * (1 + daily_rate) ** days_to_100
            else:
                days_to_100 = float('inf')
                balance_at_100 = float('inf')

    # Запись в БД
    try:
        write_prognoze(
            len(data),
            start_total,
            current_total,
            days_elapsed,
            hours_elapsed,
            gain,
            total_gain_percent,
            daily_abs,
            daily_percent,
            hourly_abs,
            hourly_percent,
            forecast.get(5000, 0.0),
            forecast.get(10000, 0.0),
            forecast.get(100000, 0.0),
            days_to_100 if not math.isinf(days_to_100) else 0.0
        )
    except Exception as e:
        print(f"⚠️ Ошибка записи в БД: {e}")

    # Вывод
    lines = [
        f"\n📊 Точек: {len(data)}",
        f"📅 Старт: {start_total:.2f}$ ({start_date})",
        f"📅 Сейчас: {current_total:.2f}$ ({datetime.now().strftime('%d.%m.%Y %H:%M')})",
        f"⏱ Прошло дней: {days_elapsed:.2f}",
        f"⏱ Прошло часов: {hours_elapsed:.2f}",
        f"💰 Прирост: {gain:.2f}$ ({total_gain_percent:.2f}%)",
        f"📊 R² = {r_squared:.4f}",
        "\n🚀 Скорость роста (медианная по точкам)",
        f"В день: +{daily_abs:.4f}$ ({daily_percent:.4f}%)",
        f"В час:  +{hourly_abs:.4f}$ ({hourly_percent:.4f}%)",
        "\n🎯 До целей (дней):"
    ]
    for t, d in forecast.items():
        if math.isinf(d):
            lines.append(f"  {t}$: недостижимо")
        else:
            lines.append(f"  {t}$: {d:.1f}")

    if not math.isinf(days_to_100):
        lines.append(f"\n💰 Дохода >100$/день через {days_to_100:.1f} дн. (баланс {balance_at_100:.0f}$)")

    if deposit_amount > 0:
        lines.append(f"\n💰 ПОПОЛНЕНИЕ: +{deposit_amount:.2f}$ ({start_date})")

    out = "\n".join(lines)
    print(out)
    return out

def calculate_growth_exp_regression_new3(current_balance, db_file='funding_earning.db'):
    """
    Расчет скорости роста на основе общего прироста за период после последнего пополнения.
    """
    import sqlite3
    import numpy as np
    from scipy import stats
    import math
    from datetime import datetime

    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    c.execute("""
        SELECT date_ms, ballance, ballance1, ballance2, ballance3, ballance4
        FROM live_akk_usdt 
        ORDER BY date_ms ASC
    """)
    rows = c.fetchall()
    conn.close()

    if len(rows) < 2:
        raise ValueError("Недостаточно данных")

    DEPOSIT_THRESHOLD = 300.0
    MIN_POINTS_AFTER_DEPOSIT = 50

    deposits = []
    prev_total = None

    for i, row in enumerate(rows):
        total = sum(float(x) for x in row[1:] if x is not None)
        total = round(total, 2)
        if prev_total is not None:
            diff = total - prev_total
            if diff > DEPOSIT_THRESHOLD:
                date_str = datetime.fromtimestamp(row[0] / 1000).strftime("%d.%m.%Y %H:%M")
                deposits.append({
                    'index': i,
                    'date_ms': row[0],
                    'amount': diff,
                    'total_after': total,
                    'date_str': date_str
                })
                print(f"🔍 Пополнение #{len(deposits)}: +{diff:.2f}$ в {date_str} (суммарно: {total:.2f})")
        prev_total = total

    # Определяем стартовую точку (последнее пополнение или начало истории)
    if deposits:
        last = deposits[-1]
        points_after = len(rows) - last['index'] - 1

        if points_after >= MIN_POINTS_AFTER_DEPOSIT:
            start_idx = last['index']
            start_total = last['total_after']
            start_date = last['date_str']
            deposit_amount = last['amount']
            print(f"\n✅ ПОСЛЕДНЕЕ ПОПОЛНЕНИЕ: +{deposit_amount:.2f}$ от {start_date} (точек после: {points_after})")
        else:
            if len(deposits) >= 2:
                prev = deposits[-2]
                start_idx = prev['index']
                start_total = prev['total_after']
                start_date = prev['date_str']
                deposit_amount = prev['amount']
                print(f"\n⚠️ Данных после последнего пополнения мало ({points_after}), используем предыдущее пополнение от {start_date}")
            else:
                start_idx = 0
                start_total = sum(float(x) for x in rows[0][1:] if x is not None)
                start_date = datetime.fromtimestamp(rows[0][0] / 1000).strftime("%d.%m.%Y")
                deposit_amount = 0
                print("\n⚠️ Данных после пополнения мало, используем начало истории")
    else:
        start_idx = 0
        start_total = sum(float(x) for x in rows[0][1:] if x is not None)
        start_date = datetime.fromtimestamp(rows[0][0] / 1000).strftime("%d.%m.%Y")
        deposit_amount = 0
        print("⚠️ Пополнений не найдено, старт с первой записи")

    # Текущий суммарный баланс
    current_total = round(current_balance, 2)

    # Данные для регрессии (только для R²)
    data = []
    start_ms = rows[start_idx][0]
    for row in rows[start_idx:]:
        ts = row[0]
        total = sum(float(x) for x in row[1:] if x is not None)
        if total <= 0:
            continue
        days = (ts - start_ms) / (1000 * 60 * 60 * 24)
        data.append((days, total))

    if len(data) < 2:
        raise ValueError("Недостаточно данных для регрессии")

    days = np.array([d for d, _ in data])
    log_balance = np.log([b for _, b in data])
    slope, intercept, r_value, _, _ = stats.linregress(days, log_balance)
    r_squared = r_value ** 2

    days_elapsed = (rows[-1][0] - start_ms) / (1000 * 60 * 60 * 24)
    hours_elapsed = days_elapsed * 24

    gain = current_total - start_total
    total_gain_percent = (current_total / start_total - 1) * 100 if start_total > 0 else 0

    # --- ОСНОВНАЯ СКОРОСТЬ: геометрическая средняя за период ---
    if days_elapsed > 0:
        daily_rate = (current_total / start_total) ** (1 / days_elapsed) - 1
    else:
        daily_rate = 0.0

    daily_percent = daily_rate * 100
    daily_abs = current_total * daily_rate
    hourly_rate = (1 + daily_rate) ** (1/24) - 1
    hourly_percent = hourly_rate * 100
    hourly_abs = current_total * hourly_rate

    # --- ПРОГНОЗЫ ДО ЦЕЛЕЙ (по геометрической ставке) ---
    targets = [5000, 10000, 100000]
    forecast = {}
    if daily_rate > 0:
        for t in targets:
            if t <= current_total:
                forecast[t] = 0.0
            else:
                forecast[t] = math.log(t / current_total) / math.log(1 + daily_rate)
    else:
        for t in targets:
            forecast[t] = float('inf') if t > current_total else 0.0

    # --- ДНИ ДО ДОХОДА >100$ В ДЕНЬ ---
    days_to_100 = float('inf')
    balance_at_100 = float('inf')
    if daily_rate > 0:
        if current_total * daily_rate >= 100:
            days_to_100 = 0.0
            balance_at_100 = current_total
        else:
            needed_ratio = 100.0 / (current_total * daily_rate)
            if needed_ratio > 0:
                days_to_100 = math.log(needed_ratio) / math.log(1 + daily_rate)
                if days_to_100 < 0:
                    days_to_100 = 0.0
                balance_at_100 = current_total * (1 + daily_rate) ** days_to_100
            else:
                days_to_100 = float('inf')
                balance_at_100 = float('inf')

    # Запись в БД
    try:
        write_prognoze(
            len(data),
            start_total,
            current_total,
            days_elapsed,
            hours_elapsed,
            gain,
            total_gain_percent,
            daily_abs,
            daily_percent,
            hourly_abs,
            hourly_percent,
            forecast.get(5000, 0.0),
            forecast.get(10000, 0.0),
            forecast.get(100000, 0.0),
            days_to_100 if not math.isinf(days_to_100) else 0.0
        )
    except Exception as e:
        print(f"⚠️ Ошибка записи в БД: {e}")

    # Вывод
    lines = [
        f"\n📊 Точек: {len(data)}",
        f"📅 Старт: {start_total:.2f}$ ({start_date})",
        f"📅 Сейчас: {current_total:.2f}$ ({datetime.now().strftime('%d.%m.%Y %H:%M')})",
        f"⏱ Прошло дней: {days_elapsed:.2f}",
        f"⏱ Прошло часов: {hours_elapsed:.2f}",
        f"💰 Прирост: {gain:.2f}$ ({total_gain_percent:.2f}%)",
        f"📊 R² = {r_squared:.4f}",
        "\n🚀 Скорость роста (средняя за период)",
        f"В день: +{daily_abs:.4f}$ ({daily_percent:.4f}%)",
        f"В час:  +{hourly_abs:.4f}$ ({hourly_percent:.4f}%)",
        "\n🎯 До целей (дней):"
    ]
    for t, d in forecast.items():
        if math.isinf(d):
            lines.append(f"  {t}$: недостижимо")
        else:
            lines.append(f"  {t}$: {d:.1f}")

    if not math.isinf(days_to_100):
        lines.append(f"\n💰 Дохода >100$/день через {days_to_100:.1f} дн. (баланс {balance_at_100:.0f}$)")

    if deposit_amount > 0:
        lines.append(f"\n💰 ПОПОЛНЕНИЕ: +{deposit_amount:.2f}$ ({start_date})")

    out = "\n".join(lines)
    print(out)
    return out

def calculate_growth_exp_regression(current_balance, db_file='funding_earning.db'):
    """
    Выполняет линейную регрессию на логарифме баланса по всем точкам.
    Возвращает словарь:
        - slope_log (float) – среднедневной логарифмический прирост
        - r_squared (float) – коэффициент детерминации
        - forecast_days (dict) – {target: days}
        - days_elapsed (float) – дней от первой записи до последней
        - slope_abs (float) – скорость в $/день (линейная регрессия)
        - current_balance (float)
        - data_points (int)
    """
    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    c.execute("SELECT date_ms, wallet FROM live_akk_usdt ORDER BY date_ms ASC")
    rows = c.fetchall()
    conn.close()

    if len(rows) < 2:
        raise ValueError("Недостаточно данных для регрессии (минимум 2 записи)")

    start_ms = rows[0][0]
    data = []
    for date_ms, wallet in rows:
        if wallet is None or wallet <= 0:
            continue
        days = (date_ms - start_ms) / (1000 * 60 * 60 * 24)
        data.append((days, float(wallet)))

    if len(data) < 2:
        raise ValueError("Нет корректных данных баланса")

    days = np.array([d for d, _ in data])
    log_balance = np.log([b for _, b in data])

    # Линейная регрессия
    slope, intercept, r_value, p_value, std_err = stats.linregress(days, log_balance)
    r_squared = r_value ** 2

    # Последний баланс в данных
    last_balance = data[-1][1]
    # Используем переданный current_balance как более актуальный, если он >0
    if current_balance is not None and current_balance > 0:
        current_balance = current_balance
    else:
        current_balance = last_balance

    # Количество дней от начала до последней записи
    days_elapsed = data[-1][0]

    # Прогноз до целей
    targets = [1000, 10000, 100000]
    forecast_days = {}
    for target in targets:
        if target <= current_balance:
            forecast_days[target] = 0.0
        else:
            if slope <= 0:
                forecast_days[target] = float('inf')
            else:
                forecast_days[target] = math.log(target / current_balance) / slope

    # Дополнительно – линейная регрессия абсолютных значений (для информации)
    slope_abs, intercept_abs, _, _, _ = stats.linregress(days, [b for _, b in data])

    return {
        'slope_log': slope,
        'r_squared': r_squared,
        'forecast_days': forecast_days,
        'days_elapsed': days_elapsed,
        'slope_abs': slope_abs,
        'current_balance': current_balance,
        'data_points': len(data),
        'method': 'exp_regression'
    }

def calculate_growth_exp_regression_new2_old(current_balance, db_file='funding_earning.db'):
    # Получаем данные из БД
    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    c.execute("SELECT date_ms, wallet FROM live_akk_usdt ORDER BY date_ms ASC")
    rows = c.fetchall()
    conn.close()

    if len(rows) < 2:
        raise ValueError("Недостаточно данных для регрессии (минимум 2 записи)")

    # Первая запись
    start_ms = rows[0][0]
    start_balance = float(rows[0][1])
    start_money = round(start_balance, 2)
    start_day = datetime.fromtimestamp(start_ms / 1000).strftime("%d.%m.%Y")

    # Текущее время и дата
    end_ms = int(datetime.now().timestamp() * 1000)
    end_day = datetime.fromtimestamp(end_ms / 1000).strftime("%d.%m.%Y")
    end_money = current_balance

    # Подготовка данных для регрессии (только положительные балансы)
    data = []
    for date_ms, wallet in rows:
        if wallet is None or wallet <= 0:
            continue
        days = (date_ms - start_ms) / (1000 * 60 * 60 * 24)
        data.append((days, float(wallet)))

    if len(data) < 2:
        raise ValueError("Нет корректных данных баланса")

    days = np.array([d for d, _ in data])
    log_balance = np.log([b for _, b in data])

    # Линейная регрессия на логарифме
    slope, intercept, r_value, p_value, std_err = stats.linregress(days, log_balance)
    r_squared = r_value ** 2

    # Количество дней от старта до текущего момента
    days_elapsed = (end_ms - start_ms) / (1000 * 60 * 60 * 24)
    hours_elapsed = days_elapsed * 24

    # Абсолютный прирост
    gain = current_balance - start_balance
    total_gain_percent = (current_balance / start_balance - 1) * 100

    # Среднедневной прирост в деньгах (как в первой функции)
    daily_abs_gain = gain / days_elapsed if days_elapsed > 0 else 0
    hourly_abs_gain = daily_abs_gain / 24 if days_elapsed > 0 else 0

    # Эффективная дневная процентная ставка из непрерывной
    daily_rate_eff = math.exp(slope) - 1  # (e^slope - 1) * 100%
    daily_gain_percent = daily_rate_eff * 100

    # Часовая эффективная ставка
    hourly_rate_eff = math.exp(slope / 24) - 1
    hourly_gain_percent = hourly_rate_eff * 100

    # Прогноз до целей (используем экспоненциальную модель)
    targets = [1000, 10000, 100000]
    forecast_days = {}
    if slope > 0:
        for target in targets:
            if target <= current_balance:
                forecast_days[target] = 0.0
            else:
                forecast_days[target] = math.log(target / current_balance) / slope
    else:
        for target in targets:
            forecast_days[target] = float('inf') if target > current_balance else 0.0

    # ---------- НОВЫЙ РАСЧЁТ: через сколько дней ежедневный доход превысит 100 $ ----------
    days_to_100_daily_profit = 0.0
    if slope > 0 and daily_rate_eff > 0:
        # Проверяем, не выполняется ли условие уже сейчас
        if current_balance * daily_rate_eff >= 100:
            days_to_100_daily_profit = 0.0
        else:
            # Нужно решить: current * exp(slope * t) * daily_rate_eff >= 100
            # => exp(slope * t) >= 100 / (current * daily_rate_eff)
            needed_ratio = 100.0 / (current_balance * daily_rate_eff)
            if needed_ratio > 0:
                days_to_100_daily_profit = math.log(needed_ratio) / slope
                if days_to_100_daily_profit < 0:
                    days_to_100_daily_profit = 0.0
            else:
                days_to_100_daily_profit = float('inf')
    else:
        days_to_100_daily_profit = float('inf')

    # Округляем до целого числа дней (потолок), чтобы показать, на какой день это произойдёт
    if math.isinf(days_to_100_daily_profit):
        days_to_100_daily_profit_str = ""
    else:
        days_to_100_daily_profit_str = f"{days_to_100_daily_profit:.1f} дней"

    # Формируем результат в виде словаря (для единообразия с первой функцией)
    result = {
        'days_elapsed': days_elapsed,
        'hours_elapsed': hours_elapsed,
        'total_gain_abs': gain,
        'total_gain_percent': total_gain_percent,
        'daily_abs_gain': daily_abs_gain,
        'daily_gain_percent': daily_gain_percent,
        'hourly_abs_gain': hourly_abs_gain,
        'hourly_gain_percent': hourly_gain_percent,
        'forecast_days': forecast_days,
        'days_to_100_daily_profit': days_to_100_daily_profit  # добавили в словарь
    }

    # Формируем строки вывода (точно как в первой функции)
    lines = [
        f"\nРассчетных точек: {len(data)} шт.",
        f'Баланс (start): {start_money}$ ({start_day})',
        f'Баланс (live):  {end_money}$ ({end_day})',
        f"Прошло дней: {result['days_elapsed']:.2f}",
        f"Прошло часов: {result['hours_elapsed']:.2f}",
        f"Абсолютный прирост: {result['total_gain_abs']:.2f} $",
        f"Прирост: {result['total_gain_percent']:.2f}%",
        "\n--- Скорость роста ---",
        f"В день: +{result['daily_abs_gain']:.4f} $  ({result['daily_gain_percent']:.4f}%)",
        f"В час:  +{result['hourly_abs_gain']:.4f} $  ({result['hourly_gain_percent']:.4f}%)",
        #f"R² модели: {r_squared:.4f} (чем ближе к 1, тем лучше)",
        "\n--- До целевых сумм (в днях) ---"
    ]
    usdt_1000 = 0.0
    usdt_10000 = 0.0
    usdt_100000 = 0.0
    for target, days_needed in result['forecast_days'].items():
        if math.isinf(days_needed):
            lines.append(f"До {target} $: недостижимо при текущей скорости")
        else:
            if target == 1000:
                usdt_1000 = days_needed
            elif target == 10000:
                usdt_10000 = days_needed
            elif target == 100000:
                usdt_100000 = days_needed

            lines.append(f"До {target} $: {days_needed:.1f} дней")

    # Добавляем строку о ежедневном доходе > 100 $
    day_do_100 = 0.0
    if days_to_100_daily_profit_str:
        lines.append(f"--------------------------\nЕжедневно > 100 $ через: {days_to_100_daily_profit_str}")
        day_do_100 = days_to_100_daily_profit

    write_prognoze(len(data), start_money, end_money, result['days_elapsed'], result['hours_elapsed'], result['total_gain_abs'], result['total_gain_percent'], result['daily_abs_gain'], result['daily_gain_percent'],
                       result['hourly_abs_gain'], result['hourly_gain_percent'],usdt_1000, usdt_10000, usdt_100000, day_do_100)


    out_metric = "\n".join(lines)
    print(out_metric)
    return out_metric

def calculate_growth_exp_regression_new(current_balance, db_file='funding_earning.db'):
    # Получаем данные из БД
    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    c.execute("SELECT date_ms, wallet FROM live_akk_usdt ORDER BY date_ms ASC")
    rows = c.fetchall()
    conn.close()

    if len(rows) < 2:
        raise ValueError("Недостаточно данных для регрессии (минимум 2 записи)")

    # Первая запись
    start_ms = rows[0][0]
    start_balance = float(rows[0][1])
    start_money = round(start_balance, 2)
    start_day = datetime.fromtimestamp(start_ms / 1000).strftime("%d.%m.%Y")

    # Текущее время и дата
    end_ms = int(datetime.now().timestamp() * 1000)
    end_day = datetime.fromtimestamp(end_ms / 1000).strftime("%d.%m.%Y")
    end_money = current_balance

    # Подготовка данных для регрессии (только положительные балансы)
    data = []
    for date_ms, wallet in rows:
        if wallet is None or wallet <= 0:
            continue
        days = (date_ms - start_ms) / (1000 * 60 * 60 * 24)
        data.append((days, float(wallet)))

    if len(data) < 2:
        raise ValueError("Нет корректных данных баланса")

    days = np.array([d for d, _ in data])
    log_balance = np.log([b for _, b in data])

    # Линейная регрессия на логарифме
    slope, intercept, r_value, p_value, std_err = stats.linregress(days, log_balance)
    r_squared = r_value ** 2

    # Количество дней от старта до текущего момента
    days_elapsed = (end_ms - start_ms) / (1000 * 60 * 60 * 24)
    hours_elapsed = days_elapsed * 24

    # Абсолютный прирост
    gain = current_balance - start_balance
    total_gain_percent = (current_balance / start_balance - 1) * 100

    # Среднедневной прирост в деньгах (как в первой функции)
    daily_abs_gain = gain / days_elapsed if days_elapsed > 0 else 0
    hourly_abs_gain = daily_abs_gain / 24 if days_elapsed > 0 else 0

    # Эффективная дневная процентная ставка из непрерывной
    daily_rate_eff = math.exp(slope) - 1  # (e^slope - 1) * 100%
    daily_gain_percent = daily_rate_eff * 100

    # Часовая эффективная ставка
    hourly_rate_eff = math.exp(slope / 24) - 1
    hourly_gain_percent = hourly_rate_eff * 100

    # Прогноз до целей (используем экспоненциальную модель)
    targets = [1000, 10000, 100000]
    forecast_days = {}
    if slope > 0:
        for target in targets:
            if target <= current_balance:
                forecast_days[target] = 0.0
            else:
                forecast_days[target] = math.log(target / current_balance) / slope
    else:
        for target in targets:
            forecast_days[target] = float('inf') if target > current_balance else 0.0

    # Формируем результат в виде словаря (для единообразия с первой функцией)
    result = {
        'days_elapsed': days_elapsed,
        'hours_elapsed': hours_elapsed,
        'total_gain_abs': gain,
        'total_gain_percent': total_gain_percent,
        'daily_abs_gain': daily_abs_gain,
        'daily_gain_percent': daily_gain_percent,
        'hourly_abs_gain': hourly_abs_gain,
        'hourly_gain_percent': hourly_gain_percent,
        'forecast_days': forecast_days
    }

    # Формируем строки вывода (точно как в первой функции)
    lines = [
        f'Стартовый баланс: {start_money}$ {start_day}',
        f'Текущий баланс: {end_money}$ {end_day}',
        f"Прошло дней: {result['days_elapsed']:.2f}",
        f"Прошло часов: {result['hours_elapsed']:.2f}",
        f"Абсолютный прирост: {result['total_gain_abs']:.2f} $",
        f"Прирост: {result['total_gain_percent']:.2f}%",
        "\n--- Скорость роста ---",
        f"В день: +{result['daily_abs_gain']:.4f} $  ({result['daily_gain_percent']:.4f}%)",
        f"В час:  +{result['hourly_abs_gain']:.4f} $  ({result['hourly_gain_percent']:.4f}%)",
        #f"R² экспоненциальный прогноз: {r_squared:.4f}\n(чем ближе к 1, тем лучше)",
        f"Количество точек прогноза: {len(data)}",
        "\n--- До целевых сумм (в днях) ---"
    ]

    for target, days_needed in result['forecast_days'].items():
        if math.isinf(days_needed):
            lines.append(f"До {target} $: недостижимо при текущей скорости")
        else:
            lines.append(f"До {target} $: {days_needed:.1f} дней")

    out_metric = "\n".join(lines)
    print(out_metric)
    return out_metric
# ===============================
# Функция сравнения и вывода отчёта
# ===============================
def compare_growth_methods(current_balance, db_file='funding_earning.db'):
    """
    Вызывает оба метода и выводит сравнительный отчёт.
    """
    # Получаем результаты
    res1 = get_method1_results(current_balance, db_file)
    res3 = calculate_growth_exp_regression(current_balance, db_file)

    print("\n" + "=" * 70)
    print("📊 СРАВНИТЕЛЬНЫЙ АНАЛИЗ МЕТОДОВ ПРОГНОЗИРОВАНИЯ")
    print("=" * 70)

    print(f"\n📌 Текущий баланс: {current_balance:.2f} USDT")
    print(f"📅 Дней с начала: {res1['days_elapsed']:.2f} (метод 1) / {res3['days_elapsed']:.2f} (метод 3)")

    print("\n📈 Скорость роста:")
    print(f"  Метод 1 (сложный % по двум точкам): {res1['daily_rate']*100:.4f}% в день")
    print(f"  Метод 3 (экспоненциальная регрессия): {res3['slope_log']*100:.4f}% в день")
    print(f"  R² модели (метод 3): {res3['r_squared']:.4f} (чем ближе к 1, тем лучше)")

    print("\n🎯 Прогноз достижения целевых сумм (в днях):")
    print(f"{'Цель (USDT)':>12} | {'Метод 1':>15} | {'Метод 3':>15} | {'Разница':>12}")
    print("-" * 65)

    for target in [1000, 10000, 100000]:
        d1 = res1['forecast_days'].get(target, float('inf'))
        d3 = res3['forecast_days'].get(target, float('inf'))
        if d1 == float('inf') or d3 == float('inf'):
            diff = '—'
        else:
            diff = f"{d1 - d3:.1f}"
        print(f"{target:>12} | {d1:>15.1f} | {d3:>15.1f} | {diff:>12}")

    print("\n📌 Дополнительная информация (метод 3):")
    print(f"  - Скорость в $/день (линейная регрессия): {res3['slope_abs']:.4f} $/день")
    print(f"  - Количество точек данных: {res3['data_points']}")
    print(f"  - Примечание: метод 3 использует все исторические данные, поэтому более устойчив к шуму.")

    return res1, res3

def is_symbol_supported_for_trading(sess, symbol):
    try:
        resp = sess.get_instruments_info(category="linear", symbol=symbol)
        if resp['retCode'] != 0 or not resp['result']['list']:
            return False
        inst = resp['result']['list'][0]
        status = inst.get('status', '')
        if status != 'Trading':
            return False
        # Проверка фильтров
        lot_filter = inst.get('lotSizeFilter', {})
        if not lot_filter:
            return False
        min_qty = float(lot_filter.get('minOrderQty', 0))
        if min_qty <= 0:
            return False
        price_filter = inst.get('priceFilter', {})
        if not price_filter:
            return False
        tick_size = float(price_filter.get('tickSize', 0))
        if tick_size <= 0:
            return False
        # Проверка риск-лимита
        risk_limit = inst.get('riskLimit', {})
        if risk_limit:
            max_leverage = float(risk_limit.get('maxLeverage', 0))
            if max_leverage <= 0:
                return False
        else:
            return False
        return True
    except Exception:
        return False

def calculate_growth_and_forecast(current_balance):

    end_ms = int(datetime.now().timestamp() * 1000)
    timestamp = end_ms / 1000  # Переводим в секунды
    dt_object = datetime.fromtimestamp(timestamp)
    end_day = dt_object.strftime("%d.%m.%Y")
    end_money = current_balance
    
    
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT date_ms, wallet FROM live_akk_usdt ORDER BY date_ms ASC")
    rows = c.fetchone()
    if rows is None:
        raise ValueError("В базе нет записей баланса")
    start_ms = int(rows[0])
    timestamp = start_ms / 1000  # Переводим в секунды
    dt_object = datetime.fromtimestamp(timestamp)
    start_day = dt_object.strftime("%d.%m.%Y")
    
    start_balance = float(rows[1])
    start_money = round(start_balance,2)
    conn.close()

    if end_ms <= start_ms:
        raise ValueError("Время окончания должно быть позже времени начала")
    if start_balance <= 0 or current_balance <= 0:
        raise ValueError("Балансы должны быть положительными")

    delta_ms = end_ms - start_ms
    days = delta_ms / (1000 * 60 * 60 * 24)
    hours = days * 24
    gain = current_balance - start_balance
    daily_abs_gain = gain / days
    hourly_abs_gain = daily_abs_gain / 24
    total_return = current_balance / start_balance

    if days > 0:
        daily_rate = total_return ** (1 / days) - 1
    else:
        daily_rate = 0.0

    if daily_rate > -1:
        hourly_rate = (1 + daily_rate) ** (1 / 24) - 1
    else:
        hourly_rate = -1.0

    targets = [1000, 10000, 100000]
    forecast_days = {}

    if daily_rate > 0:
        for target in targets:
            if target > current_balance:
                n_days = math.log(target / current_balance) / math.log(1 + daily_rate)
                forecast_days[target] = n_days
            else:
                forecast_days[target] = 0.0
    else:
        for target in targets:
            forecast_days[target] = float('inf') if target > current_balance else 0.0

    result = {
        'days_elapsed': days,
        'hours_elapsed': hours,
        'total_gain_abs': gain,
        'total_gain_percent': total_return * 100 - 100,
        'daily_abs_gain': daily_abs_gain,
        'daily_gain_percent': daily_rate * 100,
        'hourly_abs_gain': hourly_abs_gain,
        'hourly_gain_percent': hourly_rate * 100,
        'forecast_days': forecast_days
    }

    lines = [
        f'Стартовый баланс: {start_money}$ {start_day}',
        f'Текущий баланс: {end_money}$ {end_day}',
        f"Прошло дней: {result['days_elapsed']:.2f}",
        f"Прошло часов: {result['hours_elapsed']:.2f}",
        f"Абсолютный прирост: {result['total_gain_abs']:.2f} $",
        f"Прирост: {result['total_gain_percent']:.2f}%",
        "\n--- Скорость роста ---",
        f"В день: +{result['daily_abs_gain']:.4f} $  ({result['daily_gain_percent']:.4f}%)",
        f"В час:  +{result['hourly_abs_gain']:.4f} $  ({result['hourly_gain_percent']:.4f}%)",
        "\n--- Прогноз достижения целевых сумм (в днях) ---"
    ]

    # Добавляем строки прогноза
    for target, days_needed in result['forecast_days'].items():
        if math.isinf(days_needed):
            lines.append(f"До {target} $: недостижимо при текущей скорости")
        else:
            lines.append(f"До {target} $: {days_needed:.1f} дней")

    # Объединяем всё в одну строку с переносами
    out_metric = "\n".join(lines)
    print(out_metric)
    return out_metric

def calculate_growth_metrics():
    """
    Читает историю баланса из БД, вычисляет скорость роста и прогнозирует
    время до достижения целевых сумм (1000, 10000, 100000, 1000000 USDT).
    Возвращает словарь с метриками и строку для отправки в Telegram.
    """
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    # Получаем все записи баланса, сортируем по времени
    c.execute("SELECT date_ms, wallet FROM live_akk_usdt ORDER BY date_ms ASC")
    rows = c.fetchall()
    conn.close()

    if len(rows) < 2:
        return None, "Недостаточно данных для расчёта (нужно минимум 2 записи)"

    # Преобразуем в список кортежей (timestamp_sec, balance)
    data = [(row[0] / 1000.0, row[1]) for row in rows if row[1] is not None and row[1] > 0]
    if len(data) < 2:
        return None, "Нет корректных данных о балансе"

    # Ограничимся последними 7 днями (если есть)
    now_ts = time.time()
    week_ago = now_ts - 1 * 24 * 3600
    recent = [(ts, bal) for ts, bal in data if ts >= week_ago]
    if len(recent) < 2:
        # Если за неделю меньше 2 записей, берём все данные
        recent = data

    # Вычисляем линейную регрессию (скорость изменения баланса в USDT/сек)
    # Используем метод наименьших квадратов: y = a + b * x, где b - скорость
    n = len(recent)
    sum_x = sum(ts for ts, _ in recent)
    sum_y = sum(bal for _, bal in recent)
    sum_xy = sum(ts * bal for ts, bal in recent)
    sum_x2 = sum(ts * ts for ts, _ in recent)

    if n * sum_x2 - sum_x * sum_x == 0:
        return None, "Невозможно вычислить скорость (недостаточно вариативности времени)"

    b = (n * sum_xy - sum_x * sum_y) / (n * sum_x2 - sum_x * sum_x)  # USDT/сек
    a = (sum_y - b * sum_x) / n  # Начальный баланс в момент времени 0 (не используется)

    # Текущий баланс (последняя запись)
    current_balance = recent[-1][1]
    current_time = recent[-1][0]

    # Скорость в USDT/день
    daily_rate = b * 86400

    if daily_rate <= 0:
        return {
            'current_balance': current_balance,
            'daily_rate': daily_rate,
            'projections': {}
        }, f"Скорость роста не положительная ({daily_rate:.4f} USDT/день). Рост не прогнозируется."

    # Целевые суммы
    targets = [1000, 10000, 100000, 1000000]
    projections = {}
    days_to_target = {}
    for target in targets:
        if target <= current_balance:
            days = 0
            reached = True
        else:
            delta = target - current_balance
            days = delta / daily_rate
            reached = False
        days_to_target[target] = days
        projections[target] = {
            'days': days,
            'reached': reached,
            'date': datetime.now(timezone.utc).timestamp() + days * 86400
        }

    # Формируем читаемую строку
    lines = [
        f"📊 Текущий баланс: {current_balance:.2f} USDT",
        f"📈 Скорость роста: {daily_rate:.4f} USDT/день",
        ""
    ]
    for target, proj in projections.items():
        if proj['reached']:
            lines.append(f"✅ {target} USDT уже достигнут")
        else:
            days = proj['days']
            if days < 1:
                lines.append(f"⏳ {target} USDT будет достигнут менее чем через день")
            else:
                lines.append(f"⏳ {target} USDT будет достигнут через {days:.1f} дней (~{days / 30:.1f} месяцев)")

    result = {
        'current_balance': current_balance,
        'daily_rate': daily_rate,
        'projections': projections,
        'days_to_target': days_to_target
    }
    return result, "\n".join(lines)

def find_nearest_bounds(x, a):
    greater = [num[0] for num in x if num[0] > a]
    less = [num[0] for num in x if num[0] < a and num[1] == 0]

    nearest_greater = min(greater) if greater else None
    nearest_less = max(less) if less else None

    if nearest_less is None:
        min_nex_reopen = float(read_key_new('key_process','reopen_ball'))
        raznica_min_max = (float(nearest_greater) - min_nex_reopen)/2
        nearest_less = min_nex_reopen + raznica_min_max

    return nearest_greater, nearest_less

def read_level():
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute(f"SELECT level, akk FROM step_level")
        row = c.fetchall()
        conn.close()
        if row is None:
            return False
        return row #[item[0] for item in row]
    except Exception as e:
        print(f"Ошибка в read_level: {e}")
        return False



def write_level_akk(level,akk): #100.565
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute(f"UPDATE step_level SET akk = {akk} WHERE level = {level}")
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Ошибка в write_key_new: {e}")
        return False

def reset_level_akk(akk):
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute(f"UPDATE step_level SET akk = 0 WHERE akk = {akk}")
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Ошибка в reset_level_akk: {e}")
        return False

#bigger, smaller = find_nearest_bounds(read_level(), a)
# print(f"Ближайшее большее: {bigger}")   # 99.001
# print(f"Ближайшее меньшее: {smaller}")  # 90.125

def money_rovno():
    MAIN_UID = 130672905  # Главный аккаунт

    # Словарь: имя_аккаунта -> (UID, конфиг)
    accounts = {
        'test5': {'uid': 264370676, 'config': CONFIG0},  # Основной рабочий
        'test1': {'uid': 392012257, 'config': CONFIG1},  # Вспомогательный 1
        'test2': {'uid': 392016376, 'config': CONFIG2},  # Вспомогательный 2
        'test7': {'uid': 565801901, 'config': CONFIG3},  # Вспомогательный 3
        'test8': {'uid': 565802123, 'config': CONFIG4},  # Вспомогательный 4
    }

    # Сессия главного аккаунта
    main_session = HTTP(**CONFIG)

    def send_api(otkuda, kuda, skolko, description=""):
        """Выполняет перевод средств с дробной частью (2 знака)"""
        # Округляем до 2 знаков перед проверкой
        skolko = round(skolko, 2)

        if skolko <= 0:
            print(f"💰 Сумма {skolko} <= 0, пропускаем перевод {description}")
            return

        transfer_id = str(uuid.uuid4())
        try:
            resp = main_session.create_universal_transfer(
                transferId=transfer_id,
                coin="USDT",
                amount=str(skolko),  # Передаём как строку с дробной частью
                fromMemberId=str(otkuda),
                toMemberId=str(kuda),
                fromAccountType="UNIFIED",
                toAccountType="UNIFIED",
            )

            if resp.get('retCode') == 0:
                print(f"✅ {description}: {skolko:.2f} USDT с {otkuda} на {kuda}")
            else:
                print(f"❌ Ошибка {description}: {resp}")
            return resp
        except Exception as e:
            print(f"❌ Исключение при переводе {description}: {e}")
            return None

    print("=" * 60)
    print("🚀 НАЧАЛО ПРОЦЕССА РАСПРЕДЕЛЕНИЯ СРЕДСТВ")
    print("=" * 60)

    # ШАГ 1: Собираем средства со всех аккаунтов на главный
    print("\n📥 ШАГ 1: Сбор средств на главный аккаунт")
    print("-" * 40)

    total_collected = 0.0  # Изменено на float

    for name, data in accounts.items():
        try:
            sess = HTTP(**data['config'])
            balance_response = sess.get_wallet_balance(
                accountType="UNIFIED",
                coin="USDT"
            )

            if balance_response.get('retCode') == 0:
                balance = float(balance_response['result']['list'][0]['totalMarginBalance'])
                # Округляем баланс до 2 знаков
                balance = round(balance, 2)

                if balance > 0:
                    print(f"  {name} (UID: {data['uid']}): баланс = {balance:.2f} USDT")
                    send_api(data['uid'], MAIN_UID, balance, f"Сбор с {name}")
                    total_collected += balance
                    time.sleep(1)
                else:
                    print(f"  {name} (UID: {data['uid']}): баланс = 0.00 USDT (пропускаем)")
            else:
                print(f"  ❌ Ошибка получения баланса {name}: {balance_response}")

        except Exception as e:
            print(f"  ❌ Ошибка при обработке {name}: {e}")

    # Округляем итоговую сумму
    total_collected = round(total_collected, 2)
    print(f"\n📊 Собрано всего: {total_collected:.2f} USDT")
    time.sleep(2)

    # ШАГ 2: Получаем итоговый баланс главного аккаунта
    print("\n📥 ШАГ 2: Проверка баланса главного аккаунта")
    print("-" * 40)

    main_balance_response = main_session.get_wallet_balance(
        accountType="UNIFIED",
        coin="USDT"
    )

    if main_balance_response.get('retCode') != 0:
        print("❌ Не удалось получить баланс главного аккаунта")
        return

    main_balance = float(main_balance_response['result']['list'][0]['totalMarginBalance'])
    main_balance = round(main_balance, 2)
    print(f"  Баланс главного аккаунта (UID: {MAIN_UID}): {main_balance:.2f} USDT")

    # ШАГ 3: Равномерное распределение с дробными частями
    print("\n📤 ШАГ 3: Равномерное распределение средств")
    print("-" * 40)

    num_accounts = len(accounts)
    min_amount = 5.0  # Минимальная сумма для распределения (дробная)

    if main_balance > min_amount:
        # Вычисляем равную долю с дробной частью (2 знака)
        equal_share = round(main_balance / num_accounts, 2)

        print(f"  Всего аккаунтов для распределения: {num_accounts}")
        print(f"  Общая сумма: {main_balance:.2f} USDT")
        print(f"  Равная доля на каждый аккаунт: {equal_share:.2f} USDT")

        # Рассчитываем остаток
        remaining = round(main_balance - (equal_share * num_accounts), 2)
        if remaining > 0:
            print(f"  Остаток (останется на главном): {remaining:.2f} USDT")

        # Распределяем на каждый аккаунт
        for name, data in accounts.items():
            send_api(MAIN_UID, data['uid'], equal_share, f"Распределение на {name}")
            time.sleep(1)

        print(f"\n✅ Распределение завершено!")
        print(f"  Каждый аккаунт получил по {equal_share:.2f} USDT")
        if remaining > 0:
            print(f"  Остаток {remaining:.2f} USDT остался на главном аккаунте")

    else:
        print(f"  Сумма на главном аккаунте ({main_balance:.2f} USDT) меньше минимальной ({min_amount:.2f} USDT)")
        print("  Распределение не производится")

    # ШАГ 4: Финальные балансы
    print("\n📊 ШАГ 4: Финальные балансы")
    print("-" * 40)

    # Проверяем баланс главного аккаунта
    final_main_balance = main_session.get_wallet_balance(
        accountType="UNIFIED",
        coin="USDT"
    )
    if final_main_balance.get('retCode') == 0:
        final_main = round(float(final_main_balance['result']['list'][0]['totalMarginBalance']), 2)
        print(f"  Главный аккаунт ({MAIN_UID}): {final_main:.2f} USDT")

    # Проверяем балансы всех аккаунтов
    for name, data in accounts.items():
        try:
            sess = HTTP(**data['config'])
            balance_response = sess.get_wallet_balance(
                accountType="UNIFIED",
                coin="USDT"
            )
            if balance_response.get('retCode') == 0:
                balance = round(float(balance_response['result']['list'][0]['totalMarginBalance']), 2)
                print(f"  {name} ({data['uid']}): {balance:.2f} USDT")
        except Exception as e:
            print(f"  ❌ Ошибка получения баланса {name}: {e}")

    print("\n" + "=" * 60)
    print("🏁 ПРОЦЕСС ЗАВЕРШЕН")
    print("=" * 60)

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
    from pybit.unified_trading import HTTP

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


def add_symbol_step_size(sym, size, side, akk):

    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    cursor.execute('''
        INSERT OR IGNORE INTO step_size (symbol, size, side, akk)
        VALUES (?, ?, ?, ?)
    ''', (sym, size, side, akk))

    conn.commit()
    if cursor.rowcount > 0:
        #print(f"Символ {sym} добавлен.")
        return True
    else:
        #print(f"Символ {sym} уже существует. Ничего не добавлено.")
        return False


def clear_step_size():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('DELETE FROM step_size')
    conn.commit()
    conn.close()
    print("Таблица step_size полностью очищена.")


def find_best_by_last_trades_detailed(
        min_total=10,
        min_pnl_threshold=0,
        require_min_win_rate=None
):
    """
    Расширенная версия с детальной статистикой по каждому сетапу.
    """
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # Получаем все сетапы
    cursor.execute('SELECT DISTINCT akk FROM open_pos WHERE status IN ("TP", "SL") AND akk != 4')
    accounts = [row[0] for row in cursor.fetchall()]

    all_results = {}
    best_result = None

    for akk in accounts:
        cursor.execute('''
            SELECT pnl, status, date_close, sym, side, fund 
            FROM open_pos 
            WHERE akk = ? AND status IN ('TP', 'SL')
            ORDER BY date_close DESC, id DESC
            LIMIT ?
        ''', (akk, min_total))

        trades = cursor.fetchall()

        if len(trades) < min_total:
            all_results[akk] = {
                'status': '❌ Недостаточно сделок',
                'trades_count': len(trades)
            }
            continue

        pnls = [row[0] for row in trades]
        total_pnl = sum(pnls)

        if total_pnl <= min_pnl_threshold:
            all_results[akk] = {
                'status': f'❌ PnL {total_pnl:.2f} <= {min_pnl_threshold}',
                'total_pnl': total_pnl,
                'trades_count': len(trades)
            }
            continue

        avg_pnl = total_pnl / len(trades)
        win_count = sum(1 for pnl in pnls if pnl > 0)
        win_rate = win_count / len(trades) * 100

        if require_min_win_rate is not None and win_rate < require_min_win_rate:
            all_results[akk] = {
                'status': f'❌ Win Rate {win_rate:.1f}% < {require_min_win_rate}%',
                'total_pnl': total_pnl,
                'win_rate': win_rate,
                'trades_count': len(trades)
            }
            continue

        symbols = list(set([row[3] for row in trades]))

        result_data = {
            'status': '✅ Кандидат',
            'total_pnl': total_pnl,
            'avg_pnl': avg_pnl,
            'win_rate': win_rate,
            'trades_count': len(trades),
            'symbols': symbols,
            'last_trades': [
                {
                    'pnl': row[0],
                    'status': row[1],
                    'sym': row[3],
                    'side': row[4],
                    'fund': row[5] if row[5] is not None else 0
                }
                for row in trades
            ]
        }

        all_results[akk] = result_data

        if best_result is None or total_pnl > best_result[0]:
            best_result = (total_pnl, akk, result_data)

    conn.close()

    # Вывод детальной статистики
    print(f"\n{'=' * 80}")
    print(f"📊 АНАЛИЗ СЕТАПОВ (последние {min_total} сделок по всем символам)")
    print(f"{'=' * 80}")

    for akk, data in all_results.items():
        marker = "⭐" if best_result and best_result[1] == akk else "  "
        if data.get('status', '').startswith('❌'):
            print(f"{marker} AKK-{akk}: {data['status']}")
        else:
            print(f"{marker} AKK-{akk}: total_pnl={data['total_pnl']:.2f}, "
                  f"win_rate={data['win_rate']:.1f}%, trades={data['trades_count']}, "
                  f"symbols={len(data.get('symbols', []))}")

    if best_result:
        return {
            'akk': best_result[1],
            'total_pnl': best_result[0],
            'details': best_result[2]
        }
    return None


def find_best_setup(
    window=10,
    min_trades=10,
    min_win_rate=0,
    min_tp_sum=0,
):
    """
    Выбор одного или двух лучших сетапов.

    Критерии допуска:
        1. Количество сделок >= min_trades
        2. total_pnl > 0
        3. tp_sum > min_tp_sum

    Критерии рейтинга:
        1. 60% — сумма TP
        2. 40% — win rate

    При выборе кандидатов приоритет:
        1. total_pnl
        2. score
        3. win_rate
        4. tp_sum

    Возвращаемое значение:

        []                         если кандидатов нет;

        [
            {
                "akk": ...,
                "score": ...,
                "details": {...}
            }
        ]

        если найден один кандидат;

        [
            {
                "akk": ...,
                "score": ...,
                "details": {...}
            },
            {
                "akk": ...,
                "score": ...,
                "details": {...}
            }
        ]

        если найдено два кандидата.

    Параметр min_win_rate оставлен для совместимости,
    но в текущей логике не используется как фильтр.
    """

    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    try:
        # Получаем все сетапы, кроме AKK=4
        cursor.execute("""
            SELECT DISTINCT akk
            FROM open_pos
            WHERE status IN ('TP', 'SL')
              AND akk != 4
            ORDER BY akk
        """)

        accounts = [
            row[0]
            for row in cursor.fetchall()
            if row[0] is not None
        ]

        results = []

        for akk in accounts:

            # Стабильный порядок выборки.
            #
            # Если у нескольких сделок одинаковый date_close,
            # дополнительно используется id.
            cursor.execute("""
                SELECT
                    id,
                    pnl,
                    status,
                    date_close,
                    sym,
                    side,
                    fund
                FROM open_pos
                WHERE akk = ?
                  AND status IN ('TP', 'SL')
                ORDER BY date_close DESC, id DESC
                LIMIT ?
            """, (akk, window))

            trades = cursor.fetchall()
            trades_count = len(trades)

            # Если сделок нет
            if trades_count == 0:
                results.append({
                    "akk": akk,
                    "status": "❌ Нет сделок",
                    "eligible": False,

                    "trades": 0,
                    "tp_count": 0,
                    "sl_count": 0,

                    "tp_sum": 0.0,
                    "sl_sum": 0.0,
                    "total_pnl": 0.0,

                    "avg_tp": 0.0,
                    "avg_sl": 0.0,
                    "avg_pnl": 0.0,

                    "win_rate": 0.0,

                    "symbols_count": 0,
                    "symbols": [],

                    "last_trades": [],
                })

                continue

            # Отдельно выделяем TP и SL
            tp_trades = [
                row
                for row in trades
                if row[2] == "TP"
            ]

            sl_trades = [
                row
                for row in trades
                if row[2] == "SL"
            ]

            tp_count = len(tp_trades)
            sl_count = len(sl_trades)

            # PnL прибыльных сделок
            tp_pnls = [
                float(row[1] or 0.0)
                for row in tp_trades
            ]

            # PnL убыточных сделок
            sl_pnls = [
                float(row[1] or 0.0)
                for row in sl_trades
            ]

            # Сумма TP
            tp_sum = sum(tp_pnls)

            # Сумма SL
            sl_sum = sum(sl_pnls)

            # Общий результат
            total_pnl = sum(
                float(row[1] or 0.0)
                for row in trades
            )

            # Средний TP
            avg_tp = (
                tp_sum / tp_count
                if tp_count > 0
                else 0.0
            )

            # Средний SL
            avg_sl = (
                sl_sum / sl_count
                if sl_count > 0
                else 0.0
            )

            # Средний PnL на сделку
            avg_pnl = (
                total_pnl / trades_count
                if trades_count > 0
                else 0.0
            )

            # Win rate.
            #
            # TP считается выигрышной сделкой,
            # SL — проигрышной.
            win_rate = (
                tp_count / trades_count * 100.0
                if trades_count > 0
                else 0.0
            )

            # Уникальные символы
            symbols = sorted(set(
                row[4]
                for row in trades
                if row[4] is not None
            ))

            # Обязательные фильтры допуска.
            #
            # min_win_rate здесь намеренно не используется.
            # Win rate участвует только в score.
            eligible = (
                trades_count >= min_trades
                and total_pnl > 0
                and tp_sum > min_tp_sum
            )

            if not eligible:

                if trades_count < min_trades:
                    status_text = (
                        f"❌ Недостаточно сделок: "
                        f"{trades_count} < {min_trades}"
                    )

                elif total_pnl <= 0:
                    status_text = (
                        f"❌ Отрицательный PnL: "
                        f"{total_pnl:+.2f}"
                    )

                elif tp_sum <= min_tp_sum:
                    status_text = (
                        f"❌ TP сумма "
                        f"{tp_sum:+.2f} <= "
                        f"{min_tp_sum:+.2f}"
                    )

                else:
                    status_text = "❌ Не прошёл фильтр"

            else:
                status_text = "✅ Кандидат"

            # Сохраняем последние сделки в удобном формате
            last_trades = []

            for row in trades:
                last_trades.append({
                    "id": row[0],
                    "pnl": float(row[1] or 0.0),
                    "status": row[2],
                    "date_close": row[3],
                    "sym": row[4],
                    "side": row[5],
                    "fund": float(row[6] or 0.0),
                })

            results.append({
                "akk": akk,
                "status": status_text,
                "eligible": eligible,

                "trades": trades_count,

                "tp_count": tp_count,
                "sl_count": sl_count,

                "tp_sum": tp_sum,
                "sl_sum": sl_sum,
                "total_pnl": total_pnl,

                "avg_tp": avg_tp,
                "avg_sl": avg_sl,
                "avg_pnl": avg_pnl,

                "win_rate": win_rate,

                "symbols_count": len(symbols),
                "symbols": symbols,

                "last_trades": last_trades,
            })

        # В рейтинг допускаются только прибыльные сетапы
        candidates = [
            item
            for item in results
            if item.get("eligible") is True
        ]

        # Если прибыльных сетапов нет
        if not candidates:
            print()
            print("❌ Нет сетапов, прошедших обязательные фильтры")
            print(
                f"Условия: сделок >= {min_trades}, "
                f"total_pnl > 0, "
                f"TP сумма > {min_tp_sum}"
            )

            # ВАЖНО:
            # Возвращаем именно список,
            # чтобы снаружи работала проверка:
            #
            # if not rez:
            #     continue
            return []

        # Максимальные значения для нормализации
        max_tp_sum = max(
            item["tp_sum"]
            for item in candidates
        )

        max_win_rate = max(
            item["win_rate"]
            for item in candidates
        )

        max_trades = max(
            item["trades"]
            for item in candidates
        )

        # Расчёт рейтинга
        for item in candidates:

            # Нормированный размер TP
            tp_score = (
                item["tp_sum"] / max_tp_sum
                if max_tp_sum > 0
                else 0.0
            )

            # Нормированный win rate
            win_rate_score = (
                item["win_rate"] / max_win_rate
                if max_win_rate > 0
                else 0.0
            )

            # Нормированное количество сделок.
            #
            # При одинаковом window у всех обычно
            # будет одинаковое значение.
            trades_score = (
                item["trades"] / max_trades
                if max_trades > 0
                else 0.0
            )

            item["tp_score"] = tp_score
            item["win_rate_score"] = win_rate_score
            item["trades_score"] = trades_score

            # Итоговый score.
            #
            # 60% — сумма TP
            # 40% — win rate
            #
            # Количество сделок используется
            # как обязательный фильтр.
            item["score"] = (
                0.60 * tp_score
                + 0.40 * win_rate_score
            )

        # Выбираем кандидатов сначала по фактическому PnL.
        #
        # Это важно:
        # сетап с меньшим total_pnl не победит
        # только из-за чуть большего win rate.
        candidates.sort(
            key=lambda item: (
                item["total_pnl"],
                item["score"],
                item["win_rate"],
                item["tp_sum"],
            ),
            reverse=True
        )

        # Первый кандидат существует,
        # потому что выше была проверка if not candidates.
        best1 = candidates[0]

        # Второй кандидат может отсутствовать.
        best2 = (
            candidates[1]
            if len(candidates) > 1
            else None
        )

        # Сортировка для отображения всех результатов.
        #
        # Сначала идут допущенные кандидаты,
        # затем отфильтрованные сетапы.
        sorted_results = sorted(
            results,
            key=lambda item: (
                item.get("eligible", False),
                item.get("total_pnl", -999999.0),
                item.get("score", -1.0),
                item.get("win_rate", -1.0),
            ),
            reverse=True
        )

        print()
        print("=" * 110)
        print(
            f"📊 АНАЛИЗ СЕТАПОВ "
            f"(последние {window} сделок по каждому AKK)"
        )
        print("=" * 110)

        print(
            f"{'AKK':<8}"
            f"{'TP сумма':>12}"
            f"{'SL сумма':>12}"
            f"{'Total PnL':>14}"
            f"{'Win Rate':>12}"
            f"{'Сделки':>10}"
            f"{'Score':>10}"
            f"{'Статус':>30}"
        )

        print("-" * 110)

        for item in sorted_results:

            akk_label = f"AKK-{item['akk']}"

            if item.get("eligible") is True:

                if item["akk"] == best1["akk"]:
                    marker = "⭐"
                elif best2 is not None and item["akk"] == best2["akk"]:
                    marker = "🥈"
                else:
                    marker = "  "

                print(
                    f"{marker} {akk_label:<6}"
                    f"{item['tp_sum']:>12.2f}"
                    f"{item['sl_sum']:>12.2f}"
                    f"{item['total_pnl']:>14.2f}"
                    f"{item['win_rate']:>11.1f}%"
                    f"{item['trades']:>10}"
                    f"{item['score']:>10.3f}"
                    f"{item['status']:>30}"
                )

            else:

                print(
                    f"   {akk_label:<5}"
                    f"{item.get('tp_sum', 0.0):>12.2f}"
                    f"{item.get('sl_sum', 0.0):>12.2f}"
                    f"{item.get('total_pnl', 0.0):>14.2f}"
                    f"{item.get('win_rate', 0.0):>11.1f}%"
                    f"{item.get('trades', 0):>10}"
                    f"{'FILTER':>10}"
                    f"{item.get('status', ''):>30}"
                )

        print("=" * 110)

        def print_setup_details(setup, title):
            print()
            print("=" * 110)
            print(f"{title}: AKK-{setup['akk']}")
            print("=" * 110)
            print(
                f"Сумма TP:          "
                f"{setup['tp_sum']:+.2f} USDT"
            )
            print(
                f"Сумма SL:          "
                f"{setup['sl_sum']:+.2f} USDT"
            )
            print(
                f"Итоговый PnL:      "
                f"{setup['total_pnl']:+.2f} USDT"
            )
            print(
                f"Средний TP:        "
                f"{setup['avg_tp']:+.2f} USDT"
            )
            print(
                f"Средний SL:        "
                f"{setup['avg_sl']:+.2f} USDT"
            )
            print(
                f"Средний PnL:       "
                f"{setup['avg_pnl']:+.2f} USDT"
            )
            print(
                f"Win Rate:          "
                f"{setup['win_rate']:.1f}%"
            )
            print(
                f"TP-сделок:         "
                f"{setup['tp_count']}"
            )
            print(
                f"SL-сделок:         "
                f"{setup['sl_count']}"
            )
            print(
                f"Всего сделок:      "
                f"{setup['trades']}"
            )
            print(
                f"Символов:          "
                f"{setup['symbols_count']}"
            )
            print(
                f"Итоговый Score:    "
                f"{setup['score']:.3f}"
            )
            print(
                "Состав score:      "
                "60% TP + 40% Win Rate"
            )
            print("=" * 110)

        # Вывод первого кандидата
        print_setup_details(
            best1,
            "🏆 ЛУЧШИЙ СЕТАП"
        )

        # Вывод второго кандидата
        if best2 is not None:
            print_setup_details(
                best2,
                "🥈 ВТОРОЙ КАНДИДАТ"
            )
        else:
            print()
            print("🥈 Второго подходящего кандидата нет")

        # Формируем список возврата
        selected_setups = [
            {
                "akk": best1["akk"],
                "score": best1["score"],
                "details": best1,
            }
        ]

        if best2 is not None:
            selected_setups.append({
                "akk": best2["akk"],
                "score": best2["score"],
                "details": best2,
            })

        return selected_setups

    finally:
        conn.close()


def find_best(symbol, side, target_fund, tolerance=0.01, min_total=3):
    """
    Ищет лучший аккаунт (akk) для символа и стороны.
    Критерии:
    1. Минимум 3 закрытых сделки (TP или SL).
    2. Суммарный PnL за последние 3 сделки > 0.
    3. Средний Funding (fund) за последние 3 сделки находится в пределах 'tolerance' от target_fund.
       (Учитывает положительные и отрицательные значения).
    """
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # Получаем аккаунты, у которых вообще были сделки по этому символу и стороне
    cursor.execute('''
        SELECT DISTINCT akk FROM open_pos 
        WHERE sym = ? AND side = ?
    ''', (symbol, side))
    accounts = [row[0] for row in cursor.fetchall()]

    best_result = None  # (total_pnl, diff_fund, akk, trades_count)

    # Перебираем каждый аккаунт
    for akk in accounts:
        # 1. Берем последние 3 закрытые сделки (TP/SL)
        cursor.execute('''
            SELECT pnl, fund FROM open_pos 
            WHERE sym = ? AND side = ? AND akk = ? 
              AND status IN ('TP', 'SL')
            ORDER BY date_close DESC, id DESC
            LIMIT ?
        ''', (symbol, side, akk, min_total)) #ORDER BY id DESC

        trades = cursor.fetchall()

        # Проверяем: минимум 3 сделки
        if len(trades) < min_total:
            continue

        # 2. Считаем суммарный PnL
        total_pnl = sum([row[0] for row in trades])

        # 3. Проверяем, что PnL > 0
        if total_pnl <= 0:
            continue

        # 4. Вычисляем средний Funding (fund) за последние 3 сделки
        # fund может быть отрицательным, среднее арифметическое это учитывает
        avg_fund = sum([row[1] for row in trades]) / len(trades)

        # 5. Проверяем вилку (tolerance)
        # Если target_fund = None, пропускаем проверку вилки (ищем просто лучший PnL)
        if target_fund is not None:
            if abs(avg_fund - target_fund) > tolerance:
                continue  # Не подходит по Funding, пропускаем этот аккаунт

        diff_fund = abs(avg_fund - target_fund) if target_fund is not None else 0

        # 6. Сравниваем с текущим лучшим результатом
        if best_result is None:
            best_result = (total_pnl, diff_fund, akk, len(trades))
        else:
            # Приоритет 1: Максимальный PnL
            if total_pnl > best_result[0]:
                best_result = (total_pnl, diff_fund, akk, len(trades))
            # Приоритет 2: Если PnL равны, выбираем того, чей fund ближе к целевому
            elif total_pnl == best_result[0] and diff_fund < best_result[1]:
                best_result = (total_pnl, diff_fund, akk, len(trades))

    conn.close()

    if best_result:
        return {
            'akk': best_result[2],
            'total_pnl': best_result[0],
            'diff_fund': best_result[1],
            'trades_count': best_result[3],
            'avg_fund': None  # Заполним ниже, если нужно
        }
    else:
        return None

# ---------- ИНИЦИАЛИЗАЦИЯ БД ----------
def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    c.execute('''CREATE TABLE IF NOT EXISTS akk4_tp_sl
                     (symbol TEXT PRIMARY KEY,
                      side INTEGER,
                      tp REAL,
                      sl REAL
                      )''')
    conn.commit()

    c.execute('''CREATE TABLE IF NOT EXISTS step_size
                         (symbol TEXT NOT NULL,
                          size REAL NOT NULL DEFAULT 1.0,
                          side TEXT,
                          akk TEXT,
                          PRIMARY KEY (symbol, side, akk)
                          )''')
    conn.commit()

    c.execute('''CREATE TABLE IF NOT EXISTS funding_trades
                 (symbol TEXT,
                  side TEXT,
                  qty REAL,
                  open_time TEXT,
                  qty_first REAL,
                  live_pnl REAL,
                  PRIMARY KEY(symbol, side))''')
    conn.commit()

    c.execute('''CREATE TABLE IF NOT EXISTS key_process
                 (id INTEGER PRIMARY KEY CHECK (id = 1),
                  step_reopen INTEGER NOT NULL DEFAULT 1,
                  reopen_ball REAL,
                  first_ball REAL,
                  old_price_reopen REAL,
                  next_price_reopen REAL,
                  key_after_close REAL,
                  all_start_summ REAL
              )''')
    conn.commit()

    c.execute(
        "INSERT OR IGNORE INTO key_process (id, step_reopen, reopen_ball, first_ball, old_price_reopen, next_price_reopen, key_after_close, all_start_summ) VALUES (1, 1, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)"
    )
    conn.commit()

    c.execute('''CREATE TABLE IF NOT EXISTS live_prognoze
                         (id INTEGER PRIMARY KEY AUTOINCREMENT,
                            date_ms INTEGER,
                            dot INTEGER,
                            start REAL,
                            live REAL,
                            day REAL,
                            hour REAL,
                            prirost_usdt REAL,
                            prirost_proc REAL,
                            speed_day_usdt REAL,
                            speed_day_proc REAL,
                            speed_hour_usdt REAL,
                            speed_hour_proc REAL,
                            level_1000 REAL,
                            level_10000 REAL,
                            level_100000 REAL,
                            bolee_100_in_day REAL   
                        )''')
    conn.commit()

    c.execute('''CREATE TABLE IF NOT EXISTS live_fund
                     (id INTEGER PRIMARY KEY AUTOINCREMENT,
                        date_ms INTEGER NOT NULL DEFAULT 0,
                        sym TEXT,
                        fund REAL,
                        next INTEGER
                    )''')
    conn.commit()

    c.execute('''CREATE TABLE IF NOT EXISTS close_sym
                         (id INTEGER PRIMARY KEY AUTOINCREMENT,
                            date_ms INTEGER NOT NULL DEFAULT 0,
                            sym TEXT,
                            side TEXT,
                            pnl REAL,
                            stop TEXT,
                            akk INTEGER
                        )''')
    conn.commit()

    c.execute('''CREATE TABLE IF NOT EXISTS open_sym
                             (id INTEGER PRIMARY KEY AUTOINCREMENT,
                                date_ms INTEGER NOT NULL DEFAULT 0,
                                sym TEXT,
                                side TEXT,
                                size REAL,
                                akk INTEGER
                            )''')
    conn.commit()

    c.execute('''CREATE TABLE IF NOT EXISTS open_pos
                             (id INTEGER PRIMARY KEY AUTOINCREMENT,
                                date_ms INTEGER NOT NULL DEFAULT 0,
                                sym TEXT,
                                side TEXT,
                                size REAL,
                                fund REAL,
                                akk INTEGER,
                                pnl REAL NOT NULL DEFAULT 0.0, 
                                date_close INTEGER,
                                status TEXT
                            )''')
    conn.commit()

    c.execute('''CREATE TABLE IF NOT EXISTS live_akk_usdt
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date_ms INTEGER NOT NULL DEFAULT 0,
                    pnl REAL NOT NULL DEFAULT 0.0,
                    ballance REAL NOT NULL DEFAULT 0.0,
                    wallet REAL NOT NULL DEFAULT 0.0,
                    akk_m REAL NOT NULL DEFAULT 0.0,
                    akk1_m REAL NOT NULL DEFAULT 0.0,
                    akk2_m REAL NOT NULL DEFAULT 0.0,
                    akk3_m REAL NOT NULL DEFAULT 0.0,
                    akk4_m REAL NOT NULL DEFAULT 0.0,
                    ballance1 REAL NOT NULL DEFAULT 0.0,
                    ballance2 REAL NOT NULL DEFAULT 0.0,
                    ballance3 REAL NOT NULL DEFAULT 0.0,
                    ballance4 REAL NOT NULL DEFAULT 0.0
                )''')
    conn.commit()

    c.execute('''CREATE TABLE IF NOT EXISTS step_level
                     (id INTEGER PRIMARY KEY AUTOINCREMENT,
                        step INTEGER,
                        level REAL,
                        akk INTEGER NOT NULL DEFAULT 0
                    )''')
    conn.commit()

    c.execute('''CREATE TABLE IF NOT EXISTS work_all_dop_akk
                     (id INTEGER PRIMARY KEY CHECK (id = 1),
                        akk1_open REAL NOT NULL DEFAULT 0.0,
                        akk1_start REAL NOT NULL DEFAULT 0.0,
                        akk1_old REAL NOT NULL DEFAULT 0.0,
                        akk1_qty REAL NOT NULL DEFAULT 0.0,
                        akk1_reopen_reload REAL NOT NULL DEFAULT 0.0,
                        akk2_open REAL NOT NULL DEFAULT 0.0,
                        akk2_start REAL NOT NULL DEFAULT 0.0,
                        akk2_old REAL NOT NULL DEFAULT 0.0,
                        akk2_qty REAL NOT NULL DEFAULT 0.0,
                        akk2_reopen_reload REAL NOT NULL DEFAULT 0.0,
                        akk3_open REAL NOT NULL DEFAULT 0.0,
                        akk3_start REAL NOT NULL DEFAULT 0.0,
                        akk3_old REAL NOT NULL DEFAULT 0.0,
                        akk3_qty REAL NOT NULL DEFAULT 0.0,
                        akk3_reopen_reload REAL NOT NULL DEFAULT 0.0,
                        akk4_open REAL NOT NULL DEFAULT 0.0,
                        akk4_start REAL NOT NULL DEFAULT 0.0,
                        akk4_old REAL NOT NULL DEFAULT 0.0,
                        akk4_qty REAL NOT NULL DEFAULT 0.0,
                        akk4_reopen_reload REAL NOT NULL DEFAULT 0.0
                    )''')
    conn.commit()

    c.execute('''CREATE TABLE IF NOT EXISTS work_dop_akk
                 (id INTEGER PRIMARY KEY CHECK (id = 1),
                    akk1_start REAL NOT NULL DEFAULT 0.0,
                    akk1_old REAL NOT NULL DEFAULT 0.0,
                    akk2_start REAL NOT NULL DEFAULT 0.0,
                    akk2_old REAL NOT NULL DEFAULT 0.0,
                    akk3_start REAL NOT NULL DEFAULT 0.0,
                    akk3_old REAL NOT NULL DEFAULT 0.0,
                    akk4_start REAL NOT NULL DEFAULT 0.0,
                    akk4_old REAL NOT NULL DEFAULT 0.0
                )''')
    conn.commit()

    c.execute(f'''
                CREATE TABLE IF NOT EXISTS for_open (
                    id INTEGER PRIMARY KEY,
                    symbols TEXT
                )
            ''')
    conn.commit()


    conn.close()

def enable_wal_mode():
    """Включает WAL-режим для базы данных."""
    try:
        conn = sqlite3.connect(DB_FILE, timeout=10)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA cache_size=-20000")  # 20MB кэша
        conn.close()
        print("WAL-режим включён")
    except Exception as e:
        print(f"Ошибка включения WAL: {e}")

def decide_setup(symbol, fund_value,
                 min_total=5, min_group=3):
    """
    Определяет лучший сетап для реального счёта на основе накопленной статистики.

    Аргументы:
        symbol (str): Торгуемая пара.
        fund_value (float): Текущее значение ставки финансирования (%).
        min_total (int): Минимальное общее число сделок по символу.
        min_group (int): Минимальное число сделок для каждого сетапа в группе.

    Возвращает:
        int (0-3) – лучший сетап, или None, если данных недостаточно.
    """
    conn = sqlite3.connect(DB_FILE)
    fund_sign = -1 if fund_value < 0 else (1 if fund_value > 0 else 0)
    abs_fund = abs(fund_value)
    bin_idx = min(int(abs_fund / 0.05), 16)

    conn.execute('DROP TABLE IF EXISTS temp_matched')
    conn.execute('''
        CREATE TEMP TABLE temp_matched AS
        WITH
        open_close AS (
            SELECT
                o.id AS open_id,
                o.date_ms AS open_time,
                o.sym,
                o.side,
                o.akk,
                o.size,
                c.date_ms AS close_time,
                c.pnl,
                c.stop
            FROM open_sym o
            JOIN close_sym c
                ON o.sym = c.sym
                AND o.side = c.side
                AND o.akk = c.akk
                AND c.date_ms > o.date_ms
        ),
        earliest_close AS (
            SELECT open_id, MIN(close_time) AS close_time
            FROM open_close
            GROUP BY open_id
        ),
        open_close_filtered AS (
            SELECT oc.*
            FROM open_close oc
            JOIN earliest_close ec
                ON oc.open_id = ec.open_id AND oc.close_time = ec.close_time
            WHERE oc.sym = ?
        ),
        fund_ranked AS (
            SELECT
                lf.sym,
                lf.fund,
                lf.date_ms,
                of.open_id,
                ROW_NUMBER() OVER (
                    PARTITION BY of.open_id
                    ORDER BY ABS(lf.date_ms - of.open_time)
                ) AS rn
            FROM live_fund lf
            JOIN open_close_filtered of ON lf.sym = of.sym
        )
        SELECT
            of.sym,
            of.akk,
            of.pnl,
            of.close_time,
            fr.fund
        FROM open_close_filtered of
        LEFT JOIN fund_ranked fr
            ON of.open_id = fr.open_id AND fr.rn = 1
    ''', (symbol,))
    conn.commit()

    total = conn.execute('SELECT COUNT(*) FROM temp_matched').fetchone()[0]
    if total < min_total:
        conn.execute('DROP TABLE temp_matched')
        conn.close()
        return None

    cursor = conn.execute('''
        SELECT
            akk,
            COUNT(*) AS cnt,
            SUM(pnl) AS total_pnl,
            AVG(pnl) AS avg_pnl,
            SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS win_rate
        FROM temp_matched
        WHERE fund IS NOT NULL
          AND (CASE WHEN fund < 0 THEN -1 WHEN fund > 0 THEN 1 ELSE 0 END) = ?
          AND MIN(CAST(ABS(fund) / 0.05 AS INTEGER), 16) = ?
        GROUP BY akk
        HAVING COUNT(*) >= ?
    ''', (fund_sign, bin_idx, min_group))

    rows = cursor.fetchall()
    conn.execute('DROP TABLE temp_matched')
    conn.close()

    if not rows:
        return None

    # Выбираем сетап с максимальным total_pnl
    best = max(rows, key=lambda x: x[1])   # x[1] = total_pnl
    return best[0]



def get_balance_safe(idx):
    return safe_api_call(idx, get_balance)

def safe_api_call(idx, func, *args, **kwargs):
    max_attempts = 3
    for attempt in range(max_attempts):
        try:

            sess = SESS_ALL[idx]
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

def get_ball(sess, akk):
    global free_main, free_dop_1, free_dop_2, free_dop_3, free_dop_4, pnl
    live_b = None
    try:
        live_wallet = sess.get_wallet_balance(
            accountType="UNIFIED",
            coin="USDT")
        live_b = float(live_wallet['result']['list'][0]['totalMarginBalance'])
        wall_all = float(live_wallet['result']['list'][0]['totalWalletBalance'])


        # Исправлено: проверяем тип и преобразуем в float
        available_balance = live_wallet["result"]["list"][0].get("totalAvailableBalance", 0)
        if isinstance(available_balance, str):
            available_balance = float(available_balance)
        else:
            available_balance = float(available_balance)

        if akk == 0:
            pnl = float(live_wallet['result']['list'][0]['coin'][0]['unrealisedPnl'])
            free_main = available_balance
        elif akk == 1:
            free_dop_1 = available_balance
        elif akk == 2:
            free_dop_2 = available_balance
        elif akk == 3:
            free_dop_3 = available_balance
        elif akk == 4:
            free_dop_4 = available_balance




    except Exception as e:
        print(f"Ошибка в get_ball: {e}")

    return live_b

def read_tp_sl(symbol):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT side, tp, sl FROM akk4_tp_sl WHERE symbol = ?", (symbol,))
    db_rows = c.fetchall()
    conn.close()
    if db_rows is not None:
        for side, tp, sl in db_rows:
            return side, tp, sl
    else:
        return False

def write_tp_sl(symbol, side, tp, sl):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute(
        "INSERT OR REPLACE INTO akk4_tp_sl(symbol, side, tp, sl) VALUES (?, ?, ?, ?)",
        (symbol, side, tp, sl)
    )
    conn.commit()
    conn.close()

def write_fund(sym, fund, next):
    try:
        conn = sqlite3.connect(DB_FILE)
        cur = conn.cursor()
        date_ms_now = int(datetime.now().timestamp() * 1000)
        cur.execute(
            "INSERT INTO live_fund (date_ms, sym, fund, next) VALUES (?,?,?,?)",
            (date_ms_now, str(sym), round(fund, 4), next)
        )
        conn.commit()
        conn.close()
    except Exception as e:
        conn.close()
        print(f"Ошибка в write_fund: {e}")

def read_prognose():
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute(f"SELECT live FROM live_prognoze ORDER BY id DESC LIMIT 1")
        row = c.fetchone()
        conn.close()
        if row is None:
            return False
        #print(row)
        return row[0] #[item[0] for item in row]
    except Exception as e:
        print(f"Ошибка в read_level: {e}")
        return False

def write_prognoze(dot, start, live, day, hour, prirost_usdt, prirost_proc, speed_day_usdt, speed_day_proc, speed_hour_usdt, speed_hour_proc, level_1000, level_10000, level_100000, bolee_100_in_day):
    try:
        conn = sqlite3.connect(DB_FILE)
        cur = conn.cursor()
        date_ms_now = int(datetime.now().timestamp() * 1000)
        cur.execute(
            "INSERT INTO live_prognoze (date_ms, dot, start, live, day, hour, prirost_usdt, prirost_proc, speed_day_usdt, speed_day_proc, speed_hour_usdt, speed_hour_proc, level_1000, level_10000, level_100000, bolee_100_in_day) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (int(date_ms_now), int(dot), float(start), float(live), float(day), float(hour), float(prirost_usdt), float(prirost_proc), float(speed_day_usdt), float(speed_day_proc), float(speed_hour_usdt), float(speed_hour_proc), float(level_1000), float(level_10000), float(level_100000), float(bolee_100_in_day))
        )
        conn.commit()
        conn.close()
    except Exception as e:
        conn.close()
        print(f"Ошибка в write_prognoze: {e}")

def write_fund(sym, fund, next):
    try:
        conn = sqlite3.connect(DB_FILE)
        cur = conn.cursor()
        date_ms_now = int(datetime.now().timestamp() * 1000)
        cur.execute(
            "INSERT INTO live_fund (date_ms, sym, fund, next) VALUES (?,?,?,?)",
            (date_ms_now, str(sym), round(fund, 4), next)
        )
        conn.commit()
        conn.close()
    except Exception as e:
        conn.close()
        print(f"Ошибка в write_fund: {e}")

def write_close(sym, side, pnl, rezult, akk):
    try:
        conn = sqlite3.connect(DB_FILE)
        cur = conn.cursor()
        date_ms_now = int(datetime.now().timestamp() * 1000)
        cur.execute(
            "INSERT INTO close_sym (date_ms, sym, side, pnl, stop, akk) VALUES (?,?,?,?,?,?)",
            (date_ms_now, str(sym), str(side), pnl, rezult, akk)
        )
        conn.commit()
        conn.close()
    except Exception as e:
        conn.close()
        print(f"Ошибка в write_close: {e}")

def write_open(sym, side, size, akk):
    try:
        conn = sqlite3.connect(DB_FILE)
        cur = conn.cursor()
        date_ms_now = int(datetime.now().timestamp() * 1000)
        cur.execute(
            "INSERT INTO open_sym (date_ms, sym, side, size, akk) VALUES (?,?,?,?,?)",
            (date_ms_now, str(sym), str(side), size, akk)
        )
        conn.commit()
        conn.close()
    except Exception as e:
        conn.close()
        print(f"Ошибка в write_open: {e}")

def write_pos(sym, side, size, fund, akk, work, pnl, stat):
    date_ms_now = int(datetime.now().timestamp() * 1000)
    try:
        if work:
            conn = sqlite3.connect(DB_FILE)
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO open_pos (date_ms, sym, side, size, akk, fund) VALUES (?,?,?,?,?,?)",
                (date_ms_now, str(sym), str(side), float(size), akk, fund)
            )
            conn.commit()
            conn.close()
    except Exception as e:
        print(f"ОТКРЫТИЕ Ошибка в write_pos: {e} ")

    try:
        if not work:
            conn = sqlite3.connect(DB_FILE)
            c = conn.cursor()
            c.execute(
                """UPDATE open_pos 
                   SET pnl = ?, date_close = ?, status = ? 
                   WHERE sym = ? AND akk = ? AND side = ? AND status IS NULL""",
                (pnl, date_ms_now, stat, sym, akk, side)
            )
            conn.commit()
            conn.close()
    except Exception as e:
        print(f"ЗАКРЫТИЕ Ошибка в write_pos: {e} ")

    return


def write_live_all(live_b, wall_all, akk_m, akk1_m, akk2_m, akk3_m, akk4_m, ballance1, ballance2, ballance3, ballance4, key_start):
    global pnl
    if pnl != 0 or key_start:
        try:
            conn = sqlite3.connect(DB_FILE)
            cur = conn.cursor()
            date_ms_now = int(datetime.now().timestamp() * 1000)
            cur.execute(
                "INSERT INTO live_akk_usdt (date_ms, pnl, ballance, wallet, akk_m, akk1_m, akk2_m, akk3_m, akk4_m, ballance1, ballance2, ballance3, ballance4) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (date_ms_now, round(pnl, 2), round(live_b, 2), round(wall_all, 2),   round(akk_m, 2), round(akk1_m, 2), round(akk2_m, 2), round(akk3_m, 2), round(akk4_m, 2), round(ballance1, 2), round(ballance2, 2), round(ballance3, 2), round(ballance4, 2))
            )
            conn.commit()
            conn.close()
        except Exception as e:
            conn.close()
            print(f"Ошибка в write_live: {e}")

def write_step_level(step_n, level_s):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO step_level (step, level) VALUES (?, ?)",
                (step_n, level_s)
            )
    except sqlite3.Error as e:
        print(f"Ошибка в write_step_level: {e}")

def clear_step_level():
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cur = conn.cursor()
            cur.execute("DELETE FROM step_level")
            cur.execute("DELETE FROM sqlite_sequence WHERE name='step_level'")
    except sqlite3.Error as e:
        print(f"Ошибка при очистке таблицы step_level: {e}")


def analyze_setups():
    """
    Анализирует 4 сетапа (akk = 0..3) и возвращает словарь с результатами.
    """
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    results = {}
    best_akk = None
    best_score = -float('inf')

    for akk in range(4):  # предполагаем, что номера сетапов 0,1,2,3
        # Количество открытых позиций
        cursor.execute("SELECT COUNT(*) FROM open_sym WHERE akk = ?", (akk,))
        open_count = cursor.fetchone()[0]

        # Количество закрытых и суммарный PnL
        cursor.execute("SELECT COUNT(*), SUM(pnl) FROM close_sym WHERE akk = ?", (akk,))
        close_count, total_pnl = cursor.fetchone()
        total_pnl = total_pnl or 0.0  # если SUM вернул NULL, заменяем на 0

        # Дополнительные метрики
        avg_pnl = total_pnl / close_count if close_count > 0 else 0.0

        # Интегральный показатель: прибыль * доля закрытых сделок
        if (open_count + close_count) > 0:
            close_ratio = close_count / (open_count + close_count + 1)  # +1 для устойчивости
        else:
            close_ratio = 0.0
        score = total_pnl * close_ratio

        # Сохраняем данные
        results[akk] = {
            'open_count': open_count,
            'close_count': close_count,
            'total_pnl': round(total_pnl, 2),
            'avg_pnl': round(avg_pnl, 2),
            'close_ratio': round(close_ratio, 4),
            'score': round(score, 2)
        }

        # Обновляем лучший
        if score > best_score:
            best_score = score
            best_akk = akk

    conn.close()

    # Формируем итоговый ответ
    if best_akk is not None:
        best_info = results[best_akk]
        reason = (
            f"Сетап {best_akk} имеет наивысший показатель score = {best_info['score']:.2f}, "
            f"обусловленный суммой PnL = {best_info['total_pnl']:.2f} и долей закрытых сделок = {best_info['close_ratio']:.2%} "
            f"(открыто: {best_info['open_count']}, закрыто: {best_info['close_count']})."
        )
        return {
            'best_akk': best_akk,
            'best_score': best_info['score'],
            'details': results,
            'reason': reason
        }
    else:
        return {'error': 'Нет данных для анализа'}

def fine_setup():
    try:
        setup = {}
        conn = sqlite3.connect(DB_FILE)
        for i in range(1, 5):
            c = conn.cursor()
            c.execute(f"SELECT SUM(pnl) FROM close_sym WHERE akk = {i}")
            setup[str(i)] = c.fetchone()[0]  # если нет строк, вернёт None или 0 – обработайте по необходимости
            print(f'SUM setup[{str(i)}] = {setup[str(i)]}')
        conn.close()
        sorted_keys = sorted(setup, key=setup.get, reverse=True)   # получаем список ключей, отсортированных по значениям
        first_key = sorted_keys[0]                         # первый ключ
        print(first_key)
    except Exception as e:
        print(f"Ошибка в fine_setup: {e}")
        return False

def fine_syn_is_setup(akk,sym,n=10):
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()

        c.execute("""
                    SELECT SUM(pnl) FROM (
                        SELECT pnl 
                        FROM close_sym 
                        WHERE akk = ? AND sym = ? 
                        ORDER BY date_ms DESC 
                        LIMIT ?
                    )
                """, (akk, sym, n))

        total_pnl = c.fetchone()[0]
        conn.close()
        if total_pnl is None:
            #print(f'Символ {sym}, сетап {akk}: нет сделок')
            return 0
        else:
            #print(f'SYM: {sym} AKK-{akk} total_pnl: {total_pnl:.2f}')
            return total_pnl  # None если нет записей
    except Exception as e:
        print(f"Ошибка в get_total_pnl_for_symbol: {e}")
        return False

def read_level_ok(sym,akk):
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute(
            "SELECT sym FROM open_sym WHERE sym = ? AND akk = ? ORDER BY date_ms DESC LIMIT 1",
            (sym, akk)
        )
        rows = c.fetchall()  # получит последние 5 записей (самые свежие по date_ms)
        conn.close()
        return [row[0] for row in rows]
    except Exception as e:
        print(f"Ошибка в read_five_last_side: {e}")
        return False


def read_return_comsa(key):
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()

        # Получаем количество закрытых сделок
        c.execute("SELECT COUNT(*) FROM close_sym")
        close_count = c.fetchone()[0]

        # Получаем количество открытых сделок
        c.execute("SELECT COUNT(*) FROM open_sym")
        open_count = c.fetchone()[0]

        conn.close()

        # Расчёт возврата комиссии (40% от 0.055% комиссии)
        # 79 - средний размер позиции в USDT
        # 0.00055 - комиссия 0.055% (0.00055 в десятичном виде)
        # 0.4 - возврат 40% комиссии
        total_trades = close_count + open_count
        avg_position_size = 79  # USDT
        fee_rate = 0.00055  # 0.055%
        refund_rate = 0.4  # 40%

        total_fee = total_trades * avg_position_size * fee_rate
        refund = total_fee * refund_rate
        if key:
            print(f'\nТорговый объем: {int(total_trades * avg_position_size)}$\n'
                  f'Удержанная комиссию: {round(total_fee,2)}$\n'
                  f'К возврату: {round(refund, 2)}$\n')
        return round(refund, 2)

    except Exception as e:
        print(f"Ошибка в read_return_comsa: {e}")
        return False

def read_five_last_side(sym,akk):
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute(
            "SELECT side FROM close_sym WHERE sym = ? AND akk = ? AND pnl < 0 ORDER BY date_ms DESC LIMIT 3",
            (sym, akk)
        )
        rows = c.fetchall()  # получит последние 5 записей (самые свежие по date_ms)
        conn.close()
        return [row[0] for row in rows]
    except Exception as e:
        print(f"Ошибка в read_five_last_side: {e}")
        return False

def read_key_new(tab, key):
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute(f"SELECT {key} FROM {tab} WHERE id = 1")
        row = c.fetchone()
        conn.close()
        if row is None:
            return False
        return row[0]
    except Exception as e:
        print(f"Ошибка в read_key_new: {e}")
        return False

def write_key_new(tab, key, value):
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute(f"UPDATE {tab} SET {key} = ? WHERE id = 1", (value,))
        if c.rowcount == 0:
            c.execute(
                f"INSERT OR REPLACE INTO {tab} (id, {key}) VALUES (1, ?)",
                (value,)
            )
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Ошибка в write_key_new: {e}")
        return False

def upsert_funding_trade_update_pnl(conn, symbol, side, pnl):
    c = conn.cursor()
    c.execute(
        "UPDATE funding_trades SET live_pnl = ? WHERE symbol = ? AND side = ?",
        (pnl, symbol, side)
    )
    conn.commit()

def upsert_funding_trade(conn, symbol, side, qty, open_time, qty_first, pnl):
    c = conn.cursor()

    # Преобразуем Decimal в float
    if isinstance(qty, Decimal):
        qty = float(qty)
    if isinstance(qty_first, Decimal):
        qty_first = float(qty_first)
    #write_open(symbol, side, float(qty),1)
    c.execute(
        '''INSERT INTO funding_trades(symbol, side, qty, open_time, qty_first, live_pnl)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(symbol, side)
        DO UPDATE SET qty = qty + excluded.qty,
                        open_time = excluded.open_time''',
        (symbol, side, qty, open_time, qty_first, pnl)
    )
    conn.commit()

def update_coins_old(sess):
    """
    Обновляет список торговых пар linear USDT в all_coins_one.py.
    Выполняется раз в сутки в 22:22.
    """
    try:
        resp = sess.get_instruments_info(category="linear")
        if resp['retCode'] != 0:
            print(f"Ошибка получения списка инструментов: {resp}")
            return

        # Фильтруем торгуемые USDT пары
        usdt_symbols = []
        for item in resp['result']['list']:
            symbol = item['symbol']
            if symbol.endswith('USDT') and item.get('status') == 'Trading':
                usdt_symbols.append(symbol)

        if not usdt_symbols:
            print("Не найдено торгуемых USDT пар")
            return

        # Сортируем для консистентности
        usdt_symbols.sort()

        print(f"Обновлён список монет: {len(usdt_symbols)} пар")

        # Обновляем глобальный список в памяти
        return usdt_symbols

    except Exception as e:
        print(f"Ошибка обновления списка монет: {e}")

def update_coins(sess):
    """
    Обновляет список торговых пар linear USDT.
    Исключает:
    - Делистинговые пары (статус Delisting, Suspended, Halted)
    - Токены с постфиксами BULL, BEAR, 3L, 3S (леверидж-токены)
    - Пары с USDC (это не USDT фьючерсы)
    """
    try:
        resp = sess.get_instruments_info(category="linear")
        if resp['retCode'] != 0:
            print(f"Ошибка получения списка инструментов: {resp}")
            return

        # Статусы, при которых НЕЛЬЗЯ торговать
        inactive_statuses = ['Delisting', 'Suspended', 'Halted', 'Pre-delisting', 'Unlisted']
        
        # Нежелательные постфиксы (леверидж-токены и т.д.)
        exclude_postfixes = ['BULL', 'BEAR', '3L', '3S', 'USDC']
        
        # Статусы, при которых можно торговать
        active_statuses = ['Trading', 'Pre-Listing']

        usdt_symbols = []
        excluded_count = 0
        
        for item in resp['result']['list']:
            symbol = item['symbol']
            status = item.get('status', '')
            
            # Проверяем, что это USDT пара
            if not symbol.endswith('USDT'):
                continue
            
            # Проверяем на нежелательные постфиксы
            if any(x in symbol for x in exclude_postfixes):
                excluded_count += 1
                continue
            
            # Проверяем статус
            if status in inactive_statuses:
                print(f"⚠️ Исключён {symbol}: статус '{status}'")
                excluded_count += 1
                continue
            
            # Добавляем только активные пары
            if status in active_statuses:
                usdt_symbols.append(symbol)
            else:
                # Для неизвестных статусов - добавляем с предупреждением
                print(f"⚠️ Неизвестный статус '{status}' для {symbol}, добавляем с осторожностью")
                usdt_symbols.append(symbol)

        if not usdt_symbols:
            print("Не найдено активных торгуемых USDT пар")
            return

        # Сортируем для консистентности
        usdt_symbols.sort()

        print(f"✅ Обновлён список монет: {len(usdt_symbols)} пар (исключено: {excluded_count})")
        
        # Дополнительно: выводим первые 5 пар для контроля
        # if usdt_symbols:
        #     print(f"   Примеры: {', '.join(usdt_symbols[:5])}...")

        return usdt_symbols

    except Exception as e:
        print(f"❌ Ошибка обновления списка монет: {e}")
        return

def update_coins_new(sess): # без акций
    """
    Обновляет список торговых пар linear USDT.
    Исключает:
    - Делистинговые пары
    - Леверидж-токены (BULL, BEAR, 3L, 3S, USDC)
    - Пары типа 'stock' и 'innovation' (акции и инновационные токены, которые могут не поддерживать Market ордера или требовать спецсоглашения)
    """
    try:
        resp = sess.get_instruments_info(category="linear")
        if resp['retCode'] != 0:
            print(f"Ошибка получения списка инструментов: {resp}")
            return

        inactive_statuses = ['Delisting', 'Suspended', 'Halted', 'Pre-delisting', 'Unlisted']
        exclude_postfixes = ['BULL', 'BEAR', '3L', '3S', 'USDC']
        # Исключаем типы символов, которые не подходят для торговли (например, акции и инновационные)
        exclude_symbol_types = ['stock', 'innovation']

        usdt_symbols = []
        excluded_count = 0
        excluded_reasons = {}

        for item in resp['result']['list']:
            symbol = item['symbol']
            status = item.get('status', '')
            symbol_type = item.get('symbolType', '')

            if not symbol.endswith('USDT'):
                continue
            if any(x in symbol for x in exclude_postfixes):
                excluded_count += 1
                excluded_reasons['postfix'] = excluded_reasons.get('postfix', 0) + 1
                continue
            if status in inactive_statuses:
                excluded_count += 1
                excluded_reasons['status'] = excluded_reasons.get('status', 0) + 1
                continue
            if symbol_type in exclude_symbol_types:
                excluded_count += 1
                excluded_reasons['symbol_type'] = excluded_reasons.get('symbol_type', 0) + 1
                continue

            usdt_symbols.append(symbol)

        if not usdt_symbols:
            print("Не найдено активных торгуемых USDT пар")
            print(f"Причины исключений: {excluded_reasons}")
            return

        usdt_symbols.sort()
        print(f"✅ Обновлён список монет: {len(usdt_symbols)} пар (исключено: {excluded_count})")
        return usdt_symbols

    except Exception as e:
        print(f"❌ Ошибка обновления списка монет: {e}")
        return #, №

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
    return resp['result']['list'][0]  # fundingRate, nextFundingTime, lastPrice и т.д. [web:11]

def get_upcoming_funding_info(sess, symbol):
    """
    Возвращает (fundingRate, nextFundingTime_ms) для символа.
    """
    ticker = get_symbol_ticker(sess, symbol)
    if ticker is None:
        return None
    funding_rate = float(ticker.get('fundingRate', 0.0))  # напр. 0.005 = 0.5% [web:11][web:12]
    next_funding_time_ms = int(ticker.get('nextFundingTime', 0))  # ms [web:11]
    return funding_rate, next_funding_time_ms


def get_max_leverage_for_symbol(sess, symbol):
    """
    Получает максимально возможное плечо для символа.
    Возвращает целое число (например, 50, 75, 100).
    """
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

def get_qty_filters(sess, symbol):
    """
    Возвращает (min_qty, qty_step) для символа из get_instruments_info. [web:14]
    """
    resp = sess.get_instruments_info(category="linear", symbol=symbol)
    if resp['retCode'] != 0 or not resp['result']['list']:
        raise RuntimeError(f"get_instruments_info error for {symbol}: {resp}")

    inst = resp['result']['list'][0]
    lot = inst.get('lotSizeFilter', {})
    min_qty = float(lot.get('minOrderQty', 0.0))
    qty_step = float(lot.get('qtyStep', 0.0))
    return min_qty, qty_step

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
    global LEVEL_POS
    if sess is None:
        sess = HTTP(testnet=False, demo=False)

    # Получаем фильтры по инструменту
    try:
        resp = sess.get_instruments_info(category="linear", symbol=symbol)
        if resp['retCode'] != 0:
            return False
            #raise RuntimeError(f"Ошибка получения инструментов: {resp['retMsg']}")
        lot = resp['result']['list'][0]['lotSizeFilter']
        min_qty = float(lot['minOrderQty'])
        qty_step = float(lot['qtyStep'])
    except Exception as e:
        return False
        #raise RuntimeError(f"Не удалось получить фильтры для {symbol}: {e}")

    # Сырое количество
    raw_qty = (usdt / price) * LEVEL_POS
    #raw_qty = (usdt / price)

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
        #raise RuntimeError(f"Не удалось достичь минимальной стоимости для {symbol}")
        return False

    # Форматируем строку с точностью, определяемой qtyStep
    precision = abs(Decimal(str(qty_step)).as_tuple().exponent)
    qty_str = f"{qty:.{precision}f}"

    return qty_str, qty, qty_step

def quantize_qty(raw_qty, min_qty, qty_step):
    """
    Приводит raw_qty к допустимому значению: кратно шагу и не меньше минимума. [web:14][web:23]
    """
    if qty_step <= 0:
        return Decimal('0')
    raw_dec = Decimal(str(raw_qty))
    step_dec = Decimal(str(qty_step))
    min_dec = Decimal(str(min_qty))
    steps = (raw_dec / step_dec).to_integral_value(rounding=ROUND_DOWN)
    qty_dec = steps * step_dec
    if qty_dec < min_dec:
        return Decimal('0')
    return qty_dec

def format_qty_for_api_last(qty, qty_step):
    """Возвращает строку qty, корректную для Bybit API."""
    qty_dec = Decimal(qty)
    step_dec = Decimal(str(qty_step))
    exp = max(-step_dec.as_tuple().exponent, 0)
    qty_str = format(qty_dec.normalize(), 'f')
    if '.' in qty_str:
        integer_part, frac = qty_str.split('.')
        if len(frac) > exp:
            qty_str = f"{qty_dec.quantize(step_dec, rounding=ROUND_DOWN):f}"
    return qty_str

def format_qty_for_api(qty, qty_step):
    """
    Возвращает строку qty, корректную для Bybit API.
    
    Args:
        qty: количество (float или Decimal)
        qty_step: шаг количества (float)
    
    Returns:
        str: отформатированное количество с правильным количеством знаков
    """
    qty_dec = Decimal(str(qty))  # Всегда преобразуем через строку!
    step_dec = Decimal(str(qty_step))
    
    # Определяем количество знаков после запятой
    exp = max(-step_dec.as_tuple().exponent, 0)
    
    # Округляем вниз до нужного количества знаков
    qty_rounded = qty_dec.quantize(step_dec, rounding=ROUND_DOWN)
    
    # Форматируем с фиксированной точностью
    if exp > 0:
        qty_str = f"{qty_rounded:.{exp}f}"
    else:
        qty_str = f"{qty_rounded:.0f}"
    
    return qty_str

# ---------- ПРОВЕРКА УСЛОВИЙ ФАНДИНГА (БЕЗ ИСТОРИИ) ----------

def check_funding_for_next_hour(sess, symbol):
    """
    Проверяет, имеет ли смысл работать с монетой по фандингу в ближайший (следующий) час.
    Историю фандинга не используем — смотрим только предстоящее значение fundingRate и наличие nextFundingTime.

    Условия:
    - Есть nextFundingTime.
    - |fundingRate| >= MIN_ABS_RATE (0.1% и более).
    Возвращает (info, reason), где info - dict при успехе, reason - строка при отказе.
    """
    info = get_upcoming_funding_info(sess, symbol)
    if info is None:
        return None, 'нет тикера или данных по монете'
    upcoming_rate, next_funding_time_ms = info
    if next_funding_time_ms == 0:
        return None, 'нет nextFundingTime'

    if abs(upcoming_rate * 100) < MIN_ABS_RATE or abs(upcoming_rate * 100) > MAX_ABS_RATE:
        return None, f'rate {upcoming_rate * 100:.4f}% ниже MIN_ABS_RATE={MIN_ABS_RATE * 100:.4f}%'
    write_fund(symbol, upcoming_rate * 100.0, next_funding_time_ms)

    # Выбираем сторону, которая НЕ ПОЛУЧАЕТ фандинг: (идем против толпы в ожидании разворота движения цены после сбора фандинга )
    if upcoming_rate > 0:
        side = 'Sell'
    else:
        side = 'Buy'



    return {
        'symbol': symbol,
        'side': side,
        'rate': upcoming_rate,
        'rate_percent': upcoming_rate * 100.0,
        'next_funding_time_ms': next_funding_time_ms,
    }, None

# ---------- ПОДГОТОВКА СПИСКА МОНЕТ НА ЧАС ----------

def prepare_funding_list(sess, coins):
    """
    На 55-й минуте UTC: составляет список монет, у которых в ближайшем часу будет фандинг
    с |fundingRate| >= MIN_ABS_RATE. Историю не трогаем, только предстоящее значение.
    """
    global prepared_trades_for_hour, prepared_hour

    now_utc = datetime.now(timezone.utc)
    current_hour = now_utc.hour
    prepared_hour = current_hour

    prepared_trades_for_hour = []

    print(f"[{now_utc.isoformat()}] Подготовка списка монет с фандингом в ближайший час...")

    total = 0
    accepted = 0
    rejected = 0
    rejection_reasons = {}
    #all_positions_main = get_all_positions(sess, -1)
    #if len(all_positions_main) > 0:
        #open_pos = {}
        #two_sym = []
        #for row in all_positions_main:

            #symbol = row['symbol']
            # if symbol in two_sym:
            #     del open_pos[symbol]
            #     continue
            # two_sym.append(symbol)
            #side = row['side']
            #qty = float(row['size'])
            #open_pos[symbol] = {'side': side, 'qty': qty}

    sym_for_hadge = []
    for symbol in list(coins):

        total += 1
        try:
            info, reason = check_funding_for_next_hour(sess, symbol)
            if info is not None:
                accepted += 1
                prepared_trades_for_hour.append(info)
                # next_time = datetime.fromtimestamp(
                #     info['next_funding_time_ms'] / 1000.0,
                #     timezone.utc
                # )

                print(
                    f"  - {symbol} {info['side']} "
                    #f"next_funding={next_time.isoformat()} "
                    f"rate={info['rate_percent']:.4f}% ✓"
                )
                # try:
                #
                #     print(
                #         f"live funding - {symbol} {info['side']} {info['rate_percent']:.4f}% | find open {open_pos[symbol]['side']}")

                    # if info['side'] == open_pos[symbol]['side']:
                    #
                    #     sym_for_hadge.append(symbol)
                    #     # open_one_hadge(sess, symbol, open_pos[symbol])
                    #     print(
                    #         f"live funding - {symbol} FOR HADGE {info['side']} {info['rate_percent']:.4f}% | find open {open_pos[symbol]['side']}")
                    #
                    # else:
                    #     print(
                    #         f"live funding - {symbol} {info['side']} {info['rate_percent']:.4f}% | find open {open_pos[symbol]['side']}")


                # except:
                #     pass

            else:
                rejected += 1
                rejection_reasons[reason] = rejection_reasons.get(reason, 0) + 1
                if reason in ('нет тикера или данных по монете', 'нет nextFundingTime'):
                    coins.remove(symbol)
        except Exception as e:
            rejected += 1
            rejection_reasons['exception'] = rejection_reasons.get('exception', 0) + 1
            print(f"  - {symbol}: ошибка {e}")

    prepared_trades_clearned = len_for_real_open()
    now_moment = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
    if len(prepared_trades_clearned) > 0 and len(prepared_trades_clearned[2]) > 0:
        for_print_send = f"{now_moment}\nОбработано монет: {total}\nК открытию: {prepared_trades_clearned[0]}{prepared_trades_clearned[1]}"
        #send_tg(for_print_send)
        #print(for_print_send)
        prepared_trades_for_hour = prepared_trades_clearned[2]
        return prepared_trades_clearned[2]
    else:
        print(f'{now_moment} Нет подходящих пар')
        #send_tg(f'{now_moment} Нет подходящих пар')
        return []

# ---------- ОТКРЫТИЕ ПОДГОТОВЛЕННЫХ СДЕЛОК ----------



def open_one_hadge(sess,symbol, info):
    global gsess1, gsess2, gsess3, gsess4, all_pos_akk1, all_pos_akk2, all_pos_akk3, all_pos_akk4, sess1, sess2, sess3, sess4
    all_akk = [[all_pos_akk1, gsess1, 1], [all_pos_akk2, gsess2, 2], [all_pos_akk3, gsess3, 3],
               [all_pos_akk4, gsess4, 4]]
    try:
        side_one = "Sell" if info['side'] == "Buy" else "Buy"
        idx = 1 if side_one == "Buy" else 2
        qty = str(info['qty'])

        order = sess.place_order(
            category="linear",
            symbol=symbol,
            side=side_one,
            orderType="Market",
            qty=qty,
            positionIdx=idx
        )

        if order['retCode'] == 0:
            print(
                f"[OPEN HADGE POS] {symbol} {side_one} qty={qty:.8f}"

            )
            opened_count += 1
        else:
            print(f"Ошибка размещения HADGE ордера по {symbol}: {order}")
    except Exception as e:
        print(f"Ошибка в open_one_hadge: {e}")

    if opened_count > 0:
        # Получаем цену
        ticker = get_symbol_ticker(sess, symbol)
        if ticker is not None:
            last_price = float(ticker['lastPrice'])
            write_open(symbol, side_one, float(qty) * last_price, 0)

        #sync_db_positions(sess)
        now_moment = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
        txt = f"{now_moment}\n\nОткрыта ХЭДЖ позиция: {symbol} (смена направления фандинга)\n"
        for rows in all_akk:
            if len(rows[0]) > 0:
                for sym in rows[0]:
                    if symbol == sym['symbol']:
                        order1 = rows[1].place_order(
                            category="linear",
                            symbol=symbol,
                            side=side_one,
                            orderType="Market",
                            qty=str(sym['size']),
                            positionIdx=idx
                        )

                        if order1['retCode'] == 0:
                            if ticker is not None:
                                write_open(symbol, side_one, float(sym['size']) * last_price, rows[2])
                            print(
                                f"[OPEN HADGE POS] AKK {rows[2]} {symbol} {side_one} qty={str(sym['size']):.8f}"
                            )
                            txt += f"Открыта ХЭДЖ позиция AKK {rows[2]}: {symbol} (смена направления фандинга)\n"
        now_moment = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
        send_tg(txt)
        #send_tg_crypta(f'✅ Funding monitor REAL\n{txt}')
        print(txt)


def calculate_take_profit_advanced(
        usdt,
        current_price,
        symbol,
        side,
        desired_profit_usdt,
        leverage=None,
        position_value=None,
        fee_rate=0.00055,  # 0.055% комиссия taker
        include_fee=True
):
    """
    Расширенная версия с учётом комиссий и возможности задать риск.

    Параметры:
        symbol (str): Торговый символ
        side (str): 'Buy' или 'Sell'
        desired_profit_usdt (float): Желаемая чистая прибыль (после комиссий)
        leverage (float): Текущее плечо (если None - получит из API)
        position_value (float): Стоимость позиции (если None - использует баланс)
        fee_rate (float): Ставка комиссии (по умолчанию 0.00055 = 0.055%)
        include_fee (bool): Учитывать ли комиссии в расчёте

    Возвращает:
        dict: {
            'tp_price': float,          # Цена TP
            'tp_pnl': float,            # Ожидаемый PnL
            'position_size': float,     # Размер позиции
            'margin': float,            # Маржа
            'required_move_pct': float  # Необходимое движение в %
        } или None при ошибке
    """
    try:
        # Стоимость позиции в USDT
        #position_value = qty * current_price
        position_value = usdt


        session = HTTP(testnet=True)




        # Учитываем комиссии
        if include_fee:
            # Комиссия при входе
            entry_fee = position_value * fee_rate
            # Необходимая прибыль с учётом комиссии на вход и выход
            total_fee = entry_fee * 2  # вход + выход
            required_gross_profit = desired_profit_usdt + total_fee
        else:
            required_gross_profit = desired_profit_usdt

        # Расчёт изменения цены
        price_change_pct = required_gross_profit / position_value

        if side == 'Buy':
            tp_price = current_price * (1 + price_change_pct)
        else:
            tp_price = current_price * (1 - price_change_pct)

        # Округление до tick_size
        tick_size = 0.01
        try:
            inst_info = session.get_instruments_info(category="linear", symbol=symbol)
            if inst_info['retCode'] == 0 and inst_info['result']['list']:
                tick_size = float(inst_info['result']['list'][0]['priceFilter']['tickSize'])
        except:
            pass
        tp_price = round(tp_price / tick_size) * tick_size

        # Расчёт фактического PnL при TP
        if side == 'Buy':
            actual_pnl = position_value * ((tp_price - current_price) / current_price)
        else:
            actual_pnl = position_value * ((current_price - tp_price) / current_price)

        if include_fee:
            net_pnl = actual_pnl - (position_value * fee_rate * 2)
        else:
            net_pnl = actual_pnl

        result = {
            'tp_price': tp_price,
            'tp_pnl': actual_pnl,
            'net_pnl': net_pnl,
            'position_value': position_value,
            'margin': position_value / leverage,
            'leverage': leverage,
            'required_move_pct': price_change_pct * 100,
            'current_price': current_price
        }

        # Вывод информации
        print(f"\n📊 Расчёт Take Profit для {symbol}:")
        print(f"  Текущая цена:        {current_price:.4f}")
        print(f"  Направление:         {'Long' if side == 'Buy' else 'Short'}")
        print(f"  Плечо:               {leverage}x")
        print(f"  Размер позиции:      {position_value:.2f} USDT")
        print(f"  Маржа:               {result['margin']:.2f} USDT")
        print(f"  Желаемая прибыль:    {desired_profit_usdt:.2f} USDT")
        if include_fee:
            print(f"  Комиссии:            {position_value * fee_rate * 2:.2f} USDT")
            print(f"  Валовая прибыль:     {required_gross_profit:.2f} USDT")
        print(f"  Необходимое движение: {price_change_pct * 100:.2f}%")
        print(f"  Цена Take Profit:    {tp_price:.4f}")
        print(f"  Ожидаемый PnL:       {result['net_pnl']:.2f} USDT")

        return result

    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return None

def open_prepared_funding_trades():
    global live_b, live_b_akk1, live_b_akk2, live_b_akk3, live_b_akk4, live_open_coins, gsess0, gsess1, gsess2, gsess3, gsess4, all_pos_akk1, all_pos_akk2, all_pos_akk3, all_pos_akk4, sess1, sess2, sess3, sess4
    all_akk = [[gsess0, 0], [gsess1, 1], [gsess2, 2], [gsess3, 3], [gsess4, 4]]

    global SETUP0, SETUP1, SETUP2, SETUP3, SETUP4, prepared_trades_for_hour, XXX, XXX_dop_4, last_open_sym, RISK_SYMBOL_USD, coins


    # if not prepared_trades_for_hour:
    #     print(f'prepared_trades_for_hour {prepared_trades_for_hour}')
    #     return

    init_db()
    # last_open_sym = []
    # with open('last_open.txt', 'w') as f:
    #     json.dump(last_open_sym, f)
    open_sym = ''

    opened_count = 0
    old_len_pos = len(get_all_positions(sess, -1))
    #print(f'prepared_trades_for_hour {prepared_trades_for_hour}')
    error_add = False
    # find_best_by_last_trades_detailed(
    #     min_total=10,
    #     min_pnl_threshold=0,
    #     require_min_win_rate=None
    # )
    try:
        rez = find_best_setup(
            window=10,
            min_trades=10,
            min_win_rate=0,
            min_tp_sum=0
        )
    except:
        pass
    # try:
    #     find_best_setup(
    #         window=20,
    #         min_trades=20,
    #         min_win_rate=0,
    #         min_tp_sum=0
    #     )
    # except:
    #     pass

    for info in prepared_trades_for_hour:
        # if len(prepared_trades_for_hour) == 1 and info['symbol'] == 'LABUSDT':
        #     break
        #ry:
        symbol = info['symbol']

        # # Получаем максимальное плечо и устанавливаем его

        max_leverage = get_max_leverage_for_symbol(sess, symbol)

        # Получаем цену
        ticker = get_symbol_ticker(sess, symbol)
        if ticker is None:
            print(f"Пропускаем {symbol}: нет тикера при открытии")
            continue

        last_price = float(ticker['lastPrice'])
        if last_price <= 0:
            print(f"Пропускаем {symbol}: некорректная цена {last_price}")
            continue

        # qty_str, qty, qty_step = calculate_qty(symbol, RISK_SYMBOL_USD, last_price, max_leverage, sess)
        # if qty <= 0:
        #     print(f"Пропускаем {symbol}: рассчитанное qty = {qty} (<=0)")
        #     continue
        #
        # if last_price * qty < 5.0:
        #     print(f"Пропускаем {symbol}: стоимость {last_price * qty:.2f} < 5 USDT")
        #     continue

        for i, akk in enumerate(all_akk):
            best = False
            if akk[1] == 4:
                if not rez:
                    print("Нет подходящих сетапов")
                    continue

                if rez is None:
                    SETUP4 = []
                    continue
                else:
                    best = rez[0]["akk"]
                    SETUP4 = [None, None, None]
                    SETUP_ALL = [SETUP0, SETUP1, SETUP2, SETUP3, SETUP4]
                    SETUP4[0] = SETUP_ALL[best][0]
                    if SETUP4[0]:
                        in_base = 1
                    else:
                        in_base = 0
                    SETUP4[1] = SETUP_ALL[best][1]
                    SETUP4[2] = SETUP_ALL[best][2]

            bad_syms = bad_sym(akk[1], min_count=3, lookback_days=0.08)
            if best and akk[1] == 4:
                bad_syms_best = bad_sym(best, min_count=3, lookback_days=0.08)
                bad_syms += bad_syms_best
                plus_pnl_sym = fine_syn_is_setup(best, symbol, 10)
                if (plus_pnl_sym and plus_pnl_sym <= 0) or not plus_pnl_sym:
                    if len(rez) > 1:
                        best = rez[1]["akk"]
                        SETUP4 = [None, None, None]
                        SETUP_ALL = [SETUP0, SETUP1, SETUP2, SETUP3, SETUP4]
                        SETUP4[0] = SETUP_ALL[best][0]

                        if SETUP4[0]:
                            in_base = 1
                        else:
                            in_base = 0

                        SETUP4[1] = SETUP_ALL[best][1]
                        SETUP4[2] = SETUP_ALL[best][2]

                        bad_syms = bad_sym(akk[1], min_count=3, lookback_days=0.08)
                        bad_syms_best = bad_sym(best, min_count=3, lookback_days=0.08)
                        bad_syms += bad_syms_best
                        plus_pnl_sym = fine_syn_is_setup(best, symbol, 10)
                        if (plus_pnl_sym and plus_pnl_sym <= 0) or not plus_pnl_sym:
                            bad_syms.append(symbol)
                    else:
                        bad_syms.append(symbol)

            if symbol in live_open_coins[str(akk[1])] or symbol in bad_syms:
                if symbol in bad_syms:
                    #print(f'live symbol-{symbol} AKK-{akk[1]} BAD SYM: {bad_syms}')
                    update_symbol_step_size(symbol, 1.0, info['side'], akk[1])
                    #update_symbol_step_size(symbol, 1.0, "Buy", akk[1])
                continue
            if akk[1] in [0,2]:
                side_one = "Sell" if info['side'] == "Buy" else "Buy"
            elif akk[1] == 4:
                if best in [1, 3]:
                    side_one = "Sell" if info['side'] == "Buy" else "Buy"
                else:
                    side_one = info['side']
            else:
                side_one = info['side']

            idx = 1 if side_one == "Buy" else 2
            try:
                if symbol not in read_level_ok(symbol, akk[1]) and max_leverage > 1:
                    set_leverage_for_symbol(akk[0], symbol, max_leverage)
            except:
                pass
            #if akk[1] != 0:
            lev_steps = get_step_size(symbol, side_one, akk[1])
            #print(f'lev_steps {lev_steps}')
            if lev_steps <= 0:
                lev_steps = 0.1
            if akk[1] == 4:
                RISK_SYMBOL_USD = ((live_b_akk4 * 0.2) * 0.2) * 10
            else:
                RISK_SYMBOL_USD = (START_DEP * 0.2) * 0.2

            calc = calculate_qty(symbol, RISK_SYMBOL_USD * lev_steps, last_price, max_leverage, akk[0])
            if not calc:
                continue
            qty_str, qty, qty_step = calc

            try:
                order = akk[0].place_order(
                    category="linear",
                    symbol=symbol,
                    side=side_one,
                    orderType="Market",
                    qty=qty_str,
                    positionIdx=idx
                )
            except:
                order = False


            if order and order['retCode'] == 0:
                update_symbol_step_size(symbol, 1.0, side_one, akk[1])
                if akk[1] == 0:
                    opened_count += 1
                    open_sym += f'\n{symbol}: {side_one} {round(float(qty_str) * last_price, 2)}$'
                    #last_open_sym.append(symbol)
                qty_first = qty
                conn = sqlite3.connect(DB_FILE)
                upsert_funding_trade(
                    conn,
                    symbol,
                    side_one,
                    qty,
                    datetime.now().strftime("%d.%m.%Y %H:%M:%S"),
                    qty_first,
                    0
                )
                conn.close()
                if akk[1] == 4:
                    print(f"[OPEN] AKK-4 {symbol} {side_one} -> setup akk={best} {round(float(qty_str) * last_price, 2)}$")
                    write_tp_sl(symbol, int(in_base), float(SETUP4[2]), float(SETUP4[1]))
                else:
                    pass
                    # print(
                    #     f"[OPEN] AKK-{akk[1]} {symbol} {side_one} {round(float(qty_str) * last_price, 2)}$"# qty={qty:.8f} "
                    # f"leverage={max_leverage}x notional≈{risk_usd:.2f}USDT "
                    # f"rate={info['rate']:.5f}"
                #)
                write_pos(symbol, side_one, float(qty_str)*last_price, float(info['rate_percent']), akk[1], True, None, None)
                write_open(symbol, side_one, float(qty_str)*last_price, akk[1])
            else:
                pass
                #print(f"Ошибка размещения ордера по {symbol}: {order}")
        # except Exception as e:
        #     if '10001' in str(e):
        #         error_add = True
        #         open_sym += f'\n{symbol}: не торгуется'
        #     # Символ не поддерживается - удаляем из глобального списка
        #         print(f"⚠️ Пропускаем {symbol}: символ не поддерживается (ErrCode: 10001)")
        #         if symbol in coins:
        #             len_coins = len(coins)
        #             coins.remove(symbol)
        #             print(f'Список сокращен, было {len_coins} - стало {len(coins)} символов')
        #
        #     if '110126' in str(e):
        #         error_msg = f"⚠️ Ошибка подписания соглашения для {symbol}. Необходимо вручную подписать контракт."
        #         print(error_msg)
        #         send_tg(error_msg)
        #     print(f"Ошибка в open_prepared_funding_trades для {e}")
    if opened_count > 0 or error_add:
        # with open('last_open.txt', 'w') as f:
        #     json.dump(last_open_sym, f)  # сохраняет список как JSON
        now_moment = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
        txt = f"{now_moment}\nОткрыто позиций: {opened_count}{open_sym}\n"
        # send_tg(txt)
        # send_tg_crypta(f'✅ Funding monitor REAL\n{txt}')


    prepared_trades_for_hour = []


def update_symbol_step_size(sym, size, side, akk):
    if size <= 0:
        size = 0.1
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute(
        "UPDATE step_size SET size = ? WHERE symbol = ? AND side = ? AND akk = ?",
        (size, sym, side, akk)
    )
    conn.commit()
    # Проверяем, сколько строк было обновлено
    if c.rowcount > 0:
        #print(f"Размер для {sym} {side} АКК-{akk} обновлен до {size}.")
        conn.close()
        return True
    else:
        c.execute('''
                INSERT OR IGNORE INTO step_size (symbol, size, side, akk)
                VALUES (?, ?, ?, ?)
            ''', (sym, 1.0, side, akk))
        #print(f"Для {sym} {side} АКК-{akk} задан первичный размер 1.0")
        conn.commit()
        conn.close()
        return False

def get_step_size(sym, side, akk):

    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    cursor.execute('''
        SELECT size FROM step_size 
        WHERE symbol = ? AND side = ? AND akk = ?
    ''', (sym, side, akk))

    result = cursor.fetchone()

    conn.close()

    if result:
        return float(result[0])  # Приводим к float (REAL)
    else:
        return 1.0  # Или можно вернуть 0.0 или 1.0 по умолчанию

def close_one_sym(sess, row, akk):
    global gsess1, gsess2, gsess3, gsess4, all_pos_akk1, all_pos_akk2, all_pos_akk3, all_pos_akk4, sess1, sess2, sess3, sess4
    all_akk = [[all_pos_akk1,gsess1,1], [all_pos_akk2,gsess2,2], [all_pos_akk3,gsess3,3], [all_pos_akk4,gsess4,4]]
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

            time.sleep(3)
            #if akk == 4:
            print(f"[CLOSE] AKK-{akk} {symbol} {side} {'TP' if pnl_close > 0 else 'SL'} pnl={round(pnl_close,6)} ")
            conn = sqlite3.connect(DB_FILE)
            c = conn.cursor()
            c.execute(
                "DELETE FROM funding_trades WHERE symbol=? AND side=?",
                (symbol, side)
            )
            conn.commit()
            conn.close()
            #sync_db_positions(sess)
            size_step_akk = get_step_size(symbol, side, akk)
            if pnl_close < 0:
                #live_ball_for_reopen = float(read_key_new('key_process', 'reopen_ball'))
                #send_tg(f'AKK-{"Main" if akk == 0 else akk} SL {symbol} {round(pnl_close,2)}$')
                #write_key_new('key_process', 'reopen_ball', live_ball_for_reopen-abs(pnl_close))
                write_pos(symbol, side, None, None, akk, False, pnl_close, 'SL')
                write_close(symbol, side, pnl_close, 'SL', akk)
                update_symbol_step_size(symbol, size_step_akk - STEP_PLUS, side, akk)
            if pnl_close > 0:# and akk == 0:
                update_symbol_step_size(symbol, size_step_akk + STEP_PLUS, side, akk)
                #update_symbol_step_size(symbol, 1.0, side, akk)
                #send_tg(f'AKK-{"Main" if akk == 0 else akk} TP {symbol} +{round(pnl_close,2)}$')
                write_pos(symbol, side, None, None, akk, False, pnl_close, 'TP')
                write_close(symbol, side, pnl_close, 'TP', akk)
                # for rows in all_akk:
                #     if len(rows[0]) > 0:
                #         for sym in rows[0]:
                #             if symbol == sym['symbol'] and side == sym['side']:
                #                 order = rows[1].place_order(
                #                     category="linear",
                #                     symbol=symbol,
                #                     side=close_side,
                #                     orderType="Market",
                #                     qty=str(float(sym['size'])),
                #                     positionIdx=idx,
                #                     reduceOnly=True
                #                 )
                #                 if order['retCode'] == 0:
                #                     print(
                #                         f"[CLOSE] AKK {rows[2]} {symbol} {close_side} pnl={round(float(sym['unrealisedPnl']), 3)} "
                #                     )
                #                     write_close(symbol, side, round(float(sym['unrealisedPnl']), 3), rows[2])

            time.sleep(0.1)
        else:
            print(f"Ошибка закрытия ордера по {symbol}: {order}")
    except Exception as e:
        print(f"Ошибка при закрытии funding trade по {symbol}: {e}")
    return

def close_funding_trades(sess, all_positions, akk):

    #init_db()

    closed_count = 0

    if len(all_positions) > 0:
        for row in all_positions:
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
                    update_symbol_step_size(symbol, 1.0, side, akk)
                    write_close(symbol, side, pnl_close, 'ROI', akk)
                    write_pos(symbol, side, None, None, akk, False, pnl_close, 'ROI')
                    conn = sqlite3.connect(DB_FILE)
                    c = conn.cursor()
                    c.execute(
                        "DELETE FROM funding_trades WHERE symbol=? AND side=?",
                        (symbol, side)
                    )
                    conn.commit()
                    conn.close()
                    print(
                        f"[CLOSE] AKK-{akk} {symbol} {close_side} qty={qty:.8f} "
                    )

                    closed_count += 1
                    time.sleep(0.1)
                else:
                    print(f"Ошибка закрытия ордера по {symbol}: {order}")
            except Exception as e:
                print(f"Ошибка при закрытии funding trade по {symbol}: {e}")


    return closed_count

def close_dpo_akk_pos(sess, all_positions,akk):
    closed_count = 0
    all_positions = get_all_positions(sess, -1)
    if len(all_positions) > 0:
        for row in all_positions:
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
                    update_symbol_step_size(symbol, 1.0, side, akk)
                    write_close(symbol, side, pnl_close, 'ROI',akk)
                    write_pos(symbol, side, None, None, akk, False, pnl_close, 'ROI')
                    print(
                        f"[close_dpo_akk_pos] AKK-{akk} {symbol} {close_side} qty={qty:.8f} "
                    )
                    closed_count += 1
                    time.sleep(0.1)
                else:
                    print(f"Ошибка закрытия ордера по {symbol}: {order}")
            except Exception as e:
                print(f"Ошибка при закрытии close_dpo_akk_pos по {symbol}: {e}")

    return closed_count

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

def msg_tg_id(txt):
    global api_t, chat
    try:
        out = requests.get(
            f'https://api.telegram.org/bot{api_t}/sendMessage',
            params={
                'chat_id': f'{chat}',
                'text': txt}
        )
        if out.status_code == 200:
            result = out.json()
            message_id = result['result']['message_id']

            if message_id:
                pin_message(api_t, chat, message_id)
            print(f'msg_tg_id new: {message_id}')
            return message_id
    except Exception as e:
        print(f"Ошибка в msg_tg_id: {e}")

def analyze_balance4_growth_from_zero(
        start_date_str='2026-09-09 06:00:00',
        targets=[5000, 10000],
        drawdown_threshold=0.5,  # минимальная просадка в USDT для вывода
        dd_pct_base='initial'  # 'initial' — от начального баланса, 'peak' — от пика (старое поведение)
):

    conn = sqlite3.connect(DB_FILE)
    query = "SELECT date_ms, ballance4 FROM live_akk_usdt ORDER BY date_ms"
    df = pd.read_sql_query(query, conn)
    conn.close()

    if df.empty:
        #print("Таблица live_akk_usdt пуста.")
        return

    df['date'] = pd.to_datetime(df['date_ms'], unit='ms')
    start_date = pd.to_datetime(start_date_str)
    mask = df['date'] >= start_date
    df_filtered = df.loc[mask].copy()
    if df_filtered.empty:
        #print(f"Нет данных после {start_date_str}.")
        return

    df_filtered = df_filtered.sort_values('date')
    first_balance = df_filtered['ballance4'].iloc[0]

    # Вычисляем прирост (growth) относительно первой точки
    df_filtered['growth'] = df_filtered['ballance4'] - first_balance

    first_growth = df_filtered['growth'].iloc[0]  # 0
    last_growth = df_filtered['growth'].iloc[-1]
    first_date = df_filtered['date'].iloc[0]
    last_date = df_filtered['date'].iloc[-1]

    total_growth = last_growth
    delta_hours = (last_date - first_date).total_seconds() / 3600
    if delta_hours == 0:
        #print("Период слишком мал для расчёта.")
        return

    growth_per_hour = total_growth / delta_hours

    # === РАСЧЁТ ПРОСАДОК ===
    growth_values = df_filtered['growth'].values
    running_max = np.maximum.accumulate(growth_values)
    drawdown_usdt = running_max - growth_values
    max_drawdown_usdt = np.max(drawdown_usdt)

    # Вычисляем проценты просадок в зависимости от выбранного базиса
    if dd_pct_base == 'initial':
        # от начального баланса (first_balance)
        drawdown_pct = (drawdown_usdt / first_balance) * 100
        max_drawdown_pct = max_drawdown_usdt / first_balance * 100
        # текущая просадка
        current_max = np.max(growth_values)
        current_drawdown_usdt = current_max - last_growth
        current_drawdown_pct = current_drawdown_usdt / first_balance * 100
    else:  # 'peak'
        with np.errstate(divide='ignore', invalid='ignore'):
            drawdown_pct = np.where(running_max != 0, drawdown_usdt / running_max * 100, 0)
        max_drawdown_pct = np.max(drawdown_pct)
        current_max = np.max(growth_values)
        current_drawdown_usdt = current_max - last_growth
        current_drawdown_pct = (current_drawdown_usdt / current_max * 100) if current_max != 0 else 0

    # Поиск локальных просадок (падений от пика до минимума)
    significant_drawdowns = []
    i = 0
    while i < len(growth_values):
        if i == 0 or growth_values[i] >= growth_values[i - 1]:
            peak_val = growth_values[i]
            peak_idx = i
            j = i
            while j < len(growth_values) and growth_values[j] <= peak_val:
                j += 1
            min_val = np.min(growth_values[peak_idx:j]) if j > peak_idx else peak_val
            if min_val < peak_val:
                dd_usdt = peak_val - min_val
                if dd_usdt >= drawdown_threshold:
                    if dd_pct_base == 'initial':
                        dd_pct = dd_usdt / first_balance * 100
                    else:
                        dd_pct = (dd_usdt / peak_val * 100) if peak_val != 0 else 0
                    significant_drawdowns.append({
                        'peak_date': df_filtered['date'].iloc[peak_idx],
                        'trough_date': df_filtered['date'].iloc[np.argmin(growth_values[peak_idx:j]) + peak_idx],
                        'peak_growth': peak_val,
                        'trough_growth': min_val,
                        'dd_usdt': dd_usdt,
                        'dd_pct': dd_pct
                    })
            i = j
        else:
            i += 1

    # Построение графика (без изменений)
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(df_filtered['date'], df_filtered['growth'],
            label='Движение баланса', color='blue', linewidth=2)
    x_numeric = mdates.date2num(df_filtered['date'])
    coeffs = np.polyfit(x_numeric, df_filtered['growth'], 1)
    trend_line = np.polyval(coeffs, x_numeric)
    ax.plot(df_filtered['date'], trend_line, 'r--', label='Линейный тренд', linewidth=1)
    ax.axhline(y=0, color='gray', linestyle=':', linewidth=0.5, alpha=0.7)
    ax.set_title(f'График профита с {start_date_str} (стартовый баланс = 1000)', fontsize=14)
    ax.set_xlabel('Дата и время')
    ax.set_ylabel('Прирост, USDT')
    ax.legend()
    ax.grid(True, alpha=0.3)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
    ax.xaxis.set_major_locator(mdates.HourLocator(interval=1))
    ax.tick_params(axis='x', rotation=45)
    plt.tight_layout()
    #plt.show()
    plt.savefig('live_graf.png', dpi=100, bbox_inches='tight')
    plt.close(fig)  # закрываем фигуру, чтобы освободить память

    # Вывод метрик
    # print("\n" + "=" * 70)
    # print(f"📊 Анализ прироста ballance4 с {start_date_str} (относительно первой точки)")
    # print("=" * 70)
    # print(f"Начальная точка:     {first_balance:.2f} USDT (в {first_date.strftime('%Y-%m-%d %H:%M')})")
    # print(f"Текущий прирост:     {last_growth:+.2f} USDT (в {last_date.strftime('%Y-%m-%d %H:%M')})")
    # print(f"Общий прирост:       {total_growth:+.2f} USDT")
    # print(f"Общий рост в %:      {(total_growth / first_balance * 100):+.2f}%  (от начального баланса)")
    # print(f"Длительность:        {delta_hours:.1f} часов (~{delta_hours / 24:.2f} дней)")
    # print(f"Средний прирост/час: {growth_per_hour:+.4f} USDT/час")
    # print(f"Средний прирост/день: {growth_per_hour * 24:+.2f} USDT/день")

    if first_balance != 0:
        growth_pct_per_hour = (growth_per_hour / first_balance) * 100
        #print(f"Скорость роста:      {growth_pct_per_hour:+.4f} %/час (от начального баланса)")

    # Просадки
    # print("\n" + "-" * 70)
    # print("📉 Анализ просадок:")
    base_str = "от начального баланса" if dd_pct_base == 'initial' else "от пика"
    # print(f"  Проценты считаются: {base_str}")
    # print(f"  Максимальная просадка:  {max_drawdown_usdt:.2f} USDT ({max_drawdown_pct:.2f}%)")
    if current_drawdown_usdt > 0:
        pass
        #print(f"  Текущая просадка:        {current_drawdown_usdt:.2f} USDT ({current_drawdown_pct:.2f}%)")
    else:
        pass
        #print("  Текущая просадка отсутствует (новый максимум).")

    if significant_drawdowns:
        #print(f"\n  Значительные просадки (глубиной >= {drawdown_threshold:.2f} USDT):")
        for dd in significant_drawdowns:
            pass
            # print(
            #     f"    - {dd['dd_usdt']:.2f} USDT ({dd['dd_pct']:.2f}%) с {dd['peak_date'].strftime('%m-%d %H:%M')} до {dd['trough_date'].strftime('%m-%d %H:%M')}")
    else:
        pass
        #print(f"  Нет просадок глубиной >= {drawdown_threshold:.2f} USDT.")

    # Прогноз целей (без изменений)
    if growth_per_hour > 0:
        for target in targets:
            remaining = target - last_growth
            if remaining <= 0:
                print(f"\n🎯 Цель +{target} USDT уже достигнута.")
                continue
            hours_needed = remaining / growth_per_hour
            days_needed = hours_needed / 24
            eta = last_date + timedelta(days=days_needed)
            # print(f"\n🎯 До достижения +{target} USDT прироста:")
            # print(f"   Потребуется:        {days_needed:.1f} дней ({hours_needed:.1f} часов)")
            # print(f"   Ожидаемая дата:     {eta.strftime('%Y-%m-%d %H:%M')} (при текущем темпе)")
    else:
        pass
        #print("\n⚠️ Прирост отрицательный, целевые значения недостижимы.")

    # print(f"\n📈 Количество записей в выборке: {len(df_filtered)}")
    # print("=" * 70)


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
    if os.path.exists(storage_file):
        with open(storage_file, 'r') as f:
            msg_id = f.read().strip()

        if msg_id:
            # Пытаемся обновить существующее сообщение
            url = f'https://api.telegram.org/bot{api_t}/editMessageMedia'
            media = {
                'type': 'photo',
                'media': 'attach://photo'   # файл будет передан в поле 'photo'
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
                    print("Фото успешно обновлено.")
                    return
            except Exception as e:
                print(f"Ошибка при обновлении фото: {e}")
                # Если обновление не удалось, удаляем сохранённый ID
                # и переходим к отправке нового сообщения
                os.remove(storage_file)

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
        print(f"Ошибка при отправке фото: {e}")

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

def send_tg_crypta(txt):
    global api_t
    try:
        requests.get(
            f'https://api.telegram.org/bot{api_t}/sendMessage',
            params={
                'chat_id': '-1003280628832',
                'message_thread_id': 41694,
                'text': txt}
        )
    except Exception as e:
        print(f"Ошибка в send_tg_crypta: {e}")

def send_tg_update(txt):
    global api_t, chat, id_update_mes
    try:
        if 'id_update_mes' not in globals() or id_update_mes is None:
            return
            # Пропускаем если ID сообщения еще не получен
            
        buttons = [
            [{"text": "📊 Стоп", "callback_data": "button_1"}],
            [{"text": "💰 Старт", "callback_data": "button_2"}],
            [{"text": "🔴 Закрыть все позиции", "callback_data": "button_3"}]
        ]
        params = {'chat_id': f'{chat}',
                    'message_id': id_update_mes,
                    'text': txt,                   
                    'parse_mode': 'HTML'}
                    
        reply_markup = {"inline_keyboard": buttons}
        params['reply_markup'] = json.dumps(reply_markup)
        
        requests.get(
            f'https://api.telegram.org/bot{api_t}/editMessageText',
            params=params,
            timeout=10
        )
    except Exception as e:
        print(f"Ошибка в send_tg_update: {e}")

    try:
        requests.get(
            f'https://api.telegram.org/bot{api_t}/editMessageText',
            params={'chat_id': '-1003280628832',
                    'message_thread_id': 41694,
                    'message_id': 176002,
                    'text': f'✅ Funding monitor REAL (live)\n{txt}'}
        )
    except Exception as e:
        print(f"Ошибка в send_tg_update: {e}")

def reballance_old(b0, b1, b2, b3, b4):
    print('ЗАПУСК РЕБЕЛАНСА')
    for idx, bal in enumerate([b0, b1, b2, b3]):
        diff = int(b4) - int(bal)
        # Определяем тип операции
        if diff > 0:
            adjust_type = 0  # 0 = пополнение (добавить)
            action = "добавляем"
            amount = diff
        else:
            adjust_type = 1  # 1 = изъятие (убавить)
            action = "убавляем"
            amount = -diff

        print(f"Аккаунт {idx}: {action} {amount:.0f} USDT (было {int(bal)}, станет ~{int(bal + diff)})")

        # Выполняем запрос к демо-API
        try:
            config = CONFIGS[idx]
            # Подготавливаем заголовки
            headers = {
                "X-BAPI-API-KEY": config['api_key'],
                "X-BAPI-SIGN": "",  # будет подписан позже
                "X-BAPI-TIMESTAMP": "",
                "X-BAPI-RECV-WINDOW": "5000",
                "Content-Type": "application/json"
            }

            # Подготавливаем тело запроса
            body = {
                "adjustType": adjust_type,
                "utaDemoApplyMoney": [
                    {
                        "coin": "USDT",
                        "amountStr": f"{amount:.0f}"  # целое число USDT
                    }
                ]
            }

            # Генерируем timestamp
            timestamp = str(int(time.time() * 1000))
            headers["X-BAPI-TIMESTAMP"] = timestamp

            # Формируем строку для подписи
            param_str = timestamp + config['api_key'] + "5000" + json.dumps(body)
            import hashlib
            import hmac
            signature = hmac.new(
                bytes(config['api_secret'], 'utf-8'),
                bytes(param_str, 'utf-8'),
                hashlib.sha256
            ).hexdigest()
            headers["X-BAPI-SIGN"] = signature

            # Отправляем запрос
            url = "https://api-demo.bybit.com/v5/account/demo-apply-money"
            response = requests.post(url, headers=headers, json=body)

            if response.status_code == 200:
                resp_json = response.json()
                if resp_json.get('retCode') == 0:
                    ball = int(get_ball(SESS_ALL[idx], idx))
                    print(f"  ✅ Ребаланс аккаунта {idx} выполнен успешно, новый балланс {ball}")
                else:
                    print(f"  ❌ Ошибка API при ребалансе аккаунта {idx}: {resp_json.get('retMsg')}")
            else:
                print(f"  ❌ HTTP ошибка при ребалансе аккаунта {idx}: {response.status_code}")

        except Exception as e:
            print(f"  ❌ Ошибка при ребалансе аккаунта {idx}: {e}")

        time.sleep(10)

def reballance(target_balance: float):
    """
    Устанавливает одинаковый баланс для всех аккаунтов (0..3) равный target_balance.
    """
    print(f'ЗАПУСК РЕБЕЛАНСА ВСЕХ АККАУНТОВ к балансу {target_balance:.2f}')

    # 1. Получаем текущие балансы
    balances = []
    for idx in range(len(SESS_ALL) - 1):      # предполагается, что последний элемент служебный
        bal = float(get_ball(SESS_ALL[idx], idx) )
        if bal is None:
            print(f"Не удалось получить баланс для аккаунта {idx}")
            return
        balances.append(bal)

    print(f"Текущие балансы: {[f'{b:.2f}' for b in balances]}")

    # 2. Целевые балансы – все равны target_balance
    target_balances = [target_balance] * len(balances)
    print(f"Целевые балансы: {[f'{b:.2f}' for b in target_balances]}")

    # 3. Корректируем каждый аккаунт
    for idx in range(len(balances)):
        current = balances[idx]
        target = target_balances[idx]
        diff = target - current
        diff_rounded = round(diff, 2)

        if abs(diff_rounded) < 0.01:
            print(f"Аккаунт {idx}: баланс уже равен {current:.2f} (цель {target:.2f})")
            continue

        # Определяем тип операции
        if diff_rounded > 0:
            adjust_type = 0          # пополнение
            amount = diff_rounded
            action = "добавляем"
        else:
            adjust_type = 1          # изъятие
            amount = -diff_rounded
            action = "убавляем"

        amount_str = f"{amount:.2f}"
        print(f"Аккаунт {idx}: {action} {amount_str} USDT (было {current:.2f}, станет {target:.2f})")

        # 4. Выполняем запрос к демо-API
        try:
            #sess = SESS_ALL[idx]
            config = CONFIGS[idx]

            headers = {
                "X-BAPI-API-KEY": config['api_key'],
                "X-BAPI-SIGN": "",
                "X-BAPI-TIMESTAMP": "",
                "X-BAPI-RECV-WINDOW": "5000",
                "Content-Type": "application/json"
            }

            body = {
                "adjustType": adjust_type,
                "utaDemoApplyMoney": [
                    {
                        "coin": "USDT",
                        "amountStr": amount_str
                    }
                ]
            }

            timestamp = str(int(time.time() * 1000))
            headers["X-BAPI-TIMESTAMP"] = timestamp

            import hashlib
            import hmac
            import json

            param_str = timestamp + config['api_key'] + "5000" + json.dumps(body)
            signature = hmac.new(
                bytes(config['api_secret'], 'utf-8'),
                bytes(param_str, 'utf-8'),
                hashlib.sha256
            ).hexdigest()
            headers["X-BAPI-SIGN"] = signature

            url = "https://api-demo.bybit.com/v5/account/demo-apply-money"
            response = requests.post(url, headers=headers, json=body)

            if response.status_code == 200:
                resp_json = response.json()
                if resp_json.get('retCode') == 0:
                    print(f"  ✅ Ребаланс аккаунта {idx} выполнен успешно")
                else:
                    print(f"  ❌ Ошибка API: {resp_json.get('retMsg')}")
            else:
                print(f"  ❌ HTTP ошибка: {response.status_code}")

        except Exception as e:
            print(f"  ❌ Ошибка при ребалансе аккаунта {idx}: {e}")

        time.sleep(10)   # пауза между запросами

def reballance_last():
    print('ЗАПУСК РЕБЕЛАНСА ВСЕХ АККАУНТОВ')

    # Получаем текущие балансы
    balances = []
    for idx in range(len(SESS_ALL)-1):
        bal = float(get_ball(SESS_ALL[idx], idx) )
        if bal is None:
            print(f"Не удалось получить баланс для аккаунта {idx}")
            return
        balances.append(bal)

    print(f"Текущие балансы: {[f'{b:.2f}' for b in balances]}")

    total = sum(balances)
    print(f"Суммарный баланс: {total:.2f}")

    # Заданные веса (пропорции)
    #weights = [1, 1.5, 2, 2.5, 3]
    weights = [2, 2, 2, 2]
    sum_weights = sum(weights)  # = 10

    # Целевые балансы (доли от общей суммы)
    target_balances = [total * w / sum_weights for w in weights]
    print(f"Целевые балансы: {[f'{b:.2f}' for b in target_balances]}")

    for idx in range(len(SESS_ALL)-1):
        current = balances[idx]
        target = target_balances[idx]
        diff = target - current

        # Округляем разницу до 2 знаков (избегаем накопления погрешности)
        diff_rounded = round(diff, 2)

        if abs(diff_rounded) < 0.01:
            print(f"Аккаунт {idx}: баланс уже соответствует цели ({current:.2f})")
            continue

        # Определяем тип операции
        if diff_rounded > 0:
            adjust_type = 0  # пополнение
            amount = diff_rounded
            action = "добавляем"
        else:
            adjust_type = 1  # изъятие
            amount = -diff_rounded
            action = "убавляем"

        # Формируем строку суммы с двумя знаками
        amount_str = f"{amount:.2f}"

        print(f"Аккаунт {idx}: {action} {amount_str} USDT (было {current:.2f}, станет {target:.2f})")

        # Выполняем запрос к демо-API
        try:
            #sess = SESS_ALL[idx]
            config = CONFIGS[idx]

            headers = {
                "X-BAPI-API-KEY": config['api_key'],
                "X-BAPI-SIGN": "",
                "X-BAPI-TIMESTAMP": "",
                "X-BAPI-RECV-WINDOW": "5000",
                "Content-Type": "application/json"
            }

            body = {
                "adjustType": adjust_type,
                "utaDemoApplyMoney": [
                    {
                        "coin": "USDT",
                        "amountStr": amount_str   # теперь строка с дробью
                    }
                ]
            }

            timestamp = str(int(time.time() * 1000))
            headers["X-BAPI-TIMESTAMP"] = timestamp

            import hashlib
            import hmac
            import json

            param_str = timestamp + config['api_key'] + "5000" + json.dumps(body)
            signature = hmac.new(
                bytes(config['api_secret'], 'utf-8'),
                bytes(param_str, 'utf-8'),
                hashlib.sha256
            ).hexdigest()
            headers["X-BAPI-SIGN"] = signature

            url = "https://api-demo.bybit.com/v5/account/demo-apply-money"
            response = requests.post(url, headers=headers, json=body)

            if response.status_code == 200:
                resp_json = response.json()
                if resp_json.get('retCode') == 0:
                    print(f"  ✅ Ребаланс аккаунта {idx} выполнен успешно")
                else:
                    print(f"  ❌ Ошибка API: {resp_json.get('retMsg')}")
            else:
                print(f"  ❌ HTTP ошибка: {response.status_code}")

        except Exception as e:
            print(f"  ❌ Ошибка при ребалансе аккаунта {idx}: {e}")

        time.sleep(10)

def reballance_work_int():
    print('ЗАПУСК РЕБЕЛАНСА ВСЕХ АККАУНТОВ')

    # Получаем текущие балансы
    balances = []

    for idx in range(len(SESS_ALL)):
        bal = float(get_ball(SESS_ALL[idx], idx) )
        if bal is None:
            print(f"Не удалось получить баланс для аккаунта {idx}")
            return
        balances.append(bal)

    print(f"Текущие балансы: {balances}")

    total = sum(balances)
    print(f"Суммарный баланс: {total:.2f}")

    # Заданные веса (пропорции)
    weights = [1, 1.5, 2, 2.5, 3]
    sum_weights = sum(weights)  # = 10

    # Целевые балансы (доли от общей суммы)
    target_balances = [total * w / sum_weights for w in weights]
    print(f"Целевые балансы: {[round(b, 2) for b in target_balances]}")

    for idx in range(len(SESS_ALL)):
        current = balances[idx]
        target = target_balances[idx]
        diff = target - current

        if abs(diff) < 0.01:
            print(f"Аккаунт {idx}: баланс уже соответствует цели ({current:.2f})")
            continue

        # Определяем тип операции
        if diff > 0:
            adjust_type = 0  # пополнение
            amount = diff
            action = "добавляем"
        else:
            adjust_type = 1  # изъятие
            amount = -diff
            action = "убавляем"

        # Округляем до целого числа USDT (API принимает целые)
        amount_int = int(round(amount))
        if amount_int == 0:
            print(f"Аккаунт {idx}: разница менее 1 USDT, пропускаем")
            continue

        print(f"Аккаунт {idx}: {action} {amount_int} USDT (было {int(current)}, станет ~{int(current + diff)})")

        # Выполняем запрос к демо-API
        try:
            sess = SESS_ALL[idx]
            config = CONFIGS[idx]

            headers = {
                "X-BAPI-API-KEY": config['api_key'],
                "X-BAPI-SIGN": "",
                "X-BAPI-TIMESTAMP": "",
                "X-BAPI-RECV-WINDOW": "5000",
                "Content-Type": "application/json"
            }

            body = {
                "adjustType": adjust_type,
                "utaDemoApplyMoney": [
                    {
                        "coin": "USDT",
                        "amountStr": str(amount_int)
                    }
                ]
            }

            timestamp = str(int(time.time() * 1000))
            headers["X-BAPI-TIMESTAMP"] = timestamp

            import hashlib
            import hmac
            import json

            param_str = timestamp + config['api_key'] + "5000" + json.dumps(body)
            signature = hmac.new(
                bytes(config['api_secret'], 'utf-8'),
                bytes(param_str, 'utf-8'),
                hashlib.sha256
            ).hexdigest()
            headers["X-BAPI-SIGN"] = signature

            url = "https://api-demo.bybit.com/v5/account/demo-apply-money"
            response = requests.post(url, headers=headers, json=body)

            if response.status_code == 200:
                resp_json = response.json()
                if resp_json.get('retCode') == 0:
                    print(f"  ✅ Ребаланс аккаунта {idx} выполнен успешно")
                else:
                    print(f"  ❌ Ошибка API: {resp_json.get('retMsg')}")
            else:
                print(f"  ❌ HTTP ошибка: {response.status_code}")

        except Exception as e:
            print(f"  ❌ Ошибка при ребалансе аккаунта {idx}: {e}")

        time.sleep(10)

# ---------- МОНИТОР ВРЕМЕНИ ----------

def len_for_real_open():
    global prepared_trades_for_hour, live_open_coins
    if live_open_coins is None:
        live_open_coins = {}
    new_open_ok = []
    now_utc_ts = datetime.now(timezone.utc).timestamp()
    len_open = 0
    symbol_new = ''
    for info in prepared_trades_for_hour:
        try:
            next_funding_ts = info['next_funding_time_ms'] / 1000.0
            seconds_to_funding = next_funding_ts - now_utc_ts
            if 1 == 2: #not seconds_to_funding <= sec:
                continue
            else:
                # side = "LONG" if info['side'] == "Sell" else "SHORT"
                # false_is_extr1 = is_extreme_movement_hybrid(sess, info['symbol'], side=side)
                # #false_is_extr2 = is_extreme_movement(sess, info['symbol'], lookback_days=7, extreme_threshold=3.0, rvol_threshold=3.0, global_range_days=3)
                # #if (info['symbol'] in live_open_coins or not false_is_extr1) and info['symbol'] not in sym_for_hadge:# or not false_is_extr2:
                # if not false_is_extr1:
                #     continue
                new_open_ok.append(info)
                symbol_new += f"\n{info['symbol']}"
                len_open += 1
        except:
            print("Ошибка в len_for_real_open")
            pass
    prepared_trades_for_hour = new_open_ok
    if len(prepared_trades_for_hour) > 0:
        return [len_open, symbol_new, prepared_trades_for_hour]
    else:
        return []

def chet_or_not(key_x):
    if key_x % 2 == 0:
        return True
    else:
        return False

def start_funding_earning_monitor(sess, sess1, sess2, sess3, sess4, start_ball):
    global SETUP0, SETUP1, SETUP2, SETUP3, SETUP4, gsess1, gsess2, gsess3, gsess4, all_pos, all_pos_akk1, all_pos_akk2, all_pos_akk3, all_pos_akk4, coins, proc_reopen, but_close_all, prepared_trades_for_hour, free_main, free_dop_1, free_dop_2, free_dop_3, free_dop_4, live_open_coins, live_open_minus_pnl, live_b, live_b_akk1,live_b_akk2,live_b_akk3,live_b_akk4, XXX, pnl
    stert_live_ball = 0
    open_dop = True
    # Инициализируем переменные ДО их использования
    wait_akk1 = 0.0
    wait_akk2 = 0.0
    wait_akk3 = 0.0
    wait_akk4 = 0.0
    key_10_min = True

    
    besubitok = True

    re_open = read_key_new('key_process','reopen_ball')
    if re_open:
        ball_for_reopen = float(re_open)
    else:
        ball_for_reopen = start_ball - (start_ball * (proc_reopen * XXX))
    write_key_new('key_process', 'reopen_ball', ball_for_reopen)
    live_b = get_ball(sess, 0) 
    all_pos = get_all_positions(sess, 0)
    pos = len(all_pos)

    all_pos_akk1 = get_all_positions(sess1, 1)
    pos_akk1 = len(all_pos_akk1)

    all_pos_akk2 = get_all_positions(sess2, 2)
    pos_akk2 = len(all_pos_akk2)

    all_pos_akk3 = get_all_positions(sess3, 3)
    pos_akk3 = len(all_pos_akk3)

    all_pos_akk4 = get_all_positions(sess4, 4)
    pos_akk4 = len(all_pos_akk4)

    for_all_close = 0
    last_pnl = 777.0
    open_true = False

    while True:
        key_x = read_key_new('key_process', 'step_reopen')
        key_all_close = False
        try:
            gsess1 = sess1
            gsess2 = sess2
            gsess3 = sess3
            gsess4 = sess4
            now = datetime.now(timezone.utc)  # ориентируемся на UTC [web:12][web:15]
            current_minute = now.minute
            current_hour = now.hour
            current_sec = now.second
            now_time = f'{current_hour}:{current_minute}:{current_sec}'
            if current_minute % 10 == 0 and current_sec == 0:
                key_10_min = True
                try:
                    analyze_balance4_growth_from_zero(
                    start_date_str='2026-09-09 06:00:00',
                    targets=[5000, 10000, 100000],
                    drawdown_threshold=0.5)
                    send_or_update_photo()
                except:
                    pass
                #sync_db_positions(sess)
                print(f"[{now_time}] tick (минута {current_minute})")

            if (current_minute == 20 or current_minute == 50) and current_sec == 30:
                print(f"[{now_time}] RECONNECT BYBIT")
                sess = create_session_with_proxy(DEMO_CONFIG)
                if read_key_new('work_all_dop_akk','akk1_old') != 0:
                    sess1 = create_session_with_proxy(DEMO_CONFIG1)
                if read_key_new('work_all_dop_akk','akk2_old') != 0:
                    sess2 = create_session_with_proxy(DEMO_CONFIG2)
                if read_key_new('work_all_dop_akk','akk3_old') != 0:
                    sess3 = create_session_with_proxy(DEMO_CONFIG3)
                if read_key_new('work_all_dop_akk','akk4_old') != 0:
                    sess4 = create_session_with_proxy(DEMO_CONFIG4)


            # Раз в сутки в 22:22 обновляем список монет
            if current_hour == 22 and current_minute == 22 and current_sec == 30:
                now_moment = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
                print(f"{now_moment} обновление списка монет")
                #send_tg(f"{now_moment} обновление списка монет")
                # Обновляем локальный список монет после обновления файла
                coins = update_coins(sess)

            # На 55-й минуте: подготавливаем список монет
            live_min_find = [5, 15, 25, 35, 45, 55]
            if current_minute in live_min_find and 30 <= current_sec <= 35:

                #open_true = True
                now_moment = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
                print(f"{now_moment} Запуск подготовки списка монет с фандингом")
                #send_tg(f"{now_moment} Анализ монет с предстоящим фандингом")
                prepared_trades_for_hour = prepare_funding_list(sess, coins)
                if len(prepared_trades_for_hour) > 0:
                    # Пример вызова (подставьте свои значения текущих балансов)
                    # result = analyze_setups_new(live_b, live_b_akk1, live_b_akk2, live_b_akk3)
                    #
                    # if 'error' in result:
                    #     print(result['error'])
                    # else:
                    #     print(f"Лучший сетап: {result['best_akk']}")
                    #     print(result['reason'])
                    #     print("\nДетали по всем сетапам:")
                    #     for akk, data in result['details'].items():
                    #         print(f"  akk {akk}: открыто {data['open_count']}, закрыто {data['close_count']}, "
                    #               f"PnL {data['total_pnl']}, рост баланса {data['growth']:.2f}")

                    # SETUP_ALL = [SETUP0, SETUP1, SETUP2, SETUP3, SETUP4]
                    #
                    # # result = analyze_setups()
                    #
                    # if 'error' in result:
                    #     print(result['error'])
                    # else:
                    #     open_count = 0
                    #     close_count = 0
                    #
                    #     conn = sqlite3.connect(DB_FILE)
                    #     cursor = conn.cursor()
                    #
                    #     cursor.execute("SELECT sym FROM open_sym WHERE akk = ?", (result['best_akk'],))
                    #     open_count = len(cursor.fetchall())
                    #
                    #     cursor.execute("SELECT sym FROM close_sym WHERE akk = ?", (result['best_akk'],))
                    #     close_count = len(cursor.fetchall())
                    #
                    #     conn.close()
                    #
                    #     if open_count >= 10 and close_count >= 10:
                    #         SETUP4 = [None, None, None]
                    #         SETUP4[0] = SETUP_ALL[result['best_akk']][0]
                    #         SETUP4[1] = SETUP_ALL[result['best_akk']][1] * 2
                    #         SETUP4[2] = SETUP_ALL[result['best_akk']][2] * 2
                    now_moment = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
                    #open_true = False
                    print(f'{now_moment} len(prepared_trades_for_hour) {len(prepared_trades_for_hour)}')
                    open_prepared_funding_trades()


            # В 00:05 открываем позиции по подготовленному списку (за минуту до расчёта)
            # live_min_open = [0, 10, 20, 30, 40, 50]
            # if open_true and current_minute in live_min_open and 5 <= current_sec <= 55 and len(prepared_trades_for_hour) > 0:# or len(read_symbols_for_open()) > 0):
            #     now_moment = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
            #     open_true = False
            #     print(f'{now_moment} len(prepared_trades_for_hour) {len(prepared_trades_for_hour)}')
            #     open_prepared_funding_trades(sess, live_b)


            if current_sec % 10 == 0:

                live_b = get_ball(sess, 0) 
                #print(current_sec,live_b)
                comsa = get_comsa(sess, fee_rate=0.002)
                all_pos = get_all_positions(sess, 0)
                pos = len(all_pos)
                # try:
                #     open_live_for_copy = sorted(all_pos, key=lambda p: float(p["unrealisedPnl"]))#reverse=True
                #     for i in open_live_for_copy:
                #         print(i)
                #     open_live_for_copy_reverse = sorted(all_pos, key=lambda p: float(p["unrealisedPnl"]), reverse=True)  # reverse=True
                #     for i in open_live_for_copy_reverse:
                #         print(i)
                # except:
                #     pass

                live_b_akk1 = get_ball(sess1, 1) 
                comsa_akk1 = get_comsa(sess1, fee_rate=0.002)
                all_pos_akk1 = get_all_positions(sess1, 1)
                pos_akk1 = len(all_pos_akk1)

                live_b_akk2 = get_ball(sess2, 2) 
                comsa_akk2 = get_comsa(sess2, fee_rate=0.002)
                all_pos_akk2 = get_all_positions(sess2, 2)
                pos_akk2 = len(all_pos_akk2)

                live_b_akk3 = get_ball(sess3, 3) 
                comsa_akk3 = get_comsa(sess3, fee_rate=0.002)
                all_pos_akk3 = get_all_positions(sess3, 3)
                pos_akk3 = len(all_pos_akk3)

                live_b_akk4 = get_ball(sess4, 4) 
                comsa_akk4 = get_comsa(sess4, fee_rate=0.002)
                all_pos_akk4 = get_all_positions(sess4, 4)
                pos_akk4 = len(all_pos_akk4)

                key_pnl = False
                if pnl != 0 and round(pnl,1) != round(last_pnl,1): #pnl != 0 and
                    #print(pnl)
                    key_pnl = True
                    last_pnl = pnl
                    all_ball = live_b + live_b_akk1 + live_b_akk2 + live_b_akk3 + live_b_akk4
                    write_live_all(live_b, all_ball, free_main, free_dop_1, free_dop_2, free_dop_3, free_dop_4, live_b_akk1, live_b_akk2, live_b_akk3, live_b_akk4, False)

                    comsa_all = comsa + comsa_akk1 + comsa_akk2 + comsa_akk3 + comsa_akk4


                # akk1_start = read_key_new('work_all_dop_akk', 'akk1_start')
                # akk1_old = read_key_new('work_all_dop_akk', 'akk1_old')
                # akk1_qty = read_key_new('work_all_dop_akk', 'akk1_qty')
                # akk1_reopen_reload = read_key_new('work_all_dop_akk', 'akk1_reopen_reload')
                akk1_open = read_key_new('work_all_dop_akk', 'akk1_open')

                if pos_akk1 > 0: #akk1_start != 0 and akk1_old != 0 and akk1_qty != 0:

                    # if pos_akk1 == 1:
                    #     wait_akk1 = (akk1_open * (1 + (0.005 * (XXX * XXX_dop_1)))) + comsa_akk1
                    # elif pos_akk1 == 2:
                    #     wait_akk1 = (akk1_open * (1 + (0.0075 * (XXX * XXX_dop_1)))) + comsa_akk1
                    # else:
                    wait_akk1 = (akk1_open * 1.002) + comsa_akk1

                    if False: #live_b_akk1 > wait_akk1:
                        reset_level_akk(1)
                        closed_count = close_dpo_akk_pos(sess1, all_pos_akk1, 1)
                        if closed_count > 0:
                            now_moment = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
                            live_b_akk1 = get_ball(sess1, 1) 
                            write_key_new('work_all_dop_akk', 'akk1_open', live_b_akk1)
                            print(
                                f"{now_moment} Закрыто позиций доп.АКК1: {closed_count}\nБаланс: {round(live_b_akk1-800, 3)}")
                            send_tg(
                                f"{now_moment} Закрыто позиций доп.АКК1: {closed_count}\nБаланс: {round(live_b_akk1-800, 3)}")
                            send_tg_crypta(f"✅ Funding monitor REAL\nЗакрыто позиций доп.АКК1: {closed_count}\nБаланс: {round(live_b_akk1-800, 3)}")
                        live_b_akk1 = get_ball(sess1, 1) 
                        #write_key_new('work_all_dop_akk','akk1_start', 0.0)
                        # write_key_new('work_all_dop_akk','akk1_old', 0.0)
                        # write_key_new('work_all_dop_akk', 'akk1_qty', 0.0)
                        # bigger, smaller = find_nearest_bounds(read_level(), live_b)
                        # if bigger is not None and smaller is not None:
                        #     write_level_akk(smaller, 1)
                        #     write_key_new('work_all_dop_akk', 'akk1_reopen_reload', bigger)
                        #     write_key_new('work_all_dop_akk', 'akk1_start', smaller)
                        wait_akk1 = 0
                # if key_x > 1 and pos_akk1 == 0 and akk1_start != 0 and akk1_old == 0 and live_b > akk1_reopen_reload and akk1_qty == 0:
                #     write_key_new('work_all_dop_akk', 'akk1_old', 1.0)
                #     send_tg(f'Перезагрузка повторного открытия AKK1, уровень: live_b-{live_b} > {akk1_reopen_reload}-akk1_reopen_reload\nАктивация при снижении уровня до: {akk1_start}')
                #
                # if key_x > 1 and pos_akk1 == 0 and akk1_start != 0 and akk1_old != 0 and live_b < akk1_start and akk1_qty == 0:
                #     write_key_new('work_all_dop_akk', 'akk1_qty', 1.0)
                #     write_key_new('work_all_dop_akk', 'akk1_open', live_b_akk1)
                #     top_losses = sort_positions_by_pnl(all_pos, max_items=20)
                #     open_dop_akk_pos(sess1, top_losses, 2, False, False, 1)
                #     send_tg(
                #         f'Повторное открытие AKK1, уровень: live_b-{live_b} < {akk1_start}-akk1_start')

                # akk2_start = read_key_new('work_all_dop_akk', 'akk2_start')
                # akk2_old = read_key_new('work_all_dop_akk', 'akk2_old')
                # akk2_qty = read_key_new('work_all_dop_akk', 'akk2_qty') #akk1_open
                # akk2_reopen_reload = read_key_new('work_all_dop_akk', 'akk2_reopen_reload')
                akk2_open = read_key_new('work_all_dop_akk', 'akk2_open')  # akk1_open
                # print(
                #     f'{'*'*50}\nkey_x {key_x}\npos_akk2 {pos_akk2}\nakk2_start {akk2_start}\nakk2_old {akk2_old}\nakk2_qty {akk2_qty}\nakk2_reopen_reload {akk2_reopen_reload}\nlive_b_akk2 {live_b_akk2}\nlive_b {live_b}\n{'*'*50}')
                if pos_akk2 > 0: #akk2_start != 0 and akk2_old != 0 and akk2_qty != 0:

                    # if pos_akk2 == 1:
                    #     wait_akk2 = (akk2_open * (1 + (0.005 * (XXX * XXX_dop_2)))) + comsa_akk2
                    # elif pos_akk2 == 2:
                    #     wait_akk2 = (akk2_open * (1 + (0.0075 * (XXX * XXX_dop_2)))) + comsa_akk2
                    # else:
                    wait_akk2 = (akk2_open * 1.002) + comsa_akk2

                    if False: #live_b_akk2 > wait_akk2:
                        reset_level_akk(2)
                        closed_count = close_dpo_akk_pos(sess2, all_pos_akk2, 2)
                        if closed_count > 0:
                            now_moment = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
                            live_b_akk2 = get_ball(sess2, 2) 
                            write_key_new('work_all_dop_akk', 'akk2_open', live_b_akk2)
                            print(
                                f"{now_moment} Закрыто позиций доп.АКК2: {closed_count}\nБаланс: {round(live_b_akk2-800, 3)}")
                            send_tg(
                                f"{now_moment} Закрыто позиций доп.АКК2: {closed_count}\nБаланс: {round(live_b_akk2-800, 3)}")
                            send_tg_crypta(f"✅ Funding monitor REAL\nЗакрыто позиций доп.АКК2: {closed_count}\nБаланс: {round(live_b_akk2-800, 3)}")
                        live_b_akk2 = get_ball(sess2, 2) 
                        # write_key_new('work_all_dop_akk', 'akk2_start', 0.0)
                        # write_key_new('work_all_dop_akk', 'akk2_old', 0.0)
                        # write_key_new('work_all_dop_akk', 'akk2_qty', 0.0)
                        # bigger, smaller = find_nearest_bounds(read_level(), live_b)
                        # if bigger is not None and smaller is not None:
                        #     write_level_akk(smaller, 2)
                        #     write_key_new('work_all_dop_akk', 'akk2_reopen_reload', bigger)
                        #     write_key_new('work_all_dop_akk', 'akk2_start', smaller)
                        wait_akk2 = 0
                # if key_x > 2 and pos_akk2 == 0 and akk2_start != 0 and akk2_old == 0 and live_b > akk2_reopen_reload and akk2_qty == 0:
                #     write_key_new('work_all_dop_akk', 'akk2_old', 1.0)
                #     send_tg(
                #         f'Перезагрузка повторного открытия AKK2, уровень: live_b-{live_b} > {akk2_reopen_reload}-akk2_reopen_reload\nАктивация при снижении уровня до: {akk2_start}')
                #
                # if key_x > 2 and pos_akk2 == 0 and akk2_start != 0 and akk2_old != 0 and live_b < akk2_start and akk2_qty == 0:
                #     write_key_new('work_all_dop_akk', 'akk2_qty', 1.0)
                #     write_key_new('work_all_dop_akk', 'akk2_open', live_b_akk2)
                #     top_losses = sort_positions_by_pnl(all_pos, max_items=20)
                #     open_dop_akk_pos(sess2, top_losses, 3, False,False, 2)
                #     send_tg(
                #         f'Повторное открытие AKK2, уровень: live_b-{live_b} < {akk2_start}-akk2_start')

                # akk3_start = read_key_new('work_all_dop_akk', 'akk3_start')
                # akk3_old = read_key_new('work_all_dop_akk', 'akk3_old')
                # akk3_qty = read_key_new('work_all_dop_akk', 'akk3_qty')
                # akk3_reopen_reload = read_key_new('work_all_dop_akk', 'akk3_reopen_reload')
                akk3_open = read_key_new('work_all_dop_akk', 'akk3_open')

                if pos_akk3 > 0: #akk3_start != 0 and akk3_old != 0 and akk3_qty != 0:

                    # if pos_akk3 == 1:
                    #     wait_akk3 = (akk3_open * (1 + (0.005 * (XXX * XXX_dop_3)))) + comsa_akk3
                    # elif pos_akk3 == 2:
                    #     wait_akk3 = (akk3_open * (1 + (0.0075 * (XXX * XXX_dop_3)))) + comsa_akk3
                    # else:
                    wait_akk3 = (akk3_open * 1.002) + comsa_akk3

                    if False: #live_b_akk3 > wait_akk3:
                        reset_level_akk(3)
                        closed_count = close_dpo_akk_pos(sess3, all_pos_akk3,3)
                        if closed_count > 0:
                            now_moment = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
                            live_b_akk3 = get_ball(sess3, 3) 
                            write_key_new('work_all_dop_akk', 'akk3_open', live_b_akk3)
                            print(
                                f"{now_moment} Закрыто позиций доп.АКК3: {closed_count}\nБаланс: {round(live_b_akk3-800, 3)}")
                            send_tg(
                                f"{now_moment} Закрыто позиций доп.АКК3: {closed_count}\nБаланс: {round(live_b_akk3-800, 3)}")
                            send_tg_crypta(
                                f"✅ Funding monitor REAL\nЗакрыто позиций доп.АКК3: {closed_count}\nБаланс: {round(live_b_akk3-800, 3)}")
                        live_b_akk3 = get_ball(sess3, 3) 
                        # write_key_new('work_all_dop_akk', 'akk3_start', 0.0)
                        # write_key_new('work_all_dop_akk', 'akk3_old', 0.0)
                        # write_key_new('work_all_dop_akk', 'akk3_qty', 0.0)
                        # bigger, smaller = find_nearest_bounds(read_level(), live_b)
                        # if bigger is not None and smaller is not None:
                        #     write_level_akk(smaller, 3)
                        #     write_key_new('work_all_dop_akk', 'akk3_reopen_reload', bigger)
                        #     write_key_new('work_all_dop_akk', 'akk3_start', smaller)
                        wait_akk3 = 0
                # if key_x > 3 and pos_akk3 == 0 and akk3_start != 0 and akk3_old == 0 and live_b > akk3_reopen_reload and akk3_qty == 0:
                #     write_key_new('work_all_dop_akk', 'akk3_old', 1.0)
                #     send_tg(
                #         f'Перезагрузка повторного открытия AKK3, уровень: live_b-{live_b} > {akk3_reopen_reload}-akk3_reopen_reload\nАктивация при снижении уровня до: {akk3_start}')
                #
                # if key_x > 3 and pos_akk3 == 0 and akk3_start != 0 and akk3_old != 0 and live_b < akk3_start and akk3_qty == 0:
                #     write_key_new('work_all_dop_akk', 'akk3_qty', 1.0)
                #     write_key_new('work_all_dop_akk', 'akk3_open', live_b_akk3)
                #     top_losses = sort_positions_by_pnl(all_pos, max_items=20)
                #     open_dop_akk_pos(sess3, top_losses, 4, False, False, 3)
                #     send_tg(
                #         f'Повторное открытие AKK3, уровень: live_b-{live_b} < {akk3_start}-akk3_start')

                # akk4_start = read_key_new('work_all_dop_akk', 'akk4_start')
                # akk4_old = read_key_new('work_all_dop_akk', 'akk4_old')
                # akk4_qty = read_key_new('work_all_dop_akk', 'akk4_qty')
                # akk4_reopen_reload = read_key_new('work_all_dop_akk', 'akk4_reopen_reload')
                akk4_open = read_key_new('work_all_dop_akk', 'akk4_open')
                if pos_akk4 > 0: #akk4_start != 0 and akk4_old != 0 and akk4_qty != 0:

                    # if pos_akk4 == 1:
                    #     wait_akk4 = (akk4_open * (1 + (0.005* (XXX * XXX_dop_4)))) + comsa_akk4
                    # elif pos_akk4 == 2:
                    #     wait_akk4 = (akk4_open * (1 + (0.0075 * (XXX * XXX_dop_4)))) + comsa_akk4
                    # else:
                    wait_akk4 = (akk4_open * 1.011) + comsa_akk4

                    if False: #live_b_akk4 > wait_akk4:

                        reset_level_akk(4)
                        SETUP4 = []
                        closed_count = close_dpo_akk_pos(sess4, all_pos_akk4, 4)
                        if closed_count > 0:
                            now_moment = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
                            live_b_akk4 = get_ball(sess4, 4) 
                            write_key_new('work_all_dop_akk', 'akk4_open', live_b_akk4)
                            print(
                                f"{now_moment} Закрыто позиций доп.АКК4: {closed_count}\nБаланс: {round(live_b_akk4-800, 3)}")
                            # send_tg(
                            #     f"{now_moment} Закрыто позиций доп.АКК4: {closed_count}\nБаланс: {round(live_b_akk4-800, 3)}")
                            # send_tg_crypta(
                            #     f"✅ Funding monitor REAL\nЗакрыто позиций доп.АКК4: {closed_count}\nБаланс: {round(live_b_akk4-800, 3)}")
                        live_b_akk4 = get_ball(sess4, 4) 
                        # write_key_new('work_all_dop_akk', 'akk4_start', 0.0)
                        # write_key_new('work_all_dop_akk', 'akk4_old', 0.0)
                        # write_key_new('work_all_dop_akk', 'akk4_qty', 0.0)
                        # bigger, smaller = find_nearest_bounds(read_level(), live_b)
                        # if bigger is not None and smaller is not None:
                        #     write_level_akk(smaller, 4)
                        #     write_key_new('work_all_dop_akk', 'akk4_reopen_reload', bigger)
                        #     write_key_new('work_all_dop_akk', 'akk4_start', smaller)
                        wait_akk4 = 0
                        #key_all_close = True


                # if  key_x > 4 and pos_akk4 == 0 and akk4_start != 0 and akk4_old == 0 and live_b > akk4_reopen_reload and akk4_qty == 0:
                #     write_key_new('work_all_dop_akk', 'akk4_old', 1.0)
                #     send_tg(
                #         f'Перезагрузка повторного открытия AKK4, уровень: live_b-{live_b} > {akk4_reopen_reload}-akk4_reopen_reload\nАктивация при снижении уровня до: {akk4_start}')
                #
                # if key_x > 4 and pos_akk4 == 0 and akk4_start != 0 and akk4_old != 0 and live_b < akk4_start and akk4_qty == 0:
                #     write_key_new('work_all_dop_akk', 'akk4_qty', 1.0)
                #     write_key_new('work_all_dop_akk', 'akk4_open', live_b_akk4)
                #     top_losses = sort_positions_by_pnl(all_pos, max_items=20)
                #     open_dop_akk_pos(sess4, top_losses, 5, False, False, 4)
                #     send_tg(
                #         f'Повторное открытие AKK4, уровень: live_b-{live_b} < {akk4_start}-akk4_start')

                #print(1,round(stert_live_ball, 1), round(live_b, 1))
                if round(stert_live_ball, 2) != round(live_b, 2):
                    #print('меняется баланс: ',round(stert_live_ball, 2),round(live_b, 2))

                    stert_live_ball = live_b
                    #wait_max = (start_ball * (1 + (0.02 * XXX))) + comsa
                    ball_for_reopen = read_key_new('key_process','reopen_ball')
                    first_ball = read_key_new('key_process', 'first_ball')

                    # if pos == 1:
                    #
                    #     wait = (start_ball * (1 + (0.005 * XXX))) + comsa
                    #     # print(f'1 wait {wait} start_ball {start_ball} XXX {XXX}')
                    #
                    # elif pos == 2:
                    #     wait = (start_ball * (1 + (0.0075 * XXX))) + comsa
                    #     # print(f'2 wait {wait} start_ball {start_ball} XXX {XXX}')
                    #
                    # else:
                    wait = (start_ball * 1.002) + comsa
                        # print(f'3 wait {wait} start_ball {start_ball} XXX {XXX}')

                    #if current_sec % 10 == 0:
                    now_moment = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
                    #print(now_moment)

                    for_send_live = (f'{now_moment}\n'
                                     # f'Текущий баланс: {round(live_b - 800, 2)}\n'
                                     # f'Порог закрытия: {0 if wait - 800 < 0 else round(wait - 800, 2)}\n'

                                     # f'Позиций на доп.0 : {pos}\n'
                                     # f'Текущий баланс доп.0 : {round(live_b - 800, 2)}\n'

                                     # f'Средств на главном: {round(float(free_main - 800), 2)}\n'
                                     # f'--------------------------\n'
                                     # f'Средств на доп.1 : {round(float(free_dop_1 - 800), 2)}\n'

                                     # f'Позиций на доп.1 : {pos_akk1}\n'
                                     # f'Текущий баланс доп.1 : {round(live_b_akk1 - 800, 2)}\n'

                                     # f'Порог закрытия доп.1 : {0 if wait_akk1 - 800 < 0 else round(wait_akk1 - 800, 2)}\n'
                                     # f'--------------------------\n'
                                     # f'Средств на доп.2 : {round(float(free_dop_2 - 800), 2)}\n'
                                     # f'Позиций на доп.2 : {pos_akk2}\n'
                                     # f'Текущий баланс доп.2 : {round(live_b_akk2 - 800, 2)}\n'
                                     # f'Порог закрытия доп.2 : {0 if wait_akk2 - 800 < 0 else round(wait_akk2 - 800, 2)}\n'
                                     # f'--------------------------\n'
                                     # f'Средств на доп.3 : {round(float(free_dop_3 - 800), 2)}\n'
                                     # f'Позиций на доп.3 : {pos_akk3}\n'
                                     # f'Текущий баланс доп.3 : {round(live_b_akk3 - 800, 2)}\n'
                                     # f'Порог закрытия доп.3 : {0 if wait_akk3 - 800 < 0 else round(wait_akk3 - 800, 2)}\n'
                                     f'--------------------------\n'
                                     # f'Средств на доп.4 : {round(float(free_dop_4 - 800), 2)}\n'
                                     f'Старовый депозит: 1000$ (09.09.2026)\n'
                                     f'Открыто позиций: {pos_akk4}\n'
                                     f'Текущий баланс: {round(live_b_akk4, 2)}\n'
                                     f'Свободная маржа: {round(float(free_dop_4), 2)}\n'
                                     # f'Порог закрытия доп.4 : {0 if wait_akk4 - 800 < 0 else round(wait_akk4 - 800, 2)}\n'
                                     f'--------------------------\n'
                                     )
                    send_tg_update(for_send_live)
                    key_10_min = False
                    open_dop = False
                    if open_dop and 1 == 2:
                        open_dop = False
                        key_reopen_step = False
                        top_losses = sort_positions_by_pnl(all_pos, max_items=20)
                        sym_for_dop = reopen_pos(sess, top_losses, key_reopen_step, start_ball)
                        all_pos = get_all_positions(sess, -1)

                    if live_b < ball_for_reopen and 1 == 2:
                        reopen_proc = proc_reopen * XXX
                        proc_reopen += 0.001

                        key_x = read_key_new('key_process', 'step_reopen')
                        if not chet_or_not(key_x):
                            #key_reopen_step = True
                            key_reopen_step = False
                            top_losses = sort_positions_by_pnl(all_pos, max_items=20)
                            sym_for_dop = reopen_pos(sess, top_losses, key_reopen_step, start_ball)
                            all_pos = get_all_positions(sess, -1)
                        else:
                            key_reopen_step = False
                            top_losses = sort_positions_by_pnl(all_pos, max_items=20)
                            sym_for_dop = reopen_pos(sess, top_losses, key_reopen_step, start_ball)
                            all_pos = get_all_positions(sess, -1)

                        if 1 == 2 and key_x > 1:
                            write_step_level(key_x, live_b)
                        if 1 == 2 and key_x == 1:
                            write_key_new('work_all_dop_akk', 'akk1_reopen_reload', live_b)

                        if 1 == 2 and key_x >= 2:
                            if (key_x == 2 and akk1_start == 0.0) or (key_x in [6,10,14,18,22,26] and pos_akk1 == 0):
                                write_key_new('work_all_dop_akk', 'akk2_reopen_reload', live_b)
                                sess1 = create_session_with_proxy(DEMO_CONFIG1)
                                live_b_akk1 = get_ball(sess1, 1) 
                                write_key_new('work_all_dop_akk', 'akk1_open', live_b_akk1)
                                top_losses = sort_positions_by_pnl(all_pos, max_items=20)
                                open_dop_akk_pos(sess1, top_losses, 2, False, sym_for_dop, 1)
                                write_key_new('work_all_dop_akk', 'akk1_start', live_b)
                                write_key_new('work_all_dop_akk', 'akk1_old', live_b_akk1)
                                write_key_new('work_all_dop_akk', 'akk1_qty', 1.0)
                            elif key_x in [6,10,14,18,22,26] and pos_akk1 != 0 and akk1_start != 0.0 and akk1_qty != 0.0 and akk1_start > live_b:
                            #elif chet_or_not(key_x) and pos_akk1 != 0 and key_x > 2 and akk1_start != 0.0 and akk1_qty != 0.0 and akk1_start > live_b:
                                top_losses = sort_positions_by_pnl(all_pos, max_items=20)
                                open_dop_akk_pos(sess1, top_losses, 2, False, sym_for_dop, 1)

                        if 1 == 2 and key_x >= 3:
                            if (key_x == 3 and akk2_start == 0.0) or (key_x in [7,11,15,19,23,27] and pos_akk2 == 0):
                                write_key_new('work_all_dop_akk', 'akk3_reopen_reload', live_b)
                                sess2 = create_session_with_proxy(DEMO_CONFIG2)
                                live_b_akk2 = get_ball(sess2, 2) 
                                write_key_new('work_all_dop_akk', 'akk2_open', live_b_akk2)
                                top_losses = sort_positions_by_pnl(all_pos, max_items=20)
                                open_dop_akk_pos(sess2, top_losses, 3, False, sym_for_dop, 2)
                                write_key_new('work_all_dop_akk', 'akk2_start', live_b)
                                write_key_new('work_all_dop_akk', 'akk2_old', live_b_akk2)
                                write_key_new('work_all_dop_akk', 'akk2_qty', 1.0)
                            elif key_x in [7,11,15,19,23,27] and pos_akk2 != 0 and key_x > 3 and akk2_start != 0.0 and akk2_qty != 0.0 and akk2_start > live_b:
                            #elif not chet_or_not(key_x) and pos_akk2 != 0 and key_x > 3 and akk2_start != 0.0 and akk2_qty != 0.0 and akk2_start > live_b:
                                top_losses = sort_positions_by_pnl(all_pos, max_items=20)
                                open_dop_akk_pos(sess2, top_losses, 3, False, sym_for_dop, 2)

                        if 1 == 2 and key_x >= 4:
                            if (key_x == 4 and akk3_start == 0.0) or (key_x in [8,12,16,20,24,28] and pos_akk3 == 0):
                                write_key_new('work_all_dop_akk', 'akk4_reopen_reload', live_b)
                                sess3 = create_session_with_proxy(DEMO_CONFIG3)
                                live_b_akk3 = get_ball(sess3, 3) 
                                top_losses = sort_positions_by_pnl(all_pos, max_items=20)
                                open_dop_akk_pos(sess3, top_losses, 4, False, sym_for_dop, 3)
                                write_key_new('work_all_dop_akk', 'akk3_open', live_b_akk3)
                                write_key_new('work_all_dop_akk', 'akk3_start', live_b)
                                write_key_new('work_all_dop_akk', 'akk3_old', live_b_akk3)
                                write_key_new('work_all_dop_akk', 'akk3_qty', 1.0)
                            elif key_x in [8,12,16,20,24,28] and pos_akk3 != 0 and key_x > 4 and akk3_start != 0.0 and akk3_qty != 0.0 and akk3_start > live_b:
                            #elif chet_or_not(key_x) and pos_akk3 != 0 and key_x > 4 and akk3_start != 0.0 and akk3_qty != 0.0 and akk3_start > live_b:
                                top_losses = sort_positions_by_pnl(all_pos, max_items=20)
                                open_dop_akk_pos(sess3, top_losses, 4, False, sym_for_dop, 3)

                        if 1 == 2 and key_x >= 5:
                            if (key_x == 5 and akk4_start == 0.0) or (key_x in [9,13,17,21,25,29] and pos_akk4 == 0):
                                sess4 = create_session_with_proxy(DEMO_CONFIG4)
                                live_b_akk4 = get_ball(sess4, 4) 
                                write_key_new('work_all_dop_akk', 'akk4_open', live_b_akk4)
                                top_losses = sort_positions_by_pnl(all_pos, max_items=20)
                                open_dop_akk_pos(sess4, top_losses, 5, False, sym_for_dop, 4)
                                write_key_new('work_all_dop_akk', 'akk4_start', live_b)
                                write_key_new('work_all_dop_akk', 'akk4_old', live_b_akk4)
                                write_key_new('work_all_dop_akk', 'akk4_qty', 1.0)
                            elif key_x in [9,13,17,21,25,29] and pos_akk4 != 0 and key_x > 5 and akk4_start != 0.0 and akk4_qty != 0.0 and akk4_start > live_b:
                            #elif not chet_or_not(key_x) and pos_akk4 != 0 and key_x > 5 and akk4_start != 0.0 and akk4_qty != 0.0 and akk4_start > live_b:
                                top_losses = sort_positions_by_pnl(all_pos, max_items=20)
                                open_dop_akk_pos(sess4, top_losses, 5, False, sym_for_dop, 4)

                        key_x += 1


                    try:
                        summ_dop_akk_all = live_b_akk1 + live_b_akk2 + live_b_akk3 + live_b_akk4
                        comsa_all_dop = comsa_akk1 + comsa_akk2 + comsa_akk3 + comsa_akk4
                        all_start_summ = read_key_new('key_process', 'all_start_summ')
                        for_all_close = round(all_start_summ * 0.01 + (comsa + comsa_all_dop),2)
                        #print(for_all_close)

                    except:
                        pass

                    if 1 == 1: #55 >= current_minute >= 10 or live_b > wait_max or key_all_close:
                        all_dop_len = pos_akk1 + pos_akk2 + pos_akk3 + pos_akk4

                        if but_close_all:

                            key_all_close = True
                            print('key_all_close = True')
                            #send_tg('ЗАКРЫТИЕ ВСЕХ ПОЗИЦИЙ ПО ROI')
                            but_close_all = False

                        if key_pnl:
                            old_itog = float(read_prognose())
                            #print(all_ball-comsa_all,f'({all_ball}{comsa_all})', old_itog)
                            if old_itog and all_ball-comsa_all > old_itog and besubitok:
                                besubitok = False
                                print(all_ball-comsa_all, old_itog)
                                now_moment = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
                                #send_tg(f'{now_moment}\nCуммарный баланс вышел в БУ\nТекущее значение с учетом комиссий: {round(all_ball-comsa_all, 3)}$\nРанее зафисированный баланс: {round(old_itog, 3)}$')
                            if old_itog and all_ball-comsa_all < old_itog and not besubitok:
                                besubitok = True
                        #key_all_close = True
                        if key_all_close:# live_b > wait or key_all_close:
                            #print('live_b: ', live_b, 'wait: ', wait, 'key_all_close: ', key_all_close)
                            clear_step_level()
                            write_key_new('key_process', 'step_reopen', 1)
                            closed_count = close_funding_trades(sess, all_pos, 0)
                            if key_all_close:
                                all_pos1 = len(get_all_positions(sess1, -1))
                                if all_pos1 == 0:
                                    wait_akk1 = 0
                                    write_key_new('work_all_dop_akk', 'akk1_open', 0.0)
                                    write_key_new('work_all_dop_akk', 'akk1_start', 0.0)
                                    write_key_new('work_all_dop_akk', 'akk1_old', 0.0)
                                    write_key_new('work_all_dop_akk', 'akk1_qty', 0.0)
                                    write_key_new('work_all_dop_akk', 'akk1_reopen_reload', 0.0)
                                else:
                                    closed_count1 = close_dpo_akk_pos(sess1, all_pos_akk1, 1)
                                    if closed_count1 > 0:
                                        live_b_akk1 = get_ball(sess1, 1) 
                                        all_pos1 = len(get_all_positions(sess1, -1))
                                        if all_pos1 == 0:
                                            wait_akk1 = 0
                                            write_key_new('work_all_dop_akk', 'akk1_open', 0.0)
                                            write_key_new('work_all_dop_akk', 'akk1_start', 0.0)
                                            write_key_new('work_all_dop_akk', 'akk1_old', 0.0)
                                            write_key_new('work_all_dop_akk', 'akk1_qty', 0.0)
                                            write_key_new('work_all_dop_akk', 'akk1_reopen_reload', 0.0)

                                all_pos2 = len(get_all_positions(sess2, -1))
                                if all_pos2 == 0:
                                    wait_akk2 = 0
                                    write_key_new('work_all_dop_akk', 'akk2_open', 0.0)
                                    write_key_new('work_all_dop_akk', 'akk2_start', 0.0)
                                    write_key_new('work_all_dop_akk', 'akk2_old', 0.0)
                                    write_key_new('work_all_dop_akk', 'akk2_qty', 0.0)
                                    write_key_new('work_all_dop_akk', 'akk2_reopen_reload', 0.0)
                                else:
                                    closed_count2 = close_dpo_akk_pos(sess2, all_pos_akk2, 2)
                                    if closed_count2 > 0:
                                        live_b_akk2 = get_ball(sess2, 2) 
                                        all_pos2 = len(get_all_positions(sess2, -1))
                                        if all_pos2 == 0:
                                            wait_akk2 = 0
                                            write_key_new('work_all_dop_akk', 'akk2_open', 0.0)
                                            write_key_new('work_all_dop_akk', 'akk2_start', 0.0)
                                            write_key_new('work_all_dop_akk', 'akk2_old', 0.0)
                                            write_key_new('work_all_dop_akk', 'akk2_qty', 0.0)
                                            write_key_new('work_all_dop_akk', 'akk2_reopen_reload', 0.0)

                                all_pos3 = len(get_all_positions(sess3, -1))
                                if all_pos3 == 0:
                                    wait_akk3 = 0
                                    write_key_new('work_all_dop_akk', 'akk3_open', 0.0)
                                    write_key_new('work_all_dop_akk', 'akk3_start', 0.0)
                                    write_key_new('work_all_dop_akk', 'akk3_old', 0.0)
                                    write_key_new('work_all_dop_akk', 'akk3_qty', 0.0)
                                    write_key_new('work_all_dop_akk', 'akk3_reopen_reload', 0.0)
                                else:
                                    closed_count3 = close_dpo_akk_pos(sess3, all_pos_akk3, 3)
                                    if closed_count3 > 0:
                                        live_b_akk3 = get_ball(sess3, 3) 
                                        all_pos3 = len(get_all_positions(sess3, -1))
                                        if all_pos3 == 0:
                                            wait_akk3 = 0
                                            write_key_new('work_all_dop_akk', 'akk3_open', 0.0)
                                            write_key_new('work_all_dop_akk', 'akk3_start', 0.0)
                                            write_key_new('work_all_dop_akk', 'akk3_old', 0.0)
                                            write_key_new('work_all_dop_akk', 'akk3_qty', 0.0)
                                            write_key_new('work_all_dop_akk', 'akk3_reopen_reload', 0.0)

                                all_pos4 = len(get_all_positions(sess4, -1))
                                if all_pos4 == 0:
                                    wait_akk4 = 0
                                    write_key_new('work_all_dop_akk', 'akk4_open', 0.0)
                                    write_key_new('work_all_dop_akk', 'akk4_start', 0.0)
                                    write_key_new('work_all_dop_akk', 'akk4_old', 0.0)
                                    write_key_new('work_all_dop_akk', 'akk4_qty', 0.0)
                                    write_key_new('work_all_dop_akk', 'akk4_reopen_reload', 0.0)
                                else:
                                    closed_count4 = close_dpo_akk_pos(sess4, all_pos_akk4, 4)
                                    if closed_count4 > 0:
                                        live_b_akk4 = get_ball(sess4, 4) 
                                        all_pos4 = len(get_all_positions(sess4, -1))
                                        if all_pos4 == 0:
                                            wait_akk4 = 0
                                            write_key_new('work_all_dop_akk', 'akk4_open', 0.0)
                                            write_key_new('work_all_dop_akk', 'akk4_start', 0.0)
                                            write_key_new('work_all_dop_akk', 'akk4_old', 0.0)
                                            write_key_new('work_all_dop_akk', 'akk4_qty', 0.0)
                                            write_key_new('work_all_dop_akk', 'akk4_reopen_reload', 0.0)
                                now_moment = datetime.now().strftime("%d.%m.%Y %H:%M:%S")

                            if closed_count > 0 or key_all_close:
                                time.sleep(1)


                                all_pos = len(get_all_positions(sess, -1))
                                time.sleep(1)
                                all_pos1 = len(get_all_positions(sess1, -1))
                                time.sleep(1)
                                all_pos2 = len(get_all_positions(sess2, -1))
                                time.sleep(1)
                                all_pos3 = len(get_all_positions(sess3, -1))
                                time.sleep(1)
                                all_pos4 = len(get_all_positions(sess4, -1))



                                new_ball = get_ball(sess, 0) 
                                time.sleep(1)
                                live_b_akk1 = get_ball(sess1, 1) 
                                time.sleep(1)
                                live_b_akk2 = get_ball(sess2, 2) 
                                time.sleep(1)
                                live_b_akk3 = get_ball(sess3, 3) 
                                time.sleep(1)
                                live_b_akk4 = get_ball(sess4, 4) 
                                summ_dop_akk_all = new_ball + live_b_akk1 + live_b_akk2 + live_b_akk3 + live_b_akk4

                                if all_pos + all_pos1 + all_pos2 + all_pos3 + all_pos4 == 0:
                                    write_key_new('key_process', 'all_start_summ', summ_dop_akk_all)
                                    send_stat = True

                                start_ball = new_ball
                                write_key_new('key_process', 'first_ball', new_ball)
                                proc_reopen = 0.015
                                ball_for_reopen = new_ball - (new_ball * (proc_reopen * XXX))
                                write_key_new('key_process', 'reopen_ball', ball_for_reopen)
                                now_moment = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
                                #print(f"{now_moment} Закрыто позиций: {closed_count}\nБаланс: {round(start_ball, 3)}")
                                send_tg(f"{now_moment} Закрыто позиций AKK-main: {closed_count}\nБаланс: {round(start_ball-800, 3)}")
                                send_tg_crypta(f"{now_moment} Закрыто позиций AKK-main: {closed_count}\nБаланс: {round(start_ball-800, 3)}")
                                live_open_coins = {}
                                live_open_minus_pnl = []
                                read_return_comsa(False)
                                for_send_live = (f'{now_moment}\n'
                                                 #f'Текущий баланс: {round(live_b - 800, 2)}\n'
                                                 #f'Порог закрытия: {0 if wait - 800 < 0 else round(wait - 800, 2)}\n'
                                                 
                                                 #f'Позиций на доп.0 : {pos}\n'
                                                 #f'Текущий баланс доп.0 : {round(live_b - 800, 2)}\n'
                                                 
                                                 #f'Средств на главном: {round(float(free_main - 800), 2)}\n'
                                                 #f'--------------------------\n'
                                                 #f'Средств на доп.1 : {round(float(free_dop_1 - 800), 2)}\n'
                                                 
                                                 # f'Позиций на доп.1 : {pos_akk1}\n'
                                                 # f'Текущий баланс доп.1 : {round(live_b_akk1 - 800, 2)}\n'
                                                 
                                                 #f'Порог закрытия доп.1 : {0 if wait_akk1 - 800 < 0 else round(wait_akk1 - 800, 2)}\n'
                                                 #f'--------------------------\n'
                                                 #f'Средств на доп.2 : {round(float(free_dop_2 - 800), 2)}\n'
                                                 #f'Позиций на доп.2 : {pos_akk2}\n'
                                                 #f'Текущий баланс доп.2 : {round(live_b_akk2 - 800, 2)}\n'
                                                 #f'Порог закрытия доп.2 : {0 if wait_akk2 - 800 < 0 else round(wait_akk2 - 800, 2)}\n'
                                                 #f'--------------------------\n'
                                                 #f'Средств на доп.3 : {round(float(free_dop_3 - 800), 2)}\n'
                                                 #f'Позиций на доп.3 : {pos_akk3}\n'
                                                 #f'Текущий баланс доп.3 : {round(live_b_akk3 - 800, 2)}\n'
                                                 #f'Порог закрытия доп.3 : {0 if wait_akk3 - 800 < 0 else round(wait_akk3 - 800, 2)}\n'
                                                 f'--------------------------\n'
                                                 #f'Средств на доп.4 : {round(float(free_dop_4 - 800), 2)}\n'
                                                 f'Старовый депозит: 1000$ (09.09.2026)\n'
                                                 f'Открыто позиций: {pos_akk4}\n'
                                                 f'Текущий баланс: {round(live_b_akk4, 2)}\n'
                                                 f'Свободная маржа: {round(float(free_dop_4), 2)}\n'
                                                 #f'Порог закрытия доп.4 : {0 if wait_akk4 - 800 < 0 else round(wait_akk4 - 800, 2)}\n'
                                                 f'--------------------------\n'
                                                 )
                                send_tg_update(for_send_live)
                            try:
                                last_main_ball = get_ball(sess, 0) 
                                time.sleep(0.5)
                                live_b_akk1 = get_ball(sess1, 1) 
                                time.sleep(0.5)
                                live_b_akk2 = get_ball(sess2, 2) 
                                time.sleep(0.5)
                                live_b_akk3 = get_ball(sess3, 3) 
                                time.sleep(0.5)
                                live_b_akk4 = get_ball(sess4, 4) 
                                current_balance = round(last_main_ball + live_b_akk1  + live_b_akk2 + live_b_akk3 + live_b_akk4, 2)
                                write_live_all(last_main_ball, current_balance, free_main, free_dop_1, free_dop_2,
                                               free_dop_3,
                                               free_dop_4, live_b_akk1, live_b_akk2, live_b_akk3, live_b_akk4, False)

                                if key_all_close:
                                    #msg = calculate_growth_and_forecast(current_balance)
                                    #msg2 = calculate_growth_exp_regression_new(current_balance)
                                    msg3 = calculate_growth_exp_regression_new3(current_balance)
                                    #compare_growth_methods(current_balance, db_file=DB_FILE)
                                    # if msg3 and send_stat:
                                    #     send_stat = False
                                    #     send_tg_crypta(f"✅ Funding monitor REAL\n📈 Прогноз доходности:\n{msg3}")
                                    #     send_tg(f"📈 Прогноз движения депозита:\n{msg3}")
                                    reballance_old(last_main_ball, live_b_akk1, live_b_akk2, live_b_akk3, live_b_akk4)
                                    #reballance(float(live_b_akk4))
                                    # drop_all_tables(DB_FILE)
                                    # init_db()
                                    live_b = get_ball(sess, 0) 
                                    time.sleep(0.5)
                                    live_b_akk1 = get_ball(sess1, 1) 
                                    time.sleep(0.5)
                                    live_b_akk2 = get_ball(sess2, 2) 
                                    time.sleep(0.5)
                                    live_b_akk3 = get_ball(sess3, 3) 
                                    time.sleep(0.5)
                                    live_b_akk4 = get_ball(sess4, 4) 
                                    write_key_new('key_process', 'first_ball', live_b)
                                    write_key_new('work_all_dop_akk', 'akk1_open', live_b_akk1)
                                    write_key_new('work_all_dop_akk', 'akk2_open', live_b_akk2)
                                    write_key_new('work_all_dop_akk', 'akk3_open', live_b_akk3)
                                    write_key_new('work_all_dop_akk', 'akk4_open', live_b_akk4)
                                    SETUP4 = []
                                    key_all_close = False
                                    wait = (live_b * 1.01) + comsa
                            except:
                                pass
            time.sleep(1)

        except Exception as e:
            print(f"Ошибка в funding_earning_monitor: {e}")
            time.sleep(3)

def drop_all_tables(db_path):

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Получаем список всех таблиц
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()

    # Отключаем проверку внешних ключей
    cursor.execute("PRAGMA foreign_keys = OFF;")

    for table in tables:
        table_name = table[0]
        if table_name.startswith('sqlite_'):
            continue
        cursor.execute(f"DROP TABLE {table_name};")

    cursor.execute("PRAGMA foreign_keys = ON;")

    conn.commit()
    conn.close()
    print("Все таблицы удалены.")

# ---------- ТОЧКА ВХОДА ----------

def get_all_positions(sess, key_akk):

    global SETUP0, SETUP1, SETUP2, SETUP3, SETUP4, live_open_coins, live_open_minus_pnl,live_b, live_b_akk1,live_b_akk2,live_b_akk3,live_b_akk4
    live_b_all = [live_b, live_b_akk1, live_b_akk2, live_b_akk3, live_b_akk4]
    all_posit = []
    if key_akk >= 0:
        live_open_coins[str(key_akk)] = []
        #print(f'get_all_positions akk-{key_akk}')
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
    if len(all_posit) > 0:
        one_pos = 1
        remaining_positions = []  # новый список для позиций, которые останутся открытыми
        for row in all_posit:
            if key_akk == 0 and SETUP0:

                if float(row['unrealisedPnl']) < 0 and abs(float(row['unrealisedPnl'])) > 200 * (SETUP0[1] * get_step_size(row['symbol'],row['side'] , key_akk)): #(live_b_all[key_akk]-800) * (SETUP0[1] * get_step_size(row['symbol'],row['side'] , key_akk)):
                    close_one_sym(sess, row, key_akk)
                #print(f'get_all_positions AKK {key_akk}, sym: {row['symbol']}, unrealisedPnl = {row['unrealisedPnl']}, ждем live_b_all[key_akk] * 0.005 {live_b_all[key_akk] * 0.005}')
                # elif float(row['unrealisedPnl']) < 0:
                #     live_open_minus_pnl.append(row['symbol'])
                #     live_open_co.append(row['symbol'])
                #     #remaining_positions.append(row)
                elif float(row['unrealisedPnl']) > 200 * SETUP0[2] * get_step_size(row['symbol'],row['side'] , key_akk):
                    close_one_sym(sess, row, key_akk)
                else:
                    live_open_coins[str(key_akk)].append(row['symbol'])
                    remaining_positions.append(row)

            if key_akk == 1 and SETUP1:
                if float(row['unrealisedPnl']) < 0 and abs(float(row['unrealisedPnl'])) > 200 * SETUP1[1] * get_step_size(row['symbol'],row['side'], key_akk):
                    close_one_sym(sess, row, key_akk)
                #print(f'get_all_positions AKK {key_akk}, sym: {row['symbol']}, unrealisedPnl = {row['unrealisedPnl']}, ждем live_b_all[key_akk] * 0.005 {live_b_all[key_akk] * 0.005}')
                elif float(row['unrealisedPnl']) > 200 * SETUP1[2]* get_step_size(row['symbol'],row['side'], key_akk):
                    close_one_sym(sess, row, key_akk)
                else:
                    live_open_coins[str(key_akk)].append(row['symbol'])
                    remaining_positions.append(row)


            if key_akk == 2 and SETUP2:
                if float(row['unrealisedPnl']) < 0 and abs(float(row['unrealisedPnl'])) > 200 * SETUP2[1]* get_step_size(row['symbol'],row['side'] , key_akk):
                    close_one_sym(sess, row, key_akk)
                #print(f'get_all_positions AKK {key_akk}, sym: {row['symbol']}, unrealisedPnl = {row['unrealisedPnl']}, ждем live_b_all[key_akk] * 0.005 {live_b_all[key_akk] * 0.005}')
                elif float(row['unrealisedPnl']) >  200 * (SETUP2[2]* get_step_size(row['symbol'],row['side'], key_akk)):
                    close_one_sym(sess, row, key_akk)
                else:
                    live_open_coins[str(key_akk)].append(row['symbol'])
                    remaining_positions.append(row)

            if key_akk == 3 and SETUP3:
                if float(row['unrealisedPnl']) < 0 and abs(float(row['unrealisedPnl'])) > 200 * SETUP3[1]* get_step_size(row['symbol'],row['side'] , key_akk):
                    close_one_sym(sess, row, key_akk)
                #print(f'get_all_positions AKK {key_akk}, sym: {row['symbol']}, unrealisedPnl = {row['unrealisedPnl']}, ждем live_b_all[key_akk] * 0.005 {live_b_all[key_akk] * 0.005}')
                elif float(row['unrealisedPnl']) > 200 * SETUP3[2]* get_step_size(row['symbol'],row['side'], key_akk):
                    close_one_sym(sess, row, key_akk)
                else:
                    live_open_coins[str(key_akk)].append(row['symbol'])
                    remaining_positions.append(row)

            if key_akk == 4:
                tp_sl = read_tp_sl(row['symbol'])
                #print(tp_sl, float(row['unrealisedPnl']))
                if tp_sl:
                    if len(all_posit) == 1:
                        one_pos = 2
                    step_size_live = get_step_size(row['symbol'],row['side'] , key_akk)
                    # sl_live = 200 * ((tp_sl[2] * 10) * step_size_live)
                    # tp_live = 200 * ((tp_sl[1] * 10) * step_size_live)
                    sl_live = float(live_b_all[key_akk] * 0.2) * ((tp_sl[2] * 10) * step_size_live)
                    tp_live = float(live_b_all[key_akk] * 0.2) * ((tp_sl[1] * (10/one_pos)) * step_size_live)
                    #print(f'AKK 4 SYM: {row['symbol']} step_size_live: {step_size_live} PNL {round(float(row['unrealisedPnl']),2)} | SLpnl - {round(sl_live,2)} TPpnl - {round(tp_live,2)}')
                    if float(row['unrealisedPnl']) < 0 and abs(float(row['unrealisedPnl'])) > sl_live:
                        close_one_sym(sess, row, key_akk)
                    #print(f'get_all_positions AKK {key_akk}, sym: {row['symbol']}, unrealisedPnl = {row['unrealisedPnl']}, ждем live_b_all[key_akk] * 0.005 {live_b_all[key_akk] * 0.005}')
                    elif float(row['unrealisedPnl']) > tp_live:
                        close_one_sym(sess, row, key_akk)
                    else:
                        live_open_coins[str(key_akk)].append(row['symbol'])
                        remaining_positions.append(row)
        # if key_akk >= 0:
        #     print(f'get_all_positions akk-{key_akk} live_open_coins[{str(key_akk)}]:{live_open_coins[str(key_akk)]}')
        #print('\n--------------------------------------------------------------------')
        if len(remaining_positions) > 0:
            return remaining_positions
        else:
            return all_posit
    else:
        return all_posit

def sort_positions_by_pnl(positions, max_items=20):
    if not positions:
        return []
    sorted_positions = sorted(
        positions,
        key=lambda p: float(p.get('unrealisedPnl', 0.0))
    )
    return sorted_positions[:max_items]

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

def open_dop_akk_pos(sess, all_pos, step, first, for_dop_open, akk):
    global RISK_SYMBOL_USD, live_open_minus_pnl, XXX_dop_1, XXX_dop_2, XXX_dop_3, XXX_dop_4, XXX
    opened_count = 0
    sym_for_send = ""
    sym_pnl = {}
    sym_side = {}

    if not for_dop_open:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("SELECT symbol, live_pnl, side FROM funding_trades")
        db_rows = c.fetchall()
        conn.close()

        for symbol, live_pnl, side in db_rows:
            sym_pnl[f'{symbol}_{side}'] = live_pnl

    for row in all_pos:

        if not first and float(row['unrealisedPnl']) > 0:
            continue
        if not first and not for_dop_open and float(row['unrealisedPnl']) > sym_pnl[f'{row['symbol']}_{row['side']}']:
            continue
        if not first and for_dop_open and row['symbol'] not in for_dop_open:
            continue


        symbol = row['symbol']
        side = row['side']
        idx = 1 if side == "Buy" else 2
        qty_first = '0'
        XXX_dop = 1
        try:
            if step == 2:
                XXX_dop = XXX_dop_1

                max_leverage = get_max_leverage_for_symbol(sess, symbol)
                if max_leverage > 1:
                    set_leverage_for_symbol(sess, symbol, max_leverage)

                ticker = get_symbol_ticker(sess, symbol)
                if ticker is None:
                    print(f"Пропускаем {symbol}: нет тикера при открытии")
                    continue
                last_price = float(ticker['lastPrice'])
                if last_price <= 0:
                    print(f"Пропускаем {symbol}: некорректная цена {last_price}")
                    continue

            elif step == 3:
                XXX_dop = XXX_dop_2

                max_leverage = get_max_leverage_for_symbol(sess, symbol)
                if max_leverage > 1:
                    set_leverage_for_symbol(sess, symbol, max_leverage)

                ticker = get_symbol_ticker(sess, symbol)
                if ticker is None:
                    print(f"Пропускаем {symbol}: нет тикера при открытии")
                    continue
                last_price = float(ticker['lastPrice'])
                if last_price <= 0:
                    print(f"Пропускаем {symbol}: некорректная цена {last_price}")
                    continue

            elif step == 4:
                XXX_dop = XXX_dop_3

                max_leverage = get_max_leverage_for_symbol(sess, symbol)
                if max_leverage > 1:
                    set_leverage_for_symbol(sess, symbol, max_leverage)

                ticker = get_symbol_ticker(sess, symbol)
                if ticker is None:
                    print(f"Пропускаем {symbol}: нет тикера при открытии")
                    continue
                last_price = float(ticker['lastPrice'])
                if last_price <= 0:
                    print(f"Пропускаем {symbol}: некорректная цена {last_price}")
                    continue


            elif step == 5:
                XXX_dop = XXX_dop_4

                max_leverage = get_max_leverage_for_symbol(sess, symbol)
                if max_leverage > 1:
                    set_leverage_for_symbol(sess, symbol, max_leverage)

                ticker = get_symbol_ticker(sess, symbol)
                if ticker is None:
                    print(f"Пропускаем {symbol}: нет тикера при открытии")
                    continue
                last_price = float(ticker['lastPrice'])
                if last_price <= 0:
                    print(f"Пропускаем {symbol}: некорректная цена {last_price}")
                    continue

            risk_usd = RISK_SYMBOL_USD

            risk_usd_new = risk_usd * XXX_dop

            qty_str, qty, qty_step = calculate_qty(symbol, risk_usd_new, last_price, max_leverage, sess)
            if qty <= 0:
                print(f"Пропускаем {symbol}: рассчитанное qty = {qty} (<=0)")
                continue

            if last_price * qty < 5.0:
                print(f"Пропускаем {symbol}: стоимость {last_price * qty:.2f} < 5 USDT")
                continue

            qty_first = format_qty_for_api(qty, qty_step)

        except:
            pass

        print(symbol)

        try:
            order = sess.place_order(
                category="linear",
                symbol=symbol,
                side=side,
                orderType="Market",
                qty=str(qty_first),
                positionIdx=idx
            )

            if order['retCode'] == 0:
                write_open(symbol,side,float(qty_first) * last_price, akk)
                print(f"OPEN DOP_AKK {symbol} {side} qty={qty_first}")
                opened_count += 1
                #sym_for_send += f"\n{symbol}"
                sym_for_send += f'\n{symbol}: {side} {round(float(qty_first) * last_price, 2)}$'
            else:
                print(f"Ошибка переоткрытия позиции {symbol}: {order}")

        except Exception as e:
            print(f"Ошибка в open_dop_akk_pos: {e} доп.AKK{step-1}")
    if opened_count > 0:
        now_moment = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
        txt = f"{now_moment}\nОткрыты позиции доп.AKK{step-1}: {opened_count}{sym_for_send}"
        send_tg(txt)
        print(txt)

def reopen_pos(sess, all_pos, key_open, start_ball):
    global RISK_SYMBOL_USD, live_open_minus_pnl, XXX, last_open_sym
    init_db()
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    qty_sym = {}
    sym_pnl = {}
    c.execute("SELECT symbol, side, qty_first, live_pnl FROM funding_trades")
    db_rows = c.fetchall()
    conn.close()

    for symbol, side, qty_first, live_pnl in db_rows:
        qty_sym[symbol] = qty_first
        sym_pnl[f'{symbol}_{side}'] = live_pnl


    opened_count = 0
    sym_for_send = ""
    for_dop_akk = []

    for row in all_pos:
        if not key_open and float(row['unrealisedPnl']) < sym_pnl[f'{row['symbol']}_{row['side']}']:
            conn = sqlite3.connect(DB_FILE)
            side = row['side']
            upsert_funding_trade_update_pnl(
                conn,
                symbol,
                side,
                float(row['unrealisedPnl'])
            )
            conn.close()

        # if len(last_open_sym) == 0:
        #     with open('last_open.txt', 'r') as f:
        #         last_open_sym = json.load(f)  # загружает список обратно

        if row['symbol'] in last_open_sym and float(row['unrealisedPnl']) < 0:
            continue
        if float(row['unrealisedPnl']) > 0:
            continue
        elif float(row['unrealisedPnl']) > sym_pnl[f'{row['symbol']}_{row['side']}']:
            continue

        symbol = row['symbol']
        for_dop_akk.append(symbol)
        if not key_open:
            continue

        side = row['side']
        idx = 1 if side == "Buy" else 2

        try:
            # Получаем фильтры для символа
            min_qty, qty_step = get_qty_filters(sess, symbol)
            
            # Проверяем, есть ли qty в БД
            if symbol in qty_sym:
                qty = float(qty_sym[symbol])
                # Переформатируем с правильной точностью
                qty_first = format_qty_for_api(qty, qty_step)
            else:
                # Рассчитываем заново
                max_leverage = get_max_leverage_for_symbol(sess, symbol)
                if max_leverage > 1:
                    set_leverage_for_symbol(sess, symbol, max_leverage)
                
                ticker = get_symbol_ticker(sess, symbol)
                if ticker is None:
                    print(f"Пропускаем {symbol}: нет тикера при открытии")
                    continue
                last_price = float(ticker['lastPrice'])
                if last_price <= 0:
                    print(f"Пропускаем {symbol}: некорректная цена {last_price}")
                    continue
                
                risk_usd = RISK_SYMBOL_USD
                qty_str, qty, qty_step = calculate_qty(symbol, risk_usd, last_price, max_leverage, sess)
                if qty <= 0:
                    print(f"Пропускаем {symbol}: рассчитанное qty = {qty} (<=0)")
                    continue
                
                if last_price * qty < 5.0:
                    print(f"Пропускаем {symbol}: стоимость {last_price * qty:.2f} < 5 USDT")
                    continue
                
                qty_first = qty_str  # Это уже отформатированная строка из calculate_qty
            
            # Проверяем, что qty_first - строка с правильным форматом
            if not isinstance(qty_first, str):
                # Если это число, форматируем
                min_qty, qty_step = get_qty_filters(sess, symbol)
                qty_first = format_qty_for_api(float(qty_first), qty_step)
            
            # Дополнительная проверка: не слишком ли много знаков после запятой
            # и не превышает ли qty максимально допустимое (по документации Bybit)
            ticker = get_symbol_ticker(sess, symbol)
            if ticker is None:
                continue
            last_price = float(ticker['lastPrice'])
            
            print(f"REOPEN {symbol}: qty={qty_first}, price={last_price}")

        except Exception as e:
            print(f"Ошибка при подготовке qty для {symbol}: {e}")
            continue

        try:
            order = sess.place_order(
                category="linear",
                symbol=symbol,
                side=side,
                orderType="Market",
                qty=str(qty_first),  # Убеждаемся, что это строка
                positionIdx=idx
            )

            if order['retCode'] == 0:
                sym_for_send += f'\n{symbol}: {side} {round(float(qty_first) * last_price, 2)}$'
                conn = sqlite3.connect(DB_FILE)
                upsert_funding_trade(
                    conn,
                    symbol,
                    side,
                    qty_first,
                    datetime.now(timezone.utc).isoformat(),
                    qty_first,
                    float(row['unrealisedPnl'])
                )
                conn.close()
                print(f"[REOPEN] {symbol} {side} qty={qty_first}")
                opened_count += 1
                write_open(symbol, side, float(qty_first) * last_price, 0)
            else:
                print(f"Ошибка переоткрытия позиции {symbol}: {order}")

        except Exception as e:
            print(f"Ошибка в reopen_pos: {e}")

    if opened_count > 0:
        now_moment = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
        txt = f"{now_moment}\nREOPEN: {opened_count}{sym_for_send}"
        send_tg(txt)
        print(txt)

    # last_open_sym = []
    # with open('last_open.txt', 'w') as f:
    #     json.dump(last_open_sym, f)
    return for_dop_akk

def reset_table_to_defaults():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        UPDATE work_all_dop_akk 
        SET akk1_open = 0.0,
            akk1_start = 0.0,
            akk1_old = 0.0,
            akk1_qty = 0.0,
            akk2_open = 0.0,
            akk2_start = 0.0,
            akk2_old = 0.0,
            akk2_qty = 0.0,
            akk3_open = 0.0,
            akk3_start = 0.0,
            akk3_old = 0.0,
            akk3_qty = 0.0,
            akk4_open = 0.0,
            akk4_start = 0.0,
            akk4_old = 0.0,
            akk4_qty = 0.0
        WHERE id = 1
    ''')

    # Если запись не существует (affected rows = 0), вставляем новую
    if cursor.rowcount == 0:
        cursor.execute('''
            INSERT INTO work_all_dop_akk (id, akk1_start, akk1_old, akk1_qty,
                                          akk2_start, akk2_old, akk2_qty,
                                          akk3_start, akk3_old, akk3_qty,
                                          akk4_start, akk4_old, akk4_qty)
            VALUES (1, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
        ''')
    conn.commit()

def sync_db_positions_old(sess):
    """
    Синхронизирует БД с текущими открытыми позициями Bybit.
    Удаляет записи в fundingtrades, для которых нет соответствующих открытых позиций.
    """
    init_db()  # Инициализация БД, если нужно
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    # Получаем все записи из БД
    c.execute("SELECT symbol, side, qty FROM funding_trades")
    db_rows = c.fetchall()

    if not db_rows:
        conn.close()
        print("БД пуста, синхронизация не нужна")
        return 0

    # Получаем текущие открытые позиции
    positions = get_all_positions(sess, -1)
    open_pos_set = set()
    print(len(positions), len(db_rows))
    if len(positions) == len(db_rows):
        print("Все позиции в БД соответствуют открытым на Bybit")
        conn.close()
        return


    for pos in positions:
        try:
            symbol = pos.get('symbol', '')
            side_num = pos.get('side', '')  # 'Buy' или 'Sell'
            size = float(pos.get('size', 0) or 0)
            if size > 0 and symbol:
                side_db = 'Buy' if side_num == 'Buy' else 'Sell'
                open_pos_set.add((symbol, side_db))
        except (ValueError, TypeError):
            continue

    # Находим записи в БД без открытых позиций
    deleted_count = 0
    for symbol, side, qty in db_rows:
        pos_key = (symbol, side)
        if pos_key not in open_pos_set:
            c.execute(
                "DELETE FROM funding_trades WHERE symbol=? AND side=?",
                (symbol, side)
            )
            print(f"Удалена запись из БД: {symbol} {side} qty={qty}")
            deleted_count += 1

    if deleted_count > 0:
        conn.commit()
        print(f"Синхронизация завершена: удалено {deleted_count} записей")
    else:
        print("Все позиции в БД соответствуют открытым на Bybit")

    conn.close()
    return deleted_count

import time
import sqlite3
from decimal import Decimal

def sync_db_positions(sess):
    """
    Синхронизирует БД с текущими открытыми позициями Bybit.
    Удаляет записи для закрытых позиций и добавляет записи для новых открытых.
    """
    init_db()
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    # ---- 1. Получаем все ключи (symbol, side) из БД ----
    c.execute("SELECT symbol, side, qty FROM funding_trades")
    db_rows = c.fetchall()
    db_set = {(row[0], row[1]) for row in db_rows}  # множество существующих записей

    # ---- 2. Получаем текущие открытые позиции с биржи ----
    positions = get_all_positions(sess, -1)

    print(len(db_rows), len(positions))
    if len(positions) == 0:
        return

    if len(positions) == len(db_rows):
        print("Все позиции в БД соответствуют открытым на Bybit")
        conn.close()
        return

    open_set = set()
    open_data = {}  # { (symbol, side): {'qty': size} }
    for pos in positions:
        try:
            pnl = pos.get('unrealisedPnl', '0')
            time_open = pos.get('created_time', 0)
            symbol = pos.get('symbol', '')
            side_num = pos.get('side', '')
            size = float(pos.get('size', 0) or 0)
            if size > 0 and symbol:
                side_db = 'Buy' if side_num == 'Buy' else 'Sell'
                key = (symbol, side_db)
                open_set.add(key)
                open_data[key] = {'qty': size, 'pnl': pnl, 'time_op': time_open}
                # при необходимости можно добавить другие поля, например 'created_time'
        except (ValueError, TypeError):
            continue

    # ---- 3. Удаляем записи БД, для которых нет открытой позиции ----
    deleted_count = 0
    for symbol, side, qty in db_rows:
        key = (symbol, side)
        if key not in open_set:
            c.execute(
                "DELETE FROM funding_trades WHERE symbol=? AND side=?",
                (symbol, side)
            )
            print(f"Удалена запись из БД: {symbol} {side} qty={qty}")
            deleted_count += 1

    # ---- 4. Добавляем новые позиции, которых нет в БД ----
    added_count = 0
    for key, data in open_data.items():
        if key not in db_set:
            symbol, side = key
            qty = data['qty']
            pnl = data['pnl']
            time_open = data['time_op']
            # Для новых позиций используем текущее время как open_time,
            # qty_first = qty (начальный объём), pnl = 0
            open_time = datetime.now(timezone.utc).isoformat(),#int(time.time())        # можно заменить на pos.get('created_time', 0)
            qty_first = qty
            #pnl = 0.0

            # Вызываем вашу функцию upsert (она делает вставку или обновление)
            # Если вы не хотите коммитить внутри каждого вызова, уберите conn.commit() из upsert_funding_trade
            upsert_funding_trade(conn, symbol, side, qty, open_time, qty_first, pnl)
            print(f"Добавлена запись в БД: {symbol} {side} qty={qty}")
            added_count += 1

    # ---- 5. Фиксируем изменения и закрываем соединение ----
    if deleted_count > 0 or added_count > 0:
        conn.commit()
        print(f"Синхронизация завершена: удалено {deleted_count}, добавлено {added_count}")
    else:
        print("Все позиции в БД соответствуют открытым на Bybit")

    conn.close()
    return deleted_count, added_count

def get_ip_through_session(sess):
    """Проверяет IP адрес, с которого идет подключение через сессию"""
    try:
        # Пытаемся получить внешний IP через Bybit API (не прямой метод, но рабочий)
        response = sess.get_wallet_balance(accountType="UNIFIED", coin="USDT")
        # В ответе нет IP, но если запрос прошел - значит соединение работает
        print("Соединение работает")

        # Альтернатива - использовать requests через ту же сессию
        import requests
        # Для pybit сессии прямой доступ к requests.session недоступен,
        # поэтому просто проверяем что запросы проходят
        return True
    except Exception as e:
        print(f"Ошибка: {e}")
        return False

# ========== ОБРАБОТЧИК CALLBACK ==========

def but_1():
    """Действие для кнопки 1"""
    try:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Выполнена функция but_1()")
    except Exception as e:
        print(f"❌ Ошибка в but_1: {e}")
        send_tg(f"❌ Ошибка: {e}")


def but_2():
    """Действие для кнопки 2"""
    try:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Выполнена функция but_2()")
    except Exception as e:
        print(f"❌ Ошибка в but_2: {e}")
        send_tg(f"❌ Ошибка: {e}")


def but_3():
    """Действие для кнопки 3"""
    global but_close_all
    
    try:
        but_close_all = True
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Выполнена функция but_3()")
    except Exception as e:
        print(f"❌ Ошибка в but_3: {e}")
        send_tg(f"❌ Ошибка: {e}")


def handle_callback_query():
    """Обрабатывает нажатия на кнопки"""
    global stop_event, last_update_id, but_close_all


    print("🔄 Запущен обработчик callback-запросов...")

    while not stop_event.is_set():
        try:
            url = f'https://api.telegram.org/bot{api_t}/getUpdates'
            params = {
                'offset': last_update_id + 1,
                'timeout': 30
            }

            response = requests.get(url, params=params, timeout=35)

            if response.status_code != 200:
                time.sleep(1)
                continue

            data = response.json()

            if not data.get('ok'):
                continue

            for update in data.get('result', []):
                last_update_id = update['update_id']

                # Обрабатываем callback query
                if 'callback_query' in update:
                    
                    callback = update['callback_query']
                    callback_id = callback['id']
                    message = callback['message']
                    chat_id = message['chat']['id']
                    message_id = message['message_id']
                    data_callback = callback['data']

                    print(f"📩 Нажата кнопка: {data_callback}")

                    # Вызываем соответствующую функцию
                    if data_callback == "button_1":
                        threading.Thread(target=but_1, daemon=True).start()
                    elif data_callback == "button_2":
                        threading.Thread(target=but_2, daemon=True).start()
                    elif data_callback == "button_3":
                        threading.Thread(target=but_3, daemon=True).start()
                    

        except Exception as e:
            print(f"❌ Ошибка в handle_callback_query: {e}")
            time.sleep(1)

    print("🛑 Обработчик остановлен")

# Сохранение симовлов для открытия в случае пропуска времени в результате выполненеия других операций

def write_symbols_for_open(symbols_list: List[str]) -> None:
    """
    Записывает или перезаписывает список символов в единственную строку
    """
    init_db()

    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # Сохраняем список как JSON строку
    symbols_json = json.dumps(symbols_list, ensure_ascii=False)

    # UPSERT - обновляем если есть, вставляем если нет
    cursor.execute(f'''
        INSERT INTO for_open (id, symbols) 
        VALUES (?, ?)
        ON CONFLICT(id) DO UPDATE SET 
            symbols = excluded.symbols
    ''', (1, symbols_json))

    conn.commit()
    conn.close()
    print(f"✅ Записано {len(symbols_list)} символов")

def read_symbols_for_open() -> Optional[List[str]]:
    """
    Читает список символов из базы
    Возвращает список или None если данных нет
    """
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    cursor.execute(f'''
        SELECT symbols FROM for_open WHERE id = ?
    ''', (1,))

    row = cursor.fetchone()
    conn.close()

    if row and row[0]:
        return json.loads(row[0])
    return []

def is_extreme_movement_last(sess, symbol, lookback_days=7, extreme_threshold=3.0, rvol_threshold=3.0, global_range_days=3):
    """
    Проверяет, можно ли торговать монету по стратегии фандинга.
    Возвращает:
        True  - монета стабильна (можно торговать)
        False - монета пропущена (памп/дамп или экстремальная волатильность)
    С принтами причин пропуска.
    """
    try:
        # ---- 1. Запрашиваем достаточно данных для RVOL (минимум 14 дней) ----
        days_for_data = max(lookback_days + 2, 14)          # минимум 14 дней
        start_time = int((datetime.now() - timedelta(days=days_for_data)).timestamp() * 1000)
        limit = days_for_data * 24 + 48                     # запас свечей

        resp = sess.get_kline(
            category="linear",
            symbol=symbol,
            interval="60",
            start=start_time,
            limit=limit
        )

        if resp['retCode'] != 0 or not resp['result']['list']:
            print(f"⚠️ {symbol}: Нет данных - разрешаем торговлю")
            return True

        candles = resp['result']['list']
        if len(candles) < 48:
            print(f"⚠️ {symbol}: Мало данных ({len(candles)}) - разрешаем")
            return True

        # Парсим свечи в удобный формат
        ohlc = []
        for c in candles:
            try:
                ohlc.append({
                    'open': float(c[1]),
                    'high': float(c[2]),
                    'low': float(c[3]),
                    'close': float(c[4]),
                    'volume': float(c[5])
                })
            except:
                continue

        if len(ohlc) < 24:
            print(f"⚠️ {symbol}: Недостаточно свечей после парсинга - разрешаем")
            return True

        current_price = ohlc[-1]['close']

        # ---- 2. Недельный размах (глобальный памп/дамп) ----
        range_window = global_range_days * 24
        if len(ohlc) >= range_window:
            range_high = max(c['high'] for c in ohlc[-range_window:])
            range_low = min(c['low'] for c in ohlc[-range_window:])
            range_change = (range_high - range_low) / range_low * 100
            if range_change > 100:
                print(
                    f"❌ {symbol}: Пропускаем - размах за {global_range_days} дн. {range_change:.1f}% > 100% (глобальный памп/дамп)")
                return False

        # ---- 3. Суточный размах и откат от максимума ----
        daily_high = max(c['high'] for c in ohlc[-24:])
        daily_low = min(c['low'] for c in ohlc[-24:])
        daily_change = (daily_high - daily_low) / daily_low * 100
        drawdown_from_high = (daily_high - current_price) / daily_high * 100

        if daily_change > 40:
            print(f"❌ {symbol}: Пропускаем - суточный размах {daily_change:.1f}% > 40% (сильный памп/дамп)")
            return False

        if drawdown_from_high > 20 and daily_high > current_price * 1.1:
            print(f"❌ {symbol}: Пропускаем - откат от хая {drawdown_from_high:.1f}% > 20% (памп закончился)")
            return False

        # ---- 4. Последний час – экстремальная свеча ----
        last = ohlc[-1]
        candle_body = abs(last['close'] - last['open'])
        candle_range = last['high'] - last['low']

        if candle_body / last['open'] > 0.07:
            print(f"❌ {symbol}: Пропускаем - тело свечи {candle_body / last['open'] * 100:.1f}% > 7% (экстремальный импульс)")
            return False

        if candle_range / last['open'] > 0.12:
            print(f"❌ {symbol}: Пропускаем - диапазон свечи {candle_range / last['open'] * 100:.1f}% > 12% (высокая волатильность часа)")
            return False

        # ---- 5. RVOL – относительный объём (аномалия по сравнению с тем же часом в прошлые дни) ----
        # Для расчёта нужно минимум 7 полных дней (168 свечей)
        if len(ohlc) >= 24 * 7:
            # Собираем объёмы по часам за все дни, кроме последней свечи (для неё будем считать RVOL)
            hour_volumes = [[] for _ in range(24)]
            for i in range(len(ohlc) - 1):  # все свечи, кроме последней
                ts_ms = int(candles[i][0])          # временная метка из исходных данных
                #dt = datetime.utcfromtimestamp(ts_ms / 1000.0)
                dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
                hour = dt.hour
                volume = ohlc[i]['volume']
                hour_volumes[hour].append(volume)

            # Вычисляем средний объём для каждого часа (если есть хотя бы 3 замера)
            avg_volume_by_hour = []
            for h in range(24):
                vols = hour_volumes[h]
                if len(vols) >= 3:
                    avg_volume_by_hour.append(sum(vols) / len(vols))
                else:
                    avg_volume_by_hour.append(None)

            # Определяем час последней свечи и её объём
            last_ts_ms = int(candles[-1][0])
            #last_dt = datetime.utcfromtimestamp(last_ts_ms / 1000.0)
            last_dt = datetime.fromtimestamp(last_ts_ms / 1000.0, tz=timezone.utc)
            last_hour = last_dt.hour
            last_volume = ohlc[-1]['volume']

            avg_vol = avg_volume_by_hour[last_hour]
            if avg_vol is not None and avg_vol > 0:
                rvol = last_volume / avg_vol
                if rvol > rvol_threshold:
                    print(f"❌ {symbol}: Пропускаем - RVOL = {rvol:.2f} > {rvol_threshold} (аномальный всплеск объёма)")
                    return False
                # иначе объём в норме – проходим
            else:
                # Недостаточно данных для расчёта среднего по часу – используем запасную логику
                if len(ohlc) >= 24:
                    avg_volume_24h = sum(c['volume'] for c in ohlc[-24:]) / 24
                    avg_volume_6h = sum(c['volume'] for c in ohlc[-6:]) / 6
                    if avg_volume_6h < avg_volume_24h * 0.3 and avg_volume_24h > 0:
                        print(f"❌ {symbol}: Пропускаем - объём упал до {avg_volume_6h / avg_volume_24h * 100:.1f}% от среднего (активность угасла)")
                        return False
        else:
            # Не хватает истории для RVOL – используем старую проверку
            if len(ohlc) >= 24:
                avg_volume_24h = sum(c['volume'] for c in ohlc[-24:]) / 24
                avg_volume_6h = sum(c['volume'] for c in ohlc[-6:]) / 6
                if avg_volume_6h < avg_volume_24h * 0.3 and avg_volume_24h > 0:
                    print(f"❌ {symbol}: Пропускаем - объём упал до {avg_volume_6h / avg_volume_24h * 100:.1f}% от среднего (активность угасла)")
                    return False

        # ---- 6. Статистическая проверка (экстремальные отклонения) ----
        prices = [c['close'] for c in ohlc if c['close'] > 0]
        returns = []
        for i in range(1, len(prices)):
            if prices[i - 1] > 0:
                returns.append(math.log(prices[i] / prices[i - 1]))

        if len(returns) >= 24:
            normal_returns = returns[:-24] if len(returns) > 24 else returns
            recent_returns = returns[-24:] if len(returns) >= 24 else returns
            if len(normal_returns) < 10:
                normal_returns = returns

            mean_norm = sum(normal_returns) / len(normal_returns)
            std_norm = (sum((x - mean_norm) ** 2 for x in normal_returns) / len(normal_returns)) ** 0.5
            if std_norm < 0.0001:
                std_norm = 0.0001

            extreme_count = sum(1 for r in recent_returns if abs(r) > extreme_threshold * std_norm)
            total_24h_return = sum(recent_returns)

            has_extreme_24h = abs(total_24h_return) > 0.12
            has_extreme_candle = any(abs(r) > 0.06 for r in recent_returns)

            if extreme_count >= 2 or has_extreme_24h or has_extreme_candle:
                print(f"❌ {symbol}: Пропускаем - экстремальное движение (сигм: {extreme_count}, 24ч: {total_24h_return * 100:.1f}%)")
                return False

        # ---- 7. ATR (часовая волатильность) ----
        if len(ohlc) >= 14:
            true_ranges = []
            for i in range(1, min(15, len(ohlc))):
                high = ohlc[-i]['high']
                low = ohlc[-i]['low']
                prev_close = ohlc[-i - 1]['close'] if i < len(ohlc) else ohlc[-i]['close']
                tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
                true_ranges.append(tr)
            atr = sum(true_ranges) / len(true_ranges)
            atr_percent = atr / current_price * 100
            if atr_percent > 4:
                print(f"❌ {symbol}: Пропускаем - часовая волатильность {atr_percent:.1f}% > 4% (слишком высокая)")
                return False

        # ---- Все проверки пройдены ----
        print(f"✅ {symbol}: Можно торговать - движение в норме")
        return True

    except Exception as e:
        print(f"⚠️ {symbol}: Ошибка проверки - {str(e)}")
        return True

def is_extreme_movement(sess, symbol, lookback_days=7, extreme_threshold=3.0,
                        rvol_threshold=3.0, global_range_days=3,
                        drawdown_days=3, drawdown_threshold=25,
                        price_ratio_threshold=1.2):
    """
    Проверяет, можно ли торговать монету по стратегии фандинга.
    Добавлена проверка на падение от максимума за drawdown_days дней.
    """
    try:
        # ---- 1. Запрашиваем данные ----
        days_for_data = max(lookback_days + 2, 14, drawdown_days + 2)
        start_time = int((datetime.now() - timedelta(days=days_for_data)).timestamp() * 1000)
        limit = days_for_data * 24 + 48

        resp = sess.get_kline(
            category="linear",
            symbol=symbol,
            interval="60",
            start=start_time,
            limit=limit
        )

        if resp['retCode'] != 0 or not resp['result']['list']:
            print(f"⚠️ {symbol}: Нет данных - разрешаем торговлю")
            return True

        candles = resp['result']['list']
        if len(candles) < 48:
            print(f"⚠️ {symbol}: Мало данных ({len(candles)}) - разрешаем")
            return True

        # Парсим свечи
        ohlc = []
        for c in candles:
            try:
                ohlc.append({
                    'open': float(c[1]),
                    'high': float(c[2]),
                    'low': float(c[3]),
                    'close': float(c[4]),
                    'volume': float(c[5])
                })
            except:
                continue

        if len(ohlc) < 24:
            print(f"⚠️ {symbol}: Недостаточно свечей после парсинга - разрешаем")
            return True

        current_price = ohlc[-1]['close']

        # ========== НОВАЯ ПРОВЕРКА: падение от максимума за N дней ==========
        window = drawdown_days * 24
        if len(ohlc) >= window:
            max_price = max(c['high'] for c in ohlc[-window:])
            drawdown = (max_price - current_price) / max_price * 100
            # Если падение значительное и максимум был существенно выше текущей цены
            if drawdown > drawdown_threshold and max_price > current_price * price_ratio_threshold:
                print(f"❌ {symbol}: Пропускаем - падение от максимума за {drawdown_days} дн. {drawdown:.1f}% > {drawdown_threshold}% (трендовый дамп)")
                return False

        # ---- 2. Недельный размах (глобальный памп/дамп) ----
        range_window = global_range_days * 24
        if len(ohlc) >= range_window:
            range_high = max(c['high'] for c in ohlc[-range_window:])
            range_low = min(c['low'] for c in ohlc[-range_window:])
            range_change = (range_high - range_low) / range_low * 100
            if range_change > 100:
                print(f"❌ {symbol}: Пропускаем - размах за {global_range_days} дн. {range_change:.1f}% > 100% (глобальный памп/дамп)")
                return False

        # ---- 3. Суточный размах и откат от максимума (усиленная версия) ----
        daily_high = max(c['high'] for c in ohlc[-24:])
        daily_low = min(c['low'] for c in ohlc[-24:])
        daily_change = (daily_high - daily_low) / daily_low * 100
        # Откат от максимума за сутки
        drawdown_daily = (daily_high - current_price) / daily_high * 100

        if daily_change > 40:
            print(f"❌ {symbol}: Пропускаем - суточный размах {daily_change:.1f}% > 40% (сильный памп/дамп)")
            return False

        # Усиливаем: если откат от суточного хая > 15% и хай был выше 1.05*текущей цены
        if drawdown_daily > 15 and daily_high > current_price * 1.05:
            print(f"❌ {symbol}: Пропускаем - откат от дневного хая {drawdown_daily:.1f}% > 15%")
            return False

        # ---- 4. Последний час – экстремальная свеча ----
        last = ohlc[-1]
        candle_body = abs(last['close'] - last['open'])
        candle_range = last['high'] - last['low']

        if candle_body / last['open'] > 0.07:
            print(f"❌ {symbol}: Пропускаем - тело свечи {candle_body / last['open'] * 100:.1f}% > 7% (экстремальный импульс)")
            return False

        if candle_range / last['open'] > 0.12:
            print(f"❌ {symbol}: Пропускаем - диапазон свечи {candle_range / last['open'] * 100:.1f}% > 12% (высокая волатильность часа)")
            return False

        # ---- 5. RVOL – относительный объём ----
        if len(ohlc) >= 24 * 7:
            hour_volumes = [[] for _ in range(24)]
            for i in range(len(ohlc) - 1):
                ts_ms = int(candles[i][0])
                dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
                hour = dt.hour
                volume = ohlc[i]['volume']
                hour_volumes[hour].append(volume)

            avg_volume_by_hour = []
            for h in range(24):
                vols = hour_volumes[h]
                if len(vols) >= 3:
                    avg_volume_by_hour.append(sum(vols) / len(vols))
                else:
                    avg_volume_by_hour.append(None)

            last_ts_ms = int(candles[-1][0])
            last_dt = datetime.fromtimestamp(last_ts_ms / 1000.0, tz=timezone.utc)
            last_hour = last_dt.hour
            last_volume = ohlc[-1]['volume']

            avg_vol = avg_volume_by_hour[last_hour]
            if avg_vol is not None and avg_vol > 0:
                rvol = last_volume / avg_vol
                if rvol > rvol_threshold:
                    print(f"❌ {symbol}: Пропускаем - RVOL = {rvol:.2f} > {rvol_threshold} (аномальный всплеск объёма)")
                    return False
            else:
                if len(ohlc) >= 24:
                    avg_volume_24h = sum(c['volume'] for c in ohlc[-24:]) / 24
                    avg_volume_6h = sum(c['volume'] for c in ohlc[-6:]) / 6
                    if avg_volume_6h < avg_volume_24h * 0.3 and avg_volume_24h > 0:
                        print(f"❌ {symbol}: Пропускаем - объём упал до {avg_volume_6h / avg_volume_24h * 100:.1f}% от среднего (активность угасла)")
                        return False
        else:
            if len(ohlc) >= 24:
                avg_volume_24h = sum(c['volume'] for c in ohlc[-24:]) / 24
                avg_volume_6h = sum(c['volume'] for c in ohlc[-6:]) / 6
                if avg_volume_6h < avg_volume_24h * 0.3 and avg_volume_24h > 0:
                    print(f"❌ {symbol}: Пропускаем - объём упал до {avg_volume_6h / avg_volume_24h * 100:.1f}% от среднего (активность угасла)")
                    return False

        # ---- 6. Статистическая проверка ----
        prices = [c['close'] for c in ohlc if c['close'] > 0]
        returns = []
        for i in range(1, len(prices)):
            if prices[i - 1] > 0:
                returns.append(math.log(prices[i] / prices[i - 1]))

        if len(returns) >= 24:
            normal_returns = returns[:-24] if len(returns) > 24 else returns
            recent_returns = returns[-24:] if len(returns) >= 24 else returns
            if len(normal_returns) < 10:
                normal_returns = returns

            mean_norm = sum(normal_returns) / len(normal_returns)
            std_norm = (sum((x - mean_norm) ** 2 for x in normal_returns) / len(normal_returns)) ** 0.5
            if std_norm < 0.0001:
                std_norm = 0.0001

            extreme_count = sum(1 for r in recent_returns if abs(r) > extreme_threshold * std_norm)
            total_24h_return = sum(recent_returns)

            has_extreme_24h = abs(total_24h_return) > 0.12
            has_extreme_candle = any(abs(r) > 0.06 for r in recent_returns)

            if extreme_count >= 2 or has_extreme_24h or has_extreme_candle:
                print(f"❌ {symbol}: Пропускаем - экстремальное движение (сигм: {extreme_count}, 24ч: {total_24h_return * 100:.1f}%)")
                return False

        # ---- 7. ATR ----
        if len(ohlc) >= 14:
            true_ranges = []
            for i in range(1, min(15, len(ohlc))):
                high = ohlc[-i]['high']
                low = ohlc[-i]['low']
                prev_close = ohlc[-i - 1]['close'] if i < len(ohlc) else ohlc[-i]['close']
                tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
                true_ranges.append(tr)
            atr = sum(true_ranges) / len(true_ranges)
            atr_percent = atr / current_price * 100
            if atr_percent > 4:
                print(f"❌ {symbol}: Пропускаем - часовая волатильность {atr_percent:.1f}% > 4% (слишком высокая)")
                return False

        print(f"✅ {symbol}: Можно торговать - движение в норме")
        return True

    except Exception as e:
        print(f"⚠️ {symbol}: Ошибка проверки - {str(e)}")
        return True

def is_extreme_movement_old(sess, symbol, lookback_days=7, extreme_threshold=3.0):
    """
    Проверяет, можно ли торговать монету по стратегии фандинга.
    Возвращает:
        True  - монета стабильна (можно торговать)
        False - монета пропущена (памп/дамп или экстремальная волатильность)
    С принтами причин пропуска.
    """
    try:
        # Получаем часовые свечи
        start_time = int((datetime.now() - timedelta(days=lookback_days + 2)).timestamp() * 1000)
        resp = sess.get_kline(
            category="linear",
            symbol=symbol,
            interval="60",
            start=start_time,
            limit=(lookback_days + 2) * 24 + 48
        )
        
        if resp['retCode'] != 0 or not resp['result']['list']:
            print(f"⚠️ {symbol}: Нет данных - разрешаем торговлю")
            return True
        
        candles = resp['result']['list']
        if len(candles) < 48:
            print(f"⚠️ {symbol}: Мало данных ({len(candles)}) - разрешаем")
            return True
        
        # Парсим свечи
        ohlc = []
        for c in candles:
            try:
                ohlc.append({
                    'open': float(c[1]),
                    'high': float(c[2]),
                    'low': float(c[3]),
                    'close': float(c[4]),
                    'volume': float(c[5])
                })
            except:
                continue
        
        if len(ohlc) < 24:
            print(f"⚠️ {symbol}: Недостаточно свечей после парсинга - разрешаем")
            return True
        
        current_price = ohlc[-1]['close']
        
        # ------------------------------------------------------------
        # 1. Недельный размах (глобальный памп/дамп)
        # ------------------------------------------------------------
        if len(ohlc) >= 168:
            weekly_high = max(c['high'] for c in ohlc[-168:])
            weekly_low = min(c['low'] for c in ohlc[-168:])
            weekly_change = (weekly_high - weekly_low) / weekly_low * 100
            if weekly_change > 100:   # повышен с 80 до 100, чтобы не отсекать многие альты
                print(f"❌ {symbol}: Пропускаем - недельный размах {weekly_change:.1f}% > 100% (глобальный памп/дамп)")
                return False
        
        # ------------------------------------------------------------
        # 2. Суточный размах и откат от максимума
        # ------------------------------------------------------------
        daily_high = max(c['high'] for c in ohlc[-24:])
        daily_low = min(c['low'] for c in ohlc[-24:])
        daily_change = (daily_high - daily_low) / daily_low * 100
        drawdown_from_high = (daily_high - current_price) / daily_high * 100
        
        # Слишком большой размах за сутки (>40%) – памп/дамп
        if daily_change > 40:
            print(f"❌ {symbol}: Пропускаем - суточный размах {daily_change:.1f}% > 40% (сильный памп/дамп)")
            return False
        
        # Откат от максимума > 20% – памп уже завершился, фандинг скоро упадет
        if drawdown_from_high > 20 and daily_high > current_price * 1.1:
            print(f"❌ {symbol}: Пропускаем - откат от хая {drawdown_from_high:.1f}% > 20% (памп закончился)")
            return False
        
        # ------------------------------------------------------------
        # 3. Последний час – экстремальная свеча
        # ------------------------------------------------------------
        last = ohlc[-1]
        candle_body = abs(last['close'] - last['open'])
        candle_range = last['high'] - last['low']
        
        if candle_body / last['open'] > 0.07:   # снижено с 10% до 7% для большей чувствительности
            print(f"❌ {symbol}: Пропускаем - тело свечи {candle_body/last['open']*100:.1f}% > 7% (экстремальный импульс)")
            return False
        
        if candle_range / last['open'] > 0.12:  # снижено с 15% до 12%
            print(f"❌ {symbol}: Пропускаем - диапазон свечи {candle_range/last['open']*100:.1f}% > 12% (высокая волатильность часа)")
            return False
        
        # ------------------------------------------------------------
        # 4. Падение объема (активность угасла после пампа)
        # ------------------------------------------------------------
        if len(ohlc) >= 24:
            avg_volume_24h = sum(c['volume'] for c in ohlc[-24:]) / 24
            avg_volume_6h = sum(c['volume'] for c in ohlc[-6:]) / 6
            if avg_volume_6h < avg_volume_24h * 0.3 and avg_volume_24h > 0:  # порог 0.3 вместо 0.2
                print(f"❌ {symbol}: Пропускаем - объем упал до {avg_volume_6h/avg_volume_24h*100:.1f}% от среднего (активность угасла)")
                return False
        
        # ------------------------------------------------------------
        # 5. Статистическая проверка (экстремальные отклонения)
        # ------------------------------------------------------------
        prices = [c['close'] for c in ohlc if c['close'] > 0]
        returns = []
        for i in range(1, len(prices)):
            if prices[i-1] > 0:
                returns.append(math.log(prices[i] / prices[i-1]))
        
        if len(returns) >= 24:
            normal_returns = returns[:-24] if len(returns) > 24 else returns
            recent_returns = returns[-24:] if len(returns) >= 24 else returns
            if len(normal_returns) < 10:
                normal_returns = returns
            
            mean_norm = sum(normal_returns) / len(normal_returns)
            std_norm = (sum((x - mean_norm) ** 2 for x in normal_returns) / len(normal_returns)) ** 0.5
            if std_norm < 0.0001:
                std_norm = 0.0001
            
            extreme_count = sum(1 for r in recent_returns if abs(r) > extreme_threshold * std_norm)
            total_24h_return = sum(recent_returns)
            
            has_extreme_24h = abs(total_24h_return) > 0.12   # снижено с 0.15 до 0.12
            has_extreme_candle = any(abs(r) > 0.06 for r in recent_returns)  # снижено с 0.08 до 0.06
            
            if extreme_count >= 2 or has_extreme_24h or has_extreme_candle:
                print(f"❌ {symbol}: Пропускаем - экстремальное движение (сигм: {extreme_count}, 24ч: {total_24h_return*100:.1f}%)")
                return False
        
        # ------------------------------------------------------------
        # 6. ATR (часовая волатильность)
        # ------------------------------------------------------------
        if len(ohlc) >= 14:
            true_ranges = []
            for i in range(1, min(15, len(ohlc))):
                high = ohlc[-i]['high']
                low = ohlc[-i]['low']
                prev_close = ohlc[-i-1]['close'] if i < len(ohlc) else ohlc[-i]['close']
                tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
                true_ranges.append(tr)
            atr = sum(true_ranges) / len(true_ranges)
            atr_percent = atr / current_price * 100
            if atr_percent > 4:   # снижено с 5% до 4%
                print(f"❌ {symbol}: Пропускаем - часовая волатильность {atr_percent:.1f}% > 4% (слишком высокая)")
                return False
        
        # ------------------------------------------------------------
        # Все проверки пройдены
        # ------------------------------------------------------------
        print(f"✅ {symbol}: Можно торговать - движение в норме")
        return True
        
    except Exception as e:
        print(f"⚠️ {symbol}: Ошибка проверки - {str(e)}")
        return True

def is_extreme_movement_conservative(
        sess,
        symbol,
        side="SHORT",
        rvol_1h_limit=2.5,
        rvol_3h_limit=2.0,
        rvol_6h_limit=1.7
):
    """
    Жёсткий фильтр для funding + averaging.
    Цель: не входить против пампа/дампа.
    """

    try:

        print(f"\n🔎 Проверка {symbol} | сторона {side}")

        days = 10
        start_time = int(
            (datetime.now() - timedelta(days=days)).timestamp()*1000
        )

        resp = sess.get_kline(
            category="linear",
            symbol=symbol,
            interval="60",
            start=start_time,
            limit=250
        )


        if resp["retCode"] != 0:
            print("⚠ Нет данных")
            return True


        candles = resp["result"]["list"]


        ohlc=[]

        for c in candles:
            ohlc.append({
                "open":float(c[1]),
                "high":float(c[2]),
                "low":float(c[3]),
                "close":float(c[4]),
                "volume":float(c[5])
            })


        if len(ohlc)<100:
            print("⚠ Мало свечей")
            return True


        score=0


        price=ohlc[-1]["close"]


        # =====================================
        # 1. Движение цены
        # =====================================

        def change(hours):
            old=ohlc[-hours]["close"]
            return (price-old)/old*100


        move12=change(12)
        move24=change(24)


        print(
            f"Цена 12ч: {move12:.2f}% | "
            f"24ч: {move24:.2f}%"
        )


        if side=="SHORT" and move12>8:
            score+=3
            print("⚠ Рост против SHORT")

        if side=="LONG" and move12<-8:
            score+=3
            print("⚠ Падение против LONG")



        # =====================================
        # 2. Cooldown после сильного движения
        # =====================================

        if abs(move12)>15:

            score+=4

            print(
                f"🚨 Сильное движение 12ч {move12:.1f}%"
            )



        if abs(move24)>25:

            score+=4

            print(
                f"🚨 Экстремальное движение 24ч {move24:.1f}%"
            )



        # =====================================
        # 3. RVOL
        # =====================================

        volumes=[
            x["volume"] for x in ohlc
        ]


        def calc_rvol(hours):

            current=sum(volumes[-hours:])

            avg=sum(
                volumes[-hours-168:-hours]
            )/hours

            return current/avg if avg else 0



        rvol1=calc_rvol(1)
        rvol3=calc_rvol(3)
        rvol6=calc_rvol(6)


        print(
            f"RVOL 1h={rvol1:.2f} "
            f"3h={rvol3:.2f} "
            f"6h={rvol6:.2f}"
        )


        if rvol1>rvol_1h_limit:
            score+=2
            print("⚠ Аномальный часовой объём")


        if rvol3>rvol_3h_limit:
            score+=3
            print("⚠ Объёмный импульс 3h")


        if rvol6>rvol_6h_limit:
            score+=3
            print("⚠ Долгий объёмный импульс")



        # =====================================
        # 4. Ускорение объёма
        # =====================================

        vol_now=sum(volumes[-3:])
        vol_before=sum(volumes[-6:-3])


        acceleration=vol_now/vol_before if vol_before else 0


        print(
            f"Ускорение объёма: {acceleration:.2f}"
        )


        if acceleration>2:
            score+=2
            print(
                "⚠ Объём резко ускорился"
            )



        # =====================================
        # 5. Импульсные свечи
        # =====================================

        green=sum(
            1 for c in ohlc[-5:]
            if c["close"]>c["open"]
        )

        red=5-green


        if side=="SHORT" and green>=4:
            score+=2
            print(
                "⚠ 4/5 зелёных свечей"
            )


        if side=="LONG" and red>=4:
            score+=2
            print(
                "⚠ 4/5 красных свечей"
            )



        # =====================================
        # Решение
        # =====================================

        print(
            f"ИТОГОВЫЙ SCORE: {score}"
        )


        if score>=6:

            print(
                "❌ Фильтр блокирует вход"
            )

            return False


        print(
            "✅ Торговля разрешена"
        )

        return True



    except Exception as e:

        print(
            f"Ошибка фильтра {symbol}: {e}"
        )

        return True

def is_extreme_movement_balanced(
        sess,
        symbol,
        side="SHORT",
        rvol_1h_limit=3.0,
        rvol_3h_limit=2.5,
        rvol_6h_limit=2.0
):
    """
    Balanced фильтр для funding + averaging.

    Проверяет:
    - сильные движения цены;
    - направление движения относительно сделки;
    - multi-RVOL;
    - ускорение объёма;
    - импульсные свечи.

    Возвращает:
        True  - можно торговать
        False - монета опасна, пропуск

    side:
        LONG
        SHORT
    """

    try:

        print(f"\n🔎 BALANCED FILTER: {symbol} | {side}")


        # ==================================================
        # Получение данных
        # ==================================================

        days = 10

        start_time = int(
            (datetime.now() - timedelta(days=days)).timestamp() * 1000
        )


        resp = sess.get_kline(
            category="linear",
            symbol=symbol,
            interval="60",
            start=start_time,
            limit=250
        )


        if resp["retCode"] != 0:
            print(
                f"⚠ {symbol}: ошибка получения свечей"
            )
            return True


        candles = resp["result"]["list"]


        if len(candles) < 100:
            print(
                f"⚠ {symbol}: мало данных {len(candles)}"
            )
            return True



        ohlc=[]


        for c in candles:

            try:

                ohlc.append({

                    "open":float(c[1]),
                    "high":float(c[2]),
                    "low":float(c[3]),
                    "close":float(c[4]),
                    "volume":float(c[5])

                })

            except:

                continue



        if len(ohlc)<100:

            print(
                "⚠ Недостаточно свечей после обработки"
            )

            return True



        score = 0


        current_price = ohlc[-1]["close"]



        # ==================================================
        # 1. Движение цены
        # ==================================================

        def price_change(hours):

            if len(ohlc)<=hours:
                return 0

            old = ohlc[-hours]["close"]

            return (
                (current_price-old)
                /
                old
                *
                100
            )



        move6 = price_change(6)
        move12 = price_change(12)
        move24 = price_change(24)


        print(
            f"📈 Цена:"
            f" 6h {move6:.2f}%"
            f" | 12h {move12:.2f}%"
            f" | 24h {move24:.2f}%"
        )



        # движение против позиции

        if side.upper()=="SHORT":

            if move12 > 10:

                score += 3

                print(
                    "⚠ SHORT против сильного роста 12h"
                )


            if move24 > 20:

                score += 3

                print(
                    "⚠ SHORT против роста 24h"
                )



        elif side.upper()=="LONG":


            if move12 < -10:

                score += 3

                print(
                    "⚠ LONG против сильного падения 12h"
                )


            if move24 < -20:

                score += 3

                print(
                    "⚠ LONG против падения 24h"
                )



        # абсолютное движение

        if abs(move12) > 18:

            score += 2

            print(
                f"⚠ Экстремальное движение 12h {move12:.1f}%"
            )


        if abs(move24) > 30:

            score += 3

            print(
                f"⚠ Экстремальное движение 24h {move24:.1f}%"
            )



        # ==================================================
        # 2. Multi RVOL
        # ==================================================

        volumes = [
            c["volume"]
            for c in ohlc
        ]



        def calculate_rvol(hours):

            if len(volumes) < hours*8:

                return 0


            current_volume = sum(
                volumes[-hours:]
            )


            history = volumes[
                -(hours*8):-hours
            ]


            avg_volume = sum(history) / len(history)


            if avg_volume <= 0:

                return 0


            return current_volume / avg_volume



        rvol1 = calculate_rvol(1)
        rvol3 = calculate_rvol(3)
        rvol6 = calculate_rvol(6)



        print(
            f"📊 RVOL:"
            f" 1h={rvol1:.2f}"
            f" 3h={rvol3:.2f}"
            f" 6h={rvol6:.2f}"
        )



        if rvol1 > rvol_1h_limit:

            score += 2

            print(
                "⚠ RVOL 1h повышенный"
            )


        if rvol3 > rvol_3h_limit:

            score += 2

            print(
                "⚠ RVOL 3h повышенный"
            )


        if rvol6 > rvol_6h_limit:

            score += 2

            print(
                "⚠ RVOL 6h повышенный"
            )



        # ==================================================
        # 3. Ускорение объёма
        # ==================================================

        recent_volume = sum(
            volumes[-3:]
        )

        previous_volume = sum(
            volumes[-6:-3]
        )


        if previous_volume > 0:

            volume_acceleration = (
                recent_volume
                /
                previous_volume
            )

        else:

            volume_acceleration = 0



        print(
            f"🚀 Ускорение объёма:"
            f" {volume_acceleration:.2f}"
        )



        if volume_acceleration > 2.5:

            score += 2

            print(
                "⚠ Объём резко ускорился"
            )



        # ==================================================
        # 4. Последние импульсные свечи
        # ==================================================

        last5 = ohlc[-5:]


        green = sum(
            1
            for c in last5
            if c["close"] > c["open"]
        )


        red = 5-green



        if side.upper()=="SHORT" and green >=4:

            score +=2

            print(
                "⚠ 4+ зелёные свечи подряд"
            )



        if side.upper()=="LONG" and red >=4:

            score +=2

            print(
                "⚠ 4+ красные свечи подряд"
            )



        # ==================================================
        # 5. Размер последних свечей
        # ==================================================

        for c in ohlc[-3:]:

            candle_move = (
                abs(c["close"]-c["open"])
                /
                c["open"]
                *
                100
            )


            if candle_move > 8:

                score +=1

                print(
                    f"⚠ Большая свеча {candle_move:.1f}%"
                )



        # ==================================================
        # Финальное решение
        # ==================================================

        print(
            f"🎯 FINAL SCORE = {score}"
        )



        # Balanced порог

        if score >= 8:

            print(
                "❌ BALANCED FILTER: ПРОПУСК"
            )

            return False



        print(
            "✅ BALANCED FILTER: разрешено"
        )

        return True



    except Exception as e:

        print(
            f"⚠ BALANCED FILTER ERROR {symbol}: {e}"
        )

        return True



def rename_file(old_path: str, new_path: str) -> bool:
    """
    Переименовывает файл.

    Аргументы:
        old_path (str): полный путь к существующему файлу.
        new_path (str): новый путь (новое имя файла).

    Возвращает:
        bool: True, если переименование выполнено успешно, иначе False.
    """
    try:
        # Проверка существования исходного файла
        if not os.path.isfile(old_path):
            print(f"Ошибка: файл '{old_path}' не найден.")
            return False

        # Переименование
        os.rename(old_path, new_path)
        print(f"Файл успешно переименован в '{new_path}'.")
        return True

    except PermissionError:
        print(f"Ошибка доступа: нет прав на переименование '{old_path}'.")
    except FileExistsError:
        print(f"Ошибка: файл '{new_path}' уже существует.")
    except OSError as e:
        print(f"Системная ошибка: {e}")
    except Exception as e:
        print(f"Неизвестная ошибка: {e}")
    return False

# Глобальный словарь для хранения времени окончания cooldown
_extreme_cooldown: Dict[str, datetime] = {}

def is_extreme_movement_hybrid(
    sess,
    symbol: str,
    side: str = "SHORT",                  # "LONG" или "SHORT"
    rvol_1h_limit: float = 3.0,
    rvol_3h_limit: float = 2.5,
    rvol_6h_limit: float = 2.0,
    score_limit: int = 7,
    cooldown_hours: int = 24,
    range_12h_threshold: float = 20.0,    # активация cooldown при размахе 12h > %
    hard_move_limit: float = 15.0,        # движение 12h для hard block
    hard_rvol_limit: float = 2.0,         # RVOL 3h для hard block
    drawdown_days: int = 3,
    drawdown_threshold: float = 25.0,
    global_range_days: int = 3,
    global_range_limit: float = 100.0,
    daily_range_limit: float = 40.0,
    atr_median_multiplier: float = 2.0,   # во сколько раз текущий ATR может превышать медианный
    extreme_sigma: float = 3.0,
    sigma_24h_limit: float = 0.12,
    sigma_candle_limit: float = 0.06,
) -> bool:
    """
    Гибридный фильтр для фандинг-стратегии с усреднением.
    Возвращает True, если торговля разрешена, иначе False.
    side: "LONG" или "SHORT" – направление открываемой позиции.
    """
    global _extreme_cooldown

    # ----- Cooldown по времени (если есть) -----
    if symbol in _extreme_cooldown:
        if datetime.now(timezone.utc) < _extreme_cooldown[symbol]:
            print(f"⏳ {symbol}: Cooldown до {_extreme_cooldown[symbol].strftime('%H:%M')}")
            return False
        else:
            del _extreme_cooldown[symbol]

    try:
        print(f"\n🔎 FILTER: {symbol} | {side}")

        # ---- 1. Получение данных ----
        days = 14
        start_time = int((datetime.now() - timedelta(days=days)).timestamp() * 1000)
        resp = sess.get_kline(
            category="linear",
            symbol=symbol,
            interval="60",
            start=start_time,
            limit=days * 24 + 100
        )
        if resp['retCode'] != 0 or not resp['result']['list']:
            print(f"⚠️ {symbol}: Нет данных → разрешаем")
            return True

        candles = resp['result']['list']
        if len(candles) < 100:
            print(f"⚠️ {symbol}: Мало данных ({len(candles)}) → разрешаем")
            return True

        # Парсим OHLCV
        ohlc = []
        for c in candles:
            try:
                ohlc.append({
                    'open': float(c[1]),
                    'high': float(c[2]),
                    'low': float(c[3]),
                    'close': float(c[4]),
                    'volume': float(c[5])
                })
            except:
                continue
        if len(ohlc) < 48:
            print(f"⚠️ {symbol}: Недостаточно свечей → разрешаем")
            return True

        current_price = ohlc[-1]['close']
        score = 0

        # ---- Вспомогательные функции ----
        def price_change(hours):
            if len(ohlc) <= hours:
                return 0.0
            old = ohlc[-hours]['close']
            return (current_price - old) / old * 100

        def calc_rvol(hours, lookback_days=7):
            """
            RVOL = текущий объём за hours / средний объём за те же часы за lookback_days дней.
            """
            if len(ohlc) < hours * (lookback_days + 1):
                return 0.0
            current_vol = sum(c['volume'] for c in ohlc[-hours:])
            # Собираем объёмы за те же часы за предыдущие lookback_days дней
            total_vol = 0
            count = 0
            for day in range(1, lookback_days + 1):
                start_idx = -hours - day * 24
                end_idx = -day * 24 if day > 0 else None
                # Берём окно той же длины hours, смещённое на день
                if len(ohlc) >= abs(start_idx) + hours:
                    for i in range(start_idx, start_idx + hours):
                        if i < 0:
                            total_vol += ohlc[i]['volume']
                            count += 1
            avg_vol = total_vol / count if count > 0 else 0
            return current_vol / avg_vol if avg_vol > 0 else 0

        # ---- 2. Глобальный размах (3 дня) ----
        if len(ohlc) >= global_range_days * 24:
            high = max(c['high'] for c in ohlc[-global_range_days * 24:])
            low = min(c['low'] for c in ohlc[-global_range_days * 24:])
            range_pct = (high - low) / low * 100
            if range_pct > global_range_limit:
                score += 3
                print(f"⚠️ Глобальный размах {range_pct:.1f}% > {global_range_limit}%")

        # ---- 3. Падение от максимума за N дней (с учётом, был ли памп) ----
        window = drawdown_days * 24
        if len(ohlc) >= window:
            max_price = max(c['high'] for c in ohlc[-window:])
            min_price = min(c['low'] for c in ohlc[-window:])
            drawdown = (max_price - current_price) / max_price * 100
            # Если была большая амплитуда и цена сейчас ниже максимума более чем на порог
            if drawdown > drawdown_threshold:
                # Дополнительно: если цена была значительно выше текущей, то опаснее
                if max_price > current_price * 1.2:
                    score += 4
                    print(f"⚠️ Падение от хая {drawdown:.1f}% при пампинге")
                else:
                    score += 3
                    print(f"⚠️ Падение от хая {drawdown:.1f}%")

        # ---- 4. Суточный размах ----
        daily_high = max(c['high'] for c in ohlc[-24:])
        daily_low = min(c['low'] for c in ohlc[-24:])
        daily_range = (daily_high - daily_low) / daily_low * 100
        if daily_range > daily_range_limit:
            score += 2
            print(f"⚠️ Суточный размах {daily_range:.1f}% > {daily_range_limit}%")

        # ---- 5. Движение цены (направленное) ----
        move6 = price_change(6)
        move12 = price_change(12)
        move24 = price_change(24)
        print(f"📈 Цена: 6h={move6:.2f}% 12h={move12:.2f}% 24h={move24:.2f}%")

        # ---- 6. Расстояние от максимума за 24h (защита после пампа) ----
        high_24h = max(c['high'] for c in ohlc[-24:])
        dist_from_high = (high_24h - current_price) / high_24h * 100
        print(f"📏 Откат от 24h хая: {dist_from_high:.1f}%")
        if dist_from_high < 5 and high_24h > current_price * 1.05:
            # Цена всё ещё близка к хаю, возможно продолжение
            score += 2
            print("⚠️ Цена близка к 24h максимуму")

        # ---- 7. Hard protection: если за 12h размах > порога, активируем cooldown и блокируем ----
        # Вычисляем максимум и минимум за 12 часов
        if len(ohlc) >= 12:
            high_12h = max(c['high'] for c in ohlc[-12:])
            low_12h = min(c['low'] for c in ohlc[-12:])
            range_12h = (high_12h - low_12h) / low_12h * 100
            print(f"📊 Размах 12h: {range_12h:.1f}%")
            if range_12h > range_12h_threshold:
                # Активируем cooldown
                cooldown_until = datetime.now(timezone.utc) + timedelta(hours=cooldown_hours)
                _extreme_cooldown[symbol] = cooldown_until
                print(f"🚨 Cooldown активирован! Размах 12h = {range_12h:.1f}% (блок до {cooldown_until.strftime('%H:%M')})")
                return False  # жёсткий запрет

        # ---- 8. Hard block против направления при экстремальном импульсе ----
        # Вычисляем RVOL для hard block (используем 3h, как предложено)
        rvol3 = calc_rvol(3)
        if side.upper() == "SHORT":
            if move12 > hard_move_limit and rvol3 > hard_rvol_limit:
                print(f"🚫 SHORT заблокирован: рост 12h {move12:.1f}% + RVOL3 {rvol3:.2f}")
                return False
        else:  # LONG
            if move12 < -hard_move_limit and rvol3 > hard_rvol_limit:
                print(f"🚫 LONG заблокирован: падение 12h {move12:.1f}% + RVOL3 {rvol3:.2f}")
                return False

        # ---- Дополнительный hard block для экстремального 24h движения ----
        if side.upper() == "SHORT":
            # Рост за 24h >20% и цена близка к хаю (откат <5%)
            if move24 > 20 and dist_from_high < 5:
                print(f"🚫 SHORT заблокирован: рост 24h {move24:.1f}%, откат от хая {dist_from_high:.1f}%")
                return False
            # Рост за 12h >12% + умеренный объём (RVOL3 >1.5)
            if move12 > 12 and rvol3 > 1.5:
                print(f"🚫 SHORT заблокирован: рост 12h {move12:.1f}% + RVOL3 {rvol3:.2f}")
                return False

        elif side.upper() == "LONG":
            # Падение за 24h >20% и цена близка к минимуму (откат от минимума <5%)
            # Для LONG используем расстояние от минимума (аналог dist_from_high)
            low_24h = min(c['low'] for c in ohlc[-24:])
            dist_from_low = (current_price - low_24h) / low_24h * 100
            if move24 < -20 and dist_from_low < 5:
                print(f"🚫 LONG заблокирован: падение 24h {move24:.1f}%, откат от минимума {dist_from_low:.1f}%")
                return False
            # Падение за 12h >12% + умеренный объём
            if move12 < -12 and rvol3 > 1.5:
                print(f"🚫 LONG заблокирован: падение 12h {move12:.1f}% + RVOL3 {rvol3:.2f}")
                return False

        # ---- 9. Multi‑RVOL (улучшенный, с привязкой к часам) ----
        rvol1 = calc_rvol(1)
        rvol3 = calc_rvol(3)
        rvol6 = calc_rvol(6)
        print(f"📊 RVOL: 1h={rvol1:.2f}  3h={rvol3:.2f}  6h={rvol6:.2f}")

        if rvol1 > rvol_1h_limit:
            score += 2
            print("⚠️ RVOL 1h повышен")
        if rvol3 > rvol_3h_limit:
            score += 2
            print("⚠️ RVOL 3h повышен")
        if rvol6 > rvol_6h_limit:
            score += 2
            print("⚠️ RVOL 6h повышен")

        # ---- 10. RVOL momentum (ускорение объёма) ----
        if rvol3 > 0 and rvol6 > 0:
            rvol_momentum = rvol3 / rvol6
            print(f"🚀 RVOL momentum (3h/6h): {rvol_momentum:.2f}")
            if rvol_momentum > 1.5 and rvol3 > rvol_3h_limit * 0.7:
                score += 2
                print("⚠️ Объём ускоряется (rvol3 > rvol6)")

        # ---- 11. Ускорение объёма (простое сравнение 3h vs prev 3h) ----
        volumes = [c['volume'] for c in ohlc]
        vol_recent = sum(volumes[-3:])
        vol_prev = sum(volumes[-6:-3])
        acceleration = vol_recent / vol_prev if vol_prev > 0 else 0
        print(f"📊 Ускорение объёма (3h/prev3h): {acceleration:.2f}")
        if acceleration > 2.5:
            score += 2
            print("⚠️ Объём резко ускорился")

        # ---- 12. Импульсные свечи (4 из 5) ----
        last5 = ohlc[-5:]
        green = sum(1 for c in last5 if c['close'] > c['open'])
        red = 5 - green
        if side.upper() == "SHORT" and green >= 4:
            score += 2
            print("⚠️ 4+ зелёных свечей подряд")
        elif side.upper() == "LONG" and red >= 4:
            score += 2
            print("⚠️ 4+ красных свечей подряд")

        # ---- 13. Размер последних свечей (экстремальный импульс) ----
        for c in ohlc[-3:]:
            candle_move = abs(c['close'] - c['open']) / c['open'] * 100
            if candle_move > 8:
                score += 1
                print(f"⚠️ Большая свеча {candle_move:.1f}%")

        # ---- 14. ATR адаптивный (сравнение с медианным ATR за 7 дней) ----
        if len(ohlc) >= 14:
            # Вычисляем ATR за последние 7 дней (почасовой)
            atr_values = []
            for i in range(1, min(168, len(ohlc))):
                high = ohlc[-i]['high']
                low = ohlc[-i]['low']
                prev_close = ohlc[-i-1]['close'] if i < len(ohlc) else ohlc[-i]['close']
                tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
                atr_values.append(tr)
            if atr_values:
                median_atr = sorted(atr_values)[len(atr_values)//2]
                current_atr = atr_values[-1] if atr_values else 0
                if median_atr > 0:
                    atr_ratio = current_atr / median_atr
                    print(f"📊 ATR ratio (тек/медиан): {atr_ratio:.2f}")
                    if atr_ratio > atr_median_multiplier:
                        score += 2
                        print(f"⚠️ ATR аномально высок (ratio {atr_ratio:.2f})")

        # ---- 15. Статистическая экстремальность (сигмы) ----
        prices = [c['close'] for c in ohlc if c['close'] > 0]
        returns = []
        for i in range(1, len(prices)):
            if prices[i-1] > 0:
                returns.append(math.log(prices[i] / prices[i-1]))

        if len(returns) >= 24:
            normal = returns[:-24] if len(returns) > 24 else returns
            recent = returns[-24:] if len(returns) >= 24 else returns
            if len(normal) < 10:
                normal = returns
            mean_norm = sum(normal) / len(normal)
            std_norm = (sum((x - mean_norm) ** 2 for x in normal) / len(normal)) ** 0.5
            if std_norm < 0.0001:
                std_norm = 0.0001
            extreme_count = sum(1 for r in recent if abs(r) > extreme_sigma * std_norm)
            total_return_24h = sum(recent)
            if extreme_count >= 2:
                score += 2
                print(f"⚠️ {extreme_count} экстремальных свечей (сигма >{extreme_sigma})")
            if abs(total_return_24h) > sigma_24h_limit:
                score += 2
                print(f"⚠️ 24h лог-возврат {total_return_24h*100:.1f}% > {sigma_24h_limit*100}%")
            if any(abs(r) > sigma_candle_limit for r in recent):
                score += 1
                print(f"⚠️ Есть свеча с лог-возвратом >{sigma_candle_limit*100}%")

        # ---- ФИНАЛЬНОЕ РЕШЕНИЕ ----
        print(f"🎯 FINAL SCORE = {score} (порог {score_limit})")
        if score >= score_limit:
            print(f"❌ {symbol}: ПРОПУСК (score={score})")
            return False
        else:
            print(f"✅ {symbol}: Торговля разрешена")
            return True

    except Exception as e:
        print(f"⚠️ Ошибка в фильтре {symbol}: {e}")
        return True  # при ошибке разрешаем

stop_event = threading.Event()
last_update_id = 0
but_close_all = False
all_pos = []
all_pos_akk1 = []
all_pos_akk2 = []
all_pos_akk3 = []
all_pos_akk4 = []


def analyze_setups_new(b0, b1, b2, b3):

    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # Текущие балансы в списке для удобства
    current_balances = [b0, b1, b2, b3]
    results = {}
    best_akk = None
    best_score = -float('inf')

    best_growth = -float('inf')
    best_akk = None

    for akk in range(4):
        # --- 1. Данные по позициям ---
        cursor.execute("SELECT COUNT(*) FROM open_sym WHERE akk = ?", (akk,))
        open_count = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*), SUM(pnl) FROM close_sym WHERE akk = ?", (akk,))
        close_count, total_pnl = cursor.fetchone()
        total_pnl = total_pnl or 0.0

        avg_pnl = total_pnl / close_count if close_count > 0 else 0.0

        # --- 2. Балансовые метрики ---
        # Получение стартового баланса (предполагается, что read_key_new определена)
        start_balance = None
        if akk == 0:
            read_ball = read_key_new('key_process', 'first_ball')
            if read_ball:
                start_balance = float(read_ball)
        else:
            start_balance = read_key_new('work_all_dop_akk', f'akk{akk}_open')
        # Если значение не получено, считаем его равным текущему (прирост 0)
        if start_balance is None:
            start_balance = current_balances[akk]

        current_balance = current_balances[akk]
        growth = current_balance - start_balance  # абсолютный прирост
        growth_percent = (growth / start_balance * 100) if start_balance != 0 else 0.0

        # --- 3. Интегральная метрика (исходная) ---
        if (open_count + close_count) > 0:
            close_ratio = close_count / (open_count + close_count + 1)
        else:
            close_ratio = 0.0
        score = total_pnl * close_ratio

        # Сохраняем все данные
        results[akk] = {
            'open_count': open_count,
            'close_count': close_count,
            'total_pnl': round(total_pnl, 2),
            'avg_pnl': round(avg_pnl, 2),
            'close_ratio': round(close_ratio, 4),
            'score': round(score, 2),
            'start_balance': round(start_balance, 2),
            'current_balance': round(current_balance, 2),
            'growth': round(growth, 2),
            'growth_percent': round(growth_percent, 2)
        }

        # Обновляем лучший по исходному score (можно заменить на growth, если нужно)
        # if score > best_score:
        #     best_score = score
        #     best_akk = akk

        if growth > best_growth: #growth > 0 and
            best_growth = growth
            best_akk = akk

    conn.close()

    if best_akk is not None:
        best_info = results[best_akk]
        reason = (
            f"Сетап {best_akk} имеет наивысший показатель score = {best_info['score']:.2f}, "
            f"обусловленный суммой PnL = {best_info['total_pnl']:.2f} и долей закрытых сделок = {best_info['close_ratio']:.2%}. "
            f"При этом стартовый баланс = {best_info['start_balance']:.2f}, текущий = {best_info['current_balance']:.2f}, "
            f"прирост = {best_info['growth']:.2f} ({best_info['growth_percent']:.2f}%)."
        )
        return {
            'best_akk': best_akk,
            'best_score': best_info['score'],
            'details': results,
            'reason': reason
        }
    else:
        return {'error': 'Нет данных для анализа'}

def bad_sym(akk, min_count=3, lookback_trades=None, lookback_days=None):
    """
    Возвращает список символов, убыточных для заданного akk (0–3),
    с учётом скользящего окна.

    Параметры:
        akk (int): номер сетапа
        min_count (int): минимальное число сделок для включения в анализ
        lookback_trades (int или None): использовать только последние N сделок по каждому символу
        lookback_days (int или None): использовать только сделки за последние N дней

    Возвращает:
        list of str: символы, у которых суммарный PnL < 0
    """
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    time_threshold = None
    if lookback_days is not None:
        # Находим максимальную дату в таблице (чтобы не зависеть от системного времени)
        cursor.execute("SELECT MAX(date_ms) FROM close_sym")
        max_date = cursor.fetchone()[0]
        if max_date is not None:
            time_threshold = max_date - lookback_days * 24 * 60 * 60 * 1000

    if lookback_trades is not None:
        # Используем оконную функцию для отбора последних N сделок по каждому символу
        query = """
            WITH ranked AS (
                SELECT sym, pnl,
                       ROW_NUMBER() OVER (PARTITION BY sym ORDER BY date_ms DESC) AS rn
                FROM close_sym
                WHERE akk = ?
            )
            SELECT sym, SUM(pnl) AS total_pnl, COUNT(*) AS cnt
            FROM ranked
            WHERE rn <= ?
            GROUP BY sym
            HAVING cnt >= ? AND total_pnl < 0
            ORDER BY total_pnl ASC
        """
        cursor.execute(query, (akk, lookback_trades, min_count))
    elif time_threshold is not None:
        # Фильтр по дате
        query = """
            SELECT sym, SUM(pnl) AS total_pnl, COUNT(*) AS cnt
            FROM close_sym
            WHERE akk = ? AND date_ms >= ?
            GROUP BY sym
            HAVING cnt >= ? AND total_pnl < 0
            ORDER BY total_pnl ASC
        """
        cursor.execute(query, (akk, time_threshold, min_count))
    else:
        # Вся история
        query = """
            SELECT sym, SUM(pnl) AS total_pnl, COUNT(*) AS cnt
            FROM close_sym
            WHERE akk = ?
            GROUP BY sym
            HAVING cnt >= ? AND total_pnl < 0
            ORDER BY total_pnl ASC
        """
        cursor.execute(query, (akk, min_count))

    rows = cursor.fetchall()
    conn.close()
    return [row[0] for row in rows]

if __name__ == "__main__":
    #fine_setup()
    # Вызвать один раз при старте скрипта
    enable_wal_mode()
    init_db()


    rovno = False
    
    print("\n🔄 Запуск обработчика callback...")
    callback_thread = threading.Thread(target=handle_callback_query, daemon=True)
    callback_thread.start()
    
    if rovno:
        try:
            sess1 = create_session_with_proxy(DEMO_CONFIG1)
        except Exception as e:
            pass

        try:
            sess2 = create_session_with_proxy(DEMO_CONFIG2)
        except Exception as e:
            pass

        try:
            sess3 = create_session_with_proxy(DEMO_CONFIG3)
        except Exception as e:
            pass

        try:
            sess4 = create_session_with_proxy(DEMO_CONFIG4)
        except Exception as e:
            pass

        try:
            sess = create_session_with_proxy(DEMO_CONFIG)
            SESS_ALL = [sess, sess1, sess2, sess3, sess4]
        except:
            pass



        #money_rovno()
    global free_main, live_open_coins, id_update_mes, live_open_minus_pnl, \
        free_dop_1, \
        free_dop_2, \
        free_dop_3, \
        free_dop_4, \
        live_b_akk1, \
        live_b_akk2, \
        live_b_akk3, \
        live_b_akk4, \
        pnl, last_open_sym

    last_open_sym = []

    free_main = 0.0

    pnl = 0.0

    free_dop_1 = 0.0
    free_dop_2 = 0.0
    free_dop_3 = 0.0
    free_dop_4 = 0.0

    live_open_coins = {}
    live_open_minus_pnl = []
    id_update_mes = None

    live_b_akk1 = 0.0
    live_b_akk2 = 0.0
    live_b_akk3 = 0.0
    live_b_akk4 = 0.0

    sess1 = None
    sess2 = None
    sess3 = None
    sess4 = None

    gsess0 = None
    gsess1 = None
    gsess2 = None
    gsess3 = None
    gsess4 = None

    #init_db()

    try:
        sess1 = create_session_with_proxy(DEMO_CONFIG1)
        gsess1 = sess1
        live_b_akk1 = get_ball(sess1, 1) 
    except Exception as e:
        pass

    try:
        sess2 = create_session_with_proxy(DEMO_CONFIG2)
        gsess2 = sess2
        live_b_akk2 = get_ball(sess2, 2) 
    except Exception as e:
        pass

    try:
        sess3 = create_session_with_proxy(DEMO_CONFIG3)
        gsess3 = sess3
        live_b_akk3 = get_ball(sess3, 3) 
    except Exception as e:
        pass

    try:
        sess4 = create_session_with_proxy(DEMO_CONFIG4)
        gsess4 = sess4
        live_b_akk4 = get_ball(sess4, 4) 
    except Exception as e:
        pass

    now_moment = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
    print(f"{now_moment} Фандинг монитор запущен (START)")
    print(f"Базовый риск на символ: {RISK_SYMBOL_USD}")
    if not os.path.exists(ID_MSG_UPDATE):
        with open(ID_MSG_UPDATE, 'w') as f:
            id_new = msg_tg_id(
                "Обнаружен первичный запуск!\nДанное сообщение закрепелно и будет отображать статистикой торговли в режиме реального времени.")
            f.write(f'{id_new}')
            id_update_mes = int(id_new)
        print(f"Файл {ID_MSG_UPDATE} создан")
    else:
        #send_tg(f"{now_moment} СТАРТ")
        with open(ID_MSG_UPDATE, 'r') as f:
            id_update_mes = int(f.read())

    try:
        sess = create_session_with_proxy(DEMO_CONFIG)
        gsess0 = sess
        live_b = get_ball(sess, 0) 
        if rovno:
            reballance_old(live_b, live_b_akk1, live_b_akk2, live_b_akk3, 1000)

            # drop_all_tables(DB_FILE)
            # init_db()
            last_main_ball = get_ball(sess, 0) 
            time.sleep(0.5)
            live_b_akk1 = get_ball(sess1, 1) 
            time.sleep(0.5)
            live_b_akk2 = get_ball(sess2, 2) 
            time.sleep(0.5)
            live_b_akk3 = get_ball(sess3, 3) 
            time.sleep(0.5)
            live_b_akk4 = get_ball(sess4, 4)  
            write_key_new('key_process', 'first_ball', last_main_ball)
            write_key_new('work_all_dop_akk', 'akk1_open', live_b_akk1)
            write_key_new('work_all_dop_akk', 'akk2_open', live_b_akk2)
            write_key_new('work_all_dop_akk', 'akk3_open', live_b_akk3)
            write_key_new('work_all_dop_akk', 'akk4_open', live_b_akk4)
            SETUP4 = []

        # Пример вызова (подставьте свои значения текущих балансов)
        # result = analyze_setups_new(live_b, live_b_akk1, live_b_akk2, live_b_akk3)
        #
        # if 'error' in result:
        #     print(result['error'])
        # else:
        #     print(f"Лучший сетап: {result['best_akk']}")
        #     print(result['reason'])
        #     print("\nДетали по всем сетапам:")
        #     for akk, data in result['details'].items():
        #         print(f"  akk {akk}: открыто {data['open_count']}, закрыто {data['close_count']}, "
        #               f"PnL {data['total_pnl']}, рост баланса {data['growth']:.2f}")

        for i in range(4):

            if float(read_key_new('work_all_dop_akk', f'akk{i+1}_open')) == 0:
                if i + 1 == 1:
                    write_key_new('work_all_dop_akk', f'akk{i+1}_open', live_b_akk1)
                elif i + 1 == 2:
                    write_key_new('work_all_dop_akk', f'akk{i+1}_open', live_b_akk2)
                elif i + 1 == 3:
                    write_key_new('work_all_dop_akk', f'akk{i+1}_open', live_b_akk3)
                elif i + 1 == 4:
                    write_key_new('work_all_dop_akk', f'akk{i+1}_open', live_b_akk4)

        #sync_db_positions(sess)
        # with open('last_open.txt', 'r') as f:
        #     last_open_sym = json.load(f)
        # print(f'LAST_OPEN: {last_open_sym}')
        SESS_ALL = [sess,sess1,sess2,sess3,sess4]
        # reballance()
        # input()
        #print(is_extreme_movement(sess, 'AKEUSDT', lookback_days=7, extreme_threshold=3.0))

        # new_ball = get_ball(sess, 0) 
        # summ_dop_akk_all = new_ball + live_b_akk1 + live_b_akk2 + live_b_akk3 + live_b_akk4
        # prognose = calculate_growth_exp_regression_new3(round(summ_dop_akk_all, 2))
        # print(f"✅ Funding monitor REAL\n📈 Прогноз доходности:\n{prognose}")



        if rovno:
            # Получаем балансы (уже получены выше, но можно переиспользовать)
            new_ball = live_b
            summ_dop_akk_all = new_ball + live_b_akk1 + live_b_akk2 + live_b_akk3 + live_b_akk4

            # Сохраняем начальную запись в БД (чтобы была хотя бы одна точка)
            write_live_all(
                new_ball, summ_dop_akk_all,
                free_main, free_dop_1, free_dop_2, free_dop_3, free_dop_4,
                live_b_akk1, live_b_akk2, live_b_akk3, live_b_akk4,
                key_start=True  # принудительная запись даже при pnl=0
            )

            # Записываем служебные ключи
            write_key_new('key_process', 'all_start_summ', summ_dop_akk_all)
            write_key_new('key_process', 'first_ball', new_ball)
            ball_for_reopen = new_ball - (new_ball * (proc_reopen * XXX))
            write_key_new('key_process', 'reopen_ball', ball_for_reopen)

            # Прогноз – оборачиваем в try-except на случай недостатка данных
            try:
                msg = calculate_growth_exp_regression_new3(round(summ_dop_akk_all, 2))
                if msg:
                    send_tg_crypta(f"✅ Funding monitor REAL\n📈 Прогноз доходности:\n{msg}")
                    send_tg(f"✅ Funding monitor REAL\n📈 Прогноз доходности:\n{msg}")
            except ValueError as e:
                if "Недостаточно данных" in str(e):
                    print("⚠️ Недостаточно данных для прогноза (требуется минимум 2 записи баланса). Пропускаем.")
                else:
                    raise  # другие ошибки не скрываем

        if not read_key_new('key_process','all_start_summ'):
            ball_m = get_ball(sess, 0) 
            write_key_new('key_process', 'all_start_summ', ball_m + live_b_akk1 + live_b_akk2 + live_b_akk3 + live_b_akk4)
            
        

        coins = update_coins(sess)

        read_ball = read_key_new('key_process','first_ball')
        if read_ball:
            start_ball = float(read_ball)
        else:
            start_ball = float(get_ball(sess, 0) )
            write_key_new('key_process', 'first_ball', start_ball)
            all_ball = start_ball + live_b_akk1 + live_b_akk2 + live_b_akk3 + live_b_akk4
            write_live_all(start_ball, all_ball, free_main, free_dop_1, free_dop_2, free_dop_3, free_dop_4, live_b_akk1, live_b_akk2, live_b_akk3, live_b_akk4, True)


        # if start_ball * (0.1 * XXX) > RISK_SYMBOL_USD:
        #     print(f"{RISK_SYMBOL_USD} Обновлен новый риск на символ: {start_ball * (0.1 * XXX)}")
        #     RISK_SYMBOL_USD = start_ball * (0.1 * XXX)

        # Запуск монитора заработка на фандинге
        fund_earn_th = threading.Thread(
            target=start_funding_earning_monitor,
            args=(sess, sess1, sess2, sess3, sess4, start_ball,),
            daemon=True
        )
        fund_earn_th.start()

        # Основной цикл — можно добавить свою торговлю или просто держать скрипт живым
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        now_moment = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
        print(f"\n{now_moment} Получен сигнал остановки (Ctrl+C)")
        stop_event.set()
        #send_tg(f"{now_moment} СТОП")
        # Здесь можно добавить код для корректного закрытия ресурсов
        print(f"{now_moment} Завершение работы...")