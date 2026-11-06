#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Bybit USDT-Perp Synthetic Spread Bot v6.1 — мульти-комплектный, HEDGE MODE.
Основное отличие от v5:
  - Бот переведен на Hedge Mode (двусторонний режим).
  - Разрешено повторное использование символа (макс 2 раза).
  - Повторное использование возможно в противоположных направлениях (Delta Hedging).
  - PnL берется напрямую с биржи с учетом positionIdx (1 для Long, 2 для Short).
"""

import os
import sys
import time
import json
import math
import sqlite3
import logging
import threading
import trade_log

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

from datetime import datetime, timezone
from itertools import combinations, permutations
from typing import List, Dict, Optional, Tuple

import numpy as np
import pandas as pd

try:
    from pybit.unified_trading import HTTP
except ImportError:
    sys.exit("Установите pybit: pip install pybit")

try:
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
except ImportError:
    sys.exit("Установите plotly: pip install plotly")

try:
    from key import API_KEY, API_SECRET
except ImportError:
    sys.exit("Создайте key.py с API_KEY и API_SECRET.")

if not API_KEY or "PUT_" in API_KEY:
    sys.exit("API_KEY пустой или содержит заглушку — проверьте key.py")
if not API_SECRET or "PUT_" in API_SECRET:
    sys.exit("API_SECRET пустой или содержит заглушку — проверьте key.py")

# ============================================================
# КОНФИГУРАЦИЯ
# ============================================================
CONFIG = {
    # --- Подключение ---
    "demo": True,
    "api_key": API_KEY,
    "api_secret": API_SECRET,

    # --- Данные ---
    "category": "linear",
    "interval": "1",
    "history_days": 30,
    "annual_stability_enabled": True,
    "annual_history_days": 365,
    "annual_interval": "D",
    "annual_min_history_days": 350,
    "annual_min_coverage": 0.95,
    "annual_windows_days": [30, 90, 180],
    "annual_vol_ratio_soft": 3.0,
    "annual_delta_vol_ratio_soft": 3.0,
    "annual_drift_ratio_soft": 1.5,
    "annual_relative_gap_p95_soft": 0.50,
    "annual_leg_concentration_soft": 0.60,
    "annual_wf_shift_soft": 1.5,
    "annual_wf_vol_ratio_soft": 3.0,
    "annual_wf_excursion_soft": 4.0,
    "annual_short_history_factor": 0.65,
    "annual_unknown_factor": 0.90,
    "annual_min_factor": 0.15,
    "annual_request_pause": 0.15,
    "db_path": "bybit_1m.db",
    "plots_dir": "plots",
    "state_path": "state.json",
    "log_path": "bot.log",

    # --- Скрининг ---
    "min_turnover_24h": 1_000_000.0,
    "min_depth_usd": 1000.0,
    "depth_levels": 20,
    "ob_limit": 50,
    "target_symbols": 500,
    "natr_min": 0.02,

    # --- Фильтр памп/дамп ---
    "pump_filter_enabled": True,
    "pump_max_std_mult": 20.0,
    "pump_max_range_mult": 15.0,
    "pump_max_move_mult": 20.0,
    "pump_min_bars": 500,

    # --- ★ Структурные тесты спайков ---
    "structural_filter_enabled": True,
    "spike_zscore_max": 6.0,
    "spike_autocorr_max": 1.5,
    "spike_vol_regime_max": 2.5,
    "spike_bias_max": 0.7,

    # --- ★ Когерентность комплектов ---
    "coherence_corr_min": 0.05,

    # --- Сборка комплектов ---
    "leg_size": 3,
    "max_complexes": 18,
    "min_complexes": 6,
    "top_legs_for_pairs": 250,
    "max_leg_share": 0.05,

    # ★ НОВОЕ: Разрешаем использовать символ 2 раза (для хеджирования)
    "allow_symbol_reuse": True,
    "max_symbol_usage": 2,

    # --- Мульти-комплектность ---
    "max_concurrent_complexes": 5,
    "close_orphan_positions": True,
    "min_entry_ratio": 0.3,
    "rebuild_periodic_sec": 1800,
    "rebuild_failed_cooldown_sec": 300,

    # --- Торговля ---
    "deposit": 2_000.0,
    "leverage": 10,
    "usd_per_symbol": 100.0,

    "complex_take_profit_usd_net": 1.0,

    "taker_fee_rate": 0.00055,
    "take_profit_pct": 0.01,
    "estimated_exit_slippage_rate": 0.0010,

    "grid_add_loss_levels_usd": [10.0, 30.0, 50.0, 90.0, 170.0, 330.0, 650.0],
    "max_adds": 7,
    "max_layers": 8,
    "add_price_buffer_pct": 0.0,
    "add_pair_min_net_adverse_pct": 0.0,
    "hedge_buffer_pct": 0.05,
    "hedge_close_reversal": 0.10,
    "monitor_interval_sec": 10,

    "close_parallel_workers": 8,
    "close_verify_wait_sec": 0.5,
    "close_verify_retries": 2,
    "close_slippage_log_async": True,
}

# ============================================================
# ЛОГИРОВАНИЕ
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(CONFIG["log_path"], encoding="utf-8"),
    ],
)
log = logging.getLogger("spread_bot")

ALLOCATED_CAPITAL = CONFIG["deposit"] / CONFIG["max_concurrent_complexes"]
COMPLEX_TARGET = float(CONFIG["complex_take_profit_usd_net"])

MONTH_REFS: Dict[str, float] = {}
LAST_RATIO_LOG: float = 0.0
RATIO_LOG_INTERVAL: int = 300

PORTFOLIO_LOCK = threading.RLock()
REBUILD_STATE: Dict = {
    "running": False,
    "done": False,
    "reserve": None,
    "error": None,
}

# ============================================================
# БД
# ============================================================
SCHEMA = """
CREATE TABLE IF NOT EXISTS ohlcv (
    symbol TEXT NOT NULL, timeframe TEXT NOT NULL, timestamp INTEGER NOT NULL,
    open REAL, high REAL, low REAL, close REAL, volume REAL, turnover REAL,
    PRIMARY KEY (symbol, timeframe, timestamp)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS idx_ohlcv ON ohlcv(symbol, timeframe, timestamp);
"""

UPSERT = """
INSERT INTO ohlcv (symbol, timeframe, timestamp, open, high, low, close, volume, turnover)
VALUES (?,?,?,?,?,?,?,?,?)
ON CONFLICT(symbol, timeframe, timestamp) DO UPDATE SET
open=excluded.open, high=excluded.high, low=excluded.low,
close=excluded.close, volume=excluded.volume, turnover=excluded.turnover;
"""


def init_db():
    with sqlite3.connect(CONFIG["db_path"]) as conn:
        conn.executescript(SCHEMA)


def upsert_candles(symbol, candles, timeframe=None):
    if not candles:
        return
    timeframe = str(timeframe or CONFIG["interval"])
    rows = [(symbol, timeframe, int(c[0]),
             float(c[1]), float(c[2]), float(c[3]), float(c[4]),
             float(c[5]), float(c[6])) for c in candles]
    with sqlite3.connect(CONFIG["db_path"]) as conn:
        conn.executemany(UPSERT, rows)


# ============================================================
# BYBIT КЛИЕНТЫ
# ============================================================
def make_trade_session():
    return HTTP(
        testnet=False, demo=CONFIG["demo"],
        api_key=CONFIG["api_key"], api_secret=CONFIG["api_secret"],
    )


def make_market_session():
    return HTTP(testnet=False)


# ============================================================
# ИСТОРИЯ
# ============================================================
TF_MS = 60_000


def fetch_kline_paginated(session, symbol, start_ms, end_ms, category="linear",
                          interval=None, request_pause=0.08):
    interval = str(interval or CONFIG["interval"])
    all_candles = []
    cursor_end = end_ms
    while cursor_end > start_ms:
        try:
            resp = session.get_kline(
                category=category, symbol=symbol, interval=interval,
                start=start_ms, end=cursor_end, limit=1000,
            )
        except Exception as e:
            log.warning("get_kline %s/%sm: %s", symbol, interval, e)
            break
        batch = resp["result"]["list"]
        if not batch:
            break
        all_candles.extend(batch)
        oldest = int(batch[-1][0])
        if oldest <= start_ms:
            break
        cursor_end = oldest - 1
        time.sleep(max(0.0, float(request_pause)))
    all_candles.sort(key=lambda x: int(x[0]))
    return all_candles


def check_and_load_history(session, symbols, interval=None, history_days=None,
                           request_pause=0.08):
    interval = str(interval or CONFIG["interval"])
    history_days = int(history_days or CONFIG["history_days"])
    try:
        interval_minutes = int(interval)
    except ValueError as e:
        raise ValueError(f"Unsupported Bybit interval for history loader: {interval}") from e
    if interval_minutes <= 0:
        raise ValueError(f"Invalid candle interval: {interval}")

    interval_ms = interval_minutes * TF_MS
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    start_target = now_ms - history_days * 86400_000
    target_min = history_days * 1440 / interval_minutes * 0.9

    total_loaded = 0
    for i, sym in enumerate(symbols):
        if i > 0:
            time.sleep(0.30)
        with sqlite3.connect(CONFIG["db_path"]) as conn:
            row = conn.execute(
                "SELECT COUNT(*), MAX(timestamp) FROM ohlcv WHERE symbol=? AND timeframe=?",
                (sym, interval),
            ).fetchone()
        count, max_ts = row[0], row[1]

        stale = bool(max_ts and max_ts < now_ms - interval_ms)
        
        # ★ ИСПРАВЛЕНИЕ: Качаем 30 дней полностью ТОЛЬКО если данных нет вообще (count == 0).
        # Если в базе есть старые данные, бот просто докачает недостающий кусок (stale).
        needs_full_backfill = (count == 0)
        if needs_full_backfill:
            candles = fetch_kline_paginated(
                session, sym, start_target, now_ms, interval=interval,
                request_pause=request_pause,
            )
            if candles:
                upsert_candles(sym, candles, timeframe=interval)
                total_loaded += len(candles)
                log.info("  история %s/%sm: +%d свечей (было %d)",
                         sym, interval, len(candles), count)
        elif stale:
            candles = fetch_kline_paginated(
                session, sym, max_ts + interval_ms, now_ms, interval=interval,
                request_pause=request_pause,
            )
            if candles:
                upsert_candles(sym, candles, timeframe=interval)
                total_loaded += len(candles)
    if total_loaded > 0:
        log.info("Догружено всего %s/%sm свечей: %d", history_days, interval, total_loaded)
    else:
        log.info("История %s/%sm актуальна, догрузка не нужна.", history_days, interval)


def check_and_load_daily_history(session, symbols: List[str], history_days=None,
                                 request_pause=None):
    days = max(1, int(history_days or CONFIG.get("annual_history_days", 365)))
    day_ms = 86_400_000
    now = datetime.now(timezone.utc)
    today_utc = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
    end_exclusive_ms = int(today_utc.timestamp() * 1000)
    end_ms = end_exclusive_ms - 1
    last_closed_start = end_exclusive_ms - day_ms
    start_target = end_exclusive_ms - days * day_ms
    pause = float(CONFIG.get("annual_request_pause", 0.15)
                  if request_pause is None else request_pause)
    total_loaded = 0

    for i, sym in enumerate(symbols):
        if i:
            time.sleep(0.30)
        with sqlite3.connect(CONFIG["db_path"]) as conn:
            count, min_ts, max_ts = conn.execute(
                "SELECT COUNT(*), MIN(timestamp), MAX(timestamp) FROM ohlcv "
                "WHERE symbol=? AND timeframe='D'", (sym,),
            ).fetchone()

        if max_ts is not None and int(max_ts) >= last_closed_start:
            continue
        fetch_start = start_target if max_ts is None else max(
            start_target, int(max_ts) + day_ms
        )
        candles = fetch_kline_paginated(
            session, sym, fetch_start, end_ms,
            category=CONFIG["category"], interval="D", request_pause=pause,
        )
        if candles:
            upsert_candles(sym, candles, timeframe="D")
            total_loaded += len(candles)
            log.info("  годовая история %s/D: +%d свечей (было %d)",
                     sym, len(candles), count)

    if total_loaded:
        log.info("Догружено дневных свечей для годового ранжирования: %d", total_loaded)
    else:
        log.info("Дневная история для годового ранжирования актуальна.")


# ============================================================
# REF-ЦЕНЫ
# ============================================================
def ensure_month_refs(symbols: List[str], start_ms: int):
    missing = [s for s in symbols if s not in MONTH_REFS]
    if not missing:
        return
    log.info("Загрузка month_refs для %d символов...", len(missing))
    with sqlite3.connect(CONFIG["db_path"]) as conn:
        for sym in missing:
            row = conn.execute(
                "SELECT close FROM ohlcv WHERE symbol=? AND timeframe=? AND timestamp>=? "
                "ORDER BY timestamp LIMIT 1",
                (sym, CONFIG["interval"], start_ms),
            ).fetchone()
            if row and row[0] and float(row[0]) > 0:
                MONTH_REFS[sym] = float(row[0])
            else:
                log.warning("  month_ref не найден для %s", sym)
    log.info("Загружено month_refs: %d", len(missing))


# ============================================================
# СКРИНИНГ
# ============================================================
def get_liquid_volatile_symbols(session):
    cat = CONFIG["category"]
    inst = session.get_instruments_info(category=cat, status="Trading", limit=1000)["result"]["list"]
    active = {i["symbol"] for i in inst}
    tickers = session.get_tickers(category=cat)["result"]["list"]
    candidates = [
        t for t in tickers
        if t["symbol"] in active and float(t.get("turnover24h", 0)) >= CONFIG["min_turnover_24h"]
    ]
    log.info("Кандидатов по обороту ≥ $%.0fM: %d",
             CONFIG["min_turnover_24h"] / 1e6, len(candidates))

    liquid = []
    for t in candidates:
        try:
            ob = session.get_orderbook(
                category=cat, symbol=t["symbol"], limit=CONFIG["ob_limit"]
            )["result"]
        except Exception:
            continue
        bid_d = sum(float(p) * float(s) for p, s in ob["b"][:CONFIG["depth_levels"]])
        ask_d = sum(float(p) * float(s) for p, s in ob["a"][:CONFIG["depth_levels"]])
        if bid_d >= CONFIG["min_depth_usd"] and ask_d >= CONFIG["min_depth_usd"]:
            liquid.append({"symbol": t["symbol"], "turnover24h": float(t["turnover24h"])})
    log.info("Прошли глубину стакана ≥ $%.0f: %d", CONFIG["min_depth_usd"], len(liquid))

    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    start_ms = now_ms - 24 * 3600_000
    scored = []
    for item in liquid:
        sym = item["symbol"]
        with sqlite3.connect(CONFIG["db_path"]) as conn:
            df = pd.read_sql_query(
                "SELECT high, low, close FROM ohlcv WHERE symbol=? AND timeframe=? AND timestamp>=? ORDER BY timestamp",
                conn, params=(sym, CONFIG["interval"], start_ms),
            )
        if len(df) < 30:
            # ★ ФИКС: Если истории нет, не выкидываем монету, а даем ей базовый балл.
            # При следующем шаге бот сам скачает для неё историю.
            scored.append({
                "symbol": sym,
                "natr": 1.0,
                "score": 1.0 * (item["turnover24h"] ** 0.1),
            })
            continue
        c = df["close"].values
        h = df["high"].values
        l = df["low"].values
        trs = np.maximum(
            h[1:] - l[1:],
            np.maximum(np.abs(h[1:] - c[:-1]), np.abs(l[1:] - c[:-1]))
        )
        if len(trs) < 14:
            continue
        natr = (np.mean(trs[-14:]) / c[-1]) * 100 if c[-1] > 0 else 0
        if natr >= CONFIG["natr_min"]:
            scored.append({
                "symbol": sym,
                "natr": natr,
                "score": natr * (item["turnover24h"] ** 0.1),
            })
    scored.sort(key=lambda x: x["score"], reverse=True)
    return [x["symbol"] for x in scored[:CONFIG["target_symbols"]]]


# ============================================================
# НОРМАЛИЗАЦИЯ
# ============================================================
def load_norm_pivot(symbols: List[str], start_ts: int, end_ts: int) -> pd.DataFrame:
    frames = {}
    for sym in symbols:
        with sqlite3.connect(CONFIG["db_path"]) as conn:
            df = pd.read_sql_query(
                "SELECT timestamp, close FROM ohlcv "
                "WHERE symbol=? AND timeframe=? AND timestamp BETWEEN ? AND ? "
                "ORDER BY timestamp",
                conn, params=(sym, CONFIG["interval"], start_ts, end_ts),
            )
        if len(df) < 100:
            continue
        ref = float(df["close"].iloc[0])
        if ref <= 0:
            continue
        frames[sym] = pd.Series(
            df["close"].values / ref,
            index=df["timestamp"].values,
            name=sym,
        )
    if not frames:
        return pd.DataFrame()
    return pd.DataFrame(frames).sort_index().dropna()


def load_norm_pivot_hour(symbols: List[str], start_ts: int, end_ts: int) -> pd.DataFrame:
    frames = {}
    for sym in symbols:
        with sqlite3.connect(CONFIG["db_path"]) as conn:
            df = pd.read_sql_query(
                "SELECT timestamp, close FROM ohlcv "
                "WHERE symbol=? AND timeframe=? AND timestamp BETWEEN ? AND ? "
                "ORDER BY timestamp",
                conn, params=(sym, CONFIG["interval"], start_ts, end_ts),
            )
        if df.empty:
            continue
        ref = float(df["close"].iloc[0])
        if ref <= 0:
            continue
        frames[sym] = pd.Series(
            df["close"].values / ref,
            index=df["timestamp"].values,
            name=sym,
        )
    if not frames:
        return pd.DataFrame()
    return pd.DataFrame(frames).sort_index().dropna()


def load_price_pivot(symbols: List[str], start_ts: int, end_ts: int,
                     timeframe: str) -> pd.DataFrame:
    frames = {}
    with sqlite3.connect(CONFIG["db_path"]) as conn:
        for sym in symbols:
            df = pd.read_sql_query(
                "SELECT timestamp, close FROM ohlcv "
                "WHERE symbol=? AND timeframe=? AND timestamp BETWEEN ? AND ? "
                "ORDER BY timestamp",
                conn, params=(sym, str(timeframe), start_ts, end_ts),
            )
            if df.empty:
                continue
            frames[sym] = pd.Series(
                df["close"].astype(float).values,
                index=df["timestamp"].astype("int64").values,
                name=sym,
            )
    if not frames:
        return pd.DataFrame()
    return pd.DataFrame(frames).sort_index()


def leg_norm(pivot: pd.DataFrame, leg: Tuple[str, str, str]) -> pd.Series:
    cols = [c for c in leg if c in pivot.columns]
    if len(cols) < CONFIG["leg_size"]:
        return pd.Series(dtype=float)
    return pivot[cols].mean(axis=1)


# ============================================================
# СТРУКТУРНЫЕ ТЕСТЫ СПАЙКОВ
# ============================================================
def zscore_spike(series: pd.Series, window_1d: int = 1440) -> float:
    ma = series.rolling(window_1d * 7, min_periods=window_1d).mean()
    sd = series.rolling(window_1d * 7, min_periods=window_1d).std()
    z = ((series - ma) / sd).abs()
    tail = z.tail(window_1d).dropna()
    if tail.empty:
        return 0.0
    return float(tail.max())


def trend_autocorr(series: pd.Series, window_1d: int = 1440, lags: int = 5) -> float:
    rets = series.pct_change().tail(window_1d).dropna()
    if len(rets) < 200:
        return 0.0
    s = 0.0
    for k in range(1, lags + 1):
        ac = rets.autocorr(lag=k)
        if pd.notna(ac):
            s += float(ac)
    return s


def vol_regime(series: pd.Series, window_1d: int = 1440) -> float:
    rets = series.pct_change().dropna()
    if len(rets) < window_1d * 2:
        return 0.0
    v_short = rets.tail(window_1d).std()
    v_long = rets.tail(window_1d * 7).std()
    if not v_long or v_long <= 0 or pd.isna(v_short):
        return 0.0
    return float(v_short / v_long)


def direction_bias(series: pd.Series, window_1d: int = 1440) -> float:
    s = series.tail(window_1d * 7)
    if len(s) < window_1d:
        return 0.0
    net = float(s.iloc[-1] - s.iloc[0])
    rng = float(s.max() - s.min())
    if rng <= 0:
        return 0.0
    return abs(net) / rng


# ============================================================
# ФИЛЬТР ПАМП/ДАМП
# ============================================================
def filter_pump_dump_symbols(pivot: pd.DataFrame) -> pd.DataFrame:
    if not CONFIG["pump_filter_enabled"]:
        return pivot

    use_structural = CONFIG.get("structural_filter_enabled", True)
    Z_MAX = CONFIG.get("spike_zscore_max", 6.0)
    AC_MAX = CONFIG.get("spike_autocorr_max", 1.5)
    VR_MAX = CONFIG.get("spike_vol_regime_max", 2.5)
    BI_MAX = CONFIG.get("spike_bias_max", 0.7)

    stats = []
    for sym in pivot.columns:
        s = pivot[sym].dropna()
        if len(s) < CONFIG["pump_min_bars"]:
            stats.append((sym, None, None, None, len(s)))
            continue

        row = {"sym": sym, "n": len(s)}

        if use_structural:
            row["z"] = zscore_spike(s)
            row["ac"] = trend_autocorr(s)
            row["vr"] = vol_regime(s)
            row["bias"] = direction_bias(s)
        else:
            row["std"] = float(s.std())
            row["rng"] = float((s.max() - s.min()) / s.min()) if s.min() > 0 else float("inf")
            row["move"] = float(abs(s.iloc[-1] / s.iloc[0] - 1)) if s.iloc[0] > 0 else float("inf")

        stats.append((sym, row, None, None, len(s)))

    valid = [x for x in stats if x[1] is not None]
    if len(valid) < 5:
        return pivot

    if not use_structural:
        med_std = float(np.median([x[1]["std"] for x in valid]))
        med_rng = float(np.median([x[1]["rng"] for x in valid]))
        med_move = float(np.median([x[1]["move"] for x in valid]))

    to_drop = []
    reasons = {}
    for sym, row, _, _, n in stats:
        if row is None:
            to_drop.append(sym)
            reasons[sym] = f"мало баров ({n})"
            continue

        if use_structural:
            if row["z"] > Z_MAX:
                to_drop.append(sym)
                reasons[sym] = f"zscore={row['z']:.2f}>{Z_MAX}"
                continue
            if row["ac"] > AC_MAX:
                to_drop.append(sym)
                reasons[sym] = f"autocorr={row['ac']:.2f}>{AC_MAX} (тренд)"
                continue
            if row["vr"] > VR_MAX:
                to_drop.append(sym)
                reasons[sym] = f"vol_regime={row['vr']:.2f}>{VR_MAX} (скачок волатильности)"
                continue
            if row["bias"] > BI_MAX:
                to_drop.append(sym)
                reasons[sym] = f"bias={row['bias']:.2f}>{BI_MAX} (однонаправленность)"
                continue
        else:
            if med_std > 0 and row["std"] > CONFIG["pump_max_std_mult"] * med_std:
                to_drop.append(sym)
                reasons[sym] = f"std={row['std']:.4f} > {CONFIG['pump_max_std_mult']:.0f}×median({med_std:.4f})"
                continue
            if med_rng > 0 and row["rng"] > CONFIG["pump_max_range_mult"] * med_rng:
                to_drop.append(sym)
                reasons[sym] = f"range={row['rng']:.2f} > {CONFIG['pump_max_range_mult']:.0f}×median({med_rng:.2f})"
                continue
            if med_move > 0 and row["move"] > CONFIG["pump_max_move_mult"] * med_move:
                to_drop.append(sym)
                reasons[sym] = f"move={row['move']:.2f} > {CONFIG['pump_max_move_mult']:.0f}×median({med_move:.2f})"
                continue

    if to_drop:
        log.warning("ПАМП/ДАМП-ФИЛЬТР (%s): отсеиваю %d символов:",
                    "structural" if use_structural else "legacy", len(to_drop))
        for sym in to_drop:
            log.warning("  - %-15s %s", sym, reasons.get(sym, ""))
        pivot = pivot.drop(columns=[c for c in to_drop if c in pivot.columns])
        log.info("После фильтра осталось символов: %d", len(pivot.columns))
    else:
        log.info("ПАМП/ДАМП-ФИЛЬТР (%s): всё чисто, выбросов не найдено.",
                 "structural" if use_structural else "legacy")
    return pivot


# ============================================================
# МЕТРИКА КАЧЕСТВА
# ============================================================
def spread_quality(s1: pd.Series, s2: pd.Series, resample_rule: Optional[str] = None) -> Optional[Dict]:
    df = pd.DataFrame({"leg1": s1, "leg2": s2}).dropna()
    if len(df) < 100:
        return None
    if resample_rule is not None:
        df.index = pd.to_datetime(df.index, unit="ms", utc=True)
        df = df.resample(resample_rule).mean().dropna()
        if len(df) < 100:
            return None

    s = (df["leg1"] - df["leg2"]).values
    d_max = float(np.max(np.abs(s)))
    if d_max <= 0:
        return None

    n = len(s)
    window = min(72, max(20, n // 10))
    ma = pd.Series(s).rolling(window=window, min_periods=max(5, window // 4)).mean().values
    centered = s - ma
    valid = ~np.isnan(centered)
    centered_valid = centered[valid]
    if len(centered_valid) < 100:
        return None
    sd = float(centered_valid.std())
    if sd <= 0:
        return None

    abs_z = np.abs(centered_valid / sd)
    enter_th, exit_th = 1.2, 0.3

    above = abs_z >= enter_th
    below = abs_z <= exit_th

    enters = np.where(np.diff(above.astype(np.int8)) == 1)[0] + 1
    exits = np.where(np.diff(below.astype(np.int8)) == 1)[0] + 1

    cycles = 0
    amps = []
    if len(enters) > 0 and len(exits) > 0:
        for e in enters:
            idx = np.searchsorted(exits, e, side="left")
            if idx < len(exits):
                exit_i = exits[idx]
                peak = float(np.max(abs_z[e:exit_i + 1]))
                cycles += 1
                amps.append(peak * sd)

    avg_amp = float(np.mean(amps)) if amps else 0.0
    return {
        "d_max": d_max,
        "cycles": cycles,
        "avg_amplitude": avg_amp,
        "score": cycles * avg_amp,
    }


# ============================================================
# СБОРКА КОМПЛЕКТОВ
# ============================================================
def annual_stability_metrics(leg_a: Tuple[str, ...], leg_b: Tuple[str, ...],
                             price_pivot: Optional[pd.DataFrame]) -> Dict:
    unknown = float(CONFIG.get("annual_unknown_factor", 0.90))
    short_factor = float(CONFIG.get("annual_short_history_factor", 0.65))
    result = {
        "annual_status": "unavailable", "annual_days": 0.0, "annual_coverage": 0.0,
        "annual_vol_ratio": None, "annual_level_vol_ratio": None,
        "annual_delta_vol_ratio": None,
        "annual_drift_ratio": None, "annual_relative_gap_p95": None,
        "annual_leg_concentration": None, "annual_wf_shift": None,
        "annual_wf_vol_ratio": None, "annual_wf_excursion": None,
        "stability_factor": unknown,
    }
    if price_pivot is None or price_pivot.empty:
        return result

    symbols = list(leg_a) + list(leg_b)
    if any(sym not in price_pivot.columns for sym in symbols):
        result["annual_status"] = "missing_symbols"
        return result

    raw = price_pivot.loc[:, symbols].copy()
    raw = raw.replace([np.inf, -np.inf], np.nan).dropna().sort_index()
    if len(raw) < 2:
        result["annual_status"] = "insufficient_overlap"
        result["stability_factor"] = short_factor
        return result
    raw = raw[~raw.index.duplicated(keep="last")]
    if isinstance(raw.index, pd.DatetimeIndex):
        raw.index = pd.to_datetime(raw.index, utc=True)
    else:
        try:
            raw.index = pd.to_datetime(raw.index, unit="ms", utc=True)
        except (TypeError, ValueError):
            raw.index = pd.to_datetime(raw.index, utc=True)
    raw = raw[~raw.index.duplicated(keep="last")].sort_index()
    if raw.empty or (raw <= 0).any().any():
        result["annual_status"] = "invalid_prices"
        return result

    daily_prices = raw.resample("1D").last().dropna()
    if len(daily_prices) < 60:
        result["annual_status"] = "insufficient_daily_bars"
        result["annual_days"] = float(max(0, len(daily_prices) - 1))
        result["stability_factor"] = short_factor
        return result
    span_days = max(0.0, (daily_prices.index[-1] - daily_prices.index[0]).total_seconds() / 86400.0)
    expected_rows = max(1.0, span_days + 1.0)
    coverage = min(1.0, len(daily_prices) / expected_rows)
    result["annual_days"] = round(span_days, 1)
    result["annual_coverage"] = round(coverage, 3)

    normalized = daily_prices / daily_prices.iloc[0]
    a = normalized[list(leg_a)].mean(axis=1)
    b = normalized[list(leg_b)].mean(axis=1)
    spread = a - b
    midpoint = (a + b) / 2.0
    relative_gap = (spread.abs() / midpoint.replace(0, np.nan)).replace(
        [np.inf, -np.inf], np.nan
    ).dropna()
    gap_p95 = float(relative_gap.quantile(0.95)) if len(relative_gap) else 0.0

    windows = sorted({int(w) for w in CONFIG.get("annual_windows_days", [30, 90, 180])
                      if int(w) >= 10})
    global_level_scale = max(float(spread.std()), 1e-10)
    spread_delta = spread.diff().dropna()
    global_delta_scale = max(float(spread_delta.std()), 1e-10)
    level_ratios, delta_ratios, drift_ratios = [], [], []
    window_fields = {}

    def max_median_ratio(values: pd.Series, window: int):
        min_periods = max(8, int(math.ceil(window * 0.80)))
        rolling_std = values.rolling(window=window, min_periods=min_periods).std().dropna()
        positive = rolling_std[rolling_std > 1e-10]
        if len(positive) < 5:
            return None
        return float(positive.max() / max(float(positive.median()), 1e-10))

    for window in windows:
        if len(spread) < 2 * window:
            continue
        min_periods = max(8, int(math.ceil(window * 0.80)))
        level_ratio = max_median_ratio(spread, window)
        delta_ratio = max_median_ratio(spread_delta, window)
        rolling = spread.rolling(window=window, min_periods=min_periods)
        roll_mean = rolling.mean()
        drift = ((roll_mean - float(spread.mean())).abs() / global_level_scale).dropna()
        drift_value = float(drift.max()) if len(drift) else None
        if level_ratio is not None:
            level_ratios.append(level_ratio)
            window_fields[f"annual_level_vol_ratio_{window}d"] = round(level_ratio, 3)
            window_fields[f"annual_vol_ratio_{window}d"] = round(level_ratio, 3)
        if delta_ratio is not None:
            delta_ratios.append(delta_ratio)
            window_fields[f"annual_delta_vol_ratio_{window}d"] = round(delta_ratio, 3)
        if drift_value is not None:
            drift_ratios.append(drift_value)
            window_fields[f"annual_drift_ratio_{window}d"] = round(drift_value, 3)

    leg_concentration = None
    price_delta = normalized.diff().dropna()
    aligned = pd.concat([price_delta[symbols], spread_delta.rename("spread_delta")], axis=1).dropna()
    if len(aligned) >= 30:
        spread_var = float(aligned["spread_delta"].var())
        if spread_var > 1e-20:
            coefficients = {s: (1.0 / len(leg_a)) for s in leg_a}
            coefficients.update({s: (-1.0 / len(leg_b)) for s in leg_b})
            contributions = []
            for sym in symbols:
                covariance = float(aligned[sym].cov(aligned["spread_delta"]))
                contributions.append(abs(coefficients[sym] * covariance / spread_var))
            contribution_total = sum(contributions)
            if contribution_total > 1e-12:
                leg_concentration = max(contributions) / contribution_total

    wf_shift = wf_vol_ratio = wf_excursion = None
    if len(spread) >= 270:
        test_len = max(45, min(60, len(spread) // 5))
        first_test = len(spread) - 3 * test_len
        shifts, vol_ratios, excursions = [], [], []
        if first_test >= 90:
            for fold in range(3):
                test_start = first_test + fold * test_len
                test_end = min(len(spread), test_start + test_len)
                train = spread.iloc[:test_start]
                test = spread.iloc[test_start:test_end]
                if len(train) < 90 or len(test) < 30:
                    continue
                train_mean = float(train.mean())
                train_scale = max(float(train.std()), global_level_scale * 0.05, 1e-10)
                shifts.append(abs(float(test.mean()) - train_mean) / train_scale)
                excursions.append(float((test - train_mean).abs().max()) / train_scale)
                train_d = train.diff().dropna()
                test_d = pd.concat([train.tail(1), test]).diff().dropna()
                train_vol = max(float(train_d.std()), global_delta_scale * 0.05, 1e-10)
                test_vol = max(float(test_d.std()), global_delta_scale * 0.05, 1e-10)
                vol_ratios.append(max(test_vol / train_vol, train_vol / test_vol))
        if shifts:
            wf_shift = max(shifts)
            wf_vol_ratio = max(vol_ratios)
            wf_excursion = max(excursions)

    vol_ratio = max(level_ratios) if level_ratios else 1.0
    delta_vol_ratio = max(delta_ratios) if delta_ratios else 1.0
    level_vol_ratio = vol_ratio
    drift_ratio = max(drift_ratios) if drift_ratios else 0.0

    def excess(value, threshold):
        try:
            value = float(value)
            threshold = float(threshold)
        except (TypeError, ValueError):
            return 0.0
        if not np.isfinite(value) or threshold <= 0:
            return 0.0
        return max(0.0, value / threshold - 1.0)

    penalty = (
            excess(vol_ratio, CONFIG.get("annual_vol_ratio_soft", 3.0))
            + 0.75 * excess(delta_vol_ratio, CONFIG.get("annual_delta_vol_ratio_soft", 3.0))
            + excess(drift_ratio, CONFIG.get("annual_drift_ratio_soft", 1.5))
            + 0.50 * excess(gap_p95, CONFIG.get("annual_relative_gap_p95_soft", 0.50))
            + 0.75 * excess(leg_concentration, CONFIG.get("annual_leg_concentration_soft", 0.60))
            + excess(wf_shift, CONFIG.get("annual_wf_shift_soft", 1.5))
            + 0.50 * excess(wf_vol_ratio, CONFIG.get("annual_wf_vol_ratio_soft", 3.0))
            + 0.50 * excess(wf_excursion, CONFIG.get("annual_wf_excursion_soft", 4.0))
    )
    min_days = int(CONFIG.get("annual_min_history_days", 350))
    min_coverage = float(CONFIG.get("annual_min_coverage", 0.95))
    full_history = span_days >= min_days and coverage >= min_coverage
    if full_history:
        confidence = 1.0
    else:
        confidence = min(1.0, span_days / max(1, min_days),
                         coverage / max(min_coverage, 1e-6))
    factor = min(1.0, confidence / (1.0 + penalty))
    if not full_history:
        factor = min(factor, short_factor)
    factor = max(float(CONFIG.get("annual_min_factor", 0.15)), factor)

    result.update({
        "annual_status": "full" if full_history else "partial",
        "annual_vol_ratio": round(vol_ratio, 3),
        "annual_level_vol_ratio": round(level_vol_ratio, 3),
        "annual_delta_vol_ratio": round(delta_vol_ratio, 3),
        "annual_drift_ratio": round(drift_ratio, 3),
        "annual_relative_gap_p95": round(gap_p95, 4),
        "annual_leg_concentration": round(leg_concentration, 3)
        if leg_concentration is not None else None,
        "annual_wf_shift": round(wf_shift, 3) if wf_shift is not None else None,
        "annual_wf_vol_ratio": round(wf_vol_ratio, 3) if wf_vol_ratio is not None else None,
        "annual_wf_excursion": round(wf_excursion, 3) if wf_excursion is not None else None,
        "stability_factor": round(factor, 4),
    })
    result.update(window_fields)
    return result


def build_complexes(symbols: List[str], pivot: pd.DataFrame,
                    annual_prices: Optional[pd.DataFrame] = None,
                    active_exposures: Optional[Dict[str, List[str]]] = None) -> List[Dict]:
    from collections import Counter

    if active_exposures is None:
        active_exposures = {}

    used_symbols = [s for s in symbols if s in pivot.columns]
    M = len(used_symbols)
    if M < CONFIG["leg_size"] * 2:
        return []

    coherence_corr: Dict[str, float] = {}
    corr_min = CONFIG.get("coherence_corr_min", 0.10)
    if CONFIG.get("structural_filter_enabled", True):
        try:
            dt_idx = pd.to_datetime(pivot.index, unit="ms", utc=True)
            pivot_5m = pivot.set_axis(dt_idx).resample("5min").mean().dropna()
            if len(pivot_5m) > 200:
                rets_5m = pivot_5m.pct_change().dropna()
                pool_5m = rets_5m.mean(axis=1)
                for sym in used_symbols:
                    if sym in rets_5m.columns:
                        c = rets_5m[sym].corr(pool_5m)
                        coherence_corr[sym] = float(c) if pd.notna(c) else 0.0
            log.info("Кэш когерентности: %d символов, corr_min=%.2f",
                     len(coherence_corr), corr_min)
        except Exception as e:
            log.warning("Не смог посчитать корреляции для когерентности: %s", e)

    log.info("Оценка всех C(%d,3) = %d ног...", M, M * (M - 1) * (M - 2) // 6)
    pv = pivot[used_symbols].values
    t0 = time.time()
    leg_stats = {}
    for combo in combinations(range(M), CONFIG["leg_size"]):
        arr = (pv[:, combo[0]] + pv[:, combo[1]] + pv[:, combo[2]]) / 3.0
        std = float(arr.std())
        if std <= 0:
            continue
        leg = tuple(used_symbols[i] for i in combo)
        leg_stats[leg] = std
    log.info("Оценено ног: %d за %.1fs", len(leg_stats), time.time() - t0)

    sorted_legs = sorted(leg_stats.keys(), key=lambda k: leg_stats[k], reverse=True)

    diag_counter = Counter()
    for leg in sorted_legs[:1000]:
        for s in leg:
            diag_counter[s] += 1
    top5 = diag_counter.most_common(5)
    log.info("Топ-1000 ног: топ-5 символов по частоте: %s", top5)

    max_per_symbol = max(
        5,
        int(CONFIG["top_legs_for_pairs"] * CONFIG["max_leg_share"]),
    )
    log.info("Diversity-фильтр: max_per_symbol = %d (из %d ног)",
             max_per_symbol, CONFIG["top_legs_for_pairs"])

    symbol_count = Counter()
    top_legs = []
    for leg in sorted_legs:
        if len(top_legs) >= CONFIG["top_legs_for_pairs"]:
            break
        if any(symbol_count[s] >= max_per_symbol for s in leg):
            continue
        top_legs.append(leg)
        for s in leg:
            symbol_count[s] += 1

    log.info("Топ-%d ног (diversity).", len(top_legs))

    log.info("Оценка пар...")
    t0 = time.time()

    log.info("Предресемплирование %d ног к 5min...", len(top_legs))
    legs_5min = {}
    for leg in top_legs:
        s = leg_norm(pivot, leg).copy()
        if s.empty:
            continue
        s.index = pd.to_datetime(s.index, unit="ms", utc=True)
        legs_5min[leg] = s.resample("5min").mean()

    log.info("Готово %d ног, оценка пар...", len(legs_5min))

    pre_scored_pairs = []
    valid_pairs = 0
    coherence_rejected = 0
    max_cycles_seen = 0
    total_expected = len(top_legs) * (len(top_legs) - 1) // 2
    last_log = t0
    processed = 0

    for i in range(len(top_legs)):
        a = top_legs[i]
        if a not in legs_5min:
            continue
        sa = legs_5min[a]
        for j in range(i + 1, len(top_legs)):
            b = top_legs[j]
            if b not in legs_5min:
                processed += 1
                continue
            if set(a) & set(b):
                processed += 1
                continue

            if coherence_corr:
                coherent = True
                for s in a:
                    if coherence_corr.get(s, 0.0) < corr_min:
                        coherent = False
                        break
                if coherent:
                    for s in b:
                        if coherence_corr.get(s, 0.0) < corr_min:
                            coherent = False
                            break
                if not coherent:
                    coherence_rejected += 1
                    processed += 1
                    continue

            # ★ ПРОВЕРКА НАПРАВЛЕНИЙ НА ЭТАПЕ СБОРКИ
            sb = legs_5min[b]
            s_est = float(sa.iloc[-1] - sb.iloc[-1])
            will_be_leg1_long = (s_est < 0)

            valid_direction = True
            for sym in a:
                if sym in active_exposures:
                    new_dir = 'long' if will_be_leg1_long else 'short'
                    if new_dir in active_exposures[sym]:
                        valid_direction = False
                        break
            if valid_direction:
                for sym in b:
                    if sym in active_exposures:
                        new_dir = 'short' if will_be_leg1_long else 'long'
                        if new_dir in active_exposures[sym]:
                            valid_direction = False
                            break
            if not valid_direction:
                processed += 1
                continue

            valid_pairs += 1
            
            # ★ ШАГ 1: Быстрый отбор (без тяжелых годовых метрик)
            q = spread_quality(sa, legs_5min[b])
            if q:
                max_cycles_seen = max(max_cycles_seen, q["cycles"])
                if q["cycles"] > 0:
                    q["s_est"] = s_est # Сохраняем оценку спреда для проверки батча
                    pre_scored_pairs.append((a, b, q))
                    
            processed += 1

            now = time.time()
            if now - last_log > 10:
                pct = 100.0 * processed / total_expected if total_expected else 0
                log.info("  ...пар: %d/%d (%.1f%%) | валидных: %d | coherence_rej: %d | pre_scored: %d",
                         processed, total_expected, pct, valid_pairs,
                         coherence_rejected, len(pre_scored_pairs))
                last_log = now

    log.info("Шаг 1 завершен: валидных %d, прошли быстрый фильтр %d за %.1fs",
             valid_pairs, len(pre_scored_pairs), time.time() - t0)

    # ★ ШАГ 2: Тяжелый годовой анализ только для ТОП-3000 лучших пар
    pre_scored_pairs.sort(key=lambda x: x[2]["score"], reverse=True)
    top_to_check = pre_scored_pairs[:8000]  # Увеличили до 3000, чтобы набрать больше комплектов
    
    log.info("Шаг 2: Годовой анализ для топ-%d пар...", len(top_to_check))
    t1 = time.time()
    
    scored_pairs = []
    for a, b, q in top_to_check:
        if CONFIG.get("annual_stability_enabled", True):
            annual = annual_stability_metrics(a, b, annual_prices)
        else:
            annual = {
                "annual_status": "disabled", "annual_days": 0.0,
                "annual_coverage": 0.0, "annual_vol_ratio": None,
                "annual_level_vol_ratio": None, "annual_delta_vol_ratio": None,
                "annual_drift_ratio": None,
                "annual_relative_gap_p95": None,
                "annual_leg_concentration": None, "annual_wf_shift": None,
                "annual_wf_vol_ratio": None, "annual_wf_excursion": None,
                "stability_factor": 1.0,
            }
        q.update(annual)
        q["rank_score"] = q["score"] * q.get("stability_factor", 1.0)
        scored_pairs.append((a, b, q))

    log.info("Годовой анализ завершен за %.1fs", time.time() - t1)

    def select_with_overlaps(ordered_pairs):
        selected = []
        # Инициализируем счетчики и направления текущими экспозициями
        batch_exposures = {sym: list(dirs) for sym, dirs in active_exposures.items()}
        
        for a, b, q in ordered_pairs:
            s_est = q.get("s_est", 0.0)
            will_be_leg1_long = (s_est < 0)
            
            # Проверяем лимиты использования И направления
            valid_direction = True
            for sym in a:
                if len(batch_exposures.get(sym, [])) >= CONFIG.get("max_symbol_usage", 2):
                    valid_direction = False
                    break
                new_dir = 'long' if will_be_leg1_long else 'short'
                if new_dir in batch_exposures.get(sym, []):
                    valid_direction = False
                    break
            if valid_direction:
                for sym in b:
                    if len(batch_exposures.get(sym, [])) >= CONFIG.get("max_symbol_usage", 2):
                        valid_direction = False
                        break
                    new_dir = 'short' if will_be_leg1_long else 'long'
                    if new_dir in batch_exposures.get(sym, []):
                        valid_direction = False
                        break
            if not valid_direction:
                continue

            selected.append({
                "leg1": tuple(a), "leg2": tuple(b),
                "d_max": q["d_max"],
                "cycles": q["cycles"],
                "avg_amplitude": q["avg_amplitude"],
                "score": q["score"],
                "rank_score": q.get("rank_score", q["score"]),
                "annual_status": q.get("annual_status", "unavailable"),
                "annual_days": q.get("annual_days", 0.0),
                "annual_coverage": q.get("annual_coverage", 0.0),
                "annual_vol_ratio": q.get("annual_vol_ratio"),
                "annual_level_vol_ratio": q.get("annual_level_vol_ratio"),
                "annual_delta_vol_ratio": q.get("annual_delta_vol_ratio"),
                "annual_drift_ratio": q.get("annual_drift_ratio"),
                "annual_relative_gap_p95": q.get("annual_relative_gap_p95"),
                "annual_leg_concentration": q.get("annual_leg_concentration"),
                "annual_wf_shift": q.get("annual_wf_shift"),
                "annual_wf_vol_ratio": q.get("annual_wf_vol_ratio"),
                "annual_wf_excursion": q.get("annual_wf_excursion"),
                "annual_vol_ratio_30d": q.get("annual_vol_ratio_30d"),
                "annual_vol_ratio_90d": q.get("annual_vol_ratio_90d"),
                "annual_vol_ratio_180d": q.get("annual_vol_ratio_180d"),
                "annual_delta_vol_ratio_30d": q.get("annual_delta_vol_ratio_30d"),
                "annual_delta_vol_ratio_90d": q.get("annual_delta_vol_ratio_90d"),
                "annual_delta_vol_ratio_180d": q.get("annual_delta_vol_ratio_180d"),
                "annual_drift_ratio_30d": q.get("annual_drift_ratio_30d"),
                "annual_drift_ratio_90d": q.get("annual_drift_ratio_90d"),
                "annual_drift_ratio_180d": q.get("annual_drift_ratio_180d"),
                "stability_factor": q.get("stability_factor", 1.0),
            })
            
            # Фиксируем направления в батче
            for sym in a:
                new_dir = 'long' if will_be_leg1_long else 'short'
                batch_exposures.setdefault(sym, []).append(new_dir)
            for sym in b:
                new_dir = 'short' if will_be_leg1_long else 'long'
                batch_exposures.setdefault(sym, []).append(new_dir)

            if len(selected) >= CONFIG["max_complexes"]:
                break

        used_unique = set(batch_exposures.keys())
        return selected, used_unique

    legacy_pairs = sorted(scored_pairs, key=lambda x: x[2]["score"], reverse=True)
    annual_pairs = sorted(
        scored_pairs, key=lambda x: x[2].get("rank_score", x[2]["score"]), reverse=True
    )
    legacy_complexes, _ = select_with_overlaps(legacy_pairs)
    complexes, used = select_with_overlaps(annual_pairs)
    
    if (CONFIG.get("annual_stability_enabled", True)
            and len(complexes) < len(legacy_complexes)):
        log.warning(
            "Annual rank reduced non-overlapping output (%d < legacy %d); "
            "using legacy ordering to preserve the prepared-complex count.",
            len(complexes), len(legacy_complexes),
        )
        complexes, used = legacy_complexes, set(
            s for c in legacy_complexes for s in c["leg1"] + c["leg2"]
        )

    remaining = len(used_symbols) - len(used)
    possible_more = remaining // (CONFIG["leg_size"] * 2)
    log.info("Собрано комплектов: %d | использовано символов: %d/%d | возможно ещё: %d",
             len(complexes), len(used), len(used_symbols), possible_more)

    if len(complexes) < CONFIG["min_complexes"]:
        log.warning("Собрано меньше min_complexes=%d: %d.",
                    CONFIG["min_complexes"], len(complexes))

    return complexes
    
# ============================================================
# ВИЗУАЛИЗАЦИЯ
# ============================================================
def _df_for_plot(s1: pd.Series, s2: pd.Series, rule: str) -> pd.DataFrame:
    df = pd.DataFrame({"leg1": s1, "leg2": s2}).dropna()
    if df.empty:
        return df
    df.index = pd.to_datetime(df.index, unit="ms", utc=True)
    return df.resample(rule).mean().dropna()


def show_plots(complexes: List[Dict], pivot: pd.DataFrame,
               symbols_for_hour: List[str], top_n: int = 16):
    if not complexes:
        return
    os.makedirs(CONFIG["plots_dir"], exist_ok=True)
    ranked = sorted(
        complexes, key=lambda c: c.get("rank_score", c["score"]), reverse=True
    )[:top_n]

    cols = 4
    rows = math.ceil(len(ranked) / cols)
    fig_over = make_subplots(
        rows=rows, cols=cols,
        subplot_titles=[
            f"#{i + 1} rank={c.get('rank_score', c['score']):.2f} "
            f"raw={c['score']:.2f} | year×{c.get('stability_factor', 1.0):.2f} "
            f"| {c.get('annual_status', 'n/a')} {c.get('annual_days', 0):.0f}d "
            f"| level-vol={c.get('annual_vol_ratio', 0) or 0:.2f} "
            f"delta-vol={c.get('annual_delta_vol_ratio', 0) or 0:.2f} "
            f"drift={c.get('annual_drift_ratio', 0) or 0:.2f} "
            f"| Dmax={c['d_max']:.3f} | cyc={c['cycles']}"
            for i, c in enumerate(ranked)
        ],
        vertical_spacing=0.08, horizontal_spacing=0.04,
    )
    for i, comp in enumerate(ranked):
        r, c = i // cols + 1, i % cols + 1
        s1 = leg_norm(pivot, comp["leg1"])
        s2 = leg_norm(pivot, comp["leg2"])
        df = _df_for_plot(s1, s2, "30min")
        if df.empty:
            continue
        fig_over.add_trace(
            go.Scatter(x=df.index, y=df["leg1"], mode="lines",
                       line=dict(color="#1f77b4", width=1), showlegend=False),
            row=r, col=c,
        )
        fig_over.add_trace(
            go.Scatter(x=df.index, y=df["leg2"], mode="lines",
                       line=dict(color="#ff7f0e", width=1), showlegend=False),
            row=r, col=c,
        )
    fig_over.update_layout(
        height=280 * rows, width=1500,
        title_text="Ranked complexes overview",
        plot_bgcolor="#fafafa",
    )
    p_over = os.path.join(CONFIG["plots_dir"], "overview_ranked.html")
    fig_over.write_html(p_over)
    log.info("Overview: %s", p_over)
    try:
        fig_over.show()
    except Exception as e:
        log.warning("show() недоступен: %s", e)

    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    hour_ago = now_ms - 3600_000
    hour_pivot = load_norm_pivot_hour(symbols_for_hour, hour_ago, now_ms)

    fig_det = make_subplots(
        rows=2, cols=1, row_heights=[0.65, 0.35],
        subplot_titles=["Full history (5m)", "Last 1h (1m)"],
        vertical_spacing=0.12,
    )
    n_traces_per_rank = 4
    for i, comp in enumerate(ranked):
        vis = (i == 0)
        s1 = leg_norm(pivot, comp["leg1"])
        s2 = leg_norm(pivot, comp["leg2"])
        df = _df_for_plot(s1, s2, "5min")
        if not df.empty:
            fig_det.add_trace(go.Scatter(x=df.index, y=df["leg1"], mode="lines",
                                         name=f"leg1 {'/'.join(comp['leg1'])}",
                                         line=dict(color="#1f77b4", width=1.2), visible=vis),
                              row=1, col=1)
            fig_det.add_trace(go.Scatter(x=df.index, y=df["leg2"], mode="lines",
                                         name=f"leg2 {'/'.join(comp['leg2'])}",
                                         line=dict(color="#ff7f0e", width=1.2), visible=vis),
                              row=1, col=1)
        else:
            fig_det.add_trace(go.Scatter(x=[], y=[], visible=vis), row=1, col=1)
            fig_det.add_trace(go.Scatter(x=[], y=[], visible=vis), row=1, col=1)

        if not hour_pivot.empty:
            ls1 = leg_norm(hour_pivot, comp["leg1"])
            ls2 = leg_norm(hour_pivot, comp["leg2"])
            ldf = pd.DataFrame({"leg1": ls1, "leg2": ls2}).dropna()
            if not ldf.empty:
                ldf.index = pd.to_datetime(ldf.index, unit="ms", utc=True)
                fig_det.add_trace(go.Scatter(x=ldf.index, y=ldf["leg1"], mode="lines",
                                             name="leg1 (1h)", line=dict(color="#1f77b4", width=1.6),
                                             visible=vis), row=2, col=1)
                fig_det.add_trace(go.Scatter(x=ldf.index, y=ldf["leg2"], mode="lines",
                                             name="leg2 (1h)", line=dict(color="#ff7f0e", width=1.6),
                                             visible=vis), row=2, col=1)
            else:
                fig_det.add_trace(go.Scatter(x=[], y=[], visible=vis), row=2, col=1)
                fig_det.add_trace(go.Scatter(x=[], y=[], visible=vis), row=2, col=1)
        else:
            fig_det.add_trace(go.Scatter(x=[], y=[], visible=vis), row=2, col=1)
            fig_det.add_trace(go.Scatter(x=[], y=[], visible=vis), row=2, col=1)

    buttons = []
    for i, comp in enumerate(ranked):
        visibility = [False] * (len(ranked) * n_traces_per_rank)
        base = i * n_traces_per_rank
        visibility[base:base + n_traces_per_rank] = [True] * n_traces_per_rank
        buttons.append(dict(
            label=f"#{i + 1} ({comp['score']:.1f})",
            method="update", args=[{"visible": visibility}],
        ))
    fig_det.update_layout(
        height=900, width=1400,
        title_text="Detailed view — choose rank",
        updatemenus=[dict(buttons=buttons, direction="down",
                          x=0.0, xanchor="left", y=1.08, yanchor="top",
                          showactive=True, active=0)],
        plot_bgcolor="#fafafa",
    )
    p_det = os.path.join(CONFIG["plots_dir"], "detail_ranked.html")
    fig_det.write_html(p_det)
    log.info("Detail: %s", p_det)
    try:
        fig_det.show()
    except Exception as e:
        log.warning("show() недоступен: %s", e)


# ============================================================
# СОСТОЯНИЕ КОМПЛЕКТА
# ============================================================
class ComplexState:
    def __init__(self, comp: Dict, norm_refs: Dict[str, float]):
        self.complex = {
            "leg1": tuple(comp["leg1"]),
            "leg2": tuple(comp["leg2"]),
            "d_max": comp["d_max"],
            "cycles": comp.get("cycles", 0),
            "score": comp.get("score", 0.0),
        }
        self.entry_month_refs = {} # ★ Сохраняем базу 30-дневки на момент входа
        self.leg1_long = True
        self.entry_spread = 0.0
        self.layers = []
        self.hedge_open = False
        self.hedge_spread = 0.0
        self.hedge_qty = {}
        self.last_grid_level = 0
        self.grid_level_index = 0
        self.last_add_prices = {}
        self.last_spread_snapshot = {}
        self.selective_add_initialized = False
        self.norm_refs = dict(norm_refs)
        self.target = COMPLEX_TARGET
        self.realized_pnl = 0.0
        self.fees_paid = 0.0
        self.close_pending = False
        self.fill_reconciliation_required = False
        self.pending_add_orders = []

    def to_dict(self):
        return {
            "complex": {**self.complex,
                        "leg1": list(self.complex["leg1"]),
                        "leg2": list(self.complex["leg2"])},
            "leg1_long": self.leg1_long,
            "entry_spread": self.entry_spread,
            "layers": self.layers,
            "hedge_open": self.hedge_open,
            "hedge_spread": self.hedge_spread,
            "hedge_qty": self.hedge_qty,
            "last_grid_level": self.last_grid_level,
            "grid_level_index": self.grid_level_index,
            "last_add_prices": self.last_add_prices,
            "last_spread_snapshot": self.last_spread_snapshot,
            "selective_add_initialized": self.selective_add_initialized,
            "norm_refs": self.norm_refs,
            "target": self.target,
            "realized_pnl": self.realized_pnl,
            "fees_paid": self.fees_paid,
            "close_pending": self.close_pending,
            "fill_reconciliation_required": self.fill_reconciliation_required,
            "pending_add_orders": self.pending_add_orders,
            "entry_month_refs": self.entry_month_refs, # ★
        }

    @classmethod
    def from_dict(cls, d):
        inst = cls.__new__(cls)
        c = d["complex"]
        inst.complex = {
            "leg1": tuple(c["leg1"]),
            "leg2": tuple(c["leg2"]),
            "d_max": c["d_max"],
            "cycles": c.get("cycles", 0),
            "score": c.get("score", 0.0),
        }
        inst.leg1_long = d.get("leg1_long", True)
        inst.entry_spread = d.get("entry_spread", 0.0)
        inst.layers = d.get("layers", [])
        inst.hedge_open = d.get("hedge_open", False)
        inst.hedge_spread = d.get("hedge_spread", 0.0)
        inst.hedge_qty = d.get("hedge_qty", {})
        inst.last_grid_level = d.get("last_grid_level", 0)
        levels = CONFIG["grid_add_loss_levels_usd"]
        if d.get("grid_level_index") is None:
            inst.grid_level_index = min(max(0, len(inst.layers) - 1), len(levels))
            inst.last_grid_level = (-float(levels[inst.grid_level_index - 1])
                                    if inst.grid_level_index else 0.0)
        else:
            inst.grid_level_index = min(max(0, int(d["grid_level_index"])), len(levels))
        inst.last_add_prices = {str(k): float(v) for k, v in d.get("last_add_prices", {}).items()
                                if v is not None}
        inst.last_spread_snapshot = {str(k): float(v) for k, v in d.get("last_spread_snapshot", {}).items()
                                     if v is not None}
        inst.selective_add_initialized = bool(d.get("selective_add_initialized", False))
        inst.norm_refs = d.get("norm_refs", {})
        inst.target = COMPLEX_TARGET
        inst.realized_pnl = d.get("realized_pnl", 0.0)
        inst.fees_paid = float(d.get("fees_paid", 0.0))
        inst.close_pending = d.get("close_pending", False)
        inst.fill_reconciliation_required = bool(d.get("fill_reconciliation_required", False))
        inst.pending_add_orders = list(d.get("pending_add_orders", []))
        inst.entry_month_refs = d.get("entry_month_refs", {}) # ★
        return inst

    def all_symbols(self):
        return list(self.complex["leg1"]) + list(self.complex["leg2"])

    def total_qty(self, sym):
        return sum(l["qty"].get(sym, 0) for l in self.layers)


# ============================================================
# ПОРТФЕЛЬ
# ============================================================
class Portfolio:
    def __init__(self):
        self.active: List[ComplexState] = []
        self.reserve: List[Dict] = []
        self._load()

    def _load(self):
        p = CONFIG["state_path"]
        if not os.path.exists(p):
            return
        try:
            data = json.load(open(p))
        except Exception as e:
            log.warning("state.json не загружен: %s", e)
            return

        if "active" in data:
            self.active = [ComplexState.from_dict(d) for d in data.get("active", [])]
            self.reserve = data.get("reserve", [])
            log.info("state.json v2 загружен: active=%d, reserve=%d",
                     len(self.active), len(self.reserve))
            return

        if data.get("active_complex"):
            c = data["active_complex"]
            comp = {
                "leg1": tuple(c["leg1"]),
                "leg2": tuple(c["leg2"]),
                "d_max": c["d_max"],
                "cycles": c.get("cycles", 0),
                "score": c.get("score", 0.0),
            }
            cs = ComplexState.__new__(ComplexState)
            cs.complex = comp
            cs.leg1_long = data.get("leg1_long", True)
            cs.entry_spread = data.get("entry_spread", 0.0)
            cs.layers = data.get("layers", [])
            cs.hedge_open = data.get("hedge_open", False)
            cs.hedge_spread = data.get("hedge_spread", 0.0)
            cs.hedge_qty = data.get("hedge_qty", {})
            cs.last_grid_level = data.get("last_grid_level", 0)
            levels = CONFIG["grid_add_loss_levels_usd"]
            cs.grid_level_index = min(max(0, len(cs.layers) - 1), len(levels))
            cs.last_grid_level = (-float(levels[cs.grid_level_index - 1])
                                  if cs.grid_level_index else 0.0)
            cs.last_add_prices = {str(k): float(v) for k, v in data.get("last_add_prices", {}).items()}
            cs.last_spread_snapshot = {}
            cs.selective_add_initialized = False
            cs.norm_refs = data.get("norm_refs", {})
            cs.target = COMPLEX_TARGET
            cs.realized_pnl = 0.0
            cs.fees_paid = 0.0
            cs.close_pending = False
            cs.fill_reconciliation_required = False
            cs.pending_add_orders = []
            self.active = [cs]
            log.info("state.json v1 смигрирован: 1 активный комплекс")

    def save(self):
        data = {
            "active": [c.to_dict() for c in self.active],
            "reserve": self.reserve,
        }
        tmp = CONFIG["state_path"] + ".tmp"
        payload = json.dumps(data, default=str, indent=2)

        last_err = None
        for attempt in range(10):
            try:
                with open(tmp, "w", encoding="utf-8") as f:
                    f.write(payload)
            except Exception as e:
                last_err = e
                time.sleep(0.05 * (attempt + 1))
                continue

            for r in range(5):
                try:
                    os.replace(tmp, CONFIG["state_path"])
                    return
                except (PermissionError, OSError) as e:
                    last_err = e
                    time.sleep(0.03 * (r + 1))

            try:
                with open(CONFIG["state_path"], "w", encoding="utf-8") as dst:
                    dst.write(payload)
                try:
                    os.remove(tmp)
                except Exception:
                    pass
                return
            except Exception as e:
                last_err = e
                time.sleep(0.05 * (attempt + 1))

        log.error("state.json не сохранён после 10 попыток: %s", last_err)

    def get_symbol_exposures(self) -> Dict[str, List[str]]:
        """
        Возвращает словарь {symbol: [список направлений]}.
        Например: {'BTCUSDT': ['long', 'short']}
        """
        exposures = {}
        for c in self.active:
            for sym in c.complex["leg1"]:
                side = 'long' if c.leg1_long else 'short'
                exposures.setdefault(sym, []).append(side)
            for sym in c.complex["leg2"]:
                side = 'short' if c.leg1_long else 'long'
                exposures.setdefault(sym, []).append(side)
        return exposures

    def used_symbols(self):
        s = set()
        for c in self.active:
            s.update(c.all_symbols())
        return s


# ============================================================
# ТОРГОВЫЕ ПРИМИТИВЫ
# ============================================================
def get_qty(session, symbol, usd_amount, price):
    info = session.get_instruments_info(
        category="linear", symbol=symbol
    )["result"]["list"][0]
    f = info["lotSizeFilter"]
    step = float(f.get("qtyStep", 0.001))
    min_qty = float(f.get("minOrderQty", 0.0))
    raw = usd_amount / price
    qty = math.floor(raw / step) * step
    qty = max(qty, min_qty)
    decimals = max(0, -int(math.floor(math.log10(step)))) if step < 1 else 0
    return round(qty, decimals)


def place_market(session, symbol, side, qty, position_idx):
    if qty is None or float(qty) <= 0:
        raise ValueError(f"{symbol}: qty={qty} ≤ 0")
    try:
        resp = session.place_order(
            category="linear", symbol=symbol, side=side,
            orderType="Market", qty=str(qty), positionIdx=position_idx,
        )
    except Exception as e:
        raise

    if resp.get("retCode", 0) != 0:
        raise RuntimeError(
            f"{symbol}: retCode={resp.get('retCode')} retMsg={resp.get('retMsg')}"
        )
    oid = resp.get("result", {}).get("orderId")
    if not oid:
        raise RuntimeError(f"{symbol}: no orderId in response: {resp}")
    return oid


def current_price(session, symbol):
    t = session.get_tickers(category="linear", symbol=symbol)["result"]["list"][0]
    return float(t["lastPrice"])


def _weighted_execution_price(executions):
    fills = []
    for ex in executions or []:
        try:
            q = float(ex.get("execQty", 0))
            p = float(ex.get("execPrice", 0))
            if q > 0 and p > 0:
                fills.append((q, p))
        except (TypeError, ValueError):
            continue
    total = sum(q for q, _ in fills)
    return sum(q * p for q, p in fills) / total if total > 0 else None


def _weighted_execution_summary(executions):
    fills = []
    for ex in executions or []:
        try:
            q, p = float(ex.get("execQty", 0)), float(ex.get("execPrice", 0))
            if q > 0 and p > 0:
                fills.append((q, p))
        except (TypeError, ValueError):
            continue
    qty = sum(q for q, _ in fills)
    price = sum(q * p for q, p in fills) / qty if qty > 0 else None
    return price, qty


def _order_fill_summary(session, symbol, order_id, expected_qty=None):
    last_status = "ответ API не получен"
    for delay in (0.0, 0.25, 0.5, 1.0, 2.0):
        if delay:
            time.sleep(delay)
        try:
            resp = session.get_executions(category="linear", symbol=symbol, orderId=order_id)
            code, msg = resp.get("retCode"), resp.get("retMsg")
            rows = resp.get("result", {}).get("list", [])
            if code == 0:
                price, executed_qty = _weighted_execution_summary(rows)
                fee_total = 0.0
                for r in rows:
                    try:
                        fee_total += float(r.get("execFee", 0) or 0)
                    except (TypeError, ValueError):
                        pass
                tolerance = max(1e-10, abs(float(expected_qty or 0.0)) * 1e-7)
                if price and (expected_qty is None
                              or executed_qty + tolerance >= float(expected_qty)):
                    return {"price": price, "qty": executed_qty, "fee": fee_total}
                last_status = (f"retCode=0 retMsg={msg!r}, rows={len(rows)}, "
                               f"executed_qty={executed_qty}, expected={expected_qty}, "
                               f"fee_seen={fee_total}")
            else:
                last_status = f"retCode={code!r} retMsg={msg!r}"
        except Exception as e:
            last_status = f"exception={type(e).__name__}: {e}"

    taker_rate = float(CONFIG.get("taker_fee_rate", 0.00055))
    for method_name in ("get_open_orders", "get_order_history"):
        method = getattr(session, method_name, None)
        if method is None:
            continue
        try:
            resp = method(category="linear", symbol=symbol, orderId=order_id)
            code, msg = resp.get("retCode"), resp.get("retMsg")
            rows = resp.get("result", {}).get("list", [])
            if code == 0:
                for row in rows:
                    qty = float(row.get("cumExecQty", 0) or 0)
                    price = float(row.get("avgPrice", 0) or 0)
                    tolerance = max(1e-10, abs(float(expected_qty or 0.0)) * 1e-7)
                    if (qty > 0 and price > 0 and
                            (expected_qty is None or qty + tolerance >= float(expected_qty))):
                        return {"price": price, "qty": qty,
                                "fee": qty * price * taker_rate}
        except Exception:
            pass

    log.error("Не удалось подтвердить fill+fee: %s order=%s; %s",
              symbol, order_id, last_status)
    return None


def _order_fill_price(session, symbol, order_id, expected_qty=None):
    s = _order_fill_summary(session, symbol, order_id, expected_qty)
    return s["price"] if s else None


def get_wallet_equity(session):
    try:
        bal = session.get_wallet_balance(accountType="UNIFIED")["result"]["list"][0]
        return float(bal.get("totalEquity", 0))
    except Exception as e:
        log.debug("equity fetch: %s", e)
        return None


def complex_gross_pnl(cs: ComplexState, positions_map: Dict[Tuple[str, int], Dict]) -> float:
    """Считает PnL комплекта на основе нативных позиций биржи (с учетом positionIdx)."""
    pnl = float(cs.realized_pnl)

    pos_idx_leg1 = 1 if cs.leg1_long else 2
    pos_idx_leg2 = 2 if cs.leg1_long else 1

    for sym in cs.complex["leg1"]:
        p = positions_map.get((sym, pos_idx_leg1))
        if p: pnl += float(p.get("unrealisedPnl", 0))

    for sym in cs.complex["leg2"]:
        p = positions_map.get((sym, pos_idx_leg2))
        if p: pnl += float(p.get("unrealisedPnl", 0))

    return pnl


def complex_net_pnl(cs: ComplexState, positions_map: Dict[Tuple[str, int], Dict], prices: Dict[str, float],
                    taker_rate: float) -> float:
    gross = complex_gross_pnl(cs, positions_map)
    exit_cost_rate = taker_rate + float(CONFIG.get("estimated_exit_slippage_rate", 0.0010))

    pos_idx_leg1 = 1 if cs.leg1_long else 2
    pos_idx_leg2 = 2 if cs.leg1_long else 1

    exit_cost_est = 0.0
    for sym in cs.complex["leg1"]:
        p = positions_map.get((sym, pos_idx_leg1))
        if p:
            size = abs(float(p.get("size", 0)))
            price = float(prices.get(sym) or p.get("markPrice", 0) or 0)
            if size > 0 and price > 0:
                exit_cost_est += size * price * exit_cost_rate

    for sym in cs.complex["leg2"]:
        p = positions_map.get((sym, pos_idx_leg2))
        if p:
            size = abs(float(p.get("size", 0)))
            price = float(prices.get(sym) or p.get("markPrice", 0) or 0)
            if size > 0 and price > 0:
                exit_cost_est += size * price * exit_cost_rate

    return gross - cs.fees_paid - exit_cost_est


def get_all_prices(session) -> Dict[str, float]:
    try:
        tickers = session.get_tickers(category="linear")["result"]["list"]
        return {t["symbol"]: float(t["lastPrice"]) for t in tickers}
    except Exception as e:
        log.error("Ошибка get_tickers (all): %s", e)
        return {}


def get_all_positions_paginated(session) -> List[Dict]:
    all_positions: List[Dict] = []
    cursor = ""
    page = 0
    while True:
        try:
            params = {"category": "linear", "settleCoin": "USDT"}
            if cursor:
                params["cursor"] = cursor
            resp = session.get_positions(**params)
        except Exception as e:
            log.error("get_positions (page %d): %s", page, e)
            break
        result = resp.get("result", {})
        all_positions.extend(result.get("list", []))
        cursor = (result.get("nextPageCursor") or "").strip()
        page += 1
        if not cursor:
            break
        if page > 20:
            log.warning("get_positions: больше 20 страниц, прерываю пагинацию")
            break
        time.sleep(0.05)
    return all_positions


def get_all_positions_rich(session) -> Dict[Tuple[str, int], Dict]:
    """Возвращает словарь {(symbol, positionIdx): position_dict}"""
    out = {}
    for p in get_all_positions_paginated(session):
        if float(p.get("size", 0)) != 0:
            out[(p["symbol"], int(p.get("positionIdx", 0)))] = p
    return out


def compute_spread_cached(cs: ComplexState, prices: Dict[str, float]) -> float:
    vals = []
    for sym in cs.all_symbols():
        p = prices.get(sym)
        if p is None:
            return 0.0
        ref = cs.norm_refs.get(sym, p)
        vals.append(p / ref if ref > 0 else 1.0)
    return sum(vals[:3]) / 3 - sum(vals[3:]) / 3


# ============================================================
# РАЗОВЫЕ ВЫЗОВЫ
# ============================================================
def _get_unrealised(session, cs: ComplexState) -> float:
    pos_map = get_all_positions_rich(session)
    pnl = 0.0
    pos_idx_leg1 = 1 if cs.leg1_long else 2
    pos_idx_leg2 = 2 if cs.leg1_long else 1
    for sym in cs.complex["leg1"]:
        p = pos_map.get((sym, pos_idx_leg1))
        if p: pnl += float(p.get("unrealisedPnl", 0))
    for sym in cs.complex["leg2"]:
        p = pos_map.get((sym, pos_idx_leg2))
        if p: pnl += float(p.get("unrealisedPnl", 0))
    return pnl


def free_margin(session):
    try:
        bal = session.get_wallet_balance(accountType="UNIFIED")["result"]["list"][0]
        return float(bal.get("totalAvailableBalance", 0))
    except Exception:
        return 0.0


def ensure_cross_margin(session, symbols):
    for sym in symbols:
        try:
            session.switch_margin_mode(
                category="linear", symbol=sym, tradeMode=0,
                buyLeverage=str(CONFIG["leverage"]),
                sellLeverage=str(CONFIG["leverage"]),
            )
        except Exception as e:
            log.debug("switch_margin_mode %s: %s", sym, e)
        try:
            session.set_leverage(
                category="linear", symbol=sym,
                buyLeverage=str(CONFIG["leverage"]),
                sellLeverage=str(CONFIG["leverage"]),
            )
        except Exception as e:
            log.debug("set_leverage %s: %s", sym, e)


def ensure_hedge_mode(session, symbols):
    log.info("Переключение режима позиции в HEDGE MODE...")
    failed = []
    for sym in symbols:
        try:
            # mode=3 - это Hedge Mode в Bybit API
            session.switch_position_mode(category="linear", symbol=sym, mode=3)
            log.info("  %s: Hedge Mode OK", sym)
        except Exception as e:
            err_str = str(e)
            if "110025" in err_str or "not modified" in err_str.lower():
                log.info("  %s: уже Hedge Mode", sym)
            else:
                log.warning("  %s: switch_position_mode ошибка: %s", sym, err_str)
                failed.append(sym)
    if failed:
        log.error("Не удалось переключить в Hedge Mode: %s", failed)
    else:
        log.info("Все символы в Hedge Mode (или уже были).")


# ============================================================
# ОТКРЫТИЕ / ЗАКРЫТИЕ / ДОБОР / ХЕДЖ
# ============================================================
def _log_slippage(session, cs: ComplexState, expected_prices: Dict[str, float]):
    try:
        positions = get_all_positions_paginated(session)
        actual = {}
        for p in positions:
            if float(p.get("size", 0)) != 0 and p["symbol"] in expected_prices:
                actual[p["symbol"]] = float(p.get("avgPrice", 0))
    except Exception as e:
        log.warning("Слиппедж не замерен: %s", e)
        return

    slips = []
    for sym, exp in expected_prices.items():
        act = actual.get(sym)
        if not act or exp <= 0:
            continue
        slip_pct = abs(act - exp) / exp * 100.0
        slips.append((sym, exp, act, slip_pct))

    if not slips:
        return

    avg_slip = sum(s[3] for s in slips) / len(slips)
    worst = max(slips, key=lambda x: x[3])

    if avg_slip > 0.05 or worst[3] > 0.15:
        log.warning(
            "СЛИППЕДЖ: avg=%.3f%% max=%.3f%% (%s %.6f→%.6f) | %s / %s",
            avg_slip, worst[3], worst[0], worst[1], worst[2],
            "/".join(cs.complex["leg1"]), "/".join(cs.complex["leg2"]),
        )
    else:
        log.info(
            "СЛИППЕДЖ: avg=%.3f%% max=%.3f%% | %s / %s",
            avg_slip, worst[3],
            "/".join(cs.complex["leg1"]), "/".join(cs.complex["leg2"]),
        )


def open_complex(session, comp: Dict, entry_S: float) -> ComplexState:
    refs = {sym: current_price(session, sym) for sym in list(comp["leg1"]) + list(comp["leg2"])}
    cs = ComplexState(comp, refs)
    cs.entry_month_refs = {sym: MONTH_REFS.get(sym, 0.0) for sym in cs.all_symbols()} # ★
    cs.entry_spread = entry_S

    side1 = "Sell" if entry_S > 0 else "Buy"
    side2 = "Buy" if entry_S > 0 else "Sell"
    cs.leg1_long = (side1 == "Buy")

    # В Hedge Mode: Long = 1, Short = 2
    pos_idx1 = 1 if side1 == "Buy" else 2
    pos_idx2 = 1 if side2 == "Buy" else 2

    expected_prices: Dict[str, float] = {}
    qty = {}
    for sym in cs.all_symbols():
        p = current_price(session, sym)
        expected_prices[sym] = p
        qty[sym] = get_qty(session, sym, CONFIG["usd_per_symbol"], p)

    opened = []
    entry_fill_prices = {}
    entry_fees_total = 0.0
    try:
        for sym in cs.complex["leg1"]:
            oid = place_market(session, sym, side1, qty[sym], pos_idx1)
            summ = _order_fill_summary(session, sym, oid)
            entry_fill_prices[sym] = summ["price"] if summ else 0.0
            if summ:
                entry_fees_total += summ["fee"]
            opened.append((sym, side1, pos_idx1))
        for sym in cs.complex["leg2"]:
            oid = place_market(session, sym, side2, qty[sym], pos_idx2)
            summ = _order_fill_summary(session, sym, oid)
            entry_fill_prices[sym] = summ["price"] if summ else 0.0
            if summ:
                entry_fees_total += summ["fee"]
            opened.append((sym, side2, pos_idx2))
    except Exception as e:
        log.error("ОШИБКА при открытии %s / %s: %s. Откатываю %d.",
                  "/".join(cs.complex["leg1"]), "/".join(cs.complex["leg2"]),
                  e, len(opened))
        for sym, side, p_idx in opened:
            try:
                reverse = "Sell" if side == "Buy" else "Buy"
                oid = place_market(session, sym, reverse, qty[sym], p_idx)
                rb = _order_fill_summary(session, sym, oid, expected_qty=qty[sym])
                if rb:
                    entry_fees_total += rb["fee"]
            except Exception as e2:
                log.error("  откат %s не удался: %s", sym, e2)
        cs.fees_paid += entry_fees_total
        raise

    cs.fees_paid += entry_fees_total

    _log_slippage(session, cs, expected_prices)

    cs.last_add_prices = dict(entry_fill_prices)
    spread_snapshot = get_all_prices(session)
    if all(float(spread_snapshot.get(sym, 0.0) or 0.0) > 0 for sym in cs.all_symbols()):
        cs.last_spread_snapshot = {sym: float(spread_snapshot[sym]) for sym in cs.all_symbols()}
        cs.selective_add_initialized = True
    cs.layers = [{"qty": qty, "spread": entry_S, "add_prices": dict(entry_fill_prices),
                  "spread_snapshot": dict(cs.last_spread_snapshot)}]
    ratio = abs(entry_S) / cs.complex["d_max"] if cs.complex["d_max"] > 0 else 0.0
    log.info("ВХОД #%d: %s / %s | S=%.4f | ratio=%.2f | target=$%.2f | fees=%.3f",
             len(qty), "/".join(cs.complex["leg1"]), "/".join(cs.complex["leg2"]),
             entry_S, ratio, cs.target, cs.fees_paid)
    trade_log.log_event(cs, "open",
                        entry_spread=entry_S, ratio=ratio,
                        target=cs.target, score=cs.complex.get("score", 0))
    return cs


def select_balanced_add_pairs(leg1_candidates, leg2_candidates,
                              adverse_trigger_pct, min_pair_net_adverse_pct):
    def valid(left, right):
        own_moves = [x["own_adverse_pct"] for x in (left, right)
                     if x.get("own_adverse_pct") is not None]
        has_anchor = any(x > adverse_trigger_pct for x in own_moves)
        pair_spread_move = (left["spread_adverse_contribution_pct"] +
                            right["spread_adverse_contribution_pct"])
        return has_anchor and pair_spread_move > min_pair_net_adverse_pct

    max_pairs = min(len(leg1_candidates), len(leg2_candidates))
    for count in range(max_pairs, 0, -1):
        best, best_key = None, None
        for left_subset in combinations(leg1_candidates, count):
            for right_subset in combinations(leg2_candidates, count):
                for right_order in permutations(right_subset):
                    candidate_pairs = list(zip(left_subset, right_order))
                    if not all(valid(left, right) for left, right in candidate_pairs):
                        continue
                    total_adverse = sum(left["spread_adverse_contribution_pct"] +
                                        right["spread_adverse_contribution_pct"]
                                        for left, right in candidate_pairs)
                    mismatch = sum(abs(left["spread_adverse_contribution_pct"] -
                                       right["spread_adverse_contribution_pct"])
                                   for left, right in candidate_pairs)
                    key = (total_adverse, -mismatch)
                    if best_key is None or key > best_key:
                        best, best_key = candidate_pairs, key
        if best:
            return sorted(best,
                          key=lambda pair: (-(pair[0]["spread_adverse_contribution_pct"] +
                                              pair[1]["spread_adverse_contribution_pct"]),
                                            abs(pair[0]["spread_adverse_contribution_pct"] -
                                                pair[1]["spread_adverse_contribution_pct"])))
    return []


def add_layer(session, cs: ComplexState) -> bool:
    if getattr(cs, "fill_reconciliation_required", False):
        log.critical("Добор заблокирован для %s / %s до сверки неизвестных order ID: %s",
                     "/".join(cs.complex["leg1"]), "/".join(cs.complex["leg2"]),
                     getattr(cs, "pending_add_orders", []))
        return False
    side1 = "Buy" if cs.leg1_long else "Sell"
    side2 = "Sell" if cs.leg1_long else "Buy"
    pos_idx1 = 1 if side1 == "Buy" else 2
    pos_idx2 = 1 if side2 == "Buy" else 2

    positions_rich = get_all_positions_rich(session)
    prices = get_all_prices(session)
    if not prices:
        log.warning("Выборочный добор отменён: не получен снимок цен")
        return False

    if not hasattr(cs, "last_add_prices"):
        cs.last_add_prices = {}
    if not getattr(cs, "selective_add_initialized", False):
        log.warning("Выборочный добор отложен: стартовые цены новой логики ещё не зафиксированы")
        return False
    spread_snapshot = getattr(cs, "last_spread_snapshot", {})
    if not all(float(spread_snapshot.get(sym, 0.0) or 0.0) > 0 for sym in cs.all_symbols()):
        log.warning("Выборочный добор отложен: неполный общий snapshot spread")
        return False
    buffer_pct = max(0.0, float(CONFIG.get("add_price_buffer_pct", 0.0)))
    groups = []
    for group_name, symbols, expected_side, pos_idx in (
            ("leg1", list(cs.complex["leg1"]), side1, pos_idx1),
            ("leg2", list(cs.complex["leg2"]), side2, pos_idx2)):
        eligible = []
        for sym in symbols:
            pos = positions_rich.get((sym, pos_idx))
            price = float(prices.get(sym, 0.0) or 0.0)
            if not pos or price <= 0:
                continue
            try:
                symbol_pnl = float(pos.get("unrealisedPnl", 0.0) or 0.0)
                reference = float(cs.last_add_prices.get(sym, 0.0) or 0.0)
            except (TypeError, ValueError):
                continue
            own_adverse_pct = None
            if reference > 0:
                own_adverse_fraction = ((reference - price) / reference if expected_side == "Buy"
                                        else (price - reference) / reference)
                own_adverse_pct = own_adverse_fraction * 100.0

            snapshot_price = float(spread_snapshot.get(sym, 0.0) or 0.0)
            move_pct = (price / snapshot_price - 1.0) * 100.0
            spread_component_pct = move_pct / 3.0 if group_name == "leg1" else -move_pct / 3.0
            adverse_orientation = -1.0 if cs.leg1_long else 1.0
            spread_adverse_pct = adverse_orientation * spread_component_pct
            eligible.append({"symbol": sym, "side": expected_side, "pnl": symbol_pnl,
                             "price": price, "reference": reference,
                             "own_adverse_pct": own_adverse_pct,
                             "spread_adverse_contribution_pct": spread_adverse_pct,
                             "qty": get_qty(session, sym, CONFIG["usd_per_symbol"], price),
                             "pos_idx": pos_idx,
                             "group": group_name})
        eligible.sort(key=lambda item: (-item["spread_adverse_contribution_pct"],
                                        -(item["own_adverse_pct"] or 0.0), item["pnl"]))
        groups.append(eligible)

    leg1_candidates, leg2_candidates = groups
    trigger_pct = buffer_pct * 100.0
    min_pair_net = float(CONFIG.get("add_pair_min_net_adverse_pct", 0.0))
    pairs = select_balanced_add_pairs(leg1_candidates, leg2_candidates,
                                      trigger_pct, min_pair_net)
    if not pairs:
        return False

    available = free_margin(session)
    leverage = max(1.0, float(CONFIG["leverage"]))
    reserve_factor = 1.0 + max(0.0, float(CONFIG.get("hedge_buffer_pct", 0.05)))
    affordable = []
    margin_used = 0.0
    for left, right in pairs:
        pair_notional = left["qty"] * left["price"] + right["qty"] * right["price"]
        pair_margin = pair_notional / leverage * reserve_factor
        if margin_used + pair_margin > available:
            break
        affordable.append((left, right))
        margin_used += pair_margin
    if not affordable:
        log.warning("Выборочный добор отменён: свободная маржа $%.2f, нужно $%.2f",
                    available, (pairs[0][0]["qty"] * pairs[0][0]["price"] +
                                pairs[0][1]["qty"] * pairs[0][1]["price"]) / leverage * reserve_factor)
        return False

    layer_qty, fill_prices, adverse_scores, spread_scores = {}, {}, {}, {}
    completed_pairs = []
    failed_pair = False
    for left, right in affordable:
        pair_orders = []
        pair_ok = True
        pair_fee = 0.0
        for item in (left, right):
            sym = item["symbol"]
            try:
                oid = place_market(session, sym, item["side"], item["qty"], item["pos_idx"])
                summ = _order_fill_summary(session, sym, oid, expected_qty=item["qty"])
                if summ is None:
                    cs.fill_reconciliation_required = True
                    cs.pending_add_orders.append({
                        "symbol": sym, "order_id": oid, "side": item["side"],
                        "requested_qty": item["qty"], "status": "fill_unknown",
                        "created_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
                    })
                    log.critical("НЕИЗВЕСТНОЕ ИСПОЛНЕНИЕ: %s order=%s", sym, oid)
                    pair_ok = False
                    break
                pair_orders.append((sym, item["side"], item["qty"], summ["price"],
                                    item["own_adverse_pct"], item["spread_adverse_contribution_pct"],
                                    item["pos_idx"]))
                pair_fee += summ["fee"]
            except Exception as e:
                log.error("Симметричный добор %s/%s: ордер %s не прошёл: %s",
                          left["symbol"], right["symbol"], sym, e)
                pair_ok = False
                break

        if not pair_ok:
            cs.fees_paid += pair_fee
            for sym, side, qty, fill_price, adverse_pct, spread_score, p_idx in pair_orders:
                reverse = "Sell" if side == "Buy" else "Buy"
                try:
                    oid = place_market(session, sym, reverse, qty, p_idx)
                    rb = _order_fill_summary(session, sym, oid, expected_qty=qty)
                    if rb:
                        cs.fees_paid += rb["fee"]
                except Exception as rollback_error:
                    log.critical("НЕ УДАЛОСЬ ОТКАТИТЬ %s qty=%s: %s", sym, qty, rollback_error)
                    layer_qty[sym] = qty
                    fill_prices[sym] = fill_price
                    adverse_scores[sym] = adverse_pct
                    spread_scores[sym] = spread_score
            failed_pair = True
            break

        cs.fees_paid += pair_fee
        completed_pairs.append((left["symbol"], right["symbol"]))
        for sym, side, qty, fill_price, adverse_pct, spread_score, p_idx in pair_orders:
            layer_qty[sym] = qty
            fill_prices[sym] = fill_price
            adverse_scores[sym] = adverse_pct
            spread_scores[sym] = spread_score
            cs.last_add_prices[sym] = fill_price or 0.0

    if not layer_qty:
        return False

    fresh_prices = get_all_prices(session) or prices
    if not all(float(fresh_prices.get(sym, 0.0) or 0.0) > 0 for sym in cs.all_symbols()):
        fresh_prices = prices
    spread_after = compute_spread_cached(cs, fresh_prices)
    cs.last_spread_snapshot = {sym: float(fresh_prices[sym]) for sym in cs.all_symbols()}
    cs.layers.append({"qty": layer_qty, "spread": spread_after,
                      "add_prices": dict(fill_prices), "pairs": completed_pairs,
                      "adverse_from_previous_pct": dict(adverse_scores),
                      "spread_adverse_contribution_pct": dict(spread_scores),
                      "spread_snapshot": dict(cs.last_spread_snapshot),
                      "partial_failure": failed_pair})
    added_symbols = sorted(layer_qty)
    log.info("ВЫБОРОЧНЫЙ ДОБОР: слоёв=%d | пары=%s | fills=%s | fees_total=%.3f",
             len(cs.layers), completed_pairs, fill_prices, cs.fees_paid)
    trade_log.log_event(cs, "add_layer", layer_n=len(cs.layers))
    if getattr(cs, "fill_reconciliation_required", False):
        return False
    return True


def open_hedge(session, cs: ComplexState):
    pre_unrealised = _get_unrealised(session, cs)
    side1 = "Sell" if cs.leg1_long else "Buy"
    side2 = "Buy" if cs.leg1_long else "Sell"
    pos_idx1 = 2 if cs.leg1_long else 1
    pos_idx2 = 1 if cs.leg1_long else 2

    hedge_qty = {}
    hedge_fees = 0.0
    for sym in cs.complex["leg1"]:
        total = cs.total_qty(sym)
        hedge_qty[sym] = total
        oid = place_market(session, sym, side1, total, pos_idx1)
        summ = _order_fill_summary(session, sym, oid, expected_qty=total)
        if summ: hedge_fees += summ["fee"]
    for sym in cs.complex["leg2"]:
        total = cs.total_qty(sym)
        hedge_qty[sym] = total
        oid = place_market(session, sym, side2, total, pos_idx2)
        summ = _order_fill_summary(session, sym, oid, expected_qty=total)
        if summ: hedge_fees += summ["fee"]

    cs.fees_paid += hedge_fees
    cs.realized_pnl += pre_unrealised
    cs.hedge_open = True
    prices = get_all_prices(session)
    cs.hedge_spread = compute_spread_cached(cs, prices)
    cs.hedge_qty = hedge_qty
    log.info("ХЕДЖ (закрытие): S=%.4f | locked PnL=%.2f | fees=%.3f | total realized=%.2f",
             cs.hedge_spread, pre_unrealised, cs.fees_paid, cs.realized_pnl)
    trade_log.log_event(cs, "hedge", pnl_usd=pre_unrealised,
                        hedge_spread=cs.hedge_spread)


def close_hedge(session, cs: ComplexState):
    side1 = "Buy" if cs.leg1_long else "Sell"
    side2 = "Sell" if cs.leg1_long else "Buy"
    pos_idx1 = 1 if cs.leg1_long else 2
    pos_idx2 = 2 if cs.leg1_long else 1

    reopen_fees = 0.0
    for sym in cs.complex["leg1"]:
        oid = place_market(session, sym, side1, cs.hedge_qty[sym], pos_idx1)
        rb = _order_fill_summary(session, sym, oid, expected_qty=cs.hedge_qty[sym])
        if rb: reopen_fees += rb["fee"]
    for sym in cs.complex["leg2"]:
        oid = place_market(session, sym, side2, cs.hedge_qty[sym], pos_idx2)
        rb = _order_fill_summary(session, sym, oid, expected_qty=cs.hedge_qty[sym])
        if rb: reopen_fees += rb["fee"]

    cs.fees_paid += reopen_fees
    cs.hedge_open = False
    cs.hedge_qty = {}
    log.info("ПЕРЕОТКРЫТИЕ после хеджа (paid_fees=%.3f)", cs.fees_paid)
    trade_log.log_event(cs, "unhedge")


def _log_slippage_on_close(session, cs: ComplexState,
                           expected_prices: Dict[str, float],
                           order_ids: Dict[str, str]):
    slips = []
    for _attempt in range(3):
        time.sleep(1.0)
        slips = []
        for sym, oid in order_ids.items():
            exp = expected_prices.get(sym)
            if not exp or not oid:
                continue
            try:
                execs = session.get_executions(
                    category="linear", symbol=sym, orderId=oid,
                )["result"]["list"]
            except Exception as e:
                log.debug("get_executions %s: %s", sym, e)
                continue
            if not execs:
                continue
            total_qty = sum(float(e["execQty"]) for e in execs)
            if total_qty <= 0:
                continue
            avg_price = sum(float(e["execPrice"]) * float(e["execQty"])
                            for e in execs) / total_qty
            slip_pct = abs(avg_price - exp) / exp * 100.0
            slips.append((sym, exp, avg_price, slip_pct))
        if slips:
            break

    if not slips:
        return

    avg_slip = sum(s[3] for s in slips) / len(slips)
    worst = max(slips, key=lambda x: x[3])
    msg = ("СЛИППЕДЖ ВЫХОДА: avg=%.3f%% max=%.3f%% (%s %.6f→%.6f) | %s / %s"
           % (avg_slip, worst[3], worst[0], worst[1], worst[2],
              "/".join(cs.complex["leg1"]), "/".join(cs.complex["leg2"])))
    if avg_slip > 0.05 or worst[3] > 0.15:
        log.warning(msg)
    else:
        log.info(msg)


def close_complex(session, cs: ComplexState, portfolio: Portfolio = None):
    """Безопасное закрытие в Hedge Mode."""
    targets: List[Tuple[str, str, float, int]] = []

    pos_idx_leg1 = 1 if cs.leg1_long else 2
    pos_idx_leg2 = 2 if cs.leg1_long else 1

    for sym in cs.complex["leg1"]:
        qty = cs.total_qty(sym)
        if qty > 0:
            orig_side = "Buy" if cs.leg1_long else "Sell"
            close_side = "Sell" if orig_side == "Buy" else "Buy"
            targets.append((sym, close_side, qty, pos_idx_leg1))

    for sym in cs.complex["leg2"]:
        qty = cs.total_qty(sym)
        if qty > 0:
            orig_side = "Sell" if cs.leg1_long else "Buy"
            close_side = "Sell" if orig_side == "Buy" else "Buy"
            targets.append((sym, close_side, qty, pos_idx_leg2))

    if not targets:
        log.info("ЗАКРЫТ КОМПЛЕКТ (already closed): %s / %s",
                 "/".join(cs.complex["leg1"]), "/".join(cs.complex["leg2"]))
        return

    expected_prices = get_all_prices(session) or {}
    still_open, failed, order_ids = _close_symbols_fast(session, targets)

    if still_open or failed:
        log.error("⚠ КОМПЛЕКТ НЕ ЗАКРЫТ ПОЛНОСТЬЮ: %s / %s",
                  "/".join(cs.complex["leg1"]), "/".join(cs.complex["leg2"]))
        for sym in still_open: log.error("    open: %s", sym)
        for sym, err in failed: log.error("    fail: %s → %s", sym, err)
        raise RuntimeError(f"Partial close: {len(still_open)} open, {len(failed)} failed")

    log.info("ЗАКРЫТ КОМПЛЕКТ: %s / %s (paid_fees=%.3f)",
             "/".join(cs.complex["leg1"]), "/".join(cs.complex["leg2"]), cs.fees_paid)

    if CONFIG.get("close_slippage_log_async", True):
        def _bg():
            try:
                _log_slippage_on_close(session, cs, expected_prices, order_ids)
            except Exception:
                pass

        threading.Thread(target=_bg, daemon=True, name="slip-log").start()
    else:
        _log_slippage_on_close(session, cs, expected_prices, order_ids)


def _close_symbols_fast(session, symbol_side_size_idx_list, max_retries=None):
    from concurrent.futures import ThreadPoolExecutor, as_completed
    if max_retries is None: max_retries = int(CONFIG.get("close_verify_retries", 2))
    workers = int(CONFIG.get("close_parallel_workers", 8))

    if not symbol_side_size_idx_list:
        return [], [], {}

    order_ids: Dict[str, str] = {}
    failed: List[Tuple[str, str]] = []

    def _fire(sym, side, size, p_idx):
        try:
            return sym, place_market(session, sym, side, size, p_idx), None
        except Exception as e:
            return sym, None, str(e)

    with ThreadPoolExecutor(max_workers=min(workers, len(symbol_side_size_idx_list))) as ex:
        for f in as_completed([ex.submit(_fire, *t) for t in symbol_side_size_idx_list]):
            sym, oid, err = f.result()
            if err:
                failed.append((sym, err))
                log.error("  close %s: %s", sym, err)
            else:
                order_ids[sym] = oid

    time.sleep(float(CONFIG.get("close_verify_wait_sec", 0.5)))

    live = get_all_positions_rich(session)
    still_open = []
    for sym, side, size, p_idx in symbol_side_size_idx_list:
        if (sym, p_idx) in live:
            still_open.append(sym)

    for attempt in range(max_retries):
        if not still_open: break
        log.warning("close retry %d/%d: %d stuck → %s", attempt + 1, max_retries, len(still_open), still_open)
        orig_map = {(t[0], t[3]): t for t in symbol_side_size_idx_list}

        retry = []
        for sym in still_open:
            p_idx = orig_map[(sym, next(k[1] for k in orig_map if k[0] == sym))][3]
            retry.append(orig_map[(sym, p_idx)])

        with ThreadPoolExecutor(max_workers=min(workers, len(retry))) as ex:
            for f in as_completed([ex.submit(_fire, *t) for t in retry]):
                sym, oid, err = f.result()
                if err:
                    failed.append((sym, err))
                elif oid:
                    order_ids[sym] = oid
        time.sleep(float(CONFIG.get("close_verify_wait_sec", 0.5)))
        live = get_all_positions_rich(session)
        still_open = []
        for sym, side, size, p_idx in symbol_side_size_idx_list:
            if (sym, p_idx) in live:
                still_open.append(sym)

    return still_open, failed, order_ids


# ============================================================
# ПОДГОТОВКА РЕЗЕРВА (ФОНОВЫЙ ПОТОК)
# ============================================================
def prepare_reserve_bg(trade_session, active_exposures: Dict[str, List[str]]) -> Optional[List[Dict]]:
    try:
        market = make_market_session()
        symbols = get_liquid_volatile_symbols(market)
        if len(symbols) < CONFIG["leg_size"] * 2:
            return None

        check_and_load_history(market, symbols)

        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        start_ms = now_ms - CONFIG["history_days"] * 86400_000
        pivot = load_norm_pivot(symbols, start_ms, now_ms)
        if pivot.empty: return None

        ensure_month_refs(list(pivot.columns), start_ms)
        pivot = filter_pump_dump_symbols(pivot)
        if pivot.empty or pivot.shape[1] < CONFIG["leg_size"] * 2: return None

        # ★ ФИЛЬТРАЦИЯ ПО КОЛИЧЕСТВУ ИСПОЛЬЗОВАНИЙ
        available_cols = []
        for c in pivot.columns:
            if len(active_exposures.get(c, [])) < CONFIG["max_symbol_usage"]:
                available_cols.append(c)
                
        if len(available_cols) < CONFIG["leg_size"] * 2: return None
        pivot = pivot[available_cols]
        symbols_filtered = available_cols

        annual_prices = None
        if CONFIG.get("annual_stability_enabled", True):
            annual_days = int(CONFIG.get("annual_history_days", 365))
            annual_interval = str(CONFIG.get("annual_interval", "D"))
            try:
                if annual_interval == "D":
                    check_and_load_daily_history(market, symbols_filtered, history_days=annual_days)
                else:
                    check_and_load_history(market, symbols_filtered, interval=annual_interval, history_days=annual_days)
                if annual_interval == "D":
                    now_utc = datetime.now(timezone.utc)
                    today_utc = datetime(now_utc.year, now_utc.month, now_utc.day, tzinfo=timezone.utc)
                    annual_end_ms = int(today_utc.timestamp() * 1000) - 1
                else:
                    annual_end_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
                annual_start_ms = annual_end_ms - annual_days * 86400_000
                annual_prices = load_price_pivot(symbols_filtered, annual_start_ms, annual_end_ms, annual_interval)
                if not annual_prices.empty:
                    annual_prices.index = pd.to_datetime(annual_prices.index, unit="ms", utc=True)
                    annual_prices = annual_prices.resample("1D").last()
            except Exception as e:
                log.exception("[BG] Annual stability data unavailable: %s", e)

        # ★ ПЕРЕДАЕМ active_exposures В BUILD_COMPLEXES
        complexes = build_complexes(symbols_filtered, pivot, annual_prices=annual_prices, active_exposures=active_exposures)
        if not complexes: return None

        for i, c in enumerate(complexes[:8], 1):
            log.info("[BG] Комплект %d: %s / %s | Dmax=%.4f | cyc=%d | rank=%.2f",
                     i, "/".join(c["leg1"]), "/".join(c["leg2"]), c["d_max"], c["cycles"],
                     c.get("rank_score", c["score"]))

        all_syms = sorted({s for c in complexes for s in list(c["leg1"]) + list(c["leg2"])})
        ensure_cross_margin(trade_session, all_syms)
        ensure_hedge_mode(trade_session, all_syms)

        return complexes
    except Exception as e:
        log.exception("[BG] Ошибка prepare_reserve_bg: %s", e)
        return None

def rebuild_worker(active_exposures_snapshot: Dict[str, List[str]]):
    global REBUILD_STATE
    log.info("[BG] === Пересборка запущена (в фоне) ===")
    t_start = time.time()
    result = None
    err = None
    try:
        trade_session = make_trade_session()
        result = prepare_reserve_bg(trade_session, active_exposures_snapshot)
    except Exception as e:
        log.exception("[BG] Фатальная ошибка: %s", e)
        err = str(e)
    finally:
        with PORTFOLIO_LOCK:
            REBUILD_STATE["reserve"] = result
            REBUILD_STATE["error"] = err
            REBUILD_STATE["done"] = True
            REBUILD_STATE["running"] = False
        dur = time.time() - t_start
        if result:
            log.info("[BG] === Пересборка завершена за %.0fs: %d комплектов ===", dur, len(result))
        else:
            log.warning("[BG] === Пересборка не дала результата (%.0fs) ===", dur)
            
# ============================================================
# ЗАПОЛНЕНИЕ СЛОТОВ
# ============================================================
def fill_free_slots(trade, portfolio: Portfolio):
    global LAST_RATIO_LOG

    max_c = CONFIG["max_concurrent_complexes"]
    if len(portfolio.active) >= max_c: return

    free_slots = max_c - len(portfolio.active)
    min_ratio = CONFIG["min_entry_ratio"]

    while free_slots > 0 and portfolio.reserve:
        exposures = portfolio.get_symbol_exposures()
        prices = get_all_prices(trade)

        best_comp = None
        best_ratio = -1.0
        best_S = 0.0

        for c in portfolio.reserve:
            try:
                vals = []
                for sym in list(c["leg1"]) + list(c["leg2"]):
                    cur = prices.get(sym)
                    ref = MONTH_REFS.get(sym)
                    if not cur or not ref:
                        raise ValueError("no price/ref")
                    vals.append(cur / ref)
                S = sum(vals[:3]) / 3 - sum(vals[3:]) / 3
                ratio = abs(S) / c["d_max"] if c["d_max"] > 0 else 0.0

                if ratio < min_ratio: continue

                will_be_leg1_long = (S < 0)
                valid_direction = True

                for sym in list(c["leg1"]):
                    if sym in exposures:
                        new_dir = 'long' if will_be_leg1_long else 'short'
                        if new_dir in exposures[sym]:
                            valid_direction = False
                            break
                if not valid_direction: continue

                for sym in list(c["leg2"]):
                    if sym in exposures:
                        new_dir = 'short' if will_be_leg1_long else 'long'
                        if new_dir in exposures[sym]:
                            valid_direction = False
                            break
                if not valid_direction: continue

                if ratio > best_ratio:
                    best_ratio = ratio
                    best_S = S
                    best_comp = c
            except Exception:
                continue

        if best_comp is None or best_ratio < min_ratio:
            if time.time() - LAST_RATIO_LOG > RATIO_LOG_INTERVAL:
                log.info("Нет точек входа: лучший ratio=%.2f < порога %.2f — ждём.", 
                         best_ratio if best_ratio > -1.0 else 0.0, min_ratio)
                LAST_RATIO_LOG = time.time()
            break

        try:
            cs = open_complex(trade, best_comp, best_S)
            portfolio.active.append(cs)
            portfolio.reserve.remove(best_comp)
            with PORTFOLIO_LOCK:
                portfolio.save()
            log.info("Открыт комплекс #%d (ratio=%.2f).", len(portfolio.active), best_ratio)
        except Exception as e:
            log.exception("Ошибка открытия — удаляю из reserve")
            if best_comp in portfolio.reserve:
                portfolio.reserve.remove(best_comp)
            continue
        free_slots -= 1


# ============================================================
# СОПРОВОЖДЕНИЕ
# ============================================================
def monitor_active(trade, portfolio: Portfolio):
    max_c = CONFIG["max_concurrent_complexes"]
    prices = get_all_prices(trade)
    positions_rich = get_all_positions_rich(trade)
    taker_rate = float(CONFIG.get("taker_fee_rate", 0.00055))

    with PORTFOLIO_LOCK:
        excess = len(portfolio.active) - max_c
        if excess > 0:
            log.warning("⚠ Активных %d > max %d. Закрываю %d лишних.", len(portfolio.active), max_c, excess)
            sorted_cs = sorted(portfolio.active, key=lambda c: complex_gross_pnl(c, positions_rich))
            for i in range(excess):
                cs = sorted_cs[i]
                try:
                    if cs.hedge_open: close_hedge(trade, cs)
                    close_complex(trade, cs)
                    if cs in portfolio.active: portfolio.active.remove(cs)
                except Exception as e:
                    log.exception("  не удалось закрыть лишний: %s", e)

    # ─── selective-add seeding ───
    seeded_this_cycle = set()
    for cs in portfolio.active:
        old_snapshot = getattr(cs, "last_spread_snapshot", {})
        snapshot_complete = all(float(old_snapshot.get(sym, 0.0) or 0.0) > 0 for sym in cs.all_symbols())
        if not getattr(cs, "selective_add_initialized", False) or not snapshot_complete:
            baseline = {sym: float(prices.get(sym, 0.0) or 0.0) for sym in cs.all_symbols()}
            if baseline and all(px > 0 for px in baseline.values()):
                cs.last_add_prices = baseline
                cs.last_spread_snapshot = dict(baseline)
                cs.selective_add_initialized = True
                seeded_this_cycle.add(id(cs))

    to_remove = []
    for cs in portfolio.active:
        try:
            spread = compute_spread_cached(cs, prices)
            pnl_gross = complex_gross_pnl(cs, positions_rich)
            pnl_net = complex_net_pnl(cs, positions_rich, prices, taker_rate)

            if getattr(cs, "close_pending", False):
                try:
                    close_complex(trade, cs)
                    cs.close_pending = False
                    to_remove.append(cs)
                except Exception as e:
                    log.warning("retry close %s / %s: %s", "/".join(cs.complex["leg1"]), "/".join(cs.complex["leg2"]),
                                e)
                continue

            if pnl_net >= cs.target:
                log.info("P&L net=$%.2f >= target=$%.2f (gross=$%.2f) → ЗАКРЫТИЕ (%s / %s)",
                         pnl_net, cs.target, pnl_gross, "/".join(cs.complex["leg1"]), "/".join(cs.complex["leg2"]))
                cs.close_pending = True
                try:
                    if cs.hedge_open: close_hedge(trade, cs)
                    close_complex(trade, cs)
                    cs.close_pending = False
                    to_remove.append(cs)
                    trade_log.log_event(cs, "close", pnl_usd=pnl_net, reason="take_profit", spread=spread)
                except Exception as e:
                    log.error("close_complex fail — retry: %s", e)
                continue

            if cs.hedge_open:
                h = cs.hedge_spread
                if (h > 0 and spread <= h * (1 - CONFIG["hedge_close_reversal"])) or \
                        (h < 0 and spread >= h * (1 - CONFIG["hedge_close_reversal"])):
                    close_hedge(trade, cs)
                continue

            grid_levels = CONFIG["grid_add_loss_levels_usd"]
            max_adds = min(int(CONFIG.get("max_adds", len(grid_levels))), len(grid_levels))
            grid_index = min(max(0, int(getattr(cs, "grid_level_index", max(0, len(cs.layers) - 1)))), max_adds)
            cs.grid_level_index = grid_index
            if grid_index < max_adds and len(cs.layers) < CONFIG["max_layers"]:
                next_level = -abs(float(grid_levels[grid_index]))
                if getattr(cs, "fill_reconciliation_required", False):
                    log.critical("Grid-добор пропущен для %s / %s: сверить order IDs", "/".join(cs.complex["leg1"]),
                                 "/".join(cs.complex["leg2"]))
                elif pnl_gross <= next_level:
                    if id(cs) in seeded_this_cycle:
                        pass
                    elif add_layer(trade, cs):
                        cs.last_grid_level = next_level
                        cs.grid_level_index = grid_index + 1
                        log.info("Уровень добора %.2f USD выполнен; следующий индекс=%d/%d", next_level,
                                 cs.grid_level_index, max_adds)

            fm = free_margin(trade)
            total_mirror = 0.0
            for other in portfolio.active:
                for sym in other.all_symbols():
                    p = prices.get(sym, 0.0)
                    total_mirror += other.total_qty(sym) * p

            required_all = (total_mirror / CONFIG["leverage"]) * (1 + CONFIG["hedge_buffer_pct"])
            if fm <= required_all and total_mirror > 0:
                worst = min(portfolio.active, key=lambda c: complex_gross_pnl(c, positions_rich))
                if worst is cs:
                    pass
                    # open_hedge(trade, cs)

        except Exception as e:
            log.exception("Ошибка сопровождения: %s", e)

    for cs in to_remove:
        if cs in portfolio.active:
            portfolio.active.remove(cs)


# ============================================================
# СВЕРКА С БИРЖЕЙ
# ============================================================
def reconcile_state_with_exchange(trade, portfolio: Portfolio):
    log.info("════════ СВЕРКА С БИРЖЕЙ ════════")

    try:
        bal = trade.get_wallet_balance(accountType="UNIFIED")["result"]["list"][0]
        log.info("БАЛАНС:  Total Equity = $%.2f | Available = $%.2f",
                 float(bal.get("totalEquity", 0)), float(bal.get("totalAvailableBalance", 0)))
    except Exception as e:
        log.warning("Не удалось получить баланс: %s", e)

    try:
        raw_positions = get_all_positions_paginated(trade)
    except Exception as e:
        log.warning("Не удалось получить позиции: %s", e)
        return

    live = {(p["symbol"], int(p.get("positionIdx", 0))): float(p.get("size", 0))
            for p in raw_positions if float(p.get("size", 0)) != 0}

    log.info("ПОЗИЦИИ НА БИРЖЕ: %d", len(live))

    expected = {}
    for cs in portfolio.active:
        pos_idx1 = 1 if cs.leg1_long else 2
        pos_idx2 = 2 if cs.leg1_long else 1
        for sym in cs.complex["leg1"]:
            qty = cs.total_qty(sym)
            expected[(sym, pos_idx1)] = expected.get((sym, pos_idx1), 0.0) + qty
        for sym in cs.complex["leg2"]:
            qty = cs.total_qty(sym)
            expected[(sym, pos_idx2)] = expected.get((sym, pos_idx2), 0.0) + qty

    mismatches = []
    all_keys = set(live.keys()) | set(expected.keys())
    for k in all_keys:
        exp = expected.get(k, 0.0)
        act = live.get(k, 0.0)
        if abs(exp - act) > max(1e-5, abs(exp) * 0.01):
            mismatches.append((k, exp, act))

    if mismatches:
        log.warning("⚠️ РАСХОЖДЕНИЕ ОЖИДАЕМЫХ И ФАКТИЧЕСКИХ ПОЗИЦИЙ:")
        for k, exp, act in mismatches:
            log.warning(f"  {k[0]} (Idx={k[1]}): ожидали {exp:.4f}, на бирже {act:.4f}")

    state_keys = set(expected.keys())
    orphan = set(live.keys()) - state_keys

    if orphan:
        log.warning("ВНЕШНИЕ ПОЗИЦИИ (ORPHANS): %d шт → %s", len(orphan), sorted(orphan))
        if CONFIG.get("close_orphan_positions", True):
            for key in sorted(orphan):
                try:
                    sym, p_idx = key
                    size = abs(live[key])
                    p = next((x for x in raw_positions if x["symbol"] == sym and int(x.get("positionIdx", 0)) == p_idx),
                             None)
                    if not p: continue

                    side_close = "Sell" if p["side"] == "Buy" else "Buy"
                    place_market(trade, sym, side_close, size, p_idx)
                    trade_log.log_orphan_close(sym, side_close, size)
                    log.info("  закрыт orphan: %s Idx=%s %s %s", sym, p_idx, side_close, size)
                except Exception as e:
                    log.exception("Не удалось закрыть orphan %s: %s", key, e)

    with PORTFOLIO_LOCK:
        portfolio.save()


# ============================================================
# MAIN
# ============================================================
def main():
    global REBUILD_STATE
    init_db()
    trade_log.init()
    trade = make_trade_session()

    portfolio = Portfolio()

    log.info("═══ СТАРТ v6.1 (HEDGE MODE) ═══")
    log.info("Депозит: $%.0f | Макс. активных: %d | Аллокация: $%.0f | Per-complex TP: $%.2f NET",
             CONFIG["deposit"], CONFIG["max_concurrent_complexes"], ALLOCATED_CAPITAL, COMPLEX_TARGET)
    log.info("Разрешение повторного использования: %s (макс %d)",
             "ON" if CONFIG.get("allow_symbol_reuse") else "OFF", CONFIG.get("max_symbol_usage"))

    reconcile_state_with_exchange(trade, portfolio)

    all_syms = set()
    for cs in portfolio.active:
        all_syms.update(cs.all_symbols())
    for c in portfolio.reserve:
        all_syms.update(c["leg1"])
        all_syms.update(c["leg2"])
    if all_syms:
        start_ms = int(datetime.now(timezone.utc).timestamp() * 1000) - CONFIG["history_days"] * 86400_000
        ensure_month_refs(list(all_syms), start_ms)
    last_equity_snap_ts = 0.0
    prev_active_count = len(portfolio.active)
    last_rebuild_finish_ts = time.time()
    last_orphan_check = 0.0
    last_rebuild_success = True
    last_active_history_ts = 0.0

    while True:
        try:
            with PORTFOLIO_LOCK:
                has_active = len(portfolio.active) > 0
            if has_active:
                monitor_active(trade, portfolio)
                with PORTFOLIO_LOCK:
                    portfolio.save()

            if time.time() - last_active_history_ts > 300:
                with PORTFOLIO_LOCK:
                    active_syms = set()
                    for _cs in portfolio.active:
                        active_syms.update(_cs.all_symbols())
                    _rebuild_busy = REBUILD_STATE["running"]

                if not _rebuild_busy and active_syms:
                    try:
                        _market = make_market_session()
                        check_and_load_history(_market, sorted(active_syms))
                    except Exception as e:
                        log.warning("active history refresh: %s", e)
                last_active_history_ts = time.time()

            if time.time() - last_orphan_check > 300:
                try:
                    raw = get_all_positions_paginated(trade)
                    live = {p["symbol"]: p for p in raw if float(p.get("size", 0)) != 0}
                    with PORTFOLIO_LOCK:
                        tracked = set()
                        for _cs in portfolio.active:
                            tracked.update(_cs.all_symbols())

                    orphans = set(live) - tracked
                    if orphans:
                        log.warning("⚠ PERIODIC ORPHAN-CHECK: %d → %s", len(orphans), sorted(orphans))
                finally:
                    last_orphan_check = time.time()

            if time.time() - last_equity_snap_ts > 60:
                try:
                    bal = trade.get_wallet_balance(accountType="UNIFIED")["result"]["list"][0]
                    te = float(bal.get("totalEquity", 0))
                    av = float(bal.get("totalAvailableBalance", 0))
                    _pos = get_all_positions_rich(trade)
                    unrl = sum(float(p.get("unrealisedPnl", 0)) for p in _pos.values())
                    trade_log.log_equity(te, av, unrl)
                    last_equity_snap_ts = time.time()
                except Exception as e:
                    pass

            with PORTFOLIO_LOCK:
                active_count = len(portfolio.active)
                reserve_empty = not portfolio.reserve
                rebuild_running = REBUILD_STATE["running"]
                rebuild_done = REBUILD_STATE["done"]

            closed = (prev_active_count > 0 and active_count < prev_active_count)
            if closed:
                log.info("═══ ЗАКРЫТИЕ: %d→%d ═══", prev_active_count, active_count)
            prev_active_count = active_count

            if rebuild_done:
                with PORTFOLIO_LOCK:
                    new_reserve = REBUILD_STATE["reserve"]
                    err = REBUILD_STATE.get("error")
                    REBUILD_STATE["done"] = False
                    REBUILD_STATE["reserve"] = None
                    REBUILD_STATE["error"] = None
                last_rebuild_success = bool(new_reserve)

                if new_reserve:
                    with PORTFOLIO_LOCK:
                        used_now = portfolio.used_symbols()
                        portfolio.reserve = [c for c in new_reserve if
                                             not (set(c["leg1"]) & used_now or set(c["leg2"]) & used_now)]
                        portfolio.save()
                    log.info("═══ РЕЗЕРВ ОБНОВЛЁН: %d комплектов ═══", len(portfolio.reserve))
                last_rebuild_finish_ts = time.time()

                with PORTFOLIO_LOCK:
                    active_count = len(portfolio.active)
                    reserve_empty = not portfolio.reserve
                    rebuild_running = REBUILD_STATE["running"]

            need_rebuild = (closed or (reserve_empty and active_count == 0) or (
                        active_count == 0 and not rebuild_running and (time.time() - last_rebuild_finish_ts) > 1800))

            cooldown_sec = 300
            if not last_rebuild_success and (time.time() - last_rebuild_finish_ts) < cooldown_sec:
                need_rebuild = closed

            if need_rebuild:
                start_new = False
                with PORTFOLIO_LOCK:
                    if not REBUILD_STATE["running"] and not REBUILD_STATE["done"]:
                        # ★ Получаем экспозиции (направления) вместо простого set
                        used_exposures = portfolio.get_symbol_exposures()
                        
                        REBUILD_STATE["running"] = True
                        REBUILD_STATE["done"] = False
                        REBUILD_STATE["reserve"] = None
                        REBUILD_STATE["error"] = None
                        start_new = True
                if start_new:
                    # ★ Передаем экспозиции
                    t = threading.Thread(target=rebuild_worker, args=(used_exposures,), daemon=True)
                    t.start()

            with PORTFOLIO_LOCK:
                can_fill = (len(portfolio.active) < CONFIG["max_concurrent_complexes"] and portfolio.reserve)
            if can_fill:
                fill_free_slots(trade, portfolio)
                with PORTFOLIO_LOCK:
                    portfolio.save()

            with PORTFOLIO_LOCK:
                total_empty = not portfolio.active and not portfolio.reserve
                still_running = REBUILD_STATE["running"]
            if total_empty and not still_running:
                time.sleep(60)
                continue

            time.sleep(CONFIG["monitor_interval_sec"])

        except KeyboardInterrupt:
            log.info("Остановка. Состояние сохранено.")
            with PORTFOLIO_LOCK:
                portfolio.save()
            break
        except Exception as e:
            log.exception("Ошибка цикла: %s", e)
            time.sleep(30)


if __name__ == "__main__":
    main()