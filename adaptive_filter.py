#!/usr/bin/env python3
"""Daily rolling 7-day cache and deterministic filter optimizer for Bybit 5m candles."""
from __future__ import annotations

import json
import os
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import speed_hunter
from pybit.unified_trading import HTTP

CATEGORY = "linear"
INTERVAL = "5"
INTERVAL_MS = 5 * 60 * 1000
LOOKBACK_MS = 7 * 24 * 60 * 60 * 1000
OPT_DB = Path(os.getenv("FILTER_OPT_DB", "filter_optimizer.sqlite3"))
NOTIONAL_USDT = 10.0
TAKER_FEE_RATE = 0.00055
TP_SL_PCT = float(os.getenv("TP_SL_PCT", "0.10"))
MIN_TRADES = 10

# Keep the indicator periods fixed; optimize the entry thresholds.
PARAM_GRID = {
    "ema_gap_atr": (0.30, 0.50, 0.80, 1.00),
    "rsi_long_min": (50.0, 55.0, 60.0),
    "rsi_short_max": (50.0, 45.0, 40.0),
    "close_extreme": (0.15, 0.20, 0.25, 0.30),
}


@dataclass(frozen=True)
class Params:
    ema_gap_atr: float
    rsi_long_min: float
    rsi_short_max: float
    close_extreme: float

    def as_dict(self) -> dict[str, float]:
        return {
            "EMA_GAP_ATR": self.ema_gap_atr,
            "RSI_LONG_MIN": self.rsi_long_min,
            "RSI_SHORT_MAX": self.rsi_short_max,
            "CLOSE_EXTREME": self.close_extreme,
        }


class CandleStore:
    def __init__(self, path: Path = OPT_DB):
        self.db = sqlite3.connect(path, timeout=30)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS candles (
                symbol TEXT NOT NULL,
                start_ms INTEGER NOT NULL,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                PRIMARY KEY(symbol, start_ms)
            )
        """)
        self.db.execute("CREATE INDEX IF NOT EXISTS ix_candles_symbol_time ON candles(symbol, start_ms)")
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS optimizer_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        """)
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS optimization_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_day TEXT NOT NULL,
                created_at_ms INTEGER NOT NULL,
                params_json TEXT NOT NULL,
                trades INTEGER NOT NULL,
                wins INTEGER NOT NULL,
                losses INTEGER NOT NULL,
                pnl_usdt REAL NOT NULL,
                win_rate REAL NOT NULL
            )
        """)
        self.db.commit()

    def meta(self, key: str) -> str | None:
        row = self.db.execute("SELECT value FROM optimizer_meta WHERE key=?", (key,)).fetchone()
        return row[0] if row else None

    def set_meta(self, key: str, value: str) -> None:
        self.db.execute("INSERT OR REPLACE INTO optimizer_meta(key,value) VALUES (?,?)", (key, value))
        self.db.commit()

    def min_max(self, symbol: str) -> tuple[int | None, int | None]:
        row = self.db.execute("SELECT MIN(start_ms), MAX(start_ms) FROM candles WHERE symbol=?", (symbol,)).fetchone()
        return row[0], row[1]

    def upsert(self, symbol: str, rows: list[tuple[int, float, float, float, float]]) -> None:
        self.db.executemany(
            "INSERT OR REPLACE INTO candles(symbol,start_ms,open,high,low,close) VALUES (?,?,?,?,?,?)",
            [(symbol, *r) for r in rows],
        )
        self.db.commit()

    def rows(self, symbol: str, start_ms: int, end_ms: int) -> list[tuple[int, float, float, float, float]]:
        return self.db.execute(
            "SELECT start_ms,open,high,low,close FROM candles WHERE symbol=? AND start_ms BETWEEN ? AND ? ORDER BY start_ms",
            (symbol, start_ms, end_ms),
        ).fetchall()

    def save_run(self, day: str, params: Params, stats: dict[str, Any]) -> None:
        self.db.execute(
            "INSERT INTO optimization_runs(run_day,created_at_ms,params_json,trades,wins,losses,pnl_usdt,win_rate) VALUES (?,?,?,?,?,?,?,?)",
            (day, int(time.time() * 1000), json.dumps(params.as_dict()), stats["trades"], stats["wins"], stats["losses"], stats["pnl_usdt"], stats["win_rate"]),
        )
        self.set_meta("active_params", json.dumps(params.as_dict()))
        self.set_meta("last_run_day", day)
        self.db.commit()


def _fetch_range(sess: HTTP, symbol: str, start_ms: int, end_ms: int) -> list[tuple[int, float, float, float, float]]:
    result: dict[int, tuple[int, float, float, float, float]] = {}
    cursor_end = end_ms
    while cursor_end >= start_ms:
        response = sess.get_kline(category=CATEGORY, symbol=symbol, interval=INTERVAL, start=start_ms, end=cursor_end, limit=1000)
        if response.get("retCode") != 0:
            raise RuntimeError(f"get_kline {symbol}: {response}")
        raw = response["result"]["list"]
        if not raw:
            break
        for row in raw:
            ts = int(row[0])
            if start_ms <= ts <= end_ms:
                result[ts] = (ts, float(row[1]), float(row[2]), float(row[3]), float(row[4]))
        oldest = min(int(row[0]) for row in raw)
        if oldest <= start_ms or len(raw) < 1000:
            break
        cursor_end = oldest - INTERVAL_MS
        time.sleep(0.05)
    return [result[k] for k in sorted(result)]


def refresh_cache(sess: HTTP, symbols: list[str], store: CandleStore) -> dict[str, int]:
    now = int(time.time() * 1000)
    end_ms = now - (now % INTERVAL_MS) - INTERVAL_MS  # last fully completed candle
    begin_window = end_ms - LOOKBACK_MS + INTERVAL_MS
    counts: dict[str, int] = {}
    for symbol in symbols:
        lo, hi = store.min_max(symbol)
        start = begin_window if lo is None or lo > begin_window else max(hi + INTERVAL_MS, begin_window) if hi else begin_window
        if start > end_ms:
            counts[symbol] = 0
            continue
        rows = _fetch_range(sess, symbol, start, end_ms)
        if rows:
            store.upsert(symbol, rows)
        counts[symbol] = len(rows)
    # Delete data outside the rolling 7-day window.
    store.db.execute("DELETE FROM candles WHERE start_ms < ?", (begin_window,))
    store.db.commit()
    return counts


def _evaluate(rows: list[tuple[int, float, float, float, float]], p: Params) -> dict[str, Any]:
    if len(rows) < 60:
        return {"trades": 0, "wins": 0, "losses": 0, "pnl_usdt": 0.0, "win_rate": 0.0}
    ts = [r[0] for r in rows]; opens = [r[1] for r in rows]; highs = [r[2] for r in rows]; lows = [r[3] for r in rows]; closes = [r[4] for r in rows]
    fast = speed_hunter._ema(closes, speed_hunter.EMA_FAST)
    slow = speed_hunter._ema(closes, speed_hunter.EMA_SLOW)
    atr = speed_hunter._atr(highs, lows, closes, speed_hunter.ATR_PERIOD)
    rsi = speed_hunter._rsi(closes, speed_hunter.RSI_PERIOD)
    pnl = 0.0; wins = 0; losses = 0; i = max(speed_hunter.EMA_SLOW, speed_hunter.ATR_PERIOD, speed_hunter.RSI_PERIOD) + 1
    while i < len(rows) - 1:
        if any(v is None for v in (fast[i], slow[i], atr[i], rsi[i], fast[i - 1])):
            i += 1; continue
        rng = highs[i] - lows[i]
        loc = (closes[i] - lows[i]) / rng if rng else 0.5
        long_ok = fast[i] - slow[i] >= p.ema_gap_atr * atr[i] and rsi[i] >= p.rsi_long_min and loc >= 1 - p.close_extreme and fast[i] > fast[i - 1]
        short_ok = slow[i] - fast[i] >= p.ema_gap_atr * atr[i] and rsi[i] <= p.rsi_short_max and loc <= p.close_extreme and fast[i] < fast[i - 1]
        if not (long_ok or short_ok):
            i += 1; continue
        long = long_ok
        entry = opens[i + 1]
        tp = entry * (1 + TP_SL_PCT) if long else entry * (1 - TP_SL_PCT)
        sl = entry * (1 - TP_SL_PCT) if long else entry * (1 + TP_SL_PCT)
        exit_found = False
        for j in range(i + 1, len(rows)):
            hit_sl = lows[j] <= sl if long else highs[j] >= sl
            hit_tp = highs[j] >= tp if long else lows[j] <= tp
            if hit_sl or hit_tp:
                hit_tp = hit_tp and not hit_sl  # conservative if both touched in one candle
                gross = 0.10 if hit_tp else -0.10
                trade_pnl = NOTIONAL_USDT * gross - (2 * NOTIONAL_USDT * TAKER_FEE_RATE)
                pnl += trade_pnl
                if trade_pnl > 0: wins += 1
                else: losses += 1
                exit_found = True
                i = j + 1
                break
        if not exit_found:
            break
    trades = wins + losses
    return {"trades": trades, "wins": wins, "losses": losses, "pnl_usdt": round(pnl, 8), "win_rate": round(wins / trades, 6) if trades else 0.0}


def optimize(store: CandleStore, symbols: list[str]) -> tuple[Params, dict[str, Any]]:
    all_rows = {s: store.rows(s, 0, int(time.time() * 1000)) for s in symbols}
    best: tuple[tuple[float, float, float, int], Params, dict[str, Any]] | None = None
    for gap in PARAM_GRID["ema_gap_atr"]:
        for rsi_long in PARAM_GRID["rsi_long_min"]:
            for rsi_short in PARAM_GRID["rsi_short_max"]:
                for extreme in PARAM_GRID["close_extreme"]:
                    p = Params(gap, rsi_long, rsi_short, extreme)
                    total = {"trades": 0, "wins": 0, "losses": 0, "pnl_usdt": 0.0}
                    for rows in all_rows.values():
                        s = _evaluate(rows, p)
                        for k in total: total[k] += s[k]
                    total["win_rate"] = total["wins"] / total["trades"] if total["trades"] else 0.0
                    # Do not let a zero-trade/highly restrictive filter win merely
                    # because it avoided all losses.
                    eligible = total["trades"] >= MIN_TRADES
                    score = (1 if eligible else 0, total["pnl_usdt"], total["win_rate"], total["trades"])
                    if best is None or score > best[0]: best = (score, p, total)
    if best is None: raise RuntimeError("No optimizer candidates")
    return best[1], best[2]


def apply_params(params: Params) -> None:
    speed_hunter.EMA_GAP_ATR = params.ema_gap_atr
    speed_hunter.RSI_LONG_MIN = params.rsi_long_min
    speed_hunter.RSI_SHORT_MAX = params.rsi_short_max
    speed_hunter.CLOSE_EXTREME = params.close_extreme


def load_active(store: CandleStore) -> Params | None:
    raw = store.meta("active_params")
    if not raw: return None
    d = json.loads(raw)
    return Params(d["EMA_GAP_ATR"], d["RSI_LONG_MIN"], d["RSI_SHORT_MAX"], d["CLOSE_EXTREME"])


def daily_reoptimize(sess: HTTP, symbols: list[str], store: CandleStore | None = None) -> tuple[Params, dict[str, Any]] | None:
    store = store or CandleStore()
    today = time.strftime("%Y-%m-%d", time.gmtime())
    active = load_active(store)
    if store.meta("last_run_day") == today and active:
        apply_params(active)
        return None
    refresh_cache(sess, symbols, store)
    params, stats = optimize(store, symbols)
    store.save_run(today, params, stats)
    apply_params(params)
    return params, stats
