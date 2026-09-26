#!/usr/bin/env python3
"""Bybit Demo Trading bot: 5m signal + hedge-mode order + TP/SL + SQLite journal."""
from __future__ import annotations
import winsound
import ast
import json
import logging
import math
import os
import sqlite3
import threading
import time
import requests

from datetime import datetime, timedelta
from pathlib import Path
from decimal import Decimal, ROUND_DOWN
from typing import Any
from zoneinfo import ZoneInfo

from pybit.unified_trading import HTTP
from api_conf_all import DEMO_CONFIG, api_t, chat
from speed_hunter import signal
from adaptive_filter import CandleStore, daily_reoptimize, load_active, apply_params

import matplotlib
matplotlib.use("Agg", force=True)

import matplotlib.dates as mdates
import matplotlib.pyplot as plt

import pandas as pd

import struct
import wave
import tempfile

SAMPLE_RATE = 44100
BIT_DEPTH = 2  # 16-bit

MAJOR = (523.25, 659.25, 783.99)
MINOR = (523.25, 622.25, 783.99)

def make_chord_wav(freqs, duration=0.4, volume=0.25, path=None):
    if path is None:
        fd, path = tempfile.mkstemp(suffix=".wav")
        os.close(fd)

    n_samples = int(SAMPLE_RATE * duration)
    with wave.open(path, "w") as w:
        w.setnchannels(1)
        w.setsampwidth(BIT_DEPTH)
        w.setframerate(SAMPLE_RATE)
        frames = bytearray()
        for i in range(n_samples):
            t = i / SAMPLE_RATE
            # затухание в конце, чтобы не было щелчка
            envelope = 1.0 - (i / n_samples) ** 2
            sample = sum(math.sin(2 * math.pi * f * t) for f in freqs) / len(freqs)
            value = int(sample * volume * envelope * 32767)
            frames += struct.pack("<h", value)
        w.writeframes(bytes(frames))
    winsound.PlaySound(path, winsound.SND_FILENAME)
    return path

CATEGORY = "linear"
SETTLE_COIN = "USDT"
SYMBOL_FILE = Path(os.getenv("SYMBOL_FILE", "sym.txt"))
DB_PATH = Path(os.getenv("TRADER_DB", "bybit_signal_trader.sqlite3"))
NOTIONAL_USDT = Decimal(os.getenv("POSITION_NOTIONAL_USDT", "10"))
TP_SL_PCT = Decimal(os.getenv("TP_SL_PCT", "0.1"))
ROI_TARGET_PCT = Decimal(os.getenv("ROI_TARGET_PCT", "0.01"))
DEMO_TAKER_FEE_RATE = Decimal(os.getenv(
    "DEMO_TAKER_FEE_RATE", str(DEMO_CONFIG.get("taker_fee_rate", "0.00055"))
))
INITIAL_CYCLE_BALANCE_USDT = Decimal("200")
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "60"))
ENTRY_COOLDOWN_SECONDS = int(os.getenv("ENTRY_COOLDOWN_SECONDS", "300"))
ROI_CLOSE_RETRY_ROUNDS = int(os.getenv("ROI_CLOSE_RETRY_ROUNDS", "3"))
ROI_CLOSE_RETRY_DELAY_SECONDS = float(os.getenv("ROI_CLOSE_RETRY_DELAY_SECONDS", "3"))
ROI_POST_CLOSE_PAUSE_SECONDS = int(os.getenv("ROI_POST_CLOSE_PAUSE_SECONDS", "300"))
ORDER_TAG = os.getenv("ORDER_TAG", "speed")
FILTER_OPT_DB = Path(os.getenv("FILTER_OPT_DB", "filter_optimizer.sqlite3"))
OPTIMIZATION_HOUR_MSK = int(os.getenv("OPTIMIZATION_HOUR_MSK", "1"))
MSK = ZoneInfo("Europe/Moscow")
optimization_active = threading.Event()
EXCLUDED_SYMBOLS = {
    item.strip().upper()
    for item in os.getenv("EXCLUDED_SYMBOLS", "BTCUSDT,ETHUSDT").split(",")
    if item.strip()
}

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("speed-hunter-trader")


def check(response: dict[str, Any], operation: str) -> dict[str, Any]:
    if response.get("retCode") != 0:
        raise RuntimeError(f"{operation} failed: {response}")
    return response


def create_demo_session() -> HTTP:
    kwargs = {
        "testnet": DEMO_CONFIG.get("testnet", False),
        "demo": DEMO_CONFIG.get("demo", True),
        "api_key": DEMO_CONFIG["api_key"],
        "api_secret": DEMO_CONFIG["api_secret"],
        "timeout": DEMO_CONFIG.get("timeout", 20),
    }
    sess = HTTP(**kwargs)
    if DEMO_CONFIG.get("use_proxy") and DEMO_CONFIG.get("proxy"):
        proxy = DEMO_CONFIG["proxy"]
        proxy_url = proxy.get("https") or proxy.get("http")
        if proxy_url:
            sess.client.proxies.update({"http": proxy_url, "https": proxy_url})
    log.info("Bybit session: demo=%s testnet=%s", kwargs["demo"], kwargs["testnet"])
    return sess


def load_symbols() -> list[str]:
    global NOTIONAL_USDT
    value = ast.literal_eval(SYMBOL_FILE.read_text())
    if not isinstance(value, list):
        raise ValueError(f"Invalid symbol list: {SYMBOL_FILE}")
    symbols = [str(x).upper() for x in value]
    if NOTIONAL_USDT <= Decimal("10"):
        removed = [symbol for symbol in symbols if symbol in EXCLUDED_SYMBOLS]
        symbols = [symbol for symbol in symbols if symbol not in EXCLUDED_SYMBOLS]
        if removed:
            log.info("Excluded for $%s notional: %s", NOTIONAL_USDT, ", ".join(removed))
    return symbols

class Journal:
    def __init__(self, path: Path):
        self.db = sqlite3.connect(path, timeout=30)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")

        self.db.execute("""CREATE TABLE IF NOT EXISTS bank
                                            (id INTEGER PRIMARY KEY AUTOINCREMENT,
                                            bank_all REAL NOT NULL DEFAULT 0.0
                                            )""")

        self.db.execute("""
            CREATE TABLE IF NOT EXISTS positions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                position_idx INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'OPEN',
                entry_order_id TEXT,
                entry_time_ms INTEGER NOT NULL,
                close_time_ms INTEGER,
                entry_price REAL,
                exit_price REAL,
                tp_price REAL,
                sl_price REAL,
                qty REAL NOT NULL,
                notional_usdt REAL NOT NULL,
                margin_usdt REAL NOT NULL,
                leverage REAL,
                signal_candle_ms INTEGER NOT NULL,
                pnl_usdt REAL,
                close_status TEXT,
                close_pnl_json TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)
        self.db.execute("CREATE INDEX IF NOT EXISTS ix_pos_open ON positions(symbol, status)")
        self.db.execute("CREATE INDEX IF NOT EXISTS ix_pos_signal ON positions(symbol, signal_candle_ms)")
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS disabled_symbols (
                symbol TEXT PRIMARY KEY,
                reason TEXT NOT NULL,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS trader_state (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)

        self.db.execute("""
            CREATE TABLE IF NOT EXISTS wallet (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                value TEXT NOT NULL,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)

        self.db.execute("""
            CREATE TABLE IF NOT EXISTS roi_cycles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                start_time_ms INTEGER NOT NULL,
                end_time_ms INTEGER,
                start_balance_usdt TEXT NOT NULL,
                end_balance_usdt TEXT,
                status TEXT NOT NULL CHECK(status IN ('RUNNING', 'CLOSING', 'COMPLETED')),
                start_source TEXT NOT NULL DEFAULT 'tracked',
                close_started_at_ms INTEGER,
                close_attempts INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)
        self.db.execute("CREATE INDEX IF NOT EXISTS ix_roi_cycles_status ON roi_cycles(status, start_time_ms)")

        self.db.execute(
            "INSERT OR IGNORE INTO trader_state(key, value) VALUES ('cycle_start_balance', ?)",
            (str(INITIAL_CYCLE_BALANCE_USDT),),
        )

        # The bank is a singleton row. Do not use DEFAULT VALUES here: with
        # AUTOINCREMENT that creates id=2, id=3, ... on every bot restart.
        self.db.execute("INSERT OR IGNORE INTO bank(id, bank_all) VALUES (1, 0.0)")
        self.db.execute("""
            CREATE TRIGGER IF NOT EXISTS bank_only_id_one
            BEFORE INSERT ON bank
            WHEN NEW.id != 1
            BEGIN
                SELECT RAISE(ABORT, 'bank table only allows id=1');
            END
        """)

        self.db.commit()
        self._bootstrap_active_roi_cycle()

    def bank_all(self) -> float:
        row = self.db.execute("SELECT bank_all FROM bank WHERE id = 1").fetchone()
        return float(row[0]) if row else 0.0

    def bank_info(self, size: float | None = None) -> int | None:
        """Read/adjust the SL bank; participate in an existing close transaction."""
        row = self.db.execute("SELECT bank_all FROM bank WHERE id = 1").fetchone()
        if row is None:
            raise RuntimeError("Bank row id=1 is missing")
        value = float(row[0])

        if size is None or size == 0:
            return max(math.ceil(value / 10), 1)

        new_value = max(value + size, 0.0)
        if size > 0:
            pass
            #print(f"Плюсуем к банку: {size} теперь там: {new_value}")
        else:
            pass
            #print(f"Минусуем из банка: {size} теперь там: {new_value}")

        owns_transaction = not self.db.in_transaction
        try:
            self.db.execute("UPDATE bank SET bank_all = ? WHERE id = 1", (new_value,))
            if owns_transaction:
                self.db.commit()
        except Exception:
            if owns_transaction:
                self.db.rollback()
            raise
        return None

    def state(self, key: str, default: str | None = None) -> str | None:
        row = self.db.execute("SELECT value FROM trader_state WHERE key=?", (key,)).fetchone()
        return row[0] if row else default

    def set_state(self, key: str, value: str) -> None:
        self.db.execute(
            "INSERT INTO trader_state(key, value, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=CURRENT_TIMESTAMP",
            (key, value),
        )
        self.db.commit()

    @staticmethod
    def _state_timestamp_ms(value: str | None) -> int | None:
        if not value:
            return None
        try:
            parsed = datetime.fromisoformat(value)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=ZoneInfo("UTC"))
            return int(parsed.timestamp() * 1000)
        except (TypeError, ValueError, OverflowError):
            return None

    def _bootstrap_active_roi_cycle(self) -> None:
        """Migrate the current legacy cycle once; prior cycle starts were not recorded."""
        active_id = self.state("active_roi_cycle_id")
        if active_id and active_id != "0":
            try:
                row = self.db.execute(
                    "SELECT status FROM roi_cycles WHERE id=?", (int(active_id),)
                ).fetchone()
            except (TypeError, ValueError):
                row = None
            if row and row["status"] in ("RUNNING", "CLOSING"):
                return

        row = self.db.execute(
            "SELECT value, updated_at FROM trader_state WHERE key='cycle_start_balance'"
        ).fetchone()
        balance = row["value"] if row else str(INITIAL_CYCLE_BALANCE_USDT)
        start_ms = self._state_timestamp_ms(row["updated_at"] if row else None)
        if start_ms is None:
            start_ms = int(time.time() * 1000)
        pending = self.state("roi_close_pending", "0") == "1"
        pending_row = self.db.execute(
            "SELECT updated_at FROM trader_state WHERE key='roi_close_pending'"
        ).fetchone()
        close_started_ms = self._state_timestamp_ms(pending_row["updated_at"] if pending_row else None) if pending else None
        source = "legacy_state_timestamp" if row and row["updated_at"] else "startup"

        self.db.execute("BEGIN IMMEDIATE")
        try:
            cursor = self.db.execute(
                "INSERT INTO roi_cycles(start_time_ms, start_balance_usdt, status, start_source, close_started_at_ms) "
                "VALUES (?, ?, ?, ?, ?)",
                (start_ms, balance, "CLOSING" if pending else "RUNNING", source, close_started_ms),
            )
            cycle_id = cursor.lastrowid
            self.db.execute(
                "INSERT INTO trader_state(key, value, updated_at) VALUES ('active_roi_cycle_id', ?, CURRENT_TIMESTAMP) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=CURRENT_TIMESTAMP",
                (str(cycle_id),),
            )
            self.db.commit()
        except Exception:
            self.db.rollback()
            raise

    def active_roi_cycle_id(self) -> int | None:
        raw = self.state("active_roi_cycle_id")
        if not raw or raw == "0":
            return None
        try:
            return int(raw)
        except ValueError:
            return None

    def note_roi_close_attempt(self) -> None:
        cycle_id = self.active_roi_cycle_id()
        if cycle_id is None:
            self._bootstrap_active_roi_cycle()
            cycle_id = self.active_roi_cycle_id()
        if cycle_id is not None:
            self.db.execute(
                "UPDATE roi_cycles SET status='CLOSING', close_attempts=close_attempts+1 "
                "WHERE id=? AND status IN ('RUNNING', 'CLOSING')",
                (cycle_id,),
            )
            self.db.commit()

    def entry_pause_remaining_seconds(self, now_ms: int | None = None) -> int:
        raw = self.state("roi_entry_pause_until_ms", "0") or "0"
        try:
            pause_until_ms = int(raw)
        except ValueError:
            return 0
        now_ms = now_ms or int(time.time() * 1000)
        return max(0, math.ceil((pause_until_ms - now_ms) / 1000))

    def record_wallet_equity(self, equity: Decimal) -> None:
        """Append one balance snapshot to the wallet history table."""
        self.db.execute(
            "INSERT INTO wallet (value, updated_at) VALUES (?, CURRENT_TIMESTAMP)",
            (str(equity),),
        )
        self.db.commit()

    def start_roi_close(self) -> None:
        winsound.Beep(500, 500)
        # Keep positions OPEN until Bybit confirms closure; this marker lets
        # delayed PnL reconciliation preserve the ROI reason.
        self.db.execute("UPDATE positions SET close_status='ROI' WHERE status='OPEN'")
        cycle_id = self.active_roi_cycle_id()
        if cycle_id is not None:
            self.db.execute(
                "UPDATE roi_cycles SET status='CLOSING', close_started_at_ms=COALESCE(close_started_at_ms, ?) "
                "WHERE id=? AND status IN ('RUNNING', 'CLOSING')",
                (int(time.time() * 1000), cycle_id),
            )
        self.db.execute(
            "INSERT INTO trader_state(key, value, updated_at) VALUES ('roi_close_pending', '1', CURRENT_TIMESTAMP) "
            "ON CONFLICT(key) DO UPDATE SET value='1', updated_at=CURRENT_TIMESTAMP"
        )
        self.db.commit()

    def finish_cycle(self, balance: Decimal) -> None:
        winsound.MessageBeep(winsound.MB_ICONHAND)
        finished_at_ms = int(time.time() * 1000)
        pause_until_ms = finished_at_ms + ROI_POST_CLOSE_PAUSE_SECONDS * 1000
        self.db.execute("BEGIN IMMEDIATE")
        try:
            cycle_id = self.active_roi_cycle_id()
            if cycle_id is not None:
                self.db.execute(
                    "UPDATE roi_cycles SET status='COMPLETED', end_time_ms=?, end_balance_usdt=? "
                    "WHERE id=? AND status IN ('RUNNING', 'CLOSING')",
                    (finished_at_ms, str(balance), cycle_id),
                )
            self.db.execute(
                "INSERT INTO trader_state(key, value, updated_at) VALUES ('cycle_start_balance', ?, CURRENT_TIMESTAMP) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=CURRENT_TIMESTAMP",
                (str(balance),),
            )
            self.db.execute(
                "INSERT INTO trader_state(key, value, updated_at) VALUES ('roi_close_pending', '0', CURRENT_TIMESTAMP) "
                "ON CONFLICT(key) DO UPDATE SET value='0', updated_at=CURRENT_TIMESTAMP"
            )
            self.db.execute(
                "INSERT INTO trader_state(key, value, updated_at) VALUES ('roi_entry_pause_until_ms', ?, CURRENT_TIMESTAMP) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=CURRENT_TIMESTAMP",
                (str(pause_until_ms),),
            )
            next_cycle = self.db.execute(
                "INSERT INTO roi_cycles(start_time_ms, start_balance_usdt, status, start_source) "
                "VALUES (?, ?, 'RUNNING', 'tracked')",
                (finished_at_ms, str(balance)),
            )
            self.db.execute(
                "INSERT INTO trader_state(key, value, updated_at) VALUES ('active_roi_cycle_id', ?, CURRENT_TIMESTAMP) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=CURRENT_TIMESTAMP",
                (str(next_cycle.lastrowid),),
            )
            # Reset the loss bank only after all positions were confirmed closed.
            self.db.execute("UPDATE bank SET bank_all = 0 WHERE id = 1")
            self.db.commit()
        except Exception:
            self.db.rollback()
            raise

        winsound.Beep(500, 500)

    def open_symbols(self) -> set[str]:
        rows = self.db.execute("SELECT DISTINCT symbol FROM positions WHERE status='OPEN'").fetchall()
        return {r[0] for r in rows}

    def cooldown_symbols(self, now_ms: int, cooldown_ms: int) -> set[str]:
        rows = self.db.execute(
            "SELECT DISTINCT symbol FROM positions "
            "WHERE status='CLOSED' AND close_time_ms IS NOT NULL AND close_time_ms > ?",
            (now_ms - cooldown_ms,),
        ).fetchall()
        return {r[0] for r in rows}

    def signal_used(self, symbol: str, candle_ms: int) -> bool:
        return self.db.execute(
            "SELECT 1 FROM positions WHERE symbol=? AND signal_candle_ms=? LIMIT 1",
            (symbol, candle_ms),
        ).fetchone() is not None

    def disabled_symbols(self) -> set[str]:
        return {row[0] for row in self.db.execute("SELECT symbol FROM disabled_symbols")}

    def disable_symbol(self, symbol: str, reason: str) -> None:
        self.db.execute(
            "INSERT OR REPLACE INTO disabled_symbols(symbol, reason, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP)",
            (symbol, reason),
        )
        self.db.commit()

    def insert_open(self, values: dict[str, Any], bank_spend: float = 0.0) -> None:
        fields = ",".join(values)
        marks = ",".join("?" for _ in values)
        self.db.execute("BEGIN IMMEDIATE")
        try:
            self.db.execute(f"INSERT INTO positions ({fields}) VALUES ({marks})", tuple(values.values()))
            if bank_spend > 0:
                self.bank_info(-bank_spend)
            self.db.commit()
        except Exception:
            self.db.rollback()
            raise

    def open_rows(self):
        return self.db.execute("SELECT * FROM positions WHERE status='OPEN' ORDER BY entry_time_ms").fetchall()

    def close(self, row_id: int, close_time_ms: int, exit_price: float | None,
              pnl: float, status: str, payload: dict[str, Any], bank_add: float = 0.0) -> None:
        self.db.execute("BEGIN IMMEDIATE")
        try:
            cursor = self.db.execute("""
                UPDATE positions SET status='CLOSED', close_time_ms=?, exit_price=?, pnl_usdt=?,
                close_status=?, close_pnl_json=? WHERE id=? AND status='OPEN'
            """, (close_time_ms, exit_price, pnl, status,
                  json.dumps(payload, ensure_ascii=False), row_id))
            # Bank update and OPEN -> CLOSED transition commit atomically.
            # A repeated reconciliation cannot add the same SL a second time.
            if cursor.rowcount and bank_add:
                self.bank_info(bank_add)
            self.db.commit()
        except Exception:
            self.db.rollback()
            raise


def round_down(value: Decimal, step: Decimal) -> Decimal:
    return (value / step).to_integral_value(rounding=ROUND_DOWN) * step


def rules(sess: HTTP, symbol: str) -> tuple[Decimal, Decimal, Decimal]:
    info = check(sess.get_instruments_info(category=CATEGORY, symbol=symbol), "get_instruments_info")["result"]["list"][0]
    return (Decimal(info["lotSizeFilter"]["qtyStep"]), Decimal(info["lotSizeFilter"]["minOrderQty"]), Decimal(info["priceFilter"]["tickSize"]))


def positions(sess: HTTP) -> dict[str, dict[int, dict[str, Any]]]:
    result: dict[str, dict[int, dict[str, Any]]] = {}
    cursor = None
    while True:
        params = {"category": CATEGORY, "settleCoin": SETTLE_COIN, "limit": 200}
        if cursor:
            params["cursor"] = cursor
        response = check(sess.get_positions(**params), "get_positions")
        for p in response["result"]["list"]:
            if Decimal(str(p.get("size", "0"))) > 0:
                result.setdefault(p["symbol"], {})[int(p.get("positionIdx", 0))] = p
        cursor = response["result"].get("nextPageCursor")
        if not cursor:
            break
    return result


def closed_pnl(sess: HTTP, symbol: str, entry_ms: int) -> dict[str, Any] | None:
    response = check(sess.get_closed_pnl(
        category=CATEGORY, symbol=symbol, startTime=entry_ms, limit=50
    ), "get_closed_pnl")
    candidates = [p for p in response["result"]["list"] if int(p.get("updatedTime") or p.get("createdTime") or 0) >= entry_ms - 120_000]
    if candidates:
        return max(candidates, key=lambda p: int(p.get("updatedTime") or p.get("createdTime") or 0))

    # Some demo-account responses expose the fill immediately in executions,
    # while get_closed_pnl is delayed. Aggregate close fills as a fallback.
    executions = check(sess.get_executions(
        category=CATEGORY, symbol=symbol, startTime=entry_ms, limit=100
    ), "get_executions")["result"]["list"]
    executions = [e for e in executions if int(e.get("execTime") or 0) >= entry_ms - 120_000]
    if not executions:
        return None
    qty = sum(float(e.get("execQty") or 0) for e in executions)
    weighted_price = sum(float(e.get("execPrice") or 0) * float(e.get("execQty") or 0) for e in executions)
    return {
        "updatedTime": max(int(e.get("execTime") or 0) for e in executions),
        "avgExitPrice": weighted_price / qty if qty else None,
        "closedPnl": sum(float(e.get("closedPnl") or 0) for e in executions),
        "source": "execution_list",
        "executions": executions,
    }


def reconcile(sess: HTTP, journal: Journal, live: dict[str, dict[int, dict[str, Any]]]) -> None:
    for row in journal.open_rows():
        # In hedge mode a symbol may have two independent legs. A closed
        # Long/Short leg must be reconciled even if the opposite leg remains open.
        position_idx = int(row["position_idx"])
        live_leg = live.get(row["symbol"], {}).get(position_idx)
        if live_leg:
            continue
        payload = closed_pnl(sess, row["symbol"], row["entry_time_ms"])
        if not payload:
            log.warning("%s disappeared, but get_closed_pnl has not updated yet", row["symbol"])
            continue
        exit_price = float(payload.get("avgExitPrice") or 0) or None
        pnl = float(payload.get("closedPnl") or 0)
        tp = float(row["tp_price"]); sl = float(row["sl_price"])
        if row["close_status"] == "ROI":
            status = "ROI"
        elif exit_price is not None:
            tolerance = max(abs(tp), abs(sl)) * 0.002
            if abs(exit_price - tp) <= tolerance:
                status = "TP"
            elif abs(exit_price - sl) <= tolerance:
                status = "SL"
            else:
                status = "TP" if pnl >= 0 else "SL"
        else:
            status = "TP" if pnl >= 0 else "SL"
        close_time = int(payload.get("updatedTime") or payload.get("createdTime") or time.time() * 1000)
        sl_bank_add = float(row["notional_usdt"]) * 2 if status == "SL" else 0.0
        journal.close(row["id"], close_time, exit_price, pnl, status, payload,
                      bank_add=sl_bank_add)
        if sl_bank_add:
            log.info("SL bank updated for %s: +%s (2 × notional %s)",
                     row["symbol"], sl_bank_add, row["notional_usdt"])
        if abs(pnl) < 1e-12:
            log.warning("CLOSED %s %s status=%s with zero realized PnL; verify Bybit execution history", row["symbol"], row["side"], status)
        log.info("CLOSED %s %s status=%s pnl=%s", row["symbol"], row["side"], status, pnl)
        # if pnl > 0:
        #     make_chord_wav(MAJOR, duration=0.6) #MINOR
        # else:
        #     make_chord_wav(MINOR, duration=0.6)


def wallet_equity(sess: HTTP) -> Decimal:
    response = check(sess.get_wallet_balance(accountType="UNIFIED", coin=SETTLE_COIN), "get_wallet_balance")
    wallets = response["result"]["list"]
    if not wallets or wallets[0].get("totalEquity") in (None, ""):
        raise RuntimeError("Bybit response does not contain unified-account totalEquity")
    return Decimal(str(wallets[0]["totalEquity"]))


def manage_roi_cycle(sess: HTTP, journal: Journal) -> bool:
    global NOTIONAL_USDT
    """Close every position at +ROI_TARGET_PCT; persist equity as the next cycle base."""
    pending = journal.state("roi_close_pending", "0") == "1"
    start_balance = Decimal(journal.state("cycle_start_balance", str(INITIAL_CYCLE_BALANCE_USDT)))

    if not pending:
        equity = wallet_equity(sess)
        threading.Thread(target=plot_wallet_history).start()
        journal.record_wallet_equity(equity)
        target_balance = (start_balance * (Decimal("1") + ROI_TARGET_PCT)) + estimate_market_close_fee(sess)
        if equity < target_balance:
            print(equity, target_balance)
            return False
        log.info(
            "Cycle ROI target reached: equity=%s start=%s target=%s (+%s%%)",
            equity, start_balance, target_balance, ROI_TARGET_PCT * 100,
        )
        journal.start_roi_close()
        pending = True

    # Re-read exchange state before every round and retry only legs that remain.
    # Keep the pending flag set across API failures and bot restarts.
    remaining: dict[str, dict[int, dict[str, Any]]] = {}
    for attempt in range(1, max(ROI_CLOSE_RETRY_ROUNDS, 1) + 1):
        live = positions(sess)
        if not live:
            remaining = {}
            break
        journal.note_roi_close_attempt()
        remaining = live
        log.info(
            "ROI close verification round %d/%d: %d live legs",
            attempt, max(ROI_CLOSE_RETRY_ROUNDS, 1), sum(len(legs) for legs in live.values()),
        )
        for symbol, legs in live.items():
            for position_idx, position in legs.items():
                qty = str(position.get("size", "0"))
                if Decimal(qty) <= 0:
                    continue
                close_side = "Sell" if position.get("side") == "Buy" else "Buy"
                try:
                    check(sess.place_order(
                        category=CATEGORY, symbol=symbol, side=close_side,
                        positionIdx=position_idx, orderType="Market", qty=qty,
                        timeInForce="IOC", reduceOnly=True,
                        orderLinkId=f"roi-{symbol.lower()[:12]}-{int(time.time() * 1000)}-{position_idx}-{attempt}",
                    ), f"close_roi_position({symbol})")
                    log.info("ROI close submitted %s side=%s idx=%s qty=%s attempt=%d",
                             symbol, close_side, position_idx, qty, attempt)
                except Exception as exc:
                    log.warning("ROI close request failed for %s idx=%s attempt=%d: %s",
                                symbol, position_idx, attempt, exc)
        time.sleep(max(0.0, ROI_CLOSE_RETRY_DELAY_SECONDS))

    # A fresh exchange query is authoritative; then reconcile any legs that
    # disappeared from Bybit so the local journal catches up with realized PnL.
    remaining = positions(sess)
    reconcile(sess, journal, remaining)
    remaining = positions(sess)
    if remaining or journal.open_rows():
        log.warning("ROI close is still pending: live_positions=%d journal_open=%d",
                    sum(len(legs) for legs in remaining.values()), len(journal.open_rows()))
        return True

    new_start_balance = wallet_equity(sess)
    NOTIONAL_USDT = Decimal(os.getenv("POSITION_NOTIONAL_USDT", str(new_start_balance * Decimal("0.05"))))
    journal.finish_cycle(new_start_balance)
    log.info("ROI cycle complete; next cycle start balance saved: %s USDT", new_start_balance)
    return True


def open_position(sess: HTTP, journal: Journal, symbol: str, signal_side: str, candle_ms: int) -> None:
    global NOTIONAL_USDT
    side = "Buy" if signal_side == "Long" else "Sell"
    position_idx = 1 if side == "Buy" else 2  # hedge-mode: 1 long, 2 short
    if symbol in journal.cooldown_symbols(int(time.time() * 1000), ENTRY_COOLDOWN_SECONDS * 1000):
        log.info("SKIP %s: entry cooldown after recent close is active", symbol)
        return
    live = positions(sess)
    if symbol in live:
        log.info("SKIP %s: a position for this symbol is already open", symbol)
        return
    if journal.signal_used(symbol, candle_ms):
        return
    bank_balance = Decimal(str(journal.bank_all()))
    dop_usdt = bank_balance / Decimal("10") if bank_balance > Decimal("10") else Decimal("0")
    position_notional = NOTIONAL_USDT + dop_usdt
    ticker = check(sess.get_tickers(category=CATEGORY, symbol=symbol), "get_tickers")["result"]["list"][0]
    entry = Decimal(ticker["lastPrice"])
    qty_step, min_qty, tick_size = rules(sess, symbol)
    qty = round_down(position_notional / entry, qty_step)
    if qty <= 0 or qty < min_qty:
        log.warning("SKIP %s: qty %s < minOrderQty %s", symbol, qty, min_qty)
        return
    tp = round_down(entry * (1 + TP_SL_PCT if side == "Buy" else 1 - TP_SL_PCT), tick_size)
    sl = round_down(entry * (1 - TP_SL_PCT if side == "Buy" else 1 + TP_SL_PCT), tick_size)
    now = int(time.time() * 1000)
    response = check(sess.place_order(
        category=CATEGORY, symbol=symbol, side=side, positionIdx=position_idx,
        orderType="Market", qty=format(qty, "f"), timeInForce="IOC",
        takeProfit=format(tp, "f"), stopLoss=format(sl, "f"),
        tpTriggerBy="LastPrice", slTriggerBy="LastPrice", tpslMode="Full",
        tpOrderType="Market", slOrderType="Market",
        orderLinkId=f"{ORDER_TAG}-{symbol.lower()}-{now}",
    ), "place_order")
    time.sleep(0.25)
    actual = positions(sess).get(symbol, {}).get(position_idx, {})
    actual_entry = float(actual.get("avgPrice") or entry)
    actual_qty = float(actual.get("size") or qty)
    leverage = float(actual.get("leverage") or 20)
    journal.insert_open({
        "symbol": symbol, "side": signal_side, "position_idx": position_idx, "status": "OPEN",
        "entry_order_id": response["result"]["orderId"], "entry_time_ms": now,
        "entry_price": actual_entry, "tp_price": float(tp), "sl_price": float(sl),
        "qty": actual_qty, "notional_usdt": float(position_notional),
        "margin_usdt": float(position_notional / Decimal(str(leverage))), "leverage": leverage,
        "signal_candle_ms": candle_ms,
    }, bank_spend=float(dop_usdt))
    log.info("OPENED %s %s idx=%s qty=%s notional=%s (base=%s + bank=%s) TP=%s SL=%s",
             symbol, signal_side, position_idx, actual_qty, position_notional,
             NOTIONAL_USDT, dop_usdt, tp, sl)


def run_once(sess: HTTP, journal: Journal, symbols: list[str], disabled: set[str]) -> None:
    live = positions(sess)
    reconcile(sess, journal, live)
    try:
        if manage_roi_cycle(sess, journal):
            return
    except Exception:
        log.exception("ROI-cycle management failed; new entries paused")
        return
    pause_remaining = journal.entry_pause_remaining_seconds()
    if pause_remaining > 0:
        log.info("Post-ROI pause active: new entries resume in %d seconds", pause_remaining)
        return
    if optimization_active.is_set():
        log.info("Optimization window active: monitoring positions; new entries paused")
        return
    occupied = set(live) | journal.open_symbols()
    cooldown = journal.cooldown_symbols(
        int(time.time() * 1000), ENTRY_COOLDOWN_SECONDS * 1000
    )
    for symbol in symbols:
        if symbol in occupied or symbol in disabled or symbol in cooldown:
            continue
        try:
            result = signal(symbol, session=sess)
            if result.ok and result.side and result.candle_start_ms is not None:
                open_position(sess, journal, symbol, result.side, result.candle_start_ms)
                occupied.add(symbol)
        except Exception as exc:
            text = str(exc)
            if (
                "110007" in text
                or "ab not enough for new order" in text.lower()
                or "available balance is insufficient" in text.lower()
            ):
                log.warning(
                    "SKIP %s: insufficient available balance for entry; "
                    "order was rejected and the bot will continue with the next symbol. Details: %s",
                    symbol, text[:250],
                )
            elif (
                "110125" in text
                or "110126" in text
                or "Crude Oil Trading Terms" in text
                or "sign the required agreement" in text
            ):
                disabled.add(symbol)
                journal.disable_symbol(symbol, f"Bybit contract agreement required: {text[:180]}")
                log.error("DISABLED %s: Bybit requires the account agreement for this contract", symbol)
            else:
                log.exception("Processing failed for %s", symbol)


def optimizer_worker(symbols: list[str]) -> None:
    """Run the rolling 7-day optimization once per day at 01:00 Moscow time."""
    store = CandleStore(FILTER_OPT_DB)
    active = load_active(store)
    if active:
        apply_params(active)
        log.info("Loaded saved filter parameters: %s", active.as_dict())
    while True:
        now = datetime.now(MSK)
        today = now.date()
        target = datetime(today.year, today.month, today.day, OPTIMIZATION_HOUR_MSK, tzinfo=MSK)
        if now < target:
            sleep_seconds = (target - now).total_seconds()
            time.sleep(max(30.0, sleep_seconds))
            continue
        run_day = today.isoformat()
        if store.meta("last_run_day") == run_day:
            tomorrow = target + timedelta(days=1)
            time.sleep(max(30.0, (tomorrow - now).total_seconds()))
            continue
        optimization_active.set()
        try:
            log.info("Starting daily filter optimization at 01:00 MSK")
            result = daily_reoptimize(create_demo_session(), symbols, store, run_day=run_day)
            if result:
                params, stats = result
                log.info("Daily filter update: params=%s stats=%s", params.as_dict(), stats)
        except Exception:
            log.exception("Daily filter optimization failed; existing parameters remain active")
            time.sleep(300.0)
        finally:
            optimization_active.clear()
            log.info("Daily filter optimization finished; new entries resumed")

def estimate_market_close_fee(sess: HTTP) -> Decimal:
    """Estimate taker fees (USDT) to market-close every open linear position.

    Bybit calculates linear-contract trade fees from executed order value and
    the account's taker rate. Mark price is used as the execution-price
    estimate; the actual fee can differ with market movement/slippage.
    """
    live = positions(sess)
    total_fee = Decimal("0")
    use_configured_rate = bool(DEMO_CONFIG.get("demo", False) or DEMO_CONFIG.get("testnet", False))
    fee_rates: dict[str, Decimal] = {}

    for symbol, legs in live.items():
        if symbol not in fee_rates:
            if use_configured_rate:
                # Get Fee Rate is not listed among supported Demo Trading APIs.
                fee_rates[symbol] = DEMO_TAKER_FEE_RATE
            else:
                fee_response = check(
                    sess.get_fee_rates(category=CATEGORY, symbol=symbol),
                    f"get_fee_rates({symbol})",
                )
                fee_rows = fee_response.get("result", {}).get("list", [])
                fee_row = next((row for row in fee_rows if row.get("symbol") == symbol), None)
                if fee_row is None:
                    raise RuntimeError(f"Bybit returned no fee rate for {symbol}")
                fee_rates[symbol] = Decimal(str(fee_row["takerFeeRate"]))
        taker_rate = fee_rates[symbol]

        for position in legs.values():
            qty = Decimal(str(position.get("size", "0")))
            if qty <= 0:
                continue
            mark_price = Decimal(str(position.get("markPrice") or "0"))
            if mark_price <= 0:
                raise RuntimeError(f"No valid markPrice for open position {symbol}")
            total_fee += qty * mark_price * taker_rate

    return total_fee

def send_or_update_photo():

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
                #print("Фото успешно обновлено.")
                return
        except Exception as e:
            pass
            return
            #print(f"Ошибка при обновлении фото: {e}")


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
                    #print("Новое фото отправлено, ID сохранён.")
                else:
                    pass
                    #print("Ошибка отправки: ответ Telegram не содержит 'ok'.")
        except Exception as e:
            pass
            #print(f"Ошибка при отправке фото: {e}")


def plot_wallet_history(
    db_path=DB_PATH,
    output_path="live_graf.png",
):

    db_path = Path(db_path)
    output_path = Path(output_path)

    if not db_path.is_file():
        raise FileNotFoundError(f"Файл базы данных не найден: {db_path}")

    with sqlite3.connect(db_path) as conn:
        df = pd.read_sql_query(
            """
            SELECT value, updated_at
            FROM wallet
            WHERE value IS NOT NULL
            ORDER BY id
            """,
            conn,
        )

    if df.empty:
        raise ValueError("В таблице wallet пока нет данных для графика")

    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce")
    df = df.dropna(subset=["value", "updated_at"])

    if df.empty:
        raise ValueError("В таблице wallet нет корректных значений баланса или времени")

    output_path.parent.mkdir(parents=True, exist_ok=True)

    plt.rcParams["font.family"] = "DejaVu Sans"  # поддерживает кириллицу
    fig, ax = plt.subplots(figsize=(12, 6))

    ax.plot(
        df["updated_at"],
        df["value"],
        color="blue",
        linewidth=1.5,
        label="Баланс",
    )

    ax.set_title("История баланса")
    ax.set_xlabel("Время")
    ax.set_ylabel("Баланс, USDT")
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d %H:%M"))
    ax.grid(True, alpha=0.3)
    ax.legend()

    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    send_or_update_photo()

    return output_path

def main() -> None:
    global NOTIONAL_USDT

    sess = create_demo_session()
    equity = wallet_equity(sess)
    NOTIONAL_USDT = Decimal(os.getenv("POSITION_NOTIONAL_USDT", str(equity * Decimal("0.05"))))
    #print(equity, NOTIONAL_USDT)
    journal = Journal(DB_PATH)
    symbols = load_symbols()
    disabled = journal.disabled_symbols()
    if disabled:
        log.warning("Disabled symbols loaded from SQLite: %s", ", ".join(sorted(disabled)))
    threading.Thread(target=optimizer_worker, args=(symbols,), name="daily-filter-optimizer", daemon=True).start()
    log.info("Started: symbols=%d notional=$%s TP/SL=%s%% cycle ROI=+%s%% initial cycle balance=$%s poll=%ss",
             len(symbols), NOTIONAL_USDT, TP_SL_PCT * 100, ROI_TARGET_PCT * 100,
             INITIAL_CYCLE_BALANCE_USDT, POLL_SECONDS)
    log.info("ROI close retries=%d retry_delay=%ss post-close entry pause=%ss",
             max(ROI_CLOSE_RETRY_ROUNDS, 1), ROI_CLOSE_RETRY_DELAY_SECONDS,
             ROI_POST_CLOSE_PAUSE_SECONDS)
    while True:
        started = time.monotonic()
        run_once(sess, journal, symbols, disabled)
        time.sleep(max(0.0, POLL_SECONDS - (time.monotonic() - started)))


if __name__ == "__main__":
    main()
