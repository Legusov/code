#!/usr/bin/env python3
"""Bybit Demo Trading bot: 5m signal + hedge-mode order + TP/SL + SQLite journal."""
from __future__ import annotations

import ast
import json
import logging
import os
import sqlite3
import time
from pathlib import Path
from decimal import Decimal, ROUND_DOWN
from typing import Any

from pybit.unified_trading import HTTP
from api_conf_all import DEMO_CONFIG
from speed_hunter import signal

CATEGORY = "linear"
SETTLE_COIN = "USDT"
SYMBOL_FILE = Path(os.getenv("SYMBOL_FILE", "sym.txt"))
DB_PATH = Path(os.getenv("TRADER_DB", "bybit_signal_trader.sqlite3"))
NOTIONAL_USDT = Decimal(os.getenv("POSITION_NOTIONAL_USDT", "10"))
TP_SL_PCT = Decimal(os.getenv("TP_SL_PCT", "0.10"))
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "60"))
ORDER_TAG = os.getenv("ORDER_TAG", "speed")

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
    value = ast.literal_eval(SYMBOL_FILE.read_text())
    if not isinstance(value, list):
        raise ValueError(f"Invalid symbol list: {SYMBOL_FILE}")
    return [str(x).upper() for x in value]


class Journal:
    def __init__(self, path: Path):
        self.db = sqlite3.connect(path, timeout=30)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
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
        self.db.commit()

    def open_symbols(self) -> set[str]:
        rows = self.db.execute("SELECT DISTINCT symbol FROM positions WHERE status='OPEN'").fetchall()
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

    def insert_open(self, values: dict[str, Any]) -> None:
        fields = ",".join(values)
        marks = ",".join("?" for _ in values)
        self.db.execute(f"INSERT INTO positions ({fields}) VALUES ({marks})", tuple(values.values()))
        self.db.commit()

    def open_rows(self):
        return self.db.execute("SELECT * FROM positions WHERE status='OPEN' ORDER BY entry_time_ms").fetchall()

    def close(self, row_id: int, close_time_ms: int, exit_price: float | None,
              pnl: float, status: str, payload: dict[str, Any]) -> None:
        self.db.execute("""
            UPDATE positions SET status='CLOSED', close_time_ms=?, exit_price=?, pnl_usdt=?,
            close_status=?, close_pnl_json=? WHERE id=? AND status='OPEN'
        """, (close_time_ms, exit_price, pnl, status, json.dumps(payload, ensure_ascii=False), row_id))
        self.db.commit()


def round_down(value: Decimal, step: Decimal) -> Decimal:
    return (value / step).to_integral_value(rounding=ROUND_DOWN) * step


def rules(sess: HTTP, symbol: str) -> tuple[Decimal, Decimal, Decimal]:
    info = check(sess.get_instruments_info(category=CATEGORY, symbol=symbol), "get_instruments_info")["result"]["list"][0]
    return (Decimal(info["lotSizeFilter"]["qtyStep"]), Decimal(info["lotSizeFilter"]["minOrderQty"]), Decimal(info["priceFilter"]["tickSize"]))


def positions(sess: HTTP) -> dict[str, dict[int, dict[str, Any]]]:
    response = check(sess.get_positions(category=CATEGORY, settleCoin=SETTLE_COIN), "get_positions")
    result: dict[str, dict[int, dict[str, Any]]] = {}
    for p in response["result"]["list"]:
        if Decimal(str(p.get("size", "0"))) > 0:
            result.setdefault(p["symbol"], {})[int(p.get("positionIdx", 0))] = p
    return result


def closed_pnl(sess: HTTP, symbol: str, entry_ms: int) -> dict[str, Any] | None:
    response = check(sess.get_closed_pnl(category=CATEGORY, symbol=symbol, limit=50), "get_closed_pnl")
    candidates = [p for p in response["result"]["list"] if int(p.get("updatedTime") or p.get("createdTime") or 0) >= entry_ms - 120_000]
    return max(candidates, key=lambda p: int(p.get("updatedTime") or p.get("createdTime") or 0)) if candidates else None


def reconcile(sess: HTTP, journal: Journal, live: dict[str, dict[int, dict[str, Any]]]) -> None:
    for row in journal.open_rows():
        if row["symbol"] in live:
            continue
        payload = closed_pnl(sess, row["symbol"], row["entry_time_ms"])
        if not payload:
            log.warning("%s disappeared, but get_closed_pnl has not updated yet", row["symbol"])
            continue
        exit_price = float(payload.get("avgExitPrice") or 0) or None
        pnl = float(payload.get("closedPnl") or 0)
        tp = float(row["tp_price"]); sl = float(row["sl_price"])
        if exit_price is not None:
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
        journal.close(row["id"], close_time, exit_price, pnl, status, payload)
        log.info("CLOSED %s %s status=%s pnl=%s", row["symbol"], row["side"], status, pnl)


def open_position(sess: HTTP, journal: Journal, symbol: str, signal_side: str, candle_ms: int) -> None:
    side = "Buy" if signal_side == "Long" else "Sell"
    position_idx = 1 if side == "Buy" else 2  # hedge-mode: 1 long, 2 short
    live = positions(sess)
    if symbol in live:
        log.info("SKIP %s: a position for this symbol is already open", symbol)
        return
    if journal.signal_used(symbol, candle_ms):
        return
    ticker = check(sess.get_tickers(category=CATEGORY, symbol=symbol), "get_tickers")["result"]["list"][0]
    entry = Decimal(ticker["lastPrice"])
    qty_step, min_qty, tick_size = rules(sess, symbol)
    qty = round_down(NOTIONAL_USDT / entry, qty_step)
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
        "qty": actual_qty, "notional_usdt": float(NOTIONAL_USDT),
        "margin_usdt": float(NOTIONAL_USDT / Decimal(str(leverage))), "leverage": leverage,
        "signal_candle_ms": candle_ms,
    })
    log.info("OPENED %s %s idx=%s qty=%s TP=%s SL=%s", symbol, signal_side, position_idx, actual_qty, tp, sl)


def run_once(sess: HTTP, journal: Journal, symbols: list[str], disabled: set[str]) -> None:
    live = positions(sess)
    reconcile(sess, journal, live)
    occupied = set(live) | journal.open_symbols()
    for symbol in symbols:
        if symbol in occupied or symbol in disabled:
            continue
        try:
            result = signal(symbol, session=sess)
            if result.ok and result.side and result.candle_start_ms is not None:
                open_position(sess, journal, symbol, result.side, result.candle_start_ms)
                occupied.add(symbol)
        except Exception as exc:
            text = str(exc)
            if "110126" in text or "sign the required agreement" in text:
                disabled.add(symbol)
                journal.disable_symbol(symbol, "Bybit 110126: required agreement before trading")
                log.error("DISABLED %s: Bybit requires the account agreement for this contract", symbol)
            else:
                log.exception("Processing failed for %s", symbol)


def main() -> None:
    sess = create_demo_session()
    journal = Journal(DB_PATH)
    symbols = load_symbols()
    disabled = journal.disabled_symbols()
    if disabled:
        log.warning("Disabled symbols loaded from SQLite: %s", ", ".join(sorted(disabled)))
    log.info("Started: symbols=%d notional=$%s TP/SL=%s%% poll=%ss", len(symbols), NOTIONAL_USDT, TP_SL_PCT * 100, POLL_SECONDS)
    while True:
        started = time.monotonic()
        run_once(sess, journal, symbols, disabled)
        time.sleep(max(0.0, POLL_SECONDS - (time.monotonic() - started)))


if __name__ == "__main__":
    main()
