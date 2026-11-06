# trade_log.py
import json
import sqlite3
import threading
import time

DB_PATH = "trades.db"
_LOCK = threading.Lock()

SCHEMA = """
CREATE TABLE IF NOT EXISTS trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts_ms INTEGER NOT NULL,
    event TEXT NOT NULL,
    complex_id TEXT,
    leg1 TEXT,
    leg2 TEXT,
    pnl_usd REAL,
    details TEXT
);
CREATE INDEX IF NOT EXISTS idx_trades_ts ON trades(ts_ms);
CREATE INDEX IF NOT EXISTS idx_trades_complex ON trades(complex_id);

CREATE TABLE IF NOT EXISTS equity (
    ts_ms INTEGER PRIMARY KEY,
    total_equity REAL,
    available REAL,
    unrealized_pnl REAL
);
"""


def init():
    with _LOCK, sqlite3.connect(DB_PATH) as conn:
        conn.executescript(SCHEMA)


def _cid(cs) -> str:
    return "|".join(tuple(cs.complex["leg1"]) + tuple(cs.complex["leg2"]))


def log_event(cs, event: str, pnl_usd: float = None, **details):
    try:
        with _LOCK, sqlite3.connect(DB_PATH) as conn:
            conn.execute(
                "INSERT INTO trades (ts_ms, event, complex_id, leg1, leg2, pnl_usd, details) "
                "VALUES (?,?,?,?,?,?,?)",
                (
                    int(time.time() * 1000),
                    event,
                    _cid(cs),
                    "/".join(cs.complex["leg1"]),
                    "/".join(cs.complex["leg2"]),
                    pnl_usd,
                    json.dumps(details, default=str),
                ),
            )
    except Exception:
        pass  # журнал не должен ронять торговлю


def log_orphan_close(symbol: str, side: str, size: float):
    try:
        with _LOCK, sqlite3.connect(DB_PATH) as conn:
            conn.execute(
                "INSERT INTO trades (ts_ms, event, complex_id, leg1, leg2, pnl_usd, details) "
                "VALUES (?,?,?,?,?,?,?)",
                (
                    int(time.time() * 1000),
                    "orphan_close",
                    f"orphan:{symbol}",
                    symbol, "", None,
                    json.dumps({"side": side, "size": size}, default=str),
                ),
            )
    except Exception:
        pass


def log_equity(total_equity: float, available: float, unrealized_pnl: float):
    try:
        with _LOCK, sqlite3.connect(DB_PATH) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO equity (ts_ms, total_equity, available, unrealized_pnl) "
                "VALUES (?,?,?,?)",
                (int(time.time() * 1000), total_equity, available, unrealized_pnl),
            )
    except Exception:
        pass