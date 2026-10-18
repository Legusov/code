#!/usr/bin/env python3
"""Bybit/MEXC funding scanner with a persisted three-pair basket controller.

main() uses only the basket path. Legacy single-pair helpers remain for reading
old code, but active legacy ledgers block migration instead of being discarded.
"""
from __future__ import annotations

import argparse
import csv
import dataclasses
import json
import logging
import os
import sys
import threading
import time
import webbrowser
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from collections import deque
from typing import Any

import requests
from live_execution import (
    EntryRejectedAfterRollback,
    LiveTradingError,
    MexcRegionRestrictedError,
    LiveExecution, BasketExecution, BASKET_SIZE, BASKET_ENTRY_SPREAD,
    BASKET_NET_TARGET, BASKET_MAX_SPREAD, DEPTH_SAFETY_MULTIPLIER,  # [PATCH 1]
    basket_decimal, basket_spread,
)

BYBIT = "https://api.bybit.com"
MEXC = "https://contract.mexc.com"
INTERVAL_SECONDS = 60
MONITORED_CANDIDATES = 3
DEMO = False
mexc_paper = False
bybit_paper = False
TIMEOUT = 15
LIVE_TIMEOUT = 3
PRICE_OPEN_THRESHOLD_PCT = BASKET_ENTRY_SPREAD
PRICE_CLOSE_THRESHOLD_PCT = Decimal("0.2")
HYBRID_PROFIT_TARGET_PCT = Decimal("0.1")
MIN_HOLD_SECONDS = 5

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s UTC | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
})
_mexc_pace_lock = threading.Lock()
_mexc_next_request = 0.0
MEXC_MIN_REQUEST_INTERVAL = 0.40
MEXC_OPEN_BLOCKLIST_FILE = Path(__file__).with_name("mexc_open_blocklist.json")


@dataclass
class Rate:
    exchange_symbol: str
    base: str
    rate: Decimal
    next_time_ms: int | None
    turnover: Decimal
    price: Decimal
    qty_step: Decimal = Decimal(0)
    min_qty: Decimal = Decimal(0)
    contract_size: Decimal = Decimal(0)
    vol_step: Decimal = Decimal(0)
    min_vol: Decimal = Decimal(0)


def get_json(url: str, params: dict[str, Any] | None = None, timeout: int = TIMEOUT) -> dict[str, Any]:
    response = session.get(url, params=params, timeout=timeout)
    response.raise_for_status()
    return response.json()


def mexc_get_json(url: str, params: dict[str, Any] | None = None,
                  timeout: int = TIMEOUT, max_attempts: int = 4) -> dict[str, Any]:
    global _mexc_next_request
    data: dict[str, Any] = {}
    for attempt in range(max_attempts):
        with _mexc_pace_lock:
            delay = _mexc_next_request - time.monotonic()
            if delay > 0:
                time.sleep(delay)
            _mexc_next_request = time.monotonic() + MEXC_MIN_REQUEST_INTERVAL
        data = get_json(url, params, timeout)
        if str(data.get("code")) != "510":
            return data
        if attempt < max_attempts - 1:
            retry_delay = 1.0 * (attempt + 1)
            logging.warning("MEXC rate limit (510); повтор через %.1f сек.", retry_delay)
            time.sleep(retry_delay)
    return data


def dec(value: Any) -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal(0)


def bybit_rates() -> dict[str, Rate]:
    instruments: list[dict[str, Any]] = []
    cursor = None
    while True:
        params: dict[str, Any] = {"category": "linear", "status": "Trading", "limit": 1000}
        if cursor:
            params["cursor"] = cursor
        payload = get_json(f"{BYBIT}/v5/market/instruments-info", params)
        if payload.get("retCode") != 0:
            raise RuntimeError(f"Bybit instruments-info: {payload.get('retMsg')}")
        result = payload.get("result", {})
        instruments.extend(result.get("list", []))
        cursor = result.get("nextPageCursor") or None
        if not cursor:
            break

    eligible = {
        row["symbol"]: row
        for row in instruments
        if row.get("status") == "Trading"
        and row.get("contractType") == "LinearPerpetual"
        and row.get("settleCoin") == "USDT"
    }
    payload = get_json(f"{BYBIT}/v5/market/tickers", {"category": "linear"})
    if payload.get("retCode") != 0:
        raise RuntimeError(f"Bybit tickers: {payload.get('retMsg')}")

    candidates = []
    for row in payload.get("result", {}).get("list", []):
        symbol = row.get("symbol", "")
        turnover = dec(row.get("turnover24h"))
        if symbol in eligible and dec(row.get("lastPrice")) > 0:
            candidates.append((turnover, row))

    rates: dict[str, Rate] = {}
    for turnover, row in candidates:
        symbol = row["symbol"]
        base = eligible[symbol].get("baseCoin", "")
        if not base:
            continue
        next_ms = int(row["nextFundingTime"]) if row.get("nextFundingTime") else None
        lot = eligible[symbol].get("lotSizeFilter", {})
        rates[base] = Rate(
            symbol, base, dec(row.get("fundingRate")), next_ms, turnover, dec(row.get("lastPrice")),
            qty_step=dec(lot.get("qtyStep")), min_qty=dec(lot.get("minOrderQty")),
        )
    return rates


def _mexc_api_allowed(contract: dict[str, Any]) -> bool:
    value = contract.get("apiAllowed")
    if value is None:
        return True
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() not in {"0", "false", "no"}


def mexc_contract_api_allowed(symbol: str, timeout: int = LIVE_TIMEOUT) -> bool:
    payload = mexc_get_json(
        f"{MEXC}/api/v1/contract/detail/country", {"symbol": symbol},
        timeout=timeout, max_attempts=2,
    )
    if not payload.get("success"):
        raise RuntimeError(f"MEXC contract info {symbol}: {payload}")
    data = payload.get("data", [])
    rows = [data] if isinstance(data, dict) else data
    contract = next((row for row in rows if row.get("symbol") == symbol), None)
    if contract is None or "apiAllowed" not in contract:
        raise RuntimeError(f"MEXC contract info {symbol} has no apiAllowed field")
    return _mexc_api_allowed(contract)


def mexc_rates() -> dict[str, Rate]:
    contracts_payload = mexc_get_json(f"{MEXC}/api/v1/contract/detail/country")
    if not contracts_payload.get("success"):
        raise RuntimeError(f"MEXC contract list: {contracts_payload}")
    contracts = contracts_payload.get("data", [])
    if isinstance(contracts, dict):
        contracts = [contracts]
    active_contracts = [
        row for row in contracts
        if row.get("futureType") == 1
        and row.get("state") == 0
        and row.get("quoteCoin") == "USDT"
        and row.get("settleCoin") == "USDT"
        and row.get("symbol")
    ]
    api_blocked = [row for row in active_contracts if not _mexc_api_allowed(row)]
    if api_blocked:
        logging.info("MEXC preflight: пропущено %d контрактов с apiAllowed=false", len(api_blocked))
    active = {
        row.get("symbol"): row
        for row in active_contracts if _mexc_api_allowed(row)
    }

    ticker_payload = mexc_get_json(f"{MEXC}/api/v1/contract/ticker")
    if not ticker_payload.get("success"):
        raise RuntimeError(f"MEXC ticker: {ticker_payload}")
    tickers = ticker_payload.get("data", [])
    if isinstance(tickers, dict):
        tickers = [tickers]

    candidates = []
    for ticker in tickers:
        symbol = ticker.get("symbol", "")
        contract = active.get(symbol)
        turnover = dec(ticker.get("amount24"))
        if contract and dec(ticker.get("lastPrice")) > 0:
            candidates.append((turnover, ticker, contract))
    rates_by_base: dict[str, list[Rate]] = {}
    for turnover, ticker, contract in candidates:
        symbol = ticker["symbol"]
        base = contract.get("baseCoin", "")
        if not base:
            continue
        rates_by_base.setdefault(base, []).append(
            Rate(
                symbol, base, dec(ticker.get("fundingRate")), None, turnover, dec(ticker.get("lastPrice")),
                contract_size=dec(contract.get("contractSize")),
                vol_step=dec(contract.get("volUnit") or contract.get("minVol")),
                min_vol=dec(contract.get("minVol") or contract.get("volUnit")),
            )
        )
    return {base: rows[0] for base, rows in rates_by_base.items() if len(rows) == 1}


def orderbook_side_notional(levels: list[Any], quantity_index: int,
                            contract_size: Decimal = Decimal(1)) -> Decimal:
    total = Decimal(0)
    for level in levels:
        if not isinstance(level, (list, tuple)) or len(level) <= quantity_index:
            continue
        price, quantity = dec(level[0]), dec(level[quantity_index])
        if price > 0 and quantity > 0 and contract_size > 0:
            total += price * quantity * contract_size
    return total


def orderbook_liquidity(a: Rate, b: Rate, notional: Decimal, long_exchange: str,
                        timeout: int = TIMEOUT) -> tuple[bool, Decimal, Decimal]:
    def bybit_book() -> dict[str, Any]:
        payload = get_json(f"{BYBIT}/v5/market/orderbook",
                           {"category": "linear", "symbol": a.exchange_symbol, "limit": 50}, timeout)
        if payload.get("retCode") != 0:
            raise RuntimeError(f"Bybit orderbook {a.exchange_symbol}: {payload.get('retMsg')}")
        return payload.get("result", {})

    def mexc_book() -> dict[str, Any]:
        payload = mexc_get_json(f"{MEXC}/api/v1/contract/depth/{b.exchange_symbol}",
                                {"limit": 50}, timeout=timeout, max_attempts=2)
        if not payload.get("success"):
            raise RuntimeError(f"MEXC orderbook {b.exchange_symbol}: {payload}")
        return payload.get("data", {})

    with ThreadPoolExecutor(max_workers=2, thread_name_prefix="candidate-book") as pool:
        bybit_future = pool.submit(bybit_book)
        mexc_future = pool.submit(mexc_book)
        bybit, mexc = bybit_future.result(), mexc_future.result()

    if long_exchange == "Bybit":
        long_depth = orderbook_side_notional(bybit.get("a", []), 1)
        short_depth = orderbook_side_notional(mexc.get("bids", []), 2, b.contract_size)
    elif long_exchange == "MEXC":
        long_depth = orderbook_side_notional(mexc.get("asks", []), 2, b.contract_size)
        short_depth = orderbook_side_notional(bybit.get("b", []), 1)
    else:
        raise ValueError(f"Неизвестная long-биржа: {long_exchange}")
    # [PATCH 5] require depth with safety multiple
    required = notional * DEPTH_SAFETY_MULTIPLIER
    return long_depth >= required and short_depth >= required, long_depth, short_depth


def select_liquid_candidates(price_candidates: list[tuple[Decimal, Rate, Rate]],
                             notional: Decimal, min_spread: Decimal,
                             live_controller: Any = None,
                             limit: int = MONITORED_CANDIDATES,
                             report: list[dict[str, Any]] | None = None,
                             ) -> tuple[list[tuple[Decimal, Rate, Rate]], int]:
    selected = []
    checked_books = 0
    if report is not None:
        report.clear()
    ordered = sorted(price_candidates, key=lambda item: item[0], reverse=True)
    for candidate in ordered:
        if candidate[0] <= min_spread:
            break
        abs_spread, a, b = candidate
        signed_spread = basket_spread(a.price, b.price)
        long_exchange = "Bybit" if a.price < b.price else "MEXC"
        short_exchange = "MEXC" if long_exchange == "Bybit" else "Bybit"
        row = {
            "base": a.base, "bybit_symbol": a.exchange_symbol, "mexc_symbol": b.exchange_symbol,
            "bybit_price": a.price, "mexc_price": b.price,
            "signed_spread_pct": signed_spread, "abs_spread_pct": abs_spread,
            "long_exchange": long_exchange, "short_exchange": short_exchange,
            "target_notional_usd": notional, "long_depth_usd": "", "short_depth_usd": "",
            "status": "", "reason": "",
        }
        # [PATCH 6] early sanity: reject anomalies before book check
        if abs_spread > BASKET_MAX_SPREAD:
            row["status"] = "REJECTED_SANITY"
            row["reason"] = f"спред {abs_spread:.4f}% > {BASKET_MAX_SPREAD}% (несопоставимые инструменты)"
            logging.warning("Пропущен кандидат %s: аномальный спред %+.4f%% (>%s%%)",
                            a.base, signed_spread, BASKET_MAX_SPREAD)
            if report is not None:
                report.append(row)
            continue
        # [PATCH 6 end]
        if len(selected) >= limit:
            row["status"] = "NOT_CHECKED_LIMIT"
            row["reason"] = f"уже выбрано {limit} пар; дальнейшая проверка стакана не выполнялась"
            if report is not None:
                report.append(row)
            continue
        if live_controller and not live_controller.candidate_tradeable(a, b):
            if live_controller.is_mexc_region_blocked(b):
                row["status"] = "BLOCKED_REGION"
                row["reason"] = "контракт MEXC отмечен как недоступный для открытия"
                logging.info("Пропущен кандидат %s: MEXC %s сохранён как недоступный для открытия",
                             a.base, b.exchange_symbol)
            else:
                row["status"] = "REJECTED_SIZE"
                row["reason"] = "целевой номинал не проходит шаг/min-объём"
                logging.info("Пропущен кандидат %s: целевой номинал не проходит шаг/min-объём", a.base)
            if report is not None:
                report.append(row)
            continue
        checked_books += 1
        try:
            liquid, long_depth, short_depth = orderbook_liquidity(a, b, notional, long_exchange)
        except Exception as exc:
            row["status"] = "BOOK_ERROR"
            row["reason"] = f"ошибка проверки стакана: {type(exc).__name__}: {exc}"
            logging.warning("Пропущен кандидат %s: не удалось проверить стаканы: %s", a.base, exc)
            if report is not None:
                report.append(row)
            continue
        row["long_depth_usd"] = long_depth
        row["short_depth_usd"] = short_depth
        if not liquid:
            row["status"] = "REJECTED_DEPTH"
            row["reason"] = f"глубина хотя бы одной стороны меньше {DEPTH_SAFETY_MULTIPLIER}× номинала"
            logging.info(
                "Пропущен кандидат %s: стакан меньше %s× лота $%.2f "
                "(LONG %s $%.2f / SHORT %s $%.2f)",
                a.base, DEPTH_SAFETY_MULTIPLIER, notional, long_exchange, long_depth,
                "MEXC" if long_exchange == "Bybit" else "Bybit", short_depth,
            )
            if report is not None:
                report.append(row)
            continue
        selected.append(candidate)
        row["status"] = "SELECTED"
        row["reason"] = "спред и ликвидность прошли предварительный фильтр"
        if report is not None:
            report.append(row)
    return selected, checked_books


def fmt_pct(rate: Decimal) -> str:
    return f"{rate * Decimal(100):+.8f}%"


def fmt_time(ms: int | None) -> str:
    if not ms:
        return "время не указано"
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


class QuietHandler(SimpleHTTPRequestHandler):
    def log_message(self, format: str, *args: Any) -> None:
        pass


class QuietHTTPServer(ThreadingHTTPServer):
    def handle_error(self, request: Any, client_address: Any) -> None:
        error = sys.exc_info()[1]
        if isinstance(error, (BrokenPipeError, ConnectionAbortedError, ConnectionResetError)):
            return
        super().handle_error(request, client_address)


class PaperSimulator:
    """One cross-exchange simulated pair trade at a time; no real orders are sent."""

    CSV_FIELDS = [
        "opened_utc", "closed_utc", "symbol", "entry_spread_pct", "exit_spread_pct",
        "long_exchange", "short_exchange", "long_entry_price", "long_exit_price",
        "short_entry_price", "short_exit_price", "notional_per_leg_usd", "margin_per_leg_usd",
        "leverage", "gross_pnl_usd", "entry_fees_usd", "exit_fees_usd", "net_pnl_usd",
        "close_reason", "duration_seconds",
    ]

    def __init__(self, margin_per_leg: Decimal, leverage: Decimal,
                 initial_balance: Decimal, bybit_fee: Decimal, mexc_fee: Decimal,
                 trades_csv: Path, open_gap_pct: Decimal, close_gap_pct: Decimal,
                 history_limit: int = 360, state_file: Path = Path("paper_state.json"),
                 min_hold_sec: int = MIN_HOLD_SECONDS):
        self.margin = margin_per_leg
        self.leverage = leverage
        self.notional = margin_per_leg * leverage
        self.initial_balance = initial_balance
        self.wallet = {"Bybit": initial_balance, "MEXC": initial_balance}
        self.fee_rate = {"Bybit": bybit_fee, "MEXC": mexc_fee}
        self.fees_paid = {"Bybit": Decimal(0), "MEXC": Decimal(0)}
        self.trades_csv = trades_csv
        self.state_file = state_file
        self.open_gap_pct = open_gap_pct
        self.close_gap_pct = close_gap_pct
        self.min_hold_sec = max(0, int(min_hold_sec))
        self.open_trade: dict[str, Any] | None = None
        self.closed_trades: list[dict[str, Any]] = []
        self.closed_count = 0
        self.closed_wins = 0
        self.closed_net = Decimal(0)
        self.equity_history: deque[tuple[float, float, float, float]] = deque(maxlen=history_limit)
        self.equity_history.append((time.time(), float(initial_balance), float(initial_balance), float(initial_balance * 2)))
        self.pair: tuple[str, str, str] | None = None
        self.last_prices: tuple[Decimal, Decimal] | None = None
        self.live_mode = False
        self.exchange_paper = {"Bybit": True, "MEXC": True}
        self.live_balances: dict[str, dict[str, Decimal]] | None = None
        self.live_balance_updated_at: str | None = None
        if not self.load_state():
            self.recover_from_csv()
        self.save_state()

    def load_state(self) -> bool:
        if not self.state_file.exists():
            return False
        try:
            data = json.loads(self.state_file.read_text(encoding="utf-8"))
            self.initial_balance = dec(data.get("initial_balance", self.initial_balance))
            self.wallet = {name: dec(data.get("wallet", {}).get(name, self.wallet[name]))
                           for name in ("Bybit", "MEXC")}
            self.fees_paid = {name: dec(data.get("fees_paid", {}).get(name, 0))
                              for name in ("Bybit", "MEXC")}
            self.closed_count = int(data.get("closed_count", 0))
            self.closed_wins = int(data.get("closed_wins", 0))
            self.closed_net = dec(data.get("closed_net", 0))
            raw_pair = data.get("pair")
            self.pair = tuple(raw_pair) if isinstance(raw_pair, list) and len(raw_pair) == 3 else None
            raw_prices = data.get("last_prices")
            self.last_prices = tuple(dec(v) for v in raw_prices) if isinstance(raw_prices, list) and len(raw_prices) == 2 else None
            raw_trade = data.get("open_trade")
            if isinstance(raw_trade, dict):
                restored = dict(raw_trade)
                restored["entry_spread_pct"] = dec(restored["entry_spread_pct"])
                restored["entry_prices"] = {k: dec(v) for k, v in restored["entry_prices"].items()}
                restored["quantities"] = {k: dec(v) for k, v in restored["quantities"].items()}
                restored["entry_fees"] = {k: dec(v) for k, v in restored["entry_fees"].items()}
                restored["notional"] = dec(restored.get("notional", self.notional))
                restored["margin"] = dec(restored.get("margin", self.margin))
                restored["leverage"] = dec(restored.get("leverage", self.leverage))
                self.open_trade = restored
            raw_history = data.get("equity_history", [])
            self.equity_history = deque(
                ((float(r[0]), float(r[1]), float(r[2]), float(r[3])) for r in raw_history if len(r) == 4),
                maxlen=self.equity_history.maxlen,
            )
            if not self.equity_history:
                self.equity_history.append((time.time(), float(self.wallet["Bybit"]), float(self.wallet["MEXC"]),
                                            float(self.wallet["Bybit"] + self.wallet["MEXC"])))
            logging.info("Восстановлено состояние paper trading: %d закрытых сделок, equity $%.2f%s",
                         self.closed_count, self.wallet["Bybit"] + self.wallet["MEXC"],
                         ", есть открытая позиция" if self.open_trade else "")
            return True
        except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError) as exc:
            logging.error("Не удалось восстановить %s: %s; пробую импортировать CSV-журнал", self.state_file, exc)
            return False

    def recover_from_csv(self) -> None:
        if not self.trades_csv.exists():
            return
        try:
            with self.trades_csv.open("r", newline="", encoding="utf-8-sig") as f:
                records = list(csv.DictReader(f))
        except (OSError, csv.Error) as exc:
            logging.warning("Не удалось импортировать прежний CSV-журнал %s: %s", self.trades_csv, exc)
            return
        recovered_net = Decimal(0)
        for row in records:
            net = dec(row.get("net_pnl_usd"))
            self.closed_count += 1
            self.closed_wins += int(net > 0)
            self.closed_net += net
            recovered_net += net
            try:
                long_name, short_name = row["long_exchange"], row["short_exchange"]
                notional = dec(row["notional_per_leg_usd"])
                long_entry, long_exit = dec(row["long_entry_price"]), dec(row["long_exit_price"])
                short_entry, short_exit = dec(row["short_entry_price"]), dec(row["short_exit_price"])
                if notional <= 0 or min(long_entry, long_exit, short_entry, short_exit) <= 0:
                    raise ValueError("missing trade prices")
                quantities = {long_name: notional / long_entry, short_name: notional / short_entry}
                prices_in = {long_name: long_entry, short_name: short_entry}
                prices_out = {long_name: long_exit, short_name: short_exit}
                gross = {
                    long_name: quantities[long_name] * (long_exit - long_entry),
                    short_name: quantities[short_name] * (short_entry - short_exit),
                }
                for name in ("Bybit", "MEXC"):
                    entry_fee = notional * self.fee_rate[name]
                    exit_fee = quantities[name] * prices_out[name] * self.fee_rate[name]
                    self.wallet[name] += gross.get(name, Decimal(0)) - entry_fee - exit_fee
                    self.fees_paid[name] += entry_fee + exit_fee
            except (KeyError, ValueError, ZeroDivisionError):
                self.wallet["Bybit"] += net / 2
                self.wallet["MEXC"] += net / 2
                logging.warning("Для сделки %s из CSV нельзя восстановить точные балансы по биржам; net PnL распределён поровну",
                                row.get("symbol", "?"))
        if records:
            logging.warning(
                "Импортировано %d завершённых сделок из старого CSV. Открытая позиция старой версии не была сохранена и восстановить её нельзя; дальнейшие сессии будут сохранять позиции автоматически.",
                self.closed_count,
            )
            current_total = self.wallet["Bybit"] + self.wallet["MEXC"]
            correction = self.initial_balance * 2 + recovered_net - current_total
            self.wallet["Bybit"] += correction / 2
            self.wallet["MEXC"] += correction / 2
            point = (time.time(), float(self.wallet["Bybit"]), float(self.wallet["MEXC"]),
                     float(self.wallet["Bybit"] + self.wallet["MEXC"]))
            self.equity_history.clear()
            self.equity_history.append(point)

    def save_state(self) -> None:
        trade = self.open_trade
        raw_trade = None
        if trade:
            raw_trade = {
                **{k: v for k, v in trade.items() if
                   k not in ("entry_prices", "quantities", "entry_fees", "entry_spread_pct", "notional", "margin",
                             "leverage")},
                "entry_spread_pct": str(trade["entry_spread_pct"]),
                "entry_prices": {k: str(v) for k, v in trade["entry_prices"].items()},
                "quantities": {k: str(v) for k, v in trade["quantities"].items()},
                "entry_fees": {k: str(v) for k, v in trade["entry_fees"].items()},
                "notional": str(trade["notional"]), "margin": str(trade["margin"]),
                "leverage": str(trade["leverage"]),
            }
        data = {
            "version": 1, "saved_at_utc": datetime.now(timezone.utc).isoformat(),
            "initial_balance": str(self.initial_balance),
            "wallet": {k: str(v) for k, v in self.wallet.items()},
            "fees_paid": {k: str(v) for k, v in self.fees_paid.items()},
            "closed_count": self.closed_count, "closed_wins": self.closed_wins,
            "closed_net": str(self.closed_net), "pair": list(self.pair) if self.pair else None,
            "last_prices": [str(v) for v in self.last_prices] if self.last_prices else None,
            "open_trade": raw_trade,
            "equity_history": [list(row) for row in self.equity_history],
        }
        payload = json.dumps(data, ensure_ascii=False)
        try:
            self.state_file.parent.mkdir(parents=True, exist_ok=True)
        except OSError:
            pass

        temp = self.state_file.with_name(self.state_file.name + ".tmp")
        last_exc: Exception | None = None
        for attempt in range(4):
            try:
                temp.write_text(payload, encoding="utf-8")
                os.replace(temp, self.state_file)
                return
            except PermissionError as exc:
                last_exc = exc
                time.sleep(0.2 * (attempt + 1))
            except OSError as exc:
                last_exc = exc
                break

        try:
            with self.state_file.open("w", encoding="utf-8") as stream:
                stream.write(payload)
            return
        except OSError as exc:
            last_exc = exc

        now = time.monotonic()
        if now - getattr(self, "_last_save_warning", 0.0) >= 30.0:
            self._last_save_warning = now
            logging.warning("Не удалось сохранить состояние paper trading %s: %s",
                            self.state_file, last_exc)

    # The rest of PaperSimulator is unchanged from the original for compactness.
    # (select_pair / process_prices / open_trade_pair / pnl_by_exchange / exit_fees /
    #  hybrid_profit_target_hit / close_trade / save_record / equity / record_equity /
    #  summary / execution_mode_text) — identical to the source you already have.

    def select_pair(self, a: Rate | None, b: Rate | None) -> None:
        new_pair = (a.base, a.exchange_symbol, b.exchange_symbol) if a and b else None
        if new_pair != self.pair:
            if self.open_trade:
                if self.pair:
                    old_a = Rate(self.pair[1], self.pair[0], Decimal(0), None, Decimal(0),
                                 self.last_prices[0] if self.last_prices else self.open_trade["entry_prices"]["Bybit"])
                    old_b = Rate(self.pair[2], self.pair[0], Decimal(0), None, Decimal(0),
                                 self.last_prices[1] if self.last_prices else self.open_trade["entry_prices"]["MEXC"])
                    try:
                        self.last_prices = live_prices(old_a, old_b)
                    except (requests.RequestException, RuntimeError, ValueError, KeyError) as exc:
                        logging.warning("Не удалось обновить старую пару перед закрытием; использую последнюю сохранённую цену: %s", exc)
                if self.last_prices:
                    self.close_trade(*self.last_prices, time.time(), "сменился лидер расхождения")
            self.pair = new_pair
            self.last_prices = None
            self.save_state()

    def process_prices(self, bybit_price: Decimal, mexc_price: Decimal, timestamp: float,
                       allow_entry: bool = True) -> None:
        if not self.pair or bybit_price <= 0 or mexc_price <= 0:
            return
        self.last_prices = (bybit_price, mexc_price)
        spread_pct = (bybit_price / mexc_price - Decimal(1)) * Decimal(100)

        closed_this_tick = False
        if self.open_trade:
            opened_spread = self.open_trade["entry_spread_pct"]
            direction_reversed = spread_pct != 0 and (spread_pct > 0) != (opened_spread > 0)
            hold_ready = (timestamp - self.open_trade["opened_at"]) >= self.min_hold_sec
            if self.hybrid_profit_target_hit(None):
                self.close_trade(
                    bybit_price, mexc_price, timestamp,
                    "достигнут exchange-reported hybrid PnL target",
                )
                closed_this_tick = True
            elif hold_ready and abs(spread_pct) < self.close_gap_pct:
                self.close_trade(bybit_price, mexc_price, timestamp, "спред сузился ниже порога закрытия")
                closed_this_tick = True
            elif hold_ready and direction_reversed:
                self.close_trade(bybit_price, mexc_price, timestamp, "спред сменил знак")
                closed_this_tick = True

        if allow_entry and self.open_trade is None and not closed_this_tick and abs(spread_pct) > self.open_gap_pct:
            self.open_trade_pair(bybit_price, mexc_price, spread_pct, timestamp)
        self.record_equity(timestamp, bybit_price, mexc_price)
        self.save_state()

    def open_trade_pair(self, bybit_price: Decimal, mexc_price: Decimal,
                        spread_pct: Decimal, timestamp: float,
                        long_exchange: str | None = None) -> None:
        long_exchange = long_exchange or ("Bybit" if bybit_price < mexc_price else "MEXC")
        if long_exchange not in ("Bybit", "MEXC"):
            raise ValueError(f"Неизвестная long-биржа: {long_exchange}")
        short_exchange = "MEXC" if long_exchange == "Bybit" else "Bybit"
        prices = {"Bybit": bybit_price, "MEXC": mexc_price}
        quantities = {name: self.notional / prices[name] for name in ("Bybit", "MEXC")}
        entry_fees = {name: self.notional * self.fee_rate[name] for name in ("Bybit", "MEXC")}
        for name in ("Bybit", "MEXC"):
            self.wallet[name] -= entry_fees[name]
            self.fees_paid[name] += entry_fees[name]
        self.open_trade = {
            "symbol": self.pair[0], "opened_at": timestamp, "entry_spread_pct": spread_pct,
            "long_exchange": long_exchange, "short_exchange": short_exchange,
            "entry_prices": prices, "quantities": quantities, "entry_fees": entry_fees,
            "notional": self.notional, "margin": self.margin, "leverage": self.leverage,
        }
        mode = ("HYBRID ledger" if self.live_mode and any(self.exchange_paper.values())
                else "LIVE ledger" if self.live_mode else "PAPER")
        logging.info(
            "%s OPEN %s: LONG %s / SHORT %s | spread %+.5f%% | notional $%.2f each, margin $%.2f each, x%s | min-hold %ds",
            mode,
            self.pair[0], long_exchange, short_exchange, spread_pct,
            self.notional, self.margin, self.leverage, self.min_hold_sec,
        )

    def pnl_by_exchange(self, bybit_price: Decimal, mexc_price: Decimal) -> dict[str, Decimal]:
        result = {"Bybit": Decimal(0), "MEXC": Decimal(0)}
        if not self.open_trade:
            return result
        trade = self.open_trade
        prices = {"Bybit": bybit_price, "MEXC": mexc_price}
        long_name, short_name = trade["long_exchange"], trade["short_exchange"]
        long_pnl = trade["quantities"][long_name] * (prices[long_name] - trade["entry_prices"][long_name])
        short_pnl = trade["quantities"][short_name] * (trade["entry_prices"][short_name] - prices[short_name])
        result[long_name] += long_pnl
        result[short_name] += short_pnl
        return result

    def exit_fees(self, bybit_price: Decimal, mexc_price: Decimal) -> dict[str, Decimal]:
        if not self.open_trade:
            return {"Bybit": Decimal(0), "MEXC": Decimal(0)}
        prices = {"Bybit": bybit_price, "MEXC": mexc_price}
        trade = self.open_trade
        return {name: trade["quantities"][name] * prices[name] * self.fee_rate[name]
                for name in ("Bybit", "MEXC")}

    def hybrid_profit_target_hit(self, reported_live_net_pnl: Decimal | None) -> bool:
        if not self.open_trade or not self.live_mode:
            return False
        live_exchanges = [name for name in ("Bybit", "MEXC")
                          if not self.exchange_paper.get(name, True)]
        if len(live_exchanges) != 1 or reported_live_net_pnl is None:
            return False
        target = self.open_trade["notional"] * HYBRID_PROFIT_TARGET_PCT
        return reported_live_net_pnl >= target

    def close_trade(self, bybit_price: Decimal, mexc_price: Decimal,
                    timestamp: float, reason: str) -> None:
        if not self.open_trade:
            return
        trade = self.open_trade
        prices = {"Bybit": bybit_price, "MEXC": mexc_price}
        gross_by_exchange = self.pnl_by_exchange(bybit_price, mexc_price)
        closing_fees = self.exit_fees(bybit_price, mexc_price)
        for name in ("Bybit", "MEXC"):
            self.wallet[name] += gross_by_exchange[name] - closing_fees[name]
            self.fees_paid[name] += closing_fees[name]
        gross = sum(gross_by_exchange.values(), Decimal(0))
        entry_fees_total = sum(trade["entry_fees"].values(), Decimal(0))
        exit_fees_total = sum(closing_fees.values(), Decimal(0))
        net = gross - entry_fees_total - exit_fees_total
        exit_spread = (bybit_price / mexc_price - Decimal(1)) * Decimal(100) if mexc_price else Decimal(0)
        closed_utc = datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()
        opened_utc = datetime.fromtimestamp(trade["opened_at"], tz=timezone.utc).isoformat()
        record = {
            "opened_utc": opened_utc, "closed_utc": closed_utc, "symbol": trade["symbol"],
            "entry_spread_pct": trade["entry_spread_pct"], "exit_spread_pct": exit_spread,
            "long_exchange": trade["long_exchange"], "short_exchange": trade["short_exchange"],
            "long_entry_price": trade["entry_prices"][trade["long_exchange"]],
            "long_exit_price": prices[trade["long_exchange"]],
            "short_entry_price": trade["entry_prices"][trade["short_exchange"]],
            "short_exit_price": prices[trade["short_exchange"]],
            "notional_per_leg_usd": trade["notional"], "margin_per_leg_usd": trade["margin"],
            "leverage": trade["leverage"], "gross_pnl_usd": gross,
            "entry_fees_usd": entry_fees_total, "exit_fees_usd": exit_fees_total,
            "net_pnl_usd": net, "close_reason": reason,
            "duration_seconds": Decimal(str(timestamp - trade["opened_at"])),
        }
        self.closed_trades.append(record)
        self.closed_count += 1
        self.closed_wins += int(net > 0)
        self.closed_net += net
        self.open_trade = None
        try:
            self.save_record(record)
        except OSError as exc:
            logging.warning("Не удалось записать сделку в CSV %s: %s", self.trades_csv, exc)
        mode = ("HYBRID ledger" if self.live_mode and any(self.exchange_paper.values())
                else "LIVE ledger" if self.live_mode else "PAPER")
        logging.info("%s CLOSE %s | gross $%.4f | fees $%.4f | net $%+.4f | %s",
                     mode,
                     trade["symbol"], gross, entry_fees_total + exit_fees_total, net, reason)
        self.save_state()

    def save_record(self, record: dict[str, Any]) -> None:
        self.trades_csv.parent.mkdir(parents=True, exist_ok=True)
        write_header = not self.trades_csv.exists() or self.trades_csv.stat().st_size == 0
        encoding = "utf-8-sig" if write_header else "utf-8"
        with self.trades_csv.open("a", newline="", encoding=encoding) as f:
            writer = csv.DictWriter(f, fieldnames=self.CSV_FIELDS)
            if write_header:
                writer.writeheader()
            writer.writerow({key: str(value) for key, value in record.items()})

    def equity(self, bybit_price: Decimal | None = None, mexc_price: Decimal | None = None) -> tuple[Decimal, Decimal, Decimal]:
        bybit_price = bybit_price or (self.last_prices[0] if self.last_prices else Decimal(0))
        mexc_price = mexc_price or (self.last_prices[1] if self.last_prices else Decimal(0))
        unrealized = self.pnl_by_exchange(bybit_price, mexc_price)
        close_fees = self.exit_fees(bybit_price, mexc_price)
        bybit_equity = self.wallet["Bybit"] + unrealized["Bybit"] - close_fees["Bybit"]
        mexc_equity = self.wallet["MEXC"] + unrealized["MEXC"] - close_fees["MEXC"]
        return bybit_equity, mexc_equity, bybit_equity + mexc_equity

    def record_equity(self, timestamp: float, bybit_price: Decimal, mexc_price: Decimal) -> None:
        bybit_eq, mexc_eq, total = self.equity(bybit_price, mexc_price)
        self.equity_history.append((timestamp, float(bybit_eq), float(mexc_eq), float(total)))

    def summary(self) -> str:
        bybit_eq, mexc_eq, total = self.equity()
        win_rate = (Decimal(self.closed_wins) / Decimal(self.closed_count) * 100) if self.closed_count else Decimal(0)
        open_net = Decimal(0)
        if self.open_trade and self.last_prices:
            gross = sum(self.pnl_by_exchange(*self.last_prices).values(), Decimal(0))
            close_cost = sum(self.exit_fees(*self.last_prices).values(), Decimal(0))
            entry_cost = sum(self.open_trade["entry_fees"].values(), Decimal(0))
            open_net = gross - close_cost - entry_cost
        total_fees = sum(self.fees_paid.values(), Decimal(0))
        balance_lines: list[str] = []
        if self.live_balances:
            for name, metrics in self.live_balances.items():
                equity = metrics.get("equity", Decimal(0))
                available = metrics.get("available_balance", Decimal(0))
                wallet = metrics.get("wallet_balance", Decimal(0))
                unrealized = metrics.get("unrealized_pnl", Decimal(0))
                if name == "MEXC":
                    balance_lines.append(
                        f"MEXC LIVE API: equity ${equity:.4f} | доступно ${available:.4f} | "
                        f"выводимый cash ${metrics.get('withdrawable_balance', wallet):.4f} | "
                        f"uPnL ${unrealized:+.4f} | маржа позиций ${metrics.get('position_margin', Decimal(0)):.4f}"
                    )
                else:
                    balance_lines.append(
                        f"Bybit LIVE API: account equity ${equity:.4f} | wallet ${wallet:.4f} | "
                        f"доступно ${available:.4f} | unrealized PnL ${unrealized:+.4f}"
                    )
        live_balance_text = (
            "Баланс аккаунта (данные API, снимок "
            f"{self.live_balance_updated_at or 'время не указано'}):\n"
            + "\n".join(balance_lines)
            if balance_lines else
            "Баланс аккаунта LIVE API: пока нет снимка" if self.live_mode else ""
        )
        open_desc = "Нет открытой пары"
        if self.open_trade:
            age = max(0, time.time() - self.open_trade["opened_at"])
            hold_left = max(0, self.min_hold_sec - age)
            open_desc = (f"Открыта: {self.open_trade['symbol']} | "
                         f"LONG {self.open_trade['long_exchange']} / SHORT {self.open_trade['short_exchange']} | "
                         f"буфер {hold_left:.1f}с")
        return (f"Режим: {self.execution_mode_text()}\n"
                f"Позиция: {open_desc}\n"
                f"Сделки: закрыто {self.closed_count} | wins {self.closed_wins} | losses {self.closed_count - self.closed_wins} | win rate {win_rate:.1f}%\n"
                f"PnL: реализованный net ${self.closed_net:+.4f} | текущий net ${open_net:+.4f}\n"
                f"Комиссии: всего ${total_fees:.4f} | Bybit ${self.fees_paid['Bybit']:.4f} | MEXC ${self.fees_paid['MEXC']:.4f}\n"
                f"Equity: Bybit ${bybit_eq:.4f} | MEXC ${mexc_eq:.4f} | всего ${total:.4f}\n"
                f"{live_balance_text}\n"
                f"Параметры: маржа ${self.margin:.2f} × плечо {self.leverage} = номинал ${self.notional:.2f} на сторону | буфер {self.min_hold_sec}с\n"
                f"Пороги: вход > {self.open_gap_pct:.3f}% | выход < {self.close_gap_pct:.3f}%")

    def execution_mode_text(self) -> str:
        modes = {name: "PAPER" if self.exchange_paper.get(name, True) else "LIVE"
                 for name in ("Bybit", "MEXC")}
        if all(mode == "PAPER" for mode in modes.values()):
            return "PAPER: Bybit + MEXC"
        if all(mode == "LIVE" for mode in modes.values()):
            return "LIVE: Bybit + MEXC (PnL/equity — оценка)"
        return f"HYBRID: Bybit {modes['Bybit']} | MEXC {modes['MEXC']} (PnL/equity — оценка)"


class LiveDashboard:
    """Serve a browser dashboard rendered with Canvas and JSON, never PNGs."""

    PAGE = """<!doctype html><html lang="ru"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Bybit / MEXC — live monitor</title><style>body{font:16px system-ui;margin:24px;background:#f5f7fa;color:#17212b}h1{margin-bottom:4px}.sub{color:#52616b;white-space:pre-wrap}.box{margin-top:18px;padding:12px;background:#fff;border:1px solid #d8dee6;border-radius:8px}canvas{width:100%;height:320px;display:block}pre{font:14px ui-monospace,SFMono-Regular,Consolas,monospace;white-space:pre-wrap;line-height:1.5;margin:8px 0 0}</style>
<h1 id="title">Ожидание сверки funding</h1><div class="sub box" id="status">Ожидается первая выборка.</div>
<section class="box"><h2>Basket projected net PnL, USDT (не реализованная прибыль)</h2><canvas id="basket_net" width="1200" height="320"></canvas><h2>Корзина: сводка и сверка</h2><pre id="simulation">Ожидание первой сверки</pre></section>
<script>
const colors=['#2878B5','#F39C35','#8E44AD'];
function lineChart(id,rows,keys,labels,unit){const c=document.getElementById(id),x=c.getContext('2d'),d=devicePixelRatio||1,w=c.clientWidth*d,h=320*d;c.width=w;c.height=h;x.clearRect(0,0,w,h);if(!rows||!rows.length){x.fillStyle='#52616b';x.font=14*d+'px system-ui';x.fillText('Ожидание данных…',24*d,32*d);return}const pad={l:78*d,r:24*d,t:28*d,b:42*d},vals=keys.flatMap(k=>rows.map(r=>Number(r[k]))).filter(Number.isFinite),rawLo=Math.min(...vals),rawHi=Math.max(...vals),rawSpan=rawHi-rawLo,extra=rawSpan?rawSpan*.08:Math.max(Math.abs(rawHi)*.001,unit==='%'?.01:1),lo=rawLo-extra,hi=rawHi+extra,span=hi-lo||1,px=i=>pad.l+(w-pad.l-pad.r)*i/Math.max(1,rows.length-1),py=v=>h-pad.b-(h-pad.t-pad.b)*(v-lo)/span,fmt=v=>unit==='%'?v.toFixed(3)+'%':(Math.abs(v)>=100?v.toFixed(2):v.toFixed(6));x.font=12*d+'px system-ui';x.strokeStyle='#d8dee6';x.fillStyle='#52616b';x.lineWidth=d;for(let i=0;i<=5;i++){const v=lo+span*i/5,y=py(v);x.beginPath();x.moveTo(pad.l,y);x.lineTo(w-pad.r,y);x.stroke();x.fillText(fmt(v),4*d,y+4*d)};x.beginPath();x.moveTo(pad.l,pad.t);x.lineTo(pad.l,h-pad.b);x.lineTo(w-pad.r,h-pad.b);x.stroke();keys.forEach((k,n)=>{x.strokeStyle=colors[n]||'#333';x.lineWidth=2*d;x.beginPath();rows.forEach((r,i)=>{const v=Number(r[k]),q=[px(i),py(v)];Number.isFinite(v)&&(i?x.lineTo(...q):x.moveTo(...q))});x.stroke();x.fillStyle=x.strokeStyle;x.fillText(labels[n],pad.l+n*110*d,pad.t-8*d)});x.fillStyle='#52616b';x.fillText(new Date(rows[0].t*1000).toLocaleTimeString(),pad.l,h-10*d);if(rows.length>1)x.fillText(new Date(rows[rows.length-1].t*1000).toLocaleTimeString(),w-pad.r-70*d,h-10*d)}
async function refresh(){try{const r=await fetch('live_status.json?t='+Date.now(),{cache:'no-store'}),d=await r.json();document.getElementById('title').textContent=d.symbol?d.symbol+' — Bybit / MEXC':'Ожидание сверки funding';document.getElementById('status').textContent=d.message||'';document.getElementById('simulation').textContent=d.simulation||'';lineChart('basket_net',d.basket_history,['projected_net'],['Projected net'],'USDT');}catch(e){document.getElementById('status').textContent='Ожидание данных…'}}setInterval(refresh,1000);refresh();</script></html>"""

    def __init__(self, directory: Path, port: int, max_points: int, open_browser: bool = True):
        self.directory = directory.resolve()
        self.directory.mkdir(parents=True, exist_ok=True)
        (self.directory / "index.html").write_text(self.PAGE, encoding="utf-8")
        self.history: deque[tuple[float, float, float, float]] = deque(maxlen=max_points)
        self.symbol: str | None = None
        self.bybit_symbol = ""
        self.mexc_symbol = ""
        self.graph_name = ""
        self.stats_graph_name = ""
        self.current_message = ""
        self.candidate_status = ""
        self.current_simulation = "Ожидание первой котировки"
        self.server = QuietHTTPServer(("127.0.0.1", port), partial(QuietHandler, directory=str(self.directory)))
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.write_status("", "Сверка запущена; ожидается первая выборка.")
        url = f"http://127.0.0.1:{self.port}/"
        logging.info("Live-график: %s", url)
        if open_browser:
            try:
                webbrowser.open_new_tab(url)
            except Exception as exc:
                logging.warning("Не удалось открыть браузер автоматически: %s", exc)

    def write_status(self, symbol: str, message: str, simulation: str | None = None) -> None:
        self.current_message = message
        if simulation is not None:
            self.current_simulation = simulation
        path = self.directory / "live_status.json"
        display_message = message + (f"\n{self.candidate_status}" if self.candidate_status else "")
        try:
            with path.open("w", encoding="utf-8") as stream:
                json.dump({"symbol": symbol, "message": display_message, "graph": "", "stats_graph": "",
                           "simulation": self.current_simulation,
                           "basket": getattr(self, 'basket_status', {}),
                           "basket_history": getattr(self, 'basket_history', []),
                           "history": [{"t": row[0], "bybit": row[1], "mexc": row[2], "spread": row[3]}
                                       for row in self.history]}, stream, ensure_ascii=False)
        except PermissionError as exc:
            logging.warning("Не удалось обновить статус live-панели: %s", exc)

    def set_pair(self, a: Rate, b: Rate) -> bool:
        changed = (self.symbol != a.base or self.bybit_symbol != a.exchange_symbol or
                   self.mexc_symbol != b.exchange_symbol)
        if changed:
            self.history.clear()
            self.symbol = a.base
            self.bybit_symbol = a.exchange_symbol
            self.mexc_symbol = b.exchange_symbol
        pct = (a.price / b.price - Decimal(1)) * Decimal(100) if b.price else Decimal(0)
        message = (f"Bybit {a.price:.8g} | MEXC {b.price:.8g} | "
                   f"спред {pct:+.6f}% (база — MEXC) | REST, обновление раз в секунду")
        self.write_status(self.symbol, message)
        if changed:
            logging.info("Лидер расхождения цены: %s (Bybit %s / MEXC %s), спред %+.6f%%; история графика сброшена",
                         a.base, a.exchange_symbol, b.exchange_symbol, pct)
        return changed

    def clear_pair(self) -> None:
        self.symbol = None
        self.history.clear()
        self.candidate_status = ""
        self.write_status("", "Нет кандидатов с достаточным спредом и глубиной стакана; ожидается следующая сверка.")

    def update_candidate_status(self, summaries: list[str],
                                candidate_rows: list[dict[str, Any]] | None = None) -> None:
        self.candidate_status = ("КАНДИДАТЫ ПОСЛЕ ФИЛЬТРА СПРЕДА/ЛИКВИДНОСТИ:\n" +
                                 ("\n".join(summaries) if summaries else "Нет пар выше порога спреда."))
        if candidate_rows is not None:
            report_path = self.directory / "basket_candidates.csv"
            fields = ["scan_time_utc", "base", "bybit_symbol", "mexc_symbol",
                      "bybit_price", "mexc_price", "signed_spread_pct", "abs_spread_pct",
                      "long_exchange", "short_exchange", "target_notional_usd",
                      "long_depth_usd", "short_depth_usd", "status", "reason"]
            scan_time = datetime.now(timezone.utc).isoformat()
            try:
                with report_path.open("w", encoding="utf-8-sig", newline="") as stream:
                    writer = csv.DictWriter(stream, fieldnames=fields, extrasaction="ignore")
                    writer.writeheader()
                    for row in candidate_rows:
                        writer.writerow({"scan_time_utc": scan_time, **row})
                logging.info("Полный список кандидатов записан: %s (%s строк)",
                             report_path, len(candidate_rows))
            except OSError as exc:
                logging.warning("Не удалось записать CSV списка кандидатов: %s", exc)
        self.write_status(self.symbol or "", self.current_message)

    def add_point(self, timestamp: float, bybit_price: Decimal, mexc_price: Decimal) -> None:
        if not self.symbol:
            return
        spread = ((bybit_price / mexc_price) - Decimal(1)) * Decimal(100) if mexc_price else Decimal(0)
        self.history.append((timestamp, float(bybit_price), float(mexc_price), float(spread)))
        message = (f"Bybit {bybit_price:.8g} | MEXC {mexc_price:.8g} | "
                   f"спред {spread:+.6f}% (база — MEXC) | REST, обновление раз в секунду")
        self.write_status(self.symbol, message)

    def update_simulation(self, simulator: PaperSimulator) -> None:
        self.write_status(self.symbol or "", self.current_message, simulator.summary())

    def update_basket(self, simulator: Any) -> None:
        engine = simulator.engine
        state = engine.state
        self.basket_history = state.get('history', [])[-simulator.history_limit:]
        self.basket_status = json.loads(json.dumps({key: state.get(key) for key in (
            'phase', 'verified_legs', 'projection', 'projection_valid', 'last_reconcile_time',
            'balances', 'balance_updated_at', 'halt_reason')}, default=str))
        self.symbol = ', '.join(pair['base'] for pair in engine.basket['pairs']) if engine.basket else 'Basket'
        message = ('6/6 verified required; projected net is not guaranteed realized profit. '
                   + ('NEW ENTRIES BLOCKED' if not engine.can_enter else 'Waiting for exactly 3 eligible pairs'))
        self.write_status(self.symbol, message, simulator.summary())

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()


def live_prices(a: Rate, b: Rate) -> tuple[Decimal, Decimal]:
    def get_bybit() -> Decimal:
        data = get_json(f"{BYBIT}/v5/market/tickers", {"category": "linear", "symbol": a.exchange_symbol}, LIVE_TIMEOUT)
        if data.get("retCode") != 0:
            raise RuntimeError(f"Bybit live ticker: {data.get('retMsg')}")
        rows = data.get("result", {}).get("list", [])
        if not rows:
            raise RuntimeError(f"Пустой Bybit ticker для {a.exchange_symbol}")
        return dec(rows[0].get("lastPrice"))

    def get_mexc() -> Decimal:
        data = mexc_get_json(f"{MEXC}/api/v1/contract/ticker", {"symbol": b.exchange_symbol},
                             timeout=LIVE_TIMEOUT, max_attempts=1)
        if not data.get("success"):
            raise RuntimeError(f"MEXC live ticker: {data}")
        rows = data.get("data", [])
        if isinstance(rows, dict):
            rows = [rows]
        if not rows:
            raise RuntimeError(f"Пустой MEXC ticker для {b.exchange_symbol}")
        return dec(rows[0].get("lastPrice"))

    with ThreadPoolExecutor(max_workers=2) as pool:
        f_bybit = pool.submit(get_bybit)
        f_mexc = pool.submit(get_mexc)
        return f_bybit.result(), f_mexc.result()


def live_prices_many(pairs: list[tuple[Rate, Rate]]) -> dict[str, tuple[Decimal, Decimal]]:
    def get_bybit_tickers() -> dict[str, Decimal]:
        payload = get_json(f"{BYBIT}/v5/market/tickers", {"category": "linear"}, LIVE_TIMEOUT)
        if payload.get("retCode") != 0:
            raise RuntimeError(f"Bybit all-tickers: {payload.get('retMsg')}")
        return {row.get("symbol", ""): dec(row.get("lastPrice"))
                for row in payload.get("result", {}).get("list", [])}

    def get_mexc_tickers() -> dict[str, Decimal]:
        payload = mexc_get_json(f"{MEXC}/api/v1/contract/ticker", timeout=LIVE_TIMEOUT,
                                max_attempts=1)
        if not payload.get("success"):
            raise RuntimeError(f"MEXC all-tickers: {payload}")
        rows = payload.get("data", [])
        if isinstance(rows, dict):
            rows = [rows]
        return {row.get("symbol", ""): dec(row.get("lastPrice")) for row in rows}

    with ThreadPoolExecutor(max_workers=2, thread_name_prefix="candidate-tickers") as pool:
        bybit_future = pool.submit(get_bybit_tickers)
        mexc_future = pool.submit(get_mexc_tickers)
        bybit, mexc = bybit_future.result(), mexc_future.result()
    result = {}
    for a, b in pairs:
        bybit_price, mexc_price = bybit.get(a.exchange_symbol, Decimal(0)), mexc.get(b.exchange_symbol, Decimal(0))
        if bybit_price > 0 and mexc_price > 0:
            result[a.base] = (bybit_price, mexc_price)
    return result


def compare_once(min_gap_pp: Decimal, chart_gap_pp: Decimal, chart_file: Path,
                 render_static_chart: bool = True) -> list[tuple[Decimal, Decimal, Rate, Rate]]:
    bybit = bybit_rates()
    mexc = mexc_rates()
    common = sorted(set(bybit) & set(mexc))
    all_rows = []
    for base in common:
        a, b = bybit[base], mexc[base]
        gap_pp = abs(a.rate - b.rate) * Decimal(100)
        denominator = max(abs(a.rate), abs(b.rate))
        relative = (abs(a.rate - b.rate) / denominator * Decimal(100)) if denominator else Decimal(0)
        all_rows.append((gap_pp, relative, a, b))
    all_rows.sort(key=lambda row: row[0], reverse=True)
    found = [row for row in all_rows if row[0] >= min_gap_pp and row[0] > 0]

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"\n=== Сверка {now} ===", flush=True)
    print(f"Активные USDT perpetual-контракты: Bybit {len(bybit)}, MEXC {len(mexc)}", flush=True)
    print(f"Сравниваются все совпадающие базовые активы с котировками на обеих биржах: {len(common)}", flush=True)
    if all_rows:
        mean_gap = sum((row[0] for row in all_rows), Decimal(0)) / len(all_rows)
        print(f"Среднее расхождение: {mean_gap:.8f} п.п. | максимум: {all_rows[0][0]:.8f} п.п. ({all_rows[0][2].base})", flush=True)
        price_spreads = [
            (abs((a.price / b.price - Decimal(1)) * Decimal(100)), a.base)
            for _, _, a, b in all_rows if b.price > 0
        ]
        if price_spreads:
            largest_price_spread, price_symbol = max(price_spreads)
            print(f"Максимальное различие цены: {largest_price_spread:.6f}% ({price_symbol}; база сравнения — цена MEXC)", flush=True)
    print(f"Порог вывода: {min_gap_pp} п.п. | порог построения графика: {chart_gap_pp} п.п.", flush=True)

    if not found:
        print("Нет расхождений выше порога вывода.", flush=True)
        return all_rows
    return all_rows


class LiveTradingController:
    """Connect the persisted trade ledger to the guarded REST executor."""

    def __init__(self, executor: Any, simulator: PaperSimulator):
        self.executor = executor
        self.simulator = simulator
        self._last_depth_warning = 0.0
        self._entry_retry_after: dict[str, float] = {}
        self.region_restriction_detected = False
        try:
            if MEXC_OPEN_BLOCKLIST_FILE.exists():
                saved = json.loads(MEXC_OPEN_BLOCKLIST_FILE.read_text(encoding="utf-8"))
                if not isinstance(saved, dict) or not isinstance(saved.get("symbols"), list):
                    raise ValueError("expected an object with a 'symbols' list")
                symbols = saved["symbols"]
                self.mexc_region_blocked_symbols = {str(symbol) for symbol in symbols if symbol}
            else:
                self.mexc_region_blocked_symbols: set[str] = set()
        except Exception as exc:
            raise LiveTradingError(
                f"Не удалось прочитать {MEXC_OPEN_BLOCKLIST_FILE}; запуск остановлен для безопасной проверки: {exc}"
            ) from exc
        if self.mexc_region_blocked_symbols:
            logging.warning("MEXC preflight: %d контрактов сохранены как запрещённые для новых входов: %s",
                            len(self.mexc_region_blocked_symbols),
                            ", ".join(sorted(self.mexc_region_blocked_symbols)))

    def _block_mexc_region_symbol(self, symbol: str, reason: str) -> None:
        self.mexc_region_blocked_symbols.add(symbol)
        try:
            temp_path = MEXC_OPEN_BLOCKLIST_FILE.with_suffix(".json.tmp")
            temp_path.write_text(
                json.dumps({"symbols": sorted(self.mexc_region_blocked_symbols)}, indent=2),
                encoding="utf-8",
            )
            temp_path.replace(MEXC_OPEN_BLOCKLIST_FILE)
        except OSError as exc:
            logging.error("Не удалось сохранить MEXC blocklist %s: %s; блокировка действует до завершения процесса",
                          MEXC_OPEN_BLOCKLIST_FILE, exc)
        if reason == "apiAllowed=false":
            logging.warning(
                "MEXC preflight запретил открытие %s (apiAllowed=false); ордер не отправлялся. "
                "Символ сохранён в %s до ручного удаления. Закрытие существующих позиций не блокируется.",
                symbol, MEXC_OPEN_BLOCKLIST_FILE,
            )
        else:
            logging.error(
                "MEXC отклонил открытие %s (%s); сверка/rollback завершились без ошибки. "
                "Символ сохранён в %s до ручного удаления. Закрытие существующих позиций не блокируется.",
                symbol, reason, MEXC_OPEN_BLOCKLIST_FILE,
            )

    def configure_rows(self, rows: list[tuple[Decimal, Decimal, Rate, Rate]]) -> None:
        self.executor.prepare_account()
        self.refresh_balances()

    def configure_candidates(self, pairs: list[tuple[Rate, Rate]]) -> None:
        rows = [(Decimal(0), Decimal(0), a, b) for a, b in pairs]
        self.executor.configure_symbols(rows)
        self.simulator.leverage = Decimal(self.executor.leverage)
        self.simulator.notional = self.simulator.margin * self.simulator.leverage
        logging.info(
            "Плечо x%s настроено; номинал пересчитан до $%s на сторону",
            self.executor.leverage, self.simulator.notional,
        )

    def is_mexc_region_blocked(self, b: Rate) -> bool:
        return not self.is_paper("MEXC") and b.exchange_symbol in self.mexc_region_blocked_symbols

    def candidate_tradeable(self, a: Rate, b: Rate) -> bool:
        if self.is_mexc_region_blocked(b):
            return False
        return self.executor.pair_is_tradeable(a, b)

    def is_paper(self, exchange: str) -> bool:
        return bool(getattr(self.executor, f"{exchange.lower()}_paper", False))

    @staticmethod
    def _position_diagnostics(exchange: str, rows: list[dict[str, Any]]) -> str:
        if exchange == "MEXC":
            fields = ("symbol", "positionId", "positionType", "_direction", "side", "holdVol", "state")
        else:
            fields = ("symbol", "positionIdx", "side", "size", "positionStatus")
        selected = [{key: row.get(key) for key in fields if key in row} for row in rows]
        return f"{exchange} API positions={selected}"

    def reported_live_net_pnl(self, positions: dict[str, list[dict[str, Any]]],
                              trade: dict[str, Any]) -> Decimal | None:
        live_exchanges = [name for name in ("Bybit", "MEXC") if not self.is_paper(name)]
        if len(live_exchanges) != 1:
            return None
        exchange = live_exchanges[0]
        if exchange == "MEXC":
            expected_direction = 1 if trade["long_exchange"] == "MEXC" else 2
            rows = [p for p in positions["MEXC"]
                    if int(p.get("_direction", 0)) == expected_direction and dec(p.get("holdVol")) > 0]
            if not rows or any(p.get("unRealizedPnl") in (None, "") for p in rows):
                return None
            return sum((dec(p.get("unRealizedPnl")) - dec(p.get("totalFee")) +
                        dec(p.get("holdFee")) for p in rows), Decimal(0))

        expected_idx = 1 if trade["long_exchange"] == "Bybit" else 2
        rows = [p for p in positions["Bybit"]
                if int(p.get("positionIdx", -1)) == expected_idx and dec(p.get("size")) > 0]
        if not rows or any(p.get("unrealisedPnl") in (None, "") for p in rows):
            return None
        return sum((dec(p.get("unrealisedPnl")) + dec(p.get("curRealisedPnl"))
                    for p in rows), Decimal(0))

    def refresh_balances(self) -> None:
        try:
            self.simulator.live_balances = self.executor.account_metrics()
            self.simulator.live_balance_updated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        except Exception as exc:
            logging.warning("Не удалось обновить снимок live-баланса через API: %s", exc)

    def reconcile_startup(self) -> bool:
        had_trade = bool(self.simulator.open_trade)
        active = self.executor.prepare_restored_position(self.simulator) if had_trade else False
        self.refresh_balances()
        return active

    def _close_fill(self, name: str, result: Any, rate: Rate,
                    fallback_price: Decimal) -> tuple[Decimal, Decimal, Decimal]:
        if name == "MEXC":
            fills = result if isinstance(result, list) else ([result] if result else [])
            base_qty = sum((item.get("base_qty", Decimal(0)) for item in fills), Decimal(0))
            if base_qty <= 0:
                return fallback_price, Decimal(0), Decimal(0)
            weighted_price = sum((item["base_qty"] * item["avg_price"] for item in fills), Decimal(0)) / base_qty
            fee = sum((item.get("fee", Decimal(0)) for item in fills), Decimal(0))
            return weighted_price, base_qty, fee
        if not result:
            return fallback_price, Decimal(0), Decimal(0)
        return result["avg_price"], result["base_qty"], result.get("fee", Decimal(0))

    def _book_close(self, a: Rate, b: Rate, bybit_mark: Decimal, mexc_mark: Decimal,
                    timestamp: float, reason: str) -> None:
        sim = self.simulator
        trade = sim.open_trade
        if not trade:
            return
        try:
            results = self.executor.close_pair(a, b, trade["long_exchange"])
        except LiveTradingError:
            raise
        except Exception as exc:
            raise LiveTradingError(f"Не подтверждено закрытие live-пары; новые заявки запрещены: {exc}") from exc
        bybit_exit, _, bybit_fee = self._close_fill("Bybit", results.get("Bybit"), a, bybit_mark)
        mexc_exit, _, mexc_fee = self._close_fill("MEXC", results.get("MEXC"), b, mexc_mark)
        if bybit_fee <= 0:
            bybit_fee = trade["quantities"]["Bybit"] * bybit_exit * sim.fee_rate["Bybit"]
        if mexc_fee <= 0:
            mexc_fee = trade["quantities"]["MEXC"] * mexc_exit * sim.fee_rate["MEXC"]
        original_rates = dict(sim.fee_rate)
        try:
            sim.fee_rate["Bybit"] = bybit_fee / (trade["quantities"]["Bybit"] * bybit_exit)
            sim.fee_rate["MEXC"] = mexc_fee / (trade["quantities"]["MEXC"] * mexc_exit)
            sim.close_trade(bybit_exit, mexc_exit, timestamp, reason)
        finally:
            sim.fee_rate = original_rates
        self.refresh_balances()

    def check_position_integrity(self, a: Rate, b: Rate, timestamp: float) -> None:
        sim = self.simulator
        trade = sim.open_trade
        if not trade:
            return
        try:
            positions = self.executor.positions(a, b)
        except Exception as exc:
            raise LiveTradingError(f"Не удалось проверить обе live-ноги по API: {exc}") from exc

        expected_bybit_idx = 1 if trade["long_exchange"] == "Bybit" else 2
        expected_mexc_dir = 1 if trade["long_exchange"] == "MEXC" else 2

        unexpected_b = [] if self.is_paper("Bybit") else [
            p for p in positions["Bybit"]
            if int(p.get("positionIdx", -1)) != expected_bybit_idx
        ]
        unexpected_m = [] if self.is_paper("MEXC") else [
            p for p in positions["MEXC"]
            if int(p.get("_direction", 0)) != expected_mexc_dir
        ]
        if unexpected_b or unexpected_m:
            raise LiveTradingError(
                "Обнаружена позиция не ожидаемой hedge-стороны; остановка без управления ей. "
                f"Ожидался MEXC positionType={expected_mexc_dir}; "
                f"{self._position_diagnostics('MEXC', positions['MEXC'])}; "
                f"ожидался Bybit positionIdx={expected_bybit_idx}; "
                f"{self._position_diagnostics('Bybit', positions['Bybit'])}"
            )

        b_open = self.is_paper("Bybit") or any(
            int(p.get("positionIdx", -1)) == expected_bybit_idx and dec(p.get("size")) > 0
            for p in positions["Bybit"]
        )
        m_open = self.is_paper("MEXC") or any(
            int(p.get("_direction", 0)) == expected_mexc_dir and dec(p.get("holdVol")) > 0
            for p in positions["MEXC"]
        )
        if not b_open or not m_open:
            marks = sim.last_prices or (trade["entry_prices"]["Bybit"], trade["entry_prices"]["MEXC"])
            logging.critical("Ценовой API недоступен, но обнаружена отсутствующая нога; закрываю оставшуюся")
            self._book_close(a, b, marks[0], marks[1], timestamp,
                             "одна нога исчезла при сбое ценового API; вторая закрыта market")
            sim.record_equity(timestamp, marks[0], marks[1])
            sim.save_state()

    def process_prices(self, a: Rate, b: Rate, bybit_price: Decimal, mexc_price: Decimal,
                       timestamp: float, allow_entry: bool) -> None:
        sim = self.simulator
        self.region_restriction_detected = False
        if bybit_price <= 0 or mexc_price <= 0:
            return
        sim.last_prices = (bybit_price, mexc_price)
        spread = (bybit_price / mexc_price - Decimal(1)) * Decimal(100)
        trade = sim.open_trade
        if trade:
            try:
                positions = self.executor.positions(a, b)
            except Exception as exc:
                raise LiveTradingError(f"Не удалось проверить обе live-ноги по API: {exc}") from exc

            expected_bybit_idx = 1 if trade["long_exchange"] == "Bybit" else 2
            expected_mexc_dir = 1 if trade["long_exchange"] == "MEXC" else 2

            unexpected_b = [] if self.is_paper("Bybit") else [
                p for p in positions["Bybit"]
                if int(p.get("positionIdx", -1)) != expected_bybit_idx
            ]
            unexpected_m = [] if self.is_paper("MEXC") else [
                p for p in positions["MEXC"]
                if int(p.get("_direction", 0)) != expected_mexc_dir
            ]
            if unexpected_b or unexpected_m:
                raise LiveTradingError(
                    "Обнаружена позиция не ожидаемой hedge-стороны; остановка без управления ей. "
                    f"Ожидался MEXC positionType={expected_mexc_dir}; "
                    f"{self._position_diagnostics('MEXC', positions['MEXC'])}; "
                    f"ожидался Bybit positionIdx={expected_bybit_idx}; "
                    f"{self._position_diagnostics('Bybit', positions['Bybit'])}"
                )

            b_open = self.is_paper("Bybit") or any(
                int(p.get("positionIdx", -1)) == expected_bybit_idx and dec(p.get("size")) > 0
                for p in positions["Bybit"]
            )
            m_open = self.is_paper("MEXC") or any(
                int(p.get("_direction", 0)) == expected_mexc_dir and dec(p.get("holdVol")) > 0
                for p in positions["MEXC"]
            )
            reversed_direction = spread != 0 and (spread > 0) != (trade["entry_spread_pct"] > 0)
            hold_ready = (timestamp - trade["opened_at"]) >= sim.min_hold_sec

            reported_pnl = (self.reported_live_net_pnl(positions, trade)
                            if b_open and m_open else None)
            if not b_open or not m_open:
                logging.critical("Обнаружена отсутствующая нога; немедленно закрываю оставшуюся через market")
                self._book_close(a, b, bybit_price, mexc_price, timestamp,
                                 "одна нога исчезла; вторая закрыта по API-сверке")
            elif sim.hybrid_profit_target_hit(reported_pnl):
                target_amount = trade["notional"] * HYBRID_PROFIT_TARGET_PCT
                self._book_close(
                    a, b, bybit_price, mexc_price, timestamp,
                    f"биржевой net PnL live-позиции ${reported_pnl:+.4f} достиг цели ${target_amount:.4f}",
                )
            elif hold_ready and (abs(spread) < sim.close_gap_pct or reversed_direction):
                reason = ("спред сузился ниже порога закрытия"
                          if abs(spread) < sim.close_gap_pct else "спред сменил знак")
                self._book_close(a, b, bybit_price, mexc_price, timestamp, reason)
            else:
                sim.record_equity(timestamp, bybit_price, mexc_price)
                sim.save_state()
                return
            sim.record_equity(timestamp, bybit_price, mexc_price)
            sim.save_state()
            return

        if allow_entry and abs(spread) > sim.open_gap_pct:
            retry_after = self._entry_retry_after.get(b.exchange_symbol, 0.0)
            if time.monotonic() < retry_after:
                sim.record_equity(timestamp, bybit_price, mexc_price)
                sim.save_state()
                return
            if not self.is_paper("MEXC") and b.exchange_symbol in self.mexc_region_blocked_symbols:
                sim.record_equity(timestamp, bybit_price, mexc_price)
                sim.save_state()
                return
            if not self.is_paper("MEXC"):
                try:
                    api_allowed = mexc_contract_api_allowed(b.exchange_symbol, timeout=LIVE_TIMEOUT)
                except Exception as exc:
                    now = time.monotonic()
                    if now - self._last_depth_warning >= 10.0:
                        logging.warning("Вход %s отложен: MEXC apiAllowed preflight недоступен: %s",
                                        a.base, exc)
                        self._last_depth_warning = now
                    sim.record_equity(timestamp, bybit_price, mexc_price)
                    sim.save_state()
                    return
                if not api_allowed:
                    self._block_mexc_region_symbol(b.exchange_symbol, "apiAllowed=false")
                    sim.record_equity(timestamp, bybit_price, mexc_price)
                    sim.save_state()
                    return
            long_exchange = "Bybit" if bybit_price < mexc_price else "MEXC"
            book_error = None
            try:
                liquid, long_depth, short_depth = orderbook_liquidity(
                    a, b, self.executor.notional, long_exchange, timeout=LIVE_TIMEOUT,
                )
            except Exception as exc:
                liquid, long_depth, short_depth = False, Decimal(0), Decimal(0)
                book_error = str(exc)
            if not liquid:
                now = time.monotonic()
                if now - self._last_depth_warning >= 10.0:
                    short_exchange = "MEXC" if long_exchange == "Bybit" else "Bybit"
                    if book_error:
                        logging.warning("Вход %s отложен: стакан не удалось проверить: %s", a.base, book_error)
                    else:
                        logging.info(
                            "Вход %s отложен: недостаточно глубины на $%.2f; "
                            "LONG %s depth=$%.2f, SHORT %s depth=$%.2f",
                            a.base, self.executor.notional, long_exchange, long_depth,
                            short_exchange, short_depth,
                        )
                    self._last_depth_warning = now
                sim.record_equity(timestamp, bybit_price, mexc_price)
                sim.save_state()
                return
            try:
                fills = self.executor.open_pair(a, b, long_exchange)
            except MexcRegionRestrictedError as exc:
                self.region_restriction_detected = True
                self._block_mexc_region_symbol(exc.symbol, "order API code 8950")
                sim.record_equity(timestamp, bybit_price, mexc_price)
                sim.save_state()
                return
            except EntryRejectedAfterRollback as exc:
                cooldown = 60.0
                self._entry_retry_after[b.exchange_symbol] = time.monotonic() + cooldown
                logging.warning(
                    "Вход %s безопасно отклонён после подтверждённого rollback: %s. "
                    "Процесс продолжит мониторинг; повторная попытка этого контракта не раньше чем через %.0f сек.",
                    a.base, exc, cooldown,
                )
                sim.record_equity(timestamp, bybit_price, mexc_price)
                sim.save_state()
                return
            except LiveTradingError:
                raise
            except Exception as exc:
                raise LiveTradingError(f"Неопределённое состояние открытия market-пары; новые заявки запрещены: {exc}") from exc
            actual_b = fills["Bybit"]["avg_price"]
            actual_m = fills["MEXC"]["avg_price"]
            actual_spread = (actual_b / actual_m - Decimal(1)) * Decimal(100)
            sim.open_trade_pair(actual_b, actual_m, actual_spread, timestamp,
                                long_exchange=long_exchange)
            trade = sim.open_trade
            assert trade is not None
            for name, exchange_fill in fills.items():
                estimated_fee = trade["entry_fees"][name]
                actual_fee = exchange_fill.get("fee", Decimal(0))
                if actual_fee <= 0:
                    actual_fee = estimated_fee
                sim.wallet[name] += estimated_fee - actual_fee
                sim.fees_paid[name] += actual_fee - estimated_fee
                trade["entry_fees"][name] = actual_fee
                trade["entry_prices"][name] = exchange_fill["avg_price"]
                trade["quantities"][name] = exchange_fill["base_qty"]
            trade["entry_spread_pct"] = actual_spread
            trade["notional"] = sum((trade["entry_prices"][name] * trade["quantities"][name]
                                      for name in ("Bybit", "MEXC")), Decimal(0)) / Decimal(2)
            mode = "HYBRID" if self.is_paper("Bybit") or self.is_paper("MEXC") else "LIVE"
            logging.warning("%s OPEN confirmed: %s, LONG %s / SHORT %s | min-hold %ds",
                            mode, trade["symbol"], trade["long_exchange"], trade["short_exchange"],
                            sim.min_hold_sec)
            self.refresh_balances()
        sim.record_equity(timestamp, bybit_price, mexc_price)
        sim.save_state()


class BasketMonitor:
    """Dashboard-facing view; the engine's atomic ledger is the source of truth."""
    def __init__(self, engine: Any, history_limit: int = 360):
        self.engine = engine
        self.margin = basket_decimal(engine.executor.margin)
        self.leverage = basket_decimal(engine.executor.leverage)
        self.notional = basket_decimal(engine.executor.notional)
        self.live_balances = None
        self.live_balance_updated_at = None
        self.history_limit = history_limit

    def save_state(self) -> None:
        self.engine.save_state()

    def summary(self) -> str:
        state = self.engine.state
        basket = self.engine.basket
        pairs = ', '.join(pair['base'] for pair in basket['pairs']) if basket else 'нет'
        modes = ' | '.join(f'{name}={"PAPER" if self.engine.paper_flags[name] else "LIVE"}'
                           for name in ('Bybit', 'MEXC'))
        p = state.get('projection') or {}
        def amount(key: str) -> str:
            return f"${basket_decimal(p[key]):+.6f}" if key in p else 'нет данных'
        status = 'свежая оценка' if state.get('projection_valid') else 'НЕДОСТУПНА/устарела — выход запрещён'
        margin = notional = Decimal(0)
        if basket:
            for pair in basket['pairs']:
                for name in ('Bybit', 'MEXC'):
                    fill = pair.get('fills', {}).get(name)
                    if fill:
                        actual = basket_decimal(fill['base_qty']) * basket_decimal(fill['avg_price'])
                        notional += actual
                        margin += actual / basket_decimal(basket['leverage'])
        reconcile_time = state.get('last_reconcile_time')
        reconciled = datetime.fromtimestamp(float(reconcile_time), timezone.utc).isoformat() if reconcile_time else 'ещё нет'
        lines = [f'Режим: {modes}', f'Basket: {pairs} | phase={state["phase"]}',
                 f'Legs verified: {state.get("verified_legs", 0)}/6',
                 f'Проекция: {status}', f'Gross (mid mark): {amount("gross")}',
                 f'Entry fees: {amount("entry_fees")}',
                 f'Projected exit taker fees: {amount("projected_exit_fees")}',
                 f'Projected slippage (depth VWAP + buffer): {amount("projected_slippage")}',
                 f'Projected net: {amount("projected_net")} | target=$0.10',
                 f'Distance to target: {amount("distance_to_target")}',
                 f'Total actual margin: ${margin:.6f} | total entry notional (6 legs): ${notional:.6f}',
                 f'Запланировано: маржа ${self.margin} × 6; номинал ${self.notional} × 6',
                 f'Last reconcile time: {reconciled}',
                 f'Balance snapshot time: {state.get("balance_updated_at") or "нет"}']
        for name in ('Bybit', 'MEXC'):
            if self.engine.paper_flags[name]:
                lines.append(f'{name} PAPER wallet: ${basket_decimal(state["paper_wallet"][name]):.6f}')
            else:
                metrics = state.get('balances', {}).get(name)
                lines.append(f'{name} LIVE balances: {json.dumps(metrics, default=str, ensure_ascii=False) if metrics else "нет свежего снимка"}')
        if state.get('halt_reason'):
            lines.append(f'FAIL-CLOSED: {state["halt_reason"]}')
        lines.append('Вход: ровно 3 пары, каждая ticker/executable/fill price spread строго >0.5%.')
        lines.append('Выход: только ОБЩИЙ projected net >= $0.10 (либо аварийная защита).')
        lines.append('Projected ≠ гарантированная реализованная прибыль. Funding не считается доходом; fees могут быть оценкой.')
        return '\n'.join(lines)


class BasketTradingController(LiveTradingController):
    """Reuse regional/configuration guards, replace ALL single-pair trading paths."""
    def __init__(self, engine: Any, simulator: BasketMonitor,
                 book_provider: Any = None, api_allowed: Any = None,
                 entry_spread_threshold: Decimal = BASKET_ENTRY_SPREAD):
        super().__init__(engine.executor, simulator)
        self.engine = engine
        self.book_provider = book_provider or basket_orderbooks
        self.api_allowed = api_allowed or mexc_contract_api_allowed
        self.entry_spread_threshold = max(BASKET_ENTRY_SPREAD, basket_decimal(entry_spread_threshold))
        # [PATCH 7] short-lived readiness buffer
        self._pair_ready: dict[str, dict[str, Any]] = {}
        self.pair_ready_ttl = 12.0

    def configure_candidates(self, pairs: list[tuple[Rate, Rate]]) -> None:
        self.simulator.margin = basket_decimal(self.executor.margin)
        super().configure_candidates(pairs)
        self.simulator.notional = basket_decimal(self.executor.notional)
        self.engine.requested_sizing = (self.executor.margin, self.executor.leverage, self.executor.notional)

    def refresh_balances(self) -> None:
        try:
            self.engine.refresh_balances()
        except LiveTradingError:
            if self.engine.halted:
                raise
            logging.warning('Balance snapshot unavailable; prior snapshot is explicitly timestamped')
        except Exception as exc:
            logging.warning('Balance snapshot unavailable: %s', exc)

    def reconcile_startup(self) -> bool:
        had_verified_open_state = self.engine.state['phase'] == 'OPEN'
        try:
            self.engine.reconcile(startup=True)
        except LiveTradingError as exc:
            reason = str(exc)
            if had_verified_open_state and self.engine.basket and ('missing legs' in reason or 'quantity mismatch' in reason):
                saved = self.engine.basket
                self.executor.margin = basket_decimal(saved['margin_per_leg'])
                self.executor.notional = basket_decimal(saved['notional_per_leg'])
                self.executor.leverage = (basket_decimal(saved['leverage']) if all(self.engine.paper_flags.values())
                                          else int(basket_decimal(saved['leverage'])))
                self.engine.emergency_flatten_known(reason)
            raise
        self.refresh_balances()
        return self.engine.basket is not None

    def check_basket_integrity(self, emergency: bool = True) -> None:
        try:
            self.engine.reconcile()
        except LiveTradingError as exc:
            reason = str(exc)
            if emergency and self.engine.basket and ('missing legs' in reason or 'quantity mismatch' in reason):
                self.engine.emergency_flatten_known(reason)
            raise

    def process_basket(self, pairs: list[tuple[Rate, Rate]],
                       current_prices: dict[str, tuple[Decimal, Decimal]],
                       allow_entry: bool) -> bool:
        """One tick, one aggregate decision; injectable public-data providers."""
        self.region_restriction_detected = False
        if self.engine.basket:
            self.check_basket_integrity()
            active = self.engine.active_pairs()
            try:
                books = self.book_provider(active)
            except Exception as exc:
                self.engine.state['projection_valid'] = False
                self.engine.save_state()
                logging.warning('Basket exit estimate unavailable; no target exit: %s', exc)
                return False
            try:
                return self.engine.close_if_target(books)
            except LiveTradingError:
                if self.engine.halted:
                    raise
                logging.warning('Full exit depth unavailable/stale; no projected-net exit')
                return False

        if not allow_entry or not self.engine.can_enter:
            return False

        # [PATCH 8] readiness buffer: refresh survivors, drop expired, then enter
        now = time.monotonic()
        for a, b in pairs:
            prices = current_prices.get(a.base)
            if not prices or abs(basket_spread(*prices)) <= self.entry_spread_threshold:
                continue
            current_a = dataclasses.replace(a, price=prices[0])
            current_b = dataclasses.replace(b, price=prices[1])
            if not self.candidate_tradeable(current_a, current_b):
                continue
            if time.monotonic() < self._entry_retry_after.get(current_b.exchange_symbol, 0.0):
                continue
            if not self.is_paper('MEXC'):
                try:
                    permitted = self.api_allowed(current_b.exchange_symbol, timeout=LIVE_TIMEOUT)
                except Exception as exc:
                    logging.warning('Basket entry postponed: apiAllowed preflight unavailable: %s', exc)
                    continue
                if not permitted:
                    self._block_mexc_region_symbol(current_b.exchange_symbol, 'apiAllowed=false')
                    continue
            try:
                books_one = self.book_provider([(current_a, current_b)])
                self.engine.preflight_books([(current_a, current_b)], books_one)
            except Exception as exc:
                logging.info('Basket candidate %s not ready: %s', current_a.base, exc)
                continue
            self._pair_ready[current_a.base] = {
                'a': current_a, 'b': current_b, 'books': books_one,
                'expires_at': now + self.pair_ready_ttl,
            }

        for base in [k for k, v in self._pair_ready.items() if v['expires_at'] <= now]:
            self._pair_ready.pop(base, None)

        ready = sorted(self._pair_ready.items(),
                       key=lambda kv: kv[1]['expires_at'], reverse=True)
        if len(ready) < BASKET_SIZE:
            if ready:
                logging.info('Basket readiness: %d/%d pairs ready (ttl=%.0fs)',
                             len(ready), BASKET_SIZE, self.pair_ready_ttl)
            return False
        chosen = ready[:BASKET_SIZE]
        chosen_pairs = [(v['a'], v['b']) for _, v in chosen]
        try:
            fresh_books = self.book_provider(chosen_pairs)
        except Exception as exc:
            logging.warning('Basket entry postponed: fresh book fetch failed: %s', exc)
            return False
        for a, b in chosen_pairs:
            if not self.candidate_tradeable(a, b):
                logging.warning('Basket chosen pair %s no longer tradeable', a.base)
                return False
        try:
            opened = self.engine.open_basket(chosen_pairs, fresh_books)
            if opened:
                self._pair_ready.clear()
            else:
                for _, b in chosen_pairs:
                    self._entry_retry_after[b.exchange_symbol] = time.monotonic() + 60
            self.refresh_balances()
            return opened
        except MexcRegionRestrictedError as exc:
            self.region_restriction_detected = True
            self._block_mexc_region_symbol(exc.symbol, 'order API code 8950')
            return False
        except LiveTradingError:
            if self.engine.halted or self.engine.basket:
                raise
            logging.warning('Basket aggregate balance/size preflight rejected entry')
            return False


def basket_orderbooks(pairs: list[tuple[Rate, Rate]]) -> dict[str, Any]:
    def fetch_pair(a: Rate, b: Rate) -> tuple[str, Any]:
        def bybit_book() -> dict[str, Any]:
            observed_at = time.time()
            payload = get_json(f'{BYBIT}/v5/market/orderbook',
                               {'category': 'linear', 'symbol': a.exchange_symbol, 'limit': 50}, LIVE_TIMEOUT)
            if payload.get('retCode') != 0:
                raise RuntimeError('Bybit basket orderbook response rejected')
            raw = payload['result']
            return {'bids': raw.get('b', []), 'asks': raw.get('a', []), 'observed_at': observed_at}
        def mexc_book() -> dict[str, Any]:
            observed_at = time.time()
            payload = mexc_get_json(f'{MEXC}/api/v1/contract/depth/{b.exchange_symbol}',
                                    {'limit': 50}, timeout=LIVE_TIMEOUT, max_attempts=1)
            if not payload.get('success'):
                raise RuntimeError('MEXC basket orderbook response rejected')
            raw = payload['data']
            if b.contract_size <= 0:
                raise RuntimeError('Missing MEXC contractSize; depth cannot be normalized')
            def base_levels(rows: list[Any]) -> list[Any]:
                result = []
                for row in rows:
                    if len(row) < 3:
                        raise RuntimeError('MEXC depth must include price/count/volume')
                    result.append([row[0], basket_decimal(row[2]) * b.contract_size])
                return result
            return {'bids': base_levels(raw.get('bids', [])), 'asks': base_levels(raw.get('asks', [])),
                    'observed_at': observed_at}
        with ThreadPoolExecutor(max_workers=2, thread_name_prefix='basket-book-leg') as pool:
            bf, mf = pool.submit(bybit_book), pool.submit(mexc_book)
            return a.base, {'Bybit': bf.result(), 'MEXC': mf.result()}
    with ThreadPoolExecutor(max_workers=3, thread_name_prefix='basket-books') as pool:
        return dict(pool.map(lambda pair: fetch_pair(*pair), pairs))


def run_basket_monitor(args: Any, paper_flags: dict[str, bool], mode_profile: str) -> None:
    state_file = args.state_file or Path(f'{mode_profile}_state.json')
    known_profiles = ('paper', 'live', 'hybrid_bybit_paper_mexc_live', 'hybrid_bybit_live_mexc_paper')
    for profile in known_profiles:
        other = Path(f'{profile}_state.json')
        if other.resolve() == state_file.resolve() or not other.exists():
            continue
        try:
            saved = json.loads(other.read_text(encoding='utf-8'))
        except Exception as exc:
            raise LiveTradingError(f'Unreadable other-profile ledger {other}; startup blocked') from exc
        if saved.get('open_trade') or saved.get('basket') or saved.get('phase') == 'HALTED':
            raise LiveTradingError(f'Other-profile unresolved exposure {other}; mode switching blocked')
    executor = LiveExecution(int(args.leverage), args.margin_per_leg,
                             bybit_paper=paper_flags['Bybit'], mexc_paper=paper_flags['MEXC'])
    if all(paper_flags.values()):
        executor.leverage = args.leverage
        executor.notional = args.margin_per_leg * args.leverage
    engine = BasketExecution(executor, state_file,
                             {'Bybit': args.bybit_taker_fee, 'MEXC': args.mexc_taker_fee},
                             {name: args.initial_balance for name in ('Bybit', 'MEXC')},
                             args.exit_slippage_bps)
    simulator = BasketMonitor(engine, args.live_points)
    engine.history_limit = args.live_points
    dashboard = None
    scan_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix='basket-scan')
    scan_future = None
    try:
        controller = BasketTradingController(engine, simulator, entry_spread_threshold=
                                             max(args.open_gap_pct, args.min_live_gap_pct, BASKET_ENTRY_SPREAD))
        dashboard = LiveDashboard(args.live_dir, args.live_port, args.live_points, not args.no_browser)
        dashboard.update_basket(simulator)
        controller.reconcile_startup()
        if engine.basket:
            simulator.leverage = basket_decimal(engine.basket['leverage'])
            simulator.notional = basket_decimal(engine.basket['notional_per_leg'])
            simulator.margin = basket_decimal(engine.basket['margin_per_leg'])
        monitored = engine.active_pairs()
        entries_allowed = False
        next_scan = 0.0
        next_balance = time.monotonic() + 30
        next_monitor_log = 0.0
        next_entry_poll = 0.0  # [PATCH 9]
        def scan_task() -> tuple[list[Any], int, list[dict[str, Any]]]:
            rows = compare_once(args.min_gap_pp, args.chart_gap_pp, args.chart_file, False)
            candidates = [(abs(basket_spread(a.price, b.price)), a, b)
                          for _, _, a, b in rows if min(a.price, b.price) > 0]
            report: list[dict[str, Any]] = []
            selected, checked = select_liquid_candidates(
                candidates, simulator.notional,
                max(BASKET_ENTRY_SPREAD, args.min_live_gap_pct, args.open_gap_pct),
                controller, BASKET_SIZE, report)
            return selected, checked, report
        while True:
            tick_started = time.monotonic()
            now = time.monotonic()
            if now >= next_balance:
                controller.refresh_balances()
                next_balance = time.monotonic() + 30
            if engine.can_enter and scan_future is None and now >= next_scan:
                entries_allowed = False
                scan_future = scan_executor.submit(scan_task)
                next_scan = now + args.interval
            if scan_future is not None and scan_future.done():
                try:
                    selected, checked, scan_report = scan_future.result()
                    monitored = [(item[1], item[2]) for item in selected] if len(selected) == 3 else []
                    if monitored:
                        controller.configure_candidates(monitored)
                        monitored = [(a, b) for a, b in monitored if controller.candidate_tradeable(a, b)]
                    entries_allowed = len(monitored) == 3 and engine.can_enter
                    monitored_bases = {a.base for a, _ in monitored}
                    selected_bases = {item[1].base for item in selected}
                    for row in scan_report:
                        if row['status'] == 'SELECTED':
                            if row['base'] in monitored_bases and len(monitored) == 3:
                                row['status'] = 'MONITORED'
                                row['reason'] = 'входит в текущую наблюдаемую корзину 3/3'
                            elif row['base'] in selected_bases and len(selected) < 3:
                                row['status'] = 'WAITING_FOR_3'
                                row['reason'] = 'пара проходит фильтры, но пока найдено меньше трёх подходящих пар'
                            elif row['base'] in monitored_bases:
                                row['status'] = 'WAITING_FOR_3'
                                row['reason'] = 'пара проходит фильтры, но для входа нужно набрать 3/3'
                            else:
                                row['status'] = 'REJECTED_AFTER_CONFIG'
                                row['reason'] = 'не прошла повторную проверку после настройки плеча'
                    summaries = []
                    for row in scan_report:
                        depth = (f"depth LONG ${row['long_depth_usd']:.2f} / "
                                 f"SHORT ${row['short_depth_usd']:.2f}"
                                 if row['long_depth_usd'] != '' else 'depth не проверена')
                        summaries.append(
                            f"{row['base']} [{row['bybit_symbol']} / {row['mexc_symbol']}] | "
                            f"spread {row['signed_spread_pct']:+.5f}% | "
                            f"Bybit {row['bybit_price']:.8g} / MEXC {row['mexc_price']:.8g} | "
                            f"LONG {row['long_exchange']} / SHORT {row['short_exchange']} | "
                            f"{depth} | {row['status']}: {row['reason']}")
                        logging.info(
                            "Basket candidate %s | symbols=%s/%s | spread=%+.6f%% | "
                            "price_bybit=%s price_mexc=%s | LONG=%s SHORT=%s | "
                            "depth_long=%s depth_short=%s target_notional=%s | status=%s | %s",
                            row['base'], row['bybit_symbol'], row['mexc_symbol'],
                            row['signed_spread_pct'], row['bybit_price'], row['mexc_price'],
                            row['long_exchange'], row['short_exchange'],
                            row['long_depth_usd'], row['short_depth_usd'],
                            row['target_notional_usd'], row['status'], row['reason'])
                    dashboard.update_candidate_status(summaries, scan_report)
                    logging.info('Basket scan: %s/3 monitored, %s books checked, %s above spread threshold',
                                 len(monitored), checked, len(scan_report))
                except LiveTradingError:
                    raise
                except Exception as exc:
                    logging.warning('Basket scan unavailable; entries paused: %s', exc)
                    entries_allowed = False
                finally:
                    scan_future = None
                    next_scan = max(next_scan, time.monotonic() + args.interval)
            was_open = engine.basket is not None
            if was_open:
                controller.process_basket(engine.active_pairs(), {}, False)
                if engine.basket is None:
                    monitored = []
                    entries_allowed = False
                    simulator.margin = basket_decimal(executor.margin)
                    simulator.leverage = basket_decimal(executor.leverage)
                    simulator.notional = basket_decimal(executor.notional)
                    next_scan = time.monotonic() + args.candidate_delay_sec
                    controller.refresh_balances()
            elif monitored and entries_allowed:
                # [PATCH 10] entry poll on its own cadence, not tied to --interval
                if time.monotonic() >= next_entry_poll:
                    try:
                        prices = live_prices_many(monitored)
                    except Exception as exc:
                        logging.warning('Basket ticker unavailable; no new entry: %s', exc)
                        prices = {}
                    if time.monotonic() >= next_monitor_log:
                        threshold = controller.entry_spread_threshold
                        for a, b in monitored:
                            current = prices.get(a.base)
                            if not current:
                                logging.info('Basket monitor %s | текущая цена недоступна; вход отложен', a.base)
                                continue
                            current_spread = basket_spread(current[0], current[1])
                            check = 'PASS' if abs(current_spread) > threshold else 'WAIT'
                            logging.info(
                                'Basket monitor %s | Bybit=%s MEXC=%s | current_spread=%+.6f%% '
                                '| требование >%s%% | entry_check=%s',
                                a.base, current[0], current[1], current_spread, threshold, check)
                        next_monitor_log = time.monotonic() + max(10, args.interval)
                    controller.process_basket(monitored, prices, True)
                    if engine.basket:
                        entries_allowed = False
                        monitored = engine.active_pairs()
                    elif controller.region_restriction_detected:
                        entries_allowed = False
                        next_scan = time.monotonic() + args.candidate_delay_sec
                    next_entry_poll = time.monotonic() + max(1, args.entry_poll_sec)
                else:
                    if engine.basket:
                        controller.process_basket(engine.active_pairs(), {}, False)
            dashboard.update_basket(simulator)
            remaining = 1 - (time.monotonic() - tick_started)
            if remaining > 0:
                time.sleep(remaining)
    except KeyboardInterrupt:
        logging.warning('Basket monitor stopped; any active basket is persisted for restart reconciliation')
    except BaseException as exc:
        if not engine.halted:
            try:
                engine.halt(f'Controller stopped fail-closed: {exc}')
            except LiveTradingError:
                pass
        if dashboard:
            dashboard.update_basket(simulator)
        raise
    finally:
        scan_executor.shutdown(wait=False, cancel_futures=True)
        try:
            engine.save_state()
        finally:
            if dashboard:
                dashboard.close()
            engine.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Сравнение предстоящих funding Bybit и MEXC")
    parser.add_argument("--once", action="store_true", help="выполнить одну сверку funding и завершить (без live-цикла)")
    parser.add_argument("--min-gap-pp", type=Decimal, default=Decimal("0"))
    parser.add_argument("--chart-gap-pp", type=Decimal, default=Decimal("0.01"))
    parser.add_argument("--chart-file", type=Path, default=Path("funding_comparison.png"))
    parser.add_argument("--interval", type=int, default=INTERVAL_SECONDS)
    parser.add_argument("--min-live-gap-pct", type=Decimal, default=Decimal("0"))
    parser.add_argument("--live-dir", type=Path, default=Path("live_chart"))
    parser.add_argument("--live-port", type=int, default=8765)
    parser.add_argument("--live-points", type=int, default=360)
    parser.add_argument("--no-browser", action="store_true")
    parser.add_argument("--initial-balance", type=Decimal, default=Decimal("10"))
    parser.add_argument("--margin-per-leg", type=Decimal, default=Decimal("0.6"))
    parser.add_argument("--leverage", type=Decimal, default=Decimal("10"))
    parser.add_argument("--open-gap-pct", type=Decimal, default=PRICE_OPEN_THRESHOLD_PCT)
    parser.add_argument("--close-gap-pct", type=Decimal, default=PRICE_CLOSE_THRESHOLD_PCT)
    parser.add_argument("--candidate-delay-sec", type=int, default=60)
    parser.add_argument("--min-hold-sec", type=int, default=MIN_HOLD_SECONDS)
    parser.add_argument("--bybit-taker-fee", type=Decimal, default=Decimal("0.00055"))
    parser.add_argument("--mexc-taker-fee", type=Decimal, default=Decimal("0.0008"))
    parser.add_argument("--trades-csv", type=Path, default=None)
    parser.add_argument("--state-file", type=Path, default=None)
    parser.add_argument("--exit-slippage-bps", type=Decimal, default=Decimal('2'))
    # [PATCH 11] entry poll cadence
    parser.add_argument("--entry-poll-sec", type=int, default=5,
                        help="как часто пытаться открыть корзину из готовых пар (сек); "
                             "полный скан рынка остаётся на --interval")
    args = parser.parse_args()
    for name in ('min_gap_pp', 'chart_gap_pp', 'min_live_gap_pct', 'initial_balance',
                 'margin_per_leg', 'leverage', 'open_gap_pct', 'close_gap_pct',
                 'bybit_taker_fee', 'mexc_taker_fee', 'exit_slippage_bps'):
        if not getattr(args, name).is_finite():
            parser.error(f'--{name.replace("_", "-")} must be finite')
    if args.interval < 1:
        parser.error("--interval должен быть положительным")
    if args.min_gap_pp < 0 or args.chart_gap_pp < 0 or args.min_live_gap_pct < 0:
        parser.error("пороги не могут быть отрицательными")
    if args.live_points < 2:
        parser.error("--live-points должен быть не меньше 2")
    if args.candidate_delay_sec < 0:
        parser.error("--candidate-delay-sec не может быть отрицательным")
    if args.min_hold_sec < 0:
        parser.error("--min-hold-sec не может быть отрицательным")
    if args.entry_poll_sec < 1:
        parser.error("--entry-poll-sec должен быть положительным")
    if args.initial_balance <= 0 or args.margin_per_leg <= 0 or args.leverage <= 0:
        parser.error("баланс, маржа и плечо должны быть положительными")
    if args.open_gap_pct < BASKET_ENTRY_SPREAD or args.close_gap_pct < 0:
        parser.error("basket entry threshold cannot be below 0.5%; close-gap is deprecated")
    if not args.exit_slippage_bps.is_finite() or args.exit_slippage_bps < 0:
        parser.error("--exit-slippage-bps must be finite and nonnegative")
    if not (0 <= args.bybit_taker_fee < 1 and 0 <= args.mexc_taker_fee < 1):
        parser.error("ставки комиссий должны быть в диапазоне [0, 1)")
    paper_flags = {"Bybit": DEMO or bybit_paper, "MEXC": DEMO or mexc_paper}
    has_live_exchange = not all(paper_flags.values())
    if has_live_exchange and args.leverage != args.leverage.to_integral_value():
        parser.error("для реальных ордеров --leverage должен быть целым числом")

    if args.once:
        compare_once(args.min_gap_pp, args.chart_gap_pp, args.chart_file)
        return

    mode_profile = ("paper" if all(paper_flags.values()) else
                    "live" if not any(paper_flags.values()) else
                    "hybrid_" + "_".join(f"{name.lower()}_{'paper' if paper_flags[name] else 'live'}"
                                          for name in ("Bybit", "MEXC")))
    mode_text = " | ".join(f"{name}={'PAPER' if paper_flags[name] else 'LIVE'}" for name in ("Bybit", "MEXC"))
    if has_live_exchange:
        logging.critical("DEMO=%s | %s — реальные market-ордера будут отправляться только на LIVE-биржи",
                         DEMO, mode_text)
    else:
        logging.warning("DEMO=%s | %s — реальные ордера отключены; работает paper trading",
                        DEMO, mode_text)

    logging.warning('--close-gap-pct/--min-hold-sec are legacy parameters: basket exits use aggregate projected net >= Decimal("0.10") or safety abort only')
    logging.warning('Basket archive (including interrupted/rolled-back attempts and components) is persisted in --state-file; legacy --trades-csv is not used')
    run_basket_monitor(args, paper_flags, mode_profile)


if __name__ == "__main__":
    main()
