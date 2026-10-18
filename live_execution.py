"""Direct REST order execution for Bybit V5 and MEXC Futures.

No credentials are bundled here. Live order calls are only made when the
caller explicitly enables live mode (DEMO=False) and imports key.py locally.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
from threading import Lock
from typing import Any
from urllib.parse import urlencode

import requests

if os.name == "nt":
    import msvcrt
else:
    import fcntl


def _lock_state_file(stream: Any) -> None:
    """Acquire a non-blocking exclusive lock using only standard-library APIs."""
    if os.name == "nt":
        stream.seek(0, os.SEEK_END)
        if stream.tell() == 0:
            stream.write(b"\0")
            stream.flush()
        stream.seek(0)
        msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
    else:
        fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)


def _unlock_state_file(stream: Any) -> None:
    """Release the exclusive state-file lock."""
    if os.name == "nt":
        stream.seek(0)
        msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
    else:
        fcntl.flock(stream.fileno(), fcntl.LOCK_UN)

BYBIT_URL = "https://api.bybit.com"
MEXC_URL = "https://api.mexc.com"
LIVE_TIMEOUT = 5
ORDER_WAIT_SECONDS = 8
POSITION_WAIT_SECONDS = 8


class LiveTradingError(RuntimeError):
    """An API, setup, fill, or reconciliation issue that must stop live entries."""


class EntryRejectedAfterRollback(LiveTradingError):
    """A candidate entry was safely rolled back and may be skipped without stopping."""


class OrderOutcomeUnknown(LiveTradingError):
    """An order may still execute; observing flat positions is not sufficient."""


class MexcRegionRestrictedError(LiveTradingError):
    """MEXC refused a new position for a contract because of regional/risk rules."""

    def __init__(self, symbol: str, message: str):
        self.symbol = symbol
        super().__init__(message)


def _floor_step(value: Decimal, step: Decimal) -> Decimal:
    if step <= 0:
        raise LiveTradingError(f"Недопустимый шаг объёма: {step}")
    return (value / step).to_integral_value(rounding=ROUND_DOWN) * step


def _d(value: Any) -> Decimal:
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal(0)


def load_keys(bybit_required: bool = True, mexc_required: bool = True) -> tuple[str, str, str, str]:
    if not bybit_required and not mexc_required:
        return "", "", "", ""
    try:
        import key  # user's local, untracked key.py
    except ImportError as exc:
        raise LiveTradingError("Для выбранной live-биржи создайте локальный key.py по примеру key.py.example") from exc
    values = (
        getattr(key, "BYBIT_API_KEY", ""), getattr(key, "BYBIT_API_SECRET", ""),
        getattr(key, "MEXC_API_KEY", ""), getattr(key, "MEXC_API_SECRET", ""),
    )
    required = []
    if bybit_required:
        required.extend(values[:2])
    if mexc_required:
        required.extend(values[2:])
    if not all(isinstance(v, str) and v.strip() and "PASTE_" not in v for v in required):
        exchanges = " и ".join(name for name, active in (("Bybit", bybit_required), ("MEXC", mexc_required)) if active)
        raise LiveTradingError(f"В key.py задайте API-поля только для live-биржи(бирж): {exchanges}; ключи не печатаются в лог")
    return tuple(v.strip() for v in values)  # type: ignore[return-value]


class BybitREST:
    def __init__(self, api_key: str, api_secret: str, session: requests.Session | None = None):
        self.api_key, self.api_secret = api_key, api_secret
        self.session = session or requests.Session()

    def request(self, method: str, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        params = params or {}
        timestamp = str(int(time.time() * 1000))
        recv_window = "5000"
        method = method.upper()
        if method == "GET":
            query = urlencode(sorted((k, str(v)) for k, v in params.items()))
            payload = timestamp + self.api_key + recv_window + query
            url = BYBIT_URL + path + ("?" + query if query else "")
            body = None
        else:
            query = ""
            body = json.dumps(params, separators=(",", ":"), ensure_ascii=False)
            payload = timestamp + self.api_key + recv_window + body
            url = BYBIT_URL + path
        signature = hmac.new(self.api_secret.encode(), payload.encode(), hashlib.sha256).hexdigest()
        headers = {
            "X-BAPI-API-KEY": self.api_key,
            "X-BAPI-TIMESTAMP": timestamp,
            "X-BAPI-RECV-WINDOW": recv_window,
            "X-BAPI-SIGN": signature,
            "Content-Type": "application/json",
        }
        response = self.session.request(method, url, data=body, headers=headers, timeout=LIVE_TIMEOUT)
        response.raise_for_status()
        payload_json = response.json()
        ret_code = payload_json.get("retCode")
        if (str(ret_code) == "110043" and method == "POST"
                and path == "/v5/position/set-leverage"):
            logging.info("Bybit %s: плечо уже установлено (110043); продолжаю", path)
            return payload_json.get("result", {})
        if ret_code != 0:
            raise LiveTradingError(f"Bybit API {path}: {ret_code} {payload_json.get('retMsg')}")
        return payload_json.get("result", {})

    def open_positions(self, symbol: str | None = None) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"category": "linear", "settleCoin": "USDT", "limit": 200}
        if symbol:
            params.pop("settleCoin", None)
            params["symbol"] = symbol
        rows: list[dict[str, Any]] = []
        cursors: set[str] = set()
        while True:
            result = self.request("GET", "/v5/position/list", params)
            if not isinstance(result.get("list"), list):
                raise LiveTradingError("Bybit position inventory is incomplete")
            rows.extend(p for p in result["list"] if basket_decimal(p.get("size")) > 0)
            cursor = result.get("nextPageCursor") or ""
            if not cursor:
                return rows
            if cursor in cursors:
                raise LiveTradingError("Bybit position pagination repeated a cursor")
            cursors.add(cursor)
            params["cursor"] = cursor

    def open_orders(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        cursor = ""
        while True:
            params: dict[str, Any] = {"category": "linear", "settleCoin": "USDT", "limit": 50, "openOnly": 0}
            if cursor:
                params["cursor"] = cursor
            result = self.request("GET", "/v5/order/realtime", params)
            rows.extend(result.get("list", []))
            cursor = result.get("nextPageCursor") or ""
            if not cursor:
                return rows

    def wallet_available(self) -> Decimal:
        result = self.request("GET", "/v5/account/wallet-balance", {"accountType": "UNIFIED", "coin": "USDT"})
        rows = result.get("list", [])
        if not rows:
            return Decimal(0)
        return _d(rows[0].get("totalAvailableBalance"))

    def balance_metrics(self) -> dict[str, Decimal]:
        result = self.request("GET", "/v5/account/wallet-balance", {
            "accountType": "UNIFIED", "coin": "USDT",
        })
        rows = result.get("list", [])
        if not rows:
            raise LiveTradingError("Bybit wallet balance returned no account rows")
        row = rows[0]
        return {
            "equity": _d(row.get("totalEquity")),
            "wallet_balance": _d(row.get("totalWalletBalance")),
            "available_balance": _d(row.get("totalAvailableBalance")),
            "unrealized_pnl": _d(row.get("totalPerpUPL")),
        }

    def order(self, symbol: str, side: str, qty: Decimal, position_idx: int,
              reduce_only: bool = False) -> dict[str, Any]:
        body = {
            "category": "linear", "symbol": symbol, "side": side, "orderType": "Market",
            "qty": format(qty, "f"), "positionIdx": position_idx,
            "reduceOnly": reduce_only, "orderLinkId": "FM" + uuid.uuid4().hex[:32],
        }
        try:
            result = self.request("POST", "/v5/order/create", body)
        except (requests.RequestException, ValueError, TypeError) as exc:
            raise OrderOutcomeUnknown(f"Bybit create outcome unknown for {symbol}") from exc
        order_id = result.get("orderId")
        if not order_id:
            raise OrderOutcomeUnknown(f"Bybit did not return orderId for {symbol}")
        deadline = time.monotonic() + ORDER_WAIT_SECONDS
        last: dict[str, Any] = {}
        while time.monotonic() < deadline:
            try:
                last_result = self.request("GET", "/v5/order/realtime", {
                    "category": "linear", "symbol": symbol, "orderId": order_id,
                })
            except Exception as exc:
                raise OrderOutcomeUnknown(f"Bybit order {order_id} confirmation unavailable") from exc
            rows = last_result.get("list", [])
            if rows:
                last = rows[0]
                status = last.get("orderStatus")
                if status in ("Filled", "Cancelled", "Rejected", "PartiallyFilledCanceled"):
                    break
            time.sleep(0.2)
        if last.get("orderStatus") not in ("Filled", "Cancelled", "Rejected", "PartiallyFilledCanceled"):
            raise OrderOutcomeUnknown(f"Bybit order {order_id} is not confirmed terminal")
        filled = _d(last.get("cumExecQty"))
        avg = _d(last.get("avgPrice"))
        if filled <= 0 or avg <= 0:
            raise LiveTradingError(f"Bybit order {order_id} was not confirmed filled ({last.get('orderStatus', 'unknown')})")
        if last.get("orderStatus") != "Filled":
            raise LiveTradingError(f"Bybit order {order_id} was not fully filled ({last.get('orderStatus')}); partial exposure will be unwound")
        fee_detail = last.get("cumFeeDetail") or {}
        fee = _d(fee_detail.get("USDT")) if isinstance(fee_detail, dict) else Decimal(0)
        if fee <= 0:
            fee = _d(last.get("cumExecFee"))
        return {"order_id": str(order_id), "qty": filled, "base_qty": filled,
                "avg_price": avg, "fee": fee,
                "status": last.get("orderStatus", "unknown")}


class MEXCREST:
    _pace_lock = Lock()
    _next_call = 0.0

    def __init__(self, api_key: str, api_secret: str, session: requests.Session | None = None):
        self.api_key, self.api_secret = api_key, api_secret
        self.session = session or requests.Session()

    def request(self, method: str, path: str, params: dict[str, Any] | None = None) -> Any:
        method = method.upper()
        params = params or {}
        if method == "GET":
            query = urlencode(sorted((k, str(v)) for k, v in params.items()))
            param_string = query
            body = None
            url = MEXC_URL + path + ("?" + query if query else "")
        else:
            body = json.dumps(params, separators=(",", ":"), ensure_ascii=False)
            param_string = body
            url = MEXC_URL + path
        with self._pace_lock:
            delay = self._next_call - time.monotonic()
            if delay > 0:
                time.sleep(delay)
            type(self)._next_call = time.monotonic() + 0.12
        timestamp = str(int(time.time() * 1000))
        to_sign = self.api_key + timestamp + param_string
        signature = hmac.new(self.api_secret.encode(), to_sign.encode(), hashlib.sha256).hexdigest()
        headers = {"ApiKey": self.api_key, "Request-Time": timestamp, "Signature": signature,
                   "Content-Type": "application/json", "Language": "English"}
        response = self.session.request(method, url, data=body, headers=headers, timeout=LIVE_TIMEOUT)
        response.raise_for_status()
        data = response.json()
        if not data.get("success") or data.get("code") not in (0, "0", None):
            raise LiveTradingError(f"MEXC API {path}: code={data.get('code')} {data.get('message', '')}")
        return data.get("data")

    def open_positions(self, symbol: str | None = None) -> list[dict[str, Any]]:
        rows = self.request("GET", "/api/v1/private/position/open_positions",
                            {"symbol": symbol} if symbol else None) or []
        if isinstance(rows, dict):
            rows = [rows]
        result: list[dict[str, Any]] = []
        for p in rows:
            volume = basket_decimal(p.get("holdVol"))
            state = basket_decimal(p.get("state"))
            if volume < 0:
                raise LiveTradingError("MEXC position inventory contains a negative quantity")
            if volume == 0:
                continue
            if state != 1:
                raise LiveTradingError("MEXC positive position has an unknown/non-open state")
            if symbol and p.get("symbol") != symbol:
                continue
            normalized = dict(p)
            direction = p.get("positionType")
            if direction is None:
                direction = p.get("side", 0)
            normalized["_direction"] = int(direction or 0)
            result.append(normalized)
        return result

    def open_orders(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        page = 1
        while True:
            part = self.request("GET", "/api/v1/private/order/list/open_orders",
                                {"page_num": page, "page_size": 100}) or []
            if isinstance(part, dict):
                part = part.get("list", part.get("data", []))
            rows.extend(part)
            if len(part) < 100:
                return rows
            page += 1

    def wallet_available(self) -> Decimal:
        rows = self.request("GET", "/api/v1/private/account/assets") or []
        if isinstance(rows, dict):
            rows = [rows]
        for row in rows:
            if row.get("currency") == "USDT":
                return _d(row.get("availableBalance"))
        return Decimal(0)

    def balance_metrics(self) -> dict[str, Decimal]:
        rows = self.request("GET", "/api/v1/private/account/assets") or []
        if isinstance(rows, dict):
            rows = [rows]
        row = next((item for item in rows if item.get("currency") == "USDT"), None)
        if row is None:
            raise LiveTradingError("MEXC account assets returned no USDT asset")
        return {
            "equity": _d(row.get("equity")),
            "wallet_balance": _d(row.get("cashBalance")),
            "available_balance": _d(row.get("availableBalance")),
            "unrealized_pnl": _d(row.get("unrealized")),
            "position_margin": _d(row.get("positionMargin")),
            "frozen_balance": _d(row.get("frozenBalance")),
            "withdrawable_balance": _d(row.get("cashBalance")),
        }

    def order(self, symbol: str, price: Decimal, vol: Decimal, side: int,
              leverage: int, position_id: int | None = None) -> dict[str, Any]:
        body: dict[str, Any] = {
            "symbol": symbol, "price": format(price, "f"), "vol": format(vol, "f"),
            "leverage": leverage, "side": side, "type": 5, "openType": 2, "positionMode": 1,
        }
        if position_id is not None:
            body["positionId"] = position_id
        body["externalOid"] = "FM" + uuid.uuid4().hex[:30]
        try:
            result = self.request("POST", "/api/v1/private/order/create", body)
        except LiveTradingError as exc:
            if position_id is None and "code=8950" in str(exc):
                raise MexcRegionRestrictedError(symbol, str(exc)) from exc
            raise
        except (requests.RequestException, ValueError, TypeError) as exc:
            raise OrderOutcomeUnknown(f"MEXC create outcome unknown for {symbol}") from exc
        order_id = result.get("orderId") if isinstance(result, dict) else None
        if not order_id:
            raise OrderOutcomeUnknown(f"MEXC did not return orderId for {symbol}")
        deadline = time.monotonic() + ORDER_WAIT_SECONDS
        last: dict[str, Any] = {}
        while time.monotonic() < deadline:
            try:
                last = self.request("GET", f"/api/v1/private/order/get/{order_id}") or {}
            except Exception as exc:
                raise OrderOutcomeUnknown(f"MEXC order {order_id} confirmation unavailable") from exc
            if int(last.get("state", 0) or 0) in (3, 4, 5):
                break
            time.sleep(0.2)
        if int(last.get("state", 0) or 0) not in (3, 4, 5):
            raise OrderOutcomeUnknown(f"MEXC order {order_id} is not confirmed terminal")
        filled = _d(last.get("dealVol"))
        avg = _d(last.get("dealAvgPrice"))
        if filled <= 0 or avg <= 0:
            raise LiveTradingError(f"MEXC order {order_id} was not confirmed filled (state={last.get('state')})")
        if int(last.get("state", 0) or 0) != 3:
            raise LiveTradingError(f"MEXC order {order_id} was not fully filled (state={last.get('state')}); partial exposure will be unwound")
        return {"order_id": str(order_id), "qty": filled, "avg_price": avg,
                "fee": _d(last.get("takerFee")), "status": last.get("state")}


class LiveExecution:
    """Mixed paper/live paired executor; credentials and orders only for live legs."""
    def __init__(self, leverage: int = 10, margin_per_leg: Decimal = Decimal("1"),
                 bybit_paper: bool = False, mexc_paper: bool = False):
        self.bybit_paper = bybit_paper
        self.mexc_paper = mexc_paper
        bk, bs, mk, ms = load_keys(not bybit_paper, not mexc_paper)
        self.bybit = None if bybit_paper else BybitREST(bk, bs)
        self.mexc = None if mexc_paper else MEXCREST(mk, ms)
        self.leverage = leverage
        self.margin = margin_per_leg
        self.notional = margin_per_leg * Decimal(leverage)
        self.configured_bybit: set[str] = set()
        self.configured_mexc: set[str] = set()
        self.untradeable_pairs: set[tuple[str, str]] = set()
        self.prepared = False

    def configure_account(self) -> None:
        bp = self.bybit.open_positions() if self.bybit else []
        mp = self.mexc.open_positions() if self.mexc else []
        bo = self.bybit.open_orders() if self.bybit else []
        mo = self.mexc.open_orders() if self.mexc else []
        if bp or mp or bo or mo:
            raise LiveTradingError("На аккаунте уже есть фьючерсные позиции или открытые ордера. Для первичной live-настройки нужны пустые аккаунты; существующие данные автоматически не трогаю.")
        if self.bybit:
            account_info = self.bybit.request("GET", "/v5/account/info")
            if account_info.get("marginMode") != "REGULAR_MARGIN":
                self.bybit.request("POST", "/v5/account/set-margin-mode", {"setMarginMode": "REGULAR_MARGIN"})
            try:
                self.bybit.request("POST", "/v5/position/switch-mode", {"category": "linear", "coin": "USDT", "mode": 3})
            except LiveTradingError as exc:
                if "110025" not in str(exc):
                    raise
        if self.mexc:
            self._mexc_config_request("/api/v1/private/position/change_position_mode", {"positionMode": 1})
        self.prepared = True
        active = [name for name, paper in (("Bybit", self.bybit_paper), ("MEXC", self.mexc_paper)) if not paper]
        logging.warning("LIVE account configuration applied to %s", ", ".join(active))

    def prepare_account(self) -> None:
        if not self.prepared:
            self.configure_account()
        else:
            self.flat_account()

    def _mexc_config_request(self, path: str, params: dict[str, Any]) -> Any:
        for attempt in range(4):
            try:
                return self.mexc.request("POST", path, params)
            except LiveTradingError as exc:
                if "510" not in str(exc) or attempt == 3:
                    raise
                delay = 1.0 + attempt
                logging.warning("MEXC throttled configuration request; retry in %.1f sec (%d/3)", delay, attempt + 1)
                time.sleep(delay)
        raise LiveTradingError("MEXC configuration retry exhausted")

    def configure_symbols(self, rows: list[tuple[Any, Any, Any, Any]]) -> None:
        self.prepare_account()
        pairs: dict[tuple[str, str], tuple[Any, Any]] = {}
        for row in rows:
            a, b = row[2], row[3]
            pairs[(a.exchange_symbol, b.exchange_symbol)] = (a, b)

        retried_at_x10: set[tuple[str, str]] = set()
        items = list(pairs.items())
        i = 0
        while i < len(items):
            (bybit_symbol, mexc_symbol), (a, b) = items[i]
            pair_key = (bybit_symbol, mexc_symbol)
            if pair_key in self.untradeable_pairs:
                i += 1
                continue

            if self.bybit and bybit_symbol not in self.configured_bybit:
                try:
                    self.bybit.request("POST", "/v5/position/set-leverage", {
                        "category": "linear", "symbol": bybit_symbol,
                        "buyLeverage": str(self.leverage), "sellLeverage": str(self.leverage),
                    })
                    self.configured_bybit.add(bybit_symbol)
                except LiveTradingError as exc:
                    if "110013" not in str(exc):
                        raise
                    if self.leverage != 10:
                        logging.warning(
                            "Bybit 110013 для %s при x%s; переключаю весь запуск на x10 "
                            "и пересчитываю номинал", bybit_symbol, self.leverage,
                        )
                        self.leverage = 10
                        self.notional = self.margin * Decimal("10")
                        self.configured_bybit.clear()
                        self.configured_mexc.clear()
                        retried_at_x10.add(pair_key)
                        i = 0
                        continue

                    if pair_key not in retried_at_x10:
                        logging.warning(
                            "Bybit 110013 для %s при x10; повторяю установку x10 один раз",
                            bybit_symbol,
                        )
                        retried_at_x10.add(pair_key)
                        continue

                    self.untradeable_pairs.add(pair_key)
                    logging.error(
                        "Пара %s / %s пропущена: Bybit повторно вернул 110013 при x10",
                        bybit_symbol, mexc_symbol,
                    )
                    i += 1
                    continue

            if self.mexc and mexc_symbol not in self.configured_mexc:
                for position_type in (1, 2):
                    self._mexc_config_request("/api/v1/private/position/change_leverage", {
                        "symbol": mexc_symbol, "leverage": self.leverage,
                        "openType": 2, "positionType": position_type,
                    })
                self.configured_mexc.add(mexc_symbol)

            if (i + 1) % 10 == 0:
                logging.info("Live-настройка плеча: %d/%d общих контрактных пар", i + 1, len(items))
            i += 1

        active = [name for name, paper in (("Bybit", self.bybit_paper), ("MEXC", self.mexc_paper)) if not paper]
        if active:
            ready_count = sum(1 for pair_key, _ in items if pair_key not in self.untradeable_pairs)
            logging.info(
                "Плечо x%d задано для %d/%d пар на live-биржах: %s",
                self.leverage, ready_count, len(items), ", ".join(active),
            )

    def positions(self, a: Any, b: Any) -> dict[str, list[dict[str, Any]]]:
        rows: dict[str, list[dict[str, Any]]] = {"Bybit": [], "MEXC": []}
        with ThreadPoolExecutor(max_workers=2, thread_name_prefix="position-check") as pool:
            futures = {}
            if self.bybit:
                futures[pool.submit(self.bybit.open_positions, a.exchange_symbol)] = "Bybit"
            if self.mexc:
                futures[pool.submit(self.mexc.open_positions, b.exchange_symbol)] = "MEXC"
            for future, name in futures.items():
                rows[name] = future.result()
        return rows

    def flat_account(self) -> None:
        if ((self.bybit and (self.bybit.open_positions() or self.bybit.open_orders())) or
                (self.mexc and (self.mexc.open_positions() or self.mexc.open_orders()))):
            raise LiveTradingError("Обнаружены открытые фьючерсные позиции/ордера; изменения настроек остановлены без управления ими")

    def balance_snapshot(self) -> dict[str, Decimal]:
        result: dict[str, Decimal] = {}
        if self.bybit:
            result["Bybit"] = self.bybit.wallet_available()
        if self.mexc:
            result["MEXC"] = self.mexc.wallet_available()
        return result

    def account_metrics(self) -> dict[str, dict[str, Decimal]]:
        result: dict[str, dict[str, Decimal]] = {}
        with ThreadPoolExecutor(max_workers=2, thread_name_prefix="balance-check") as pool:
            futures = {}
            if self.bybit:
                futures[pool.submit(self.bybit.balance_metrics)] = "Bybit"
            if self.mexc:
                futures[pool.submit(self.mexc.balance_metrics)] = "MEXC"
            for future, name in futures.items():
                result[name] = future.result()
        return result

    def _bybit_spec_qty(self, a: Any) -> Decimal:
        step = getattr(a, "qty_step", Decimal(0))
        qty = _floor_step(self.notional / a.price, step)
        minimum = getattr(a, "min_qty", Decimal(0))
        actual = qty * a.price
        if qty < minimum or actual < self.notional * Decimal("0.95"):
            raise LiveTradingError(f"Bybit {a.exchange_symbol}: $ {self.notional} номинала не проходит минимальный шаг/объём; ордер пропущен")
        return qty

    def _mexc_spec_vol(self, b: Any) -> Decimal:
        contract_size = getattr(b, "contract_size", Decimal(0))
        step = getattr(b, "vol_step", Decimal(0))
        if contract_size <= 0:
            raise LiveTradingError(f"MEXC {b.exchange_symbol}: в данных нет contractSize")
        vol = _floor_step(self.notional / (b.price * contract_size), step)
        minimum = getattr(b, "min_vol", Decimal(0))
        actual = vol * contract_size * b.price
        if vol < minimum or actual < self.notional * Decimal("0.95"):
            raise LiveTradingError(f"MEXC {b.exchange_symbol}: $ {self.notional} номинала не проходит minVol/шаг контракта; ордер пропущен")
        return vol

    def pair_is_tradeable(self, a: Any, b: Any) -> bool:
        if (a.exchange_symbol, b.exchange_symbol) in self.untradeable_pairs:
            return False
        lower, upper = self.notional * Decimal("0.90"), self.notional * Decimal("1.10")
        if self.bybit:
            try:
                bybit_notional = self._bybit_spec_qty(a) * a.price
            except LiveTradingError:
                return False
            if not lower <= bybit_notional <= upper:
                return False
        if self.mexc:
            try:
                mexc_notional = self._mexc_spec_vol(b) * b.contract_size * b.price
            except LiveTradingError:
                return False
            if not lower <= mexc_notional <= upper:
                return False
        return True

    def _open_one(self, exchange: str, rate: Any, is_long: bool) -> dict[str, Any]:
        if exchange == "Bybit":
            if self.bybit_paper:
                if rate.price <= 0:
                    raise LiveTradingError(f"Bybit paper {rate.exchange_symbol}: нет цены для моделирования")
                base_qty = self.notional / rate.price
                return {"order_id": "PAPER", "qty": base_qty, "base_qty": base_qty,
                        "avg_price": rate.price, "fee": Decimal(0), "status": "PAPER"}
            qty = self._bybit_spec_qty(rate)
            fill = self.bybit.order(rate.exchange_symbol, "Buy" if is_long else "Sell", qty,
                                    1 if is_long else 2, False)
            return {**fill, "base_qty": fill["qty"], "expected_qty": qty}
        if self.mexc_paper:
            if rate.price <= 0:
                raise LiveTradingError(f"MEXC paper {rate.exchange_symbol}: нет цены для моделирования")
            if rate.contract_size <= 0:
                raise LiveTradingError(f"MEXC paper {rate.exchange_symbol}: нет contractSize для моделирования")
            base_qty = self.notional / rate.price
            vol = base_qty / rate.contract_size
            return {"order_id": "PAPER", "qty": vol, "base_qty": base_qty,
                    "avg_price": rate.price, "fee": Decimal(0), "status": "PAPER"}
        vol = self._mexc_spec_vol(rate)
        fill = self.mexc.order(rate.exchange_symbol, rate.price, vol, 1 if is_long else 3,
                               self.leverage)
        return {**fill, "base_qty": fill["qty"] * rate.contract_size,
                "expected_qty": vol, "contract_size": rate.contract_size}

    def open_pair(self, a: Any, b: Any, long_exchange: str) -> dict[str, Any]:
        if (a.exchange_symbol, b.exchange_symbol) in self.untradeable_pairs:
            raise LiveTradingError(
                f"Пара {a.exchange_symbol}/{b.exchange_symbol} исключена после ошибки настройки плеча"
            )
        required = self.margin * Decimal("1.10")
        balances = self.balance_snapshot()
        if any(balances.get(name, Decimal(0)) < required for name, paper in
               (("Bybit", self.bybit_paper), ("MEXC", self.mexc_paper)) if not paper):
            details = ", ".join(f"{name} ${balances.get(name, Decimal(0)):.2f}"
                                 for name, paper in (("Bybit", self.bybit_paper), ("MEXC", self.mexc_paper))
                                 if not paper)
            raise LiveTradingError(f"Недостаточно свободного USDT для запаса маржи/комиссий на live-бирже: {details}")
        rates = {"Bybit": a, "MEXC": b}
        jobs = {}
        errors = {}
        fills: dict[str, dict[str, Any]] = {}
        with ThreadPoolExecutor(max_workers=2, thread_name_prefix="live-open") as pool:
            for exchange, rate in rates.items():
                jobs[pool.submit(self._open_one, exchange, rate, exchange == long_exchange)] = exchange
            for future, exchange in jobs.items():
                try:
                    fills[exchange] = future.result()
                except Exception as exc:
                    errors[exchange] = exc
        if errors:
            region_error = next(
                (exc for exc in errors.values() if isinstance(exc, MexcRegionRestrictedError)),
                None,
            )
            unknown_error = next((exc for exc in errors.values() if isinstance(exc, OrderOutcomeUnknown)), None)
            try:
                self.close_pair(a, b, long_exchange)
            except Exception as unwind_exc:
                if unknown_error:
                    raise OrderOutcomeUnknown(f"Entry and unwind remain ambiguous: {unwind_exc}") from unknown_error
                raise LiveTradingError(f"Одна нога не открылась; аварийное закрытие второй также не подтвердилось. Немедленно проверьте позиции на биржах: {unwind_exc}") from next(iter(errors.values()))
            if unknown_error:
                raise OrderOutcomeUnknown(f"Pair entry outcome remains ambiguous after unwind: {unknown_error}") from unknown_error
            if region_error:
                raise region_error
            raise LiveTradingError(f"Не удалось открыть обе ноги; исполненная нога аварийно закрыта. Ошибка: {next(iter(errors.values()))}")
        lower_ratio = Decimal("0.90")
        upper_ratio = Decimal("1.10")
        for name, fill in fills.items():
            actual = fill["base_qty"] * fill["avg_price"]
            if actual < self.notional * lower_ratio or actual > self.notional * upper_ratio:
                try:
                    self.close_pair(a, b, long_exchange)
                except Exception as unwind_exc:
                    raise LiveTradingError(f"Размер неполного исполнения вне допуска; аварийное закрытие не подтверждено: {unwind_exc}")
                raise LiveTradingError(f"Размер исполнения {name} (${actual:.4f}) вне допуска к цели ${self.notional}; обе ноги закрыты")
        current: dict[str, list[dict[str, Any]]] | None = None
        last_exc: Exception | None = None
        for attempt in range(6):
            try:
                current = self.positions(a, b)
                expected_bybit_idx = 1 if long_exchange == "Bybit" else 2
                expected_mexc_side = 1 if long_exchange == "MEXC" else 2
                b_visible = self.bybit_paper or any(
                    int(p.get("positionIdx", -1)) == expected_bybit_idx and _d(p.get("size")) > 0
                    for p in current["Bybit"]
                )
                m_visible = self.mexc_paper or any(
                    int(p.get("_direction", 0)) == expected_mexc_side and _d(p.get("holdVol")) > 0
                    for p in current["MEXC"]
                )
                if b_visible and m_visible:
                    break
                time.sleep(0.4)
            except Exception as exc:
                last_exc = exc
                time.sleep(0.4)

        if current is None:
            try:
                self.close_pair(a, b, long_exchange)
            except Exception as unwind_exc:
                raise LiveTradingError(
                    f"Не удалось сверить исполненные позиции и подтвердить откат; "
                    f"проверьте live-биржу немедленно: {unwind_exc}"
                ) from last_exc
            raise LiveTradingError(
                f"Не удалось сверить исполненные позиции; live-вход откачен: {last_exc}"
            ) from last_exc

        expected_bybit_idx = 1 if long_exchange == "Bybit" else 2

        unexpected_b = [] if self.bybit_paper else [
            p for p in current["Bybit"] if int(p.get("positionIdx", -1)) != expected_bybit_idx
        ]
        unexpected_m = [] if self.mexc_paper else [
            p for p in current["MEXC"] if int(p.get("_direction", 0)) != expected_mexc_side
        ]

        b_open = self.bybit_paper or any(
            int(p.get("positionIdx", -1)) == expected_bybit_idx and _d(p.get("size")) > 0
            for p in current["Bybit"]
        )
        m_open = self.mexc_paper or any(
            int(p.get("_direction", 0)) == expected_mexc_side and _d(p.get("holdVol")) > 0
            for p in current["MEXC"]
        )
        if not b_open or not m_open or unexpected_b or unexpected_m:
            try:
                self.close_pair(a, b, long_exchange)
            except Exception as unwind_exc:
                raise LiveTradingError(f"API не подтвердил обе ноги; аварийное закрытие не подтверждено: {unwind_exc}")
            raise LiveTradingError("API не подтвердил обе ожидаемые позиции либо обнаружил неизвестную hedge-сторону; вход откачен")

        actual_spread = (fills["Bybit"]["avg_price"] / fills["MEXC"]["avg_price"] - Decimal(1)) * Decimal(100)
        direction_matches = ((long_exchange == "MEXC" and actual_spread > 0) or
                             (long_exchange == "Bybit" and actual_spread < 0))
        if not direction_matches:
            try:
                self.close_pair(a, b, long_exchange)
            except Exception as unwind_exc:
                raise LiveTradingError(
                    "Fill-цены перевернули направление входа, и откат не подтверждён; "
                    f"немедленно проверьте позиции MEXC/Bybit: {unwind_exc}"
                ) from unwind_exc
            raise EntryRejectedAfterRollback(
                f"Вход отменён и обе live-ноги откачены: фактический fill-спред "
                f"{actual_spread:+.5f}% противоречит выбранному LONG {long_exchange}; "
                f"Bybit order={fills['Bybit'].get('order_id')}, "
                f"MEXC order={fills['MEXC'].get('order_id')}"
            )
        return fills

    def _close_bybit(self, symbol: str, position_idx: int, price_rate: Any) -> dict[str, Any] | None:
        positions = self.bybit.open_positions(symbol)
        position = next((p for p in positions if int(p.get("positionIdx", -1)) == position_idx), None)
        if not position or _d(position.get("size")) <= 0:
            return None
        side = "Sell" if position_idx == 1 else "Buy"
        return self.bybit.order(symbol, side, _d(position["size"]), position_idx, True)

    def _close_mexc(self, symbol: str, position_type: int, rate: Any) -> list[dict[str, Any]]:
        if position_type not in (1, 2):
            raise LiveTradingError("MEXC close requires an explicitly owned direction")
        positions = self.mexc.open_positions(symbol)
        results: list[dict[str, Any]] = []
        for position in positions:
            vol = _d(position.get("holdVol"))
            if vol <= 0:
                continue
            direction = int(position.get("_direction", 0) or 0)
            if direction != position_type:
                raise LiveTradingError(f"MEXC {symbol}: unknown/opposite direction; no blind liquidation")
            if direction == 1:
                side = 4
            elif direction == 2:
                side = 2
            else:
                raise LiveTradingError(
                    f"MEXC {symbol}: невозможно определить сторону позиции "
                    f"для закрытия (side={position.get('side')}, "
                    f"positionType={position.get('positionType')})"
                )
            position_id = int(position.get("positionId", 0)) or None
            fill = self.mexc.order(symbol, rate.price, vol, side,
                                   self.leverage, position_id)
            fill["base_qty"] = fill["qty"] * getattr(rate, "contract_size", Decimal(0))
            results.append(fill)
        return results

    def close_pair(self, a: Any, b: Any, long_exchange: str) -> dict[str, Any]:
        bybit_idx = 1 if long_exchange == "Bybit" else 2
        mexc_direction = 1 if long_exchange == "MEXC" else 2
        results: dict[str, Any] = {"Bybit": None, "MEXC": []}
        deadline = time.monotonic() + POSITION_WAIT_SECONDS
        retry_at = 0.0
        while time.monotonic() < deadline:
            remaining = self.positions(a, b)
            b_open = any(
                int(p.get("positionIdx", -1)) == bybit_idx and _d(p.get("size")) > 0
                for p in remaining["Bybit"]
            )
            m_open = any(
                int(p.get("_direction", 0)) == mexc_direction and _d(p.get("holdVol")) > 0
                for p in remaining["MEXC"]
            )
            if not b_open and not m_open:
                return results
            if time.monotonic() >= retry_at:
                jobs = {}
                with ThreadPoolExecutor(max_workers=2, thread_name_prefix="live-close") as pool:
                    if b_open:
                        jobs[pool.submit(self._close_bybit, a.exchange_symbol, bybit_idx, a)] = "Bybit"
                    if m_open:
                        jobs[pool.submit(self._close_mexc, b.exchange_symbol, mexc_direction, b)] = "MEXC"
                    for future, exchange in jobs.items():
                        try:
                            value = future.result()
                            if exchange == "Bybit" and value:
                                previous = results["Bybit"]
                                if previous:
                                    total_qty = previous["base_qty"] + value["base_qty"]
                                    value = {**value, "base_qty": total_qty,
                                             "avg_price": (previous["avg_price"] * previous["base_qty"] +
                                                           value["avg_price"] * value["base_qty"]) / total_qty,
                                             "fee": previous.get("fee", Decimal(0)) + value.get("fee", Decimal(0))}
                                results["Bybit"] = value
                            elif exchange == "MEXC" and value:
                                results["MEXC"].extend(value)
                        except Exception as exc:
                            logging.warning("Повтор закрытия %s не подтверждён; проверю остаток и повторю: %s", exchange, exc)
                retry_at = time.monotonic() + 0.75
            time.sleep(0.2)
        remaining = self.positions(a, b)
        raise LiveTradingError(
            f"Не подтверждено закрытие обеих ног "
            f"(Bybit positions={len(remaining['Bybit'])}, "
            f"MEXC positions={len(remaining['MEXC'])}); новые ордера остановлены"
        )

    def prepare_restored_position(self, simulator: Any) -> bool:
        trade = simulator.open_trade
        if not trade or not simulator.pair:
            return False
        base, bsym, msym = simulator.pair
        last = simulator.last_prices or (trade["entry_prices"]["Bybit"], trade["entry_prices"]["MEXC"])
        from types import SimpleNamespace
        a = SimpleNamespace(exchange_symbol=bsym, base=base, price=last[0])
        b = SimpleNamespace(exchange_symbol=msym, base=base, price=last[1])
        long_exchange = trade["long_exchange"]
        bybit_idx = 1 if long_exchange == "Bybit" else 2
        mexc_direction = 1 if long_exchange == "MEXC" else 2

        if ((self.bybit and self.bybit.open_orders()) or
                (self.mexc and self.mexc.open_orders())):
            raise LiveTradingError("После перезапуска найдены незавершённые заявки; автоматическое восстановление остановлено без новых ордеров")

        if self.bybit:
            all_bybit = self.bybit.open_positions()
            if any(p.get("symbol") != bsym or int(p.get("positionIdx", -1)) != bybit_idx
                   for p in all_bybit):
                raise LiveTradingError("На live-аккаунте Bybit найдена позиция вне сохранённой пары/стороны; восстановление остановлено")
        if self.mexc:
            all_mexc = self.mexc.open_positions()
            if any(p.get("symbol") != msym or int(p.get("_direction", 0)) != mexc_direction
                   for p in all_mexc):
                raise LiveTradingError("На live-аккаунте MEXC найдена позиция вне сохранённой пары/стороны; восстановление остановлено")

        positions = self.positions(a, b)
        b_open = self.bybit_paper or any(
            int(p.get("positionIdx", -1)) == bybit_idx and _d(p.get("size")) > 0
            for p in positions["Bybit"]
        )
        m_open = self.mexc_paper or any(
            int(p.get("_direction", 0)) == mexc_direction and _d(p.get("holdVol")) > 0
            for p in positions["MEXC"]
        )
        if b_open and m_open:
            logging.info("Проверка live-позиции после перезапуска: обе биржевые ноги найдены")
            return True
        if b_open or m_open:
            logging.critical("После перезапуска обнаружена только одна нога; немедленно закрываю оставшуюся")
            try:
                self.close_pair(a, b, long_exchange)
            except Exception as exc:
                raise LiveTradingError(f"Не удалось закрыть оставшуюся ногу после перезапуска: {exc}") from exc
        simulator.close_trade(*last, time.time(), "сверка live-позиций после перезапуска")
        simulator.record_equity(time.time(), *last)
        simulator.save_state()
        return False


# [PATCH 1] New sanity constants for instrument equivalence and depth.
BASKET_SIZE = 3
BASKET_ENTRY_SPREAD = Decimal('0.5')  # percent, STRICTLY greater
BASKET_MAX_SPREAD = Decimal('5')      # hard sanity ceiling; >5% = mismatched instruments
MAX_TICKER_RATIO_DEVIATION = Decimal('0.10')  # |a/b - 1| <= 10%
DEPTH_SAFETY_MULTIPLIER = Decimal('3')        # visible depth must be >= 3x leg notional
BASKET_NET_TARGET = Decimal('0.10')  # USDT, aggregate projected net
EXCHANGES = ('Bybit', 'MEXC')


def basket_decimal(value: Any) -> Decimal:
    """Financial/state inputs must fail closed, never silently become zero."""
    try:
        result = Decimal(str(value))
    except Exception as exc:
        raise LiveTradingError(f'Invalid decimal: {value!r}') from exc
    if not result.is_finite():
        raise LiveTradingError('Non-finite financial input')
    return result


def basket_spread(bybit: Any, mexc: Any) -> Decimal:
    a, b = basket_decimal(bybit), basket_decimal(mexc)
    if min(a, b) <= 0:
        raise LiveTradingError('Basket prices must be positive')
    return (a / b - 1) * 100


def depth_vwap(levels: list[Any], quantity: Any, buy: bool) -> Decimal:
    """Consume base-unit depth; never extrapolate insufficient visible depth."""
    remaining = basket_decimal(quantity)
    if remaining <= 0 or not levels:
        raise LiveTradingError('Missing depth/invalid quantity')
    rows = []
    for row in levels:
        if not isinstance(row, (tuple, list)) or len(row) < 2:
            raise LiveTradingError('Malformed depth level')
        price, size = basket_decimal(row[0]), basket_decimal(row[1])
        if price <= 0 or size < 0:
            raise LiveTradingError('Invalid depth level')
        if size:
            rows.append((price, size))
    rows.sort(key=lambda row: row[0], reverse=not buy)
    cost = Decimal(0)
    for price, available in rows:
        take = min(remaining, available)
        cost += take * price
        remaining -= take
        if remaining == 0:
            return cost / basket_decimal(quantity)
    raise LiveTradingError('Insufficient visible depth for the complete leg')


def validate_basket_book(book: dict[str, Any], now: float, max_age: float = 5.0) -> None:
    age = float(basket_decimal(now)) - float(basket_decimal(book.get('observed_at', 0)))
    if age < -1 or age > max_age:
        raise LiveTradingError('Stale orderbook snapshot')
    bids, asks = book.get('bids', []), book.get('asks', [])
    if not bids or not asks:
        raise LiveTradingError('Both orderbook sides are required')
    bid = max(basket_decimal(row[0]) for row in bids)
    ask = min(basket_decimal(row[0]) for row in asks)
    if min(bid, ask) <= 0 or bid >= ask:
        raise LiveTradingError('Invalid/crossed orderbook')


def project_basket(basket: dict[str, Any], books: dict[str, Any],
                   fee_rates: dict[str, Any], extra_slippage_bps: Any = Decimal('2'),
                   now: float | None = None) -> dict[str, Any]:
    now = time.time() if now is None else now
    buffer_rate = basket_decimal(extra_slippage_bps) / 10000
    if not 0 <= buffer_rate < 1:
        raise LiveTradingError('Slippage buffer must be in [0, 10000) bps')
    gross = entry = fees = slippage = notional = margin = Decimal(0)
    components, exits = [], {}
    if len(basket.get('pairs', [])) != BASKET_SIZE:
        raise LiveTradingError('Basket must contain exactly three pairs')
    stamps = []
    for pair in basket['pairs']:
        key = pair['base']
        exit_prices = {}
        for name in EXCHANGES:
            fill = pair['fills'][name]
            quantity = basket_decimal(fill['base_qty'])
            entered = basket_decimal(fill['avg_price'])
            entry_fee = basket_decimal(fill['fee'])
            fee_rate = basket_decimal(fee_rates[name])
            if quantity <= 0 or entered <= 0 or entry_fee < 0 or not 0 <= fee_rate < 1:
                raise LiveTradingError('Invalid basket fill/fee')
            book = books[key][name]
            validate_basket_book(book, now)
            stamps.append(float(book['observed_at']))
            is_long = pair['long_exchange'] == name
            bid = max(basket_decimal(row[0]) for row in book['bids'])
            ask = min(basket_decimal(row[0]) for row in book['asks'])
            mark = (bid + ask) / 2
            vwap = depth_vwap(book['bids'] if is_long else book['asks'], quantity, not is_long)
            expected_exit = vwap * (1 - buffer_rate if is_long else 1 + buffer_rate)
            sign = Decimal(1) if is_long else Decimal(-1)
            leg_gross = sign * quantity * (mark - entered)
            leg_slippage = sign * quantity * (mark - expected_exit)
            leg_exit_fee = quantity * expected_exit * fee_rate
            gross += leg_gross
            entry += entry_fee
            fees += leg_exit_fee
            slippage += leg_slippage
            notional += quantity * entered
            margin += quantity * entered / basket_decimal(basket['leverage'])
            exit_prices[name] = expected_exit
            components.append({'base': key, 'exchange': name, 'gross': leg_gross,
                               'entry_fee': entry_fee, 'exit_fee': leg_exit_fee,
                               'slippage': leg_slippage, 'exit_vwap': vwap,
                               'buffered_exit': expected_exit})
        exits[key] = exit_prices
    if max(stamps) - min(stamps) > 3.0:
        raise LiveTradingError('Basket orderbook snapshots are not synchronized')
    net = gross - entry - fees - slippage
    return {'gross': gross, 'entry_fees': entry, 'projected_exit_fees': fees,
            'projected_slippage': slippage, 'projected_net': net,
            'distance_to_target': max(Decimal(0), BASKET_NET_TARGET - net),
            'total_margin': margin, 'total_notional': notional,
            'target': BASKET_NET_TARGET, 'components': components,
            'exit_prices': exits, 'observed_at': now}


class BasketExecution:
    """Durable three-pair saga. Unknown exposure/accounting is sticky fail-closed."""
    def __init__(self, executor: Any, state_file: Path, fee_rates: dict[str, Decimal],
                 paper_balances: dict[str, Decimal] | None = None,
                 extra_slippage_bps: Decimal = Decimal('2'), clock: Any = time.time):
        self.executor = executor
        self.state_file = Path(state_file)
        self.requested_sizing = (executor.margin, executor.leverage, executor.notional)
        self.fee_rates = {k: basket_decimal(fee_rates[k]) for k in EXCHANGES}
        self.extra_slippage_bps = basket_decimal(extra_slippage_bps)
        if any(not 0 <= v < 1 for v in self.fee_rates.values()) or not 0 <= self.extra_slippage_bps < 10000:
            raise LiveTradingError('Invalid fees/slippage configuration')
        self.clock = clock
        self.paper_flags = {k: bool(getattr(executor, k.lower() + '_paper')) for k in EXCHANGES}
        self._lock_file = None
        self.reconciled = False
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        self._lock_file = self.state_file.with_name(self.state_file.name + '.lock').open('a+b')
        try:
            _lock_state_file(self._lock_file)
        except OSError as exc:
            self._lock_file.close()
            raise LiveTradingError('Basket ledger is already owned by another process') from exc
        self.state = {'version': 2, 'paper_flags': self.paper_flags, 'phase': 'FLAT',
                      'halt_reason': None, 'basket': None, 'archive': [],
                      'last_reconcile_time': None, 'verified_legs': 0, 'inventory': {},
                      'projection': None, 'projection_valid': False, 'history': [],
                      'balances': {}, 'balance_updated_at': None,
                      'paper_wallet': paper_balances or {k: Decimal('10') for k in EXCHANGES}}
        try:
            if self.state_file.exists():
                saved = json.loads(self.state_file.read_text(encoding='utf-8'))
                if not isinstance(saved, dict):
                    raise LiveTradingError('Invalid basket state object')
                if saved.get('version') != 2:
                    if saved.get('open_trade') or saved.get('basket'):
                        raise LiveTradingError('Legacy active position: migration blocked; original state preserved')
                    if saved.get('version') != 1:
                        raise LiveTradingError('Unknown state version; original state preserved')
                    self.state['legacy_state'] = saved
                    self.state['paper_wallet'] = saved.get('wallet', self.state['paper_wallet'])
                else:
                    required = {'phase', 'basket', 'archive', 'paper_wallet', 'paper_flags'}
                    if not required.issubset(saved) or saved['phase'] not in {
                            'FLAT', 'OPENING', 'OPEN', 'CLOSING', 'ROLLING_BACK', 'HALTED'}:
                        raise LiveTradingError('Incomplete/corrupt basket state; original preserved')
                    if saved['paper_flags'] != self.paper_flags:
                        raise LiveTradingError('Execution-mode change requires a different reconciled ledger')
                    if saved['phase'] == 'FLAT' and saved['basket'] is not None:
                        raise LiveTradingError('FLAT state contains exposure; original preserved')
                    if saved['phase'] != 'FLAT' and not isinstance(saved['basket'], dict):
                        if saved['phase'] != 'HALTED':
                            raise LiveTradingError('Active state has no basket; original preserved')
                    self.state.update(saved)
                    if self.state['basket']:
                        self._validate_saved_basket(self.state['basket'])
                if not isinstance(self.state['archive'], list) or not isinstance(self.state['paper_wallet'], dict):
                    raise LiveTradingError('Invalid ledger archive/wallet; original preserved')
                for name in EXCHANGES:
                    basket_decimal(self.state['paper_wallet'][name])
                self.state['verified_legs'] = 0
                self.state['projection_valid'] = False
        except BaseException:
            self.close()
            raise

    def _validate_saved_basket(self, basket: dict[str, Any]) -> None:
        if len(basket.get('pairs', [])) != 3 or basket_decimal(basket['leverage']) <= 0:
            raise LiveTradingError('Invalid saved basket')
        bases, bybit, mexc = set(), set(), set()
        for pair in basket['pairs']:
            if pair['long_exchange'] not in EXCHANGES:
                raise LiveTradingError('Invalid saved direction')
            bases.add(pair['base'])
            bybit.add(pair['rates']['Bybit']['exchange_symbol'])
            mexc.add(pair['rates']['MEXC']['exchange_symbol'])
            for name in EXCHANGES:
                if basket_decimal(pair['planned_qty'][name]) <= 0:
                    raise LiveTradingError('Invalid saved planned quantity')
                if self.state['phase'] == 'OPEN':
                    fill = pair['fills'][name]
                    if min(basket_decimal(fill['qty']), basket_decimal(fill['base_qty']),
                           basket_decimal(fill['avg_price'])) <= 0 or basket_decimal(fill['fee']) < 0:
                        raise LiveTradingError('Invalid persisted fill')
                    multiplier = (Decimal(1) if name == 'Bybit' else
                                  basket_decimal(pair['rates'][name]['contract_size']))
                    if multiplier <= 0 or basket_decimal(fill['base_qty']) != basket_decimal(fill['qty']) * multiplier:
                        raise LiveTradingError('Inconsistent persisted contract/base quantity')
        if min(len(bases), len(bybit), len(mexc)) != 3:
            raise LiveTradingError('Duplicate saved basket instruments')

    @property
    def basket(self) -> dict[str, Any] | None:
        return self.state['basket']

    @property
    def halted(self) -> bool:
        return self.state['phase'] == 'HALTED'

    @property
    def can_enter(self) -> bool:
        return self.reconciled and self.state['phase'] == 'FLAT' and self.basket is None

    def save_state(self) -> None:
        temp = self.state_file.with_name(self.state_file.name + '.tmp')
        try:
            with temp.open('w', encoding='utf-8') as stream:
                json.dump(self.state, stream, ensure_ascii=False, default=str, allow_nan=False)
                stream.write('\n')
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temp, self.state_file)
            directory_flag = getattr(os, 'O_DIRECTORY', None)
            if directory_flag is not None:
                directory = os.open(self.state_file.parent, os.O_RDONLY | directory_flag)
                try:
                    os.fsync(directory)
                finally:
                    os.close(directory)
        except Exception as exc:
            self.reconciled = False
            self.state['phase'] = 'HALTED'
            self.state['halt_reason'] = f'Persistence failure: {exc}'
            raise LiveTradingError(self.state['halt_reason']) from exc

    def halt(self, reason: str) -> None:
        self.reconciled = False
        self.state['phase'] = 'HALTED'
        self.state['halt_reason'] = reason
        self.state['projection_valid'] = False
        self.save_state()
        raise LiveTradingError(reason)

    def inventory(self) -> dict[str, Any]:
        if hasattr(self.executor, 'basket_inventory'):
            result = self.executor.basket_inventory()
        else:
            result = {}
            for name in EXCHANGES:
                rest = getattr(self.executor, name.lower())
                result[name] = {'positions': rest.open_positions() if rest else [],
                                'orders': rest.open_orders() if rest else []}
        if any(name not in result or not isinstance(result[name].get('positions'), list)
               or not isinstance(result[name].get('orders'), list) for name in EXCHANGES):
            raise LiveTradingError('Incomplete account inventory')
        return result

    @staticmethod
    def position_key(name: str, row: dict[str, Any]) -> tuple[str, int]:
        side = int(row.get('positionIdx', -1) if name == 'Bybit' else row.get('_direction', 0))
        return str(row.get('symbol', '')), side

    def expected(self, include_unfilled: bool = False) -> dict[str, dict[Any, Any]]:
        result = {k: {} for k in EXCHANGES}
        for pair in (self.basket or {}).get('pairs', []):
            for name in EXCHANGES:
                long = pair['long_exchange'] == name
                direction = (1 if long else 2)
                fill = pair.get('fills', {}).get(name)
                if fill or include_unfilled:
                    quantity = basket_decimal(fill['qty'] if fill else pair['planned_qty'][name])
                    result[name][(pair['rates'][name]['exchange_symbol'], direction)] = quantity
        return result

    def reconcile(self, startup: bool = False) -> int:
        try:
            inventory = self.inventory()
            self.state['inventory'] = inventory
            self.state['last_reconcile_time'] = self.clock()
            verified = 0
            expected = self.expected()
            problems = []
            for name in EXCHANGES:
                if self.paper_flags[name]:
                    if self.basket and self.state['phase'] == 'OPEN':
                        verified += 3
                    continue
                rows = inventory[name]
                if rows['orders']:
                    problems.append(f'{name}: outstanding/unknown orders')
                seen = set()
                for row in rows['positions']:
                    key = self.position_key(name, row)
                    qty = basket_decimal(row['size'] if name == 'Bybit' else row['holdVol'])
                    if key in seen or key not in expected[name]:
                        problems.append(f'{name}: unknown/duplicate position {key}')
                    elif qty != expected[name][key]:
                        problems.append(f'{name}: quantity mismatch {key} {qty} != {expected[name][key]}')
                    else:
                        verified += 1
                    seen.add(key)
                missing = set(expected[name]) - seen
                if missing:
                    problems.append(f'{name}: missing legs {sorted(missing)}')
            self.state['verified_legs'] = verified
            if self.state['phase'] not in ('FLAT', 'OPEN'):
                problems.append(f'Interrupted/sticky state {self.state["phase"]}; operator reconciliation required')
            if self.state['phase'] == 'OPEN' and verified != 6:
                problems.append(f'Only {verified}/6 legs verified')
            if problems:
                if self.basket:
                    self.basket.setdefault('safety_events', []).append({
                        'time': self.clock(), 'problems': problems, 'inventory': inventory})
                self.halt('; '.join(problems))
            self.reconciled = True
            if startup and self.basket:
                self.executor.prepared = True
                self.executor.leverage = (basket_decimal(self.basket['leverage']) if all(self.paper_flags.values())
                                          else int(basket_decimal(self.basket['leverage'])))
                self.executor.notional = basket_decimal(self.basket['notional_per_leg'])
                self.executor.margin = basket_decimal(self.basket['margin_per_leg'])
            self.save_state()
            return verified
        except LiveTradingError:
            if not self.halted:
                self.halt('Account reconciliation failed; exposure ledger retained')
            raise
        except Exception as exc:
            self.halt(f'Account reconciliation unavailable: {exc}')
        return 0

    def refresh_balances(self) -> None:
        self.state['balances'] = self.executor.account_metrics()
        self.state['balance_updated_at'] = self.clock()
        self.save_state()

    @staticmethod
    def rate_record(rate: Any) -> dict[str, Any]:
        return {key: getattr(rate, key, Decimal(0)) for key in (
            'exchange_symbol', 'base', 'price', 'qty_step', 'min_qty',
            'contract_size', 'vol_step', 'min_vol')}

    @staticmethod
    def restore_rate(record: dict[str, Any]) -> Any:
        from types import SimpleNamespace
        return SimpleNamespace(**{k: v if k in ('base', 'exchange_symbol') else basket_decimal(v)
                                  for k, v in record.items()})

    def active_pairs(self) -> list[tuple[Any, Any]]:
        return [(self.restore_rate(pair['rates']['Bybit']), self.restore_rate(pair['rates']['MEXC']))
                for pair in (self.basket or {}).get('pairs', [])]

    def _plan(self, pairs: list[tuple[Any, Any]]) -> list[dict[str, Any]]:
        if len(pairs) != 3 or len({a.base for a, _ in pairs}) != 3:
            raise LiveTradingError('Entry requires exactly three distinct pairs')
        if len({a.exchange_symbol for a, _ in pairs}) != 3 or len({b.exchange_symbol for _, b in pairs}) != 3:
            raise LiveTradingError('Duplicate exchange instruments')
        result = []
        for a, b in pairs:
            spread = basket_spread(a.price, b.price)
            if a.base != b.base or abs(spread) <= BASKET_ENTRY_SPREAD:
                raise LiveTradingError('Every entry price spread must be strictly > 0.5%')
            # [PATCH 2] sanity ceiling and ticker-ratio check
            if abs(spread) > BASKET_MAX_SPREAD:
                raise LiveTradingError(
                    f"{a.base}: ticker spread {spread:+.4f}% exceeds sanity ceiling "
                    f"{BASKET_MAX_SPREAD}% — mismatched instruments or corrupted ticker")
            ratio = basket_decimal(a.price) / basket_decimal(b.price)
            if abs(ratio - 1) > MAX_TICKER_RATIO_DEVIATION:
                raise LiveTradingError(
                    f"{a.base}: ticker price ratio {ratio:.6f} deviates >"
                    f"{MAX_TICKER_RATIO_DEVIATION * 100}% — likely different instruments")
            # [PATCH 2 end]
            if not self.executor.pair_is_tradeable(a, b):
                raise LiveTradingError('Pair fails size/leverage/region preflight')
            qty = {}
            for name, rate in zip(EXCHANGES, (a, b)):
                if self.paper_flags[name]:
                    base_qty = basket_decimal(self.executor.notional) / basket_decimal(rate.price)
                    qty[name] = base_qty if name == 'Bybit' else base_qty / basket_decimal(rate.contract_size)
                else:
                    qty[name] = (self.executor._bybit_spec_qty(rate) if name == 'Bybit'
                                 else self.executor._mexc_spec_vol(rate))
            result.append({'base': a.base, 'long_exchange': 'Bybit' if spread < 0 else 'MEXC',
                           'rates': {'Bybit': self.rate_record(a), 'MEXC': self.rate_record(b)},
                           'planned_qty': qty, 'fills': {}, 'attempted': False})
        return result

    def preflight_books(self, pairs: list[tuple[Any, Any]], books: dict[str, Any]) -> None:
        now = self.clock()
        stamps = []
        failures = []
        for pair in self._plan(pairs):
            prices = {}
            try:
                for name in EXCHANGES:
                    book = books[pair['base']][name]
                    validate_basket_book(book, now)
                    stamps.append(float(book['observed_at']))
                    buy = pair['long_exchange'] == name
                    base_qty = basket_decimal(pair['planned_qty'][name])
                    if name == 'MEXC':
                        base_qty *= basket_decimal(pair['rates'][name]['contract_size'])
                    # [PATCH 3] require depth with safety multiple
                    leg_notional = basket_decimal(self.executor.notional)
                    side_levels = book['asks'] if buy else book['bids']
                    side_notional = sum(
                        (basket_decimal(row[0]) * basket_decimal(row[1])
                         for row in side_levels), Decimal(0))
                    if side_notional < leg_notional * DEPTH_SAFETY_MULTIPLIER:
                        raise LiveTradingError(
                            f"visible depth ${side_notional:.4f} < "
                            f"{DEPTH_SAFETY_MULTIPLIER}x notional ${leg_notional:.4f}")
                    # [PATCH 3 end]
                    prices[name] = depth_vwap(side_levels, base_qty, buy)
            except Exception as exc:
                logging.warning(
                    'Basket VWAP preflight %s | LONG=%s SHORT=%s | VWAP unavailable: %s: %s | BLOCK',
                    pair['base'], pair['long_exchange'],
                    'MEXC' if pair['long_exchange'] == 'Bybit' else 'Bybit',
                    type(exc).__name__, exc)
                failures.append(f"{pair['base']}: VWAP unavailable ({type(exc).__name__}: {exc})")
                continue
            executable = basket_spread(prices['Bybit'], prices['MEXC'])
            # [PATCH 4] sanity ceiling on executable VWAP spread
            if abs(executable) > BASKET_MAX_SPREAD:
                logging.warning(
                    'Basket VWAP preflight %s | executable_spread=%+.4f%% exceeds ceiling %s%% '
                    '| likely mismatched instruments | BLOCK',
                    pair['base'], executable, BASKET_MAX_SPREAD)
                failures.append(
                    f"{pair['base']}: executable spread {executable:+.4f}% > "
                    f"ceiling {BASKET_MAX_SPREAD}%")
                continue
            # [PATCH 4 end]
            passes = (abs(executable) > BASKET_ENTRY_SPREAD and
                      (executable < 0) == (pair['long_exchange'] == 'Bybit'))
            status = 'PASS' if passes else 'BLOCK'
            short_exchange = 'MEXC' if pair['long_exchange'] == 'Bybit' else 'Bybit'
            logging.info(
                'Basket VWAP preflight %s | LONG=%s SHORT=%s | '
                'Bybit VWAP=%s MEXC VWAP=%s | executable_spread=%+.6f%% | '
                'required abs(spread)>%.3f%%, intended direction | %s',
                pair['base'], pair['long_exchange'], short_exchange,
                prices['Bybit'], prices['MEXC'], executable,
                BASKET_ENTRY_SPREAD, status)
            if not passes:
                failures.append(f"{pair['base']}: executable spread {executable:+.6f}%")
        if stamps and max(stamps) - min(stamps) > 3:
            failures.append('entry book snapshots are not synchronized')
        if failures:
            raise LiveTradingError(
                'Executable entry preflight blocked: ' + '; '.join(failures) +
                '; every pair must have abs(VWAP spread) strictly > 0.5% in intended direction')

    def open_basket(self, pairs: list[tuple[Any, Any]], books: dict[str, Any]) -> bool:
        if not self.can_enter:
            raise LiveTradingError('New entries blocked until a clean account reconciliation')
        self.reconcile()
        plan = self._plan(pairs)
        self.preflight_books(pairs, books)
        required = basket_decimal(self.executor.margin) * 3 * Decimal('1.10')
        balances = self.executor.balance_snapshot()
        for name in EXCHANGES:
            available = self.state['paper_wallet'][name] if self.paper_flags[name] else balances.get(name, 0)
            if basket_decimal(available) < required:
                raise LiveTradingError(f'{name}: insufficient aggregate margin/fee reserve for three pairs')
        self.preflight_books(pairs, books)
        self.state['basket'] = {'id': uuid.uuid4().hex, 'opened_at': self.clock(),
                                'leverage': self.executor.leverage,
                                'notional_per_leg': self.executor.notional,
                                'margin_per_leg': self.executor.margin, 'pairs': plan,
                                'close_results': {}, 'rollback_results': {}}
        self.state['phase'] = 'OPENING'
        self.state['projection'] = None
        self.state['history'] = []
        self.reconciled = False
        self.save_state()
        try:
            for index, (a, b) in enumerate(pairs):
                pair = plan[index]
                pair['attempted'] = True
                self.save_state()
                fills = self.executor.open_pair(a, b, pair['long_exchange'])
                pair['fills'] = fills
                self.save_state()
                for name in EXCHANGES:
                    fill = fills[name]
                    qty, price = basket_decimal(fill['base_qty']), basket_decimal(fill['avg_price'])
                    if min(qty, price, basket_decimal(fill['qty'])) <= 0:
                        raise LiveTradingError('Invalid returned entry fill')
                    fee = basket_decimal(fill.get('fee', 0))
                    fill['fee_source'] = 'reported' if fee > 0 else 'configured_estimate'
                    fill['fee'] = fee if fee > 0 else qty * price * self.fee_rates[name]
                spread = basket_spread(fills['Bybit']['avg_price'], fills['MEXC']['avg_price'])
                if abs(spread) <= BASKET_ENTRY_SPREAD or (spread < 0) != (pair['long_exchange'] == 'Bybit'):
                    raise EntryRejectedAfterRollback('Actual basket entry fill spread is not strictly > 0.5%')
                self.save_state()
            for pair in plan:
                for name in EXCHANGES:
                    if self.paper_flags[name]:
                        self.state['paper_wallet'][name] = basket_decimal(self.state['paper_wallet'][name]) - basket_decimal(pair['fills'][name]['fee'])
            self.basket['paper_entry_fees_committed'] = True
            self.state['phase'] = 'OPEN'
            self.reconcile()
            self.save_state()
            logging.warning('BASKET OPEN %s | %s | 6/6 verified | margin/leg=%s notional/leg=%s',
                            self.basket['id'], ','.join(p['base'] for p in plan),
                            self.executor.margin, self.executor.notional)
            return True
        except Exception as exc:
            if self.halted:
                if not str(self.state.get('halt_reason', '')).startswith('Persistence failure'):
                    self.rollback(f'Entry verification failed: {exc}', ambiguous=True)
                raise
            self.rollback(f'Partial/invalid basket entry: {exc}', ambiguous=isinstance(exc, OrderOutcomeUnknown))
            if isinstance(exc, MexcRegionRestrictedError):
                raise
            logging.warning('Basket entry rejected; complete rollback verified: %s', exc)
            return False

    def _rollback_safe_inventory(self, reason: str = 'pre-rollback inventory') -> None:
        inventory = self.inventory()
        self.state['inventory'] = inventory
        self.basket.setdefault('safety_events', []).append({
            'time': self.clock(), 'reason': reason, 'inventory': inventory})
        allowed = self.expected(include_unfilled=True)
        for name in EXCHANGES:
            if self.paper_flags[name]:
                continue
            for row in inventory[name]['positions']:
                key = self.position_key(name, row)
                qty = basket_decimal(row['size'] if name == 'Bybit' else row['holdVol'])
                if key not in allowed[name] or qty > allowed[name][key]:
                    self.halt('Unknown/oversized exposure during rollback; no blind liquidation')

    def _verify_flat(self) -> None:
        settle = (0 if all(self.paper_flags.values()) else
                  max(0.0, float(getattr(self.executor, 'basket_settle_seconds', POSITION_WAIT_SECONDS))))
        deadline = time.monotonic() + settle
        while True:
            inventory = self.inventory()
            self.state['inventory'] = inventory
            self.state['last_reconcile_time'] = self.clock()
            for name in EXCHANGES:
                if not self.paper_flags[name] and (inventory[name]['positions'] or inventory[name]['orders']):
                    raise LiveTradingError(f'{name}: flatten not confirmed; residual positions/orders')
            if time.monotonic() >= deadline:
                self.state['verified_legs'] = 0
                return
            time.sleep(min(0.25, max(0, deadline - time.monotonic())))

    def rollback(self, reason: str, ambiguous: bool = False) -> None:
        self.state['phase'] = 'ROLLING_BACK'
        self.state['basket']['rollback_reason'] = reason
        self.save_state()
        errors = []
        try:
            self._rollback_safe_inventory()
            self.save_state()
            for pair in reversed(self.basket['pairs']):
                if not pair['attempted']:
                    continue
                try:
                    a, b = (self.restore_rate(pair['rates'][name]) for name in EXCHANGES)
                    result = self.executor.close_pair(a, b, pair['long_exchange'])
                    self.basket['rollback_results'][pair['base']] = result
                    self.save_state()
                except Exception as exc:
                    if self.halted:
                        raise
                    errors.append(f'{pair["base"]}: {exc}')
            self._verify_flat()
            if errors or ambiguous:
                raise LiveTradingError('; '.join(errors) or 'Entry order outcome unknown; late execution cannot be excluded')
            paper_net = Decimal(0)
            for pair in self.basket['pairs']:
                for name, fill in pair['fills'].items():
                    quantity = basket_decimal(fill['base_qty'])
                    if not self.paper_flags[name]:
                        raw = self.basket['rollback_results'].get(pair['base'], {}).get(name)
                        rows = raw if isinstance(raw, list) else ([raw] if raw else [])
                        receipt_qty = sum((basket_decimal(row['base_qty']) for row in rows), Decimal(0))
                        if receipt_qty != quantity:
                            raise LiveTradingError('Known entry fill has no matching rollback receipt; outcome/accounting unknown')
                    else:
                        entry_price = basket_decimal(fill['avg_price'])
                        buffer_rate = self.extra_slippage_bps / 10000
                        long = pair['long_exchange'] == name
                        exit_price = entry_price * (1 - buffer_rate if long else 1 + buffer_rate)
                        gross = (1 if long else -1) * quantity * (exit_price - entry_price)
                        entry_fee = basket_decimal(fill.get('fee', quantity * entry_price * self.fee_rates[name]))
                        exit_fee = quantity * exit_price * self.fee_rates[name]
                        debit = Decimal(0) if self.basket.get('paper_entry_fees_committed') else entry_fee
                        self.state['paper_wallet'][name] = basket_decimal(self.state['paper_wallet'][name]) + gross - exit_fee - debit
                        paper_net += gross - entry_fee - exit_fee
            self.basket['paper_rollback_net_estimate'] = paper_net
        except Exception as exc:
            self.halt(f'Rollback NOT confirmed: {exc}; all intended legs/fills retained')
        self.basket['status'] = 'ROLLED_BACK'
        self.state['archive'].append(self.basket)
        self.state['basket'] = None
        self.state['phase'] = 'FLAT'
        self.state['verified_legs'] = 0
        self.state['projection_valid'] = False
        self.reconciled = True
        self.save_state()

    def emergency_flatten_known(self, reason: str) -> None:
        if not self.basket:
            self.halt(reason)
        self.rollback(reason, ambiguous=True)

    def update_projection(self, books: dict[str, Any]) -> dict[str, Any]:
        if self.state['phase'] != 'OPEN' or self.state['verified_legs'] != 6 or not self.reconciled:
            raise LiveTradingError('Projection/exit prohibited without six verified legs')
        try:
            result = project_basket(self.basket, books, self.fee_rates, self.extra_slippage_bps, self.clock())
        except Exception:
            self.state['projection_valid'] = False
            self.save_state()
            raise
        self.state['projection'] = result
        self.state['projection_valid'] = True
        self.state['history'].append({'t': self.clock(), 'basket_id': self.basket['id'],
                                      'projected_net': str(result['projected_net'])})
        self.state['history'] = self.state['history'][-getattr(self, 'history_limit', 360):]
        self.save_state()
        logging.info('BASKET PNL %s gross=%s entry_fees=%s projected_exit_fees=%s '
                     'projected_slippage=%s projected_net=%s distance=%s components=%s',
                     self.basket['id'], result['gross'], result['entry_fees'],
                     result['projected_exit_fees'], result['projected_slippage'],
                     result['projected_net'], result['distance_to_target'], result['components'])
        return result

    def close_if_target(self, books: dict[str, Any]) -> bool:
        self.reconcile()
        projection = self.update_projection(books)
        if projection['projected_net'] < BASKET_NET_TARGET:
            return False
        self.state['phase'] = 'CLOSING'
        self.basket['exit_trigger_projection'] = projection
        self.reconciled = False
        self.save_state()
        try:
            errors = []
            for pair in self.basket['pairs']:
                a, b = (self.restore_rate(pair['rates'][name]) for name in EXCHANGES)
                try:
                    self._rollback_safe_inventory('pre-basket-exit inventory')
                    self.save_state()
                    result = self.executor.close_pair(a, b, pair['long_exchange'])
                    self.basket['close_results'][pair['base']] = result
                    self.save_state()
                except Exception as exc:
                    if self.halted:
                        raise
                    errors.append(f'{pair["base"]}: {exc}')
                    self.basket.setdefault('close_errors', {})[pair['base']] = str(exc)
                    self.save_state()
            self._verify_flat()
            if errors:
                raise LiveTradingError('; '.join(errors))
            gross = entry_fees = exit_fees = Decimal(0)
            paper_changes = {k: Decimal(0) for k in EXCHANGES}
            for pair in self.basket['pairs']:
                for name in EXCHANGES:
                    opened = pair['fills'][name]
                    quantity = basket_decimal(opened['base_qty'])
                    if self.paper_flags[name]:
                        price = basket_decimal(projection['exit_prices'][pair['base']][name])
                        fee = quantity * price * self.fee_rates[name]
                    else:
                        raw = self.basket['close_results'][pair['base']].get(name)
                        rows = raw if isinstance(raw, list) else ([raw] if raw else [])
                        closed_qty = sum((basket_decimal(row['base_qty']) for row in rows), Decimal(0))
                        if closed_qty != quantity:
                            raise LiveTradingError('Flat account but exit fill quantity/accounting unknown; ledger retained')
                        price = sum((basket_decimal(row['base_qty']) * basket_decimal(row['avg_price']) for row in rows), Decimal(0)) / quantity
                        fee = sum((basket_decimal(row.get('fee', 0)) for row in rows), Decimal(0))
                        if fee <= 0:
                            fee = quantity * price * self.fee_rates[name]
                    sign = Decimal(1) if name == pair['long_exchange'] else Decimal(-1)
                    leg_gross = sign * quantity * (price - basket_decimal(opened['avg_price']))
                    gross += leg_gross
                    entry_fees += basket_decimal(opened['fee'])
                    exit_fees += fee
                    if self.paper_flags[name]:
                        paper_changes[name] += leg_gross - fee
            self.basket['status'] = 'CLOSED'
            self.basket['closed_at'] = self.clock()
            self.basket['accounting'] = {'gross': gross, 'entry_fees': entry_fees,
                                         'exit_fees': exit_fees, 'net': gross - entry_fees - exit_fees,
                                         'kind': 'fill_based_with_configured_fee_fallback'}
            for name in EXCHANGES:
                self.state['paper_wallet'][name] = basket_decimal(self.state['paper_wallet'][name]) + paper_changes[name]
            logging.warning('BASKET CLOSE %s | projected=%s | fill-based net=%s '
                            '(not guaranteed; estimated fees may differ)', self.basket['id'],
                            projection['projected_net'], self.basket['accounting']['net'])
            self.state['archive'].append(self.basket)
            self.state['basket'] = None
            self.state['phase'] = 'FLAT'
            self.state['verified_legs'] = 0
            self.state['projection_valid'] = False
            self.reconciled = True
            self.executor.margin, self.executor.leverage, self.executor.notional = self.requested_sizing
            for name in ('configured_bybit', 'configured_mexc'):
                if hasattr(self.executor, name):
                    getattr(self.executor, name).clear()
            self.save_state()
            return True
        except Exception as exc:
            self.halt(f'Basket exit incomplete/unknown: {exc}; closing journal retained')
        return False

    def close(self) -> None:
        if self._lock_file and not self._lock_file.closed:
            _unlock_state_file(self._lock_file)
            self._lock_file.close()
