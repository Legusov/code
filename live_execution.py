"""Direct REST order execution for Bybit V5 and MEXC Futures.

No credentials are bundled here. Live order calls are only made when the
caller explicitly enables live mode (DEMO=False) and imports key.py locally.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import logging
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal, ROUND_DOWN
from threading import Lock
from typing import Any
from urllib.parse import urlencode

import requests

BYBIT_URL = "https://api.bybit.com"
MEXC_URL = "https://api.mexc.com"
LIVE_TIMEOUT = 5
ORDER_WAIT_SECONDS = 8
POSITION_WAIT_SECONDS = 8


class LiveTradingError(RuntimeError):
    """An API, setup, fill, or reconciliation issue that must stop live entries."""


class EntryRejectedAfterRollback(LiveTradingError):
    """A candidate entry was safely rolled back and may be skipped without stopping."""


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
        if payload_json.get("retCode") != 0:
            raise LiveTradingError(f"Bybit API {path}: {payload_json.get('retCode')} {payload_json.get('retMsg')}")
        return payload_json.get("result", {})

    def open_positions(self, symbol: str | None = None) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"category": "linear", "settleCoin": "USDT", "limit": 200}
        if symbol:
            params.pop("settleCoin", None)
            params["symbol"] = symbol
        result = self.request("GET", "/v5/position/list", params)
        return [p for p in result.get("list", []) if _d(p.get("size")) > 0]

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
        result = self.request("POST", "/v5/order/create", body)
        order_id = result.get("orderId")
        if not order_id:
            raise LiveTradingError(f"Bybit did not return orderId for {symbol}")
        deadline = time.monotonic() + ORDER_WAIT_SECONDS
        last: dict[str, Any] = {}
        while time.monotonic() < deadline:
            last_result = self.request("GET", "/v5/order/realtime", {
                "category": "linear", "symbol": symbol, "orderId": order_id,
            })
            rows = last_result.get("list", [])
            if rows:
                last = rows[0]
                status = last.get("orderStatus")
                if status in ("Filled", "Cancelled", "Rejected", "PartiallyFilledCanceled"):
                    break
            time.sleep(0.2)
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
            if _d(p.get("state")) != 1:
                continue
            if _d(p.get("holdVol")) <= 0:
                continue
            if symbol and p.get("symbol") != symbol:
                continue
            # Get Open Positions documents positionType as the direction
            # (1 = long, 2 = short). The response does not define a `side` field.
            normalized = dict(p)
            direction = p.get("positionType")
            if direction is None:  # compatibility with older/nonstandard responses
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
        order_id = result.get("orderId") if isinstance(result, dict) else None
        if not order_id:
            raise LiveTradingError(f"MEXC did not return orderId for {symbol}")
        deadline = time.monotonic() + ORDER_WAIT_SECONDS
        last: dict[str, Any] = {}
        while time.monotonic() < deadline:
            last = self.request("GET", f"/api/v1/private/order/get/{order_id}") or {}
            if int(last.get("state", 0) or 0) in (3, 4, 5):
                break
            time.sleep(0.2)
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
                # Bybit's 110025 explicitly means the requested mode is already active.
                if "110025" not in str(exc):
                    raise
        if self.mexc:
            self._mexc_config_request("/api/v1/private/position/change_position_mode", {"positionMode": 1})
        self.prepared = True
        active = [name for name, paper in (("Bybit", self.bybit_paper), ("MEXC", self.mexc_paper)) if not paper]
        logging.warning("LIVE account configuration applied to %s", ", ".join(active))

    def prepare_account(self) -> None:
        """Prepare the selected live accounts without configuring every symbol."""
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
        for i, ((bybit_symbol, mexc_symbol), (a, b)) in enumerate(pairs.items(), 1):
            if self.bybit and bybit_symbol not in self.configured_bybit:
                self.bybit.request("POST", "/v5/position/set-leverage", {
                    "category": "linear", "symbol": bybit_symbol,
                    "buyLeverage": str(self.leverage), "sellLeverage": str(self.leverage),
                })
                self.configured_bybit.add(bybit_symbol)
            if self.mexc and mexc_symbol not in self.configured_mexc:
                for position_type in (1, 2):
                    self._mexc_config_request("/api/v1/private/position/change_leverage", {
                        "symbol": mexc_symbol, "leverage": self.leverage,
                        "openType": 2, "positionType": position_type,
                    })
                self.configured_mexc.add(mexc_symbol)
            if i % 10 == 0:
                logging.info("Live-настройка плеча: %d/%d общих контрактных пар", i, len(pairs))
        active = [name for name, paper in (("Bybit", self.bybit_paper), ("MEXC", self.mexc_paper)) if not paper]
        if active:
            logging.info("Плечо x%d задано для %d общих пар на live-биржах: %s", self.leverage, len(pairs), ", ".join(active))

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
        """Read the current account metrics from enabled live exchanges only."""
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
        """Preflight sizing only; never rounds a requested exposure upward."""
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
            try:
                self.close_pair(a, b, long_exchange)
            except Exception as unwind_exc:
                raise LiveTradingError(f"Одна нога не открылась; аварийное закрытие второй также не подтвердилось. Немедленно проверьте позиции на биржах: {unwind_exc}") from next(iter(errors.values()))
            if region_error:
                raise region_error
            raise LiveTradingError(f"Не удалось открыть обе ноги; исполненная нога аварийно закрыта. Ошибка: {next(iter(errors.values()))}")
        lower_ratio = Decimal("0.90")  # допускаем округление шага контракта вниз
        upper_ratio = Decimal("1.10")
        for name, fill in fills.items():
            actual = fill["base_qty"] * fill["avg_price"]
            if actual < self.notional * lower_ratio or actual > self.notional * upper_ratio:
                try:
                    self.close_pair(a, b, long_exchange)
                except Exception as unwind_exc:
                    raise LiveTradingError(f"Размер неполного исполнения вне допуска; аварийное закрытие не подтверждено: {unwind_exc}")
                raise LiveTradingError(f"Размер исполнения {name} (${actual:.4f}) вне допуска к цели ${self.notional}; обе ноги закрыты")
        # MEXC может отставать с индексацией позиции на 1-2 секунды после fill.
        # Даём несколько попыток, прежде чем считать, что позиции нет.
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
        # Use the normalized documented MEXC positionType direction
        # (1=long, 2=short), not order-side codes.
        # Закрываем всё, что фактически открыто по этому символу.
        positions = self.mexc.open_positions(symbol)
        results: list[dict[str, Any]] = []
        for position in positions:
            vol = _d(position.get("holdVol"))
            if vol <= 0:
                continue
            direction = int(position.get("_direction", 0) or 0)
            if direction == 1:
                side = 4   # close long
            elif direction == 2:
                side = 2   # close short
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
        # MEXC positionType: 1 = long, 2 = short.
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
            # Market orders are IOC-like. If a close did not fully flatten a leg,
            # re-read the actual residual size and submit another reduce-only close.
            # Never resend based only on a timeout: position state is re-queried first.
            if time.monotonic() >= retry_at:
                jobs = {}
                with ThreadPoolExecutor(max_workers=2, thread_name_prefix="live-close") as pool:
                    if b_open:
                        jobs[pool.submit(self._close_bybit, a.exchange_symbol, bybit_idx, a)] = "Bybit"
                    if m_open:
                        # position_type здесь не важен: _close_mexc смотрит на side.
                        jobs[pool.submit(self._close_mexc, b.exchange_symbol, 0, b)] = "MEXC"
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
        """Reconcile persisted bot state with exchange positions; close any orphan leg."""
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
        # MEXC positionType: 1 = long, 2 = short.
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
