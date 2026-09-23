#!/usr/bin/env python3
"""Universal 5-minute Bybit entry filter.

open_ok('BTCUSDT') returns True when either the Long or Short signal is active.
No order is sent by this module.
"""
from __future__ import annotations
import os
import threading
from dataclasses import dataclass
from typing import Literal
from pybit.unified_trading import HTTP
from api_conf_all import DEMO_CONFIG

INTERVAL = "5"
CANDLE_LIMIT = 200
EMA_FAST = 10
EMA_SLOW = 50
ATR_PERIOD = 14
RSI_PERIOD = 14
EMA_GAP_ATR = 0.5
RSI_LONG_MIN = 50.0
RSI_SHORT_MAX = 50.0
CLOSE_EXTREME = 0.25
CATEGORY = "linear"


@dataclass(frozen=True)
class Signal:
    symbol: str
    side: Literal["Long", "Short"] | None
    ok: bool
    reason: str
    candle_start_ms: int | None = None
    ema_fast: float | None = None
    ema_slow: float | None = None
    atr14: float | None = None
    rsi14: float | None = None
    close_location: float | None = None


def _ema(values: list[float], period: int) -> list[float | None]:
    out: list[float | None] = [None] * len(values)
    if len(values) < period:
        return out
    value = sum(values[:period]) / period
    out[period - 1] = value
    alpha = 2.0 / (period + 1.0)
    for i in range(period, len(values)):
        value = alpha * values[i] + (1.0 - alpha) * value
        out[i] = value
    return out


def _atr(high: list[float], low: list[float], close: list[float], period: int) -> list[float | None]:
    out: list[float | None] = [None] * len(close)
    if len(close) <= period:
        return out
    tr: list[float | None] = [None]
    for i in range(1, len(close)):
        tr.append(max(
            high[i] - low[i],
            abs(high[i] - close[i - 1]),
            abs(low[i] - close[i - 1]),
        ))
    value = sum(x for x in tr[1:period + 1] if x is not None) / period
    out[period] = value
    for i in range(period + 1, len(close)):
        value = (value * (period - 1) + tr[i]) / period  # Wilder ATR
        out[i] = value
    return out


def _rsi(close: list[float], period: int) -> list[float | None]:
    out: list[float | None] = [None] * len(close)
    if len(close) <= period:
        return out
    gains = [0.0]
    losses = [0.0]
    for i in range(1, len(close)):
        delta = close[i] - close[i - 1]
        gains.append(max(delta, 0.0))
        losses.append(max(-delta, 0.0))
    avg_gain = sum(gains[1:period + 1]) / period
    avg_loss = sum(losses[1:period + 1]) / period
    out[period] = 100.0 if avg_loss == 0 else 100.0 - 100.0 / (1.0 + avg_gain / avg_loss)
    for i in range(period + 1, len(close)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        out[i] = 100.0 if avg_loss == 0 else (0.0 if avg_gain == 0 else 100.0 - 100.0 / (1.0 + avg_gain / avg_loss))
    return out


def _fetch_completed_candles(session: HTTP, symbol: str) -> list[tuple[int, float, float, float, float]]:
    response = session.get_kline(category=CATEGORY, symbol=symbol.upper(), interval=INTERVAL, limit=CANDLE_LIMIT)
    if response.get("retCode") != 0:
        raise RuntimeError(f"Bybit get_kline failed: {response}")
    # Bybit returns newest first. The newest 5-minute candle may still be forming.
    raw = sorted(response["result"]["list"], key=lambda row: int(row[0]))
    if len(raw) < 3:
        raise ValueError(f"Not enough candles for {symbol}")
    raw = raw[:-1]
    return [(int(row[0]), float(row[1]), float(row[2]), float(row[3]), float(row[4])) for row in raw]


def signal(symbol: str, session: HTTP | None = None) -> Signal:
    """Return detailed signal information for the latest completed 5-minute candle."""
    symbol = symbol.upper()
    session = session if session is not None else HTTP(testnet=False)
    candles = _fetch_completed_candles(session, symbol)
    timestamps = [x[0] for x in candles]
    opens = [x[1] for x in candles]
    highs = [x[2] for x in candles]
    lows = [x[3] for x in candles]
    closes = [x[4] for x in candles]
    fast = _ema(closes, EMA_FAST)
    slow = _ema(closes, EMA_SLOW)
    atr = _atr(highs, lows, closes, ATR_PERIOD)
    rsi = _rsi(closes, RSI_PERIOD)
    i = len(closes) - 1
    if i < 1 or any(v is None for v in (fast[i], slow[i], atr[i], rsi[i], fast[i - 1])):
        return Signal(symbol, None, False, "insufficient_indicator_history", timestamps[i])
    candle_range = highs[i] - lows[i]
    close_location = (closes[i] - lows[i]) / candle_range if candle_range else 0.5
    long_ok = (
        fast[i] - slow[i] >= EMA_GAP_ATR * atr[i]
        and rsi[i] >= RSI_LONG_MIN
        and close_location >= 1.0 - CLOSE_EXTREME
        and fast[i] > fast[i - 1]
    )
    short_ok = (
        slow[i] - fast[i] >= EMA_GAP_ATR * atr[i]
        and rsi[i] <= RSI_SHORT_MAX
        and close_location <= CLOSE_EXTREME
        and fast[i] < fast[i - 1]
    )
    side = "Long" if long_ok else "Short" if short_ok else None
    return Signal(symbol, side, side is not None, "signal" if side else "no_signal", timestamps[i], fast[i], slow[i], atr[i], rsi[i], close_location)


def open_ok(symbol: str, session: HTTP | None = None) -> bool:
    """Return True when the latest completed candle gives a Long or Short entry."""
    return signal(symbol, session=session).ok


def open_side(symbol: str, session: HTTP | None = None) -> str | None:
    """Return 'Long', 'Short', or None. Useful when order direction is also needed."""
    return signal(symbol, session=session).side

def create_session_with_proxy(api_config):
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

        sess = HTTP(**session_kwargs)
        if proxy_url:
            sess.client.proxies.update({'http': proxy_url, 'https': proxy_url})
        print(f"[{api_config.get('name', 'Unknown')}] Сессия с прокси: {proxy_url}")
        return sess
    else:
        print(f"[{api_config.get('name', 'Unknown')}] Сессия без прокси")
        return HTTP(**session_kwargs)

if __name__ == "__main__":
    import sys

    thread_local = threading.local()

    all_sym = ['0GUSDT', '1000000BABYDOGEUSDT', '1000000MOGUSDT', '1000BONKUSDT', '1000CATUSDT', '1000FLOKIUSDT',
               '1000LUNCUSDT', '1000NEIROCTOUSDT', '1000PEPEUSDT', '1000RATSUSDT', '1000TOSHIUSDT', '1000TURBOUSDT',
               '1000XECUSDT', '1INCHUSDT', '2ZUSDT', 'AAVEUSDT', 'ACEUSDT', 'ACHUSDT', 'ACTUSDT', 'ADAUSDT', 'AEROUSDT',
               'AEVOUSDT', 'AGLDUSDT', 'AIXBTUSDT', 'AKTUSDT', 'ALCHUSDT', 'ALGOUSDT', 'ALICEUSDT', 'ALTUSDT',
               'ANIMEUSDT', 'ANKRUSDT', 'APEUSDT', 'APEXUSDT', 'API3USDT', 'APTUSDT', 'ARBUSDT', 'ARKKUSDT', 'ARKMUSDT',
               'ARKUSDT', 'ARPAUSDT', 'ARUSDT', 'ASTERUSDT', 'ASTRUSDT', 'ATHUSDT', 'ATOMUSDT', 'AUCTIONUSDT', 'AUSDT',
               'AVAAIUSDT', 'AVAXUSDT', 'AXLUSDT', 'AXSUSDT', 'B3USDT', 'BABYUSDT', 'BANANAS31USDT', 'BANANAUSDT',
               'BANDUSDT', 'BARDUSDT', 'BATUSDT', 'BBUSDT', 'BCHUSDT', 'BEAMUSDT', 'BERAUSDT', 'BICOUSDT',
               'BIGTIMEUSDT', 'BIOUSDT', 'BITOUSDT', 'BLASTUSDT', 'BLURUSDT', 'BNBUSDT', 'BOMEUSDT', 'BRETTUSDT',
               'BREVUSDT', 'BSVUSDT', 'BTCUSDT', 'BZUSDT', 'C98USDT', 'CAKEUSDT', 'CARVUSDT', 'CATIUSDT', 'CCUSDT',
               'CELOUSDT', 'CFGUSDT', 'CFXUSDT', 'CGPTUSDT', 'CHILLGUYUSDT', 'CHIPUSDT', 'CHRUSDT', 'CHZUSDT',
               'CKBUSDT', 'CLUSDT', 'COMPUSDT', 'CONLUSDT', 'COOKIEUSDT', 'COREUSDT', 'COTIUSDT', 'COWUSDT', 'CROUSDT',
               'CRVUSDT', 'CSOPSAMSUNG2LUSDT', 'CSOPSKHYNIX2LUSDT', 'CTCUSDT', 'CVCUSDT', 'CVXUSDT', 'CYBERUSDT',
               'DASHUSDT', 'DATAUSDT', 'DEEPUSDT', 'DEXEUSDT', 'DOGEUSDT', 'DOTUSDT', 'DRAMUSDT', 'DRIFTUSDT',
               'DYDXUSDT', 'DYMUSDT', 'EDENUSDT', 'EDGEUSDT', 'EGLDUSDT', 'EIGENUSDT', 'ENAUSDT', 'ENJUSDT', 'ENSOUSDT',
               'ENSUSDT', 'ERAUSDT', 'ESPUSDT', 'ETCUSDT', 'ETHBTCUSDT', 'ETHFIUSDT', 'ETHUSDT', 'EULUSDT', 'EWJUSDT',
               'EWTUSDT', 'EWYUSDT', 'EWZUSDT', 'FARTCOINUSDT', 'FFUSDT', 'FIDAUSDT', 'FILUSDT', 'FLOWUSDT', 'FLRUSDT',
               'FLUIDUSDT', 'FLUXUSDT', 'FOGOUSDT', 'FORMUSDT', 'FUSDT', 'GALAUSDT', 'GASUSDT', 'GBPUSDUSDT', 'GDXUSDT',
               'GLMUSDT', 'GMTUSDT', 'GMXUSDT', 'GOATUSDT', 'GRAMUSDT', 'GRASSUSDT', 'GRIFFAINUSDT', 'GRTUSDT',
               'GUNUSDT', 'HBARUSDT', 'HIVEUSDT', 'HMSTRUSDT', 'HOLOUSDT', 'HOMEUSDT', 'HUMAUSDT', 'HYPERUSDT',
               'HYPEUSDT', 'IBITUSDT', 'ICNTUSDT', 'ICPUSDT', 'IDUSDT', 'ILVUSDT', 'IMXUSDT', 'INITUSDT', 'INJUSDT',
               'INTWUSDT', 'IOSTUSDT', 'IOTAUSDT', 'IOTXUSDT', 'IOUSDT', 'IWMUSDT', 'JASMYUSDT', 'JSTUSDT', 'JTOUSDT',
               'JUPUSDT', 'KAIAUSDT', 'KAITOUSDT', 'KASUSDT', 'KATUSDT', 'KAVAUSDT', 'KERNELUSDT', 'KGENUSDT',
               'KITEUSDT', 'KMNOUSDT', 'KNCUSDT', 'KODEX200USDT', 'KORUUSDT', 'KSMUSDT', 'KSTRUSDT', 'LAUSDT',
               'LDOUSDT', 'LINEAUSDT', 'LINKUSDT', 'LITUSDT', 'LPTUSDT', 'LQTYUSDT', 'LRCUSDT', 'LSKUSDT', 'LTCUSDT',
               'LUNA2USDT', 'LYNUSDT', 'LYTEUSDT', 'MAGICUSDT', 'MAGSUSDT', 'MANAUSDT', 'MANTAUSDT', 'MASKUSDT',
               'MAVIAUSDT', 'MAVUSDT', 'MEGAUSDT', 'MELANIAUSDT', 'MEMEUSDT', 'MERLUSDT', 'METISUSDT', 'METUSDT',
               'MEUSDT', 'MEWUSDT', 'MINAUSDT', 'MIRAUSDT', 'MMTUSDT', 'MNTUSDT', 'MOCAUSDT', 'MONUSDT']

    sess = create_session_with_proxy(DEMO_CONFIG)
    for symbol in all_sym:
        result = signal(symbol, session=sess)
        print(result)
