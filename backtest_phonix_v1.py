# backtest_phonix_v1.py

import os
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import pandas as pd

import start_phonix_v1 as sp

logger = sp.logger

# Директория с минутными CSV
DATA_DIR = r"C:\Users\PRO\PycharmProjects\Phonix\bybit_linear_1m"

# Явно задаём список торгуемых символов для бэктеста (как в live, но без XAGUSDT)
BACKTEST_SYMBOLS = [
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT",
    "XRPUSDT",
    "DOGEUSDT",
    "SUIUSDT",
    "ADAUSDT",
    "NEARUSDT",
    "LINKUSDT",
    "ENSOUSDT",
    "BNBUSDT",
    "ZECUSDT",
    "AAVEUSDT",
    "AVAXUSDT",
    "ICPUSDT",
    "BCHUSDT",
    "WLDUSDT",
    "WIFUSDT",
    "RENDERUSDT",
    "DOTUSDT",
    "LTCUSDT",
    "GALAUSDT",
    "FLOWUSDT",
    "CRVUSDT",
    "APTUSDT",
    "XAUTUSDT",
    # "XAGUSDT",  # исключён, т.к. истории нет
]




# ========= CSV minute feed =========

class CsvMinuteFeed:
    def __init__(self, data_dir: str, symbols: List[str]):
        self.data_dir = data_dir
        self.symbols = symbols
        self.data_1m: Dict[str, pd.DataFrame] = {}
        self._load_all()

    def _load_all(self):
        all_times = None
        for symbol in self.symbols:
            fname = f"{symbol}-1m-2026-01_02.csv"
            path = os.path.join(self.data_dir, fname)
            if not os.path.exists(path):
                logger.warning("CSV for %s not found at %s, skipping symbol", symbol, path)
                continue

            df = pd.read_csv(
                path,
                header=None,
                names=[
                    "open_time",  # ms
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "quote_volume",
                ],
            )
            df["datetime"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
            df.set_index("datetime", inplace=True)
            df = df[["open", "high", "low", "close", "volume"]].astype(float)
            df.sort_index(inplace=True)

            self.data_1m[symbol] = df

            if all_times is None:
                all_times = set(df.index)
            else:
                all_times |= set(df.index)

        if not all_times:
            raise RuntimeError("No data loaded for any symbol")

        self.global_times = sorted(all_times)
        self.global_pos = 0

        logger.info(
            "Loaded minute data for %d symbols, total %d minutes",
            len(self.data_1m),
            len(self.global_times),
        )

    def has_next(self) -> bool:
        return self.global_pos < len(self.global_times)

    def next_timestamp(self) -> datetime:
        ts = self.global_times[self.global_pos]
        self.global_pos += 1
        return ts

    def get_bar_1m(self, symbol: str, ts: datetime) -> Optional[dict]:
        df = self.data_1m.get(symbol)
        if df is None:
            return None
        try:
            row = df.loc[ts]
        except KeyError:
            return None
        return {
            "open": float(row["open"]),
            "high": float(row["high"]),
            "low": float(row["low"]),
            "close": float(row["close"]),
            "volume": float(row["volume"]),
            "timestamp": ts,
        }


# ========= Dummy session for Bybit HTTP =========

class DummySession:
    """
    Мок-объект вместо реального HTTP-соединения с Bybit.
    Все методы возвращают успешный пустой результат.
    """

    def __getattr__(self, item):
        def _dummy(*args, **kwargs):
            logger.debug("DummySession.%s called with %s %s", item, args, kwargs)
            return {"retCode": 0, "retMsg": "OK", "result": {"list": []}}

        return _dummy


BT_SESSION = DummySession()
CURRENT_TIME: Optional[datetime] = None


def bt_set_time(ts: datetime):
    global CURRENT_TIME
    CURRENT_TIME = ts


# ========= In-memory "DB" for TradeState =========

ACTIVE_TRADES: Dict[str, sp.TradeState] = {}
ALL_TRADES: List[sp.TradeState] = []


def bt_save_trade_state(trade: sp.TradeState, trade_id: Optional[int] = None) -> int:
    """
    Заменяет sp.save_trade_state в режиме бэктеста.
    Сохраняет сделки в память вместо SQLite.
    """
    ACTIVE_TRADES[trade.symbol] = trade

    if trade.status in ("closed", "timeout", "cancelled", "max_hold"):
        ALL_TRADES.append(trade)
        ACTIVE_TRADES.pop(trade.symbol, None)

    # id нам не нужен, возвращаем фиктивный 0
    return 0


def bt_load_active_trade(symbol: str) -> Tuple[Optional[sp.TradeState], Optional[int]]:
    """
    Заменяет sp.load_active_trade в режиме бэктеста.
    """
    trade = ACTIVE_TRADES.get(symbol)
    if trade is None:
        return None, None
    return trade, 0


def bt_count_active_trades() -> int:
    """
    Заменяет sp.count_active_trades в режиме бэктеста.
    """
    return len(ACTIVE_TRADES)


# Подменяем функции в основном модуле
sp.save_trade_state = bt_save_trade_state
sp.load_active_trade = bt_load_active_trade
sp.count_active_trades = bt_count_active_trades


# ========= Backtest main loop =========

def run_backtest():
    feed = CsvMinuteFeed(DATA_DIR, BACKTEST_SYMBOLS)
    logger.info("Starting backtest over %d minutes", len(feed.global_times))

    prev_hour: Optional[datetime] = None

    while feed.has_next():
        ts = feed.next_timestamp()
        bt_set_time(ts)

        # 1) Обновляем активные сделки по 1m барам
        for symbol, trade in list(ACTIVE_TRADES.items()):
            bar = feed.get_bar_1m(symbol, ts)
            if bar is None:
                continue

            updated = sp.update_trade_state_with_1m_bar(
                session=BT_SESSION,
                trade=trade,
                bar_time=bar["timestamp"],
                bar_open=bar["open"],
                bar_high=bar["high"],
                bar_low=bar["low"],
                bar_close=bar["close"],
            )

            ACTIVE_TRADES[symbol] = updated
            if updated.status in ("closed", "timeout", "cancelled", "max_hold"):
                ALL_TRADES.append(updated)
                ACTIVE_TRADES.pop(symbol, None)

        # 2) На смене часа запускаем генерацию 1H-сигналов
        cur_hour = ts.replace(minute=0, second=0, microsecond=0)
        if prev_hour is None:
            prev_hour = cur_hour

        # Новый час, и мы на первом баре часа (minute == 0)
        if cur_hour != prev_hour and ts.minute == 0:
            logger.info("1H signal tick at %s", ts.isoformat())
            for symbol in BACKTEST_SYMBOLS:
                sp.process_symbol_1h_signal(session=BT_SESSION, symbol=symbol)
            prev_hour = cur_hour

    logger.info(
        "Backtest finished. Closed trades: %d, still active: %d",
        len(ALL_TRADES),
        len(ACTIVE_TRADES),
    )
    build_report_from_trades(ALL_TRADES)


# ========= Simple report =========

def build_report_from_trades(trades: List[sp.TradeState]):
    if not trades:
        logger.info("No trades in backtest")
        return

    pnls = []
    wins = 0
    for t in trades:
        if t.entry_price is None or t.exit_price is None:
            continue
        if t.direction == 1:
            pnl_pct = (t.exit_price - t.entry_price) / t.entry_price
        else:
            pnl_pct = (t.entry_price - t.exit_price) / t.entry_price

        pnl_usdt = pnl_pct * sp.TARGET_NOTIONAL
        pnls.append(pnl_usdt)
        if pnl_pct > 0:
            wins += 1

    if not pnls:
        logger.info("No completed trades with entry/exit prices")
        return

    total_pnl = sum(pnls)
    n = len(pnls)
    wr = wins / n * 100.0

    logger.info("===== BACKTEST RESULTS =====")
    logger.info("Trades: %d", n)
    logger.info("Total PnL (USDT): %.2f", total_pnl)
    logger.info("Avg PnL per trade (USDT): %.3f", total_pnl / n)
    logger.info("Win rate: %.1f%%", wr)


if __name__ == "__main__":
    run_backtest()
