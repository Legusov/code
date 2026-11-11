#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Streamlit-дашборд для мониторинга активных комплексов.
"""

import asyncio
import sys
import json
import os
import sqlite3
import threading
import time
from datetime import datetime, timezone

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st
from pybit.unified_trading import HTTP
from typing import Dict, List, Tuple

try:
    from key import API_KEY, API_SECRET
except ImportError:
    st.error("Не найден key.py с API_KEY и API_SECRET")
    st.stop()

# ============================================================
# ФИКС ОШИБКИ ASYNCIO НА WINDOWS (WinError 10054)
# ============================================================
def _ignore_asyncio_windows_errors(loop, context):
    exception = context.get("exception")
    if isinstance(exception, ConnectionResetError):
        return
    loop.default_exception_handler(context)

if sys.platform == "win32":
    try:
        loop = asyncio.get_event_loop()
        loop.set_exception_handler(_ignore_asyncio_windows_errors)
    except Exception:
        pass

# ============================================================
# КОНФИГ
# ============================================================
STATE_PATH = "state.json"
DB_PATH = r"C:\Users\Admin\bybit_1m.db"
TRADES_DB = "trades.db"
REFRESH_STATS_SEC = 10
REFRESH_CHARTS_SEC = 30

st.set_page_config(
    page_title="Spread Bot Dashboard",
    layout="wide",
    page_icon="📊",
)

st.markdown("<style>div[data-testid='stStatusWidget'] {display: none;}</style>", unsafe_allow_html=True)


# ============================================================
# ФОНОВЫЙ ЗАГРУЗЧИК ДАННЫХ
# ============================================================
class DashboardData:
    def __init__(self):
        self.balance = {"equity": 0.0, "available": 0.0}
        self.prices = {}
        self.last_update = 0.0
        self.lock = threading.Lock()


@st.cache_resource
def get_data_fetcher():
    data = DashboardData()

    def worker():
        session = HTTP(testnet=False, demo=True, api_key=API_KEY, api_secret=API_SECRET)
        while True:
            try:
                b = session.get_wallet_balance(accountType="UNIFIED")["result"]["list"][0]
                bal = {
                    "equity": float(b.get("totalEquity", 0)),
                    "available": float(b.get("totalAvailableBalance", 0)),
                }

                tickers = session.get_tickers(category="linear")["result"]["list"]
                prices = {t["symbol"]: float(t["lastPrice"]) for t in tickers}

                with data.lock:
                    data.balance = bal
                    data.prices = prices
                    data.last_update = time.time()
            except Exception as e:
                print(f"[FETCHER ERROR] Ошибка фонового потока: {e}")

            time.sleep(5)

    t = threading.Thread(target=worker, daemon=True)
    t.start()
    return data


FETCHER = get_data_fetcher()


# ============================================================
# БЫСТРЫЕ ЗАПРОСЫ ИЗ БД
# ============================================================
@st.cache_data(ttl=3600)
def get_month_refs_cached(symbols_tuple: tuple) -> Dict[str, float]:
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    month_ago = now_ms - 30 * 86400_000
    refs = {}
    try:
        with sqlite3.connect(DB_PATH) as conn:
            for sym in symbols_tuple:
                row = conn.execute(
                    "SELECT close FROM ohlcv WHERE symbol=? AND timeframe='1' "
                    "AND timestamp>=? ORDER BY timestamp LIMIT 1",
                    (sym, month_ago)
                ).fetchone()
                if row and row[0] > 0:
                    refs[sym] = float(row[0])
    except Exception:
        pass
    return refs


@st.cache_data(ttl=2)
def load_state() -> Dict:
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"active": [], "reserve": []}


@st.cache_data(ttl=REFRESH_CHARTS_SEC)
def get_trade_events(leg1: tuple, leg2: tuple, layers: List[Dict], d_max: float) -> List[Dict]:
    """Сопоставляет события текущего цикла слоям состояния по порядку."""
    if not os.path.exists(TRADES_DB):
        return []
    complex_id = "|".join(list(leg1) + list(leg2))
    try:
        with sqlite3.connect(TRADES_DB, timeout=10) as conn:
            df = pd.read_sql_query(
                "SELECT ts_ms, event, details FROM trades WHERE complex_id=? ORDER BY ts_ms ASC, id ASC",
                conn, params=(complex_id,)
            )
    except Exception as e:
        print(f"Ошибка чтения trades.db: {e}")
        return []
    if df.empty:
        return []

    cycle = []
    for _, row in df.iterrows():
        if row["event"] == "open":
            cycle = [row]
        elif row["event"] == "add_layer" and cycle:
            cycle.append(row)
        elif row["event"] == "close" and cycle:
            cycle = []
    if not cycle:
        return []

    events = []
    for idx, row in enumerate(cycle[:len(layers)]):
        try:
            details = json.loads(row.get("details") or "{}")
        except (TypeError, ValueError):
            details = {}
        layer = layers[idx]
        qty = layer.get("qty", {})
        prices = layer.get("add_prices", {})
        notionals = []
        for sym, q in qty.items():
            px = float(prices.get(sym, 0.0) or 0.0)
            if px <= 0:
                px = float(FETCHER.prices.get(sym, 0.0) or 0.0)
            if px > 0:
                notionals.append(f"{sym.replace('USDT', '')}: ${float(q) * px:,.0f}")
        events.append({
            "ts_ms": int(row["ts_ms"]),
            "type": str(row["event"]),
            "layer_idx": idx,
            "label": "Вход" if idx == 0 else f"Добор {idx}",
            "notionals": " · ".join(notionals),
            "stored_spread": float(layer.get("spread", details.get("entry_spread", 0.0)) or 0.0),
            "spread_snapshot": layer.get("spread_snapshot", {}),
            "norm_refs": {},
            "leg1": list(leg1),
            "leg2": list(leg2),
            "entry_spread": 0.0,
            "d_max": float(d_max or 0.0),
        })
    return events


@st.cache_data(ttl=REFRESH_CHARTS_SEC)
def load_multi_chart_data_cached(leg1: tuple, leg2: tuple, entry_refs_tuple: tuple,
                                 entry_spread: float, open_ts: int):
    """Строит все периоды на одной нормировке и одном сдвиге относительно входа."""
    refs = {str(k): float(v) for k, v in entry_refs_tuple if float(v or 0) > 0}
    all_syms = list(dict.fromkeys(list(leg1) + list(leg2)))
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    month_ago = now_ms - 30 * 86400_000
    year_ago = now_ms - 365 * 86400_000

    def _query(symbol, timeframe, lo_ms, hi_ms):
        try:
            with sqlite3.connect(DB_PATH, timeout=10) as conn:
                return pd.read_sql_query(
                    "SELECT timestamp, close FROM ohlcv WHERE symbol=? AND timeframe=? "
                    "AND timestamp BETWEEN ? AND ? ORDER BY timestamp",
                    conn, params=(symbol, timeframe, lo_ms, hi_ms))
        except Exception as exc:
            print(f"Ошибка чтения OHLCV {symbol}/{timeframe}: {exc}")
            return pd.DataFrame()

    def _build(timeframe, lo_ms, rule=None):
        frames = {}
        for sym in all_syms:
            part = _query(sym, timeframe, lo_ms, now_ms)
            if not part.empty:
                frames[sym] = part.drop_duplicates("timestamp").set_index("timestamp")["close"]
        if len(frames) != len(all_syms):
            return pd.DataFrame()
        raw = pd.DataFrame(frames).sort_index().ffill().dropna()
        if raw.empty:
            return pd.DataFrame()
        # У одной и той же монеты во всех вкладках один базовый уровень.
        # При отсутствии сохранённой базы используем первую точку общего ряда.
        for sym in all_syms:
            ref = refs.get(sym)
            if not ref or ref <= 0:
                ref = float(raw[sym].iloc[0])
            raw[sym] = raw[sym] / ref
        n1, n2 = len(leg1), len(leg2)
        if n1 == 0 or n2 == 0:
            return pd.DataFrame()
        out = pd.DataFrame(index=raw.index)
        out["leg1"] = raw[list(leg1)].mean(axis=1)
        out["leg2"] = raw[list(leg2)].mean(axis=1)
        out["spread"] = out["leg1"] - out["leg2"]
        out.index = pd.to_datetime(out.index, unit="ms", utc=True)
        if rule:
            # Берём последнее фактическое значение периода, а не среднее,
            # чтобы закрытие бара совпадало с ценовой кривой на этом TF.
            out = out.resample(rule).last().dropna()
        if out.empty:
            return out
        open_dt = pd.to_datetime(open_ts, unit="ms", utc=True)
        # Постоянный сдвиг для всех таймфреймов: сигнал первичного входа
        # плюс изменение нормализованного спреда от сохранённой базы norm_refs.
        offset = float(entry_spread)
        out["leg1"] += offset / 2.0
        out["leg2"] -= offset / 2.0
        out["spread"] += offset
        return out

    year = _build("60", year_ago)
    month = _build("1", month_ago, "1h")
    day = _build("1", now_ms - 24 * 3600_000, "5min")
    hour = _build("1", now_ms - 3600_000)
    return year, month, day, hour


def _event_value(df: pd.DataFrame, timestamp_ms: int):
    """Значение той же кривой на точном времени события (интерполяция между барами)."""
    if df.empty:
        return None
    dt = pd.to_datetime(timestamp_ms, unit="ms", utc=True)
    series = df["spread"].dropna()
    if series.empty or dt < series.index.min() or dt > series.index.max():
        return None
    augmented = series.reindex(series.index.union([dt])).sort_index().interpolate(method="time")
    value = augmented.get(dt)
    return float(value) if pd.notna(value) else None


def plot_chart(df: pd.DataFrame, title: str, chart_key: str, events: List[Dict]):
    if df.empty:
        st.info("Для выбранного периода недостаточно совместных данных по всем ногам.")
        return

    # Цены и spread намеренно на отдельных панелях: spread не маскируется
    # масштабом нормализованных цен и не выглядит как касание кривых.
    fig = make_subplots(
        rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.08,
        row_heights=[0.66, 0.34],
        subplot_titles=("Средние нормализованные цены ног", "Спред Leg 1 − Leg 2"),
    )
    fig.add_trace(go.Scatter(
        x=df.index, y=df["leg1"], name="Среднее норм. Leg 1",
        line=dict(color="#2878B5", width=1.7),
    ), row=1, col=1)
    fig.add_trace(go.Scatter(
        x=df.index, y=df["leg2"], name="Среднее норм. Leg 2",
        line=dict(color="#E87524", width=1.7),
    ), row=1, col=1)
    fig.add_trace(go.Scatter(
        x=df.index, y=df["spread"], name="Спред Leg1 − Leg2",
        line=dict(color="#238636", width=2), fill="tozeroy",
        fillcolor="rgba(35,134,54,0.10)",
    ), row=2, col=1)

    visible = []
    last_event_spread = None
    for ev in events:
        ev_dt = pd.to_datetime(ev["ts_ms"], unit="ms", utc=True)
        # Не расширять ось времени событием вне периода графика. Иначе на
        # вкладке последнего часа маркер старого входа оказывается в пустоте.
        if ev_dt < df.index.min() or ev_dt > df.index.max():
            continue
        # Событие использует ценовой snapshot слоя, сохранённый ботом.
        snapshot = ev.get("spread_snapshot", {})
        norm_refs = ev.get("norm_refs", {})
        raw_snapshot_spread = None
        if snapshot and norm_refs:
            try:
                v1 = [float(snapshot[s]) / float(norm_refs[s]) for s in ev["leg1"]]
                v2 = [float(snapshot[s]) / float(norm_refs[s]) for s in ev["leg2"]]
                if all(x > 0 for x in v1 + v2):
                    raw_snapshot_spread = sum(v1) / len(v1) - sum(v2) / len(v2)
            except (KeyError, TypeError, ValueError, ZeroDivisionError):
                pass
        curve_value = _event_value(df, ev["ts_ms"])
        value = (float(ev["entry_spread"]) + raw_snapshot_spread
                 if raw_snapshot_spread is not None else curve_value)
        if value is None:
            continue

        is_open = ev["type"] == "open"
        color = "#00a878" if is_open else "#e87524"
        delta = None if last_event_spread is None else value - last_event_spread
        ratio = abs(value) / ev["d_max"] if ev["d_max"] > 0 else 0.0
        ev["chart_spread"] = value
        ev["delta_from_previous"] = delta
        ev["ratio"] = ratio
        visible.append(ev)
        last_event_spread = value

        # Вертикаль связывает событие с ценовыми кривыми; кружок же расположен
        # только на отдельной оси спреда и показывает его реальную величину.
        fig.add_vline(x=ev_dt, line_width=1.2, line_dash="dash", line_color=color,
                      opacity=0.8, row="all", col=1)
        fig.add_trace(go.Scatter(
            x=[ev_dt], y=[value], mode="markers+text", text=[str(ev["layer_idx"] + 1)],
            textposition="top center", name=f"{ev['label']} · S {value:+.5f}",
            marker=dict(size=12, color=color, symbol="circle", line=dict(color="white", width=1.5)),
            customdata=[[ev["label"], value, ratio,
                         delta if delta is not None else 0.0, ev["notionals"]]],
            hovertemplate=("<b>%{customdata[0]}</b><br>Спред при событии: %{customdata[1]:+.5f}"
                           "<br>|S| / D-max: %{customdata[2]:.2f}"
                           "<br>Изменение к предыдущему событию: %{customdata[3]:+.5f}"
                           "<br>%{customdata[4]}<br>%{x|%d.%m.%Y %H:%M:%S UTC}<extra></extra>"),
        ), row=2, col=1)

    date_range = f"{df.index.min():%d.%m.%Y %H:%M} — {df.index.max():%d.%m.%Y %H:%M} UTC"
    fig.update_layout(
        title=f"{title} · {date_range}", height=560,
        margin=dict(l=25, r=25, t=80, b=25), hovermode="x unified",
        legend=dict(orientation="h", y=1.08, x=0), plot_bgcolor="#fafafa",
    )
    fig.update_yaxes(title_text="Средняя нормализованная цена", gridcolor="#e8e8e8", row=1, col=1)
    fig.update_yaxes(title_text="Спред (значение события)", zeroline=True,
                     zerolinecolor="#777", gridcolor="#e8e8e8", row=2, col=1)
    fig.add_hline(y=0, line_dash="dash", line_color="#888", row=2, col=1)
    st.plotly_chart(fig, width="stretch", key=chart_key)

    if visible:
        rows = ["<div style='background:#f8f9fa;padding:10px;border-radius:6px;border:1px solid #e9ecef;font-family:monospace;font-size:13px'>"]
        for ev in visible:
            color = "#00a878" if ev["type"] == "open" else "#e87524"
            change = "первичный вход" if ev["delta_from_previous"] is None else f"изменение к прошлому событию {ev['delta_from_previous']:+.5f}"
            rows.append(f"<div style='color:{color};font-weight:600'>● {ev['layer_idx'] + 1}. {ev['label']}: "
                        f"S={ev['chart_spread']:+.5f} · |S|/D-max={ev['ratio']:.2f} · {change} "
                        f"· {datetime.fromtimestamp(ev['ts_ms']/1000, timezone.utc):%d.%m.%Y %H:%M:%S UTC}</div>")
            if ev["notionals"]:
                rows.append(f"<div style='padding:0 0 5px 18px;color:#555'>{ev['notionals']}</div>")
        rows.append("</div>")
        st.markdown("".join(rows), unsafe_allow_html=True)


# ============================================================
# МАТЕМАТИЧЕСКИЙ РАСЧЕТ PNL
# ============================================================
def complex_gross_pnl(cs: Dict, prices: Dict[str, float]) -> float:
    pnl = float(cs.get("realized_pnl", 0.0))
    leg1_long = cs.get("leg1_long", True)
    
    for sym in cs["complex"]["leg1"]:
        current = prices.get(sym, 0.0)
        if current <= 0: continue
        
        total_qty = 0.0
        total_value = 0.0
        for layer in cs.get("layers", []):
            qty = layer.get("qty", {}).get(sym, 0.0)
            entry = layer.get("add_prices", {}).get(sym, 0.0)
            if qty > 0 and entry > 0:
                total_qty += qty
                total_value += qty * entry
                
        if total_qty <= 0: continue
        avg_entry = total_value / total_qty
        
        if leg1_long:
            pnl += total_qty * (current - avg_entry)
        else:
            pnl += total_qty * (avg_entry - current)
            
    for sym in cs["complex"]["leg2"]:
        current = prices.get(sym, 0.0)
        if current <= 0: continue
        
        total_qty = 0.0
        total_value = 0.0
        for layer in cs.get("layers", []):
            qty = layer.get("qty", {}).get(sym, 0.0)
            entry = layer.get("add_prices", {}).get(sym, 0.0)
            if qty > 0 and entry > 0:
                total_qty += qty
                total_value += qty * entry
                
        if total_qty <= 0: continue
        avg_entry = total_value / total_qty
        
        if leg1_long:
            pnl += total_qty * (avg_entry - current)
        else:
            pnl += total_qty * (current - avg_entry)
            
    return pnl


def complex_net_pnl(cs: Dict, prices: Dict[str, float], taker_rate: float) -> float:
    gross = complex_gross_pnl(cs, prices)
    fees_paid = float(cs.get("fees_paid", 0.0))
    exit_cost_rate = taker_rate + 0.0010
    
    exit_cost_est = 0.0
    for layer in cs.get("layers", []):
        for sym, qty in layer.get("qty", {}).items():
            p = prices.get(sym, 0.0)
            if qty > 0 and p > 0:
                exit_cost_est += qty * p * exit_cost_rate
                
    return gross - fees_paid - exit_cost_est


def complex_spread_now(cs: Dict, prices: Dict[str, float]) -> float:
    syms = list(cs["complex"]["leg1"]) + list(cs["complex"]["leg2"])
    n1 = len(cs["complex"]["leg1"])
    n2 = len(cs["complex"]["leg2"])
    if n1 == 0 or n2 == 0:
        return 0.0
    refs = cs.get("entry_month_refs", {}) or get_month_refs_cached(tuple(syms))
    vals = []
    for sym in syms:
        p = float(prices.get(sym, 0.0) or 0.0)
        ref = float(refs.get(sym, 0.0) or 0.0)
        if p <= 0 or ref <= 0:
            return 0.0
        vals.append(p / ref)
    return float(cs.get("entry_spread", 0.0)) + sum(vals[:n1]) / n1 - sum(vals[n1:]) / n2


# ============================================================
# HTML ТАБЛИЦЫ
# ============================================================
def generate_active_html_table(df: pd.DataFrame) -> str:
    css = "<style>.custom-table{width:100%;border-collapse:collapse;font-family:'Source Sans Pro',sans-serif;font-size:14px;}.custom-table th{padding:8px 12px;text-align:left;border-bottom:1px solid rgba(128,128,128,0.2);font-weight:600;}.custom-table td{padding:8px 12px;border-bottom:1px solid rgba(128,128,128,0.2);vertical-align:middle;}.pnl-pos{color:#21c354;font-weight:600;}.pnl-neg{color:#ff4b4b;font-weight:600;}.progress-wrapper{display:flex;align-items:center;gap:10px;min-width:130px;}.progress-track{flex-grow:1;height:8px;background-color:rgba(128,128,128,0.2);border-radius:4px;position:relative;}.progress-fill-pos{position:absolute;left:50%;height:100%;background-color:#21c354;border-radius:0 4px 4px 0;}.progress-fill-neg{position:absolute;right:50%;height:100%;background-color:#ff4b4b;border-radius:4px 0 0 4px;}.progress-text{font-size:12px;min-width:55px;text-align:right;}</style>"
    html = [css,
            "<table class='custom-table'><thead><tr><th>#</th><th>Статус</th><th>Комплект</th><th>Слоёв</th><th>Gross $</th><th>Net $</th><th>Target $</th><th>Прогресс %</th><th>Entry S</th><th>Current S</th></tr></thead><tbody>"]

    for _, row in df.iterrows():
        gross = row['Gross $']
        net = row['Net $']
        prog = row['Прогресс %']

        g_cls = "pnl-pos" if gross >= 0 else "pnl-neg"
        n_cls = "pnl-pos" if net >= 0 else "pnl-neg"

        fill_html = ""
        if prog > 0:
            width = min(prog, 100) / 2
            fill_html = f"<div class='progress-fill-pos' style='width: {width}%;'></div>"
        elif prog < 0:
            width = min(abs(prog), 100) / 2
            fill_html = f"<div class='progress-fill-neg' style='width: {width}%;'></div>"

        row_html = f"<tr><td>{row['#']}</td><td>{row['Статус']}</td><td>{row['Комплект']}</td><td>{row['Слоёв']}</td><td class='{g_cls}'>${gross:,.2f}</td><td class='{n_cls}'>${net:,.2f}</td><td>${row['Target $']:,.2f}</td><td><div class='progress-wrapper'><div class='progress-track'>{fill_html}</div><div class='progress-text'>{prog:.1f}%</div></div></td><td>{row['Entry S']:+.4f}</td><td>{row['Current S']:+.4f}</td></tr>"
        html.append(row_html)

    html.append("</tbody></table>")
    return "".join(html)


def generate_reserve_html_table(reserve: List[Dict]) -> str:
    css = "<style>.res-table{width:100%;border-collapse:collapse;font-family:'Source Sans Pro',sans-serif;font-size:14px;margin-top:10px;}.res-table th{padding:8px 12px;text-align:left;border-bottom:1px solid rgba(128,128,128,0.2);font-weight:600;}.res-table td{padding:8px 12px;border-bottom:1px solid rgba(128,128,128,0.2);}</style>"
    html = [css,
            "<table class='res-table'><thead><tr><th>#</th><th>Leg 1</th><th>Leg 2</th><th>Score</th><th>D-Max</th></tr></thead><tbody>"]
    for i, r in enumerate(reserve, 1):
        score = r.get('score', 0)
        dmax = r.get('d_max', 0)
        html.append(
            f"<tr><td>{i}</td><td>{' / '.join(r['leg1'])}</td><td>{' / '.join(r['leg2'])}</td><td>{score:.2f}</td><td>{dmax:.4f}</td></tr>")
    html.append("</tbody></table>")
    return "".join(html)


# ============================================================
# UI: СТАТИСТИКА
# ============================================================
@st.fragment(run_every=REFRESH_STATS_SEC)
def render_stats():
    state = load_state()
    active: List[Dict] = state.get("active", [])
    reserve: List[Dict] = state.get("reserve", [])

    with FETCHER.lock:
        balance = FETCHER.balance
        prices = FETCHER.prices
        last_update = FETCHER.last_update

    update_time = datetime.fromtimestamp(last_update).strftime('%H:%M:%S') if last_update > 0 else "Ожидание..."
    st.caption(f"Текущие цены обновлены: {update_time}")

    taker_rate = 0.00055
    total_gross = sum(complex_gross_pnl(cs, prices) for cs in active)
    total_pnl = sum(complex_net_pnl(cs, prices, taker_rate) for cs in active)

    total_realized = sum(cs.get("realized_pnl", 0) for cs in active)
    total_unrealized = total_gross - total_realized

    color_total = "#21c354" if total_pnl >= 0 else "#ff4b4b"
    color_gross = "#21c354" if total_gross >= 0 else "#ff4b4b"
    color_unrl = "#21c354" if total_unrealized >= 0 else "#ff4b4b"
    color_equity = "#21c354" if balance['equity'] >= 2000 else "#ff4b4b"

    kpi1, kpi2, kpi3, kpi4 = st.columns(4)

    kpi1.markdown(
        f'<div style="font-size: 14px; color: #31333f; margin-bottom: 4px;">💰 Equity</div><div style="font-size: 36px; font-weight: 600; color: {color_equity}; line-height: 1.2;">${balance["equity"]:.2f}</div><div style="font-size: 14px; color: rgba(49, 51, 63, 0.6);">${balance["equity"] - 2000:+.2f} от старта</div>',
        unsafe_allow_html=True)

    kpi2.markdown(
        f'<div style="font-size: 14px; color: #31333f; margin-bottom: 4px;">📈 Net PnL</div><div style="font-size: 36px; font-weight: 600; color: {color_total}; line-height: 1.2;">${total_pnl:.2f}</div><div style="font-size: 14px; color: rgba(49, 51, 63, 0.6);">gross <span style="color: {color_gross}; font-weight: 600;">{total_gross:+.2f}</span> / unrl <span style="color: {color_unrl}; font-weight: 600;">{total_unrealized:+.2f}</span></div>',
        unsafe_allow_html=True)

    kpi3.metric("🟢 Активных", len(active), delta=f"{len(reserve)} в резерве")
    kpi4.metric("💵 Свободно", f"${balance['available']:.2f}")

    st.divider()
    st.subheader("Активные комплексы")

    if not active:
        st.info("⏳ Нет активных комплексов. Бот ждет раздвижения спреда для входа.")
        if reserve:
            st.write("**Комплекты в резерве (ожидают входа):**")
            st.markdown(generate_reserve_html_table(reserve), unsafe_allow_html=True)
    else:
        rows = []
        for i, cs in enumerate(active, 1):
            pnl_net = complex_net_pnl(cs, prices, taker_rate)
            pnl_gross = complex_gross_pnl(cs, prices)
            target = cs.get("target", 0)
            progress = max(-100.0, min(100.0, (pnl_net / target * 100) if target > 0 else 0))

            spread_now = complex_spread_now(cs, prices)
            entry = cs.get("entry_spread", 0)

            if pnl_net >= target:
                status = "🎯 ЦЕЛЬ"
            elif cs.get("hedge_open"):
                status = "🛡 ХЕДЖ"
            elif pnl_net < -10:
                status = "⚠ ПРОСАДКА"
            else:
                status = "⏳ В РАБОТЕ"

            rows.append({
                "#": i,
                "Статус": status,
                "Комплект": f"{'/'.join(cs['complex']['leg1'])} | {'/'.join(cs['complex']['leg2'])}",
                "Слоёв": len(cs.get("layers", [])),
                "Gross $": pnl_gross,
                "Net $": pnl_net,
                "Target $": target,
                "Прогресс %": progress,
                "Entry S": entry,
                "Current S": spread_now,
            })

        df = pd.DataFrame(rows)
        st.markdown(generate_active_html_table(df), unsafe_allow_html=True)


# ============================================================
# UI: ГРАФИКИ
# ============================================================
@st.fragment(run_every=REFRESH_CHARTS_SEC)
def render_charts():
    state = load_state()
    active: List[Dict] = state.get("active", [])
    if not active:
        return
    with FETCHER.lock:
        prices = dict(FETCHER.prices)
    st.divider()
    st.subheader("История спреда и фактические моменты входа / доборов")
    st.caption("Dashboard build: 2026-11-06-spread-panel-v2 · верхняя панель — нормализованные цены; нижняя — величина спреда и события. Вкладка года использует часовые свечи.")
    for i, cs in enumerate(active, 1):
        leg1 = tuple(cs["complex"]["leg1"])
        leg2 = tuple(cs["complex"]["leg2"])
        title = f"#{i} · {' / '.join(leg1)} ⟷ {' / '.join(leg2)}"
        key_id = str(cs.get("id") or "|".join(leg1 + leg2)).replace(" ", "_")
        col_title, col_toggle = st.columns([4, 1])
        with col_title:
            st.markdown(f"**{title}**")
        with col_toggle:
            show_chart = st.toggle("Показать график", key=f"tg_{key_id}")
        if not show_chart:
            continue
        pnl_net = complex_net_pnl(cs, prices, 0.00055)
        pnl_gross = complex_gross_pnl(cs, prices)
        st.markdown(
            f"Net PnL: **{pnl_net:+.2f} USD** · "
            f"Gross PnL: **{pnl_gross:+.2f} USD** · "
            f"Target: **{float(cs.get('target', 0)):.2f} USD**"
        )
        refs = cs.get("norm_refs", {}) or cs.get("entry_month_refs", {}) or {}
        layers = cs.get("layers", [])
        events = get_trade_events(leg1, leg2, layers, float(cs.get("complex", {}).get("d_max", 0.0)))
        chart_refs = cs.get("norm_refs", {}) or cs.get("entry_month_refs", {}) or {}
        for ev in events:
            ev["norm_refs"] = chart_refs
            ev["entry_spread"] = float(cs.get("entry_spread", 0.0))
        open_ts = events[0]["ts_ms"] if events else int(time.time() * 1000)
        year, month, day, hour = load_multi_chart_data_cached(
            leg1, leg2, tuple(sorted(refs.items())), float(cs.get("entry_spread", 0.0)), open_ts)
        tab_y, tab_m, tab_d, tab_h = st.tabs(["📈 Год (1ч)", "📅 Месяц (1ч)", "☀️ День (5м)", "⏱️ Час (1м)"])
        with tab_y:
            plot_chart(year, "Год · часовые свечи", f"chart_{key_id}_year", events)
        with tab_m:
            plot_chart(month, "Месяц · часовые свечи", f"chart_{key_id}_month", events)
        with tab_d:
            plot_chart(day, "День · 5-минутные бары", f"chart_{key_id}_day", events)
        with tab_h:
            plot_chart(hour, "Час · минутные свечи", f"chart_{key_id}_hour", events)
        if not hour.empty:
            st.metric("Текущий спред", f"{hour['spread'].iloc[-1]:+.5f}",
                      delta=f"{hour['spread'].iloc[-1] - float(cs.get('entry_spread', 0)):+.5f} от входа")
        st.divider()


# ============================================================
# ЗАПУСК
# ============================================================
st.title("📊 Spread Bot — Dashboard")
st.caption(f"Обновление: KPI каждые {REFRESH_STATS_SEC}с · Графики каждые {REFRESH_CHARTS_SEC}с")

render_stats()
render_charts()
