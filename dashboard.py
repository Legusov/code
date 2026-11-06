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
DB_PATH = "bybit_1m.db"
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
    if not os.path.exists(TRADES_DB) or not layers:
        return []

    complex_id = "|".join(list(leg1) + list(leg2))

    try:
        with sqlite3.connect(TRADES_DB, timeout=10) as conn:
            df = pd.read_sql_query(
                "SELECT ts_ms, event FROM trades WHERE complex_id=? ORDER BY ts_ms ASC",
                conn, params=(complex_id,)
            )
    except Exception as e:
        print(f"Ошибка чтения trades.db: {e}")
        return []

    if df.empty: return []

    current_cycle = []
    for _, row in df.iterrows():
        if row['event'] == 'open':
            current_cycle = [row]
        elif row['event'] == 'add_layer' and current_cycle:
            current_cycle.append(row)
        elif row['event'] == 'close' and current_cycle:
            current_cycle = []

    if not current_cycle: return []

    events = []
    layer_idx = 0

    for row in current_cycle:
        if layer_idx >= len(layers): break
        
        layer = layers[layer_idx]
        qty = layer.get("qty", {})
        prices = layer.get("add_prices", {})
        
        # Используем сохраненный spread для точки на графике
        sp_val = layer.get("spread", 0.0)
        ratio_val = abs(sp_val) / d_max if d_max > 0 else 0.0
        
        parts = []
        for sym, q in qty.items():
            p = prices.get(sym, 0.0)
            if p == 0: 
                p = FETCHER.prices.get(sym, 1.0)
            val = q * p
            parts.append(f"{sym.replace('USDT','')}:${val:.0f}")
        
        full_label = f"<b>ratio={ratio_val:.2f}</b> | <b>spread={sp_val:+.4f}</b> | " + " | ".join(parts)
        short_label = str(layer_idx + 1)
        
        events.append({
            "ts_ms": int(row["ts_ms"]),
            "type": row["event"],
            "layer_idx": layer_idx,
            "short_label": short_label,
            "full_label": full_label,
            "spread": sp_val
        })
        
        layer_idx += 1

    return events


@st.cache_data(ttl=REFRESH_CHARTS_SEC)
def load_multi_chart_data_cached(leg1: tuple, leg2: tuple, entry_month_refs_tuple: tuple, entry_spread: float, open_ts: int) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    entry_month_refs = dict(entry_month_refs_tuple)
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    month_ago = now_ms - 30 * 86400_000
    all_syms = list(leg1) + list(leg2)

    current_refs = get_month_refs_cached(tuple(all_syms))

    def _query(sym, lo_ms, hi_ms):
        try:
            with sqlite3.connect(DB_PATH) as conn:
                return pd.read_sql_query(
                    "SELECT timestamp, close FROM ohlcv WHERE symbol=? AND timeframe='1' "
                    "AND timestamp BETWEEN ? AND ? ORDER BY timestamp",
                    conn, params=(sym, lo_ms, hi_ms),
                )
        except Exception:
            return pd.DataFrame()

    frames = {}
    for sym in all_syms:
        df = _query(sym, month_ago, now_ms)
        if df.empty:
            continue
        frames[sym] = df.set_index("timestamp")["close"]

    if len(frames) < len(all_syms):
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    pivot = pd.DataFrame(frames).sort_index().ffill()

    if pivot.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    def _process(slice_df, resample_rule):
        if slice_df.empty: return pd.DataFrame()
        
        valid_df = slice_df.dropna()
        if valid_df.empty: return pd.DataFrame()

        # Нормализация (сначала entry_month_refs, если нет - current_refs)
        for col in valid_df.columns:
            ref = entry_month_refs.get(col)
            if not ref or ref <= 0:
                ref = current_refs.get(col)
            if not ref or ref <= 0:
                ref = valid_df[col].iloc[0]
            valid_df[col] = valid_df[col] / ref

        leg1_cols = [c for c in leg1 if c in valid_df.columns]
        leg2_cols = [c for c in leg2 if c in valid_df.columns]

        leg1_raw = valid_df[leg1_cols].mean(axis=1)
        leg2_raw = valid_df[leg2_cols].mean(axis=1)
        spread_raw = leg1_raw - leg2_raw

        out = pd.DataFrame(index=valid_df.index)
        out["leg1"] = leg1_raw
        out["leg2"] = leg2_raw
        out["spread"] = spread_raw

        # Сначала делаем ресемплинг (усреднение)
        if resample_rule:
            out.index = pd.to_datetime(out.index, unit="ms", utc=True)
            out = out.resample(resample_rule).mean().dropna()
        else:
            out.index = pd.to_datetime(out.index, unit="ms", utc=True)

        # ★ МАТЕМАТИЧЕСКИЙ СДВИГ (OFFSET) ПРИМЕНЯЕМ ПОСЛЕ РЕСЕМПЛИНГА
        if not out.empty:
            open_dt = pd.to_datetime(open_ts, unit="ms", utc=True)
            idx = out.index.get_indexer([open_dt], method='nearest')[0]
            
            if idx != -1:
                s_plot = out["spread"].iloc[idx]
                offset = entry_spread - s_plot
                
                out["leg1"] = out["leg1"] + offset / 2.0
                out["leg2"] = out["leg2"] - offset / 2.0
                out["spread"] = out["spread"] + offset

        return out

    df_month = _process(pivot, '1h')
    day_ago = now_ms - 24 * 3600_000
    df_day = _process(pivot[pivot.index >= day_ago], '5min')
    hour_ago = now_ms - 3600_000
    df_hour = _process(pivot[pivot.index >= hour_ago], None)

    return df_month, df_day, df_hour


def plot_chart(df: pd.DataFrame, title: str, chart_key: str, events: List[Dict]):
    if df.empty: return
    
    ts = df.index
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(x=ts, y=df["leg1"], name="leg1", line=dict(color="#1f77b4", width=1.6)))
    fig.add_trace(go.Scatter(x=ts, y=df["leg2"], name="leg2", line=dict(color="#ff7f0e", width=1.6)))
    fig.add_trace(go.Scatter(x=ts, y=df["spread"], name="spread", line=dict(color="#2ca02c", width=1.6, dash="dot"), yaxis="y2"))

    df_min_ts = ts.min()
    df_max_ts = ts.max()
    
    visible_events = []
    if events:
        for ev in events:
            ev_ts = pd.to_datetime(ev["ts_ms"], unit="ms", utc=True)
            if df_min_ts <= ev_ts <= df_max_ts:
                visible_events.append(ev)
                is_open = ev["type"] == "open"
                color = "#00cc96" if is_open else "#ff7f0e"
                
                fig.add_vline(
                    x=ev_ts,
                    line_width=1.5,
                    line_dash="dash",
                    line_color=color,
                    layer="above",
                    annotation_text=f"<b>{ev['short_label']}</b>",
                    annotation_position="top",
                    annotation_font=dict(size=12, color="white"),
                    annotation_bgcolor=color,
                    annotation_borderpad=4
                )
                
                fig.add_trace(go.Scatter(
                    x=[ev_ts], y=[ev["spread"]],
                    mode="markers",
                    name=f"{ev['short_label']} ({'Вход' if is_open else 'Добор'})",
                    marker=dict(size=12, color=color, symbol="circle", line=dict(color="white", width=1.5)),
                    showlegend=False,
                    yaxis="y2",
                    hovertemplate=(
                        f"<b>{ev['short_label']} ({'Вход' if is_open else 'Добор'})</b><br>"
                        f"Spread: {ev['spread']:+.4f}<br>"
                        f"{ev['full_label']}<extra></extra>"
                    )
                ))

    date_range = f"{df_min_ts.strftime('%d.%m %H:%M')} - {df_max_ts.strftime('%d.%m %H:%M')}"
    
    fig.update_layout(
        title=f"{title} (Данные: {date_range})",
        height=350, 
        margin=dict(l=10, r=10, t=40, b=10),
        hovermode="x unified",
        yaxis=dict(title="leg (norm)", gridcolor="#eee"),
        yaxis2=dict(title="spread", overlaying="y", side="right", showgrid=False, zeroline=True, zerolinecolor="#aaa"),
        legend=dict(orientation="h", y=1.1, x=0),
        plot_bgcolor="#fafafa",
    )
    fig.add_hline(y=0, line_dash="dash", line_color="#888", yref="y2")
    
    st.plotly_chart(fig, width="stretch", key=chart_key)
    
    if visible_events:
        legend_html = "<div style='background-color: #f8f9fa; padding: 10px; border-radius: 5px; border: 1px solid #e9ecef; font-family: monospace; font-size: 13px;'>"
        for ev in visible_events:
            is_open = ev["type"] == "open"
            color = "#00cc96" if is_open else "#ff7f0e"
            type_str = "Вход" if is_open else "Добор"
            legend_html += f"<span style='color:{color}; font-weight:bold;'>■ {ev['short_label']} ({type_str})</span>: {ev['full_label']}<br>"
        legend_html += "</div>"
        st.markdown(legend_html, unsafe_allow_html=True)


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
    refs_month = get_month_refs_cached(tuple(syms))
    if len(refs_month) < 6:
        return 0.0

    vals = []
    for sym in syms:
        p = prices.get(sym, 0.0)
        ref = refs_month.get(sym, p)
        vals.append(p / ref if ref > 0 else 1.0)
    return sum(vals[:3]) / 3 - sum(vals[3:]) / 3


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
        prices = FETCHER.prices

    st.divider()
    st.subheader("Графики нормализованных цен")
    
    taker_rate = 0.00055

    for i, cs in enumerate(active, 1):
        leg1 = tuple(cs["complex"]["leg1"])
        leg2 = tuple(cs["complex"]["leg2"])
        title = f"#{i} · {' / '.join(leg1)} ⟷ {' / '.join(leg2)}"

        pnl_net = complex_net_pnl(cs, prices, taker_rate)
        pnl_gross = complex_gross_pnl(cs, prices)
        target = cs.get("target", 0)

        col_title, col_toggle = st.columns([4, 1])
        with col_title:
            st.markdown(f"**{title}**")
        with col_toggle:
            show_chart = st.toggle("Показать график", key=f"tg_{i}")

        if show_chart:
            st.markdown(
                f"<div style='font-size: 16px; margin-bottom: 10px;'>"
                f"Net PnL: <b>${pnl_net:.2f}</b> | "
                f"Gross PnL: <b>${pnl_gross:.2f}</b> | "
                f"Target: <b>${target:.2f}</b>"
                f"</div>",
                unsafe_allow_html=True
            )

            entry_month_refs = cs.get("entry_month_refs", {})
            entry_month_refs_tuple = tuple(entry_month_refs.items())
            entry_spread_val = float(cs.get("entry_spread", 0.0))
            
            d_max_val = float(cs.get("complex", {}).get("d_max", 0.0))
            events = get_trade_events(leg1, leg2, cs.get("layers", []), d_max_val)
            
            # ★ Передаем время входа для точного сдвига графика
            open_ts = events[0]["ts_ms"] if events else int(time.time() * 1000)

            df_month, df_day, df_hour = load_multi_chart_data_cached(
                leg1, leg2, entry_month_refs_tuple, entry_spread_val, open_ts
            )

            tab_m, tab_d, tab_h = st.tabs(["📅 Месяц (1ч)", "☀️ День (5м)", "⏱️ Час (1м)"])

            with tab_m:
                if df_month.empty:
                    st.info("Нет данных за месяц")
                else:
                    plot_chart(df_month, f"Месяц (реальный спред, 1ч)", f"chart_{i}_month", events)

            with tab_d:
                if df_day.empty:
                    st.info("Нет данных за день")
                else:
                    plot_chart(df_day, f"День (реальный спред, 5м)", f"chart_{i}_day", events)

            with tab_h:
                if df_hour.empty:
                    st.info("Нет данных за час")
                else:
                    plot_chart(df_hour, f"Часы (реальный спред, 1м)", f"chart_{i}_hour", events)

            if not df_hour.empty:
                col1, col2, col3, col4 = st.columns(4)
                cur_S = df_hour["spread"].iloc[-1]
                entry_S = cs.get("entry_spread", 0)
                col1.metric("Текущий спред", f"{cur_S:+.4f}", delta=f"{(cur_S - entry_S):+.4f} от входа")
                col2.metric("Слоёв в позиции", len(cs.get("layers", [])))
                col3.metric("Реализовано", f"${cs.get('realized_pnl', 0):.2f}")
                col4.metric("Хедж", "🛡 открыт" if cs.get("hedge_open") else "—")
            st.divider()


# ============================================================
# ЗАПУСК
# ============================================================
st.title("📊 Spread Bot — Dashboard")
st.caption(f"Обновление: KPI каждые {REFRESH_STATS_SEC}с · Графики каждые {REFRESH_CHARTS_SEC}с")

render_stats()
render_charts()