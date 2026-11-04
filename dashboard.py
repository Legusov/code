#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Streamlit-дашборд для мониторинга активных комплексов.
"""

import json
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
# КОНФИГ
# ============================================================
STATE_PATH = "state.json"
DB_PATH = "bybit_1m.db"
CHART_PERIOD_MIN = 60
REFRESH_STATS_SEC = 10
REFRESH_CHARTS_SEC = 30

st.set_page_config(
    page_title="Spread Bot Dashboard",
    layout="wide",
    page_icon="📊",
)

# Отключаем спиннер в правом верхнем углу
st.markdown("<style>div[data-testid='stStatusWidget'] {display: none;}</style>", unsafe_allow_html=True)


# ============================================================
# ФОНОВЫЙ ЗАГРУЗЧИК ДАННЫХ
# ============================================================
class DashboardData:
    def __init__(self):
        self.balance = {"equity": 0.0, "available": 0.0}
        self.live = {}
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

                live = {}
                cursor = ""
                seen = set()
                page = 0
                while True:
                    params = {"category": "linear", "settleCoin": "USDT", "limit": 200}
                    if cursor: params["cursor"] = cursor
                    resp = session.get_positions(**params)
                    res = resp.get("result", {})
                    for p in res.get("list", []):
                        if float(p.get("size", 0)) != 0:
                            live[p["symbol"]] = p
                    next_c = (res.get("nextPageCursor") or "").strip()
                    if not next_c or next_c in seen or page >= 20: break
                    seen.add(next_c)
                    cursor = next_c
                    page += 1

                tickers = session.get_tickers(category="linear")["result"]["list"]
                prices = {t["symbol"]: float(t["lastPrice"]) for t in tickers}

                with data.lock:
                    data.balance = bal
                    data.live = live
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


@st.cache_resource
def get_session() -> HTTP:
    return HTTP(testnet=False, demo=True, api_key=API_KEY, api_secret=API_SECRET)


@st.cache_data(ttl=2)
def load_state() -> Dict:
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"active": [], "reserve": []}


@st.cache_data(ttl=REFRESH_CHARTS_SEC)
def load_chart_data_cached(leg1: tuple, leg2: tuple, norm_refs: dict, period_min: int) -> Tuple[pd.DataFrame, dict]:
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    expected_start_ms = now_ms - period_min * 60_000
    all_syms = list(leg1) + list(leg2)

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

    def _query_last_n(sym, n=500):
        try:
            with sqlite3.connect(DB_PATH) as conn:
                return pd.read_sql_query(
                    "SELECT timestamp, close FROM ohlcv WHERE symbol=? AND timeframe='1' "
                    "ORDER BY timestamp DESC LIMIT ?",
                    conn, params=(sym, n),
                ).sort_values("timestamp").reset_index(drop=True)
        except Exception:
            return pd.DataFrame()

    frames = {}
    syms_missing = []
    for sym in all_syms:
        df = _query(sym, expected_start_ms, now_ms)
        if df.empty:
            syms_missing.append(sym)
            continue
        ref = norm_refs.get(sym) or float(df["close"].iloc[0])
        if ref <= 0:
            ref = float(df["close"].iloc[0])
        df["norm"] = df["close"] / ref
        frames[sym] = df.set_index("timestamp")["norm"]

    effective_period = period_min

    if len(frames) < len(all_syms):
        lo2 = now_ms - period_min * 4 * 60_000
        effective_period = period_min * 4
        for sym in syms_missing[:]:
            df = _query(sym, lo2, now_ms)
            if df.empty: continue
            ref = norm_refs.get(sym) or float(df["close"].iloc[0])
            if ref <= 0: ref = float(df["close"].iloc[0])
            df["norm"] = df["close"] / ref
            frames[sym] = df.set_index("timestamp")["norm"]
            syms_missing.remove(sym)

    if syms_missing:
        for sym in syms_missing[:]:
            df = _query_last_n(sym, 500)
            if df.empty: continue
            ref = norm_refs.get(sym) or float(df["close"].iloc[0])
            if ref <= 0: ref = float(df["close"].iloc[0])
            df["norm"] = df["close"] / ref
            frames[sym] = df.set_index("timestamp")["norm"]
            syms_missing.remove(sym)

    meta = {
        "empty": not frames,
        "data_start_ts": None,
        "data_end_ts": None,
        "expected_start_ts": expected_start_ms,
        "now_ts": now_ms,
        "syms_missing": syms_missing,
        "effective_period_min": effective_period,
    }

    if not frames:
        return pd.DataFrame(), meta

    pivot = pd.DataFrame(frames).sort_index().ffill()
    if pivot.empty:
        return pd.DataFrame(), meta

    meta["data_start_ts"] = int(pivot.index[0])
    meta["data_end_ts"] = int(pivot.index[-1])

    leg1_cols = [c for c in leg1 if c in pivot.columns]
    leg2_cols = [c for c in leg2 if c in pivot.columns]
    if not leg1_cols or not leg2_cols:
        meta["empty"] = True
        return pd.DataFrame(), meta

    out = pd.DataFrame(index=pivot.index)
    out["leg1"] = pivot[leg1_cols].mean(axis=1)
    out["leg2"] = pivot[leg2_cols].mean(axis=1)
    out["spread"] = out["leg1"] - out["leg2"]
    out = out.dropna()

    if out.empty:
        meta["empty"] = True
    return out, meta


# ============================================================
# РАСЧЁТЫ
# ============================================================
def complex_gross_pnl(cs: Dict, live: Dict[str, Dict]) -> float:
    pnl = cs.get("realized_pnl", 0.0)
    for sym in list(cs["complex"]["leg1"]) + list(cs["complex"]["leg2"]):
        if sym in live:
            pnl += float(live[sym].get("unrealisedPnl", 0))
    return pnl


def complex_net_pnl(cs: Dict, live: Dict[str, Dict], prices: Dict[str, float]) -> float:
    gross = complex_gross_pnl(cs, live)
    fees_paid = float(cs.get("fees_paid", 0.0))
    exit_cost_rate = 0.00055 + 0.0010

    exit_cost_est = 0.0
    for sym in list(cs["complex"]["leg1"]) + list(cs["complex"]["leg2"]):
        if sym in live:
            p = live[sym]
            size = abs(float(p.get("size", 0)))
            price = float(p.get("markPrice", 0) or prices.get(sym, 0) or 0)
            if size > 0 and price > 0:
                exit_cost_est += size * price * exit_cost_rate
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
# HTML ТАБЛИЦЫ (Без отступов, чтобы Markdown не ломал)
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
# UI: СТАТИСТИКА (10 сек)
# ============================================================
@st.fragment(run_every=REFRESH_STATS_SEC)
def render_stats():
    state = load_state()
    active: List[Dict] = state.get("active", [])
    reserve: List[Dict] = state.get("reserve", [])

    with FETCHER.lock:
        balance = FETCHER.balance
        live = FETCHER.live
        prices = FETCHER.prices
        last_update = FETCHER.last_update

    update_time = datetime.fromtimestamp(last_update).strftime('%H:%M:%S') if last_update > 0 else "Ожидание..."
    st.caption(f"Открытых позиций: {len(live)} · Обновлено: {update_time}")

    total_gross = sum(complex_gross_pnl(cs, live) for cs in active)
    total_pnl = sum(complex_net_pnl(cs, live, prices) for cs in active)

    total_target = sum(cs.get("target", 0) for cs in active)

    total_realized = sum(cs.get("realized_pnl", 0) for cs in active)
    total_unrealized = total_gross - total_realized

    progress_avg = 0.0
    if active:
        progs = [min(complex_net_pnl(cs, live, prices) / cs.get("target", 1) * 100, 999) for cs in active]
        progress_avg = sum(progs) / len(progs) if progs else 0.0

    color_total = "#21c354" if total_pnl >= 0 else "#ff4b4b"
    color_gross = "#21c354" if total_gross >= 0 else "#ff4b4b"
    color_unrl = "#21c354" if total_unrealized >= 0 else "#ff4b4b"
    color_equity = "#21c354" if balance['equity'] >= 2000 else "#ff4b4b"

    kpi1, kpi2, kpi3, kpi4, kpi5 = st.columns(5)

    kpi1.markdown(
        f'<div style="font-size: 14px; color: #31333f; margin-bottom: 4px;">💰 Equity</div><div style="font-size: 36px; font-weight: 600; color: {color_equity}; line-height: 1.2;">${balance["equity"]:.2f}</div><div style="font-size: 14px; color: rgba(49, 51, 63, 0.6);">${balance["equity"] - 2000:+.2f} от старта</div>',
        unsafe_allow_html=True)

    kpi2.markdown(
        f'<div style="font-size: 14px; color: #31333f; margin-bottom: 4px;">📈 Net PnL</div><div style="font-size: 36px; font-weight: 600; color: {color_total}; line-height: 1.2;">${total_pnl:.2f}</div><div style="font-size: 14px; color: rgba(49, 51, 63, 0.6);">gross <span style="color: {color_gross}; font-weight: 600;">{total_gross:+.2f}</span> / unrl <span style="color: {color_unrl}; font-weight: 600;">{total_unrealized:+.2f}</span></div>',
        unsafe_allow_html=True)

    kpi3.metric("🎯 Target sum", f"${total_target + 96:.2f}", delta=f"{progress_avg:.1f}% avg progress")
    kpi4.metric("🟢 Активных", len(active), delta=f"{len(reserve)} в резерве")
    kpi5.metric("💵 Свободно", f"${balance['available']:.2f}")

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
            pnl_net = complex_net_pnl(cs, live, prices)
            pnl_gross = complex_gross_pnl(cs, live)
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
# UI: ГРАФИКИ (30 сек)
# ============================================================
@st.fragment(run_every=REFRESH_CHARTS_SEC)
def render_charts():
    state = load_state()
    active: List[Dict] = state.get("active", [])

    if not active:
        return

    with FETCHER.lock:
        live = FETCHER.live
        prices = FETCHER.prices

    st.divider()
    st.subheader(f"Кривые нормализованных цен · последние {CHART_PERIOD_MIN} мин")

    for i, cs in enumerate(active, 1):
        leg1 = tuple(cs["complex"]["leg1"])
        leg2 = tuple(cs["complex"]["leg2"])
        title = f"#{i} · {' / '.join(leg1)} ⟷ {' / '.join(leg2)}"

        pnl_net = complex_net_pnl(cs, live, prices)
        pnl_gross = complex_gross_pnl(cs, live)
        target = cs.get("target", 0)

        col_title, col_toggle = st.columns([4, 1])
        with col_title:
            st.markdown(f"**{title}**")
        with col_toggle:
            show_chart = st.toggle("Показать график", key=f"tg_{i}")

        if show_chart:
            st.markdown(f"Net PnL: **${pnl_net:.2f}** | Gross PnL: **${pnl_gross:.2f}** | Target: **${target:.2f}**")

            df_chart, meta = load_chart_data_cached(leg1, leg2, cs.get("norm_refs", {}), CHART_PERIOD_MIN)

            if meta["data_end_ts"]:
                lag_min = (meta["now_ts"] - meta["data_end_ts"]) / 60_000
                dt_end = datetime.fromtimestamp(meta["data_end_ts"] / 1000, tz=timezone.utc).strftime(
                    "%Y-%m-%d %H:%M UTC")
                dt_start = datetime.fromtimestamp(meta["data_start_ts"] / 1000, tz=timezone.utc).strftime(
                    "%Y-%m-%d %H:%M UTC")
                st.caption(
                    f"Данные: **{dt_start}** → **{dt_end}**  ·  точек: {len(df_chart)}  ·  свежесть: **{lag_min:.0f} мин**")

            if df_chart.empty:
                st.info(f"Нет общих точек в окне {meta['effective_period_min']} мин.")
            else:
                ts = pd.to_datetime(df_chart.index, unit="ms", utc=True)
                fig = go.Figure()
                fig.add_trace(go.Scatter(x=ts, y=df_chart["leg1"], name="leg1", line=dict(color="#1f77b4", width=1.6)))
                fig.add_trace(go.Scatter(x=ts, y=df_chart["leg2"], name="leg2", line=dict(color="#ff7f0e", width=1.6)))
                fig.add_trace(go.Scatter(x=ts, y=df_chart["spread"], name="spread",
                                         line=dict(color="#2ca02c", width=1.6, dash="dot"), yaxis="y2"))

                fig.update_layout(
                    height=320,
                    margin=dict(l=10, r=10, t=30, b=10),
                    hovermode="x unified",
                    yaxis=dict(title="leg (norm)", gridcolor="#eee"),
                    yaxis2=dict(title="spread", overlaying="y", side="right", showgrid=False, zeroline=True,
                                zerolinecolor="#aaa"),
                    legend=dict(orientation="h", y=1.1, x=0),
                    plot_bgcolor="#fafafa",
                )
                fig.add_hline(y=0, line_dash="dash", line_color="#888", yref="y2")

                st.plotly_chart(fig, width="stretch", key=f"chart_{i}")

                col1, col2, col3, col4 = st.columns(4)
                cur_S = df_chart["spread"].iloc[-1]
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