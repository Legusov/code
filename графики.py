#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
plot_complexes.py — графики нормализованных цен и спреда для каждого
активного комплекса из state.json + CSV со всеми рядами.

Запуск:
    python plot_complexes.py

Результат:
    plots/complex_XX_<legs>.html   — интерактивный график
    csv/complex_XX_<legs>.csv      — сырые нормализованные данные
"""

import json
import os
import sqlite3
from datetime import datetime, timezone

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# ============================================================
# КОНФИГ
# ============================================================
STATE_PATH      = "state.json"
DB_PATH         = "bybit_1m.db"
PLOTS_DIR       = "plots"
CSV_DIR         = "csv"

RESAMPLE_RULE   = "15min"
HISTORY_DAYS    = 30
MIN_POINTS      = 50

CSV_ENABLED     = True
CSV_FLOAT_FMT   = "%.6f"

_SYMBOL_CACHE = {}


# ============================================================
# ЧТЕНИЕ ДАННЫХ
# ============================================================
def load_state():
    with open(STATE_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def get_db_start_ms():
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT MIN(timestamp) FROM ohlcv WHERE timeframe='1'"
        ).fetchone()
    return row[0] if row and row[0] else None


def load_symbol_series(symbol, start_ms, end_ms):
    key = (symbol, start_ms, end_ms)
    if key in _SYMBOL_CACHE:
        return _SYMBOL_CACHE[key]

    with sqlite3.connect(DB_PATH) as conn:
        df = pd.read_sql_query(
            "SELECT timestamp, close FROM ohlcv "
            "WHERE symbol=? AND timeframe='1' AND timestamp BETWEEN ? AND ? "
            "ORDER BY timestamp",
            conn, params=(symbol, start_ms, end_ms),
        )

    if df.empty:
        _SYMBOL_CACHE[key] = None
        return None

    s = pd.Series(df["close"].values, index=df["timestamp"].values, name=symbol)
    _SYMBOL_CACHE[key] = s
    return s


# ============================================================
# СБОРКА И НОРМАЛИЗАЦИЯ
# ============================================================
def build_complex_frame(cs, start_ms, end_ms):
    """
    Возвращает DataFrame с колонками:
      <6 individual symbols>, leg1, leg2, spread.
    Каждая монета нормализована к своей первой цене в окне (старт = 1.0).
    Индекс — UTC-время (шаг RESAMPLE_RULE).
    """
    leg1_syms = cs["complex"]["leg1"]
    leg2_syms = cs["complex"]["leg2"]
    all_syms = list(leg1_syms) + list(leg2_syms)

    frames = {}
    for sym in all_syms:
        s = load_symbol_series(sym, start_ms, end_ms)
        if s is None or s.empty:
            print(f"    [{sym}] нет данных — комплекс пропускаем")
            return None
        first = float(s.iloc[0])
        if first <= 0:
            print(f"    [{sym}] нулевая первая цена — комплекс пропускаем")
            return None
        norm = s / first
        norm.index = pd.to_datetime(norm.index, unit="ms", utc=True)
        norm = norm.resample(RESAMPLE_RULE).mean().dropna()
        if norm.empty:
            return None
        frames[sym] = norm

    pivot = pd.DataFrame(frames).sort_index().ffill().dropna()
    if len(pivot) < MIN_POINTS:
        return None

    out = pivot.copy()
    out["leg1"] = pivot[leg1_syms].mean(axis=1)
    out["leg2"] = pivot[leg2_syms].mean(axis=1)
    out["spread"] = out["leg1"] - out["leg2"]
    out.index.name = "datetime_utc"
    return out


# ============================================================
# ОТРИСОВКА
# ============================================================
def plot_complex(idx, cs, df, out_path):
    leg1_syms = cs["complex"]["leg1"]
    leg2_syms = cs["complex"]["leg2"]
    leg1_str = "/".join(leg1_syms)
    leg2_str = "/".join(leg2_syms)
    d_max = float(cs["complex"]["d_max"])
    cycles = int(cs["complex"].get("cycles", 0))
    score = float(cs["complex"].get("score", 0))
    entry_spread = float(cs.get("entry_spread", 0))
    layers_n = len(cs.get("layers", []))
    hedge = bool(cs.get("hedge_open", False))

    cur_leg1 = float(df["leg1"].iloc[-1])
    cur_leg2 = float(df["leg2"].iloc[-1])
    cur_spread = float(df["spread"].iloc[-1])
    s_max = float(df["spread"].max())
    s_min = float(df["spread"].min())

    subtitle = (
        f"Entry S={entry_spread:+.4f} · Dmax={d_max:.4f} · "
        f"cycles={cycles} · score={score:.2f} · слоёв={layers_n}"
        + (" · 🛡 хедж" if hedge else "")
        + f"<br>Текущий: leg1={cur_leg1:.4f}, leg2={cur_leg2:.4f}, "
          f"spread={cur_spread:+.4f}  |  диапазон спреда: [{s_min:+.4f} … {s_max:+.4f}]"
    )

    fig = make_subplots(
        rows=2, cols=1, row_heights=[0.55, 0.45],
        subplot_titles=[
            "Legs (норм. к первой точке в окне)",
            "Spread = mean(leg1) − mean(leg2)",
        ],
        vertical_spacing=0.10, shared_xaxes=True,
    )

    fig.add_trace(
        go.Scatter(x=df.index, y=df["leg1"], name=f"leg1: {leg1_str}",
                   line=dict(color="#1f77b4", width=1.4)),
        row=1, col=1,
    )
    fig.add_trace(
        go.Scatter(x=df.index, y=df["leg2"], name=f"leg2: {leg2_str}",
                   line=dict(color="#ff7f0e", width=1.4)),
        row=1, col=1,
    )

    # Добавим на верхний график индивидуальные монеты тонкими пунктирами
    # — сразу видно, какая нога улетела.
    colors = ["#7fb2d8", "#a7c957", "#e07a5f", "#f2cc8f", "#81b29a", "#8d6b94"]
    for k, sym in enumerate(list(leg1_syms) + list(leg2_syms)):
        if sym in df.columns:
            fig.add_trace(
                go.Scatter(x=df.index, y=df[sym], name=sym,
                           line=dict(color=colors[k % len(colors)],
                                     width=0.8, dash="dot"),
                           visible="legendonly"),
                row=1, col=1,
            )

    fig.add_trace(
        go.Scatter(x=df.index, y=df["spread"], name="spread",
                   line=dict(color="#2ca02c", width=1.4),
                   fill="tozeroy", fillcolor="rgba(44,160,44,0.08)"),
        row=2, col=1,
    )

    fig.add_hline(y=0, line_dash="dot", line_color="#888", row=2, col=1)
    fig.add_hline(y=d_max, line_dash="dot", line_color="#c9c9c9",
                  annotation_text="+Dmax", annotation_position="top right", row=2, col=1)
    fig.add_hline(y=-d_max, line_dash="dot", line_color="#c9c9c9",
                  annotation_text="−Dmax", annotation_position="bottom right", row=2, col=1)

    fig.update_layout(
        title=dict(
            text=f"#{idx}  {leg1_str}  ⟷  {leg2_str}<br><sub>{subtitle}</sub>",
            x=0.01, xanchor="left",
        ),
        height=760,
        margin=dict(l=10, r=10, t=110, b=10),
        hovermode="x unified",
        plot_bgcolor="#fafafa",
        legend=dict(orientation="h", y=1.02, x=0),
    )
    fig.update_yaxes(title_text="норм. цена", row=1, col=1, gridcolor="#eee")
    fig.update_yaxes(title_text="spread",     row=2, col=1, gridcolor="#eee")

    fig.write_html(out_path)


# ============================================================
# CSV
# ============================================================
def export_complex_csv(idx, cs, df, out_path):
    """
    Сохраняет DataFrame в CSV.
    Колонки: datetime_utc + 6 монет + leg1 + leg2 + spread.
    """
    leg1_syms = cs["complex"]["leg1"]
    leg2_syms = cs["complex"]["leg2"]
    all_syms = list(leg1_syms) + list(leg2_syms)

    # Порядок: время → leg1 монеты → leg2 монеты → leg1 → leg2 → spread
    cols = ["datetime_utc"] + all_syms + ["leg1", "leg2", "spread"]
    out = df.reset_index()[cols]
    out.to_csv(out_path, index=False, float_format=CSV_FLOAT_FMT)


# ============================================================
# MAIN
# ============================================================
def main():
    os.makedirs(PLOTS_DIR, exist_ok=True)
    if CSV_ENABLED:
        os.makedirs(CSV_DIR, exist_ok=True)

    if not os.path.exists(STATE_PATH):
        print(f"Нет файла {STATE_PATH}")
        return
    if not os.path.exists(DB_PATH):
        print(f"Нет файла {DB_PATH}")
        return

    state = load_state()
    active = state.get("active", [])
    if not active:
        print("В state.json нет активных комплексов.")
        return

    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    if HISTORY_DAYS is None:
        start_ms = get_db_start_ms() or (now_ms - 30 * 86400_000)
    else:
        start_ms = now_ms - HISTORY_DAYS * 86400_000

    dt_start = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc)
    dt_end   = datetime.fromtimestamp(now_ms   / 1000, tz=timezone.utc)

    print(f"Активных комплексов: {len(active)}")
    print(f"Окно: {dt_start:%Y-%m-%d %H:%M} → {dt_end:%Y-%m-%d %H:%M} UTC")
    print(f"Ресемплинг: {RESAMPLE_RULE}")
    print(f"CSV: {'включено' if CSV_ENABLED else 'выключено'}")
    print("-" * 72)

    generated = 0
    for i, cs in enumerate(active, 1):
        leg1_str = "/".join(cs["complex"]["leg1"])
        leg2_str = "/".join(cs["complex"]["leg2"])
        print(f"\n#{i}: {leg1_str} / {leg2_str}")

        df = build_complex_frame(cs, start_ms, now_ms)
        if df is None or df.empty:
            print("    → пропущено (нет данных)")
            continue

        safe = (leg1_str + "__" + leg2_str).replace("/", "_")
        if len(safe) > 80:
            safe = safe[:80]

        html_path = os.path.join(PLOTS_DIR, f"complex_{i:02d}_{safe}.html")
        plot_complex(i, cs, df, html_path)
        print(f"    HTML → {html_path}")

        if CSV_ENABLED:
            csv_path = os.path.join(CSV_DIR, f"complex_{i:02d}_{safe}.csv")
            export_complex_csv(i, cs, df, csv_path)
            print(f"    CSV  → {csv_path} ({len(df)} строк)")

        generated += 1

    print("\n" + "-" * 72)
    print(f"Готово. Сгенерировано {generated} из {len(active)}.")


if __name__ == "__main__":
    main()