#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
plot_pnl.py — строит эквити-кривую и точки закрытых сделок из trades.db.

Запуск:
    python plot_pnl.py

Вывод: plots/pnl_chart.html
"""
import sqlite3
import os
from datetime import datetime

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

DB = "trades.db"
OUT = "plots/pnl_chart.html"
os.makedirs("plots", exist_ok=True)


def load():
    with sqlite3.connect(DB) as conn:
        eq = pd.read_sql_query(
            "SELECT ts_ms, total_equity, available, unrealized_pnl "
            "FROM equity ORDER BY ts_ms", conn,
        )
        tr = pd.read_sql_query(
            "SELECT ts_ms, event, complex_id, leg1, leg2, pnl_usd, details "
            "FROM trades ORDER BY ts_ms", conn,
        )
    if not eq.empty:
        eq["dt"] = pd.to_datetime(eq["ts_ms"], unit="ms", utc=True)
    if not tr.empty:
        tr["dt"] = pd.to_datetime(tr["ts_ms"], unit="ms", utc=True)
    return eq, tr


def main():
    eq, tr = load()
    if eq.empty and tr.empty:
        print("trades.db пуст.")
        return

    closes = tr[tr["event"] == "close"].copy() if not tr.empty else pd.DataFrame()
    opens = tr[tr["event"] == "open"].copy() if not tr.empty else pd.DataFrame()

    fig = make_subplots(
        rows=2, cols=1, row_heights=[0.65, 0.35],
        subplot_titles=["Equity (по снимкам раз в 60с)", "Realized PnL (cumulative)"],
        vertical_spacing=0.10, shared_xaxes=True,
    )

    # ─── Эквити ───
    if not eq.empty:
        fig.add_trace(
            go.Scatter(x=eq["dt"], y=eq["total_equity"], name="Equity",
                       line=dict(color="#1f77b4", width=2)),
            row=1, col=1,
        )
        fig.add_trace(
            go.Scatter(x=eq["dt"], y=eq["total_equity"] - eq["unrealized_pnl"],
                       name="Equity − unrealized (≈реализованный)",
                       line=dict(color="#888", width=1, dash="dot")),
            row=1, col=1,
        )

    # ─── Точки закрытия сделок на эквити ───
    if not closes.empty:
        wins = closes[closes["pnl_usd"] > 0]
        losses = closes[closes["pnl_usd"] <= 0]

        # Для Y на графике эквити берём ближайший снимок — простая аппроксимация:
        # если есть snapshots, используем join по времени
        if not eq.empty:
            closes_sorted = closes.sort_values("dt")
            eq_sorted = eq.sort_values("dt")[["dt", "total_equity"]]
            merged = pd.merge_asof(
                closes_sorted, eq_sorted, on="dt", direction="nearest",
            )
            wins_y = merged[merged["pnl_usd"] > 0]
            losses_y = merged[merged["pnl_usd"] <= 0]
        else:
            wins_y, losses_y = wins, losses

        for df_, color, label in [
            (wins_y, "#2ca02c", "Win"),
            (losses_y, "#d62728", "Loss"),
        ]:
            if df_.empty:
                continue
            y_col = "total_equity" if "total_equity" in df_.columns else None
            y_vals = df_[y_col] if y_col else [0] * len(df_)
            sizes = (df_["pnl_usd"].abs().clip(upper=20) * 0.8 + 6).tolist()
            fig.add_trace(
                go.Scatter(
                    x=df_["dt"], y=y_vals,
                    mode="markers",
                    marker=dict(size=sizes, color=color, opacity=0.7,
                                line=dict(color="white", width=1)),
                    name=f"{label} ({len(df_)})",
                    customdata=df_[["leg1", "leg2", "pnl_usd"]].values,
                    hovertemplate=(
                        "<b>%{customdata[0]} ⟷ %{customdata[1]}</b><br>"
                        "PnL: $%{customdata[2]:.2f}<br>"
                        "%{x|%Y-%m-%d %H:%M}<extra></extra>"
                    ),
                ),
                row=1, col=1,
            )

    # ─── Кумулятивный realized PnL ───
    if not closes.empty:
        closes = closes.sort_values("dt").copy()
        closes["cum_pnl"] = closes["pnl_usd"].cumsum()
        fig.add_trace(
            go.Scatter(x=closes["dt"], y=closes["cum_pnl"], name="Cumulative PnL",
                       line=dict(color="#2ca02c", width=1.6),
                       fill="tozeroy", fillcolor="rgba(44,160,44,0.08)"),
            row=2, col=1,
        )
        fig.add_hline(y=0, line_dash="dot", line_color="#888", row=2, col=1)

    fig.update_layout(
        height=800,
        margin=dict(l=10, r=10, t=60, b=10),
        hovermode="x unified",
        plot_bgcolor="#fafafa",
        legend=dict(orientation="h", y=1.06, x=0),
    )
    fig.update_yaxes(title_text="USD", row=1, col=1, gridcolor="#eee")
    fig.update_yaxes(title_text="USD", row=2, col=1, gridcolor="#eee")

    fig.write_html(OUT)
    print(f"Готово: {OUT}")

    if not closes.empty:
        print("\nСводка по сделкам:")
        print(f"  Всего закрытий: {len(closes)}")
        print(f"  Wins: {(closes['pnl_usd'] > 0).sum()}  "
              f"Losses: {(closes['pnl_usd'] <= 0).sum()}")
        print(f"  Суммарный realized PnL: ${closes['pnl_usd'].sum():.2f}")
        print(f"  Средний PnL/сделку:    ${closes['pnl_usd'].mean():.2f}")


if __name__ == "__main__":
    main()