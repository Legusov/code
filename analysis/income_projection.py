#!/usr/bin/env python3
"""Reproduce trading-income projections from local SQLite snapshots.

This tool is offline: it does not connect to Bybit and does not modify inputs.
It deliberately emits results to stdout/--output and should not commit personal
SQLite databases or generated reports to a public repository.
"""
from __future__ import annotations

import argparse
import json
import math
import sqlite3
from collections import defaultdict
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from statistics import mean, median
from zoneinfo import ZoneInfo

MSK = ZoneInfo("Europe/Moscow")
DEFAULT_ROI_TARGET = 0.02
DEFAULT_POSITION_SHARE = 0.05
DEFAULT_DAILY_TARGET_USDT = 500.0
DEFAULT_FILTER_BACKTEST_PNL_USDT = 80.544
DEFAULT_FILTER_BACKTEST_DAYS = 7
DEFAULT_FILTER_BACKTEST_START_BALANCE = 200.0
DEFAULT_ROI_CLUSTER_GAP_MINUTES = 10


def read_sqlite(path: Path, query: str, params=()):
    if not path.is_file():
        raise FileNotFoundError(f"SQLite database not found: {path}")
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        return [dict(row) for row in conn.execute(query, params)]
    finally:
        conn.close()


def load_trader_snapshot(path: Path, cluster_gap_minutes: int) -> dict:
    state_rows = read_sqlite(
        path,
        "SELECT key,value,updated_at FROM trader_state WHERE key IN "
        "('cycle_start_balance','roi_close_pending')",
    )
    state = {row["key"]: row for row in state_rows}
    if "cycle_start_balance" not in state:
        raise ValueError("trader_state.cycle_start_balance was not found")
    current_balance = float(state["cycle_start_balance"]["value"])
    if current_balance <= 0:
        raise ValueError(f"Invalid cycle start balance: {current_balance}")

    position_rows = read_sqlite(
        path,
        "SELECT status,close_status,close_time_ms,pnl_usdt,notional_usdt "
        "FROM positions WHERE status='CLOSED' AND close_time_ms IS NOT NULL "
        "AND pnl_usdt IS NOT NULL",
    )
    daily_pnl: dict[date, float] = defaultdict(float)
    for row in position_rows:
        local_day = datetime.fromtimestamp(row["close_time_ms"] / 1000, timezone.utc).astimezone(MSK).date()
        daily_pnl[local_day] += float(row["pnl_usdt"])

    bank_rows = read_sqlite(path, "SELECT bank_all FROM bank WHERE id=1")
    bank = float(bank_rows[0]["bank_all"]) if bank_rows else 0.0

    roi_rows = read_sqlite(
        path,
        "SELECT close_time_ms FROM positions WHERE status='CLOSED' "
        "AND close_status='ROI' AND close_time_ms IS NOT NULL ORDER BY close_time_ms",
    )
    roi_close_batches = cluster_roi_closes(
        [int(row["close_time_ms"]) for row in roi_rows], cluster_gap_minutes
    )

    return {
        "current_balance_usdt": current_balance,
        "cycle_start_balance_updated_at_utc": state["cycle_start_balance"]["updated_at"],
        "roi_close_pending": state.get("roi_close_pending", {}).get("value", "unknown"),
        "bank_all_usdt": bank,
        "daily_closed_pnl_msk": {day.isoformat(): pnl for day, pnl in sorted(daily_pnl.items())},
        "roi_close_batches": roi_close_batches,
        "closed_position_rows": len(position_rows),
        "roi_closed_position_rows": len(roi_rows),
    }


def cluster_roi_closes(close_times_ms: list[int], gap_minutes: int) -> list[dict]:
    """Group position-close rows into ROI batches using a time-gap heuristic."""
    if gap_minutes < 0:
        raise ValueError("ROI cluster gap must be non-negative")
    threshold_ms = gap_minutes * 60_000
    groups: list[list[int]] = []
    for timestamp in sorted(close_times_ms):
        if not groups or timestamp - groups[-1][-1] > threshold_ms:
            groups.append([timestamp])
        else:
            groups[-1].append(timestamp)
    return [
        {"start_time_ms": group[0], "end_time_ms": group[-1], "positions": len(group)}
        for group in groups
    ]


def select_full_daily_returns(daily_pnl: dict[str, float], as_of_date: date, days: int) -> dict:
    if days <= 0:
        raise ValueError("Number of observed days must be positive")
    eligible = {date.fromisoformat(day): pnl for day, pnl in daily_pnl.items()}
    chosen = [as_of_date - timedelta(days=offset) for offset in range(days, 0, -1)]
    missing = [day.isoformat() for day in chosen if day not in eligible]
    if missing:
        raise ValueError(
            "The trader database has no closed-position rows for these full calendar days: "
            + ", ".join(missing)
        )
    return {day.isoformat(): eligible[day] for day in chosen}


def projection_from_daily_return(
    current_balance: float,
    daily_return: float,
    daily_target: float,
    position_share: float,
) -> dict:
    if current_balance <= 0 or daily_target <= 0 or position_share <= 0:
        raise ValueError("Balance, daily target, and position share must be positive")
    if daily_return <= 0:
        return {
            "daily_return": daily_return,
            "target_balance_usdt": None,
            "target_base_notional_usdt": None,
            "days_from_current_balance": None,
            "warning": "Non-positive modeled daily return; target projection is undefined.",
        }
    target_balance = daily_target / daily_return
    if target_balance <= current_balance:
        days = 0.0
    else:
        days = math.log(target_balance / current_balance) / math.log1p(daily_return)
    return {
        "daily_return": daily_return,
        "target_balance_usdt": target_balance,
        "target_base_notional_usdt": target_balance * position_share,
        "days_from_current_balance": days,
        "warning": None,
    }


def compute_report(
    trader_db: Path,
    *,
    as_of_date: date,
    daily_target: float = DEFAULT_DAILY_TARGET_USDT,
    roi_target: float = DEFAULT_ROI_TARGET,
    position_share: float = DEFAULT_POSITION_SHARE,
    observed_days: int = 2,
    filter_backtest_pnl: float = DEFAULT_FILTER_BACKTEST_PNL_USDT,
    filter_backtest_days: int = DEFAULT_FILTER_BACKTEST_DAYS,
    filter_backtest_start_balance: float = DEFAULT_FILTER_BACKTEST_START_BALANCE,
    cluster_gap_minutes: int = DEFAULT_ROI_CLUSTER_GAP_MINUTES,
) -> dict:
    snapshot = load_trader_snapshot(trader_db, cluster_gap_minutes)
    if not 0 < roi_target:
        raise ValueError("ROI target must be positive")
    if filter_backtest_days <= 0 or filter_backtest_start_balance <= 0:
        raise ValueError("Backtest duration and starting balance must be positive")

    selected_days = select_full_daily_returns(
        snapshot["daily_closed_pnl_msk"], as_of_date, observed_days
    )
    avg_daily_pnl = mean(selected_days.values())
    current_balance = snapshot["current_balance_usdt"]
    observed_daily_return = avg_daily_pnl / current_balance

    source_batches = snapshot["roi_close_batches"]
    cycle_intervals_hours = [
        (source_batches[i]["end_time_ms"] - source_batches[i - 1]["end_time_ms"]) / 3_600_000
        for i in range(1, len(source_batches))
    ]
    avg_cycle_hours = mean(cycle_intervals_hours) if cycle_intervals_hours else None
    median_cycle_hours = median(cycle_intervals_hours) if cycle_intervals_hours else None

    scenarios = {
        "recent_closed_pnl": {
            "period_days_msk": selected_days,
            "average_closed_pnl_per_day_usdt": avg_daily_pnl,
            "normalization_balance_usdt": current_balance,
            "projection": projection_from_daily_return(
                current_balance, observed_daily_return, daily_target, position_share
            ),
            "caveat": "Uses the latest full calendar days with any closed trades; return is normalized by the latest saved cycle balance.",
        }
    }

    if avg_cycle_hours and avg_cycle_hours > 0:
        cycles_per_day = 24.0 / avg_cycle_hours
        equivalent_daily_return = (1.0 + roi_target) ** cycles_per_day - 1.0
        scenarios["roi_cycle_timing"] = {
            "roi_target_per_cycle": roi_target,
            "completed_roi_batches": len(source_batches),
            "interval_count": len(cycle_intervals_hours),
            "average_interval_hours": avg_cycle_hours,
            "median_interval_hours": median_cycle_hours,
            "min_interval_hours": min(cycle_intervals_hours),
            "max_interval_hours": max(cycle_intervals_hours),
            "equivalent_cycles_per_day": cycles_per_day,
            "equivalent_daily_return": equivalent_daily_return,
            "projection": projection_from_daily_return(
                current_balance, equivalent_daily_return, daily_target, position_share
            ),
            "caveat": "ROI close batches are grouped by a configurable time-gap heuristic; the short sample may not represent future cycle durations.",
        }
    else:
        scenarios["roi_cycle_timing"] = {
            "completed_roi_batches": len(source_batches),
            "interval_count": 0,
            "projection": None,
            "caveat": "Not enough ROI-close batches to estimate cycle duration.",
        }

    weekly_factor = 1.0 + filter_backtest_pnl / filter_backtest_start_balance
    if weekly_factor > 0:
        equivalent_daily_return = weekly_factor ** (1.0 / filter_backtest_days) - 1.0
        scenarios["filter_backtest"] = {
            "backtest_pnl_usdt": filter_backtest_pnl,
            "backtest_start_balance_usdt": filter_backtest_start_balance,
            "backtest_window_days": filter_backtest_days,
            "weekly_or_window_growth_factor": weekly_factor,
            "equivalent_daily_return": equivalent_daily_return,
            "projection": projection_from_daily_return(
                current_balance, equivalent_daily_return, daily_target, position_share
            ),
            "caveat": "The strategy optimizer evaluated this same historical window; in-sample results are not an independent forward forecast.",
        }
    else:
        scenarios["filter_backtest"] = {
            "backtest_pnl_usdt": filter_backtest_pnl,
            "projection": None,
            "caveat": "Backtest window factor is non-positive; projection is undefined.",
        }

    current_base_notional = current_balance * position_share
    bank_extra = snapshot["bank_all_usdt"] / 10.0 if snapshot["bank_all_usdt"] > 10 else 0.0
    return {
        "as_of_msk_date": as_of_date.isoformat(),
        "source_db": trader_db.name,
        "source_db_sha256": sha256_file(trader_db),
        "inputs": {
            "current_cycle_balance_usdt": current_balance,
            "current_base_notional_usdt": current_base_notional,
            "current_bank_all_usdt": snapshot["bank_all_usdt"],
            "current_bank_addition_estimate_usdt": bank_extra,
            "target_income_usdt_per_day": daily_target,
            "position_share_of_balance": position_share,
            "roi_target_per_cycle": roi_target,
            "full_days_used_for_closed_pnl": observed_days,
            "roi_cluster_gap_minutes": cluster_gap_minutes,
            "cycle_balance_updated_at_utc": snapshot["cycle_start_balance_updated_at_utc"],
            "roi_close_pending": snapshot["roi_close_pending"],
        },
        "counts": {
            "closed_position_rows": snapshot["closed_position_rows"],
            "roi_closed_position_rows": snapshot["roi_closed_position_rows"],
            "roi_close_batches": len(snapshot["roi_close_batches"]),
        },
        "scenarios": scenarios,
        "disclosure": (
            "Scenario math assumes modeled returns persist, compound, and scale proportionally with balance and 5% base notional. "
            "It excludes slippage, funding, liquidity constraints, unavailable margin, and future changes in win rate/trade frequency. "
            "The position-size addition from the loss bank is shown for current state only, not extrapolated."
        ),
    }


def sha256_file(path: Path) -> str:
    import hashlib
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trader-db", required=True, type=Path, help="Local Bybit trader SQLite database (read-only)")
    parser.add_argument("--as-of-date", type=date.fromisoformat,
                        default=datetime.now(MSK).date(), help="Only use full days before this Moscow date")
    parser.add_argument("--daily-target", type=float, default=DEFAULT_DAILY_TARGET_USDT)
    parser.add_argument("--roi-target", type=float, default=DEFAULT_ROI_TARGET)
    parser.add_argument("--position-share", type=float, default=DEFAULT_POSITION_SHARE)
    parser.add_argument("--observed-days", type=int, default=2)
    parser.add_argument("--filter-backtest-pnl", type=float, default=DEFAULT_FILTER_BACKTEST_PNL_USDT,
                        help="Aggregate model PnL from the separate optimizer backtest")
    parser.add_argument("--filter-backtest-days", type=int, default=DEFAULT_FILTER_BACKTEST_DAYS)
    parser.add_argument("--filter-backtest-start-balance", type=float, default=DEFAULT_FILTER_BACKTEST_START_BALANCE)
    parser.add_argument("--roi-cluster-gap-minutes", type=int, default=DEFAULT_ROI_CLUSTER_GAP_MINUTES)
    parser.add_argument("--output", type=Path, help="Optional JSON output path; otherwise print to stdout")
    args = parser.parse_args()
    report = compute_report(
        args.trader_db,
        as_of_date=args.as_of_date,
        daily_target=args.daily_target,
        roi_target=args.roi_target,
        position_share=args.position_share,
        observed_days=args.observed_days,
        filter_backtest_pnl=args.filter_backtest_pnl,
        filter_backtest_days=args.filter_backtest_days,
        filter_backtest_start_balance=args.filter_backtest_start_balance,
        cluster_gap_minutes=args.roi_cluster_gap_minutes,
    )
    output = json.dumps(report, indent=2, ensure_ascii=False)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(output + "\n", encoding="utf-8")
        print(f"Wrote report: {args.output}")
    else:
        print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
