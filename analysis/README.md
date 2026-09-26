# Reproducible income projection

This offline analysis reproduces the scenario estimate for reaching a modeled net income target (default **$500/day**) from the trader's SQLite database. It does not connect to Bybit, does not modify the database, and does not place orders.

## Inputs and privacy

Use the user's **local** `bybit_signal_trader.sqlite3`. Do not commit that database, API credentials, logs, or a generated report containing trading values to the public repository. The program opens the database in SQLite read-only mode. It prints a SHA-256 digest of the input file so the exact source snapshot can be identified without publishing it.

The seven-day filter-test PnL (`$80.544` on a `$200` model balance) is an explicit command-line input with defaults matching the prior analysis. To rerun the prior comparison, pin the as-of date and provide the local database:

```bash
python analysis/income_projection.py \
  --trader-db /path/to/bybit_signal_trader.sqlite3 \
  --as-of-date 2026-09-26 \
  --output /path/to/private-output/income_projection.json
```

For a later snapshot, update `--as-of-date`; the program uses the complete Moscow calendar days immediately before that date. It fails if any selected date has no closed-position records rather than silently choosing older or partial days.

## Scenarios calculated

1. **Recent closed PnL:** average closed PnL over the latest two complete Moscow calendar days, divided by the latest saved `cycle_start_balance`.
2. **ROI-cycle timing:** clusters rows closed as `ROI` using a 10-minute gap heuristic, measures intervals between cluster endpoints, and models the configured ROI target (default 2%) compounding at the measured average cycle duration.
3. **Seven-day filter replay assumption:** uses `$80.544 / $200` total model return over 7 days, converted to an equivalent geometric daily return. This is an in-sample strategy result, not an independent forecast.

Each scenario reports the balance where modeled daily PnL reaches the target, the base `NOTIONAL_USDT` at a configurable balance share (default 5%), and the compound-growth time from the balance currently saved in the DB. The current loss-bank add-on is reported separately but is not extrapolated.

## Re-run tests

```bash
python -m unittest -v analysis/test_income_projection.py
python -m py_compile analysis/income_projection.py analysis/test_income_projection.py
```

## Limitations

These projections assume observed returns persist, compound, and scale linearly with balance and 5% base notional. They do not model market regime changes, execution slippage, funding, liquidity, margin constraints, varying trade frequency, or taxes. ROI batches are inferred from position close timestamps because historical cycle-start/end records were not present in the original database. Use enough out-of-sample live data before treating any estimate as a planning baseline.
