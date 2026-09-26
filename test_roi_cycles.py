"""Offline tests for ROI-cycle timestamps and resilient close verification."""
from __future__ import annotations

import importlib
import sqlite3
import sys
import tempfile
import types
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

# The trader imports platform/exchange modules at module import time. Stub them
# so these tests never access Windows audio or a Bybit account.
winsound = types.ModuleType("winsound")
winsound.MB_ICONHAND = 0
winsound.Beep = lambda *_args, **_kwargs: None
winsound.MessageBeep = lambda *_args, **_kwargs: None
sys.modules.setdefault("winsound", winsound)

pybit = types.ModuleType("pybit")
unified = types.ModuleType("pybit.unified_trading")
unified.HTTP = type("HTTP", (), {})
pybit.unified_trading = unified
sys.modules.setdefault("pybit", pybit)
sys.modules.setdefault("pybit.unified_trading", unified)

api_conf = types.ModuleType("api_conf_all")
api_conf.DEMO_CONFIG = {"api_key": "offline", "api_secret": "offline", "demo": True}
api_conf.api_t = ""
api_conf.chat = ""
sys.modules.setdefault("api_conf_all", api_conf)

speed_hunter = types.ModuleType("speed_hunter")
speed_hunter.signal = lambda *_args, **_kwargs: None
sys.modules.setdefault("speed_hunter", speed_hunter)

adaptive_filter = types.ModuleType("adaptive_filter")
adaptive_filter.CandleStore = type("CandleStore", (), {})
adaptive_filter.daily_reoptimize = lambda *_args, **_kwargs: None
adaptive_filter.load_active = lambda *_args, **_kwargs: None
adaptive_filter.apply_params = lambda *_args, **_kwargs: None
sys.modules.setdefault("adaptive_filter", adaptive_filter)

import speed_hunter_trader as bot  # noqa: E402


class RoiCycleTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "trader.sqlite3"
        self.journal = bot.Journal(self.db_path)
        bot.ROI_POST_CLOSE_PAUSE_SECONDS = 300
        bot.ROI_CLOSE_RETRY_ROUNDS = 3
        bot.ROI_CLOSE_RETRY_DELAY_SECONDS = 0

    def tearDown(self):
        self.journal.db.close()
        self.temp_dir.cleanup()

    def test_finish_records_end_and_next_start_atomically(self):
        old_cycle_id = self.journal.active_roi_cycle_id()
        self.journal.bank_info(25.0)
        self.journal.start_roi_close()
        before = int(bot.time.time() * 1000)

        self.journal.finish_cycle(Decimal("210.50"))
        after = int(bot.time.time() * 1000)

        old = self.journal.db.execute(
            "SELECT * FROM roi_cycles WHERE id=?", (old_cycle_id,)
        ).fetchone()
        self.assertEqual(old["status"], "COMPLETED")
        self.assertGreaterEqual(old["end_time_ms"], before)
        self.assertLessEqual(old["end_time_ms"], after)
        self.assertEqual(old["end_balance_usdt"], "210.50")

        new_id = self.journal.active_roi_cycle_id()
        self.assertNotEqual(new_id, old_cycle_id)
        new = self.journal.db.execute("SELECT * FROM roi_cycles WHERE id=?", (new_id,)).fetchone()
        self.assertEqual(new["status"], "RUNNING")
        self.assertEqual(new["start_balance_usdt"], "210.50")
        self.assertEqual(new["start_time_ms"], old["end_time_ms"])
        self.assertEqual(self.journal.bank_all(), 0.0)
        self.assertEqual(self.journal.state("roi_close_pending"), "0")

        pause_until = int(self.journal.state("roi_entry_pause_until_ms"))
        self.assertGreaterEqual(pause_until, old["end_time_ms"] + 300_000)
        self.assertEqual(self.journal.entry_pause_remaining_seconds(old["end_time_ms"]), 300)
        self.assertEqual(self.journal.entry_pause_remaining_seconds(pause_until), 0)

    def test_legacy_database_migrates_current_cycle_once(self):
        self.journal.db.close()
        legacy_path = Path(self.temp_dir.name) / "legacy.sqlite3"
        conn = sqlite3.connect(legacy_path)
        conn.execute("CREATE TABLE trader_state (key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)")
        conn.execute("INSERT INTO trader_state(key,value,updated_at) VALUES ('cycle_start_balance','210.50047559','2026-09-25 20:41:50')")
        conn.execute("INSERT INTO trader_state(key,value,updated_at) VALUES ('roi_close_pending','0','2026-09-25 20:41:50')")
        conn.commit()
        conn.close()

        migrated = bot.Journal(legacy_path)
        first_id = migrated.active_roi_cycle_id()
        row = migrated.db.execute("SELECT * FROM roi_cycles WHERE id=?", (first_id,)).fetchone()
        self.assertEqual(row["status"], "RUNNING")
        self.assertEqual(row["start_balance_usdt"], "210.50047559")
        self.assertEqual(row["start_source"], "legacy_state_timestamp")
        expected = int(datetime(2026, 9, 25, 20, 41, 50, tzinfo=timezone.utc).timestamp() * 1000)
        self.assertEqual(row["start_time_ms"], expected)
        migrated.db.close()

        reopened = bot.Journal(legacy_path)
        self.assertEqual(reopened.active_roi_cycle_id(), first_id)
        self.assertEqual(reopened.db.execute("SELECT COUNT(*) FROM roi_cycles").fetchone()[0], 1)
        reopened.db.close()

    def test_pending_close_retries_only_remaining_positions(self):
        self.journal.set_state("roi_close_pending", "1")
        long_leg = {"BTCUSDT": {1: {"size": "0.01", "side": "Buy"}}}
        short_leg = {"ETHUSDT": {2: {"size": "0.1", "side": "Sell"}}}
        session = types.SimpleNamespace(place_order=lambda **_kwargs: {"retCode": 0})
        positions_sequence = [
            {**long_leg, **short_leg},
            short_leg,
            {},  # Round 3 sees the account flat.
            {},  # Final verification.
            {},  # Verification after journal reconciliation.
        ]
        with patch.object(bot, "positions", side_effect=positions_sequence), \
             patch.object(bot, "reconcile"), \
             patch.object(bot, "wallet_equity", return_value=Decimal("205.00")), \
             patch.object(bot.time, "sleep"):
            result = bot.manage_roi_cycle(session, self.journal)

        self.assertTrue(result)
        self.assertEqual(self.journal.state("roi_close_pending"), "0")
        self.assertEqual(self.journal.db.execute("SELECT COUNT(*) FROM roi_cycles WHERE status='COMPLETED'").fetchone()[0], 1)
        cycle = self.journal.db.execute("SELECT close_attempts FROM roi_cycles WHERE status='COMPLETED'").fetchone()
        self.assertEqual(cycle["close_attempts"], 2)

    def test_partial_closure_keeps_cycle_pending_and_does_not_reset_bank(self):
        self.journal.set_state("roi_close_pending", "1")
        self.journal.bank_info(25.0)
        live = {"BTCUSDT": {1: {"size": "0.01", "side": "Buy"}}}
        session = types.SimpleNamespace(place_order=lambda **_kwargs: {"retCode": 0})
        with patch.object(bot, "positions", return_value=live), \
             patch.object(bot, "reconcile"), \
             patch.object(bot.time, "sleep"):
            result = bot.manage_roi_cycle(session, self.journal)

        self.assertTrue(result)
        self.assertEqual(self.journal.state("roi_close_pending"), "1")
        self.assertEqual(self.journal.bank_all(), 25.0)
        self.assertEqual(self.journal.db.execute("SELECT COUNT(*) FROM roi_cycles WHERE status='COMPLETED'").fetchone()[0], 0)
        self.assertEqual(self.journal.db.execute("SELECT close_attempts FROM roi_cycles WHERE status='CLOSING'").fetchone()[0], 3)

    def test_entry_scan_is_paused_until_persisted_deadline(self):
        now_ms = int(bot.time.time() * 1000)
        self.journal.set_state("roi_entry_pause_until_ms", str(now_ms + 300_000))
        session = object()
        with patch.object(bot, "positions", return_value={}), \
             patch.object(bot, "reconcile"), \
             patch.object(bot, "manage_roi_cycle", return_value=False), \
             patch.object(bot, "signal") as signal_mock:
            bot.run_once(session, self.journal, ["BTCUSDT"], set())
        signal_mock.assert_not_called()


if __name__ == "__main__":
    unittest.main(verbosity=2)
