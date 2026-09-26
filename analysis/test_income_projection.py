"""Deterministic unit tests for the offline income projection tool."""
from datetime import date
import math
import unittest

from income_projection import (
    cluster_roi_closes,
    projection_from_daily_return,
    select_full_daily_returns,
)


class IncomeProjectionTests(unittest.TestCase):
    def test_roi_close_clustering_uses_gap_boundary(self):
        batches = cluster_roi_closes(
            [0, 60_000, 600_000, 1_260_001], gap_minutes=10
        )
        self.assertEqual(len(batches), 2)
        self.assertEqual(batches[0], {
            "start_time_ms": 0,
            "end_time_ms": 600_000,
            "positions": 3,
        })
        self.assertEqual(batches[1]["start_time_ms"], 1_260_001)

    def test_selects_exact_full_days_before_as_of_date(self):
        daily = {
            "2026-09-23": 8.0,
            "2026-09-24": 66.0,
            "2026-09-25": 29.0,
            "2026-09-26": 1000.0,  # Must be excluded for as-of Sep 26.
        }
        selected = select_full_daily_returns(daily, date(2026, 9, 26), 2)
        self.assertEqual(selected, {"2026-09-24": 66.0, "2026-09-25": 29.0})

    def test_missing_calendar_day_is_not_silently_skipped(self):
        with self.assertRaisesRegex(ValueError, "2026-09-24"):
            select_full_daily_returns(
                {"2026-09-23": 8.0, "2026-09-25": 29.0},
                date(2026, 9, 26),
                2,
            )

    def test_target_balance_notional_and_compounding_time(self):
        result = projection_from_daily_return(
            current_balance=1000.0,
            daily_return=0.10,
            daily_target=500.0,
            position_share=0.05,
        )
        self.assertAlmostEqual(result["target_balance_usdt"], 5000.0)
        self.assertAlmostEqual(result["target_base_notional_usdt"], 250.0)
        self.assertAlmostEqual(
            result["days_from_current_balance"], math.log(5) / math.log(1.1)
        )

    def test_nonpositive_return_has_no_finite_projection(self):
        result = projection_from_daily_return(
            current_balance=1000.0,
            daily_return=0.0,
            daily_target=500.0,
            position_share=0.05,
        )
        self.assertIsNone(result["target_balance_usdt"])
        self.assertIsNone(result["days_from_current_balance"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
