"""Unit tests for the month helpers of the admin budgets listing (utils/budget_periods.py, #7510).

The admin Budgets page can show a past month, so these pin the rules it depends on: which
months are selectable, how a bad period is refused, and the reporting range.

Run standalone: python3 tests/unit/test_7510_budget_periods.py
"""

import datetime
import importlib.util
import os
import unittest


def _load_module():
    """Load utils/budget_periods.py on its own: its database imports are lazy."""
    plugin_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    spec = importlib.util.spec_from_file_location(
        "budget_periods_under_test", os.path.join(plugin_root, "utils", "budget_periods.py"),
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    #
    return module


periods = _load_module()


def utc(year, month, day=15):
    return datetime.datetime(year, month, day, 12, 0, tzinfo=datetime.timezone.utc)


NOV = utc(2026, 11, 12)


class TestPeriodWindow(unittest.TestCase):
    def test_current_month_then_three_before_it_newest_first(self):
        self.assertEqual(periods.period_window(NOV), ["202611", "202610", "202609", "202608"])

    def test_the_window_crosses_a_year_boundary(self):
        self.assertEqual(
            periods.period_window(utc(2027, 1, 3)), ["202701", "202612", "202611", "202610"],
        )

    def test_current_period_is_utc(self):
        # 23:30 on Oct 31 in UTC is already Nov 1 in UTC+2; the month follows UTC
        late = datetime.datetime(2026, 10, 31, 23, 30, tzinfo=datetime.timezone.utc)
        self.assertEqual(periods.current_period(late), "202610")


class TestValidatePeriod(unittest.TestCase):
    def test_missing_means_the_current_month(self):
        self.assertEqual(periods.validate_period(None, NOV), "202611")
        self.assertEqual(periods.validate_period("", NOV), "202611")

    def test_a_month_in_the_window_is_accepted(self):
        for period in ("202611", "202610", "202609", "202608"):
            self.assertEqual(periods.validate_period(period, NOV), period)

    def test_malformed_values_are_refused(self):
        for bad in ("2026-10", "20261", "2026100", "abcdef", "20261x"):
            with self.assertRaises(ValueError, msg=bad):
                periods.validate_period(bad, NOV)

    def test_month_thirteen_is_refused(self):
        with self.assertRaisesRegex(ValueError, "01-12"):
            periods.validate_period("202613", NOV)

    def test_a_future_month_is_refused(self):
        with self.assertRaisesRegex(ValueError, "future"):
            periods.validate_period("202612", NOV)

    def test_a_month_older_than_the_window_is_refused(self):
        with self.assertRaisesRegex(ValueError, "last 4 months"):
            periods.validate_period("202607", NOV)


class TestPeriodRange(unittest.TestCase):
    def test_current_month_runs_from_the_first_to_today(self):
        self.assertEqual(periods.period_range("202611", NOV), ("2026-11-01", "2026-11-12"))

    def test_a_past_month_is_the_whole_calendar_month(self):
        self.assertEqual(periods.period_range("202610", NOV), ("2026-10-01", "2026-10-31"))
        self.assertEqual(periods.period_range("202609", NOV), ("2026-09-01", "2026-09-30"))

    def test_february_follows_the_leap_year_rule(self):
        self.assertEqual(
            periods.period_range("202402", utc(2024, 4, 10)), ("2024-02-01", "2024-02-29"),
        )
        self.assertEqual(
            periods.period_range("202502", utc(2025, 4, 10)), ("2025-02-01", "2025-02-28"),
        )

    def test_december_ends_on_the_31st(self):
        self.assertEqual(
            periods.period_range("202612", utc(2027, 1, 5)), ("2026-12-01", "2026-12-31"),
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
