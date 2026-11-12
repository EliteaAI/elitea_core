"""Unit tests for the monthly budget history helpers (utils/budget_periods.py, #7510).

The admin Budgets page can show a past month, so these pin the rules it depends on: which
months are selectable, how a bad period is refused, the reporting range, and what a snapshot
records. The snapshot rules are the point of the feature: a closed month must keep the limit
and source it had, and "nothing recorded" must never read as "unlimited".

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


class TestLimitSource(unittest.TestCase):
    def test_a_stored_limit_is_explicit(self):
        self.assertEqual(periods.limit_source({"monthly_limit": 50.0, "enabled": True}, 50.0), "explicit")

    def test_no_row_with_a_default_is_default(self):
        self.assertEqual(periods.limit_source({}, 20.0), "default")

    def test_no_limit_at_all_is_unlimited(self):
        self.assertEqual(periods.limit_source({}, None), "unlimited")

    def test_an_exempt_row_does_not_read_as_explicit(self):
        # enabled=false means deliberately exempt; the stored number does not apply
        self.assertEqual(
            periods.limit_source({"monthly_limit": 50.0, "enabled": False}, None), "unlimited",
        )

    def test_zero_is_a_real_explicit_limit(self):
        self.assertEqual(periods.limit_source({"monthly_limit": 0.0, "enabled": True}, 0.0), "explicit")


class TestDesiredSnapshots(unittest.TestCase):
    def test_every_requested_project_gets_a_snapshot(self):
        desired = periods.desired_snapshots(
            [1, 2, 3],
            {1: {"monthly_limit": 50.0, "enabled": True, "currency": "USD"}},
            {1: 50.0, 2: 20.0, 3: None},
        )
        self.assertEqual(desired[1]["limit_source"], "explicit")
        self.assertEqual(desired[2]["limit_source"], "default")
        self.assertEqual(desired[3]["limit_source"], "unlimited")
        self.assertIsNone(desired[3]["effective_limit"])

    def test_string_keys_from_rpc_serialization_still_match(self):
        desired = periods.desired_snapshots(
            [1], {"1": {"monthly_limit": 9.0, "enabled": True}}, {"1": 9.0},
        )
        self.assertEqual(desired[1]["effective_limit"], 9.0)
        self.assertEqual(desired[1]["limit_source"], "explicit")

    def test_currency_defaults_to_usd(self):
        self.assertEqual(periods.desired_snapshots([1], {}, {1: None})[1]["currency"], "USD")


class TestChangedSnapshots(unittest.TestCase):
    WANT = {"effective_limit": 50.0, "limit_source": "explicit", "currency": "USD"}

    def test_an_identical_snapshot_is_not_rewritten(self):
        existing = {1: {"project_id": 1, "period": "202611", **self.WANT}}
        self.assertEqual(periods.changed_snapshots(existing, {1: dict(self.WANT)}), {})

    def test_a_missing_snapshot_is_written(self):
        self.assertEqual(periods.changed_snapshots({}, {1: dict(self.WANT)}), {1: self.WANT})

    def test_a_changed_limit_is_written(self):
        existing = {1: {"project_id": 1, "period": "202611", **self.WANT, "effective_limit": 10.0}}
        self.assertEqual(periods.changed_snapshots(existing, {1: dict(self.WANT)}), {1: self.WANT})

    def test_a_changed_source_alone_is_written(self):
        # Same number, different reason: an admin deleting an explicit limit that equals the default
        existing = {1: {"project_id": 1, "period": "202611", **self.WANT, "limit_source": "default"}}
        self.assertEqual(periods.changed_snapshots(existing, {1: dict(self.WANT)}), {1: self.WANT})

    def test_unlimited_is_distinct_from_missing(self):
        unlimited = {"effective_limit": None, "limit_source": "unlimited", "currency": "USD"}
        existing = {1: {"project_id": 1, "period": "202611", **unlimited}}
        # stored as unlimited: nothing to write; absent: must be written
        self.assertEqual(periods.changed_snapshots(existing, {1: dict(unlimited)}), {})
        self.assertEqual(periods.changed_snapshots({}, {1: dict(unlimited)}), {1: unlimited})


if __name__ == "__main__":
    unittest.main(verbosity=2)
