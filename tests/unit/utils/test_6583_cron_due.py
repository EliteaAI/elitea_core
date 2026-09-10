"""Issue #6583 - the cron cursor is what stops a failed schedule re-firing every tick.

`is_cron_due` had no test coverage at all, yet it is the hinge the whole scheduler turns
on: it answers "is this schedule due?" purely as `next_firing_after(last_run) <= now`.
Because the only write of `last_run` used to be after a successful dispatch, a schedule
that failed before dispatching stayed due on *every* 60s tick forever, appending a failed
history entry and notifying its owner each time.

These tests pin the two halves of that: a cursor that does not move stays due, and a
cursor that moves does not.

Run via:
    python tests/run_tests.py unit/utils/test_6583_cron_due.py -v
"""

import importlib.util
import pathlib
import sys
import types
from datetime import datetime, timedelta, timezone

import pytest


PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[3]


@pytest.fixture(scope="module")
def cron_utils():
    """Load utils/cron_utils.py standalone (it has no plugin-relative imports)."""
    pylon_tools = types.ModuleType("pylon.core.tools")
    pylon_tools.log = types.SimpleNamespace(
        info=lambda *a, **k: None, warning=lambda *a, **k: None,
        error=lambda *a, **k: None, debug=lambda *a, **k: None,
        exception=lambda *a, **k: None,
    )
    sys.modules.setdefault("pylon", types.ModuleType("pylon"))
    sys.modules.setdefault("pylon.core", types.ModuleType("pylon.core"))
    sys.modules["pylon.core.tools"] = pylon_tools

    spec = importlib.util.spec_from_file_location(
        "elitea_core_cron_utils", PLUGIN_ROOT / "utils" / "cron_utils.py"
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _iso(dt):
    return dt.astimezone(timezone.utc).isoformat()


DAILY_AT_0300 = "0 3 * * *"


class TestTheRunaway:
    """The bug: a cursor that never advances leaves the schedule due on every tick."""

    def test_a_stuck_cursor_is_still_due_on_every_repeated_check(self, cron_utils):
        # Yesterday 03:00 UTC: the daily firing has passed and last_run never moved.
        stuck = _iso(datetime.now(timezone.utc).replace(
            hour=3, minute=0, second=0, microsecond=0) - timedelta(days=1))
        # Five consecutive ticks, nothing changing in between - this is the loop.
        assert [cron_utils.is_cron_due(DAILY_AT_0300, stuck, "UTC") for _ in range(5)] \
            == [True] * 5

    def test_an_advanced_cursor_is_not_due(self, cron_utils):
        """The assertion the whole fix rests on: stamping last_run ends the loop."""
        just_stamped = _iso(datetime.now(timezone.utc))
        assert cron_utils.is_cron_due(DAILY_AT_0300, just_stamped, "UTC") is False

    def test_a_cursor_advanced_to_now_is_due_again_only_after_the_next_firing(self, cron_utils):
        """One dispatch per cron period: due again a day later, not a minute later."""
        stamped = datetime.now(timezone.utc)
        assert cron_utils.is_cron_due(
            DAILY_AT_0300, _iso(stamped - timedelta(minutes=1)), "UTC") is False
        assert cron_utils.is_cron_due(
            DAILY_AT_0300, _iso(stamped - timedelta(days=2)), "UTC") is True


class TestBoundaries:
    def test_a_future_cursor_is_not_due(self, cron_utils):
        ahead = _iso(datetime.now(timezone.utc) + timedelta(days=1))
        assert cron_utils.is_cron_due(DAILY_AT_0300, ahead, "UTC") is False

    def test_the_timezone_decides_when_the_same_instant_is_due(self, cron_utils, monkeypatch):
        """A daily '03:00' cron fires at a different instant per zone, so the tz argument
        has to reach croniter. Pinned to a fixed instant: against the real clock the
        expected answers move through the day and the test flakes.
        """
        pinned = datetime(2026, 3, 10, 6, 0, tzinfo=timezone.utc)

        class _FixedDatetime(datetime):
            @classmethod
            def now(cls, tz=None):
                return pinned.astimezone(tz) if tz else pinned.replace(tzinfo=None)

        monkeypatch.setattr(cron_utils, "datetime", _FixedDatetime)
        cursor = _iso(pinned - timedelta(hours=6))

        assert cron_utils.is_cron_due(DAILY_AT_0300, cursor, "UTC") is True
        assert cron_utils.is_cron_due(DAILY_AT_0300, cursor, "Asia/Tokyo") is False


class TestBadInputNeverRaises:
    """A single bad schedule must not take the tick down - is_cron_due absorbs and says
    'not due', which keeps it out of the dispatch path."""

    @pytest.mark.parametrize("cron,last_run,tz", [
        ("not a cron", "2026-01-01T00:00:00+00:00", "UTC"),
        ("0 3 * * *", "not a timestamp", "UTC"),
        ("0 3 * * *", "2026-01-01T00:00:00+00:00", "Mars/Olympus_Mons"),
        ("", "2026-01-01T00:00:00+00:00", "UTC"),
        ("0 3 * * *", None, "UTC"),
    ])
    def test_malformed_input_returns_false_without_raising(self, cron_utils, cron, last_run, tz):
        assert cron_utils.is_cron_due(cron, last_run, tz) is False

    def test_a_naive_last_run_is_still_handled(self, cron_utils):
        """Stored cursors are always tz-aware, but a hand-edited row may not be."""
        naive = datetime.now().replace(hour=3, minute=0).isoformat()
        assert cron_utils.is_cron_due(DAILY_AT_0300, naive, "UTC") in (True, False)
