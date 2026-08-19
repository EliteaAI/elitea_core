"""Unit tests for the orphaned-run staleness decision (§14.2 durability).

``launch_run`` executes on a worker thread of an in-memory task pool, so a process death —
hard restart, OOM kill, or a drain that outlasts ``task_wait_timeout`` — leaves the row stuck
in ``running``. The reaper fails those rows — but several API workers
share the database, so it must key off the per-case progress heartbeat rather than on startup
state, or worker B booting would kill a run worker A is still executing. These tests lock that
distinction.
"""
import pathlib
import sys
from datetime import datetime, timedelta

import pytest

TESTS_DIR = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(TESTS_DIR))

from fixtures.helpers import load_utils_module  # noqa: E402

NOW = datetime(2026, 8, 19, 12, 0, 0)


@pytest.fixture(scope='module')
def reaper(utils_path):
    return load_utils_module(utils_path, 'evaluation_run_reaper')


def test_a_run_that_committed_progress_recently_is_left_alone(reaper):
    """The load-bearing case: a live run in another worker must survive a boot elsewhere."""
    assert reaper.is_stale_run(
        started_at=NOW - timedelta(hours=4),
        updated_at=NOW - timedelta(minutes=2),
        now=NOW,
    ) is False


def test_a_long_quiet_run_is_stale(reaper):
    assert reaper.is_stale_run(
        started_at=NOW - timedelta(hours=4),
        updated_at=NOW - timedelta(hours=1),
        now=NOW,
    ) is True


def test_started_but_never_progressed_falls_back_to_started_at(reaper):
    """A run dies before finishing its first case, so ``updated_at`` is still NULL."""
    assert reaper.is_stale_run(NOW - timedelta(minutes=5), None, NOW) is False
    assert reaper.is_stale_run(NOW - timedelta(hours=2), None, NOW) is True


def test_the_later_timestamp_wins(reaper):
    """``started_at`` after a stale ``updated_at`` still counts as a heartbeat."""
    assert reaper.is_stale_run(
        started_at=NOW - timedelta(minutes=1),
        updated_at=NOW - timedelta(hours=3),
        now=NOW,
    ) is False


def test_no_timestamps_at_all_is_stale(reaper):
    """A ``running`` row cannot be claimed without ``started_at``, so it predates the guard."""
    assert reaper.is_stale_run(None, None, NOW) is True


def test_threshold_is_a_per_case_ceiling_not_a_run_ceiling(reaper):
    """Worst case per case is ~agent timeout + a few 60s dispatches, so the default must sit
    well above that while staying short enough to be useful."""
    assert reaper.RUN_STALE_AFTER_SECONDS >= 15 * 60

    boundary = NOW - timedelta(seconds=reaper.RUN_STALE_AFTER_SECONDS)
    assert reaper.is_stale_run(None, boundary, NOW) is False
    assert reaper.is_stale_run(None, boundary - timedelta(seconds=1), NOW) is True


def test_threshold_is_overridable(reaper):
    assert reaper.is_stale_run(None, NOW - timedelta(minutes=5), NOW, 60) is True
    assert reaper.is_stale_run(None, NOW - timedelta(minutes=5), NOW, 3600) is False
