"""Issue #7023 - when the scheduler may remove the schedules of an index it cannot find."""

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

PLUGIN_ROOT = Path(__file__).parents[2]
sys.path.insert(0, str(PLUGIN_ROOT / "tests"))

from fixtures.helpers import register_index_pd_module  # noqa: E402

NOW = datetime(2026, 10, 13, 12, 0, tzinfo=timezone.utc)


@pytest.fixture(scope="module")
def index_pd():
    return register_index_pd_module(PLUGIN_ROOT)


def _entry(missing_since):
    return {"schedules": {"7": {}}, "missing_since": missing_since}


def test_the_first_miss_starts_the_grace(index_pd):
    assert index_pd.missing_index_verdict({"schedules": {}}, NOW) == \
        index_pd.MissingIndexVerdict.START_GRACE


@pytest.mark.parametrize("missing_since", ["not a date", 42, (NOW + timedelta(hours=1)).isoformat()])
def test_an_unusable_start_restarts_the_grace_rather_than_pruning(index_pd, missing_since):
    assert index_pd.missing_index_verdict(_entry(missing_since), NOW) == \
        index_pd.MissingIndexVerdict.START_GRACE


def test_a_miss_on_the_same_day_waits(index_pd):
    started = NOW - timedelta(hours=23)
    assert index_pd.missing_index_verdict(_entry(started.isoformat()), NOW) == \
        index_pd.MissingIndexVerdict.WAIT


def test_a_daily_schedule_prunes_on_its_second_firing_despite_tick_jitter(index_pd):
    started = NOW - timedelta(hours=23, minutes=59, seconds=25)
    assert index_pd.missing_index_verdict(_entry(started.isoformat()), NOW) == \
        index_pd.MissingIndexVerdict.PRUNE


def test_a_miss_that_outlived_the_grace_prunes(index_pd):
    started = NOW - index_pd.MISSING_INDEX_GRACE
    assert index_pd.missing_index_verdict(_entry(started.isoformat()), NOW) == \
        index_pd.MissingIndexVerdict.PRUNE


def test_a_naive_start_is_read_as_utc(index_pd):
    started = (NOW - timedelta(hours=1)).replace(tzinfo=None)
    assert index_pd.missing_index_verdict(_entry(started.isoformat()), NOW) == \
        index_pd.MissingIndexVerdict.WAIT

