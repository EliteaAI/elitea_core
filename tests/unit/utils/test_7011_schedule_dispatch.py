"""EL-7011 US-08 R3.5: the run starter map and the start that does not wait."""
import threading

import pytest
from unit._tool_spend_loader import load

DISPATCH = "utils/schedule_dispatch.py"
WAIT = 5


@pytest.fixture
def dispatch(monkeypatch):
    module = load(DISPATCH, fresh=True)
    monkeypatch.setattr(module, "RUN_STARTERS", {})
    return module


def test_7011_r3_5_starter_map_starts_empty():
    assert load(DISPATCH, fresh=True).RUN_STARTERS == {}


def test_7011_r3_5_start_returns_before_the_starter_finishes(dispatch):
    entered, release, calls = threading.Event(), threading.Event(), []

    def starter(project_id, schedule_id, run_id):
        calls.append((project_id, schedule_id, run_id))
        entered.set()
        release.wait(WAIT)

    dispatch.RUN_STARTERS["agent"] = starter
    try:
        dispatch.start_schedule_run("agent", 1, 2, 3)
        assert entered.wait(WAIT)
        assert not release.is_set()
        assert calls == [(1, 2, 3)]
    finally:
        release.set()


def test_7011_r3_5_starter_runs_in_a_daemon_thread(dispatch):
    seen = []
    done = threading.Event()

    def starter(*_):
        seen.append(threading.current_thread())
        done.set()

    dispatch.RUN_STARTERS["pipeline"] = starter
    dispatch.start_schedule_run("pipeline", 1, 2, 3)
    assert done.wait(WAIT)
    assert seen[0] is not threading.current_thread()
    assert seen[0].daemon


def test_7011_r3_5_starter_error_does_not_reach_the_caller_and_closes_the_run(dispatch, monkeypatch):
    closed = threading.Event()
    calls = []

    def close(*args):
        calls.append(args)
        closed.set()

    def starter(*_):
        raise RuntimeError("boom")

    monkeypatch.setattr(dispatch, "_close_unstarted_run", close)
    dispatch.RUN_STARTERS["agent"] = starter
    dispatch.start_schedule_run("agent", 1, 2, 3)
    assert closed.wait(WAIT)
    assert calls == [(1, 2, 3)]


def test_7011_r3_5_type_without_starter_is_not_started(dispatch):
    assert dispatch.start_schedule_run("toolkit", 1, 2, 3) is False
