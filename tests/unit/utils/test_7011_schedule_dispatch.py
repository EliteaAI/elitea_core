"""EL-7011 US-08 R3.5: the run starter map and the start that does not wait."""
import json
import pathlib
import sys
import threading
import types

import pytest
import yaml
from unit._tool_spend_loader import load

DISPATCH = "utils/schedule_dispatch.py"
WAIT = 5


@pytest.fixture
def dispatch(monkeypatch):
    module = load(DISPATCH, fresh=True)
    monkeypatch.setattr(module, "RUN_STARTERS", {})
    module.config = {}  # the schedules section of the plugin config, read when the pool is first used
    descriptor = types.SimpleNamespace(config={"scheduler": {"schedules": module.config}})
    monkeypatch.setattr(sys.modules["tools"], "this", types.SimpleNamespace(
        module=types.SimpleNamespace(descriptor=descriptor)), raising=False)
    return module


def test_a_start_with_no_free_slot_returns_false_and_the_slot_frees_when_the_starter_ends(dispatch):
    dispatch.config["max_parallel_runs"] = 1
    entered, release = threading.Event(), threading.Event()

    def starter(*_):
        entered.set()
        release.wait(WAIT)

    dispatch.RUN_STARTERS["agent"] = starter
    assert dispatch.start_schedule_run("agent", 1, 2, 3) is True
    assert entered.wait(WAIT)
    assert dispatch.start_schedule_run("agent", 1, 2, 4) is False
    release.set()
    for _ in range(100):
        if dispatch.start_schedule_run("agent", 1, 2, 5):
            break
        threading.Event().wait(0.05)
    else:
        pytest.fail("the slot was never freed")


def test_the_slot_pool_is_cut_to_the_admin_maximum(dispatch):
    dispatch.config["max_parallel_runs"] = 1_000_000
    pool = dispatch._slot_pool()
    assert sum(pool.acquire(blocking=False) for _ in range(1001)) == 1000


def test_a_failed_starter_frees_its_slot(dispatch, monkeypatch):
    dispatch.config["max_parallel_runs"] = 1
    monkeypatch.setattr(dispatch, "_close_unstarted_run", lambda *a: None)
    done = threading.Event()

    def starter(*_):
        done.set()
        raise RuntimeError("boom")

    dispatch.RUN_STARTERS["agent"] = starter
    assert dispatch.start_schedule_run("agent", 1, 2, 3) is True
    assert done.wait(WAIT)
    for _ in range(100):
        if dispatch._slot_pool().acquire(blocking=False):
            return
        threading.Event().wait(0.05)
    pytest.fail("the slot was never freed")


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


def test_shipped_defaults_keep_run_slots_at_least_the_dispatch_cap(dispatch):
    root = pathlib.Path(__file__).resolve().parents[3]
    section = yaml.safe_load((root / "config.yml").read_text())["scheduler"]["schedules"]
    props = json.loads((root / "admin_schema.json").read_text())["properties"]
    assert section["max_parallel_runs"] >= section["dispatch_cap"]
    assert props["schedules_max_parallel_runs"]["default"] >= props["schedules_dispatch_cap"]["default"]
    assert dispatch.DEFAULT_MAX_PARALLEL_RUNS >= section["dispatch_cap"]
