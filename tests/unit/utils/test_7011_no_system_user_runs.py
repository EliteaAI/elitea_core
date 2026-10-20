"""EL-7011 US-12 R1.4, R3.1: no schedule run path calls predict as the system user.

Each path starts through the run starter map, as the tick does. The starter is loaded from source;
its loader, pre-run check and run helpers are replaced by fakes, and predict is a recording stub.
"""
import sys
import types

import pytest
from unit._tool_spend_loader import load
from unit.utils.test_7011_run_starter_pipeline import OWNER, PKG, load_starter


@pytest.fixture
def world(monkeypatch):
    """run(entity_type, prerun_ok, predict_error) starts one run and returns the recorded predict calls."""
    predict = []

    def run(entity_type, prerun_ok=True, predict_error=None):
        def record(**kwargs):
            predict.append(kwargs)
            if predict_error:
                raise predict_error

        starter = load_starter(
            monkeypatch,
            create=lambda **kw: ("uuid-1", 7, "resp-1"),
            predict=record,
            prerun=lambda *a: types.SimpleNamespace(ok=prerun_ok, owner_id=OWNER if prerun_ok else None),
        )
        monkeypatch.setitem(sys.modules, f"{PKG}.utils.schedule_run_starter", starter)
        loaded = types.SimpleNamespace(
            schedule=types.SimpleNamespace(id=1, entity_type=entity_type, run_mode="prompt", approval_rule="pause_and_ask"),
            version=types.SimpleNamespace(id=9), entity_name="Flow", prompt="Go", variables=[])
        monkeypatch.setattr(starter, "_load", lambda *a: loaded)
        load("utils/schedule_dispatch.py", fresh=True).RUN_STARTERS[entity_type](34, 1, 3)
        return predict

    return run


# TC-US12-04 / R1.4: a cron run of either type calls predict as the owner, marked scheduled (R3.1)
@pytest.mark.parametrize("entity_type", ["agent", "pipeline"])
def test_7011_r1_4_cron_run_calls_predict_as_the_owner(world, entity_type):
    calls = world(entity_type)

    assert len(calls) == 1
    assert calls[0]["is_system_user"] is False and calls[0]["user_id"] == OWNER
    assert calls[0]["trigger_source"] == "scheduled"


# TC-US12-04 / R1.4: a failed pre-run check never falls back to the system user
@pytest.mark.parametrize("entity_type", ["agent", "pipeline"])
def test_7011_r1_4_failed_prerun_check_calls_no_predict(world, entity_type):
    assert world(entity_type, prerun_ok=False) == []


# TC-US12-04, TC-US12-05 / R1.4: a predict error is not retried as the system user
@pytest.mark.parametrize("entity_type", ["agent", "pipeline"])
def test_7011_r1_4_predict_error_is_not_retried_as_the_system_user(world, entity_type):
    calls = world(entity_type, predict_error=RuntimeError("boom"))

    assert [call["is_system_user"] for call in calls] == [False]
