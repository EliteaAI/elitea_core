"""EL-7011 US-12 R1.4, R3.1: no schedule run path calls predict as the system user.

Each path starts through the run starter map, as the tick does. The starter is loaded from source;
its loader, pre-run check and run helpers are replaced by fakes, and predict is a recording stub.
"""
import sys
import types

import pytest
from unit._tool_spend_loader import load

PKG = "plugins.elitea_core"
OWNER = 12


def _module(name, **attrs):
    module = types.ModuleType(name)
    module.__path__ = []
    vars(module).update(attrs)
    return module


@pytest.fixture
def world(monkeypatch):
    """run(entity_type, prerun_ok, predict_error) starts one run and returns the recorded predict calls."""
    predict = []

    def run(entity_type, prerun_ok=True, predict_error=None):
        def record(**kwargs):
            predict.append(kwargs)
            if predict_error:
                raise predict_error

        stubs = {
            "models.all": {"Application": object, "ApplicationVersion": object},
            "models.conversation": {"Conversation": object},
            "models.entity_schedule": {"EntitySchedule": object, "EntityScheduleRun": object},
            "utils.pipeline_execution": {
                "TriggerType": types.SimpleNamespace(SCHEDULED="scheduled"),
                "create_trigger_run_conversation": lambda **kw: ("uuid-1", 7, "resp-1"),
                "execute_pipeline_via_predict_sio": record,
                "finish_trigger_run_with_error": lambda *a: None},
            "utils.schedule_prerun": {
                "run_prerun_check": lambda *a: types.SimpleNamespace(
                    ok=prerun_ok, owner_id=OWNER if prerun_ok else None)},
            "utils.schedule_runs": {"fail_schedule_run": lambda *a: None},
        }
        for dotted, attrs in stubs.items():
            monkeypatch.setitem(sys.modules, f"{PKG}.{dotted}", _module(f"{PKG}.{dotted}", **attrs))
        monkeypatch.setitem(sys.modules, "tools", _module("tools", db=types.SimpleNamespace()))
        starter = load("utils/schedule_run_starter.py", fresh=True)
        monkeypatch.setitem(sys.modules, f"{PKG}.utils.schedule_run_starter", starter)
        monkeypatch.setattr(starter, "_link_conversation", lambda *a: True)
        loaded = types.SimpleNamespace(
            schedule=types.SimpleNamespace(id=1, entity_type=entity_type, run_mode="prompt"),
            version=types.SimpleNamespace(id=9), agent_name="Flow", prompt="Go", variables=[])
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
