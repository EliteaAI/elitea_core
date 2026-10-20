"""EL-7011 US-12 R1.2, R1.3, R2.1, R2.2, R2.3: the owner run starter for a pipeline schedule.

`run_owner_schedule` is loaded from source; its loader, pre-run check and run helpers are replaced by fakes.
"""
import sys
import types

import pytest
from unit._tool_spend_loader import load

PKG = "plugins.elitea_core"
OWNER, RUN_ID, SCHEDULE_ID = 12, 3, 1
VERSION_VARIABLES = [{"name": "region", "value": "EU"}]


def _module(name, **attrs):
    module = types.ModuleType(name)
    module.__path__ = []
    vars(module).update(attrs)
    return module


def _ignore(*_args):
    return None


def load_starter(monkeypatch, create, predict, prerun, *, finish_error=_ignore, fail=_ignore, block=_ignore,
                 budget_scope=_ignore, budget_error=None):
    """The real starter on faked neighbours; also used by test_7011_no_system_user_runs and _budget."""
    stubs = {
        "models.all": {"Application": object, "ApplicationVersion": object},
        "models.conversation": {"Conversation": object},
        "models.entity_schedule": {"EntitySchedule": object, "EntityScheduleRun": object},
        "utils.pipeline_execution": {
            "TriggerType": types.SimpleNamespace(SCHEDULED="scheduled"),
            "create_trigger_run_conversation": create,
            "execute_pipeline_via_predict_sio": predict,
            "finish_trigger_run_with_error": finish_error},
        "utils.schedule_prerun": {"run_prerun_check": prerun},
        "utils.schedule_runs": {"fail_schedule_run": fail, "block_schedule_run": block},
        "utils.budget_door": {"closed_budget_scope": budget_scope},
        "utils.exceptions": {
            "BudgetDoorClosedError": budget_error or type("BudgetDoorClosedError", (Exception,), {})},
    }
    for dotted, attrs in stubs.items():
        monkeypatch.setitem(sys.modules, f"{PKG}.{dotted}", _module(f"{PKG}.{dotted}", **attrs))
    monkeypatch.setitem(sys.modules, "tools", _module("tools", db=types.SimpleNamespace()))
    starter = load("utils/schedule_run_starter.py", fresh=True)
    monkeypatch.setattr(starter, "_link_conversation", lambda *a: True)
    return starter


@pytest.fixture
def world(monkeypatch):
    calls = types.SimpleNamespace(conversation=[], predict=[])
    starter = load_starter(
        monkeypatch,
        create=lambda **kw: calls.conversation.append(kw) or ("uuid-1", 7, "resp-1"),
        predict=lambda **kw: calls.predict.append(kw),
        prerun=lambda *a: types.SimpleNamespace(ok=True, owner_id=OWNER),
    )

    def run(entity_type="pipeline", run_mode=None, prompt="", variables=()):
        loaded = types.SimpleNamespace(
            schedule=types.SimpleNamespace(id=SCHEDULE_ID, entity_type=entity_type, run_mode=run_mode),
            version=types.SimpleNamespace(id=9), entity_name="Nightly flow", prompt=prompt, variables=list(variables))
        monkeypatch.setattr(starter, "_load", lambda *a: loaded)
        starter.run_owner_schedule(34, SCHEDULE_ID, RUN_ID)
        return calls

    return run


# TC-US12-01 / R1.2
def test_7011_r1_2_pipeline_run_conversation_has_source_pipeline_and_the_scheduled_name(world):
    calls = world(run_mode="prompt", prompt="Go")

    conversation = calls.conversation[0]
    assert conversation["source"] == "pipeline"
    assert conversation["conversation_name"] == "Scheduled run: Nightly flow"


# TC-US12-02 / R1.3
def test_7011_r1_3_pipeline_run_conversation_meta_names_the_schedule_and_the_run(world):
    calls = world(run_mode="prompt", prompt="Go")

    assert calls.conversation[0]["extra_meta"] == {
        "schedule_id": SCHEDULE_ID, "schedule_run_id": RUN_ID, "execution_type": "scheduled"}
    assert calls.conversation[0]["trigger_type"] == "scheduled"


# TC-US12-06, TC-US12-07, TC-US12-14 / R2.1
def test_7011_r2_1_prompt_mode_sends_the_filled_prompt_and_stores_it_as_first_message(world):
    calls = world(run_mode="prompt", prompt="Report for {{ region }} and {{team}}",
                  variables=[{"name": "region", "value": "US"}, {"name": "team", "value": "{{region}}"}])

    expected = "Report for US and {{region}}"
    assert calls.predict[0]["user_input"] == calls.conversation[0]["trigger_message"] == expected


# TC-US12-08, TC-US12-09, TC-US12-16 / R2.2
@pytest.mark.parametrize("prompt", ["", "Ignored {{x}}"])
def test_7011_r2_2_entry_point_mode_sends_empty_input_and_the_default_first_message(world, prompt):
    calls = world(run_mode="entry_point", prompt=prompt, variables=[{"name": "x", "value": "1"}])

    assert calls.predict[0]["user_input"] == ""
    assert calls.conversation[0]["trigger_message"] == "[Scheduled execution triggered]"


# TC-US12-15, TC-US12-17 / R2.3
@pytest.mark.parametrize("run_mode", ["prompt", "entry_point"])
def test_7011_r2_3_pipeline_predict_call_carries_no_schedule_variables(world, run_mode):
    calls = world(run_mode=run_mode, prompt="Go {{region}}", variables=VERSION_VARIABLES)

    predicted = calls.predict[0]
    assert "variables" not in predicted
    assert predicted["is_system_user"] is False and predicted["user_id"] == OWNER
    assert predicted["trigger_source"] == "scheduled"


# R2.1: an agent schedule keeps its prompt rule whatever run_mode holds
def test_7011_r2_1_agent_schedule_still_sends_the_filled_prompt(world):
    calls = world(entity_type="agent", prompt="Hi {{region}}", variables=VERSION_VARIABLES)

    assert calls.conversation[0]["source"] == "agent"
    assert calls.predict[0]["user_input"] == "Hi EU"
