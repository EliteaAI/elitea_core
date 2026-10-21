"""EL-7011 BUG-12 R1.1, R1.2, R1.3: one shared trigger enum; Run now and Test run send schedule_manual.

Contract under test: utils/pipeline_execution.py `TriggerType` and utils/schedule_run_starter.py
`run_owner_schedule`, both loaded from source; the starter's loader, pre-run check and run helpers are fakes.
"""
import types

import pytest
from unit.utils.test_7011_pipeline_execution_owner import world  # noqa: F401 -- the real TriggerType
from unit.utils.test_7011_run_starter_pipeline import (
    OWNER,
    RUN_ID,
    SCHEDULE_ID,
    load_starter,
)


@pytest.fixture
def run(monkeypatch, world):  # noqa: F811 -- the fixture imported above
    """run(trigger) starts one agent run with that run trigger and returns the recorded calls."""
    calls = types.SimpleNamespace(conversation=[], predict=[])
    trigger_type = world.execution.TriggerType
    starter = load_starter(
        monkeypatch,
        create=lambda **kw: calls.conversation.append(kw) or ("uuid-1", 7, "resp-1"),
        predict=lambda **kw: calls.predict.append(kw),
        prerun=lambda *a: types.SimpleNamespace(ok=True, owner_id=OWNER),
    )
    monkeypatch.setattr(starter, "TriggerType", trigger_type)

    def start(trigger):
        loaded = types.SimpleNamespace(
            schedule=types.SimpleNamespace(id=SCHEDULE_ID, entity_type="agent", run_mode="prompt",
                                           approval_rule="pause_and_ask"),
            version=types.SimpleNamespace(id=9, name="v3"), entity_name="Nightly", prompt="Go", variables=[], trigger=trigger)
        monkeypatch.setattr(starter, "_load", lambda *a: loaded)
        starter.run_owner_schedule(34, SCHEDULE_ID, RUN_ID)
        return calls

    return start


def test_el_7011_bug12_r1_1_trigger_type_holds_the_shared_enum(world):  # noqa: F811
    trigger_type = world.execution.TriggerType

    assert (trigger_type.CHAT, trigger_type.API, trigger_type.MCP, trigger_type.WEBHOOK,
            trigger_type.SCHEDULED, trigger_type.SCHEDULE_MANUAL) == (
        "chat", "api", "mcp", "webhook", "scheduled", "schedule_manual")
    assert {"chat", "api", "mcp", "webhook", "scheduled", "schedule_manual", "manual"} == set(trigger_type.VALUES)


@pytest.mark.parametrize("trigger", ["manual", "test"])
def test_el_7011_bug12_r1_1_run_now_and_test_run_send_schedule_manual(run, trigger):
    calls = run(trigger)

    assert calls.conversation[0]["trigger_type"] == "schedule_manual"
    assert calls.conversation[0]["extra_meta"]["execution_type"] == "schedule_manual"
    assert calls.predict[0]["trigger_source"] == "schedule_manual"


def test_el_7011_bug12_r1_2_cron_run_still_sends_scheduled(run):
    calls = run("cron")

    assert calls.conversation[0]["trigger_type"] == "scheduled"
    assert calls.conversation[0]["extra_meta"]["execution_type"] == "scheduled"
    assert calls.predict[0]["trigger_source"] == "scheduled"
