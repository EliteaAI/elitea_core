"""EL-7011 US-15 R1.2, R1.3: a manual run goes through the owner run starter with the manual trigger.

The real starter runs on faked neighbours (TC-US15-01, -06, -07); a cron run keeps its values.
"""
import types

import pytest
from unit.utils.test_7011_run_starter_pipeline import (
    OWNER,
    RUN_ID,
    SCHEDULE_ID,
    load_starter,
)


@pytest.fixture
def world(monkeypatch):
    calls = types.SimpleNamespace(conversation=[], predict=[])
    starter = load_starter(
        monkeypatch,
        create=lambda **kw: calls.conversation.append(kw) or ("uuid-1", 7, "resp-1"),
        predict=lambda **kw: calls.predict.append(kw),
        prerun=lambda *a: types.SimpleNamespace(ok=True, owner_id=OWNER),
    )
    # the stubbed TriggerType of the shared harness has no SCHEDULE_MANUAL member
    monkeypatch.setattr(starter, "TriggerType", types.SimpleNamespace(SCHEDULED="scheduled",
                                                                      SCHEDULE_MANUAL="schedule_manual"))

    def run(trigger):
        loaded = types.SimpleNamespace(
            schedule=types.SimpleNamespace(id=SCHEDULE_ID, entity_type="agent", run_mode="prompt", approval_rule="pause_and_ask"),
            version=types.SimpleNamespace(id=9), entity_name="Nightly", prompt="Go", variables=[], trigger=trigger)
        monkeypatch.setattr(starter, "_load", lambda *a: loaded)
        starter.run_owner_schedule(34, SCHEDULE_ID, RUN_ID)
        return calls

    return run


# TC-US15-06 / R1.3
@pytest.mark.parametrize("trigger", ["manual", "test"])
def test_7011_r1_3_manual_run_conversation_has_execution_type_manual(world, trigger):
    calls = world(trigger)

    assert calls.conversation[0]["trigger_type"] == "schedule_manual"
    assert calls.conversation[0]["extra_meta"] == {
        "schedule_id": SCHEDULE_ID, "schedule_run_id": RUN_ID, "execution_type": "schedule_manual"}


# TC-US15-01 / R1.2
def test_7011_r1_2_manual_run_starts_as_the_owner_with_the_manual_source(world):
    calls = world("manual")

    assert calls.predict[0]["trigger_source"] == "schedule_manual"  # BUG-12: Run now is told apart
    assert calls.predict[0]["is_system_user"] is False
    assert calls.predict[0]["user_id"] == OWNER


# TC-US15-07 / R1.3
def test_7011_r1_3_cron_run_keeps_the_scheduled_values(world):
    calls = world("cron")

    assert calls.conversation[0]["trigger_type"] == "scheduled"
    assert calls.conversation[0]["extra_meta"]["execution_type"] == "scheduled"
    assert calls.predict[0]["trigger_source"] == "scheduled"
