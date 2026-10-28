"""A run delivered into a shared chat ends or pauses through the stream hooks of its own answer group.

The shared chat meta has no run id: the run id sits in the answer group meta and the schedule owner,
not the chat author, is the author of the run. Harness of the private-chat hook test, shared by import.
"""
import pytest

# Shared fixture of the stream hook tests: pytest injects it by name
from unit.events.test_7011_stream_end_schedule_step import (  # noqa: F401
    AUTHOR_ID,
    CONVERSATION_ID,
    PROJECT_ID,
    RUN_ID,
    make_world,
)

# ruff: noqa: F811

OWNER, GROUP_ID = 12, 1
GROUP_META = {"scheduled_run": True, "schedule_id": 3, "schedule_run_id": RUN_ID}
SENSITIVE = {"hitl_interrupt": {"guardrail_type": "sensitive_tool"}}


def _delivered(make_world, **kwargs):
    return make_world({}, dict(GROUP_META), schedule_owner=OWNER, **kwargs)


def test_stream_end_finishes_a_delivered_run_as_success_for_the_schedule_owner(make_world):
    world = _delivered(make_world)
    world.end({})
    finish = ("finish", PROJECT_ID, RUN_ID, "success", None, CONVERSATION_ID, OWNER, GROUP_ID)
    assert finish in world.events
    assert world.events.index("commit") < world.events.index(finish)


def test_stream_end_with_an_error_finishes_a_delivered_run_as_failed(make_world):
    world = _delivered(make_world)
    world.end({"is_error": True, "error": "the model failed"})
    assert ("finish", PROJECT_ID, RUN_ID, "failed", "short(the model failed)", CONVERSATION_ID, OWNER,
            GROUP_ID) in world.events


def test_stream_pause_hands_the_interrupt_of_a_delivered_run_to_the_schedule_run(make_world):
    world = _delivered(make_world)
    world.pause_event(SENSITIVE)
    pause = ("pause", PROJECT_ID, RUN_ID, {"chat_project_id": PROJECT_ID, **SENSITIVE}, CONVERSATION_ID, OWNER,
             GROUP_ID)
    assert world.events == ["commit", pause]


def test_the_chat_author_is_never_the_author_of_a_delivered_run(make_world):
    world = _delivered(make_world)
    world.end({})
    assert all(AUTHOR_ID not in event for event in world.events if isinstance(event, tuple))


@pytest.mark.parametrize("group_meta", [
    {"schedule_run_id": RUN_ID, "schedule_id": 3},
    {"scheduled_run": True, "schedule_id": 3},
    {},
], ids=["no-answer-marker", "no-run-id", "plain-group"])
def test_a_group_that_is_not_a_delivered_run_skips_the_schedule_step(make_world, group_meta):
    world = make_world({}, group_meta, schedule_owner=OWNER)
    world.end({"is_error": True, "error": "x"})
    assert world.events == ["commit", ("emit", "sync")]


def test_a_delivered_run_of_a_deleted_schedule_has_no_author_and_is_left_to_the_stale_sweep(make_world):
    world = make_world({}, dict(GROUP_META), schedule_owner=None)
    world.end({})
    world.pause_event(SENSITIVE)
    assert [event for event in world.events if isinstance(event, tuple) and event[0] in ("finish", "pause")] == []


def test_a_pause_that_keeps_a_delivered_run_streaming_leaves_the_run_open(make_world):
    world = _delivered(make_world)
    world.pause_event({"hitl_interrupt": {"resume_strategy": "aggregate_child"}})
    assert world.events == ["commit"]


@pytest.mark.parametrize("forged", [
    {"schedule_run_id": RUN_ID + 1},
    {"schedule_id": 99},
    {"schedule_run_id": RUN_ID + 1, "schedule_id": 99},
], ids=["other-run-id", "other-schedule-id", "both"])
def test_a_forged_group_meta_binds_the_end_to_the_real_group_id_the_writer_checks(make_world, forged):
    world = make_world({}, {**GROUP_META, **forged}, schedule_owner=OWNER)
    world.end({})
    finish = next(event for event in world.events if isinstance(event, tuple) and event[0] == "finish")
    # The run named in the meta ends only when its stored group id equals this group, whatever the meta says
    assert finish[-1] == GROUP_ID
