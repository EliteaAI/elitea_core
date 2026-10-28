"""A chat hook ends or pauses a schedule run only for the conversation of that run.

Contract under test: utils/schedule_runs.py (finish_schedule_run, pause_schedule_run with `author_id`) on the
real models, in-memory SQLite. The run id of a hook comes from conversation meta, which a client can write.
"""
import pytest
from unit._tool_spend_loader import load

# Shared fixtures of the schedule writer tests: pytest injects them by name
from unit.utils.test_7011_schedule_runs_fail import _run, _schedule, runs, session  # noqa: F401
from unit.utils.test_7011_schedule_runs_outcome import sent  # noqa: F401
from unit.utils.test_7011_schedule_pause import SENSITIVE

# ruff: noqa: F811

OWNER, STRANGER, CONVERSATION = 1, 2, 55


@pytest.fixture(autouse=True)
def timing():
    """schedule_runs imports its sibling schedule_timing; load it so the file runs alone."""
    return load("utils/schedule_timing.py")


def _owned_run(runs, session, conversation_id=CONVERSATION):
    schedule = _schedule(runs, session, created_by=OWNER)
    run = _run(runs, session, schedule)
    run.conversation_id = conversation_id
    session.flush()
    return schedule, run


def test_the_owner_conversation_ends_its_run(runs, session, sent):
    _, run = _owned_run(runs, session)
    assert runs.finish_schedule_run(1, run.id, "success", None, CONVERSATION, author_id=OWNER) is True
    session.refresh(run)
    assert run.status == "success"


def test_the_chat_of_a_run_still_ends_it_after_the_schedule_owner_changed(runs, session, sent):
    schedule, run = _owned_run(runs, session)
    schedule.created_by = STRANGER
    session.flush()
    assert runs.finish_schedule_run(1, run.id, "success", None, CONVERSATION, author_id=OWNER) is True


def test_a_conversation_of_another_author_cannot_end_the_run(runs, session, sent):
    schedule, run = _owned_run(runs, session)
    assert runs.finish_schedule_run(1, run.id, "failed", "x", CONVERSATION + 1, author_id=STRANGER) is False
    session.refresh(run)
    session.refresh(schedule)
    assert (run.status, schedule.last_run_status, sent.notices) == ("running", None, [])


def test_another_conversation_of_the_owner_cannot_end_the_run(runs, session, sent):
    _, run = _owned_run(runs, session)
    assert runs.finish_schedule_run(1, run.id, "success", None, CONVERSATION + 1, author_id=OWNER) is False
    session.refresh(run)
    assert run.status == "running"


def test_a_run_without_a_conversation_yet_ends_for_its_owner_and_links_the_conversation(runs, session, sent):
    _, run = _owned_run(runs, session, conversation_id=None)
    assert runs.finish_schedule_run(1, run.id, "success", None, CONVERSATION, author_id=OWNER) is True
    session.refresh(run)
    assert (run.status, run.conversation_id) == ("success", CONVERSATION)


def test_a_server_caller_without_an_author_ends_any_run(runs, session, sent):
    _, run = _owned_run(runs, session)
    assert runs.finish_schedule_run(1, run.id, "failed", "stale") is True


def test_a_pause_from_the_owner_conversation_makes_the_run_wait(runs, session, sent):
    _, run = _owned_run(runs, session)
    runs.pause_schedule_run(1, run.id, SENSITIVE, CONVERSATION, OWNER)
    session.refresh(run)
    assert run.status == "waiting_for_approval"


def test_a_pause_from_another_author_leaves_the_run_running(runs, session, sent):
    _, run = _owned_run(runs, session)
    runs.pause_schedule_run(1, run.id, SENSITIVE, CONVERSATION + 1, STRANGER)
    session.refresh(run)
    assert (run.status, sent.notices) == ("running", [])


def test_a_human_step_pause_from_another_author_does_not_fail_the_run(runs, session, sent):
    _, run = _owned_run(runs, session)
    runs.pause_schedule_run(1, run.id, {"hitl_interrupt": {"node_name": "Review"}}, CONVERSATION + 1, STRANGER)
    session.refresh(run)
    assert run.status == "running"
