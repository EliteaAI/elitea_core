"""A run delivered into a shared chat reaches each outcome through the chat hooks of its own answer group.

Contract under test: utils/schedule_runs.py on the real models, in-memory SQLite. The hooks pass the
schedule owner as author, the shared chat as conversation and the answer group of the run.
"""
import pytest
from unit._tool_spend_loader import load
from unit.utils.test_7011_schedule_pause import SENSITIVE

# Shared fixtures of the schedule writer tests: pytest injects them by name
from unit.utils.test_7011_schedule_runs_fail import _run, _schedule, runs, session  # noqa: F401
from unit.utils.test_7011_schedule_runs_outcome import sent  # noqa: F401

# ruff: noqa: F811

OWNER, MEMBER, CHAT, ANSWER_GROUP = 1, 2, 55, 10


@pytest.fixture(autouse=True)
def timing():
    """schedule_runs imports its sibling schedule_timing; load it so the file runs alone."""
    return load("utils/schedule_timing.py")


@pytest.fixture
def delivered(runs, session):
    schedule = _schedule(runs, session, created_by=OWNER)
    run = _run(runs, session, schedule)
    run.conversation_id, run.message_group_id = CHAT, ANSWER_GROUP
    session.flush()
    return run


def test_a_delivered_run_reaches_success(runs, session, sent, delivered):
    assert runs.finish_schedule_run(1, delivered.id, "success", None, CHAT, author_id=OWNER,
                                    message_group_id=ANSWER_GROUP) is True
    session.refresh(delivered)
    assert delivered.status == "success"


def test_a_delivered_run_reaches_failed_with_its_reason(runs, session, sent, delivered):
    assert runs.finish_schedule_run(1, delivered.id, "failed", "the model failed", CHAT, author_id=OWNER,
                                    message_group_id=ANSWER_GROUP) is True
    session.refresh(delivered)
    assert (delivered.status, delivered.reason) == ("failed", "the model failed")


def test_a_delivered_run_reaches_waiting_for_approval(runs, session, sent, delivered):
    runs.pause_schedule_run(1, delivered.id, SENSITIVE, CHAT, OWNER, ANSWER_GROUP)
    session.refresh(delivered)
    assert delivered.status == "waiting_for_approval"


def test_another_member_of_the_chat_cannot_end_a_delivered_run_from_a_foreign_group(runs, session, sent, delivered):
    assert runs.finish_schedule_run(1, delivered.id, "success", None, CHAT, author_id=MEMBER,
                                    message_group_id=ANSWER_GROUP + 1) is False
    session.refresh(delivered)
    assert delivered.status == "running"
