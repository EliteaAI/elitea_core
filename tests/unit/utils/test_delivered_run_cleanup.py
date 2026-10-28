"""A delivered run never drops the checkpoints of its shared chat, and its stale end closes its answer group.

Contract under test: utils/schedule_runs.py (apply_schedule_decision, expire_waiting_runs, fail_stale_runs) on
the real schedule models, in-memory SQLite. The chat tables are small tables with the columns the writers read.
"""
import sys
import types
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import JSON, Boolean, Column, Integer, MetaData, String, Table

# Shared fixtures of the schedule writer tests: pytest injects them by name
from unit._tool_spend_loader import load
from unit.utils.test_7011_schedule_runs_fail import _run, _schedule, runs, session  # noqa: F401
from unit.utils.test_7011_schedule_runs_outcome import sent  # noqa: F401

# ruff: noqa: F811

NOW = datetime(2026, 10, 19, 12, 0, tzinfo=UTC)
OWNER, CHAT, CHAT_UUID, OWN_GROUP, OTHER_GROUP = 12, 55, "chat-uuid", 10, 11
PACKAGE = "plugins.elitea_core."


@pytest.fixture(autouse=True)
def timing():
    """schedule_runs imports its sibling schedule_timing; load it so the file runs alone."""
    return load("utils/schedule_timing.py")


@pytest.fixture
def chat(runs, session, monkeypatch):
    """The chat of the runs with two answer groups; the writers' late imports read these tables."""
    metadata = MetaData()
    conversation = Table("conversation", metadata, Column("id", Integer, primary_key=True), Column("uuid", String))
    group = Table("message_group", metadata, Column("id", Integer, primary_key=True),
                  Column("uuid", String), Column("conversation_id", Integer), Column("meta", JSON),
                  Column("is_streaming", Boolean))
    metadata.create_all(session.get_bind())
    session.execute(conversation.insert().values(id=CHAT, uuid=CHAT_UUID))
    session.execute(group.insert(), [
        {"id": OWN_GROUP, "uuid": "own-uuid", "conversation_id": CHAT, "meta": {"thread_id": "own-thread"},
         "is_streaming": True},
        {"id": OTHER_GROUP, "uuid": "other-uuid", "conversation_id": CHAT, "meta": {"thread_id": "other-thread"},
         "is_streaming": True},
    ])
    ended_groups = []
    stand_ins = {
        "models.conversation": {"Conversation": types.SimpleNamespace(id=conversation.c.id, uuid=conversation.c.uuid)},
        "models.message_group": {"ConversationMessageGroup": types.SimpleNamespace(
            **{column.name: column for column in group.c})},
        "utils.vectorstore": {"get_pgvector_connection_string": lambda project_id: "connection"},
        "utils.pipeline_execution": {"finish_trigger_run_with_error": lambda project_id, group_uuid, error: (
            ended_groups.append((project_id, group_uuid, error)))},
    }
    for dotted, names in stand_ins.items():
        module = types.ModuleType(PACKAGE + dotted)
        vars(module).update(names)
        monkeypatch.setitem(sys.modules, PACKAGE + dotted, module)
    return types.SimpleNamespace(ended_groups=ended_groups, group=group)


def _waiting(runs, session, delivered, **values):
    run = _run(runs, session, _schedule(runs, session, created_by=OWNER), status="waiting_for_approval")
    run.conversation_id = CHAT
    run.message_group_id = OWN_GROUP if delivered else None
    for name, value in values.items():
        setattr(run, name, value)
    session.flush()
    return run


def _deleted_threads(runs):
    return [set(payload["thread_ids"]) for name, payload in runs.emitted if name == "indexer_delete_checkpoint"]


def test_a_reject_of_a_delivered_run_drops_only_the_thread_of_its_own_answer(runs, session, sent, chat):
    run = _waiting(runs, session, delivered=True)

    runs.apply_schedule_decision(1, run.id, "reject")

    assert _deleted_threads(runs) == [{"own-thread"}]


def test_an_expiry_of_a_delivered_run_drops_only_the_thread_of_its_own_answer(runs, session, sent, chat):
    _waiting(runs, session, delivered=True, approval_expires_at=NOW - timedelta(minutes=1))

    assert runs.expire_waiting_runs(1, NOW) == 1

    assert _deleted_threads(runs) == [{"own-thread"}]


def test_a_reject_of_a_private_run_drops_every_thread_of_its_chat(runs, session, sent, chat):
    run = _waiting(runs, session, delivered=False)

    runs.apply_schedule_decision(1, run.id, "reject")

    assert _deleted_threads(runs) == [{CHAT_UUID, "own-thread", "other-thread"}]


def test_a_delivered_run_without_a_thread_drops_nothing(runs, session, sent, chat):
    session.execute(chat.group.update().where(chat.group.c.id == OWN_GROUP).values(meta={}))
    run = _waiting(runs, session, delivered=True)

    runs.apply_schedule_decision(1, run.id, "reject")

    assert _deleted_threads(runs) == []


def _stale(runs, session, delivered=True):
    run = _run(runs, session, _schedule(runs, session, created_by=OWNER))
    run.conversation_id = CHAT
    run.message_group_id = OWN_GROUP if delivered else None
    run.started_at = NOW - timedelta(hours=5)
    session.flush()
    return run


def test_the_stale_sweep_ends_the_answer_group_of_a_delivered_run(runs, session, sent, chat):
    run = _stale(runs, session)

    assert runs.fail_stale_runs(1, NOW, 120) == 1

    session.refresh(run)
    assert run.status == "failed"
    assert chat.ended_groups == [(1, "own-uuid", runs.STALE_REASON)]


def test_the_stale_sweep_leaves_a_group_that_no_longer_streams(runs, session, sent, chat):
    session.execute(chat.group.update().where(chat.group.c.id == OWN_GROUP).values(is_streaming=False))
    _stale(runs, session)

    assert runs.fail_stale_runs(1, NOW, 120) == 1

    assert chat.ended_groups == []


def test_the_stale_sweep_has_no_group_to_end_for_a_private_run(runs, session, sent, chat):
    _stale(runs, session, delivered=False)

    assert runs.fail_stale_runs(1, NOW, 120) == 1

    assert chat.ended_groups == []
