"""EL-7385 B1/B3: the two queries that find running chats, run on SQLite over stand-in models.

Contract under test: utils/running_conversations.py (the list spinner field, the end of rows whose run
never started) and run_snapshot.streaming_groups (which groups chat_enter_room catches a client up on).
The stand-in table has the real column names, so the compiled SQL is what production runs.
"""
import sys
import types
from datetime import datetime, timedelta

import pytest
from sqlalchemy import Boolean, DateTime, ForeignKey, Integer, String, create_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

from fixtures import identity
from unit._tool_spend_loader import load


class Base(DeclarativeBase):
    pass


class ConversationMessageGroup(Base):
    __tablename__ = 'chat_message_group'
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    uuid: Mapped[str] = mapped_column(String)
    conversation_id: Mapped[int] = mapped_column(Integer)
    reply_to_id: Mapped[int] = mapped_column(ForeignKey('chat_message_group.id'), nullable=True)
    is_streaming: Mapped[bool] = mapped_column(Boolean, default=False)
    task_id: Mapped[str] = mapped_column(String, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime)

NOW = datetime.utcnow()
OLD = NOW - timedelta(hours=2)


@pytest.fixture
def modules(monkeypatch, isolated_sys_modules):
    # Earlier suites leave sqlalchemy stand-ins behind; the helper lazily imports sqlalchemy.orm
    identity.fresh_sqlalchemy()
    for name in ("plugins.elitea_core.models",):
        package = types.ModuleType(name)
        package.__path__ = []
        monkeypatch.setitem(sys.modules, name, package)
    model_module = types.ModuleType("plugins.elitea_core.models.message_group")
    model_module.ConversationMessageGroup = ConversationMessageGroup
    monkeypatch.setitem(sys.modules, "plugins.elitea_core.models.message_group", model_module)
    load("utils/chat_constants.py", fresh=True)
    return load("utils/running_conversations.py", fresh=True), load("utils/run_snapshot.py", fresh=True)


@pytest.fixture
def session():
    engine = create_engine("sqlite://")
    Base.metadata.create_all(engine)
    with Session(engine) as s:
        rows = [
            # conversation 1: answered, then a second question still generating
            dict(id=1, uuid="q1", conversation_id=1), dict(id=2, uuid="a1", conversation_id=1, reply_to_id=1),
            dict(id=3, uuid="q2", conversation_id=1),
            dict(id=4, uuid="a2", conversation_id=1, reply_to_id=3, is_streaming=True, task_id="t-1"),
            # conversation 2: finished
            dict(id=5, uuid="q3", conversation_id=2), dict(id=6, uuid="a3", conversation_id=2, reply_to_id=5),
            # conversation 3: two runs at once (two participants answering)
            dict(id=7, uuid="q4", conversation_id=3),
            dict(id=8, uuid="a4", conversation_id=3, reply_to_id=7, is_streaming=True, task_id="t-2"),
            # just sent: no task id yet, still starting
            dict(id=9, uuid="a5", conversation_id=3, reply_to_id=7, is_streaming=True, created_at=NOW),
            # conversation 4: a send that broke before its task started, months ago
            dict(id=10, uuid="q6", conversation_id=4),
            dict(id=11, uuid="a6", conversation_id=4, reply_to_id=10, is_streaming=True),
        ]
        s.add_all(ConversationMessageGroup(**{
            "is_streaming": False, "reply_to_id": None, "task_id": None, "created_at": OLD, **r,
        }) for r in rows)
        s.commit()
        yield s


class TestRunningConversationIds:
    @staticmethod
    def test_only_conversations_with_a_streaming_group(modules, session):
        running, _ = modules
        assert running.running_conversation_ids(session, [1, 2, 3, 4, 99]) == {1, 3}

    @staticmethod
    def test_only_the_listed_page_is_looked_at(modules, session):
        running, _ = modules
        assert running.running_conversation_ids(session, [2, 3]) == {3}

    @staticmethod
    @pytest.mark.parametrize("ids", [[], None, [None]])
    def test_an_empty_page_runs_no_query(modules, ids):
        running, _ = modules

        class NoQuery:
            def query(self, *_):
                raise AssertionError("queried")

        assert running.running_conversation_ids(NoQuery(), ids) == set()


class TestNeverStarted:
    @staticmethod
    def test_a_run_that_never_started_is_not_running(modules, session):
        running, snapshot = modules
        assert running.running_conversation_ids(session, [4]) == set()
        assert snapshot.streaming_groups(session, 4) == []

    @staticmethod
    def test_it_is_ended_and_live_runs_are_left_alone(modules, session):
        running, _ = modules
        ended = running.close_never_started_runs(session, 4)
        assert [row.uuid for row in ended] == ["a6"]
        assert session.get(ConversationMessageGroup, 11).is_streaming is False
        # A run with a task, and one still inside its start grace, keep streaming
        assert running.close_never_started_runs(session, 3) == []
        assert running.close_never_started_runs(session, 1) == []
        assert session.get(ConversationMessageGroup, 9).is_streaming is True
        assert session.get(ConversationMessageGroup, 4).is_streaming is True


class TestStreamingGroups:
    @staticmethod
    def test_streaming_answers_with_their_question(modules, session):
        _, snapshot = modules
        assert snapshot.streaming_groups(session, 1) == [("a2", "q2")]
        assert sorted(snapshot.streaming_groups(session, 3)) == [("a4", "q4"), ("a5", "q4")]

    @staticmethod
    def test_a_finished_conversation_has_none(modules, session):
        _, snapshot = modules
        assert snapshot.streaming_groups(session, 2) == []
