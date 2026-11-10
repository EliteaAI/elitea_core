"""EL-7385: the queries that find running chats, run on SQLite over stand-in models.

Contract under test: utils/running_conversations.py (the list spinner field, and the admin cleanup of rows
stuck "streaming"). The stand-in table has the real column names, so the compiled SQL is what
production runs.
"""
import sys
import types
from contextlib import contextmanager
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
ANCIENT = NOW - timedelta(days=30)


@pytest.fixture
def running(monkeypatch, isolated_sys_modules):
    # Earlier suites leave sqlalchemy stand-ins behind
    identity.fresh_sqlalchemy()
    package = types.ModuleType("plugins.elitea_core.models")
    package.__path__ = []
    monkeypatch.setitem(sys.modules, "plugins.elitea_core.models", package)
    model_module = types.ModuleType("plugins.elitea_core.models.message_group")
    model_module.ConversationMessageGroup = ConversationMessageGroup
    monkeypatch.setitem(sys.modules, "plugins.elitea_core.models.message_group", model_module)
    return load("utils/running_conversations.py", fresh=True)


ROWS = [
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
    # conversation 4: a send that broke before its task started, two hours ago
    dict(id=10, uuid="q6", conversation_id=4),
    dict(id=11, uuid="a6", conversation_id=4, reply_to_id=10, is_streaming=True),
    # conversation 5: the same, a month ago
    dict(id=12, uuid="q7", conversation_id=5),
    dict(id=13, uuid="a7", conversation_id=5, reply_to_id=12, is_streaming=True, created_at=ANCIENT),
    # conversation 6: a task that died without a stream end a month ago; it keeps its task id
    dict(id=14, uuid="q8", conversation_id=6),
    dict(id=15, uuid="a8", conversation_id=6, reply_to_id=14, is_streaming=True, task_id="t-dead", created_at=ANCIENT),
]


@pytest.fixture
def engine():
    engine = create_engine("sqlite://")
    Base.metadata.create_all(engine)
    with Session(engine) as s:
        s.add_all(ConversationMessageGroup(**{
            "is_streaming": False, "reply_to_id": None, "task_id": None, "created_at": OLD, **r,
        }) for r in ROWS)
        s.commit()
    return engine


@pytest.fixture
def session(engine):
    with Session(engine) as s:
        yield s


def streaming(session, row_id):
    return session.get(ConversationMessageGroup, row_id).is_streaming


class TestRunningConversationIds:
    @staticmethod
    def test_only_conversations_with_a_streaming_group(running, session):
        assert running.running_conversation_ids(session, [1, 2, 3, 4, 5, 99]) == {1, 3}

    @staticmethod
    def test_only_the_listed_page_is_looked_at(running, session):
        assert running.running_conversation_ids(session, [2, 3]) == {3}

    @staticmethod
    @pytest.mark.parametrize("ids", [[], None, [None]])
    def test_an_empty_page_runs_no_query(running, ids):
        class NoQuery:
            def query(self, *_):
                raise AssertionError("queried")

        assert running.running_conversation_ids(NoQuery(), ids) == set()

    @staticmethod
    def test_a_run_that_never_started_is_not_running(running, session):
        assert running.running_conversation_ids(session, [4, 5]) == set()

    @staticmethod
    def test_a_run_older_than_any_real_run_is_not_running_even_with_a_task_id(running, session):
        assert running.running_conversation_ids(session, [6]) == set()


class TestQueryShape:
    @staticmethod
    def test_streaming_is_compared_with_equals_so_the_partial_index_is_used(running):
        # Postgres matches `is_streaming = true`, not `is_streaming IS true`, to `WHERE is_streaming`
        for clause in (running.is_live_run(), running.is_stuck_run(timedelta(hours=1))):
            sql = str(clause.compile(compile_kwargs={"literal_binds": True}))
            assert "is_streaming = true" in sql.lower() or "is_streaming = 1" in sql.lower(), sql
            assert " is true" not in sql.lower() and "is 1" not in sql.lower(), sql


class TestCloseStuck:
    @staticmethod
    def test_old_rows_end_with_a_task_id_or_without(running, session):
        # everything streaming for over an hour: two runs with a task id (2 h), never started (2 h, a month),
        # and the dead task (a month)
        assert running.close_stuck_runs(session, timedelta(hours=1)) == 5
        session.expire_all()
        for row_id in (4, 8, 11, 13, 15):
            assert streaming(session, row_id) is False

    @staticmethod
    def test_live_runs_are_left_alone(running, session):
        running.close_stuck_runs(session, timedelta(hours=1))
        session.expire_all()
        assert streaming(session, 9) is True  # just sent

    @staticmethod
    def test_a_longer_age_spares_younger_rows(running, session):
        assert running.close_stuck_runs(session, timedelta(hours=24)) == 2
        session.expire_all()
        assert streaming(session, 11) is True and streaming(session, 4) is True
        assert streaming(session, 13) is False and streaming(session, 15) is False

    @staticmethod
    def test_count_matches_what_close_would_end(running, session):
        assert running.count_stuck_runs(session, timedelta(hours=1)) == 5
        assert running.count_stuck_runs(session, timedelta(hours=24)) == 2
        # Counting writes nothing
        assert streaming(session, 11) is True and streaming(session, 15) is True

    @staticmethod
    def test_nothing_to_end_is_zero(running, session):
        assert running.close_stuck_runs(session, timedelta(days=365)) == 0


class TestCleanupHours:
    @staticmethod
    @pytest.mark.parametrize("param, hours", [
        (None, 24), ("", 24), ("project_id=all", 24),
        ("project_id=all;older_than_hours=48", 48), (" older_than_hours = 6 ;dry_run=false", 6),
    ])
    def test_hours_from_the_param(running, param, hours):
        assert running.cleanup_hours(param) == hours

    @staticmethod
    @pytest.mark.parametrize("param", ["older_than_hours=0", "older_than_hours=-1", "older_than_hours=x"])
    def test_a_bad_value_is_refused(running, param):
        assert running.cleanup_hours(param) is None


class TestCleanupRun:
    @staticmethod
    def sessions(engine, fail_for=()):
        @contextmanager
        def get_session(pid):
            if pid in fail_for:
                raise RuntimeError("schema missing")
            with Session(engine) as s:
                yield s
        return get_session

    def test_a_dry_run_counts_and_writes_nothing(self, running, engine, session):
        result = running.run_stuck_cleanup(self.sessions(engine), [1], 24, dry_run=True)
        assert result == {
            "closed": 2, "projects": {1: 2}, "failed": 0, "failed_projects": [],
            "older_than_hours": 24, "dry_run": True,
        }
        assert streaming(session, 13) is True and streaming(session, 15) is True

    def test_a_real_run_ends_the_rows_past_the_grace(self, running, engine, session):
        result = running.run_stuck_cleanup(self.sessions(engine), [1], 1, dry_run=False)
        assert result["closed"] == 5 and result["dry_run"] is False
        session.expire_all()
        assert streaming(session, 11) is False and streaming(session, 13) is False
        assert streaming(session, 15) is False
        assert streaming(session, 9) is True

    def test_a_failing_project_does_not_stop_the_others(self, running, engine):
        result = running.run_stuck_cleanup(self.sessions(engine, fail_for={7}), [7, 1], 24, dry_run=True)
        assert result["failed_projects"] == [{"project_id": 7}]
        assert result["projects"] == {1: 2}
