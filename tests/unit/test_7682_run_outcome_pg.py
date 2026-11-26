"""EliteaAI/elitea_issues#7682 (PR elitea_core#766 review): the run outcome SQL, executed on PostgreSQL.

`outcome_condition` (utils/run_outcome.py) is loaded from source and compiled to PostgreSQL SQL against mapped
stand-ins that have the same table and column names as the real models. The compiled SQL runs against temporary
copies of those tables, so a NULL conversation id, or a meta link that does not match, is caught by the test.

Set ELITEA_7011_PG_DSN to run them, for example:
    ELITEA_7011_PG_DSN=postgresql://centry:changeme@localhost:5432/run_history_test \
        /tmp/elitea_core_venv/bin/python -m pytest tests/unit/test_7682_run_outcome_pg.py
"""
import importlib.util
import os
import pathlib
import sys
import types

import pytest
from sqlalchemy import Boolean, Column, DateTime, Integer, MetaData, String, create_engine, select, text
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import declarative_base

PG_DSN = os.environ.get('ELITEA_7011_PG_DSN')
pytestmark = pytest.mark.skipif(not PG_DSN, reason='ELITEA_7011_PG_DSN is not set')

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]

# Mapped stand-ins with the real table and column names the outcome SQL reads
Base = declarative_base(metadata=MetaData())


class Conversation(Base):
    __tablename__ = 'chat_conversations'
    id = Column(Integer, primary_key=True)
    meta = Column(JSONB, nullable=False, default=dict)


class EntityScheduleRun(Base):
    __tablename__ = 'entity_schedule_runs'
    id = Column(Integer, primary_key=True)
    schedule_id = Column(Integer)
    conversation_id = Column(Integer, nullable=True)
    status = Column(String(32))


class ConversationMessageGroup(Base):
    __tablename__ = 'chat_message_group'
    id = Column(Integer, primary_key=True)
    conversation_id = Column(Integer)
    created_at = Column(DateTime(timezone=True))
    meta = Column(JSONB, nullable=False, default=dict)


class MessageTraceStep(Base):
    __tablename__ = 'chat_message_trace_step'
    id = Column(Integer, primary_key=True)
    message_group_id = Column(Integer)
    kind = Column(String(32))
    is_error = Column(Boolean)
    tool_name = Column(String(255))
    started_at = Column(DateTime(timezone=True))


def _load_outcome_module():
    """Load utils/run_outcome.py from source, with the model modules it imports pointed at the stand-ins above."""
    package = 'run_outcome_pg_under_test'
    pkg = types.ModuleType(package)
    pkg.__path__ = []
    sys.modules[package] = pkg
    for name, owner in {
        'models.conversation': Conversation,
        'models.entity_schedule': types.SimpleNamespace(EntityScheduleRun=EntityScheduleRun),
        'models.message_group': types.SimpleNamespace(ConversationMessageGroup=ConversationMessageGroup),
        'models.message_trace_step': types.SimpleNamespace(MessageTraceStep=MessageTraceStep),
    }.items():
        module = types.ModuleType(f'{package}.{name}')
        for attr, value in vars(owner).items() if not isinstance(owner, type) else [(owner.__name__, owner)]:
            setattr(module, attr, value)
        if isinstance(owner, types.SimpleNamespace):
            for attr, value in vars(owner).items():
                setattr(module, attr, value)
        sys.modules[f'{package}.{name}'] = module
    module_running = types.ModuleType(f'{package}.utils.running_conversations')
    module_running.is_live_run = lambda: Conversation.id == -1  # the live-run test is not under test here
    sys.modules[f'{package}.utils.running_conversations'] = module_running
    sys.modules[f'{package}.utils'] = types.ModuleType(f'{package}.utils')
    spec = importlib.util.spec_from_file_location(
        f'{package}.utils.run_outcome', PLUGIN_ROOT / 'utils' / 'run_outcome.py',
        submodule_search_locations=None)
    module = importlib.util.module_from_spec(spec)
    module.__package__ = f'{package}.utils'
    sys.modules[f'{package}.utils.run_outcome'] = module
    spec.loader.exec_module(module)
    return module


OUTCOME = _load_outcome_module()


def _compile(condition):
    query = select(Conversation.id).where(condition)
    return str(query.compile(dialect=postgresql.dialect(), compile_kwargs={'literal_binds': True}))


@pytest.fixture
def conn():
    engine = create_engine(PG_DSN)
    try:
        with engine.connect() as connection:
            connection.execute(text('CREATE TEMPORARY TABLE chat_conversations (id integer PRIMARY KEY, meta jsonb)'))
            connection.execute(text('CREATE TEMPORARY TABLE entity_schedule_runs (id integer PRIMARY KEY, '
                                    'schedule_id integer, conversation_id integer, status text)'))
            connection.execute(text('CREATE TEMPORARY TABLE chat_message_group (id integer PRIMARY KEY, '
                                    'conversation_id integer, created_at timestamptz, meta jsonb)'))
            connection.execute(text('CREATE TEMPORARY TABLE chat_message_trace_step (id integer PRIMARY KEY, '
                                    'message_group_id integer, kind text, is_error boolean, tool_name text, '
                                    'started_at timestamptz)'))
            yield connection
    finally:
        engine.dispose()


def _kept(conn, outcome):
    return set(conn.scalars(text(_compile(OUTCOME.outcome_condition(outcome)))).all())


# --- review fix 1: a NULL conversation id on a run record must not drop the plain runs -----------------------------

def test_a_null_conversation_run_does_not_drop_the_plain_success_runs(conn):
    """A skipped webhook run or a Test Run has no conversation: the plain chats must still read success."""
    conn.execute(text("INSERT INTO chat_conversations VALUES (1, '{}'), (2, '{}')"))
    conn.execute(text("INSERT INTO entity_schedule_runs VALUES (10, 1, NULL, 'skipped')"))

    assert _kept(conn, 'success') == {1, 2}


# --- review fix 5: a run record counts only through the conversation's meta link, as the display does --------------

def test_a_record_the_meta_does_not_name_does_not_decide_the_filter(conn):
    """The run's meta does not name the record: the row is a plain run, so its record's status must not match it."""
    conn.execute(text("INSERT INTO chat_conversations VALUES (1, '{\"schedule_run_id\": 10}'), (2, '{}')"))
    conn.execute(text("INSERT INTO entity_schedule_runs VALUES (10, 1, 2, 'failed')"))

    assert _kept(conn, 'failed') == set()
