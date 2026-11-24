"""Discarding a refused REST/MCP run's row leaves nothing behind (#7610).

The real ``ApiRunConversation.discard`` deletes the row through the real conversation, message group, message
item and participant models, against sqlite with foreign keys on. Only the share token model is mirrored
(models/all.py pulls in the whole plugin); the sibling utils the discard never touches are faked.

Run via:
    python tests/run_tests.py integration/test_7610_api_run_discard.py -v
"""
import importlib.util
import pathlib
import sys
import types
from contextlib import contextmanager

import pytest
from sqlalchemy import Column, ForeignKey, Integer, create_engine, event, func, select
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import DeclarativeBase, Session, relationship

# Other tests in the run swap ``sqlalchemy`` in sys.modules for a stub; keep the real one for ours.
create_engine('sqlite://').dispose()
_REAL_SQLALCHEMY = {k: v for k, v in sys.modules.items() if k == 'sqlalchemy' or k.startswith('sqlalchemy.')}

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
PKG = 'elitea_core_7610_api_run_discard'


@compiles(JSONB, 'sqlite')
def _jsonb_as_json(type_, compiler, **kw):
    return 'JSON'


def _package(monkeypatch, name, **attrs):
    module = types.ModuleType(name)
    module.__path__ = []
    vars(module).update(attrs)
    monkeypatch.setitem(sys.modules, name, module)
    return module


def _load(monkeypatch, dotted, relative):
    spec = importlib.util.spec_from_file_location(dotted, PLUGIN_ROOT / relative)
    loaded = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, dotted, loaded)
    spec.loader.exec_module(loaded)
    return loaded


@pytest.fixture
def world(monkeypatch):
    for name, module in _REAL_SQLALCHEMY.items():
        monkeypatch.setitem(sys.modules, name, module)

    class Base(DeclarativeBase):
        pass

    config = types.SimpleNamespace(POSTGRES_TENANT_SCHEMA='tenant')
    _package(monkeypatch, PKG)
    _package(
        monkeypatch, f'{PKG}.models', db=types.SimpleNamespace(Base=Base), config=config,
        CONVERSATION_TABLE_NAME='chat_conversations', PARTICIPANT_TABLE_NAME='chat_participants',
        CONVERSATION_MESSAGE_GROUP_TABLE_NAME='chat_message_group', MESSAGE_ITEMS_TABLE_NAME='chat_message_items',
    )
    _package(monkeypatch, f'{PKG}.models.enums')
    _load(monkeypatch, f'{PKG}.models.enums.all', 'models/enums/all.py')
    _package(monkeypatch, f'{PKG}.models.message_items')
    models = types.SimpleNamespace()
    for attr, dotted, relative in (
        ('folder', 'folder', 'models/folder.py'),
        ('participants', 'participants', 'models/participants.py'),
        ('conversation', 'conversation', 'models/conversation.py'),
        ('item', 'message_items.base', 'models/message_items/base.py'),
        ('text', 'message_items.text', 'models/message_items/text.py'),
        ('group', 'message_group', 'models/message_group.py'),
    ):
        setattr(models, attr, _load(monkeypatch, f'{PKG}.models.{dotted}', relative))

    class ConversationShareToken(Base):  # the mapped columns models/all.py's cascade reaches
        __tablename__ = 'chat_conversation_share_tokens'
        __table_args__ = ({'schema': 'tenant'},)
        id = Column(Integer, primary_key=True)
        conversation_id = Column(ForeignKey('tenant.chat_conversations.id', ondelete='CASCADE'), nullable=False)
        conversation = relationship('Conversation', back_populates='share_tokens')

    models.ShareToken = ConversationShareToken

    engine = create_engine('sqlite://').execution_options(schema_translate_map={'tenant': None})

    @event.listens_for(engine, 'connect')
    def _foreign_keys_on(conn, _record):
        conn.execute('PRAGMA foreign_keys=ON')

    Base.metadata.create_all(engine)

    @contextmanager
    def get_session(_project_id):
        with Session(engine) as session:
            yield session

    errors = []
    _package(monkeypatch, 'pylon.core.tools', log=types.SimpleNamespace(exception=lambda *a, **k: errors.append(a)))
    _package(monkeypatch, 'tools', db=types.SimpleNamespace(get_session=get_session), this=None)
    _package(monkeypatch, f'{PKG}.utils')
    _package(monkeypatch, f'{PKG}.utils.input_guardrail', redacted_input=None)
    _package(monkeypatch, f'{PKG}.utils.pipeline_execution', TriggerType=types.SimpleNamespace(API='api', MCP='mcp'),
             create_trigger_run_conversation=None, run_label=None)
    _package(monkeypatch, f'{PKG}.utils.predict_utils', user_input_preview=None)
    _package(monkeypatch, f'{PKG}.utils.sio_utils', SioEvents=None)
    history = _load(monkeypatch, f'{PKG}.utils.api_run_history', 'utils/api_run_history.py')
    yield types.SimpleNamespace(models=models, engine=engine, history=history, errors=errors)
    engine.dispose()


def _run_row(world, other=False):
    """A conversation shaped like create_trigger_run_conversation's: two participants, a trigger message
    with an item, and the streaming answer placeholder."""
    m = world.models
    with Session(world.engine) as session:
        user = m.participants.Participant(entity_name='user', entity_meta={'id': 3})
        agent = m.participants.Participant(entity_name='application', entity_meta={'id': 85})
        conversation = m.conversation.Conversation(name='API run: Echo', source='agent', author_id=3, meta={})
        conversation.participants.extend([user, agent])
        session.add(conversation)
        session.flush()
        trigger = m.group.ConversationMessageGroup(
            conversation=conversation, author_participant=user, sent_to_id=agent.id, meta={})
        session.add(m.text.TextMessageItem(message_group=trigger, item_type='text_message', content='hi', order_index=0))
        session.add(m.group.ConversationMessageGroup(
            conversation=conversation, author_participant_id=agent.id, is_streaming=True, reply_to=trigger, meta={}))
        session.add(m.ShareToken(conversation=conversation))
        session.commit()
        return conversation.uuid  # Postgres binds the str do_predict holds; sqlite's UUID type needs a uuid.UUID


def _count(world, table):
    with world.engine.connect() as conn:
        return conn.execute(select(func.count()).select_from(table)).scalar()


def test_a_discarded_row_leaves_no_message_groups_items_or_mappings(world):
    kept = _run_row(world)
    dropped = _run_row(world)
    m = world.models
    assert _count(world, m.group.ConversationMessageGroup.__table__) == 4

    world.history.ApiRunConversation(2, dropped, 'msg-1', 'agent').discard()

    assert world.errors == []
    with Session(world.engine) as session:
        assert [c.uuid for c in session.query(m.conversation.Conversation)] == [kept]
    assert _count(world, m.group.ConversationMessageGroup.__table__) == 2
    assert _count(world, m.item.MessageItem.__table__) == 1
    assert _count(world, m.text.TextMessageItem.__table__) == 1
    assert _count(world, m.participants.ParticipantMapping.__table__) == 2
    assert _count(world, m.ShareToken.__table__) == 1
