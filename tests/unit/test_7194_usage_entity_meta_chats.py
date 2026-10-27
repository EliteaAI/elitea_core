"""#7194: usage_entity_meta decides which conversations are user chats and finds them by name.

These run the real queries against SQLite, so the filters are checked against rows rather than
against a mocked ORM.
"""

import importlib.util
import pathlib
import sys
import types
import uuid
from contextlib import contextmanager
from unittest import mock

import pytest
from sqlalchemy import Boolean, Column, ForeignKey, Integer, String, Uuid, create_engine
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import DeclarativeBase, Session

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
PKG = 'plugins.elitea_core'
PROJECT_ID = 2
OWNER = 11
OTHER = 12


@compiles(JSONB, 'sqlite')
def _jsonb_as_json(type_, compiler, **kwargs):
    return 'JSON'


class Base(DeclarativeBase):
    pass


class FakeConversation(Base):
    __tablename__ = 'conversation'
    id = Column(Integer, primary_key=True)
    uuid = Column(Uuid(as_uuid=True))
    name = Column(String)
    is_private = Column(Boolean, default=True)
    author_id = Column(Integer)
    source = Column(String, default='elitea')
    meta = Column(JSONB, default=dict)
    created_at = Column(String)


class FakeParticipant(Base):
    __tablename__ = 'participant'
    id = Column(Integer, primary_key=True)
    entity_name = Column(String)
    meta = Column(JSONB, default=dict)


class FakeParticipantMapping(Base):
    __tablename__ = 'participant_mapping'
    id = Column(Integer, primary_key=True)
    conversation_id = Column(Integer, ForeignKey('conversation.id'))
    participant_id = Column(Integer, ForeignKey('participant.id'))
    entity_settings = Column(JSONB, default=dict)


class AnyModule(types.ModuleType):
    def __getattr__(self, name):
        if name.startswith('__'):
            raise AttributeError(name)
        return mock.MagicMock(name=f'{self.__name__}.{name}')


@pytest.fixture
def engine():
    db_engine = create_engine('sqlite://')
    Base.metadata.create_all(db_engine)
    return db_engine


@pytest.fixture
def rpc(monkeypatch, engine):
    @contextmanager
    def get_session(project_id):
        with Session(engine) as session:
            yield session

    tools = types.ModuleType('tools')
    tools.db = types.SimpleNamespace(get_session=get_session)
    monkeypatch.setitem(sys.modules, 'tools', tools)
    pylon_tools = AnyModule('pylon.core.tools')
    pylon_tools.web = types.SimpleNamespace(rpc=lambda *a, **k: (lambda func: func))
    monkeypatch.setitem(sys.modules, 'pylon.core.tools', pylon_tools)
    for name in ('pylon', 'pylon.core', f'{PKG}.models.enums.all'):
        monkeypatch.setitem(sys.modules, name, AnyModule(name))
    for name in ('plugins', PKG, f'{PKG}.rpc', f'{PKG}.models', f'{PKG}.models.enums', f'{PKG}.utils'):
        package = types.ModuleType(name)
        package.__path__ = []
        monkeypatch.setitem(sys.modules, name, package)
    models = {
        f'{PKG}.models.all': {'Application': mock.MagicMock(), 'ApplicationVersion': mock.MagicMock()},
        f'{PKG}.models.conversation': {'Conversation': FakeConversation},
        f'{PKG}.models.participants': {
            'Participant': FakeParticipant, 'ParticipantMapping': FakeParticipantMapping,
        },
        f'{PKG}.models.skill': {'Skill': mock.MagicMock(), 'SkillVersion': mock.MagicMock()},
        f'{PKG}.utils.utils': {'get_public_project_id': lambda: 1},
    }
    for module_name, attrs in models.items():
        module = types.ModuleType(module_name)
        module.__dict__.update(attrs)
        monkeypatch.setitem(sys.modules, module_name, module)
    spec = importlib.util.spec_from_file_location(
        f'{PKG}.rpc.usage_entity_meta', PLUGIN_ROOT / 'rpc' / 'usage_entity_meta.py',
    )
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, spec.name, module)
    spec.loader.exec_module(module)
    return module


def add_conversation(engine, conv_id, name='Chat', source='elitea', hidden=False, private=False, author=OWNER):
    with Session(engine) as session:
        session.add(FakeConversation(
            id=conv_id, uuid=uuid.UUID(int=conv_id), name=name, is_private=private,
            author_id=author, source=source, meta={'is_hidden': 'true'} if hidden else {},
        ))
        session.commit()
    return str(uuid.UUID(int=conv_id))


def call(rpc, **kwargs):
    return rpc.RPC().usage_entity_meta(PROJECT_ID, **kwargs)


class TestNonChatConversations:
    def test_hidden_conversations_are_not_chats(self, rpc, engine):
        add_conversation(engine, 1)
        hidden = add_conversation(engine, 2, hidden=True)

        assert call(rpc, fetch_non_chat_uuids=True)['non_chat_uuids'] == [hidden]

    @pytest.mark.parametrize('source', ['pipeline', 'support', 'system', 'agent'])
    def test_other_sources_are_not_chats(self, rpc, engine, source):
        add_conversation(engine, 1)
        other = add_conversation(engine, 2, source=source)

        assert call(rpc, fetch_non_chat_uuids=True)['non_chat_uuids'] == [other]

    def test_elitea_and_the_legacy_alita_source_are_chats(self, rpc, engine):
        add_conversation(engine, 1, source='elitea')
        add_conversation(engine, 2, source='alita')

        assert call(rpc, fetch_non_chat_uuids=True)['non_chat_uuids'] == []

    def test_a_result_within_the_cap_is_not_truncated(self, rpc, engine):
        add_conversation(engine, 1, source='pipeline')

        assert call(rpc, fetch_non_chat_uuids=True)['non_chat_truncated'] is False

    def test_reaching_the_cap_is_reported_and_keeps_the_newest(self, rpc, engine):
        for conv_id in range(1, 6):
            add_conversation(engine, conv_id, source='pipeline')

        uuids, truncated = rpc._fetch_non_chat_uuids(PROJECT_ID, limit=3)

        assert truncated is True
        assert uuids == [str(uuid.UUID(int=i)) for i in (5, 4, 3)]


class TestNameSearch:
    def search(self, rpc, text, requester_id=OWNER, **kwargs):
        result = call(rpc, conversation_name_search=text, requester_id=requester_id, **kwargs)
        return [c['uuid'] for c in result['conversations']], result['search_truncated']

    def test_matches_case_insensitively(self, rpc, engine):
        shared = add_conversation(engine, 1, name='Code Review', private=False)

        assert self.search(rpc, 'code r')[0] == [shared]

    def test_a_private_chat_is_found_only_by_its_author(self, rpc, engine):
        private = add_conversation(engine, 1, name='salary negotiation', private=True, author=OWNER)

        assert self.search(rpc, 'salary', requester_id=OWNER)[0] == [private]
        assert self.search(rpc, 'salary', requester_id=OTHER)[0] == []

    def test_without_a_requester_private_chats_are_never_matched(self, rpc, engine):
        add_conversation(engine, 1, name='salary negotiation', private=True)

        assert self.search(rpc, 'salary', requester_id=None)[0] == []

    @pytest.mark.parametrize('text', ['%', '_', '\\'])
    def test_wildcards_in_the_search_text_are_literal(self, rpc, engine, text):
        add_conversation(engine, 1, name='plain name', private=False)
        literal = add_conversation(engine, 2, name=f'has {text} inside', private=False)

        assert self.search(rpc, text)[0] == [literal]

    def test_a_capped_search_is_ordered_and_reported(self, rpc, engine):
        for conv_id in range(1, 5):
            add_conversation(engine, conv_id, name='match', private=False)

        found, truncated = self.search(rpc, 'match', conversation_search_limit=2)

        assert truncated is True
        assert found == [str(uuid.UUID(int=i)) for i in (4, 3)]

    def test_an_uncapped_search_is_not_truncated(self, rpc, engine):
        add_conversation(engine, 1, name='match', private=False)

        assert self.search(rpc, 'match')[1] is False


class TestParticipantTypes:
    def add_application_participant(self, engine, conv_id, participant_meta, settings):
        with Session(engine) as session:
            participant = FakeParticipant(entity_name='application', meta=participant_meta)
            session.add(participant)
            session.flush()
            session.add(FakeParticipantMapping(
                conversation_id=conv_id, participant_id=participant.id, entity_settings=settings,
            ))
            session.commit()

    def types_of(self, rpc, conversation_uuid):
        return call(rpc, conversation_uuids=[conversation_uuid])['conversations'][0]['participant_types']

    def test_a_pipeline_is_read_from_the_participant_meta(self, rpc, engine):
        conv = add_conversation(engine, 1)
        self.add_application_participant(engine, 1, {'agent_type': 'pipeline'}, {})

        assert self.types_of(rpc, conv) == ['pipeline']

    def test_the_mapping_settings_are_the_fallback(self, rpc, engine):
        conv = add_conversation(engine, 1)
        self.add_application_participant(engine, 1, {}, {'agent_type': 'pipeline'})

        assert self.types_of(rpc, conv) == ['pipeline']

    def test_any_other_agent_type_is_an_agent(self, rpc, engine):
        conv = add_conversation(engine, 1)
        self.add_application_participant(engine, 1, {'agent_type': 'openai'}, {})

        assert self.types_of(rpc, conv) == ['agent']
