"""Messages of a deleted participant keep its name and type (#7332).

Deleting a skill, agent, toolkit, prompt or model hands its messages to the dummy participant, which
the UI shows as "Elitea". The real ``keep_former_participant_on_messages`` and
``replace_participant_by_dummy`` run against sqlite here, with the tables mirrored (JSON for the
Postgres JSONB columns, no tenant schema) and the sibling imports that touch no data faked. sqlite
gets Postgres' ``jsonb_build_object``, ``||`` and ``?``; set ELITEA_TEST_POSTGRES_URL to also run the
cases on a scratch schema of a real Postgres.

Run via:
    python tests/run_tests.py integration/test_7332_deleted_participant_keeps_author.py -v
"""
import importlib.util
import json
import os
import pathlib
import uuid
import sys
import types

import pytest
from pydantic import BaseModel
from sqlalchemy import JSON, Column, Integer, String, create_engine, event, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.sqlite.base import SQLiteCompiler
from sqlalchemy.orm import declarative_base, sessionmaker

# Other tests in the run swap ``sqlalchemy`` in sys.modules for a stub; keep the real one for ours.
create_engine('sqlite://').dispose()
_REAL_SQLALCHEMY = {k: v for k, v in sys.modules.items() if k == 'sqlalchemy' or k.startswith('sqlalchemy.')}

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
PKG = 'elitea_core_7332_deleted_participant'
Base = declarative_base()
Jsonb = JSONB().with_variant(JSON(), 'sqlite')


class Participant(Base):
    __tablename__ = 'chat_participants'
    id = Column(Integer, primary_key=True)
    entity_name = Column(String, nullable=False)
    entity_meta = Column(Jsonb, nullable=False, default=dict)
    meta = Column(JSON, nullable=False, default=dict)


class ConversationMessageGroup(Base):
    __tablename__ = 'chat_message_group'
    id = Column(Integer, primary_key=True)
    author_participant_id = Column(Integer)
    sent_to_id = Column(Integer)
    meta = Column(Jsonb, nullable=False, default=dict)


def _module(name, **attrs):
    mod = types.ModuleType(name)
    mod.__dict__.update(attrs)
    sys.modules[name] = mod
    return mod


def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _render_postgres_json_operators(original):
    def visit_custom_op_binary(compiler, element, operator, **kw):
        left, right = compiler.process(element.left, **kw), compiler.process(element.right, **kw)
        if operator.opstring == '?':
            return f"json_type({left}, '$.' || {right}) IS NOT NULL"
        if operator.opstring == '||':
            return f"json_patch({left}, {right})"
        return original(compiler, element, operator, **kw)
    return visit_custom_op_binary


def _jsonb_build_object(*parts):
    def value(part):
        return json.loads(part) if isinstance(part, str) and part.startswith('{') else part
    return json.dumps({parts[index]: value(parts[index + 1]) for index in range(0, len(parts), 2)})


def _sqlite_engine():
    engine = create_engine('sqlite://')
    event.listen(engine, 'connect', lambda connection, _: connection.create_function(
        'jsonb_build_object', -1, _jsonb_build_object, deterministic=True))
    return engine


class _Stub:
    def __getattr__(self, name):
        return _Stub()

    def __call__(self, *args, **kwargs):
        return _Stub()


@pytest.fixture(scope='module')
def participant_utils():
    saved = dict(sys.modules)
    sys.modules.update(_REAL_SQLALCHEMY)
    original_custom_op = SQLiteCompiler.visit_custom_op_binary
    SQLiteCompiler.visit_custom_op_binary = _render_postgres_json_operators(original_custom_op)
    stub = _Stub()
    _module('pylon', __path__=[])
    _module('pylon.core', __path__=[])
    _module('pylon.core.tools', log=stub)
    _module('tools', db=stub, rpc_tools=stub, context=stub, auth=stub, serialize=stub, VaultClient=stub,
            this=stub)
    for package in ('', '.models', '.models.enums', '.models.pd', '.utils'):
        _module(f'{PKG}{package}', __path__=[])
    _load(f'{PKG}.models.enums.all', PLUGIN_ROOT / 'models' / 'enums' / 'all.py')
    _module(f'{PKG}.models.conversation', Conversation=stub)
    _module(f'{PKG}.models.message_group', ConversationMessageGroup=ConversationMessageGroup)
    _module(f'{PKG}.models.participants', Participant=Participant, ParticipantMapping=stub)
    _module(f'{PKG}.models.pd.participant', ParticipantBase=BaseModel, ParticipantCreate=BaseModel,
            EntityMetaType=BaseModel, ParticipantEntityDummy=BaseModel, ParticipantEntityApplication=BaseModel,
            entity_meta_mapping={}, ParticipantEntityUser=BaseModel, ParticipantEntityToolkit=BaseModel)
    _module(f'{PKG}.models.pd.participant_settings', EntitySettingsApplication=BaseModel,
            EntitySettingsLlm=BaseModel, EntitySettingsUser=BaseModel)
    _module(f'{PKG}.utils.authors', get_authors_data=stub)
    _module(f'{PKG}.utils.sio_utils', get_chat_room=stub, SioEvents=stub)
    _module(f'{PKG}.utils.skill_participant_utils', load_skill_participant_target=stub,
            load_skill_participant_details=stub)
    yield _load(f'{PKG}.utils.participant_utils', PLUGIN_ROOT / 'utils' / 'participant_utils.py')
    SQLiteCompiler.visit_custom_op_binary = original_custom_op
    sys.modules.clear()
    sys.modules.update(saved)


DATABASES = ['sqlite', *(['postgres'] if os.environ.get('ELITEA_TEST_POSTGRES_URL') else [])]


@pytest.fixture(params=DATABASES)
def session(request, participant_utils):
    if request.param == 'sqlite':
        engine, schema = _sqlite_engine(), None
    else:
        schema = f'test_7332_{uuid.uuid4().hex[:8]}'
        engine = create_engine(os.environ['ELITEA_TEST_POSTGRES_URL'],
                               connect_args={'options': f'-csearch_path={schema}'})
        with engine.begin() as connection:
            connection.execute(text(f'CREATE SCHEMA {schema}'))
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine)()
    session.add(Participant(entity_name='dummy', entity_meta={}, meta={'name': 'EliteA'}))
    session.flush()
    yield session
    session.close()
    if schema:
        with engine.begin() as connection:
            connection.execute(text(f'DROP SCHEMA {schema} CASCADE'))
    engine.dispose()


def _delete(participant_utils, session, participant):
    participant_utils.keep_former_participant_on_messages(participant, session)
    participant_utils.replace_participant_by_dummy(2, participant.id, session)


def _participant(session, entity_name, meta):
    participant = Participant(entity_name=entity_name, entity_meta={'id': 5, 'project_id': 2}, meta=meta)
    session.add(participant)
    session.flush()
    return participant


def _message(session, author, sent_to=None, meta=None):
    message_group = ConversationMessageGroup(
        author_participant_id=author.id, sent_to_id=sent_to.id if sent_to else None, meta=meta or {},
    )
    session.add(message_group)
    session.flush()
    return message_group.id


def _messages(session, *message_ids):
    session.expire_all()
    return [session.get(ConversationMessageGroup, message_id) for message_id in message_ids]


def _dummy(session):
    return session.query(Participant).filter(Participant.entity_name == 'dummy').one()


def test_a_deleted_skill_keeps_its_name_on_the_messages_it_wrote_and_received(participant_utils, session):
    user = _participant(session, 'user', {'user_name': 'alice', 'user_avatar': None})
    skill = _participant(session, 'skill', {'name': 'test', 'icon_meta': {'url': 'http://x/icon.png'}})
    question = _message(session, user, sent_to=skill)
    answer = _message(session, skill, meta={'is_error': True, 'error': 'boom'})

    _delete(participant_utils, session, skill)

    question_group, answer_group = _messages(session, question, answer)
    assert question_group.sent_to_id == answer_group.author_participant_id == _dummy(session).id
    assert question_group.author_participant_id == user.id
    assert question_group.meta == {'imported_sent_to': {'entity_name': 'skill', 'name': 'test'}}
    assert answer_group.meta == {
        'is_error': True, 'error': 'boom', 'imported_author': {'entity_name': 'skill', 'name': 'test'},
    }


def test_a_deleted_pipeline_keeps_its_agent_type_for_the_icon(participant_utils, session):
    pipeline = _participant(session, 'application', {'name': 'flow', 'agent_type': 'pipeline'})
    answer = _message(session, pipeline)

    _delete(participant_utils, session, pipeline)

    assert _messages(session, answer)[0].meta == {
        'imported_author': {'entity_name': 'application', 'name': 'flow', 'agent_type': 'pipeline'},
    }


def test_a_removed_user_stays_no_longer_available(participant_utils, session):
    user = _participant(session, 'user', {'user_name': 'alice', 'user_avatar': None})
    question = _message(session, user)

    _delete(participant_utils, session, user)

    question_group = _messages(session, question)[0]
    assert question_group.author_participant_id == _dummy(session).id
    assert question_group.meta == {}


def test_an_imported_author_name_is_not_overwritten(participant_utils, session):
    agent = _participant(session, 'application', {'name': 'local-agent', 'agent_type': 'openai'})
    imported = {'entity_name': 'application', 'name': 'original-agent', 'avatar': 'a.png'}
    answer = _message(session, agent, meta={'imported_author': imported})

    _delete(participant_utils, session, agent)

    assert _messages(session, answer)[0].meta == {'imported_author': imported}


def test_messages_of_other_participants_are_untouched(participant_utils, session):
    user = _participant(session, 'user', {'user_name': 'alice', 'user_avatar': None})
    deleted = _participant(session, 'toolkit', {'name': 'gone'})
    kept = _participant(session, 'toolkit', {'name': 'kept'})
    question = _message(session, user, sent_to=kept)
    answer = _message(session, kept, meta={'context': {'included': True}})

    _delete(participant_utils, session, deleted)

    question_group, answer_group = _messages(session, question, answer)
    assert (question_group.sent_to_id, question_group.meta) == (kept.id, {})
    assert (answer_group.author_participant_id, answer_group.meta) == (kept.id, {'context': {'included': True}})
