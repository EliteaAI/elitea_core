"""Skill Run History lists runs started through the skill predict API (#7298).

An API run was never stored, so the skill's Run History had nothing to list. The real
``open_skill_api_run_conversation`` writes the run conversation and the real ``list_conversations_rpc``
reads it back, both against sqlite: the tables are mirrored (JSON for the Postgres JSONB columns, no
tenant schema), Postgres' ``?`` key operator is rendered as ``json_type``, and sibling imports that
touch no data are faked.

Run via:
    python tests/run_tests.py integration/test_7298_skill_api_run_history.py -v
"""
import importlib.util
import json
import pathlib
import sys
import types
import uuid
from contextlib import contextmanager
from datetime import datetime

import pytest
from pydantic import BaseModel
from sqlalchemy import JSON, Boolean, Column, DateTime, ForeignKey, Integer, String, create_engine
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.sqlite.base import SQLiteCompiler
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

# Other tests in the run swap ``sqlalchemy`` in sys.modules for a stub; keep the real one for ours.
create_engine('sqlite://').dispose()
_REAL_SQLALCHEMY = {k: v for k, v in sys.modules.items() if k == 'sqlalchemy' or k.startswith('sqlalchemy.')}

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
PKG = 'elitea_core_7298_api_run_history'
Base = declarative_base()
Jsonb = JSONB().with_variant(JSON(), 'sqlite')

USER_ID = 7
SKILL_ID = 70
VERSION_ID = 87
CALLER_PROJECT_ID = 3
PUBLIC_PROJECT_ID = 1


class ParticipantMapping(Base):
    __tablename__ = 'chat_participant_mapping'
    id = Column(Integer, primary_key=True)
    conversation_id = Column(Integer, ForeignKey('chat_conversations.id'), nullable=False)
    participant_id = Column(Integer, ForeignKey('chat_participants.id'), nullable=False)
    entity_settings = Column(Jsonb, nullable=False, default=dict)


class Participant(Base):
    __tablename__ = 'chat_participants'
    id = Column(Integer, primary_key=True)
    entity_name = Column(String, nullable=False)
    entity_meta = Column(Jsonb, nullable=False, default=dict)


class Conversation(Base):
    __tablename__ = 'chat_conversations'
    id = Column(Integer, primary_key=True)
    uuid = Column(String, default=lambda: str(uuid.uuid4()))
    name = Column(String, nullable=False)
    is_private = Column(Boolean, nullable=False, default=True)
    author_id = Column(Integer, nullable=False)
    meta = Column(Jsonb, nullable=False, default=dict)
    source = Column(String, nullable=False, default='elitea')
    instructions = Column(String)
    attachment_participant_id = Column(Integer)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime)
    participants = relationship(Participant, secondary=ParticipantMapping.__table__)


class ConversationMessageGroup(Base):
    __tablename__ = 'chat_message_group'
    id = Column(Integer, primary_key=True)
    uuid = Column(String, default=lambda: str(uuid.uuid4()))
    conversation_id = Column(Integer, ForeignKey('chat_conversations.id'), nullable=False)
    author_participant_id = Column(Integer, ForeignKey('chat_participants.id'))
    sent_to_id = Column(Integer)
    reply_to_id = Column(Integer)
    is_streaming = Column(Boolean, nullable=False, default=False)
    meta = Column(Jsonb, nullable=False, default=dict)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime)
    conversation = relationship(Conversation)
    author_participant = relationship(Participant)


class TextMessageItem(Base):
    __tablename__ = 'chat_messages_text'
    id = Column(Integer, primary_key=True)
    message_group_id = Column(Integer, ForeignKey('chat_message_group.id'), nullable=False)
    item_type = Column(String, nullable=False)
    content = Column(String)
    order_index = Column(Integer)
    message_group = relationship(ConversationMessageGroup)
    __mapper_args__ = {'polymorphic_on': item_type, 'polymorphic_identity': 'text_message'}


class MessageTraceStep(Base):
    __tablename__ = 'message_trace_step'
    id = Column(Integer, primary_key=True)
    message_group_id = Column(Integer, nullable=False)
    kind = Column(String, nullable=False)
    started_at = Column(DateTime)
    finished_at = Column(DateTime)


class ParticipantEntityUser(BaseModel):
    id: int


class ParticipantEntitySkill(BaseModel):
    id: int
    project_id: int


def get_or_create_one(session, entity_name, entity_meta, **kwargs):
    meta = entity_meta.model_dump()
    existing = next(
        (p for p in session.query(Participant).filter(Participant.entity_name == entity_name.value)
         if p.entity_meta == meta),
        None,
    )
    if existing is not None:
        return existing, None
    participant = Participant(entity_name=entity_name.value, entity_meta=meta)
    session.add(participant)
    session.commit()
    return participant, None


SECRET = 'sk-live-0123456789'


def redacted_input(user_input):
    if isinstance(user_input, list):
        return [{**b, 'text': b['text'].replace(SECRET, '[REDACTED]')} for b in user_input]
    return user_input.replace(SECRET, '[REDACTED]')


def user_input_preview(source, limit=100):
    text = source if isinstance(source, str) else ' '.join(b['text'] for b in source if b.get('type') == 'text')
    return text[:limit] if text else None


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


def _serialize(obj):
    if hasattr(obj, '__table__'):
        return {column.name: getattr(obj, column.name) for column in obj.__table__.columns}
    return json.loads(json.dumps(obj, default=str))


class _Stub:
    def __getattr__(self, name):
        return _Stub()

    def __call__(self, *args, **kwargs):
        return _Stub()


class _Web:
    @staticmethod
    def rpc(*_names):
        return lambda func: func


def _render_postgres_has_key(original):
    def visit_custom_op_binary(compiler, element, operator, **kw):
        if operator.opstring == '?':
            return (
                f"json_type({compiler.process(element.left, **kw)}, "
                f"'$.' || {compiler.process(element.right, **kw)}) IS NOT NULL"
            )
        return original(compiler, element, operator, **kw)
    return visit_custom_op_binary


@pytest.fixture(scope='module')
def modules():
    saved = dict(sys.modules)
    sys.modules.update(_REAL_SQLALCHEMY)
    original_custom_op = SQLiteCompiler.visit_custom_op_binary
    SQLiteCompiler.visit_custom_op_binary = _render_postgres_has_key(original_custom_op)
    stub = _Stub()
    session_holder = {}

    @contextmanager
    def get_session(_project_id):
        yield session_holder['session']

    _module('pylon', __path__=[])
    _module('pylon.core', __path__=[])
    _module('pylon.core.tools', web=_Web, log=stub)
    _module('tools', db=types.SimpleNamespace(get_session=get_session), config=stub, auth=stub,
            serialize=_serialize, rpc_tools=stub, MinioClient=stub, this=stub)

    for package in ('', '.models', '.models.enums', '.models.pd', '.models.message_items', '.utils', '.rpc'):
        _module(f'{PKG}{package}', __path__=[])
    _load(f'{PKG}.models.enums.all', PLUGIN_ROOT / 'models' / 'enums' / 'all.py')
    _module(f'{PKG}.models.all', Application=stub, ApplicationVersion=stub)
    _module(f'{PKG}.models.conversation', Conversation=Conversation)
    _module(f'{PKG}.models.participants', Participant=Participant, ParticipantMapping=ParticipantMapping)
    _module(f'{PKG}.models.message_group', ConversationMessageGroup=ConversationMessageGroup)
    _module(f'{PKG}.models.message_trace_step', MessageTraceStep=MessageTraceStep)
    _module(f'{PKG}.models.entity_schedule', EntitySchedule=stub, EntityScheduleRun=stub)
    _module(f'{PKG}.models.folder', ConversationFolder=stub)
    _module(f'{PKG}.models.message_items.base', MessageItem=stub)
    _module(f'{PKG}.models.message_items.text', TextMessageItem=TextMessageItem)
    _module(f'{PKG}.models.pd.message', MessageGroupDetail=BaseModel)
    _module(f'{PKG}.models.pd.participant', ParticipantBase=BaseModel, ParticipantCreate=BaseModel,
            ParticipantEntityUser=ParticipantEntityUser, ParticipantEntitySkill=ParticipantEntitySkill,
            ParticipantEntityApplication=BaseModel)
    _module(f'{PKG}.utils.participant_utils', get_or_create_one=get_or_create_one,
            add_participant_to_conversation=stub)
    _module(f'{PKG}.utils.predict_utils', user_input_preview=user_input_preview)
    _module(f'{PKG}.utils.input_guardrail', redacted_input=redacted_input)
    _load(f'{PKG}.utils.mcp_versioning', PLUGIN_ROOT / 'utils' / 'mcp_versioning.py')
    _module(f'{PKG}.utils.sio_utils', SioEvents=stub)
    _module(f'{PKG}.utils.chat_constants', CONVERSATION_NAME_MAX_LENGTH=256)
    _load(f'{PKG}.utils.pipeline_execution', PLUGIN_ROOT / 'utils' / 'pipeline_execution.py')
    _load(f'{PKG}.models.pd.conversation', PLUGIN_ROOT / 'models' / 'pd' / 'conversation.py')
    _module(f'{PKG}.utils.authors', get_authors_data=stub)
    _module(f'{PKG}.utils.meta_guard', strip_heavy_meta_expr=stub)
    _module(f'{PKG}.utils.skill_participant_utils', describe_skill_participants=stub)
    _module(f'{PKG}.utils.chat_feature_flags', get_context_manager_feature_flag=stub)
    _module(f'{PKG}.utils.context_analytics', merge_meta_expression=stub, set_context_strategy=stub)
    _module(f'{PKG}.utils.chat_usage', strip_server_owned_meta=stub)
    _module(f'{PKG}.utils.context_window', new_chat_window_mode=stub)
    _module(f'{PKG}.utils.exceptions', InputGuardrailBlockedError=Exception, PoolSaturationError=Exception)
    _module(f'{PKG}.utils.model_defaults', creation_llm_settings=stub, chat_request_default=stub)
    _load(f'{PKG}.utils.conversation_utils', PLUGIN_ROOT / 'utils' / 'conversation_utils.py')
    _load(f'{PKG}.utils.skill_run_history', PLUGIN_ROOT / 'utils' / 'skill_run_history.py')
    history = _load(f'{PKG}.utils.skill_api_run_history', PLUGIN_ROOT / 'utils' / 'skill_api_run_history.py')
    rpc = _load(f'{PKG}.rpc.chat_conversation', PLUGIN_ROOT / 'rpc' / 'chat_conversation.py')
    yield types.SimpleNamespace(history=history, rpc=rpc, session_holder=session_holder)
    SQLiteCompiler.visit_custom_op_binary = original_custom_op
    sys.modules.clear()
    sys.modules.update(saved)


@pytest.fixture
def session(modules):
    engine = create_engine('sqlite://')
    Base.metadata.create_all(engine)
    db_session = sessionmaker(bind=engine)()
    modules.session_holder['session'] = db_session
    yield db_session
    db_session.close()


def target(version_id=VERSION_ID):
    return types.SimpleNamespace(skill_id=SKILL_ID, skill_name='Reviewer', version_id=version_id)


def open_run(modules, skill_project_id=CALLER_PROJECT_ID, user_input='Review this paragraph.'):
    return modules.history.open_skill_api_run_conversation(
        caller_project_id=CALLER_PROJECT_ID, skill_project_id=skill_project_id, target=target(),
        user_id=USER_ID, user_input=user_input,
    )


def skill_history(modules, skill_project_id=CALLER_PROJECT_ID):
    return modules.rpc.RPC().list_conversations_rpc(
        project_id=CALLER_PROJECT_ID, user_id=USER_ID, source='skill', participant_id=SKILL_ID,
        entity_name='skill', entity_project_id=skill_project_id,
    )['rows']


def test_an_api_run_is_listed_in_the_skill_run_history_as_an_api_run(modules, session):
    run = open_run(modules)

    rows = skill_history(modules)

    assert [row['uuid'] for row in rows] == [run.conversation_uuid]
    assert rows[0]['execution_type'] == 'api'
    assert rows[0]['meta']['single_participant']['entity_settings'] == {'version_id': VERSION_ID}


def test_the_run_holds_the_input_and_an_open_answer_from_the_skill(modules, session):
    run = open_run(modules)

    question, answer = session.query(ConversationMessageGroup).order_by(ConversationMessageGroup.id).all()
    skill = session.query(Participant).filter(Participant.entity_name == 'skill').one()
    assert session.query(TextMessageItem).one().content == 'Review this paragraph.'
    assert question.sent_to_id == skill.id and question.author_participant.entity_meta == {'id': USER_ID}
    assert (answer.uuid, answer.author_participant_id, answer.reply_to_id) == (run.response_message_id, skill.id, question.id)
    assert answer.is_streaming


def test_the_skill_participant_is_pinned_to_the_run_version(modules, session):
    run = open_run(modules)

    conversation = session.query(Conversation).filter(Conversation.uuid == run.conversation_uuid).one()
    pins = {m.participant_id: m.entity_settings for m in session.query(ParticipantMapping)
            .filter(ParticipantMapping.conversation_id == conversation.id)}
    skill = session.query(Participant).filter(Participant.entity_name == 'skill').one()
    assert pins[skill.id] == {'version_id': VERSION_ID}


def test_content_block_input_is_stored_as_its_text(modules, session):
    open_run(modules, user_input=[{'type': 'text', 'text': 'Review'}, {'type': 'text', 'text': 'this.'}])

    assert session.query(TextMessageItem).one().content == 'Review this.'


def test_a_catalog_skill_run_is_listed_under_the_catalog_skill_only(modules, session):
    open_run(modules, skill_project_id=PUBLIC_PROJECT_ID)

    assert len(skill_history(modules, skill_project_id=PUBLIC_PROJECT_ID)) == 1
    assert skill_history(modules) == []


def test_a_discarded_run_is_gone_from_the_history(modules, session):
    run = open_run(modules)

    run.discard()

    assert skill_history(modules) == []


@pytest.mark.parametrize('user_input', [
    f'Use key {SECRET} here.',
    [{'type': 'text', 'text': f'Use key {SECRET} here.'}],
])
def test_the_stored_input_is_redacted(modules, session, user_input):
    open_run(modules, user_input=user_input)

    assert session.query(TextMessageItem).one().content == 'Use key [REDACTED] here.'


@pytest.mark.parametrize('environ, execution_type', [
    ({}, 'api'),
    ({'elitea.internal_mcp_request': True}, 'mcp'),
])
def test_a_run_over_mcp_is_listed_as_an_mcp_run(modules, session, environ, execution_type):
    modules.history.run_conversation_opener(environ)(
        caller_project_id=CALLER_PROJECT_ID, skill_project_id=CALLER_PROJECT_ID, target=target(),
        user_id=USER_ID, user_input='hi',
    )

    assert [row['execution_type'] for row in skill_history(modules)] == [execution_type]


def test_a_long_input_is_stored_whole(modules, session):
    long_input = ' '.join(['Review this paragraph.'] * 20)

    open_run(modules, user_input=long_input)

    assert session.query(TextMessageItem).one().content == long_input
