"""Skill Run History lists chat runs (#7215).

A skill added to a chat as a participant runs there, but the history matched only the Run panel
conversations (``meta.single_participant``). The real ``list_conversations_rpc`` and duration math
run real SQL against sqlite; the tables are mirrored (JSON for the Postgres JSONB columns, no tenant
schema), Postgres' ``?`` key operator is rendered as ``json_type``, and sibling imports that touch
no data are faked. Reply durations come from ``execution_time_seconds``, the branch of the duration
expression sqlite can evaluate.

Run via:
    python tests/run_tests.py integration/test_7215_skill_chat_run_history.py -v
"""
import importlib.util
import json
import pathlib
import sys
import types
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Optional

import pytest
from pydantic import BaseModel
from sqlalchemy import JSON, Column, DateTime, Integer, String, Boolean, create_engine
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.sqlite.base import SQLiteCompiler
from sqlalchemy.orm import declarative_base, sessionmaker

# Other tests in the run swap ``sqlalchemy`` in sys.modules for a stub; keep the real one for ours.
create_engine('sqlite://').dispose()
_REAL_SQLALCHEMY = {k: v for k, v in sys.modules.items() if k == 'sqlalchemy' or k.startswith('sqlalchemy.')}

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
PKG = 'elitea_core_7215_run_history'
Base = declarative_base()
Jsonb = JSONB().with_variant(JSON(), 'sqlite')

USER_ID = 7
OTHER_USER_ID = 8
SKILL_ID = 70
SKILL_PROJECT_ID = 3
PUBLIC_PROJECT_ID = 1
AGENT_ID = 70
T0 = datetime(2026, 10, 26, 15, 0, 0)
SKILL_ADDED_TO_OLD_CHAT_AT = T0 + timedelta(hours=1)


class Conversation(Base):
    __tablename__ = 'chat_conversations'
    id = Column(Integer, primary_key=True)
    uuid = Column(String)
    name = Column(String, nullable=False)
    is_private = Column(Boolean, nullable=False, default=True)
    author_id = Column(Integer, nullable=False)
    meta = Column(Jsonb, nullable=False, default=dict)
    source = Column(String, nullable=False, default='elitea')
    instructions = Column(String)
    attachment_participant_id = Column(Integer)
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime)


class Participant(Base):
    __tablename__ = 'chat_participants'
    id = Column(Integer, primary_key=True)
    entity_name = Column(String, nullable=False)
    entity_meta = Column(Jsonb, nullable=False, default=dict)


class ParticipantMapping(Base):
    __tablename__ = 'chat_participant_mapping'
    id = Column(Integer, primary_key=True)
    conversation_id = Column(Integer, nullable=False)
    participant_id = Column(Integer, nullable=False)
    entity_settings = Column(Jsonb, nullable=False, default=dict)


class ConversationMessageGroup(Base):
    __tablename__ = 'chat_message_group'
    id = Column(Integer, primary_key=True)
    conversation_id = Column(Integer, nullable=False)
    author_participant_id = Column(Integer)
    sent_to_id = Column(Integer)
    reply_to_id = Column(Integer)
    meta = Column(Jsonb, nullable=False, default=dict)
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime)


class MessageTraceStep(Base):
    __tablename__ = 'message_trace_step'
    id = Column(Integer, primary_key=True)
    message_group_id = Column(Integer, nullable=False)
    kind = Column(String, nullable=False)
    started_at = Column(DateTime)
    finished_at = Column(DateTime)


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
def rpc_module():
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
    _module(f'{PKG}.models.conversation', Conversation=Conversation)
    _module(f'{PKG}.models.participants', Participant=Participant, ParticipantMapping=ParticipantMapping)
    _module(f'{PKG}.models.message_group', ConversationMessageGroup=ConversationMessageGroup)
    _module(f'{PKG}.models.message_trace_step', MessageTraceStep=MessageTraceStep)
    _module(f'{PKG}.models.message_items.base', MessageItem=stub)
    _module(f'{PKG}.models.message_items.text', TextMessageItem=stub)
    _module(f'{PKG}.models.pd.message', MessageGroupDetail=BaseModel)
    _module(f'{PKG}.models.pd.participant', ParticipantBase=BaseModel, ParticipantCreate=BaseModel,
            ParticipantEntityUser=BaseModel)
    _module(f'{PKG}.utils.chat_constants', CONVERSATION_NAME_MAX_LENGTH=256)
    _load(f'{PKG}.models.pd.conversation', PLUGIN_ROOT / 'models' / 'pd' / 'conversation.py')
    _module(f'{PKG}.utils.authors', get_authors_data=stub)
    _module(f'{PKG}.utils.meta_guard', strip_heavy_meta_expr=stub)
    _module(f'{PKG}.utils.skill_participant_utils', describe_skill_participants=stub)
    _module(f'{PKG}.utils.participant_utils', add_participant_to_conversation=stub)
    _module(f'{PKG}.utils.chat_feature_flags', get_context_manager_feature_flag=stub)
    _module(f'{PKG}.utils.context_analytics', merge_meta_expression=stub, set_context_strategy=stub)
    _module(f'{PKG}.utils.chat_usage', strip_server_owned_meta=stub)
    _module(f'{PKG}.utils.context_window', new_chat_window_mode=stub)
    _module(f'{PKG}.utils.exceptions', InputGuardrailBlockedError=Exception, PoolSaturationError=Exception)
    _module(f'{PKG}.utils.model_defaults', creation_llm_settings=stub, chat_request_default=stub)
    _load(f'{PKG}.utils.conversation_utils', PLUGIN_ROOT / 'utils' / 'conversation_utils.py')
    _load(f'{PKG}.utils.skill_run_history', PLUGIN_ROOT / 'utils' / 'skill_run_history.py')
    module = _load(f'{PKG}.rpc.chat_conversation', PLUGIN_ROOT / 'rpc' / 'chat_conversation.py')
    module.session_holder = session_holder
    yield module
    SQLiteCompiler.visit_custom_op_binary = original_custom_op
    sys.modules.clear()
    sys.modules.update(saved)


class _Project:
    def __init__(self, session):
        self.session = session
        self.users = {}
        self.replies = 0

    def user(self, user_id):
        if user_id not in self.users:
            self.users[user_id] = self.participant('user', {'id': user_id})
        return self.users[user_id]

    def participant(self, entity_name, entity_meta):
        participant = Participant(entity_name=entity_name, entity_meta=entity_meta)
        self.session.add(participant)
        self.session.flush()
        return participant

    def conversation(self, conversation_id, participants, source='elitea', meta=None, author=USER_ID, created_at=None):
        conversation = Conversation(
            id=conversation_id, name=f'conversation {conversation_id}', author_id=author, source=source,
            meta=meta or {}, created_at=created_at or T0 + timedelta(minutes=conversation_id),
        )
        self.session.add(conversation)
        for participant in [self.user(author), *participants]:
            self.session.add(ParticipantMapping(conversation_id=conversation_id, participant_id=participant.id))
        self.session.flush()
        return conversation

    def reply(self, conversation_id, author, seconds, meta=None, at=None):
        question = ConversationMessageGroup(
            conversation_id=conversation_id, author_participant_id=self.user(USER_ID).id, meta={},
            created_at=T0,
        )
        self.session.add(question)
        self.session.flush()
        self.replies += 1
        started = at or T0 + timedelta(minutes=conversation_id, seconds=self.replies)
        self.session.add(ConversationMessageGroup(
            conversation_id=conversation_id, author_participant_id=author.id, reply_to_id=question.id,
            meta={**(meta or {}), 'execution_time_seconds': seconds}, created_at=started,
        ))
        self.session.flush()


def run_panel_meta(entity_name, entity_id, project_id, version_id):
    return {'single_participant': {
        'entity_name': entity_name,
        'entity_meta': {'id': entity_id, 'project_id': project_id},
        'entity_settings': {'version_id': version_id},
    }}


def skill_run(version_id):
    return {'skill_run': {'skill_id': SKILL_ID, 'skill_version_id': version_id, 'version_name': 'base'}}


@pytest.fixture
def project(rpc_module):
    engine = create_engine('sqlite://')
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine)()
    rpc_module.session_holder['session'] = session
    project = _Project(session)

    skill = project.participant('skill', {'id': SKILL_ID, 'project_id': SKILL_PROJECT_ID})
    catalog_twin = project.participant('skill', {'id': SKILL_ID, 'project_id': PUBLIC_PROJECT_ID})
    agent = project.participant('application', {'id': AGENT_ID, 'project_id': SKILL_PROJECT_ID})
    model = project.participant('llm', {'model_name': 'gpt-5-mini'})

    project.conversation(1, [skill], source='skill', meta=run_panel_meta('skill', SKILL_ID, SKILL_PROJECT_ID, 87))
    project.reply(1, skill, 3, skill_run(87))

    project.conversation(2, [skill, model])
    project.reply(2, model, 10)
    project.reply(2, skill, 4, skill_run(86))
    project.reply(2, skill, 5, skill_run(88))

    project.conversation(3, [skill])

    project.conversation(4, [catalog_twin])
    project.reply(4, catalog_twin, 2, skill_run(5))

    project.conversation(5, [agent], source='application',
                         meta=run_panel_meta('application', AGENT_ID, SKILL_PROJECT_ID, 40))
    project.reply(5, agent, 1)
    project.conversation(6, [agent])
    project.reply(6, agent, 1)

    project.conversation(7, [skill], author=OTHER_USER_ID)
    project.reply(7, skill, 1, skill_run(88))

    project.conversation(8, [skill], author=OTHER_USER_ID).is_private = False
    project.reply(8, skill, 1, skill_run(88))

    project.conversation(10, [skill, model], created_at=T0 - timedelta(days=7))
    project.reply(10, model, 2, at=T0 - timedelta(days=7))
    project.reply(10, skill, 3, skill_run(88), at=SKILL_ADDED_TO_OLD_CHAT_AT)
    project.reply(10, skill, 3, skill_run(88), at=SKILL_ADDED_TO_OLD_CHAT_AT + timedelta(minutes=5))

    project.conversation(9, [agent, skill], source='application',
                         meta=run_panel_meta('application', AGENT_ID, SKILL_PROJECT_ID, 41))
    project.reply(9, skill, 1, skill_run(88))

    session.commit()
    yield project
    session.close()


def list_history(rpc_module, entity_name, entity_id, entity_project_id, source, is_admin=False):
    return rpc_module.RPC().list_conversations_rpc(
        project_id=SKILL_PROJECT_ID, user_id=USER_ID, source=source, is_admin=is_admin,
        participant_id=entity_id, entity_name=entity_name, entity_project_id=entity_project_id,
    )


def rows_by_id(result):
    return {row['id']: row for row in result['rows']}


def test_skill_history_lists_its_run_panel_and_chat_runs(rpc_module, project):
    result = list_history(rpc_module, 'skill', SKILL_ID, SKILL_PROJECT_ID, 'skill')

    assert [row['id'] for row in result['rows']] == [10, 8, 2, 1]
    assert result['total'] == 4


def test_a_chat_row_carries_the_skill_share_of_the_chat(rpc_module, project):
    rows = rows_by_id(list_history(rpc_module, 'skill', SKILL_ID, SKILL_PROJECT_ID, 'skill'))

    assert rows[2]['duration'] == 9.0
    assert rows[2]['skill_version_id'] == 88
    assert rows[1]['duration'] == 3.0
    assert rows[1]['skill_version_id'] is None


def test_a_skill_reply_in_an_agent_run_panel_stays_in_the_agent_history(rpc_module, project):
    skill_rows = list_history(rpc_module, 'skill', SKILL_ID, SKILL_PROJECT_ID, 'skill')['rows']
    agent_rows = list_history(rpc_module, 'application', AGENT_ID, SKILL_PROJECT_ID, 'application')['rows']

    assert 9 not in [row['id'] for row in skill_rows]
    assert [row['id'] for row in agent_rows] == [9, 5]


def test_the_catalog_skill_with_the_same_id_has_its_own_history(rpc_module, project):
    result = list_history(rpc_module, 'skill', SKILL_ID, PUBLIC_PROJECT_ID, 'skill')

    assert [row['id'] for row in result['rows']] == [4]
    assert result['rows'][0]['skill_version_id'] == 5


def test_a_project_admin_is_not_shown_private_chats_they_cannot_open(rpc_module, project):
    result = list_history(rpc_module, 'skill', SKILL_ID, SKILL_PROJECT_ID, 'skill', is_admin=True)

    assert [row['id'] for row in result['rows']] == [10, 8, 2, 1]


def test_a_chat_run_is_dated_and_ordered_by_the_skill_first_run_in_it(rpc_module, project):
    rows = list_history(rpc_module, 'skill', SKILL_ID, SKILL_PROJECT_ID, 'skill')['rows']
    by_id = {row['id']: row for row in rows}

    assert rows[0]['id'] == 10
    assert datetime.fromisoformat(by_id[10]['run_started_at']) == SKILL_ADDED_TO_OLD_CHAT_AT
    assert by_id[1]['run_started_at'] is None


def test_oldest_first_puts_the_old_chat_after_the_runs_before_the_skill_joined(rpc_module, project):
    result = rpc_module.RPC().list_conversations_rpc(
        project_id=SKILL_PROJECT_ID, user_id=USER_ID, source='skill', sort_order='asc',
        participant_id=SKILL_ID, entity_name='skill', entity_project_id=SKILL_PROJECT_ID,
    )

    assert [row['id'] for row in result['rows']] == [1, 2, 8, 10]


def test_agent_history_still_lists_run_panel_conversations_only(rpc_module, project):
    rows = rows_by_id(list_history(rpc_module, 'application', AGENT_ID, SKILL_PROJECT_ID, 'application'))

    assert sorted(rows) == [5, 9]
    assert rows[5]['duration'] == 1.0
