"""EL-7011 US-16 R2.1 to R2.3: the schedule filter of the conversation list.

The RPC and the API module are loaded from source with fake neighbours. The RPC runs on a
recording session over real SQLAlchemy columns, so the test compiles the filters it adds.
"""
import contextlib
import pathlib
import sys
import types
from unittest import mock

import pytest
from pydantic import BaseModel
from fixtures.helpers import load_module_with_stubs
from sqlalchemy import Boolean, Integer, String, select
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
PACKAGE = 'plugins.elitea_core'
PROJECT_ID = 7
CALLER_ID = 42
SCHEDULE_FILTER = (
    "chat_conversations.id IN (SELECT entity_schedule_runs.conversation_id FROM entity_schedule_runs "
    "WHERE entity_schedule_runs.schedule_id = 5)"
)


class Base(DeclarativeBase):
    pass


class Conversation(Base):
    __tablename__ = 'chat_conversations'
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String)
    source: Mapped[str] = mapped_column(String)
    is_private: Mapped[bool] = mapped_column(Boolean)
    meta: Mapped[dict] = mapped_column(JSONB)
    created_at: Mapped[int] = mapped_column(Integer)


class EntityScheduleRun(Base):
    __tablename__ = 'entity_schedule_runs'
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    schedule_id: Mapped[int] = mapped_column(Integer)
    conversation_id: Mapped[int] = mapped_column(Integer)
    trigger: Mapped[str] = mapped_column(String)
    status: Mapped[str] = mapped_column(String)
    version_id: Mapped[int] = mapped_column(Integer)
    form: Mapped[dict] = mapped_column(JSONB)


class EntitySchedule(Base):
    __tablename__ = 'entity_schedules'
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String)


class Participant(Base):
    __tablename__ = 'chat_participants'
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    entity_name: Mapped[str] = mapped_column(String)
    entity_meta: Mapped[dict] = mapped_column(JSONB)


class ParticipantMapping(Base):
    __tablename__ = 'chat_participant_mappings'
    conversation_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    participant_id: Mapped[int] = mapped_column(Integer, primary_key=True)


class FakeQuery:
    """Chainable query that remembers its target and every filter it received."""

    def __init__(self, target):
        self.target, self.clauses = target, []

    def where(self, *clauses):
        self.clauses.extend(clauses)
        return self

    filter = where

    def count(self):
        return 0

    def all(self):
        return []

    def subquery(self):
        return select(Participant.id).scalar_subquery()

    def __getattr__(self, name):  # distinct, join, order_by, limit, offset
        return lambda *args, **kwargs: self

    def sql(self):
        return ' AND '.join(
            str(c.compile(dialect=postgresql.dialect(), compile_kwargs={'literal_binds': True}))
            for c in self.clauses
        )


class FakeSession:
    def __init__(self):
        self.queries = []

    def query(self, target):
        self.queries.append(FakeQuery(target))
        return self.queries[-1]

    @property
    def base(self):
        return next(q for q in self.queries if q.target is Conversation)

    @property
    def participant_query(self):
        return next(q for q in self.queries if q.target is Participant.id)


class FuzzyModule(types.ModuleType):
    """A stub module where any imported name is a mock."""

    def __getattr__(self, name):
        if name.startswith('__'):
            raise AttributeError(name)
        return mock.MagicMock(name=name)


def _package(name):
    module = types.ModuleType(name)
    module.__path__ = []
    sys.modules[name] = module


def _fuzzy(name):
    sys.modules[name] = FuzzyModule(name)


def _install_packages():
    for name in ('plugins', PACKAGE, f'{PACKAGE}.rpc', f'{PACKAGE}.api', f'{PACKAGE}.api.v2',
                 f'{PACKAGE}.models', f'{PACKAGE}.models.pd', f'{PACKAGE}.utils', 'pylon', 'pylon.core'):
        _package(name)
    sys.modules['pylon.core.tools'] = types.SimpleNamespace(
        web=types.SimpleNamespace(rpc=lambda *args, **kwargs: (lambda func: func)),
        log=mock.MagicMock(),
    )


@pytest.fixture
def rpc_module(isolated_sys_modules):
    _install_packages()
    session = FakeSession()
    sys.modules['tools'] = types.SimpleNamespace(
        db=types.SimpleNamespace(get_session=contextlib.contextmanager(lambda project_id: (yield session))),
        config=mock.MagicMock(), auth=mock.MagicMock(), serialize=mock.MagicMock(),
        rpc_tools=mock.MagicMock(), MinioClient=mock.MagicMock(),
    )
    for name in ('models.enums.all', 'models.pd.conversation', 'models.pd.participant', 'models.pd.message',
                 'models.message_group', 'models.message_items.text', 'utils.conversation_utils',
                 'utils.participant_utils', 'utils.chat_feature_flags', 'utils.context_analytics',
                 'utils.exceptions', 'utils.model_defaults', 'utils.chat_usage',
                 'utils.context_window', 'utils.skill_run_history'):
        _fuzzy(f'{PACKAGE}.{name}')
    _package(f'{PACKAGE}.models.enums')
    _package(f'{PACKAGE}.models.message_items')
    _fuzzy(f'{PACKAGE}.models.enums.all')
    _fuzzy(f'{PACKAGE}.models.message_items.text')
    sys.modules[f'{PACKAGE}.models.conversation'] = types.SimpleNamespace(Conversation=Conversation)
    sys.modules[f'{PACKAGE}.models.folder'] = types.SimpleNamespace(ConversationFolder=mock.MagicMock())
    sys.modules[f'{PACKAGE}.models.entity_schedule'] = types.SimpleNamespace(
        EntitySchedule=EntitySchedule, EntityScheduleRun=EntityScheduleRun,
        RUN_FORM=EntityScheduleRun.__table__.c.form)
    sys.modules[f'{PACKAGE}.models.participants'] = types.SimpleNamespace(
        Participant=Participant, ParticipantMapping=ParticipantMapping)
    module = load_module_with_stubs(PLUGIN_ROOT / 'rpc' / 'chat_conversation.py',
                                    f'{PACKAGE}.rpc.chat_conversation')
    module.session = session
    return module


def _list(rpc_module, **kwargs):
    kwargs.setdefault('is_admin', False)
    result = rpc_module.RPC().list_conversations_rpc(PROJECT_ID, CALLER_ID, **kwargs)
    assert result == {'total': 0, 'rows': []}
    return rpc_module.session


def test_rpc_keeps_the_conversations_of_the_schedule_runs(rpc_module):
    session = _list(rpc_module, schedule_id=5)
    assert SCHEDULE_FILTER in ' '.join(session.base.sql().split())


def test_7011_r2_1_rpc_without_schedule_id_adds_no_schedule_filter(rpc_module):
    session = _list(rpc_module)
    assert 'schedule_id' not in session.base.sql()


@pytest.mark.parametrize('is_admin', [False, True])
def test_7011_r2_2_rpc_keeps_participant_and_private_filters(rpc_module, is_admin):
    """TC-US16-06: the participant and private filters stay next to the schedule filter."""
    session = _list(rpc_module, schedule_id=5, is_admin=is_admin)
    base = ' '.join(session.base.sql().split())
    assert SCHEDULE_FILTER in base
    assert 'chat_conversations.id IN' in base
    participant = session.participant_query.sql()
    assert ('= 42' in participant) is not is_admin
    assert any('chat_conversations.is_private = false' in q.sql() for q in session.queries)


class Args(dict):
    """The part of the Werkzeug query-args API the endpoint uses."""

    def get(self, key, default=None, type=None):
        if key not in self:
            return default
        try:
            return type(self[key]) if type else self[key]
        except ValueError:
            return default


@pytest.fixture
def api(isolated_sys_modules):
    _install_packages()
    calls = []
    rpc = types.SimpleNamespace(
        timeout=lambda seconds: rpc,
        admin_check_user_is_admin=lambda project_id, user_id: False,
        chat_list_conversations_rpc=lambda **kwargs: calls.append(kwargs) or {'total': 0, 'rows': []},
    )
    flask = types.ModuleType('flask')
    flask.request = types.SimpleNamespace(args=Args())
    sys.modules['flask'] = flask
    sys.modules['tools'] = types.SimpleNamespace(
        api_tools=types.SimpleNamespace(APIModeHandler=type('APIModeHandler', (), {}),
                                        APIBase=type('APIBase', (), {}),
                                        with_modes=lambda urls: urls,
                                        endpoint_metrics=lambda func: func),
        auth=types.SimpleNamespace(
            decorators=types.SimpleNamespace(check_api=lambda spec: (lambda func: func)),
            current_user=lambda: {'id': CALLER_ID},
        ),
        config=mock.MagicMock(), db=mock.MagicMock(), MinioClient=mock.MagicMock(),
        rpc_tools=types.SimpleNamespace(RpcMixin=lambda: types.SimpleNamespace(rpc=rpc)),
        register_openapi=lambda **meta: (lambda func: func),
        serialize=mock.MagicMock(),
    )
    for name in ('models.pd.conversation', 'utils.conversation_utils', 'utils.skill_participant_utils',
                 'utils.constants'):
        _fuzzy(f'{PACKAGE}.{name}')
    load_module_with_stubs(PLUGIN_ROOT / 'models' / 'pd' / 'schedule_list.py', f'{PACKAGE}.models.pd.schedule_list')
    module = load_module_with_stubs(PLUGIN_ROOT / 'api' / 'v2' / 'conversations.py',
                                    f'{PACKAGE}.api.v2.conversations')
    return types.SimpleNamespace(args=flask.request.args, calls=calls, module=module)


def _get(api, **args):
    api.args.update(args)
    return api.module.PromptLibAPI().get(project_id=PROJECT_ID)


def test_7011_r2_1_api_passes_the_integer_to_the_rpc(api):
    """TC-US16-05: the endpoint hands the parsed schedule_id to the list RPC."""
    _, status = _get(api, schedule_id='5')
    assert status == 200
    assert api.calls[0]['schedule_id'] == 5


def test_7011_r2_1_api_without_schedule_id_passes_none(api):
    _get(api)
    assert api.calls[0]['schedule_id'] is None


@pytest.mark.parametrize('value', ['abc', '', '1.5', ' 5', '5x', '-1', '٣'])
def test_7011_r2_3_api_rejects_a_non_integer_schedule_id(api, value):
    """TC-US16-07: a bad value answers 400 and the RPC is not called."""
    body, status = _get(api, schedule_id=value)
    assert status == 400
    assert body['field'] == 'schedule_id'
    assert body['error']
    assert api.calls == []


def _run_filter_sql(rpc_module, **kwargs):
    return ' '.join(_list(rpc_module, **kwargs).base.sql().split())


def test_run_options_share_one_semi_join_on_the_run_row_with_the_schedule(rpc_module):
    sql = _run_filter_sql(rpc_module, schedule_id=5, run_status='failed', run_trigger='cron',
                          run_version_id=9)
    assert (
        "chat_conversations.id IN (SELECT entity_schedule_runs.conversation_id FROM entity_schedule_runs "
        "WHERE entity_schedule_runs.schedule_id = 5 AND entity_schedule_runs.status = 'failed' "
        "AND entity_schedule_runs.trigger = 'cron' AND entity_schedule_runs.version_id = 9)"
    ) in sql
    assert sql.count('entity_schedule_runs.conversation_id') == 1


@pytest.mark.parametrize('trigger', ['cron', 'manual', 'test'])
def test_the_trigger_filter_compares_the_stored_trigger(rpc_module, trigger):
    sql = _run_filter_sql(rpc_module, run_trigger=trigger)
    assert f"entity_schedule_runs.trigger = '{trigger}'" in sql
    assert 'schedule_id' not in sql


def test_the_old_trigger_type_names_are_gone(rpc_module, api):
    assert not hasattr(rpc_module, 'RUN_TRIGGER_OF_TYPE')
    names = dir(sys.modules[f'{PACKAGE}.models.pd.schedule_list'])
    assert [n for n in names if n.startswith('RunTrigger')] == ['RunTrigger']


class RecordingRunQuery:
    """Query for the run records: it keeps the columns and the join kind, and returns fixed rows."""

    def __init__(self, rows):
        self.rows, self.outer = rows, None

    def outerjoin(self, *args):
        self.outer = True
        return self

    def join(self, *args):
        self.outer = False
        return self

    def filter(self, *args):
        return self

    def all(self):
        return self.rows


class RunRef(BaseModel):
    id: int
    schedule_id: int | None
    schedule_name: str | None
    status: str
    trigger: str


def test_a_run_without_a_schedule_is_listed_with_the_name_of_its_form(rpc_module):
    rpc_module.ScheduleRunRef = RunRef
    conversation = types.SimpleNamespace(id=4, meta={'schedule_run_id': 9})
    query = RecordingRunQuery([(9, 4, None, 'Draft form name', 'success', 'test')])
    session = types.SimpleNamespace(query=lambda *columns: query)
    runs = rpc_module._schedule_runs(session, [conversation])
    assert query.outer is True
    assert runs == {4: {'id': 9, 'schedule_id': None, 'schedule_name': 'Draft form name', 'status': 'success',
                        'trigger': 'test'}}


def test_a_list_without_run_options_adds_no_run_filter(rpc_module):
    assert 'entity_schedule_runs' not in _run_filter_sql(rpc_module)


@pytest.mark.parametrize(('args', 'field'), [
    ({'status': 'done'}, 'status'), ({'trigger': 'scheduled'}, 'trigger'), ({'trigger': ''}, 'trigger'),
    ({'version_id': 'x'}, 'version_id'), ({'version_id': '0'}, 'version_id'),
])
def test_an_unknown_run_option_is_400_and_the_rpc_is_not_called(api, args, field):
    body, status = _get(api, **args)
    assert (status, body['code'], body['field']) == (400, 'invalid_input', field)
    assert api.calls == []


def test_valid_run_options_reach_the_rpc(api):
    _, status = _get(api, status='running', trigger='test', version_id='12', schedule_id='5')
    assert status == 200
    assert {k: api.calls[0][k] for k in ('schedule_id', 'run_status', 'run_trigger', 'run_version_id')} == {
        'schedule_id': 5, 'run_status': 'running', 'run_trigger': 'test', 'run_version_id': 12}


def test_the_trigger_option_does_not_take_the_old_query_name(api):
    _get(api, trigger_type='test')
    assert api.calls[0]['run_trigger'] is None
