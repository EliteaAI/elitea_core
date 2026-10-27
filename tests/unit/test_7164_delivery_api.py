"""The schedule handlers save and read the delivery target of a schedule.

The route module `api/v2/schedules.py`, the item handlers, the service, the body models and the schedule table are
real. The chat tables, the membership lookup, the permission check and the plugin package around them are small
stand-ins. Rows live in SQLite.

Run via:
    python tests/run_tests.py unit/test_7164_delivery_api.py -v
"""
import sys
import types
from pathlib import Path
from uuid import uuid4

import pytest
from fixtures import schedule_sqlite as sqlite_db
from fixtures.helpers import load_module_with_stubs

PLUGIN_ROOT = Path(__file__).resolve().parents[2]
PACKAGE = 'plugins.elitea_core'
PROJECT_ID, AGENT_ID = 7, 1
OWNER_ID, ADMIN_ID = 42, 99
CHAT = uuid4()
# The chat holds two agents and the two users: (participant id, entity name, entity id).
PARTICIPANT_OF_AGENT, PARTICIPANT_OF_OTHER_AGENT, OWNER_PARTICIPANT, ADMIN_PARTICIPANT = 5, 6, 10, 11
MEMBERS = [
    (PARTICIPANT_OF_AGENT, 'application', AGENT_ID), (PARTICIPANT_OF_OTHER_AGENT, 'application', 2),
    (OWNER_PARTICIPANT, 'user', OWNER_ID), (ADMIN_PARTICIPANT, 'user', ADMIN_ID),
]
USER_PARTICIPANT = {OWNER_ID: OWNER_PARTICIPANT, ADMIN_ID: ADMIN_PARTICIPANT}
NOT_FOUND = 'delivery.conversation_uuid not found'
NOT_MEMBER = 'owner is not a member of the conversation'
NOT_ENTITY = 'entity is not a participant of the conversation'
PREFIX = 'models.applications.schedules.'
EDITOR = {PREFIX + verb for verb in ('list', 'details', 'create', 'update', 'delete', 'run')}
NO_DELIVERY = {'mode': 'run_history', 'conversation_uuid': None, 'participant_id': None}


class Harness:
    def __init__(self):
        self.permissions = set(EDITOR)
        self.user_id = OWNER_ID
        self.body = None

    def check_api(self, spec):
        def decorator(func):
            return func
        return decorator

    def call(self, verb, body=None, schedule_id=None):
        self.body = body
        handler = self.routes.PromptLibAPI()
        handler.module = object()
        kwargs = {'project_id': PROJECT_ID}
        if schedule_id is not None:
            kwargs['schedule_id'] = schedule_id
        return getattr(handler, verb)(**kwargs)

    def row(self, schedule_id):
        with self.session_factory() as session:
            return session.get(self.EntitySchedule, schedule_id)

    def seed(self, created_by=OWNER_ID, **fields):
        return sqlite_db.seed_schedule(self.session_factory, self.EntitySchedule, created_by, **fields)

    def drop_member(self, participant_id):
        with self.session_factory() as session:
            session.query(self.ParticipantMapping).filter_by(participant_id=participant_id).delete()
            session.commit()

    def stored_count(self):
        with self.session_factory() as session:
            return session.query(self.EntitySchedule).count()


@pytest.fixture
def harness(isolated_sys_modules, monkeypatch):
    for name, module in sqlite_db.REAL_SQLALCHEMY.items():
        monkeypatch.setitem(sys.modules, name, module)
    harness = Harness()
    for name in ('plugins', PACKAGE, f'{PACKAGE}.api', f'{PACKAGE}.api.v2', f'{PACKAGE}.utils',
                 f'{PACKAGE}.models', f'{PACKAGE}.models.enums', f'{PACKAGE}.models.pd', 'pylon', 'pylon.core'):
        sqlite_db.package(name)
    Base, Application, ApplicationVersion = sqlite_db.install_parent_models(PACKAGE)
    sa = sqlite_db.sa

    flask = types.ModuleType('flask')
    flask.request = types.SimpleNamespace(
        path='', args={}, get_json=lambda *args, **kwargs: harness.body)
    sys.modules['flask'] = flask
    sqlite_db.install_trace_span(types.SimpleNamespace(set_attribute=lambda *a: None), [])

    def chat_table(class_name, table_name, **columns):
        return type(class_name, (Base,), {
            '__tablename__': table_name, '__table_args__': ({'schema': 'tenant'},),
            'id': sa.Column(sa.Integer, primary_key=True), **columns,
        })

    Conversation = chat_table('Conversation', 'chat_conversations', uuid=sa.Column(sa.Uuid, unique=True))
    Participant = chat_table(
        'Participant', 'chat_participants', entity_name=sa.Column(sa.String), entity_meta=sa.Column(sa.JSON))
    ParticipantMapping = chat_table(
        'ParticipantMapping', 'chat_participant_mapping',
        conversation_id=sa.Column(sa.Integer), participant_id=sa.Column(sa.Integer))
    harness.ParticipantMapping = ParticipantMapping
    chat_models = types.ModuleType(f'{PACKAGE}.models.conversation')
    chat_models.Conversation = Conversation
    participant_models = types.ModuleType(f'{PACKAGE}.models.participants')
    participant_models.Participant, participant_models.ParticipantMapping = Participant, ParticipantMapping
    access = types.ModuleType(f'{PACKAGE}.utils.conversation_access')
    access.is_conversation_participant = lambda session, conversation_id, user_id: session.query(
        ParticipantMapping).filter_by(
        conversation_id=conversation_id, participant_id=USER_PARTICIPANT[user_id]).count() > 0
    for module in (chat_models, participant_models, access):
        sys.modules[module.__name__] = module

    engine = sqlite_db.sqlite_engine()

    class _Session(sqlite_db.orm.Session):
        def __exit__(self, *exc):
            self.close()

    harness.session_factory = lambda: _Session(engine)
    tools = sqlite_db.LenientModule('tools')
    tools.api_tools = types.SimpleNamespace(
        APIModeHandler=type('APIModeHandler', (), {}), APIBase=type('APIBase', (), {}),
        with_modes=lambda urls: urls, endpoint_metrics=lambda func: func,
    )
    tools.auth = types.SimpleNamespace(
        decorators=types.SimpleNamespace(check_api=harness.check_api),
        current_user=lambda: {'id': harness.user_id},
        resolve_permissions=lambda **kwargs: set(harness.permissions),
    )
    tools.config = types.SimpleNamespace(
        ADMINISTRATION_MODE='administration', DEFAULT_MODE='default', POSTGRES_TENANT_SCHEMA='tenant',
    )
    tools.db = types.SimpleNamespace(get_session=lambda project_id=None: harness.session_factory(), Base=Base)
    tools.register_openapi = lambda **meta: (lambda func: func)
    tools.serialize = lambda value: value
    tools.this = types.SimpleNamespace(module=types.SimpleNamespace(descriptor=types.SimpleNamespace(config={})))
    sys.modules['tools'] = tools

    def load(relative_path, name):
        return load_module_with_stubs(PLUGIN_ROOT / relative_path, f'{PACKAGE}.{name}')

    folder_access = types.ModuleType(f'{PACKAGE}.utils.folder_access')
    folder_access.entity_access_error = lambda *args, **kwargs: None
    notifications = types.ModuleType(f'{PACKAGE}.utils.schedule_notifications')
    notifications.notify_schedule_event = lambda *args, **kwargs: True
    risk = types.ModuleType(f'{PACKAGE}.utils.schedule_risk')
    risk.scan_schedule = lambda *args: {
        'blocking': [], 'sensitive': [], 'notices': [], 'fingerprint': 'fp-1'}
    risk.scan_complete = lambda review: True
    settings = types.ModuleType(f'{PACKAGE}.utils.schedule_settings')
    settings.is_run_without_asking_allowed = lambda project_id: False
    governance = types.ModuleType(f'{PACKAGE}.utils.trigger_governance')
    governance.governance_notice_for = lambda *args: None
    for module in (folder_access, notifications, risk, settings, governance):
        sys.modules[module.__name__] = module

    harness.EntitySchedule = sqlite_db.create_schedule_table(load, Base, engine)
    load('utils/constants.py', 'utils.constants')
    load('utils/schedule_timing.py', 'utils.schedule_timing')
    load('utils/schedule_access.py', 'utils.schedule_access')
    load('models/pd/index.py', 'models.pd.index')
    load('models/pd/entity_schedule.py', 'models.pd.entity_schedule')
    load('models/pd/schedule_run.py', 'models.pd.schedule_run')
    load('models/pd/schedule_list.py', 'models.pd.schedule_list')
    load('utils/schedule_claim.py', 'utils.schedule_claim')
    load('utils/schedule_dispatch.py', 'utils.schedule_dispatch')
    load('utils/schedule_service.py', 'utils.schedule_service')
    load('utils/schedule_item_handlers.py', 'utils.schedule_item_handlers')
    sys.modules.update(sqlite_db.handler_stubs(PACKAGE, real='schedule_item_handlers'))
    harness.routes = load('api/v2/schedules.py', 'api.v2.schedules')

    sqlite_db.add_agent_and_pipeline(harness.session_factory, Application, ApplicationVersion)
    with harness.session_factory() as session:
        session.add(Conversation(id=1, uuid=CHAT))
        session.add_all(Participant(id=pid, entity_name=kind, entity_meta={'id': ref}) for pid, kind, ref in MEMBERS)
        session.add_all(ParticipantMapping(conversation_id=1, participant_id=pid) for pid, _, _ in MEMBERS)
        session.commit()
    return harness


def agent_body(**changes):
    body = {
        'name': 'Nightly', 'entity_type': 'agent', 'application_id': AGENT_ID, 'version_id': 10,
        'prompt': 'Say hi', 'cron': '0 9 * * *', 'timezone': 'Etc/UTC',
    }
    return {**body, **changes}


def to_chat(participant_id=PARTICIPANT_OF_AGENT, chat=CHAT):
    return {'mode': 'conversation', 'conversation_uuid': str(chat), 'participant_id': participant_id}


def chat_delivery():
    return {'mode': 'conversation', 'conversation_uuid': str(CHAT), 'participant_id': PARTICIPANT_OF_AGENT}


def assert_400(response, text=None, field=None):
    body, status = response
    assert status == 400
    if text:
        assert body['error'] == text
    if field:
        assert body['field'] == field


def test_create_without_delivery_stores_run_history(harness):
    body, status = harness.call('post', agent_body())
    row = harness.row(body['id'])
    assert status == 201
    assert (row.delivery_mode, row.delivery_conversation_uuid, row.delivery_participant_id) == (
        'run_history', None, None)
    assert body['delivery'] == NO_DELIVERY


def test_details_of_a_schedule_without_delivery_read_run_history(harness):
    schedule_id = harness.seed()
    body, status = harness.call('get', schedule_id=schedule_id)
    assert status == 200
    assert body['delivery'] == NO_DELIVERY


def test_create_with_chat_stores_chat_and_participant(harness):
    body, status = harness.call('post', agent_body(delivery=to_chat()))
    row = harness.row(body['id'])
    assert status == 201
    assert row.delivery_mode == 'conversation'
    assert row.delivery_conversation_uuid == CHAT
    assert row.delivery_participant_id == PARTICIPANT_OF_AGENT


def test_details_return_the_chat_delivery(harness):
    saved, _ = harness.call('post', agent_body(delivery=to_chat()))
    body, status = harness.call('get', schedule_id=saved['id'])
    assert status == 200
    assert body['delivery'] == chat_delivery()


@pytest.mark.parametrize('missing', ['conversation_uuid', 'participant_id'])
def test_chat_mode_without_a_target_field_is_400(harness, missing):
    delivery = {key: value for key, value in to_chat().items() if key != missing}
    assert_400(harness.call('post', agent_body(delivery=delivery)), field='delivery')
    assert harness.stored_count() == 0


def test_edit_without_delivery_keeps_the_stored_delivery(harness):
    saved, _ = harness.call('post', agent_body(delivery=to_chat()))
    body, status = harness.call('put', {'name': 'Renamed'}, saved['id'])
    assert status == 200
    assert body['delivery'] == chat_delivery()
    assert harness.row(saved['id']).delivery_conversation_uuid == CHAT


def test_unknown_chat_is_400_and_stores_nothing(harness):
    assert_400(harness.call('post', agent_body(delivery=to_chat(chat=uuid4()))), NOT_FOUND)
    assert harness.stored_count() == 0


def test_edit_with_unknown_chat_is_400_and_keeps_the_row(harness):
    schedule_id = harness.seed()
    assert_400(harness.call('put', {'delivery': to_chat(chat=uuid4())}, schedule_id), NOT_FOUND)
    assert harness.row(schedule_id).delivery_mode == 'run_history'


def test_participant_outside_the_chat_is_400(harness):
    harness.drop_member(PARTICIPANT_OF_AGENT)
    assert_400(harness.call('post', agent_body(delivery=to_chat())), NOT_ENTITY)
    assert harness.stored_count() == 0


def test_participant_of_another_entity_is_400(harness):
    assert_400(harness.call('post', agent_body(delivery=to_chat(PARTICIPANT_OF_OTHER_AGENT))), NOT_ENTITY)


def test_owner_outside_the_chat_is_400(harness):
    harness.drop_member(OWNER_PARTICIPANT)
    assert_400(harness.call('post', agent_body(delivery=to_chat())), NOT_MEMBER)
    assert harness.stored_count() == 0


def test_admin_edit_checks_the_owner_membership(harness):
    schedule_id = harness.seed()
    harness.user_id = ADMIN_ID
    harness.permissions = EDITOR | {PREFIX + 'admin'}
    harness.drop_member(ADMIN_PARTICIPANT)
    body, status = harness.call('put', {'delivery': to_chat()}, schedule_id)
    assert status == 200
    assert body['delivery'] == chat_delivery()


def test_admin_edit_is_400_when_only_the_admin_is_a_member(harness):
    schedule_id = harness.seed()
    harness.user_id = ADMIN_ID
    harness.permissions = EDITOR | {PREFIX + 'admin'}
    harness.drop_member(OWNER_PARTICIPANT)
    assert_400(harness.call('put', {'delivery': to_chat()}, schedule_id), NOT_MEMBER)
    assert harness.row(schedule_id).delivery_mode == 'run_history'
