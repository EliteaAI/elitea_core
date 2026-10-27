"""The schedule service checks and stores the delivery target.

The service, the body models and the schedule table are real.
The chat tables and the membership lookup are small stand-ins, and the rest of the plugin package is stubbed.

Run via:
    python tests/run_tests.py unit/test_schedule_delivery_save.py -v
"""
import pathlib
import sys
import types
from uuid import uuid4

import pytest
from fixtures import schedule_sqlite as sqlite_db
from fixtures.helpers import load_module_with_stubs

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
PACKAGE = 'plugins.elitea_core'
PROJECT_ID = 7
OWNER_ID = 42
ADMIN_ID = 99
AGENT_ID = 1
CHAT = uuid4()
PARTICIPANT_OF_AGENT = 5
PARTICIPANT_OF_OTHER_AGENT = 6
NOT_FOUND = 'delivery.conversation_uuid not found'
NOT_ENTITY = 'entity is not a participant of the conversation'
NOT_MEMBER = 'owner is not a member of the conversation'


class Svc:
    pass


@pytest.fixture
def svc(isolated_sys_modules, monkeypatch):
    for name, module in sqlite_db.REAL_SQLALCHEMY.items():
        monkeypatch.setitem(sys.modules, name, module)
    for name in ('plugins', PACKAGE, f'{PACKAGE}.utils', f'{PACKAGE}.models', f'{PACKAGE}.models.enums',
                 f'{PACKAGE}.models.pd', 'pylon', 'pylon.core'):
        sqlite_db.package(name)
    Base, Application, ApplicationVersion = sqlite_db.install_parent_models(PACKAGE)
    sa = sqlite_db.sa
    sqlite_db.install_trace_span(types.SimpleNamespace(set_attribute=lambda *a: None))

    class Conversation(Base):
        __tablename__ = 'chat_conversations'
        __table_args__ = ({'schema': 'tenant'},)
        id = sa.Column(sa.Integer, primary_key=True)
        uuid = sa.Column(sa.Uuid, unique=True)

    class Participant(Base):
        __tablename__ = 'chat_participants'
        __table_args__ = ({'schema': 'tenant'},)
        id = sa.Column(sa.Integer, primary_key=True)
        entity_name = sa.Column(sa.String)
        entity_meta = sa.Column(sa.JSON)

    class ParticipantMapping(Base):
        __tablename__ = 'chat_participant_mapping'
        __table_args__ = ({'schema': 'tenant'},)
        id = sa.Column(sa.Integer, primary_key=True)
        conversation_id = sa.Column(sa.Integer)
        participant_id = sa.Column(sa.Integer)

    chat_models = types.ModuleType(f'{PACKAGE}.models.conversation')
    chat_models.Conversation = Conversation
    participant_models = types.ModuleType(f'{PACKAGE}.models.participants')
    participant_models.Participant, participant_models.ParticipantMapping = Participant, ParticipantMapping
    access = types.ModuleType(f'{PACKAGE}.utils.conversation_access')

    def is_conversation_participant(session, conversation_id, user_id):
        rows = session.query(Participant).join(
            ParticipantMapping, ParticipantMapping.participant_id == Participant.id
        ).filter(ParticipantMapping.conversation_id == conversation_id, Participant.entity_name == 'user').all()
        return any(str(p.entity_meta.get('id')) == str(user_id) for p in rows)

    access.is_conversation_participant = is_conversation_participant
    for module in (chat_models, participant_models, access):
        sys.modules[module.__name__] = module

    tools = sqlite_db.LenientModule('tools')
    tools.this = types.SimpleNamespace(module=types.SimpleNamespace(descriptor=types.SimpleNamespace(config={})))
    tools.config = types.SimpleNamespace(DEFAULT_MODE='default')
    tools.auth = types.SimpleNamespace(
        current_user=lambda: {'id': OWNER_ID}, resolve_permissions=lambda **kwargs: set(),
    )
    sys.modules['tools'] = tools
    folder_access = types.ModuleType(f'{PACKAGE}.utils.folder_access')
    folder_access.entity_access_error = lambda *args, **kwargs: None
    notifications = types.ModuleType(f'{PACKAGE}.utils.schedule_notifications')
    notifications.notify_schedule_event = lambda *args, **kwargs: True
    sys.modules[folder_access.__name__], sys.modules[notifications.__name__] = folder_access, notifications

    engine = sqlite_db.sqlite_engine()
    session = sqlite_db.orm.Session(engine)

    def load(relative_path, name):
        return load_module_with_stubs(PLUGIN_ROOT / relative_path, f'{PACKAGE}.{name}')

    svc = Svc()
    svc.EntitySchedule = sqlite_db.create_schedule_table(load, Base, engine)
    load('utils/constants.py', 'utils.constants')
    load('utils/schedule_timing.py', 'utils.schedule_timing')
    load('utils/schedule_access.py', 'utils.schedule_access')
    load('models/pd/index.py', 'models.pd.index')
    pd = load('models/pd/entity_schedule.py', 'models.pd.entity_schedule')
    svc.service = load('utils/schedule_service.py', 'utils.schedule_service')
    svc.session, svc.pd = session, pd
    svc.scope = lambda user_id, is_admin=False: sys.modules[f'{PACKAGE}.utils.schedule_access'].ScheduleScope(
        user_id=user_id, is_admin=is_admin, permissions=frozenset(),
    )
    sqlite_db.add_agent_and_pipeline(lambda: session, Application, ApplicationVersion)
    session.add(Conversation(id=1, uuid=CHAT))
    session.add_all([
        Participant(id=PARTICIPANT_OF_AGENT, entity_name='application', entity_meta={'id': AGENT_ID}),
        Participant(id=PARTICIPANT_OF_OTHER_AGENT, entity_name='application', entity_meta={'id': 2}),
        Participant(id=10, entity_name='user', entity_meta={'id': OWNER_ID}),
        Participant(id=11, entity_name='user', entity_meta={'id': ADMIN_ID}),
    ])
    session.add_all([ParticipantMapping(conversation_id=1, participant_id=pid) for pid in (5, 6, 10, 11)])
    session.commit()
    svc.drop_member = lambda participant_id: (
        session.query(ParticipantMapping).filter_by(participant_id=participant_id).delete(), session.commit())
    yield svc
    session.close()


def create_body(svc, **changes):
    values = {
        'name': 'Nightly', 'entity_type': 'agent', 'application_id': AGENT_ID, 'version_id': 10,
        'prompt': 'Say hi', 'cron': '0 9 * * *', 'timezone': 'Etc/UTC',
    }
    return svc.pd.ScheduleCreate(**{**values, **changes})


def to_chat(participant_id=PARTICIPANT_OF_AGENT, chat=CHAT):
    return {'mode': 'conversation', 'conversation_uuid': str(chat), 'participant_id': participant_id}


def create(svc, **changes):
    return svc.service.create_schedule(svc.session, PROJECT_ID, create_body(svc, **changes), svc.scope(OWNER_ID))


def stored_count(svc):
    return svc.session.query(svc.EntitySchedule).count()


def refusal_text(svc, call):
    with pytest.raises(svc.service.ScheduleError) as caught:
        call()
    body, status = caught.value.response
    assert status == 400
    return body['error']


def test_save_without_delivery_stores_run_history(svc):
    row = create(svc)
    assert (row.delivery_mode, row.delivery_conversation_uuid, row.delivery_participant_id) == (
        'run_history', None, None)


def test_save_with_chat_stores_chat_and_participant(svc):
    row = create(svc, delivery=to_chat())
    assert row.delivery_mode == 'conversation'
    assert row.delivery_conversation_uuid == CHAT
    assert row.delivery_participant_id == PARTICIPANT_OF_AGENT


def test_unknown_chat_is_400_and_stores_nothing(svc):
    text = refusal_text(svc, lambda: create(svc, delivery=to_chat(chat=uuid4())))
    assert text == NOT_FOUND
    assert stored_count(svc) == 0


def test_malformed_chat_uuid_is_400_not_found(svc):
    body = to_chat() | {'conversation_uuid': 'not-a-uuid'}
    assert refusal_text(svc, lambda: create(svc, delivery=body)) == NOT_FOUND


def test_entity_outside_the_chat_is_400(svc):
    svc.drop_member(PARTICIPANT_OF_AGENT)
    assert refusal_text(svc, lambda: create(svc, delivery=to_chat())) == NOT_ENTITY


def test_participant_of_another_entity_is_400(svc):
    assert refusal_text(svc, lambda: create(svc, delivery=to_chat(PARTICIPANT_OF_OTHER_AGENT))) == NOT_ENTITY


def test_owner_outside_the_chat_is_400(svc):
    svc.drop_member(10)
    assert refusal_text(svc, lambda: create(svc, delivery=to_chat())) == NOT_MEMBER


def test_edit_without_delivery_keeps_the_stored_delivery(svc):
    row = create(svc, delivery=to_chat())
    svc.service.update_schedule(
        svc.session, PROJECT_ID, row, svc.pd.ScheduleUpdate(name='Renamed'), svc.scope(OWNER_ID))
    assert row.delivery_mode == 'conversation'
    assert row.delivery_conversation_uuid == CHAT


def test_edit_to_run_history_clears_the_target(svc):
    row = create(svc, delivery=to_chat())
    svc.service.update_schedule(
        svc.session, PROJECT_ID, row, svc.pd.ScheduleUpdate(delivery={'mode': 'run_history'}), svc.scope(OWNER_ID))
    assert (row.delivery_mode, row.delivery_conversation_uuid, row.delivery_participant_id) == (
        'run_history', None, None)


def test_edit_with_unknown_chat_is_400_and_keeps_the_row(svc):
    row = create(svc)
    update = svc.pd.ScheduleUpdate(delivery=to_chat(chat=uuid4()))
    assert refusal_text(
        svc, lambda: svc.service.update_schedule(svc.session, PROJECT_ID, row, update, svc.scope(OWNER_ID))
    ) == NOT_FOUND
    assert row.delivery_mode == 'run_history'


def test_admin_edit_saves_when_the_owner_is_a_member(svc):
    row = create(svc)
    svc.drop_member(11)  # the admin is no member: only the owner counts
    svc.service.update_schedule(
        svc.session, PROJECT_ID, row, svc.pd.ScheduleUpdate(delivery=to_chat()), svc.scope(ADMIN_ID, is_admin=True))
    assert row.delivery_mode == 'conversation'


def test_admin_edit_is_400_when_only_the_admin_is_a_member(svc):
    row = create(svc)
    svc.drop_member(10)
    update = svc.pd.ScheduleUpdate(delivery=to_chat())
    assert refusal_text(
        svc, lambda: svc.service.update_schedule(svc.session, PROJECT_ID, row, update, svc.scope(ADMIN_ID, True))
    ) == NOT_MEMBER
