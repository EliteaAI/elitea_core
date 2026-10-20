"""EL-7011 US-07: handler tests of the schedule resource (owner, scope, run input, notice).

Covers R1.3-R1.6, R2.2, R2.5, R3.1, R4.2-R4.4 and R5.1, R5.2, R5.4-R5.7. The handler module, the
service, the body models and the schedule table are real. The plugin package around them is
stubbed: `tools`, folder rights, notices, the trace span and the parent tables. Rows live in
SQLite, so the own-only filter is checked against real queries.

Run via:
    python tests/run_tests.py unit/test_7011_schedule_crud_api.py -v
"""
import json
import pathlib
import sys
import types

import pytest
from fixtures import schedule_sqlite as sqlite_db
from fixtures.helpers import load_module_with_stubs

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
PACKAGE = 'plugins.elitea_core'
PROJECT_ID = 7
OWNER_ID = 42
OTHER_ID = 43
ADMIN_ID = 99
PERMISSION_PREFIX = 'models.applications.schedules.'
EDITOR = {PERMISSION_PREFIX + verb for verb in ('list', 'details', 'create', 'update', 'delete', 'run')}
ADMIN = EDITOR | {PERMISSION_PREFIX + 'admin'}
NOT_FOUND = {'error': 'Schedule not found'}


class FakeSpan:
    def __init__(self):
        self.attributes = {}

    def set_attribute(self, key, value):
        self.attributes[key] = value


class Harness:
    def __init__(self):
        self.permissions = set(EDITOR)
        self.user_id = OWNER_ID
        self.body = None
        self.span = FakeSpan()
        self.notices = []
        self.notice_extras = []
        self.notice_error = None
        self.access_error = None
        self.risk_warnings = []
        self.run_without_asking_allowed = False
        self.scans = []
        self.path = ''

    def request_as(self, user_id, permissions):
        self.user_id, self.permissions = user_id, set(permissions)

    def check_api(self, spec):
        def decorator(func):
            def wrapper(*args, **kwargs):
                if not set(spec['permissions']) & self.permissions:
                    return {'error': 'access_denied'}, 403
                return func(*args, **kwargs)
            wrapper.__name__ = func.__name__
            return wrapper
        return decorator

    def notify(self, project_id, owner_id, event_type, schedule, run=None, extra=None):
        if self.notice_error:
            raise self.notice_error
        self.notices.append((owner_id, str(event_type), schedule.id))
        self.notice_extras.append(extra)
        return True

    def row(self, schedule_id):
        with self.session_factory() as session:
            return session.get(self.EntitySchedule, schedule_id)

    def call(self, verb, body=None, schedule_id=None, suffix=''):
        """Call a handler verb; `suffix` is the URL tail the router matched, such as `/run`."""
        self.body = body
        self.path = f'/schedule/prompt_lib/{PROJECT_ID}/{schedule_id or ""}{suffix}'
        handler = self.module.PromptLibAPI()
        handler.module = object()
        kwargs = {'project_id': PROJECT_ID}
        if schedule_id is not None:
            kwargs['schedule_id'] = schedule_id
        return getattr(handler, verb)(**kwargs)

    def seed(self, created_by=OWNER_ID, **fields):
        return sqlite_db.seed_schedule(self.session_factory, self.EntitySchedule, created_by, **fields)


@pytest.fixture
def harness(isolated_sys_modules, monkeypatch):
    for name, module in sqlite_db.REAL_SQLALCHEMY.items():
        monkeypatch.setitem(sys.modules, name, module)
    harness = Harness()
    for name in ('plugins', PACKAGE, f'{PACKAGE}.api', f'{PACKAGE}.api.v2', f'{PACKAGE}.utils',
                 f'{PACKAGE}.models', f'{PACKAGE}.models.enums', f'{PACKAGE}.models.pd', 'pylon', 'pylon.core'):
        sqlite_db.package(name)

    Base, Application, ApplicationVersion = sqlite_db.install_parent_models(PACKAGE)
    flask = types.ModuleType('flask')
    class _Request:
        path = property(lambda self: harness.path)

        def get_json(self, *args, **kwargs):
            return harness.body

    flask.request = _Request()
    sys.modules['flask'] = flask
    sqlite_db.install_trace_span(harness.span)

    engine = sqlite_db.sqlite_engine()

    class _Session(sqlite_db.orm.Session):
        def __exit__(self, *exc):
            self.close()

    harness.session_factory = lambda: _Session(engine)
    tools = sqlite_db.LenientModule('tools')
    tools.api_tools = types.SimpleNamespace(
        APIModeHandler=type('APIModeHandler', (), {}),
        APIBase=type('APIBase', (), {}),
        with_modes=lambda urls: urls,
        endpoint_metrics=lambda func: func,
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
    folder_access.entity_access_error = lambda *args, **kwargs: harness.access_error
    notifications = types.ModuleType(f'{PACKAGE}.utils.schedule_notifications')
    notifications.notify_schedule_event = lambda *args, **kwargs: harness.notify(*args, **kwargs)
    sys.modules[folder_access.__name__] = folder_access
    risk = types.ModuleType(f'{PACKAGE}.utils.schedule_risk')  # the scan has its own tests (US-17)
    risk.scan_schedule_risks = lambda *args: harness.scans.append(args) or list(harness.risk_warnings)
    risk.is_run_without_asking_allowed = lambda project_id: harness.run_without_asking_allowed
    sys.modules[risk.__name__] = risk
    sys.modules[notifications.__name__] = notifications

    harness.EntitySchedule = sqlite_db.create_schedule_table(load, Base, engine)
    load('utils/constants.py', 'utils.constants')
    load('utils/schedule_timing.py', 'utils.schedule_timing')
    load('utils/schedule_access.py', 'utils.schedule_access')
    load('models/pd/index.py', 'models.pd.index')
    load('models/pd/entity_schedule.py', 'models.pd.entity_schedule')
    load('models/pd/schedule_run.py', 'models.pd.schedule_run')
    load('utils/schedule_claim.py', 'utils.schedule_claim')
    load('utils/schedule_dispatch.py', 'utils.schedule_dispatch')
    load('utils/schedule_service.py', 'utils.schedule_service')
    harness.module = load('api/v2/schedule.py', 'api.v2.schedule')

    sqlite_db.add_agent_and_pipeline(harness.session_factory, Application, ApplicationVersion)
    return harness


def agent_body(**changes):
    body = {
        'name': 'Nightly', 'entity_type': 'agent', 'application_id': 1, 'version_id': 10,
        'prompt': 'Say hi to {{ who }}', 'variables': [{'name': 'who', 'value': 'Ann'}],
        'cron': '0 9 * * *', 'timezone': 'Etc/UTC',
    }
    body.update(changes)
    return body


def pipeline_body(**changes):
    body = agent_body(
        entity_type='pipeline', application_id=2, version_id=11, run_mode='entry_point', prompt=None, variables=[],
    )
    body.update(changes)
    return body


def assert_400_names(response, field):
    body, status = response
    assert status == 400
    assert field in json.dumps(body)


# --- R1.3, R1.4, R1.5, R1.6: entity, folder rights, permission, type

def test_7011_r1_3_entity_outside_the_project_is_400_on_application_id(harness):
    assert_400_names(harness.call('post', agent_body(application_id=99, version_id=10)), 'application_id')


def test_7011_r1_3_version_of_another_entity_is_400_on_version_id(harness):
    assert_400_names(harness.call('post', agent_body(application_id=1, version_id=11)), 'version_id')


def test_7011_r1_4_folder_rights_denial_is_returned_and_nothing_is_saved(harness):
    harness.access_error = ({'ok': False, 'error': 'No access'}, 404)
    assert harness.call('post', agent_body()) == ({'ok': False, 'error': 'No access'}, 404)
    assert harness.row(1) is None


@pytest.mark.parametrize('verb, permission', [
    ('post', 'create'), ('get', 'details'), ('put', 'update'), ('delete', 'delete'),
])
def test_7011_r1_5_missing_verb_permission_is_403(harness, verb, permission):
    schedule_id = harness.seed()
    harness.permissions.discard(PERMISSION_PREFIX + permission)
    body = agent_body() if verb in ('post', 'put') else None
    response = harness.call(verb, body, None if verb == 'post' else schedule_id)
    assert response[1] == 403
    assert harness.row(schedule_id).name == 'Nightly'


@pytest.mark.parametrize('entity_type, body', [
    ('pipeline', agent_body(entity_type='pipeline')),
    ('agent', pipeline_body(entity_type='agent')),
])
def test_7011_r1_6_entity_type_must_match_the_version(harness, entity_type, body):
    assert_400_names(harness.call('post', body), 'entity_type')


# --- R2.2, R2.5: owner fixed, resume needs a version

def test_7011_r2_2_update_ignores_created_by_and_principal(harness):
    schedule_id = harness.seed()
    harness.request_as(OWNER_ID, EDITOR)
    _, status = harness.call(
        'put', {'name': 'Renamed', 'created_by': OTHER_ID, 'principal': {'kind': 'mandate', 'ref': 1}}, schedule_id,
    )
    row = harness.row(schedule_id)
    assert status == 200
    assert (row.name, row.created_by, row.principal) == ('Renamed', OWNER_ID, {'kind': 'delegation', 'ref': OWNER_ID})
    assert row.updated_by == OWNER_ID


def test_7011_r2_5_resume_without_a_version_is_400_and_stays_disabled(harness):
    schedule_id = harness.seed(version_id=None, enabled=False, disabled_reason='version_deleted')
    assert harness.call('put', {'enabled': True}, schedule_id) == (
        {'error': 'Choose a version before you resume this schedule', 'field': 'version_id'}, 400,
    )
    assert harness.row(schedule_id).enabled is False


# --- R3.1: own-only scope

@pytest.mark.parametrize('verb', ['get', 'put', 'delete'])
def test_7011_r3_1_another_owners_schedule_is_404_for_a_regular_user(harness, verb):
    schedule_id = harness.seed(created_by=OTHER_ID)
    body = {'name': 'Hijacked'} if verb == 'put' else None
    assert harness.call(verb, body, schedule_id) == (NOT_FOUND, 404)
    assert harness.row(schedule_id).name == 'Nightly'


def test_7011_r3_1_an_admin_reads_and_deletes_another_owners_schedule(harness):
    schedule_id = harness.seed(created_by=OTHER_ID)
    harness.request_as(ADMIN_ID, ADMIN)
    assert harness.call('get', schedule_id=schedule_id)[1] == 200
    assert harness.call('delete', schedule_id=schedule_id)[1] == 204
    assert harness.row(schedule_id) is None


# --- R4.2, R4.3, R4.4: audit span and notice

def admin_edit(harness):
    schedule_id = harness.seed(created_by=OTHER_ID)
    harness.request_as(ADMIN_ID, ADMIN)
    return schedule_id, harness.call('put', {'name': 'Edited by admin'}, schedule_id)


def test_7011_r4_2_admin_edit_marks_the_trace_span(harness):
    schedule_id, (_, status) = admin_edit(harness)
    assert status == 200
    assert {key: str(value) for key, value in harness.span.attributes.items()} == {
        'entity.type': 'schedule', 'entity.id': str(schedule_id), 'entity.name': 'Edited by admin',
    }


def test_7011_r4_3_admin_edit_notifies_the_owner(harness):
    schedule_id, _ = admin_edit(harness)
    assert harness.notices == [(OTHER_ID, 'schedule_edited_by_admin', schedule_id)]
    assert harness.row(schedule_id).created_by == OTHER_ID


def test_7011_r4_3_owner_edit_sends_no_notice_and_no_span_mark(harness):
    schedule_id = harness.seed()
    assert harness.call('put', {'name': 'Mine'}, schedule_id)[1] == 200
    assert harness.notices == []
    assert harness.span.attributes == {}


def test_7011_r4_4_failed_notice_keeps_the_change_committed(harness):
    harness.notice_error = RuntimeError('socket down')
    schedule_id, (_, status) = admin_edit(harness)
    assert status == 200
    assert harness.row(schedule_id).name == 'Edited by admin'


def admin_delete(harness):
    schedule_id = harness.seed(created_by=OTHER_ID)
    harness.request_as(ADMIN_ID, ADMIN)
    return schedule_id, harness.call('delete', schedule_id=schedule_id)


def test_7011_r4_2_r4_3_admin_delete_marks_the_span_and_tells_the_owner(harness):
    schedule_id, (_, status) = admin_delete(harness)
    assert status == 204
    assert harness.row(schedule_id) is None
    assert {key: str(value) for key, value in harness.span.attributes.items()} == {
        'entity.type': 'schedule', 'entity.id': str(schedule_id), 'entity.name': 'Nightly',
    }
    assert harness.notices == [(OTHER_ID, 'schedule_edited_by_admin', schedule_id)]
    assert harness.notice_extras == [{'action': 'deleted'}]


def test_7011_r4_3_owner_delete_sends_no_notice_and_no_span_mark(harness):
    schedule_id = harness.seed()
    assert harness.call('delete', schedule_id=schedule_id)[1] == 204
    assert (harness.notices, harness.span.attributes) == ([], {})


def test_7011_r4_4_failed_delete_notice_keeps_the_delete(harness):
    harness.notice_error = RuntimeError('socket down')
    schedule_id, (_, status) = admin_delete(harness)
    assert status == 204
    assert harness.row(schedule_id) is None


# --- R5.1, R5.2, R5.4, R5.5, R5.6, R5.7: run input rules

@pytest.mark.parametrize('prompt', [None, '', '   '])
def test_7011_r5_1_agent_without_a_prompt_is_400_on_prompt(harness, prompt):
    assert_400_names(harness.call('post', agent_body(prompt=prompt, variables=[])), 'prompt')
    schedule_id = harness.seed()
    assert_400_names(harness.call('put', {'prompt': prompt, 'variables': []}, schedule_id), 'prompt')


def test_7011_r5_2_entry_point_pipeline_without_prompt_and_variables_is_saved(harness):
    body, status = harness.call('post', pipeline_body())
    assert status == 201
    row = harness.row(body['id'])
    assert (row.run_mode, row.prompt, row.variables, row.created_by) == ('entry_point', None, [], OWNER_ID)


@pytest.mark.parametrize('body', [
    agent_body(run_mode='prompt'),
    pipeline_body(run_mode=None),
    pipeline_body(run_mode='other'),
])
def test_7011_r5_4_run_mode_must_fit_the_entity_type(harness, body):
    assert_400_names(harness.call('post', body), 'run_mode')


@pytest.mark.parametrize('changes, field', [
    ({'prompt': 'Say hi'}, 'prompt'),
    ({'variables': [{'name': 'who', 'value': 'Ann'}]}, 'variables'),
    ({'run_mode': 'prompt', 'prompt': None}, 'prompt'),
])
def test_7011_r5_5_run_mode_rules_name_the_broken_field(harness, changes, field):
    assert_400_names(harness.call('post', pipeline_body(**changes)), field)


def test_7011_r5_6_variable_names_must_equal_the_placeholders(harness):
    response = harness.call('post', agent_body(variables=[{'name': 'other', 'value': 'x'}]))
    assert_400_names(response, 'variables')
    assert 'who' in json.dumps(response[0]) and 'other' in json.dumps(response[0])


def test_7011_r5_6_description_over_2304_characters_is_400(harness):
    assert harness.call('post', agent_body(description='a' * 2304))[1] == 201
    assert_400_names(harness.call('post', agent_body(description='a' * 2305)), 'description')


@pytest.mark.parametrize('prompt', [
    'Run {% if x %}a{% endif %}',
    'Note {# hidden #}',
    'Say {{ who.name }}',
    "Say {{ who|upper }}",
    'Say {{ who + 1 }}',
    'Say {{ who }} }}',
])
def test_7011_r5_7_template_syntax_other_than_placeholders_is_400_on_prompt(harness, prompt):
    body = agent_body(prompt=prompt, variables=[{'name': 'who', 'value': 'Ann'}])
    assert_400_names(harness.call('post', body), 'prompt')


def test_7011_r5_7_simple_placeholders_with_spaces_are_saved(harness):
    body = agent_body(prompt='Hi {{who}} and {{  who  }}')
    assert harness.call('post', body)[1] == 201
