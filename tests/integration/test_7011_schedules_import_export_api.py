"""EL-7011 US-22: the admin-only schedule file routes (R1.1, R1.2, R1.4, R2.6, R3.1, R3.3, R3.4) on SQLite.

The routes, the file module, the create service, the access helper and the schedule table are real.
Flask, the check_api decorator and the plugin package around them are stubbed. The check_api stub
answers 403 when the spec permissions are not all granted, as the platform does.

Run via:
    python tests/run_tests.py -m integration -v integration/test_7011_schedules_import_export_api.py
"""
import io
import sys
import types

import pytest
import yaml
from fixtures import schedule_sqlite as sqlite_db
from unit.test_7011_schedule_yaml_import import (
    file_text,
    install_service_stubs,
    install_tools,
    item,
    load_plugin_module,
    sandbox,
)

PACKAGE = 'plugins.elitea_core'
PROJECT_ID = 35
ADMIN_ID = 99
PREFIX = 'models.applications.schedules.'
BASE = f'/api/v2/elitea_core/schedules/prompt_lib/{PROJECT_ID}'


class FakeArgs(dict):
    """The query string: one value per key."""

    def to_dict(self):
        return dict(self)

    def getlist(self, key):
        return [self[key]] if key in self else []


class FakeResponse:
    def __init__(self, response, mimetype=None, headers=None):
        self.text, self.mimetype, self.headers = response, mimetype, headers or {}


class Env:
    permissions = frozenset()

    def grant(self, *verbs):
        self.permissions = frozenset(PREFIX + verb for verb in verbs)

    def call(self, method, tail, args=None, upload=None):
        """Run the handler of `method` on the URL `BASE/<tail>`; `upload` is the bytes of the file part."""
        files = {'file': types.SimpleNamespace(filename='f.yaml', **_stream(upload))} if upload is not None else {}
        self.request.path = f'{BASE}/{tail}' if tail else BASE
        self.request.args = FakeArgs(args or {})
        self.request.files = files
        return getattr(self.module.PromptLibAPI(), method)(project_id=PROJECT_ID)

    def export(self, **args):
        return self.call('get', 'export', args)

    def import_file(self, data):
        return self.call('post', 'import', upload=data if isinstance(data, bytes) else data.encode())

    def seed(self, name='Nightly', **fields):
        return sqlite_db.seed_schedule(self.session_factory, self.EntitySchedule, ADMIN_ID, name=name, **fields)

    def rows(self):
        with self.session_factory() as session:
            return session.query(self.EntitySchedule).order_by(self.EntitySchedule.id).all()


def _stream(data):
    buffer = io.BytesIO(data)
    return {'seek': buffer.seek, 'read': buffer.read}


@pytest.fixture
def env(monkeypatch):
    env = Env()
    packages = (f'{PACKAGE}.api', f'{PACKAGE}.api.v2')
    with sandbox(monkeypatch, env, packages) as (Base, engine):
        yield from _install(env, Base, engine)


def _install(env, Base, engine):
    sqlite_db.install_trace_span(types.SimpleNamespace(set_attribute=lambda key, value: None))

    def check_api(spec):
        def decorator(func):
            def wrapper(*args, **kwargs):
                if not set(spec['permissions']) <= env.permissions:
                    return {'error': 'forbidden'}, 403
                return func(*args, **kwargs)
            return wrapper
        return decorator

    install_tools(
        env, Base,
        api_tools=types.SimpleNamespace(
            APIModeHandler=object, APIBase=object, endpoint_metrics=lambda func: func,
            with_modes=lambda params: list(params),
        ),
        register_openapi=lambda **meta: (lambda func: func),
        auth=types.SimpleNamespace(
            decorators=types.SimpleNamespace(check_api=check_api),
            current_user=lambda: {'id': ADMIN_ID},
            resolve_permissions=lambda **kwargs: set(env.permissions),
        ),
    )
    env.request = types.SimpleNamespace(path='', args={}, files={})
    flask = types.ModuleType('flask')
    flask.request, flask.Response = env.request, FakeResponse
    sys.modules['flask'] = flask
    quiet = lambda *a, **k: None
    sys.modules['pylon.core.tools'] = types.SimpleNamespace(log=types.SimpleNamespace(
        warning=quiet, info=quiet, error=quiet, debug=quiet, exception=quiet,
    ))

    install_service_stubs(env)
    authors = types.ModuleType(f'{PACKAGE}.utils.authors')
    queries = types.ModuleType(f'{PACKAGE}.utils.schedule_queries')
    for stub in (authors, queries):
        sys.modules[stub.__name__] = stub
    authors.get_authors_data = quiet
    queries.list_schedules = lambda *args: (0, [])

    load = load_plugin_module
    env.EntitySchedule = sqlite_db.create_schedule_table(load, Base, engine)
    for path, name in (
        ('models/pd/schedule_list.py', 'models.pd.schedule_list'), ('utils/constants.py', 'utils.constants'), ('utils/schedule_timing.py', 'utils.schedule_timing'),
        ('utils/schedule_access.py', 'utils.schedule_access'), ('utils/pipeline_utils.py', 'utils.pipeline_utils'),
        ('utils/export_import_utils.py', 'utils.export_import_utils'), ('models/pd/index.py', 'models.pd.index'),
        ('models/pd/entity_schedule.py', 'models.pd.entity_schedule'),
        ('models/pd/schedule_yaml.py', 'models.pd.schedule_yaml'),
        ('utils/schedule_service.py', 'utils.schedule_service'), ('utils/schedule_yaml.py', 'utils.schedule_yaml'),
    ):
        load(path, name)
    env.module = load('api/v2/schedules.py', 'api.v2.schedules')
    with env.session_factory() as session:
        session.add(env.Application(id=1, name='Entity'))
        session.flush()
        session.add(env.ApplicationVersion(id=10, application_id=1, name='base', agent_type='openai'))
        session.commit()
    yield env


# --- R1.1, R1.2, R1.4, R3.3: export

def test_7011_r1_1_export_returns_one_yaml_file_with_every_schedule(env):
    env.grant('export')
    env.seed('First')
    env.seed('Second')
    response = env.export()
    data = yaml.safe_load(response.text)
    assert (data['kind'], data['schema_version']) == ('elitea_schedules', 1)
    assert [entry['name'] for entry in data['schedules']] == ['First', 'Second']
    assert response.mimetype == 'application/x-yaml'
    assert 'schedules_35.yaml' in response.headers['Content-Disposition']


def test_7011_r1_2_export_with_ids_writes_only_those_schedules(env):
    env.grant('export')
    env.seed('First')
    second = env.seed('Second')
    data = yaml.safe_load(env.export(ids=str(second)).text)
    assert [entry['name'] for entry in data['schedules']] == ['Second']


def test_7011_r1_4_export_with_an_unknown_id_answers_400_and_lists_it(env):
    env.grant('export')
    first = env.seed('First')
    body, status = env.export(ids=f'{first},777,888')
    assert status == 400
    assert '777' in body['error'] and '888' in body['error']


@pytest.mark.parametrize('ids', ['1,a', '', '1,,2', '١', '2147483648', '1,99999999999', ','.join(['1'] * 501)])
def test_7011_r1_4_export_with_a_malformed_ids_list_answers_400(env, ids):
    env.grant('export')
    env.seed('First')
    assert env.export(ids=ids)[1] == 400


def test_7011_r1_4_export_accepts_the_largest_id_and_500_ids_and_answers_the_unknown_id_400(env):
    env.grant('export')
    first = env.seed('First')
    assert env.export(ids=f'{first},2147483647')[1] == 400
    assert env.export(ids=','.join([str(first)] * 500)).text


@pytest.mark.parametrize('verbs', [(), ('list',), ('import',), ('admin',)])
def test_7011_r3_3_export_without_the_export_permission_answers_403(env, verbs):
    env.grant(*verbs)
    env.seed('First')
    assert env.export()[1] == 403


def test_7011_r3_3_a_caller_with_only_export_is_not_blocked_by_the_list_permission(env):
    env.grant('export')
    env.seed('First')
    assert yaml.safe_load(env.export().text)['schedules'][0]['name'] == 'First'


def test_7011_r3_3_a_caller_with_only_list_still_reaches_the_list(env):
    env.grant('list')
    assert env.call('get', '')[1] == 200


# --- R2.6, R3.4: import

def test_7011_r2_6_import_answers_the_report_and_creates_disabled_rows(env):
    env.grant('import')
    body, status = env.import_file(file_text(item(), item(name='Lost', entity_name='Nobody')))
    assert status == 200
    (row,) = env.rows()
    assert (row.enabled, row.created_by) == (False, ADMIN_ID)
    assert body['created'] == [{'name': 'Daily digest', 'schedule_id': row.id}]
    assert [entry['name'] for entry in body['skipped']] == ['Lost']
    assert body['failed'] == []


def test_7011_r2_6_an_item_with_an_unknown_key_is_failed_and_the_next_item_goes_on(env):
    env.grant('import')
    body, status = env.import_file(file_text(item(extra='x'), item(name='Good')))
    assert status == 200
    assert [entry['name'] for entry in body['failed']] == ['Daily digest']
    assert [entry['name'] for entry in body['created']] == ['Good']


def test_7011_r2_6_a_folder_rights_denial_makes_the_item_failed(env):
    env.grant('import')
    env.folder_error = ({'error': 'No write access to the folder'}, 403)
    body, status = env.import_file(file_text(item()))
    assert status == 200
    assert [entry['name'] for entry in body['failed']] == ['Daily digest']
    assert body['created'] == [] and env.rows() == []


@pytest.mark.parametrize('verbs', [(), ('list',), ('export',), ('admin',)])
def test_7011_r3_4_import_without_the_import_permission_answers_403_and_creates_nothing(env, verbs):
    env.grant(*verbs)
    assert env.import_file(file_text(item()))[1] == 403
    assert env.rows() == []


@pytest.mark.parametrize('data', [
    b'a: [', b'- 1\n- 2\n', b'', b'\xff\xfe\x00bad', b'x' * (1024 * 1024 + 1),
    file_text(item(), kind='other').encode(), file_text(item(), schema_version=2).encode(),
    yaml.safe_dump({'schedules': []}).encode(),
], ids=['broken', 'list', 'empty', 'not utf8', 'too large', 'wrong kind', 'wrong version', 'no header'])
def test_7011_r3_1_a_bad_file_answers_400_and_creates_nothing(env, data):
    env.grant('import')
    body, status = env.import_file(data)
    assert status == 400 and body['error']
    assert env.rows() == []


def test_7011_r3_1_a_file_with_more_than_500_items_answers_400_and_creates_nothing(env):
    env.grant('import')
    body, status = env.import_file(file_text(*[item()] * 501))
    assert (status, body['error']) == (400, 'The schedule file has more than 500 schedules')
    assert env.rows() == []


def test_7011_r3_1_a_missing_file_answers_400_and_creates_nothing(env):
    env.grant('import')
    body, status = env.call('post', 'import')
    assert (status, body['error']) == (400, 'No file provided')
    assert env.rows() == []


def test_7011_r3_1_the_size_and_header_messages_are_in_schedule_words(env):
    env.grant('import')
    assert env.import_file(b'x' * (1024 * 1024 + 1))[0]['error'] == 'The schedule file is larger than 1 MB'
    assert env.import_file(file_text(item(), kind='other'))[0]['error'] == (
        'This is not a schedule file of schema version 1'
    )


def test_7011_r3_4_a_post_to_the_list_url_is_not_allowed(env):
    env.grant('import', 'export', 'list')
    assert env.call('post', '')[1] == 405
