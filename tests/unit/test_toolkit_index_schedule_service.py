"""Toolkit Index schedules in the body models and the schedule service (SQLite, real service and models)."""
import pathlib
import sys
import types
from unittest.mock import create_autospec

import pytest
from fixtures import schedule_sqlite as sqlite_db
from fixtures.helpers import load_module_with_stubs
from pydantic import ValidationError
from unit import test_7011_legacy_migration_rows as legacy_rows

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
PACKAGE = 'plugins.elitea_core'
PROJECT_ID = 7
OWNER_ID, OTHER_ID = 42, 43
DAILY = '0 9 * * *'
SAVED = {'chunk_size': 500}
INDEX_EDIT = 'models.applications.index_meta.edit'
migration = legacy_rows.migration  # the fixture of the migration tests, bound here under its own name


class Env:
    """The loaded service, the body models and one SQLite database with an artifact toolkit."""

    def scope(self, user_id=OWNER_ID, is_admin=False):
        return self.access.ScheduleScope(user_id=user_id, is_admin=is_admin, permissions=frozenset(self.permissions))

    def index_body(self, **changes):
        values = {'name': 'Nightly index', 'entity_type': 'toolkit_index', 'toolkit_id': 3, 'index_name': 'docs',
                  'cron': DAILY, 'timezone': 'Etc/UTC'}
        return self.pd.ScheduleCreate(**{**values, **changes})

    def create(self, **changes):
        with self.session_factory() as session:
            return self.service.create_schedule(session, PROJECT_ID, self.index_body(**changes), self.scope())

    def seed_index(self, index_name='docs', **fields):
        return sqlite_db.seed_schedule(
            self.session_factory, self.EntitySchedule, OWNER_ID,
            **{'entity_type': 'toolkit_index', 'application_id': None, 'version_id': None, 'prompt': None,
               'toolkit_id': 3, 'index_name': index_name, **fields})

    def update(self, schedule_id, is_admin=False, **changes):
        with self.session_factory() as session:
            row = session.get(self.EntitySchedule, schedule_id)
            return self.service.update_schedule(
                session, PROJECT_ID, row, self.pd.ScheduleUpdate(**changes), self.scope(is_admin=is_admin))

    def check(self, **overrides):
        values = {'toolkit_id': 3, 'index_name': 'docs', **overrides}
        with self.session_factory() as session:
            return self.service.check_entity(session, PROJECT_ID, 'toolkit_index', None, None, **values)


@pytest.fixture
def env(monkeypatch):
    original = sys.modules.copy()
    for name, module in sqlite_db.REAL_SQLALCHEMY.items():
        monkeypatch.setitem(sys.modules, name, module)
    env = Env()
    for name in ('plugins', PACKAGE, f'{PACKAGE}.utils', f'{PACKAGE}.models', f'{PACKAGE}.models.enums',
                 f'{PACKAGE}.models.pd', 'pylon', 'pylon.core'):
        sqlite_db.package(name)
    Base, Application, ApplicationVersion = sqlite_db.install_parent_models(PACKAGE)
    sqlite_db.install_trace_span(types.SimpleNamespace(set_attribute=lambda key, value: None))
    engine = sqlite_db.sqlite_engine()
    env.session_factory = lambda: sqlite_db.orm.Session(engine, expire_on_commit=False)
    tools = sqlite_db.LenientModule('tools')
    env.permissions = {INDEX_EDIT}
    tools.auth = types.SimpleNamespace(
        current_user=lambda: {'id': OWNER_ID}, resolve_permissions=lambda mode=None, project_id=None: env.permissions)
    tools.config = types.SimpleNamespace(
        ADMINISTRATION_MODE='administration', DEFAULT_MODE='default', POSTGRES_TENANT_SCHEMA='tenant')
    tools.db = types.SimpleNamespace(get_session=lambda project_id=None: env.session_factory(), Base=Base)
    tools.this = types.SimpleNamespace(module=types.SimpleNamespace(descriptor=types.SimpleNamespace(config={})))
    sys.modules['tools'] = tools
    env.config = tools.this.module.descriptor.config

    env.folder_refusal = None
    env.folder_calls = []
    folder_access = types.ModuleType(f'{PACKAGE}.utils.folder_access')

    def entity_access_error(project_id, entity_type, entity_id, write=False, user_id=None):
        env.folder_calls.append((entity_type, entity_id, write))
        return env.folder_refusal

    folder_access.entity_access_error = entity_access_error
    folder_access.TOOLKIT_ENTITY_TYPES = ['toolkit', 'mcp']
    notifications = types.ModuleType(f'{PACKAGE}.utils.schedule_notifications')
    notifications.notify_schedule_event = lambda *args, **kwargs: True
    sys.modules[folder_access.__name__] = folder_access
    sys.modules[notifications.__name__] = notifications

    def load(relative_path, name):
        return load_module_with_stubs(PLUGIN_ROOT / relative_path, f'{PACKAGE}.{name}')

    env.EntitySchedule = sqlite_db.create_schedule_table(load, Base, engine)
    load('utils/constants.py', 'utils.constants')
    env.timing = load('utils/schedule_timing.py', 'utils.schedule_timing')
    env.access = load('utils/schedule_access.py', 'utils.schedule_access')
    load('models/pd/index.py', 'models.pd.index')
    load('models/pd/schedule_list.py', 'models.pd.schedule_list')
    env.pd = load('models/pd/entity_schedule.py', 'models.pd.entity_schedule')
    env.service = load('utils/schedule_service.py', 'utils.schedule_service')
    load('utils/schedule_test_runs.py', 'utils.schedule_test_runs')
    sqlite_db.add_agent_and_pipeline(env.session_factory, Application, ApplicationVersion)
    all_models = sys.modules[f'{PACKAGE}.models.all']
    with env.session_factory() as session:
        session.add(all_models.EliteATool(id=3, name='Docs toolkit', type='artifact'))
        session.commit()
    env.toolkit_meta = {}
    env.configuration = SAVED
    env.lookups = []

    def lookup(session, project_id, toolkit, index_name, user_id):
        env.lookups.append((index_name, user_id))
        if isinstance(env.configuration, Exception):
            raise env.configuration
        return env.configuration

    env.read_configuration = env.service.read_index_configuration  # the real function, for its own test
    env.service.read_index_configuration = create_autospec(env.service.read_index_configuration, side_effect=lookup)
    yield env
    for key in set(sys.modules) - set(original):
        sys.modules.pop(key, None)
    sys.modules.update(original)  # a module that was loaded before and got replaced comes back too


def error_of(excinfo):
    body, status = excinfo.value.response
    return body['code'], status, body.get('field')


def test_toolkit_index_body_needs_no_application_or_version(env):
    body = env.index_body()
    assert (body.toolkit_id, body.index_name, body.application_id, body.version_id) == (3, 'docs', None, None)


@pytest.mark.parametrize('missing', ['toolkit_id', 'index_name'])
def test_toolkit_index_body_refuses_a_missing_target_field(env, missing):
    with pytest.raises(ValidationError) as excinfo:
        env.index_body(**{missing: None})
    assert missing in {error['loc'][0] for error in excinfo.value.errors() if error['loc']}


@pytest.mark.parametrize('missing', ['application_id', 'version_id'])
def test_agent_body_still_needs_application_and_version(env, missing):
    body = {'name': 'Report', 'entity_type': 'agent', 'application_id': 15, 'version_id': 10, 'prompt': 'Go',
            'cron': DAILY, 'timezone': 'UTC', missing: None}
    with pytest.raises(ValidationError) as excinfo:
        env.pd.ScheduleCreate(**body)
    assert missing in {error['loc'][0] for error in excinfo.value.errors() if error['loc']}


def test_check_entity_accepts_an_index_with_saved_settings(env):
    env.check()
    assert env.folder_calls == [(['toolkit', 'mcp'], 3, True)]


def test_check_entity_reads_the_index_as_the_caller(env):
    env.check()
    assert env.lookups == [('docs', OWNER_ID)]


def test_the_index_read_expands_the_settings_for_the_caller_after_it_ends_the_read_transaction(env, monkeypatch):
    events = []

    class Rpc:
        def timeout(self, seconds):
            return self

        def configurations_expand(self, project_id, settings, user_id, unsecret):
            events.append(('expand', user_id, unsecret))
            return {}

    rpc_tools = types.SimpleNamespace(RpcMixin=lambda: types.SimpleNamespace(rpc=Rpc()))
    monkeypatch.setattr(sys.modules['tools'], 'rpc_tools', rpc_tools, raising=False)
    for name, attributes in (('application_tools', {'_get_pgvector_engine': None}),
                             ('orphaned_index_schedules', {'pgvector_settings': lambda settings: settings})):
        module = types.ModuleType(f'{PACKAGE}.utils.{name}')
        vars(module).update(attributes)
        monkeypatch.setitem(sys.modules, module.__name__, module)
    session = types.SimpleNamespace(rollback=lambda: events.append('rollback'))
    toolkit = types.SimpleNamespace(id=3, settings={'pgvector_configuration': {}}, author_id=99)
    with pytest.raises(ValueError):  # the stand-in answer has no connection string
        env.read_configuration(session, PROJECT_ID, toolkit, 'docs', OWNER_ID)
    assert events == ['rollback', ('expand', OWNER_ID, True)]


def test_check_entity_refuses_an_unknown_toolkit(env):
    with pytest.raises(env.service.ScheduleError) as excinfo:
        env.check(toolkit_id=99)
    assert error_of(excinfo) == ('invalid_input', 400, 'toolkit_id')
    assert env.lookups == []


@pytest.mark.parametrize('status, code', [(404, 'not_found'), (403, 'read_only')])
def test_check_entity_maps_a_folder_refusal_before_it_reads_the_index(env, status, code):
    env.folder_refusal = ({'ok': False, 'error': 'no'}, status)
    with pytest.raises(env.service.ScheduleError) as excinfo:
        env.check()
    assert error_of(excinfo)[:2] == (code, status)
    assert env.lookups == []


def test_check_entity_refuses_an_index_the_store_does_not_have(env):
    env.configuration = None
    with pytest.raises(env.service.ScheduleError) as excinfo:
        env.check()
    assert error_of(excinfo) == ('invalid_input', 400, 'index_name')


def test_check_entity_refuses_an_index_without_saved_settings(env):
    env.configuration = {}
    with pytest.raises(env.service.ScheduleError) as excinfo:
        env.check()
    assert error_of(excinfo) == ('invalid_input', 400, 'index_name')
    assert 'Run it once' in str(excinfo.value)


def test_check_entity_answers_503_when_the_store_cannot_be_read(env):
    env.configuration = RuntimeError('connection refused to db-host')
    with pytest.raises(env.service.ScheduleError) as excinfo:
        env.check()
    assert error_of(excinfo)[:2] == ('index_unavailable', 503)
    assert 'db-host' not in str(excinfo.value)


def test_check_entity_refuses_an_index_that_has_a_schedule_row(env):
    env.seed_index()
    with pytest.raises(env.service.ScheduleError) as excinfo:
        env.check()
    assert error_of(excinfo)[:2] == ('index_scheduled', 409)


def test_check_entity_refuses_an_index_with_a_legacy_schedule(env):
    all_models = sys.modules[f'{PACKAGE}.models.all']
    with env.session_factory() as session:
        session.get(all_models.EliteATool, 3).meta = {'indexes_meta': {'docs': {'schedules': [{'cron': DAILY}]}}}
        session.commit()
    with pytest.raises(env.service.ScheduleError) as excinfo:
        env.check()
    assert error_of(excinfo)[:2] == ('index_scheduled', 409)


def test_check_entity_refuses_a_caller_without_the_index_edit_permission(env):
    env.permissions = set()
    with pytest.raises(env.service.ScheduleError) as excinfo:
        env.check()
    assert error_of(excinfo)[:2] == ('read_only', 403)
    assert env.lookups == []


def test_check_entity_keeps_a_hidden_toolkit_a_404_for_a_caller_without_the_permission(env):
    env.permissions = set()
    env.folder_refusal = ({'ok': False, 'error': 'Not found'}, 404)
    with pytest.raises(env.service.ScheduleError) as excinfo:
        env.check()
    assert error_of(excinfo)[:2] == ('not_found', 404)


def test_create_refuses_a_caller_without_the_index_edit_permission_and_stores_nothing(env):
    env.permissions = set()
    with pytest.raises(env.service.ScheduleError) as excinfo:
        env.create()
    assert error_of(excinfo)[:2] == ('read_only', 403)
    with env.session_factory() as session:
        assert session.query(env.EntitySchedule).count() == 0


def test_update_of_an_index_row_refuses_an_owner_without_the_index_edit_permission(env):
    schedule_id = env.seed_index()
    env.permissions = set()
    with pytest.raises(env.service.ScheduleError) as excinfo:
        env.update(schedule_id, enabled=False)
    assert error_of(excinfo)[:2] == ('read_only', 403)
    with env.session_factory() as session:
        assert session.get(env.EntitySchedule, schedule_id).enabled is True


def test_update_of_an_index_row_refuses_an_owner_without_folder_write(env):
    schedule_id = env.seed_index()
    env.folder_refusal = ({'ok': False, 'error': 'read only'}, 403)
    with pytest.raises(env.service.ScheduleError) as excinfo:
        env.update(schedule_id, name='Renamed')
    assert error_of(excinfo)[:2] == ('read_only', 403)
    assert env.folder_calls == [(['toolkit', 'mcp'], 3, True)]


def test_update_of_an_index_row_lets_an_admin_change_any_row_without_the_permission(env):
    schedule_id = env.seed_index()
    env.permissions = set()
    assert env.update(schedule_id, is_admin=True, name='Renamed').name == 'Renamed'
    assert env.folder_calls == []


def test_update_of_an_agent_row_needs_neither_the_index_permission_nor_a_toolkit_folder(env):
    schedule_id = sqlite_db.seed_schedule(env.session_factory, env.EntitySchedule, OWNER_ID)
    env.permissions = set()
    assert env.update(schedule_id, name='Renamed').name == 'Renamed'
    assert env.folder_calls == []


def test_check_entity_lets_another_index_of_the_toolkit_through(env):
    env.seed_index(index_name='wiki')
    env.check()


def test_create_stores_an_index_row_without_application_version_or_prompt(env):
    row = env.create()
    assert (row.entity_type, row.toolkit_id, row.index_name) == ('toolkit_index', 3, 'docs')
    assert (row.application_id, row.version_id, row.run_mode, row.prompt) == (None, None, None, None)
    assert row.next_run is not None


def test_create_turns_a_lost_race_on_the_unique_index_into_409(env):
    env.seed_index()
    env.service.check_toolkit_index = lambda *args: None  # both saves passed the read; the index decides
    with pytest.raises(env.service.ScheduleError) as excinfo:
        env.create()
    assert error_of(excinfo)[:2] == ('index_scheduled', 409)
    with env.session_factory() as session:
        assert session.query(env.EntitySchedule).count() == 1


@pytest.mark.parametrize('field, value', [
    ('prompt', 'Say hi'), ('run_mode', 'prompt'), ('version_id', 10), ('application_id', 1),
    ('approval_rule', 'run_without_asking'), ('variables', [{'name': 'a', 'value': 'b'}]),
])
def test_create_refuses_what_an_index_schedule_cannot_hold(env, field, value):
    with pytest.raises(env.service.ScheduleError) as excinfo:
        env.create(**{field: value})
    assert error_of(excinfo)[:2] == ('invalid_input', 400)
    assert error_of(excinfo)[2] == field


def test_create_refuses_a_cron_below_the_index_floor_that_the_agent_floor_allows(env):
    with pytest.raises(env.service.ScheduleError) as excinfo:
        env.create(cron='0 */6 * * *')
    assert error_of(excinfo) == ('invalid_input', 400, 'cron')
    assert '1440' in str(excinfo.value)


def test_create_accepts_a_cron_at_the_configured_index_floor(env):
    env.config['scheduler'] = {'schedules': {'toolkit_index_min_interval_minutes': 360}}
    assert env.create(cron='0 */6 * * *').cron == '0 */6 * * *'


def test_an_agent_body_refuses_a_toolkit_and_an_index(env):
    body = env.pd.ScheduleCreate(name='A', entity_type='agent', application_id=1, version_id=10, prompt='Hi',
                                 cron=DAILY, timezone='UTC', toolkit_id=3, index_name='docs')
    with env.session_factory() as session, pytest.raises(env.service.ScheduleError) as excinfo:
        env.service.check_schedule_body(session, PROJECT_ID, body)
    assert error_of(excinfo)[:2] == ('invalid_input', 400)


@pytest.mark.parametrize('changes, field', [
    ({'version_id': 10}, 'version_id'),
    ({'risk_ack': {'fingerprint': 'x', 'acknowledged': []}}, 'risk_ack'),
    ({'approval_rule': 'run_without_asking'}, 'approval_rule'),
])
def test_update_refuses_what_an_index_row_cannot_hold_with_a_400(env, changes, field):
    schedule_id = env.seed_index()
    with pytest.raises(env.service.ScheduleError) as excinfo:
        env.update(schedule_id, **changes)
    assert error_of(excinfo) == ('invalid_input', 400, field)


def test_update_keeps_a_form_that_sends_the_stored_approval_rule_back(env):
    schedule_id = env.seed_index()
    assert env.update(schedule_id, name='Renamed', approval_rule='pause_and_ask').name == 'Renamed'


def test_update_applies_the_index_floor_to_a_new_cron(env):
    schedule_id = env.seed_index()
    with pytest.raises(env.service.ScheduleError) as excinfo:
        env.update(schedule_id, cron='*/30 * * * *')
    assert error_of(excinfo) == ('invalid_input', 400, 'cron')


def test_delete_of_an_index_row_is_refused_without_the_index_edit_permission(env):
    schedule_id = env.seed_index()
    env.permissions = set()
    with env.session_factory() as session:
        row = session.get(env.EntitySchedule, schedule_id)
        with pytest.raises(env.service.ScheduleError) as refused:
            env.service.delete_schedule(session, PROJECT_ID, row, env.scope())
    assert error_of(refused)[:2] == ('read_only', 403)
    with env.session_factory() as session:
        assert session.get(env.EntitySchedule, schedule_id) is not None


def test_check_entity_refuses_an_admin_without_the_index_edit_permission(env):
    env.permissions = {env.access.SCHEDULE_PERMISSIONS['admin']}
    with pytest.raises(env.service.ScheduleError) as excinfo:
        env.check()
    assert error_of(excinfo)[:2] == ('read_only', 403)
    assert env.folder_calls == [(['toolkit', 'mcp'], 3, True)]  # the folder answers first


def test_create_refuses_an_admin_without_the_index_edit_permission_and_stores_nothing(env):
    env.permissions = {env.access.SCHEDULE_PERMISSIONS['admin']}
    with pytest.raises(env.service.ScheduleError) as excinfo:
        env.create()
    assert error_of(excinfo)[:2] == ('read_only', 403)
    with env.session_factory() as session:
        assert session.query(env.EntitySchedule).count() == 0


def test_check_entity_passes_an_admin_who_holds_the_index_edit_permission(env):
    env.permissions = {env.access.SCHEDULE_PERMISSIONS['admin'], INDEX_EDIT}
    assert env.check() is None


def test_resume_of_an_index_row_needs_no_version(env):
    schedule_id = env.seed_index(enabled=False)
    assert env.update(schedule_id, enabled=True).enabled is True


def test_resume_of_an_agent_row_without_a_version_is_still_refused(env):
    schedule_id = sqlite_db.seed_schedule(env.session_factory, env.EntitySchedule, OWNER_ID, enabled=False,
                                          version_id=None)
    with pytest.raises(env.service.ScheduleError) as excinfo:
        env.update(schedule_id, enabled=True)
    assert error_of(excinfo) == ('invalid_input', 400, 'version_id')


def test_below_minimum_uses_the_floor_of_the_entity_type(migration):
    assert migration.below_minimum('*/30 * * * *') is False
    assert migration.below_minimum('*/30 * * * *', 'toolkit_index') is True
    assert migration.below_minimum('0 9 * * *', 'toolkit_index') is False


def _fake_database(monkeypatch, events):
    """`tools.db` and the lock key helper, recording the order of the lock, the work and the unlock."""

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def execution_options(self, **options):
            events.append(('options', options))
            return self

        def execute(self, statement, params=None):
            events.append(('sql', str(statement)))

        def invalidate(self):
            events.append(('invalidate',))

    class Session:
        def __enter__(self):
            return 'session'

        def __exit__(self, *exc):
            return False

    db = types.SimpleNamespace(engine=types.SimpleNamespace(connect=Connection),
                               get_session=lambda project_id: Session())
    monkeypatch.setattr(sys.modules['tools'], 'db', db, raising=False)
    claim = types.ModuleType('plugins.elitea_core.utils.schedule_claim')
    claim.legacy_lock_key = lambda project_id: project_id
    monkeypatch.setitem(sys.modules, claim.__name__, claim)


def test_migrate_project_runs_the_default_body_under_the_lock(migration, monkeypatch):
    events = []
    _fake_database(monkeypatch, events)
    monkeypatch.setattr(migration, '_migrate_versions',
                        lambda session, project_id, dry_run, enforce: events.append(('versions', dry_run, enforce))
                        or {'migrated': 1})
    assert migration.migrate_project(5, True) == {'migrated': 1}
    kinds = [event[0] if event[0] != 'sql' else event[1] for event in events]
    assert kinds[-2:] == ['versions', 'SELECT pg_advisory_unlock(:key)']
    assert ('options', {'isolation_level': 'AUTOCOMMIT'}) in events


def test_migrate_project_runs_a_given_body_under_the_same_lock(migration, monkeypatch):
    events = []
    _fake_database(monkeypatch, events)
    calls = []

    def body(session, project_id, dry_run, enforce_limits):
        calls.append((session, project_id, dry_run, enforce_limits))
        return {'moved': 2}

    monkeypatch.setattr(migration, '_migrate_versions', lambda *args: pytest.fail('the default body ran'))
    assert migration.migrate_project(5, False, True, body=body) == {'moved': 2}
    assert calls == [('session', 5, False, True)]
    assert any(event == ('sql', 'SELECT pg_advisory_unlock(:key)') for event in events)
