"""EL-7011 US-22: import of a schedule YAML file (R2.1 to R2.6, R3.1, R3.2, R3.5, R4, R5) on SQLite.

The import module, the create service, the body models and the schedule table are real.
The plugin package around them is stubbed.

Run via:
    python tests/run_tests.py unit/test_7011_schedule_yaml_import.py -v
"""
import contextlib
import datetime
import pathlib
import sys
import types
import uuid

import pytest
import yaml
from fixtures import schedule_sqlite as sqlite_db
from fixtures.helpers import load_module_with_stubs
from sqlalchemy.exc import OperationalError

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
PACKAGE = 'plugins.elitea_core'
PROJECT_ID = 35
ADMIN_ID = 99
NOT_YAML = 'The schedule file is not valid YAML'
TOO_LARGE = 'The schedule file is larger than 1 MB'
TOO_MANY = 'The schedule file has more than 500 schedules'
HEADER = {'kind': 'elitea_schedules', 'schema_version': 1}


def item(**overrides):
    values = {
        'name': 'Daily digest', 'description': None, 'entity_type': 'agent', 'entity_name': 'Entity',
        'version_name': 'base', 'run_mode': None, 'cron': '0 9 * * 1-5', 'timezone': 'Europe/Berlin',
        'prompt': 'Summarise', 'variables': [], 'expires_at': None, 'approval_rule': 'pause_and_ask',
    }
    return {**values, **overrides}


def pipeline_item(**overrides):
    return item(**{
        'name': 'Report flow', 'entity_type': 'pipeline', 'entity_name': 'Flow', 'run_mode': 'prompt',
        'prompt': 'Go', **overrides,
    })


def file_text(*items, **header):
    return yaml.safe_dump({**HEADER, **header, 'schedules': list(items)}, sort_keys=False)


def load_plugin_module(relative_path, name):
    """Load one plugin file as a module of the stubbed package; shared by the three 7011 YAML test files."""
    return load_module_with_stubs(PLUGIN_ROOT / relative_path, f'{PACKAGE}.{name}')


@contextlib.contextmanager
def sandbox(monkeypatch, env, packages=()):
    """Real SQLAlchemy, empty plugin packages and an SQLite session factory on `env`; yields (Base, engine).

    Every module the test loads is dropped again on exit.
    """
    original = sys.modules.copy()
    for name, module in sqlite_db.REAL_SQLALCHEMY.items():
        monkeypatch.setitem(sys.modules, name, module)
    for name in ('plugins', PACKAGE, f'{PACKAGE}.utils', f'{PACKAGE}.models', f'{PACKAGE}.models.enums',
                 f'{PACKAGE}.models.pd', 'pylon', 'pylon.core', *packages):
        sqlite_db.package(name)
    Base, env.Application, env.ApplicationVersion = sqlite_db.install_parent_models(PACKAGE)
    engine = sqlite_db.sqlite_engine()
    env.session_factory = lambda: sqlite_db.orm.Session(engine, expire_on_commit=False)
    try:
        yield Base, engine
    finally:
        for key in set(sys.modules) - set(original):
            sys.modules.pop(key, None)


def install_service_stubs(env):
    """Stub the folder rights, the "Allow Run without asking" setting and the notifications the create service calls."""
    env.folder_error = None
    env.run_without_asking = False
    env.notices = []
    stubs = {
        'folder_access': {'entity_access_error': lambda *args, **kwargs: env.folder_error},
        'schedule_settings': {'is_run_without_asking_allowed': lambda project_id: env.run_without_asking},
        'schedule_notifications': {'notify_schedule_event': lambda *args, **kwargs: env.notices.append(args) or True},
    }
    for name, attributes in stubs.items():
        stub = types.ModuleType(f'{PACKAGE}.utils.{name}')
        vars(stub).update(attributes)
        sys.modules[stub.__name__] = stub


def install_tools(env, Base, **extra):
    """The `tools` module of the platform: admin user, modes, the SQLite session; `extra` adds or replaces members."""
    tools = sqlite_db.LenientModule('tools')
    tools.auth = types.SimpleNamespace(current_user=lambda: {'id': ADMIN_ID})
    tools.config = types.SimpleNamespace(
        ADMINISTRATION_MODE='administration', DEFAULT_MODE='default', POSTGRES_TENANT_SCHEMA='tenant',
    )
    tools.db = types.SimpleNamespace(get_session=lambda project_id=None: env.session_factory(), Base=Base)
    tools.this = types.SimpleNamespace(module=types.SimpleNamespace(descriptor=types.SimpleNamespace(config={})))
    vars(tools).update(extra)
    sys.modules['tools'] = tools


class Env:
    def import_text(self, text):
        return self.module.import_schedules(PROJECT_ID, ADMIN_ID, self.module.parse_schedules_file(text))

    def import_items(self, *items, **header):
        return self.import_text(file_text(*items, **header))

    def rows(self):
        with self.session_factory() as session:
            return session.query(self.EntitySchedule).order_by(self.EntitySchedule.id).all()

    def add_entity(self, application_id, name, version_name='base', agent_type='openai'):
        with self.session_factory() as session:
            session.add(self.Application(id=application_id, name=name))
            session.flush()
            session.add(self.ApplicationVersion(
                id=application_id * 10, application_id=application_id, name=version_name, agent_type=agent_type,
            ))
            session.commit()

    def add_version(self, application_id, version_id, name):
        with self.session_factory() as session:
            session.add(self.ApplicationVersion(
                id=version_id, application_id=application_id, name=name, agent_type='openai',
            ))
            session.commit()


@pytest.fixture
def env(monkeypatch):
    env = Env()
    with sandbox(monkeypatch, env) as (Base, engine):
        sqlite_db.install_trace_span(types.SimpleNamespace(set_attribute=lambda key, value: None))
        install_tools(env, Base)
        install_service_stubs(env)

        load = load_plugin_module
        env.EntitySchedule = sqlite_db.create_schedule_table(load, Base, engine)
        load('utils/constants.py', 'utils.constants')
        load('utils/schedule_timing.py', 'utils.schedule_timing')
        load('utils/schedule_access.py', 'utils.schedule_access')
        load('utils/pipeline_utils.py', 'utils.pipeline_utils')
        load('models/pd/index.py', 'models.pd.index')
        load('models/pd/entity_schedule.py', 'models.pd.entity_schedule')
        env.WRONG_HEADER = load('models/pd/schedule_yaml.py', 'models.pd.schedule_yaml').WRONG_HEADER
        env.service = load('utils/schedule_service.py', 'utils.schedule_service')
        env.module = load('utils/schedule_yaml.py', 'utils.schedule_yaml')
        env.add_entity(1, 'Entity')
        env.add_entity(2, 'Flow', agent_type='pipeline')
        yield env


# --- R3.1, R3.2: the file as a whole

@pytest.mark.parametrize('text', ['a: [', '- 1\n- 2\n', '', 'just text'])
def test_7011_r3_1_a_file_that_is_not_a_yaml_mapping_raises_the_schedule_message(env, text):
    with pytest.raises(ValueError, match=NOT_YAML):
        env.module.parse_schedules_file(text)


def test_7011_r3_1_a_file_above_the_size_limit_raises_the_size_message(env):
    text = file_text(item(prompt='x' * (1024 * 1024)))
    with pytest.raises(ValueError, match=TOO_LARGE):
        env.module.parse_schedules_file(text)


def test_7011_r3_1_a_file_just_under_the_size_limit_is_accepted_and_reports_per_item(env):
    padding = 1024 * 1024 - len(file_text(item(prompt='x')).encode())
    text = file_text(item(prompt='x' * padding))
    assert len(text.encode()) == 1024 * 1024 - 1
    report = env.import_text(text)
    assert [entry['name'] for entry in report['created']] == ['Daily digest']


@pytest.mark.parametrize('header', [
    {'kind': 'other'}, {'schema_version': 3}, {'kind': None}, {'schema_version': '1'}, {'schema_version': True},
])
def test_7011_r3_2_a_wrong_header_raises_the_header_message(env, header):
    with pytest.raises(ValueError, match=env.WRONG_HEADER):
        env.module.parse_schedules_file(file_text(item(), **header))


@pytest.mark.parametrize('missing', ['kind', 'schema_version'])
def test_7011_r3_2_a_missing_header_key_raises_the_header_message(env, missing):
    data = {**HEADER, 'schedules': []}
    del data[missing]
    with pytest.raises(ValueError, match=env.WRONG_HEADER):
        env.module.parse_schedules_file(yaml.safe_dump(data))


def test_7011_r3_1_a_file_with_500_items_is_accepted_and_one_with_501_raises_the_count_message(env):
    assert len(env.module.parse_schedules_file(file_text(*[item()] * 500)).schedules) == 500
    with pytest.raises(ValueError, match=TOO_MANY):
        env.module.parse_schedules_file(file_text(*[item()] * 501))


# --- R2.1, R2.2, R2.6: a valid item

def test_7011_r2_1_import_creates_a_disabled_schedule_owned_by_the_admin(env):
    report = env.import_items(item(description='Digest'))
    (row,) = env.rows()
    assert (row.enabled, row.created_by, row.updated_by, row.principal) == (
        False, ADMIN_ID, ADMIN_ID, {'kind': 'delegation', 'ref': ADMIN_ID},
    )
    assert (row.name, row.description, row.cron, row.timezone) == ('Daily digest', 'Digest', '0 9 * * 1-5', 'Europe/Berlin')
    assert report == {
        'created': [{'name': 'Daily digest', 'schedule_id': row.id}], 'updated': [], 'skipped': [], 'failed': [],
    }


def test_7011_r2_2_import_resolves_the_entity_and_version_by_name(env):
    env.add_entity(3, 'Other', version_name='v2')
    env.import_items(item(entity_name='Other', version_name='v2'))
    (row,) = env.rows()
    assert (row.application_id, row.version_id) == (3, 30)


def test_7011_r2_2_import_finds_a_pipeline_by_its_type(env):
    env.add_entity(3, 'Flow', agent_type='openai')  # an agent with the pipeline's name
    env.import_items(pipeline_item())
    assert [(row.entity_type, row.application_id) for row in env.rows()] == [('pipeline', 2)]


def test_7011_r2_1_import_keeps_the_run_mode_the_variables_and_the_stop_date(env):
    env.import_items(pipeline_item(variables=[]), item(
        name='Vars', prompt='Hi {{ who }}', variables=[{'name': 'who', 'value': 'Ann'}], expires_at='2999-03-05',
    ))
    pipeline, agent = env.rows()
    assert (pipeline.run_mode, agent.run_mode) == ('prompt', None)
    assert agent.variables == [{'name': 'who', 'value': 'Ann'}]
    assert agent.expires_at.date().isoformat() in ('2999-03-04', '2999-03-05')


# --- R2.3: names that do not match

@pytest.mark.parametrize('changes, reason', [
    ({'entity_name': 'Report bot'}, "Agent 'Report bot' not found in this project"),
    ({'version_name': 'v9'}, "Version 'v9' of agent 'Entity' not found"),
    ({'version_name': None}, "Version of agent 'Entity' is not named"),
])
def test_7011_r2_3_an_unmatched_name_is_skipped_with_the_reason(env, changes, reason):
    report = env.import_items(item(**changes))
    assert report['skipped'] == [{'name': 'Daily digest', 'reason': reason}]
    assert env.rows() == []


def test_7011_r2_3_a_name_found_twice_is_skipped(env):
    env.add_entity(3, 'Entity')
    report = env.import_items(item())
    assert report['skipped'] == [{'name': 'Daily digest', 'reason': "Agent 'Entity' found more than once"}]
    assert report['created'] == []


def test_7011_r2_3_an_agent_name_does_not_match_a_pipeline(env):
    report = env.import_items(item(entity_name='Flow'))
    assert [entry['name'] for entry in report['skipped']] == ['Daily digest']


# --- R2.4, R3.5: failed items, the next item goes on

def test_7011_r2_4_a_failed_item_does_not_stop_the_next_one(env):
    report = env.import_items(
        item(name='Too often', cron='* * * * *'), item(name='Owner', owner=12), item(name='Good'),
    )
    assert [entry['name'] for entry in report['created']] == ['Good']
    failed = {entry['name']: entry['reason'] for entry in report['failed']}
    assert set(failed) == {'Too often', 'Owner'}
    assert 'at least' in failed['Too often']
    assert failed['Owner'].startswith('owner')
    assert [row.name for row in env.rows()] == ['Good']


def test_7011_r2_4_an_item_without_a_required_key_fails(env):
    broken = item()
    del broken['cron']
    report = env.import_items(broken)
    assert report['failed'][0]['reason'].startswith('cron')


def test_7011_r2_4_a_folder_rights_denial_makes_the_item_failed(env):
    env.folder_error = ({'error': 'You cannot write to this folder'}, 403)
    report = env.import_items(item())
    assert report['failed'] == [{'name': 'Daily digest', 'reason': 'You cannot write to this folder'}]
    assert report['skipped'] == []


@pytest.mark.parametrize('changes, reason', [
    ({'run_mode': None}, 'Choose entry_point or prompt as the run mode of a pipeline'),
    ({'run_mode': 'chat'}, 'run_mode'),
    ({'run_mode': 'entry_point'}, 'Entry point runs take no prompt or variables'),
])
def test_7011_r3_5_a_pipeline_item_without_a_valid_run_mode_fails_and_the_agent_item_goes_on(env, changes, reason):
    report = env.import_items(pipeline_item(**changes), item(name='Agent'))
    assert [entry['name'] for entry in report['created']] == ['Agent']
    assert [entry['name'] for entry in report['failed']] == ['Report flow']
    assert reason in report['failed'][0]['reason']
    assert [row.entity_type for row in env.rows()] == ['agent']


def test_7011_r2_4_a_database_error_fails_the_item_with_a_generic_reason_and_the_next_item_goes_on(env, monkeypatch):
    real = env.service.create_schedule

    def broken_once(session, project_id, body, scope, **kwargs):
        if body.name == 'Broken':
            raise OperationalError('INSERT secret_table', {}, Exception('connection lost'))
        return real(session, project_id, body, scope, **kwargs)

    monkeypatch.setattr(env.service, 'create_schedule', broken_once)
    report = env.import_items(item(name='Broken'), item(name='Good'))
    assert report['failed'] == [{'name': 'Broken', 'reason': 'The schedule could not be saved'}]
    assert [entry['name'] for entry in report['created']] == ['Good']
    assert [row.name for row in env.rows()] == ['Good']


def test_7011_r2_2_import_reads_entity_names_with_one_query(env):
    from sqlalchemy import event
    selects = []
    engine = env.session_factory().get_bind()
    event.listen(engine, 'before_cursor_execute', lambda conn, cur, stmt, *a: selects.append(stmt))
    env.import_items(*[item(name=f'S{n}') for n in range(5)])
    assert sum('applications.name IN' in stmt for stmt in selects) == 1


# --- R2.5: run without asking

@pytest.mark.parametrize('allowed, stored, noted', [
    (False, 'pause_and_ask', True), (True, 'run_without_asking', False),
])
def test_7011_r2_5_run_without_asking_needs_the_project_setting(env, allowed, stored, noted):
    env.run_without_asking = allowed
    report = env.import_items(item(approval_rule='run_without_asking'))
    assert env.rows()[0].approval_rule == stored
    assert ('note' in report['created'][0]) is noted


def test_7011_r2_5_pause_and_ask_needs_no_note(env):
    assert 'note' not in env.import_items(item())['created'][0]


# --- R2.6: the report

def test_7011_r2_6_the_report_lists_created_updated_skipped_and_failed_items(env):
    env.import_items(item(name='Again'))
    report = env.import_items(
        item(name='Good'), item(name='Again'), item(name='Gone', entity_name='Nobody'), item(name='Bad', cron='x'),
    )
    assert {key: [entry['name'] for entry in value] for key, value in report.items()} == {
        'created': ['Good'], 'updated': ['Again'], 'skipped': ['Gone'], 'failed': ['Bad'],
    }


# --- R4, R5: an item that matches a stored schedule updates it

KEY = str(uuid.UUID(int=1))


def keyed(**overrides):
    return item(key=KEY, **overrides)


def test_7011_r4_1_a_second_import_of_the_same_file_updates_and_creates_nothing(env):
    env.import_items(keyed(), schema_version=2)
    with env.session_factory() as session:  # another owner took it over and turned it on
        (stored,) = session.query(env.EntitySchedule).all()
        stored.created_by, stored.enabled = 42, True
        session.commit()
    report = env.import_items(keyed(cron='0 7 * * *', description='New'), schema_version=2)
    (row,) = env.rows()
    assert report == {'created': [], 'updated': [{'name': 'Daily digest', 'schedule_id': row.id}], 'skipped': [], 'failed': []}
    assert (row.id, row.cron, row.description) == (stored.id, '0 7 * * *', 'New')
    assert (row.created_by, row.principal, row.enabled, row.updated_by) == (
        42, {'kind': 'delegation', 'ref': ADMIN_ID}, True, ADMIN_ID,
    )
    assert len(env.notices) == 1  # R5.5: the owner hears of the admin change


def test_7011_r4_1_a_key_of_a_schedule_of_another_agent_fails_and_keeps_the_schedule(env):
    env.add_entity(3, 'Other')
    env.import_items(keyed(), schema_version=2)
    report = env.import_items(keyed(entity_name='Other', cron='0 7 * * *'), schema_version=2)
    assert [entry['name'] for entry in report['failed']] == ['Daily digest']
    assert [(row.application_id, row.cron) for row in env.rows()] == [(1, '0 9 * * 1-5')]


def test_7011_r4_1_an_update_keeps_the_approval_downgrade_note(env):
    env.import_items(item())
    report = env.import_items(item(approval_rule='run_without_asking'))
    assert 'note' in report['updated'][0]
    assert env.rows()[0].approval_rule == 'pause_and_ask'


def test_7011_r4_2_a_keyless_item_updates_by_natural_key(env):
    env.import_items(item())
    report = env.import_items(item(prompt='Changed'))
    (row,) = env.rows()
    assert [entry['schedule_id'] for entry in report['updated']] == [row.id]
    assert (row.prompt, row.enabled, row.name) == ('Changed', False, 'Daily digest')


def test_7011_r4_2_a_keyless_duplicate_in_one_file_updates_the_first_row(env):
    report = env.import_items(item(), item(prompt='Second'))
    assert ([len(report['created']), len(report['updated'])], [row.prompt for row in env.rows()]) == ([1, 1], ['Second'])


def test_7011_r4_3_a_natural_key_found_twice_is_skipped(env):
    env.import_items(keyed(), item(key=str(uuid.UUID(int=2))), schema_version=2)
    report = env.import_items(item(prompt='Changed'))
    assert [entry['name'] for entry in report['skipped']] == ['Daily digest']
    assert [row.prompt for row in env.rows()] == ['Summarise', 'Summarise']


def test_7011_r4_4_a_repeated_key_fails_the_second_item(env):
    report = env.import_items(keyed(), keyed(name='Copy', prompt='Second'), schema_version=2)
    assert [entry['name'] for entry in report['created']] == ['Daily digest']
    assert [entry['name'] for entry in report['failed']] == ['Copy']
    assert [(row.name, row.prompt) for row in env.rows()] == [('Daily digest', 'Summarise')]


def test_7011_r4_5_a_new_key_is_stored_and_matches_next_import(env):
    env.import_items(keyed(), schema_version=2)
    assert str(env.rows()[0].uuid) == KEY
    report = env.import_items(keyed(name='Renamed in the file'), schema_version=2)
    assert len(report['updated']) == 1
    assert [row.name for row in env.rows()] == ['Daily digest']  # R5.1: the name stays


def test_7011_r5_6_a_version_1_file_without_keys_still_imports(env):
    report = env.import_items(item(), schema_version=1)
    assert [entry['name'] for entry in report['created']] == ['Daily digest']


# CR-04 follow-up: an unchanged item makes no write and sends no notice (R4.1, R5.5)
QUIET_RUN = datetime.datetime(2999, 1, 1, 9, 0)


def import_owned_by_another(env, **overrides):
    """Import one keyed item, then give the row to owner 42 with a marked next run; return the item."""
    unchanged = keyed(
        prompt='Hi {{ who }}', variables=[{'name': 'who', 'value': 'Ann'}], expires_at='2999-03-05',
        approval_rule='run_without_asking', **overrides,
    )
    env.import_items(unchanged, schema_version=2)
    with env.session_factory() as session:
        (stored,) = session.query(env.EntitySchedule).all()
        stored.created_by, stored.updated_by, stored.next_run = 42, 42, QUIET_RUN
        session.commit()
    return unchanged


def test_7011_r4_1_a_second_import_of_an_unchanged_item_is_quiet_and_still_reported_as_updated(env):
    unchanged = import_owned_by_another(env)
    report = env.import_items(unchanged, schema_version=2)
    (row,) = env.rows()
    assert [entry['schedule_id'] for entry in report['updated']] == [row.id]
    assert (row.updated_by, row.next_run, env.notices) == (42, QUIET_RUN, [])


def test_7011_r5_5_an_import_that_changes_the_cron_still_sends_one_notice(env):
    changed = {**import_owned_by_another(env), 'cron': '0 7 * * *'}
    report = env.import_items(changed, schema_version=2)
    (row,) = env.rows()
    assert [entry['schedule_id'] for entry in report['updated']] == [row.id]
    assert (row.cron, row.updated_by, len(env.notices)) == ('0 7 * * *', ADMIN_ID, 1)
    assert row.next_run != QUIET_RUN


# P2 review: a key update that moves the version also moves the natural key for later items of the file
def test_7011_r4_2_a_keyless_item_after_a_key_update_of_the_version_updates_that_row(env):
    env.add_version(1, 12, 'v2')
    env.import_items(keyed(), schema_version=2)
    report = env.import_items(keyed(version_name='v2', cron='0 7 * * *'), item(version_name='v2'), schema_version=2)
    (row,) = env.rows()
    assert [len(report[group]) for group in ('created', 'updated', 'skipped', 'failed')] == [0, 2, 0, 0]
    assert row.version_id == 12


# P3 review: an updated entry names the stored schedule, which a key match never renames
def test_7011_r4_1_an_updated_entry_names_the_stored_schedule(env):
    env.import_items(keyed(), schema_version=2)
    report = env.import_items(keyed(name='Renamed in the file'), schema_version=2)
    assert [entry['name'] for entry in report['updated']] == ['Daily digest']
