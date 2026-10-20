"""EL-7011 US-22: import of a schedule YAML file (R2.1 to R2.6, R3.1, R3.2, R3.5) on SQLite.

The import module, the create service, the body models and the schedule table are real.
The plugin package around them is stubbed.

Run via:
    python tests/run_tests.py unit/test_7011_schedule_yaml_import.py -v
"""
import pathlib
import sys
import types

import pytest
import yaml
from fixtures import schedule_sqlite as sqlite_db
from fixtures.helpers import load_module_with_stubs

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
PACKAGE = 'plugins.elitea_core'
PROJECT_ID = 35
ADMIN_ID = 99
NOT_YAML = 'The schedule file is not valid YAML'
WRONG_HEADER = 'This is not a schedule file of schema version 1'
TOO_LARGE = 'The schedule file is larger than 1 MB'
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


class Env:
    def import_text(self, text):
        return self.module.import_schedules(PROJECT_ID, ADMIN_ID, self.module.parse_schedules_file(text))

    def import_items(self, *items):
        return self.import_text(file_text(*items))

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


@pytest.fixture
def env(monkeypatch):
    original = sys.modules.copy()
    for name, module in sqlite_db.REAL_SQLALCHEMY.items():
        monkeypatch.setitem(sys.modules, name, module)
    env = Env()
    for name in ('plugins', PACKAGE, f'{PACKAGE}.utils', f'{PACKAGE}.models', f'{PACKAGE}.models.enums',
                 f'{PACKAGE}.models.pd', 'pylon', 'pylon.core'):
        sqlite_db.package(name)
    Base, env.Application, env.ApplicationVersion = sqlite_db.install_parent_models(PACKAGE)
    sqlite_db.install_trace_span(types.SimpleNamespace(set_attribute=lambda key, value: None))
    engine = sqlite_db.sqlite_engine()
    env.session_factory = lambda: sqlite_db.orm.Session(engine, expire_on_commit=False)
    tools = sqlite_db.LenientModule('tools')
    tools.auth = types.SimpleNamespace(current_user=lambda: {'id': ADMIN_ID})
    tools.config = types.SimpleNamespace(
        ADMINISTRATION_MODE='administration', DEFAULT_MODE='default', POSTGRES_TENANT_SCHEMA='tenant',
    )
    tools.db = types.SimpleNamespace(get_session=lambda project_id=None: env.session_factory(), Base=Base)
    tools.this = types.SimpleNamespace(module=types.SimpleNamespace(descriptor=types.SimpleNamespace(config={})))
    sys.modules['tools'] = tools

    env.folder_error = None
    folder_access = types.ModuleType(f'{PACKAGE}.utils.folder_access')
    folder_access.entity_access_error = lambda *args, **kwargs: env.folder_error
    env.run_without_asking = False
    risk = types.ModuleType(f'{PACKAGE}.utils.schedule_risk')
    risk.is_run_without_asking_allowed = lambda project_id: env.run_without_asking
    notifications = types.ModuleType(f'{PACKAGE}.utils.schedule_notifications')
    notifications.notify_schedule_event = lambda *args, **kwargs: True
    for stub in (folder_access, risk, notifications):
        sys.modules[stub.__name__] = stub

    def load(relative_path, name):
        return load_module_with_stubs(PLUGIN_ROOT / relative_path, f'{PACKAGE}.{name}')

    env.EntitySchedule = sqlite_db.create_schedule_table(load, Base, engine)
    load('utils/constants.py', 'utils.constants')
    load('utils/schedule_timing.py', 'utils.schedule_timing')
    load('utils/schedule_access.py', 'utils.schedule_access')
    load('utils/pipeline_utils.py', 'utils.pipeline_utils')
    load('models/pd/index.py', 'models.pd.index')
    load('models/pd/entity_schedule.py', 'models.pd.entity_schedule')
    load('models/pd/schedule_yaml.py', 'models.pd.schedule_yaml')
    env.service = load('utils/schedule_service.py', 'utils.schedule_service')
    env.module = load('utils/schedule_yaml.py', 'utils.schedule_yaml')
    env.add_entity(1, 'Entity')
    env.add_entity(2, 'Flow', agent_type='pipeline')
    yield env
    for key in set(sys.modules) - set(original):
        sys.modules.pop(key, None)


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
    {'kind': 'other'}, {'schema_version': 2}, {'kind': None}, {'schema_version': '1'}, {'schema_version': True},
])
def test_7011_r3_2_a_wrong_header_raises_the_header_message(env, header):
    with pytest.raises(ValueError, match=WRONG_HEADER):
        env.module.parse_schedules_file(file_text(item(), **header))


@pytest.mark.parametrize('missing', ['kind', 'schema_version'])
def test_7011_r3_2_a_missing_header_key_raises_the_header_message(env, missing):
    data = {**HEADER, 'schedules': []}
    del data[missing]
    with pytest.raises(ValueError, match=WRONG_HEADER):
        env.module.parse_schedules_file(yaml.safe_dump(data))


# --- R2.1, R2.2, R2.6: a valid item

def test_7011_r2_1_import_creates_a_disabled_schedule_owned_by_the_admin(env):
    report = env.import_items(item(description='Digest'))
    (row,) = env.rows()
    assert (row.enabled, row.created_by, row.updated_by, row.principal) == (
        False, ADMIN_ID, ADMIN_ID, {'kind': 'delegation', 'ref': ADMIN_ID},
    )
    assert (row.name, row.description, row.cron, row.timezone) == ('Daily digest', 'Digest', '0 9 * * 1-5', 'Europe/Berlin')
    assert report == {'created': [{'name': 'Daily digest', 'schedule_id': row.id}], 'skipped': [], 'failed': []}


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


# --- R2.5: run without asking

@pytest.mark.parametrize('allowed, stored, noted', [
    (False, 'pause_and_ask', True), (True, 'run_without_asking', False),
])
def test_7011_r2_5_run_without_asking_needs_the_project_secret(env, allowed, stored, noted):
    env.run_without_asking = allowed
    report = env.import_items(item(approval_rule='run_without_asking'))
    assert env.rows()[0].approval_rule == stored
    assert ('note' in report['created'][0]) is noted


def test_7011_r2_5_pause_and_ask_needs_no_note(env):
    assert 'note' not in env.import_items(item())['created'][0]


# --- R2.6: the report

def test_7011_r2_6_the_report_lists_created_skipped_and_failed_items(env):
    report = env.import_items(item(name='Good'), item(name='Gone', entity_name='Nobody'), item(name='Bad', cron='x'))
    assert {key: [entry['name'] for entry in value] for key, value in report.items()} == {
        'created': ['Good'], 'skipped': ['Gone'], 'failed': ['Bad'],
    }
