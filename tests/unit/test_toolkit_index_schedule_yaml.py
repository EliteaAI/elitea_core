"""Toolkit Index schedules in the schedule YAML file: the export shape and the import of an index item, on SQLite.

The export and import modules, the create and update services, the body models and the schedule table are real.
The vector store read (`read_index_configuration`) is an autospec of the real function.

Run via:
    python tests/run_tests.py unit/test_toolkit_index_schedule_yaml.py -v
"""
import sys
import types
from datetime import UTC, datetime
from unittest import mock

import pytest
import yaml
from fixtures import schedule_sqlite as sqlite_db
from unit import test_7011_schedule_yaml_import as import_tests
from unit.test_7011_schedule_yaml_import import ADMIN_ID, PACKAGE, PROJECT_ID, item, load_plugin_module, sandbox

import_env = import_tests.env  # the fixture of the import tests: real service, SQLite, stubbed neighbours

INDEX_KEYS = ['key', 'name', 'description', 'entity_type', 'entity_name', 'index_name', 'cron', 'timezone', 'expires_at']
AGENT_KEYS = [
    'key', 'name', 'description', 'entity_type', 'entity_name', 'version_name', 'run_mode', 'cron', 'timezone',
    'prompt', 'variables', 'expires_at', 'approval_rule',
]
TOOLKIT_ID = 7
TOOLKIT_NAME = 'Docs toolkit'
INDEX_SCHEDULED = 'This index already has a schedule'


def index_item(**overrides):
    values = {
        'name': 'Docs refresh', 'description': None, 'entity_type': 'toolkit_index', 'entity_name': TOOLKIT_NAME,
        'index_name': 'docs', 'cron': '0 3 * * *', 'timezone': 'Europe/Berlin', 'expires_at': None,
    }
    return {**values, **overrides}


def index_fields(**overrides):
    """The columns of a stored Toolkit Index row."""
    return {
        'entity_type': 'toolkit_index', 'application_id': None, 'version_id': None, 'toolkit_id': TOOLKIT_ID,
        'index_name': 'docs', 'name': 'Docs refresh', 'prompt': None, 'timezone': 'Europe/Berlin', 'cron': '0 3 * * *',
        **overrides,
    }


def add_toolkit(session_factory, toolkit_id, name):
    toolkit_model = sys.modules[f'{PACKAGE}.models.all'].EliteATool
    with session_factory() as session:
        session.add(toolkit_model(id=toolkit_id, name=name))
        session.commit()


# --- export

class ExportEnv:
    def seed(self, created_by=42, **fields):
        return sqlite_db.seed_schedule(self.session_factory, self.EntitySchedule, created_by, **fields)

    def seed_index(self, created_by=42, **overrides):
        return self.seed(created_by, **index_fields(**overrides))

    def items(self, ids=None):
        return yaml.safe_load(self.module.export_schedules_yaml(PROJECT_ID, ids))['schedules']


@pytest.fixture
def export_env(monkeypatch):
    env = ExportEnv()
    with sandbox(monkeypatch, env) as (Base, engine):
        tools = sqlite_db.LenientModule('tools')
        tools.db = types.SimpleNamespace(get_session=lambda project_id=None: env.session_factory(), Base=Base)
        sys.modules['tools'] = tools
        service = types.ModuleType(f'{PACKAGE}.utils.schedule_service')

        class ScheduleError(Exception):
            def __init__(self, code, text, status=400, **extra):
                super().__init__(text)
                self.response = {'code': code, 'error': text, **extra}, status

        service.ScheduleError = env.ScheduleError = ScheduleError
        sys.modules[service.__name__] = service
        env.EntitySchedule = sqlite_db.create_schedule_table(load_plugin_module, Base, engine)
        load_plugin_module('utils/constants.py', 'utils.constants')
        load_plugin_module('utils/schedule_timing.py', 'utils.schedule_timing')
        load_plugin_module('utils/pipeline_utils.py', 'utils.pipeline_utils')
        env.module = load_plugin_module('utils/schedule_yaml.py', 'utils.schedule_yaml')
        sqlite_db.add_agent_and_pipeline(env.session_factory, env.Application, env.ApplicationVersion)
        add_toolkit(env.session_factory, TOOLKIT_ID, TOOLKIT_NAME)
        yield env


def test_export_writes_an_index_item_with_the_toolkit_name_and_the_index_name_and_no_ids(export_env):
    export_env.seed_index(description='Nightly', expires_at=datetime(2026, 12, 31, 22, 59, 59, tzinfo=UTC))
    [exported] = export_env.items()
    assert list(exported) == INDEX_KEYS
    assert exported['entity_type'] == 'toolkit_index'
    assert (exported['entity_name'], exported['index_name']) == (TOOLKIT_NAME, 'docs')
    assert (exported['cron'], exported['timezone']) == ('0 3 * * *', 'Europe/Berlin')
    assert exported['expires_at'].isoformat() == '2026-12-31'  # the local stop day, not the UTC instant


def test_export_keeps_the_agent_item_keys_next_to_an_index_item(export_env):
    export_env.seed(name='Agent run')
    export_env.seed_index()
    agent, index = export_env.items()
    assert list(agent) == AGENT_KEYS
    assert 'index_name' not in agent
    assert list(index) == INDEX_KEYS


def test_export_by_ids_writes_the_index_schedule_and_names_an_unknown_id(export_env):
    export_env.seed(name='Agent run')
    index_id = export_env.seed_index()
    assert [exported['index_name'] for exported in export_env.items([index_id])] == ['docs']
    with pytest.raises(export_env.ScheduleError, match='Unknown schedule ids: 9999'):
        export_env.module.export_schedules_yaml(PROJECT_ID, [index_id, 9999])


def test_export_leaves_out_an_index_schedule_whose_toolkit_is_gone(export_env):
    export_env.seed_index(toolkit_id=404)
    assert export_env.items() == []


# --- import

class ImportEnv:
    """The import test environment plus a toolkit store: `stored` maps an index name to its saved settings."""

    def __init__(self, base):
        self.base = base
        self.stored = {'docs': {'chunk_size': 500}, 'wiki': {'chunk_size': 200}}
        self.unreachable = False
        self.reads = []

    def __getattr__(self, name):
        return getattr(self.base, name)

    @property
    def folder_error(self):
        return self.base.folder_error

    @folder_error.setter
    def folder_error(self, value):  # the folder rights stub reads it from the base environment
        self.base.folder_error = value

    def read(self, project_id, toolkit, index_name):
        self.reads.append((project_id, toolkit.id, index_name))
        if self.unreachable:
            raise ConnectionError('store down')
        return self.stored.get(index_name)

    def seed_index(self, created_by=42, **overrides):
        return sqlite_db.seed_schedule(self.session_factory, self.EntitySchedule, created_by, **index_fields(**overrides))

    def index_rows(self):
        return [row for row in self.rows() if row.entity_type == 'toolkit_index']


@pytest.fixture
def env(import_env, monkeypatch):
    folder_access = sys.modules[f'{PACKAGE}.utils.folder_access']
    folder_access.TOOLKIT_ENTITY_TYPES = ('toolkit',)
    load_plugin_module('utils/schedule_test_runs.py', 'utils.schedule_test_runs')  # the index insert reads its error codes
    holder = ImportEnv(import_env)
    reader = mock.create_autospec(import_env.service.read_index_configuration, side_effect=holder.read)
    monkeypatch.setattr(import_env.service, 'read_index_configuration', reader)
    add_toolkit(import_env.session_factory, TOOLKIT_ID, TOOLKIT_NAME)
    holder.reader = reader
    return holder


def test_import_creates_a_paused_index_schedule_owned_by_the_admin_with_the_toolkit_and_index(env):
    report = env.import_items(index_item(description='Nightly', expires_at='2027-01-31'), schema_version=2)
    assert [entry['name'] for entry in report['created']] == ['Docs refresh']
    assert not (report['skipped'] or report['failed'] or report['updated'])
    [row] = env.index_rows()
    assert (row.toolkit_id, row.index_name) == (TOOLKIT_ID, 'docs')
    assert (row.application_id, row.version_id, row.run_mode, row.prompt) == (None, None, None, None)
    assert (row.enabled, row.created_by, row.principal) == (False, ADMIN_ID, {'kind': 'delegation', 'ref': ADMIN_ID})
    assert (row.description, row.cron, row.timezone) == ('Nightly', '0 3 * * *', 'Europe/Berlin')
    assert row.expires_at is not None
    assert env.reads[0] == (PROJECT_ID, TOOLKIT_ID, 'docs')


def test_import_keeps_the_key_of_a_new_index_schedule(env):
    key = '0b5f4f6e-8e59-4b0e-9f3a-5f1c2f0f9a11'
    report = env.import_items(index_item(key=key), schema_version=2)
    assert len(report['created']) == 1
    assert [str(row.uuid) for row in env.index_rows()] == [key]


def test_import_updates_the_schedule_of_the_index_when_the_item_has_no_key(env):
    stored_id = env.seed_index(enabled=True, name='Kept name', description='old')
    report = env.import_items(index_item(name='Other name', description='new', cron='30 4 * * *'), schema_version=2)
    assert [entry['schedule_id'] for entry in report['updated']] == [stored_id]
    [row] = env.index_rows()
    assert (row.name, row.description, row.cron, row.enabled) == ('Kept name', 'new', '30 4 * * *', True)
    assert env.reads == []  # an existing schedule needs no read of the store


def test_import_updates_the_schedule_a_key_names(env):
    stored_id = env.seed_index()
    [stored] = env.index_rows()
    report = env.import_items(index_item(key=str(stored.uuid), timezone='Etc/UTC', expires_at='2027-03-01'),
                              schema_version=2)
    assert [entry['schedule_id'] for entry in report['updated']] == [stored_id]
    [row] = env.index_rows()
    assert row.timezone == 'Etc/UTC'
    assert row.expires_at is not None


def test_import_of_an_unchanged_index_item_writes_nothing(env):
    env.seed_index(created_by=42)
    [before] = env.index_rows()
    report = env.import_items(index_item(), schema_version=2)
    assert len(report['updated']) == 1
    [after] = env.index_rows()
    assert (after.updated_by, after.updated_at) == (before.updated_by, before.updated_at)
    assert after.updated_by != ADMIN_ID


def test_import_skips_an_item_for_an_index_the_toolkit_does_not_have(env):
    report = env.import_items(index_item(index_name='ghost'), schema_version=2)
    assert report['skipped'] == [{'name': 'Docs refresh', 'reason': "Index 'ghost' not found in toolkit 'Docs toolkit'"}]
    assert not (report['created'] or report['failed'])
    assert env.index_rows() == []


def test_import_skips_an_item_for_an_unknown_toolkit(env):
    report = env.import_items(index_item(entity_name='No such toolkit'), schema_version=2)
    assert report['skipped'] == [{'name': 'Docs refresh', 'reason': "Toolkit 'No such toolkit' not found in this project"}]
    assert env.reads == []


def test_import_skips_an_item_whose_toolkit_name_is_found_twice(env):
    add_toolkit(env.session_factory, 8, TOOLKIT_NAME)
    report = env.import_items(index_item(), schema_version=2)
    assert report['skipped'] == [{'name': 'Docs refresh', 'reason': f"Toolkit '{TOOLKIT_NAME}' found more than once"}]
    assert env.index_rows() == []


def test_import_does_not_look_up_a_toolkit_name_as_an_agent_name(env):
    report = env.import_items(index_item(entity_name='Entity'), item(), schema_version=2)  # 'Entity' is the agent
    assert [entry['reason'] for entry in report['skipped']] == ["Toolkit 'Entity' not found in this project"]
    assert [entry['name'] for entry in report['created']] == ['Daily digest']


def test_import_fails_an_item_for_an_index_that_already_has_a_schedule_under_another_key(env):
    env.seed_index()
    report = env.import_items(index_item(key='0b5f4f6e-8e59-4b0e-9f3a-5f1c2f0f9a22'), schema_version=2)
    assert report['failed'] == [{'name': 'Docs refresh', 'reason': INDEX_SCHEDULED}]
    assert len(env.index_rows()) == 1


def test_import_creates_one_schedule_for_two_new_items_of_the_same_index(env):
    report = env.import_items(
        index_item(key='0b5f4f6e-8e59-4b0e-9f3a-5f1c2f0f9a31'),
        index_item(key='0b5f4f6e-8e59-4b0e-9f3a-5f1c2f0f9a32', name='Second'),
        schema_version=2,
    )
    assert len(report['created']) == 1
    assert report['failed'] == [{'name': 'Second', 'reason': INDEX_SCHEDULED}]
    assert len(env.index_rows()) == 1


def test_import_fails_the_item_when_the_store_cannot_be_read(env):
    env.unreachable = True
    report = env.import_items(index_item(), schema_version=2)
    assert report['failed'] == [{'name': 'Docs refresh', 'reason': 'The index could not be checked; try again'}]
    assert env.index_rows() == []


def test_import_fails_an_item_the_admin_may_not_write_with_the_service_refusal(env):
    env.folder_error = ({'error': 'Toolkit not found'}, 404)
    report = env.import_items(index_item(), schema_version=2)
    assert report['failed'] == [{'name': 'Docs refresh', 'reason': 'Toolkit not found'}]
    assert env.index_rows() == []


def test_import_fails_an_index_item_with_an_interval_below_the_index_floor(env):
    report = env.import_items(index_item(cron='* * * * *'), schema_version=2)
    assert len(report['failed']) == 1
    assert 'at least' in report['failed'][0]['reason']
    assert env.index_rows() == []


@pytest.mark.parametrize('field, value', [
    ('version_name', 'base'), ('run_mode', 'prompt'), ('prompt', 'Go'),
    ('variables', [{'name': 'a', 'value': 'b'}]), ('approval_rule', 'run_without_asking'),
])
def test_import_fails_an_index_item_with_a_run_field(env, field, value):
    report = env.import_items(index_item(**{field: value}), schema_version=2)
    assert report['failed'] == [{'name': 'Docs refresh', 'reason': f'A Toolkit Index schedule takes no {field}'}]
    assert env.index_rows() == []


def test_import_fails_an_index_item_without_an_index_name(env):
    values = index_item()
    del values['index_name']
    report = env.import_items(values, schema_version=2)
    assert report['failed'] == [{'name': 'Docs refresh', 'reason': 'A Toolkit Index schedule needs an index_name'}]


def test_import_fails_an_agent_item_that_names_an_index(env):
    report = env.import_items(item(index_name='docs'), schema_version=2)
    assert report['failed'] == [{'name': 'Daily digest', 'reason': 'Only a Toolkit Index schedule takes an index_name'}]


def test_import_refuses_the_key_of_an_agent_schedule_for_an_index_item(env):
    sqlite_db.seed_schedule(env.session_factory, env.EntitySchedule, 42, name='Agent run')
    [agent_row] = [row for row in env.rows() if row.entity_type == 'agent']
    report = env.import_items(index_item(key=str(agent_row.uuid)), schema_version=2)
    assert report['failed'] == [{
        'name': 'Docs refresh', 'reason': 'The key belongs to a schedule of another index, agent or pipeline',
    }]


def test_import_refuses_the_key_of_an_index_schedule_for_an_agent_item(env):
    env.seed_index()
    [index_row] = env.index_rows()
    report = env.import_items(item(key=str(index_row.uuid)), schema_version=2)
    assert report['failed'] == [{
        'name': 'Daily digest', 'reason': 'The key belongs to a schedule of another agent or pipeline',
    }]


def test_import_refuses_the_key_of_another_index_for_an_index_item(env):
    env.seed_index(index_name='wiki', name='Wiki refresh')
    [wiki_row] = env.index_rows()
    report = env.import_items(index_item(key=str(wiki_row.uuid)), schema_version=2)
    assert [entry['name'] for entry in report['failed']] == ['Docs refresh']
    assert env.reads == []


def test_import_handles_index_agent_and_pipeline_items_of_one_file(env):
    report = env.import_items(index_item(), item(), schema_version=2)
    assert sorted(entry['name'] for entry in report['created']) == ['Daily digest', 'Docs refresh']
    assert not (report['skipped'] or report['failed'])
