"""EL-7011 US-22: export of schedules to a YAML file (R1.1 to R1.5) on SQLite.

The export module and the schedule table are real. The plugin package around them is stubbed.

Run via:
    python tests/run_tests.py unit/test_7011_schedule_yaml_export.py -v
"""
import sys
import types
from datetime import UTC, datetime

import pytest
import yaml
from fixtures import schedule_sqlite as sqlite_db
from unit.test_7011_schedule_yaml_import import load_plugin_module, sandbox

PACKAGE = 'plugins.elitea_core'
PROJECT_ID = 7
ALLOWED_KEYS = [
    'name', 'description', 'entity_type', 'entity_name', 'version_name', 'run_mode', 'cron', 'timezone',
    'prompt', 'variables', 'expires_at', 'approval_rule',
]


class Env:
    def seed(self, created_by=42, **fields):
        return sqlite_db.seed_schedule(self.session_factory, self.EntitySchedule, created_by, **fields)

    def export(self, ids=None):
        return yaml.safe_load(self.module.export_schedules_yaml(PROJECT_ID, ids))

    def items(self, ids=None):
        return self.export(ids)['schedules']


@pytest.fixture
def env(monkeypatch):
    env = Env()
    with sandbox(monkeypatch, env) as (Base, engine):
        tools = sqlite_db.LenientModule('tools')
        tools.db = types.SimpleNamespace(get_session=lambda project_id=None: env.session_factory(), Base=Base)
        sys.modules['tools'] = tools
        service = types.ModuleType(f'{PACKAGE}.utils.schedule_service')

        class ScheduleError(Exception):
            def __init__(self, body, status=400):
                super().__init__(body.get('error'))
                self.response = body, status

        service.ScheduleError = ScheduleError
        sys.modules[service.__name__] = service
        env.ScheduleError = ScheduleError
        env.EntitySchedule = sqlite_db.create_schedule_table(load_plugin_module, Base, engine)
        load_plugin_module('utils/pipeline_utils.py', 'utils.pipeline_utils')
        env.module = load_plugin_module('utils/schedule_yaml.py', 'utils.schedule_yaml')
        sqlite_db.add_agent_and_pipeline(env.session_factory, env.Application, env.ApplicationVersion)
        yield env


def test_7011_r1_1_export_writes_the_header_and_every_schedule_of_every_owner(env):
    env.seed(name='A', created_by=42)
    env.seed(name='B', created_by=43)
    data = env.export()
    assert (data['kind'], data['schema_version']) == ('elitea_schedules', 1)
    assert [item['name'] for item in data['schedules']] == ['A', 'B']


def test_7011_r1_1_export_of_no_schedules_writes_an_empty_list(env):
    assert env.export()['schedules'] == []


def test_7011_r1_2_export_with_ids_writes_only_those_schedules(env):
    env.seed(name='A')
    second = env.seed(name='B')
    env.seed(name='C')
    assert [item['name'] for item in env.items([second])] == ['B']


def test_7011_r1_3_export_writes_only_the_allow_listed_keys_in_order(env):
    env.seed(description='Digest', version_id=10)
    item = env.items()[0]
    assert list(item) == ALLOWED_KEYS
    assert (item['entity_name'], item['version_name'], item['description']) == ('Entity', 'base', 'Digest')


def test_7011_r1_3_export_leaves_out_owner_state_and_run_data(env):
    env.seed(last_run=datetime(2026, 1, 1, tzinfo=UTC), next_run=datetime(2026, 1, 2, tzinfo=UTC))
    text = env.module.export_schedules_yaml(PROJECT_ID, None)
    for key in ('created_by', 'principal', 'enabled', 'disabled_reason', 'next_run', 'last_run', ' id:'):
        assert key not in text


def test_7011_r1_3_export_writes_the_stop_date_as_the_local_date(env):
    # 22:30 UTC on 4 March is already 5 March in Tokyo
    env.seed(timezone='Asia/Tokyo', expires_at=datetime(2027, 3, 4, 22, 30, tzinfo=UTC))
    assert env.export()['schedules'][0]['expires_at'] == datetime(2027, 3, 5).date()


def test_7011_r1_3_export_writes_null_for_a_deleted_version(env):
    env.seed(version_id=None)
    assert env.items()[0]['version_name'] is None


def test_7011_r1_4_export_with_unknown_ids_raises_400_listing_them(env):
    known = env.seed()
    with pytest.raises(env.ScheduleError) as error:
        env.module.export_schedules_yaml(PROJECT_ID, [known, 901, 902])
    body, status = error.value.response
    assert status == 400
    assert body['error'] == 'Unknown schedule ids: 901, 902'


def test_7011_r1_5_export_writes_null_run_mode_for_an_agent(env):
    env.seed(run_mode='prompt')
    assert env.items()[0]['run_mode'] is None


def test_7011_r1_5_export_writes_no_prompt_and_no_variables_for_an_entry_point_pipeline(env):
    env.seed(entity_type='pipeline', application_id=2, version_id=11, run_mode='entry_point',
             prompt='Left over', variables=[{'name': 'x', 'value': '1'}])
    item = env.items()[0]
    assert (item['run_mode'], item['prompt'], item['variables']) == ('entry_point', None, [])


def test_7011_r1_5_export_keeps_prompt_and_variables_of_a_prompt_pipeline(env):
    env.seed(entity_type='pipeline', application_id=2, version_id=11, run_mode='prompt',
             prompt='Go {{ x }}', variables=[{'name': 'x', 'value': '1'}])
    item = env.items()[0]
    assert (item['run_mode'], item['prompt'], item['variables']) == (
        'prompt', 'Go {{ x }}', [{'name': 'x', 'value': '1'}])
