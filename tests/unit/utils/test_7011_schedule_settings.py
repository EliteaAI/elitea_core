"""EL-7011 owner CR 2026-10-21: the project setting "Allow Run without asking" (US-17, US-18 R3.1-R3.2, US-33).

utils/schedule_settings.py loads alone; a fake configurations RPC stands in for the project settings store.
"""
import pathlib
import sys
import types

import pytest
from fixtures.helpers import load_module_with_stubs
from pydantic import ValidationError

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[3]
PKG = 'plugins.elitea_core'
PROJECT_ID = 12
FILTER = {'type': 'schedule_settings', 'elitea_title': 'schedule_settings'}


class FakeConfigurations:
    """The configurations RPCs the module calls, over one project's records."""

    def __init__(self):
        self.records = {}
        self.calls = []
        self.error = None

    def timeout(self, _seconds):
        return self

    def _call(self, name, **kwargs):
        self.calls.append((name, kwargs))
        if self.error:
            raise self.error

    def configurations_get_first_filtered_project(self, project_id, filter_fields):
        self._call('get', project_id=project_id, filter_fields=filter_fields)
        return self.records.get((project_id, filter_fields['elitea_title']))

    def configurations_create_if_not_exists(self, payload):
        self._call('create', payload=payload)
        key = (payload['project_id'], payload['elitea_title'])
        if key in self.records:
            return self.records[key], False
        self.records[key] = {'id': 5, **payload}
        return self.records[key], True

    def configurations_update(self, project_id, config_id, payload):
        self._call('update', project_id=project_id, config_id=config_id, payload=payload)
        record = next(r for r in self.records.values() if r['id'] == config_id)
        record.update(payload)
        return record

    def configurations_register(self, **kwargs):
        self._call('register', **kwargs)


@pytest.fixture
def store(monkeypatch):
    # monkeypatch, not isolated_sys_modules: dropping pydantic's lazy submodules breaks later constraint checks
    store = FakeConfigurations()
    tools = types.ModuleType('tools')
    tools.rpc_tools = types.SimpleNamespace(RpcMixin=lambda: types.SimpleNamespace(rpc=store))
    pylon_tools = types.ModuleType('pylon.core.tools')
    pylon_tools.log = types.SimpleNamespace(exception=lambda *a, **k: None)
    for name, module in (('tools', tools), ('pylon', types.ModuleType('pylon')),
                         ('pylon.core', types.ModuleType('pylon.core')), ('pylon.core.tools', pylon_tools)):
        monkeypatch.setitem(sys.modules, name, module)
    name = f'{PKG}.utils.schedule_settings'
    monkeypatch.setitem(sys.modules, name, None)  # restored or removed after the test
    store.module = load_module_with_stubs(PLUGIN_ROOT / 'utils' / 'schedule_settings.py', name)
    return store


def _saved(store, value):
    store.records[(PROJECT_ID, 'schedule_settings')] = {'id': 5, 'data': {'allow_run_without_asking': value}}


def test_el7011_cr_never_saved_is_not_allowed(store):
    assert store.module.is_run_without_asking_allowed(PROJECT_ID) is False
    assert store.calls == [('get', {'project_id': PROJECT_ID, 'filter_fields': FILTER})]


@pytest.mark.parametrize('value', [True, False])
def test_el7011_cr_reads_the_saved_value(store, value):
    _saved(store, value)
    assert store.module.is_run_without_asking_allowed(PROJECT_ID) is value


@pytest.mark.parametrize('data', [{}, {'allow_run_without_asking': 'true'}, None])
def test_el7011_cr_a_broken_record_is_not_allowed(store, data):
    store.records[(PROJECT_ID, 'schedule_settings')] = {'id': 5, 'data': data}
    assert store.module.is_run_without_asking_allowed(PROJECT_ID) is False


def test_el7011_r3_2_a_store_error_is_not_allowed(store):
    """Fail closed: an unreadable setting never allows the rule."""
    _saved(store, True)
    store.error = RuntimeError('configurations down')
    assert store.module.is_run_without_asking_allowed(PROJECT_ID) is False


@pytest.mark.parametrize('value', [True, False])
def test_el7011_cr_the_first_save_creates_the_project_record(store, value):
    settings = store.module.ScheduleProjectSettings(allow_run_without_asking=value)
    assert store.module.save_schedule_settings(PROJECT_ID, settings) == {'allow_run_without_asking': value}
    [(name, kwargs)] = store.calls
    assert name == 'create'
    assert kwargs['payload'] == {
        'project_id': PROJECT_ID, 'elitea_title': 'schedule_settings', 'label': 'Schedules',
        'type': 'schedule_settings', 'data': {'allow_run_without_asking': value},
    }
    assert store.module.is_run_without_asking_allowed(PROJECT_ID) is value


def test_el7011_cr_a_later_save_updates_the_record(store):
    _saved(store, False)
    settings = store.module.ScheduleProjectSettings(allow_run_without_asking=True)
    assert store.module.save_schedule_settings(PROJECT_ID, settings) == {'allow_run_without_asking': True}
    assert store.calls[-1] == ('update', {'project_id': PROJECT_ID, 'config_id': 5,
                                          'payload': {'data': {'allow_run_without_asking': True}}})
    assert store.module.is_run_without_asking_allowed(PROJECT_ID) is True


def test_el7011_cr_a_save_error_propagates(store):
    store.error = RuntimeError('configurations down')
    with pytest.raises(RuntimeError):
        store.module.save_schedule_settings(PROJECT_ID, store.module.ScheduleProjectSettings(
            allow_run_without_asking=True))


def test_el7011_cr_the_type_is_registered_as_a_project_setting(store):
    store.module.register_schedule_settings_type()
    [(name, kwargs)] = store.calls
    assert name == 'register'
    assert kwargs == {'type_name': 'schedule_settings', 'section': 'project_settings',
                      'model': store.module.ScheduleProjectSettings, 'replace': True}


def test_el7011_cr_a_register_error_does_not_stop_the_plugin(store):
    store.error = RuntimeError('configurations down')
    store.module.register_schedule_settings_type()


@pytest.mark.parametrize('body', [{}, {'allow_run_without_asking': 1}, {'allow_run_without_asking': True, 'x': 1}])
def test_el7011_cr_the_model_takes_one_strict_boolean(store, body):
    with pytest.raises(ValidationError):
        store.module.ScheduleProjectSettings.model_validate(body)
