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
        self.old_owner = False  # a configurations plugin whose create and update know no owner argument
        self.old_register = False  # a configurations plugin from before owned types
        self.register_errors = []  # one error per register call, in order; None lets the call pass
        self.logged = []

    def timeout(self, _seconds):
        return self

    def _call(self, name, **kwargs):
        self.calls.append((name, kwargs))
        if self.error and not (name == 'register' and self.register_errors):
            raise self.error

    def configurations_get_first_filtered_project(self, project_id, filter_fields):
        self._call('get', project_id=project_id, filter_fields=filter_fields)
        return self.records.get((project_id, filter_fields['elitea_title']))

    def _owner_check(self, kwargs):
        if self.old_owner and 'owner' in kwargs:
            raise TypeError("got an unexpected keyword argument 'owner'")

    def configurations_create_if_not_exists(self, payload, **kwargs):
        self._call('create', payload=payload, **kwargs)
        self._owner_check(kwargs)
        key = (payload['project_id'], payload['elitea_title'])
        if key in self.records:
            return self.records[key], False
        self.records[key] = {'id': 5, **payload}
        return self.records[key], True

    def configurations_update(self, project_id, config_id, payload, **kwargs):
        self._call('update', project_id=project_id, config_id=config_id, payload=payload, **kwargs)
        self._owner_check(kwargs)
        record = next(r for r in self.records.values() if r['id'] == config_id)
        record.update(payload)
        return record

    def configurations_register(self, **kwargs):
        self._call('register', **kwargs)
        if self.register_errors:
            error = self.register_errors.pop(0)
            if error:
                raise error
        if self.old_register and 'managed_by' in kwargs:
            raise TypeError("configurations_register() got an unexpected keyword argument 'managed_by'")


@pytest.fixture
def store(monkeypatch):
    # monkeypatch, not isolated_sys_modules: dropping pydantic's lazy submodules breaks later constraint checks
    store = FakeConfigurations()
    tools = types.ModuleType('tools')
    tools.rpc_tools = types.SimpleNamespace(RpcMixin=lambda: types.SimpleNamespace(rpc=store))
    pylon_tools = types.ModuleType('pylon.core.tools')
    pylon_tools.log = types.SimpleNamespace(
        exception=lambda *a, **k: store.logged.append(('exception', a)),
        warning=lambda *a, **k: store.logged.append(('warning', a)),
    )
    for name, module in (('tools', tools), ('pylon', types.ModuleType('pylon')),
                         ('pylon.core', types.ModuleType('pylon.core')), ('pylon.core.tools', pylon_tools)):
        monkeypatch.setitem(sys.modules, name, module)
    name = f'{PKG}.utils.schedule_settings'
    monkeypatch.setitem(sys.modules, name, None)  # restored or removed after the test
    store.module = load_module_with_stubs(PLUGIN_ROOT / 'utils' / 'schedule_settings.py', name)
    return store


def _saved(store, value):
    store.records[(PROJECT_ID, 'schedule_settings')] = {'id': 5, 'data': {'allow_run_without_asking': value}}


def test_never_saved_is_not_allowed(store):
    assert store.module.is_run_without_asking_allowed(PROJECT_ID) is False
    assert store.calls == [('get', {'project_id': PROJECT_ID, 'filter_fields': FILTER})]


@pytest.mark.parametrize('value', [True, False])
def test_reads_the_saved_value(store, value):
    _saved(store, value)
    assert store.module.is_run_without_asking_allowed(PROJECT_ID) is value


@pytest.mark.parametrize('data', [{}, {'allow_run_without_asking': 'true'}, None])
def test_a_broken_record_is_not_allowed(store, data):
    store.records[(PROJECT_ID, 'schedule_settings')] = {'id': 5, 'data': data}
    assert store.module.is_run_without_asking_allowed(PROJECT_ID) is False


def test_a_store_error_is_not_allowed(store):
    """Fail closed: an unreadable setting never allows the rule."""
    _saved(store, True)
    store.error = RuntimeError('configurations down')
    assert store.module.is_run_without_asking_allowed(PROJECT_ID) is False


@pytest.mark.parametrize('value', [True, False])
def test_the_first_save_creates_the_project_record(store, value):
    settings = store.module.ScheduleProjectSettings(allow_run_without_asking=value)
    assert store.module.save_schedule_settings(PROJECT_ID, settings) == {'allow_run_without_asking': value}
    [(name, kwargs)] = store.calls
    assert name == 'create'
    assert kwargs['payload'] == {
        'project_id': PROJECT_ID, 'elitea_title': 'schedule_settings', 'label': 'Schedules',
        'type': 'schedule_settings', 'data': {'allow_run_without_asking': value},
    }
    assert store.module.is_run_without_asking_allowed(PROJECT_ID) is value


def test_a_later_save_updates_the_record(store):
    _saved(store, False)
    settings = store.module.ScheduleProjectSettings(allow_run_without_asking=True)
    assert store.module.save_schedule_settings(PROJECT_ID, settings) == {'allow_run_without_asking': True}
    assert store.calls[-1] == ('update', {'project_id': PROJECT_ID, 'config_id': 5,
                                          'payload': {'data': {'allow_run_without_asking': True}},
                                          'owner': 'elitea_core'})
    assert store.module.is_run_without_asking_allowed(PROJECT_ID) is True


def test_a_save_names_the_owner_on_create(store):
    store.module.save_schedule_settings(PROJECT_ID, store.module.ScheduleProjectSettings(
        allow_run_without_asking=True))
    assert store.calls[0][1]['owner'] == 'elitea_core'


@pytest.mark.parametrize('saved', [None, False])
def test_an_older_configurations_refuses_the_save_instead_of_writing_unguarded(store, saved):
    """A configurations plugin that does not know `owner` raises TypeError: the call is not repeated without it."""
    if saved is not None:
        _saved(store, saved)
    store.old_owner = True
    settings = store.module.ScheduleProjectSettings(allow_run_without_asking=True)
    with pytest.raises(TypeError):
        store.module.save_schedule_settings(PROJECT_ID, settings)
    assert all('owner' in kwargs for _name, kwargs in store.calls)
    assert store.module.is_run_without_asking_allowed(PROJECT_ID) is bool(saved)


def test_a_type_error_about_another_argument_is_raised_on_save(store):
    store.error = TypeError("unexpected keyword argument 'payload'")
    with pytest.raises(TypeError):
        store.module.save_schedule_settings(PROJECT_ID, store.module.ScheduleProjectSettings(
            allow_run_without_asking=True))
    assert len(store.calls) == 1


def test_a_save_error_propagates(store):
    store.error = RuntimeError('configurations down')
    with pytest.raises(RuntimeError):
        store.module.save_schedule_settings(PROJECT_ID, store.module.ScheduleProjectSettings(
            allow_run_without_asking=True))


PLAIN_REGISTRATION = {'type_name': 'schedule_settings', 'section': 'project_settings', 'replace': True}
OWNER = {'managed_by': 'elitea_core', 'manage_url': '/api/v2/elitea_core/schedules/{mode}/{project_id}/settings'}


def test_the_type_is_registered_as_an_owned_project_setting(store):
    """The configurations REST API then refuses writes; only the schedules settings PUT changes the record."""
    store.module.register_schedule_settings_type()
    [(name, kwargs)] = store.calls
    assert name == 'register'
    assert kwargs == {**PLAIN_REGISTRATION, 'model': store.module.ScheduleProjectSettings, **OWNER}


def test_an_older_configurations_gets_no_unguarded_type(store):
    """A plugin without owned types rejects the owner arguments: the type stays unregistered, with an error logged."""
    store.old_register = True
    store.module.register_schedule_settings_type()
    [(name, kwargs)] = store.calls
    assert name == 'register' and kwargs['managed_by'] == 'elitea_core'
    assert [level for level, _args in store.logged] == ['exception']


@pytest.mark.parametrize('error', [TimeoutError('rpc timeout'), RuntimeError('configurations down')])
def test_another_error_keeps_the_guard_and_is_logged(store, error):
    """A timeout never leads to the unguarded type: one call, one logged error, nothing raised."""
    store.register_errors = [error]
    store.module.register_schedule_settings_type()
    [(name, kwargs)] = store.calls
    assert name == 'register'
    assert kwargs['managed_by'] == 'elitea_core'
    assert [level for level, _args in store.logged] == ['exception']


def test_a_register_error_does_not_stop_the_plugin(store):
    store.error = RuntimeError('configurations down')
    store.module.register_schedule_settings_type()
    assert len(store.calls) == 1


@pytest.mark.parametrize('body', [{}, {'allow_run_without_asking': 1}, {'allow_run_without_asking': True, 'x': 1}])
def test_the_model_takes_one_strict_boolean(store, body):
    with pytest.raises(ValidationError):
        store.module.ScheduleProjectSettings.model_validate(body)
