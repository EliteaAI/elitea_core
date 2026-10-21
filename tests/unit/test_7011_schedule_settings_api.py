"""EL-7011 owner CR 2026-10-21: `GET` and `PUT .../schedules/{mode}/{project_id}/settings`.

The route module and its project handlers load from source with a fake `tools`; a fake settings module
stands in for the project settings store. No Pylon runtime.
"""
import functools
import pathlib
import sys
import types

import pytest
from fixtures.helpers import load_module_with_stubs
from fixtures.schedule_sqlite import LenientModule, handler_stubs

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
PACKAGE = 'plugins.elitea_core'
PROJECT_ID = 7
LIST_PERMISSION = 'models.applications.schedules.list'
ADMIN_PERMISSION = 'models.applications.schedules.admin'
INVALID = {'error': 'allow_run_without_asking must be true or false', 'field': 'allow_run_without_asking'}


class Harness:
    def __init__(self):
        self.permissions = {LIST_PERMISSION, ADMIN_PERMISSION}
        self.saved = {}
        self.body = None
        self.specs = {}
        self.read_error = False
        self.save_error = None

    def check_api(self, spec):
        required = set(spec['permissions'])

        def decorator(func):
            self.specs[func.__name__] = spec['permissions']

            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                if not required & self.permissions:
                    return {'error': 'access_denied'}, 403
                return func(*args, **kwargs)
            return wrapper
        return decorator

    def is_allowed(self, project_id):
        assert project_id == PROJECT_ID
        return False if self.read_error else self.saved.get(project_id, False)

    def save(self, project_id, settings):
        if self.save_error:
            raise self.save_error
        self.saved[project_id] = settings.allow_run_without_asking
        return settings.model_dump()


def _package(name):
    module = types.ModuleType(name)
    module.__path__ = []
    sys.modules[name] = module


def _load(relative_path, module_name):
    return load_module_with_stubs(PLUGIN_ROOT / relative_path, module_name)


@pytest.fixture
def harness(isolated_sys_modules):
    harness = Harness()
    for name in ('plugins', PACKAGE, f'{PACKAGE}.api', f'{PACKAGE}.api.v2', f'{PACKAGE}.utils',
                 f'{PACKAGE}.models', 'pylon', 'pylon.core'):
        _package(name)
    sys.modules['pylon.core.tools'] = types.SimpleNamespace(
        log=types.SimpleNamespace(debug=print, info=print, warning=print, error=print, exception=print),
    )
    flask = types.ModuleType('flask')
    flask.request = types.SimpleNamespace(
        get_json=lambda *args, **kwargs: harness.body,
        path=f'/api/v2/elitea_core/schedules/prompt_lib/{PROJECT_ID}/settings',
    )
    sys.modules['flask'] = flask

    tools = types.ModuleType('tools')
    tools.api_tools = types.SimpleNamespace(
        APIModeHandler=type('APIModeHandler', (), {}), APIBase=type('APIBase', (), {}),
        with_modes=lambda urls: urls, endpoint_metrics=lambda func: func,
    )
    tools.auth = types.SimpleNamespace(decorators=types.SimpleNamespace(check_api=harness.check_api),
                                       current_user=lambda: {'id': 42})
    tools.config = types.SimpleNamespace(ADMINISTRATION_MODE='administration', DEFAULT_MODE='default')
    tools.db = LenientModule('db')
    sys.modules['tools'] = tools

    real_settings = _load('utils/schedule_settings.py', f'{PACKAGE}.utils.schedule_settings')
    settings = types.ModuleType(real_settings.__name__)  # the real model, a fake store
    settings.ScheduleProjectSettings = real_settings.ScheduleProjectSettings
    settings.is_run_without_asking_allowed = harness.is_allowed
    settings.save_schedule_settings = harness.save
    sys.modules[settings.__name__] = settings

    for name in ('models.entity_schedule', 'models.pd', 'models.pd.schedule_list', 'utils.authors',
                 'utils.schedule_queries'):
        sys.modules[f'{PACKAGE}.{name}'] = LenientModule(f'{PACKAGE}.{name}')  # the list handler, not run here
    _load('utils/constants.py', f'{PACKAGE}.utils.constants')
    _load('utils/schedule_access.py', f'{PACKAGE}.utils.schedule_access')
    _load('utils/schedule_project_handlers.py', f'{PACKAGE}.utils.schedule_project_handlers')
    sys.modules.update(handler_stubs(PACKAGE, real='schedule_project_handlers'))
    harness.routes = _load('api/v2/schedules.py', f'{PACKAGE}.api.v2.schedules')
    return harness


def _call(harness, verb, body=None):
    harness.body = body
    handler = harness.routes.PromptLibAPI()
    return getattr(handler, verb)(project_id=PROJECT_ID)


def test_el7011_cr_get_is_false_when_never_saved(harness):
    assert _call(harness, 'get') == ({'allow_run_without_asking': False}, 200)


@pytest.mark.parametrize('value', [True, False])
def test_el7011_cr_put_saves_and_get_reads_the_value(harness, value):
    assert _call(harness, 'put', {'allow_run_without_asking': value}) == ({'allow_run_without_asking': value}, 200)
    assert harness.saved == {PROJECT_ID: value}
    assert _call(harness, 'get') == ({'allow_run_without_asking': value}, 200)


def test_el7011_r3_2_get_is_false_when_the_store_fails(harness):
    harness.saved[PROJECT_ID] = True
    harness.read_error = True
    assert _call(harness, 'get') == ({'allow_run_without_asking': False}, 200)


@pytest.mark.parametrize('body, field', [
    ({'allow_run_without_asking': True, 'enabled': True}, 'enabled'),
    ({'get_notified': False}, 'get_notified'),
])
def test_el7011_cr_put_with_an_unknown_key_is_400_naming_it(harness, body, field):
    response, status = _call(harness, 'put', body)
    assert status == 400
    assert response == {'error': f'{field} is not a schedule setting', 'field': field}
    assert harness.saved == {}


@pytest.mark.parametrize('body', [{}, None, [], {'allow_run_without_asking': 'true'},
                                  {'allow_run_without_asking': 1}, {'allow_run_without_asking': None}])
def test_el7011_cr_put_without_one_boolean_is_400(harness, body):
    assert _call(harness, 'put', body) == (INVALID, 400)
    assert harness.saved == {}


def test_el7011_cr_put_answers_503_when_the_store_fails(harness):
    harness.save_error = RuntimeError('configurations down')
    response, status = _call(harness, 'put', {'allow_run_without_asking': True})
    assert status == 503
    assert response['error']


def test_el7011_cr_get_needs_schedules_list_and_put_needs_schedules_admin(harness):
    assert harness.specs['_get_settings'] == [LIST_PERMISSION]
    assert harness.specs['_save_settings'] == [ADMIN_PERMISSION]


def test_el7011_cr_a_member_reads_but_cannot_save(harness):
    harness.permissions = {LIST_PERMISSION}
    assert _call(harness, 'get')[1] == 200
    assert _call(harness, 'put', {'allow_run_without_asking': True})[1] == 403
    assert harness.saved == {}


def test_el7011_cr_without_schedules_list_get_is_403(harness):
    harness.permissions = set()
    assert _call(harness, 'get')[1] == 403
