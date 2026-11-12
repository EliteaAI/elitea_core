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
UPDATE_PERMISSION = 'models.applications.schedules.update'
USER_ID = 42
INVALID = {'code': 'invalid_input', 'error': 'allow_run_without_asking must be true or false', 'field': 'allow_run_without_asking'}
# #7400: the GET also answers the per-webhook cap, its platform default and maximum
CAPS = {'webhook_max_concurrent_runs': None, 'webhook_max_concurrent_runs_default': 3,
        'webhook_max_concurrent_runs_limit': 20}


class Harness:
    def __init__(self):
        self.permissions = {LIST_PERMISSION, UPDATE_PERMISSION, ADMIN_PERMISSION}
        self.saved = {}
        self.body = None
        self.specs = {}
        self.read_error = False
        self.save_error = None
        self.caps = {}
        self.project_roles = []  # the caller's role names in the project, as `admin_get_user_roles` answers
        self.personal_project_id = None
        self.rpc_error = None
        self.rpc = types.SimpleNamespace(admin_get_user_roles=self.roles_rpc,
                                         projects_get_personal_project_id=self.personal_rpc)
        self.rpc.timeout = lambda _seconds: self.rpc

    def roles_rpc(self, project_id, user_id):
        if self.rpc_error:
            raise self.rpc_error
        return [{'name': name} for name in self.project_roles]

    def personal_rpc(self, user_id):
        if self.rpc_error:
            raise self.rpc_error
        return self.personal_project_id

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
        self.caps[project_id] = settings.webhook_max_concurrent_runs
        return settings.model_dump(exclude_none=True)

    def saved_cap(self, project_id):
        return self.caps.get(project_id)

    def read_saved(self, project_id):
        if self.read_error:
            raise RuntimeError('configurations down')
        stored = {}
        if project_id in self.saved:
            stored['allow_run_without_asking'] = self.saved[project_id]
        if self.caps.get(project_id) is not None:
            stored['webhook_max_concurrent_runs'] = self.caps[project_id]
        return stored


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
                                       current_user=lambda: {'id': USER_ID},
                                       resolve_permissions=lambda **kwargs: set(harness.permissions))
    tools.rpc_tools = types.SimpleNamespace(RpcMixin=lambda: types.SimpleNamespace(rpc=harness.rpc))
    tools.config = types.SimpleNamespace(ADMINISTRATION_MODE='administration', DEFAULT_MODE='default')
    tools.db = LenientModule('db')
    sys.modules['tools'] = tools

    real_settings = _load('utils/schedule_settings.py', f'{PACKAGE}.utils.schedule_settings')
    settings = types.ModuleType(real_settings.__name__)  # the real model, a fake store
    settings.ScheduleProjectSettings = real_settings.ScheduleProjectSettings
    settings.is_run_without_asking_allowed = harness.is_allowed
    settings.save_schedule_settings = harness.save
    settings.saved_webhook_cap = harness.saved_cap
    settings.read_saved_settings = harness.read_saved
    sys.modules[settings.__name__] = settings
    timing = types.ModuleType(f'{PACKAGE}.utils.schedule_timing')
    timing.webhook_limits = lambda: types.SimpleNamespace(per_webhook=3, per_webhook_limit=20, per_project=10, waiting=10)
    sys.modules[timing.__name__] = timing

    for name in ('models.entity_schedule', 'models.pd', 'models.pd.schedule_list', 'utils.authors',
                 'utils.schedule_queries'):
        sys.modules[f'{PACKAGE}.{name}'] = LenientModule(f'{PACKAGE}.{name}')  # the list handler, not run here
    _load('utils/constants.py', f'{PACKAGE}.utils.constants')
    _load('utils/schedule_access.py', f'{PACKAGE}.utils.schedule_access')
    _load('utils/project_settings_access.py', f'{PACKAGE}.utils.project_settings_access')
    _load('utils/schedule_project_handlers.py', f'{PACKAGE}.utils.schedule_project_handlers')
    sys.modules.update(handler_stubs(PACKAGE, real='schedule_project_handlers'))
    harness.routes = _load('api/v2/schedules.py', f'{PACKAGE}.api.v2.schedules')
    return harness


def _call(harness, verb, body=None):
    harness.body = body
    handler = harness.routes.PromptLibAPI()
    return getattr(handler, verb)(project_id=PROJECT_ID)


def test_el7011_cr_get_is_false_when_never_saved(harness):
    assert _call(harness, 'get') == ({'allow_run_without_asking': False, **CAPS}, 200)


@pytest.mark.parametrize('value', [True, False])
def test_el7011_cr_put_saves_and_get_reads_the_value(harness, value):
    assert _call(harness, 'put', {'allow_run_without_asking': value}) == ({'allow_run_without_asking': value}, 200)
    assert harness.saved == {PROJECT_ID: value}
    assert _call(harness, 'get') == ({'allow_run_without_asking': value, **CAPS}, 200)


def test_el7011_r3_2_get_is_false_when_the_store_fails(harness):
    harness.saved[PROJECT_ID] = True
    harness.read_error = True
    assert _call(harness, 'get') == ({'allow_run_without_asking': False, **CAPS}, 200)


@pytest.mark.parametrize('body, field', [
    ({'allow_run_without_asking': True, 'enabled': True}, 'enabled'),
    ({'get_notified': False}, 'get_notified'),
])
def test_el7011_cr_put_with_an_unknown_key_is_400_naming_it(harness, body, field):
    response, status = _call(harness, 'put', body)
    assert status == 400
    assert response == {'code': 'invalid_input', 'error': f'{field} is not a schedule setting', 'field': field}
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


def test_put_answers_503_when_configurations_has_no_owned_types(harness):
    harness.save_error = TypeError("unexpected keyword argument 'owner'")
    _, status = _call(harness, 'put', {'allow_run_without_asking': True})
    assert status == 503


def test_el7011_cr_get_needs_schedules_list_and_put_needs_schedules_admin(harness):
    assert harness.specs['_get_settings'] == [LIST_PERMISSION]
    assert harness.specs['_save_settings'] == [UPDATE_PERMISSION]


def test_el7011_cr_a_member_reads_but_cannot_save(harness):
    harness.permissions = {LIST_PERMISSION}
    assert _call(harness, 'get')[1] == 200
    assert _call(harness, 'put', {'allow_run_without_asking': True})[1] == 403
    assert harness.saved == {}


def test_el7011_cr_without_schedules_list_get_is_403(harness):
    harness.permissions = set()
    assert _call(harness, 'get')[1] == 403


# --- #7400: the per-project lower value of the running-run cap

def test_put_saves_a_lower_webhook_cap_and_get_reads_it(harness):
    body = {'allow_run_without_asking': False, 'webhook_max_concurrent_runs': 2}
    assert _call(harness, 'put', body) == (body, 200)
    assert _call(harness, 'get')[0]['webhook_max_concurrent_runs'] == 2


def test_put_above_the_platform_maximum_is_400_naming_the_limit(harness):
    response, status = _call(harness, 'put', {'allow_run_without_asking': False, 'webhook_max_concurrent_runs': 25})
    assert status == 400
    assert response == {'code': 'invalid_input', 'error': 'Running runs per webhook can be at most 20',
                        'field': 'webhook_max_concurrent_runs', 'limit': 20}
    assert harness.saved == {}


@pytest.mark.parametrize('value', [0, -1, 2.5, '3', True])
def test_put_with_a_cap_that_is_not_a_whole_number_of_at_least_one_is_400(harness, value):
    response, status = _call(harness, 'put', {'allow_run_without_asking': False, 'webhook_max_concurrent_runs': value})
    assert (status, response['field']) == (400, 'webhook_max_concurrent_runs')
    assert harness.saved == {}


def test_put_without_the_cap_key_keeps_the_saved_cap(harness):
    harness.caps[PROJECT_ID] = 2  # an older client sends only the approval switch
    assert _call(harness, 'put', {'allow_run_without_asking': True})[0]['webhook_max_concurrent_runs'] == 2


def test_put_with_a_null_cap_clears_it(harness):
    harness.caps[PROJECT_ID] = 2
    assert _call(harness, 'put', {'allow_run_without_asking': True, 'webhook_max_concurrent_runs': None}) == (
        {'allow_run_without_asking': True}, 200)
    assert harness.caps[PROJECT_ID] is None


def test_put_with_only_the_cap_keeps_the_saved_approval_switch(harness):
    harness.saved[PROJECT_ID] = True  # the checkbox was saved a moment before the cap field
    assert _call(harness, 'put', {'webhook_max_concurrent_runs': 2}) == (
        {'allow_run_without_asking': True, 'webhook_max_concurrent_runs': 2}, 200)
    assert harness.saved == {PROJECT_ID: True}


# --- review: a key left out comes from one read; a failed read writes nothing

@pytest.mark.parametrize('body', [{'webhook_max_concurrent_runs': 2}, {'allow_run_without_asking': True}])
def test_a_partial_save_after_a_failed_read_is_503_and_writes_nothing(harness, body):
    harness.saved[PROJECT_ID], harness.caps[PROJECT_ID] = False, 4
    harness.read_error = True
    response, status = _call(harness, 'put', body)
    assert status == 503 and response['code'] == 'settings_unavailable'
    assert (harness.saved[PROJECT_ID], harness.caps[PROJECT_ID]) == (False, 4)  # neither key was reset


def test_a_save_with_both_keys_needs_no_read(harness):
    harness.read_error = True  # the whole record is in the body
    body = {'allow_run_without_asking': True, 'webhook_max_concurrent_runs': 3}
    assert _call(harness, 'put', body) == (body, 200)


# --- who may save: a project admin, a schedules admin, or the owner of their own personal project

def _as_editor(harness):
    harness.permissions = {LIST_PERMISSION, UPDATE_PERMISSION}


def test_the_owner_of_a_personal_project_without_an_admin_role_may_save(harness):
    _as_editor(harness)
    harness.project_roles = ['editor', 'viewer', 'monitor']
    harness.personal_project_id = PROJECT_ID
    assert _call(harness, 'put', {'allow_run_without_asking': True}) == ({'allow_run_without_asking': True}, 200)
    assert harness.saved == {PROJECT_ID: True}


def test_an_editor_of_a_team_project_is_refused_with_the_access_denied_body(harness):
    _as_editor(harness)
    harness.project_roles = ['editor', 'viewer']
    harness.personal_project_id = PROJECT_ID + 1  # their own personal project is another one
    response, status = _call(harness, 'put', {'allow_run_without_asking': True})
    assert (status, response['error'], response['required']) == (403, 'access_denied', [ADMIN_PERMISSION])
    assert harness.saved == {}


def test_a_project_admin_role_may_save_without_the_schedules_admin_permission(harness):
    _as_editor(harness)
    harness.project_roles = ['admin']
    assert _call(harness, 'put', {'allow_run_without_asking': True})[1] == 200
    assert harness.saved == {PROJECT_ID: True}


def test_a_failing_role_and_personal_project_lookup_refuses_the_save(harness):
    _as_editor(harness)
    harness.rpc_error = RuntimeError('rpc unavailable')
    assert _call(harness, 'put', {'allow_run_without_asking': True})[1] == 403
    assert harness.saved == {}
