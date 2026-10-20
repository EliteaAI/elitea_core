"""EL-7011 US-06: the "Get notified" endpoint (R2.1-R2.4).

The endpoint module is loaded from source with a fake `tools`, a fake row model and a fake
session; no Pylon runtime. The fake session offers `get`, `add`, `merge`, `flush` and `commit`.
"""
import contextlib
import functools
import pathlib
import sys
import types

import pytest
from fixtures.helpers import load_module_with_stubs

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
PACKAGE = 'plugins.elitea_core'
PROJECT_ID = 7
CALLER_ID = 42
LIST_PERMISSION = 'models.applications.schedules.list'
MODES = ('administration', 'default')


class FakePreference:
    def __init__(self, user_id, get_notified=True):
        self.user_id = user_id
        self.get_notified = get_notified


class FakeSession:
    def __init__(self, rows):
        self.rows = rows

    def get(self, model, key, **kwargs):
        return self.rows.get(key)

    def add(self, row):
        self.rows[row.user_id] = row

    def merge(self, row):
        self.rows[row.user_id] = row
        return row

    def flush(self):
        pass

    def commit(self):
        pass


class Harness:
    def __init__(self):
        self.permissions = {LIST_PERMISSION}
        self.rows = {}
        self.body = None
        self.specs = []

    def check_api(self, spec):
        self.specs.append(spec)
        required = set(spec['permissions'])

        def decorator(func):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                if not required & self.permissions:
                    return {'error': 'access_denied'}, 403
                return func(*args, **kwargs)
            return wrapper
        return decorator

    @contextlib.contextmanager
    def get_session(self, project_id=None):
        assert project_id == PROJECT_ID
        yield FakeSession(self.rows)


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
        json=property(lambda self: harness.body),
    )
    sys.modules['flask'] = flask

    tools = types.ModuleType('tools')
    tools.api_tools = types.SimpleNamespace(
        APIModeHandler=type('APIModeHandler', (), {}),
        endpoint_metrics=lambda func: func,
    )
    tools.auth = types.SimpleNamespace(
        decorators=types.SimpleNamespace(check_api=harness.check_api),
        current_user=lambda: {'id': CALLER_ID},
    )
    tools.config = types.SimpleNamespace(ADMINISTRATION_MODE=MODES[0], DEFAULT_MODE=MODES[1])
    tools.db = types.SimpleNamespace(get_session=harness.get_session)
    tools.register_openapi = lambda **meta: (lambda func: func)
    tools.serialize = lambda value: value
    sys.modules['tools'] = tools

    models_all = types.ModuleType(f'{PACKAGE}.models.all')
    models_all.EntitySchedulePreference = FakePreference
    sys.modules[models_all.__name__] = models_all

    _load('utils/schedule_access.py', f'{PACKAGE}.utils.schedule_access')
    harness.module = _load('api/v2/schedule_preferences.py', f'{PACKAGE}.api.v2.schedule_preferences')
    return harness


def _call(harness, verb, body=None):
    harness.body = body
    handler = harness.module.PromptLibAPI()
    handler.module = object()
    return getattr(handler, verb)(project_id=PROJECT_ID)


def test_7011_r2_1_get_without_a_row_returns_true(harness):
    body, status = _call(harness, 'get')
    assert (body, status) == ({'get_notified': True}, 200)


@pytest.mark.parametrize('saved', [True, False])
def test_7011_r2_1_get_returns_the_saved_value(harness, saved):
    harness.rows[CALLER_ID] = FakePreference(CALLER_ID, saved)
    body, status = _call(harness, 'get')
    assert (body, status) == ({'get_notified': saved}, 200)


def test_7011_r2_1_get_reads_only_the_caller_row(harness):
    harness.rows[CALLER_ID + 1] = FakePreference(CALLER_ID + 1, False)
    body, _ = _call(harness, 'get')
    assert body == {'get_notified': True}


@pytest.mark.parametrize('value', [True, False])
def test_7011_r2_2_put_creates_the_row_and_returns_the_value(harness, value):
    body, status = _call(harness, 'put', {'get_notified': value})
    assert (body, status) == ({'get_notified': value}, 200)
    assert harness.rows[CALLER_ID].get_notified is value


def test_7011_r2_2_put_updates_the_existing_row(harness):
    harness.rows[CALLER_ID] = FakePreference(CALLER_ID, True)
    other = harness.rows[CALLER_ID + 1] = FakePreference(CALLER_ID + 1, True)
    body, status = _call(harness, 'put', {'get_notified': False})
    assert (body, status) == ({'get_notified': False}, 200)
    assert harness.rows[CALLER_ID].get_notified is False
    assert other.get_notified is True


@pytest.mark.parametrize('body', [
    {},
    None,
    {'get_notified': 'true'},
    {'get_notified': 1},
    {'get_notified': None},
    {'other': True},
])
def test_7011_r2_3_put_without_a_boolean_is_400(harness, body):
    response_body, status = _call(harness, 'put', body)
    assert status == 400
    assert response_body == {'error': 'get_notified must be true or false'}
    assert harness.rows == {}


@pytest.mark.parametrize('verb, body', [('get', None), ('put', {'get_notified': False})])
def test_7011_r2_4_without_schedules_list_is_403(harness, verb, body):
    harness.permissions = set()
    _, status = _call(harness, verb, body)
    assert status == 403
    assert harness.rows == {}


def test_7011_r2_4_both_calls_need_schedules_list(harness):
    assert [spec['permissions'] for spec in harness.specs] == [[LIST_PERMISSION]] * 2
