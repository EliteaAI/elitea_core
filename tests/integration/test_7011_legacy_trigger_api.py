"""EL-7011 US-23: the legacy pipeline trigger PUT rejects schedule writes (R1.1-R1.4).

The resource is loaded from source with fake host modules and a fake session; the real
`TriggerType` and `UpdatePipelineTrigger` models validate the request body.
"""
import contextlib
import pathlib
import sys
import types

import pytest
from fixtures.helpers import load_module_with_stubs

pytestmark = pytest.mark.integration

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
PACKAGE = 'plugins.elitea_core'
PROJECT_ID = 7
VERSION_ID = 11
LEGACY_SCHEDULE = {'type': 'schedule', 'cron': '0 9 * * *', 'timezone': 'UTC', 'created_by': 1}


class FakeVersion:
    id = VERSION_ID

    def __init__(self, trigger):
        self.pipeline_settings = {'trigger': trigger}


class FakeQuery:
    def __init__(self, version):
        self.version = version

    def options(self, *args):
        return self

    def filter(self, *args):
        return self

    def first(self):
        return self.version


class FakeSession:
    def __init__(self, harness):
        self.harness = harness

    def query(self, model):
        return FakeQuery(self.harness.version)

    def commit(self):
        self.harness.commits += 1


class Harness:
    def __init__(self):
        self.version = FakeVersion(dict(LEGACY_SCHEDULE))
        self.body = None
        self.commits = 0

    @contextlib.contextmanager
    def get_session(self, project_id=None):
        yield FakeSession(self)


def _package(name):
    module = types.ModuleType(name)
    module.__path__ = []
    sys.modules[name] = module


def _passthrough(*args, **kwargs):
    return lambda func: func


@pytest.fixture
def harness():
    saved = dict(sys.modules)
    harness = Harness()
    for name in ('plugins', PACKAGE, f'{PACKAGE}.api', f'{PACKAGE}.api.v2', f'{PACKAGE}.utils',
                 f'{PACKAGE}.models', f'{PACKAGE}.models.pd', 'pylon', 'pylon.core'):
        _package(name)

    noop = lambda *args, **kwargs: None
    sys.modules['pylon.core.tools'] = types.SimpleNamespace(
        log=types.SimpleNamespace(debug=noop, info=noop, warning=noop, error=noop, exception=noop),
    )
    flask = types.ModuleType('flask')
    flask.request = types.SimpleNamespace(json=None)
    sys.modules['flask'] = flask
    sys.modules['sqlalchemy.orm'] = types.SimpleNamespace(joinedload=lambda *a: None)
    sys.modules['sqlalchemy.orm.attributes'] = types.SimpleNamespace(flag_modified=noop)

    tools = types.ModuleType('tools')
    tools.api_tools = types.SimpleNamespace(
        APIModeHandler=type('APIModeHandler', (), {}),
        APIBase=type('APIBase', (), {}),
        with_modes=lambda urls: urls,
        endpoint_metrics=lambda func: func,
    )
    tools.auth = types.SimpleNamespace(
        decorators=types.SimpleNamespace(check_api=_passthrough),
        current_user=lambda: {'id': 1},
        resolve_permissions=lambda **kwargs: {'models.applications.version.update'},
    )
    tools.config = types.SimpleNamespace(ADMINISTRATION_MODE='administration', DEFAULT_MODE='default')
    tools.db = types.SimpleNamespace(get_session=harness.get_session)
    tools.register_openapi = _passthrough
    tools.serialize = lambda value: value
    sys.modules['tools'] = tools

    sys.modules[f'{PACKAGE}.models.all'] = types.SimpleNamespace(
        ApplicationVersion=types.SimpleNamespace(id=None, application=None))
    sys.modules[f'{PACKAGE}.utils.folder_access'] = types.SimpleNamespace(
        require_folder_access=_passthrough, APPLICATION_ENTITY_TYPES=())
    sys.modules[f'{PACKAGE}.utils.constants'] = types.SimpleNamespace(PROMPT_LIB_MODE='prompt_lib')
    sys.modules[f'{PACKAGE}.utils.pipeline_trigger'] = types.SimpleNamespace(
        GITLAB_AUTH_SIGNING_TOKEN='signing_token',
        get_trigger_from_pipeline_settings=lambda settings: settings.get('trigger') or {},
        build_trigger_for_storage=lambda data, user_id: {
            'type': data.type, 'webhook_type': data.webhook_type},
        build_webhook_url=lambda *args: 'https://hook.invalid',
        get_webhook_secret_for_display=lambda *args, **kwargs: {},
        generate_webhook_secret=lambda: 'secret',
        store_webhook_secret=lambda *args: 'ref',
        store_webhook_signing_secret=lambda *args: 'ref',
        validate_webhook_secret_strength=lambda value: None,
        validate_gitlab_signing_token_format=lambda value: None,
    )

    load_module_with_stubs(PLUGIN_ROOT / 'models' / 'pd' / 'index.py', f'{PACKAGE}.models.pd.index')
    load_module_with_stubs(
        PLUGIN_ROOT / 'models' / 'pd' / 'pipeline_trigger.py', f'{PACKAGE}.models.pd.pipeline_trigger')
    harness.module = load_module_with_stubs(
        PLUGIN_ROOT / 'api' / 'v2' / 'pipeline_trigger.py', f'{PACKAGE}.api.v2.pipeline_trigger')
    yield harness
    sys.modules.clear()
    sys.modules.update(saved)


def _call(harness, verb, body=None):
    harness.module.request.json = body
    return getattr(harness.module.PromptLibAPI(), verb)(project_id=PROJECT_ID, version_id=VERSION_ID)


@pytest.mark.parametrize('cron', ['0 9 * * *', 'not a cron'])
def test_7011_r1_1_put_schedule_is_400_naming_the_schedules_api(harness, cron):
    """Even a schedule body that fails the body check gets the Schedules API answer (review round 1)."""
    body, status = _call(harness, 'put', {'type': 'schedule', 'cron': cron, 'timezone': 'UTC'})
    assert status == 400
    assert f'/elitea_core/schedules/prompt_lib/{PROJECT_ID}' in body['error']


def test_7011_r1_2_rejected_schedule_put_leaves_the_stored_trigger(harness):
    harness.version.pipeline_settings = {'trigger': {'type': 'chat_message'}}
    _call(harness, 'put', {'type': 'schedule', 'cron': '0 9 * * *', 'timezone': 'UTC'})
    assert harness.version.pipeline_settings == {'trigger': {'type': 'chat_message'}}
    assert harness.commits == 0


@pytest.mark.parametrize('body, stored_type', [
    ({'type': 'chat_message'}, 'chat_message'),
    ({'type': 'webhook', 'webhook_type': 'custom'}, 'webhook'),
])
def test_7011_r1_3_put_chat_message_and_webhook_store_as_before(harness, body, stored_type):
    response, status = _call(harness, 'put', body)
    assert status == 200
    assert response['type'] == stored_type
    assert harness.version.pipeline_settings['trigger']['type'] == stored_type
    assert harness.commits == 1


def test_7011_r1_4_get_returns_a_stored_legacy_schedule(harness):
    response, status = _call(harness, 'get')
    assert status == 200
    assert (response['type'], response['cron'], response['timezone']) == ('schedule', '0 9 * * *', 'UTC')
