import functools
import importlib.util
import pathlib
import sys
import types

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
PACKAGE = 'plugins.elitea_core'
CALLER_PROJECT_ID = 7
PUBLIC_PROJECT_ID = 1

PREDICT = 'models.applications.predict.post'
SKILL_READ = 'models.applications.skills.details'
CATALOG_READ = 'models.applications.public_application.details'


def _package(name):
    module = types.ModuleType(name)
    module.__path__ = []
    sys.modules[name] = module
    return module


def _load(relative_path, module_name):
    spec = importlib.util.spec_from_file_location(module_name, PLUGIN_ROOT / relative_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


class Harness:
    def __init__(self):
        self.user_permissions = set()
        self.folder_denied = set()
        self.runs = []
        self.openapi = {}

    def check_api(self, descriptor):
        required = set(descriptor['permissions'])

        def decorator(func):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                if not required & self.user_permissions:
                    return {'error': 'access_denied'}, 403
                return func(*args, **kwargs)
            return wrapper
        return decorator

    def register_openapi(self, **meta):
        def decorator(func):
            self.openapi[func.__module__] = meta
            return func
        return decorator

    def require_folder_access(self, entity_types, id_param, **kwargs):
        def decorator(func):
            @functools.wraps(func)
            def wrapper(handler, *args, **call_kwargs):
                if (entity_types, call_kwargs.get(id_param)) in self.folder_denied:
                    return {'ok': False, 'error': 'no access'}, 404
                return func(handler, *args, **call_kwargs)
            return wrapper
        return decorator

    def execute_skill_predict(self, module, body, **kwargs):
        self.runs.append(kwargs)
        return {'ok': True}, 200


@pytest.fixture
def harness(isolated_sys_modules):
    harness = Harness()
    for name in ('plugins', PACKAGE, f'{PACKAGE}.api', f'{PACKAGE}.api.v2', f'{PACKAGE}.utils',
                 f'{PACKAGE}.models', f'{PACKAGE}.models.pd'):
        _package(name)

    flask = types.ModuleType('flask')
    flask.request = types.SimpleNamespace(get_json=lambda silent=False: {'user_input': 'hi'}, args={})
    sys.modules['flask'] = flask

    tools = types.ModuleType('tools')
    tools.api_tools = types.SimpleNamespace(
        APIModeHandler=type('APIModeHandler', (), {}),
        APIBase=type('APIBase', (), {}),
        endpoint_metrics=lambda func: func,
        with_modes=lambda params: params,
    )
    tools.auth = types.SimpleNamespace(
        decorators=types.SimpleNamespace(check_api=harness.check_api),
        current_user=lambda: {'id': 42},
    )
    tools.config = types.SimpleNamespace(ADMINISTRATION_MODE='administration', DEFAULT_MODE='default')
    tools.register_openapi = harness.register_openapi
    sys.modules['tools'] = tools

    stubs = {
        'models.pd.skill_predict': {'SkillPredictRequest': object},
        'utils.constants': {'PROMPT_LIB_MODE': 'prompt_lib'},
        'utils.folder_access': {'require_folder_access': harness.require_folder_access},
        'utils.skill_run_utils': {
            'execute_skill_predict': harness.execute_skill_predict,
            'is_async_query': lambda args: False,
        },
        'utils.utils': {'get_public_project_id': lambda: PUBLIC_PROJECT_ID},
    }
    for name, attrs in stubs.items():
        module = types.ModuleType(f'{PACKAGE}.{name}')
        module.__dict__.update(attrs)
        sys.modules[module.__name__] = module

    harness.own = _load('api/v2/skill_predict.py', f'{PACKAGE}.api.v2.skill_predict')
    harness.catalog = _load('api/v2/public_skill_predict.py', f'{PACKAGE}.api.v2.public_skill_predict')
    return harness


def _own_post(harness, skill_id=10, version_id=None):
    handler = harness.own.PromptLibAPI()
    handler.module = object()
    return handler.post(project_id=CALLER_PROJECT_ID, skill_id=skill_id, version_id=version_id)


def _catalog_post(harness, public_skill_id=10, version_id=None):
    handler = harness.catalog.PromptLibAPI()
    handler.module = object()
    return handler.post(project_id=CALLER_PROJECT_ID, public_skill_id=public_skill_id, version_id=version_id)


@pytest.mark.parametrize('permissions, expected_status', [
    ({PREDICT, SKILL_READ}, 200),
    ({PREDICT}, 403),
    ({SKILL_READ}, 403),
    ({PREDICT, CATALOG_READ}, 403),
])
def test_own_skill_run_needs_predict_and_skill_read(harness, permissions, expected_status):
    harness.user_permissions = permissions
    _, status = _own_post(harness)
    assert status == expected_status


@pytest.mark.parametrize('permissions, expected_status', [
    ({PREDICT, CATALOG_READ}, 200),
    ({PREDICT}, 403),
    ({CATALOG_READ}, 403),
    ({PREDICT, SKILL_READ}, 403),
])
def test_catalog_skill_run_needs_predict_and_catalog_read(harness, permissions, expected_status):
    harness.user_permissions = permissions
    _, status = _catalog_post(harness)
    assert status == expected_status


def test_folder_no_access_on_an_own_skill_is_a_404(harness):
    harness.user_permissions = {PREDICT, SKILL_READ}
    harness.folder_denied = {('skill', 10)}
    _, status = _own_post(harness)
    assert status == 404 and harness.runs == []


def test_own_skill_is_read_from_the_caller_project(harness):
    harness.user_permissions = {PREDICT, SKILL_READ}
    _own_post(harness, version_id=5)
    assert harness.runs == [{
        'caller_project_id': CALLER_PROJECT_ID,
        'skill_project_id': CALLER_PROJECT_ID,
        'skill_id': 10,
        'version_id': 5,
        'published_only': False,
        'user_id': 42,
        'async_requested': False,
    }]


@pytest.mark.parametrize('project_id, published_only', [(PUBLIC_PROJECT_ID, True), (CALLER_PROJECT_ID, False)])
def test_own_route_runs_only_published_versions_in_the_public_project(harness, project_id, published_only):
    harness.user_permissions = {PREDICT, SKILL_READ}
    handler = harness.own.PromptLibAPI()
    handler.module = object()
    handler.post(project_id=project_id, skill_id=10)
    assert harness.runs[0]['skill_project_id'] == project_id
    assert harness.runs[0]['published_only'] is published_only


def test_catalog_skill_is_read_from_the_public_project_and_billed_to_the_caller(harness):
    harness.user_permissions = {PREDICT, CATALOG_READ}
    _catalog_post(harness, version_id=5)
    assert harness.runs == [{
        'caller_project_id': CALLER_PROJECT_ID,
        'skill_project_id': PUBLIC_PROJECT_ID,
        'skill_id': 10,
        'version_id': 5,
        'published_only': True,
        'user_id': 42,
        'async_requested': False,
    }]


def test_routes_accept_an_optional_trailing_version(harness):
    assert harness.own.API.url_params == [
        '<int:project_id>/<int:skill_id>',
        '<int:project_id>/<int:skill_id>/<int:version_id>',
    ]
    assert harness.catalog.API.url_params == [
        '<int:project_id>/<int:public_skill_id>',
        '<int:project_id>/<int:public_skill_id>/<int:version_id>',
    ]


def test_run_routes_are_published_to_users_but_not_as_mcp_tools(harness):
    for module in (harness.own, harness.catalog):
        meta = harness.openapi[module.__name__]
        assert meta['available_to_users'] is True
        assert not meta.get('mcp_tool', False)
