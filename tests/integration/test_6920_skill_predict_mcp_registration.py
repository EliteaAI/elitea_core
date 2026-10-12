import functools
import importlib.util
import pathlib
import sys
import types

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
OPENAPI_TOOLS = PLUGIN_ROOT.parent / 'shared' / 'tools' / 'openapi_tools.py'

pytestmark = pytest.mark.skipif(
    not OPENAPI_TOOLS.exists(),
    reason='shared plugin is not checked out next to elitea_core',
)

PKG = 'skillpkg_6920_integration'
ROUTES = {
    'skill_predict': ('/api/v2/elitea_core/skill_predict', 'skill_id'),
    'public_skill_predict': ('/api/v2/elitea_core/public_skill_predict', 'public_skill_id'),
}
CLIENT_ONLY_FIELDS = {'callback_url', 'callback_headers', 'sid', 'async_mode'}


def _package(name, path=None):
    module = types.ModuleType(name)
    module.__path__ = [path] if path else []
    sys.modules[name] = module
    return module


def _stub(name, **attrs):
    module = types.ModuleType(name)
    module.__dict__.update(attrs)
    sys.modules[name] = module
    return module


def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _with_modes(url_params):
    return [f'<string:mode>/{p}' for p in url_params] + list(url_params)


def _install(openapi_tools):
    class ApiTools:
        class APIModeHandler:
            pass

        class APIBase:
            pass

        with_modes = staticmethod(_with_modes)
        endpoint_metrics = staticmethod(lambda func: functools.wraps(func)(lambda *a, **k: func(*a, **k)))

    _stub('tools',
          api_tools=ApiTools(),
          config=types.SimpleNamespace(ADMINISTRATION_MODE='administration', DEFAULT_MODE='default'),
          auth=types.SimpleNamespace(decorators=types.SimpleNamespace(check_api=lambda *a, **k: (lambda f: f))),
          register_openapi=openapi_tools.register_openapi,
          sanitize_property_name=lambda name: name)
    _stub('flask', request=types.SimpleNamespace(args={}, environ={}, get_json=lambda silent=False: {}))

    for name in (PKG, f'{PKG}.models', f'{PKG}.models.pd', f'{PKG}.utils'):
        _package(name)
    _package(f'{PKG}.api')
    _package(f'{PKG}.api.v2', str(PLUGIN_ROOT / 'api' / 'v2'))
    _load(f'{PKG}.models.pd.skill_predict', PLUGIN_ROOT / 'models' / 'pd' / 'skill_predict.py')
    _stub(f'{PKG}.api.v2.skill', resolve_version_id=lambda version_id, *a, **k: (version_id, None))
    _stub(f'{PKG}.utils.constants', PROMPT_LIB_MODE='prompt_lib')
    _stub(f'{PKG}.utils.folder_access', require_folder_access=lambda *a, **k: (lambda f: f))
    _stub(f'{PKG}.utils.skill_run_utils', execute_skill_predict=None, is_async_query=None, request_model_for=None)
    _stub(f'{PKG}.utils.utils', get_public_project_id=lambda: 1)

    return {
        name: _load(f'{PKG}.api.v2.{name}', PLUGIN_ROOT / 'api' / 'v2' / f'{name}.py')
        for name in ROUTES
    }


@pytest.fixture()
def mcp_tools():
    saved = dict(sys.modules)
    pylon_tools = _stub('pylon.core.tools', log=types.SimpleNamespace(
        info=lambda *a, **k: None, warning=lambda *a, **k: None, error=lambda *a, **k: None,
        debug=lambda *a, **k: None, exception=lambda *a, **k: None))
    _package('pylon').core = _package('pylon.core')
    sys.modules['pylon.core'].tools = pylon_tools
    openapi_tools = _load('openapi_tools_6920', OPENAPI_TOOLS)
    routes = _install(openapi_tools)
    registry = openapi_tools.OpenAPIRegistry()
    for name, module in routes.items():
        openapi_tools.register_api_class(module.API, 'elitea_core', ROUTES[name][0], registry)
    tools = {tool['value']: tool for tool in registry.get_mcp_api_tools(
        plugins=['elitea_core'], filter_tags=['elitea_core/skills'])}
    yield tools
    sys.modules.clear()
    sys.modules.update(saved)


def _schema(mcp_tools, route):
    return next(
        tool['args_schema'] for name, tool in mcp_tools.items() if name.endswith(f'_elitea_core_{route}')
    )


@pytest.mark.parametrize('route', list(ROUTES))
def test_run_route_is_an_mcp_tool(mcp_tools, route):
    assert any(name.endswith(f'_elitea_core_{route}') for name in mcp_tools)


@pytest.mark.parametrize('route', list(ROUTES))
def test_mcp_schema_offers_no_callback_socket_or_async(mcp_tools, route):
    properties = set(_schema(mcp_tools, route)['properties'])
    assert not CLIENT_ONLY_FIELDS & properties
    assert {'user_input', 'chat_history', 'llm_settings', 'return_chat_history'} <= properties


@pytest.mark.parametrize('route', list(ROUTES))
def test_mcp_schema_takes_an_optional_version_and_requires_the_skill(mcp_tools, route):
    schema = _schema(mcp_tools, route)
    skill_param = ROUTES[route][1]
    assert schema['properties']['version_id']['type'] == 'integer'
    assert 'version_id' not in schema['required']
    assert {'project_id', skill_param, 'user_input'} <= set(schema['required'])
