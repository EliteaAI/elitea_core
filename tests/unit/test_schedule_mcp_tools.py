"""The six schedule operations are registered as named MCP tools; every other schedule operation is not.

The real `utils/schedule_openapi.py` and the request models it reads are loaded; the handler modules
and the platform around them are lenient stubs, and the registry is a recorder.

Run via:
    python tests/run_tests.py unit/test_schedule_mcp_tools.py -v
"""
import importlib.util
import pathlib
import sys
import types

import pytest
from fixtures import schedule_sqlite as sqlite_db

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
PKG = 'schedmcp_unit'
TAG = 'elitea_core/schedules'
BASE = '/api/v2/elitea_core/schedules/<string:mode>/<int:project_id>'
ITEM = BASE + '/<int:schedule_id>'
TOOLS = {
    ('get', BASE): 'list_schedules',
    ('get', ITEM): 'get_schedule',
    ('post', BASE): 'create_schedule',
    ('put', ITEM): 'update_schedule',
    ('delete', ITEM): 'delete_schedule',
    ('post', f'{ITEM}/run'): 'run_schedule_now',
}
REAL_MODULES = (
    'utils/constants.py', 'models/pd/entity_schedule.py', 'models/pd/schedule_run.py',
    'models/pd/schedule_list.py', 'models/pd/generate_schedule_draft.py', 'utils/schedule_item_handlers.py',
    'utils/schedule_settings.py', 'utils/schedule_project_handlers.py', 'utils/schedule_openapi.py',
)
LENIENT_MODULES = (
    f'{PKG}.models.entity_schedule', f'{PKG}.models.enums.all', f'{PKG}.models.pd.index',
    *(f'{PKG}.utils.{name}' for name in (
        'schedule_access', 'schedule_claim', 'schedule_dispatch', 'schedule_notifications',
        'schedule_service', 'schedule_timing', 'authors', 'schedule_queries', 'trigger_governance',
    )),
)


def _load(monkeypatch, path, name):
    spec = importlib.util.spec_from_file_location(name, PLUGIN_ROOT / path)
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, name, module)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def openapi(monkeypatch):
    for name, module in sqlite_db.REAL_SQLALCHEMY.items():
        monkeypatch.setitem(sys.modules, name, module)
    for name in ('pylon', 'pylon.core', 'pylon.core.tools', 'tools', 'flask'):
        monkeypatch.setitem(sys.modules, name, sqlite_db.LenientModule(name))
    for name in ('', '.models', '.models.pd', '.models.enums', '.utils'):
        package = types.ModuleType(PKG + name)
        package.__path__ = []
        monkeypatch.setitem(sys.modules, PKG + name, package)
    for name in LENIENT_MODULES:
        monkeypatch.setitem(sys.modules, name, sqlite_db.LenientModule(name))
    service = sys.modules[f'{PKG}.utils.schedule_service']
    service.refusal, service.INVALID_INPUT = lambda code, text, status=400, **extra: ({}, status), 'invalid_input'
    for path in REAL_MODULES:
        module = _load(monkeypatch, path, f'{PKG}.{path.removesuffix(".py").replace("/", ".")}')
    return module


class Registry:
    """Records what `register_endpoint` receives; `named` makes it accept `operation_name` like a newer shared plugin."""

    def __init__(self, named):
        self.calls = []
        if named:
            self.register_endpoint = self._register_named

    def register_endpoint(self, plugin_name, path, method, name, mcp_tool=False, **rest):
        self.calls.append({'path': path, 'method': method, 'mcp_tool': mcp_tool, 'rest': rest})

    def _register_named(self, plugin_name, path, method, name, mcp_tool=False, operation_name=None, **rest):
        self.calls.append({'path': path, 'method': method, 'mcp_tool': mcp_tool, 'operation_name': operation_name,
                           'rest': rest})


def _registered(openapi, named):
    registry = Registry(named)
    openapi.register_schedule_endpoints(registry, TAG)
    return registry.calls


def test_only_the_six_schedule_operations_are_mcp_tools(openapi):
    tools = {(c['method'], c['path']) for c in _registered(openapi, named=False) if c['mcp_tool']}
    assert tools == set(TOOLS)


def test_every_schedule_operation_is_still_registered(openapi):
    assert len(_registered(openapi, named=False)) == len(openapi.OPENAPI_ENDPOINTS)


def test_tools_carry_the_issue_names_when_the_registry_takes_them(openapi):
    names = {(c['method'], c['path']): c['operation_name'] for c in _registered(openapi, named=True) if c['mcp_tool']}
    assert names == TOOLS


def test_other_operations_carry_no_tool_name(openapi):
    assert all(c['operation_name'] is None for c in _registered(openapi, named=True) if not c['mcp_tool'])


def test_tool_names_are_unique(openapi):
    names = [endpoint['operation_name'] for endpoint in openapi.OPENAPI_ENDPOINTS if endpoint.get('operation_name')]
    assert len(names) == len(set(names)) == 6
