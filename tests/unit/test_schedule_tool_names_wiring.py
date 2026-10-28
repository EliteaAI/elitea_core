"""The schedule registry wrapper of module.py gives each schedule operation its tool name.

`_NamedToolRegistry` is read from the source of module.py (the module needs the Pylon runtime).
"""
import ast
import pathlib

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
BASE = '/api/v2/elitea_core/schedules/<string:mode>/<int:project_id>'
ITEM = BASE + '/<int:schedule_id>'
NAMES = {
    ('get', BASE): 'list_schedules',
    ('get', ITEM): 'get_schedule',
    ('post', BASE): 'create_schedule',
    ('put', ITEM): 'update_schedule',
    ('delete', ITEM): 'delete_schedule',
    ('post', ITEM + '/run'): 'run_schedule_now',
}


def _wrapper_class():
    tree = ast.parse((PLUGIN_ROOT / 'module.py').read_text())
    node = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == '_NamedToolRegistry')
    namespace = {'inspect': __import__('inspect')}
    exec(compile(ast.Module(body=[node], type_ignores=[]), '<module>', 'exec'), namespace)  # noqa: S102
    return namespace['_NamedToolRegistry']


class NewRegistry:
    def __init__(self):
        self.calls = []

    def register_endpoint(self, plugin_name, path, method, name, mcp_tool=False, tool_name=''):
        self.calls.append({'path': path, 'method': method, 'mcp_tool': mcp_tool, 'tool_name': tool_name})


class OldRegistry:
    def __init__(self):
        self.calls = []

    def register_endpoint(self, plugin_name, path, method, name, mcp_tool=False):
        self.calls.append({'path': path, 'method': method, 'mcp_tool': mcp_tool})


def _register_all(registry):
    wrapper = _wrapper_class()(registry, NAMES)
    for (method, path) in NAMES:
        wrapper.register_endpoint(plugin_name='elitea_core', path=path, method=method, name='x')
    wrapper.register_endpoint(plugin_name='elitea_core', path=BASE + '/export', method='get', name='x')
    return registry.calls


def test_each_schedule_operation_gets_its_tool_name():
    calls = _register_all(NewRegistry())
    assert {(c['method'], c['path']): c['tool_name'] for c in calls[:6]} == NAMES
    assert all(c['mcp_tool'] for c in calls[:6])


def test_an_operation_without_a_name_is_not_a_tool():
    export = _register_all(NewRegistry())[6]
    assert export['tool_name'] == '' and not export['mcp_tool']


def test_a_registry_without_tool_name_support_is_called_unchanged():
    calls = _register_all(OldRegistry())
    assert all('tool_name' not in c and not c['mcp_tool'] for c in calls)
    assert len(calls) == 7


def test_other_registry_methods_pass_through():
    registry = NewRegistry()
    registry.marker = 5
    assert _wrapper_class()(registry, NAMES).marker == 5
