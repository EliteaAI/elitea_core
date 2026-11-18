"""#7383 AC-18: typed Builder calls reach the existing routes in-process, always in the session project."""
import pathlib
import sys
import types

import pytest

TESTS_DIR = pathlib.Path(__file__).resolve().parents[3]
PLUGIN_ROOT = TESTS_DIR.parent
PKG = 'plugins_builder.elitea_core.utils.builder'
sys.path.insert(0, str(TESTS_DIR))

from fixtures.helpers import load_module_with_stubs  # noqa: E402


class FakeExecutor:
    calls = []

    @staticmethod
    def execute(api_tool, arguments, default_path_params=None, scoped_path_params=None):
        path_params, query, body = {}, {}, {}
        for param in api_tool['parameters']:
            name = param['name']
            if param['in'] == 'path':
                if name in (scoped_path_params or {}):
                    if arguments.get(name) not in (None, scoped_path_params[name]):
                        return {'error': f'{name} outside the current project'}
                    path_params[name] = scoped_path_params[name]
                elif name in arguments:
                    path_params[name] = arguments[name]
                elif 'default' in param.get('schema', {}):
                    path_params[name] = param['schema']['default']
            elif param['in'] == 'query' and name in arguments:
                query[name] = arguments[name]
        consumed = {p['name'] for p in api_tool['parameters']}
        body = {k: v for k, v in arguments.items() if k not in consumed}
        url = api_tool['path']
        for k, v in path_params.items():
            url = url.replace('{' + k + '}', str(v))
        FakeExecutor.calls.append((api_tool['method'], url, query, body))
        return {'result': {'ok': True}}


@pytest.fixture
def dispatch(builder):
    stub = types.ModuleType('plugins_builder.elitea_core.utils.mcp_service')
    stub.McpApiToolExecutor = FakeExecutor
    sys.modules[stub.__name__] = stub
    FakeExecutor.calls = []
    return load_module_with_stubs(PLUGIN_ROOT / 'utils' / 'builder' / 'dispatch.py', f'{PKG}.dispatch')


@pytest.mark.parametrize('op, args, expected', [
    ('get_version', {'application_id': 1, 'version_id': 10},
     ('GET', '/api/v2/elitea_core/version/prompt_lib/12/1/10', {}, {})),
    ('patch_instructions', {'application_id': 1, 'version_id': 10, 'old_text': 'a', 'replacement': 'b',
                            'expected_instructions_sha256': 'x'},
     ('POST', '/api/v2/elitea_core/version_instruction_patch/prompt_lib/12/1/10', {},
      {'old_text': 'a', 'replacement': 'b', 'expected_instructions_sha256': 'x'})),
    ('list_applications', {'query': 'daily', 'limit': 5},
     ('GET', '/api/v2/elitea_core/applications/prompt_lib/12', {'query': 'daily', 'limit': 5}, {})),
    ('put_project_context', {'content': 'c', 'enabled': True},
     ('PUT', '/api/v2/elitea_core/project_context/prompt_lib/12/project-context', {},
      {'content': 'c', 'enabled': True})),
    ('update_skill', {'skill_id': 4, 'version_id': 8, 'description': 'd'},
     ('PUT', '/api/v2/elitea_core/skill/prompt_lib/12/4', {'version_id': 8}, {'description': 'd'})),
    ('delete_application', {'application_id': 3}, ('DELETE', '/api/v2/elitea_core/application/prompt_lib/12/3', {}, {})),
    ('delete_skill', {'skill_id': 4}, ('DELETE', '/api/v2/elitea_core/skill/prompt_lib/12/4', {}, {})),
    ('update_application', {'application_id': 1, 'name': 'X'},
     ('PUT', '/api/v2/elitea_core/application/prompt_lib/12/1', {}, {'name': 'X'})),
])
def test_typed_ops_map_to_existing_routes(dispatch, op, args, expected):
    dispatch.make_call(12, catalog=None)(op, args)
    assert FakeExecutor.calls == [expected]


def test_a_model_supplied_project_is_refused(dispatch):
    result = dispatch.make_call(12, catalog=None)('get_project_context', {'project_id': 99})
    assert 'error' in result and FakeExecutor.calls == []


def test_none_arguments_are_dropped(dispatch):
    dispatch.make_call(12, catalog=None)('get_skill', {'skill_id': 4, 'version_id': None})
    assert FakeExecutor.calls == [('GET', '/api/v2/elitea_core/skill/prompt_lib/12/4', {}, {})]
