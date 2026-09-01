"""Guards the server-side path-param defaults for MCP API tools.

Re-broken by dropping default_path_params from the executor, or by letting it win
over a value the model supplied.
"""
import importlib.util
import pathlib
import sys
import types

import pytest


MODULE_PATH = pathlib.Path(__file__).resolve().parents[3] / 'utils' / 'mcp_service.py'


@pytest.fixture(scope='module')
def parse_arguments():
    import ast
    import textwrap

    source = MODULE_PATH.read_text()
    tree = ast.parse(source)
    executor = next(
        node for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == 'McpApiToolExecutor'
    )
    func = next(
        node for node in executor.body
        if isinstance(node, ast.FunctionDef) and node.name == '_parse_arguments'
    )
    func.decorator_list = []

    namespace = {
        'log': types.SimpleNamespace(warning=lambda *a, **k: None),
        'sanitize_property_name': lambda name: name.replace('[]', ''),
    }
    exec(compile(ast.Module(body=[func], type_ignores=[]), '<parse_arguments>', 'exec'), namespace)
    return namespace['_parse_arguments']


PROJECT_ID_PARAM = [{'name': 'project_id', 'in': 'path', 'schema': {'type': 'integer'}}]


def test_missing_project_id_filled_from_session(parse_arguments):
    path_params, _, _ = parse_arguments({}, PROJECT_ID_PARAM, {'project_id': 5})
    assert path_params['project_id'] == 5


def test_model_supplied_value_wins(parse_arguments):
    path_params, _, _ = parse_arguments({'project_id': 9}, PROJECT_ID_PARAM, {'project_id': 5})
    assert path_params['project_id'] == 9


def test_schema_default_wins_over_session_default(parse_arguments):
    params = [{'name': 'mode', 'in': 'path', 'schema': {'type': 'string', 'default': 'prompt_lib'}}]
    path_params, _, _ = parse_arguments({}, params, {'mode': 'administration'})
    assert path_params['mode'] == 'prompt_lib'


def test_without_defaults_param_stays_unresolved(parse_arguments):
    path_params, _, _ = parse_arguments({}, PROJECT_ID_PARAM)
    assert 'project_id' not in path_params


def test_unrelated_path_param_not_invented(parse_arguments):
    params = [{'name': 'skill_id', 'in': 'path', 'schema': {'type': 'integer'}}]
    path_params, _, _ = parse_arguments({}, params, {'project_id': 5})
    assert path_params == {}


def test_body_and_query_split_still_works(parse_arguments):
    params = [
        {'name': 'project_id', 'in': 'path', 'schema': {'type': 'integer'}},
        {'name': 'limit', 'in': 'query', 'schema': {'type': 'integer'}},
    ]
    path_params, query_params, body_params = parse_arguments(
        {'limit': 10, 'name': 'incident-triage'}, params, {'project_id': 5}
    )
    assert path_params == {'project_id': 5}
    assert query_params == {'limit': 10}
    assert body_params == {'name': 'incident-triage'}
