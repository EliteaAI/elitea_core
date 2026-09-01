"""Guards the project clamp on MCP builder tool arguments.

Re-broken by letting a model-supplied project_id win over the session's scope, or by
demoting the scope to just another default.
"""
import pathlib
import types

import pytest


MODULE_PATH = pathlib.Path(__file__).resolve().parents[3] / 'utils' / 'mcp_service.py'


class ScopeError(Exception):
    pass


@pytest.fixture(scope='module')
def parse_arguments():
    import ast

    tree = ast.parse(MODULE_PATH.read_text())
    executor = next(
        node for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == 'McpApiToolExecutor'
    )
    func = next(
        node for node in executor.body
        if isinstance(node, ast.FunctionDef) and node.name == '_parse_arguments'
    )
    func.decorator_list = []
    # The value helpers live at module scope and are part of the behaviour under test.
    helpers = [
        node for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name in ('_is_blank', '_same_path_value')
    ]
    assert len(helpers) == 2, 'value helpers moved or were renamed'

    namespace = {
        'log': types.SimpleNamespace(warning=lambda *a, **k: None),
        'sanitize_property_name': lambda name: name.replace('[]', ''),
        'McpProjectScopeError': ScopeError,
    }
    body = helpers + [func]
    exec(compile(ast.Module(body=body, type_ignores=[]), '<parse_arguments>', 'exec'), namespace)
    return namespace['_parse_arguments']


PROJECT_ID_PARAM = [{'name': 'project_id', 'in': 'path', 'schema': {'type': 'integer'}}]
SCOPE = {'project_id': 2}


def test_foreign_project_is_refused(parse_arguments):
    with pytest.raises(ScopeError) as excinfo:
        parse_arguments({'project_id': 7}, PROJECT_ID_PARAM, None, SCOPE)
    assert 'project 2' in str(excinfo.value)
    assert '7' in str(excinfo.value)


def test_matching_project_passes(parse_arguments):
    path_params, _, _ = parse_arguments({'project_id': 2}, PROJECT_ID_PARAM, None, SCOPE)
    assert path_params['project_id'] == 2


def test_string_and_int_forms_of_the_same_project_match(parse_arguments):
    path_params, _, _ = parse_arguments({'project_id': '2'}, PROJECT_ID_PARAM, None, SCOPE)
    assert path_params['project_id'] == 2


def test_omitted_project_is_filled_from_the_scope(parse_arguments):
    path_params, _, _ = parse_arguments({}, PROJECT_ID_PARAM, None, SCOPE)
    assert path_params['project_id'] == 2


def test_scope_beats_the_session_default(parse_arguments):
    path_params, _, _ = parse_arguments({}, PROJECT_ID_PARAM, {'project_id': 5}, SCOPE)
    assert path_params['project_id'] == 2


def test_without_a_scope_a_foreign_project_still_goes_through(parse_arguments):
    path_params, _, _ = parse_arguments({'project_id': 7}, PROJECT_ID_PARAM, {'project_id': 2})
    assert path_params['project_id'] == 7


def test_other_params_are_untouched_by_the_scope(parse_arguments):
    params = [
        {'name': 'project_id', 'in': 'path', 'schema': {'type': 'integer'}},
        {'name': 'skill_id', 'in': 'path', 'schema': {'type': 'integer'}},
        {'name': 'limit', 'in': 'query', 'schema': {'type': 'integer'}},
    ]
    path_params, query_params, body_params = parse_arguments(
        {'project_id': 2, 'skill_id': 42, 'limit': 10, 'name': 'incident-triage'}, params, None, SCOPE
    )
    assert path_params == {'project_id': 2, 'skill_id': 42}
    assert query_params == {'limit': 10}
    assert body_params == {'name': 'incident-triage'}


@pytest.mark.parametrize('blank', [None, '', '   '])
def test_a_blank_project_is_treated_as_omitted_not_denied(parse_arguments, blank):
    path_params, _, _ = parse_arguments({'project_id': blank}, PROJECT_ID_PARAM, None, SCOPE)
    assert path_params['project_id'] == 2


@pytest.mark.parametrize('same', [2, '2', 2.0, ' 2 '])
def test_equivalent_spellings_of_the_scope_are_not_denied(parse_arguments, same):
    path_params, _, _ = parse_arguments({'project_id': same}, PROJECT_ID_PARAM, None, SCOPE)
    assert path_params['project_id'] == 2


def test_a_scoped_param_the_tool_does_not_declare_is_not_invented(parse_arguments):
    params = [{'name': 'skill_id', 'in': 'path', 'schema': {'type': 'integer'}}]
    path_params, _, _ = parse_arguments({'skill_id': 42}, params, None, SCOPE)
    assert path_params == {'skill_id': 42}
