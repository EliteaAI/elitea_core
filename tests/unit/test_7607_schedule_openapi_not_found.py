"""EL-7607: the Swagger example of every Schedules 404 shows the body the server answers, not an entity name."""
import ast
import pathlib

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]


def _load(relative_path, names, namespace):
    tree = ast.parse((PLUGIN_ROOT / relative_path).read_text())
    nodes = [node for node in tree.body if (
        isinstance(node, (ast.FunctionDef, ast.ClassDef)) and node.name in names
    ) or (
        isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id in names for t in node.targets)
    )]
    exec(compile(ast.Module(nodes, []), relative_path, 'exec'), namespace)  # pylint: disable=W0122
    return namespace


@pytest.fixture(scope='module')
def spec():
    namespace = _load('utils/schedule_service.py',
                      {'NOT_FOUND_TEXT', 'refusal', 'ScheduleError', 'ScheduleNotFound'}, {})
    return _load('utils/schedule_openapi.py',
                 {'ERRORS', 'DEFAULT_CODES', 'REFUSAL_NAME', 'REFUSAL_REF', '_responses'}, namespace)


@pytest.mark.parametrize('description', [None, 'The agent, pipeline or version is not found or not visible'])
def test_el7607_a_404_example_is_the_body_the_server_answers(spec, description):
    answer = spec['_responses'](200, 'ok', None, {404: description})['404']
    example = answer['content']['application/json']['example']
    assert example == spec['ScheduleNotFound']().response[0]
    assert answer['description'] == (description or spec['ERRORS'][404])


def test_el7607_other_errors_keep_their_own_text(spec):
    answer = spec['_responses'](200, 'ok', None, {409: 'The project limit of active schedules is reached'})['409']
    assert answer['content']['application/json']['example']['error'] == 'The project limit of active schedules is reached'
