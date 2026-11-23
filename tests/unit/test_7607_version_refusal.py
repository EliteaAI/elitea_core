"""EL-7607: `folder_access.version_refusal` answers a missing version and one in a hidden folder alike."""
import ast
import pathlib
from typing import Optional

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
NOT_FOUND = {'error': 'The requested item could not be found'}, 404
READ_ONLY = {'ok': False, 'error': 'You have read-only access to this folder'}, 403


@pytest.fixture
def folder():
    tree = ast.parse((PLUGIN_ROOT / 'utils' / 'folder_access.py').read_text())
    nodes = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == 'version_refusal']
    state = {'applications': {10: 1}, 'answer': None, 'calls': []}

    def entity_access_error(project_id, entity_types, entity_id, write=False, user_id=None):
        state['calls'].append((project_id, entity_types, entity_id, write))
        return state['answer']

    namespace = {
        'Optional': Optional, 'APPLICATION_ENTITY_TYPES': ['agent', 'pipeline'],
        'application_id_from_version': lambda project_id, version_id: state['applications'].get(version_id),
        'entity_access_error': entity_access_error,
    }
    exec(compile(ast.Module(nodes, []), 'folder_access.py', 'exec'), namespace)  # pylint: disable=W0122
    state['refusal'] = namespace['version_refusal']
    return state


def test_el7607_a_missing_version_answers_not_found_without_a_folder_lookup(folder):
    assert folder['refusal'](7, 99, NOT_FOUND) == NOT_FOUND
    assert folder['calls'] == []


@pytest.mark.parametrize('write', [False, True])
def test_el7607_a_hidden_version_answers_like_a_missing_one(folder, write):
    folder['answer'] = ({'ok': False, 'error': 'Not found'}, 404)
    assert folder['refusal'](7, 10, NOT_FOUND, write=write) == NOT_FOUND
    assert folder['calls'] == [(7, ['agent', 'pipeline'], 1, write)]


def test_el7607_a_read_only_folder_keeps_its_403(folder):
    folder['answer'] = READ_ONLY
    assert folder['refusal'](7, 10, NOT_FOUND, write=True) == READ_ONLY


def test_el7607_a_visible_version_is_not_refused(folder):
    assert folder['refusal'](7, 10, NOT_FOUND) is None
