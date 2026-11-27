"""Guards the cost of the REST ownership check (#6894 follow-up, #7757).

The check runs on every conversation PUT, and the UI issues one after each chat turn. The
author (the common caller) and plain updates by people with access must be answered without
any lookup; permissions are only resolved for a privileged change, and the support-project
RPC is only worth paying for when access would otherwise be denied.
"""
import ast
import pathlib
import types

import pytest


MODULE_PATH = pathlib.Path(__file__).resolve().parents[3] / 'utils' / 'conversation_access.py'
FUNCS = (
    'decide_access', 'check_conversation_access', 'find_user_participant_id', '_manage_checker', '_is_support_project',
)
MANAGE_SHARED = 'models.chat.conversations.manage_shared'


@pytest.fixture
def access():
    tree = ast.parse(MODULE_PATH.read_text())
    keep = [
        node for node in tree.body
        if (isinstance(node, ast.FunctionDef) and node.name in FUNCS)
        or (isinstance(node, ast.Assign) and node.targets[0].id.isupper())
    ]
    calls = {'permissions': 0, 'support': 0}
    state = {'permissions': set(), 'support_project': 99}

    def resolve_permissions(mode, project_id):
        calls['permissions'] += 1
        assert mode == 'default'
        return state['permissions']

    def support_config():
        calls['support'] += 1
        return {'project_id': state['support_project']}

    namespace = {
        'auth': types.SimpleNamespace(resolve_permissions=resolve_permissions),
        'c': types.SimpleNamespace(DEFAULT_MODE='default'),
        'get_support_config': support_config,
        'ParticipantTypes': types.SimpleNamespace(user=types.SimpleNamespace(value='user')),
    }
    exec(compile(ast.Module(body=keep, type_ignores=[]), '<conversation_access>', 'exec'), namespace)
    return types.SimpleNamespace(check=namespace['check_conversation_access'], calls=calls, state=state, ns=namespace)


def conv(author_id=1, private=True, member_ids=()):
    members = [types.SimpleNamespace(id=i, entity_name='user', entity_meta={'id': m}) for i, m in enumerate(member_ids)]
    return types.SimpleNamespace(author_id=author_id, is_private=private, participants=members)


@pytest.mark.parametrize('privileged', [True, False])
def test_author_needs_no_lookup_at_all(access, privileged):
    assert access.check(5, conv(author_id=2), 2, needs_privilege=privileged) is None
    assert access.calls == {'permissions': 0, 'support': 0}


def test_participant_plain_update_needs_no_lookup(access):
    assert access.check(5, conv(member_ids=[2]), 2) is None
    assert access.calls == {'permissions': 0, 'support': 0}


def test_privileged_update_resolves_permissions_once(access):
    access.state['permissions'] = {MANAGE_SHARED}
    assert access.check(5, conv(member_ids=[2]), 2, needs_privilege=True) is None
    assert access.calls == {'permissions': 1, 'support': 0}


def test_viewer_is_refused_after_one_lookup_and_one_support_check(access):
    assert access.check(5, conv(member_ids=[2]), 2, needs_privilege=True) == access.ns['NOT_MANAGER']
    assert access.calls == {'permissions': 1, 'support': 1}


def test_outsider_is_refused_without_resolving_permissions(access):
    access.state['permissions'] = {MANAGE_SHARED}
    assert access.check(5, conv(), 2, needs_privilege=True) == access.ns['NOT_FOUND']
    assert access.calls == {'permissions': 0, 'support': 1}


def test_support_project_still_bypasses_a_denial(access):
    assert access.check(99, conv(), 2, needs_privilege=True) is None
