"""Guards the cost of the REST ownership check (#6894 follow-up).

The check runs on every conversation PUT, and the UI issues one after each chat turn. The
author (the common caller) must be answered without any RPC; the support-project RPC is only
worth paying for when access would otherwise be denied.
"""
import ast
import pathlib
import types

import pytest


MODULE_PATH = pathlib.Path(__file__).resolve().parents[3] / 'utils' / 'conversation_access.py'
FUNCS = ('decide_access', 'check_conversation_access', 'find_user_participant_id', '_admin_checker', '_is_support_project')


@pytest.fixture
def access():
    tree = ast.parse(MODULE_PATH.read_text())
    keep = [
        node for node in tree.body
        if (isinstance(node, ast.FunctionDef) and node.name in FUNCS)
        or (isinstance(node, ast.Assign) and node.targets[0].id.isupper())
    ]
    calls = {'admin': 0, 'support': 0}
    state = {'admin': False, 'support_project': 99}

    def admin_check(project_id, user_id):
        calls['admin'] += 1
        return state['admin']

    def support_config():
        calls['support'] += 1
        return {'project_id': state['support_project']}

    rpc = types.SimpleNamespace(timeout=lambda _t: types.SimpleNamespace(admin_check_user_is_admin=admin_check))
    namespace = {
        'rpc_tools': types.SimpleNamespace(RpcMixin=lambda: types.SimpleNamespace(rpc=rpc)),
        'get_support_config': support_config,
        'ParticipantTypes': types.SimpleNamespace(user=types.SimpleNamespace(value='user')),
    }
    exec(compile(ast.Module(body=keep, type_ignores=[]), '<conversation_access>', 'exec'), namespace)
    return types.SimpleNamespace(check=namespace['check_conversation_access'], calls=calls, state=state, ns=namespace)


def conv(author_id=1, private=True, member_ids=()):
    members = [types.SimpleNamespace(id=i, entity_name='user', entity_meta={'id': m}) for i, m in enumerate(member_ids)]
    return types.SimpleNamespace(author_id=author_id, is_private=private, participants=members)


@pytest.mark.parametrize('privileged', [True, False])
def test_author_needs_no_rpc_at_all(access, privileged):
    assert access.check(5, conv(author_id=2), 2, needs_privilege=privileged) is None
    assert access.calls == {'admin': 0, 'support': 0}


def test_participant_rename_needs_no_rpc(access):
    assert access.check(5, conv(member_ids=[2]), 2) is None
    assert access.calls == {'admin': 0, 'support': 0}


def test_denial_in_a_normal_project_checks_support_once_and_still_denies(access):
    assert access.check(5, conv(), 2, needs_privilege=True) == access.ns['NOT_FOUND']
    assert access.calls == {'admin': 1, 'support': 1}


def test_support_project_still_bypasses_a_denial(access):
    assert access.check(99, conv(), 2, needs_privilege=True) is None


def test_admin_override_skips_the_support_lookup(access):
    access.state['admin'] = True
    assert access.check(5, conv(), 2, needs_privilege=True) is None
    assert access.calls == {'admin': 1, 'support': 0}
