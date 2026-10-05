"""Guards message-sending access on private conversations (#6895).

Before the fix, sending a message to any conversation UUID silently made the sender a
participant, so a non-member could join a private chat and then read it. Rules under test:
- participant / author / project admin: may send
- anyone else on a private conversation: rejected as 'not found', no admin lookup for members
- public conversations keep auto-join on first message
- support project is exempt
"""
import ast
import pathlib
import types

import pytest


MODULE_PATH = pathlib.Path(__file__).resolve().parents[3] / 'utils' / 'conversation_access.py'
FUNCS = ('decide_access', 'check_post_access')


@pytest.fixture
def access():
    tree = ast.parse(MODULE_PATH.read_text())
    keep = [
        node for node in tree.body
        if (isinstance(node, ast.FunctionDef) and node.name in FUNCS)
        or (isinstance(node, ast.Assign) and node.targets[0].id.isupper())
    ]
    calls = []
    admin = {'value': False}

    def admin_check(project_id, user_id):
        calls.append((project_id, user_id))
        return admin['value']

    rpc = types.SimpleNamespace(timeout=lambda _t: types.SimpleNamespace(admin_check_user_is_admin=admin_check))
    namespace = {
        'rpc_tools': types.SimpleNamespace(RpcMixin=lambda: types.SimpleNamespace(rpc=rpc)),
        'get_support_config': lambda: {'project_id': 99},
    }
    exec(compile(ast.Module(body=keep, type_ignores=[]), '<conversation_access>', 'exec'), namespace)
    return types.SimpleNamespace(check=namespace['check_post_access'], admin=admin, calls=calls, ns=namespace)


def conv(private=True, author_id=1):
    return types.SimpleNamespace(is_private=private, author_id=author_id)


def test_participant_may_send_without_any_lookup(access):
    assert access.check(None, 5, conv(), user_id=2, is_participant=True) is None
    assert access.calls == []


def test_outsider_on_private_conversation_is_rejected_as_not_found(access):
    # The original repro: a Viewer who knows the UUID posts into someone's private chat
    denied = access.check(None, 5, conv(), user_id=2, is_participant=False)
    assert denied == access.ns['NOT_FOUND']
    assert denied[0] == {'error': 'Conversation not found'}


def test_project_admin_may_send_to_private_conversation(access):
    access.admin['value'] = True
    assert access.check(None, 5, conv(), user_id=2, is_participant=False) is None
    assert access.calls == [(5, 2)]


def test_author_who_is_not_a_participant_may_send(access):
    assert access.check(None, 5, conv(author_id=2), user_id=2, is_participant=False) is None
    assert access.calls == []


def test_public_conversation_keeps_auto_join(access):
    assert access.check(None, 5, conv(private=False), user_id=2, is_participant=False) is None
    assert access.calls == []


def test_support_project_is_exempt(access):
    assert access.check(None, 99, conv(), user_id=2, is_participant=False) is None
    assert access.calls == []
