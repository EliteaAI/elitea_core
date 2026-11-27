"""Guards message-sending access on private conversations (#6895).

Before the fix, sending a message to any conversation UUID silently made the sender a
participant, so a non-member could join a private chat and then read it. Rules under test:
- participant / author: may send
- anyone else on a private conversation: rejected as 'not found', a project Admin too (#7757)
- public conversations keep auto-join on first message
- support project is exempt
"""
import ast
import pathlib
import types

import pytest


MODULE_PATH = pathlib.Path(__file__).resolve().parents[3] / 'utils' / 'conversation_access.py'
FUNCS = ('check_post_access', 'find_user_participant_id', '_is_support_project')


@pytest.fixture
def access():
    tree = ast.parse(MODULE_PATH.read_text())
    keep = [
        node for node in tree.body
        if (isinstance(node, ast.FunctionDef) and node.name in FUNCS)
        or (isinstance(node, ast.Assign) and node.targets[0].id.isupper())
    ]
    calls = []

    def support_config():
        calls.append('support')
        return {'project_id': 99}

    namespace = {
        'get_support_config': support_config,
        'ParticipantTypes': types.SimpleNamespace(user=types.SimpleNamespace(value='user')),
    }
    exec(compile(ast.Module(body=keep, type_ignores=[]), '<conversation_access>', 'exec'), namespace)
    return types.SimpleNamespace(check=namespace['check_post_access'], calls=calls, ns=namespace)


def member(user_id, pid=10, name='user'):
    return types.SimpleNamespace(id=pid, entity_name=name, entity_meta={'id': user_id})


def conv(private=True, author_id=1, members=()):
    return types.SimpleNamespace(is_private=private, author_id=author_id, participants=list(members))


def test_participant_may_send_without_any_lookup(access):
    assert access.check(5, conv(members=[member(2)]), 2) is None
    assert access.calls == []


def test_outsider_on_private_conversation_is_rejected_as_not_found(access):
    # The original repro: a Viewer who knows the UUID posts into someone's private chat
    denied = access.check(5, conv(members=[member(1)]), 2)
    assert denied == access.ns['NOT_FOUND']
    assert denied[0] == {'error': 'Conversation not found'}


def test_outsider_lookup_is_only_the_support_check(access):
    # No role lookup: a project Admin who was not invited is an outsider too (#7757)
    access.check(5, conv(members=[member(1)]), 2)
    assert access.calls == ['support']


def test_author_who_is_not_a_participant_may_send(access):
    assert access.check(5, conv(author_id=2), 2) is None
    assert access.calls == []


def test_public_conversation_keeps_auto_join(access):
    assert access.check(5, conv(private=False), 2) is None
    assert access.calls == []


def test_support_project_is_exempt(access):
    assert access.check(99, conv(), 2) is None


def test_missing_conversation_is_not_found(access):
    assert access.check(5, None, 2) == access.ns['NOT_FOUND']


@pytest.mark.parametrize('stored', ['2', 2])
def test_participant_id_type_does_not_matter(access, stored):
    # JSON ids may be stored as "2"; a real member must never be treated as an outsider
    assert access.check(5, conv(members=[member(stored)]), 2) is None
    assert access.check(5, conv(members=[member(stored)]), '2') is None
    assert access.calls == []


def test_only_user_participants_count(access):
    # An application participant whose entity id happens to equal the user id is not a member
    assert access.check(5, conv(members=[member(2, name='application')]), 2) == access.ns['NOT_FOUND']


def test_find_user_participant_id(access):
    find = access.ns['find_user_participant_id']
    assert find([member(1, pid=7), member('2', pid=8)], 2) == 8
    assert find([member(1, pid=7)], 2) is None
    assert find([types.SimpleNamespace(id=1, entity_name='user', entity_meta=None)], 2) is None
