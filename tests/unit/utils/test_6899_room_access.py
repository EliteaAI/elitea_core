"""Guards Socket.IO room joins on private conversations (#6899).

Before the fix, chat_enter_room joined any conversation in the project by sequential id,
so a Viewer could enumerate ids and receive another user's private live stream. Rules:
- public conversation: anyone in the project may join, no lookups at all
- private: author / participant may join without any RPC
- private non-member: only the support project; a project Admin is an outsider (#7757),
  except on a run (single_participant), whose history project admins read (#6927)
- the participant lookup only runs for private conversations the user does not own
"""
import ast
import pathlib
import types

import pytest


MODULE_PATH = pathlib.Path(__file__).resolve().parents[3] / 'utils' / 'conversation_access.py'
FUNCS = (
    'can_join_room', 'room_access_facts', 'find_user_participant_id', '_is_support_project',
    '_admin_checker', 'is_run_conversation',
)


@pytest.fixture
def access():
    tree = ast.parse(MODULE_PATH.read_text())
    keep = [
        node for node in tree.body
        if (isinstance(node, ast.FunctionDef) and node.name in FUNCS)
        or (isinstance(node, ast.Assign) and node.targets[0].id.isupper())
    ]
    calls = []
    lookups = []
    state = {'support': 99, 'participant': False, 'admin': False, 'raise': False}

    def admin_check(project_id, user_id):
        calls.append('admin')
        if state['raise']:
            raise TimeoutError('rpc timeout')
        return state['admin']

    def support_config():
        calls.append('support')
        return {'project_id': state['support']}

    def is_conversation_participant(session, conversation_id, user_id):
        lookups.append((conversation_id, user_id))
        return state['participant']

    rpc = types.SimpleNamespace(timeout=lambda _t: types.SimpleNamespace(admin_check_user_is_admin=admin_check))
    namespace = {
        'rpc_tools': types.SimpleNamespace(RpcMixin=lambda: types.SimpleNamespace(rpc=rpc)),
        'get_support_config': support_config,
        'ParticipantTypes': types.SimpleNamespace(user=types.SimpleNamespace(value='user')),
        'log': types.SimpleNamespace(warning=lambda *a, **k: None),
        'is_conversation_participant': is_conversation_participant,
    }
    exec(compile(ast.Module(body=keep, type_ignores=[]), '<conversation_access>', 'exec'), namespace)
    return types.SimpleNamespace(
        join=namespace['can_join_room'], facts=namespace['room_access_facts'],
        find=namespace['find_user_participant_id'], state=state, calls=calls, lookups=lookups,
    )


def facts(private=True, author=False, participant=False, run=False):
    return {'is_private': private, 'is_author': author, 'is_participant': participant, 'is_run': run}


def test_public_conversation_is_joinable_without_lookup(access):
    assert access.join(5, 2, **facts(private=False)) is True
    assert access.calls == []


def test_author_and_participant_join_private_without_lookup(access):
    assert access.join(5, 2, **facts(author=True)) is True
    assert access.join(5, 2, **facts(participant=True)) is True
    assert access.calls == []


def test_outsider_is_denied_private_room(access):
    # The original repro: a Viewer enumerating conversation ids; a project Admin fares no better (#7757)
    assert access.join(5, 2, **facts()) is False
    assert access.calls == ['support']


def test_support_project_is_exempt(access):
    # Same ordering as check_conversation_access: support is only consulted about to deny
    assert access.join(99, 2, **facts()) is True


def test_project_admin_joins_a_run_room_but_not_a_chat_room(access):
    access.state['admin'] = True
    assert access.join(5, 2, **facts(run=True)) is True
    assert access.join(5, 2, **facts()) is False


def test_admin_rpc_failure_on_a_run_fails_closed(access):
    access.state['raise'] = True
    assert access.join(5, 2, **facts(run=True)) is False


def test_run_flag_comes_from_single_participant(access):
    assert access.facts(None, 7, False, 1, 2, meta={'single_participant': {'id': 3}}) == facts(private=False, run=True)


def test_public_conversation_skips_participant_lookup(access):
    assert access.facts(None, 7, False, 1, 2) == facts(private=False)
    assert access.lookups == []


def test_author_skips_participant_lookup(access):
    assert access.facts(None, 7, True, 2, 2) == facts(author=True)
    assert access.lookups == []


def test_private_non_author_looks_up_participation_once(access):
    access.state['participant'] = True
    assert access.facts(None, 7, True, 1, 2) == facts(participant=True)
    assert access.lookups == [(7, 2)]


def test_participant_match_tolerates_string_ids(access):
    members = [types.SimpleNamespace(id=10, entity_name='user', entity_meta={'id': '2'})]
    assert access.find(members, 2) == 10
