"""Notifications about access to a shared chat (#7757).

- one notification per person whose access really changed, never to the person who made the change
- added: the chat name links to the chat; removed: plain text
- Whole team on / off reaches the project members who are not in the chat
- adding someone to a chat open to the whole project changes nothing for them

    python tests/run_tests.py unit/utils/test_7757_chat_access_notifications.py -v
"""
import ast
import pathlib
import types

import pytest


PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[3]
MODULE = PLUGIN_ROOT / 'utils' / 'chat_access_notifications.py'
API = PLUGIN_ROOT / 'api' / 'v2'


@pytest.fixture
def notes():
    tree = ast.parse(MODULE.read_text())
    keep = [
        node for node in tree.body
        if isinstance(node, ast.FunctionDef)
        or (isinstance(node, ast.Assign) and node.targets[0].id.isupper())
    ]
    state = {'members': [1, 2, 3, 4], 'project': {'name': 'Team Alpha'}}
    fired = []

    rpc = types.SimpleNamespace(timeout=lambda _t: types.SimpleNamespace(
        admin_get_users_ids_in_project=lambda project_id, filter_system_user: state['members'],
        project_get_by_id=lambda project_id: state['project'],
    ))
    event_manager = types.SimpleNamespace(fire_event=lambda name, payload: fired.append((name, payload)))
    namespace = {
        'types': types,
        'log': types.SimpleNamespace(warning=lambda *a, **k: None, exception=lambda *a, **k: None),
        'rpc_tools': types.SimpleNamespace(
            RpcMixin=lambda: types.SimpleNamespace(rpc=rpc),
            EventManagerMixin=lambda: types.SimpleNamespace(event_manager=event_manager),
        ),
        'NotificationEventTypes': types.SimpleNamespace(
            chat_user_added=types.SimpleNamespace(value='chat_user_added'),
            chat_user_removed=types.SimpleNamespace(value='chat_user_removed'),
        ),
        'ParticipantTypes': types.SimpleNamespace(user=types.SimpleNamespace(value='user')),
    }
    exec(compile(ast.Module(body=keep, type_ignores=[]), '<chat_access_notifications>', 'exec'), namespace)
    return types.SimpleNamespace(ns=namespace, fired=fired, state=state)


def user(pid, user_id):
    return types.SimpleNamespace(id=pid, entity_name='user', entity_meta={'id': user_id})


def chat(participants=(), author_id=1, private=True):
    return types.SimpleNamespace(
        id=42, name='Release plan', author_id=author_id, is_private=private, participants=list(participants),
    )


def payloads(notes):
    return [payload for name, payload in notes.fired if name == 'notifications_stream']


def test_added_message_links_the_chat_and_names_the_project(notes):
    notes.ns['notify_chat_access'](3, chat(), [2], added=True, initiator_id=1)

    payload, = payloads(notes)
    assert payload['user_id'] == 2
    assert payload['event_type'] == 'chat_user_added'
    assert payload['meta']['message'] == 'You were added to the [Release plan]() in the Team Alpha project.'
    assert payload['meta']['conversation_id'] == 42
    assert payload['meta']['project_name'] == 'Team Alpha'


def test_removed_message_has_no_link(notes):
    notes.ns['notify_chat_access'](3, chat(), [2], added=False, initiator_id=1)

    payload, = payloads(notes)
    assert payload['event_type'] == 'chat_user_removed'
    assert payload['meta']['message'] == 'You were removed from Release plan in the Team Alpha project.'
    assert '](' not in payload['meta']['message']


@pytest.mark.parametrize('name,phrase', [
    ('Team Alpha', 'the Team Alpha project'),
    ('team project', 'the team project'),
    ('New Project', 'the New Project'),
    ('  Ops  ', 'the Ops project'),
])
def test_project_word_is_not_doubled(notes, name, phrase):
    assert notes.ns['project_phrase'](name) == phrase


def test_each_person_once_and_never_the_initiator(notes):
    notes.ns['notify_chat_access'](3, chat(), [2, '2', 1, 3, None], added=True, initiator_id=1)

    assert [payload['user_id'] for payload in payloads(notes)] == [2, 3]


def test_nobody_to_tell_resolves_nothing(notes):
    notes.ns['notify_chat_access'](3, chat(), [1], added=True, initiator_id=1)

    assert payloads(notes) == []


def test_chat_user_ids_reads_people_only(notes):
    participants = [user(10, 1), user(11, '2'), types.SimpleNamespace(id=12, entity_name='application', entity_meta={'id': 3})]

    assert notes.ns['chat_user_ids'](participants) == {1, 2}


def test_whole_team_reaches_members_outside_the_chat(notes):
    conversation = chat([user(10, 1), user(11, 2)], author_id=1)

    assert notes.ns['team_access_recipients']({1, 2, 3, 4}, conversation) == {3, 4}


def test_project_members_come_without_the_system_user(notes):
    assert notes.ns['project_member_ids'](3) == {1, 2, 3, 4}


def test_a_missing_project_name_keeps_the_message_readable(notes):
    notes.state['project'] = None
    notes.ns['notify_chat_access'](3, chat(), [2], added=True, initiator_id=1)

    payload, = payloads(notes)
    assert payload['meta']['message'] == 'You were added to the [Release plan]().'


def test_removed_user_is_found_by_participant_id(notes):
    conversation = chat([user(10, 1), user(11, 2)])

    assert notes.ns['removed_user_id'](conversation, 11) == 2
    assert notes.ns['removed_user_id'](conversation, 99) is None


def _function(path, name):
    found, = [
        node for node in ast.walk(ast.parse(path.read_text()))
        if isinstance(node, ast.FunctionDef) and node.name == name
    ]
    return ast.unparse(found)


def test_adding_to_a_team_wide_chat_tells_nobody():
    source = _function(PLUGIN_ROOT / 'utils' / 'participant_utils.py', 'notify_user_added_to_conversation')
    assert 'if not conversation.is_private:' in source
    assert 'notify_chat_access' in source


def test_removal_is_told_only_for_a_private_chat_and_not_to_someone_who_left():
    source = _function(API / 'participant.py', 'delete')
    assert 'snapshot.is_private and (not is_self)' in source
    assert 'added=False' in source


def test_whole_team_switch_and_team_wide_creation_tell_the_members():
    assert 'notify_team_access_change' in _function(API / 'conversation.py', 'put')
    assert 'notify_team_access_change' in _function(API / 'conversations.py', 'post')
