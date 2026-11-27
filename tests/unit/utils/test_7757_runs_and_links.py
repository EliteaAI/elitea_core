"""Runs stay runs and every link of a shared chat stays in its managers' hands (#7757 review).

- `meta.single_participant` makes a chat a run, which project admins may read: only the run starter writes it,
  when the chat is created, and an update never adds, changes or removes it
- a run holds no one else's messages: people cannot be added to it
- the owner and the managers list and revoke every link of the chat, whoever made it

    python tests/run_tests.py unit/utils/test_7757_runs_and_links.py -v
"""
import ast
import pathlib
import sys
import types
from typing import Optional

import pytest


PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[3]
UTILS = PLUGIN_ROOT / 'utils'
API = PLUGIN_ROOT / 'api' / 'v2'
RPC = PLUGIN_ROOT / 'rpc'


def _load(path, names, namespace):
    tree = ast.parse(path.read_text())
    keep = [
        node for node in tree.body
        if (isinstance(node, ast.FunctionDef) and node.name in names)
        or (isinstance(node, ast.Assign) and isinstance(node.targets[0], ast.Name) and node.targets[0].id.isupper())
    ]
    exec(compile(ast.Module(body=keep, type_ignores=[]), str(path), 'exec'), namespace)
    return types.SimpleNamespace(**namespace)


def _function(path, name):
    found = [
        node for node in ast.walk(ast.parse(path.read_text()))
        if isinstance(node, ast.FunctionDef) and node.name == name
    ]
    return ast.unparse(found[0])


@pytest.fixture
def usage():
    return _load(UTILS / 'chat_usage.py', {'strip_server_owned_meta'}, {'Optional': Optional})


RUN = {'entity_name': 'application', 'entity_meta': {'id': 5, 'project_id': 3}}


def test_an_update_cannot_turn_a_chat_into_a_run(usage):
    assert 'single_participant' not in usage.strip_server_owned_meta({'single_participant': RUN, 'x': 1}, {})


def test_an_update_keeps_the_run_it_does_not_name(usage):
    stored = {'single_participant': RUN}
    assert usage.strip_server_owned_meta({'single_participant': {'entity_meta': {'id': 9}}}, stored)[
        'single_participant'] == RUN
    assert usage.strip_server_owned_meta({}, stored)['single_participant'] == RUN


def test_the_run_starter_still_writes_it_at_creation(usage):
    # A new chat drops only the server-owned keys: the agent, pipeline, skill and toolkit runs name their entity
    assert 'single_participant' not in usage.SERVER_OWNED_META_KEYS


@pytest.fixture
def access():
    return _load(UTILS / 'conversation_access.py', {'invites_people', 'check_run_people', 'is_run_conversation'}, {
        'ParticipantTypes': types.SimpleNamespace(user=types.SimpleNamespace(value='user')),
    })


def person(user_id, entity_name='user'):
    return types.SimpleNamespace(entity_name=entity_name, entity_meta=types.SimpleNamespace(id=user_id))


@pytest.mark.parametrize('meta,participants,denied', [
    ({'single_participant': RUN}, [person(2)], True),
    ({'single_participant': RUN}, [person('2')], True),
    ({'single_participant': RUN}, [person(1)], False),
    ({'single_participant': RUN}, [person(5, 'application')], False),
    ({'single_participant': RUN}, None, False),
    ({}, [person(2)], False),
    (None, [person(2)], False),
])
def test_a_run_takes_no_other_people(access, meta, participants, denied):
    assert (access.check_run_people(meta, participants, 1) == access.PEOPLE_IN_RUN) is denied


def test_an_enum_entity_name_counts_as_a_person(access):
    enum_user = types.SimpleNamespace(value='user')
    assert access.invites_people([person(2, enum_user)], 1) is True


@pytest.mark.parametrize('module,method', [('participants.py', 'post'), ('conversations.py', 'post')])
def test_adding_people_checks_for_a_run_first(module, method):
    assert 'check_run_people' in _function(API / module, method)


@pytest.fixture
def links(monkeypatch):
    state = {'share': types.SimpleNamespace(conversation_id=42), 'denied': None, 'checked': []}

    class Query:
        def filter_by(self, **kwargs):
            return self

        def first(self):
            return state['share']

    class Session:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def query(self, *columns):
            return Query()

    def check_access_by_id(project_id, conversation_id, user_id, needs_privilege=False):
        state['checked'].append((conversation_id, needs_privilege))
        return state['denied']

    # The function imports the model lazily; a stub package answers that import
    models_all = types.ModuleType('stub_7757.models.all')
    models_all.ConversationShareToken = types.SimpleNamespace(conversation_id='conversation_id')
    for name in ('stub_7757', 'stub_7757.utils', 'stub_7757.models'):
        monkeypatch.setitem(sys.modules, name, types.ModuleType(name))
    monkeypatch.setitem(sys.modules, 'stub_7757.models.all', models_all)
    namespace = {
        '__name__': 'stub_7757.utils.conversation_access',
        '__package__': 'stub_7757.utils',
        'db': types.SimpleNamespace(get_session=lambda project_id: Session()),
        'check_access_by_id': check_access_by_id,
    }
    module = _load(UTILS / 'conversation_access.py', {'managed_link_conversation'}, namespace)
    return types.SimpleNamespace(managed=lambda: module.managed_link_conversation(3, 'tok', 7), state=state)


def test_a_manager_may_revoke_any_link_of_the_chat(links):
    assert links.managed() == 42
    assert links.state['checked'] == [(42, True)]


def test_someone_who_does_not_manage_the_chat_gets_nothing(links):
    links.state['denied'] = ({'error': 'x'}, 403)
    assert links.managed() is None


def test_an_unknown_link_gets_nothing(links):
    links.state['share'] = None
    assert links.managed() is None
    assert links.state['checked'] == []


def test_managers_list_every_link_and_others_their_own():
    listing = _function(RPC / 'shared_chat_links.py', 'list_share_tokens')
    assert 'if not can_manage:' in listing
    assert "filters['created_by'] = user_id" in listing
    api = _function(API / 'shared_chat_links.py', 'get')
    assert 'can_manage=check_access_by_id(project_id, conversation_id, user_id, needs_privilege=True) is None' in api


def test_revoke_lets_the_chat_s_managers_through():
    revoke = _function(RPC / 'shared_chat_links.py', 'revoke_share_token')
    assert 'share.created_by != user_id and share.conversation_id != managed_conversation_id' in revoke
    api = _function(API / 'shared_chat_link.py', 'delete')
    assert 'managed_conversation_id=managed_link_conversation(project_id, token, user_id)' in api
