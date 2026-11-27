"""Guards the conversation ownership rules (#6894, #7757).

Before #6894 any project member (even a Viewer) could rename, join, strip participants
from, or delete other users' private conversations by ID. Rules under test (#7757):
- author: everything
- anyone with access (participant, or any member on a public chat): plain updates
- privileged updates: also a holder of manage_shared
- no access to a private chat: 404 (no existence leak), whatever the project role
"""
import ast
import pathlib
import types

import pytest


MODULE_PATH = pathlib.Path(__file__).resolve().parents[3] / 'utils' / 'conversation_access.py'
PURE = ('decide_access', 'is_privileged_update', 'changes_privacy')


@pytest.fixture(scope='module')
def access():
    tree = ast.parse(MODULE_PATH.read_text())
    keep = [
        node for node in tree.body
        if (isinstance(node, ast.FunctionDef) and node.name in PURE)
        or (isinstance(node, ast.Assign) and node.targets[0].id.isupper())
    ]
    namespace = {}
    exec(compile(ast.Module(body=keep, type_ignores=[]), '<conversation_access>', 'exec'), namespace)
    return types.SimpleNamespace(**namespace)


def _never_resolved():
    raise AssertionError('permissions must not be resolved when author/participant rules suffice')


def decide(access, *, private=True, author=False, participant=False, manager=False, privileged=False):
    needs_lookup = not author and (participant or not private) and privileged
    can_manage = (lambda: manager) if needs_lookup else _never_resolved
    return access.decide_access(private, author, participant, can_manage, privileged)


@pytest.mark.parametrize('private', [True, False])
@pytest.mark.parametrize('privileged', [True, False])
def test_author_can_do_everything_without_lookup(access, private, privileged):
    assert decide(access, private=private, author=True, privileged=privileged) is None


@pytest.mark.parametrize('private', [True, False])
def test_manager_with_access_may_do_privileged_updates(access, private):
    assert decide(access, private=private, participant=True, manager=True, privileged=True) is None


def test_manager_on_a_public_chat_needs_no_participation(access):
    # Whole team: every project member has access, so the permission decides
    assert decide(access, private=False, manager=True, privileged=True) is None


@pytest.mark.parametrize('private', [True, False])
def test_access_is_enough_for_plain_updates(access, private):
    assert decide(access, private=private, participant=True) is None
    assert decide(access, private=False) is None


@pytest.mark.parametrize('private', [True, False])
def test_viewer_with_access_cannot_do_privileged_updates(access, private):
    assert decide(access, private=private, participant=True, privileged=True) == access.NOT_MANAGER


@pytest.mark.parametrize('privileged', [True, False])
@pytest.mark.parametrize('manager', [True, False])
def test_outsider_on_private_conversation_sees_not_found(access, privileged, manager):
    # A project Admin gets no access to a private chat by role alone (#7757)
    assert decide(access, private=True, manager=manager, privileged=privileged) == access.NOT_FOUND


def conv(**kw):
    base = dict(
        name='keep', instructions='keep', is_private=True, folder_id=None,
        meta={'persona': 'generic', 'steps_limit': 25, 'internal_tools': ['web']},
    )
    base.update(kw)
    return types.SimpleNamespace(**base)


@pytest.mark.parametrize('data', [
    {'name': 'keep'},
    # UI resends the whole meta with unchanged persona / tools when toggling steps_limit
    {'meta': {'persona': 'generic', 'steps_limit': 50}},
    {'meta': {'internal_tools': ['web'], 'steps_limit': 50}},
    {'instructions': 'keep', 'is_private': True, 'is_hidden': False},
    {'instructions': None, 'is_private': None, 'is_hidden': None},
    # Folder moves are owner-only and checked apart
    {'folder_id': 7},
])
def test_plain_updates(access, data):
    assert access.is_privileged_update(conv(), data) is False


@pytest.mark.parametrize('data', [
    {'name': 'renamed'},
    {'instructions': 'injected'},
    {'is_private': False},
    {'is_hidden': True},
    {'meta': {'persona': 'quirky'}},
    # Modules are shared by everyone in the chat (#7757)
    {'meta': {'internal_tools': ['web', 'elitea_builder']}},
])
def test_privileged_updates(access, data):
    assert access.is_privileged_update(conv(), data) is True
