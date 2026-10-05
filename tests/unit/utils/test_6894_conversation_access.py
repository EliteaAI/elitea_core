"""Guards the conversation ownership rules (#6894).

Before the fix any project member (even a Viewer) could rename, join, strip participants
from, or delete other users' private conversations by ID. Rules under test:
- author or project admin: everything
- participant: rename / add participants only
- non-participant: 404 on private (no existence leak), 403 on public
"""
import ast
import pathlib
import types

import pytest


MODULE_PATH = pathlib.Path(__file__).resolve().parents[3] / 'utils' / 'conversation_access.py'
PURE = ('decide_access', 'is_privileged_update')


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


def _never_admin():
    raise AssertionError('admin RPC must not be called when author/participant rules suffice')


def decide(access, *, private=True, author=False, participant=False, admin=False, privileged=False):
    is_admin = (lambda: admin) if (admin or not (author or (participant and not privileged))) else _never_admin
    return access.decide_access(private, author, participant, is_admin, privileged)


@pytest.mark.parametrize('private', [True, False])
@pytest.mark.parametrize('privileged', [True, False])
def test_author_can_do_everything_without_admin_lookup(access, private, privileged):
    assert decide(access, private=private, author=True, privileged=privileged) is None


@pytest.mark.parametrize('private', [True, False])
@pytest.mark.parametrize('participant', [True, False])
def test_admin_overrides_like_an_author(access, private, participant):
    assert decide(access, private=private, participant=participant, admin=True, privileged=True) is None


@pytest.mark.parametrize('private', [True, False])
def test_participant_may_rename_or_add(access, private):
    assert decide(access, private=private, participant=True) is None


@pytest.mark.parametrize('private', [True, False])
def test_participant_cannot_do_destructive_actions(access, private):
    assert decide(access, private=private, participant=True, privileged=True) == access.NOT_PRIVILEGED


@pytest.mark.parametrize('privileged', [True, False])
def test_outsider_on_private_conversation_sees_not_found(access, privileged):
    # The original repro: Viewer, not a participant, attacking someone's private chat
    assert decide(access, private=True, privileged=privileged) == access.NOT_FOUND


@pytest.mark.parametrize('privileged', [True, False])
def test_outsider_on_public_conversation_is_forbidden(access, privileged):
    assert decide(access, private=False, privileged=privileged) == access.NOT_PARTICIPANT


def conv(**kw):
    base = dict(instructions='keep', is_private=True, meta={'persona': 'generic', 'steps_limit': 25}, folder_id=None)
    base.update(kw)
    return types.SimpleNamespace(**base)


@pytest.mark.parametrize('data', [
    {'name': 'renamed'},
    # UI resends the whole meta with unchanged persona when toggling steps_limit / internal tools
    {'meta': {'persona': 'generic', 'steps_limit': 50}},
    {'meta': {'internal_tools': ['internal_mcp']}},
    {'instructions': 'keep', 'is_private': True, 'is_hidden': False},
    {'instructions': None, 'is_private': None, 'is_hidden': None},
    {'folder_id': None},
])
def test_participant_level_updates(access, data):
    assert access.is_privileged_update(conv(), data) is False


@pytest.mark.parametrize('data', [
    {'instructions': 'injected'},
    {'is_private': False},
    {'is_hidden': True},
    {'meta': {'persona': 'quirky'}},
    # folder_id/is_hidden are shared across participants, so moving is author-level
    {'folder_id': 7},
])
def test_privileged_updates(access, data):
    assert access.is_privileged_update(conv(), data) is True


def test_removing_from_folder_is_privileged_when_filed(access):
    assert access.is_privileged_update(conv(folder_id=7), {'folder_id': None}) is True
