"""Conversation export (#7061, #7165, #7723, #7757).

The size summary is open to everyone who can open the chat (the Share window reads it). The
export itself (JSON, ZIP) is for the owner or a manager; an invited Viewer is refused (#7723).

    python tests/run_tests.py unit/test_7061_participant_export.py -v
"""
import ast
import pathlib
import types

import pytest


PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
EXPORT_API = PLUGIN_ROOT / 'api' / 'v2' / 'conversation_export.py'
ACCESS_MODULE = PLUGIN_ROOT / 'utils' / 'conversation_access.py'


def _access_calls():
    tree = ast.parse(EXPORT_API.read_text())
    return [
        node for node in ast.walk(tree)
        if isinstance(node, ast.Call) and getattr(node.func, 'id', None) == 'check_conversation_access'
    ]


@pytest.fixture(scope='module')
def access():
    tree = ast.parse(ACCESS_MODULE.read_text())
    keep = [
        node for node in tree.body
        if (isinstance(node, ast.FunctionDef) and node.name == 'decide_access')
        or (isinstance(node, ast.Assign) and node.targets[0].id.isupper())
    ]
    namespace = {}
    exec(compile(ast.Module(body=keep, type_ignores=[]), '<conversation_access>', 'exec'), namespace)
    return types.SimpleNamespace(**namespace)


def test_export_checks_access_once_for_every_chat():
    # JSON, ZIP and ?summary=true share this single check; public chats are not skipped any more
    call, = _access_calls()
    guards = [
        node for node in ast.walk(ast.parse(EXPORT_API.read_text()))
        if isinstance(node, ast.If) and node.lineno < call.lineno <= node.end_lineno
    ]
    assert not guards


def test_export_needs_privilege_unless_it_is_the_summary():
    call, = _access_calls()
    privileged, = [kw for kw in call.keywords if kw.arg == 'needs_privilege']
    assert ast.unparse(privileged.value) == 'not summary_only'


@pytest.mark.parametrize('private', [True, False])
def test_manager_can_export(access, private):
    assert access.decide_access(private, False, True, lambda: True, True) is None


@pytest.mark.parametrize('private', [True, False])
def test_invited_viewer_cannot_export(access, private):
    assert access.decide_access(private, False, True, lambda: False, True) == access.NOT_MANAGER


@pytest.mark.parametrize('private', [True, False])
def test_anyone_with_access_reads_the_summary(access, private):
    assert access.decide_access(private, False, True, lambda: False, False) is None


def test_outsider_on_private_conversation_sees_not_found(access):
    assert access.decide_access(True, False, False, lambda: True, True) == access.NOT_FOUND
