"""Conversation export is participant-level (#7061).

Every current participant may export (JSON, ZIP, size summary); outsiders are still denied
like on other participant-level endpoints.

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


def test_export_checks_access_once():
    # JSON, ZIP and ?summary=true share this single check
    assert len(_access_calls()) == 1


def test_export_does_not_require_owner_privilege():
    call, = _access_calls()
    privileged = [kw for kw in call.keywords if kw.arg == 'needs_privilege']
    assert not privileged or ast.literal_eval(privileged[0].value) is False


@pytest.mark.parametrize('private', [True, False])
def test_participant_can_export(access, private):
    assert access.decide_access(private, False, True, lambda: False, False) is None


def test_outsider_on_private_conversation_sees_not_found(access):
    assert access.decide_access(True, False, False, lambda: False, False) == access.NOT_FOUND


def test_outsider_on_public_conversation_is_forbidden(access):
    assert access.decide_access(False, False, False, lambda: False, False) == access.NOT_PARTICIPANT
