"""get_system_user_token must delegate the pick to admin's get_project_system_token RPC (#6489).

Rotation (in admin) decides which system-user 'api' tokens stay alive. Picking locally here - e.g.
"newest by name" - silently drifted from that rule and could hand out a token the next rotation
revokes, or a named expiring one. The single owner is admin_get_project_system_token.

The function is compiled from source: predict_utils needs the whole pylon runtime to import.
"""
import ast
import pathlib
import types
from typing import Optional

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]


class FakeRpc:
    def __init__(self, result='jwt-9'):
        self.result = result
        self.calls = []

    def timeout(self, _seconds):
        return self

    def admin_get_project_system_token(self, project_id, **kwargs):
        self.calls.append((project_id, kwargs))
        return self.result


class ForbiddenAuth:
    """Any direct token-table access means the pick has leaked back into elitea_core."""

    def __getattr__(self, name):
        raise AssertionError(f'auth.{name} must not be called; the pick belongs to admin')


def _load(rpc):
    source = (PLUGIN_ROOT / 'utils' / 'predict_utils.py').read_text()
    node = next(n for n in ast.parse(source).body if isinstance(n, ast.FunctionDef) and n.name == 'get_system_user_token')
    namespace = {
        'Optional': Optional,
        'auth': ForbiddenAuth(),
        'rpc_tools': types.SimpleNamespace(RpcMixin=lambda: types.SimpleNamespace(rpc=rpc)),
    }
    exec(compile(ast.Module([node], []), 'predict_utils', 'exec'), namespace)  # noqa: S102
    return namespace['get_system_user_token']


def test_delegates_to_admin_rpc():
    rpc = FakeRpc()
    assert _load(rpc)(5) == 'jwt-9'
    assert rpc.calls == [(5, {'create_if_not_exists': True})]


def test_passes_create_flag_through():
    rpc = FakeRpc(result=None)
    assert _load(rpc)(5, create_if_not_exists=False) is None
    assert rpc.calls == [(5, {'create_if_not_exists': False})]
