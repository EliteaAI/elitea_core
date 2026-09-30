"""get_system_user_token must hand out the newest 'api' token (#6489).

Rotation keeps the Vault token (the newest) plus the previous one and deletes the rest. Predicts that
took the first row of the unordered token list held the older one, which the next rotation revokes.

The function is compiled from source: predict_utils needs the whole pylon runtime to import.
"""
import ast
import pathlib
import types
from typing import Optional

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]


class FakeAuth:
    def __init__(self, tokens):
        self.tokens = tokens
        self.added = []

    def list_tokens(self, user_id):
        return list(self.tokens)

    def encode_token(self, token_id):
        return f'jwt-{token_id}'

    def add_token(self, user_id, name):
        self.added.append((user_id, name))
        return 999


def _load(auth):
    source = (PLUGIN_ROOT / 'utils' / 'predict_utils.py').read_text()
    node = next(n for n in ast.parse(source).body if isinstance(n, ast.FunctionDef) and n.name == 'get_system_user_token')
    rpc = types.SimpleNamespace(timeout=lambda _s: types.SimpleNamespace(admin_get_project_system_user=lambda pid: {'id': 7}))
    namespace = {
        'Optional': Optional,
        'auth': auth,
        'rpc_tools': types.SimpleNamespace(RpcMixin=lambda: types.SimpleNamespace(rpc=rpc)),
    }
    exec(compile(ast.Module([node], []), 'predict_utils', 'exec'), namespace)  # noqa: S102
    return namespace['get_system_user_token']


def _tok(token_id, name='api'):
    return {'id': token_id, 'name': name, 'expires': None}


def test_newest_token_wins_regardless_of_list_order():
    for order in ([_tok(5), _tok(9)], [_tok(9), _tok(5)]):
        assert _load(FakeAuth(order))(1) == 'jwt-9'


def test_other_names_are_ignored():
    assert _load(FakeAuth([_tok(5), _tok(50, 'my-ci')]))(1) == 'jwt-5'


def test_missing_token_is_created():
    auth = FakeAuth([_tok(50, 'my-ci')])
    assert _load(auth)(1) == 'jwt-999'
    assert auth.added == [(7, 'api')]


def test_missing_token_is_not_created_when_disabled():
    assert _load(FakeAuth([]))(1, create_if_not_exists=False) is None
