"""Trace-step endpoints are ELITEA MCP tools and check conversation access first.

Tool inputs and outputs can hold secrets, so a private conversation's traces are readable only by
whoever can open the chat; public chats stay open to the project (#7165).

    python tests/run_tests.py unit/test_trace_steps_mcp_access.py -v
"""
import ast
import pathlib
import types

import pytest


PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
ACCESS_MODULE = PLUGIN_ROOT / 'utils' / 'conversation_access.py'
TRACE_APIS = {
    'chat_list_trace_steps': PLUGIN_ROOT / 'api' / 'v2' / 'message_traces.py',
    'chat_get_trace_step': PLUGIN_ROOT / 'api' / 'v2' / 'message_trace.py',
}
FUNCS = ('decide_access', 'check_conversation_access', 'check_read_access', 'find_user_participant_id',
         '_admin_checker', '_is_support_project')


@pytest.fixture
def access():
    tree = ast.parse(ACCESS_MODULE.read_text())
    keep = [
        node for node in tree.body
        if (isinstance(node, ast.FunctionDef) and node.name in FUNCS)
        or (isinstance(node, ast.Assign) and node.targets[0].id.isupper())
    ]
    state = {'admin': False, 'support_project': 99}
    rpc = types.SimpleNamespace(timeout=lambda _t: types.SimpleNamespace(
        admin_check_user_is_admin=lambda project_id, user_id: state['admin']))
    namespace = {
        'rpc_tools': types.SimpleNamespace(RpcMixin=lambda: types.SimpleNamespace(rpc=rpc)),
        'get_support_config': lambda: {'project_id': state['support_project']},
        'ParticipantTypes': types.SimpleNamespace(user=types.SimpleNamespace(value='user')),
    }
    exec(compile(ast.Module(body=keep, type_ignores=[]), '<conversation_access>', 'exec'), namespace)
    return types.SimpleNamespace(check=namespace['check_read_access'], state=state, ns=namespace)


def conv(author_id=1, private=True, member_ids=()):
    members = [types.SimpleNamespace(id=i, entity_name='user', entity_meta={'id': m}) for i, m in enumerate(member_ids)]
    return types.SimpleNamespace(author_id=author_id, is_private=private, participants=members)


def test_missing_conversation_is_not_found(access):
    assert access.check(5, None, 2) == access.ns['NOT_FOUND']


def test_public_conversation_is_open_to_the_project(access):
    assert access.check(5, conv(private=False), 2) is None


@pytest.mark.parametrize('conversation', [conv(author_id=2), conv(member_ids=[2])])
def test_author_and_participant_can_read_private_traces(access, conversation):
    assert access.check(5, conversation, 2) is None


def test_outsider_cannot_read_private_traces(access):
    assert access.check(5, conv(member_ids=[3]), 2) == access.ns['NOT_FOUND']


def test_admin_can_read_private_traces(access):
    access.state['admin'] = True
    assert access.check(5, conv(member_ids=[3]), 2) is None


def test_support_project_can_read_private_traces(access):
    access.state['support_project'] = 5
    assert access.check(5, conv(member_ids=[3]), 2) is None


def _openapi_keywords(tree):
    call, = [
        node for node in ast.walk(tree)
        if isinstance(node, ast.Call) and getattr(node.func, 'id', None) == 'register_openapi'
    ]
    return {kw.arg: kw.value for kw in call.keywords}


@pytest.mark.parametrize('path', TRACE_APIS.values(), ids=TRACE_APIS.keys())
def test_trace_endpoint_is_an_mcp_tool(path):
    keywords = _openapi_keywords(ast.parse(path.read_text()))
    assert ast.literal_eval(keywords['mcp_tool']) is True
    assert ast.literal_eval(keywords['mcp_description']).strip()


@pytest.mark.parametrize('rpc_name,path', TRACE_APIS.items(), ids=TRACE_APIS.keys())
def test_trace_endpoint_checks_access_before_reading(rpc_name, path):
    get, = [
        node for node in ast.walk(ast.parse(path.read_text()))
        if isinstance(node, ast.FunctionDef) and node.name == 'get'
    ]
    lines = {}
    for node in ast.walk(get):
        if isinstance(node, ast.Call):
            name = getattr(node.func, 'id', None) or getattr(node.func, 'attr', None)
            lines.setdefault(name, node.lineno)
    denied_return, = [
        node for node in ast.walk(get)
        if isinstance(node, ast.If) and ast.unparse(node.test) == 'denied'
    ]
    assert lines['check_read_access'] < denied_return.lineno < lines[rpc_name]
