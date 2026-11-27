"""Role-based chat sharing (#7757).

- The owner and anyone with access holding manage_shared manage a chat: people, participants,
  rename, privacy, Modules, context settings, export, external links.
- Deleting the chat and filing it in a folder stay with the owner; the owner cannot be removed.
- Every endpoint that changes a chat is wired to the matching check.

    python tests/run_tests.py unit/utils/test_7757_manage_shared.py -v
"""
import ast
import pathlib
import types

import pytest


PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[3]
ACCESS_MODULE = PLUGIN_ROOT / 'utils' / 'conversation_access.py'
API = PLUGIN_ROOT / 'api' / 'v2'
FUNCS = ('check_owner', 'moves_folder', 'is_owner_participant', 'find_user_participant_id', '_is_support_project')


@pytest.fixture
def access():
    tree = ast.parse(ACCESS_MODULE.read_text())
    keep = [
        node for node in tree.body
        if (isinstance(node, ast.FunctionDef) and node.name in FUNCS)
        or (isinstance(node, ast.Assign) and node.targets[0].id.isupper())
    ]
    namespace = {
        'get_support_config': lambda: {'project_id': 99},
        'ParticipantTypes': types.SimpleNamespace(user=types.SimpleNamespace(value='user')),
    }
    exec(compile(ast.Module(body=keep, type_ignores=[]), '<conversation_access>', 'exec'), namespace)
    return types.SimpleNamespace(**namespace)


def member(user_id, pid, name='user'):
    return types.SimpleNamespace(id=pid, entity_name=name, entity_meta={'id': user_id})


def conv(author_id=1, folder_id=None, members=()):
    return types.SimpleNamespace(author_id=author_id, folder_id=folder_id, participants=list(members))


def test_manage_shared_is_the_permission_name(access):
    assert access.MANAGE_SHARED == 'models.chat.conversations.manage_shared'


def test_only_the_owner_passes_the_owner_check(access):
    assert access.check_owner(conv(author_id=2), 2) is None
    assert access.check_owner(conv(author_id=1), 2) == access.NOT_OWNER
    assert access.check_owner(conv(author_id=1), 2, 5) == access.NOT_OWNER


def test_support_project_passes_the_owner_check(access):
    assert access.check_owner(conv(author_id=1), 2, 99) is None


@pytest.mark.parametrize('folder_id,data,moves', [
    (None, {'folder_id': 7}, True),
    (7, {'folder_id': None}, True),
    (7, {'folder_id': 8}, True),
    (7, {'folder_id': 7}, False),
    (7, {'name': 'x'}, False),
])
def test_moves_folder(access, folder_id, data, moves):
    assert access.moves_folder(conv(folder_id=folder_id), data) is moves


def test_owner_participant_is_found_by_the_author_id(access):
    conversation = conv(author_id=1, members=[member(1, 10), member('2', 11), member(1, 12, name='application')])
    assert access.is_owner_participant(conversation, 10) is True
    assert access.is_owner_participant(conversation, 11) is False
    assert access.is_owner_participant(conversation, 12) is False


def _function(path, name):
    found, = [
        node for node in ast.walk(ast.parse(path.read_text()))
        if isinstance(node, ast.FunctionDef) and node.name == name
    ]
    return found


def _calls(node, name):
    return [
        call for call in ast.walk(node)
        if isinstance(call, ast.Call) and (getattr(call.func, 'id', None) or getattr(call.func, 'attr', None)) == name
    ]


def _keyword(call, name):
    found = [kw.value for kw in call.keywords if kw.arg == name]
    return ast.unparse(found[0]) if found else None


def test_adding_any_participant_needs_privilege():
    call, = _calls(_function(API / 'participants.py', 'post'), 'check_conversation_access')
    assert _keyword(call, 'needs_privilege') == 'True'


def test_the_owner_participant_cannot_be_removed():
    delete = _function(API / 'participant.py', 'delete')
    assert _calls(delete, 'is_owner_participant')
    assert 'OWNER_NOT_REMOVABLE' in ast.unparse(delete)


def test_delete_and_folder_moves_are_owner_only():
    assert _calls(_function(API / 'conversation.py', 'delete'), 'check_owner')
    put = _function(API / 'conversation.py', 'put')
    owner_check, = _calls(put, 'check_owner')
    folder_guard, = [
        node for node in ast.walk(put)
        if isinstance(node, ast.If) and 'moves_folder' in ast.unparse(node.test)
    ]
    assert folder_guard.lineno < owner_check.lineno <= folder_guard.end_lineno


@pytest.mark.parametrize('module,method,privileged', [
    ('context_strategy.py', 'put', 'True'),
    ('entity_settings.py', '_put', None),
    ('shared_chat_links.py', 'post', 'True'),
    ('shared_chat_links.py', 'get', None),
])
def test_endpoint_checks_access_before_acting(module, method, privileged):
    # The first check guards the endpoint; the link list then asks whether the caller manages the chat
    call = _calls(_function(API / module, method), 'check_access_by_id')[0]
    assert _keyword(call, 'needs_privilege') == privileged


def test_share_link_create_tells_the_rpc_the_caller_was_checked():
    call, = _calls(_function(API / 'shared_chat_links.py', 'post'), 'chat_create_share_token')
    assert _keyword(call, 'can_manage') == 'True'


def test_permission_is_registered_at_init():
    assert 'register_manage_shared_permission()' in (PLUGIN_ROOT / 'module.py').read_text()


def test_conversation_update_tells_the_rpc_the_caller_was_checked():
    put = _function(API / 'conversation.py', 'put')
    assert "'can_manage': True" in ast.unparse(put)
    rpc = _function(PLUGIN_ROOT / 'rpc' / 'chat_conversation.py', 'update_conversation_rpc')
    assert 'not can_manage' in ast.unparse(rpc)


@pytest.fixture
def members():
    tree = ast.parse(ACCESS_MODULE.read_text())
    keep = [
        node for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name in ('users_outside_project', '_is_support_project')
    ]
    state = {'members': [1, 2], 'calls': 0}

    def ids_in_project(project_id):
        state['calls'] += 1
        return state['members']

    rpc = types.SimpleNamespace(timeout=lambda _t: types.SimpleNamespace(admin_get_users_ids_in_project=ids_in_project))
    namespace = {
        'rpc_tools': types.SimpleNamespace(RpcMixin=lambda: types.SimpleNamespace(rpc=rpc)),
        'get_support_config': lambda: {'project_id': 99},
        'ParticipantTypes': types.SimpleNamespace(user=types.SimpleNamespace(value='user')),
    }
    exec(compile(ast.Module(body=keep, type_ignores=[]), '<conversation_access>', 'exec'), namespace)
    return types.SimpleNamespace(outside=namespace['users_outside_project'], state=state)


def new(name, entity_id):
    return types.SimpleNamespace(
        entity_name=types.SimpleNamespace(value=name), entity_meta=types.SimpleNamespace(id=entity_id),
    )


def test_people_outside_the_project_are_named(members):
    assert members.outside(5, [new('user', 2), new('user', '9'), new('application', 9)], 1) == ['9']


def test_no_people_means_no_lookup(members):
    assert members.outside(5, [new('application', 9)], 1) == []
    assert members.outside(5, [new('user', 1)], 1) == []
    assert members.state['calls'] == 0


def test_support_project_skips_the_member_check(members):
    assert members.outside(99, [new('user', 9)], 1) == []


@pytest.mark.parametrize('module,method', [('participants.py', 'post'), ('conversations.py', 'post')])
def test_new_people_are_checked_against_the_project(module, method):
    assert _calls(_function(API / module, method), 'users_outside_project')


@pytest.fixture
def switch():
    tree = ast.parse(ACCESS_MODULE.read_text())
    keep = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == 'is_external_sharing_enabled']
    descriptor = types.SimpleNamespace(config={})
    namespace = {'this': types.SimpleNamespace(module=types.SimpleNamespace(descriptor=descriptor))}
    exec(compile(ast.Module(body=keep, type_ignores=[]), '<conversation_access>', 'exec'), namespace)
    return types.SimpleNamespace(enabled=namespace['is_external_sharing_enabled'], descriptor=descriptor)


@pytest.mark.parametrize('config,enabled', [
    ({}, True),
    ({'extra_ui_config': {}}, True),
    ({'extra_ui_config': {'chat_sharing': None}}, True),
    ({'extra_ui_config': {'chat_sharing': {'external_enabled': True}}}, True),
    ({'extra_ui_config': {'chat_sharing': {'external_enabled': False}}}, False),
])
def test_external_sharing_switch_defaults_to_on(switch, config, enabled):
    switch.descriptor.config = config
    assert switch.enabled() is enabled


def test_new_external_links_follow_the_switch_after_the_access_check():
    post = _function(API / 'shared_chat_links.py', 'post')
    access, = _calls(post, 'check_access_by_id')
    switch, = _calls(post, 'is_external_sharing_enabled')
    create, = _calls(post, 'chat_create_share_token')
    assert access.lineno < switch.lineno < create.lineno
    # Existing links are not touched: listing and revoking never read the switch
    assert not _calls(_function(API / 'shared_chat_links.py', 'get'), 'is_external_sharing_enabled')


def test_switch_is_in_the_admin_schema_and_on_by_default():
    import json
    schema = json.loads((PLUGIN_ROOT / 'admin_schema.json').read_text())
    field = schema['properties']['chat_sharing_external_enabled']
    assert field['path'] == 'extra_ui_config.chat_sharing.external_enabled'
    assert field['section'] == 'chat_sharing'
    assert field['default'] is True
