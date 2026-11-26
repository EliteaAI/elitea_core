"""#7744 - a fork carrying a chat starter over 768 characters is rejected as a whole.

Runs the real fork handler against an import wizard that, like the real one, saves each application
on its own and skips only the one that fails the real ``import_starters_errors``. A fork that trimmed
the starters, or left the check to the wizard, would create some or all of the applications.
"""
import importlib.util
import pathlib
import sys
import types

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]

PKG = 'forkpkg_7744'


class _Request:
    json = {}


def _load(name, rel_path):
    spec = importlib.util.spec_from_file_location(name, PLUGIN_ROOT / rel_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _install_package():
    for sub in ('', '.api', '.api.v2', '.utils'):
        mod = types.ModuleType(f'{PKG}{sub}')
        mod.__path__ = []
        sys.modules[f'{PKG}{sub}'] = mod

    constants = types.ModuleType(f'{PKG}.utils.constants')
    constants.PROMPT_LIB_MODE = 'prompt_lib'
    sys.modules[f'{PKG}.utils.constants'] = constants

    permissions = types.ModuleType(f'{PKG}.utils.permissions')

    class ProjectPermissionChecker:
        def __init__(self, project_id):
            self.project_id = project_id

        def check_permissions(self, _perms):
            return {}, 200

    permissions.ProjectPermissionChecker = ProjectPermissionChecker
    sys.modules[f'{PKG}.utils.permissions'] = permissions

    folder_access = types.ModuleType(f'{PKG}.utils.folder_access')
    folder_access.fork_payload_access_error = lambda *a, **kw: None
    sys.modules[f'{PKG}.utils.folder_access'] = folder_access

    tools = types.ModuleType('tools')
    tools.api_tools = types.SimpleNamespace(
        APIModeHandler=type('APIModeHandler', (), {}),
        APIBase=type('APIBase', (), {}),
        with_modes=lambda params: params,
        endpoint_metrics=lambda func: func,
    )
    tools.auth = types.SimpleNamespace(
        current_user=lambda: {'id': 99},
        decorators=types.SimpleNamespace(check_api=lambda *a, **kw: (lambda f: f)),
    )
    tools.config = types.SimpleNamespace(ADMINISTRATION_MODE='administration', DEFAULT_MODE='default')
    tools.this = types.SimpleNamespace()
    sys.modules['tools'] = tools

    flask = types.ModuleType('flask')
    flask.request = _Request
    sys.modules['flask'] = flask

    starters = _load(f'{PKG}.utils.conversation_starters_utils', 'utils/conversation_starters_utils.py')
    _load(f'{PKG}.utils.export_import_utils', 'utils/export_import_utils.py')
    fork = _load(f'{PKG}.api.v2.fork', 'api/v2/fork.py')
    return fork, starters


@pytest.fixture
def fork_with_starters():
    touched = ('tools', 'flask')
    saved = {name: sys.modules.get(name) for name in touched}
    try:
        fork, starters = _install_package()
        created = []

        def _import_wizard(entities, project_id, author_id, force_new_skills=False):
            result, errors = {'agents': []}, {'agents': []}
            for entity in entities:
                entity_errors = starters.import_starters_errors(entity['name'], entity['versions'])
                if entity_errors:
                    errors['agents'].extend(entity_errors)
                else:
                    created.append(entity['name'])
                    result['agents'].append(entity)
            return result, errors

        def _application(name, import_uuid, conversation_starters):
            return {
                'id': 234, 'owner_id': 2, 'name': name, 'entity': 'agents', 'import_uuid': import_uuid,
                'versions': [{'id': 351, 'name': 'base', 'author_id': 1,
                              'conversation_starters': conversation_starters}],
            }

        def _fork(agent_starters, sub_agent_starters=None):
            created.clear()
            applications = [_application('Probe', 'u-1', agent_starters)]
            if sub_agent_starters is not None:
                applications.append(_application('Helper', 'u-2', sub_agent_starters))
            handler = fork.PromptLibAPI()
            handler.module = types.SimpleNamespace(import_wizard=_import_wizard)
            fork.request.json = {'main_entity': 'agents', 'applications': applications}
            body, status = handler.post(project_id=6)
            return body, status, list(created)

        yield _fork
    finally:
        for name, original in saved.items():
            if original is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = original
        for name in list(sys.modules):
            if name.startswith(PKG):
                del sys.modules[name]


@pytest.mark.integration
class TestForkStarterLength:
    def test_769_characters_is_rejected_and_nothing_created(self, fork_with_starters):
        body, status, created = fork_with_starters(['ok', 'a' * 769])
        assert status == 400
        assert created == []
        assert body['errors']['agents'] == [{
            'index': 0, 'import_uuid': 'u-1',
            'msg': 'Probe / base: a conversation starter must be 768 characters or less, got 769',
        }]

    def test_a_long_sub_agent_starter_rejects_the_whole_fork(self, fork_with_starters):
        body, status, created = fork_with_starters(['ok'], sub_agent_starters=['b' * 900])
        assert status == 400
        assert created == []
        assert [error['index'] for error in body['errors']['agents']] == [1]
        assert set(body['result']) == set(body['errors']) == {'agents', 'toolkits', 'skills'}

    def test_768_characters_forks(self, fork_with_starters):
        body, status, created = fork_with_starters(['ok', 'a' * 768], sub_agent_starters=['b' * 768])
        assert status == 201
        assert created == ['Probe', 'Helper']
        assert body['result']['agents'][0]['versions'][0]['conversation_starters'] == ['ok', 'a' * 768]
