"""#6544 - forking a toolkit must not carry its index schedules into the destination project.

Runs the real ``fork_toolkit`` handler and inspects what it hands to the import wizard. Before
the fix ``meta`` was deep-copied wholesale and only ``icon_meta`` was cleared, so
``indexes_meta`` - cron, the credential title, and the ``created_by`` of whoever armed the
schedule - arrived in the destination project. The index data cannot follow (it lives in the
source toolkit's own vector schema), so the schedule then failed on every scheduler pass and
notified its author about a project they need not be a member of.
"""
import importlib.util
import pathlib
import sys
import types

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]

PKG = 'forkpkg_6544'


class _Request:
    json = {}


def _install_package(forwarded):
    pkg = types.ModuleType(PKG)
    pkg.__path__ = []
    for sub in ('api', 'api.v2', 'models', 'models.pd', 'utils'):
        mod = types.ModuleType(f'{PKG}.{sub}')
        mod.__path__ = []
        sys.modules[f'{PKG}.{sub}'] = mod
    sys.modules[PKG] = pkg

    fork_models = types.ModuleType(f'{PKG}.models.pd.fork')

    class ForkToolInput:
        def __init__(self, toolkits):
            self.toolkits = toolkits

        @classmethod
        def parse_obj(cls, obj):
            return cls(list(obj['toolkits']))

    fork_models.ForkToolInput = ForkToolInput
    sys.modules[f'{PKG}.models.pd.fork'] = fork_models

    constants = types.ModuleType(f'{PKG}.utils.constants')
    constants.PROMPT_LIB_MODE = 'prompt_lib'
    sys.modules[f'{PKG}.utils.constants'] = constants

    permissions = types.ModuleType(f'{PKG}.utils.permissions')

    class ProjectPermissionChecker:
        def __init__(self, owner_id):
            self.owner_id = owner_id

        def check_permissions(self, _perms):
            return {}, 200

    permissions.ProjectPermissionChecker = ProjectPermissionChecker
    sys.modules[f'{PKG}.utils.permissions'] = permissions

    folder_access = types.ModuleType(f'{PKG}.utils.folder_access')
    folder_access.fork_payload_access_error = lambda *a, **kw: None
    sys.modules[f'{PKG}.utils.folder_access'] = folder_access

    # The code under test: the real helper, not a stand-in.
    spec = importlib.util.spec_from_file_location(
        f'{PKG}.utils.toolkit_meta', PLUGIN_ROOT / 'utils' / 'toolkit_meta.py'
    )
    toolkit_meta = importlib.util.module_from_spec(spec)
    sys.modules[f'{PKG}.utils.toolkit_meta'] = toolkit_meta
    spec.loader.exec_module(toolkit_meta)

    # Self-contained: sibling suites swap `tools` out from under each other, so build the
    # one this module needs rather than mutating whatever happens to be installed.
    tools = types.ModuleType('tools')
    tools.api_tools = types.SimpleNamespace(
        APIModeHandler=type('APIModeHandler', (), {}),
        APIBase=type('APIBase', (), {}),
        with_modes=lambda params: params,
        endpoint_metrics=lambda func: func,
    )
    tools.rpc_tools = types.SimpleNamespace()
    tools.db = types.SimpleNamespace()
    tools.auth = types.SimpleNamespace(
        current_user=lambda: {'id': 99},
        decorators=types.SimpleNamespace(check_api=lambda *a, **kw: (lambda f: f)),
    )
    tools.config = types.SimpleNamespace(
        ADMINISTRATION_MODE='administration', DEFAULT_MODE='default',
    )
    sys.modules['tools'] = tools

    flask = types.ModuleType('flask')
    flask.request = _Request
    flask.send_file = lambda *a, **kw: None
    sys.modules['flask'] = flask

    pydantic_v1 = types.ModuleType('pydantic.v1')

    class ValidationError(Exception):
        pass

    pydantic_v1.ValidationError = ValidationError
    sys.modules['pydantic.v1'] = pydantic_v1

    spec = importlib.util.spec_from_file_location(
        f'{PKG}.api.v2.fork_toolkit', PLUGIN_ROOT / 'api' / 'v2' / 'fork_toolkit.py'
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def forked():
    """Return a callable that forks one toolkit and yields what reached the import wizard."""
    touched = ('tools', 'flask', 'pydantic.v1')
    saved = {name: sys.modules.get(name) for name in touched}
    forwarded = []
    try:
        module = _install_package(forwarded)
        yield _make_fork(module, forwarded)
    finally:
        for name, original in saved.items():
            if original is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = original
        for name in list(sys.modules):
            if name.startswith(PKG):
                del sys.modules[name]


def _make_fork(module, forwarded):

    def _fork(meta):
        forwarded.clear()

        def _import_wizard(toolkits, project_id, author_id):
            forwarded.extend(toolkits)
            return {'toolkits': list(toolkits)}, {'toolkits': []}

        handler = module.PromptLibAPI()
        handler.module = types.SimpleNamespace(
            find_existing_toolkit_fork=lambda **kw: None,
            get_toolkit_by_id=lambda *a, **kw: {},
            context=types.SimpleNamespace(
                rpc_manager=types.SimpleNamespace(
                    call=types.SimpleNamespace(prompt_lib_import_wizard=_import_wizard)
                )
            ),
        )
        module.request.json = {
            'toolkits': [{
                'id': 872,
                'name': 'ado-toolkit',
                'owner_id': 406,
                'author_id': 12,
                'import_uuid': 'uuid-1',
                'meta': meta,
            }]
        }
        module.auth = types.SimpleNamespace(
            current_user=lambda: {'id': 99},
            decorators=types.SimpleNamespace(check_api=lambda *a, **kw: (lambda f: f)),
        )
        handler.post(project_id=591)
        assert len(forwarded) == 1
        return forwarded[0]

    return _fork


class TestForkDropsIndexSchedules:
    def _schedules_meta(self, **extra):
        meta = {
            'indexes_meta': {
                'test2': {'schedules': {'12': {
                    'cron': '0 * * * *',
                    'enabled': True,
                    'created_by': 12,
                    'credentials': {'elitea_title': 'adomarian', 'private': False},
                }}}
            }
        }
        meta.update(extra)
        return meta

    def test_a_fork_carries_no_index_schedules(self, forked):
        assert 'indexes_meta' not in forked(self._schedules_meta())['meta']

    def test_a_refork_of_an_already_forked_toolkit_carries_none_either(self, forked):
        """The parentage branch is skipped when `parent_entity_id` is already set, which is
        exactly where a rebound `meta` could have been dropped on the floor."""
        meta = self._schedules_meta(parent_entity_id=800, parent_project_id=406)
        result = forked(meta)['meta']
        assert 'indexes_meta' not in result
        assert result['parent_entity_id'] == 800

    def test_the_rest_of_meta_survives(self, forked):
        result = forked(self._schedules_meta(icon_meta={'name': 'x'}))['meta']
        assert result['parent_entity_id'] == 872
        assert result['parent_project_id'] == 406
        assert result['parent_author_id'] == 12
        assert result['icon_meta'] == {}

    def test_a_toolkit_with_no_meta_still_forks(self, forked):
        assert forked({})['meta']['parent_entity_id'] == 872
