"""Guards which project the builder MCP toolkits are confined to.

Re-broken by trusting a client-sent runtime_context project in an ordinary chat, or by
confining the support assistant to its own project instead of the one the user is viewing.
"""
import pathlib
import sys
import types

import pytest

TESTS_DIR = pathlib.Path(__file__).resolve().parents[2]
PLUGIN_ROOT = TESTS_DIR.parent
sys.path.insert(0, str(TESTS_DIR))

from fixtures.helpers import load_module_with_stubs  # noqa: E402


CONVERSATION_PROJECT = 2
SUPPORT_PROJECT = 99
VIEWED_PROJECT = 7
PKG = "plugins_6404.elitea_core"


class _Log:
    def __getattr__(self, name):
        return lambda *args, **kwargs: None


@pytest.fixture(scope='module')
def internal_tools():
    saved = dict(sys.modules)
    tools = types.ModuleType("tools")
    tools.auth = types.SimpleNamespace()
    tools.this = types.SimpleNamespace()
    tools.config = types.SimpleNamespace(APP_HOST="http://localhost")
    tools.VaultClient = object
    tools.rpc_tools = types.SimpleNamespace(RpcMixin=object)
    pylon_tools = types.ModuleType("pylon.core.tools")
    pylon_tools.log = _Log()
    stubs = {"tools": tools, "pylon": types.ModuleType("pylon"), "pylon.core": types.ModuleType("pylon.core"),
             "pylon.core.tools": pylon_tools}
    for name in ("plugins_6404", PKG, f"{PKG}.utils"):
        package = types.ModuleType(name)
        package.__path__ = []
        stubs[name] = package
    mcp_config = types.ModuleType(f"{PKG}.utils.mcp_config")
    mcp_config.is_mcp_exposure_enabled = lambda: True
    support = types.ModuleType(f"{PKG}.utils.support_utils")
    support.get_support_config = lambda: {'enabled': False, 'project_id': None}
    stubs.update({mcp_config.__name__: mcp_config, support.__name__: support})
    sys.modules.update(stubs)
    for dep in ("governance", "internal_base_url", "platform_buckets", "builder_keys"):
        load_module_with_stubs(PLUGIN_ROOT / "utils" / f"{dep}.py", f"{PKG}.utils.{dep}")
    builder_pkg = types.ModuleType(f"{PKG}.utils.builder")
    builder_pkg.__path__ = []
    sys.modules[builder_pkg.__name__] = builder_pkg
    load_module_with_stubs(PLUGIN_ROOT / "utils" / "builder" / "prompts.py", f"{PKG}.utils.builder.prompts")
    module = load_module_with_stubs(PLUGIN_ROOT / "utils" / "internal_tools.py", f"{PKG}.utils.internal_tools")
    yield module
    sys.modules.clear()
    sys.modules.update(saved)


@pytest.fixture
def support_project(internal_tools, monkeypatch):
    def _configure(project_id):
        monkeypatch.setattr(
            internal_tools, 'get_support_config', lambda: {'enabled': True, 'project_id': project_id}
        )
    return _configure


class TestScopeResolution:

    def test_ordinary_chat_uses_the_conversation_project(self, internal_tools):
        assert internal_tools.resolve_builder_scope_project_id(CONVERSATION_PROJECT, None) == CONVERSATION_PROJECT

    def test_ordinary_chat_ignores_a_client_supplied_project(self, internal_tools, support_project):
        support_project(SUPPORT_PROJECT)
        resolved = internal_tools.resolve_builder_scope_project_id(
            CONVERSATION_PROJECT, {'project_id': VIEWED_PROJECT}
        )
        assert resolved == CONVERSATION_PROJECT

    def test_support_assistant_follows_the_viewed_project(self, internal_tools, support_project):
        support_project(SUPPORT_PROJECT)
        resolved = internal_tools.resolve_builder_scope_project_id(
            SUPPORT_PROJECT, {'project_id': VIEWED_PROJECT}
        )
        assert resolved == VIEWED_PROJECT

    def test_support_assistant_without_a_viewed_project_falls_back(self, internal_tools, support_project):
        support_project(SUPPORT_PROJECT)
        assert internal_tools.resolve_builder_scope_project_id(SUPPORT_PROJECT, {}) == SUPPORT_PROJECT
        assert internal_tools.resolve_builder_scope_project_id(
            SUPPORT_PROJECT, {'project_id': None}
        ) == SUPPORT_PROJECT

    def test_unusable_viewed_project_falls_back(self, internal_tools, support_project):
        support_project(SUPPORT_PROJECT)
        resolved = internal_tools.resolve_builder_scope_project_id(
            SUPPORT_PROJECT, {'project_id': 'not-a-project'}
        )
        assert resolved == SUPPORT_PROJECT

    def test_support_assistant_not_installed(self, internal_tools):
        resolved = internal_tools.resolve_builder_scope_project_id(
            CONVERSATION_PROJECT, {'project_id': VIEWED_PROJECT}
        )
        assert resolved == CONVERSATION_PROJECT


class TestContinueFlowPayloadModel:
    """generate_payload serves BOTH SioPredictModel and SioContinuePredictModel; the latter
    carries no runtime_context, so every read of it must go through getattr. A plain attribute
    access raises AttributeError, which the caller's `except PayloadGenerationError` does not
    catch, killing every Continue (OAuth resume, HITL resume, token-limit continuation)."""

    def test_continue_model_has_no_runtime_context(self):
        source = (TESTS_DIR.parent / 'models' / 'pd' / 'predict.py').read_text()
        continue_model = source.split('class SioContinuePredictModel')[1].split('\nclass ')[0]
        assert 'runtime_context' not in continue_model

    def test_every_payload_read_of_runtime_context_is_guarded(self):
        source = (TESTS_DIR.parent / 'rpc' / 'chat_all.py').read_text()
        assert 'predict_payload.runtime_context' not in source
        assert source.count("getattr(predict_payload, 'runtime_context', None)") == 1
        assert source.count('resolve_turn_runtime_context(msg_group, predict_payload)') == 2


class TestResumedTurnKeepsItsScope:
    """Continue and regenerate rebuild the payload from a model with no runtime_context. Without a
    read-back the resumed turn scopes to the conversation's project, while the persisted block still
    names the project the support assistant was working in — the model is then denied its own scope.
    """

    @staticmethod
    def _msg_group(items):
        return types.SimpleNamespace(message_items=items)

    @staticmethod
    def _context_item(project_id):
        return types.SimpleNamespace(context_data={'user_id': 3, 'project_id': project_id})

    @pytest.fixture
    def resolve_turn(self):
        import ast
        source = (TESTS_DIR.parent / 'rpc' / 'chat_all.py').read_text()
        func = next(
            node for node in ast.parse(source).body
            if isinstance(node, ast.FunctionDef) and node.name == 'resolve_turn_runtime_context'
        )
        namespace = {'ConversationMessageGroup': object}
        exec(compile(ast.Module(body=[func], type_ignores=[]), '<resolve>', 'exec'), namespace)
        return namespace['resolve_turn_runtime_context']

    def test_a_live_payload_wins_over_the_persisted_block(self, resolve_turn):
        payload = types.SimpleNamespace(runtime_context={'project_id': VIEWED_PROJECT})
        group = self._msg_group([self._context_item(999)])
        assert resolve_turn(group, payload)['project_id'] == VIEWED_PROJECT

    def test_a_continue_payload_falls_back_to_the_persisted_block(self, resolve_turn):
        payload = types.SimpleNamespace(project_id=SUPPORT_PROJECT)  # no runtime_context at all
        group = self._msg_group([self._context_item(VIEWED_PROJECT)])
        assert resolve_turn(group, payload)['project_id'] == VIEWED_PROJECT

    def test_non_context_items_are_skipped(self, resolve_turn):
        payload = types.SimpleNamespace(project_id=SUPPORT_PROJECT)
        group = self._msg_group([
            types.SimpleNamespace(content='hello'),
            self._context_item(VIEWED_PROJECT),
        ])
        assert resolve_turn(group, payload)['project_id'] == VIEWED_PROJECT

    def test_no_block_and_no_payload_yields_nothing(self, resolve_turn):
        payload = types.SimpleNamespace(project_id=CONVERSATION_PROJECT)
        assert resolve_turn(self._msg_group([]), payload) is None
        assert resolve_turn(self._msg_group(None), payload) is None

    def test_resolver_accepts_a_missing_runtime_context(self, internal_tools):
        assert internal_tools.resolve_builder_scope_project_id(
            CONVERSATION_PROJECT, None
        ) == CONVERSATION_PROJECT


class TestSubAgentPathIsScoped:
    """A sub-agent whose own version meta enables builder tools gets its toolkits from
    utils/subagent_prefetch.py (PATCH version and the predict-time prefetch, #6913), not from
    generate_toolkit_payload. Unscoped there, delegating to such a
    sub-agent is a way out of a clamped conversation."""

    def test_every_injection_site_passes_a_scope(self):
        import ast
        plugin_root = TESTS_DIR.parent
        calls = []
        for path in ((plugin_root / 'utils' / 'subagent_prefetch.py'),
                     (plugin_root / 'rpc' / 'chat_all.py')):
            for node in ast.walk(ast.parse(path.read_text())):
                if not isinstance(node, ast.Call):
                    continue
                if getattr(node.func, 'id', None) != 'inject_mcp_toolkits':
                    continue
                keywords = {kw.arg for kw in node.keywords}
                assert 'scope_project_id' in keywords, f'{path.name} injects without a scope'
                calls.append(path.name)
        assert len(calls) == 2, f'expected both injection sites, found {calls}'


class TestScopedSuffixes:

    def test_the_three_builder_entity_groups_are_scoped(self, internal_tools):
        assert internal_tools.MCP_PROJECT_SCOPED_SUFFIXES == {
            'elitea_core/skills',
            'elitea_core/project_context',
            'elitea_core/applications',
            'elitea_core/builder',
        }


@pytest.fixture
def injected_urls(internal_tools, monkeypatch):
    monkeypatch.setattr(internal_tools, '_get_user_token', lambda user_id: 'pat-token')
    monkeypatch.setattr(internal_tools, '_get_internal_base_url', lambda: 'http://localhost')
    monkeypatch.setattr(
        internal_tools.rpc_tools, 'RpcMixin',
        lambda: types.SimpleNamespace(
            rpc=types.SimpleNamespace(
                timeout=lambda _s: types.SimpleNamespace(
                    admin_get_user_private_project=lambda user_id: types.SimpleNamespace(id=1)
                )
            )
        ),
        raising=False,
    )

    def _inject(internal_tool_keys, scope_project_id):
        tools = internal_tools.inject_mcp_toolkits(
            user_id=10,
            current_project_id=CONVERSATION_PROJECT,
            internal_tools=internal_tool_keys,
            scope_project_id=scope_project_id,
        )
        return {
            internal_tools._extract_internal_mcp_suffix(t['settings']['url']): t['settings']['url']
            for t in tools
        }
    return _inject


class TestInjectedToolkitUrls:

    def test_the_builder_toolkit_carries_the_scope(self, injected_urls):
        urls = injected_urls(['elitea_builder'], CONVERSATION_PROJECT)
        assert set(urls) == {'elitea_core/builder'}
        assert urls['elitea_core/builder'].endswith(f'?scope_project_id={CONVERSATION_PROJECT}')

    def test_no_scope_leaves_the_url_unconfined(self, injected_urls):
        urls = injected_urls(['elitea_builder'], None)
        assert all('scope_project_id' not in url for url in urls.values())

    def test_a_scoped_url_still_resolves_to_its_bare_suffix(self, internal_tools, injected_urls):
        urls = injected_urls(['elitea_builder'], CONVERSATION_PROJECT)
        assert internal_tools._extract_internal_mcp_suffix(
            urls['elitea_core/builder']
        ) == 'elitea_core/builder'

    def test_the_builder_is_served_from_the_conversation_project(self, injected_urls):
        urls = injected_urls(['elitea_builder'], CONVERSATION_PROJECT)
        assert urls['elitea_core/builder'].startswith(
            f'http://localhost/app/{CONVERSATION_PROJECT}/mcp/'
        )


class TestPromptNoLongerInvitesCrossProject:

    def test_the_model_is_told_not_to_leave_the_project(self, internal_tools):
        prompt = internal_tools.get_builder_payload(['elitea_builder'])['core_prompt']
        assert 'Use a different project' not in prompt
        assert 'Work only in the current project' in prompt
