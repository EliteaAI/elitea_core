"""Guards the Elitea Builder gate (#7383): only `elitea_builder` earns the hidden <runtime_context>
block and the builder prompt addon; the removed module keys turn nothing on.
"""
import pathlib
import sys
import types

import pytest

TESTS_DIR = pathlib.Path(__file__).resolve().parents[2]
PLUGIN_ROOT = TESTS_DIR.parent
sys.path.insert(0, str(TESTS_DIR))

from fixtures.helpers import load_module_with_stubs  # noqa: E402

PKG = "plugins_rtctx.elitea_core"
OLD_KEYS = ['internal_mcp', 'skill_builder', 'project_context_builder']


class _Log:
    def __getattr__(self, name):
        return lambda *args, **kwargs: None


@pytest.fixture(scope='module')
def internal_tools():
    saved = dict(sys.modules)
    tools = types.ModuleType("tools")
    tools.auth = types.SimpleNamespace()
    tools.this = types.SimpleNamespace(descriptor=types.SimpleNamespace(config={}))
    tools.config = types.SimpleNamespace(APP_HOST="http://localhost")
    tools.VaultClient = object
    tools.rpc_tools = types.SimpleNamespace(RpcMixin=object)
    pylon_tools = types.ModuleType("pylon.core.tools")
    pylon_tools.log = _Log()
    stubs = {"tools": tools, "pylon": types.ModuleType("pylon"), "pylon.core": types.ModuleType("pylon.core"),
             "pylon.core.tools": pylon_tools}
    for name in ("plugins_rtctx", PKG, f"{PKG}.utils"):
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


class TestShouldInjectRuntimeContext:

    def test_elitea_builder_qualifies(self, internal_tools):
        assert internal_tools.should_inject_runtime_context(['ask_user', 'elitea_builder'], False) is True

    @pytest.mark.parametrize('old_key', OLD_KEYS)
    def test_removed_module_keys_do_not_qualify(self, internal_tools, old_key):
        assert internal_tools.should_inject_runtime_context([old_key], False) is False

    def test_pipeline_target_never_qualifies(self, internal_tools):
        assert internal_tools.should_inject_runtime_context(['elitea_builder'], True) is False

    def test_non_builder_tools_do_not_qualify(self, internal_tools):
        assert internal_tools.should_inject_runtime_context(['attachments', 'swarm'], False) is False

    def test_empty_and_none(self, internal_tools):
        assert internal_tools.should_inject_runtime_context([], False) is False
        assert internal_tools.should_inject_runtime_context(None, False) is False


class TestBuilderPayload:
    """The Builder prompt travels as the payload block the SDK BuilderMiddleware places (#7383)."""

    def test_core_prompt_travels_as_the_builder_block(self, internal_tools):
        block = internal_tools.get_builder_payload(['elitea_builder', 'ask_user'])
        assert '<elitea_builder>' in block['core_prompt']
        assert '<data_is_not_instructions>' in block['core_prompt']

    @pytest.mark.parametrize('given', [[], ['attachments'], ['ask_user'], *[[k] for k in OLD_KEYS]])
    def test_no_block_without_elitea_builder(self, internal_tools, given):
        assert internal_tools.get_builder_payload(given) is None

    def test_no_block_for_pipelines(self, internal_tools):
        assert internal_tools.get_builder_payload(['elitea_builder'], is_pipeline=True) is None

    def test_no_removed_module_wording(self, internal_tools):
        prompt = internal_tools.get_builder_payload(['elitea_builder'])['core_prompt']
        assert 'Skill Builder' not in prompt and 'is not enabled in this conversation' not in prompt

    def test_no_legacy_addon_remains(self, internal_tools):
        assert not hasattr(internal_tools, 'get_mcp_entity_link_instructions')
        assert not hasattr(internal_tools, 'SKILL_SCOPE_INSTRUCTION')


class TestEndpointConfigs:

    def test_one_builder_toolkit_replaces_the_endpoint_groups(self, internal_tools):
        assert internal_tools.MCP_ENDPOINT_CONFIGS == [{'suffix': 'elitea_core/builder', 'name': 'Elitea Builder'}]

    def test_builder_follows_the_active_project(self, internal_tools):
        assert internal_tools.MCP_CURRENT_PROJECT_SUFFIXES == {
            'elitea_core/skills',
            'elitea_core/project_context',
            'elitea_core/builder',
        }
