"""#7214: under enforce the internal MCP toolkits of a run carry no personal or system token (the run token is the
bearer, added by the worker), the PAT-status endpoint says GOVERNED, and governance off keeps the PAT path as it was.

``internal_tools`` and the PAT-status endpoint are loaded from disk with their Pylon imports stubbed.
"""
import pathlib
import sys
import types
from datetime import datetime, timedelta

import pytest

TESTS_DIR = pathlib.Path(__file__).resolve().parents[2]
PLUGIN_ROOT = TESTS_DIR.parent
sys.path.insert(0, str(TESTS_DIR))

from fixtures.helpers import load_module_with_stubs  # noqa: E402

PKG = "plugins_7214.elitea_core"


class Auth:
    def __init__(self):
        self.calls = []
        self.tokens = [{"id": 9, "expires": datetime.now() + timedelta(days=1)}]

    def list_tokens(self, user_id):
        self.calls.append(("list_tokens", user_id))
        return self.tokens

    def encode_token(self, token_id):
        self.calls.append(("encode_token", token_id))
        return f"pat-{token_id}"

    def ensure_system_token(self, user_id):
        self.calls.append(("ensure_system_token", user_id))
        return "system-token"

    def current_user(self):
        return {"id": 42}


class _Log:
    def __getattr__(self, name):
        return lambda *args, **kwargs: None


@pytest.fixture
def loaded():
    saved = dict(sys.modules)
    auth = Auth()
    module_state = types.SimpleNamespace(governance_profile=types.SimpleNamespace(enabled=False))
    tools = types.ModuleType("tools")
    tools.auth = auth
    tools.this = types.SimpleNamespace(module=module_state)
    tools.config = types.SimpleNamespace(APP_HOST="http://localhost", ADMINISTRATION_MODE="administration",
                                         DEFAULT_MODE="default")
    tools.VaultClient = object
    project = types.SimpleNamespace(id=5)
    tools.rpc_tools = types.SimpleNamespace(RpcMixin=lambda: types.SimpleNamespace(rpc=types.SimpleNamespace(
        timeout=lambda seconds: types.SimpleNamespace(admin_get_user_private_project=lambda user_id: project))))
    tools.api_tools = types.SimpleNamespace(APIModeHandler=object, APIBase=object,
                                            with_modes=lambda params: params, endpoint_metrics=lambda f: f)
    tools.auth.decorators = types.SimpleNamespace(check_api=lambda spec: (lambda f: f))
    pylon_tools = types.ModuleType("pylon.core.tools")
    pylon_tools.log = _Log()
    stubs = {"tools": tools, "pylon": types.ModuleType("pylon"), "pylon.core": types.ModuleType("pylon.core"),
             "pylon.core.tools": pylon_tools}
    for name in ("plugins_7214", PKG, f"{PKG}.utils", f"{PKG}.api", f"{PKG}.api.v2"):
        package = types.ModuleType(name)
        package.__path__ = []
        stubs[name] = package
    mcp_config = types.ModuleType(f"{PKG}.utils.mcp_config")
    mcp_config.is_mcp_exposure_enabled = lambda: True
    support = types.ModuleType(f"{PKG}.utils.support_utils")
    support.get_support_config = lambda: {}
    constants = types.ModuleType(f"{PKG}.utils.constants")
    constants.PROMPT_LIB_MODE = "prompt_lib"
    stubs.update({mcp_config.__name__: mcp_config, support.__name__: support, constants.__name__: constants})
    for name, obj in stubs.items():
        sys.modules[name] = obj
    load_module_with_stubs(PLUGIN_ROOT / "utils" / "governance.py", f"{PKG}.utils.governance")
    internal_tools = load_module_with_stubs(PLUGIN_ROOT / "utils" / "internal_tools.py", f"{PKG}.utils.internal_tools")
    status_api = load_module_with_stubs(PLUGIN_ROOT / "api" / "v2" / "internal_mcp_pat_status.py",
                                        f"{PKG}.api.v2.internal_mcp_pat_status")
    yield types.SimpleNamespace(it=internal_tools, api=status_api, auth=auth, module=module_state)
    sys.modules.clear()
    sys.modules.update(saved)


def governed(loaded, on=True):
    loaded.module.governance_profile.enabled = on


def test_off_injects_the_personal_token_exactly_as_before(loaded):
    tools = loaded.it.inject_mcp_toolkits(42, current_project_id=1, internal_tools=["internal_mcp"])
    assert tools and all(tool["settings"]["headers"] == {"Authorization": "Bearer pat-9"} for tool in tools)
    assert ("list_tokens", 42) in loaded.auth.calls


def test_enforce_injects_the_toolkits_with_no_token_and_looks_none_up(loaded):
    governed(loaded)
    loaded.auth.tokens = []   # a user without a PAT: no system token is created on their behalf either
    tools = loaded.it.inject_mcp_toolkits(42, current_project_id=1, internal_tools=["internal_mcp"])
    assert len(tools) == len(loaded.it.MCP_ENDPOINT_CONFIGS)
    assert all(tool["settings"]["headers"] == {} for tool in tools)
    # the schedule group follows the active project (1); every other group is served from the private project (5)
    assert all(("/app/1/mcp/elitea_core/schedules" if "schedules" in tool["settings"]["url"] else "/app/5/mcp/")
               in tool["settings"]["url"] for tool in tools)
    assert loaded.auth.calls == []


def test_enforce_resolves_a_manual_internal_toolkit_without_a_personal_token(loaded):
    governed(loaded)
    tool = {"type": "mcp_elitea", "settings": {"url": "http://pylon_main:8080/app/{project_id}/mcp/secrets",
                                               "headers": {"Authorization": "Bearer {personal_token}"}}}
    loaded.it.resolve_internal_mcp_tools([tool], 42, 3)
    assert "personal_token" not in tool["settings"] and tool["settings"]["url"].endswith("/app/3/mcp/secrets")
    assert loaded.auth.calls == []


def test_enforce_keeps_the_personal_token_for_a_toolkit_test_that_is_not_an_admitted_run(loaded):
    # The toolkit Test run and the MCP connection test are dispatched without admission, so no run token
    # exists for them; they keep the user's own token (PAT or the platform's system token) under enforce.
    governed(loaded)
    tool = {"type": "mcp_elitea", "settings": {"url": "http://pylon_main:8080/app/{project_id}/mcp/secrets",
                                               "headers": {"Authorization": "Bearer {personal_token}"}}}
    loaded.it.resolve_internal_mcp_tools([tool], 42, 3, admitted_run=False)
    assert tool["settings"]["personal_token"] == "pat-9"
    assert tool["settings"]["headers"]["Authorization"] == "Bearer pat-9"


def test_off_resolves_a_manual_internal_toolkit_with_the_personal_token(loaded):
    tool = {"type": "mcp_elitea", "settings": {"url": "http://pylon_main:8080/app/{project_id}/mcp/secrets",
                                               "headers": {"Authorization": "Bearer {personal_token}"}}}
    loaded.it.resolve_internal_mcp_tools([tool], 42, 3)
    assert tool["settings"]["personal_token"] == "pat-9"
    assert tool["settings"]["headers"]["Authorization"] == "Bearer pat-9"


def test_the_pat_status_endpoint_says_governed_under_enforce_only(loaded):
    loaded.it.this.module = loaded.module
    loaded.it.is_internal_mcp_toolkit = lambda config: True
    loaded.api.is_internal_mcp_toolkit = lambda config: True
    handler = loaded.api.PromptLibAPI()
    assert handler.get(1, "mcp_elitea") == ({"internal": True, "state": "VALID"}, 200)
    governed(loaded)
    assert handler.get(1, "mcp_elitea") == ({"internal": True, "state": "GOVERNED"}, 200)
