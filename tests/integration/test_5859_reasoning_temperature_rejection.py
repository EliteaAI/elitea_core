"""Issue #5859 - reject invalid llm_settings for a reasoning-capable model at write time.

Covers invalid_llm_settings_for_reasoning_model (utils/participant_utils.py), the write-time guard
behind the entity_settings PUT/PATCH endpoint. A reasoning model must run with a non-null
reasoning_effort and no temperature; a stale temperature OR a null effort runs it thinking-off,
which triggers the write-tool fabrication of #5826.

Run via:
    python tests/run_tests.py integration/test_5859_reasoning_temperature_rejection.py -v
"""

import importlib.util
import pathlib
import sys
import types

import pytest


PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]


@pytest.fixture
def participant_utils_module():
    """Load participant_utils with a controllable configurations RPC stub."""
    for name in (
        "plugins",
        "plugins.elitea_core",
        "plugins.elitea_core.models",
        "plugins.elitea_core.models.pd",
        "plugins.elitea_core.utils",
    ):
        mod = sys.modules.setdefault(name, types.ModuleType(name))
        mod.__path__ = []

    pylon = types.ModuleType("pylon")
    core = types.ModuleType("pylon.core")
    tools_mod = types.ModuleType("pylon.core.tools")
    tools_mod.log = types.SimpleNamespace(
        info=lambda *a, **k: None, warning=lambda *a, **k: None,
        error=lambda *a, **k: None, debug=lambda *a, **k: None,
        exception=lambda *a, **k: None,
    )
    sys.modules.setdefault("pylon", pylon)
    sys.modules.setdefault("pylon.core", core)
    sys.modules.setdefault("pylon.core.tools", tools_mod)

    # Captured last (model_project_id, model_name) so tests can assert the resolved project.
    calls = []

    class _Rpc:
        def __init__(self, configs):
            self._configs = configs

        def timeout(self, _seconds):
            return self

        def configurations_get_configuration_model(self, model_project_id, model_name):
            calls.append((model_project_id, model_name))
            cfg = self._configs.get(model_name)
            if cfg is None:
                raise RuntimeError(f"model {model_name!r} not found")
            return cfg

    # Configs keyed by model_name; overridden per-test via module attribute below.
    rpc_state = {"configs": {}}

    class _RpcMixin:
        def __init__(self):
            self.rpc = _Rpc(rpc_state["configs"])

    tools_pkg = types.ModuleType("tools")
    tools_pkg.db = types.SimpleNamespace(get_session=lambda pid: None)
    tools_pkg.rpc_tools = types.SimpleNamespace(
        RpcMixin=_RpcMixin,
        EventManagerMixin=lambda: types.SimpleNamespace(event_manager=None),
    )
    tools_pkg.context = types.SimpleNamespace()
    tools_pkg.auth = types.SimpleNamespace(decorators=types.SimpleNamespace())
    tools_pkg.serialize = lambda x: x
    tools_pkg.VaultClient = type("VaultClient", (), {})
    tools_pkg.this = types.SimpleNamespace()
    sys.modules["tools"] = tools_pkg

    # Sibling elitea_core modules imported at participant_utils top — stubbed to empty symbols.
    def _stub(mod_name, **attrs):
        m = types.ModuleType(mod_name)
        for k, v in attrs.items():
            setattr(m, k, v)
        sys.modules[mod_name] = m

    _stub("plugins.elitea_core.utils.sio_utils", get_chat_room=lambda *a, **k: None, SioEvents=type("SioEvents", (), {}))
    _stub("plugins.elitea_core.models.conversation", Conversation=type("Conversation", (), {}))

    class _PT:
        llm = "llm"; user = "user"; dummy = "dummy"; toolkit = "toolkit"; application = "application"

    _stub(
        "plugins.elitea_core.models.enums.all",
        ParticipantTypes=_PT,
        ChatHistoryTemplates=types.SimpleNamespace(all=types.SimpleNamespace(value="all")),
        NotificationEventTypes=type("NotificationEventTypes", (), {}),
    )
    _stub("plugins.elitea_core.models.message_group", ConversationMessageGroup=type("CMG", (), {}))
    _stub(
        "plugins.elitea_core.models.participants",
        Participant=type("Participant", (), {}),
        ParticipantMapping=type("ParticipantMapping", (), {}),
    )
    _stub(
        "plugins.elitea_core.models.pd.participant",
        ParticipantBase=type("ParticipantBase", (), {}),
        ParticipantCreate=type("ParticipantCreate", (), {}),
        EntityMetaType=object,
        ParticipantEntityDummy=type("ParticipantEntityDummy", (), {}),
        ParticipantEntityApplication=type("ParticipantEntityApplication", (), {}),
        entity_meta_mapping={},
        ParticipantEntityUser=type("ParticipantEntityUser", (), {}),
        ParticipantEntityToolkit=type("ParticipantEntityToolkit", (), {}),
    )
    _stub(
        "plugins.elitea_core.models.pd.participant_settings",
        EntitySettingsApplication=type("EntitySettingsApplication", (), {}),
        EntitySettingsLlm=type("EntitySettingsLlm", (), {}),
        EntitySettingsUser=type("EntitySettingsUser", (), {}),
    )
    _stub("plugins.elitea_core.utils.authors", get_authors_data=lambda *a, **k: [])

    spec = importlib.util.spec_from_file_location(
        "plugins.elitea_core.utils.participant_utils",
        PLUGIN_ROOT / "utils" / "participant_utils.py",
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)

    module._test_configs = rpc_state["configs"]
    module._test_calls = calls
    return module


REASONING = {"supports_reasoning": True}
NON_REASONING = {"supports_reasoning": False}


class TestInvalidLlmSettingsForReasoningModel:
    def test_rejects_temperature_on_reasoning_model(self, participant_utils_module):
        m = participant_utils_module
        m._test_configs["claude-sonnet-4-5"] = REASONING
        settings = {"temperature": 0.6, "model_name": "claude-sonnet-4-5", "model_project_id": 1}
        assert m.invalid_llm_settings_for_reasoning_model(1, settings) is True

    def test_rejects_null_effort_on_reasoning_model(self, participant_utils_module):
        # The core bug: no temperature but reasoning_effort null -> thinking-off on a reasoning model.
        m = participant_utils_module
        m._test_configs["claude-sonnet-4-5"] = REASONING
        settings = {
            "temperature": None, "reasoning_effort": None,
            "model_name": "claude-sonnet-4-5", "model_project_id": 1,
        }
        assert m.invalid_llm_settings_for_reasoning_model(1, settings) is True

    def test_rejects_none_string_effort_on_reasoning_model(self, participant_utils_module):
        m = participant_utils_module
        m._test_configs["claude-sonnet-4-5"] = REASONING
        settings = {"reasoning_effort": "none", "model_name": "claude-sonnet-4-5", "model_project_id": 1}
        assert m.invalid_llm_settings_for_reasoning_model(1, settings) is True

    def test_allows_active_effort_no_temperature_on_reasoning_model(self, participant_utils_module):
        m = participant_utils_module
        m._test_configs["claude-sonnet-4-5"] = REASONING
        settings = {"reasoning_effort": "low", "model_name": "claude-sonnet-4-5", "model_project_id": 1}
        # Already-valid shape -> no RPC, no rejection.
        assert m.invalid_llm_settings_for_reasoning_model(1, settings) is False
        assert m._test_calls == []

    def test_allows_null_effort_on_non_reasoning_model(self, participant_utils_module):
        # Non-reasoning model: null/null is fine (effort is meaningless there).
        m = participant_utils_module
        m._test_configs["gpt-5.2"] = NON_REASONING
        settings = {
            "temperature": None, "reasoning_effort": None,
            "model_name": "gpt-5.2", "model_project_id": 1,
        }
        assert m.invalid_llm_settings_for_reasoning_model(1, settings) is False

    def test_allows_temperature_on_non_reasoning_model(self, participant_utils_module):
        m = participant_utils_module
        m._test_configs["gpt-5.2"] = NON_REASONING
        settings = {"temperature": 0.6, "model_name": "gpt-5.2", "model_project_id": 1}
        assert m.invalid_llm_settings_for_reasoning_model(1, settings) is False

    def test_no_rpc_for_already_valid_reasoning_shape(self, participant_utils_module):
        m = participant_utils_module
        settings = {"reasoning_effort": "medium", "model_name": "claude-sonnet-4-5", "model_project_id": 1}
        assert m.invalid_llm_settings_for_reasoning_model(1, settings) is False
        assert m._test_calls == []

    def test_fails_open_when_model_unresolved(self, participant_utils_module):
        m = participant_utils_module
        # Model not in configs -> RPC raises -> must not block the write.
        settings = {"temperature": 0.6, "model_name": "ghost-model", "model_project_id": 1}
        assert m.invalid_llm_settings_for_reasoning_model(1, settings) is False

    def test_falls_back_to_project_id_when_model_project_id_missing(self, participant_utils_module):
        m = participant_utils_module
        m._test_configs["claude-sonnet-4-5"] = REASONING
        settings = {"temperature": 0.6, "model_name": "claude-sonnet-4-5"}
        assert m.invalid_llm_settings_for_reasoning_model(42, settings) is True
        assert m._test_calls[-1] == (42, "claude-sonnet-4-5")

    def test_empty_settings_no_rejection(self, participant_utils_module):
        m = participant_utils_module
        assert m.invalid_llm_settings_for_reasoning_model(1, {}) is False
        assert m.invalid_llm_settings_for_reasoning_model(1, None) is False
