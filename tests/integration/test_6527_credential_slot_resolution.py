"""Issue #6527 - a scheduled reindex must find the credential slot it actually uses.

`resolve_credentials` derived the settings key as `{toolkit_type}_configuration`. That
guess is wrong whenever the toolkit type is narrower than its credential family: an
`ado_wiki` toolkit stores its credential under `ado_configuration` and looks up a
configuration of type `ado`. The miss was silent — the function returned True as if
nothing needed replacing — so the tick indexed with whatever credential the toolkit was
last saved with, and the credential the schedule named was ignored.

Run via:
    python tests/run_tests.py integration/test_6527_credential_slot_resolution.py -v
"""

import importlib.util
import pathlib
import sys
import types

import pytest


PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]


class FakeRpc:
    """Records lookups and answers them from a fixed configuration store."""

    def __init__(self, project_configs=None, personal_configs=None):
        self.project_configs = project_configs or {}
        self.personal_configs = personal_configs or {}
        self.calls = []

    def timeout(self, _seconds):
        return self

    def configurations_get_first_filtered_project(self, project_id, filter_fields):
        self.calls.append(("project", project_id, dict(filter_fields)))
        return self.project_configs.get((project_id, filter_fields["elitea_title"]))

    def configurations_get_filtered_personal(self, user_id, include_shared, filter_fields):
        self.calls.append(("personal", user_id, dict(filter_fields)))
        found = self.personal_configs.get((user_id, filter_fields["elitea_title"]))
        return [found] if found else []


@pytest.fixture(scope="module")
def index_scheduling():
    """Load utils/index_scheduling.py with its relative imports stubbed out."""
    for name in (
        "plugins",
        "plugins.elitea_core",
        "plugins.elitea_core.models",
        "plugins.elitea_core.utils",
    ):
        mod = sys.modules.setdefault(name, types.ModuleType(name))
        mod.__path__ = []

    pylon_tools = types.ModuleType("pylon.core.tools")
    pylon_tools.web = types.SimpleNamespace()
    pylon_tools.log = types.SimpleNamespace(
        info=lambda *a, **k: None, warning=lambda *a, **k: None,
        error=lambda *a, **k: None, debug=lambda *a, **k: None,
        exception=lambda *a, **k: None,
    )
    sys.modules.setdefault("pylon", types.ModuleType("pylon"))
    sys.modules.setdefault("pylon.core", types.ModuleType("pylon.core"))
    sys.modules["pylon.core.tools"] = pylon_tools

    tools_pkg = types.ModuleType("tools")
    tools_pkg.db = types.SimpleNamespace(get_session=lambda pid: None)
    tools_pkg.VaultClient = type("VaultClient", (), {})
    tools_pkg.this = types.SimpleNamespace(module=types.SimpleNamespace())
    tools_pkg.rpc_tools = types.SimpleNamespace(
        RpcMixin=type("RpcMixin", (), {"rpc": None})
    )
    sys.modules["tools"] = tools_pkg

    enums = types.ModuleType("plugins.elitea_core.models.enums")
    enums.InitiatorType = type("InitiatorType", (), {"schedule": "schedule"})
    sys.modules["plugins.elitea_core.models.enums"] = enums

    app_tools = types.ModuleType("plugins.elitea_core.utils.application_tools")
    app_tools.IndexMetaLockTimeoutError = type("IndexMetaLockTimeoutError", (Exception,), {})
    app_tools.update_toolkit_index_meta_history_with_failed_state = lambda *a, **k: None
    sys.modules["plugins.elitea_core.utils.application_tools"] = app_tools

    spec = importlib.util.spec_from_file_location(
        "plugins.elitea_core.utils.index_scheduling",
        PLUGIN_ROOT / "utils" / "index_scheduling.py",
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def install_rpc(index_scheduling):
    """Point the module's RpcMixin at a FakeRpc for the duration of one test."""
    def _install(fake):
        index_scheduling.rpc_tools.RpcMixin.rpc = fake
        return fake
    yield _install
    index_scheduling.rpc_tools.RpcMixin.rpc = None


TEAM_PROJECT = 14002
CREATOR = 501

ADO_CONFIG = {
    "id": 812, "project_id": TEAM_PROJECT, "elitea_title": "ado_wiki_lfa_cloud_credentials",
    "type": "ado", "data": {"token": "shared"},
}


def _ado_wiki_settings():
    """An ado_wiki toolkit as stored in production: the slot is `ado_configuration`."""
    return {
        "ado_configuration": {"elitea_title": "stale-toolkit-default", "private": False},
        "pgvector_configuration": {"elitea_title": "elitea-pgvector", "private": False},
    }


class TestSlotResolution:
    def test_the_slot_is_found_even_when_it_does_not_match_the_toolkit_type(self, index_scheduling):
        assert index_scheduling.resolve_credential_config_key(
            _ado_wiki_settings(), "ado_wiki"
        ) == "ado_configuration"

    def test_an_exact_type_match_still_wins(self, index_scheduling):
        settings = {"github_configuration": {}, "pgvector_configuration": {}}
        assert index_scheduling.resolve_credential_config_key(
            settings, "github"
        ) == "github_configuration"

    def test_pgvector_is_never_mistaken_for_the_credential_slot(self, index_scheduling):
        # Substituting a toolkit credential into pgvector_configuration would point the
        # index at the wrong database and trip the shared-project pgvector guard.
        settings = {"pgvector_configuration": {}, "index_configuration": {}}
        assert index_scheduling.resolve_credential_config_key(settings, "ado_wiki") is None

    def test_an_ambiguous_settings_shape_resolves_to_nothing(self, index_scheduling):
        settings = {"ado_configuration": {}, "jira_configuration": {}}
        assert index_scheduling.resolve_credential_config_key(settings, "ado_wiki") is None


class TestResolutionUsesTheSlotType:
    def test_the_credential_is_looked_up_by_the_slot_type_not_the_toolkit_type(
        self, index_scheduling, install_rpc
    ):
        rpc = install_rpc(FakeRpc(
            project_configs={(TEAM_PROJECT, "ado_wiki_lfa_cloud_credentials"): dict(ADO_CONFIG)}
        ))
        settings = _ado_wiki_settings()

        ok = index_scheduling.resolve_credentials(
            project_settings=settings,
            toolkit_type="ado_wiki",
            user_config={"credentials": {"elitea_title": "ado_wiki_lfa_cloud_credentials"}},
            project_id=TEAM_PROJECT,
            is_team_schedule=True,
            creator_id=CREATOR,
        )

        assert ok is True
        assert settings["ado_configuration"]["id"] == 812
        kind, _, filter_fields = rpc.calls[0]
        assert (kind, filter_fields["type"]) == ("project", "ado")

    def test_pgvector_settings_are_left_alone(self, index_scheduling, install_rpc):
        install_rpc(FakeRpc(
            project_configs={(TEAM_PROJECT, "ado_wiki_lfa_cloud_credentials"): dict(ADO_CONFIG)}
        ))
        settings = _ado_wiki_settings()

        index_scheduling.resolve_credentials(
            project_settings=settings,
            toolkit_type="ado_wiki",
            user_config={"credentials": {"elitea_title": "ado_wiki_lfa_cloud_credentials"}},
            project_id=TEAM_PROJECT,
            is_team_schedule=True,
        )

        assert settings["pgvector_configuration"] == {
            "elitea_title": "elitea-pgvector", "private": False,
        }


class TestUnresolvableSlotFailsLoudly:
    def test_named_credentials_with_no_slot_are_a_failure_not_a_silent_success(
        self, index_scheduling, install_rpc
    ):
        # The old behaviour returned True here, so the tick ran with stale credentials
        # instead of reporting the misconfiguration on the index history.
        rpc = install_rpc(FakeRpc())

        ok = index_scheduling.resolve_credentials(
            project_settings={"pgvector_configuration": {}},
            toolkit_type="ado_wiki",
            user_config={"credentials": {"elitea_title": "ado_wiki_lfa_cloud_credentials"}},
            project_id=TEAM_PROJECT,
            is_team_schedule=True,
        )

        assert ok is False
        assert rpc.calls == []


class TestLegacyTitleKey:
    def test_a_credential_stored_under_alita_title_still_resolves(self, index_scheduling, install_rpc):
        # Same tolerance as configurations.expand_configuration, which reads either key.
        install_rpc(FakeRpc(
            project_configs={(TEAM_PROJECT, "ado_wiki_lfa_cloud_credentials"): dict(ADO_CONFIG)}
        ))
        settings = _ado_wiki_settings()

        ok = index_scheduling.resolve_credentials(
            project_settings=settings,
            toolkit_type="ado_wiki",
            user_config={"credentials": {"alita_title": "ado_wiki_lfa_cloud_credentials"}},
            project_id=TEAM_PROJECT,
            is_team_schedule=True,
        )

        assert ok is True
        assert settings["ado_configuration"]["id"] == 812
