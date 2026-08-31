"""Issue #5872 - a scheduled reindex must run when its credential is private.

A schedule stores the credential the author picked as `{elitea_title, private}`.
`resolve_credentials` only ever looked that title up in the team project, but a
private credential lives in the author's personal project, so the lookup came back
empty and every tick failed the schedule with "toolkit credentials resolving issue".
Project-level credentials were unaffected, which is why one ADO-wiki toolkit ran and
its private-credential twin never did.

Second, narrower trap: ConfigurationDetails has no `private` field, so even a
successful personal lookup produced a substituted payload that read as project-level
and sent the downstream configurations_expand back to the team project.

Run via:
    python tests/run_tests.py integration/test_5872_private_schedule_credentials.py -v
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
TOOLKIT_TYPE = "ado_wiki"
CONFIG_KEY = f"{TOOLKIT_TYPE}_configuration"

PRIVATE_CONFIG = {
    "id": 77, "project_id": 9001, "elitea_title": "LFAADOWikiPrivateCreds",
    "type": TOOLKIT_TYPE, "data": {"token": "secret"},
}
PROJECT_CONFIG = {
    "id": 267, "project_id": TEAM_PROJECT, "elitea_title": "Ado_wiki",
    "type": TOOLKIT_TYPE, "data": {"token": "shared"},
}


def _settings():
    return {CONFIG_KEY: {"elitea_title": "stale-toolkit-default"}}


class TestPrivateCredentialResolution:
    """The regression: a private credential resolved against the wrong project."""

    def test_a_private_credential_resolves_from_the_creators_personal_project(
        self, index_scheduling, install_rpc
    ):
        rpc = install_rpc(FakeRpc(personal_configs={(CREATOR, "LFAADOWikiPrivateCreds"): dict(PRIVATE_CONFIG)}))
        settings = _settings()

        ok = index_scheduling.resolve_credentials(
            project_settings=settings,
            toolkit_type=TOOLKIT_TYPE,
            user_config={"credentials": {"elitea_title": "LFAADOWikiPrivateCreds", "private": True}},
            project_id=TEAM_PROJECT,
            creator_id=CREATOR,
        )

        assert ok is True
        assert settings[CONFIG_KEY]["id"] == 77
        assert [kind for kind, *_ in rpc.calls] == ["personal"]

    def test_the_private_lookup_is_keyed_on_the_creator_not_the_team_project(
        self, index_scheduling, install_rpc
    ):
        rpc = install_rpc(FakeRpc(personal_configs={(CREATOR, "LFAADOWikiPrivateCreds"): dict(PRIVATE_CONFIG)}))

        index_scheduling.resolve_credentials(
            project_settings=_settings(),
            toolkit_type=TOOLKIT_TYPE,
            user_config={"credentials": {"elitea_title": "LFAADOWikiPrivateCreds", "private": True}},
            project_id=TEAM_PROJECT,
            creator_id=CREATOR,
        )

        kind, user_id, filter_fields = rpc.calls[0]
        assert (kind, user_id) == ("personal", CREATOR)
        assert filter_fields == {"type": TOOLKIT_TYPE, "elitea_title": "LFAADOWikiPrivateCreds"}

    def test_the_substituted_payload_keeps_the_private_flag(self, index_scheduling, install_rpc):
        # Without this, configurations_expand re-resolves the title against the team
        # project and raises LookupError — the bug moves downstream instead of going away.
        install_rpc(FakeRpc(personal_configs={(CREATOR, "LFAADOWikiPrivateCreds"): dict(PRIVATE_CONFIG)}))
        settings = _settings()

        index_scheduling.resolve_credentials(
            project_settings=settings,
            toolkit_type=TOOLKIT_TYPE,
            user_config={"credentials": {"elitea_title": "LFAADOWikiPrivateCreds", "private": True}},
            project_id=TEAM_PROJECT,
            creator_id=CREATOR,
        )

        assert settings[CONFIG_KEY]["private"] is True

    def test_a_private_credential_no_longer_falls_back_to_the_team_project(
        self, index_scheduling, install_rpc
    ):
        # A same-titled project-level config must not be silently substituted for the
        # private one the author chose — that would index with the wrong account.
        rpc = install_rpc(FakeRpc(
            project_configs={(TEAM_PROJECT, "LFAADOWikiPrivateCreds"): dict(PROJECT_CONFIG)},
            personal_configs={},
        ))

        ok = index_scheduling.resolve_credentials(
            project_settings=_settings(),
            toolkit_type=TOOLKIT_TYPE,
            user_config={"credentials": {"elitea_title": "LFAADOWikiPrivateCreds", "private": True}},
            project_id=TEAM_PROJECT,
            creator_id=CREATOR,
        )

        assert ok is False
        assert [kind for kind, *_ in rpc.calls] == ["personal"]

    def test_a_private_credential_without_a_creator_fails_loudly(self, index_scheduling, install_rpc):
        rpc = install_rpc(FakeRpc())

        ok = index_scheduling.resolve_credentials(
            project_settings=_settings(),
            toolkit_type=TOOLKIT_TYPE,
            user_config={"credentials": {"elitea_title": "LFAADOWikiPrivateCreds", "private": True}},
            project_id=TEAM_PROJECT,
            creator_id=None,
        )

        assert ok is False
        assert rpc.calls == []


class TestProjectCredentialResolutionUnchanged:
    """The working case in the report — project-level creds must not regress."""

    def test_a_project_level_credential_still_resolves_from_the_project(
        self, index_scheduling, install_rpc
    ):
        rpc = install_rpc(FakeRpc(project_configs={(TEAM_PROJECT, "Ado_wiki"): dict(PROJECT_CONFIG)}))
        settings = _settings()

        ok = index_scheduling.resolve_credentials(
            project_settings=settings,
            toolkit_type=TOOLKIT_TYPE,
            user_config={"credentials": {"elitea_title": "Ado_wiki", "private": False}},
            project_id=TEAM_PROJECT,
        )

        assert ok is True
        assert settings[CONFIG_KEY]["id"] == 267
        assert settings[CONFIG_KEY]["private"] is False
        assert [kind for kind, *_ in rpc.calls] == ["project"]

    def test_an_omitted_private_flag_reads_as_project_level(self, index_scheduling, install_rpc):
        rpc = install_rpc(FakeRpc(project_configs={(TEAM_PROJECT, "Ado_wiki"): dict(PROJECT_CONFIG)}))

        ok = index_scheduling.resolve_credentials(
            project_settings=_settings(),
            toolkit_type=TOOLKIT_TYPE,
            user_config={"credentials": {"elitea_title": "Ado_wiki"}},
            project_id=TEAM_PROJECT,
        )

        assert ok is True
        assert [kind for kind, *_ in rpc.calls] == ["project"]

    def test_a_missing_project_credential_still_fails(self, index_scheduling, install_rpc):
        install_rpc(FakeRpc())

        assert index_scheduling.resolve_credentials(
            project_settings=_settings(),
            toolkit_type=TOOLKIT_TYPE,
            user_config={"credentials": {"elitea_title": "Ado_wiki"}},
            project_id=TEAM_PROJECT,
        ) is False


class TestTeamSchedulePassthroughUnchanged:
    """Guards the #254 fix: a team schedule with no credential override is not a failure."""

    def test_a_team_schedule_with_no_credentials_passes_through(self, index_scheduling, install_rpc):
        rpc = install_rpc(FakeRpc())
        settings = _settings()

        ok = index_scheduling.resolve_credentials(
            project_settings=settings,
            toolkit_type=TOOLKIT_TYPE,
            user_config={},
            project_id=TEAM_PROJECT,
            is_team_schedule=True,
        )

        assert ok is True
        assert settings[CONFIG_KEY] == {"elitea_title": "stale-toolkit-default"}
        assert rpc.calls == []

    def test_a_personal_schedule_with_no_credentials_still_fails(self, index_scheduling, install_rpc):
        install_rpc(FakeRpc())

        assert index_scheduling.resolve_credentials(
            project_settings=_settings(),
            toolkit_type=TOOLKIT_TYPE,
            user_config={},
            project_id=TEAM_PROJECT,
            is_team_schedule=False,
        ) is False

    def test_a_toolkit_without_the_config_key_needs_no_resolution(self, index_scheduling, install_rpc):
        rpc = install_rpc(FakeRpc())

        assert index_scheduling.resolve_credentials(
            project_settings={},
            toolkit_type=TOOLKIT_TYPE,
            user_config={"credentials": {"elitea_title": "Ado_wiki", "private": True}},
            project_id=TEAM_PROJECT,
        ) is True
        assert rpc.calls == []
