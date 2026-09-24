"""Issue #6515 (D1) - a scheduled reindex must run when its credential is a shared one.

A shared credential lives in the public project with `shared=True`; the toolkit's own
project never holds it. Manual runs resolve it through configurations.expand_configuration,
which falls back to the public project's shared rows. `resolve_credentials` looked a
non-private title up in the toolkit's project only, so every tick failed the schedule
with "credential '<title>' of type '<type>' no longer exists".

Run via:
    python tests/run_tests.py integration/test_6515_schedule_shared_credential.py -v
"""

import importlib.util
import pathlib
import sys
import types

import pytest


from fixtures.helpers import register_index_pd_module

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]

pytestmark = pytest.mark.unit


class ConfigurationsRpc:
    """Answers lookups the way the configurations plugin does.

    Both project getters are modelled, so reverting the resolver to the project-only
    getter fails on a missing row rather than on a missing stub method.
    """

    def __init__(self, project_rows=(), shared_rows=()):
        self.project_rows = list(project_rows)
        self.shared_rows = list(shared_rows)
        self.calls = []

    def timeout(self, _seconds):
        return self

    def _project_matches(self, project_id, filter_fields):
        return [
            dict(row) for row in self.project_rows
            if row["project_id"] == project_id and _matches(row, filter_fields)
        ]

    def configurations_get_filtered_project(self, project_id, include_shared=False, filter_fields=None):
        self.calls.append(("project", project_id, include_shared, dict(filter_fields)))
        found = self._project_matches(project_id, filter_fields)
        if include_shared:
            found.extend(dict(row) for row in self.shared_rows if _matches(row, filter_fields))
        return found

    def configurations_get_first_filtered_project(self, project_id, filter_fields=None):
        self.calls.append(("first_project", project_id, False, dict(filter_fields)))
        found = self._project_matches(project_id, filter_fields)
        return found[0] if found else None

    def configurations_get_filtered_personal(self, user_id, include_shared=False, filter_fields=None):
        self.calls.append(("personal", user_id, include_shared, dict(filter_fields)))
        return []


def _matches(row, filter_fields):
    return all(row.get(key) == value for key, value in filter_fields.items())


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

    register_index_pd_module(PLUGIN_ROOT)
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
    """Point the module's RpcMixin at a ConfigurationsRpc for the duration of one test."""
    def _install(fake):
        index_scheduling.rpc_tools.RpcMixin.rpc = fake
        return fake
    yield _install
    index_scheduling.rpc_tools.RpcMixin.rpc = None


PUBLIC_PROJECT = 1
TEAM_PROJECT = 14002
CREATOR = 501
TOOLKIT_TYPE = "sharepoint"
CONFIG_KEY = f"{TOOLKIT_TYPE}_configuration"
TITLE = "sharepoint_app-only"

SHARED_ROW = {
    "id": 12, "project_id": PUBLIC_PROJECT, "shared": True, "elitea_title": TITLE,
    "type": TOOLKIT_TYPE, "data": {"client_secret": "public"},
}
TEAM_ROW = {
    "id": 340, "project_id": TEAM_PROJECT, "shared": False, "elitea_title": TITLE,
    "type": TOOLKIT_TYPE, "data": {"client_secret": "team"},
}


def _settings():
    return {CONFIG_KEY: {"elitea_title": "stale-toolkit-default"}}


def _resolve(index_scheduling, settings, title=TITLE):
    return index_scheduling.resolve_credentials(
        project_settings=settings,
        toolkit_type=TOOLKIT_TYPE,
        user_config={"credentials": {"elitea_title": title, "private": False}},
        project_id=TEAM_PROJECT,
        creator_id=CREATOR,
    )


class TestSharedCredentialResolution:
    """The regression: a shared credential was looked up in the team project only."""

    def test_a_shared_only_credential_resolves_from_the_public_project(
        self, index_scheduling, install_rpc
    ):
        install_rpc(ConfigurationsRpc(shared_rows=[SHARED_ROW]))
        settings = _settings()

        ok, issue, retryable = _resolve(index_scheduling, settings)

        assert (ok, issue, retryable) == (True, None, False)
        assert settings[CONFIG_KEY]["id"] == SHARED_ROW["id"]
        assert settings[CONFIG_KEY]["project_id"] == PUBLIC_PROJECT

    def test_a_resolved_shared_credential_is_stamped_non_private(
        self, index_scheduling, install_rpc
    ):
        install_rpc(ConfigurationsRpc(shared_rows=[SHARED_ROW]))
        settings = _settings()

        _resolve(index_scheduling, settings)

        assert settings[CONFIG_KEY]["private"] is False

    def test_the_lookup_is_scoped_to_the_team_project_type_and_title(
        self, index_scheduling, install_rpc
    ):
        rpc = install_rpc(ConfigurationsRpc(shared_rows=[SHARED_ROW]))

        _resolve(index_scheduling, _settings())

        assert rpc.calls == [
            ("project", TEAM_PROJECT, True, {"type": TOOLKIT_TYPE, "elitea_title": TITLE}),
        ]

    def test_a_shared_row_of_another_type_is_not_substituted(self, index_scheduling, install_rpc):
        install_rpc(ConfigurationsRpc(shared_rows=[dict(SHARED_ROW, type="confluence")]))

        ok, issue, retryable = _resolve(index_scheduling, _settings())

        assert (ok, retryable) == (False, False)
        assert "no longer exists" in issue


class TestProjectCredentialPrecedence:
    """A team credential sharing a title with a public one must keep winning."""

    def test_the_project_row_wins_a_title_collision_with_a_shared_row(
        self, index_scheduling, install_rpc
    ):
        install_rpc(ConfigurationsRpc(project_rows=[TEAM_ROW], shared_rows=[SHARED_ROW]))
        settings = _settings()

        ok, issue, retryable = _resolve(index_scheduling, settings)

        assert (ok, issue, retryable) == (True, None, False)
        assert settings[CONFIG_KEY]["id"] == TEAM_ROW["id"]
        assert settings[CONFIG_KEY]["data"] == {"client_secret": "team"}

    def test_another_projects_row_is_not_a_project_match(self, index_scheduling, install_rpc):
        install_rpc(ConfigurationsRpc(
            project_rows=[dict(TEAM_ROW, id=999, project_id=TEAM_PROJECT + 1)],
            shared_rows=[SHARED_ROW],
        ))
        settings = _settings()

        _resolve(index_scheduling, settings)

        assert settings[CONFIG_KEY]["id"] == SHARED_ROW["id"]


class TestMissingCredentialStillFails:
    """No project row and no shared row is still a hard, non-retryable failure."""

    def test_a_title_found_nowhere_fails_with_no_longer_exists(self, index_scheduling, install_rpc):
        install_rpc(ConfigurationsRpc(
            project_rows=[dict(TEAM_ROW, elitea_title="other")],
            shared_rows=[dict(SHARED_ROW, elitea_title="other")],
        ))
        settings = _settings()

        ok, issue, retryable = _resolve(index_scheduling, settings)

        assert (ok, retryable) == (False, False)
        assert issue == f"credential '{TITLE}' of type '{TOOLKIT_TYPE}' no longer exists"
        assert settings[CONFIG_KEY] == {"elitea_title": "stale-toolkit-default"}
