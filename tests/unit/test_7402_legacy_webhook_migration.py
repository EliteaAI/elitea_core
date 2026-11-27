"""#7402: one-way migration of legacy pipeline webhook triggers to webhook rows.

Contract under test: utils/legacy_webhook_migration.py. The row mapping, the secret lookup and the old-key
rule are real; the database, vault and auth RPC are fakes, so each scenario reads as data in, report out.
"""
import sys
import types
from unittest.mock import MagicMock

import pytest
from unit._tool_spend_loader import load

MODULE = "utils/legacy_webhook_migration.py"
GITHUB = {"type": "webhook", "webhook_type": "github", "created_by": 7,
          "webhook_secret": "{{secret.webhook_secret_v11}}"}
SIGNING = {"type": "webhook", "webhook_type": "gitlab", "created_by": 7, "gitlab_auth_method": "signing_token",
           "webhook_secret": "{{secret.webhook_secret_v12}}",
           "webhook_signing_secret": "{{secret.webhook_signing_secret_v12}}"}


def _stub(monkeypatch, name, **attrs):
    module = types.ModuleType(name)
    vars(module).update(attrs)
    monkeypatch.setitem(sys.modules, name, module)


class FakeVault:
    """The project's visible secrets; records what the migration removes."""
    store: dict = {}
    removed: list = []

    def __init__(self, project_id):
        self.project_id = project_id

    def get_secrets(self):
        return dict(FakeVault.store)

    def update_secrets(self, add=None, remove=None):
        FakeVault.removed.extend(remove or [])


@pytest.fixture
def migration(monkeypatch):
    """The module with fake vault and owner check; `migration.members` are the users in the project."""
    _stub(monkeypatch, "tools", VaultClient=FakeVault, this=types.SimpleNamespace(), rpc_tools=None, db=None)
    heavy = types.ModuleType("heavy")
    heavy.__getattr__ = lambda name: MagicMock() if not name.startswith("__") else getattr(types.ModuleType, name)
    for name in ("models.all", "models.participants", "models.pd.llm", "utils.utils", "models.pd.index",
                 "models.pd.pipeline_trigger", "utils.schedule_timing", "utils.llm_migration_utils"):
        monkeypatch.setitem(sys.modules, "plugins.elitea_core." + name, heavy)
    # pipeline_trigger pulls the ORM; the two names used here are mirrored
    _stub(monkeypatch, "plugins.elitea_core.utils.pipeline_trigger",
          WEBHOOK_TYPE_CONFIG={"github": {}, "gitlab": {}, "custom": {}},
          is_gitlab_signing_mode=lambda kind, method: kind == "gitlab" and method == "signing_token")
    _stub(monkeypatch, "plugins.elitea_core.utils.schedule_prerun", ACCESS_CHECK_UNAVAILABLE="access_check_unavailable",
          DISABLED_OWNER_LOST_ACCESS="owner_lost_access", check_owner_access=None)
    # load() writes sys.modules directly; register both names so teardown restores what other suites loaded
    for name in ("utils.legacy_schedule_migration", "utils.legacy_webhook_migration"):
        monkeypatch.setitem(sys.modules, "plugins.elitea_core." + name, None)
    load("utils/legacy_schedule_migration.py", fresh=True)
    module = load(MODULE, fresh=True)
    module.members, module.versions, module.done, module.moved = {7}, [], set(), []
    FakeVault.store, FakeVault.removed = {}, []

    def owner_access(project_id, owner_id):
        return None if owner_id in module.members else "owner_lost_access"
    monkeypatch.setattr(module, "check_owner_access", owner_access)
    monkeypatch.setattr(module, "_legacy_versions", lambda session: [(v, "Release bot") for v in module.versions])
    monkeypatch.setattr(module, "_migrated_ids", lambda session: set(module.done))

    def migrate_version(session, project_id, version, row, value):
        module.moved.append((version.id, row, value))
        module._drop_legacy_trigger(version)
    monkeypatch.setattr(module, "migrate_version", migrate_version)
    monkeypatch.setattr(module, "flag_modified", lambda *a: None)
    return module


def _version(vid, trigger):
    return types.SimpleNamespace(id=vid, application_id=3, name=f"v{vid}",
                                 pipeline_settings={"nodes": [], "trigger": dict(trigger)})


def _run(migration, dry_run=False):
    return migration._migrate_versions(MagicMock(), 5, dry_run)


# --- row mapping -------------------------------------------------------------------------------------------

@pytest.mark.parametrize("trigger, method", [
    (GITHUB, None),
    ({**GITHUB, "webhook_type": "custom"}, None),
    ({**GITHUB, "webhook_type": "gitlab", "gitlab_auth_method": "secret_token"}, "secret_token"),
    ({**GITHUB, "webhook_type": "gitlab"}, "secret_token"),  # older GitLab triggers stored no method
    (SIGNING, "signing_token"),
])
def test_row_keeps_type_method_owner_and_old_key(migration, trigger, method):
    row = migration.migrated_row(3, 11, "Release bot", "base", trigger)
    assert (row["webhook_type"], row["gitlab_auth_method"]) == (trigger["webhook_type"], method)
    assert row["created_by"] == row["updated_by"] == 7
    assert row["principal"] == {"kind": "delegation", "ref": 7}
    assert (row["trigger_kind"], row["entity_type"], row["run_mode"]) == ("webhook", "pipeline", "entry_point")
    assert (row["approval_rule"], row["enabled"], row["legacy_version_id"]) == ("pause_and_ask", True, 11)
    assert row["name"] == "Release bot (base)"
    assert "secret_ref" not in row and "original_trigger" not in row  # the value goes to a new key, no undo copy


def test_long_name_is_cut_to_the_column(migration):
    assert len(migration.migrated_row(3, 11, "p" * 300, "v", GITHUB)["name"]) == 256


@pytest.mark.parametrize("change, reason", [
    ({"created_by": None}, "owner_missing"),
    ({"created_by": "abc"}, "owner_missing"),
    ({"created_by": 0}, "owner_missing"),
    ({"webhook_type": "bitbucket"}, "unknown_webhook_type"),
])
def test_row_refused(migration, change, reason):
    with pytest.raises(migration.LegacyWebhookError) as error:
        migration.migrated_row(3, 11, "p", "v", {**GITHUB, **change})
    assert error.value.reason == reason


def test_secret_value_reads_the_active_key(migration):
    store = {"webhook_secret_v12": "plain-token", "webhook_signing_secret_v12": "whsec_abc"}
    assert migration.secret_value(SIGNING, store) == "whsec_abc"
    assert migration.secret_value({**SIGNING, "gitlab_auth_method": "secret_token"}, store) == "plain-token"
    with pytest.raises(migration.LegacyWebhookError, match="secret_missing"):
        migration.secret_value(GITHUB, store)


def test_trigger_keys_cover_both_gitlab_refs(migration):
    assert migration.trigger_keys(SIGNING) == {"webhook_secret_v12", "webhook_signing_secret_v12"}
    assert migration.trigger_keys({"webhook_secret": "plain text"}) == set()


# --- the project run ---------------------------------------------------------------------------------------

def test_mixed_project(migration):
    # Scenario: the local fixture set in miniature. 11 and 14 share key v11 (a copied version, #6440);
    # 13's creator left and uses key v12, so v12 must stay; 15 points at a key that does not exist.
    FakeVault.store = {"webhook_secret_v11": "s11", "webhook_secret_v12": "s12",
                       "webhook_signing_secret_v12": "whsec_x", "unrelated": "u"}
    migration.versions = [
        _version(11, GITHUB),
        _version(12, SIGNING),
        _version(13, {**GITHUB, "created_by": 99, "webhook_secret": "{{secret.webhook_secret_v12}}"}),
        _version(14, GITHUB),
        _version(15, {**GITHUB, "webhook_secret": "{{secret.webhook_secret_v999}}"}),
    ]
    report = _run(migration)
    assert report["migrated"] == [11, 12, 14]
    assert report["failed"] == [{"version_id": 13, "reason": "owner_lost_access"},
                                {"version_id": 15, "reason": "secret_missing"}]
    assert [(vid, value) for vid, _, value in migration.moved] == [(11, "s11"), (12, "whsec_x"), (14, "s11")]
    # v11 is used by migrated versions only; v12 is still referred to by the failed 13
    assert sorted(FakeVault.removed) == ["webhook_secret_v11", "webhook_signing_secret_v12"]
    assert (report["old_secrets_removed"], report["old_secrets_kept"]) == (2, 1)
    assert [v.pipeline_settings["trigger"] for v in migration.versions] == [
        {"type": "chat_message"}, {"type": "chat_message"}, migration.versions[2].pipeline_settings["trigger"],
        {"type": "chat_message"}, migration.versions[4].pipeline_settings["trigger"]]
    assert migration.versions[2].pipeline_settings["trigger"]["type"] == "webhook"


def test_dry_run_writes_nothing(migration):
    FakeVault.store = {"webhook_secret_v11": "s11"}
    migration.versions = [_version(11, GITHUB)]
    report = _run(migration, dry_run=True)
    assert report["migrated"] == [11] and report["old_secrets_removed"] == 1
    assert migration.moved == [] and FakeVault.removed == []
    assert migration.versions[0].pipeline_settings["trigger"]["type"] == "webhook"


def test_rerun_skips_and_drops_a_stale_trigger(migration):
    # A row exists but the version still has its trigger (e.g. a crash between commits): no second row
    FakeVault.store = {"webhook_secret_v11": "s11"}
    migration.versions, migration.done = [_version(11, GITHUB)], {11}
    report = _run(migration)
    assert (report["migrated"], report["skipped"], migration.moved) == ([], [11], [])
    assert migration.versions[0].pipeline_settings["trigger"] == {"type": "chat_message"}


def test_access_check_outage_is_reported_not_lost(migration, monkeypatch):
    FakeVault.store = {"webhook_secret_v11": "s11"}
    migration.versions = [_version(11, GITHUB)]
    monkeypatch.setattr(migration, "check_owner_access", lambda *a: migration.ACCESS_CHECK_UNAVAILABLE)
    assert _run(migration)["failed"] == [{"version_id": 11, "reason": migration.ACCESS_CHECK_UNAVAILABLE}]
    assert FakeVault.removed == []


def test_one_failing_write_does_not_stop_the_project(migration, monkeypatch):
    FakeVault.store = {"webhook_secret_v11": "s11", "webhook_secret_v14": "s14"}
    migration.versions = [_version(11, GITHUB), _version(14, {**GITHUB, "webhook_secret": "{{secret.webhook_secret_v14}}"})]

    def flaky(session, project_id, version, row, value):
        if version.id == 11:
            raise RuntimeError("vault down")
        migration.moved.append(version.id)
    monkeypatch.setattr(migration, "migrate_version", flaky)
    report = _run(migration)
    assert report["failed"] == [{"version_id": 11, "reason": "RuntimeError"}] and report["migrated"] == [14]
    assert FakeVault.removed == ["webhook_secret_v14"]  # 11 kept its trigger, so its key stays


def test_report_never_holds_refs_or_values(migration):
    FakeVault.store = {"webhook_secret_v11": "s11"}
    migration.versions = [_version(11, GITHUB)]
    text = repr(_run(migration))
    assert "s11" not in text and "{{secret" not in text and "webhook_secret_v" not in text
