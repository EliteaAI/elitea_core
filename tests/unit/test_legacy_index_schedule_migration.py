"""The row of one legacy index schedule and the project report of the index migration.

Contract under test: utils/legacy_index_schedule_migration.py. The schedule body model, the cron check and the
timing helper are real; the plugin shell, the owner checks, the database reads and the write are stubbed.
"""
import sys
import types
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from unit._tool_spend_loader import load

MODULE = "utils/legacy_index_schedule_migration.py"
NOW = datetime(2026, 11, 9, 8, 30, tzinfo=UTC)
BODY = {"cron": "0 3 * * *", "enabled": True, "created_by": 7, "timezone": "UTC",
        "last_run": "2026-11-08T03:00:00+00:00", "credentials": None}


def _stub(monkeypatch, name, **attrs):
    module = types.ModuleType(name)
    vars(module).update(attrs)
    monkeypatch.setitem(sys.modules, name, module)


def toolkit(toolkit_id=3, indexes=None, author_id=9, settings=None):
    return SimpleNamespace(
        id=toolkit_id, name="Docs", type="github", author_id=author_id, settings=settings or {},
        meta={"indexes_meta": indexes or {}},
    )


@pytest.fixture
def migration(monkeypatch):
    """The module with the owner checks answering from `migration.access` and `migration.rights` (None = fine)."""
    descriptor = SimpleNamespace(config={})
    _stub(monkeypatch, "tools", this=SimpleNamespace(module=SimpleNamespace(descriptor=descriptor)),
          VaultClient=None, rpc_tools=None, context=None)
    _stub(monkeypatch, "plugins.elitea_core.utils.pipeline_trigger",
          get_trigger_from_pipeline_settings=None, build_trigger_for_storage=None)
    heavy = types.ModuleType("heavy")
    heavy.__getattr__ = lambda name: MagicMock() if not name.startswith("__") else getattr(types.ModuleType, name)
    for name in ("models.all", "models.participants", "models.pd.llm", "utils.utils", "models.agent_identity"):
        monkeypatch.setitem(sys.modules, "plugins.elitea_core." + name, heavy)

    def resolve_credential_config_key(project_settings, toolkit_type):
        return "github_configuration" if "github_configuration" in project_settings else None

    _stub(monkeypatch, "plugins.elitea_core.utils.index_scheduling",
          resolve_credential_config_key=resolve_credential_config_key)
    load("utils/constants.py")
    load("models/pd/index.py")
    load("models/pd/pipeline_trigger.py")
    load("utils/schedule_timing.py")
    load("utils/orphaned_index_schedules.py")
    load("utils/schedule_prerun.py")
    load("utils/trigger_principal.py")
    load("utils/legacy_schedule_migration.py")
    module = load(MODULE, fresh=True)
    module.access, module.rights, module.written = None, None, []
    monkeypatch.setattr(module, "check_owner_access", lambda project_id, owner: module.access)
    monkeypatch.setattr(module, "check_owner_rights", lambda project_id, owner, kind, toolkit_id: module.rights)
    monkeypatch.setattr(module, "_enabled_count", lambda session: 0)
    monkeypatch.setattr(module, "schedule_limits", lambda: SimpleNamespace(max_enabled_per_project=50))
    return module


def row(migration, key="7", **body):
    return migration.index_row(5, toolkit(), "docs", key, {**BODY, **body}, NOW)


def report(migration, monkeypatch, toolkits, names=None, enforce_limits=False, dry_run=True, lookup=None):
    monkeypatch.setattr(migration, "_legacy_toolkits", lambda session: toolkits)

    def write(session, toolkit_id, new_row, index_name, keys):
        migration.written.append((toolkit_id, index_name, keys))
        return True

    monkeypatch.setattr(migration, "_write_index", write)
    lookup = lookup or (lambda project_id, found: names)
    return migration._migrate_indexes(None, 5, dry_run, enforce_limits, lookup=lookup)  # noqa: SLF001


def test_an_active_personal_schedule_becomes_an_enabled_row(migration):
    made = row(migration)
    assert (made["enabled"], made["disabled_reason"], made["created_by"]) == (True, None, 7)
    assert made["name"] == "Docs / docs"
    assert made["principal"] == {"kind": "delegation", "ref": 7}
    assert made["last_run"] == datetime(2026, 11, 8, 3, 0, tzinfo=UTC)
    assert made["next_run"] > NOW


def test_a_disabled_schedule_is_paused_without_a_reason(migration):
    made = row(migration, enabled=False)
    assert (made["enabled"], made["disabled_reason"], made["next_run"]) == (False, None, None)


def test_a_personal_schedule_saved_by_another_user_goes_to_the_toolkit_author(migration):
    made = row(migration, key="8")
    assert (made["created_by"], made["disabled_reason"], made["enabled"]) == (9, "owner_lost_access", False)


def test_an_owner_without_access_pauses_the_row(migration):
    migration.rights = "owner lost access"
    assert row(migration)["disabled_reason"] == "owner_lost_access"


def test_an_owner_check_that_cannot_answer_refuses_the_row(migration):
    migration.access = "access check unavailable"
    with pytest.raises(migration.LegacyTriggerError):
        row(migration)


@pytest.mark.parametrize("body", [{"cron": "not a cron"}, {"timezone": "Mars/Base"}, {"cron": "*/5 * * * *"}])
def test_a_bad_or_too_fast_cron_is_paused_as_invalid(migration, body):
    made = row(migration, **body)
    assert (made["enabled"], made["disabled_reason"]) == (False, "invalid_cron")


def test_a_cron_below_the_minimum_is_reported(migration):
    assert row(migration, cron="*/5 * * * *")["fast"] is True
    assert row(migration)["fast"] is False


def test_the_stop_date_carries_over_and_an_expired_schedule_is_paused(migration):
    assert row(migration, expires_at="2027-01-01T00:00:00+00:00")["expires_at"] == datetime(2027, 1, 1, tzinfo=UTC)
    assert row(migration)["expires_at"] is None
    past = row(migration, expires_at="2026-10-01T00:00:00+00:00")
    assert (past["enabled"], past["disabled_reason"]) == (False, "expired")
    assert row(migration, expired=True)["disabled_reason"] == "expired"


def test_a_personal_credential_other_than_the_toolkit_one_needs_review(migration):
    other = {"credentials": {"elitea_title": "mine", "private": True}}
    made = migration.index_row(5, toolkit(settings={"github_configuration": {"elitea_title": "team"}}), "docs",
                               "7", {**BODY, **other}, NOW)
    assert (made["enabled"], made["disabled_reason"]) == (False, "credential_review")
    same = {"credentials": {"elitea_title": "team", "private": False}}
    kept = migration.index_row(5, toolkit(settings={"github_configuration": {"elitea_title": "team"}}), "docs",
                               "7", {**BODY, **same}, NOW)
    assert kept["enabled"] is True


def test_several_schedules_of_one_index_keep_the_enabled_then_the_team_then_the_latest(migration):
    pick = migration.choose_schedule
    off_team = {**BODY, "enabled": False}
    assert pick({"-1": off_team, "7": BODY})[0] == "7"
    assert pick({"7": BODY, "-1": BODY})[0] == "-1"
    older = {**BODY, "last_run": "2026-01-01T00:00:00+00:00"}
    key, _, merged = pick({"7": older, "8": BODY})
    assert (key, merged) == ("8", ["7"])


def test_the_report_counts_migrated_paused_merged_and_orphaned(migration, monkeypatch):
    indexes = {
        "docs": {"schedules": {"7": BODY, "-1": {**BODY, "enabled": False}}},
        "slow": {"schedules": {"7": {**BODY, "enabled": False}}},
        "gone": {"schedules": {"7": BODY}},
        "plain": {},
    }
    result = report(migration, monkeypatch, [toolkit(indexes=indexes)], names={"docs", "slow", "plain"})
    assert result["found"] == 3
    assert result["migrated_enabled"] == 1
    assert [item["index"] for item in result["migrated_paused"]] == ["slow"]
    assert result["merged"] == [{"toolkit_id": 3, "index": "docs", "kept": "7", "dropped": ["-1"]}]
    assert result["orphaned"] == [{"toolkit_id": 3, "index": "gone"}]
    assert migration.written == []  # a dry run writes nothing


def test_a_run_writes_each_index_with_all_its_moved_keys(migration, monkeypatch):
    indexes = {"docs": {"schedules": {"7": BODY, "-1": BODY}}}
    report(migration, monkeypatch, [toolkit(indexes=indexes)], names={"docs"}, dry_run=False)
    assert migration.written == [(3, "docs", ["-1", "7"])]


def test_a_failed_lookup_holds_every_index_of_the_toolkit(migration, monkeypatch):
    def broken(project_id, found):
        raise TimeoutError("vault")

    indexes = {"a": {"schedules": {"7": BODY}}, "b": {"schedules": {"7": BODY}}}
    result = report(migration, monkeypatch, [toolkit(indexes=indexes)], lookup=broken, dry_run=False)
    assert [(item["index"], item["reason"]) for item in result["held"]] == [
        ("a", "index_lookup_failed"), ("b", "index_lookup_failed")]
    assert migration.written == []


def test_an_index_that_already_has_a_row_is_held(migration, monkeypatch):
    monkeypatch.setattr(migration, "_write_index", lambda *args: False)
    monkeypatch.setattr(migration, "_legacy_toolkits",
                        lambda session: [toolkit(indexes={"docs": {"schedules": {"7": BODY}}})])
    result = migration._migrate_indexes(None, 5, False, lookup=lambda project_id, found: {"docs"})  # noqa: SLF001
    assert result["held"] == [{"toolkit_id": 3, "index": "docs", "reason": "index_already_scheduled"}]
    assert result["migrated_enabled"] == 0


def test_the_project_cap_holds_enabled_rows_only_with_enforce_limits(migration, monkeypatch):
    monkeypatch.setattr(migration, "schedule_limits", lambda: SimpleNamespace(max_enabled_per_project=1))
    indexes = {"a": {"schedules": {"7": BODY}}, "b": {"schedules": {"7": BODY}}}
    held = report(migration, monkeypatch, [toolkit(indexes=indexes)], names={"a", "b"}, enforce_limits=True)
    assert (held["migrated_enabled"], [item["index"] for item in held["held"]]) == (1, ["b"])
    free = report(migration, monkeypatch, [toolkit(indexes=indexes)], names={"a", "b"})
    assert (free["migrated_enabled"], free["held"], free["above_project_cap"]) == (2, [], True)


def test_a_restore_is_refused_while_the_legacy_index_step_is_on(migration):
    assert migration.index_restore_refusal({"legacy_index_step_enabled": True})
    assert migration.index_restore_refusal({}) is None
    assert migration.index_restore_refusal(None) is None


def test_the_legacy_toolkit_query_loads_full_rows_only_for_toolkits_with_a_schedule(migration, monkeypatch):
    import sqlalchemy as sa
    from sqlalchemy import orm

    class Base(orm.DeclarativeBase):
        pass

    class EliteATool(Base):
        __tablename__ = "elitea_tools"
        id = sa.Column(sa.Integer, primary_key=True)
        meta = sa.Column(sa.JSON)

    monkeypatch.setitem(sys.modules, "plugins.elitea_core.models.all", SimpleNamespace(EliteATool=EliteATool))
    engine = sa.create_engine("sqlite://")
    Base.metadata.create_all(engine)
    scheduled = {"a": {"schedules": {"7": BODY}}}
    with orm.Session(engine) as session:
        session.add_all([
            EliteATool(id=1, meta=None),
            EliteATool(id=2, meta={"indexes_meta": {"a": {"schedules": {}}}}),
            EliteATool(id=3, meta={"indexes_meta": scheduled}),
            EliteATool(id=4, meta={"other": 1}),
            EliteATool(id=5, meta={"indexes_meta": {"b": {"schedules": {"8": BODY}}}}),
        ])
        session.commit()
        statements = []
        sa.event.listen(engine, "before_cursor_execute",
                        lambda conn, cursor, statement, *rest: statements.append(statement))
        assert [found.id for found in migration._legacy_toolkits(session)] == [3, 5]
    assert len(statements) == 2
    assert "elitea_tools.id, elitea_tools.meta" not in statements[0]  # the first read takes the JSON element only
    assert "elitea_tools.id, elitea_tools.meta" in statements[1]
