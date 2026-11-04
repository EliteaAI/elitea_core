"""EL-7011 US-24 R1.3, R2.1, R2.2, R2.3, R2.6, R3.1, R3.2: task params and the row of one legacy trigger.

Contract under test: utils/legacy_schedule_migration.py. The trigger model, the cron check and the
timing helper are real; only the plugin shell around them and the auth RPC are stubbed.
"""
import contextlib
import sys
import types
from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest
from unit._tool_spend_loader import load

MODULE = "utils/legacy_schedule_migration.py"
NOW = datetime(2026, 10, 20, 8, 30, tzinfo=UTC)
TRIGGER = {"type": "schedule", "cron": "0 9 * * *", "timezone": "Asia/Tokyo",
           "last_run": "2026-10-19T00:00:00", "created_by": 7}


def _stub(monkeypatch, name, **attrs):
    module = types.ModuleType(name)
    vars(module).update(attrs)
    monkeypatch.setitem(sys.modules, name, module)


@pytest.fixture
def migration(monkeypatch):
    """The module with `check_owner_access` answering from `migration.verdict` (None = active)."""
    descriptor = types.SimpleNamespace(config={})
    _stub(monkeypatch, "tools", this=types.SimpleNamespace(module=types.SimpleNamespace(descriptor=descriptor)),
          VaultClient=None, rpc_tools=None, context=None)
    _stub(monkeypatch, "plugins.elitea_core.utils.pipeline_trigger",
          get_trigger_from_pipeline_settings=None, build_trigger_for_storage=None)
    heavy = types.ModuleType("heavy")
    heavy.__getattr__ = lambda name: MagicMock() if not name.startswith("__") else getattr(types.ModuleType, name)
    for name in ("models.all", "models.participants", "models.pd.llm", "utils.utils"):
        monkeypatch.setitem(sys.modules, "plugins.elitea_core." + name, heavy)
    load("models/pd/index.py")
    load("models/pd/pipeline_trigger.py")
    load("utils/schedule_timing.py")
    # Another suite may leave a stub under this name; fresh load, restored by monkeypatch at teardown
    monkeypatch.setitem(sys.modules, "plugins.elitea_core.utils.llm_migration_utils", None)
    load("utils/llm_migration_utils.py", fresh=True)
    prerun = load("utils/schedule_prerun.py")
    module = load(MODULE, fresh=True)
    module.verdict, module.calls = None, []

    def check(project_id, owner_id):
        module.calls.append((project_id, owner_id))
        return module.verdict

    monkeypatch.setattr(module, "check_owner_access", check)
    module.prerun = prerun
    return module


def _row(migration, **trigger):
    return migration.migrated_row(5, 11, 21, "Nightly report", "v1", {**TRIGGER, **trigger}, NOW)


def test_el7011_r13_dry_run_is_the_default(migration):
    assert migration.parse_task_params("project_id=all") == (("all", None), True)
    assert migration.parse_task_params("project_id=5;dry_run=false") == (("single", 5), False)
    assert migration.parse_task_params(" project_id = 2-4 ; dry_run = true ") == (("range", (2, 4)), True)


@pytest.mark.parametrize("raw", ["", "dry_run=false", "project_id=", "project_id=abc"])
def test_el7011_r13_bad_params_name_the_format(migration, raw):
    with pytest.raises(ValueError, match=r"Format: project_id=<all\|N>\[;dry_run=false\]"):
        migration.parse_task_params(raw)


def test_el7011_r13_bad_dry_run_is_named(migration):
    with pytest.raises(ValueError, match="dry_run"):
        migration.parse_task_params("project_id=5;dry_run=maybe")


def test_el7011_r21_r22_active_row_copies_the_trigger(migration):
    row = _row(migration)
    assert row == {
        "entity_type": "pipeline", "application_id": 11, "version_id": 21, "name": "Nightly report (v1)",
        "description": None, "cron": "0 9 * * *", "timezone": "Asia/Tokyo", "run_mode": "entry_point",
        "prompt": None, "variables": [], "enabled": True, "disabled_reason": None,
        "created_by": 7, "principal": {"kind": "delegation", "ref": 7},
        "last_run": datetime(2026, 10, 19, tzinfo=UTC), "next_run": datetime(2026, 10, 21, 0, 0, tzinfo=UTC),
        "original_trigger": {"type": "schedule", "cron": "0 9 * * *", "timezone": "Asia/Tokyo",
                             "last_run": "2026-10-19T00:00:00", "created_by": 7},
    }
    assert migration.calls == [(5, 7)]


def test_el7011_r23_next_run_is_after_the_migration_time(migration):
    assert _row(migration, timezone="UTC")["next_run"] == datetime(2026, 10, 20, 9, 0, tzinfo=UTC)


def test_el7011_r21_name_fits_the_column(migration):
    row = migration.migrated_row(5, 11, 21, "p" * 300, "v1", TRIGGER, NOW)
    assert len(row["name"]) == 256


def test_el7011_r26_no_row_runs_as_the_system_user(migration):
    migration.verdict = migration.prerun.OWNER_LOST_ACCESS
    kinds = {_row(migration)["principal"]["kind"], _row(migration, cron="bad")["principal"]["kind"]}
    assert kinds == {"delegation"}


def test_el7011_r31_lost_owner_gives_a_disabled_row(migration):
    migration.verdict = migration.prerun.OWNER_LOST_ACCESS
    row = _row(migration)
    assert (row["enabled"], row["disabled_reason"], row["next_run"]) == (False, "owner_lost_access", None)
    assert row["cron"] == "0 9 * * *"


UNUSABLE_CREATORS = [{"created_by": None}, {"created_by": 0}, {"created_by": "x"}, {"created_by": ...}]


def _trigger(**changes):
    """TRIGGER with `changes`; a value of `...` drops the key."""
    merged = {**TRIGGER, **changes}
    return {key: value for key, value in merged.items() if value is not ...}


@pytest.mark.parametrize("bad", UNUSABLE_CREATORS)
def test_el7011_r31_unusable_creator_gives_a_disabled_row_owned_by_the_author(migration, bad):
    row = migration.migrated_row(5, 11, 21, "p", "v", _trigger(**bad), NOW, fallback_owner=3)
    assert (row["enabled"], row["disabled_reason"], row["next_run"]) == (False, "owner_lost_access", None)
    assert (row["created_by"], row["principal"]) == (3, {"kind": "delegation", "ref": 3})
    assert migration.calls == []


# TC-US24-05 / R3.1, R3.2: a bad cron without a creator keeps invalid_cron and the author holds the row
def test_el7011_r31_r32_bad_cron_without_a_creator_keeps_invalid_cron(migration):
    row = migration.migrated_row(5, 11, 21, "p", "v", _trigger(cron="not a cron", created_by=...), NOW,
                                 fallback_owner=3)
    assert (row["enabled"], row["disabled_reason"], row["created_by"]) == (False, "invalid_cron", 3)


@pytest.mark.parametrize("bad", UNUSABLE_CREATORS)
def test_el7011_r31_unusable_creator_without_an_author_cannot_make_a_row(migration, bad):
    trigger = _trigger(**bad)
    with pytest.raises(migration.LegacyTriggerError) as error:
        migration.migrated_row(5, 11, 21, "p", "v", trigger, NOW)
    assert error.value.reason == "owner_lost_access"


def test_el7011_r31_unavailable_access_check_keeps_the_legacy_trigger(migration):
    migration.verdict = migration.prerun.ACCESS_CHECK_UNAVAILABLE
    with pytest.raises(migration.LegacyTriggerError) as error:
        _row(migration)
    assert error.value.reason == migration.prerun.ACCESS_CHECK_UNAVAILABLE


@pytest.mark.parametrize("bad", [{"cron": "not a cron"}, {"timezone": "Mars/Base"}, {"cron": ""}])
def test_el7011_r32_bad_cron_or_zone_gives_a_disabled_row_with_stored_values(migration, bad):
    row = _row(migration, **bad)
    assert (row["enabled"], row["disabled_reason"], row["next_run"]) == (False, "invalid_cron", None)
    assert (row["cron"], row["timezone"]) == (bad.get("cron", "0 9 * * *"), bad.get("timezone", "Asia/Tokyo"))
    assert migration.calls == []


def test_el7011_r32_missing_cron_is_stored_empty(migration):
    trigger = {k: v for k, v in TRIGGER.items() if k != "cron"}
    row = migration.migrated_row(5, 11, 21, "p", "v", trigger, NOW)
    assert (row["cron"], row["disabled_reason"]) == ("", "invalid_cron")


def test_el7011_r32_unreadable_last_run_is_dropped(migration):
    assert _row(migration, last_run="yesterday")["last_run"] is None


def test_el7011_r33_fast_cron_still_moves_and_is_reported_below_minimum(migration):
    row = _row(migration, cron="* * * * *")
    assert row["enabled"] is True
    assert migration.below_minimum("* * * * *") is True
    assert migration.below_minimum("*/5 * * * *") is False


# ── The project loop and its lock ────────────────────────────────────────


class FakeSession:
    """Counts no enabled schedule; `migrate_version` is replaced, so nothing else is read."""

    def scalar(self, _statement):
        return 0


@pytest.fixture
def project_loop(migration, monkeypatch):
    """The module with versions from `project_loop.versions` and `migrate_version` recording moves."""
    from sqlalchemy import column, table

    schedules = table("entity_schedules", column("enabled"))
    schedules.enabled = schedules.c.enabled
    _stub(monkeypatch, "plugins.elitea_core.models.entity_schedule", EntitySchedule=schedules)
    migration.versions, migration.moved = [], []
    monkeypatch.setattr(migration, "_legacy_versions", lambda _session: migration.versions)
    monkeypatch.setattr(migration, "migrate_version", lambda _s, version, _row: migration.moved.append(version.id))
    return migration


def _version(version_id, trigger):
    settings = {"trigger": trigger} if trigger is not None else {}
    return (types.SimpleNamespace(id=version_id, application_id=11, name="v1", author_id=3,
                                  pipeline_settings=settings), "Nightly")


def test_el7011_r31_missing_creator_is_counted_as_migrated_disabled(project_loop):
    project_loop.versions = [_version(1, _trigger(created_by=...))]
    report = project_loop._migrate_versions(FakeSession(), 5, dry_run=False)
    assert report["migrated_disabled"] == [{"version_id": 1, "created_by": 3, "reason": "owner_lost_access"}]
    assert (report["errors"], project_loop.moved) == ([], [1])


def test_el7011_r21_version_without_a_schedule_trigger_is_skipped(project_loop):
    project_loop.versions = [_version(1, None), _version(2, {"type": "chat_message"}), _version(3, TRIGGER)]
    report = project_loop._migrate_versions(FakeSession(), 5, dry_run=False)
    assert (report["found"], report["errors"], project_loop.moved) == (1, [], [3])


def test_el7011_r33_failed_floor_check_keeps_the_legacy_trigger(project_loop, monkeypatch):
    def broken(_cron):
        raise RuntimeError("no floor")

    monkeypatch.setattr(project_loop, "below_minimum", broken)
    project_loop.versions = [_version(1, TRIGGER)]
    report = project_loop._migrate_versions(FakeSession(), 5, dry_run=False)
    assert (report["errors"], project_loop.moved) == ([{"version_id": 1, "error": "no floor"}], [])


def test_enforced_limits_hold_a_fast_cron_and_keep_the_legacy_trigger(project_loop):
    project_loop.versions = [_version(1, _trigger(cron="* * * * *")), _version(2, TRIGGER)]
    report = project_loop._migrate_versions(FakeSession(), 5, dry_run=False, enforce_limits=True)
    assert (report["held"], project_loop.moved) == ([1], [2])


def test_enforced_limits_hold_the_row_that_passes_the_project_cap(project_loop, monkeypatch):
    monkeypatch.setattr(project_loop, "schedule_limits", lambda: types.SimpleNamespace(
        min_interval_minutes=1, max_enabled_per_project=1))
    project_loop.versions = [_version(1, TRIGGER), _version(2, TRIGGER)]
    report = project_loop._migrate_versions(FakeSession(), 5, dry_run=False, enforce_limits=True)
    assert (report["held"], project_loop.moved) == ([2], [1])


def test_the_operator_run_moves_a_fast_cron_without_enforced_limits(project_loop):
    project_loop.versions = [_version(1, _trigger(cron="* * * * *"))]
    report = project_loop._migrate_versions(FakeSession(), 5, dry_run=False)
    assert (report["held"], project_loop.moved) == ([], [1])


class FakeLockConnection:
    """Records each statement; the lock or the unlock raises when asked to."""

    def __init__(self, fail_on=None):
        self.statements, self.fail_on, self.invalidated = [], fail_on, False

    def execution_options(self, **_options):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False

    def execute(self, statement, _params=None):
        sql = str(statement)
        self.statements.append(sql)
        if self.fail_on and self.fail_on in sql:
            raise RuntimeError("canceling statement due to lock timeout")

    def invalidate(self):
        self.invalidated = True


@pytest.fixture
def locked(migration, monkeypatch):
    """(module, connection factory) with `tools.db` and the key helper stubbed; the project report is fixed."""
    sessions = []

    @contextlib.contextmanager
    def get_session(project_id):
        sessions.append(project_id)
        yield FakeSession()

    def use(connection):
        engine = types.SimpleNamespace(connect=lambda: connection)
        monkeypatch.setattr(sys.modules["tools"], "db", types.SimpleNamespace(engine=engine, get_session=get_session),
                            raising=False)
        return sessions

    _stub(monkeypatch, "plugins.elitea_core.utils.schedule_claim", legacy_lock_key=lambda project_id: project_id)
    monkeypatch.setattr(migration, "_migrate_versions", lambda _s, project_id, _dry, _enforce: {"project_id": project_id})
    return migration, use


def test_el7011_r42_project_lock_wait_is_bounded(locked):
    module, use = locked
    connection = FakeLockConnection(fail_on="pg_advisory_lock(")
    sessions = use(connection)
    with pytest.raises(RuntimeError, match="lock timeout"):
        module.migrate_project(5, dry_run=False)
    timeout = next(i for i, sql in enumerate(connection.statements) if "lock_timeout" in sql)
    lock = next(i for i, sql in enumerate(connection.statements) if "pg_advisory_lock(" in sql)
    assert timeout < lock
    assert sessions == []


def test_el7011_r42_failed_unlock_still_returns_the_report(locked):
    module, use = locked
    connection = FakeLockConnection(fail_on="pg_advisory_unlock")
    use(connection)
    assert module.migrate_project(5, dry_run=False) == {"project_id": 5}
    assert connection.invalidated is True


class _RestoreSession:
    def __init__(self, schedule, version):
        self.items, self.deleted, self.commits = {("S", 1): schedule, ("V", 21): version}, [], 0

    def get(self, model, key):
        return self.items.get(("S" if model.__name__ == "EntitySchedule" else "V", key))

    def delete(self, row):
        self.deleted.append(row)

    def commit(self):
        self.commits += 1

    def rollback(self):
        pass


@pytest.fixture
def restore(migration, monkeypatch):
    class EntitySchedule:
        pass

    class ApplicationVersion:
        pass

    _stub(monkeypatch, "plugins.elitea_core.models.entity_schedule", EntitySchedule=EntitySchedule)
    _stub(monkeypatch, "plugins.elitea_core.models.all", ApplicationVersion=ApplicationVersion)
    return migration.restore_version_trigger, EntitySchedule, ApplicationVersion


def test_restore_writes_the_original_trigger_back_and_deletes_the_row(restore):
    function, schedule_class, version_class = restore
    schedule, version = schedule_class(), version_class()
    schedule.version_id, schedule.original_trigger = 21, TRIGGER
    version.pipeline_settings = {"trigger": {"type": "chat_message"}, "entry_point": "start"}
    session = _RestoreSession(schedule, version)
    function(session, 1)
    assert version.pipeline_settings == {"trigger": TRIGGER, "entry_point": "start"}
    assert session.deleted == [schedule] and session.commits == 1


def test_restore_refuses_a_row_whose_version_is_gone_and_keeps_it(restore):
    function, schedule_class, _ = restore
    schedule = schedule_class()
    schedule.version_id, schedule.original_trigger = 21, TRIGGER
    session = _RestoreSession(schedule, None)
    with pytest.raises(ValueError, match="version of the schedule is gone"):
        function(session, 1)
    assert session.deleted == [] and session.commits == 0


def test_restore_refuses_a_row_moved_before_the_column_existed(restore):
    function, schedule_class, _ = restore
    schedule = schedule_class()
    schedule.version_id, schedule.original_trigger = 21, None
    with pytest.raises(ValueError, match="no original trigger"):
        function(_RestoreSession(schedule, None), 1)
