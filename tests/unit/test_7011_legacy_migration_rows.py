"""EL-7011 US-24 R1.3, R2.1, R2.2, R2.3, R2.6, R3.1, R3.2: task params and the row of one legacy trigger.

Contract under test: utils/legacy_schedule_migration.py. The trigger model, the cron check and the
timing helper are real; only the plugin shell around them and the auth RPC are stubbed.
"""
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


@pytest.mark.parametrize("bad", [{"created_by": None}, {"created_by": 0}, {"created_by": "x"}])
def test_el7011_r31_unusable_creator_cannot_make_a_row(migration, bad):
    with pytest.raises(migration.LegacyTriggerError) as error:
        _row(migration, **bad)
    assert error.value.reason == "owner_lost_access"


def test_el7011_r31_missing_creator_cannot_make_a_row(migration):
    trigger = {k: v for k, v in TRIGGER.items() if k != "created_by"}
    with pytest.raises(migration.LegacyTriggerError):
        migration.migrated_row(5, 11, 21, "p", "v", trigger, NOW)


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
