"""The database write of the index schedule migration and its restore (SQLite, real service and models)."""
import sys
import types

import pytest
from fixtures.helpers import load_module_with_stubs
from unit import test_toolkit_index_schedule_service as service_env

PACKAGE, PLUGIN_ROOT, env = service_env.PACKAGE, service_env.PLUGIN_ROOT, service_env.env

BODY = {"cron": "0 3 * * *", "enabled": True, "created_by": 7, "timezone": "UTC"}
TEAM = {**BODY, "created_by": 8}


@pytest.fixture
def migration(env):  
    """The migration module loaded over the SQLite environment; `env.toolkit_row` reads the toolkit JSON."""
    trigger = types.ModuleType(f"{PACKAGE}.utils.pipeline_trigger")
    trigger.get_trigger_from_pipeline_settings = trigger.build_trigger_for_storage = None
    sys.modules[trigger.__name__] = trigger
    sys.modules[f"{PACKAGE}.models.agent_identity"] = sys.modules["tools"]

    def load(relative, name):
        return load_module_with_stubs(PLUGIN_ROOT / relative, f"{PACKAGE}.{name}")

    load("models/pd/pipeline_trigger.py", "models.pd.pipeline_trigger")
    load("utils/schedule_prerun.py", "utils.schedule_prerun")
    load("utils/orphaned_index_schedules.py", "utils.orphaned_index_schedules")
    load("utils/trigger_principal.py", "utils.trigger_principal")
    load("utils/legacy_schedule_migration.py", "utils.legacy_schedule_migration")
    env.migration = load("utils/legacy_index_schedule_migration.py", "utils.legacy_index_schedule_migration")
    all_models = sys.modules[f"{PACKAGE}.models.all"]

    def set_meta(indexes_meta):
        with env.session_factory() as session:
            session.get(all_models.EliteATool, 3).meta = {"indexes_meta": indexes_meta}
            session.commit()

    def schedules_of(index_name):
        with env.session_factory() as session:
            meta = session.get(all_models.EliteATool, 3).meta
            return meta["indexes_meta"][index_name]["schedules"]

    env.set_meta, env.schedules_of = set_meta, schedules_of
    return env


def row_values(index_name="docs", moved=None):
    return {
        "entity_type": "toolkit_index", "toolkit_id": 3, "index_name": index_name, "name": f"Docs toolkit / {index_name}",
        "description": None, "cron": BODY["cron"], "timezone": "UTC", "run_mode": None, "prompt": None,
        "variables": [], "enabled": True, "disabled_reason": None, "created_by": 7,
        "principal": {"kind": "delegation", "ref": 7}, "last_run": None, "next_run": None, "expires_at": None,
        "original_trigger": {"schedules": moved or {"7": BODY}}, "fast": False,
    }


def write(env, values, index_name="docs", keys=("7",)):
    with env.session_factory() as session:
        return env.migration._write_index(session, 3, values, index_name, list(keys))  # noqa: SLF001


def rows(env):
    with env.session_factory() as session:
        return session.query(env.EntitySchedule).all()


def test_the_write_adds_the_row_and_drops_only_the_moved_keys(migration):
    migration.set_meta({"docs": {"schedules": {"7": BODY, "-1": TEAM, "9": BODY}}, "other": {"schedules": {"7": BODY}}})
    assert write(migration, row_values(moved={"7": BODY, "-1": TEAM}), keys=("7", "-1")) is True
    found = rows(migration)
    assert [(r.index_name, r.toolkit_id, r.entity_type) for r in found] == [("docs", 3, "toolkit_index")]
    assert list(migration.schedules_of("docs")) == ["9"]
    assert list(migration.schedules_of("other")) == ["7"]


def test_the_write_keeps_the_json_when_the_index_already_has_a_row(migration):
    migration.set_meta({"docs": {"schedules": {"7": BODY}}})
    migration.seed_index("docs")
    assert write(migration, row_values()) is False
    assert len(rows(migration)) == 1
    assert list(migration.schedules_of("docs")) == ["7"]


def test_the_restore_writes_the_keys_back_and_deletes_the_row(migration):
    migration.set_meta({"docs": {"schedules": {}}})
    write(migration, row_values(moved={"7": BODY, "-1": TEAM}), keys=())
    report = migration.migration.restore_index_schedules(5)
    assert (len(report["restored"]), report["errors"], report["without_original_trigger"]) == (1, [], 0)
    assert rows(migration) == []
    assert set(migration.schedules_of("docs")) == {"7", "-1"}


def test_the_restore_never_overwrites_a_key_and_keeps_rows_without_an_original(migration):
    newer = {**BODY, "cron": "0 5 * * *"}
    migration.set_meta({"docs": {"schedules": {"7": newer}}})
    write(migration, row_values(moved={"7": BODY, "-1": TEAM}), keys=())
    migration.seed_index("manual")
    report = migration.migration.restore_index_schedules(5)
    assert report["without_original_trigger"] == 1
    assert migration.schedules_of("docs")["7"] == newer
    assert [r.index_name for r in rows(migration)] == ["manual"]
