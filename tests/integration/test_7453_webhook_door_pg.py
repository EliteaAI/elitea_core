"""#7453 on a real PostgreSQL schema: the last-delivery columns and the door's DB writes.

Reuses the `world` fixture of test_7398_webhook_rows_pg.py (old R-2.0.8 schema, own `p_<big id>`, dropped after).
Covers: a schema migrated by the #7398 task (no last-delivery columns) is reported pending again and gets them;
`admit_webhook_run` inserts parallel `webhook` runs; `record_delivery` writes time and outcome.

    ELITEA_7011_PG_DSN=postgresql://centry:changeme@localhost:5432/db python3 run_tests.py integration/test_7453_webhook_door_pg.py
"""
import sys
import types
from datetime import UTC, datetime

from integration.test_7398_webhook_rows_pg import (  # noqa: F401
    PACKAGE, PLUGIN_ROOT, PROJECT_ID, SCHEMA, _add, _migrate, _pending, pytestmark, world,
)

DELIVERY_COLUMNS = ("last_delivery_at", "last_delivery_outcome")


def _columns(world):
    rows = world.sql("SELECT column_name FROM information_schema.columns WHERE table_schema = :s "
                     "AND table_name = 'entity_schedules'", s=SCHEMA)
    return {name for (name,) in rows}


def _door(world, monkeypatch):
    for name, attrs in {
        f"{PACKAGE}.utils.pipeline_trigger": {
            "WEBHOOK_TYPE_CONFIG": {}, "get_webhook_signature_header": None,
            "normalize_secret_value": None, "validate_webhook_secret": None},
        f"{PACKAGE}.utils.webhook_secrets": {"current_key": None, "is_signing_row": None, "prev_key": None},
    }.items():
        monkeypatch.setitem(sys.modules, name, types.SimpleNamespace(**attrs))
    # #7400: the project value is an RPC to configurations; None here, so the platform default applies
    monkeypatch.setitem(sys.modules, f"{PACKAGE}.utils.schedule_settings", types.SimpleNamespace(
        project_webhook_cap=lambda project_id, limits: min(world.project_cap or limits.per_webhook,
                                                           limits.per_webhook_limit)))
    world.project_cap = getattr(world, "project_cap", None)
    sys.modules["tools"].VaultClient = None
    import importlib.util  # pylint: disable=C0415
    name = f"{PACKAGE}.utils.webhook_door"
    spec = importlib.util.spec_from_file_location(name, PLUGIN_ROOT / "utils/webhook_door.py")
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, name, module)
    spec.loader.exec_module(module)
    return module


def test_7453_task_adds_the_delivery_columns_to_a_schema_the_7398_task_already_migrated(world):
    _migrate(world)
    for column in DELIVERY_COLUMNS:  # the state the #7398 version of the task left behind
        world.sql(f"ALTER TABLE {SCHEMA}.entity_schedules DROP COLUMN {column}")
    assert _pending(world)
    _migrate(world)
    assert set(DELIVERY_COLUMNS) <= _columns(world) and not _pending(world)
    _migrate(world)  # idempotent


def test_7453_admit_inserts_parallel_webhook_runs_and_record_delivery_writes_the_outcome(world, monkeypatch):
    _migrate(world)
    door = _door(world, monkeypatch)
    hook = _add(world, trigger_kind="webhook", webhook_type="github")
    now = datetime.now(UTC)
    with world.session() as session:
        row = session.get(world.models.EntitySchedule, hook)
        first, second = door.admit_webhook_run(session, PROJECT_ID, hook), door.admit_webhook_run(session, PROJECT_ID, hook)
        door.record_delivery(session, hook, "accepted", now)
        session.commit()
    assert first and second and first != second  # webhook runs are outside the active-run index
    runs = world.sql(f"SELECT trigger, status FROM {SCHEMA}.entity_schedule_runs WHERE schedule_id = :i", i=hook)
    assert runs == [("webhook", "running"), ("webhook", "running")]
    (stored,) = world.sql(f"SELECT last_delivery_at, last_delivery_outcome, last_run_status "
                          f"FROM {SCHEMA}.entity_schedules WHERE id = :i", i=hook)
    assert stored[1:] == ("accepted", "running") and abs((stored[0] - now).total_seconds()) < 1
