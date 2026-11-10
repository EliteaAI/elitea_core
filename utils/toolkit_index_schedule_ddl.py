"""Toolkit index shape of ``entity_schedules`` in project schemas that predate it.

New projects get it from ``create_all``; existing ones from the ``ensure_toolkit_index_schedule_columns`` admin task,
which must run before the release code reads a schedule row (the model maps the new columns).
"""
from sqlalchemy import text
from pylon.core.tools import log
from tools import db

TABLE = "entity_schedules"
TYPE_CHECK = "ck_entity_schedules_entity_type"

COMMENTS = (
    ("application_id", "Agent and pipeline rows; NULL on toolkit index rows"),
    ("toolkit_id", "Toolkit index rows: the toolkit (elitea_tools.id) whose index the schedule refreshes"),
    ("index_name", "Toolkit index rows: the name of the index inside the toolkit"),
)

# Fixed text only: the project schema is chosen with search_path (a bound parameter), so no statement is built from
# a formatted string. The order matters: relax and add first, constrain after.
_STATEMENTS = (
    "ALTER TABLE entity_schedules ALTER COLUMN application_id DROP NOT NULL",
    "ALTER TABLE entity_schedules ADD COLUMN IF NOT EXISTS toolkit_id INTEGER "
    "REFERENCES elitea_tools (id) ON DELETE CASCADE",
    "ALTER TABLE entity_schedules ADD COLUMN IF NOT EXISTS index_name VARCHAR(256)",
    "DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'ck_entity_schedules_entity_type' "
    "AND conrelid = 'entity_schedules'::regclass AND pg_get_constraintdef(oid) NOT LIKE '%toolkit_index%') "
    "THEN ALTER TABLE entity_schedules DROP CONSTRAINT ck_entity_schedules_entity_type; END IF; END $$",
    "DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'ck_entity_schedules_entity_type' "
    "AND conrelid = 'entity_schedules'::regclass) "
    "THEN ALTER TABLE entity_schedules ADD CONSTRAINT ck_entity_schedules_entity_type "
    "CHECK (entity_type IN ('agent', 'pipeline', 'toolkit_index')); END IF; END $$",
    "DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'ck_entity_schedules_target' "
    "AND conrelid = 'entity_schedules'::regclass) "
    "THEN ALTER TABLE entity_schedules ADD CONSTRAINT ck_entity_schedules_target CHECK ("
    "(entity_type <> 'toolkit_index' AND application_id IS NOT NULL AND toolkit_id IS NULL AND index_name IS NULL)"
    " OR (entity_type = 'toolkit_index' AND toolkit_id IS NOT NULL AND index_name IS NOT NULL"
    " AND application_id IS NULL AND version_id IS NULL AND run_mode IS NULL AND prompt IS NULL"
    " AND trigger_kind = 'cron')); END IF; END $$",
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_entity_schedules_toolkit_index ON entity_schedules "
    "(toolkit_id, index_name) WHERE toolkit_id IS NOT NULL",
    "COMMENT ON COLUMN entity_schedules.application_id IS 'Agent and pipeline rows; NULL on toolkit index rows'",
    "COMMENT ON COLUMN entity_schedules.toolkit_id IS "
    "'Toolkit index rows: the toolkit (elitea_tools.id) whose index the schedule refreshes'",
    "COMMENT ON COLUMN entity_schedules.index_name IS 'Toolkit index rows: the name of the index inside the toolkit'",
)
_STATEMENT_TEXTS = tuple(text(sql) for sql in _STATEMENTS)
_SET_SCHEMA = text("SELECT set_config('search_path', :path, true)")

# A schema needs the task while the toolkit column is missing or the type check still refuses toolkit_index
_PENDING_QUERY = text(
    "SELECT t.table_schema FROM information_schema.tables t WHERE t.table_name = :table "
    "AND t.table_schema ~ '^p_[0-9]+$' AND (NOT EXISTS (SELECT 1 FROM information_schema.columns col "
    "WHERE col.table_schema = t.table_schema AND col.table_name = :table AND col.column_name = 'toolkit_id') "
    "OR NOT EXISTS (SELECT 1 FROM pg_constraint k JOIN pg_namespace n ON n.oid = k.connamespace "
    "WHERE n.nspname = t.table_schema AND k.conname = :check AND pg_get_constraintdef(k.oid) LIKE '%toolkit_index%'))"
)


def toolkit_index_pending_project_ids():
    with db.get_session(None) as session:
        rows = session.execute(_PENDING_QUERY, {"table": TABLE, "check": TYPE_CHECK}).fetchall()
    return [int(row[0][2:]) for row in rows]


def toolkit_index_statements():
    """The idempotent DDL for one project schema, in order; the schema is set with ``search_path``."""
    return _STATEMENTS


def apply_toolkit_index_schedule_columns(project_ids, dry_run=False):
    """``(migrated, failed)``; one transaction per project, so a failure leaves that schema unchanged."""
    migrated, failed = [], []
    for pid in project_ids:
        if dry_run:
            migrated.append(pid)
            continue
        try:
            with db.get_session(pid) as session:
                session.execute(_SET_SCHEMA, {"path": f"p_{int(pid)}"})
                for statement in _STATEMENT_TEXTS:
                    session.execute(statement)
                session.commit()
            migrated.append(pid)
        except Exception:  # pylint: disable=W0703
            log.exception("toolkit index schedule columns: failed in project %s", pid)
            failed.append(pid)
    return migrated, failed
