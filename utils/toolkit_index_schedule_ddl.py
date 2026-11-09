"""Toolkit index shape of ``entity_schedules`` in project schemas that predate it.

New projects get it from ``create_all``; existing ones from the ``ensure_toolkit_index_schedule_columns`` admin task,
which must run before the release code reads a schedule row (the model maps the new columns).
"""
from sqlalchemy import text
from pylon.core.tools import log
from tools import db

TABLE = "entity_schedules"
TYPE_CHECK = "ck_entity_schedules_entity_type"
TARGET_CHECK = "ck_entity_schedules_target"
INDEX = "uq_entity_schedules_toolkit_index"

TYPE_CHECK_BODY = "entity_type IN ('agent', 'pipeline', 'toolkit_index')"
TARGET_CHECK_BODY = (
    "(entity_type <> 'toolkit_index' AND application_id IS NOT NULL AND toolkit_id IS NULL AND index_name IS NULL)"
    " OR (entity_type = 'toolkit_index' AND toolkit_id IS NOT NULL AND index_name IS NOT NULL"
    " AND application_id IS NULL AND version_id IS NULL AND run_mode IS NULL AND prompt IS NULL"
    " AND trigger_kind = 'cron')"
)
COMMENTS = (
    ("application_id", "Agent and pipeline rows; NULL on toolkit index rows"),
    ("toolkit_id", "Toolkit index rows: the toolkit (elitea_tools.id) whose index the schedule refreshes"),
    ("index_name", "Toolkit index rows: the name of the index inside the toolkit"),
)

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


def toolkit_index_statements(schema):
    """The idempotent DDL for one schema, in order; the schema name comes from an integer id only."""
    table = f'"{schema}".{TABLE}'
    guard = (
        "DO $$ BEGIN IF {condition} THEN {action}; END IF; END $$"
    )
    return [
        f"ALTER TABLE {table} ALTER COLUMN application_id DROP NOT NULL",
        f'ALTER TABLE {table} ADD COLUMN IF NOT EXISTS toolkit_id INTEGER REFERENCES "{schema}".elitea_tools (id) '
        f"ON DELETE CASCADE",
        f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS index_name VARCHAR(256)",
        guard.format(
            condition=(f"EXISTS (SELECT 1 FROM pg_constraint WHERE conname = '{TYPE_CHECK}' "
                       f"AND conrelid = '{table}'::regclass AND pg_get_constraintdef(oid) NOT LIKE '%toolkit_index%')"),
            action=f"ALTER TABLE {table} DROP CONSTRAINT {TYPE_CHECK}"),
        guard.format(
            condition=f"NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = '{TYPE_CHECK}' "
                      f"AND conrelid = '{table}'::regclass)",
            action=f"ALTER TABLE {table} ADD CONSTRAINT {TYPE_CHECK} CHECK ({TYPE_CHECK_BODY})"),
        guard.format(
            condition=f"NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = '{TARGET_CHECK}' "
                      f"AND conrelid = '{table}'::regclass)",
            action=f"ALTER TABLE {table} ADD CONSTRAINT {TARGET_CHECK} CHECK ({TARGET_CHECK_BODY})"),
        f"CREATE UNIQUE INDEX IF NOT EXISTS {INDEX} ON {table} (toolkit_id, index_name) WHERE toolkit_id IS NOT NULL",
        *[f"COMMENT ON COLUMN {table}.{name} IS '{comment}'" for name, comment in COMMENTS],
    ]


def apply_toolkit_index_schedule_columns(project_ids, dry_run=False):
    """``(migrated, failed)``; one transaction per project, so a failure leaves that schema unchanged."""
    migrated, failed = [], []
    for pid in project_ids:
        if dry_run:
            migrated.append(pid)
            continue
        try:
            with db.get_session(pid) as session:
                for sql in toolkit_index_statements(f"p_{int(pid)}"):
                    session.execute(text(sql))
                session.commit()
            migrated.append(pid)
        except Exception:  # pylint: disable=W0703
            log.exception("toolkit index schedule columns: failed in project %s", pid)
            failed.append(pid)
    return migrated, failed
