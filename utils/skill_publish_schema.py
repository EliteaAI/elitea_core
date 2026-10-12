"""Idempotent per-project schema setup for skill publishing and skill run settings.

Adds the columns skill publishing and skill run settings rely on to each project
(tenant) schema.
`create_all` provisions these for new projects automatically; existing project
schemas predate the columns and are brought up to date here — driven either by
the admin migration task or the startup safety net.
"""
from sqlalchemy import text
from pylon.core.tools import log
from tools import db


_MIGRATION_STATEMENTS = (
    "ALTER TABLE p_{pid}.skills ADD COLUMN IF NOT EXISTS shared_owner_id INTEGER",
    "ALTER TABLE p_{pid}.skills ADD COLUMN IF NOT EXISTS shared_id INTEGER",
    "ALTER TABLE p_{pid}.skill_versions "
    "ADD COLUMN IF NOT EXISTS status VARCHAR NOT NULL DEFAULT 'draft'",
    "CREATE INDEX IF NOT EXISTS ix_p_{pid}_skill_versions_status "
    "ON p_{pid}.skill_versions (status)",
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_skills_shared_owner "
    "ON p_{pid}.skills (shared_owner_id, shared_id) WHERE shared_owner_id IS NOT NULL",
)

_RUN_SETTINGS_STATEMENTS = (
    "ALTER TABLE p_{pid}.skill_versions ADD COLUMN IF NOT EXISTS run_settings JSONB",
)


def _apply_statements(project_ids, statements):
    migrated, failed = [], []
    for pid in project_ids:
        try:
            with db.with_project_schema_session(pid) as session:
                for statement in statements:
                    session.execute(text(statement.format(pid=pid)))
                session.commit()
            migrated.append(pid)
        except Exception:
            log.exception("skill schema: failed to migrate project %s", pid)
            failed.append({"project_id": pid})
    return migrated, failed


def apply_skill_publish_columns(project_ids):
    return _apply_statements(project_ids, _MIGRATION_STATEMENTS)


def apply_skill_run_settings_column(project_ids):
    return _apply_statements(project_ids, _RUN_SETTINGS_STATEMENTS)


def apply_skill_columns(project_ids):
    return _apply_statements(project_ids, _MIGRATION_STATEMENTS + _RUN_SETTINGS_STATEMENTS)


def project_ids_missing_skill_columns():
    query = text(
        "SELECT t.table_schema FROM information_schema.tables t "
        "WHERE t.table_name = 'skill_versions' AND t.table_schema ~ '^p_[0-9]+$' "
        "AND (SELECT count(*) FROM information_schema.columns c "
        "WHERE c.table_schema = t.table_schema AND c.table_name = 'skill_versions' "
        "AND c.column_name IN ('status', 'run_settings')) < 2"
    )
    with db.get_session(None) as session:
        rows = session.execute(query).fetchall()
    return [int(row[0][2:]) for row in rows]
