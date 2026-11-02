"""entity_schedules.draft in project schemas that predate it.

New projects get the column from create_all. Existing schemas get it from the startup safety net in module.py:
one catalog query, then one transaction per schema that lacks it. Unlike refusal_streak the column is mapped, so a
schema without it fails every schedule read until the net runs.
"""
from sqlalchemy import text
from pylon.core.tools import log
from tools import db

from ..models.entity_schedule import EntitySchedule

COLUMN = "draft"
INDEX = "uq_entity_schedules_draft_owner_entity"
_MISSING_QUERY = text(
    "SELECT t.table_schema FROM information_schema.tables t WHERE t.table_name = :table "
    "AND t.table_schema ~ '^p_[0-9]+$' AND NOT EXISTS (SELECT 1 FROM information_schema.columns col "
    "WHERE col.table_schema = t.table_schema AND col.table_name = :table AND col.column_name = :column)")


def project_ids_missing_draft_column():
    with db.get_session(None) as session:
        rows = session.execute(_MISSING_QUERY, {"table": EntitySchedule.__tablename__, "column": COLUMN}).fetchall()
    return [int(row[0][2:]) for row in rows]


def apply_draft_column(project_ids):
    """(migrated, failed); one transaction per project. The schema name is built from the integer id only."""
    migrated, failed = [], []
    for pid in project_ids:
        schema = f'"p_{int(pid)}"'
        try:
            with db.get_session(pid) as session:
                session.execute(text(
                    f'ALTER TABLE {schema}.{EntitySchedule.__tablename__} '
                    f'ADD COLUMN IF NOT EXISTS {COLUMN} BOOLEAN NOT NULL DEFAULT FALSE'))
                session.execute(text(
                    f'CREATE UNIQUE INDEX IF NOT EXISTS {INDEX} ON {schema}.{EntitySchedule.__tablename__} '
                    f'(created_by, application_id) WHERE {COLUMN}'))
                session.commit()
            migrated.append(pid)
        except Exception:  # pylint: disable=W0703
            log.exception("schedule draft schema: failed to add the column in project %s", pid)
            failed.append(pid)
    return migrated, failed
