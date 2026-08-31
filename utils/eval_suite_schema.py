"""Idempotent per-project schema setup for eval suite description column.

Fresh environments get the column from create_all; existing p_N schemas need it
backfilled via this admin task.
"""
from sqlalchemy import text

from pylon.core.tools import log
from tools import db

_MIGRATION_STATEMENTS = (
    "ALTER TABLE p_{pid}.eval_suite ADD COLUMN IF NOT EXISTS description TEXT",
)


def apply_eval_suite_columns(project_ids):
    migrated, failed = [], []
    for pid in project_ids:
        try:
            with db.with_project_schema_session(pid) as session:
                for statement in _MIGRATION_STATEMENTS:
                    session.execute(text(statement.format(pid=pid)))
                session.commit()
            migrated.append(pid)
        except Exception:  # pylint: disable=W0703
            log.exception("eval suite schema: failed to migrate project %s", pid)
            failed.append({"project_id": pid})
    return migrated, failed
