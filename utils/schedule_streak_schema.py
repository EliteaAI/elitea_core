"""``entity_schedules.refusal_streak`` in project schemas that predate it (#7254, C-1009).

New projects get the column from ``create_all``. Existing schemas get it from the startup safety net in ``module.py``:
one catalog query, then one ``ADD COLUMN IF NOT EXISTS`` per schema that lacks it, so a steady-state boot issues no DDL.
The column is deferred in the model; until it is added, in that schema, a governance refusal of a schedule run is
still recorded on the run but not counted (the schedule does not pause itself), and resuming a schedule fails.
"""
from sqlalchemy import text
from pylon.core.tools import log
from tools import db

from ..models.entity_schedule import EntitySchedule

COLUMN = "refusal_streak"
_MISSING_QUERY = text(
    "SELECT t.table_schema FROM information_schema.tables t WHERE t.table_name = :table "
    "AND t.table_schema ~ '^p_[0-9]+$' AND NOT EXISTS (SELECT 1 FROM information_schema.columns col "
    "WHERE col.table_schema = t.table_schema AND col.table_name = :table AND col.column_name = :column)")


def project_ids_missing_streak_column():
    with db.get_session(None) as session:
        rows = session.execute(_MISSING_QUERY, {"table": EntitySchedule.__tablename__, "column": COLUMN}).fetchall()
    return [int(row[0][2:]) for row in rows]


def apply_streak_column(project_ids):
    """``(migrated, failed)``; one transaction per project. The schema name is built from the integer id only."""
    migrated, failed = [], []
    for pid in project_ids:
        try:
            with db.get_session(pid) as session:
                session.execute(text(
                    f'ALTER TABLE "p_{int(pid)}".{EntitySchedule.__tablename__} '
                    f'ADD COLUMN IF NOT EXISTS {COLUMN} SMALLINT DEFAULT 0'))
                session.commit()
            migrated.append(pid)
        except Exception:  # pylint: disable=W0703
            log.exception("schedule streak schema: failed to add the column in project %s", pid)
            failed.append(pid)
    return migrated, failed
