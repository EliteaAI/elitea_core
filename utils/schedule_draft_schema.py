"""entity_schedules.draft in project schemas that predate it.

New projects get the column from create_all. Existing schemas get it from the startup safety net in module.py:
one catalog query, then one transaction per schema that lacks it. Unlike refusal_streak the column is mapped, so a
schema without it fails every schedule read until the net runs. The net therefore runs on the boot thread, before
the module serves, and a failed project is tried again a few times.
"""
import time

from sqlalchemy import text
from pylon.core.tools import log
from tools import db

from ..models.entity_schedule import EntitySchedule

COLUMN = "draft"
INDEX = "uq_entity_schedules_draft_owner_entity"
LOCK_TIMEOUT = "5s"  # a long lock on the schedule table would stall the boot of every replica
ATTEMPTS, RETRY_DELAY_S = 3, 5
BOOT_BUDGET_S = 30  # the boot waits at most this long, however many projects fail
_MISSING_QUERY = text(
    "SELECT t.table_schema FROM information_schema.tables t WHERE t.table_name = :table "
    "AND t.table_schema ~ '^p_[0-9]+$' AND NOT EXISTS (SELECT 1 FROM information_schema.columns col "
    "WHERE col.table_schema = t.table_schema AND col.table_name = :table AND col.column_name = :column)")


def project_ids_missing_draft_column():
    with db.get_session(None) as session:
        rows = session.execute(_MISSING_QUERY, {"table": EntitySchedule.__tablename__, "column": COLUMN}).fetchall()
    return [int(row[0][2:]) for row in rows]


def _apply_once(pid) -> None:
    schema = f'"p_{int(pid)}"'
    with db.get_session(pid) as session:
        session.execute(text(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT}'"))
        session.execute(text(
            f'ALTER TABLE {schema}.{EntitySchedule.__tablename__} '
            f'ADD COLUMN IF NOT EXISTS {COLUMN} BOOLEAN NOT NULL DEFAULT FALSE'))
        session.execute(text(
            f'CREATE UNIQUE INDEX IF NOT EXISTS {INDEX} ON {schema}.{EntitySchedule.__tablename__} '
            f'(created_by, application_id) WHERE {COLUMN}'))
        session.commit()


def apply_draft_column(project_ids, attempts=ATTEMPTS, delay_s=RETRY_DELAY_S, sleep=time.sleep,
                       budget_s=BOOT_BUDGET_S, clock=time.monotonic):
    """(migrated, failed); one transaction per project, each tried up to `attempts` times.

    The loop stops when `budget_s` is spent; the projects left are returned as failed.
    The schema name is built from the integer id only.
    """
    deadline = clock() + budget_s
    migrated, pending = [], [int(pid) for pid in project_ids]
    for attempt in range(attempts):
        failed = []
        for pid in pending:
            if clock() >= deadline:
                failed.append(pid)
                continue
            try:
                _apply_once(pid)
                migrated.append(pid)
            except Exception:  # pylint: disable=W0703
                log.exception("schedule draft schema: failed to add the column in project %s", pid)
                failed.append(pid)
        pending = failed
        if not pending:
            break
        if attempt + 1 < attempts and clock() + delay_s < deadline:
            sleep(delay_s)
        elif pending:
            break
    return migrated, pending
