"""``autonomous_mandate`` identity columns in project schemas that predate them (#7131, C-1009).

New projects get them from ``create_all``. Existing schemas get them from the startup safety net in ``module.py``: one
catalog query, then one ``ALTER TABLE`` per schema that lacks any of them (``ADD COLUMN IF NOT EXISTS``, nullable, no
default: metadata only), under a 5 s ``lock_timeout`` and tried again a few times, so a busy table never queues the
mandate reads of that schema for long. A schema where it still failed is named at ERROR: creating a mandate there fails
until the next start adds them (the INSERT names them). The columns are deferred in the model and read only where the
deployment allows a mandate identity, so until they are added nothing else changes.
"""
import time

from sqlalchemy import text
from pylon.core.tools import log
from tools import db

from ..models.agent_identity import AutonomousMandate

COLUMNS = (("identity_source", "VARCHAR(24)"), ("identity_material_ref", "UUID"),
           ("identity_consented_at", "TIMESTAMP WITH TIME ZONE"), ("identity_scope", "JSONB"))
LOCK_TIMEOUT = "5s"
ATTEMPTS, RETRY_DELAY_S = 3, 5
_MISSING_QUERY = text(
    "SELECT t.table_schema FROM information_schema.tables t WHERE t.table_name = :table "
    "AND t.table_schema ~ '^p_[0-9]+$' AND (SELECT count(*) FROM information_schema.columns col "
    "WHERE col.table_schema = t.table_schema AND col.table_name = :table AND col.column_name = ANY(:columns)) < :expected")


def project_ids_missing_identity_columns():
    with db.get_session(None) as session:
        rows = session.execute(_MISSING_QUERY, {
            "table": AutonomousMandate.__tablename__, "columns": [name for name, _ in COLUMNS],
            "expected": len(COLUMNS)}).fetchall()
    return [int(row[0][2:]) for row in rows]


def _apply_once(pid) -> None:
    added = ", ".join(f"ADD COLUMN IF NOT EXISTS {name} {kind}" for name, kind in COLUMNS)
    with db.get_session(pid) as session:
        session.execute(text(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT}'"))
        session.execute(text(f'ALTER TABLE "p_{int(pid)}".{AutonomousMandate.__tablename__} {added}'))
        session.commit()


def apply_identity_columns(project_ids, attempts=ATTEMPTS, delay_s=RETRY_DELAY_S, sleep=time.sleep):
    """``(migrated, failed)``; one transaction per project, each tried up to ``attempts`` times. The schema name is
    built from the integer id only."""
    migrated, pending = [], [int(pid) for pid in project_ids]
    for attempt in range(attempts):
        failed = []
        for pid in pending:
            try:
                _apply_once(pid)
                migrated.append(pid)
            except Exception:  # pylint: disable=W0703
                failed.append(pid)
        pending = failed
        if not pending:
            break
        if attempt + 1 < attempts:
            sleep(delay_s)
    if pending:
        log.error("mandate identity schema: the columns could not be added in projects %s; creating a mandate there "
                  "fails until the next start adds them", pending)
    return migrated, pending
