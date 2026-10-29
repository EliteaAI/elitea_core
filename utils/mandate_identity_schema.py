"""``autonomous_mandate`` identity columns in project schemas that predate them (#7131, C-1009).

New projects get them from ``create_all``. Existing schemas get them from the startup safety net in ``module.py``: one
catalog query, then one ``ALTER TABLE`` per schema that lacks any of them (``ADD COLUMN IF NOT EXISTS``), so a
steady-state boot issues no DDL. The columns are deferred in the model and read only where the deployment allows a
mandate identity, so until they are added nothing changes.
"""
from sqlalchemy import text
from pylon.core.tools import log
from tools import db

from ..models.agent_identity import AutonomousMandate

COLUMNS = (("identity_source", "VARCHAR(24)"), ("identity_material_ref", "UUID"),
           ("identity_consented_at", "TIMESTAMP WITH TIME ZONE"))
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


def apply_identity_columns(project_ids):
    """``(migrated, failed)``; one transaction per project. The schema name is built from the integer id only."""
    migrated, failed = [], []
    added = ", ".join(f"ADD COLUMN IF NOT EXISTS {name} {kind}" for name, kind in COLUMNS)
    for pid in project_ids:
        try:
            with db.get_session(pid) as session:
                session.execute(text(f'ALTER TABLE "p_{int(pid)}".{AutonomousMandate.__tablename__} {added}'))
                session.commit()
            migrated.append(pid)
        except Exception:  # pylint: disable=W0703
            log.exception("mandate identity schema: failed to add the columns in project %s", pid)
            failed.append(pid)
    return migrated, failed
