"""``agent_identity_binding.allowed_platform_operations`` in project schemas that predate it (#7214, C-1009).

New projects get the column from ``create_all``. Existing schemas get it from the startup safety net in ``module.py``:
one catalog query, then one ``ADD COLUMN IF NOT EXISTS`` per schema that lacks it, so a steady-state boot issues no DDL.
Until it is added the column is not read (it is deferred in the model) and a binding allows ``read`` only.
"""
from sqlalchemy import text
from pylon.core.tools import log
from tools import db

from ..models.agent_identity import AgentIdentityBinding

COLUMN = "allowed_platform_operations"
_MISSING_QUERY = text(
    "SELECT t.table_schema FROM information_schema.tables t WHERE t.table_name = :table "
    "AND t.table_schema ~ '^p_[0-9]+$' AND NOT EXISTS (SELECT 1 FROM information_schema.columns col "
    "WHERE col.table_schema = t.table_schema AND col.table_name = :table AND col.column_name = :column)")


def project_ids_missing_operations_column():
    with db.get_session(None) as session:
        rows = session.execute(
            _MISSING_QUERY, {"table": AgentIdentityBinding.__tablename__, "column": COLUMN}).fetchall()
    return [int(row[0][2:]) for row in rows]


def apply_operations_column(project_ids):
    """``(migrated, failed)``; one transaction per project. The schema name is built from the integer id only."""
    migrated, failed = [], []
    for pid in project_ids:
        try:
            with db.get_session(pid) as session:
                session.execute(text(
                    f'ALTER TABLE "p_{int(pid)}".{AgentIdentityBinding.__tablename__} '
                    f'ADD COLUMN IF NOT EXISTS {COLUMN} JSONB'))
                session.commit()
            migrated.append(pid)
        except Exception:  # pylint: disable=W0703
            log.exception("binding operations schema: failed to add the column in project %s", pid)
            failed.append(pid)
    return migrated, failed
