"""Idempotent per-project setup of the agent identity tables (#6756, C-1009).

``create_all`` provisions them for new projects. Existing project schemas get them here, from the
startup safety net in ``module.py`` or from the ``create_agent_identity_tables`` admin task. Both
only create what is missing, so a steady-state boot issues no DDL.
"""
from sqlalchemy import bindparam, text
from pylon.core.tools import log
from tools import db

from ..models.agent_identity import AgentIdentityBinding, ExecutionSecurityContext

_MODELS = (AgentIdentityBinding, ExecutionSecurityContext)

# Only fully provisioned project schemas (they already hold elitea_core's own tables) are considered.
_MISSING_QUERY = text(
    "SELECT t.table_schema FROM information_schema.tables t "
    "WHERE t.table_name = 'application_versions' AND t.table_schema ~ '^p_[0-9]+$' "
    "AND (SELECT count(*) FROM information_schema.tables x "
    "WHERE x.table_schema = t.table_schema AND x.table_name IN :names) < :expected"
).bindparams(bindparam("names", expanding=True))


def project_ids_missing_identity_tables():
    """One catalog query: project schemas that lack either identity table."""
    params = {"names": [model.__tablename__ for model in _MODELS], "expected": len(_MODELS)}
    with db.get_session(None) as session:
        rows = session.execute(_MISSING_QUERY, params).fetchall()
    return [int(row[0][2:]) for row in rows]


def apply_identity_tables(project_ids, dry_run=False):
    """Create the missing tables, one transaction per project, so one broken schema does not stop
    the rest. Returns ``(migrated, failed)``; on a dry run ``migrated`` is what would be created."""
    migrated, failed = [], []
    for pid in project_ids:
        if dry_run:
            migrated.append(pid)
            continue
        try:
            with db.get_session(pid) as session:
                for model in _MODELS:
                    model.__table__.create(session.connection(), checkfirst=True)
                session.commit()
            migrated.append(pid)
        except Exception:  # pylint: disable=W0703
            log.exception("agent identity schema: failed to migrate project %s", pid)
            failed.append({"project_id": pid})
    return migrated, failed
