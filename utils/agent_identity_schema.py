"""Idempotent per-project setup of the agent identity tables (#6756, C-1009) and the autonomous mandate
table (#6947).

``create_all`` provisions them for new projects. Existing project schemas get them here, from the
startup safety net in ``module.py`` or from the ``create_agent_identity_tables`` and
``create_mandate_tables`` admin tasks. All only create what is missing, so a steady-state boot issues no DDL.

A development run (#7213) has no binding, so ``execution_security_context.agent_binding_ref`` must accept NULL, and a
toolkit run (#7229) has no application version, so ``application_id`` and ``version_id`` must too: a schema that predates
that gets the constraints dropped by the same safety net and task (``apply_nullable_binding_ref``).
"""
from sqlalchemy import bindparam, text
from pylon.core.tools import log
from tools import db

from ..models.agent_identity import AgentIdentityBinding, AutonomousMandate, ExecutionSecurityContext

IDENTITY_TABLES = (AgentIdentityBinding, ExecutionSecurityContext)
MANDATE_TABLES = (AutonomousMandate,)
CONTEXT_TABLE, BINDING_REF_COLUMN = ExecutionSecurityContext.__tablename__, "agent_binding_ref"
NULLABLE_COLUMNS = (BINDING_REF_COLUMN, "application_id", "version_id")   # #7213, #7229

# Only fully provisioned project schemas (they already hold elitea_core's own tables) are considered.
_MISSING_QUERY = text(
    "SELECT t.table_schema FROM information_schema.tables t "
    "WHERE t.table_name = 'application_versions' AND t.table_schema ~ '^p_[0-9]+$' "
    "AND (SELECT count(*) FROM information_schema.tables x "
    "WHERE x.table_schema = t.table_schema AND x.table_name IN :names) < :expected"
).bindparams(bindparam("names", expanding=True))


# Project schemas whose context table still refuses a NULL in one of ``NULLABLE_COLUMNS`` (created before #7213/#7229)
_NOT_NULL_QUERY = text(
    "SELECT DISTINCT table_schema FROM information_schema.columns "
    "WHERE table_name = :table AND column_name IN :columns AND is_nullable = 'NO' AND table_schema ~ '^p_[0-9]+$'"
).bindparams(bindparam("columns", expanding=True))


def project_ids_missing_tables(models=IDENTITY_TABLES):
    """One catalog query: project schemas that lack any of these tables."""
    params = {"names": [model.__tablename__ for model in models], "expected": len(models)}
    with db.get_session(None) as session:
        rows = session.execute(_MISSING_QUERY, params).fetchall()
    return [int(row[0][2:]) for row in rows]


def project_ids_missing_identity_tables():
    return project_ids_missing_tables(IDENTITY_TABLES)


def project_ids_missing_mandate_tables():
    return project_ids_missing_tables(MANDATE_TABLES)


def apply_tables(project_ids, models, dry_run=False):
    """Create the missing tables, one transaction per project, so one broken schema does not stop
    the rest. Returns ``(migrated, failed)``; on a dry run ``migrated`` is what would be created."""
    migrated, failed = [], []
    for pid in project_ids:
        if dry_run:
            migrated.append(pid)
            continue
        try:
            with db.get_session(pid) as session:
                for model in models:
                    model.__table__.create(session.connection(), checkfirst=True)
                session.commit()
            migrated.append(pid)
        except Exception:  # pylint: disable=W0703
            log.exception("agent identity schema: failed to migrate project %s", pid)
            failed.append({"project_id": pid})
    return migrated, failed


def apply_identity_tables(project_ids, dry_run=False):
    return apply_tables(project_ids, IDENTITY_TABLES, dry_run)


def apply_mandate_tables(project_ids, dry_run=False):
    return apply_tables(project_ids, MANDATE_TABLES, dry_run)


def project_ids_with_not_null_binding_ref():
    """One catalog query: project schemas where a column of ``NULLABLE_COLUMNS`` of ``execution_security_context`` is
    still NOT NULL."""
    with db.get_session(None) as session:
        rows = session.execute(_NOT_NULL_QUERY, {"table": CONTEXT_TABLE, "columns": list(NULLABLE_COLUMNS)}).fetchall()
    return [int(row[0][2:]) for row in rows]


def apply_nullable_binding_ref(project_ids, dry_run=False):
    """Drop the NOT NULL of ``NULLABLE_COLUMNS`` where it is still there (#7213, #7229), one transaction per project
    (dropping it from a column that has none is a no-op). Returns ``(migrated, failed)`` as ``apply_tables``. The schema
    name is built from the integer project id only, the column names are the constants above."""
    migrated, failed = [], []
    for pid in project_ids:
        if dry_run:
            migrated.append(pid)
            continue
        try:
            with db.get_session(pid) as session:
                for column in NULLABLE_COLUMNS:
                    session.execute(text(
                        f'ALTER TABLE "p_{int(pid)}".{CONTEXT_TABLE} ALTER COLUMN {column} DROP NOT NULL'))
                session.commit()
            migrated.append(pid)
        except Exception:  # pylint: disable=W0703
            log.exception("agent identity schema: failed to relax the binding reference of project %s", pid)
            failed.append({"project_id": pid})
    return migrated, failed
