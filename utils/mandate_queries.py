"""Reads behind the autonomous mandate REST API (#7131, C-1009): pages of mandates with the names an operator needs.

Parameterized ORM statements only; the project id is part of every WHERE. The identity columns are selected only where
the deployment allows a mandate identity (``mandate_identity_sources``), so a schema that predates them lists as before.
Owners are named by email; no subject id and no consent material is ever returned (``consented`` is a yes or no).
"""
from sqlalchemy import func, select
from tools import db

from ..models.agent_identity import MANDATE_STATES, AutonomousMandate as Mandate
from .binding_queries import users_by_id
from .mandate_identity import allowed_sources

DEFAULT_LIMIT, MAX_LIMIT = 20, 50


def _iso(moment):
    return moment.isoformat() if moment else None


def list_page(module, project_id, *, state=None, limit=DEFAULT_LIMIT, offset=0):
    """``{total, rows, identity_sources}``: one page of the project's mandates, newest change first, and which
    identities the deployment lets a mandate name."""
    from ..models.all import Application, ApplicationVersion  # pylint: disable=C0415
    from ..models.elitea_tools import EliteATool  # pylint: disable=C0415
    sources = allowed_sources(module)
    identity = (Mandate.identity_source, Mandate.identity_material_ref, Mandate.identity_consented_at) if sources else ()
    conditions = [Mandate.project_id == project_id] + ([Mandate.state == state] if state in MANDATE_STATES else [])
    statement = (
        select(Mandate.id, Mandate.application_id, Mandate.agent_version, Mandate.owner_user_id, Mandate.state,
               Mandate.suspended_reason, Mandate.revision, Mandate.allowed_operations, Mandate.max_runtime_s,
               Mandate.valid_until, Mandate.created_by, Mandate.approved_by, Mandate.approval_ref,
               Mandate.policy_revision, Mandate.updated_at, Application.name.label("application_name"),
               ApplicationVersion.name.label("version_name"), Mandate.toolkit_id,
               EliteATool.name.label("toolkit_name"), *identity)
        .outerjoin(Application, Application.id == Mandate.application_id)
        .outerjoin(ApplicationVersion, ApplicationVersion.id == Mandate.agent_version)
        .outerjoin(EliteATool, EliteATool.id == Mandate.toolkit_id)
        .where(*conditions))
    with db.get_session(project_id) as session:
        total = session.execute(select(func.count()).select_from(
            select(Mandate.id).where(*conditions).subquery())).scalar_one()
        page = session.execute(statement.order_by(Mandate.updated_at.desc(), Mandate.id).limit(limit)
                               .offset(offset)).all()
    users = users_by_id([uid for row in page for uid in (row.owner_user_id, row.created_by, row.approved_by)])
    rows = []
    for row in page:
        rows.append({
            "id": str(row.id), "application": {"id": row.application_id, "name": row.application_name},
            "version": {"id": row.agent_version, "name": row.version_name}, "owner": users.get(row.owner_user_id),
            # a toolkit mandate (#7305) covers the owner's index schedules of this toolkit; null for an agent mandate
            "toolkit": None if row.toolkit_id is None else {"id": row.toolkit_id, "name": row.toolkit_name},
            "state": row.state, "suspended_reason": row.suspended_reason, "revision": row.revision,
            "operations": list(row.allowed_operations or []), "max_runtime_s": row.max_runtime_s,
            "valid_until": _iso(row.valid_until), "created_by": users.get(row.created_by),
            "approved_by": users.get(row.approved_by), "approval_ref": row.approval_ref,
            "policy_revision": row.policy_revision, "updated_at": _iso(row.updated_at),
            "identity_source": row.identity_source if sources else None,
            "consented": bool(sources and row.identity_material_ref is not None),
            "consented_at": _iso(row.identity_consented_at) if sources else None,
        })
    return {"total": total, "rows": rows, "identity_sources": list(sources)}
