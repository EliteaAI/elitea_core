"""Reads behind the agent identity binding REST API (#7105, C-1009): pages of bindings with the names and users an
operator needs, the digest status of each row, and one binding with the inputs of its digest.

Parameterized ORM statements only. The project id is part of every WHERE, so an id from another project is simply
not found. Nothing here returns instruction text or toolkit settings.
"""
from pylon.core.tools import log
from sqlalchemy import func, select
from tools import auth, db

from ..models.agent_identity import AgentIdentityBinding as Binding
from .definition_digest import DigestError, digest_for_version, digest_inputs_for_version

MAX_VERSIONS_PER_AGENT = 50
SORTS = ("updated_at", "created_at", "state", "application_name")
DIGEST_PREFIX_LENGTH = 12


def _models():
    from ..models.all import Application, ApplicationVersion  # pylint: disable=C0415
    return Application, ApplicationVersion


def _like(text):
    """``text`` as a contains-pattern with the wildcard characters escaped."""
    return "%" + text.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%"


def digest_prefix(digest):
    """The first characters of the hash: enough to tell two digests apart on screen."""
    return (digest or "").split(":", 1)[-1][:DIGEST_PREFIX_LENGTH]


def digest_now(session, version_id):
    """The digest of the version's definition as it is now, or None when it cannot be computed."""
    try:
        return digest_for_version(session, version_id)
    except DigestError:
        return None


def digest_status(stored, current):
    return "unknown" if current is None else ("match" if current == stored else "changed")


def users_by_id(user_ids):
    """``{id: {id, email}}`` for the ids, from one identity call; an id that cannot be resolved has no email."""
    ids = sorted({uid for uid in user_ids if isinstance(uid, int) and uid > 0})
    found = {}
    if ids:
        try:
            found = {user["id"]: user for user in auth.list_users(user_ids=ids)}
        except Exception as exc:  # pylint: disable=W0703
            log.warning("binding list: users not resolved (%s)", type(exc).__name__)
    return {uid: {"id": uid, "email": (found.get(uid) or {}).get("email")} for uid in ids}


def _iso(moment):
    return moment.isoformat() if moment else None


def _row(binding, application_name, version_name, users):
    return {
        "id": str(binding.id),
        "application": {"id": binding.application_id, "name": application_name},
        "version": {"id": binding.version_id, "name": version_name},
        "identity_mode": binding.identity_mode,
        "state": binding.state,
        "policy_revision": binding.policy_revision,
        "approval_ref": binding.approval_ref,
        "revocation_epoch": binding.revocation_epoch,
        "created_by": users.get(binding.created_by),
        "state_changed_by": users.get(binding.state_changed_by),
        "created_at": _iso(binding.created_at),
        "updated_at": _iso(binding.updated_at),
    }


def _select(project_id):
    application, version = _models()
    return (
        select(Binding, application.name.label("application_name"), version.name.label("version_name"))
        .select_from(Binding)
        .outerjoin(application, application.id == Binding.application_id)
        .outerjoin(version, version.id == Binding.version_id)
        .where(Binding.project_id == project_id)
    ), application


def list_page(project_id, *, state=None, identity_mode=None, application_id=None, search=None,
              sort_by="updated_at", sort_order="desc", limit=20, offset=0):
    """``{total, rows}``: one page of the project's bindings. The digest is recomputed for the page's rows only."""
    statement, application = _select(project_id)
    if state:
        statement = statement.where(Binding.state == state)
    if identity_mode:
        statement = statement.where(Binding.identity_mode == identity_mode)
    if application_id:
        statement = statement.where(Binding.application_id == application_id)
    if search:
        statement = statement.where(application.name.ilike(_like(search), escape="\\"))
    column = application.name if sort_by == "application_name" else getattr(Binding, sort_by if sort_by in SORTS else "updated_at")
    order = column.asc() if sort_order == "asc" else column.desc()
    with db.get_session(project_id) as session:
        total = session.execute(select(func.count()).select_from(statement.subquery())).scalar_one()
        page = session.execute(statement.order_by(order, Binding.id).limit(limit).offset(offset)).all()
        users = users_by_id([uid for binding, _, _ in page for uid in (binding.created_by, binding.state_changed_by)])
        rows = []
        for binding, application_name, version_name in page:
            item = _row(binding, application_name, version_name, users)
            item["definition_digest"] = digest_prefix(binding.definition_digest)
            item["digest_status"] = digest_status(binding.definition_digest, digest_now(session, binding.version_id))
            rows.append(item)
    return {"total": total, "rows": rows}


def find_binding(project_id, binding_id):
    """The binding's identity, or None when this project has no such binding (another project's id is None too)."""
    with db.get_session(project_id) as session:
        row = session.execute(
            select(Binding).where(Binding.project_id == project_id, Binding.id == binding_id)).scalar_one_or_none()
        return None if row is None else {
            "id": row.id, "application_id": row.application_id, "version_id": row.version_id,
            "identity_mode": row.identity_mode, "state": row.state}


def get_detail(project_id, binding_id):
    """One binding with the digest as stored and as it is now, and the inputs of the digest; None when not found."""
    statement, _ = _select(project_id)
    with db.get_session(project_id) as session:
        row = session.execute(statement.where(Binding.id == binding_id)).one_or_none()
        if row is None:
            return None
        binding, application_name, version_name = row
        users = users_by_id([binding.created_by, binding.state_changed_by])
        item = _row(binding, application_name, version_name, users)
        current = digest_now(session, binding.version_id)
        item["definition_digest"] = binding.definition_digest
        item["digest_now"] = current
        item["digest_status"] = digest_status(binding.definition_digest, current)
        try:
            item["digest_inputs"] = digest_inputs_for_version(session, binding.version_id)
        except DigestError:
            item["digest_inputs"] = None
        item["grants_count"] = None   # the grants live in the secrets plugin; no count is exposed to this plugin yet
    return item


def list_agents(project_id, *, search=None, limit=20, offset=0):
    """``{total, rows}``: agents of the project (name order) with their versions and the bindings each version has."""
    application, version = _models()
    statement = select(application.id, application.name)
    if search:
        statement = statement.where(application.name.ilike(_like(search), escape="\\"))
    with db.get_session(project_id) as session:
        total = session.execute(select(func.count()).select_from(statement.subquery())).scalar_one()
        agents = session.execute(statement.order_by(application.name, application.id).limit(limit).offset(offset)).all()
        ids = [agent.id for agent in agents]
        versions, bindings = {}, {}
        if ids:
            ranked = select(
                version.id, version.application_id, version.name,
                func.row_number().over(partition_by=version.application_id, order_by=version.id.desc()).label("rank"),
            ).where(version.application_id.in_(ids)).subquery()
            for row in session.execute(
                    select(ranked.c.id, ranked.c.application_id, ranked.c.name)
                    .where(ranked.c.rank <= MAX_VERSIONS_PER_AGENT).order_by(ranked.c.application_id, ranked.c.id.desc())):
                versions.setdefault(row.application_id, []).append(row)
            for row in session.execute(
                    select(Binding.id, Binding.version_id, Binding.identity_mode, Binding.state)
                    .where(Binding.project_id == project_id, Binding.application_id.in_(ids))
                    .order_by(Binding.identity_mode)):
                bindings.setdefault(row.version_id, []).append(
                    {"id": str(row.id), "identity_mode": row.identity_mode, "state": row.state})
    return {"total": total, "rows": [
        {"id": agent.id, "name": agent.name, "versions": [
            {"id": ver.id, "name": ver.name, "bindings": bindings.get(ver.id, [])} for ver in versions.get(agent.id, [])]}
        for agent in agents]}
