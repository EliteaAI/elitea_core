"""Which version of an agent the Elitea MCP server exposes to external clients (#7341, C-1009).

By rule, not by the order the database returns the versions in: under enforce the version with an active identity
binding (the latest published one of them when several, else the latest bound one), so an external client reaches what
its owner had approved; otherwise, and under off and audit only, the latest published version; and with no published
version the first stored one, as before.
"""
from datetime import datetime

from sqlalchemy import select

from ..models.agent_identity import AgentIdentityBinding

PUBLISHED = "published"
RULES = ("bound", "published", "stored")


def _latest(versions):
    return max(versions, key=lambda version: (version.created_at or datetime.min, version.id))


def exposed_version(versions, bound_version_ids, governed):
    """``(version, rule)`` of ``versions`` (in stored order, each with ``id``, ``status``, ``created_at``), or
    ``(None, None)``. Pure."""
    if not versions:
        return None, None
    if governed:
        bound = [version for version in versions if version.id in bound_version_ids]
        if bound:
            return _latest([version for version in bound if version.status == PUBLISHED] or bound), "bound"
    published = [version for version in versions if version.status == PUBLISHED]
    if published:
        return _latest(published), "published"
    return versions[0], "stored"


def bound_version_ids(session, project_id, application_ids):
    """``{application id: {version ids with an active binding}}``; one statement."""
    found = {}
    if not application_ids:
        return found
    rows = session.execute(select(AgentIdentityBinding.application_id, AgentIdentityBinding.version_id).where(
        AgentIdentityBinding.project_id == project_id, AgentIdentityBinding.application_id.in_(list(application_ids)),
        AgentIdentityBinding.state == "active"))
    for application_id, version_id in rows:
        found.setdefault(application_id, set()).add(version_id)
    return found
