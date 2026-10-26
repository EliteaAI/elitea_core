"""What an unattended trigger still needs under governance (#7213, C-1009).

Schedules and webhooks saved while a version is being developed are not development runs: they take the governed path,
which needs an active binding and an approved mandate. The trigger API saves the trigger as before and, when governance
is on and the version has no active binding, says so in its answer, so the editor can warn the author. One indexed read
in the request's session; nothing on the run path.
"""
from sqlalchemy import select

from ..models.agent_identity import AgentIdentityBinding
from .governance import is_auditing, is_governed

UNATTENDED_TRIGGERS = frozenset({"schedule", "webhook"})
UNBOUND_NOTICE = ("Credential governance is on and this version has no active identity binding: runs started by this "
                  "trigger will be refused until an operator binds the version and approves a mandate for it. The "
                  "editor's test chat is not affected.")


def version_is_bound(session, project_id, application_id, version_id) -> bool:
    """Does an active binding exist for exactly this version (any identity mode)?"""
    return session.execute(
        select(AgentIdentityBinding.id).where(
            AgentIdentityBinding.project_id == project_id, AgentIdentityBinding.application_id == application_id,
            AgentIdentityBinding.version_id == version_id, AgentIdentityBinding.state == "active",
        ).limit(1)
    ).first() is not None


def governance_notice_for(module, session, project_id, application_id, version_id, trigger_type):
    """The notice for a saved trigger, or None: an unattended trigger, governance on (audit only or enforce), no active
    binding. Governance off asks nothing of the database."""
    if trigger_type not in UNATTENDED_TRIGGERS or not (is_governed(module) or is_auditing(module)):
        return None
    if version_is_bound(session, project_id, application_id, version_id):
        return None
    return UNBOUND_NOTICE
