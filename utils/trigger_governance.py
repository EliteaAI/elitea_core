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
# An owner schedule (#7011) runs on a mandate made for its owner (#7254): said at save time, the save goes on
NO_MANDATE_NOTICE = ("Scheduled (cron) runs need an approved mandate for {whom} on this version; Run now and Test run "
                     "act as {actor}. Ask a project admin (Admin → Governance → Mandates).")
AUDIT_ONLY_SUFFIX = " Until then each run is recorded as 'would deny'."


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


def schedule_governance_notice(module, session, project_id, application_id, version_id, owner_id, saver_id):
    """The notice for a saved owner schedule (#7011), or None: the unbound notice first; else, governance on, the
    owner's mandate on the version (one indexed read in ``session``) as a run would need it. Governance off asks
    nothing of the database."""
    notice = governance_notice_for(module, session, project_id, application_id, version_id, "schedule")
    if notice is not None or not (is_governed(module) or is_auditing(module)):
        return notice
    # late: only a governed save reads the mandate
    from .mandate_admission import audit_only_refusal, owner_mandate  # pylint: disable=C0415
    if audit_only_refusal(owner_mandate(session, project_id, version_id, owner_id)) is None:
        return None
    own = saver_id == owner_id
    notice = NO_MANDATE_NOTICE.format(whom="you" if own else "the schedule owner", actor="you" if own else "the owner")
    return notice if is_governed(module) else notice + AUDIT_ONLY_SUFFIX
