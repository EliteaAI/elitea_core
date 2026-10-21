"""Autonomous mandate lifecycle (#6947, C-1009): which state changes are legal, and the one conditional
statement that applies each of them without losing a race.

A mandate becomes ``active`` only through ``approve_mandate``, by someone who is neither its creator nor its
owner. Anything that takes it out of use (suspend, expire, revoke) raises ``revocation_epoch``, so work admitted
earlier can be told apart from work admitted after the change. None of these functions commits: the caller owns
the transaction, so it can add the audit event and the revocation of the live contexts to the same one.
"""
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import func, select, update

from ..models.agent_identity import SUSPENDED_REASONS, AutonomousMandate
from .binding_state import InvalidTransition

MAX_MANDATE_VALIDITY_DAYS = 180
TRANSITIONS = frozenset({
    ("draft", "pending_approval"), ("pending_approval", "draft"),
    ("pending_approval", "active"),
    ("active", "suspended"), ("suspended", "active"),
    ("active", "expired"), ("suspended", "expired"),
    ("draft", "revoked"), ("pending_approval", "revoked"), ("active", "revoked"), ("suspended", "revoked"),
})
BUMPS_EPOCH = frozenset({"suspended", "expired", "revoked"})


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _apply(session, conditions, values) -> int:
    return session.execute(
        update(AutonomousMandate).where(*conditions).values(**values).execution_options(synchronize_session=False)
    ).rowcount


def transition_mandate(  # pylint: disable=R0913
    session, mandate_id, expected: str, target: str, *, actor: int, reason: Optional[str] = None, now=None,
) -> bool:
    """Move a mandate from ``expected`` to ``target``. True when this call made the change.

    False means the row is not in ``expected`` any more (another writer won), does not exist, or the guard of
    the statement refused (resuming one's own mandate, resuming past ``valid_until``); the caller re-reads.
    Raises ``InvalidTransition`` for a change that is never allowed here. Approval is ``approve_mandate``; expiry is
    ``expire_if_due`` only, which checks the clock.
    """
    if (expected, target) not in TRANSITIONS:
        raise InvalidTransition("INVALID_TRANSITION", f"{expected} -> {target} is not a legal mandate change")
    if target == "active" and expected == "pending_approval":
        raise InvalidTransition("APPROVAL_REQUIRED", "activation is done by approving the mandate")
    if target == "expired":
        raise InvalidTransition("EXPIRY_BY_CLOCK", "a mandate expires by the clock only (expire_if_due)")
    if target == "suspended":
        reason = reason or "MANUAL"
        if reason not in SUSPENDED_REASONS:
            raise InvalidTransition("INVALID_REASON", "unknown suspension reason")
    else:
        reason = None
    conditions = [AutonomousMandate.id == mandate_id, AutonomousMandate.state == expected]
    if expected == "suspended" and target == "active":
        # Resuming is a fresh approval-grade decision: not by the owner, and not past the validity window
        conditions += [AutonomousMandate.owner_user_id != actor, AutonomousMandate.valid_until > (now or _now())]
    values = {"state": target, "suspended_reason": reason, "state_changed_by": actor, "updated_at": func.now()}
    if target in BUMPS_EPOCH:
        values["revocation_epoch"] = AutonomousMandate.revocation_epoch + 1
    return _apply(session, conditions, values) == 1


def approve_mandate(session, mandate_id, *, actor: int, approval_ref: str, policy_revision: str, now=None) -> bool:
    """``pending_approval`` -> ``active``, once, by someone who is neither the creator nor the owner, before
    ``valid_until``. True when this call approved it; False when any of those did not hold (the caller reads the
    row to tell which). Raises ``InvalidTransition`` when the approval or the policy revision is missing."""
    if not approval_ref or not policy_revision:
        raise InvalidTransition("APPROVAL_REQUIRED", "approval needs an approval reference and a policy revision")
    return _apply(
        session,
        [AutonomousMandate.id == mandate_id, AutonomousMandate.state == "pending_approval",
         AutonomousMandate.approved_by.is_(None), AutonomousMandate.created_by != actor,
         AutonomousMandate.owner_user_id != actor, AutonomousMandate.valid_until > (now or _now())],
        {"state": "active", "approved_by": actor, "approval_ref": approval_ref, "policy_revision": policy_revision,
         "approved_at": func.now(), "state_changed_by": actor, "updated_at": func.now()},
    ) == 1


def expire_if_due(session, mandate_id, now=None) -> bool:
    """Lazy expiry: an ``active`` or ``suspended`` mandate whose ``valid_until`` is now or past becomes
    ``expired`` (a system change: ``state_changed_by`` is cleared). True when this call changed it."""
    return _apply(
        session,
        [AutonomousMandate.id == mandate_id, AutonomousMandate.state.in_(("active", "suspended")),
         AutonomousMandate.valid_until <= (now or _now())],
        {"state": "expired", "suspended_reason": None, "revocation_epoch": AutonomousMandate.revocation_epoch + 1,
         "state_changed_by": None, "updated_at": func.now()},
    ) == 1


def suspend_owner_mandates(session, owner_user_id) -> list:
    """Suspend every active mandate of an owner who is no longer active; the ids moved. The caller revokes
    the contexts of each and records the events."""
    return list(session.execute(
        update(AutonomousMandate)
        .where(AutonomousMandate.owner_user_id == owner_user_id, AutonomousMandate.state == "active")
        .values(state="suspended", suspended_reason="OWNER_SUSPENDED",
                revocation_epoch=AutonomousMandate.revocation_epoch + 1, state_changed_by=None,
                updated_at=func.now())
        .returning(AutonomousMandate.id).execution_options(synchronize_session=False)
    ).scalars())


def next_revision(session, project_id, agent_version) -> int:
    """``1 + max(revision)`` over every mandate ever made for this agent version."""
    return session.execute(
        select(func.coalesce(func.max(AutonomousMandate.revision), 0) + 1)
        .where(AutonomousMandate.project_id == project_id, AutonomousMandate.agent_version == agent_version)
    ).scalar_one()
