"""Agent identity binding lifecycle (#6756, C-1009): which state changes are legal, and the one
statement that applies a change without losing a race.

A binding becomes ``active`` only for a definition that was approved. Anything that takes it
out of use (suspend, retire) raises ``revocation_epoch``, so work admitted earlier can be told
apart from work admitted after the change.
"""
from sqlalchemy import func, select, update

from ..models.agent_identity import AgentIdentityBinding

TRANSITIONS = frozenset({
    ("draft", "provisioning"), ("provisioning", "pending_approval"), ("pending_approval", "active"),
    ("pending_approval", "draft"), ("active", "suspended"), ("suspended", "active"),
    ("active", "retiring"), ("suspended", "retiring"), ("retiring", "retired"),
    ("draft", "retired"), ("provisioning", "retired"), ("pending_approval", "retired"),
    # rebind (#7105): a live binding is approved again for the definition as it is now
    ("active", "pending_approval"), ("suspended", "pending_approval"),
})
BUMPS_EPOCH = frozenset({"suspended", "retiring", "retired"})
REBIND_FROM = frozenset({"active", "suspended"})


def is_rebind(expected: str, target: str) -> bool:
    return target == "pending_approval" and expected in REBIND_FROM


def bumps_epoch(expected: str, target: str) -> bool:
    """Entering a state that takes the binding out of use, or taking it back to approval (a rebind)."""
    return target in BUMPS_EPOCH or is_rebind(expected, target)


class InvalidTransition(ValueError):
    """The change is refused before touching the row. ``reason_code`` is stable, the message is for logs."""

    def __init__(self, reason_code: str, message: str):
        super().__init__(message)
        self.reason_code = reason_code


def transition_binding(  # pylint: disable=R0913
    session, binding_id, expected: str, target: str, *, actor_user_id: int,
    approval_ref=None, policy_revision=None, current_digest=None, allow_runtime_domain_binding=False,
) -> bool:
    """Move a binding from ``expected`` to ``target``. True when this call made the change.

    False means the row is not in ``expected`` any more (another writer won) or does not exist;
    the caller re-reads. Raises ``InvalidTransition`` for a change that is never allowed.
    Does not commit: the caller owns the transaction, so it can include related writes.

    ``current_digest`` is the definition the activation is checked against; for a rebind it is the definition the
    binding is approved for from now on. Approving (``pending_approval`` -> ``active``) is refused for the operator who
    created the binding or who last changed its state (``SELF_APPROVAL``); resuming a suspended binding is not an
    approval and has no such rule.
    """
    if (expected, target) not in TRANSITIONS:
        raise InvalidTransition("INVALID_TRANSITION", f"{expected} -> {target} is not a legal binding change")
    rebind = is_rebind(expected, target)
    approving = expected == "pending_approval" and target == "active"
    if rebind and not current_digest:
        raise InvalidTransition("DIGEST_REQUIRED", "a rebind needs the current definition digest")
    guards = []
    if target == "active":
        if not approval_ref or not policy_revision:
            raise InvalidTransition("APPROVAL_REQUIRED", "activation needs an approval and a policy revision")
        row = session.execute(
            select(AgentIdentityBinding.identity_mode, AgentIdentityBinding.definition_digest,
                   AgentIdentityBinding.created_by, AgentIdentityBinding.state_changed_by)
            .where(AgentIdentityBinding.id == binding_id)
        ).one_or_none()
        if row is None:
            return False
        if approving:
            if not actor_user_id:
                raise InvalidTransition("SELF_APPROVAL", "approving needs an identified operator")
            if actor_user_id in (row.created_by, row.state_changed_by):
                raise InvalidTransition("SELF_APPROVAL", "another operator must approve this binding")
            # the same rule once more in the statement, so a change between the read and the write cannot slip by
            guards = [AgentIdentityBinding.created_by.is_distinct_from(actor_user_id),
                      AgentIdentityBinding.state_changed_by.is_distinct_from(actor_user_id)]
        if current_digest != row.definition_digest:
            raise InvalidTransition("BINDING_DIGEST_MISMATCH", "the definition changed since the binding was created")
        if row.identity_mode == "runtime_domain_binding" and not allow_runtime_domain_binding:
            raise InvalidTransition("DENIED_BINDING", "runtime domain bindings cannot be activated here")

    values = {"state": target, "state_changed_by": actor_user_id, "updated_at": func.now()}
    if approval_ref is not None:
        values["approval_ref"] = approval_ref
    if policy_revision is not None:
        values["policy_revision"] = policy_revision
    if rebind:   # approved again from scratch, for the definition as it is now
        values.update(definition_digest=current_digest, approval_ref=None, policy_revision=None)
    if bumps_epoch(expected, target):
        values["revocation_epoch"] = AgentIdentityBinding.revocation_epoch + 1
    result = session.execute(
        update(AgentIdentityBinding)
        .where(AgentIdentityBinding.id == binding_id, AgentIdentityBinding.state == expected, *guards)
        .values(**values)
        .execution_options(synchronize_session=False)
    )
    return result.rowcount == 1
