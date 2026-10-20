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
})
BUMPS_EPOCH = frozenset({"suspended", "retiring", "retired"})


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
    """
    if (expected, target) not in TRANSITIONS:
        raise InvalidTransition("INVALID_TRANSITION", f"{expected} -> {target} is not a legal binding change")
    if target == "active":
        if not approval_ref or not policy_revision:
            raise InvalidTransition("APPROVAL_REQUIRED", "activation needs an approval and a policy revision")
        row = session.execute(
            select(AgentIdentityBinding.identity_mode, AgentIdentityBinding.definition_digest)
            .where(AgentIdentityBinding.id == binding_id)
        ).one_or_none()
        if row is None:
            return False
        if current_digest != row.definition_digest:
            raise InvalidTransition("BINDING_DIGEST_MISMATCH", "the definition changed since the binding was created")
        if row.identity_mode == "runtime_domain_binding" and not allow_runtime_domain_binding:
            raise InvalidTransition("DENIED_BINDING", "runtime domain bindings cannot be activated here")

    values = {"state": target, "state_changed_by": actor_user_id, "updated_at": func.now()}
    if approval_ref is not None:
        values["approval_ref"] = approval_ref
    if policy_revision is not None:
        values["policy_revision"] = policy_revision
    if target in BUMPS_EPOCH:
        values["revocation_epoch"] = AgentIdentityBinding.revocation_epoch + 1
    result = session.execute(
        update(AgentIdentityBinding)
        .where(AgentIdentityBinding.id == binding_id, AgentIdentityBinding.state == expected)
        .values(**values)
        .execution_options(synchronize_session=False)
    )
    return result.rowcount == 1
