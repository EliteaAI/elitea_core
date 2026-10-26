"""Operator actions on agent identity bindings (#6756, C-1009): the admin task behind
``manage_agent_identity_binding``.

Works with governance off, so bindings can be prepared before the flag is switched on. Every state
change is recorded as a ``lifecycle`` audit event in the same transaction as the change; suspending
or retiring also revokes the live contexts of the binding that sit in the same project schema
(contexts in other schemas see the raised epoch when the worker validates them).

``run_binding_action`` is the one implementation of the lifecycle: the admin task parses its parameter into
values and calls it, and so does the operator REST API (#7105). It raises ``BindingActionError`` with a stable
``code``; ``manage_binding`` renders that as the task's readable text.
"""
import re

from pylon.core.tools import log
from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError
from tools import db, this

from ..models.agent_identity import IDENTITY_MODES, REFERENCE_PATTERN, AgentIdentityBinding
from .admission import revoke_binding_contexts, version_application_id
from .binding_state import InvalidTransition, bumps_epoch, transition_binding
from .definition_digest import DigestError, digest_for_version
from .internal_obo import OPERATIONS as PLATFORM_OPERATIONS

ACTIONS = ("create", "submit", "activate", "suspend", "resume", "retire", "rebind")
USAGE = (
    "Format: project_id=N;application_id=A;version_id=V;mode=<dedicated_agent|runtime_domain_binding|elitea_native>;"
    "action=<create|submit|activate|suspend|resume|retire|rebind>[;approval_ref=...][;policy_revision=...]"
    "[;platform_operations=read,build,credentials,run][;dry_run]"
)
INT_KEYS = ("project_id", "application_id", "version_id")
TEXT_KEYS = ("mode", "action", "approval_ref", "policy_revision", "platform_operations")
# The actions that may set what the agent may do on the platform through the internal MCP (#7214): it is part of what
# is approved, so it is set on a new binding or sent back to approval with ``rebind``
OPERATIONS_ACTIONS = ("create", "rebind")
# The states each action walks through, by the state it starts from
PATHS = {
    "submit": {"draft": ("provisioning", "pending_approval")},
    "activate": {"pending_approval": ("active",)},
    "resume": {"suspended": ("active",)},
    "suspend": {"active": ("suspended",)},
    "retire": {
        "active": ("retiring", "retired"), "suspended": ("retiring", "retired"), "retiring": ("retired",),
        "draft": ("retired",), "provisioning": ("retired",), "pending_approval": ("retired",),
    },
    # back to approval for the definition as it is now: the digest is recomputed, the approval cleared, the epoch raised
    "rebind": {"active": ("pending_approval",), "suspended": ("pending_approval",)},
}
# Fixed readable text for the codes the lifecycle raises with a message that names an id
FIXED_MESSAGES = {"VERSION_NOT_FOUND": "the version does not exist in this application"}
# The actions that read the definition (a digest) or admit it again: the version must still be in the application.
# suspend, retire and submit work on the row alone, so a binding whose version was deleted can still be taken out of use.
NEEDS_VERSION = ("create", "activate", "resume", "rebind")


class BindingActionError(Exception):
    """A binding action that was refused or failed with nothing written. ``code`` is stable; ``message`` is fixed
    text without ids; ``task_text`` is what the admin task has always printed for it."""

    def __init__(self, code, message, task_text=None):
        super().__init__(message)
        self.code, self.message = code, message
        self.task_text = task_text or message


def parse_binding_param(param):
    """``(values, None)`` or ``(None, readable error)``."""
    values = {"dry_run": False}
    for seg in [s.strip() for s in (param or "").split(";")]:
        if not seg:
            continue
        if seg.lower() == "dry_run":
            values["dry_run"] = True
            continue
        key, sep, value = seg.partition("=")
        key, value = key.strip().lower(), value.strip()
        if not sep or key not in INT_KEYS + TEXT_KEYS:
            return None, f"unknown parameter '{seg}'. {USAGE}"
        if key in INT_KEYS:
            try:
                values[key] = int(value)
            except ValueError:
                return None, f"invalid {key}: '{value}'"
            if values[key] <= 0:
                return None, f"invalid {key}: '{value}'"
        else:
            values[key] = value
    missing = [key for key in INT_KEYS + ("mode", "action") if key not in values]
    if missing:
        return None, f"{', '.join(missing)} required. {USAGE}"
    if values["mode"] not in IDENTITY_MODES:
        return None, f"invalid mode: '{values['mode']}'"
    if values["action"] not in ACTIONS:
        return None, f"invalid action: '{values['action']}'"
    given = [key for key in ("approval_ref", "policy_revision") if key in values]
    if given and values["action"] != "activate":
        return None, "approval_ref and policy_revision belong to action=activate only"
    for key in given:
        if not re.fullmatch(REFERENCE_PATTERN, values[key]):
            return None, f"invalid {key}: use letters, digits and . _ : / @ - (at most 128 characters)"
    if "platform_operations" in values:
        operations, error = parse_platform_operations(values["platform_operations"], values["action"])
        if error:
            return None, error
        values["platform_operations"] = operations
    return values, None


def parse_platform_operations(raw, action):
    """``(sorted list, None)`` or ``(None, readable error)`` for a ``platform_operations`` value (#7214): a comma list,
    or a list, of ``read``/``build``/``credentials``/``run``, on a create or a rebind only. Empty means none."""
    if action not in OPERATIONS_ACTIONS:
        return None, "platform_operations belongs to action=create or action=rebind only"
    items = raw.split(",") if isinstance(raw, str) else raw
    if not isinstance(items, (list, tuple)):
        return None, "invalid platform_operations"
    operations = {str(item).strip() for item in items if str(item).strip()}
    unknown = sorted(operations - set(PLATFORM_OPERATIONS))
    if unknown:
        return None, f"invalid platform_operations: use {', '.join(PLATFORM_OPERATIONS)}"
    return [op for op in PLATFORM_OPERATIONS if op in operations], None


def _lifecycle_event(values, binding, target, actor_user_id, epoch):
    """The ``lifecycle`` audit event of one state change, with only the fields the event schema has today. The rebind
    event therefore does not carry the definition digests (new and previous); a ``definition_digest`` and
    ``previous_digest`` pair is to be added to the tracing event model as a follow-up (#7105)."""
    reason = "BINDING_REBIND_PENDING_APPROVAL" if values["action"] == "rebind" else f"BINDING_{target.upper()}"
    event = {
        "event_type": "lifecycle", "reason_code": reason, "outcome": "success",
        "project_id": values["project_id"], "application_id": values["application_id"],
        "agent_binding_ref": str(binding.id), "revocation_epoch": epoch,
        "caller_ref": f"operator:{actor_user_id}" if actor_user_id else "operator",
    }
    if target == "active":   # what the row holds now: the approver's own text for an activation, the stored one for a resume
        event["approver_ref"] = binding.approval_ref
        event["policy_revision"] = binding.policy_revision
    if values.get("platform_operations") is not None and target in ("draft", "pending_approval"):
        event["operation_id"] = "platform_operations:" + (",".join(values["platform_operations"]) or "none")
    return {key: value for key, value in event.items() if value is not None}


def _create(session, tracing, values, actor_user_id):
    if session.execute(_find(values)).scalar_one_or_none() is not None:
        raise BindingActionError("BINDING_EXISTS", "binding already exists; use another action")
    digest = digest_for_version(session, values["version_id"])
    if values["dry_run"]:
        return {"dry_run": True, "action": "create", "state": "draft", "definition_digest": digest}
    binding = AgentIdentityBinding(
        project_id=values["project_id"], application_id=values["application_id"],
        version_id=values["version_id"], identity_mode=values["mode"],
        definition_digest=digest, state="draft", created_by=actor_user_id or None,
        allowed_platform_operations=values.get("platform_operations"),
    )
    session.add(binding)
    session.flush()
    binding_ref = str(binding.id)
    tracing.security_audit_record(session, _lifecycle_event(values, binding, "draft", actor_user_id, 0))
    session.commit()
    return {"action": "create", "binding_ref": binding_ref, "state": "draft",
            "definition_digest": digest, "dry_run": False}


def _find(values):
    if values.get("binding_id") is not None:   # the REST API names the row; the project is still part of the match
        return select(AgentIdentityBinding).where(
            AgentIdentityBinding.project_id == values["project_id"], AgentIdentityBinding.id == values["binding_id"])
    return select(AgentIdentityBinding).where(
        AgentIdentityBinding.project_id == values["project_id"],
        AgentIdentityBinding.application_id == values["application_id"],
        AgentIdentityBinding.version_id == values["version_id"],
        AgentIdentityBinding.identity_mode == values["mode"],
    )


def _transition(module, session, tracing, values, actor_user_id):  # pylint: disable=R0914
    action = values["action"]
    binding = session.execute(_find(values)).scalar_one_or_none()
    if binding is None:
        raise BindingActionError("NOT_FOUND", "no such binding; create it first")
    binding_id = binding.id
    if values.get("expected_state") and binding.state != values["expected_state"]:
        raise BindingActionError("CONCURRENT_CHANGE", "the binding changed while this ran; read it again and retry")
    path = PATHS[action].get(binding.state)
    if path is None:
        raise BindingActionError("INVALID_STATE", f"cannot {action} a binding in state '{binding.state}'")
    if values["dry_run"]:
        return {"dry_run": True, "action": action, "binding_ref": str(binding_id),
                "from": binding.state, "to": list(path)}
    current_digest = digest_for_version(session, values["version_id"]) if path[-1] == "active" or action == "rebind" else None
    settings = getattr(module, "admission_settings", None)
    state = binding.state
    # Only an activation takes the request's approval, and it takes nothing else. A resume carries the stored approval
    # (it is checked, never rewritten); every other action carries none.
    approval_ref = values.get("approval_ref") if action == "activate" else (
        binding.approval_ref if action == "resume" else None)
    policy_revision = values.get("policy_revision") if action == "activate" else (
        binding.policy_revision if action == "resume" else None)
    for target in path:
        if not transition_binding(
            session, binding_id, state, target, actor_user_id=actor_user_id,
            approval_ref=approval_ref, policy_revision=policy_revision,
            current_digest=current_digest,
            allow_runtime_domain_binding=bool(settings and settings.allow_runtime_domain_binding),
        ):
            session.rollback()
            raise BindingActionError("CONCURRENT_CHANGE", "the binding changed while this ran; read it again and retry")
        session.expire(binding)  # the UPDATE bypassed the instance
        if bumps_epoch(state, target):
            revoke_binding_contexts(session, binding_id)
        tracing.security_audit_record(
            session, _lifecycle_event(values, binding, target, actor_user_id, binding.revocation_epoch))
        state = target
    if action == "rebind" and values.get("platform_operations") is not None:
        session.execute(update(AgentIdentityBinding).where(AgentIdentityBinding.id == binding_id)
                        .values(allowed_platform_operations=values["platform_operations"])
                        .execution_options(synchronize_session=False))
    session.commit()
    return {"action": action, "binding_ref": str(binding_id), "state": state, "dry_run": False}


def run_binding_action(module, values, actor_user_id=0):
    """Run one binding action for parsed ``values`` (what ``parse_binding_param`` returns; the REST API adds
    ``binding_id`` to name the row and ``expected_state`` to refuse a stale view). Returns the result dict, or raises
    ``BindingActionError`` with nothing written. Both the admin task and the API call this."""
    try:
        tracing = this.for_module("tracing").module
        with db.get_session(values["project_id"]) as session:
            try:
                if (values["action"] in NEEDS_VERSION
                        and version_application_id(session, values["version_id"]) != values["application_id"]):
                    raise BindingActionError("VERSION_NOT_FOUND", FIXED_MESSAGES["VERSION_NOT_FOUND"])
                if values["action"] == "create":
                    return _create(session, tracing, values, actor_user_id)
                return _transition(module, session, tracing, values, actor_user_id)
            except Exception:
                session.rollback()
                raise
    except BindingActionError:
        raise
    except (InvalidTransition, DigestError) as exc:
        raise BindingActionError(
            exc.reason_code, FIXED_MESSAGES.get(exc.reason_code, str(exc)), f"{exc.reason_code}: {exc}") from None
    except IntegrityError:   # two creates of the same binding raced past the pre-check; the unique key decided
        if values["action"] != "create":
            log.error("manage_agent_identity_binding: failed (IntegrityError)")
            raise BindingActionError(
                "AUDIT_UNAVAILABLE", "failed; nothing was changed (is the audit trail available?)") from None
        raise BindingActionError("BINDING_EXISTS", "binding already exists; use another action") from None
    except Exception as exc:  # pylint: disable=W0703
        log.error("manage_agent_identity_binding: failed (%s)", type(exc).__name__)
        raise BindingActionError(
            "AUDIT_UNAVAILABLE", "failed; nothing was changed (is the audit trail available?)") from None


def manage_binding(module, param, actor_user_id=0):
    """Run one binding action from the admin task's parameter. Returns a result dict, or ``{"error": ...}`` with
    nothing written."""
    values, error = parse_binding_param(param)
    if error:
        log.error("manage_agent_identity_binding: %s", error)
        return {"error": error}
    try:
        return run_binding_action(module, values, actor_user_id)
    except BindingActionError as exc:
        return {"error": exc.task_text}
