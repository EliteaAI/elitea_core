"""Operator actions on agent identity bindings (#6756, C-1009): the admin task behind
``manage_agent_identity_binding``.

Works with governance off, so bindings can be prepared before the flag is switched on. Every state
change is recorded as a ``lifecycle`` audit event in the same transaction as the change; suspending
or retiring also revokes the live contexts of the binding that sit in the same project schema
(contexts in other schemas see the raised epoch when the worker validates them).
"""
from pylon.core.tools import log
from sqlalchemy import select
from tools import db, this

from ..models.agent_identity import IDENTITY_MODES, AgentIdentityBinding
from .admission import revoke_binding_contexts, version_application_id
from .binding_state import BUMPS_EPOCH, InvalidTransition, transition_binding
from .definition_digest import DigestError, digest_for_version

ACTIONS = ("create", "submit", "activate", "suspend", "resume", "retire")
USAGE = (
    "Format: project_id=N;application_id=A;version_id=V;mode=<dedicated_agent|runtime_domain_binding|elitea_native>;"
    "action=<create|submit|activate|suspend|resume|retire>[;approval_ref=...][;policy_revision=...][;dry_run]"
)
INT_KEYS = ("project_id", "application_id", "version_id")
TEXT_KEYS = ("mode", "action", "approval_ref", "policy_revision")
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
}


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
    return values, None


def _lifecycle_event(values, binding, target, actor_user_id, epoch):
    event = {
        "event_type": "lifecycle", "reason_code": f"BINDING_{target.upper()}", "outcome": "success",
        "project_id": values["project_id"], "application_id": values["application_id"],
        "agent_binding_ref": str(binding.id), "revocation_epoch": epoch,
        "caller_ref": f"operator:{actor_user_id}" if actor_user_id else "operator",
    }
    if target == "active":
        event["approver_ref"] = values.get("approval_ref") or binding.approval_ref
        event["policy_revision"] = values.get("policy_revision") or binding.policy_revision
    return {key: value for key, value in event.items() if value is not None}


def _create(session, tracing, values, actor_user_id):
    if session.execute(_find(values)).scalar_one_or_none() is not None:
        return {"error": "binding already exists; use another action"}
    digest = digest_for_version(session, values["version_id"])
    if values["dry_run"]:
        return {"dry_run": True, "action": "create", "state": "draft", "definition_digest": digest}
    binding = AgentIdentityBinding(
        project_id=values["project_id"], application_id=values["application_id"],
        version_id=values["version_id"], identity_mode=values["mode"],
        definition_digest=digest, state="draft", created_by=actor_user_id or None,
    )
    session.add(binding)
    session.flush()
    binding_ref = str(binding.id)
    tracing.security_audit_record(session, _lifecycle_event(values, binding, "draft", actor_user_id, 0))
    session.commit()
    return {"action": "create", "binding_ref": binding_ref, "state": "draft",
            "definition_digest": digest, "dry_run": False}


def _find(values):
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
        return {"error": "no such binding; create it first"}
    binding_id = binding.id
    path = PATHS[action].get(binding.state)
    if path is None:
        return {"error": f"cannot {action} a binding in state '{binding.state}'"}
    if values["dry_run"]:
        return {"dry_run": True, "action": action, "binding_ref": str(binding_id),
                "from": binding.state, "to": list(path)}
    current_digest = digest_for_version(session, values["version_id"]) if path[-1] == "active" else None
    settings = getattr(module, "admission_settings", None)
    state = binding.state
    for target in path:
        if not transition_binding(
            session, binding_id, state, target, actor_user_id=actor_user_id,
            approval_ref=values.get("approval_ref") or (binding.approval_ref if target == "active" else None),
            policy_revision=values.get("policy_revision") or (binding.policy_revision if target == "active" else None),
            current_digest=current_digest,
            allow_runtime_domain_binding=bool(settings and settings.allow_runtime_domain_binding),
        ):
            session.rollback()
            return {"error": "the binding changed while this ran; read it again and retry"}
        session.expire(binding)  # the UPDATE bypassed the instance
        if target in BUMPS_EPOCH:
            revoke_binding_contexts(session, binding_id)
        tracing.security_audit_record(
            session, _lifecycle_event(values, binding, target, actor_user_id, binding.revocation_epoch))
        state = target
    session.commit()
    return {"action": action, "binding_ref": str(binding_id), "state": state, "dry_run": False}


def manage_binding(module, param, actor_user_id=0):
    """Run one binding action. Returns a result dict, or ``{"error": ...}`` with nothing written."""
    values, error = parse_binding_param(param)
    if error:
        log.error("manage_agent_identity_binding: %s", error)
        return {"error": error}
    try:
        tracing = this.for_module("tracing").module
        with db.get_session(values["project_id"]) as session:
            try:
                if version_application_id(session, values["version_id"]) != values["application_id"]:
                    return {"error": "the version does not exist in this application"}
                if values["action"] == "create":
                    return _create(session, tracing, values, actor_user_id)
                return _transition(module, session, tracing, values, actor_user_id)
            except Exception:
                session.rollback()
                raise
    except (InvalidTransition, DigestError) as exc:
        return {"error": f"{exc.reason_code}: {exc}"}
    except Exception as exc:  # pylint: disable=W0703
        log.error("manage_agent_identity_binding: failed (%s)", type(exc).__name__)
        return {"error": "failed; nothing was changed (is the audit trail available?)"}
