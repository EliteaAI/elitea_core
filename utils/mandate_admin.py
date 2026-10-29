"""Operator actions on autonomous mandates (#6947, C-1009): the admin task behind ``manage_autonomous_mandate``.

A mandate lets one agent version run on schedules and webhooks as its owner's autonomous agent. It is created as
a draft against the version's active identity binding, and becomes active only when someone who is neither its
creator nor its owner approves it. Works with governance off, so mandates can be prepared before the flag is
switched on. Every change is an audit event in the same transaction; leaving ``active`` also revokes the live
contexts of the mandate's runs. Results and messages carry ids and states only, never a credential.
"""
import re
import uuid
from datetime import datetime, timedelta, timezone

from pylon.core.tools import log
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from tools import auth, db, this

from ..models.agent_identity import LIVE_MANDATE_STATES, AgentIdentityBinding, AutonomousMandate
from .admission import revoke_mandate_contexts, version_application_id
from .binding_state import InvalidTransition
from .definition_digest import DigestError, digest_for_version
from .mandate_identity import IDENTITY_SOURCES, allowed_sources, retire_consent
from .mandate_state import (
    BUMPS_EPOCH, MAX_MANDATE_VALIDITY_DAYS, approve_mandate, expire_if_due, next_revision, transition_mandate,
)
from .trigger_principal import OPERATION_OF

ACTIONS = ("create", "submit", "reject", "approve", "suspend", "resume", "revoke")
USAGE = (
    "Format: project_id=N;action=<create|submit|reject|approve|suspend|resume|revoke>;"
    "version_id=V;owner_user_id=U;operations=run:schedule[,run:webhook];max_runtime_s=60..86400;"
    "valid_until=<ISO-8601 with offset> (create)[;targets=*][;identity=owner_delegated|application]"
    "[;mandate=M] (all other actions)[;approval_ref=...;policy_revision=...] (approve)[;dry_run]"
)
INT_KEYS = ("project_id", "version_id", "owner_user_id", "max_runtime_s")
TEXT_KEYS = ("action", "operations", "targets", "valid_until", "mandate", "approval_ref", "policy_revision", "identity")
# What each action takes (project_id and action always); `targets` is optional and must be `*`
ACTION_KEYS = {
    "create": ("version_id", "owner_user_id", "operations", "max_runtime_s", "valid_until", "targets", "identity"),
    "approve": ("mandate", "approval_ref", "policy_revision"),
}
NEEDED = {"create": ("version_id", "owner_user_id", "operations", "max_runtime_s", "valid_until"),
          "approve": ("mandate", "approval_ref", "policy_revision")}
OPERATIONS = tuple(sorted(set(OPERATION_OF.values())))
ANY_TARGET = "*"
RUNTIME_RANGE_S = (60, 86400)
MAX_INT = 2 ** 31 - 1
REFERENCE_KEYS = ("approval_ref", "policy_revision")
REFERENCE_PATTERN = re.compile(r"^[A-Za-z0-9._:/@-]{1,128}$")
# Moves that are not creation or approval: action -> (states it starts from, state it ends in)
MOVES = {
    "submit": (("draft",), "pending_approval"),
    "reject": (("pending_approval",), "draft"),
    "suspend": (("active",), "suspended"),
    "resume": (("suspended",), "active"),
    "revoke": (LIVE_MANDATE_STATES, "revoked"),
}
REASON_OF = {"reject": "MANDATE_REJECTED"}
MESSAGES = {
    "ACTOR_REQUIRED": "A signed-in operator is required to change a mandate.",
    "VERSION_NOT_FOUND": "The agent version does not exist in this project.",
    "DENIED_BINDING": "The agent version has no single active identity binding.",
    "BINDING_DIGEST_MISMATCH": "The agent changed after its identity binding was approved.",
    "BINDING_NOT_ACTIVE": "The identity binding this mandate was made for is no longer active.",
    "OWNER_NOT_FOUND": "The owner does not exist.",
    "DEPENDENCY_UNAVAILABLE": "The identity service did not answer; try again.",
    "OWNER_NOT_LINKED": "The owner has no linked identity-provider subject.",
    "OWNER_INACTIVE": "The owner is not active.",
    "MANDATE_EXISTS": "A live mandate already exists for this agent version.",
    "MANDATE_NOT_FOUND": "No such mandate in this project.",
    "INVALID_STATE": "The mandate is not in a state that allows this action.",
    "SELF_APPROVAL": "The creator or the owner of a mandate cannot approve or resume it.",
    "MANDATE_EXPIRED": "The mandate is past its validity.",
    "CONCURRENT_CHANGE": "The mandate changed while this ran; read it again and retry.",
    "IDENTITY_SOURCE_NOT_ALLOWED": "This platform does not allow mandates to name this identity "
                                   "(mandate_identity_sources).",
}


class MandateTaskError(Exception):
    """A refused action. ``result()`` is what the admin task returns."""

    def __init__(self, code):
        self.code = code
        super().__init__(MESSAGES[code])

    def result(self):
        return {"error": f"{self.code}: {MESSAGES[self.code]}", "code": self.code}


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _aware(moment: datetime) -> datetime:
    return moment if moment.tzinfo is not None else moment.replace(tzinfo=timezone.utc)


def _parse_valid_until(text):
    try:
        until = datetime.fromisoformat(text)
        if until.tzinfo is None or until.utcoffset() is None:
            return None, f"invalid valid_until: '{text}' (needs an offset)"
        until = until.astimezone(timezone.utc)
    except (ValueError, OverflowError):
        return None, f"invalid valid_until: '{text}'"
    now = _now()
    if until <= now:
        return None, f"invalid valid_until: '{text}' (must be in the future)"
    if until > now + timedelta(days=MAX_MANDATE_VALIDITY_DAYS):
        return None, f"invalid valid_until: '{text}' (at most {MAX_MANDATE_VALIDITY_DAYS} days ahead)"
    return until, None


def parse_mandate_param(param):  # pylint: disable=R0911,R0912
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
            if not 0 < values[key] <= MAX_INT:
                return None, f"invalid {key}: '{value}'"
        else:
            values[key] = value
    action = values.get("action")
    if "project_id" not in values or not action:
        return None, f"{', '.join(k for k in ('project_id', 'action') if not values.get(k))} required. {USAGE}"
    if action not in ACTIONS:
        return None, f"invalid action: '{action}'"
    allowed = ACTION_KEYS.get(action, ("mandate",))
    stray = [key for key in INT_KEYS + TEXT_KEYS if key in values and key not in allowed + ("project_id", "action")]
    if stray:
        return None, f"{', '.join(stray)} does not apply to {action}. {USAGE}"
    missing = [key for key in NEEDED.get(action, ("mandate",)) if not values.get(key)]
    if missing:
        return None, f"{', '.join(missing)} required. {USAGE}"
    for key in REFERENCE_KEYS:
        if key in values and not REFERENCE_PATTERN.match(values[key]):
            return None, f"invalid {key}: 1 to 128 characters of letters, digits and . _ : / @ - are allowed"
    if "mandate" in values:
        try:
            values["mandate"] = uuid.UUID(values["mandate"])
        except ValueError:
            return None, f"invalid mandate: '{values['mandate']}'"
    if action != "create":
        return values, None
    operations = list(dict.fromkeys(op.strip() for op in values["operations"].split(",")))
    unknown = [op for op in operations if op not in OPERATIONS]
    if unknown:
        return None, f"invalid operations: '{values['operations']}' (allowed: {', '.join(OPERATIONS)})"
    values["operations"] = operations
    if values.get("identity") is not None and values["identity"] not in IDENTITY_SOURCES:
        return None, f"invalid identity: '{values['identity']}' (allowed: {', '.join(IDENTITY_SOURCES)})"
    if values.get("targets", ANY_TARGET) != ANY_TARGET:
        return None, f"invalid targets: '{values['targets']}' (only '{ANY_TARGET}' in this release)"
    low, high = RUNTIME_RANGE_S
    if not low <= values["max_runtime_s"] <= high:
        return None, f"invalid max_runtime_s: '{values['max_runtime_s']}' (between {low} and {high})"
    values["valid_until"], error = _parse_valid_until(values["valid_until"])
    if error:
        return None, error
    return values, None


def _event(event_type, reason, mandate_row, actor, **fields):
    event = {
        "event_type": event_type, "reason_code": reason, "outcome": "success",
        "project_id": mandate_row.project_id, "application_id": mandate_row.application_id,
        "agent_binding_ref": str(mandate_row.agent_binding_ref), "mandate_ref": str(mandate_row.id),
        "caller_ref": f"operator:{actor}", **fields,
    }
    return {key: value for key, value in event.items() if value is not None}


def _active_binding(session, project_id, application_id, version_id, allow_runtime_domain_binding):
    modes = ["dedicated_agent", "elitea_native"] + (["runtime_domain_binding"] if allow_runtime_domain_binding else [])
    rows = session.execute(
        select(AgentIdentityBinding.id, AgentIdentityBinding.definition_digest).where(
            AgentIdentityBinding.project_id == project_id, AgentIdentityBinding.application_id == application_id,
            AgentIdentityBinding.version_id == version_id, AgentIdentityBinding.state == "active",
            AgentIdentityBinding.identity_mode.in_(modes))
    ).all()
    if len(rows) != 1:
        raise MandateTaskError("DENIED_BINDING")
    if digest_for_version(session, version_id) != rows[0].definition_digest:
        raise MandateTaskError("BINDING_DIGEST_MISMATCH")
    return rows[0].id


def _owner_ref(owner_user_id):
    try:
        user = auth.get_user(user_id=owner_user_id)
    except RuntimeError as exc:  # the identity service answers "No such user found" as a RuntimeError
        raise MandateTaskError(
            "OWNER_NOT_FOUND" if str(exc).startswith("No such user found") else "DEPENDENCY_UNAVAILABLE") from None
    except Exception:  # pylint: disable=W0703
        raise MandateTaskError("DEPENDENCY_UNAVAILABLE") from None
    user = user if isinstance(user, dict) else {}
    issuer, tid, oid = user.get("idp_issuer"), user.get("idp_tid"), user.get("idp_oid")
    if not (issuer and tid and oid):
        raise MandateTaskError("OWNER_NOT_LINKED")
    return {"user_id": owner_user_id, "issuer": issuer, "tid": tid, "oid": oid}


def _live_mandate(session, project_id, version_id):
    """The one live mandate of the version (the unique index allows one), or None."""
    return session.execute(select(AutonomousMandate).where(
        AutonomousMandate.project_id == project_id, AutonomousMandate.agent_version == version_id,
        AutonomousMandate.state.in_(LIVE_MANDATE_STATES))).scalar_one_or_none()


def _overdue(mandate, now):
    return mandate.state in ("active", "suspended") and _aware(mandate.valid_until) <= now


def _expire_overdue(session, tracing, mandate, actor, now):
    """Lazy expiry, decided by the clock: an active or suspended mandate past its validity becomes ``expired``,
    its live contexts are revoked and the event is recorded, all in the caller's transaction (not committed here).
    True when this call expired it."""
    if not _overdue(mandate, now):
        return False
    mandate_id = mandate.id
    if not expire_if_due(session, mandate_id, now=now):
        return False
    session.expire(mandate)  # the UPDATE bypassed the instance
    revoke_mandate_contexts(session, mandate_id, "MANDATE_EXPIRED")
    tracing.security_audit_record(session, _event(
        "lifecycle", "MANDATE_EXPIRED", mandate, actor, revocation_epoch=mandate.revocation_epoch))
    return True


def _create(module, session, tracing, values, actor):
    project_id, version_id = values["project_id"], values["version_id"]
    application_id = version_application_id(session, version_id)
    if application_id is None:
        raise MandateTaskError("VERSION_NOT_FOUND")
    settings = getattr(module, "admission_settings", None)
    binding_ref = _active_binding(
        session, project_id, application_id, version_id, bool(settings and settings.allow_runtime_domain_binding))
    identity = values.get("identity")
    if identity is not None and identity not in allowed_sources(module):
        raise MandateTaskError("IDENTITY_SOURCE_NOT_ALLOWED")
    owner = _owner_ref(values["owner_user_id"])
    live, now, expired = _live_mandate(session, project_id, version_id), _now(), None
    if live is not None:
        # An overdue active or suspended mandate does not hold the version's slot: renewal must be possible
        if not _overdue(live, now):
            raise MandateTaskError("MANDATE_EXISTS")
        if not values["dry_run"] and _expire_overdue(session, tracing, live, actor, now):
            expired = live.id
    revision = next_revision(session, project_id, version_id)
    if values["dry_run"]:
        return {"dry_run": True, "action": "create", "state": "draft", "revision": revision}
    named = {"identity_source": identity} if identity is not None else {}   # no identity: the row as before
    mandate = AutonomousMandate(
        project_id=project_id, application_id=application_id, agent_version=version_id, agent_binding_ref=binding_ref,
        owner_user_id=values["owner_user_id"], owner_user_ref=owner, allowed_operations=values["operations"],
        allowed_targets=[ANY_TARGET], max_runtime_s=values["max_runtime_s"], valid_until=values["valid_until"],
        state="draft", created_by=actor, state_changed_by=actor, revision=revision, **named,
    )
    session.add(mandate)
    try:
        session.flush()
    except IntegrityError:
        session.rollback()
        raise MandateTaskError("CONCURRENT_CHANGE") from None
    mandate_ref = str(mandate.id)
    tracing.security_audit_record(session, _event("lifecycle", "MANDATE_DRAFT", mandate, actor, revocation_epoch=0))
    session.commit()
    if expired is not None:   # the expired mandate's consent goes with it (#7131)
        retire_consent(project_id, expired, "MANDATE_IDENTITY_WITHDRAWN", f"operator:{actor}", actor)
    result = {"action": "create", "mandate_ref": mandate_ref, "revision": revision, "state": "draft", "dry_run": False}
    return {**result, "identity": identity} if identity is not None else result


def _read(session, values):
    mandate = session.execute(select(AutonomousMandate).where(
        AutonomousMandate.id == values["mandate"], AutonomousMandate.project_id == values["project_id"]
    )).scalar_one_or_none()
    if mandate is None:
        raise MandateTaskError("MANDATE_NOT_FOUND")
    return mandate


def _guard_change(session, mandate, action, actor):
    """The refusals that need more than the row's state; the conditional UPDATE repeats the ones that race."""
    if action == "approve" and actor in (mandate.created_by, mandate.owner_user_id):
        raise MandateTaskError("SELF_APPROVAL")
    if action == "resume" and actor == mandate.owner_user_id:
        raise MandateTaskError("SELF_APPROVAL")
    if action not in ("approve", "resume"):
        return
    state = session.execute(
        select(AgentIdentityBinding.state).where(AgentIdentityBinding.id == mandate.agent_binding_ref)
    ).scalar_one_or_none()
    if state != "active":
        raise MandateTaskError("BINDING_NOT_ACTIVE")
    if action == "resume" and mandate.suspended_reason == "OWNER_SUSPENDED":
        try:
            verdict = auth.read_identity_epochs("user", mandate.owner_user_id)
        except Exception:  # pylint: disable=W0703
            raise MandateTaskError("DEPENDENCY_UNAVAILABLE") from None
        if not isinstance(verdict, dict) or verdict.get("active") is not True:
            raise MandateTaskError("OWNER_INACTIVE")


def _change(module, session, tracing, values, actor):
    action = values["action"]
    mandate = _read(session, values)
    now = _now()
    if action in ("submit", "approve", "resume") and mandate.state in LIVE_MANDATE_STATES \
            and _aware(mandate.valid_until) <= now:
        # Past its validity: refused, not acted on. An active or suspended one is expired on the spot.
        if not values["dry_run"] and _expire_overdue(session, tracing, mandate, actor, now):
            session.commit()
            retire_consent(values["project_id"], values["mandate"], "MANDATE_IDENTITY_WITHDRAWN", f"operator:{actor}",
                           actor)
        raise MandateTaskError("MANDATE_EXPIRED")
    starts, target = ("pending_approval",), "active"
    if action != "approve":
        starts, target = MOVES[action]
    if mandate.state not in starts:
        raise MandateTaskError("INVALID_STATE")
    _guard_change(session, mandate, action, actor)
    source, mandate_id = mandate.state, mandate.id
    if values["dry_run"]:
        return {"dry_run": True, "action": action, "mandate_ref": str(mandate_id), "from": source, "to": target}
    if action == "approve":
        changed = approve_mandate(session, mandate_id, actor=actor, approval_ref=values["approval_ref"],
                                  policy_revision=values["policy_revision"], now=now)
    else:
        changed = transition_mandate(session, mandate_id, source, target, actor=actor, now=now)
    if not changed:
        session.rollback()
        raise MandateTaskError("CONCURRENT_CHANGE")
    session.expire(mandate)  # the UPDATE bypassed the instance
    if target in BUMPS_EPOCH:
        revoke_mandate_contexts(session, mandate_id)
    reason = REASON_OF.get(action, f"MANDATE_{target.upper()}")
    extra = {}
    if action == "approve":
        extra = {"approver_ref": values["approval_ref"], "policy_revision": values["policy_revision"]}
    tracing.security_audit_record(session, _event(
        "approval" if action == "approve" else "lifecycle", reason, mandate, actor,
        revocation_epoch=mandate.revocation_epoch, **extra))
    session.commit()
    if target == "revoked":   # the owner's consent goes with the mandate (#7131), whatever the switch
        retire_consent(values["project_id"], mandate_id, "MANDATE_IDENTITY_WITHDRAWN", f"operator:{actor}", actor)
    return {"action": action, "mandate_ref": str(mandate_id), "state": target, "dry_run": False}


def manage_mandate(module, param, actor_user_id=0):
    """Run one mandate action. Returns a result dict, or ``{"error": ..., "code": ...}`` with nothing written."""
    try:
        values, error = parse_mandate_param(param)
    except Exception:  # pylint: disable=W0703
        values, error = None, f"invalid parameter. {USAGE}"
    if error:
        log.error("manage_autonomous_mandate: %s", error)
        return {"error": error}
    try:
        if not values["dry_run"] and (
                isinstance(actor_user_id, bool) or not isinstance(actor_user_id, int) or actor_user_id <= 0):
            raise MandateTaskError("ACTOR_REQUIRED")
        tracing = this.for_module("tracing").module
        with db.get_session(values["project_id"]) as session:
            try:
                if values["action"] == "create":
                    return _create(module, session, tracing, values, actor_user_id)
                return _change(module, session, tracing, values, actor_user_id)
            except Exception:
                session.rollback()
                raise
    except MandateTaskError as exc:
        return exc.result()
    except InvalidTransition as exc:
        return {"error": f"{exc.reason_code}: {exc}", "code": exc.reason_code}
    except DigestError as exc:
        return {"error": f"{exc.reason_code}: the agent definition could not be read", "code": exc.reason_code}
    except Exception as exc:  # pylint: disable=W0703
        log.error("manage_autonomous_mandate: failed (%s)", type(exc).__name__)
        return {"error": "failed; nothing was changed (is the audit trail available?)", "code": "FAILED"}
