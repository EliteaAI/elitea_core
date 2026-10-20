"""Trusted release of credential material to a governed worker (#6946, WP-1D slice 1, C-1009).

A governed payload carries credential slots, never material (``credential_slots``). The worker that builds a toolkit
asks Main for the material of one slot over the worker bus (``restricted_release_credential``). The bus has no caller
authentication, so authority never comes from the caller: it comes from ids nobody can guess (``context_ref`` and the
``decision_id`` of the slot, or the single-use ``probe_ref`` ticket) checked against rows Main owns, and the answer is
encrypted to a key the requester generated for that one call.

Checks, in order, the first failure wins (a run; a probe has its ticket instead of 3a and 4, and no 6, 7):

  1. the request is well formed                                            STALE_CONTEXT
  2. the audit trail can take a decision                                   AUDIT_UNAVAILABLE
  3a. the context is governed, running, in time, and this worker's         STALE_CONTEXT / the revoke reason
  4. the context was admitted for exactly this toolkit, field, credential,  CREDENTIAL_REFERENCE_REQUIRED
     generation and decision
  5. at most ``MAX_RELEASES_PER_FIELD`` releases per context, toolkit and   RATE_LIMITED
     field (the children and reconcile runs of a fan-out share the parent's
     context, so the bound is per use, not per context)
  6/7. the live admission still stands: user and binding epochs, digest,    the codes of ``validate_context``
     mandate, and the credential decision of this use rechecked (WP-1B); a
     refusal here revokes the context, as ``validate_context`` does. Read-only
     otherwise: it does not move the context's state
  8. the store read, by the secrets plugin, which re-reads the reference    CREDENTIAL_REFERENCE_INACTIVE / STORE_*
  9. the release event is committed before the reply leaves                 AUDIT_UNAVAILABLE

Every release, allowed or denied, from check 3 on is audited (``credential_acquired``). A request that is not well formed,
one that arrives while the audit trail cannot take it, one for a context or ticket that does not exist, is not: nobody
admitted anything there, so it is counted (``credential_release_total{decision="rejected_request"}``) and logged at debug
level only. Nothing here logs, stores or returns a value except the sealed reply, and no exception text of a dependency is
repeated.
"""
import base64
import binascii
import importlib
import json
import re
import uuid
from datetime import timedelta
from typing import Optional

from pylon.core.tools import log
from sqlalchemy import select
from tools import auth, db, this

from ..models.agent_identity import AgentIdentityBinding, ExecutionSecurityContext
from . import admission as adm   # the live checks of validate_context, the audit and counter helpers
from .credential_decision import recheck_credentials
from .credential_slots import MAX_SLOT_FIELDS
from .exceptions import AdmissionDenied
from .governance import governance_lifecycle_active

# pylint: disable=W0212

MAX_RELEASES_PER_FIELD = 16
RELEASE_TTL_S = 300
KEY_BYTES = 32
PROBE_TASK = "indexer_governed_check_connection"
PROBE_TOOLKIT_INSTANCE_ID = "probe"
PROBE_FIELD = "configuration"
PROBE_KEY = "c1009:probe:"       # single-use ticket of a probe: metadata only, minted by credential_probe
COUNTER_KEY = "c1009:rel:"
REQUEST_KEYS = ("project_id", "context_ref", "probe_ref", "caller", "toolkit_instance_id", "field", "slot", "recipient_key")
AAD_VERSION = "v1"
RELEASED = "CREDENTIAL_RELEASED"
EVENT = "credential_acquired"

_NAME = re.compile(r"^[A-Za-z0-9_.:-]{1,128}$")
_PATH = re.compile(r"^[A-Za-z0-9_-]{1,64}(\.[A-Za-z0-9_-]{1,64}){0,7}$")
_SLOT_KEYS = frozenset({"v", "credential_ref", "credential_generation", "decision_id", "probe_ref", "fields"})

CREDENTIAL_RELEASE = adm._LabeledCounter(
    "credential_release_total", "Credential releases to governed workers, by decision and reason", ["decision", "reason_code"],
)


def _transport():
    """The sealed transport of the shared plugin (loaded on first use, never when governance is off)."""
    from tools import secret_stores  # pylint: disable=C0415
    return importlib.import_module(".transport", secret_stores.__name__)


def _uuid(value) -> Optional[str]:
    """The value when it is a uuid in its canonical spelling (lower case, dashes), else ``None``: the other spellings
    parse to the same uuid and would each get a counter of their own."""
    try:
        return value if isinstance(value, str) and str(uuid.UUID(value)) == value else None
    except ValueError:
        return None


class _Unaudited(AdmissionDenied):
    """A refusal of a request that no admitted run stands behind: counted, not audited."""


def _unaudited(code) -> _Unaudited:
    return _Unaudited(code, adm.MESSAGES.get(code, adm.GENERIC_MESSAGE))


def _int(value) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


class _Request:  # pylint: disable=R0902,R0903
    """A parsed release request; ``context_ref`` / ``probe_ref`` keep the exact text the worker sent (it is in the AAD)."""

    def __init__(self, project_id, context_ref, probe_ref, caller, toolkit_instance_id, field, slot, recipient_key):  # pylint: disable=R0913
        self.project_id, self.context_ref, self.probe_ref = project_id, context_ref, probe_ref
        self.caller, self.toolkit_instance_id, self.field = caller, toolkit_instance_id, field
        self.slot, self.recipient_key = slot, recipient_key

    @property
    def ref(self) -> str:
        return self.context_ref or self.probe_ref

    @property
    def task_name(self) -> str:
        return self.caller["task_name"]

    @property
    def decision_id(self) -> str:
        decision_id = self.slot.get("decision_id")
        return decision_id if isinstance(decision_id, str) else ""


def _parse(project_id, context_ref, probe_ref, caller, toolkit_instance_id, field, slot, recipient_key) -> _Request:  # pylint: disable=R0913
    """Check 1. Anything off is ``STALE_CONTEXT``: the request names nothing that could be released."""
    stale = _unaudited("STALE_CONTEXT")
    if not _int(project_id) or (context_ref is None) == (probe_ref is None):
        raise stale
    ref = context_ref if probe_ref is None else probe_ref
    if _uuid(ref) is None:
        raise stale
    if (
            not isinstance(caller, dict) or caller.get("kind") != "indexer_worker"
            or not isinstance(caller.get("task_name"), str) or not _NAME.match(caller["task_name"])
            or not (caller.get("run_id") is None or (isinstance(caller["run_id"], str) and _NAME.match(caller["run_id"])))
    ):
        raise stale
    if not (isinstance(toolkit_instance_id, str) and _NAME.match(toolkit_instance_id)
            and isinstance(field, str) and _NAME.match(field)):
        raise stale
    if not isinstance(slot, dict) or not set(slot) <= _SLOT_KEYS or slot.get("v") != 1:
        raise stale
    fields = slot.get("fields")
    if (
            _uuid(slot.get("credential_ref")) is None or not _int(slot.get("credential_generation"))
            or slot["credential_generation"] < 1
            or not isinstance(fields, list) or not 1 <= len(fields) <= MAX_SLOT_FIELDS or len(set(fields)) != len(fields)
            or not all(isinstance(path, str) and _PATH.match(path) for path in fields)
    ):
        raise stale
    if context_ref is not None:
        if _uuid(slot.get("decision_id")) is None or "probe_ref" in slot:
            raise stale
    elif slot.get("probe_ref") != probe_ref or slot.get("decision_id") is not None:
        raise stale
    if not isinstance(recipient_key, str):
        raise stale
    try:
        if len(base64.b64decode(recipient_key, validate=True)) != KEY_BYTES:
            raise stale
    except (ValueError, binascii.Error):
        raise stale from None
    return _Request(project_id, context_ref, probe_ref, caller, toolkit_instance_id, field, slot, recipient_key)


def _reply(code) -> dict:
    return {"ok": False, "reason_code": code, "message": adm.MESSAGES.get(code, adm.GENERIC_MESSAGE)}


def _deny_reply(module, code, known) -> dict:
    adm._record_deny(module, code, event_type=EVENT, **known)
    adm._metric(CREDENTIAL_RELEASE, "credential_release_total", "decision", "deny", code)
    return _reply(code)


def release_credential(  # pylint: disable=R0913
    module, project_id=None, context_ref=None, probe_ref=None, caller=None, toolkit_instance_id=None, field=None,
    slot=None, recipient_key=None, **_ignored,
) -> dict:
    """Worker-bus handler: the material of one credential slot, sealed to ``recipient_key``. Never raises.

    ``{"ok": True, "release_id", "fields": [paths], "expires_at", "sealed": {v, epk, nonce, ct}}`` or
    ``{"ok": False, "reason_code", "message"}``."""
    known = {}
    try:
        request = _parse(project_id, context_ref, probe_ref, caller, toolkit_instance_id, field, slot, recipient_key)
        known.update(
            project_id=request.project_id, caller_ref=f"indexer_worker:{request.task_name}",
            toolkit_instance_id=request.toolkit_instance_id, credential_ref=slot["credential_ref"],
            credential_generation=slot["credential_generation"], run_id=request.caller.get("run_id"))
        return _release(module, request, known)
    except _Unaudited as rejected:
        log.debug("credential release: request rejected (%s)", rejected.reason_code)
        adm._metric(CREDENTIAL_RELEASE, "credential_release_total", "decision", "rejected_request", rejected.reason_code)
        return _reply(rejected.reason_code)
    except AdmissionDenied as denied:
        code = denied.reason_code
    except Exception as exc:  # pylint: disable=W0703
        log.error("credential release: unexpected failure (%s)", type(exc).__name__)
        code = "DEPENDENCY_UNAVAILABLE"
    return _deny_reply(module, code, known)


def _release(module, request: _Request, known: dict) -> dict:
    if not governance_lifecycle_active(module):   # also after a switch to off: admitted runs keep their admission
        raise _unaudited("STALE_CONTEXT")
    try:
        audit_allowed = adm._tracing().security_audit_admission_allowed_local()   # 2.
    except Exception:  # pylint: disable=W0703
        audit_allowed = False
    if not audit_allowed:
        raise _unaudited("AUDIT_UNAVAILABLE")   # nothing can be recorded, so nothing is
    if request.context_ref is not None:
        scope = _check_run(module, request, known)
    else:
        scope = _check_probe(module, request, known)
    return _read_and_reply(module, request, known, scope)


# --- a run -----------------------------------------------------------------------------------

class _Scope:  # pylint: disable=R0903
    """What the checks established and the reply needs: which project holds the credential, which reference states
    may be released, the audit identity of the requester, and when the release stops being usable."""

    def __init__(self, binding_project_id, allow_validated, expires_at, event, revocation_epoch=None, on_revoke=None):  # pylint: disable=R0913
        self.binding_project_id, self.allow_validated, self.expires_at = binding_project_id, allow_validated, expires_at
        self.event, self.revocation_epoch, self.on_revoke = event, revocation_epoch, on_revoke


def _check_run(module, request: _Request, known: dict) -> _Scope:  # pylint: disable=R0914
    """Checks 3a to 7 of a run: one read transaction, the counter, the live checks."""
    context_id = uuid.UUID(request.context_ref)
    columns = ExecutionSecurityContext
    binding_columns = (AgentIdentityBinding.state, AgentIdentityBinding.revocation_epoch,
                       AgentIdentityBinding.definition_digest)
    with db.get_session(request.project_id) as session:
        row = session.execute(
            select(columns.schema_version, columns.profile, columns.state, columns.revoked_reason, columns.expires_at,
                   columns.run_id, columns.attempt, columns.application_id, columns.agent_binding_ref,
                   columns.binding_project_id, columns.project_id, columns.tenant, columns.subject,
                   columns.allowed_bindings, columns.trace_ref, columns.revocation_epochs, columns.caller,
                   columns.definition_digest, columns.policy_revision, columns.environment)
            .where(columns.id == context_id)
        ).one_or_none()
        if row is None:
            raise _unaudited("STALE_CONTEXT")   # no admitted run behind it
        binding_project_id = row.binding_project_id or row.project_id
        binding = session.execute(select(*binding_columns).where(AgentIdentityBinding.id == row.agent_binding_ref)
                                  ).one_or_none() if binding_project_id == request.project_id else None
    if binding_project_id != request.project_id:   # the agent lives in another project: a second read
        binding = adm._read_binding(binding_project_id, row.agent_binding_ref)
    subject = row.subject if isinstance(row.subject, dict) else {}
    is_mandate = subject.get("kind") == "mandate"
    known.update(
        application_id=row.application_id, attempt=row.attempt, agent_binding_ref=str(row.agent_binding_ref),
        run_id=row.run_id, traceparent=row.trace_ref, policy_revision=row.policy_revision, environment=row.environment,
        mandate_ref=str(subject.get("ref"))[len("mandate:"):] if is_mandate else None)
    #   3a. a live, running context of this worker's run
    if row.schema_version != adm.SCHEMA_VERSION or row.profile != "governed":
        raise adm._denied("STALE_CONTEXT")
    if request.task_name not in adm.ADMITTED_TASKS or request.caller.get("run_id") != row.run_id:
        raise adm._denied("STALE_CONTEXT")
    if row.state != "running":
        reason = row.revoked_reason if row.state == "revoked" else None
        raise adm._denied(reason if reason in adm.REVOKE_CODES else "STALE_CONTEXT")
    if adm._now() >= adm._aware(row.expires_at):
        raise adm._denied("STALE_CONTEXT")
    #   4. the exact use the context was admitted for
    slot = request.slot
    wanted = (request.toolkit_instance_id, request.field, slot["credential_ref"], slot["credential_generation"],
              slot["decision_id"])
    entry = next((item for item in row.allowed_bindings or [] if isinstance(item, dict) and wanted == (
        item.get("toolkit_instance_id"), item.get("field"), item.get("credential_ref"),
        item.get("credential_generation"), item.get("decision_id"))), None)
    if entry is None:
        raise adm._denied("CREDENTIAL_REFERENCE_REQUIRED")
    #   5. a bounded number of releases per use
    remaining = max(1, int((adm._aware(row.expires_at) - adm._now()).total_seconds()))
    _count_release(module, request, remaining, MAX_RELEASES_PER_FIELD)
    #   6, 7. the admission still stands for this use (read-only; a refusal with a revoke code ends the run)
    _live_check(request, row, binding, entry, is_mandate)
    user_ref = subject.get("owner") if is_mandate else subject.get("ref")
    event = dict(
        tenant=row.tenant, project_id=row.project_id, application_id=row.application_id,
        caller_ref=known["caller_ref"], user_ref=user_ref, mandate_ref=known["mandate_ref"],
        agent_binding_ref=str(row.agent_binding_ref), run_id=row.run_id, attempt=row.attempt,
        toolkit_instance_id=request.toolkit_instance_id, credential_ref=slot["credential_ref"],
        credential_generation=slot["credential_generation"], revocation_epoch=entry.get("reference_epoch"),
        traceparent=row.trace_ref, policy_revision=row.policy_revision, environment=row.environment)

    def on_revoke(code):   # a reference that changed between the recheck and the read ends the run, as the recheck would
        _revoke(request, code)

    return _Scope(binding_project_id, False, min(adm._aware(row.expires_at), _horizon()), event, on_revoke=on_revoke)


def _revoke(request: _Request, code) -> None:
    """End the run with a revoke code (the one write of a failing release); other codes change nothing."""
    if code in adm.REVOKE_CODES:
        with db.get_session(request.project_id) as session:
            adm._set_state(session, uuid.UUID(request.context_ref), "revoked", code)
            session.commit()


def _live_check(request: _Request, row, binding, entry, is_mandate) -> None:  # pylint: disable=R0912
    """Checks 6 and 7 for one use: the checks of ``validate_context`` that decide whether the admission still stands
    (user and binding epochs, digest, mandate, this use's credential decision), without its state write. A refusal with a
    revoke code revokes the context; a fact that cannot be read is unknown, not revoked."""
    def refuse(code):
        _revoke(request, code)
        return adm._denied(code)

    stamped = row.revocation_epochs if isinstance(row.revocation_epochs, dict) else {}
    binding_project_id = row.binding_project_id or row.project_id
    user_id = (row.caller or {}).get("user_id")
    if isinstance(user_id, bool) or not isinstance(user_id, int):
        raise refuse("EPOCH_MISMATCH")
    try:
        epochs = auth.read_identity_epochs("user", user_id)
    except Exception as exc:  # pylint: disable=W0703
        log.error("credential release: user epochs unreadable (%s)", type(exc).__name__)
        epochs = None
    if not isinstance(epochs, dict):
        raise adm._denied("DEPENDENCY_UNAVAILABLE")
    if not epochs.get("active") and is_mandate:
        adm._suspend_owner(binding_project_id, user_id)   # the owner is gone: the mandate goes with them
        raise refuse("MANDATE_INACTIVE")
    if not epochs.get("active") or epochs.get("user") != stamped.get("user"):
        raise refuse("EPOCH_MISMATCH")
    if binding is None or binding.state != "active":
        raise refuse("DENIED_BINDING")
    if binding.revocation_epoch != stamped.get("binding"):
        raise refuse("EPOCH_MISMATCH")
    if binding.definition_digest != row.definition_digest:
        raise refuse("BINDING_DIGEST_MISMATCH")
    if is_mandate:
        code = adm._mandate_revoke_code(row, stamped)
        if code is not None:
            raise refuse(code)
    try:
        code = recheck_credentials(
            binding_project_id, [entry], (row.subject or {}).get("ref"), str(row.agent_binding_ref), adm._now())
    except Exception as exc:  # pylint: disable=W0703
        log.error("credential release: credential facts unreadable (%s)", type(exc).__name__)
        raise adm._denied("DEPENDENCY_UNAVAILABLE") from None
    if code is not None:
        raise refuse(code)


def _horizon():
    return adm._now() + timedelta(seconds=RELEASE_TTL_S)


def _count_release(module, request: _Request, ttl_s, limit) -> None:
    """Check 5: ``INCR`` the release counter of a context's use (toolkit and field); over ``limit`` is ``RATE_LIMITED``.
    Redis down is a refusal."""
    key = "|".join((COUNTER_KEY + request.context_ref, request.toolkit_instance_id, request.field))   # '|' is not in a name
    try:
        pipe = module.get_redis_client().pipeline(transaction=False)
        pipe.incr(key)
        pipe.expire(key, ttl_s)
        count = pipe.execute()[0]
    except Exception as exc:  # pylint: disable=W0703
        log.error("credential release: the release counter failed (%s)", type(exc).__name__)
        raise adm._denied("DEPENDENCY_UNAVAILABLE") from None
    if not _int(count) or count > limit:
        raise adm._denied("RATE_LIMITED")


# --- a probe ---------------------------------------------------------------------------------

def _check_probe(module, request: _Request, known: dict) -> _Scope:
    """Check 3b: the single-use ticket minted by ``governed_check_connection`` for exactly this probe."""
    slot = request.slot
    if (request.toolkit_instance_id != PROBE_TOOLKIT_INSTANCE_ID or request.field != PROBE_FIELD
            or request.task_name != PROBE_TASK or request.caller.get("run_id") is not None):
        raise adm._denied("STALE_CONTEXT")
    try:
        pipe = module.get_redis_client().pipeline(transaction=True)   # GET and DEL together: one release per ticket
        pipe.get(PROBE_KEY + request.probe_ref)
        pipe.delete(PROBE_KEY + request.probe_ref)
        raw = pipe.execute()[0]
        ticket = json.loads(raw) if raw else None
    except Exception as exc:  # pylint: disable=W0703
        log.error("credential release: the probe ticket could not be taken (%s)", type(exc).__name__)
        raise adm._denied("DEPENDENCY_UNAVAILABLE") from None
    if ticket is None:
        raise _unaudited("STALE_CONTEXT")   # no probe was opened for it (or it was spent): nothing to record
    if not isinstance(ticket, dict) or (
            ticket.get("project_id"), ticket.get("credential_ref"), ticket.get("credential_generation"),
            ticket.get("task_name")) != (request.project_id, slot["credential_ref"], slot["credential_generation"],
                                         request.task_name) or not _int(ticket.get("user_id")):
        raise adm._denied("STALE_CONTEXT")
    event = dict(
        project_id=request.project_id, caller_ref=known["caller_ref"], user_ref=f"user:{ticket['user_id']}",
        toolkit_instance_id=request.toolkit_instance_id, credential_ref=slot["credential_ref"],
        credential_generation=slot["credential_generation"])
    known.update(user_ref=event["user_ref"])
    return _Scope(request.project_id, True, _horizon(), event)


# --- the store read and the reply ------------------------------------------------------------

def _read_and_reply(module, request: _Request, known: dict, scope: _Scope) -> dict:  # pylint: disable=R0914
    """Checks 8 and 9: read exactly the requested generation, seal it, commit the release event, then answer."""
    slot = request.slot
    release_id = str(uuid.uuid4())   # the store's decision id (a Vault correlation id) and the audit event id
    try:
        result = this.for_module("secrets").module.release_credential_material(
            scope.binding_project_id, slot["credential_ref"], slot["credential_generation"], release_id,
            known["caller_ref"], scope.allow_validated)
    except Exception as exc:  # pylint: disable=W0703
        log.error("credential release: the store read failed (%s)", type(exc).__name__)
        raise adm._denied("DEPENDENCY_UNAVAILABLE") from None
    if not isinstance(result, dict) or result.get("ok") is not True:
        code = result.get("reason_code") if isinstance(result, dict) else None
        code = code if isinstance(code, str) and code else "DEPENDENCY_UNAVAILABLE"
        if scope.on_revoke is not None:
            scope.on_revoke(code)
        raise adm._denied(code)
    value = result["value"]
    result = None
    if value.generation != slot["credential_generation"]:
        raise adm._denied("CREDENTIAL_GENERATION_MISMATCH")
    if not set(slot["fields"]) <= set(value.fields):
        raise adm._denied("STORE_NOT_FOUND")   # nothing partial is released
    plaintext = json.dumps({
        "fields": {path: value.fields[path].get_secret_value() for path in slot["fields"]},
        "generation": value.generation, "expires_at": scope.expires_at.isoformat(),
    }).encode("utf-8")
    value = None
    aad = "|".join((AAD_VERSION, str(request.project_id), request.ref, request.toolkit_instance_id, request.field,
                    request.decision_id, release_id)).encode("utf-8")
    try:
        sealed = _transport().seal(plaintext, request.recipient_key, aad)
    except Exception as exc:  # pylint: disable=W0703
        log.error("credential release: the reply could not be sealed (%s)", type(exc).__name__)
        raise adm._denied("DEPENDENCY_UNAVAILABLE") from None
    finally:
        plaintext = None   # noqa: F841  # pylint: disable=W0642
    #   9. the event is committed before the reply leaves; if it cannot be, the sealed bytes are dropped with it
    try:
        with db.get_session(request.project_id) as session:
            stamped = {"environment": module.admission_settings.environment,   # a probe has no admitted context
                       "policy_revision": module.governance_profile.policy_revision}
            stamped.update({key: item for key, item in scope.event.items() if item is not None})
            adm._tracing().security_audit_record(session, adm._event(
                event_type=EVENT, decision="allow", reason_code=RELEASED, event_id=release_id, **stamped))
            session.commit()
    except Exception as exc:  # pylint: disable=W0703
        log.error("credential release: the release event was not recorded (%s)", type(exc).__name__)
        raise adm._denied("AUDIT_UNAVAILABLE") from None
    adm._metric(CREDENTIAL_RELEASE, "credential_release_total", "decision", "allow", RELEASED)
    return {
        "ok": True, "release_id": release_id, "fields": list(slot["fields"]),
        "expires_at": scope.expires_at.isoformat(), "sealed": sealed,
    }
