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
  5. at most ``MAX_RELEASES_PER_CONTEXT`` releases per context              RATE_LIMITED
  6/7. the live admission still stands: user and binding epochs, digest,    the codes of ``validate_context``
     mandate, and every credential decision rechecked (WP-1B); a refusal
     here revokes the context, as ``validate_context`` does
  8. the store read, by the secrets plugin, which re-reads the reference    CREDENTIAL_REFERENCE_INACTIVE / STORE_*
  9. the release event is committed before the reply leaves                 AUDIT_UNAVAILABLE

Every release, allowed or denied, is audited (``credential_acquired``). Nothing here logs, stores or returns a value
except the sealed reply, and no exception text of a dependency is repeated.
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
from tools import db, this

from ..models.agent_identity import ExecutionSecurityContext
from . import admission as adm   # the live checks of validate_context, the audit and counter helpers
from .exceptions import AdmissionDenied

# pylint: disable=W0212

MAX_RELEASES_PER_CONTEXT = 64
MAX_SLOT_FIELDS = 32
RELEASE_TTL_S = 300
KEY_BYTES = 32
PROBE_TASK = "indexer_governed_check_connection"
PROBE_TOOLKIT_INSTANCE_ID = "probe"
PROBE_FIELD = "configuration"
PROBE_KEY = "c1009:probe:"       # single-use ticket of a probe: metadata only, minted by credential_probe
COUNTER_KEY = "c1009:rel:"
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
    try:
        return str(uuid.UUID(str(value))) if isinstance(value, str) else None
    except ValueError:
        return None


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
    stale = adm._denied("STALE_CONTEXT")
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


def _deny_reply(module, code, known) -> dict:
    adm._record_deny(module, code, event_type=EVENT, **known)
    adm._metric(CREDENTIAL_RELEASE, "credential_release_total", "decision", "deny", code)
    return {"ok": False, "reason_code": code, "message": adm.MESSAGES.get(code, adm.GENERIC_MESSAGE)}


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
            toolkit_instance_id=request.toolkit_instance_id, credential_ref=_uuid(slot["credential_ref"]),
            credential_generation=slot["credential_generation"], run_id=request.caller.get("run_id"))
        return _release(module, request, known)
    except AdmissionDenied as denied:
        code = denied.reason_code
    except Exception as exc:  # pylint: disable=W0703
        log.error("credential release: unexpected failure (%s)", type(exc).__name__)
        code = "DEPENDENCY_UNAVAILABLE"
    return _deny_reply(module, code, known)


def _release(module, request: _Request, known: dict) -> dict:
    if not module.governance_profile.enabled:
        raise adm._denied("STALE_CONTEXT")
    try:
        audit_allowed = adm._tracing().security_audit_admission_allowed_local()   # 2.
    except Exception:  # pylint: disable=W0703
        audit_allowed = False
    if not audit_allowed:
        raise adm._denied("AUDIT_UNAVAILABLE")
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
    """Checks 3a to 7 of a run."""
    context_id = uuid.UUID(request.context_ref)
    columns = ExecutionSecurityContext
    with db.get_session(request.project_id) as session:
        row = session.execute(
            select(columns.schema_version, columns.profile, columns.state, columns.revoked_reason, columns.expires_at,
                   columns.run_id, columns.attempt, columns.application_id, columns.agent_binding_ref,
                   columns.binding_project_id, columns.project_id, columns.tenant, columns.subject,
                   columns.allowed_bindings, columns.trace_ref)
            .where(columns.id == context_id)
        ).one_or_none()
    if row is None:
        raise adm._denied("STALE_CONTEXT")
    subject = row.subject if isinstance(row.subject, dict) else {}
    is_mandate = subject.get("kind") == "mandate"
    known.update(
        application_id=row.application_id, attempt=row.attempt, agent_binding_ref=str(row.agent_binding_ref),
        run_id=row.run_id, traceparent=row.trace_ref,
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
    #   5. a bounded number of releases per context
    remaining = max(1, int((adm._aware(row.expires_at) - adm._now()).total_seconds()))
    _count_release(module, request.context_ref, remaining, MAX_RELEASES_PER_CONTEXT)
    #   6, 7. the admission still stands (user, binding, mandate, every credential decision): the live checks of
    #   validate_context, which also revoke the context for a revoke code
    validated = {}
    try:
        adm._validate(request.project_id, request.context_ref, request.caller, validated)
    except AdmissionDenied:
        known.update(validated)
        raise
    user_ref = subject.get("owner") if is_mandate else subject.get("ref")
    event = dict(
        tenant=row.tenant, project_id=row.project_id, application_id=row.application_id,
        caller_ref=known["caller_ref"], user_ref=user_ref, mandate_ref=known["mandate_ref"],
        agent_binding_ref=str(row.agent_binding_ref), run_id=row.run_id, attempt=row.attempt,
        toolkit_instance_id=request.toolkit_instance_id, credential_ref=slot["credential_ref"],
        credential_generation=slot["credential_generation"], revocation_epoch=entry.get("reference_epoch"),
        traceparent=row.trace_ref)

    def on_revoke(code):   # a reference that changed between the recheck and the read ends the run, as the recheck would
        if code in adm.REVOKE_CODES:
            with db.get_session(request.project_id) as session:
                adm._set_state(session, context_id, "revoked", code)
                session.commit()

    return _Scope(row.binding_project_id or row.project_id, False, min(adm._aware(row.expires_at), _horizon()),
                  event, on_revoke=on_revoke)


def _horizon():
    return adm._now() + timedelta(seconds=RELEASE_TTL_S)


def _count_release(module, ref, ttl_s, limit) -> None:
    """Check 5: ``INCR`` the release counter of a context; over ``limit`` is ``RATE_LIMITED``. Redis down is a refusal."""
    try:
        pipe = module.get_redis_client().pipeline(transaction=False)
        pipe.incr(COUNTER_KEY + ref)
        pipe.expire(COUNTER_KEY + ref, ttl_s)
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
            adm._tracing().security_audit_record(session, adm._event(
                event_type=EVENT, decision="allow", reason_code=RELEASED, event_id=release_id,
                environment=module.admission_settings.environment,
                policy_revision=module.governance_profile.policy_revision,
                **{key: item for key, item in scope.event.items() if item is not None}))
            session.commit()
    except Exception as exc:  # pylint: disable=W0703
        log.error("credential release: the release event was not recorded (%s)", type(exc).__name__)
        raise adm._denied("AUDIT_UNAVAILABLE") from None
    adm._metric(CREDENTIAL_RELEASE, "credential_release_total", "decision", "allow", RELEASED)
    return {
        "ok": True, "release_id": release_id, "fields": list(slot["fields"]),
        "expires_at": scope.expires_at.isoformat(), "sealed": sealed,
    }
