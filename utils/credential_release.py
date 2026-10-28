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
     otherwise: it does not move the context's state. A development run
     (#7213) has no binding: its decisions are rechecked against the author's
     own grants, and its release events carry ``development:<digest>``
  8. the store read, by the secrets plugin, which re-reads the reference    CREDENTIAL_REFERENCE_INACTIVE / STORE_*
     (an Entra-delegated reference holds no value: the secrets plugin      the codes of ``entra_obo``
     answers its descriptor, and the slot's token is minted now on behalf
     of the run's person, bound to the context's subject; #6756 step 4)
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
import time
import uuid
from datetime import timedelta
from typing import Optional

from pylon.core.tools import log
from sqlalchemy import select
from tools import auth, db, this

from ..models.agent_identity import AgentIdentityBinding, ExecutionSecurityContext
from . import admission as adm   # the live checks of validate_context, the audit and counter helpers
from . import entra_obo
from .credential_decision import MAX_GOVERNED_CREDENTIALS_PER_RUN, PROFILE_DEVELOPMENT, recheck_development, recheck_failure
from .credential_slots import DELEGATED_FIELD, MAX_SLOT_FIELDS
from .exceptions import AdmissionDenied
from .governance import governance_lifecycle_active

# pylint: disable=W0212

MAX_RELEASES_PER_FIELD = 16
# A context a fan-out reopened (``admission.reopen_contexts`` marks it) is built from again by every child (up to 16 per
# parent) and by each reconcile round, so the bound of a use under it is higher. Still a bound: a loop is refused.
MAX_RELEASES_PER_FIELD_INHERITED = 128
MAX_BATCH_SLOTS = MAX_GOVERNED_CREDENTIALS_PER_RUN
RELEASE_TTL_S = 300
KEY_BYTES = 32
PROBE_TASK = "indexer_governed_check_connection"
PROBE_TOOLKIT_INSTANCE_ID = "probe"
PROBE_FIELD = "configuration"
PROBE_KEY = "c1009:probe:"       # single-use ticket of a probe: metadata only, minted by credential_probe
COUNTER_KEY = "c1009:rel:"
REQUEST_KEYS = ("project_id", "context_ref", "probe_ref", "caller", "toolkit_instance_id", "field", "slot", "recipient_key")
BATCH_REQUEST_KEYS = ("project_id", "context_ref", "probe_ref", "caller", "recipient_key", "slots")
BATCH_SLOT_KEYS = frozenset({"toolkit_instance_id", "field", "slot"})
AAD_VERSION = "v1"
RELEASED = "CREDENTIAL_RELEASED"
RELEASED_DELEGATED = "CREDENTIAL_RELEASED_DELEGATED"   # a token minted on behalf of the run's person (#6756 step 4)
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
        self.known = {   # what an audit event of a refusal may say about this slot
            "project_id": project_id, "caller_ref": f"indexer_worker:{caller['task_name']}",
            "toolkit_instance_id": toolkit_instance_id, "credential_ref": slot["credential_ref"],
            "credential_generation": slot["credential_generation"], "run_id": caller.get("run_id")}
        self.counter_key = None   # the release counter this request raised, until it is released or given back

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


def _denied_at(request: _Request, code) -> AdmissionDenied:
    """A refusal caused by one slot of a request: the batch reply names it."""
    denied = adm._denied(code)
    denied.slot = request
    return denied


def _deny_reply(module, code, requests, batch=False, failing=None) -> dict:
    """Audit and count the refusal, once for a slot request (as an event of its own, autocommit) and for a batch in one
    transaction with one event per slot, then the reply. A batch reply names the slot that caused it, when one did."""
    _refund(module, requests)   # the cap counts releases, not attempts
    if batch:
        _record_denies(module, code, requests)
    else:
        for request in requests:
            adm._record_deny(module, code, event_type=EVENT, **request.known)
    for _ in requests:
        adm._metric(CREDENTIAL_RELEASE, "credential_release_total", "decision", "deny", code)
    reply = _reply(code)
    if batch and failing is not None:
        reply.update(toolkit_instance_id=failing.toolkit_instance_id, field=failing.field)
    return reply


def _refund(module, requests) -> None:
    """Give back what ``_count_releases`` took for a request that was refused: one pipeline of ``DECR``. A counter that
    would go below zero (it expired meanwhile) is put back to zero. A failure is logged; the refusal stands."""
    keys = [request.counter_key for request in requests if request.counter_key]
    for request in requests:
        request.counter_key = None
    if not keys:
        return
    try:
        client = module.get_redis_client()
        pipe = client.pipeline(transaction=False)
        for key in keys:
            pipe.decr(key)
        for key, left in zip(keys, pipe.execute()):
            if _int(left) and left < 0:
                client.incr(key)
    except Exception as exc:  # pylint: disable=W0703
        log.error("credential release: the release counter was not given back (%s)", type(exc).__name__)


def _record_denies(module, code, requests) -> None:
    """The refusals of a batch, one ``credential_acquired`` event per slot, in one transaction. A failure to write them
    is logged; the refusal stands (as ``admission._record_deny``)."""
    if not requests:
        return
    try:
        with db.get_session(requests[0].project_id) as session:
            for request in requests:
                fields = {"policy_revision": module.governance_profile.policy_revision,
                          "environment": module.admission_settings.environment, **request.known}
                adm._tracing().security_audit_record(session, adm._event(
                    event_type=EVENT, decision="deny", reason_code=code, **fields))
            session.commit()
    except Exception as exc:  # pylint: disable=W0703
        log.error("credential release: the deny events %s were not recorded (%s)", code, type(exc).__name__)


def release_credential(  # pylint: disable=R0913
    module, project_id=None, context_ref=None, probe_ref=None, caller=None, toolkit_instance_id=None, field=None,
    slot=None, recipient_key=None, **_ignored,
) -> dict:
    """Worker-bus handler: the material of one credential slot, sealed to ``recipient_key``. Never raises.

    ``{"ok": True, "release_id", "fields": [paths], "expires_at", "sealed": {v, epk, nonce, ct}}`` or
    ``{"ok": False, "reason_code", "message"}``. A worker that asks for every slot of a run uses
    ``release_credentials``."""
    requests = []
    try:
        requests = [_parse(project_id, context_ref, probe_ref, caller, toolkit_instance_id, field, slot, recipient_key)]
        [release] = _release(module, requests)
        return {"ok": True, **{key: release[key] for key in ("release_id", "fields", "expires_at", "sealed")}}
    except _Unaudited as rejected:
        return _rejected(rejected)
    except AdmissionDenied as denied:
        code = denied.reason_code
    except Exception as exc:  # pylint: disable=W0703
        log.error("credential release: unexpected failure (%s)", type(exc).__name__)
        code = "DEPENDENCY_UNAVAILABLE"
    return _deny_reply(module, code, requests)


def release_credentials(  # pylint: disable=R0913
    module, project_id=None, context_ref=None, probe_ref=None, caller=None, recipient_key=None, slots=None, **_ignored,
) -> dict:
    """Worker-bus handler: the material of every credential slot of a run, each sealed to ``recipient_key`` under the
    binding of its own slot. One live check of the context, one counter round trip, one entry-scoped recheck for all the
    uses, one secrets call, and one audit transaction with an event per slot. The first failure refuses the whole
    batch: no slot of it is released. Never raises.

    ``slots``: ``[{"toolkit_instance_id", "field", "slot"}]`` (at most ``MAX_BATCH_SLOTS``, a toolkit and field once).
    ``{"ok": True, "releases": [{"toolkit_instance_id", "field", "release_id", "fields", "expires_at", "sealed"}]}`` in
    the order of ``slots``, or ``{"ok": False, "reason_code", "message"}`` (with ``toolkit_instance_id`` and ``field``
    when one slot caused it). A probe (``probe_ref``) asks for exactly one slot."""
    requests = []
    try:
        requests = _parse_batch(project_id, context_ref, probe_ref, caller, recipient_key, slots)
        releases = _release(module, requests)
        return {"ok": True, "releases": releases}
    except _Unaudited as rejected:
        return _rejected(rejected)
    except AdmissionDenied as denied:
        code, failing = denied.reason_code, getattr(denied, "slot", None)
    except Exception as exc:  # pylint: disable=W0703
        log.error("credential release: unexpected failure (%s)", type(exc).__name__)
        code, failing = "DEPENDENCY_UNAVAILABLE", None
    return _deny_reply(module, code, requests, batch=True, failing=failing)


def _rejected(rejected) -> dict:
    log.debug("credential release: request rejected (%s)", rejected.reason_code)
    adm._metric(CREDENTIAL_RELEASE, "credential_release_total", "decision", "rejected_request", rejected.reason_code)
    return _reply(rejected.reason_code)


def _parse_batch(project_id, context_ref, probe_ref, caller, recipient_key, slots) -> list:
    """Check 1 for every slot of a batch. Anything off is ``STALE_CONTEXT``."""
    stale = _unaudited("STALE_CONTEXT")
    if not isinstance(slots, list) or not 1 <= len(slots) <= MAX_BATCH_SLOTS:
        raise stale
    requests, seen = [], set()
    for item in slots:
        if not isinstance(item, dict) or set(item) != BATCH_SLOT_KEYS:
            raise stale
        request = _parse(project_id, context_ref, probe_ref, caller, item["toolkit_instance_id"], item["field"],
                         item["slot"], recipient_key)
        if (request.toolkit_instance_id, request.field) in seen:   # one slot per use: the counter and the AAD are per use
            raise stale
        seen.add((request.toolkit_instance_id, request.field))
        requests.append(request)
    if probe_ref is not None and len(requests) != 1:
        raise stale
    return requests


def _release(module, requests: list) -> list:
    """Checks 2 to 9 for the slots of one request (a single slot, or a batch). The releases, one per slot, in order."""
    if not governance_lifecycle_active(module):   # also after a switch to off: admitted runs keep their admission
        raise _unaudited("STALE_CONTEXT")
    try:
        audit_allowed = adm._tracing().security_audit_admission_allowed_local()   # 2.
    except Exception:  # pylint: disable=W0703
        audit_allowed = False
    if not audit_allowed:
        raise _unaudited("AUDIT_UNAVAILABLE")   # nothing can be recorded, so nothing is
    if requests[0].context_ref is not None:
        scope = _check_run(module, requests)
    else:
        scope = _check_probe(module, requests[0])
    return _read_and_reply(module, requests, scope)


# --- a run -----------------------------------------------------------------------------------

class _Scope:  # pylint: disable=R0903
    """What the checks established and the reply needs: which project holds the credential, which reference states
    may be released, the audit identity of the requester of each slot, and when the release stops being usable."""

    def __init__(self, binding_project_id, allow_validated, expires_at, events, on_revoke=None,
                 allow_pending=False, person=None):  # pylint: disable=R0913
        self.binding_project_id, self.allow_validated, self.expires_at = binding_project_id, allow_validated, expires_at
        self.events, self.on_revoke = events, on_revoke
        self.allow_pending = allow_pending   # a probe of a value the credential onboarding has just written
        # (subject, tenant, caller) of the context row the checks read: who an Entra-delegated slot acts for. A probe
        # ticket names no person, so a delegated slot is refused there (the credential onboarding probes it itself)
        self.person = person


def _check_run(module, requests: list) -> _Scope:  # pylint: disable=R0914
    """Checks 3a to 7 of a run: one read transaction, the counter, the live checks. For every slot of the request, in
    one pass: the context and the binding are read once, the counter is bumped in one round trip and the recheck of all
    the uses is one read of the facts."""
    first = requests[0]
    context_id = uuid.UUID(first.context_ref)
    columns = ExecutionSecurityContext
    binding_columns = (AgentIdentityBinding.state, AgentIdentityBinding.revocation_epoch,
                       AgentIdentityBinding.definition_digest)
    with db.get_session(first.project_id) as session:
        row = session.execute(
            select(columns.schema_version, columns.profile, columns.state, columns.revoked_reason, columns.expires_at,
                   columns.run_id, columns.attempt, columns.application_id, columns.agent_binding_ref,
                   columns.binding_project_id, columns.project_id, columns.tenant, columns.subject,
                   columns.allowed_bindings, columns.trace_ref, columns.revocation_epochs, columns.caller,
                   columns.definition_digest, columns.policy_revision, columns.environment, columns.parent_context_ref,
                   columns.call_lineage, columns.entry_point)
            .where(columns.id == context_id)
        ).one_or_none()
        if row is None:
            raise _unaudited("STALE_CONTEXT")   # no admitted run behind it
        binding_project_id = row.binding_project_id or row.project_id
        binding = session.execute(select(*binding_columns).where(AgentIdentityBinding.id == row.agent_binding_ref)
                                  ).one_or_none() if (binding_project_id == first.project_id
                                                      and row.agent_binding_ref is not None) else None
    if binding_project_id != first.project_id and row.agent_binding_ref is not None:   # the agent lives in another project
        binding = adm._read_binding(binding_project_id, row.agent_binding_ref)
    subject = row.subject if isinstance(row.subject, dict) else {}
    is_mandate = subject.get("kind") == "mandate"
    label = adm.context_binding_label(row)   # the binding, or development:<digest> (#7213)
    for request in requests:
        request.known.update(
            application_id=row.application_id, attempt=row.attempt, agent_binding_ref=label,
            run_id=row.run_id, traceparent=row.trace_ref, policy_revision=row.policy_revision,
            environment=row.environment,
            mandate_ref=str(subject.get("ref"))[len("mandate:"):] if is_mandate else None)
    #   3a. a live, running context of this worker's run
    if row.schema_version != adm.SCHEMA_VERSION or row.profile not in adm.PROFILES:
        raise adm._denied("STALE_CONTEXT")
    if (first.task_name not in adm.ADMITTED_TASKS or first.caller.get("run_id") != row.run_id
            or not adm.task_fits_entry(first.task_name, row.entry_point)):   # C9-SEC-32
        raise adm._denied("STALE_CONTEXT")
    if row.state != "running":
        reason = row.revoked_reason if row.state == "revoked" else None
        raise adm._denied(reason if reason in adm.REVOKE_CODES else "STALE_CONTEXT")
    if adm._now() >= adm._aware(row.expires_at):
        raise adm._denied("STALE_CONTEXT")
    if not adm.root_context_live(first.project_id, row):   # a fan-out sub-agent of a run that ended (#7227)
        raise adm._denied("STALE_CONTEXT")
    #   4. the exact use the context was admitted for, for every slot
    entries, missing = _entries_of(row, requests)
    if missing is not None:
        # a sub-agent the run calls in its own process was admitted with a context of its own (#7227): its slots are
        # checked and released against that context. Read only when a slot is not the run's own
        owner = next((child for child in adm.descendant_contexts(first.project_id, row.run_id, first.context_ref)
                      if _entries_of(child, requests)[1] is None), None)
        if owner is None or owner.schema_version != adm.SCHEMA_VERSION or owner.profile not in adm.PROFILES:
            raise _denied_at(missing, "CREDENTIAL_REFERENCE_REQUIRED")
        row, entries = owner, _entries_of(owner, requests)[0]
        binding_project_id = row.binding_project_id or row.project_id
        binding = None if row.agent_binding_ref is None else adm._read_binding(binding_project_id, row.agent_binding_ref)
        label = adm.context_binding_label(row)
        for request in requests:
            request.known.update(application_id=row.application_id, attempt=row.attempt, agent_binding_ref=label)
    #   5. a bounded number of releases per use
    remaining = max(1, int((adm._aware(row.expires_at) - adm._now()).total_seconds()))
    _count_releases(module, requests, remaining)
    #   6, 7. the admission still stands for these uses (read-only; a refusal with a revoke code ends the run)
    _live_check(first, row, binding, entries, is_mandate, requests)
    user_ref = subject.get("owner") if is_mandate else subject.get("ref")
    events = [dict(
        tenant=row.tenant, project_id=row.project_id, application_id=row.application_id,
        caller_ref=request.known["caller_ref"], user_ref=user_ref, mandate_ref=request.known["mandate_ref"],
        agent_binding_ref=label, run_id=row.run_id, attempt=row.attempt,
        toolkit_instance_id=request.toolkit_instance_id, credential_ref=request.slot["credential_ref"],
        credential_generation=request.slot["credential_generation"], revocation_epoch=entry.get("reference_epoch"),
        traceparent=row.trace_ref, policy_revision=row.policy_revision, environment=row.environment)
        for request, entry in zip(requests, entries)]

    def on_revoke(code):   # a reference that changed between the recheck and the read ends the run, as the recheck would
        _revoke(first, code)

    return _Scope(binding_project_id, False, min(adm._aware(row.expires_at), _horizon()), events, on_revoke=on_revoke,
                  person=(row.subject, row.tenant, row.caller))


def _entries_of(row, requests: list) -> tuple:
    """``(entries, None)``: the decision of ``row`` each slot names (toolkit, field, credential, generation, decision),
    in order; or ``([], request)`` for the first slot ``row`` holds no decision for."""
    entries = []
    for request in requests:
        slot = request.slot
        wanted = (request.toolkit_instance_id, request.field, slot["credential_ref"], slot["credential_generation"],
                  slot["decision_id"])
        entry = next((item for item in row.allowed_bindings or [] if isinstance(item, dict) and wanted == (
            item.get("toolkit_instance_id"), item.get("field"), item.get("credential_ref"),
            item.get("credential_generation"), item.get("decision_id"))), None)
        if entry is None:
            return [], request
        entries.append(entry)
    return entries, None


def _revoke(request: _Request, code) -> None:
    """End the run with a revoke code (the one write of a failing release); other codes change nothing."""
    if code in adm.REVOKE_CODES:
        with db.get_session(request.project_id) as session:
            adm._set_state(session, uuid.UUID(request.context_ref), "revoked", code)
            session.commit()


def _live_check(request: _Request, row, binding, entries, is_mandate, requests) -> None:  # pylint: disable=R0912,R0913
    """Checks 6 and 7 for the uses of the slots: the checks of ``validate_context`` that decide whether the admission
    still stands (user and binding epochs, digest, mandate, the credential decision of every use), without its state
    write. A refusal with a revoke code revokes the context; a fact that cannot be read is unknown, not revoked."""
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
    development = row.profile == PROFILE_DEVELOPMENT   # no binding to hold a development run to (#7213)
    if not development:
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
        subject_ref = (row.subject or {}).get("ref")
        if development:
            code, failed = recheck_development(binding_project_id, entries, subject_ref, adm._now())
        else:
            code, failed = recheck_failure(binding_project_id, entries, subject_ref, str(row.agent_binding_ref), adm._now())
    except Exception as exc:  # pylint: disable=W0703
        log.error("credential release: credential facts unreadable (%s)", type(exc).__name__)
        raise adm._denied("DEPENDENCY_UNAVAILABLE") from None
    if code is not None:
        denied = refuse(code)
        denied.slot = next((item for item, entry in zip(requests, entries) if entry is failed), None)
        raise denied


def _horizon():
    return adm._now() + timedelta(seconds=RELEASE_TTL_S)


def _count_releases(module, requests: list, ttl_s) -> None:
    """Check 5: ``INCR`` the release counter of each use (toolkit and field) of a context, all in one round trip; over
    the bound is ``RATE_LIMITED``. The bound of a use under a context a fan-out reopened (``INHERITED_KEY``) is higher,
    and only a count over the lower one asks whether it is. Redis down is a refusal."""
    base = COUNTER_KEY + requests[0].context_ref
    keys = ["|".join((base, request.toolkit_instance_id, request.field)) for request in requests]   # '|' is not in a name
    try:
        client = module.get_redis_client()
        pipe = client.pipeline(transaction=False)
        for key in keys:
            pipe.incr(key)
            pipe.expire(key, ttl_s)
        counts = pipe.execute()[0::2]
        for request, key in zip(requests, keys):
            request.counter_key = key
        over = [request for request, count in zip(requests, counts) if not _int(count) or count > MAX_RELEASES_PER_FIELD]
        limit = MAX_RELEASES_PER_FIELD_INHERITED if over and client.exists(adm.INHERITED_KEY + requests[0].context_ref) \
            else MAX_RELEASES_PER_FIELD
    except Exception as exc:  # pylint: disable=W0703
        log.error("credential release: the release counter failed (%s)", type(exc).__name__)
        raise adm._denied("DEPENDENCY_UNAVAILABLE") from None
    for request, count in zip(requests, counts):
        if not _int(count) or count > limit:
            raise _denied_at(request, "RATE_LIMITED")


# --- a probe ---------------------------------------------------------------------------------

def _check_probe(module, request: _Request) -> _Scope:
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
        project_id=request.project_id, caller_ref=request.known["caller_ref"], user_ref=f"user:{ticket['user_id']}",
        toolkit_instance_id=request.toolkit_instance_id, credential_ref=slot["credential_ref"],
        credential_generation=slot["credential_generation"])
    request.known.update(user_ref=event["user_ref"])
    return _Scope(request.project_id, True, _horizon(), [event], allow_pending=ticket.get("onboarding") is True)


# --- the store read and the reply ------------------------------------------------------------

def _read_store(module, requests: list, scope: _Scope, release_ids: list) -> dict:
    """Check 8: the values of the requested generations, ``{credential ref: value}`` (``fields`` and ``generation``).
    One secrets call for all the references of a request; a secrets plugin without the batch read goes one reference at
    a time. The first refusal ends it (a revoke code revokes the run).

    Known limit (follow-up, the secrets contract is unchanged): one ``release_id`` is given to the store for the whole
    read, so the store's correlation ids of the second slot on match no audit event; the first slot's does."""
    wanted = {}
    for request in requests:
        wanted.setdefault(request.slot["credential_ref"], request.slot["credential_generation"])
    secrets = this.for_module("secrets").module
    batch = getattr(secrets, "release_credential_materials", None)
    caller_ref = requests[0].known["caller_ref"]
    values = {}
    try:
        if callable(batch):
            result = batch(scope.binding_project_id,
                           [{"ref_id": ref, "generation": generation} for ref, generation in wanted.items()],
                           release_ids[0], caller_ref, allow_pending=scope.allow_pending,
                           allow_validated=scope.allow_validated)
            results = [(result, None)]
        else:
            results = []
            for index, (ref, generation) in enumerate(wanted.items()):
                one = secrets.release_credential_material(
                    scope.binding_project_id, ref, generation, release_ids[index if index < len(release_ids) else 0],
                    caller_ref, allow_validated=scope.allow_validated,
                    **({"allow_pending": True} if scope.allow_pending else {}))
                results.append((one, ref))
    except Exception as exc:  # pylint: disable=W0703
        log.error("credential release: the store read failed (%s)", type(exc).__name__)
        raise adm._denied("DEPENDENCY_UNAVAILABLE") from None
    for result, ref in results:
        if not isinstance(result, dict) or result.get("ok") is not True:
            code = result.get("reason_code") if isinstance(result, dict) else None
            code = code if isinstance(code, str) and code else "DEPENDENCY_UNAVAILABLE"
            if scope.on_revoke is not None:
                scope.on_revoke(code)
            failed = ref if ref is not None else (result.get("ref_id") if isinstance(result, dict) else None)
            denied = adm._denied(code)
            denied.slot = next((request for request in requests if request.slot["credential_ref"] == failed), None)
            raise denied
        if ref is None:
            found = result.get("values")
            values.update(found if isinstance(found, dict) else {})
        else:
            values[ref] = result["value"]
    return values


def _read_and_reply(module, requests: list, scope: _Scope) -> list:  # pylint: disable=R0914
    """Checks 8 and 9: read exactly the requested generations, seal each slot, commit the release events in one
    transaction, then answer. ``release_id`` is the audit event id and part of the binding of the slot's ciphertext; the
    first one is also the store's decision id (a Vault correlation id) for the read of the whole batch."""
    release_ids = [str(uuid.uuid4()) for _ in requests]
    values = _read_store(module, requests, scope, release_ids)
    releases, kinds = [], []
    deadline = entra_obo.deadline_for(len(requests))   # one budget for every delegated slot of this request
    for request, release_id in zip(requests, release_ids):
        slot = request.slot
        value = values.get(slot["credential_ref"])
        if value is None:
            raise _denied_at(request, "STORE_NOT_FOUND")
        if value.generation != slot["credential_generation"]:
            raise _denied_at(request, "CREDENTIAL_GENERATION_MISMATCH")
        delegation = getattr(value, "delegation", None)
        if delegation is not None:   # no stored value: a token minted now, on behalf of the run's person
            fields, expires_at = _delegated(module, request, scope, delegation, deadline)
            kinds.append(delegation.get("resource"))
        else:
            if not set(slot["fields"]) <= set(value.fields):
                raise _denied_at(request, "STORE_NOT_FOUND")   # nothing partial is released
            fields = {path: value.fields[path].get_secret_value() for path in slot["fields"]}
            expires_at = scope.expires_at
            kinds.append(None)
        plaintext = json.dumps({
            "fields": fields, "generation": value.generation, "expires_at": expires_at.isoformat(),
        }).encode("utf-8")
        value = fields = None
        aad = "|".join((AAD_VERSION, str(request.project_id), request.ref, request.toolkit_instance_id, request.field,
                        request.decision_id, release_id)).encode("utf-8")
        try:
            sealed = _transport().seal(plaintext, request.recipient_key, aad)
        except Exception as exc:  # pylint: disable=W0703
            log.error("credential release: the reply could not be sealed (%s)", type(exc).__name__)
            raise adm._denied("DEPENDENCY_UNAVAILABLE") from None
        finally:
            plaintext = None   # noqa: F841  # pylint: disable=W0642
        releases.append({
            "toolkit_instance_id": request.toolkit_instance_id, "field": request.field, "release_id": release_id,
            "fields": list(slot["fields"]), "expires_at": expires_at.isoformat(), "sealed": sealed})
    values = None
    #   9. the events are committed before the reply leaves; if they cannot be, the sealed bytes are dropped with them
    try:
        with db.get_session(requests[0].project_id) as session:
            for event, release_id, resource in zip(scope.events, release_ids, kinds):
                stamped = {"environment": module.admission_settings.environment,   # a probe has no admitted context
                           "policy_revision": module.governance_profile.policy_revision}
                stamped.update({key: item for key, item in event.items() if item is not None})
                if resource is not None:   # the kind is in the reason code, the resource is the target
                    stamped["target_ref"] = resource
                adm._tracing().security_audit_record(session, adm._event(
                    event_type=EVENT, decision="allow", reason_code=RELEASED if resource is None else RELEASED_DELEGATED,
                    event_id=release_id, **stamped))
            session.commit()
    except Exception as exc:  # pylint: disable=W0703
        log.error("credential release: the release event was not recorded (%s)", type(exc).__name__)
        raise adm._denied("AUDIT_UNAVAILABLE") from None
    for resource in kinds:
        adm._metric(CREDENTIAL_RELEASE, "credential_release_total", "decision", "allow",
                    RELEASED if resource is None else RELEASED_DELEGATED)
    return releases


def _delegated(module, request: _Request, scope: _Scope, delegation: dict, deadline=None) -> tuple:
    """The value of an Entra-delegated slot: ``({"access_token": token}, expires_at)``. The slot asks for the token
    only; the run must be a signed-in person's, bound to the reference's tenant and custodian (``entra_obo``). The
    token lives in this reply (sealed) and in ``entra_obo``'s in-memory cache, nowhere else. No read, no write here."""
    if request.slot["fields"] != [DELEGATED_FIELD]:
        raise _denied_at(request, "STORE_NOT_FOUND")
    try:
        if scope.person is None:
            raise entra_obo.DelegationRefused(entra_obo.CALLER_UNSUPPORTED)
        person = entra_obo.run_subject(*scope.person)
        token, expires = entra_obo.acquire(module, person, delegation, scope.binding_project_id, deadline)
    except entra_obo.DelegationRefused as refused:
        raise _denied_at(request, refused.code) from None
    expires_at = min(scope.expires_at, adm._now() + timedelta(seconds=max(0, expires - time.time()
                                                                            - entra_obo.SLOT_MARGIN_S)))
    return {DELEGATED_FIELD: token}, expires_at
