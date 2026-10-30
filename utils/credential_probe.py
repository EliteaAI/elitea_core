"""Governed test of a saved credential (#6946, WP-1D slice 1, C-1009).

The check-connection routes of the configurations plugin hand a saved credential here instead of resolving its secret
fields themselves. This module decides whether the user may probe it, sends the *stored non-secret configuration* plus a
slot to the ``governed_ops`` pool (the ``indexer_governed_check_connection`` task), and lets the worker ask Main for the
material through the same trusted release as a run (``credential_release``), under a single-use ticket.

Order: the actor holds ``credentials.probe``; the configuration is a credential of this project and type; its reference is
``validated`` or ``active`` (``pending`` too when the credential onboarding probes a value it has just written) at the
generation the configuration is at; a worker can run the task; the ``probe`` event is
committed, then the ticket (metadata only, 90 s, taken once by the release) is stored, then the task runs. Every refusal
is a code with a fixed readable message, audited; no value of the credential passes through this module.

Two users can be involved (#7119). ``user_id`` is the user whose personal reference is preferred over the shared one (the
tester, or the custodian when an operator tests on their behalf); only that user may probe their personal reference. The
actor (``actor_user_id``, the same user unless given) is who holds ``credentials.probe``, is named as the caller of the
events and is the user the ticket and the stored-configuration read are for. The caller that passes a different actor has
authorised them already (the credential onboarding's REST layer).

An Entra-delegated reference (#6756 step 4) holds no value: the probe acts with the tester's own Microsoft sign-in, as a
run they start does (#7281). Its ticket names the verified subject ``(issuer, tid, oid)`` of the signed-in session the
request came with (never a value of the request, never a user id alone), and the release mints the token on behalf of
that person through ``entra_obo``, with the same tenant, custodian and custody checks as a run. A project reference
needs a grant to that person. No session (an API token) is ``DENIED_DELEGATION_UNSUPPORTED_CALLER``; a session without
a verifiable Microsoft identity, or without a kept sign-in, is ``DENIED_ASSERTION_UNAVAILABLE``.
"""
import json
import threading
import time
import types
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

from pydantic import SecretStr
from pylon.core.tools import log
from tools import auth, db, this

from . import admission as adm   # audit and counter helpers
from . import entra_obo
from .credential_decision import (
    CREDENTIAL_SECTION, DELEGATED_AUTH_MODE, MAX_FACT_SELECTORS, STORE_KINDS, read_configuration_facts,
    subject_grant_in_force, subject_grant_key,
)
from .credential_release import PROBE_KEY, PROBE_TASK
from .credential_slots import (
    AUTH_MODE_KEY, DELEGATED_FIELD, DELEGATED_TYPES, SLOT_KEY, configuration_secret_fields, has_secret_template,
    secret_paths_for, slot_of, strip_secret_fields,
)
from .exceptions import AdmissionDenied
from .worker_capabilities import CAPABILITY_CREDENTIAL_SLOTS_V1, CAPABILITY_GOVERNED_OPS_POOL_V1

# pylint: disable=W0212

PROBE_PERMISSION = "credentials.probe"
PROBE_TTL_S = 90
JOIN_TIMEOUT_S = 60
EXPAND_TIMEOUT_S = 15
POOL = "governed_ops"
PROBE_PARALLEL = 4   # probes of one batch that run at the same time: the size of the governed_ops pool
MAX_PROBE_BATCH = 64
# The validators (#7281) ask for the same credential each time an agent or a chat participant is opened: an outcome is
# kept this long per tester, reference, generation, revocation epoch, person and grant (so a change of any of them is a
# new key), dropped when the person signs out; a transient refusal and a missing sign-in are never kept
OUTCOME_TTL_S = 120
MAX_OUTCOMES = 1024
NOT_KEPT = frozenset({"DEPENDENCY_UNAVAILABLE", "RATE_LIMITED", "WORKER_CAPABILITY_UNAVAILABLE", "AUDIT_UNAVAILABLE",
                      "STALE_CONTEXT", "DENIED_ASSERTION_UNAVAILABLE"})
_OUTCOMES = {}   # key -> (answer, until): this process only, no value of the credential in it
_OUTCOMES_LOCK = threading.Lock()
ALLOWED, SUCCEEDED, FAILED = "CREDENTIAL_PROBE_ALLOWED", "CREDENTIAL_PROBE_SUCCEEDED", "CREDENTIAL_PROBE_FAILED"
GENERIC_PROBE_MESSAGE = "The credential check was refused."
_STORE_MESSAGE = "The credential store is unavailable; the check was not run. Try again later."
# What a refusal says on the check-connection route: the wording of a run ("the run was not started") does not fit
PROBE_MESSAGES = {
    "CREDENTIAL_REFERENCE_REQUIRED": "This credential is registered for governed use; save it and test the saved credential.",
    "CREDENTIAL_REFERENCE_INACTIVE": "This credential is suspended or retired; the check was not run.",
    "CREDENTIAL_GENERATION_MISMATCH": "This credential changed after it was approved; the check was not run.",
    "CREDENTIAL_CLASS_NOT_ALLOWED": "Only the owner can test a personal credential.",
    "DENIED_RBAC": "You do not have permission to test saved credentials.",
    "AUDIT_UNAVAILABLE": "The security audit trail is unavailable; the check was not run. Try again later.",
    "WORKER_CAPABILITY_UNAVAILABLE": "No worker can test governed credentials right now; the check was not run.",
    "DEPENDENCY_UNAVAILABLE": "A required service is unavailable; the check was not run. Try again later.",
    "RATE_LIMITED": "This credential was asked for too often; the check was not run.",
    "STALE_CONTEXT": "The check could not be completed. Try again.",
    "UNSUPPORTED_AUTH_MODE": "This credential cannot be tested through the governed path yet.",
    **dict.fromkeys(
        ("STORE_DENIED", "STORE_SEALED", "STORE_UNAVAILABLE", "STORE_TLS_INVALID", "STORE_NOT_FOUND",
         "STORE_VERSION_CONFLICT"), _STORE_MESSAGE),
    # an Entra-delegated credential acts with the tester's own Microsoft sign-in (#7281): the texts of a run (step 4)
    **{code: adm.MESSAGES[code] for code in entra_obo.CODES},
    "DENIED_GRANT": "You hold no grant for this shared Microsoft credential; ask an administrator for one, then test "
                    "again.",
    "EPOCH_MISMATCH": "Your access to this credential changed while it was being tested; test it again.",
}


CREDENTIAL_PROBE = adm._LabeledCounter(
    "credential_probe_total", "Governed tests of saved credentials, by decision and reason", ["decision", "reason_code"],
)


def refusal(code) -> dict:
    return {"success": False, "code": code, "message": PROBE_MESSAGES.get(code, GENERIC_PROBE_MESSAGE)}


def _deny(code):
    return adm._denied(code)


def _stored_configuration(project_id, configuration_uuid, user_id) -> dict:
    """The saved configuration as stored: its secret fields are references (``{{secret.*}}``), not values."""
    from tools import context  # pylint: disable=C0415
    return context.rpc_manager.timeout(EXPAND_TIMEOUT_S).configurations_expand(
        project_id=project_id,
        settings={"configuration_uuid": configuration_uuid, "configuration_project_id": project_id},
        user_id=user_id, unsecret=False)


def _check_arguments(project_id, configuration_uuid, config_type, user_id, actor_user_id) -> None:
    try:
        canonical = str(uuid.UUID(configuration_uuid)) if isinstance(configuration_uuid, str) else None
    except ValueError:
        canonical = None
    ints = all(isinstance(value, int) and not isinstance(value, bool) for value in (project_id, user_id, actor_user_id))
    if (not ints or canonical is None or canonical != configuration_uuid or not isinstance(config_type, str)
            or not 0 < len(config_type) <= 128):
        raise _deny("CREDENTIAL_REFERENCE_REQUIRED")


def _may_probe(user_id, project_id) -> None:
    try:
        permitted = PROBE_PERMISSION in set(auth.get_user_permissions(user_id, mode="default", project_id=project_id))
    except Exception as exc:  # pylint: disable=W0703
        log.error("governed check_connection: permissions unreadable (%s)", type(exc).__name__)
        raise _deny("DEPENDENCY_UNAVAILABLE") from None
    if not permitted:
        raise _deny("DENIED_RBAC")


def _reference_of(project_id, configuration_uuid, config_type, user_id, known, onboarding=False, prefetched=None) -> tuple:  # pylint: disable=R0913
    """The configuration's facts and its credential reference; the refusal when it cannot be probed. ``onboarding``
    also admits a ``pending`` reference with a committed generation: the value the credential onboarding has just
    written, probed before it is validated. ``prefetched``: the facts of a whole batch, read once
    (``governed_check_connection_many``)."""
    try:
        configurations = read_configuration_facts([(configuration_uuid, project_id)]) if prefetched is None \
            else prefetched.configurations
        configuration = configurations.get(configuration_uuid)
        if not isinstance(configuration, dict):
            raise _deny("CREDENTIAL_REFERENCE_REQUIRED")
        if (configuration.get("section") != CREDENTIAL_SECTION or configuration.get("project_id") != project_id
                or configuration.get("type") != config_type):
            raise _deny("CREDENTIAL_REFERENCE_REQUIRED")
        found = prefetched.found if prefetched is not None else this.for_module("secrets").module.credential_use_facts(
            project_id, [configuration["id"]], [], None, personal_user_id=user_id)   # the tester's own personal reference first
    except AdmissionDenied:
        raise
    except Exception as exc:  # pylint: disable=W0703
        log.error("governed check_connection: the credential facts could not be read (%s)", type(exc).__name__)
        raise _deny("DEPENDENCY_UNAVAILABLE") from None
    reference = (found.get("references") or {}).get(configuration["id"]) if isinstance(found, dict) else None
    if not isinstance(reference, dict):
        raise _deny("CREDENTIAL_REFERENCE_REQUIRED")
    known.update(credential_ref=reference.get("ref_id"), credential_generation=reference.get("generation"))
    states = ("pending", "validated", "active") if onboarding else ("validated", "active")
    if (reference.get("state") not in states or reference.get("store_kind") not in STORE_KINDS
            or (reference.get("state") == "pending" and not (reference.get("generation") or 0) >= 1)):
        raise _deny("CREDENTIAL_REFERENCE_INACTIVE")
    if reference.get("ownership_class") == "personal" and reference.get("custodian_user_id") != user_id:
        raise _deny("CREDENTIAL_CLASS_NOT_ALLOWED")
    approved = reference.get("configuration_generation")
    if approved is None or configuration.get("generation") != approved:
        raise _deny("CREDENTIAL_GENERATION_MISMATCH")
    return configuration, reference


def _session_holder():
    """The authenticated holder of the HTTP request this probe serves (``flask.g.auth``), or ``None``: read as a run's
    admission reads it."""
    from ..models.pd.admission import _holder  # pylint: disable=C0415
    return _holder(None)


def _probe_person(module, actor_user_id) -> dict:
    """``{issuer, tid, oid}`` of the person testing an Entra-delegated credential: the verified subject of the signed-in
    session the request came with, verified as a run's admission verifies its caller (the identity service, the
    session's own reference), never a value of the request or a user id alone (#7281)."""
    holder = _session_holder()
    reference = getattr(holder, "reference", None)
    try:
        holder_id = int(getattr(holder, "id", None))
    except (TypeError, ValueError):
        holder_id = None
    if getattr(holder, "type", None) != "user" or holder_id != actor_user_id or not isinstance(reference, str) \
            or not reference:
        raise _deny(entra_obo.CALLER_UNSUPPORTED)   # an API token, or no signed-in session behind the probe
    caller = types.SimpleNamespace(caller_type="user", caller_id=actor_user_id, caller_reference=SecretStr(reference))
    try:
        subject = adm._subject_of(adm._verify_identity(caller, module.admission_settings))
    except AdmissionDenied as denied:
        if denied.reason_code == "DEPENDENCY_UNAVAILABLE":
            raise
        log.info("governed check_connection: no verified Microsoft identity (%s)", denied.reason_code)
        raise _deny(entra_obo.ASSERTION_UNAVAILABLE) from None
    person = {"issuer": subject.get("issuer"), "tid": subject.get("tid"), "oid": subject.get("oid")}
    if not all(isinstance(value, str) and value for value in person.values()):
        raise _deny(entra_obo.ASSERTION_UNAVAILABLE)
    return person


def _require_grant(project_id, reference, person) -> Optional[dict]:
    """A shared Entra-delegated reference acts for the tester only with a grant to them in force (a run's development
    rule, #7213): ``{"grant_id", "epoch"}`` of it, which the release checks again (C9-SEC-64). A personal one is its
    custodian's, bound again at the release: ``None``. One facts read, delegated only."""
    if reference.get("ownership_class") == "personal":
        return None
    subject_ref = f"idp:{person['tid']}:{person['oid']}"
    try:
        found = this.for_module("secrets").module.credential_use_facts(
            project_id, [], [], [reference["ref_id"]], subject_grants=[(reference["ref_id"], subject_ref)])
        grants = (found.get("subject_grants") or {}).get(subject_grant_key(reference["ref_id"], subject_ref))
    except Exception as exc:  # pylint: disable=W0703
        log.error("governed check_connection: the grants could not be read (%s)", type(exc).__name__)
        raise _deny("DEPENDENCY_UNAVAILABLE") from None
    grant = subject_grant_in_force(grants, adm._now())
    if grant is None:
        raise _deny("DENIED_GRANT")
    return {"grant_id": grant.get("grant_id"), "epoch": grant.get("revocation_epoch")}


def _kept_outcome(key) -> Optional[dict]:
    with _OUTCOMES_LOCK:
        kept = _OUTCOMES.get(key)
        if kept is not None and kept[1] > time.monotonic():
            return dict(kept[0])
        _OUTCOMES.pop(key, None)
        return None


def _keep_outcome(key, answer) -> None:
    if answer.get("code") in NOT_KEPT:
        return
    now = time.monotonic()
    with _OUTCOMES_LOCK:
        if len(_OUTCOMES) >= MAX_OUTCOMES:
            for stale in [item for item, (_, until) in _OUTCOMES.items() if until <= now]:
                del _OUTCOMES[stale]
            while len(_OUTCOMES) >= MAX_OUTCOMES:
                del _OUTCOMES[next(iter(_OUTCOMES))]
        _OUTCOMES[key] = (dict(answer), now + OUTCOME_TTL_S)


def forget_outcomes(tid=None, oid=None) -> None:
    """Drop the kept outcomes of one person's delegated probes (they signed out), or all of them."""
    with _OUTCOMES_LOCK:
        for key in [key for key in _OUTCOMES if tid is None or key[-2] == (str(tid).lower(), oid)]:
            del _OUTCOMES[key]


def _probe_event(module, **fields) -> dict:
    return adm._event(
        event_type="probe", policy_revision=module.governance_profile.policy_revision,
        environment=module.admission_settings.environment, **fields)


def _open_probe(  # pylint: disable=R0913
    module, project_id, user_id, actor_user_id, probe_ref, reference, onboarding=False, person=None, grant=None,
) -> None:
    """Commit the ``probe`` event, then store the ticket the release takes. The event comes first: a ticket never
    exists without its record, and a lost ticket only makes the probe fail. ``person``: the verified subject an
    Entra-delegated probe acts for (``_probe_person``); ``grant``: the grant a shared one is used on (``_require_grant``)."""
    event = _probe_event(
        module, decision="allow", reason_code=ALLOWED, event_id=probe_ref, project_id=project_id,
        caller_ref=f"user:{actor_user_id}", user_ref=f"user:{user_id}", credential_ref=reference["ref_id"],
        credential_generation=reference["generation"])
    try:
        with db.get_session(project_id) as session:
            adm._tracing().security_audit_record(session, event)
            session.commit()
    except Exception as exc:  # pylint: disable=W0703
        log.error("governed check_connection: the probe event was not recorded (%s)", type(exc).__name__)
        raise _deny("AUDIT_UNAVAILABLE") from None
    ticket = {
        "project_id": project_id, "credential_ref": reference["ref_id"],
        "credential_generation": reference["generation"], "task_name": PROBE_TASK, "user_id": actor_user_id,
    }
    if onboarding:   # the release may read a pending reference for this ticket (and only this ticket)
        ticket["onboarding"] = True
    if person is not None:   # the release mints the token on behalf of this person, and only for this ticket
        ticket["person"] = person
    if grant is not None:    # and checks this grant again
        ticket["grant"] = grant
    ticket = json.dumps(ticket)
    try:
        module.get_redis_client().set(PROBE_KEY + probe_ref, ticket, ex=PROBE_TTL_S)
    except Exception as exc:  # pylint: disable=W0703
        log.error("governed check_connection: the probe ticket was not stored (%s)", type(exc).__name__)
        raise _deny("DEPENDENCY_UNAVAILABLE") from None


def _close_probe(module, probe_ref) -> None:
    """The ticket is spent by the release; when the task never asked (it did not start, timed out, died) it goes now."""
    try:
        module.get_redis_client().delete(PROBE_KEY + probe_ref)
    except Exception as exc:  # pylint: disable=W0703
        log.error("governed check_connection: the probe ticket was not removed (%s)", type(exc).__name__)


def _run_probe(module, project_id, config_type, settings):
    """Run the task in the ``governed_ops`` pool and wait for its result."""
    try:
        task_id = module.task_node.start_task(
            PROBE_TASK, kwargs={"configuration_type": config_type, "settings": settings}, pool=POOL,
            meta={"task_name": PROBE_TASK, "project_id": project_id, "configuration_type": config_type,
                  "llm_free": True, "user_input_preview": f"check connection {config_type}"[:100]})
    except Exception as exc:  # pylint: disable=W0703
        code = getattr(exc, "reason_code", None)
        log.error("governed check_connection: the task was not started (%s)", type(exc).__name__)
        raise _deny(code if code == "WORKER_CAPABILITY_UNAVAILABLE" else "DEPENDENCY_UNAVAILABLE") from None
    if task_id is None:   # the pool is saturated
        raise _deny("DEPENDENCY_UNAVAILABLE")
    result = module.task_node.join_task(task_id, timeout=JOIN_TIMEOUT_S)
    if result is ...:
        try:
            module.task_node.stop_task(task_id)
        except Exception:  # pylint: disable=W0703
            pass
        raise _deny("DEPENDENCY_UNAVAILABLE")
    return result


def shape_result(result) -> dict:
    """The answer to the route from what the worker returned (already scrubbed of the released values)."""
    if result is None:
        return {"success": True}
    if isinstance(result, str):
        return {"success": False, "message": result}   # the provider's own failure text
    if not isinstance(result, dict):
        return refusal("DEPENDENCY_UNAVAILABLE")
    if result.get("requires_authorization"):
        return {"success": False, "requires_authorization": True, "auth_metadata": result.get("auth_metadata") or {},
                "message": result.get("error") or "Authorization required"}
    if result.get("success") is False and isinstance(result.get("reason_code"), str):
        code = result["reason_code"]   # the worker refused (a release denial, or an envelope it cannot honour)
        return refusal(code if code in PROBE_MESSAGES else "WORKER_CAPABILITY_UNAVAILABLE")
    return {**result, "success": True}


def _outcome(module, project_id, user_id, actor_user_id, reference, answer) -> None:
    if answer.get("success"):
        outcome, reason = "success", SUCCEEDED
    elif answer.get("requires_authorization"):
        outcome, reason = "interaction_required", FAILED
    else:
        outcome, reason = ("denied" if answer.get("code") else "failed"), FAILED
    try:
        adm._tracing().security_audit_record_autocommit(_probe_event(
            module, decision="allow", reason_code=reason, outcome=outcome, project_id=project_id,
            caller_ref=f"user:{actor_user_id}", user_ref=f"user:{user_id}", credential_ref=reference["ref_id"],
            credential_generation=reference["generation"]))
    except Exception as exc:  # pylint: disable=W0703
        log.error("governed check_connection: the outcome was not recorded (%s)", type(exc).__name__)


def _probe(module, project_id, configuration_uuid, config_type, user_id, known, actor_user_id=None, onboarding=False,  # pylint: disable=R0913,R0914
           shared=None, reuse=False) -> dict:
    if not module.governance_profile.enabled:
        raise _deny("DEPENDENCY_UNAVAILABLE")
    actor_user_id = user_id if actor_user_id is None else actor_user_id
    _check_arguments(project_id, configuration_uuid, config_type, user_id, actor_user_id)
    known.update(project_id=project_id, caller_ref=f"user:{actor_user_id}", user_ref=f"user:{user_id}")
    try:
        audit_allowed = adm._tracing().security_audit_admission_allowed_local()
    except Exception:  # pylint: disable=W0703
        audit_allowed = False
    if not audit_allowed:
        raise _deny("AUDIT_UNAVAILABLE")
    if shared is None:   # a batch has asked once for all its probes
        _may_probe(actor_user_id, project_id)
    _configuration, reference = _reference_of(
        project_id, configuration_uuid, config_type, user_id, known, onboarding, None if shared is None else shared.facts)
    delegated = reference.get("auth_mode") == DELEGATED_AUTH_MODE
    person = grant = None
    if delegated:   # acts with the tester's own Microsoft sign-in (#7281)
        person = _probe_person(module, actor_user_id) if shared is None else shared.person
        if not isinstance(person, dict):   # the batch could not verify the person: the code it was refused with
            raise _deny(person if isinstance(person, str) else entra_obo.CALLER_UNSUPPORTED)
        grant = _require_grant(project_id, reference, person)
    reuse_key = None
    if reuse:   # a validator asking again (#7281): the same tester, reference state, person and grant
        reuse_key = (actor_user_id, user_id, reference.get("ref_id"), reference.get("generation"),
                     reference.get("configuration_generation"), reference.get("revocation_epoch"),
                     None if person is None else (person["tid"].lower(), person["oid"]),
                     None if grant is None else (grant["grant_id"], grant["epoch"]))
        kept = _kept_outcome(reuse_key)
        if kept is not None:
            return kept
    required = {"task_envelope.v1", CAPABILITY_CREDENTIAL_SLOTS_V1, CAPABILITY_GOVERNED_OPS_POOL_V1}
    if not module.worker_capabilities.has_capable_worker(required):
        raise _deny("WORKER_CAPABILITY_UNAVAILABLE")
    # the stored configuration without its secret fields, and a slot for them
    try:
        settings = _stored_configuration(project_id, configuration_uuid, actor_user_id)
        if settings.get("configuration_generation") != reference.get("configuration_generation"):
            raise _deny("CREDENTIAL_GENERATION_MISMATCH")   # changed between the facts and the read: no probe
        paths = secret_paths_for({config_type}, configuration_secret_fields)[config_type]
        if delegated:   # a container with no value (the seal of a run's delegated entry): the slot asks for the token
            stripped = strip_secret_fields(settings, list(dict.fromkeys([*paths, DELEGATED_FIELD])))
            if config_type not in DELEGATED_TYPES or stripped:   # a value carried inline is refused, as at the seal
                raise _deny("CREDENTIAL_REFERENCE_REQUIRED")
            settings[AUTH_MODE_KEY] = DELEGATED_AUTH_MODE
            held = [DELEGATED_FIELD]
        else:
            held = strip_secret_fields(settings, paths)
    except AdmissionDenied:
        raise
    except Exception as exc:  # pylint: disable=W0703
        log.error("governed check_connection: the stored configuration could not be prepared (%s)", type(exc).__name__)
        code = getattr(exc, "reason_code", None)
        raise _deny(code if code == "CREDENTIAL_REFERENCE_REQUIRED" else "DEPENDENCY_UNAVAILABLE") from None
    if not held or has_secret_template(settings):
        # nothing to ask for would run the probe without its credential; a stored template outside the secret fields
        # would be sent as it is
        raise _deny("CREDENTIAL_REFERENCE_REQUIRED")
    probe_ref = str(uuid.uuid4())
    settings[SLOT_KEY] = slot_of(
        {"credential_ref": reference["ref_id"], "credential_generation": reference["generation"]}, held,
        ticket=probe_ref)
    _open_probe(module, project_id, user_id, actor_user_id, probe_ref, reference, onboarding, person, grant)
    try:
        answer = shape_result(_run_probe(module, project_id, config_type, settings))
    finally:
        _close_probe(module, probe_ref)
    _outcome(module, project_id, user_id, actor_user_id, reference, answer)
    if reuse_key is not None:
        _keep_outcome(reuse_key, answer)
    adm._metric(CREDENTIAL_PROBE, "credential_probe_total", "decision", "allow",
                SUCCEEDED if answer.get("success") else FAILED)
    return answer


def governed_check_connection(  # pylint: disable=R0913
    module, project_id, configuration_uuid, config_type, user_id, actor_user_id=None, onboarding=False, shared=None,
    reuse=False,
) -> dict:
    """Test a saved credential through the governed path. Never raises. ``user_id``: whose personal reference is
    preferred; ``actor_user_id`` (default ``user_id``): who holds ``credentials.probe``; ``onboarding``: a ``pending``
    reference may be probed (see the module docstring); ``reuse``: a validator's check, answered from a kept outcome
    while it stands (``OUTCOME_TTL_S``).

    ``{"success": True, ...}`` | ``{"success": False, "message"}`` (the provider failed) |
    ``{"success": False, "requires_authorization": True, "auth_metadata", "message"}`` |
    ``{"success": False, "code", "message"}`` (a refusal: permission, governance, capability, store)."""
    known = {}
    try:
        return _probe(module, project_id, configuration_uuid, config_type, user_id, known, actor_user_id, onboarding,
                      shared, reuse)
    except AdmissionDenied as denied:
        code = denied.reason_code
    except Exception as exc:  # pylint: disable=W0703
        log.error("governed check_connection: unexpected failure (%s)", type(exc).__name__)
        code = "DEPENDENCY_UNAVAILABLE"
    adm._record_deny(module, code, event_type="probe", **known)
    adm._metric(CREDENTIAL_PROBE, "credential_probe_total", "decision", "deny", code)
    return refusal(code)


def _refuse(module, code, known) -> dict:
    """The refusal ``governed_check_connection`` gives, audited and counted the same way."""
    adm._record_deny(module, code, event_type="probe", **known)
    adm._metric(CREDENTIAL_PROBE, "credential_probe_total", "decision", "deny", code)
    return refusal(code)


def _shared_checks(module, project_id, uuids, user_id, actor_user_id) -> types.SimpleNamespace:
    """What every probe of a batch needs and the batch can ask once: the actor's permission, the facts of all the
    configurations (``read_configuration_facts``) and of their references, and, when one is Entra-delegated, the
    verified person (here, on the request's own greenlet: the probes run on others), or the code that refused it.
    Raises ``AdmissionDenied`` or whatever the reads raise."""
    _may_probe(actor_user_id, project_id)
    configurations = read_configuration_facts([(item, project_id) for item in uuids])
    ids = sorted({fact["id"] for fact in configurations.values() if isinstance(fact, dict) and "id" in fact})
    references = {}
    secrets = this.for_module("secrets").module
    for start in range(0, len(ids), MAX_FACT_SELECTORS):
        found = secrets.credential_use_facts(
            project_id, ids[start:start + MAX_FACT_SELECTORS], [], None, personal_user_id=user_id)
        references.update(found.get("references") or {})
    person = None
    if any(isinstance(item, dict) and item.get("auth_mode") == DELEGATED_AUTH_MODE for item in references.values()):
        try:
            person = _probe_person(module, actor_user_id)
        except AdmissionDenied as denied:
            person = denied.reason_code
    return types.SimpleNamespace(
        facts=types.SimpleNamespace(configurations=configurations, found={"references": references}), person=person)


def governed_check_connection_many(  # pylint: disable=R0913,R0914
    module, project_id, items, user_id, actor_user_id=None, onboarding=False, parallel=PROBE_PARALLEL, reuse=False,
) -> list:
    """Test several saved credentials of one project: ``items`` is ``[{"configuration_uuid", "config_type"}]``. The
    actor's permission and the facts of all the configurations are read once; the probes then run at the same time, at
    most ``parallel`` of them (each is a task of the ``governed_ops`` pool, which has as many slots), instead of one
    after the other on the greenlet of the request. One answer per item, in order, each as ``governed_check_connection``
    answers it (a refusal of one item does not touch the others); ``reuse`` as there (a validator's batch, #7294).
    Never raises."""
    answers = [None] * len(items) if isinstance(items, (list, tuple)) else []
    if not answers:
        return answers
    actor_user_id = user_id if actor_user_id is None else actor_user_id
    known = {"project_id": project_id, "caller_ref": f"user:{actor_user_id}", "user_ref": f"user:{user_id}"}
    valid = []
    for index, item in enumerate(items):
        configuration_uuid = item.get("configuration_uuid") if isinstance(item, dict) else None
        config_type = item.get("config_type") if isinstance(item, dict) else None
        try:
            if index >= MAX_PROBE_BATCH:
                raise _deny("RATE_LIMITED")
            _check_arguments(project_id, configuration_uuid, config_type, user_id, actor_user_id)
            valid.append((index, configuration_uuid, config_type))
        except AdmissionDenied as denied:
            answers[index] = _refuse(module, denied.reason_code, known)
    if not valid:
        return answers
    try:
        if not module.governance_profile.enabled:
            raise _deny("DEPENDENCY_UNAVAILABLE")
        shared = _shared_checks(module, project_id, sorted({item[1] for item in valid}), user_id, actor_user_id)
    except AdmissionDenied as denied:
        code = denied.reason_code
    except Exception as exc:  # pylint: disable=W0703
        log.error("governed check_connection: the batch could not be prepared (%s)", type(exc).__name__)
        code = "DEPENDENCY_UNAVAILABLE"
    else:
        code = None
    if code is not None:
        for index, _, _ in valid:
            answers[index] = _refuse(module, code, known)
        return answers

    def probe(item):
        index, configuration_uuid, config_type = item
        answers[index] = governed_check_connection(
            module, project_id, configuration_uuid, config_type, user_id, actor_user_id, onboarding, shared, reuse)

    with ThreadPoolExecutor(max_workers=max(1, min(parallel, len(valid)))) as pool:
        list(pool.map(probe, valid))
    return answers
