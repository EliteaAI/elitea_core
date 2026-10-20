"""Governed test of a saved credential (#6946, WP-1D slice 1, C-1009).

The check-connection routes of the configurations plugin hand a saved credential here instead of resolving its secret
fields themselves. This module decides whether the user may probe it, sends the *stored non-secret configuration* plus a
slot to the ``governed_ops`` pool (the ``indexer_governed_check_connection`` task), and lets the worker ask Main for the
material through the same trusted release as a run (``credential_release``), under a single-use ticket.

Order: the user holds ``credentials.probe``; the configuration is a credential of this project and type; its reference is
``validated`` or ``active`` at the generation the configuration is at; a worker can run the task; the ``probe`` event is
committed, then the ticket (metadata only, 90 s, taken once by the release) is stored, then the task runs. Every refusal
is a code with a fixed readable message, audited; no value of the credential passes through this module.
"""
import json
import uuid

from pylon.core.tools import log
from tools import auth, db, this

from . import admission as adm   # audit and counter helpers
from .credential_decision import CREDENTIAL_SECTION, STORE_KINDS, read_configuration_facts
from .credential_release import PROBE_KEY, PROBE_TASK
from .credential_slots import (
    SLOT_KEY, configuration_secret_fields, has_secret_template, secret_paths_for, slot_of, strip_secret_fields,
)
from .exceptions import AdmissionDenied
from .worker_capabilities import CAPABILITY_CREDENTIAL_SLOTS_V1, CAPABILITY_GOVERNED_OPS_POOL_V1

# pylint: disable=W0212

PROBE_PERMISSION = "credentials.probe"
PROBE_TTL_S = 90
JOIN_TIMEOUT_S = 60
EXPAND_TIMEOUT_S = 15
POOL = "governed_ops"
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


def _check_arguments(project_id, configuration_uuid, config_type, user_id) -> None:
    try:
        canonical = str(uuid.UUID(configuration_uuid)) if isinstance(configuration_uuid, str) else None
    except ValueError:
        canonical = None
    ints = all(isinstance(value, int) and not isinstance(value, bool) for value in (project_id, user_id))
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


def _reference_of(project_id, configuration_uuid, config_type, user_id, known) -> tuple:
    """The configuration's facts and its credential reference; the refusal when it cannot be probed."""
    try:
        configuration = read_configuration_facts([(configuration_uuid, project_id)]).get(configuration_uuid)
        if not isinstance(configuration, dict):
            raise _deny("CREDENTIAL_REFERENCE_REQUIRED")
        if (configuration.get("section") != CREDENTIAL_SECTION or configuration.get("project_id") != project_id
                or configuration.get("type") != config_type):
            raise _deny("CREDENTIAL_REFERENCE_REQUIRED")
        found = this.for_module("secrets").module.credential_use_facts(project_id, [configuration["id"]], [])
    except AdmissionDenied:
        raise
    except Exception as exc:  # pylint: disable=W0703
        log.error("governed check_connection: the credential facts could not be read (%s)", type(exc).__name__)
        raise _deny("DEPENDENCY_UNAVAILABLE") from None
    reference = (found.get("references") or {}).get(configuration["id"]) if isinstance(found, dict) else None
    if not isinstance(reference, dict):
        raise _deny("CREDENTIAL_REFERENCE_REQUIRED")
    known.update(credential_ref=reference.get("ref_id"), credential_generation=reference.get("generation"))
    if reference.get("state") not in ("validated", "active") or reference.get("store_kind") not in STORE_KINDS:
        raise _deny("CREDENTIAL_REFERENCE_INACTIVE")
    if reference.get("ownership_class") == "personal" and reference.get("custodian_user_id") != user_id:
        raise _deny("CREDENTIAL_CLASS_NOT_ALLOWED")
    approved = reference.get("configuration_generation")
    if approved is None or configuration.get("generation") != approved:
        raise _deny("CREDENTIAL_GENERATION_MISMATCH")
    return configuration, reference


def _probe_event(module, **fields) -> dict:
    return adm._event(
        event_type="probe", policy_revision=module.governance_profile.policy_revision,
        environment=module.admission_settings.environment, **fields)


def _open_probe(module, project_id, user_id, probe_ref, reference) -> None:
    """Commit the ``probe`` event, then store the ticket the release takes. The event comes first: a ticket never
    exists without its record, and a lost ticket only makes the probe fail."""
    event = _probe_event(
        module, decision="allow", reason_code=ALLOWED, event_id=probe_ref, project_id=project_id,
        caller_ref=f"user:{user_id}", user_ref=f"user:{user_id}", credential_ref=reference["ref_id"],
        credential_generation=reference["generation"])
    try:
        with db.get_session(project_id) as session:
            adm._tracing().security_audit_record(session, event)
            session.commit()
    except Exception as exc:  # pylint: disable=W0703
        log.error("governed check_connection: the probe event was not recorded (%s)", type(exc).__name__)
        raise _deny("AUDIT_UNAVAILABLE") from None
    ticket = json.dumps({
        "project_id": project_id, "credential_ref": reference["ref_id"],
        "credential_generation": reference["generation"], "task_name": PROBE_TASK, "user_id": user_id,
    })
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


def _outcome(module, project_id, user_id, reference, answer) -> None:
    if answer.get("success"):
        outcome, reason = "success", SUCCEEDED
    elif answer.get("requires_authorization"):
        outcome, reason = "interaction_required", FAILED
    else:
        outcome, reason = ("denied" if answer.get("code") else "failed"), FAILED
    try:
        adm._tracing().security_audit_record_autocommit(_probe_event(
            module, decision="allow", reason_code=reason, outcome=outcome, project_id=project_id,
            caller_ref=f"user:{user_id}", user_ref=f"user:{user_id}", credential_ref=reference["ref_id"],
            credential_generation=reference["generation"]))
    except Exception as exc:  # pylint: disable=W0703
        log.error("governed check_connection: the outcome was not recorded (%s)", type(exc).__name__)


def _probe(module, project_id, configuration_uuid, config_type, user_id, known) -> dict:
    if not module.governance_profile.enabled:
        raise _deny("DEPENDENCY_UNAVAILABLE")
    _check_arguments(project_id, configuration_uuid, config_type, user_id)
    known.update(project_id=project_id, caller_ref=f"user:{user_id}", user_ref=f"user:{user_id}")
    try:
        audit_allowed = adm._tracing().security_audit_admission_allowed_local()
    except Exception:  # pylint: disable=W0703
        audit_allowed = False
    if not audit_allowed:
        raise _deny("AUDIT_UNAVAILABLE")
    _may_probe(user_id, project_id)
    _configuration, reference = _reference_of(project_id, configuration_uuid, config_type, user_id, known)
    required = {"task_envelope.v1", CAPABILITY_CREDENTIAL_SLOTS_V1, CAPABILITY_GOVERNED_OPS_POOL_V1}
    if not module.worker_capabilities.has_capable_worker(required):
        raise _deny("WORKER_CAPABILITY_UNAVAILABLE")
    # the stored configuration without its secret fields, and a slot for them
    try:
        settings = _stored_configuration(project_id, configuration_uuid, user_id)
        if settings.get("configuration_generation") != reference.get("configuration_generation"):
            raise _deny("CREDENTIAL_GENERATION_MISMATCH")   # changed between the facts and the read: no probe
        paths = secret_paths_for({config_type}, configuration_secret_fields)[config_type]
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
    _open_probe(module, project_id, user_id, probe_ref, reference)
    try:
        answer = shape_result(_run_probe(module, project_id, config_type, settings))
    finally:
        _close_probe(module, probe_ref)
    _outcome(module, project_id, user_id, reference, answer)
    adm._metric(CREDENTIAL_PROBE, "credential_probe_total", "decision", "allow",
                SUCCEEDED if answer.get("success") else FAILED)
    return answer


def governed_check_connection(module, project_id, configuration_uuid, config_type, user_id) -> dict:
    """Test a saved credential through the governed path. Never raises.

    ``{"success": True, ...}`` | ``{"success": False, "message"}`` (the provider failed) |
    ``{"success": False, "requires_authorization": True, "auth_metadata", "message"}`` |
    ``{"success": False, "code", "message"}`` (a refusal: permission, governance, capability, store)."""
    known = {}
    try:
        return _probe(module, project_id, configuration_uuid, config_type, user_id, known)
    except AdmissionDenied as denied:
        code = denied.reason_code
    except Exception as exc:  # pylint: disable=W0703
        log.error("governed check_connection: unexpected failure (%s)", type(exc).__name__)
        code = "DEPENDENCY_UNAVAILABLE"
    adm._record_deny(module, code, event_type="probe", **known)
    adm._metric(CREDENTIAL_PROBE, "credential_probe_total", "decision", "deny", code)
    return refusal(code)
