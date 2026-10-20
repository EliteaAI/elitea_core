"""Execution admission and the per-run security context (#6756, C-1009).

Before a governed run is dispatched, ``admit_execution`` proves who is asking and which approved
agent definition will run, and records that as one ``ExecutionSecurityContext`` row plus one audit
event in the same transaction. The worker later proves the run still holds that admission with
``validate_context`` before it builds an LLM client or touches a tool.

Everything fails closed: any step that cannot be proven denies, and a deny is audited on its own
transaction so a failing audit never lets a run through. No token, cookie, delegation material or
exception text from a dependency is written to the row, the audit event, a log line or a message.
"""
import re
import threading
import uuid
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Mapping, Optional

from pylon.core.tools import log
from sqlalchemy import func, select, update
from sqlalchemy.exc import IntegrityError
from tools import auth, config as c, db, this

from ..models.agent_identity import AgentIdentityBinding, ExecutionSecurityContext
from ..models.pd.admission import AdmissionRequest, ExecutionSecurityContextView
from ..models.pd.task_envelope import PROFILE_GOVERNED
from .credential_decision import (
    ALLOWED as CREDENTIAL_USE_ALLOWED, WHOLE_TARGET, CredentialDecisionContext, CredentialDecisionError,
    binding_ref, collect_credential_uses, decide_credential_uses, recheck_credentials, toolkit_secret_fields,
    uncovered_credentials, user_may_use_credentials,
)
from .definition_digest import DigestError, digest_for_version
from .exceptions import AdmissionDenied
from .governance import CONFIG_SECTION, is_governed
from .run_id import PREDICT_RUN_ID_KWARGS_KEY

ADMITTED_TASKS = frozenset({"indexer_agent", "indexer_predict_agent"})
SCHEMA_VERSION = 1
MAX_TRACKED_CONTEXTS = 10_000
TTL_RANGE_S = (60, 86400)
LIVE_STATES = ("admitted", "running")
# Codes that explain why a context was revoked; the worker is told the same reason, not a generic one
REVOKE_CODES = frozenset({
    "EPOCH_MISMATCH", "BINDING_DIGEST_MISMATCH", "DENIED_BINDING",
    "CREDENTIAL_REFERENCE_INACTIVE", "CREDENTIAL_GENERATION_MISMATCH", "DENIED_GRANT",
})
_CODE = re.compile(r"^[A-Z0-9_]{1,64}$")

MESSAGES = {
    "AUDIT_UNAVAILABLE": "The security audit trail is unavailable; the run was not started. Try again later.",
    "MANDATE_REQUIRED": "This run needs a user mandate that is not available yet; it was not started.",
    "IDENTITY_EVIDENCE_MISSING": "No verifiable user identity came with this run; it was not started.",
    "IDENTITY_SUBJECT_UNLINKED": "The signed-in user does not match the verified identity; the run was not started.",
    "IDENTITY_KIND_NOT_ADMITTED": "This kind of credential cannot run governed agents; the run was not started.",
    "IDENTITY_REVOKED": "This session or token was revoked; the run was not started.",
    "IDENTITY_EXPIRED": "Sign in again to run governed agents.",
    "IDENTITY_SIGNATURE_INVALID": "The user identity could not be verified; the run was not started.",
    "IDENTITY_AUDIENCE_MISMATCH": "The user identity could not be verified; the run was not started.",
    "IDENTITY_TENANT_MISMATCH": "The user identity could not be verified; the run was not started.",
    "DENIED_BINDING": "This agent version has no active identity binding; the run was not started.",
    "BINDING_DIGEST_MISMATCH": "The agent changed after its identity binding was approved; the run was not started.",
    "DEPENDENCY_UNAVAILABLE": "Identity service is unavailable; the run was not started. Try again.",
    "STALE_CONTEXT": "The admission for this run is no longer valid; the run was stopped.",
    "EPOCH_MISMATCH": "Access was revoked after this run was admitted; the run was stopped.",
    "UNKNOWN_SCHEMA_VERSION": "The admission record is not understood by this version; the run was stopped.",
    "CREDENTIAL_REFERENCE_REQUIRED": "This agent uses a credential that is not registered for governed use; the run was not started.",
    "CREDENTIAL_REFERENCE_INACTIVE": "A credential this agent uses is suspended or retired; the run was not started.",
    "CREDENTIAL_GENERATION_MISMATCH": "A credential changed after it was approved; the run was not started.",
    "CREDENTIAL_CLASS_NOT_ALLOWED": "Personal credentials cannot be used by governed agents.",
    "DENIED_GRANT": "This agent is not allowed to use one of its credentials for you.",
    "DENIED_RBAC": "You do not have permission to use this agent's credentials.",
}
GENERIC_MESSAGE = "The run was not started."


# --- configuration ---------------------------------------------------------------------------

@dataclass(frozen=True)
class AdmissionSettings:
    allow_runtime_domain_binding: bool = False
    admit_legacy_pat: bool = False
    context_ttl_s: int = 3600
    environment: Optional[str] = None


def load_admission_settings(config: Mapping) -> AdmissionSettings:
    """Operator settings of the governance section. Validated only when governance is enabled."""
    section = config.get(CONFIG_SECTION)
    if not isinstance(section, Mapping) or section.get("enabled") is not True:
        return AdmissionSettings()
    #
    defaults = AdmissionSettings()
    values = {}
    for key in ("allow_runtime_domain_binding", "admit_legacy_pat"):
        value = section.get(key, getattr(defaults, key))
        if not isinstance(value, bool):
            raise ValueError(f"{CONFIG_SECTION}.{key} must be a boolean (true/false), got {value!r}")
        values[key] = value
    ttl = section.get("context_ttl_s", defaults.context_ttl_s)
    if isinstance(ttl, bool) or not isinstance(ttl, int) or not TTL_RANGE_S[0] <= ttl <= TTL_RANGE_S[1]:
        raise ValueError(
            f"{CONFIG_SECTION}.context_ttl_s must be an integer from {TTL_RANGE_S[0]} to {TTL_RANGE_S[1]}, got {ttl!r}"
        )
    environment = section.get("environment")
    if environment == "":
        environment = None   # config.yml quotes the environment expansion, so an unset variable arrives as ""
    if environment is not None and (not isinstance(environment, str) or not 0 < len(environment.strip()) <= 64):
        raise ValueError(f"{CONFIG_SECTION}.environment must be null or a non-empty string of at most 64 characters")
    return AdmissionSettings(context_ttl_s=ttl, environment=environment, **values)


def check_governed_startup(module):
    """Refuse to start governed on a platform that cannot prove identity or record the decision."""
    if not is_governed(module):
        return
    problems = []
    if getattr(auth, "auth_mode", None) != "rpc":
        problems.append("auth.auth_mode must be 'rpc'")
    try:
        verifier_ready = bool(auth.identity_verifier.configured("session_user"))
    except Exception:  # pylint: disable=W0703
        verifier_ready = False
    if not verifier_ready:
        problems.append("auth.identity_verifier must be configured for session_user")
    try:
        audit_ready = callable(getattr(this.for_module("tracing").module, "security_audit_record", None))
    except Exception:  # pylint: disable=W0703
        audit_ready = False
    if not audit_ready:
        problems.append("the tracing plugin must provide security_audit_record")
    for plugin, method in (("secrets", "credential_use_facts"), ("configurations", "configuration_credential_facts")):
        try:
            provided = callable(getattr(this.for_module(plugin).module, method, None))
        except Exception:  # pylint: disable=W0703
            provided = False
        if not provided:
            problems.append(f"the {plugin} plugin must provide {method}")
    if getattr(c, "SECRETS_ENGINE", None) == "database" and not getattr(c, "SECRETS_MASTER_KEY", None):
        problems.append("SECRETS_MASTER_KEY must be set when SECRETS_ENGINE is 'database'")
    if problems:
        log.error("Credential governance cannot start: %s", "; ".join(problems))
        raise ValueError(f"{CONFIG_SECTION}: " + "; ".join(problems))


# --- counters --------------------------------------------------------------------------------

class _LabeledCounter:
    """prometheus_client Counter with labels when installed, always an in-process count as well."""

    def __init__(self, name, documentation, labelnames):
        self._counts = defaultdict(int)
        self._lock = threading.Lock()
        self._prom = None
        try:
            from prometheus_client import Counter  # pylint: disable=C0415,E0401
            self._prom = Counter(name, documentation, labelnames)
        except Exception:  # pylint: disable=W0718
            pass

    def inc(self, *labels):
        with self._lock:
            self._counts[labels] += 1
        if self._prom is not None:
            self._prom.labels(*labels).inc()

    def total(self, *labels):
        return self._counts[labels]


ADMISSION_DECISION = _LabeledCounter(
    "admission_decision_total", "Governed run admission decisions", ["decision", "reason_code"],
)
CREDENTIAL_DECISION = _LabeledCounter(
    "credential_decision_total", "Governed run credential-use decisions", ["decision", "reason_code"],
)
CONTEXT_VALIDATION = _LabeledCounter(
    "execution_context_validation_total", "Governed run context validations on Main", ["result", "reason_code"],
)


def _metric(counter, name, label_name, first_label, reason_code):
    counter.inc(first_label, reason_code)
    log.info("security_metric name=%s %s=%s reason_code=%s", name, label_name, first_label, reason_code)


# --- audit -----------------------------------------------------------------------------------

def _tracing():
    return this.for_module("tracing").module


def _event(**fields):
    return {key: value for key, value in fields.items() if value is not None}


def _record_deny(module, code, *, event_type="admission", **fields):
    """Audit a refusal on its own transaction. A failure to write it is logged; the refusal stands."""
    event = _event(
        event_type=event_type, decision="deny", reason_code=code,
        policy_revision=module.governance_profile.policy_revision,
        environment=module.admission_settings.environment, **fields,
    )
    try:
        _tracing().security_audit_record_autocommit(event)
    except Exception as exc:  # pylint: disable=W0703
        log.error("admission: the deny event %s was not recorded (%s)", code, type(exc).__name__)


def _denied(code, audit=None) -> AdmissionDenied:
    return AdmissionDenied(code, MESSAGES.get(code, GENERIC_MESSAGE), audit)


def _caller_ref(request: AdmissionRequest) -> str:
    return "public" if request.caller_id is None else f"{request.caller_type}:{request.caller_id}"


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _aware(moment: datetime) -> datetime:
    return moment if moment.tzinfo is not None else moment.replace(tzinfo=timezone.utc)


# --- admission -------------------------------------------------------------------------------

def _verify_identity(request: AdmissionRequest, settings: AdmissionSettings) -> dict:
    """Step 3: the verified principal of the caller, from evidence built out of the trusted holder only."""
    if request.caller_type == "user":
        reference = request.caller_reference.get_secret_value() if request.caller_reference else ""
        evidence = {"kind": "session_user", "user_id": request.caller_id, "auth_reference": reference}
    elif request.caller_type == "token":
        evidence = {"kind": "legacy_pat", "token_id": request.caller_id}
    else:
        raise _denied("IDENTITY_EVIDENCE_MISSING")
    admitted = {"session_user"} | ({"legacy_pat"} if settings.admit_legacy_pat else set())
    try:
        result = auth.verify_identity(evidence, admitted_kinds=admitted)
    except Exception as exc:  # pylint: disable=W0703
        log.error("admission: identity verification failed (%s)", type(exc).__name__)
        raise _denied("DEPENDENCY_UNAVAILABLE") from None
    if not isinstance(result, dict):
        raise _denied("DEPENDENCY_UNAVAILABLE")
    if not result.get("ok"):
        code = result.get("reason_code")
        raise _denied(code if isinstance(code, str) and _CODE.match(code) else "DEPENDENCY_UNAVAILABLE")
    principal = result.get("principal")
    if not isinstance(principal, dict):
        raise _denied("DEPENDENCY_UNAVAILABLE")
    if request.caller_type == "user" and principal.get("user_id") != request.caller_id:
        raise _denied("IDENTITY_SUBJECT_UNLINKED")
    return principal


def version_application_id(session, version_id):
    """The application a version row belongs to, or None when there is no such version."""
    from ..models.all import ApplicationVersion  # pylint: disable=C0415
    return session.execute(
        select(ApplicationVersion.application_id).where(ApplicationVersion.id == version_id)
    ).scalar_one_or_none()


def _find_binding(request: AdmissionRequest, settings: AdmissionSettings):
    """Step 4: the one active binding of this exact version, whose digest still matches the definition."""
    if request.version_id is None or request.application_id is None:
        raise _denied("DENIED_BINDING")
    modes = ["dedicated_agent", "elitea_native"]
    if settings.allow_runtime_domain_binding:
        modes.append("runtime_domain_binding")
    with db.get_session(request.binding_project_id) as session:
        if version_application_id(session, request.version_id) != request.application_id:
            raise _denied("DENIED_BINDING")
        rows = session.execute(
            select(AgentIdentityBinding.id, AgentIdentityBinding.revocation_epoch,
                   AgentIdentityBinding.definition_digest)
            .where(
                AgentIdentityBinding.project_id == request.binding_project_id,
                AgentIdentityBinding.application_id == request.application_id,
                AgentIdentityBinding.version_id == request.version_id,
                AgentIdentityBinding.state == "active",
                AgentIdentityBinding.identity_mode.in_(modes),
            )
        ).all()
        if len(rows) != 1:
            raise _denied("DENIED_BINDING")
        binding = rows[0]
        try:
            current = digest_for_version(session, request.version_id)
        except DigestError as exc:
            raise _denied("DENIED_BINDING" if exc.reason_code == "VERSION_NOT_FOUND" else "BINDING_DIGEST_MISMATCH") from None
        if current != binding.definition_digest:
            raise _denied("BINDING_DIGEST_MISMATCH")
    return binding


def _subject_of(principal: dict) -> dict:
    """Step 5: only a verified user becomes a delegation subject; an asserted one needs a mandate (#6947)."""
    evidence = principal.get("evidence")
    if not isinstance(evidence, dict) or evidence.get("type") != "verified_user":
        raise _denied("MANDATE_REQUIRED")
    if not evidence.get("tid") or not evidence.get("oid"):
        raise _denied("IDENTITY_EVIDENCE_MISSING")
    return {
        "kind": "delegation", "ref": f"idp:{evidence.get('tid')}:{evidence.get('oid')}",
        "delegation_ref": evidence.get("delegation_ref"), "tid": evidence.get("tid"),
    }


def _decide_credentials(module, request: AdmissionRequest, subject: dict, binding, payload, expires_at) -> list:
    """Step 5b: may this agent use each credential it uses, for this user? The allowed decisions (none for an
    agent without governed credentials), or ``AdmissionDenied`` for the first refusal."""
    context = CredentialDecisionContext(
        binding_project_id=request.binding_project_id, agent_binding_ref=str(binding.id), subject_ref=subject["ref"],
        policy_revision=module.governance_profile.policy_revision, now=_now(), expires_at=expires_at,
        can_use=user_may_use_credentials(request.caller_id, request.binding_project_id),
    )
    try:
        with db.get_session(request.binding_project_id) as session:
            candidates = collect_credential_uses(
                session, request.binding_project_id, request.version_id, payload,
                lambda tool_type: toolkit_secret_fields(tool_type, request.binding_project_id, request.caller_id))
        return decide_credential_uses(context, candidates)
    except CredentialDecisionError as exc:
        error = exc
    except Exception as exc:  # pylint: disable=W0703
        log.error("admission: the credential decision failed (%s)", type(exc).__name__)
        error = CredentialDecisionError("DEPENDENCY_UNAVAILABLE")
    _metric(CREDENTIAL_DECISION, "credential_decision_total", "decision", "deny", error.reason_code)
    raise _denied(error.reason_code, error.audit)


def _admit(module, request: AdmissionRequest, run_id, payload=None) -> ExecutionSecurityContextView:  # pylint: disable=R0914
    settings = module.admission_settings
    policy_revision = module.governance_profile.policy_revision
    # 1. the audit trail can take a decision
    try:
        audit_allowed = _tracing().security_audit_admission_allowed_local()
    except Exception:  # pylint: disable=W0703
        audit_allowed = False
    if not audit_allowed:
        raise _denied("AUDIT_UNAVAILABLE")
    # 2. a run with no person behind it needs a mandate
    if request.run_kind == "system":
        raise _denied("MANDATE_REQUIRED")
    if not run_id:
        raise _denied("DEPENDENCY_UNAVAILABLE")
    # 3. who is asking
    principal = _verify_identity(request, settings)
    # 4. which approved agent definition runs
    binding = _find_binding(request, settings)
    # 5. on whose behalf
    subject = _subject_of(principal)
    expires_at = _now() + timedelta(seconds=settings.context_ttl_s)
    # 5b. which credentials the run uses, and whether the agent may use them for this user
    decisions = _decide_credentials(module, request, subject, binding, payload, expires_at)
    # 6. context row and allow events, atomically
    epochs = principal.get("epochs") if isinstance(principal.get("epochs"), dict) else {}
    context_id = uuid.uuid4()
    caller = {"kind": request.caller_type, "ref": _caller_ref(request), "user_id": request.caller_id}
    for _ in range(3):
        with db.get_session(request.project_id) as session:
            try:
                attempt = 1 + (session.execute(
                    select(func.max(ExecutionSecurityContext.attempt))
                    .where(ExecutionSecurityContext.run_id == run_id)
                ).scalar() or 0)
                session.add(ExecutionSecurityContext(
                    id=context_id, schema_version=SCHEMA_VERSION, profile="governed",
                    policy_revision=policy_revision, tenant=subject["tid"], environment=settings.environment,
                    project_id=request.project_id, binding_project_id=request.binding_project_id,
                    entry_point=request.entry_point, caller=caller,
                    subject={key: subject[key] for key in ("kind", "ref", "delegation_ref")},
                    agent_binding_ref=binding.id, application_id=request.application_id,
                    version_id=request.version_id, definition_digest=binding.definition_digest,
                    run_id=run_id, attempt=attempt,
                    revocation_epochs={"user": epochs.get("user"), "token": epochs.get("token"),
                                       "binding": binding.revocation_epoch, "project": None,
                                       "credential": {d.credential_ref: d.reference_epoch for d in decisions} or None},
                    allowed_bindings=[binding_ref(d) for d in decisions],
                    trace_ref=request.trace_ref, expires_at=expires_at, state="admitted",
                ))
                for decision in decisions:
                    _tracing().security_audit_record(session, _event(
                        event_type="decision", decision="allow", reason_code=CREDENTIAL_USE_ALLOWED,
                        event_id=decision.decision_id, tenant=subject["tid"], project_id=request.project_id,
                        application_id=request.application_id, environment=settings.environment,
                        caller_ref=caller["ref"], user_ref=subject["ref"], agent_binding_ref=str(binding.id),
                        run_id=run_id, attempt=attempt, credential_ref=decision.credential_ref,
                        credential_generation=decision.credential_generation,
                        toolkit_instance_id=decision.toolkit_instance_id, target_ref=WHOLE_TARGET,
                        policy_revision=policy_revision, revocation_epoch=decision.reference_epoch,
                        traceparent=request.trace_ref,
                    ))
                _tracing().security_audit_record(session, _event(
                    event_type="admission", decision="allow", reason_code="ADMITTED",
                    tenant=subject["tid"], project_id=request.project_id, application_id=request.application_id,
                    environment=settings.environment, caller_ref=caller["ref"], user_ref=subject["ref"],
                    agent_binding_ref=str(binding.id), run_id=run_id, attempt=attempt,
                    policy_revision=policy_revision, revocation_epoch=binding.revocation_epoch,
                    traceparent=request.trace_ref,
                ))
                session.commit()
            except IntegrityError:
                # a concurrent admission of the same run took this attempt number: read the maximum again
                session.rollback()
                continue
            except Exception:
                session.rollback()
                raise
        break
    else:
        raise _denied("DEPENDENCY_UNAVAILABLE")
    for _ in decisions:
        _metric(CREDENTIAL_DECISION, "credential_decision_total", "decision", "allow", CREDENTIAL_USE_ALLOWED)
    return ExecutionSecurityContextView(
        context_ref=str(context_id), run_id=run_id, attempt=attempt, project_id=request.project_id,
        application_id=request.application_id, version_id=request.version_id,
        agent_binding_ref=str(binding.id), policy_revision=policy_revision, expires_at=expires_at,
    )


def admit_execution(module, request: AdmissionRequest, *, run_id: str, payload=None) -> ExecutionSecurityContextView:
    """Admit one governed run or raise ``AdmissionDenied``. Every decision is counted and audited.
    ``payload`` is the dispatched payload; it is cross-checked against the agent's stored credentials, never trusted."""
    try:
        view = _admit(module, request, run_id, payload)
    except AdmissionDenied as exc:
        error = exc
    except Exception as exc:  # pylint: disable=W0703
        log.error("admission: unexpected failure (%s)", type(exc).__name__)
        error = _denied("DEPENDENCY_UNAVAILABLE")
    else:
        _metric(ADMISSION_DECISION, "admission_decision_total", "decision", "allow", "ADMITTED")
        return view
    _record_deny(
        module, error.reason_code, project_id=request.project_id, application_id=request.application_id,
        caller_ref=_caller_ref(request), run_id=run_id if isinstance(run_id, str) else None,
        traceparent=request.trace_ref, **error.audit,
    )
    _metric(ADMISSION_DECISION, "admission_decision_total", "decision", "deny", error.reason_code)
    raise error


def admit_dispatch(module, task_name, call_kwargs, admission: Optional[AdmissionRequest]) -> Optional[str]:
    """Admit the run behind a ``start_task`` call. Returns the new ``context_ref``, or ``None`` when
    nothing was admitted here (governance off, a task that is not admitted, or a context inherited
    from the parent run). Sets ``context_ref`` in the envelope and in the task meta."""
    if not is_governed(module) or task_name not in ADMITTED_TASKS:
        return None
    payload = call_kwargs.setdefault("kwargs", {})
    meta = call_kwargs.get("meta")
    if not isinstance(meta, dict):
        meta = call_kwargs["meta"] = {}
    run_id = payload.get(PREDICT_RUN_ID_KWARGS_KEY)
    envelope = payload.get("envelope")
    #
    if not isinstance(envelope, dict) or envelope.get("security_profile") != PROFILE_GOVERNED:
        # no governed envelope to carry the context (e.g. a parent started before governance was
        # switched on): the worker would not check it, so the run is not started
        return _refuse_dispatch(module, "STALE_CONTEXT", call_kwargs, run_id)
    if envelope.get("context_ref"):
        # a child or reconcile dispatch runs under its parent's admission, which decided the parent's credentials only
        _refuse_uncovered_credentials(module, payload, envelope["context_ref"], call_kwargs, run_id)
        meta.setdefault("context_ref", envelope["context_ref"])
        return None
    if admission is None:
        return _refuse_dispatch(module, "IDENTITY_EVIDENCE_MISSING", call_kwargs, run_id)
    #
    if admission.trace_ref is None and isinstance(meta.get("traceparent"), str):
        admission = admission.model_copy(update={"trace_ref": meta["traceparent"]})
    view = admit_execution(module, admission, run_id=run_id, payload=payload)
    envelope["context_ref"] = view.context_ref
    meta["context_ref"] = view.context_ref
    return view.context_ref


def _parent_covered(project_id, context_ref) -> set:
    """The configuration uuids the parent context decided credentials for."""
    if isinstance(project_id, bool) or not isinstance(project_id, int):
        raise _denied("STALE_CONTEXT")
    try:
        context_id = uuid.UUID(str(context_ref))
    except ValueError:
        raise _denied("STALE_CONTEXT") from None
    with db.get_session(project_id) as session:
        entries = session.execute(
            select(ExecutionSecurityContext.allowed_bindings).where(ExecutionSecurityContext.id == context_id)
        ).scalar_one_or_none()
    if not isinstance(entries, list):
        raise _denied("STALE_CONTEXT")
    return {entry.get("configuration_uuid") for entry in entries if isinstance(entry, dict)}


def _refuse_uncovered_credentials(module, payload, context_ref, call_kwargs, run_id):
    """Refuse a dispatch under a parent context whose payload carries a credential the parent was not
    admitted for (a credential-bearing sub-agent, a conversation toolkit)."""
    project_id = (call_kwargs.get("meta") or {}).get("project_id") or payload.get("project_id")
    try:
        code = "CREDENTIAL_REFERENCE_REQUIRED" if uncovered_credentials(
            payload, lambda: _parent_covered(project_id, context_ref), project_id) else None
    except AdmissionDenied as exc:
        code = exc.reason_code
    except Exception as exc:  # pylint: disable=W0703
        log.error("admission: the inherited credentials could not be checked (%s)", type(exc).__name__)
        code = "DEPENDENCY_UNAVAILABLE"
    if code is not None:
        _metric(CREDENTIAL_DECISION, "credential_decision_total", "decision", "deny", code)
        _refuse_dispatch(module, code, call_kwargs, run_id)


def _refuse_dispatch(module, code, call_kwargs, run_id):
    """Deny a dispatch that has no admission request to audit against."""
    project_id = (call_kwargs.get("meta") or {}).get("project_id") or (call_kwargs.get("kwargs") or {}).get("project_id")
    _record_deny(
        module, code, project_id=project_id if isinstance(project_id, int) else None,
        run_id=run_id if isinstance(run_id, str) else None,
    )
    _metric(ADMISSION_DECISION, "admission_decision_total", "decision", "deny", code)
    raise _denied(code)


# --- context lifecycle -----------------------------------------------------------------------

def _set_state(session, context_id, state, reason=None, from_states=LIVE_STATES) -> int:
    """Conditional move of one context; the number of rows changed (0 when it already left ``from_states``)."""
    if state == "running":
        values = {"state": state,
                  "started_at": func.coalesce(ExecutionSecurityContext.started_at, func.now())}
    else:
        values = {"state": state, "ended_at": func.now()}
    if reason is not None:
        values["revoked_reason"] = reason
    return session.execute(
        update(ExecutionSecurityContext)
        .where(ExecutionSecurityContext.id == context_id, ExecutionSecurityContext.state.in_(from_states))
        .values(**values).execution_options(synchronize_session=False)
    ).rowcount


def revoke_binding_contexts(session, binding_id, reason="EPOCH_MISMATCH") -> int:
    """Revoke every live context of a binding in this session's schema. Does not commit."""
    return session.execute(
        update(ExecutionSecurityContext)
        .where(ExecutionSecurityContext.agent_binding_ref == binding_id,
               ExecutionSecurityContext.state.in_(LIVE_STATES))
        .values(state="revoked", revoked_reason=reason, ended_at=func.now())
        .execution_options(synchronize_session=False)
    ).rowcount


def revoke_mandate_contexts(session, mandate_id, reason="MANDATE_INACTIVE") -> int:
    """Revoke every live context of runs admitted under a mandate, in this session's schema. Does not commit."""
    probe = {"kind": "mandate", "ref": f"mandate:{mandate_id}"}
    return session.execute(
        update(ExecutionSecurityContext)
        .where(ExecutionSecurityContext.subject.contains(probe), ExecutionSecurityContext.state.in_(LIVE_STATES))
        .values(state="revoked", revoked_reason=reason, ended_at=func.now())
        .execution_options(synchronize_session=False)
    ).rowcount


def revoke_credential_contexts(session, credential_ref, reason="CREDENTIAL_REFERENCE_INACTIVE") -> int:
    """Revoke every live context in this session's schema that holds a decision for this credential
    reference. Does not commit. Contexts of other schemas fail at ``validate_context``."""
    probe = [{"credential_ref": str(uuid.UUID(str(credential_ref)))}]
    return session.execute(
        update(ExecutionSecurityContext)
        .where(ExecutionSecurityContext.state.in_(LIVE_STATES),
               ExecutionSecurityContext.allowed_bindings.contains(probe))
        .values(state="revoked", revoked_reason=reason, ended_at=func.now())
        .execution_options(synchronize_session=False)
    ).rowcount


def track_context(module, task_id, project_id, context_ref):
    """After ``start_task`` returned: remember which task runs the context, or revoke it if none started."""
    try:
        if task_id is None:
            with db.get_session(project_id) as session:
                _set_state(session, uuid.UUID(context_ref), "revoked", "NOT_DISPATCHED", ("admitted",))
                session.commit()
            return
        tracked = module._context_by_task  # pylint: disable=W0212
        tracked[task_id] = (project_id, context_ref)
        while len(tracked) > MAX_TRACKED_CONTEXTS:
            tracked.popitem(last=False)
    except Exception as exc:  # pylint: disable=W0703
        log.error("admission: could not track context of task %s (%s)", task_id, type(exc).__name__)


def complete_context(module, task_id):
    """A task stopped: close its context. A context the map lost is closed by its expiry."""
    if not is_governed(module):
        return
    found = module._context_by_task.pop(task_id, None)  # pylint: disable=W0212
    if found is None:
        return
    project_id, context_ref = found
    with db.get_session(project_id) as session:
        _set_state(session, uuid.UUID(context_ref), "completed")
        session.commit()


# --- validation on behalf of the worker ------------------------------------------------------

def _read_binding(project_id, binding_ref):
    with db.get_session(project_id) as session:
        return session.execute(
            select(AgentIdentityBinding.state, AgentIdentityBinding.revocation_epoch,
                   AgentIdentityBinding.definition_digest)
            .where(AgentIdentityBinding.id == binding_ref)
        ).one_or_none()


def _validate(project_id, context_ref, caller, known):  # pylint: disable=R0912,R0914,R0915
    if isinstance(project_id, bool) or not isinstance(project_id, int):
        raise _denied("STALE_CONTEXT")
    known["project_id"] = project_id
    try:
        context_id = uuid.UUID(str(context_ref))
    except ValueError:
        raise _denied("STALE_CONTEXT") from None
    if not isinstance(caller, dict):
        raise _denied("STALE_CONTEXT")
    if isinstance(caller.get("run_id"), str):
        known["run_id"] = caller["run_id"]
    #
    with db.get_session(project_id) as session:
        row = session.execute(
            select(ExecutionSecurityContext).where(ExecutionSecurityContext.id == context_id)
        ).scalar_one_or_none()
        if row is None:
            raise _denied("STALE_CONTEXT")
        known.update(application_id=row.application_id, agent_binding_ref=str(row.agent_binding_ref))
        if row.schema_version != SCHEMA_VERSION or row.profile != "governed":
            raise _denied("UNKNOWN_SCHEMA_VERSION")
        if (caller.get("kind") != "indexer_worker" or caller.get("task_name") not in ADMITTED_TASKS
                or caller.get("run_id") != row.run_id):
            raise _denied("STALE_CONTEXT")
        if row.state not in LIVE_STATES:
            reason = row.revoked_reason if row.state == "revoked" else None
            raise _denied(reason if reason in REVOKE_CODES else "STALE_CONTEXT")
        if _now() >= _aware(row.expires_at):
            raise _denied("STALE_CONTEXT")
        #
        def revoke(code):
            _set_state(session, context_id, "revoked", code)
            session.commit()
            return _denied(code)
        #
        stamped = row.revocation_epochs if isinstance(row.revocation_epochs, dict) else {}
        user_id = (row.caller or {}).get("user_id")
        if isinstance(user_id, bool) or not isinstance(user_id, int):
            raise revoke("EPOCH_MISMATCH")
        try:
            epochs = auth.read_identity_epochs("user", user_id)
        except Exception as exc:  # pylint: disable=W0703
            log.error("validate_context: user epochs unreadable (%s)", type(exc).__name__)
            epochs = None
        if not isinstance(epochs, dict):
            raise _denied("DEPENDENCY_UNAVAILABLE")  # unknown, not revoked: the run may be retried
        if not epochs.get("active") or epochs.get("user") != stamped.get("user"):
            raise revoke("EPOCH_MISMATCH")
        #
        binding = _read_binding(row.binding_project_id or project_id, row.agent_binding_ref)
        if binding is None or binding.state != "active":
            raise revoke("DENIED_BINDING")
        if binding.revocation_epoch != stamped.get("binding"):
            raise revoke("EPOCH_MISMATCH")
        if binding.definition_digest != row.definition_digest:
            raise revoke("BINDING_DIGEST_MISMATCH")
        #
        if isinstance(row.allowed_bindings, list) and row.allowed_bindings:
            try:
                code = recheck_credentials(
                    row.binding_project_id or project_id, row.allowed_bindings, (row.subject or {}).get("ref"),
                    str(row.agent_binding_ref), _now())
            except Exception as exc:  # pylint: disable=W0703
                log.error("validate_context: credential facts unreadable (%s)", type(exc).__name__)
                raise _denied("DEPENDENCY_UNAVAILABLE") from None   # unknown, not revoked: the run may be retried
            if code is not None:
                raise revoke(code)
        #
        if _set_state(session, context_id, "running", from_states=LIVE_STATES) != 1:
            raise _denied("STALE_CONTEXT")
        session.commit()
        return {
            "context_ref": str(context_id), "run_id": row.run_id, "attempt": row.attempt,
            "project_id": row.project_id, "application_id": row.application_id, "version_id": row.version_id,
            "agent_binding_ref": str(row.agent_binding_ref), "policy_revision": row.policy_revision,
            "expires_at": _aware(row.expires_at).isoformat(),
        }


def validate_context(module, project_id, context_ref, caller) -> dict:
    """Worker-bus handler: is this run still the one that was admitted? Never raises, returns no secrets.

    ``{"ok": True, "context": {...}}`` or ``{"ok": False, "reason_code", "message"}``.
    """
    known = {}
    try:
        context = _validate(project_id, context_ref, caller, known)
    except AdmissionDenied as denied:
        code = denied.reason_code
    except Exception as exc:  # pylint: disable=W0703
        log.error("validate_context: unexpected failure (%s)", type(exc).__name__)
        code = "DEPENDENCY_UNAVAILABLE"
    else:
        _metric(CONTEXT_VALIDATION, "execution_context_validation_total", "result", "allow", "VALIDATED")
        return {"ok": True, "context": context}
    _record_deny(module, code, event_type="decision", **known)
    _metric(CONTEXT_VALIDATION, "execution_context_validation_total", "result", "deny", code)
    return {"ok": False, "reason_code": code, "message": MESSAGES.get(code, GENERIC_MESSAGE)}
