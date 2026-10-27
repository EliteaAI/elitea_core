"""Execution admission and the per-run security context (#6756, C-1009).

Before a governed run is dispatched, ``admit_execution`` proves who is asking and which approved
agent definition will run, and records that as one ``ExecutionSecurityContext`` row plus one audit
event in the same transaction. The worker later proves the run still holds that admission with
``validate_context`` before it builds an LLM client or touches a tool.

Everything fails closed: any step that cannot be proven denies, and a deny is audited on its own
transaction so a failing audit never lets a run through. No token, cookie, delegation material or
exception text from a dependency is written to the row, the audit event, a log line or a message.

A run from the agent or pipeline editor's test chat (entry point ``editor_chat``, #7213) may be admitted as a
``development`` run when the operator allows it: the version needs no binding, the subject is the signed-in author,
and each credential is released only if the author could already use it (a personal credential, or a project credential
the author's subject holds an active grant on, whatever the binding). Nothing is released by virtue of the draft, and a
child of such a run inherits exactly its decisions.

Runs under an autonomous mandate (#6947) are also stopped by Main: once a minute ``enforce_run_deadlines`` stops a
tracked task that ran past its mandate's time limit, or whose context was revoked meanwhile. Known limits: only the
tasks this process dispatched and tracks are reached (children that inherit a mandate context are not tracked: a
revocation means no new starts, and a running child finishes or hits its parent's deadline), and the map is lost when
Main restarts (the context ``expires_at`` still bounds any restart).
"""
import contextlib
import contextvars
import copy
import importlib
import re
import threading
import time
import uuid
from collections import defaultdict, deque, namedtuple
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from typing import Optional, Union

from pylon.core.tools import log
from sqlalchemy import func, select, update
from sqlalchemy.exc import IntegrityError
from tools import auth, config as c, db, this

from ..models.agent_identity import AgentIdentityBinding, AutonomousMandate, ExecutionSecurityContext
from ..models.pd.admission import (
    ENTRY_EDITOR_CHAT, ENTRY_INDEX_ON_DEMAND, ENTRY_TOOLKIT_TEST, AdmissionRequest, ExecutionSecurityContextView,
)
from ..models.pd.task_envelope import CAPABILITY_ENVELOPE_V1, PROFILE_GOVERNED
from .credential_decision import (
    ALLOWED as CREDENTIAL_USE_ALLOWED, DEVELOPMENT_GRANT_REQUIRED, PROFILE_DEVELOPMENT, USE_PERMISSION, WHOLE_TARGET,
    CredentialDecisionContext, CredentialDecisionError, _uuid_text, binding_ref, collect_credential_uses, decide_credential_uses,
    configurations_in, read_configuration_facts, recheck_credentials, recheck_development, toolkit_secret_fields,
    user_may_use_credentials, walk_scope,
)
from .credential_slots import FREE_FORM_SECRET_MARK, prefetched_tool_lists, seal_payload
from .definition_digest import DIGEST_VERSION, DigestError, definition_digest, digest_for_version, norm_json, toolkit_rows
from .exceptions import AdmissionDenied
from .governance import MODE_AUDIT_ONLY, governance_lifecycle_active, is_auditing, is_governed, is_unavailable
from .governance_settings import Check
from . import internal_obo
from .mandate_state import expire_if_due, suspend_owner_mandates
from .run_id import PREDICT_RUN_ID_KWARGS_KEY
from .trigger_principal import OPERATION_OF
from .worker_capabilities import GOVERNED_TASK_CAPABILITIES

TOOLKIT_TASK = "indexer_test_toolkit_tool"   # a toolkit Test run and on-demand indexing (#7229): one toolkit, no agent
ADMITTED_TASKS = frozenset({"indexer_agent", "indexer_predict_agent", TOOLKIT_TASK})
# The entry points a development run may come from (#7213, #7229): the editor's test chat, and a person testing a toolkit
# or indexing it on demand. A toolkit run has no other profile: it is never a governed (bound) run
TOOLKIT_ENTRY_POINTS = frozenset({ENTRY_TOOLKIT_TEST, ENTRY_INDEX_ON_DEMAND})
DEVELOPMENT_ENTRY_POINTS = frozenset({ENTRY_EDITOR_CHAT, *TOOLKIT_ENTRY_POINTS})
# Passed as ``admission`` by the platform's own read of chat attachments (a built-in toolkit with no credential): such a
# dispatch is not admitted, and its payload must be shown to carry no credential (sealed with no decisions)
CREDENTIAL_FREE = type("CredentialFreeDispatch", (), {"__repr__": lambda self: "CREDENTIAL_FREE"})()
SCHEMA_VERSION = 1
MAX_TRACKED_CONTEXTS = 10_000
CREDENTIAL_FACTS_MIN_VERSION = 3   # the secrets plugin's credential_use_facts shape this module reads
LIVE_STATES = ("admitted", "running")
# Codes that explain why a context was revoked; the worker is told the same reason, not a generic one
REVOKE_CODES = frozenset({
    "EPOCH_MISMATCH", "BINDING_DIGEST_MISMATCH", "DENIED_BINDING",
    "CREDENTIAL_REFERENCE_INACTIVE", "CREDENTIAL_GENERATION_MISMATCH", "DENIED_GRANT", DEVELOPMENT_GRANT_REQUIRED,
    "MANDATE_INACTIVE", "MANDATE_EXPIRED", "RUNTIME_LIMIT_EXCEEDED",
})
PROFILES = frozenset({"governed", PROFILE_DEVELOPMENT})   # the context profiles this version understands
DEVELOPMENT_LABEL = "development:"   # + the definition digest: what a development run has instead of a binding ref
AUTHOR_PERMISSION = "models.applications.application.update"   # what the editor itself needs: who may author
_CODE = re.compile(r"^[A-Z0-9_]{1,64}$")
STOP_CHUNK = 500   # context ids per query of the sweep
INHERITED_KEY = "c1009:inh:"   # set when a context is reopened for its children: the release cap reads it
# When Main stops a mandate run, and the code of the bound that applied (RUNTIME_LIMIT_EXCEEDED or MANDATE_EXPIRED)
RunDeadline = namedtuple("RunDeadline", "at code")
PREDICT_PERMISSION = "models.applications.predict.post"
# The mandate operation each trigger entry point needs (the scheduler and both webhook kinds, #6947)
MANDATE_OPERATION_OF = {"schedule": OPERATION_OF["pipeline_schedule"], "webhook": OPERATION_OF["pipeline_webhook"]}
SUBJECT_COLUMNS = ("kind", "ref", "delegation_ref", "owner", "evidence")   # what a context row remembers of its subject
MANDATE_COLUMNS = (
    AutonomousMandate.id, AutonomousMandate.project_id, AutonomousMandate.application_id,
    AutonomousMandate.agent_version, AutonomousMandate.agent_binding_ref, AutonomousMandate.owner_user_id,
    AutonomousMandate.owner_user_ref, AutonomousMandate.allowed_operations, AutonomousMandate.max_runtime_s,
    AutonomousMandate.valid_until, AutonomousMandate.state, AutonomousMandate.revocation_epoch,
)

MESSAGES = {
    "AUDIT_UNAVAILABLE": "The security audit trail is unavailable; the run was not started. Try again later.",
    "MANDATE_REQUIRED": "No approved mandate or delegation covers this run; it was not started.",
    "AUTHORIZATION_REQUIRED": "This schedule runs on a user's behalf, which needs the user's renewed authorization; "
                              "it was not started.",
    "DENIED_OPERATION": "The mandate does not allow this kind of trigger; the run was not started.",
    "MANDATE_INACTIVE": "The mandate for this autonomous run is suspended or revoked; the run was not started.",
    "MANDATE_EXPIRED": "The mandate for this autonomous run has expired; renew it to run again.",
    "RUNTIME_LIMIT_EXCEEDED": "The run exceeded the time its mandate allows and was stopped.",
    "IDENTITY_EVIDENCE_MISSING": "No verifiable user identity came with this run; it was not started.",
    "IDENTITY_SUBJECT_UNLINKED": "The signed-in user does not match the verified identity; the run was not started.",
    "IDENTITY_KIND_NOT_ADMITTED": "Runs started with an API token (the built-in Elitea tools included) are refused until "
                                  "legacy API tokens are admitted or the run carries its own identity (#7214); the run "
                                  "was not started.",
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
    "CREDENTIAL_CLASS_NOT_ALLOWED": "This agent uses a personal credential that belongs to someone else; the run was not started.",
    "DENIED_GRANT": "This agent is not allowed to use one of its credentials for you.",
    "DENIED_RBAC": "You do not have permission to use this agent's credentials.",
    # development runs (#7213): the editor's test chat of an unbound version runs on the author's own rights only
    DEVELOPMENT_GRANT_REQUIRED: "This credential needs a grant before a draft can use it.",
    "DENIED_DEVELOPMENT_CHILD": "A sub-agent of this draft asked for a credential that was not checked for it when the "
                                "draft started (only the sub-agents the draft calls are checked, each on its own); the "
                                "run was not started.",
    "DEVELOPMENT_RUNS_DISABLED": "Toolkit tests and on-demand indexing run on your own credentials as development runs, "
                                 "which are switched off on this platform; the run was not started.",
    # the release of credential material to a worker (#6946); fixed per code, no titles, ids or field names
    "RATE_LIMITED": "This run asked for its credentials too often and was stopped.",
    "UNSUPPORTED_AUTH_MODE": "This toolkit cannot use a governed credential yet; the run was not started.",
    "WORKER_CAPABILITY_UNAVAILABLE": "No worker can apply governed credentials right now; the run was not started.",
    # the run token of the internal Elitea MCP (#7214)
    "INTERNAL_TOKEN_UNAVAILABLE": "The built-in Elitea tools of this agent cannot get the run's own identity right now; "
                                  "the run was not started. Try again later.",
    "INTERNAL_MCP_ORIGIN_NOT_ALLOWED": "An Elitea MCP toolkit of this agent points to an address the platform does not "
                                       "serve its tools from; the run was not started. Use the built-in Elitea tools.",
    **dict.fromkeys(
        ("STORE_DENIED", "STORE_SEALED", "STORE_UNAVAILABLE", "STORE_TLS_INVALID", "STORE_NOT_FOUND",
         "STORE_VERSION_CONFLICT"),
        "The credential store is unavailable; the run was not started. Try again later."),
}
GENERIC_MESSAGE = "The run was not started."


# --- configuration ---------------------------------------------------------------------------

@dataclass(frozen=True)
class AdmissionSettings:
    allow_runtime_domain_binding: bool = False
    admit_legacy_pat: bool = False
    allow_development_runs: bool = False   # the editor's test chat may run an unbound version as the author (#7213)
    context_ttl_s: int = 3600
    environment: Optional[str] = None


class _ProbeSession:
    """A session that stores nothing: handed to the audit method with a probe event, it shows whether the audit schema
    accepts the event (it validates before it adds) without writing a row."""
    is_probe = True

    def add(self, row):
        """Drop the row."""


def governance_preflight(module, table_read="ok", mode=None) -> list:  # pylint: disable=R0912,R0915
    """What audit only and enforce depend on, as a fixed list of checks with stable ids (``Check``). Local attribute
    checks only: nothing is called that reads or writes data. A failing blocking check keeps the mode from being
    switched on, and makes governed admissions ``DEPENDENCY_UNAVAILABLE`` while it is on; a warning limits a feature.

    ``mode`` is the mode the list is for: ``audit_schema`` blocks audit only, which cannot record what it rehearsed
    without the ``would_deny`` field, and is a warning for any other mode (enforce never writes the field)."""
    checks = []

    def add(check_id, ok, code, blocking=True):
        checks.append(Check(check_id, "ok" if ok else ("fail" if blocking else "warn"), "OK" if ok else code, blocking))

    try:
        verifier_ready = bool(auth.identity_verifier.configured("session_user"))
    except Exception:  # pylint: disable=W0703
        verifier_ready = False
    # the mandate owner proof and the owner's RBAC (#6947) belong to the same identity services
    add("identity_verifier",
        verifier_ready and all(callable(getattr(auth, name, None)) for name in
                               ("get_user_by_idp_subject", "get_user_permissions")), "NOT_CONFIGURED")
    add("auth_rpc_mode", getattr(auth, "auth_mode", None) == "rpc", "NOT_RPC")
    try:
        tracing = this.for_module("tracing").module
        audit_ready = callable(getattr(tracing, "security_audit_record", None))
    except Exception:  # pylint: disable=W0703
        tracing, audit_ready = None, False
    add("security_audit", audit_ready, "UNAVAILABLE")
    try:
        schema_current = audit_ready and tracing.security_audit_record(
            _ProbeSession(), {"event_type": "lifecycle", "reason_code": "GOVERNANCE_SCHEMA_PROBE", "would_deny": False,
                              "outcome": "success"}) is not None
    except Exception:  # pylint: disable=W0703
        schema_current = False
    add("audit_schema", schema_current, "OUTDATED" if audit_ready else "UNAVAILABLE",
        blocking=mode == MODE_AUDIT_ONLY)
    try:
        backlog_ok = audit_ready and bool(tracing.security_audit_admission_allowed_local())
    except Exception:  # pylint: disable=W0703
        backlog_ok = False
    add("audit_backlog", backlog_ok, "OVER_LIMIT" if audit_ready else "UNAVAILABLE")
    provided = True
    for plugin, method in (
            ("secrets", "credential_use_facts"), ("configurations", "configuration_credential_facts"),
            ("secrets", "release_credential_material"), ("configurations", "configuration_secret_fields")):
        try:
            provided = provided and callable(getattr(this.for_module(plugin).module, method, None))
        except Exception:  # pylint: disable=W0703
            provided = False
    try:
        from tools import secret_stores  # pylint: disable=C0415
        provided = provided and callable(importlib.import_module(".transport", secret_stores.__name__).seal)
    except Exception:  # pylint: disable=W0703
        provided = False   # the reply that carries released material is sealed to the worker (#6946)
    outdated = False
    try:
        facts_version = this.for_module("secrets").module.credential_facts_version()
        current = isinstance(facts_version, int) and not isinstance(facts_version, bool)
        outdated = current and facts_version < CREDENTIAL_FACTS_MIN_VERSION   # v2: grant subject_kind; v3: personal references (#7119)
        provided = provided and current and not outdated
    except Exception:  # pylint: disable=W0703
        provided = False
    add("credential_facts", provided, "OUTDATED" if outdated else "NOT_LOADED")
    add("master_key", not (getattr(c, "SECRETS_ENGINE", None) == "database" and not getattr(c, "SECRETS_MASTER_KEY", None)),
        "NOT_SET")
    add("settings_table", table_read == "ok", "MISSING_TABLE" if table_read == "missing_table" else "UNAVAILABLE")
    add("corporate_vault", bool(getattr(c, "CORPORATE_VAULT_ADDR", None)), "NOT_CONFIGURED", blocking=False)
    required = frozenset({CAPABILITY_ENVELOPE_V1, *GOVERNED_TASK_CAPABILITIES["indexer_agent"]})
    try:
        worker_online = bool(module.worker_capabilities.has_capable_worker(required))
    except Exception:  # pylint: disable=W0703
        worker_online = False
    add("governed_worker", worker_online, "NONE_ONLINE", blocking=False)
    return checks


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


class _LabeledHistogram:
    """prometheus_client Histogram with labels when installed, always an in-process count and sum as well."""

    def __init__(self, name, documentation, labelnames):
        self._observations = defaultdict(lambda: [0, 0.0])
        self._lock = threading.Lock()
        self._prom = None
        try:
            from prometheus_client import Histogram  # pylint: disable=C0415,E0401
            self._prom = Histogram(name, documentation, labelnames)
        except Exception:  # pylint: disable=W0718
            pass

    def observe(self, seconds, *labels):
        with self._lock:
            entry = self._observations[labels]
            entry[0] += 1
            entry[1] += seconds
        if self._prom is not None:
            self._prom.labels(*labels).observe(seconds)

    def count(self, *labels):
        return self._observations[labels][0] if labels in self._observations else 0


ADMISSION_DECISION = _LabeledCounter(
    "admission_decision_total", "Governed run admission decisions", ["decision", "reason_code"],
)
CREDENTIAL_DECISION = _LabeledCounter(
    "credential_decision_total", "Governed run credential-use decisions", ["decision", "reason_code"],
)
# How long one admission takes, in enforce and in audit only: the measurement a customer compares before switching on
ADMISSION_DURATION = _LabeledHistogram(
    "admission_duration_seconds", "Time to decide one run admission", ["mode", "decision"],
)
CONTEXT_VALIDATION = _LabeledCounter(
    "execution_context_validation_total", "Governed run context validations on Main", ["result", "reason_code"],
)
RUN_STOP = _LabeledCounter(
    "governed_run_stop_total", "Governed runs Main tried to stop (result: stopped or stop_failed)",
    ["result", "reason_code"],
)


def _metric(counter, name, label_name, first_label, reason_code):
    counter.inc(first_label, reason_code)
    log.info("security_metric name=%s %s=%s reason_code=%s", name, label_name, first_label, reason_code)


# --- audit -----------------------------------------------------------------------------------

def _tracing():
    return this.for_module("tracing").module


def _event(**fields):
    return {key: value for key, value in fields.items() if value is not None}


def _record_deny(module, code, *, event_type="admission", would_deny=False, **fields):
    """Audit a refusal on its own transaction. A failure to write it is logged; the refusal stands.

    ``would_deny``: audit only. Nothing was refused, so the event says what happened (``allow``) and what enforce would
    have said (``reason_code``, ``would_deny=true``)."""
    # a run admitted earlier is reported under the revision it was admitted with, not under today's
    fields.setdefault("policy_revision", module.governance_profile.policy_revision)
    fields.setdefault("environment", module.admission_settings.environment)
    event = _event(
        event_type=event_type, decision="allow" if would_deny else "deny", reason_code=code,
        would_deny=True if would_deny else None, **fields,
    )
    try:
        _tracing().security_audit_record_autocommit(event)
    except Exception as exc:  # pylint: disable=W0703
        log.error("admission: the %s event %s was not recorded (%s)", "would-deny" if would_deny else "deny", code,
                  type(exc).__name__)


def _denied(code, audit=None) -> AdmissionDenied:
    return AdmissionDenied(code, MESSAGES.get(code, GENERIC_MESSAGE), audit)


def _caller_ref(request: AdmissionRequest) -> str:
    if request.caller_type == "trigger":
        return request.trigger_ref or "trigger"
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
    """Step 4: the one active binding of this exact version, whose digest still matches the definition. One session
    reads everything the admission needs of the agent: the version (once), the binding and the toolkit rows (once). The
    binding and the toolkit rows are returned (the rows feed the credential decision, which reads nothing again)."""
    if request.version_id is None or request.application_id is None:
        raise _denied("DENIED_BINDING")
    from ..models.all import ApplicationVersion  # pylint: disable=C0415
    modes = _identity_modes(settings)
    with db.get_session(request.binding_project_id) as session:
        version = session.query(ApplicationVersion).filter(ApplicationVersion.id == request.version_id).first()
        if version is None or version.application_id != request.application_id:
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
        mappings = toolkit_rows(session, request.version_id)
        try:
            current = digest_for_version(session, request.version_id, (version, mappings))
        except DigestError as exc:
            raise _denied("DENIED_BINDING" if exc.reason_code == "VERSION_NOT_FOUND" else "BINDING_DIGEST_MISMATCH") from None
        if current != binding.definition_digest:
            raise _denied("BINDING_DIGEST_MISMATCH")
    return binding, mappings


def _identity_modes(settings: AdmissionSettings) -> list:
    """The identity modes a binding may have to admit a run."""
    return ["dedicated_agent", "elitea_native", *(["runtime_domain_binding"] if settings.allow_runtime_domain_binding else [])]


# What a development run has where a governed one has its binding: no id, no epoch, the digest of what runs (#7213)
Draft = namedtuple("Draft", "id revocation_epoch definition_digest")


def _binding_label(binding) -> str:
    """How the audit trail names what was admitted: the binding id, or ``development:<digest>`` for a draft."""
    return str(binding.id) if binding.id is not None else DEVELOPMENT_LABEL + binding.definition_digest


def _read_draft(request: AdmissionRequest):
    """Step 4 of a development run (#7213): the definition that runs, with no binding to hold it to. One session reads
    the version (it must exist and belong to the application), its toolkit rows and the digest of both; the rows feed the
    credential decision as in ``_find_binding``."""
    if request.version_id is None or request.application_id is None:
        raise _denied("DENIED_BINDING")
    from ..models.all import ApplicationVersion  # pylint: disable=C0415
    with db.get_session(request.binding_project_id) as session:
        version = session.query(ApplicationVersion).filter(ApplicationVersion.id == request.version_id).first()
        if version is None or version.application_id != request.application_id:
            raise _denied("DENIED_BINDING")
        mappings = toolkit_rows(session, request.version_id)
        try:
            digest = digest_for_version(session, request.version_id, (version, mappings))
        except DigestError as exc:
            raise _denied("DENIED_BINDING" if exc.reason_code == "VERSION_NOT_FOUND" else "DEPENDENCY_UNAVAILABLE") from None
    return Draft(None, None, digest), mappings


def _read_toolkit(request: AdmissionRequest):
    """Step 4 of a toolkit run (#7229): the stored toolkit the run tests or indexes, with no binding to hold it to. One
    read of the toolkit row in its project; it is the authority on which credential the run selects (the dispatched
    toolkit is cross-checked against it, as an agent's toolkits are against its version). The digest is of the stored
    toolkit: its type and settings, which hold credential references only. A toolkit that is not saved in the project
    declares nothing, so any credential the dispatch carries is refused."""
    from ..models.elitea_tools import EliteATool  # pylint: disable=C0415
    with db.get_session(request.binding_project_id) as session:
        tool = None if request.toolkit_id is None else \
            session.query(EliteATool).filter(EliteATool.id == request.toolkit_id).first()
        try:
            digest = definition_digest({
                "v": DIGEST_VERSION, "kind": "toolkit", "toolkit_id": request.toolkit_id,
                "type": getattr(tool, "type", None), "settings": norm_json(getattr(tool, "settings", None) or {})})
        except DigestError:
            raise _denied("DEPENDENCY_UNAVAILABLE") from None
    return Draft(None, None, digest), [] if tool is None else [(None, tool)]


# A sub-agent admitted with its parent (#7227): its context id, the context it was called from, the ancestor contexts
# (the run's first), what runs and what it was decided
ChildAdmission = namedtuple(
    "ChildAdmission", "key context_id parent_id lineage application_id version_id binding decisions")


def _child_order(payload, prefetched) -> list:
    """``[(key, key of its caller or None for the run)]`` of the prefetched sub-agents, each after the one that calls
    it: the walk the prefetch made (the run's application tools, then each sub-agent's own). Dict reads only."""
    application = payload.get("application") if isinstance(payload.get("application"), dict) else {}
    details = application.get("version_details") if isinstance(application.get("version_details"), dict) else {}
    own = [tool for tools in (payload.get("tools"), details.get("tools")) if isinstance(tools, list) for tool in tools]
    parent_of, order, queue = {}, [], deque([(None, own)])
    while queue:
        parent, tools = queue.popleft()
        for tool in tools:
            settings = tool.get("settings") if isinstance(tool, dict) and tool.get("type") == "application" else None
            if not isinstance(settings, dict):
                continue
            key = f"{settings.get('application_id')}:{settings.get('application_version_id')}"
            if key in prefetched and key not in parent_of and key != parent:
                parent_of[key] = parent
                order.append(key)
                queue.append((key, prefetched[key]))
    order += [key for key in prefetched if key not in parent_of]   # nothing calls it: a sub-agent of the run
    return [(key, parent_of.get(key)) for key in order]


def _read_children(request: AdmissionRequest, order, settings: AdmissionSettings) -> list:
    """Step 4 of the sub-agents (#7227), in one session: for each, its version (it must belong to the application its
    key names), its active bindings, its toolkit rows and its digest. ``[(key, parent key, application id, version id,
    binding or Draft, rows)]``: a sub-agent is bound when exactly one active binding still matches its definition; a key
    whose version does not exist (or belongs to another application) has ``None`` for both."""
    from ..models.all import ApplicationVersion  # pylint: disable=C0415
    modes, read = _identity_modes(settings), []
    with db.get_session(request.binding_project_id) as session:
        for key, parent in order:
            try:
                application_id, version_id = (int(part) for part in key.split(":"))
            except ValueError:
                raise _denied("DENIED_BINDING") from None
            version = session.query(ApplicationVersion).filter(ApplicationVersion.id == version_id).first()
            if version is None or version.application_id != application_id:
                read.append((key, parent, application_id, version_id, None, None))   # no such version: never admitted
                continue
            bindings = session.execute(
                select(AgentIdentityBinding.id, AgentIdentityBinding.revocation_epoch,
                       AgentIdentityBinding.definition_digest)
                .where(AgentIdentityBinding.project_id == request.binding_project_id,
                       AgentIdentityBinding.application_id == application_id,
                       AgentIdentityBinding.version_id == version_id,
                       AgentIdentityBinding.state == "active", AgentIdentityBinding.identity_mode.in_(modes))
            ).all()
            mappings = toolkit_rows(session, version_id)
            try:
                digest = digest_for_version(session, version_id, (version, mappings))
            except DigestError as exc:
                raise _denied("DENIED_BINDING" if exc.reason_code == "VERSION_NOT_FOUND"
                              else "DEPENDENCY_UNAVAILABLE") from None
            bound = len(bindings) == 1 and bindings[0].definition_digest == digest
            read.append((key, parent, application_id, version_id, bindings[0] if bound else Draft(None, None, digest),
                         mappings))
    return read


def _uncovered(prefetched, decisions) -> bool:
    """Does a prefetched sub-agent carry a configuration the run's own decisions do not name (toolkit, field and
    uuid)? Dict reads only."""
    decided = {(d.toolkit_instance_id, d.field, d.configuration_uuid) for d in decisions}
    for tools in prefetched.values():
        for tool in tools:
            settings = tool.get("settings")
            if not isinstance(settings, dict):
                continue
            top = {id(value): field for field, value in settings.items() if isinstance(value, dict)}
            for found in configurations_in(settings):
                data = found["data"]
                if (str(tool.get("id")), top.get(id(data)), _uuid_text(data.get("configuration_uuid"))) not in decided:
                    return True
    return False


def _admit_children(  # pylint: disable=R0913,R0914
    module, request: AdmissionRequest, subject, payload, expires_at, root_id, *, development, shadow, can_use,
    decisions=(),
) -> list:
    """Step 5c (#7227): the sub-agents the run calls in its own process, each decided on its own. A bound one (one active
    binding that still matches) is a governed child on its own binding and grants, for the same person (never a
    mandate): the parent's profile does not reach it. Under a development parent any other one is a development child
    of the same author with the same per-turn rule; under a governed parent it is not admitted, so a credential it
    carries has no decision and is refused at the seal (no development inside a governed run).

    A governed parent reads nothing here unless a sub-agent carries a configuration its own decisions do not name
    (``decisions``: a sub-agent that uses the run's own toolkit is sealed against the run's decision, as before). The
    sub-agents are those the prefetch expanded (same project, its depth and node limits). ``[ChildAdmission]``, or
    ``AdmissionDenied`` for the first refusal, which refuses the whole run."""
    prefetched = prefetched_tool_lists(payload)
    if not prefetched or (not development and not _uncovered(prefetched, decisions)):
        return []
    read = _read_children(request, _child_order(payload, prefetched), module.admission_settings)
    admitted, children = {}, []
    for key, parent, application_id, version_id, binding, rows in read:
        if binding is None and development:
            raise _denied("DENIED_BINDING")   # a sub-agent of a draft that is not a version of its application
        if binding is None or (not development and binding.id is None):
            continue
        while parent is not None and parent not in admitted:   # the nearest caller that has a context of its own
            parent = next(item[1] for item in read if item[0] == parent)
        lineage = [*admitted[parent].lineage, str(admitted[parent].context_id)] if parent else [str(root_id)]
        decisions = _decide_credentials(
            module, request.model_copy(update={"application_id": application_id, "version_id": version_id}), subject,
            binding, {"project_id": payload.get("project_id"), "tools": prefetched[key]}, expires_at, shadow=shadow,
            rows=rows, can_use=can_use)
        child = ChildAdmission(key, uuid.uuid4(), admitted[parent].context_id if parent else root_id, lineage,
                               application_id, version_id, binding, decisions)
        admitted[key] = child
        children.append(child)
    return children


def _author_permissions(request: AdmissionRequest) -> set:
    """Step 4a of a development run: what the author may do in the agent's project, read once (the editor's own right
    and ``credentials.use`` are both in it)."""
    try:
        return set(auth.get_user_permissions(request.caller_id, mode="default", project_id=request.binding_project_id))
    except Exception as exc:  # pylint: disable=W0703
        log.error("admission: the author permissions could not be read (%s)", type(exc).__name__)
        raise _denied("DEPENDENCY_UNAVAILABLE") from None


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


def _decide_credentials(  # pylint: disable=R0913
    module, request: AdmissionRequest, subject: dict, binding, payload, expires_at, shadow=False, rows=None,
    can_use=None,
) -> list:
    """Step 5b: may this agent use each credential it uses, for this user? The allowed decisions (none for an
    agent without governed credentials), or ``AdmissionDenied`` for the first refusal. ``shadow``: audit only, the
    refusal is counted as a ``would_deny``. ``rows``: the toolkit rows ``_find_binding`` has read. ``can_use``: the
    ``credentials.use`` answer when the caller read the permissions already (a development run, #7213: the rule is then
    the author's own right, not a grant to the binding)."""
    user_id = subject.get("user_id", request.caller_id)   # a mandate acts for its owner
    context = CredentialDecisionContext(
        binding_project_id=request.binding_project_id, agent_binding_ref=_binding_label(binding), subject_ref=subject["ref"],
        policy_revision=module.governance_profile.policy_revision, now=_now(), expires_at=expires_at,
        can_use=can_use or user_may_use_credentials(user_id, request.binding_project_id),
        subject_kind="mandate" if subject["kind"] == "mandate" else "user_delegation", subject_user_id=user_id,
        profile=PROFILE_DEVELOPMENT if binding.id is None else "governed",
    )
    try:
        inline_fields = lambda tool_type: toolkit_secret_fields(tool_type, request.binding_project_id, user_id)  # noqa: E731
        if rows is None:
            with db.get_session(request.binding_project_id) as session:
                candidates = collect_credential_uses(
                    session, request.binding_project_id, request.version_id, payload, inline_fields)
        else:   # no session is open while the schemas and the facts are read
            candidates = collect_credential_uses(
                None, request.binding_project_id, request.version_id, payload, inline_fields, rows=rows)
        return decide_credential_uses(context, candidates)
    except CredentialDecisionError as exc:
        error = exc
    except Exception as exc:  # pylint: disable=W0703
        log.error("admission: the credential decision failed (%s)", type(exc).__name__)
        error = CredentialDecisionError("DEPENDENCY_UNAVAILABLE")
    _metric(CREDENTIAL_DECISION, "credential_decision_total", "decision", "would_deny" if shadow else "deny",
            error.reason_code)
    raise _denied(error.reason_code, error.audit)


def _store_admission(  # pylint: disable=R0913,R0914
    module, request: AdmissionRequest, run_id, *, binding, subject, decisions, user_epochs, extra_epochs, caller,
    expires_at, context_id=None, children=(),
) -> ExecutionSecurityContextView:
    """Step 6: the context row and the allow events, in one transaction. A development run (``binding`` a ``Draft``,
    #7213) stores ``profile=development``, no binding reference and the digest of the definition that runs; its events
    name ``development:<digest>`` where a governed run names its binding, under ``ADMITTED_DEVELOPMENT``.

    ``children`` (#7227): the sub-agents admitted with the run (``ChildAdmission``), in the same transaction: a row each,
    linked to its caller's context (``parent_context_ref``, ``call_lineage``), the next attempt numbers of the run, and
    its own allow events (``parent_call_id`` = its caller's context)."""
    settings = module.admission_settings
    policy_revision = module.governance_profile.policy_revision
    user_ref = subject.get("user_ref", subject["ref"])
    context_id = context_id or uuid.uuid4()
    subject_columns = {key: subject[key] for key in SUBJECT_COLUMNS if key in subject}

    def write(session, row_id, attempt, application_id, version_id, admitted, uses, parent_id=None, lineage=None):
        label, profile = _binding_label(admitted), PROFILE_DEVELOPMENT if admitted.id is None else "governed"
        session.add(ExecutionSecurityContext(
            id=row_id, schema_version=SCHEMA_VERSION, profile=profile,
            policy_revision=policy_revision, tenant=subject["tid"], environment=settings.environment,
            project_id=request.project_id, binding_project_id=request.binding_project_id,
            entry_point=request.entry_point, caller=caller, subject=subject_columns,
            agent_binding_ref=admitted.id, application_id=application_id,
            version_id=version_id, definition_digest=admitted.definition_digest,
            run_id=run_id, attempt=attempt, parent_context_ref=parent_id, call_lineage=lineage or [],
            revocation_epochs={"user": user_epochs.get("user"), "token": user_epochs.get("token"),
                               "binding": admitted.revocation_epoch, "project": None, **extra_epochs,
                               "credential": {d.credential_ref: d.reference_epoch for d in uses} or None},
            allowed_bindings=[binding_ref(d) for d in uses],
            trace_ref=request.trace_ref, expires_at=expires_at, state="admitted",
        ))
        for decision in uses:
            _tracing().security_audit_record(session, _event(
                event_type="decision", decision="allow", reason_code=CREDENTIAL_USE_ALLOWED,
                event_id=decision.decision_id, tenant=subject["tid"], project_id=request.project_id,
                application_id=application_id, environment=settings.environment,
                caller_ref=caller["ref"], user_ref=user_ref, mandate_ref=subject.get("mandate_ref"),
                agent_binding_ref=label,
                run_id=run_id, attempt=attempt, credential_ref=decision.credential_ref,
                credential_generation=decision.credential_generation,
                toolkit_instance_id=decision.toolkit_instance_id, target_ref=WHOLE_TARGET,
                policy_revision=policy_revision, revocation_epoch=decision.reference_epoch,
                traceparent=request.trace_ref,
            ))
        _tracing().security_audit_record(session, _event(
            event_type="admission", decision="allow", reason_code=_admitted_code(profile),
            tenant=subject["tid"], project_id=request.project_id, application_id=application_id,
            environment=settings.environment, caller_ref=caller["ref"], user_ref=user_ref,
            mandate_ref=subject.get("mandate_ref"),
            agent_binding_ref=label, run_id=run_id, attempt=attempt,
            parent_call_id=None if parent_id is None else str(parent_id),
            toolkit_instance_id=None if request.toolkit_id is None or parent_id is not None else str(request.toolkit_id),
            policy_revision=policy_revision, revocation_epoch=admitted.revocation_epoch,
            traceparent=request.trace_ref,
        ))

    for _ in range(3):
        with db.get_session(request.project_id) as session:
            try:
                attempt = 1 + (session.execute(
                    select(func.max(ExecutionSecurityContext.attempt))
                    .where(ExecutionSecurityContext.run_id == run_id)
                ).scalar() or 0)
                write(session, context_id, attempt, request.application_id, request.version_id, binding, decisions)
                for offset, child in enumerate(children, start=1):
                    write(session, child.context_id, attempt + offset, child.application_id, child.version_id,
                          child.binding, child.decisions, child.parent_id, child.lineage)
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
    for _ in [*decisions, *(d for child in children for d in child.decisions)]:
        _metric(CREDENTIAL_DECISION, "credential_decision_total", "decision", "allow", CREDENTIAL_USE_ALLOWED)
    label = _binding_label(binding)
    return ExecutionSecurityContextView(
        context_ref=str(context_id), run_id=run_id, attempt=attempt, project_id=request.project_id,
        subject_ref=subject["ref"], subject_user_id=caller.get("user_id"), binding_project_id=request.binding_project_id,
        application_id=request.application_id, version_id=request.version_id,
        agent_binding_ref=label, policy_revision=policy_revision, expires_at=expires_at,
        allowed_bindings=[binding_ref(d) for d in decisions],
        children={child.key: [binding_ref(d) for d in child.decisions] for child in children},
    )


def _admitted_code(profile) -> str:
    """The reason code of an allow: ``ADMITTED_DEVELOPMENT`` names a development run in every audit view."""
    return "ADMITTED_DEVELOPMENT" if profile == PROFILE_DEVELOPMENT else "ADMITTED"


def _record_shadow_allow(  # pylint: disable=R0913
    module, request: AdmissionRequest, run_id, *, binding, subject, decisions, caller, children=(),
) -> None:
    """Step 6 of audit only: the allow events of an evaluation that passed, with ``would_deny=false``, the run's and
    those of each sub-agent admitted with it (#7227). There is no context row: nothing would carry it, and a row would
    look like a live admission."""
    with db.get_session(request.project_id) as session:
        try:
            _shadow_events(module, request, run_id, session, binding, subject, decisions, caller, request.application_id)
            for child in children:
                _shadow_events(module, request, run_id, session, child.binding, subject, child.decisions, caller,
                               child.application_id)
            session.commit()
        except Exception:
            session.rollback()
            raise
    for _ in [*decisions, *(d for child in children for d in child.decisions)]:
        _metric(CREDENTIAL_DECISION, "credential_decision_total", "decision", "would_allow", CREDENTIAL_USE_ALLOWED)


def _shadow_events(  # pylint: disable=R0913
    module, request: AdmissionRequest, run_id, session, binding, subject, decisions, caller, application_id,
) -> None:
    """The audit-only allow events of one admitted definition (the run's or a sub-agent's), in ``session``."""
    settings = module.admission_settings
    policy_revision = module.governance_profile.policy_revision
    user_ref = subject.get("user_ref", subject["ref"])
    label, profile = _binding_label(binding), PROFILE_DEVELOPMENT if binding.id is None else "governed"
    for decision in decisions:
        _tracing().security_audit_record(session, _event(
            event_type="decision", decision="allow", reason_code=CREDENTIAL_USE_ALLOWED, would_deny=False,
            event_id=decision.decision_id, tenant=subject["tid"], project_id=request.project_id,
            application_id=application_id, environment=settings.environment,
            caller_ref=caller["ref"], user_ref=user_ref, agent_binding_ref=label, run_id=run_id,
            credential_ref=decision.credential_ref, credential_generation=decision.credential_generation,
            toolkit_instance_id=decision.toolkit_instance_id, target_ref=WHOLE_TARGET,
            policy_revision=policy_revision, revocation_epoch=decision.reference_epoch,
            traceparent=request.trace_ref,
        ))
    _tracing().security_audit_record(session, _event(
        event_type="admission", decision="allow", reason_code=_admitted_code(profile), would_deny=False,
        tenant=subject["tid"], project_id=request.project_id, application_id=application_id,
        environment=settings.environment, caller_ref=caller["ref"], user_ref=user_ref,
        agent_binding_ref=label, run_id=run_id, policy_revision=policy_revision,
        revocation_epoch=binding.revocation_epoch, traceparent=request.trace_ref,
    ))


# --- autonomous mandates (#6947) -------------------------------------------------------------

def _mandate_event(mandate, reason) -> dict:
    """The lifecycle event of a change the platform made to a mandate by itself (expiry, owner suspension)."""
    return _event(
        event_type="lifecycle", reason_code=reason, outcome="success", project_id=mandate.project_id,
        application_id=mandate.application_id, agent_binding_ref=str(mandate.agent_binding_ref),
        mandate_ref=str(mandate.id), caller_ref="system:admission", revocation_epoch=mandate.revocation_epoch,
    )


def _read_mandate(project_id, mandate_id):
    with db.get_session(project_id) as session:
        return session.execute(select(*MANDATE_COLUMNS).where(AutonomousMandate.id == mandate_id)).one_or_none()


def _overdue(mandate) -> bool:
    return mandate.state in ("active", "suspended") and _aware(mandate.valid_until) <= _now()


def _expire_mandate(project_id, mandate_id) -> None:
    """Persist a lazy expiry (state, epoch, live contexts, event) in its own transaction. The caller has already read
    the clock and refuses the run either way, so a failure here is only logged: the next check does it again."""
    try:
        with db.get_session(project_id) as session:
            if not expire_if_due(session, mandate_id, now=_now()):
                return
            mandate = session.execute(select(*MANDATE_COLUMNS).where(AutonomousMandate.id == mandate_id)).one()
            revoke_mandate_contexts(session, mandate_id, "MANDATE_EXPIRED")
            _tracing().security_audit_record(session, _mandate_event(mandate, "MANDATE_EXPIRED"))
            session.commit()
    except Exception as exc:  # pylint: disable=W0703
        log.error("admission: a mandate expiry could not be recorded (%s)", type(exc).__name__)


def suspend_owner_in_project(project_id, owner_user_id) -> list:
    """The owner is no longer active: suspend every active mandate they own in this project, revoke the live runs
    of each and record the event, in one transaction of its own. The ids suspended."""
    with db.get_session(project_id) as session:
        suspended = suspend_owner_mandates(session, owner_user_id)
        if not suspended:
            return []
        for mandate in session.execute(select(*MANDATE_COLUMNS).where(AutonomousMandate.id.in_(suspended))).all():
            revoke_mandate_contexts(session, mandate.id, "MANDATE_INACTIVE")
            _tracing().security_audit_record(session, _mandate_event(mandate, "MANDATE_OWNER_SUSPENDED"))
        session.commit()
        return suspended


def _suspend_owner(project_id, owner_user_id) -> None:
    try:
        suspend_owner_in_project(project_id, owner_user_id)
    except Exception as exc:  # pylint: disable=W0703
        # the run is refused either way; the next firing or the owner event suspends the mandate
        log.error("admission: the mandate of an inactive owner could not be suspended (%s)", type(exc).__name__)


def _admit_mandate(  # pylint: disable=R0912,R0914
    module, request: AdmissionRequest, run_id, payload,
) -> ExecutionSecurityContextView:
    """Steps 2 to 6 of a run under an autonomous mandate: the mandate, its owner, the binding, the owner's right to
    run the agent and use credentials, then the context. The mandate is read from the table at this moment; the
    request only names it."""
    settings = module.admission_settings
    operation = MANDATE_OPERATION_OF.get(request.entry_point)
    try:
        mandate_id = uuid.UUID(str(request.mandate_ref))
    except ValueError:
        raise _denied("MANDATE_REQUIRED") from None
    if (request.caller_type != "trigger" or not request.trigger_ref or operation is None
            or request.version_id is None or request.application_id is None):
        raise _denied("MANDATE_REQUIRED")
    if not run_id:
        raise _denied("DEPENDENCY_UNAVAILABLE")
    # 2. the mandate exists for this agent version, is in time, active and allows this kind of trigger
    mandate = _read_mandate(request.binding_project_id, mandate_id)
    if (mandate is None or mandate.project_id != request.binding_project_id
            or mandate.agent_version != request.version_id or mandate.application_id != request.application_id):
        raise _denied("MANDATE_REQUIRED")
    if _overdue(mandate):
        _expire_mandate(request.binding_project_id, mandate_id)
        raise _denied("MANDATE_EXPIRED")
    if mandate.state == "expired":
        raise _denied("MANDATE_EXPIRED")
    if mandate.state != "active":
        raise _denied("MANDATE_INACTIVE")
    if operation not in (mandate.allowed_operations or []):
        raise _denied("DENIED_OPERATION")
    # 3. the owner is active and still the person the mandate was made for
    owner = mandate.owner_user_id
    try:
        epochs = auth.read_identity_epochs("user", owner)
    except Exception as exc:  # pylint: disable=W0703
        log.error("admission: owner epochs unreadable (%s)", type(exc).__name__)
        epochs = None
    if not isinstance(epochs, dict):
        raise _denied("DEPENDENCY_UNAVAILABLE")
    if epochs.get("active") is False:   # only an explicit verdict suspends; a reply without one is "unknown"
        _suspend_owner(request.binding_project_id, owner)
        raise _denied("MANDATE_INACTIVE")
    if epochs.get("active") is not True:
        raise _denied("DEPENDENCY_UNAVAILABLE")
    ref = mandate.owner_user_ref if isinstance(mandate.owner_user_ref, dict) else {}
    issuer, tid, oid = ref.get("issuer"), ref.get("tid"), ref.get("oid")
    if not (isinstance(issuer, str) and isinstance(tid, str) and isinstance(oid, str) and issuer and tid and oid):
        raise _denied("IDENTITY_SUBJECT_UNLINKED")
    try:
        linked = auth.get_user_by_idp_subject(issuer, tid, oid)
    except ValueError:   # the identity service refused the stored reference itself: it names nobody
        raise _denied("IDENTITY_SUBJECT_UNLINKED") from None
    except Exception as exc:  # pylint: disable=W0703
        log.error("admission: the owner link could not be read (%s)", type(exc).__name__)
        raise _denied("DEPENDENCY_UNAVAILABLE") from None
    if not isinstance(linked, dict) or linked.get("id") != owner:
        raise _denied("IDENTITY_SUBJECT_UNLINKED")
    # 4. the approved agent definition the mandate was made for is the one that runs
    binding, rows = _find_binding(request, settings)
    if str(binding.id) != str(mandate.agent_binding_ref):
        raise _denied("DENIED_BINDING")
    # 4a. the owner may still run agents in this project
    try:
        permitted = PREDICT_PERMISSION in set(
            auth.get_user_permissions(owner, mode="default", project_id=request.binding_project_id))
    except Exception as exc:  # pylint: disable=W0703
        log.error("admission: the owner permissions could not be read (%s)", type(exc).__name__)
        raise _denied("DEPENDENCY_UNAVAILABLE") from None
    if not permitted:
        raise _denied("DENIED_RBAC")
    # 5. on whose authority: the mandate, vouched for by the platform (never turned into a verified user)
    owner_ref = f"idp:{tid}:{oid}"
    subject = {
        "kind": "mandate", "ref": f"mandate:{mandate.id}", "delegation_ref": None, "tid": tid, "owner": owner_ref,
        "evidence": {"type": "asserted_user", "issuer": "elitea:mandate", "subject": owner_ref,
                     "mandate_ref": str(mandate.id)},
        "user_ref": owner_ref, "user_id": owner, "mandate_ref": str(mandate.id),
    }
    now = _now()
    by_runtime, by_validity = now + timedelta(seconds=mandate.max_runtime_s), _aware(mandate.valid_until)
    # the window to start (or restart) the run, and the moment Main stops it: the context ttl bounds only the first
    expires_at = min(now + timedelta(seconds=settings.context_ttl_s), by_runtime, by_validity)
    deadline = (RunDeadline(by_validity, "MANDATE_EXPIRED") if by_validity < by_runtime
                else RunDeadline(by_runtime, "RUNTIME_LIMIT_EXCEEDED"))
    # 5b. which credentials the run uses, and whether the agent may use them for the mandate
    decisions = _decide_credentials(module, request, subject, binding, payload, expires_at, rows=rows)
    # 6. context row and allow events, atomically
    caller = {"kind": "trigger", "ref": _caller_ref(request), "user_id": owner}
    view = _store_admission(
        module, request, run_id, binding=binding, subject=subject, decisions=decisions, caller=caller,
        user_epochs={"user": epochs.get("user"), "token": None},
        extra_epochs={"mandate": mandate.revocation_epoch}, expires_at=expires_at)
    return view.model_copy(update={"run_deadline": deadline.at, "run_deadline_code": deadline.code})


def index_schedule_refusal_code(principal) -> str:
    """A scheduled index run is not admitted in this release (no agent binding to hold a mandate). The code that
    refuses it: ``AUTHORIZATION_REQUIRED`` for a per-user schedule (it acts on a user's behalf and has no renewed
    authorization), ``MANDATE_REQUIRED`` for a team schedule (it ran as the system user)."""
    return "AUTHORIZATION_REQUIRED" if principal.kind == "delegation" else "MANDATE_REQUIRED"


def record_index_schedule_refusal(module, code, *, project_id, toolkit_id) -> None:
    """Audit and count the refusal of one scheduled index run. Called once the refusal was concluded (written into
    the index history), so a slot that is retried does not record one deny per minute."""
    _record_deny(module, code, project_id=project_id, caller_ref=f"schedule:index:{toolkit_id}")
    _metric(ADMISSION_DECISION, "admission_decision_total", "decision", "deny", code)


def _admit(  # pylint: disable=R0914
    module, request: AdmissionRequest, run_id, payload=None, shadow=False,
) -> Union[ExecutionSecurityContextView, list, None]:
    """Steps 1 to 6. ``shadow`` (audit only): the same steps 1 to 5b, then the allow events without a context row; it
    returns the credential decisions the run was allowed (what dispatch would seal against), and a run under a mandate
    is not evaluated."""
    settings = module.admission_settings
    # 1. the audit trail can take a decision
    try:
        audit_allowed = _tracing().security_audit_admission_allowed_local()
    except Exception:  # pylint: disable=W0703
        audit_allowed = False
    if not audit_allowed:
        raise _denied("AUDIT_UNAVAILABLE")
    if request.run_kind == "mandate":
        if shadow:
            return None
        return _admit_mandate(module, request, run_id, payload)
    # 2. a run with no person behind it needs a mandate
    if request.run_kind == "system":
        raise _denied("MANDATE_REQUIRED")
    if not run_id:
        raise _denied("DEPENDENCY_UNAVAILABLE")
    # 3. who is asking
    principal = _verify_identity(request, settings)
    can_use = None
    if request.entry_point in TOOLKIT_ENTRY_POINTS:
        # 4. (a toolkit Test run or on-demand indexing, #7229) a development run of the person, or nothing: a toolkit has
        # no binding of its own to be governed by
        if not _is_development(request, settings):
            raise _denied("DEVELOPMENT_RUNS_DISABLED")
        binding, rows = _read_toolkit(request)
        permissions = _author_permissions(request)
        can_use = lambda: USE_PERMISSION in permissions  # noqa: E731  # pylint: disable=C3001
    elif _is_development(request, settings):
        # 4. (development, #7213) the definition that runs, with no binding: the author's own right stands for it. Decided
        # here, before any binding is looked for, so the governed path reads nothing more than before
        binding, rows = _read_draft(request)
        permissions = _author_permissions(request)
        if AUTHOR_PERMISSION not in permissions:   # 4a. only someone who may edit the agent may author it
            raise _denied("DENIED_RBAC")
        can_use = lambda: USE_PERMISSION in permissions  # noqa: E731  # pylint: disable=C3001
    else:
        # 4. which approved agent definition runs
        binding, rows = _find_binding(request, settings)
    # 5. on whose behalf
    subject = _subject_of(principal)
    expires_at = _now() + timedelta(seconds=settings.context_ttl_s)
    # 5b. which credentials the run uses, and whether the agent may use them for this user
    decisions = _decide_credentials(module, request, subject, binding, payload, expires_at, shadow=shadow, rows=rows,
                                    can_use=can_use)
    # 5c. the sub-agents it runs in its own process, each on its own decision (#7227)
    context_id = uuid.uuid4()
    children = _admit_children(
        module, request, subject, payload, expires_at, context_id, development=binding.id is None, shadow=shadow,
        can_use=can_use or user_may_use_credentials(request.caller_id, request.binding_project_id), decisions=decisions)
    caller = {"kind": request.caller_type, "ref": _caller_ref(request), "user_id": request.caller_id}
    if shadow:
        _record_shadow_allow(module, request, run_id, binding=binding, subject=subject, decisions=decisions,
                             caller=caller, children=children)
        return ShadowAllowed([binding_ref(d) for d in decisions],
                             {child.key: [binding_ref(d) for d in child.decisions] for child in children})
    # 6. context rows and allow events, atomically
    epochs = principal.get("epochs") if isinstance(principal.get("epochs"), dict) else {}
    return _store_admission(
        module, request, run_id, binding=binding, subject=subject, decisions=decisions, caller=caller,
        user_epochs=epochs, extra_epochs={}, expires_at=expires_at, context_id=context_id, children=children)


# What audit only rehearses for a run: the decisions dispatch would seal against, the run's and each sub-agent's (#7227)
ShadowAllowed = namedtuple("ShadowAllowed", "entries children")


def _is_development(request: AdmissionRequest, settings: AdmissionSettings) -> bool:
    """Is this run the editor's test chat, or a toolkit Test run or on-demand indexing (#7229), of a signed-in person,
    and does the operator allow development runs (#7213)? Decided from the request kind alone: no read. Anything else
    (a shared or public chat, the API, a trigger, a system user, a token) takes the governed path, as does the editor
    when the setting is off (``DENIED_BINDING`` as before)."""
    return (settings.allow_development_runs and request.entry_point in DEVELOPMENT_ENTRY_POINTS
            and request.run_kind == "interactive" and request.caller_type == "user")


def admit_execution(module, request: AdmissionRequest, *, run_id: str, payload=None) -> ExecutionSecurityContextView:
    """Admit one governed run or raise ``AdmissionDenied``. Every decision is counted and audited.
    ``payload`` is the dispatched payload; it is cross-checked against the agent's stored credentials, never trusted."""
    started = time.perf_counter()
    try:
        view = _admit(module, request, run_id, payload)
    except AdmissionDenied as exc:
        error = exc
    except Exception as exc:  # pylint: disable=W0703
        log.error("admission: unexpected failure (%s)", type(exc).__name__)
        error = _denied("DEPENDENCY_UNAVAILABLE")
    else:
        ADMISSION_DURATION.observe(time.perf_counter() - started, "enforce", "allow")
        _metric(ADMISSION_DECISION, "admission_decision_total", "decision", "allow", "ADMITTED")
        return view
    _record_deny(
        module, error.reason_code, project_id=request.project_id, application_id=request.application_id,
        caller_ref=_caller_ref(request), run_id=run_id if isinstance(run_id, str) else None,
        traceparent=request.trace_ref,
        **{**({"mandate_ref": request.mandate_ref} if request.run_kind == "mandate" else {}), **error.audit},
    )
    ADMISSION_DURATION.observe(time.perf_counter() - started, "enforce", "deny")   # the audit write is in the span
    _metric(ADMISSION_DECISION, "admission_decision_total", "decision", "deny", error.reason_code)
    raise error


def _toolkit_snapshot(payload) -> dict:
    """The part of a dispatched payload that the credential decision and the seal read, copied: the toolkit lists of
    the request, of the application version and of the prefetched sub-agents. The history, the input and the
    instructions are not touched (a deep copy of a long chat is milliseconds on the event loop). The lists are copied
    under one memo, so a tool dict that two of them share is still one dict in the copy."""
    memo = {}

    def copied(tools):
        return copy.deepcopy(tools, memo) if isinstance(tools, list) else None

    application = payload.get("application") if isinstance(payload.get("application"), dict) else {}
    details = application.get("version_details") if isinstance(application.get("version_details"), dict) else {}
    prefetched = application.get("subagent_version_details")
    return {
        "project_id": payload.get("project_id"),
        "tools": copied(payload.get("tools")),
        "application": {
            "version_details": {"tools": copied(details.get("tools"))},
            "subagent_version_details": None if not isinstance(prefetched, dict) else {
                key: {"tools": copied(tools)} for key, tools in prefetched_tool_lists(payload).items()},
        },
    }


def _dispatched_view(task_name, payload) -> dict:
    """What the decision, the seal and the internal MCP checks read of a dispatched payload: the payload, or for a
    toolkit run (#7229) its one toolkit as the run's toolkit list. The same toolkit dict, so sealing the view seals the
    payload."""
    if task_name != TOOLKIT_TASK:
        return payload
    config = payload.get("toolkit_config")
    return {"project_id": payload.get("project_id"), "tools": [config] if isinstance(config, dict) else []}


def _shadow_admit(  # pylint: disable=R0912,R0913
    module, call_kwargs, admission: Optional[AdmissionRequest], unavailable=False, deferred=None, task_name=None,
) -> None:
    """Audit only: evaluate this dispatch exactly as enforce would, and record what enforce would have done.
    Nothing here refuses or raises into ``start_task``, and the real payload is never changed (but for the marker
    the entry point left on it): a denial becomes a ``would_deny`` event, any other failure a ``would_deny_error``
    metric and a log line (the type of the exception only).

    Evaluated: a person's run (``interactive``). Not evaluated: a run with no person behind it (a schedule, a webhook,
    a system user: enforce refuses those without a mandate, and a mandate cannot be rehearsed without its trigger
    ticket), a mandate run, and a dispatch with no request (a child).

    Nothing in audit only waits for the evaluation, so with a ``deferred`` list only what must see the call as it is
    now is done here (the marker is taken off, the toolkit lists are copied) and the evaluation is appended to the
    list: the caller runs it after ``start_task``, in the same greenlet. Without one it runs here."""
    payload = call_kwargs.get("kwargs")
    payload = payload if isinstance(payload, dict) else {}
    free_form_secret = payload.pop(FREE_FORM_SECRET_MARK, False)   # Main-internal: never goes to the worker
    if not isinstance(admission, AdmissionRequest) or admission.run_kind != "interactive":
        return
    run_id = payload.get(PREDICT_RUN_ID_KWARGS_KEY)   # bound before anything that can raise
    payload = _dispatched_view(task_name, payload)
    failure, snapshot, legacy_internal_mcp, mcp_findings = None, None, False, []
    try:
        # its internal Elitea MCP toolkits authenticate with a personal token: enforce gives them the run token (#7214),
        # strips what a toolkit brings of its own and refuses one on another origin
        legacy_internal_mcp = internal_obo.has_internal_mcp(payload)
        mcp_findings = internal_obo.inspect_internal_mcp(payload)
        meta = call_kwargs.get("meta")
        if admission.trace_ref is None and isinstance(meta, dict) and isinstance(meta.get("traceparent"), str):
            admission = admission.model_copy(update={"trace_ref": meta["traceparent"]})
        if not (free_form_secret or unavailable):
            snapshot = _toolkit_snapshot(payload)
    except Exception as exc:  # pylint: disable=W0703
        failure = exc

    def evaluate():
        _shadow_evaluate(module, admission, run_id, snapshot, free_form_secret, unavailable, failure)
        for code in ([internal_obo.LEGACY_PAT_INTERNAL_MCP] if legacy_internal_mcp else []) + mcp_findings:
            _record_deny(
                module, code, would_deny=True, project_id=admission.project_id,
                application_id=admission.application_id, caller_ref=_caller_ref(admission),
                run_id=run_id if isinstance(run_id, str) else None, traceparent=admission.trace_ref)
            _metric(ADMISSION_DECISION, "admission_decision_total", "decision", "would_deny", code)

    if deferred is None:
        evaluate()
    else:
        deferred.append(evaluate)


def _shadow_evaluate(  # pylint: disable=R0912,R0913
    module, admission: AdmissionRequest, run_id, snapshot, free_form_secret, unavailable, failure,
) -> None:
    """The evaluation of ``_shadow_admit``. Never raises."""
    started, span, decision = time.perf_counter(), None, "would_allow"
    try:
        if failure is not None:
            raise failure
        try:
            if free_form_secret:   # enforce refuses a {{secret.*}} outside a credential configuration before admitting
                _metric(CREDENTIAL_DECISION, "credential_decision_total", "decision", "would_deny",
                        "CREDENTIAL_REFERENCE_REQUIRED")
                raise _denied("CREDENTIAL_REFERENCE_REQUIRED")
            if unavailable:   # enforce would refuse every run: say so for this one
                raise _denied("DEPENDENCY_UNAVAILABLE")
            with walk_scope():
                allowed = _admit(module, admission, run_id, snapshot, shadow=True)
                span = time.perf_counter() - started   # as for enforce: the admission, not the sealing that follows it
                if allowed is not None:   # the seal enforce does after admitting, on the copy of the toolkits
                    _seal_dispatch(snapshot, lambda: allowed.entries, decided=True, shadow=True,
                                   children=allowed.children or None)
        except AdmissionDenied as exc:
            decision = "would_deny"
            if exc.reason_code != "AUDIT_UNAVAILABLE":   # the audit trail cannot take it: a metric and a log line only
                _record_deny(
                    module, exc.reason_code, would_deny=True, project_id=admission.project_id,
                    application_id=admission.application_id, caller_ref=_caller_ref(admission),
                    run_id=run_id if isinstance(run_id, str) else None, traceparent=admission.trace_ref, **exc.audit)
            _metric(ADMISSION_DECISION, "admission_decision_total", "decision", "would_deny", exc.reason_code)
        else:
            _metric(ADMISSION_DECISION, "admission_decision_total", "decision", "would_allow", "ADMITTED")
    except Exception as exc:  # pylint: disable=W0703
        decision = "would_deny_error"
        log.error("admission: the audit-only evaluation failed (%s)", type(exc).__name__)
        try:
            _metric(ADMISSION_DECISION, "admission_decision_total", "decision", "would_deny_error", "EVALUATION_FAILED")
        except Exception:  # pylint: disable=W0703
            pass
    try:
        ADMISSION_DURATION.observe(time.perf_counter() - started if span is None else span, "audit_only", decision)
    except Exception:  # pylint: disable=W0703
        pass


class _AsOf:
    """The module as of one state of the switch: everything is the module's, but ``governance_effective``,
    ``governance_profile`` and the flags of ``admission_settings`` are those of the state the dispatch read once, so no
    step of one dispatch sees the switch in a different position than another."""

    def __init__(self, module, effective):
        self._module, self.governance_effective, self.governance_profile = module, effective, effective.profile

    def __getattr__(self, name):
        return getattr(self._module, name)

    @property
    def admission_settings(self):
        effective = self.governance_effective
        return replace(self._module.admission_settings, allow_runtime_domain_binding=effective.allow_runtime_domain_binding,
                       admit_legacy_pat=effective.admit_legacy_pat, environment=effective.environment,
                       allow_development_runs=effective.allow_development_runs)


def _carries_parent_context(call_kwargs) -> bool:
    """Does this dispatch already carry the admission of a parent run (a governed envelope with a context)? Dict reads
    only, so that off stays free."""
    payload = call_kwargs.get("kwargs")
    envelope = payload.get("envelope") if isinstance(payload, dict) else None
    return isinstance(envelope, dict) and envelope.get("security_profile") == PROFILE_GOVERNED \
        and bool(envelope.get("context_ref"))


def admit_dispatch(  # pylint: disable=R0911
    module, task_name, call_kwargs, admission: Optional[AdmissionRequest], effective=None, deferred=None,
) -> Optional[str]:
    """Admit the run behind a ``start_task`` call. Returns the new ``context_ref``, or ``None`` when
    nothing was admitted here (governance off or audit only, a task that is not admitted, or a context inherited
    from the parent run). Sets ``context_ref`` in the envelope and in the task meta. Under audit only the run is
    evaluated as enforce would and the call is left untouched.

    ``deferred``: a list the caller runs after ``start_task`` (see ``_shadow_admit``); audit only appends its evaluation
    to it. Without one the evaluation runs here, before the call returns.

    ``effective`` is the state the caller read once for this dispatch (so the envelope and the admission agree).
    A child or reconcile dispatch of a run admitted under enforce is sealed against its parent's decisions whatever
    the mode is now: the parent's admission outlives a switch."""
    if task_name not in ADMITTED_TASKS:
        return None
    if effective is None:
        effective = getattr(module, "governance_effective", None)
    if effective is not None:
        governed, auditing, unavailable = effective.profile.enabled, effective.mode == MODE_AUDIT_ONLY, effective.unavailable
        module = _AsOf(module, effective)
    else:
        governed, auditing, unavailable = is_governed(module), is_auditing(module), is_unavailable(module)
    if not governed:
        if auditing:
            _shadow_admit(module, call_kwargs, admission, unavailable, deferred, task_name)   # never refuses: the run goes on as a legacy run
        if not _carries_parent_context(call_kwargs):
            return None
    payload = call_kwargs.setdefault("kwargs", {})
    meta = call_kwargs.get("meta")
    if not isinstance(meta, dict):
        meta = call_kwargs["meta"] = {}
    run_id = payload.get(PREDICT_RUN_ID_KWARGS_KEY)
    envelope = payload.get("envelope")
    meta.pop("context_deadline", None)   # Main-side only (below); never taken from the caller
    free_form_secret = payload.pop(FREE_FORM_SECRET_MARK, False)   # set by the predict entry points (credential_slots)
    payload.pop(internal_obo.TOKEN_KWARG, None)   # only minted below, never taken from what the caller built
    view_payload = _dispatched_view(task_name, payload)   # a toolkit run: its toolkit as the run's toolkit list (#7229)
    #
    if not isinstance(envelope, dict) or envelope.get("security_profile") != PROFILE_GOVERNED:
        # no governed envelope to carry the context (e.g. a parent started before governance was
        # switched on): the worker would not check it, so the run is not started
        return _refuse_dispatch(module, "STALE_CONTEXT", call_kwargs, run_id)
    try:
        # an internal Elitea MCP toolkit reaches the platform with the run token only (#7214): a toolkit that brought
        # credentials of its own on a disguised internal path or another platform route has them removed, and recorded
        brought_own = internal_obo.INTERNAL_MCP_OWN_AUTH in internal_obo.inspect_internal_mcp(view_payload)
        internal_obo.police_internal_mcp(view_payload)
    except internal_obo.ForeignInternalMcp as foreign:
        return _refuse_dispatch(module, foreign.code, call_kwargs, run_id)
    if brought_own:
        project_id = meta.get("project_id") or payload.get("project_id")
        _record_deny(module, internal_obo.INTERNAL_MCP_OWN_AUTH, event_type="decision",
                     project_id=project_id if isinstance(project_id, int) else None,
                     run_id=run_id if isinstance(run_id, str) else None)
        _metric(ADMISSION_DECISION, "admission_decision_total", "decision", "deny", internal_obo.INTERNAL_MCP_OWN_AUTH)
    if free_form_secret:
        # a {{secret.*}} outside a credential configuration would have been resolved into the payload, where no slot
        # governs it: only credentials registered for governed use reach a governed run
        _metric(CREDENTIAL_DECISION, "credential_decision_total", "decision", "deny", "CREDENTIAL_REFERENCE_REQUIRED")
        return _refuse_dispatch(module, "CREDENTIAL_REFERENCE_REQUIRED", call_kwargs, run_id)
    if admission is CREDENTIAL_FREE:
        # the platform's own read of chat attachments (a built-in toolkit, no toolkit row): not admitted, and nothing
        # was decided for it, so any credential configuration in it is refused
        try:
            _seal_dispatch(view_payload, lambda: [])
        except AdmissionDenied as denied:
            _refuse_dispatch(module, denied.reason_code, call_kwargs, run_id)
        return None
    if envelope.get("context_ref"):
        # a child or reconcile dispatch runs under its parent's admission, which decided the parent's credentials
        # only: its credentials are sealed against those decisions, any other credential is refused. A sub-agent that
        # was admitted with the run (#7227) runs under its own context instead, which ``_seal_inherited`` names
        own_ref = _seal_inherited(module, view_payload, envelope["context_ref"], call_kwargs, run_id)
        if own_ref != envelope["context_ref"]:
            envelope = payload["envelope"] = {**envelope, "context_ref": own_ref}
            meta["context_ref"] = own_ref
        meta.setdefault("context_ref", own_ref)
        inherited_project = meta.get("project_id") or payload.get("project_id")
        if not _deliver_run_token(payload, lambda: internal_obo.mint_for_context(
                inherited_project, own_ref, task_name, view_payload), view_payload):
            _refuse_dispatch(module, "INTERNAL_TOKEN_UNAVAILABLE", call_kwargs, run_id)
        return None
    if unavailable:
        # enforce is on but a required service is not: a new admission is refused readably and audited (the platform
        # cannot prove anything right now) until the preflight passes again; runs already admitted are not touched
        return _refuse_dispatch(module, "DEPENDENCY_UNAVAILABLE", call_kwargs, run_id)
    if admission is None:
        return _refuse_dispatch(module, "IDENTITY_EVIDENCE_MISSING", call_kwargs, run_id)
    #
    if admission.trace_ref is None and isinstance(meta.get("traceparent"), str):
        admission = admission.model_copy(update={"trace_ref": meta["traceparent"]})
    with walk_scope():   # the decision and the seal walk the settings of a toolkit once
        view = admit_execution(module, admission, run_id=run_id, payload=view_payload)
        try:
            # admission decided every credential of this payload: those are sealed, anything else is not a credential;
            # the toolkits of each sub-agent admitted with the run against its own decisions (#7227)
            _seal_dispatch(view_payload, lambda: view.allowed_bindings, decided=True, children=view.children or None)
        except AdmissionDenied as denied:
            track_context(module, None, admission.project_id, view.context_ref)   # nothing is dispatched: end the context
            _record_deny(
                module, denied.reason_code, project_id=admission.project_id, application_id=admission.application_id,
                caller_ref=_caller_ref(admission), run_id=run_id if isinstance(run_id, str) else None,
                traceparent=admission.trace_ref, **denied.audit)
            raise
    if task_name == TOOLKIT_TASK and isinstance(meta.get("toolkit_config"), dict):
        # the task meta is a tracking surface: it gets the sealed toolkit, never the one expansion left material in
        meta["toolkit_config"] = copy.deepcopy(payload.get("toolkit_config"))
    if not _deliver_run_token(payload, lambda: internal_obo.mint_for_admission(view, task_name, view_payload),
                              view_payload):
        track_context(module, None, admission.project_id, view.context_ref)   # nothing is dispatched: end the context
        _record_deny(
            module, "INTERNAL_TOKEN_UNAVAILABLE", project_id=admission.project_id,
            application_id=admission.application_id, caller_ref=_caller_ref(admission),
            run_id=run_id if isinstance(run_id, str) else None, traceparent=admission.trace_ref)
        _metric(ADMISSION_DECISION, "admission_decision_total", "decision", "deny", "INTERNAL_TOKEN_UNAVAILABLE")
        raise _denied("INTERNAL_TOKEN_UNAVAILABLE")
    envelope["context_ref"] = view.context_ref
    meta["context_ref"] = view.context_ref
    if admission.run_kind == "mandate":
        # the wrapper takes it out again; Main stops the run at this moment (not the context ttl)
        meta["context_deadline"] = RunDeadline(view.run_deadline, view.run_deadline_code)
    return view.context_ref


def _deliver_run_token(payload, minted, view=None) -> bool:
    """Put the run token (#7214) on the payload of a run that can reach the internal MCP (``needs_run_token``: an
    internal toolkit, or a sub-agent the runtime may fetch itself): one signature, the binding's operations, the user's
    cached permissions. Any other run mints and reads nothing. False when a token is needed and none could be minted:
    that run is refused; its toolkits never fall back to a personal or system token. A sub-agent that the runtime would
    fetch only gets a token when the minting works; without one its internal toolkits are refused at the MCP. ``view``:
    what the checks read of the payload (a toolkit run's toolkit, ``_dispatched_view``); the token goes on the payload."""
    view = payload if view is None else view
    if not internal_obo.needs_run_token(view):
        return True
    try:
        payload[internal_obo.TOKEN_KWARG] = minted()
        return True
    except Exception as exc:  # pylint: disable=W0703
        if internal_obo.has_internal_mcp(view):
            log.error("admission: no run token could be minted (%s); the run is refused", type(exc).__name__)
            return False
        log.warning("admission: no run token (%s); fetched sub-agents get no internal tools", type(exc).__name__)
        return True


_PARENTS = contextvars.ContextVar("c1009_parent_entries", default=None)


@contextlib.contextmanager
def parent_entries_scope():
    """Within this scope the decisions of a parent context are read once, however many children are dispatched under it
    (one fan-out, ``parallel_dispatch``). They do not change after the admission; whether the parent still holds its
    admission is checked by each child's own ``validate_context``."""
    token = _PARENTS.set({})
    try:
        yield
    finally:
        _PARENTS.reset(token)


def _parent_entries(project_id, context_ref) -> list:
    """The credential-use decisions (``allowed_bindings``) of the parent context."""
    return _parent_admission(project_id, context_ref)[0]


def _parent_admission(project_id, context_ref) -> tuple:
    """``(allowed_bindings, profile, run_id, version_id)`` of the parent context: one read, remembered for the fan-out
    scope."""
    if isinstance(project_id, bool) or not isinstance(project_id, int):
        raise _denied("STALE_CONTEXT")
    try:
        context_id = uuid.UUID(str(context_ref))
    except ValueError:
        raise _denied("STALE_CONTEXT") from None
    read = _PARENTS.get()
    if read is not None and (project_id, context_id) in read:
        return read[(project_id, context_id)]
    with db.get_session(project_id) as session:
        row = session.execute(
            select(ExecutionSecurityContext.allowed_bindings, ExecutionSecurityContext.profile,
                   ExecutionSecurityContext.run_id, ExecutionSecurityContext.version_id)
            .where(ExecutionSecurityContext.id == context_id)
        ).one_or_none()
    if row is None or not isinstance(row.allowed_bindings, list):
        raise _denied("STALE_CONTEXT")
    found = (row.allowed_bindings, row.profile, row.run_id, row.version_id)
    if read is not None:
        read[(project_id, context_id)] = found
    return found


def _seal_dispatch(payload, entries_of, decided=False, shadow=False, children=None) -> None:  # pylint: disable=R0913
    """Replace the secret fields of the payload's credentials with slots (``credential_slots.seal_payload``).
    ``decided``: admission just decided every credential of the run's own toolkits. ``children``: the decisions of
    the sub-agents admitted with the run, by prefetch key (#7227). ``AdmissionDenied`` for a refusal; the credential
    decision counter takes the deny (a ``would_deny`` under audit only, ``shadow``)."""
    outcome = "would_deny" if shadow else "deny"
    try:
        seal_payload(payload, entries_of, classify=read_configuration_facts, decided=decided, children=children)
        return
    except AdmissionDenied as exc:   # the parent context could not be read
        _metric(CREDENTIAL_DECISION, "credential_decision_total", "decision", outcome, exc.reason_code)
        raise
    except CredentialDecisionError as exc:
        error = exc
    except Exception as exc:  # pylint: disable=W0703
        log.error("admission: the credentials could not be sealed (%s)", type(exc).__name__)
        error = CredentialDecisionError("DEPENDENCY_UNAVAILABLE")
    _metric(CREDENTIAL_DECISION, "credential_decision_total", "decision", outcome, error.reason_code)
    raise _denied(error.reason_code, error.audit)


def _seal_inherited(module, payload, context_ref, call_kwargs, run_id) -> str:  # pylint: disable=R0913
    """A dispatch under a parent context: seal the credentials the parent was admitted for (exactly: toolkit, field
    and configuration) and refuse any other credential (a credential-bearing sub-agent, a conversation toolkit). A
    configuration without a decision of the parent must be shown not to be a credential. The context the dispatch runs
    under is returned: the parent's, or (#7227) that of a sub-agent admitted with the run when the dispatch is that
    sub-agent (a fan-out child or its HITL resume)."""
    project_id = (call_kwargs.get("meta") or {}).get("project_id") or payload.get("project_id")
    parent = {}

    def entries():
        parent["admission"] = _parent_admission(project_id, context_ref)
        return parent["admission"][0]

    try:
        _seal_dispatch(payload, entries)
        return context_ref
    except AdmissionDenied as exc:
        code = exc.reason_code
    if code == "CREDENTIAL_REFERENCE_REQUIRED" and "admission" in parent:
        # read only when the seal against the parent alone refused: the governed path reads nothing more (#7227)
        try:
            own_ref = _seal_with_children(payload, project_id, context_ref, parent["admission"])
        except AdmissionDenied as exc:
            own_ref, code = None, exc.reason_code
        if own_ref is not None:
            return own_ref
    if code == "CREDENTIAL_REFERENCE_REQUIRED" and parent.get("admission", (None, None))[1] == PROFILE_DEVELOPMENT:
        # a child of a development run asked for a credential nothing was decided for when the draft started: the
        # author's identity is not lent beyond the decisions of the draft and of its own sub-agents (#7213, #7227)
        code = "DENIED_DEVELOPMENT_CHILD"
    return _refuse_dispatch(module, code, call_kwargs, run_id)


# What the release and the dispatch of a sub-agent read of a context admitted with the run (#7227): the checks of
# ``_check_run`` and the seal of a child dispatch
CHILD_COLUMNS = (
    ExecutionSecurityContext.id, ExecutionSecurityContext.schema_version, ExecutionSecurityContext.profile,
    ExecutionSecurityContext.state, ExecutionSecurityContext.revoked_reason, ExecutionSecurityContext.expires_at,
    ExecutionSecurityContext.run_id, ExecutionSecurityContext.attempt, ExecutionSecurityContext.application_id,
    ExecutionSecurityContext.version_id, ExecutionSecurityContext.agent_binding_ref,
    ExecutionSecurityContext.binding_project_id, ExecutionSecurityContext.project_id, ExecutionSecurityContext.tenant,
    ExecutionSecurityContext.subject, ExecutionSecurityContext.allowed_bindings, ExecutionSecurityContext.trace_ref,
    ExecutionSecurityContext.revocation_epochs, ExecutionSecurityContext.caller,
    ExecutionSecurityContext.definition_digest, ExecutionSecurityContext.policy_revision,
    ExecutionSecurityContext.environment, ExecutionSecurityContext.parent_context_ref,
    ExecutionSecurityContext.call_lineage,
)


def descendant_contexts(project_id, run_id, ancestor_ref) -> list:
    """The live, unexpired contexts of the sub-agents admitted under ``ancestor_ref`` (at any depth: its ref is in their
    lineage), in admission order. One read, by the run id (indexed)."""
    with db.get_session(project_id) as session:
        rows = session.execute(
            select(*CHILD_COLUMNS)
            .where(ExecutionSecurityContext.run_id == run_id, ExecutionSecurityContext.parent_context_ref.isnot(None))
            .order_by(ExecutionSecurityContext.attempt)
        ).all()
    now, ancestor = _now(), str(ancestor_ref)
    return [row for row in rows if ancestor in (row.call_lineage or []) and row.state in LIVE_STATES
            and _aware(row.expires_at) > now]


def _seal_with_children(payload, project_id, context_ref, parent_admission) -> Optional[str]:
    """The seal of a dispatch whose toolkits carry the decisions of sub-agents admitted with the run (#7227): a fan-out
    child (its version is one of them) runs under that sub-agent's own context; a reconcile run under the parent's.
    Either way the dispatched toolkits are sealed against the context they run under, and the toolkits of its
    prefetched sub-agents against theirs. The context ref, None when the run has no such sub-agent, ``AdmissionDenied``
    for a refusal."""
    entries, _profile, run_id, own_version = parent_admission
    descendants = descendant_contexts(project_id, run_id, context_ref)
    if not descendants:
        return None
    application = payload.get("application") if isinstance(payload.get("application"), dict) else {}
    version_id = application.get("version_id")
    own_ref, own_entries, scope = str(context_ref), entries, descendants
    if version_id is not None and version_id != own_version:
        match = [row for row in descendants if row.version_id == version_id]
        match.sort(key=lambda row: str(row.parent_context_ref) != str(context_ref))   # a direct sub-agent first
        if not match:
            return None
        own_ref, own_entries = str(match[0].id), match[0].allowed_bindings
        scope = [row for row in descendants if own_ref in (row.call_lineage or [])]
    children = {}
    for row in scope:
        children.setdefault(f"{row.application_id}:{row.version_id}", row.allowed_bindings or [])
    _seal_dispatch(payload, lambda: own_entries, children=children)
    return own_ref


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


def _context_lock(module):
    return module._context_lock  # pylint: disable=W0212


def track_context(module, task_id, project_id, context_ref, deadline=None):
    """After ``start_task`` returned: remember which task runs the context, or revoke it if none started.
    A run with a ``deadline`` (a ``RunDeadline``, mandate runs) is remembered with it, for ``enforce_run_deadlines``."""
    try:
        if task_id is None:
            with db.get_session(project_id) as session:
                _set_state(session, uuid.UUID(context_ref), "revoked", "NOT_DISPATCHED", ("admitted",))
                session.commit()
            return
        with _context_lock(module):
            tracked = module._context_by_task  # pylint: disable=W0212
            tracked[task_id] = (project_id, context_ref) if deadline is None else (project_id, context_ref, deadline)
            while len(tracked) > MAX_TRACKED_CONTEXTS:
                tracked.popitem(last=False)
    except Exception as exc:  # pylint: disable=W0703
        log.error("admission: could not track context of task %s (%s)", task_id, type(exc).__name__)


def complete_context(module, task_id):
    """A task stopped: close its context. A context the map lost is closed by its expiry."""
    if not governance_lifecycle_active(module):
        return
    with _context_lock(module):
        found = module._context_by_task.pop(task_id, None)  # pylint: disable=W0212
    if found is None:
        return
    project_id, context_ref = found[:2]
    with db.get_session(project_id) as session:
        _set_state(session, uuid.UUID(context_ref), "completed")
        session.commit()


def _revoked_among(entries) -> set:
    """The context refs, among tracked ``(task_id, entry)`` pairs, that are ``revoked`` now: one query per project
    and chunk of ids."""
    by_project = defaultdict(list)
    for _, entry in entries:
        by_project[entry[0]].append(uuid.UUID(entry[1]))
    revoked = set()
    for project_id, ids in by_project.items():
        with db.get_session(project_id) as session:
            for start in range(0, len(ids), STOP_CHUNK):
                revoked.update(str(found) for found in session.execute(
                    select(ExecutionSecurityContext.id).where(
                        ExecutionSecurityContext.id.in_(ids[start:start + STOP_CHUNK]),
                        ExecutionSecurityContext.state == "revoked")).scalars())
    return revoked


def reopen_contexts(module, project_id, context_refs) -> int:
    """A parked parent stops when it fans out, which completes its context, but its children and the reconcile run
    still run under that context: bring each one back to ``running`` when it is neither expired nor revoked, in one
    statement for all of them. The number reopened. Governed only (also after a switch to off: the children keep the
    admission of their parent); a failure is logged and the children then fail at ``validate_context``.

    Each context is also marked as inherited (``INHERITED_KEY``): its children and reconcile runs build the parent's
    toolkits again, which the release counter allows more of than the parent's own."""
    if not governance_lifecycle_active(module):
        return 0
    wanted = []
    for context_ref in sorted({ref for ref in context_refs if isinstance(ref, str) and ref}):
        try:
            wanted.append(uuid.UUID(context_ref))
        except ValueError:
            log.error("admission: a context could not be reopened for its children (ValueError)")
    if not wanted:
        return 0
    reopened = 0
    try:
        with db.get_session(project_id) as session:
            for start in range(0, len(wanted), STOP_CHUNK):
                reopened += session.execute(
                    update(ExecutionSecurityContext)
                    .where(ExecutionSecurityContext.id.in_(wanted[start:start + STOP_CHUNK]),
                           ExecutionSecurityContext.state == "completed",
                           ExecutionSecurityContext.expires_at > _now())
                    .values(state="running", ended_at=None).execution_options(synchronize_session=False)
                ).rowcount
            session.commit()
    except Exception as exc:  # pylint: disable=W0703
        reopened = 0
        log.error("admission: a context could not be reopened for its children (%s)", type(exc).__name__)
    _mark_inherited(module, wanted)
    return reopened


def _mark_inherited(module, context_ids) -> None:
    """One Redis round trip for all the contexts of a fan-out. Without the mark the children are held to the lower
    release bound, which refuses (``RATE_LIMITED``) rather than lets through, so a failure is only logged."""
    try:
        ttl = max(1, int(module.admission_settings.context_ttl_s))
        pipe = module.get_redis_client().pipeline(transaction=False)
        for context_id in context_ids:
            pipe.set(INHERITED_KEY + str(context_id), "1", ex=ttl)
        pipe.execute()
    except Exception as exc:  # pylint: disable=W0703
        log.error("admission: the inherited mark of a context was not stored (%s)", type(exc).__name__)


def enforce_run_deadlines(module, now=None) -> int:
    """Stop the tracked runs that must not go on: a mandate run past its time limit (the worker has no deadline of
    its own), and any tracked run whose context was revoked while it runs. Called once a minute by the pipeline
    scheduler tick. Governed only (also after a switch to off, for the runs admitted before it). The number of runs
    stopped.

    ``stop_task`` comes first: the run is forgotten (and, for a time limit, its context revoked and the event recorded)
    only once it was stopped, so a failed stop is tried again on the next tick. Runs this process does not track
    (after a Main restart, or children that inherit a mandate context) are not reached."""
    if not governance_lifecycle_active(module):
        return 0
    now = now or _now()
    with _context_lock(module):
        snapshot = list(module._context_by_task.items())  # pylint: disable=W0212
    overdue = [(task_id, entry, entry[2].code) for task_id, entry in snapshot if len(entry) > 2 and entry[2].at <= now]
    rest = [(task_id, entry) for task_id, entry in snapshot if not (len(entry) > 2 and entry[2].at <= now)]
    work = list(overdue)
    if rest:
        try:
            revoked = _revoked_among(rest)
        except Exception as exc:  # pylint: disable=W0703
            log.error("admission: the revoked runs could not be listed (%s)", type(exc).__name__)
            revoked = set()
        work += [(task_id, entry, None) for task_id, entry in rest if entry[1] in revoked]
    stopped = 0
    for task_id, entry, code in work:
        code_label = code or "CONTEXT_REVOKED"
        try:
            module.task_node.stop_task(task_id)
        except Exception as exc:  # pylint: disable=W0703
            log.error("admission: a governed run could not be stopped, it is tried again (%s)", type(exc).__name__)
            RUN_STOP.inc("stop_failed", code_label)
            continue
        with _context_lock(module):
            tracked = module._context_by_task.pop(task_id, None) is not None  # pylint: disable=W0212
        if not tracked:   # the task finished (and closed its context) meanwhile
            continue
        stopped += 1
        RUN_STOP.inc("stopped", code_label)
        if code is not None:
            try:
                _revoke_overrun(module, entry[0], entry[1], code)
            except Exception as exc:  # pylint: disable=W0703
                log.error("admission: a run past its time limit could not be recorded (%s)", type(exc).__name__)
    return stopped


def _revoke_overrun(module, project_id, context_ref, code):
    """The run was stopped for running past ``code`` (``RUNTIME_LIMIT_EXCEEDED`` or ``MANDATE_EXPIRED``)."""
    context_id = uuid.UUID(context_ref)
    with db.get_session(project_id) as session:
        row = session.execute(
            select(ExecutionSecurityContext.run_id, ExecutionSecurityContext.attempt,
                   ExecutionSecurityContext.application_id, ExecutionSecurityContext.agent_binding_ref,
                   ExecutionSecurityContext.subject, ExecutionSecurityContext.policy_revision,
                   ExecutionSecurityContext.environment)
            .where(ExecutionSecurityContext.id == context_id)
        ).one_or_none()
        changed = _set_state(session, context_id, "revoked", code)
        session.commit()
    if row is None or not changed:
        return
    subject_ref = (row.subject or {}).get("ref")
    _record_deny(
        module, code, event_type="decision", project_id=project_id,
        application_id=row.application_id, run_id=row.run_id, attempt=row.attempt,
        agent_binding_ref=None if row.agent_binding_ref is None else str(row.agent_binding_ref),
        policy_revision=row.policy_revision, environment=row.environment,
        mandate_ref=subject_ref[len("mandate:"):] if isinstance(subject_ref, str) else None,
    )
    _metric(CONTEXT_VALIDATION, "execution_context_validation_total", "result", "deny", code)


# --- validation on behalf of the worker ------------------------------------------------------

def _read_binding(project_id, binding_ref):
    with db.get_session(project_id) as session:
        return session.execute(
            select(AgentIdentityBinding.state, AgentIdentityBinding.revocation_epoch,
                   AgentIdentityBinding.definition_digest)
            .where(AgentIdentityBinding.id == binding_ref)
        ).one_or_none()


def _mandate_revoke_code(row, stamped) -> Optional[str]:
    """Does the mandate a run was admitted under still stand? ``None`` when it does, else the code to revoke with."""
    subject_ref = (row.subject or {}).get("ref")
    try:
        mandate_id = uuid.UUID(str(subject_ref)[len("mandate:"):])
    except ValueError:
        return "MANDATE_INACTIVE"
    project_id = row.binding_project_id or row.project_id
    mandate = _read_mandate(project_id, mandate_id)
    if mandate is None:
        return "MANDATE_INACTIVE"
    if _overdue(mandate):
        _expire_mandate(project_id, mandate_id)
        return "MANDATE_EXPIRED"
    if mandate.state == "expired":
        return "MANDATE_EXPIRED"
    if mandate.state != "active":
        return "MANDATE_INACTIVE"
    if mandate.revocation_epoch != stamped.get("mandate"):
        return "EPOCH_MISMATCH"
    if str(mandate.agent_binding_ref) != str(row.agent_binding_ref):
        return "DENIED_BINDING"
    return None


def _validate(project_id, context_ref, caller, known, touch=True):  # pylint: disable=R0912,R0914,R0915
    """Is this run still the one that was admitted? No database connection is held while anything else is asked: the
    context (and, in the same project, its binding) is read in a short session, the identity service and the facts are
    asked with none open, and the state is written in a short session of its own, as a conditional update (a context
    that left its live states meanwhile is not moved)."""
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
    columns = ExecutionSecurityContext
    with db.get_session(project_id) as session:
        row = session.execute(
            select(columns.schema_version, columns.profile, columns.state, columns.revoked_reason, columns.expires_at,
                   columns.run_id, columns.attempt, columns.project_id, columns.application_id, columns.version_id,
                   columns.agent_binding_ref, columns.binding_project_id, columns.subject, columns.caller,
                   columns.definition_digest, columns.policy_revision, columns.environment, columns.revocation_epochs,
                   columns.allowed_bindings)
            .where(columns.id == context_id)
        ).one_or_none()
        binding = None
        if (row is not None and row.agent_binding_ref is not None
                and (row.binding_project_id or project_id) == project_id):   # the same schema: the same read
            binding = session.execute(
                select(AgentIdentityBinding.state, AgentIdentityBinding.revocation_epoch,
                       AgentIdentityBinding.definition_digest)
                .where(AgentIdentityBinding.id == row.agent_binding_ref)
            ).one_or_none()
    if row is None:
        raise _denied("STALE_CONTEXT")
    development = row.profile == PROFILE_DEVELOPMENT   # a development run has no binding to hold it to (#7213)
    known.update(application_id=row.application_id, agent_binding_ref=context_binding_label(row),
                 policy_revision=row.policy_revision, environment=row.environment)   # as admitted, not as today
    is_mandate = (row.subject or {}).get("kind") == "mandate"
    if is_mandate and str((row.subject or {}).get("ref")).startswith("mandate:"):
        known["mandate_ref"] = str(row.subject["ref"])[len("mandate:"):]
    if row.schema_version != SCHEMA_VERSION or row.profile not in PROFILES:
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
        with db.get_session(project_id) as session:
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
    if is_mandate and epochs.get("active") is False:
        # the owner is gone: the mandate goes with them, for every run, not only this one
        _suspend_owner(row.binding_project_id or project_id, user_id)
        raise revoke("MANDATE_INACTIVE")
    if is_mandate and epochs.get("active") is not True:
        raise _denied("DEPENDENCY_UNAVAILABLE")   # no verdict: unknown, not revoked
    if not epochs.get("active") or epochs.get("user") != stamped.get("user"):
        raise revoke("EPOCH_MISMATCH")
    #
    if not development:
        if (row.binding_project_id or project_id) != project_id:   # the agent lives in another project
            binding = _read_binding(row.binding_project_id, row.agent_binding_ref)
        if binding is None or binding.state != "active":
            raise revoke("DENIED_BINDING")
        if binding.revocation_epoch != stamped.get("binding"):
            raise revoke("EPOCH_MISMATCH")
        if binding.definition_digest != row.definition_digest:
            raise revoke("BINDING_DIGEST_MISMATCH")
    #
    if is_mandate:
        code = _mandate_revoke_code(row, stamped)
        if code is not None:
            raise revoke(code)
    #
    if isinstance(row.allowed_bindings, list) and row.allowed_bindings:
        try:
            code = recheck_context_credentials(row, project_id, _now())
        except Exception as exc:  # pylint: disable=W0703
            log.error("validate_context: credential facts unreadable (%s)", type(exc).__name__)
            raise _denied("DEPENDENCY_UNAVAILABLE") from None   # unknown, not revoked: the run may be retried
        if code is not None:
            raise revoke(code)
    #
    if touch or row.state != "running":   # the internal MCP asks per request: no write once the run is running (#7214)
        with db.get_session(project_id) as session:
            if _set_state(session, context_id, "running", from_states=LIVE_STATES) != 1:
                raise _denied("STALE_CONTEXT")
            session.commit()
    return {
        "context_ref": str(context_id), "run_id": row.run_id, "attempt": row.attempt,
        "project_id": row.project_id, "application_id": row.application_id, "version_id": row.version_id,
        "agent_binding_ref": context_binding_label(row), "policy_revision": row.policy_revision,
        "expires_at": _aware(row.expires_at).isoformat(),
    }


def context_binding_label(row) -> str:
    """What a context row was admitted on, as the audit trail and the worker see it: the binding id, or
    ``development:<digest>`` for a development run (#7213)."""
    if row.agent_binding_ref is None:
        return DEVELOPMENT_LABEL + str(row.definition_digest)
    return str(row.agent_binding_ref)


def recheck_context_credentials(row, project_id, now) -> Optional[str]:
    """Do the credential decisions of a context still hold? The governed recheck against the binding's grants, or the
    development one against the author's own grants (#7213). ``None`` when they hold, else the code to revoke with."""
    binding_project_id = row.binding_project_id or project_id
    subject_ref = (row.subject or {}).get("ref")
    if row.profile == PROFILE_DEVELOPMENT:
        return recheck_development(binding_project_id, row.allowed_bindings, subject_ref, now)[0]
    return recheck_credentials(binding_project_id, row.allowed_bindings, subject_ref, str(row.agent_binding_ref), now)


def validate_context(module, project_id, context_ref, caller, touch=True) -> dict:
    """Worker-bus handler: is this run still the one that was admitted? Never raises, returns no secrets.

    ``{"ok": True, "context": {...}}`` or ``{"ok": False, "reason_code", "message"}``.
    """
    known = {}
    try:
        context = _validate(project_id, context_ref, caller, known, touch=touch)
    except AdmissionDenied as denied:
        code = denied.reason_code
    except Exception as exc:  # pylint: disable=W0703
        log.error("validate_context: unexpected failure (%s)", type(exc).__name__)
        code = "DEPENDENCY_UNAVAILABLE"
    else:
        _metric(CONTEXT_VALIDATION, "execution_context_validation_total", "result", "allow", "VALIDATED")
        reply = {"ok": True, "context": context}
        if (getattr(module, "_governed_handlers", None) or {}).get("release_batch"):
            reply["batch_release"] = True   # the worker may ask for all the slots of the run in one release
        return reply
    _record_deny(module, code, event_type="decision", **known)
    _metric(CONTEXT_VALIDATION, "execution_context_validation_total", "result", "deny", code)
    return {"ok": False, "reason_code": code, "message": MESSAGES.get(code, GENERIC_MESSAGE)}
