"""The platform-wide credential governance switch (#7120, C-1009).

The mode (``off``, ``audit_only``, ``enforce``), the policy revision and the admission flags are stored in the
database and changed from the Admin UI. The deployment keeps exactly three keys, read from ``os.environ`` directly (never
from the plugin config, whose ``pylon.db`` copy can shadow the file): ``ELITEA_CREDENTIAL_GOVERNANCE_MAX_MODE`` (the only
ceiling), ``ELITEA_CREDENTIAL_GOVERNANCE_ENABLED`` (its deprecated alias) and ``ELITEA_CREDENTIAL_GOVERNANCE_ENVIRONMENT``
(locks the environment label). Everything else that used to configure governance is ignored with one warning each.

Dispatch never queries: it reads the snapshot kept by ``GovernanceState``. A refresher thread re-reads the row every
``TTL_S`` seconds (``RETRY_S`` after a failed read), and a change made on any replica fires an event that refreshes the
others at once.
"""
import json
import os
import re
import threading
import time
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from typing import Callable, Literal, Mapping, Optional

from pydantic import BaseModel, ConfigDict, Field, StrictBool, StrictInt, ValidationError, field_validator
from pylon.core.tools import log
from sqlalchemy import func, insert, inspect, select, text, update
from sqlalchemy.exc import IntegrityError
from tools import auth, config as c, db, this

from ..models.governance_settings import (
    ADDED_COLUMNS, HISTORY_TABLE, METADATA, SCOPE_PLATFORM, SETTINGS_TABLE, CredentialGovernanceSettings as Settings,
    CredentialGovernanceSettingsHistory as History,
)
from .governance import (
    CONFIG_SECTION, MODE_AUDIT_ONLY, MODE_ENFORCE, MODE_OFF, MODE_RANK, MODES, PROFILE_GOVERNED, PROFILE_LEGACY,
    GovernanceProfile,
)

ENV_MAX_MODE = "ELITEA_CREDENTIAL_GOVERNANCE_MAX_MODE"
ENV_ENABLED = "ELITEA_CREDENTIAL_GOVERNANCE_ENABLED"
ENV_ENVIRONMENT = "ELITEA_CREDENTIAL_GOVERNANCE_ENVIRONMENT"
IGNORED_ENV = tuple(f"ELITEA_CREDENTIAL_GOVERNANCE_{name}" for name in (
    "POLICY_REVISION", "MODE", "ADMIT_LEGACY_PAT", "ALLOW_RUNTIME_DOMAIN_BINDING", "ALLOW_DEVELOPMENT_RUNS"))
IGNORED_CONFIG_KEYS = ("enabled", "policy_revision", "environment", "admit_legacy_pat", "allow_runtime_domain_binding",
                       "allow_development_runs")

SOURCE_MAX_MODE, SOURCE_ENABLED, SOURCE_DEFAULT = "MAX_MODE", "ENABLED (deprecated)", "default"
TTL_S = 10.0            # staleness bound of the snapshot when no event arrives
RETRY_S = 5.0           # a failed read (or a failing preflight) is read again after this long
TTL_RANGE_S = (60, 86400)   # context_ttl_s of the plugin config
DEFAULT_CONTEXT_TTL_S = 3600
MAX_POLICY_REVISION, MAX_ENVIRONMENT, MAX_CHANGE_NOTE = 128, 64, 256
MAX_BODY_BYTES = 16 * 1024   # at most this much of a request body is read; more is refused (413)
MAX_REPORTED_FIELDS = 10
HISTORY_MAX_LIMIT, HISTORY_DEFAULT_LIMIT = 100, 20
CHANGED_EVENT = "credential_governance_settings_changed"
READ_OK, READ_MISSING_TABLE, READ_ERROR = "ok", "missing_table", "error"

# Blocking checks that make governed admissions unavailable while the mode is on. ``audit_backlog`` is evaluated live
# by every admission (step 1, ``AUDIT_UNAVAILABLE``) and ``settings_table`` only matters for saving, so neither is read
# from a snapshot
SNAPSHOT_IGNORED_CHECKS = frozenset({"audit_backlog", "settings_table"})

_SECRET_SHAPES = (   # the shapes tracing's ``assert_no_secret_shaped`` rejects; a note, revision or label is no place for one
    re.compile(r"(?i)\bbearer\s+\S{8,}"), re.compile(r"eyJ[A-Za-z0-9_-]*\.[A-Za-z0-9_-]+"),
    re.compile(r"-----BEGIN"), re.compile(r"gh[pousr]_[A-Za-z0-9]{10,}|github_pat_[A-Za-z0-9_]{10,}"),
    re.compile(r"(?<![A-Za-z0-9])sk-[A-Za-z0-9_-]{16,}"), re.compile(r"xox[bp]-[A-Za-z0-9-]{8,}"),
    re.compile(r"(?<![A-Za-z0-9])(?:AKIA|ASIA)[0-9A-Z]{16}"),
)

MESSAGES = {
    "INVALID_REQUEST": "The request is not valid.",
    "PAYLOAD_TOO_LARGE": "The request is too large.",
    "FORBIDDEN": "Only a super administrator can change the governance switch.",
    "REVISION_CONFLICT": "The settings changed since they were read; read them again and retry.",
    "FIELD_LOCKED": "This value is set by the deployment and cannot be changed here.",
    "MODE_ABOVE_CEILING": "The deployment does not allow this mode.",
    "PREFLIGHT_FAILED": "Governance cannot be turned on until the failed checks are fixed.",
    "POLICY_REVISION_REQUIRED": "A policy revision is required for audit only and enforce.",
    "MIGRATION_REQUIRED": "Settings storage is not set up. Run the task create_governance_settings_tables.",
    "DEPENDENCY_UNAVAILABLE": "The settings could not be read or saved. Try again.",
    "AUDIT_UNAVAILABLE": "The security audit trail is unavailable; nothing was changed.",
}
STATUS = {
    "INVALID_REQUEST": 400, "POLICY_REVISION_REQUIRED": 400, "PAYLOAD_TOO_LARGE": 413, "FORBIDDEN": 403, "REVISION_CONFLICT": 409,
    "FIELD_LOCKED": 409, "MODE_ABOVE_CEILING": 409, "PREFLIGHT_FAILED": 409, "MIGRATION_REQUIRED": 409,
    "DEPENDENCY_UNAVAILABLE": 503, "AUDIT_UNAVAILABLE": 503,
}

# What the Admin UI shows. Returned by the GET so the page renders them as they are
DESCRIPTIONS = {
    "modes": {
        "off": {
            "label": "Off",
            "tooltip": "Agents run exactly as they do without governance: no identity checks, no agent bindings, no "
                       "credential decisions. Use this until bindings and credentials are prepared, or to roll back.",
        },
        "audit_only": {
            "label": "Audit only",
            "tooltip": "Every run is checked as if governance were enforced, and the result is recorded, but nothing "
                       "is refused. Users see no refusals; runs take as long to start as under enforce, so you can "
                       "measure the cost. Use it to find agents and credentials that are not ready before you enforce. "
                       "Sub-agent runs and runs started by schedules or webhooks are not evaluated in this mode.",
        },
        "enforce": {
            "label": "Enforce",
            "tooltip": "Runs are refused unless the user's identity is verified, the agent version has an active "
                       "binding, and every credential it uses is approved and granted to that user. Users see a short "
                       "refusal message. Turn this on only after audit-only shows no unexpected refusals.",
        },
    },
    "fields": {
        "policy_revision": {
            "label": "Policy revision",
            "tooltip": "A label for the rules you approved, for example a change ticket or date. It is recorded on "
                       "every run and approval. Changing it does not stop runs already started. Required for "
                       "audit-only and enforce.",
        },
        "environment": {
            "label": "Environment label",
            "tooltip": "Names this deployment in audit records, for example 'prod-eu'. It changes no behaviour. Use it "
                       "so security reviews can tell environments apart.",
        },
        "admit_legacy_pat": {
            "label": "Admit legacy API tokens",
            "tooltip": "When on, runs started with a personal API token pass the identity check. They still cannot act "
                       "for a user without a mandate. Leave it off unless integrations that use tokens must keep "
                       "running under governance.",
        },
        "allow_runtime_domain_binding": {
            "label": "Allow runtime domain bindings",
            "tooltip": "When on, agents may use a binding whose identity is resolved by the identity provider at run "
                       "time. Leave it off unless your identity team has set up runtime agent identities.",
        },
        "allow_development_runs": {
            "label": "Allow development runs",
            "tooltip": "When on, the agent and pipeline editor's test chat, toolkit tests and on-demand indexing "
                       "may run what has no identity binding yet. Such a run acts as the signed-in author only and may "
                       "use only the author's own credentials: personal ones, and project credentials already granted "
                       "to that person. Nothing it produces can be shared, published or scheduled. Switching to "
                       "enforce switches it on unless you switch it off in the same change.",
        },
        "change_note": {
            "label": "Change note",
            "placeholder": "Why are you changing this? (recorded in the history)",
        },
    },
    "locks": {
        "max_mode": ("This deployment allows governance up to '<max>'. Raising the limit is a deployment change "
                     f"(`{ENV_MAX_MODE}`)."),
        "environment": (f"Set by the deployment (`{ENV_ENVIRONMENT}`). Change it in the deployment configuration; it "
                        "cannot be changed here."),
        "capped": ("The deployment limits governance to '<max>', so the saved mode '<stored>' is not in effect. Raise "
                   f"`{ENV_MAX_MODE}` or choose a lower mode."),
    },
    "preflight": {
        "ready": "Ready: everything audit-only and enforce depend on is in place.",
        "column_missing": "Settings storage needs an update. Run the task create_governance_settings_tables under "
                          "System → Tasks.",
        "not_ready": "Not ready: governance can stay off, but cannot be turned on until the failed checks below are "
                     "fixed.",
        "ready_with_warnings": "Ready, with warnings: governance can be turned on; the items below limit some "
                               "features.",
        "unavailable": "Governance is on but a required service is unavailable. Governed runs are being refused. Fix "
                       "the failed check or switch to Off.",
        "table_missing": "Settings storage is not set up. Run the task create_governance_settings_tables under System "
                         "→ Tasks.",
    },
    "checks": {
        "identity_verifier": "The user identity verifier is not configured.",
        "auth_rpc_mode": "Authentication must run in RPC mode.",
        "security_audit": "The security audit trail is not available.",
        "audit_backlog": "The security audit backlog is over its limit; new governed runs would be refused.",
        "credential_facts": "Credential services are not loaded.",
        "master_key": "The secrets master key is not set.",
        "settings_table": "Settings storage is not set up. Run the task create_governance_settings_tables under System "
                          "→ Tasks.",
        "audit_schema": "The security audit trail cannot record what audit only would have refused yet; audit only "
                        "cannot be turned on until the audit service is updated (enforce is not affected).",
        "preflight": "The preflight could not run.",
        "corporate_vault": "Corporate Vault is not configured; only database-stored credentials can be registered.",
        "governed_worker": "No worker that can run governed agents is online right now.",
    },
    "switch_dialog": {
        "title": "Switch governance to <Mode>?",
        "enforce": "In the last 24 hours, audit-only recorded <n> runs that would have been refused (top reasons "
                   "below). Those runs will now be refused.",
        "off": "Identity, binding and credential checks stop for new runs. Runs already admitted keep their admission.",
    },
}


# --- the deployment keys ---------------------------------------------------------------------

@dataclass(frozen=True)
class Ceiling:
    max_mode: str
    source: str


def _env(environ, name) -> Optional[str]:
    value = environ.get(name)
    value = value.strip() if isinstance(value, str) else None
    return value or None   # an empty variable (a compose `${VAR:-}`) is an unset one


def read_ceiling(environ: Optional[Mapping] = None) -> Ceiling:
    """The deployment ceiling. ``ValueError`` (the plugin does not start) for an unknown value or a deprecated alias
    that disagrees with ``MAX_MODE``."""
    environ = os.environ if environ is None else environ
    raw_max, raw_enabled = _env(environ, ENV_MAX_MODE), _env(environ, ENV_ENABLED)
    max_mode = alias = None
    if raw_max is not None:
        max_mode = raw_max.lower().replace("-", "_")
        if max_mode not in MODES:
            raise ValueError(f"{ENV_MAX_MODE} must be one of off, audit-only, enforce")
    if raw_enabled is not None:
        if raw_enabled.lower() not in ("true", "false"):
            raise ValueError(f"{ENV_ENABLED} must be true or false")
        alias = MODE_ENFORCE if raw_enabled.lower() == "true" else MODE_OFF
    if max_mode is not None and alias is not None and max_mode != alias:
        raise ValueError(f"{ENV_ENABLED} (deprecated) and {ENV_MAX_MODE} disagree; set only {ENV_MAX_MODE}")
    if max_mode is not None:
        return Ceiling(max_mode, SOURCE_MAX_MODE)
    if alias is not None:
        return Ceiling(alias, SOURCE_ENABLED)
    return Ceiling(MODE_ENFORCE, SOURCE_DEFAULT)


def environment_label(environ: Optional[Mapping] = None, strict=True) -> Optional[str]:
    """The environment label the deployment sets (it locks the field), or None."""
    value = _env(os.environ if environ is None else environ, ENV_ENVIRONMENT)
    if value is not None and len(value) > MAX_ENVIRONMENT:
        if strict:
            raise ValueError(f"{ENV_ENVIRONMENT} must be at most {MAX_ENVIRONMENT} characters")
        return None
    return value


def ignored_settings(config: Optional[Mapping], environ: Optional[Mapping] = None) -> list:
    """The keys that used to configure governance and are still present: every one is ignored."""
    environ = os.environ if environ is None else environ
    found = [name for name in IGNORED_ENV if _env(environ, name) is not None]
    section = (config or {}).get(CONFIG_SECTION)
    if isinstance(section, Mapping):
        found += [f"{CONFIG_SECTION}.{key}" for key in IGNORED_CONFIG_KEYS if key in section]
    return found


def load_context_ttl(config: Optional[Mapping], strict=True) -> int:
    """``context_ttl_s`` of the plugin config (how long an admitted run may start or restart)."""
    section = (config or {}).get(CONFIG_SECTION)
    ttl = section.get("context_ttl_s", DEFAULT_CONTEXT_TTL_S) if isinstance(section, Mapping) else DEFAULT_CONTEXT_TTL_S
    if isinstance(ttl, bool) or not isinstance(ttl, int) or not TTL_RANGE_S[0] <= ttl <= TTL_RANGE_S[1]:
        if not strict:
            return DEFAULT_CONTEXT_TTL_S
        raise ValueError(
            f"{CONFIG_SECTION}.context_ttl_s must be an integer from {TTL_RANGE_S[0]} to {TTL_RANGE_S[1]}, got {ttl!r}")
    return ttl


# --- the resolved state ----------------------------------------------------------------------

@dataclass(frozen=True)
class Check:
    """One preflight check: ``status`` is ``ok``, ``fail`` (blocking or not) or ``warn``."""
    id: str
    status: str
    code: str
    blocking: bool = True


@dataclass(frozen=True)
class Stored:
    mode: str
    policy_revision: Optional[str]
    admit_legacy_pat: bool
    allow_runtime_domain_binding: bool
    environment: Optional[str]
    revision: int
    updated_by: Optional[int]
    updated_at: Optional[datetime]
    allow_development_runs: Optional[bool] = None   # None: not chosen, the default of the mode applies (#7213)


@dataclass(frozen=True)
class Effective:
    mode: str
    policy_revision: Optional[str]
    admit_legacy_pat: bool
    allow_runtime_domain_binding: bool
    environment: Optional[str]
    capped: bool          # the stored mode is above the ceiling
    unavailable: bool     # the mode is on and a required service is not
    read: str             # ok | missing_table | error
    allow_development_runs: bool = False   # the editor's test chat may run an unbound version as the author (#7213)
    profile: GovernanceProfile = field(init=False, compare=False)

    def __post_init__(self):
        enforce = self.mode == MODE_ENFORCE
        revision = self.policy_revision if self.mode != MODE_OFF else None
        object.__setattr__(self, "profile", GovernanceProfile(
            enabled=enforce, policy_revision=revision, profile=PROFILE_GOVERNED if enforce else PROFILE_LEGACY))


def development_runs_default(mode: str) -> bool:
    """The ``allow_development_runs`` an operator has not chosen: on under audit only (the editor's test chat is then
    rehearsed as a development run, not recorded as a would-deny), off under enforce and off (#7213). A deployment
    already in enforce keeps what it has; only a switch into enforce turns development runs on (#7227 D14)."""
    return mode == MODE_AUDIT_ONLY


DEVELOPMENT_RUNS_WITH_ENFORCE_NOTE = " (development runs enabled with enforce)"


def development_runs_on_change(update_mode: str, old_mode: str, chosen: Optional[bool]) -> Optional[bool]:
    """What the write stores for ``allow_development_runs`` (#7227 D14). The operator's explicit value always wins: it
    is stored as sent. Only a write that switches into enforce and says nothing about it (an API caller) switches
    development runs on, so an author meets enforce with the editor, toolkit tests and indexing working; the Admin UI
    sends the switch it shows (on when the mode is changed to enforce, unless the operator turns it off)."""
    if chosen is None and update_mode == MODE_ENFORCE and old_mode != MODE_ENFORCE:
        return True
    return chosen


def resolve(stored: Optional[Stored], *, ceiling: str, env_environment: Optional[str], preflight, read: str) -> Effective:
    """What is in effect. Pure.

    The mode is the stored one (``off`` without a row or a table), capped to the ceiling. A settings row that cannot be
    read on a cold start counts as ``enforce`` (so governed admissions are refused) unless the ceiling is ``off``. The
    policy revision and the flags are the stored ones, or the defaults (``allow_development_runs`` not chosen: the
    default of the mode in effect). The environment is the deployment's label when it sets one. A failing blocking check
    makes an on mode ``unavailable``."""
    row = stored if read == READ_OK else None
    wanted = row.mode if row is not None else (MODE_ENFORCE if read == READ_ERROR else MODE_OFF)
    mode = wanted if MODE_RANK[wanted] <= MODE_RANK[ceiling] else ceiling
    failing = [check for check in preflight
               if check.status == "fail" and check.blocking and check.id not in SNAPSHOT_IGNORED_CHECKS]
    chosen = row.allow_development_runs if row is not None else None
    return Effective(
        mode=mode,
        policy_revision=row.policy_revision if row is not None else None,
        admit_legacy_pat=bool(row.admit_legacy_pat) if row is not None else False,
        allow_runtime_domain_binding=bool(row.allow_runtime_domain_binding) if row is not None else False,
        allow_development_runs=bool(chosen) if chosen is not None else development_runs_default(mode),
        environment=env_environment or (row.environment if row is not None else None),
        capped=row is not None and mode != wanted,
        unavailable=mode != MODE_OFF and (read == READ_ERROR or bool(failing)),
        read=read,
    )


# --- the stored row --------------------------------------------------------------------------

def _now() -> datetime:
    return datetime.now(timezone.utc)


def _table_present(session, name=SETTINGS_TABLE) -> bool:
    return inspect(session.connection()).has_table(name, schema=c.POSTGRES_SCHEMA)


_ROW = (Settings.mode, Settings.policy_revision, Settings.admit_legacy_pat, Settings.allow_runtime_domain_binding,
        Settings.environment, Settings.revision, Settings.updated_by, Settings.updated_at)
_ADDED = (Settings.allow_development_runs,)   # read only once the table has the column (#7213)
_PLATFORM = (Settings.scope_kind == SCOPE_PLATFORM, Settings.scope_id == 0)
_COLUMNS_SEEN = set()   # tables seen with every added column: a column never goes away, so it is looked for once


def added_columns_present(session, table=SETTINGS_TABLE) -> bool:
    """Does this table have the columns added after the tables first shipped (``ADDED_COLUMNS``)? One catalog read
    per table and process until it does."""
    if table in _COLUMNS_SEEN:
        return True
    names = {column["name"] for column in inspect(session.connection()).get_columns(table, schema=c.POSTGRES_SCHEMA)}
    if all(name in names for name, _ in ADDED_COLUMNS):
        _COLUMNS_SEEN.add(table)
        return True
    return False


def _stored_of(row) -> Stored:
    return Stored(mode=row.mode, policy_revision=row.policy_revision, admit_legacy_pat=bool(row.admit_legacy_pat),
                  allow_runtime_domain_binding=bool(row.allow_runtime_domain_binding),
                  allow_development_runs=getattr(row, "allow_development_runs", None), environment=row.environment,
                  revision=row.revision, updated_by=row.updated_by, updated_at=row.updated_at)


def read_settings():
    """``(read, stored)``: ``ok`` with the row (or None), ``missing_table``, or ``error`` (logged by type only). A table
    that predates a column is read without it (the flag then takes its default): a deploy before the task is not an
    error."""
    try:
        with db.get_session(None) as session:
            if not _table_present(session):
                return READ_MISSING_TABLE, None
            columns = _ROW + _ADDED if added_columns_present(session) else _ROW
            row = session.execute(select(*columns).where(*_PLATFORM)).one_or_none()
            return READ_OK, _stored_of(row) if row is not None else None
    except Exception as exc:  # pylint: disable=W0703
        log.error("governance settings: the stored switch could not be read (%s)", type(exc).__name__)
        return READ_ERROR, None


class GovernanceState:
    """The snapshot dispatch reads, and the refresher that keeps it fresh. ``effective`` never does I/O.

    ``refresh`` is serialised: the read and the swap happen under one lock, so a refresh that read the row before a
    change cannot publish it after the refresh that read the change."""

    POLL_S = 1.0   # how often the refresher looks at the clock
    JOIN_S = 2.0   # how long ``stop`` waits for the refresher

    def __init__(  # pylint: disable=R0913
        self, *, ceiling: Ceiling, env_environment: Optional[str], context_ttl_s: int,
        preflight: Callable, reader: Callable = read_settings, clock: Callable = time.monotonic,
        on_enforce: Optional[Callable] = None, on_unreadable: Optional[Callable] = None,
    ):
        self.ceiling = ceiling
        self.env_environment = env_environment
        self.context_ttl_s = context_ttl_s
        self._preflight = preflight       # (read, mode) -> list[Check]
        self._reader = reader
        self._clock = clock
        self._on_enforce = on_enforce         # registers what a governed run needs; raises when it cannot
        self._on_unreadable = on_unreadable   # (effective) the stored mode vanished: say so
        self._lock = threading.Lock()
        self._last_good = None            # (read, stored) of the last read that did not fail
        self._unreadable_reported = False
        self.revision = 0                 # the stored revision the snapshot was built from
        self._due = 0.0
        self._stop = threading.Event()
        self._thread = None
        self.effective = resolve(None, ceiling=ceiling.max_mode, env_environment=env_environment, preflight=[],
                                 read=READ_ERROR)   # until the first read: unknown

    def _checks(self, read, mode):
        try:
            return list(self._preflight(read, mode))
        except Exception as exc:  # pylint: disable=W0703
            log.error("governance settings: the preflight could not run (%s)", type(exc).__name__)
            return [Check("preflight", "fail", "UNAVAILABLE", True)]

    def refresh(self) -> Effective:  # pylint: disable=R0912
        """Read the row and the preflight and swap the snapshot.

        A failed read keeps the last good row (a failure on a cold start is ``error``). So does a read that finds no
        table or no row after the mode was seen on: that is not a switch to off, so it is kept, logged and audited.
        The governed handlers are registered before an ``enforce`` snapshot is published; if they cannot be, the
        snapshot says ``unavailable`` and the next tick tries again."""
        with self._lock:
            read, stored = self._reader()
            previous, failed, vanished = self._last_good, read == READ_ERROR, False
            if failed and previous is not None:
                read, stored = previous
            elif not failed and previous is not None and previous[1] is not None and previous[1].mode != MODE_OFF \
                    and (read == READ_MISSING_TABLE or stored is None):
                read, stored, failed, vanished = previous[0], previous[1], True, True
            elif not failed:
                self._last_good, self._unreadable_reported = (read, stored), False
                self.revision = stored.revision if stored is not None else 0
            args = dict(ceiling=self.ceiling.max_mode, env_environment=self.env_environment, read=read)
            effective = resolve(stored, preflight=[], **args)
            if effective.mode != MODE_OFF:   # off asks nothing of the platform: no preflight, no audit probe
                effective = resolve(stored, preflight=self._checks(read, effective.mode), **args)
            if effective.mode == MODE_ENFORCE and self._on_enforce is not None and not self._stop.is_set():
                try:
                    self._on_enforce()
                except Exception as exc:  # pylint: disable=W0703
                    log.error("governance settings: the governed handlers could not be registered (%s)",
                              type(exc).__name__)
                    effective = replace(effective, unavailable=True)
            self.effective = effective
            self._due = self._clock() + (RETRY_S if failed or effective.unavailable else TTL_S)
            report = vanished and not self._unreadable_reported
            if vanished:
                self._unreadable_reported = True
        if vanished:
            log.error("governance settings: the stored switch is gone (no table or row) while the mode was %s; the "
                      "last known state is kept", effective.mode)
        if report and self._on_unreadable is not None:
            try:
                self._on_unreadable(effective)
            except Exception as exc:  # pylint: disable=W0703
                log.error("governance settings: the lost switch was not recorded (%s)", type(exc).__name__)
        return effective

    def tick(self, now: Optional[float] = None) -> bool:
        """Refresh when the snapshot is older than ``TTL_S`` (``RETRY_S`` after a failure). True when it did."""
        if (self._clock() if now is None else now) < self._due:
            return False
        self.refresh()
        return True

    def start(self):
        """Start the refresher. A deployment whose ceiling is ``off`` has nothing to refresh: the mode cannot leave off."""
        if self._thread is not None or self.ceiling.max_mode == MODE_OFF:
            return

        def _run():
            while not self._stop.wait(self.POLL_S):
                try:
                    self.tick()
                except Exception as exc:  # pylint: disable=W0703
                    log.error("governance settings: the refresh failed (%s)", type(exc).__name__)

        self._thread = threading.Thread(target=_run, name="governance-settings", daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()
        thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(self.JOIN_S)


def create_state(module) -> GovernanceState:
    """The state of this module: deployment keys validated (``ValueError`` refuses startup), ignored keys warned about,
    first read done."""
    environ, config = os.environ, module.descriptor.config
    ceiling = read_ceiling(environ)
    strict = ceiling.max_mode != MODE_OFF   # a platform that cannot turn governance on is not stopped by its settings
    if _env(environ, ENV_ENABLED) is not None:
        log.warning("%s is deprecated: use %s. It now sets the ceiling only and does not switch governance on",
                    ENV_ENABLED, ENV_MAX_MODE)
    for name in ignored_settings(config, environ):
        log.warning("%s is ignored: the mode, policy revision and flags are set in the Admin UI", name)

    def preflight(read, mode):
        from .admission import governance_preflight  # pylint: disable=C0415
        return governance_preflight(module, table_read=read, mode=mode)

    def on_enforce():
        ensure = getattr(module, "_ensure_governed_handlers_registered", None)
        if ensure is not None and ensure() is False:
            raise RuntimeError("the governed worker handlers are not registered")

    state = GovernanceState(
        ceiling=ceiling, env_environment=environment_label(environ, strict),
        context_ttl_s=load_context_ttl(config, strict), preflight=preflight, on_enforce=on_enforce,
        on_unreadable=record_settings_unreadable)
    state.refresh()
    return state


def record_settings_unreadable(effective) -> None:
    """The stored switch disappeared (table or row) while the mode was on: the last state is kept; leave a trace."""
    try:
        this.for_module("tracing").module.security_audit_record_autocommit({
            key: value for key, value in dict(
                event_type="lifecycle", reason_code="GOVERNANCE_SETTINGS_UNREADABLE", outcome="failed",
                caller_ref="system:governance", policy_revision=effective.policy_revision,
                environment=effective.environment).items() if value is not None})
    except Exception as exc:  # pylint: disable=W0703
        log.error("governance settings: the lost switch was not recorded (%s)", type(exc).__name__)


def record_boot_preflight(module) -> None:
    """Boot with the mode on and a failing preflight: governed admissions are refused until it is fixed. Say so in the
    log and the audit trail (best effort: the audit trail may be the thing that is down)."""
    effective = module.governance_state.effective
    if effective.mode == MODE_OFF or not effective.unavailable:
        return
    log.error("Credential governance is %s but a required service is unavailable: governed runs are refused "
              "until the preflight passes", effective.mode)
    try:
        this.for_module("tracing").module.security_audit_record_autocommit({
            key: value for key, value in dict(
                event_type="lifecycle", reason_code="GOVERNANCE_PREFLIGHT_FAILED", outcome="failed",
                caller_ref="system:boot", policy_revision=effective.policy_revision,
                environment=effective.environment).items() if value is not None})
    except Exception as exc:  # pylint: disable=W0703
        log.error("governance settings: the failed preflight was not recorded (%s)", type(exc).__name__)


def on_settings_changed(module, payload=None) -> None:
    """Another replica (or this one) changed the switch: read it now instead of at the next tick."""
    state = getattr(module, "governance_state", None)
    if state is None:
        return
    revision = payload.get("revision") if isinstance(payload, dict) else None
    if isinstance(revision, int) and not isinstance(revision, bool) and revision <= state.revision:
        return   # a change this process already read (its own, or one it caught up with)
    state.refresh()


# --- the API payloads ------------------------------------------------------------------------

SESSION_REQUIRED_MESSAGE = "The governance switch needs an interactive session; an API token cannot change it."


class SettingsError(Exception):
    def __init__(self, code, message=None, **extra):
        super().__init__(code)
        self.code, self.extra = code, extra
        self.message, self.status = message or MESSAGES[code], STATUS[code]

    def body(self) -> dict:
        return {"error": self.message, "code": self.code, **self.extra}


def _iso(moment):
    return moment.isoformat() if isinstance(moment, datetime) else None


def _stored_json(stored: Optional[Stored]):
    if stored is None:
        return None
    return {"mode": stored.mode, "policy_revision": stored.policy_revision, "admit_legacy_pat": stored.admit_legacy_pat,
            "allow_runtime_domain_binding": stored.allow_runtime_domain_binding,
            "allow_development_runs": stored.allow_development_runs, "environment": stored.environment,
            "revision": stored.revision, "updated_by": stored.updated_by, "updated_at": _iso(stored.updated_at)}


def _preflight_json(checks) -> dict:
    blocking_failed = any(check.status == "fail" and check.blocking for check in checks)
    warned = any(check.status == "warn" for check in checks)
    return {
        "ok": not blocking_failed,
        "state": "not_ready" if blocking_failed else ("ready_with_warnings" if warned else "ready"),
        "checks": [{"id": check.id, "status": check.status, "code": check.code, "blocking": check.blocking,
                    "message": None if check.status == "ok" else DESCRIPTIONS["checks"].get(check.id)}
                   for check in checks],
    }


def settings_payload(module, read, stored, checks, columns_present=True) -> dict:
    """The body of the GET (and of a successful PUT): what is in effect, what is stored, what the deployment fixes,
    the preflight, and the texts the page shows. ``columns_present`` false: the table predates a column and cannot be
    saved until the task runs (the page says so, as for a missing table)."""
    state = module.governance_state
    effective = resolve(stored, ceiling=state.ceiling.max_mode, env_environment=state.env_environment,
                        preflight=checks, read=read)
    if read == READ_ERROR:   # the row could not be read just now: show what this process actually enforces
        effective = state.effective
    return {
        "effective": {
            "mode": effective.mode, "policy_revision": effective.policy_revision,
            "admit_legacy_pat": effective.admit_legacy_pat,
            "allow_runtime_domain_binding": effective.allow_runtime_domain_binding,
            "allow_development_runs": effective.allow_development_runs,
            "environment": effective.environment, "capped": effective.capped, "unavailable": effective.unavailable,
        },
        "stored": _stored_json(stored),
        "locks": {"environment": {"env_var": ENV_ENVIRONMENT}} if state.env_environment else {},
        "ceiling": {"max_mode": state.ceiling.max_mode, "source": state.ceiling.source},
        "preflight": _preflight_json(checks),
        "table_present": read != READ_MISSING_TABLE,
        "columns_present": columns_present,
        "read": read,
        "descriptions": DESCRIPTIONS,
    }


def current_state(module) -> dict:
    """The GET: a fresh read of the row and a fresh preflight (the snapshot dispatch uses may be a few seconds old)."""
    from .admission import governance_preflight  # pylint: disable=C0415
    read, stored = read_settings()
    state = module.governance_state
    mode = resolve(stored, ceiling=state.ceiling.max_mode, env_environment=state.env_environment, preflight=[],
                   read=read).mode
    return settings_payload(module, read, stored, governance_preflight(
        module, table_read=read, mode=None if mode == MODE_OFF else mode), columns_present=_columns_present())


def _columns_present() -> bool:
    """Do the settings tables have every column this build writes? Unknown (unreadable) counts as yes: saving then
    fails readably on its own."""
    if SETTINGS_TABLE in _COLUMNS_SEEN:
        return True
    try:
        with db.get_session(None) as session:
            return not _table_present(session) or added_columns_present(session)
    except Exception as exc:  # pylint: disable=W0703
        log.error("governance settings: the settings columns could not be read (%s)", type(exc).__name__)
        return True


# --- the change ------------------------------------------------------------------------------

def _no_secret_shape(value):
    if value is not None and any(shape.search(value) for shape in _SECRET_SHAPES):
        raise ValueError("a secret-shaped value is not accepted here")
    return value


class SettingsUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    mode: Literal["off", "audit_only", "enforce"]
    policy_revision: Optional[str] = Field(default=None, max_length=MAX_POLICY_REVISION)
    admit_legacy_pat: StrictBool
    allow_runtime_domain_binding: StrictBool
    # Absent or null: not chosen, the default of the mode applies (on under audit only, off otherwise; #7213); a switch into
    # enforce that says nothing about it switches development runs on (#7227 D14)
    allow_development_runs: Optional[StrictBool] = None
    environment: Optional[str] = Field(default=None, max_length=MAX_ENVIRONMENT)
    change_note: str = Field(min_length=1, max_length=MAX_CHANGE_NOTE)
    expected_revision: StrictInt = Field(ge=0)   # the revision that was read; 0 when there was no row

    @field_validator("policy_revision", "environment", mode="after")
    @classmethod
    def _blank_is_none(cls, value):
        return _no_secret_shape(value or None)

    @field_validator("change_note", mode="after")
    @classmethod
    def _note(cls, value):
        return _no_secret_shape(value)


def read_body(stream, content_length=None):
    """The JSON body of a request, reading at most ``MAX_BODY_BYTES + 1`` bytes (a chunked body has no length to
    trust). ``SettingsError`` 413 above the limit, 400 when it is not JSON."""
    if isinstance(content_length, int) and content_length > MAX_BODY_BYTES:
        raise SettingsError("PAYLOAD_TOO_LARGE")
    raw = stream.read(MAX_BODY_BYTES + 1)
    if len(raw) > MAX_BODY_BYTES:
        raise SettingsError("PAYLOAD_TOO_LARGE")
    try:
        return json.loads(raw)
    except (ValueError, UnicodeDecodeError):
        raise SettingsError("INVALID_REQUEST") from None


def parse_update(body) -> SettingsUpdate:
    if not isinstance(body, dict):
        raise SettingsError("INVALID_REQUEST")
    try:
        return SettingsUpdate.model_validate(body)
    except ValidationError as exc:
        problems = exc.errors(include_input=False, include_context=False, include_url=False)[:MAX_REPORTED_FIELDS]
        fields = [{"field": ".".join(str(part) for part in item["loc"])[:64], "problem": item["msg"][:128]}
                  for item in problems]
        raise SettingsError("INVALID_REQUEST", fields=fields) from None


def _audit_events(update_, actor_id, environment, moved) -> list:
    base = dict(event_type="lifecycle", outcome="success", caller_ref=f"operator:{actor_id}",
                policy_revision=update_.policy_revision, environment=environment)
    events = [dict(base, reason_code="GOVERNANCE_SETTINGS_CHANGED")]
    if moved:
        events.append(dict(base, reason_code=f"GOVERNANCE_MODE_{update_.mode.upper()}"))
    return [{key: value for key, value in event.items() if value is not None} for event in events]


def apply_update(module, body, actor_id: int) -> dict:  # pylint: disable=R0912,R0914,R0915
    """The PUT: validate, check the ceiling, the lock and the preflight, then change the row (compare-and-set on the
    revision), write the history and the audit events in one transaction. ``SettingsError`` for every refusal."""
    from .admission import governance_preflight  # pylint: disable=C0415
    update_ = parse_update(body)
    state = module.governance_state
    read, current = read_settings()
    if read == READ_MISSING_TABLE:
        raise SettingsError("MIGRATION_REQUIRED")
    if read != READ_OK:
        raise SettingsError("DEPENDENCY_UNAVAILABLE")
    if MODE_RANK[update_.mode] > MODE_RANK[state.ceiling.max_mode]:
        raise SettingsError("MODE_ABOVE_CEILING", max_mode=state.ceiling.max_mode)
    if state.env_environment and update_.environment not in (None, state.env_environment):
        raise SettingsError("FIELD_LOCKED", field="environment")
    if update_.mode != MODE_OFF:
        if update_.policy_revision is None:
            raise SettingsError("POLICY_REVISION_REQUIRED")
        failed = [check.id for check in governance_preflight(module, table_read=read, mode=update_.mode)
                  if check.status == "fail" and check.blocking]
        if failed:
            raise SettingsError("PREFLIGHT_FAILED", failed_checks=failed)
    old_mode = current.mode if current is not None else MODE_OFF
    development = development_runs_on_change(update_.mode, old_mode, update_.allow_development_runs)
    automatic = development is True and update_.allow_development_runs is None   # switched on with enforce
    change_note = update_.change_note
    if automatic:   # the history says so: nobody chose it in this write
        change_note = (change_note[:MAX_CHANGE_NOTE - len(DEVELOPMENT_RUNS_WITH_ENFORCE_NOTE)]
                       + DEVELOPMENT_RUNS_WITH_ENFORCE_NOTE)
    values = dict(mode=update_.mode, policy_revision=update_.policy_revision,
                  admit_legacy_pat=update_.admit_legacy_pat,
                  allow_runtime_domain_binding=update_.allow_runtime_domain_binding,
                  allow_development_runs=development, environment=update_.environment)
    events = _audit_events(update_, actor_id, state.env_environment or update_.environment, update_.mode != old_mode)
    tracing = this.for_module("tracing").module
    now = _now()
    with db.get_session(None) as session:
        try:
            if not added_columns_present(session):   # a table from before the column: the task adds it
                raise SettingsError("MIGRATION_REQUIRED")
            row = session.execute(select(Settings.id, Settings.revision).where(*_PLATFORM)).one_or_none()
            if row is None:
                if update_.expected_revision != 0:
                    raise SettingsError("REVISION_CONFLICT")
                revision = 1
                session.execute(insert(Settings).values(
                    **values, scope_kind=SCOPE_PLATFORM, scope_id=0, revision=revision, updated_by=actor_id,
                    updated_at=now, created_at=now))
            else:
                revision = row.revision + 1
                changed = session.execute(
                    update(Settings).where(Settings.id == row.id, Settings.revision == update_.expected_revision)
                    .values(**values, revision=revision, updated_by=actor_id, updated_at=now)
                    .execution_options(synchronize_session=False)).rowcount
                if changed != 1:
                    raise SettingsError("REVISION_CONFLICT")
            session.execute(insert(History).values(
                **values, scope_kind=SCOPE_PLATFORM, scope_id=0, revision=revision, change_note=change_note,
                changed_by=actor_id, changed_at=now))
            try:
                for event in events:
                    tracing.security_audit_record(session, event)
            except Exception as exc:  # pylint: disable=W0703
                log.error("governance settings: the change was not audited (%s)", type(exc).__name__)
                raise SettingsError("AUDIT_UNAVAILABLE") from None
            session.commit()
        except SettingsError:
            session.rollback()
            raise
        except IntegrityError:   # a concurrent first write took the row
            session.rollback()
            raise SettingsError("REVISION_CONFLICT") from None
        except Exception as exc:  # pylint: disable=W0703
            session.rollback()
            log.error("governance settings: the change was not saved (%s)", type(exc).__name__)
            raise SettingsError("DEPENDENCY_UNAVAILABLE") from None
    state.refresh()   # this replica reads the new row right away; its own event is then skipped by revision
    try:   # the other replicas read it now
        module.context.event_manager.fire_event(
            CHANGED_EVENT, {"revision": revision, "old_mode": old_mode, "new_mode": update_.mode})
    except Exception as exc:  # pylint: disable=W0703
        log.error("governance settings: the change event was not sent (%s)", type(exc).__name__)
    return current_state(module)


def history_page(limit=HISTORY_DEFAULT_LIMIT, offset=0) -> dict:
    """The GET of the history: newest first, ``{total, rows, table_present}``. Raises ``SettingsError`` when it cannot
    be read."""
    limit = max(1, min(int(limit), HISTORY_MAX_LIMIT))
    offset = max(0, int(offset))
    try:
        with db.get_session(None) as session:
            if not _table_present(session, HISTORY_TABLE):
                return {"total": 0, "rows": [], "table_present": False}
            total = session.execute(select(func.count()).select_from(History).where(
                History.scope_kind == SCOPE_PLATFORM, History.scope_id == 0)).scalar_one()
            columns = [History.revision, History.mode, History.policy_revision, History.admit_legacy_pat,
                       History.allow_runtime_domain_binding, History.environment, History.change_note,
                       History.changed_by, History.changed_at]
            if added_columns_present(session, HISTORY_TABLE):
                columns.append(History.allow_development_runs)
            rows = session.execute(
                select(*columns)
                .where(History.scope_kind == SCOPE_PLATFORM, History.scope_id == 0)
                .order_by(History.revision.desc()).limit(limit).offset(offset)).all()
    except Exception as exc:  # pylint: disable=W0703
        log.error("governance settings: the history could not be read (%s)", type(exc).__name__)
        raise SettingsError("DEPENDENCY_UNAVAILABLE") from None
    emails = {}
    ids = sorted({row.changed_by for row in rows if row.changed_by is not None})
    if ids:
        try:
            emails = {user.get("id"): user.get("email") for user in auth.list_users(user_ids=ids) or []}
        except Exception as exc:  # pylint: disable=W0703
            log.error("governance settings: the users of the history could not be read (%s)", type(exc).__name__)
    return {"total": total, "table_present": True, "rows": [{
        "revision": row.revision, "mode": row.mode, "policy_revision": row.policy_revision,
        "flags": {"admit_legacy_pat": bool(row.admit_legacy_pat),
                  "allow_runtime_domain_binding": bool(row.allow_runtime_domain_binding),
                  "allow_development_runs": getattr(row, "allow_development_runs", None)},
        "environment": row.environment, "change_note": row.change_note,
        "changed_by": None if row.changed_by is None else {"id": row.changed_by, "email": emails.get(row.changed_by)},
        "changed_at": _iso(row.changed_at),
    } for row in rows]}


def is_super_admin(user_id) -> bool:
    """Does this user hold the ``super_admin`` role? The switch is pinned to it in addition to its permission, so a
    role matrix edit cannot widen it. Fails closed."""
    if isinstance(user_id, bool) or not isinstance(user_id, int):
        return False
    try:
        roles = auth.get_user_roles(user_id, mode="administration") or []
    except Exception as exc:  # pylint: disable=W0703
        log.error("governance settings: the roles of the caller could not be read (%s)", type(exc).__name__)
        return False
    return "super_admin" in {role.get("name") if isinstance(role, dict) else role for role in roles}


# --- the admin task --------------------------------------------------------------------------

def create_tables(dry_run=False) -> dict:
    """Create the two settings tables where missing, and add the columns that came after them to a table that
    predates them (``ADDED_COLUMNS``; the only DDL of the switch). Idempotent."""
    with db.get_session(None) as session:
        connection = session.connection()
        missing = [name for name in (SETTINGS_TABLE, HISTORY_TABLE)
                   if not inspect(connection).has_table(name, schema=c.POSTGRES_SCHEMA)]
        if missing and not dry_run:
            METADATA.create_all(connection, checkfirst=True)
        added = []
        for table in (SETTINGS_TABLE, HISTORY_TABLE):
            if table in missing:
                continue   # created above with every column, or would be
            present = {column["name"] for column in inspect(connection).get_columns(table, schema=c.POSTGRES_SCHEMA)}
            for name, definition in ADDED_COLUMNS:
                if name in present:
                    continue
                added.append(f"{table}.{name}")
                if not dry_run:   # the catalog said it is missing; the names come from this module, not from input
                    session.execute(text(f'ALTER TABLE "{c.POSTGRES_SCHEMA}".{table} ADD COLUMN {name} {definition}'))
        if not dry_run and (missing or added):
            session.commit()
            _COLUMNS_SEEN.clear()   # read the columns again: they are there now
    return {"created": [] if dry_run else missing, "would_create": missing if dry_run else [],
            "added_columns": [] if dry_run else added, "would_add_columns": added if dry_run else [], "dry_run": dry_run}


# --- governed runs that outlive a restart ---------------------------------------------------------

LIVE_CHUNK = 100   # project schemas per query
_CONTEXT_SCHEMAS = text(
    "SELECT table_schema FROM information_schema.tables "
    "WHERE table_name = 'execution_security_context' AND table_schema ~ '^p_[0-9]+$'")
_PROJECT_SCHEMA = re.compile(r"^p_[0-9]+$")


def live_context_exists(session, schemas, now) -> bool:
    """Is there an admitted or running context that has not expired in any of these project schemas? One query per
    ``LIVE_CHUNK`` schemas (each part reads the index on ``(state, expires_at)``), and the first chunk with a hit ends it.
    Schema names come from the catalog and are checked against ``p_<digits>`` before they are quoted into the text."""
    names = [name for name in schemas if _PROJECT_SCHEMA.match(name)]
    for start in range(0, len(names), LIVE_CHUNK):
        parts = " UNION ALL ".join(
            f'SELECT 1 AS live FROM "{name}".execution_security_context '
            "WHERE state IN ('admitted', 'running') AND expires_at > :now"
            for name in names[start:start + LIVE_CHUNK])
        if session.execute(text(f"SELECT 1 FROM ({parts}) AS found LIMIT 1"), {"now": now}).first() is not None:
            return True
    return False


def has_live_contexts() -> bool:
    """A restart into ``off`` must still serve the runs admitted under ``enforce`` before it: do any project schemas hold
    a live context? One catalog query, then bounded chunks. A failure is logged and answers no."""
    try:
        with db.get_session(None) as session:
            schemas = list(session.execute(_CONTEXT_SCHEMAS).scalars())
            return live_context_exists(session, schemas, _now())
    except Exception as exc:  # pylint: disable=W0703
        log.error("governance settings: live governed runs could not be looked for (%s)", type(exc).__name__)
        return False
