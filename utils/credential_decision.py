"""Credential-use decision for governed runs (#6757, C-1009).

A governed run may use a toolkit credential only through an active credential reference, a use grant
for this agent binding, toolkit and verified user, and the ``credentials.use`` permission. The decision
is taken at admission, from the stored definition of the agent (what the toolkits select) cross-checked
against the dispatched payload (what will actually run); nothing in the payload is used as authority.

Metadata only: this module reads configuration and credential-governance facts through two in-process
methods and never touches a secret store. A fact that cannot be read is a refusal, never a pass.

A configuration can have a shared reference and personal ones, one per owner (#7119). The facts are read for the caller,
so the reference found is the caller's own personal one when there is one, else the shared one; a personal reference is
then usable only by its custodian: the person (``custodian_subject_ref`` equals the subject of the run) or, for a
mandate, the mandate's owner.

The caller's personal reference wins even when it is not active (fail closed): while it is pending, validated or
suspended the owner's runs are refused, and they do not fall back to the shared credential. The facts carry a ``note``
saying so for the UI.

Known limits (follow-ups, no code yet): the owner of a mandate is read at admission, and a change of owner after it is
not re-read when a queued run is re-checked; the facts' fallback to another owner's personal reference is capped, so a
crowded configuration can crowd another out (both only change which refusal is named); and ``ref_ids`` are keyed by
configuration id, so one configuration's two references asked for together return one.
"""
import contextlib
import contextvars
import uuid
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Callable, Literal, Mapping, Optional

from pydantic import BaseModel, ConfigDict
from pylon.core.tools import log
from tools import auth, this

from . import schema_registry_version
from .definition_digest import toolkit_rows

MAX_GOVERNED_CREDENTIALS_PER_RUN = 64
MAX_CANDIDATE_CONFIGURATIONS = 256   # configurations looked up per run, platform-internal ones included
MAX_FACT_SELECTORS = 64              # what one call of a facts method accepts
GRANTS_FOR_FACTS_VERSION = 4         # the secrets facts that answer the grants of a configuration's reference in the same call
MAX_CACHED_SECRET_FIELDS = 512
CREDENTIAL_SECTION = "credentials"
USE_PERMISSION = "credentials.use"
STORE_KINDS = frozenset({"database", "corporate_vault"})
ALLOWED = "CREDENTIAL_USE_ALLOWED"
WHOLE_TARGET = "*"
MANDATE_SUBJECT_PREFIX = "mandate:"
NESTED_FIELD = "<nested>"
# The shape a stored toolkit credential has (``_expand_toolkit_settings`` accepts exactly this)
CREDENTIAL_KEYS = frozenset({"elitea_title", "private", "configuration_uuid", "configuration_project_id"})
SELECTOR_KEYS = frozenset({"elitea_title", "configuration_uuid"})

Selector = Literal["uuid", "title", "inline", "undeclared"]


class CredentialDecisionError(Exception):
    """A run is refused for a credential reason. ``audit`` holds reference fields for the deny event only."""

    def __init__(self, reason_code: str, audit: Optional[dict] = None):
        super().__init__(reason_code)
        self.reason_code = reason_code
        self.audit = audit or {}


@dataclass(frozen=True)
class CredentialUse:
    """One credential a run uses. ``selector`` is how the stored definition selects it; ``undeclared`` is a
    credential the payload carries that the stored definition does not declare at that place."""
    toolkit_instance_id: str
    field: str
    configuration_uuid: Optional[str]
    selector: Selector
    configuration_project_id: Optional[int] = None   # the project to look the configuration up in


@dataclass(frozen=True)
class CredentialDecisionContext:
    binding_project_id: int
    agent_binding_ref: str
    subject_ref: str
    policy_revision: str
    now: datetime
    expires_at: datetime
    can_use: Callable[[], bool]   # ``credentials.use`` of the verified user; asked only once a use reaches it
    # Whose authority the run has. A mandate subject acts for its owner: a personal credential is usable only when the
    # owner is its custodian, and only a grant made to the mandate (not to the person) counts (#6947)
    subject_kind: str = "user_delegation"
    subject_user_id: Optional[int] = None


@dataclass(frozen=True)
class CredentialFacts:
    configurations: Mapping[str, Mapping]   # configuration uuid -> {id, project_id, type, section, generation}
    references: Mapping[int, Mapping]       # configuration id -> live credential reference
    grants: Mapping[str, Mapping]           # "binding|toolkit|credential|target|subject" -> live grant


class Decision(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    decision_id: str
    allow: bool
    reason_code: str
    toolkit_instance_id: str
    field: str
    policy_revision: str
    expires_at: datetime
    configuration_uuid: Optional[str] = None
    configuration_generation: Optional[int] = None
    credential_ref: Optional[str] = None
    credential_generation: Optional[int] = None
    reference_epoch: Optional[int] = None
    grant_id: Optional[str] = None
    grant_epoch: Optional[int] = None


def binding_ref(decision: Decision) -> dict:
    """What an admitted context remembers of an allowed use (the re-check at the worker reads exactly this)."""
    return {
        "toolkit_instance_id": decision.toolkit_instance_id, "field": decision.field,
        "configuration_uuid": decision.configuration_uuid, "credential_ref": decision.credential_ref,
        "credential_generation": decision.credential_generation, "reference_epoch": decision.reference_epoch,
        "grant_id": decision.grant_id, "grant_epoch": decision.grant_epoch,
        "configuration_generation": decision.configuration_generation, "decision_id": decision.decision_id,
    }


def subject_kind_of(subject_ref) -> str:
    """The kind of grant a subject reference is for: ``mandate:<uuid>`` or (``idp:<tid>:<oid>``) a user delegation."""
    return "mandate" if str(subject_ref).startswith(MANDATE_SUBJECT_PREFIX) else "user_delegation"


def grant_key(binding_ref_, toolkit_instance_id, credential_ref, subject_ref) -> str:
    """The unique key of a use grant as ``credential_use_facts`` returns it (whole target only in slice 1)."""
    return "|".join((str(binding_ref_), str(toolkit_instance_id), str(credential_ref), WHOLE_TARGET, subject_ref))


# --- collecting the uses ---------------------------------------------------------------------

def _uuid_text(value) -> Optional[str]:
    try:
        return str(uuid.UUID(str(value))) if value else None
    except ValueError:
        return None


def _int_or_none(value) -> Optional[int]:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _is_credential_dict(value) -> bool:
    return isinstance(value, dict) and bool(value) and set(value) <= CREDENTIAL_KEYS and bool(set(value) & SELECTOR_KEYS)


def _payload_toolkits(payload) -> list:
    """The toolkits a payload dispatches: request-level tools and those of the application version."""
    if not isinstance(payload, dict):
        return []
    application = payload.get("application")
    details = application.get("version_details") if isinstance(application, dict) else None
    lists = (payload.get("tools"), details.get("tools") if isinstance(details, dict) else None)
    return [tool for tools in lists if isinstance(tools, list) for tool in tools if isinstance(tool, dict)]


_SECRET_FIELDS = {}   # (toolkit type, project id, user id) -> (registry version, field names)


def forget_secret_fields() -> None:
    """Drop what ``toolkit_secret_fields`` kept (tests)."""
    _SECRET_FIELDS.clear()


def toolkit_secret_fields(tool_type, project_id, user_id) -> frozenset:
    """Names of the provider-hub fields of a toolkit type that hold a secret inline (``secret: true``).

    A type that has a schema is answered from this process for as long as the dynamic schemas did not change
    (``schema_registry_version``): finding the schema of a provider-hub or MCP type aggregates every such schema of the
    project and the user, which was done once per type and admission. A type with no schema is looked for every time (it
    can appear, or stop being blocked, without a registration)."""
    from .application_tools import find_toolkit_schema_by_type_everywhere  # pylint: disable=C0415

    key, version = (tool_type, project_id, user_id), schema_registry_version.current()
    hit = _SECRET_FIELDS.get(key)
    if hit is not None and hit[0] == version:
        return hit[1]
    schema, _ = find_toolkit_schema_by_type_everywhere(tool_type, project_id, user_id)
    properties = (schema or {}).get("properties") or {}
    names = frozenset(
        name for name, spec in properties.items()
        if isinstance(spec, dict) and spec.get("secret") is True
        and not (spec.get("configuration_types") or spec.get("configuration_sections") or spec.get("toolkit_types"))
    )
    if schema:
        if len(_SECRET_FIELDS) >= MAX_CACHED_SECRET_FIELDS:
            _SECRET_FIELDS.clear()
        _SECRET_FIELDS[key] = (version, names)
    return names


# --- one walk of a toolkit's settings per dispatch ---------------------------------------------

_WALKS = contextvars.ContextVar("c1009_settings_walks", default=None)


@contextlib.contextmanager
def walk_scope():
    """Within this scope the configurations found in one toolkit's settings are looked for once, whoever asks: the
    credential decision at admission and the seal that follows it walk the same settings."""
    token = _WALKS.set({})
    try:
        yield
    finally:
        _WALKS.reset(token)


def configurations_in(settings) -> list:
    """``_find_configurations_in_settings(settings)``, remembered by the identity of ``settings`` for the current
    ``walk_scope``. The settings are not copied or changed here; a list found earlier is returned as it is."""
    from .application_utils import _find_configurations_in_settings  # pylint: disable=C0415

    walks = _WALKS.get()
    if walks is None:
        return _find_configurations_in_settings(settings)
    hit = walks.get(id(settings))
    if hit is None or hit[0] is not settings:   # the id of a collected object can be reused: hold the object itself
        hit = walks[id(settings)] = (settings, _find_configurations_in_settings(settings))
    return hit[1]


def _declared_uses(rows, binding_project_id, inline_fields) -> dict:
    """D, the authority: ``{toolkit id: {field: use}}`` from the stored settings of the version's toolkits
    (``rows``: ``(mapping, toolkit)`` of the version)."""
    declared, secret_names = {}, {}
    for _mapping, tool in rows:
        settings = tool.settings if isinstance(tool.settings, dict) else {}
        names = frozenset()
        if any(isinstance(value, str) and value for value in settings.values()):
            if tool.type not in secret_names:
                secret_names[tool.type] = frozenset(inline_fields(tool.type))
            names = secret_names[tool.type]
        toolkit_id, fields = str(tool.id), {}
        for field, value in settings.items():
            if _is_credential_dict(value):
                cfg_uuid = _uuid_text(value.get("configuration_uuid"))
                fields[field] = CredentialUse(
                    toolkit_id, field, cfg_uuid, "uuid" if cfg_uuid else "title",
                    _int_or_none(value.get("configuration_project_id")) or binding_project_id)
            elif field in names and value not in (None, ""):
                fields[field] = CredentialUse(toolkit_id, field, None, "inline", binding_project_id)
        declared[toolkit_id] = fields
    return declared


def collect_credential_uses(  # pylint: disable=R0913,R0914
    session, binding_project_id, version_id, payload, inline_fields: Callable = lambda tool_type: (), rows=None,
) -> list:
    """Every credential the run uses: the stored credentials of the version's toolkits (D) with the selection
    resolved from the dispatched payload (P), plus each configuration the payload carries that D does not
    declare at that toolkit and field (conversation participant toolkit, request-level tool, nested toolkit
    reference, sub-agent prefetch, a configuration swapped in the payload). Platform-internal sections are not
    told apart here; ``decide_credential_uses`` drops them once the sections are known. ``rows``: the version's toolkit
    rows when the caller has read them already (then ``session`` is not used)."""
    uses = {}
    declared = _declared_uses(toolkit_rows(session, version_id) if rows is None else rows, binding_project_id,
                              inline_fields)
    for fields in declared.values():
        for use in fields.values():
            uses[(use.toolkit_instance_id, use.field, use.configuration_uuid, use.selector)] = use
    for tool in _payload_toolkits(payload):
        settings = tool.get("settings")
        if not isinstance(settings, dict):
            continue
        toolkit_id = str(tool.get("id"))
        claims = {}
        for field, use in declared.get(toolkit_id, {}).items():
            value = settings.get(field)
            if use.selector in ("uuid", "title") and isinstance(value, dict) and "configuration_type" in value:
                claims[id(value)] = use
        for found in configurations_in(settings):
            data = found["data"]
            sent_uuid = _uuid_text(data.get("configuration_uuid"))
            sent_project = _int_or_none(data.get("configuration_project_id")) or binding_project_id
            use = claims.get(id(data))
            if use is not None and use.configuration_uuid in (None, sent_uuid):
                uses.pop((use.toolkit_instance_id, use.field, use.configuration_uuid, use.selector), None)
                use = replace(use, configuration_uuid=sent_uuid, configuration_project_id=sent_project)
            else:
                use = CredentialUse(toolkit_id, NESTED_FIELD, sent_uuid, "undeclared", sent_project)
            uses[(use.toolkit_instance_id, use.field, use.configuration_uuid, use.selector)] = use
    return sorted(uses.values(), key=lambda use: (use.toolkit_instance_id, use.field, use.configuration_uuid or ""))


# --- reading the facts -----------------------------------------------------------------------

def _configuration_facts(project_id, uuids) -> dict:
    return this.for_module("configurations").module.configuration_credential_facts(project_id, uuids)


def _credential_use_facts(project_id, config_ids, grant_keys, ref_ids=None, **caller) -> dict:
    """``caller``: ``personal_subject_ref`` / ``personal_user_id``, whose personal references the facts may prefer."""
    return this.for_module("secrets").module.credential_use_facts(project_id, config_ids, grant_keys, ref_ids, **caller)


def _caller_of(ctx) -> dict:
    """Whose personal reference a configuration's facts are read for: the person of a user delegation, the owner of
    the mandate of a mandate run."""
    if ctx.subject_kind == "mandate":
        return {} if ctx.subject_user_id is None else {"personal_user_id": ctx.subject_user_id}
    return {"personal_subject_ref": ctx.subject_ref}


def read_configuration_facts(pairs, reader=None) -> dict:
    """``{uuid: facts}`` for ``(uuid, project id)`` pairs, at most ``MAX_FACT_SELECTORS`` per call."""
    reader = reader or _configuration_facts
    wanted = {}
    for cfg_uuid, project_id in pairs:
        if cfg_uuid:
            wanted.setdefault(project_id, set()).add(cfg_uuid)
    if sum(len(uuids) for uuids in wanted.values()) > MAX_CANDIDATE_CONFIGURATIONS:
        raise CredentialDecisionError("CREDENTIAL_REFERENCE_REQUIRED")
    facts = {}
    for project_id, uuids in wanted.items():
        ordered = sorted(uuids)
        for start in range(0, len(ordered), MAX_FACT_SELECTORS):
            facts.update(reader(project_id, ordered[start:start + MAX_FACT_SELECTORS]))
    return facts


def _governed_uses(candidates, configurations) -> list:
    """Credential-section uses only. A configuration that cannot be found cannot be shown to be
    platform-internal, so it stays and is refused by the decision."""
    uses = []
    for use in candidates:
        fact = configurations.get(use.configuration_uuid) if use.configuration_uuid else None
        if fact is None or fact.get("section") == CREDENTIAL_SECTION:
            uses.append(use)
    if len(uses) > MAX_GOVERNED_CREDENTIALS_PER_RUN:
        raise CredentialDecisionError("CREDENTIAL_REFERENCE_REQUIRED")
    return uses


# --- the decision ----------------------------------------------------------------------------

def _moment(value) -> Optional[datetime]:
    if value is None:
        return None
    moment = datetime.fromisoformat(value) if isinstance(value, str) else value
    return moment if moment.tzinfo is not None else moment.replace(tzinfo=timezone.utc)


def grant_in_force(grant, now) -> bool:
    """A grant that is active and inside its validity window."""
    if not isinstance(grant, Mapping) or grant.get("state") != "active":
        return False
    try:
        start, end = _moment(grant.get("valid_from")), _moment(grant.get("valid_until"))
    except (TypeError, ValueError):
        return False
    return (start is None or start <= now) and (end is None or now < end)


def holds_personal(ctx: CredentialDecisionContext, reference: Mapping) -> bool:
    """May the run use this personal reference? Its custodian may: the person whose identity the run carries (the
    subject of the run is the custodian's), or, for a mandate, the mandate's owner (#6947). Nobody else, and a
    reference that names no custodian is nobody's."""
    if ctx.subject_kind == "mandate":
        return ctx.subject_user_id is not None and _int_or_none(reference.get("custodian_user_id")) == ctx.subject_user_id
    custodian = reference.get("custodian_subject_ref")
    return isinstance(custodian, str) and bool(custodian) and custodian == ctx.subject_ref


def authorize_credential_use(ctx: CredentialDecisionContext, use: CredentialUse, facts: CredentialFacts) -> Decision:
    """One use, AND of the checks, first failure wins. Pure: it reads only its arguments (``ctx.can_use`` aside)."""
    known = {}

    def verdict(allow, code, **more) -> Decision:
        return Decision(
            decision_id=str(uuid.uuid4()), allow=allow, reason_code=code, toolkit_instance_id=use.toolkit_instance_id,
            field=use.field, policy_revision=ctx.policy_revision, expires_at=ctx.expires_at,
            configuration_uuid=use.configuration_uuid, **known, **more)

    if use.selector != "uuid" or not use.configuration_uuid:                       # 1. a stored reference by uuid
        return verdict(False, "CREDENTIAL_REFERENCE_REQUIRED")
    config = facts.configurations.get(use.configuration_uuid)
    if not isinstance(config, Mapping) or config.get("section") != CREDENTIAL_SECTION:   # 2. a credential
        return verdict(False, "CREDENTIAL_REFERENCE_REQUIRED")
    if config.get("project_id") != ctx.binding_project_id:                         # 2. of the agent's own project
        return verdict(False, "DENIED_GRANT")
    reference = facts.references.get(config.get("id"))                             # 3. a credential reference
    if not isinstance(reference, Mapping):
        return verdict(False, "CREDENTIAL_REFERENCE_REQUIRED")
    known.update(configuration_generation=config.get("generation"), credential_ref=reference.get("ref_id"),
                 credential_generation=reference.get("generation"))
    if reference.get("state") != "active" or reference.get("store_kind") not in STORE_KINDS:   # 4. in force
        return verdict(False, "CREDENTIAL_REFERENCE_INACTIVE")
    if reference.get("ownership_class") == "personal" and not holds_personal(ctx, reference):   # 5. a personal one
        return verdict(False, "CREDENTIAL_CLASS_NOT_ALLOWED")                  #    only its custodian may use
    approved = reference.get("configuration_generation")                          # 6. unchanged since approval
    if approved is None or config.get("generation") != approved:
        return verdict(False, "CREDENTIAL_GENERATION_MISMATCH")
    if not ctx.can_use():                                                          # 7. the user may use credentials
        return verdict(False, "DENIED_RBAC")
    grant = facts.grants.get(grant_key(                                            # 8, 9. this binding, toolkit, user
        ctx.agent_binding_ref, use.toolkit_instance_id, reference.get("ref_id"), ctx.subject_ref))
    if not grant_in_force(grant, ctx.now) or grant.get("subject_kind") != ctx.subject_kind:
        return verdict(False, "DENIED_GRANT")                                      # a missing kind is no grant
    return verdict(True, ALLOWED, reference_epoch=reference.get("revocation_epoch"),
                   grant_id=grant.get("grant_id"), grant_epoch=grant.get("revocation_epoch"))


def facts_answer_grants() -> bool:
    """Does the secrets plugin answer the grants of a configuration's reference in the same call as the reference
    (``grants_for``, facts version 4)? An older one is asked twice. Unreadable counts as no."""
    try:
        version = this.for_module("secrets").module.credential_facts_version()
    except Exception:  # pylint: disable=W0703
        return False
    return isinstance(version, int) and not isinstance(version, bool) and version >= GRANTS_FOR_FACTS_VERSION


def _read_decision_facts(ctx, uses, configurations) -> CredentialFacts:
    """References of the agent's own project's credentials, and the grants their ids key: one read when the secrets
    plugin joins them (``grants_for``: ``(binding, toolkit, configuration id, "*", subject)`` per use, at most
    ``MAX_FACT_SELECTORS``; the grant of the reference found for that configuration, keyed by its ``ref_id``), else
    two."""
    named = [(use, configurations.get(use.configuration_uuid)) for use in uses if use.selector == "uuid"]
    own = {fact["id"] for _, fact in named
           if fact is not None and fact.get("section") == CREDENTIAL_SECTION
           and fact.get("project_id") == ctx.binding_project_id}
    if not own:
        return CredentialFacts(configurations, {}, {})
    wanted = sorted({(ctx.agent_binding_ref, use.toolkit_instance_id, fact["id"], WHOLE_TARGET, ctx.subject_ref)
                     for use, fact in named if fact is not None and fact.get("id") in own})
    if len(wanted) <= MAX_FACT_SELECTORS and facts_answer_grants():
        found = _credential_use_facts(ctx.binding_project_id, sorted(own), [], None, grants_for=wanted, **_caller_of(ctx))
        return CredentialFacts(configurations, found["references"], found["grants"])
    references = _credential_use_facts(ctx.binding_project_id, sorted(own), [], **_caller_of(ctx))["references"]
    keys = {(ctx.agent_binding_ref, use.toolkit_instance_id, references[fact["id"]]["ref_id"], WHOLE_TARGET, ctx.subject_ref)
            for use, fact in named if fact is not None and fact.get("id") in references}
    grants = _credential_use_facts(ctx.binding_project_id, [], sorted(keys))["grants"] if keys else {}
    return CredentialFacts(configurations, references, grants)


def decide_credential_uses(ctx: CredentialDecisionContext, candidates) -> list:
    """Decide every governed use; the allows, or ``CredentialDecisionError`` for the first refusal."""
    try:
        configurations = read_configuration_facts(
            (use.configuration_uuid, use.configuration_project_id) for use in candidates)
        uses = _governed_uses(candidates, configurations)
        if not uses:
            return []
        facts = _read_decision_facts(ctx, uses, configurations)
        decisions = []
        for use in uses:
            decision = authorize_credential_use(ctx, use, facts)
            if not decision.allow:
                raise CredentialDecisionError(decision.reason_code, {
                    key: getattr(decision, key) for key in ("toolkit_instance_id", "credential_ref", "credential_generation")
                    if getattr(decision, key) is not None})
            decisions.append(decision)
        return decisions
    except CredentialDecisionError:
        raise
    except Exception as exc:  # pylint: disable=W0703
        log.error("credential decision: a dependency failed (%s)", type(exc).__name__)
        raise CredentialDecisionError("DEPENDENCY_UNAVAILABLE") from None


def user_may_use_credentials(user_id, project_id) -> Callable[[], bool]:
    """``credentials.use`` of the user in the agent's project: an exact permission, asked once and only if needed."""
    answers = []

    def can_use() -> bool:
        if not answers:
            answers.append(USE_PERMISSION in set(auth.get_user_permissions(user_id, mode="default", project_id=project_id)))
        return answers[0]

    return can_use


# --- later: a queued run, an inherited dispatch ----------------------------------------------

def recheck_credentials(project_id, entries, subject_ref, agent_binding_ref, now) -> Optional[str]:
    """Do the decisions recorded on an admitted context still hold? ``None`` when they do, else the code to
    revoke the context with. An unreadable fact raises: the run is neither passed nor revoked."""
    return recheck_failure(project_id, entries, subject_ref, agent_binding_ref, now)[0]


def recheck_failure(project_id, entries, subject_ref, agent_binding_ref, now) -> tuple:  # pylint: disable=R0911,R0914
    """``recheck_credentials`` that also says which entry failed: ``(code, entry)``, ``(None, None)`` when all hold."""
    if not entries:
        return None, None
    keys = sorted({(agent_binding_ref, entry["toolkit_instance_id"], entry["credential_ref"], WHOLE_TARGET, subject_ref)
                   for entry in entries})
    facts = _credential_use_facts(project_id, [], keys, [entry["credential_ref"] for entry in entries])   # one read
    refs, grants = facts["references"], facts["grants"]
    references = {reference["ref_id"]: reference for reference in refs.values()}
    uuids = sorted({entry["configuration_uuid"] for entry in entries})
    configurations = read_configuration_facts(((cfg_uuid, project_id) for cfg_uuid in uuids))
    for entry in entries:
        reference = references.get(entry["credential_ref"])
        if reference is None or reference.get("state") != "active":
            return "CREDENTIAL_REFERENCE_INACTIVE", entry
        if (reference.get("ownership_class") == "personal" and subject_kind_of(subject_ref) == "user_delegation"
                and reference.get("custodian_subject_ref") != subject_ref):   # a mandate's owner is checked at admission
            return "CREDENTIAL_CLASS_NOT_ALLOWED", entry
        if reference.get("revocation_epoch") != entry["reference_epoch"]:
            return "EPOCH_MISMATCH", entry
        config = configurations.get(entry["configuration_uuid"])
        if config is None:
            return "CREDENTIAL_REFERENCE_INACTIVE", entry
        if (reference.get("generation") != entry["credential_generation"]
                or config.get("generation") != entry["configuration_generation"]):
            return "CREDENTIAL_GENERATION_MISMATCH", entry
        grant = grants.get(grant_key(agent_binding_ref, entry["toolkit_instance_id"], entry["credential_ref"], subject_ref))
        if not grant_in_force(grant, now) or grant.get("subject_kind") != subject_kind_of(subject_ref):
            return "DENIED_GRANT", entry
        if grant.get("revocation_epoch") != entry["grant_epoch"]:
            return "EPOCH_MISMATCH", entry
    return None, None
