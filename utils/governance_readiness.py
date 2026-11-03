"""What an agent or pipeline version, a toolkit or a schedule still needs to run governed for one person (#7269, C-1009).

The Save card asks this after a save, and every member opening or running the entity asks it for themselves. The answer
is a list of items, each ``{key, kind, ref, title, state, requestable, for}``:

* ``binding`` (shared): the version's agent identity binding, when it has none in force for its definition as it is now;
* ``reference`` (the person's own copy when one is to be requested: ``for: me``): a credential the version's toolkits
  select that has no governed reference, or one that is not active;
* ``grant`` (``for: me``; for a schedule: its mandate's): a use grant on the reference for this binding and toolkit;
* ``mandate`` (shared) and ``consent`` (``for: me``, the owner's own act on the schedule card): for a schedule;
* ``blocking``: what the schedule risk scan says stops a scheduled run (``scan_schedule``), never requestable;
* ``identity`` / ``permission``: the person has no linked sign-in, or may not use credentials in the project.

It is computed by the checks admission and the credential decision ladder use (``authorize_credential_use`` over the
same facts, the binding rule of ``_find_binding``, the schedule's ``owner_mandate``), read only: no audit row, no decision
recorded, no context, no release. Under off and audit only it is empty and nothing is read. ``requestable`` says what the
person may file now through ``governance_requests`` (in one batch: the binding first, then references, the mandate,
the grants), given ``governance.request`` and, for a binding, the right to edit the version.
"""
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from pylon.core.tools import log
from sqlalchemy import select
from tools import auth, db

from ..models.agent_identity import LIVE_MANDATE_STATES, AgentIdentityBinding
from .credential_decision import (
    CREDENTIAL_SECTION, DELEGATED_AUTH_MODE, MANDATE_CONSENT_REQUIRED, MANDATE_DELEGATION_UNSUPPORTED,
    PROFILE_DEVELOPMENT, CredentialDecisionContext, authorize_credential_use, collect_credential_uses, grant_key,
    read_configuration_facts, toolkit_secret_fields, user_may_use_credentials, _governed_uses, _read_decision_facts,
)
from .definition_digest import DigestError, digest_for_version, toolkit_rows
from .governance import is_governed

KINDS = ("version", "toolkit", "schedule")
REQUEST_PERMISSION = "governance.request"
EDIT_PERMISSION = "models.applications.version.update"
GOVERNED_MODES = ("dedicated_agent", "elitea_native")
REQUESTED_MODE = "elitea_native"   # what a member's binding request asks for (an operator may rebind it otherwise)
MAX_ITEMS = 128
NO_BINDING = "00000000-0000-0000-0000-000000000000"
NO_SUBJECT = "idp:unlinked:unlinked"   # a person with no linked sign-in: a well-formed subject no grant names


class ReadinessUnavailable(Exception):
    """A fact the answer needs could not be read: the caller answers DEPENDENCY_UNAVAILABLE (never a wrong list)."""


class EntityNotFound(Exception):
    """No such entity in the project."""


class _ConsentApart(tuple):
    """The scope of a mandate's consent as readiness reads it: the owner's consent is an item of its own (``consent``),
    so the ladder goes on to the grants of every credential."""

    def __contains__(self, item):
        return True


@dataclass
class Readiness:
    """The items, and what ``governance_requests`` needs to file them (never returned to the caller)."""
    items: list = field(default_factory=list)
    plan: dict = field(default_factory=dict)
    delegated: bool = False   # a credential of the version acts with a person's Microsoft sign-in

    def add(self, key, kind, title, state, requestable=False, ref=None, for_="shared", **plan):
        if len(self.items) < MAX_ITEMS and all(item["key"] != key for item in self.items):
            self.items.append({"key": key, "kind": kind, "ref": ref, "title": title, "state": state,
                               "requestable": bool(requestable), "for": for_})
            if plan:
                self.plan[key] = plan


def _now():
    return datetime.now(timezone.utc)


def _person(user_id):
    """``idp:<tid>:<oid>`` of the person, or None when they have no linked sign-in."""
    try:
        user = auth.get_user(user_id) or {}
    except Exception as exc:  # pylint: disable=W0703
        log.error("governance readiness: the user could not be read (%s)", type(exc).__name__)
        raise ReadinessUnavailable() from None
    tid, oid = user.get("idp_tid"), user.get("idp_oid")
    return f"idp:{tid}:{oid}" if user.get("idp_issuer") and tid and oid else None


def _permissions(user_id, project_id):
    try:
        return set(auth.get_user_permissions(user_id, mode="default", project_id=project_id))
    except Exception as exc:  # pylint: disable=W0703
        log.error("governance readiness: the permissions could not be read (%s)", type(exc).__name__)
        raise ReadinessUnavailable() from None


def _binding_of(session, project_id, application_id, version_id, current_digest):
    """The version's binding as admission sees it: ``(row, state)``, ``state`` one of missing, draft, provisioning,
    pending_approval, active, suspended, changed (in force but approved for another definition)."""
    rows = session.execute(
        select(AgentIdentityBinding.id, AgentIdentityBinding.state, AgentIdentityBinding.definition_digest,
               AgentIdentityBinding.created_by, AgentIdentityBinding.state_changed_by)
        .where(AgentIdentityBinding.project_id == project_id, AgentIdentityBinding.application_id == application_id,
               AgentIdentityBinding.version_id == version_id, AgentIdentityBinding.identity_mode.in_(GOVERNED_MODES),
               AgentIdentityBinding.state.notin_(("retiring", "retired")))
        .order_by(AgentIdentityBinding.updated_at.desc())
    ).all()
    for wanted in ("active", "suspended", "pending_approval", "provisioning", "draft"):
        for row in rows:
            if row.state == wanted:
                changed = wanted in ("active", "suspended") and row.definition_digest != current_digest
                return row, "changed" if changed else wanted
    return None, "missing"


def _uses(session, project_id, version_id, user_id, rows):
    """The governed credential uses of the version's stored definition, and the facts of their configurations."""
    candidates = collect_credential_uses(
        None, project_id, version_id, None, lambda tool_type: toolkit_secret_fields(tool_type, project_id, user_id),
        rows=rows)
    configurations = read_configuration_facts(
        (use.configuration_uuid, use.configuration_project_id) for use in candidates)
    return _governed_uses(candidates, configurations), configurations


def _credential_items(result, ctx, uses, configurations, tools, *, binding_requestable, may_request, subject_for,  # pylint: disable=R0912,R0913,R0914
                      with_grants=True):
    """The reference and grant items of each use, from the ladder's own verdict (``authorize_credential_use``).
    ``with_grants``: False for a schedule with no mandate yet (its grants are the mandate's, listed with it)."""
    if not uses:
        return
    facts = _read_decision_facts(ctx, uses, configurations)
    for use in uses:
        config = configurations.get(use.configuration_uuid) or {}
        known = facts.references.get(config.get("id")) if config else None
        if isinstance(known, dict) and known.get("ref_id"):
            result.plan.setdefault("_refs", {})[use.configuration_uuid] = known["ref_id"]
        result.delegated = result.delegated or (isinstance(known, dict) and known.get("auth_mode") == DELEGATED_AUTH_MODE)
        verdict = authorize_credential_use(ctx, use, facts)
        if verdict.allow:
            continue
        title = config.get("title") or tools.get(use.toolkit_instance_id) or "Credential"
        code = verdict.reason_code
        if use.selector != "uuid" or not config or config.get("section") != CREDENTIAL_SECTION \
                or config.get("project_id") != ctx.binding_project_id:
            result.add(f"reference:{use.toolkit_instance_id}:{use.field}", "reference", title,
                       "not_registrable" if use.selector != "uuid" else "other_project")
            continue
        reference = facts.references.get(config.get("id"))
        ref_key = f"reference:{use.configuration_uuid}"
        delegated = isinstance(reference, dict) and reference.get("auth_mode") == DELEGATED_AUTH_MODE
        if reference is None:
            # a member registers their own copy (a personal reference, value through their own value page)
            result.add(ref_key, "reference", title, "missing", requestable=may_request and not config.get("delegated"),
                       for_="me", configuration_uuid=use.configuration_uuid)
        elif code in ("CREDENTIAL_REFERENCE_INACTIVE", "CREDENTIAL_GENERATION_MISMATCH"):
            state = "changed" if code == "CREDENTIAL_GENERATION_MISMATCH" else reference.get("state")
            if reference.get("ownership_class") == "personal" and not reference.get("generation"):
                state = "needs_value"
            result.add(ref_key, "reference", title, state, ref=reference.get("ref_id"),
                       for_="me" if reference.get("ownership_class") == "personal" else "shared")
        elif code == "CREDENTIAL_CLASS_NOT_ALLOWED":
            result.add(ref_key, "reference", title, "personal_to_another", ref=reference.get("ref_id"))
            continue
        elif code == MANDATE_DELEGATION_UNSUPPORTED:   # a sign-in credential, and the mandate names no owner sign-in
            result.add(ref_key, "reference", title, "needs_owner_sign_in", ref=reference.get("ref_id"))
            continue
        elif code == MANDATE_CONSENT_REQUIRED:   # the consent item says it
            continue
        elif code == "DENIED_RBAC":
            result.add("permission:credentials.use", "permission", "Use of credentials in this project",
                       "not_permitted")
            continue
        elif code not in ("DENIED_GRANT", "DEVELOPMENT_GRANT_REQUIRED"):
            result.add(ref_key, "reference", title, code.lower(), ref=reference.get("ref_id"))
            continue
        if not with_grants:
            continue
        if ctx.profile == PROFILE_DEVELOPMENT:   # a toolkit's own run: a grant to the person on some agent, by an admin
            if code in ("DENIED_GRANT", "DEVELOPMENT_GRANT_REQUIRED"):
                result.add(f"grant:any:{use.configuration_uuid}", "grant", f"{title} for you", "needs_an_agent_grant",
                           for_="me")
            continue
        grant = facts.grants.get(grant_key(ctx.agent_binding_ref, use.toolkit_instance_id,
                                           (reference or {}).get("ref_id"), ctx.subject_ref)) if reference else None
        state = grant.get("state") if isinstance(grant, dict) else "missing"
        if state == "active":   # in force but outside its validity window, or of another kind
            state = "expired"
        reference_ok = reference is not None or (may_request and not delegated)
        result.add(f"grant:{use.toolkit_instance_id}:{use.configuration_uuid}", "grant", f"{title} for {subject_for}",
                   state, ref=(grant or {}).get("grant_id") if isinstance(grant, dict) else None,
                   requestable=state == "missing" and may_request and binding_requestable and reference_ok,
                   for_="me" if ctx.subject_kind != "mandate" else "shared",
                   toolkit_id=int(use.toolkit_instance_id) if str(use.toolkit_instance_id).isdecimal() else None,
                   configuration_uuid=use.configuration_uuid,
                   reference_ref=(reference or {}).get("ref_id"))


def _version_items(result, module, session, project_id, version, user_id, subject_ref, permissions, *,  # pylint: disable=R0913,R0914
                   mandate=None, owner_ref=None, with_grants=True):
    """The binding and credential items of one version (for a schedule: its mandate's grants)."""
    may_request = REQUEST_PERMISSION in permissions
    rows = toolkit_rows(session, version.id)
    try:
        current = digest_for_version(session, version.id, (version, rows))
    except DigestError:
        raise ReadinessUnavailable() from None
    binding, state = _binding_of(session, project_id, version.application_id, version.id, current)
    result.plan["_binding_id"] = None if binding is None else str(binding.id)
    may_edit = may_request and EDIT_PERMISSION in permissions
    own_draft = binding is not None and state == "draft" and binding.created_by == user_id
    binding_requestable = may_edit and (state in ("missing", "changed") or own_draft)
    title = " ".join(part for part in (getattr(version, "application_name", None), version.name) if part) or "Agent"
    if state != "active":
        result.add("binding", "binding", title, state, ref=None if binding is None else str(binding.id),
                   requestable=binding_requestable,
                   application_id=version.application_id, version_id=version.id,
                   binding_id=None if binding is None else str(binding.id), action={
                       "missing": "create", "changed": "rebind", "draft": "submit"}.get(state))
    tools = {str(tool.id): tool.name or tool.type for _, tool in rows}
    uses, configurations = _uses(session, project_id, version.id, user_id, rows)
    # a rebind keeps the row, and its grants; with no binding a well-formed id no grant names (the facts refuse a
    # malformed one)
    label = NO_BINDING if binding is None else str(binding.id)
    if mandate is not None:
        ctx = CredentialDecisionContext(
            binding_project_id=project_id, agent_binding_ref=label, subject_ref=f"mandate:{mandate.id}",
            policy_revision="", now=_now(), expires_at=_now(), can_use=user_may_use_credentials(user_id, project_id),
            subject_kind="mandate", subject_user_id=mandate.owner_user_id, mandate_owner_ref=owner_ref,
            mandate_identity=lambda: {"source": mandate.identity_source, "consented": True, "scope": _ConsentApart()})
        subject_for = "the schedule"
    else:
        ctx = CredentialDecisionContext(
            binding_project_id=project_id, agent_binding_ref=label, subject_ref=subject_ref or NO_SUBJECT,
            policy_revision="", now=_now(), expires_at=_now(), can_use=user_may_use_credentials(user_id, project_id),
            subject_kind="user_delegation", subject_user_id=user_id)
        subject_for = "you"
    _credential_items(result, ctx, uses, configurations, tools,
                      binding_requestable=binding is not None or binding_requestable,
                      may_request=may_request, subject_for=subject_for, with_grants=with_grants)
    return binding, state, uses, configurations


WAITING = ("pending", "validated", "pending_approval", "provisioning", "draft")


def readiness(module, project_id, kind, entity_id, user_id) -> Readiness:
    """What ``kind`` ``entity_id`` still needs to run governed for ``user_id``. Empty, with nothing read, unless
    governance is enforced. ``EntityNotFound`` for an entity of another project or none; ``ReadinessUnavailable``
    when a fact cannot be read. A waiting item says whether it needs a platform operator (``escalated``: no project
    admin but the person, ``governance_inbox``; one membership read, only when something waits)."""
    result = Readiness()
    if not is_governed(module):
        return result
    _collect(result, module, project_id, kind, entity_id, user_id)
    waiting = [item for item in result.items if item["state"] in WAITING and item["ref"]]
    if waiting:
        from .governance_inbox import AdminsUnavailable, escalated, project_admins  # pylint: disable=C0415
        try:
            admins = project_admins(project_id)
        except AdminsUnavailable:
            admins = None
        for item in waiting:
            item["escalated"] = None if admins is None else escalated(admins, user_id)
    return result


def _collect(result, module, project_id, kind, entity_id, user_id):  # pylint: disable=R0912,R0914
    from ..models.all import Application, ApplicationVersion  # pylint: disable=C0415
    subject_ref = _person(user_id)
    permissions = _permissions(user_id, project_id)
    result.plan["_subject"] = subject_ref
    if subject_ref is None:   # nothing can be granted to, or kept for, a person with no linked sign-in
        result.add("identity", "identity", "Your sign-in", "unlinked", for_="me")
        permissions.discard(REQUEST_PERMISSION)
    if "credentials.use" not in permissions:
        result.add("permission:credentials.use", "permission", "Use of credentials in this project", "not_permitted")
    with db.get_session(project_id) as session:
        if kind == "toolkit":
            from ..models.elitea_tools import EliteATool  # pylint: disable=C0415
            tool = session.query(EliteATool).filter(EliteATool.id == entity_id).first()
            if tool is None:
                raise EntityNotFound()
            rows = [(None, tool)]
            uses, configurations = _uses(session, project_id, None, user_id, rows)
            ctx = CredentialDecisionContext(
                binding_project_id=project_id, agent_binding_ref=NO_BINDING, subject_ref=subject_ref or NO_SUBJECT,
                policy_revision="", now=_now(), expires_at=_now(), can_use=user_may_use_credentials(user_id, project_id),
                subject_kind="user_delegation", subject_user_id=user_id, profile=PROFILE_DEVELOPMENT)
            _credential_items(result, ctx, uses, configurations, {str(tool.id): tool.name or tool.type},
                              binding_requestable=False, may_request=REQUEST_PERMISSION in permissions,
                              subject_for="you")
            return result
        if kind == "schedule":
            from ..models.entity_schedule import EntitySchedule  # pylint: disable=C0415
            schedule = session.get(EntitySchedule, entity_id)
            if schedule is None:
                raise EntityNotFound()
            version_id, owner_id = schedule.version_id, schedule.created_by
            if owner_id != user_id and "models.applications.schedules.admin" not in permissions:
                raise EntityNotFound()   # a schedule is its owner's (and the project's schedule admins')
        else:
            version_id, owner_id = entity_id, None
        version = session.execute(
            select(ApplicationVersion, Application.name.label("application_name"))
            .join(Application, Application.id == ApplicationVersion.application_id)
            .where(ApplicationVersion.id == version_id)).first() if version_id else None
        if version is None:
            if kind == "schedule":
                result.add("blocking:version", "blocking", "The schedule's agent version", "missing")
                return result
            raise EntityNotFound()
        row, application_name = version
        row.application_name = application_name
        if kind != "schedule":
            _version_items(result, module, session, project_id, row, user_id, subject_ref, permissions)
            return result
        # a schedule runs on its owner's mandate: the version's binding, the mandate, its grants, the owner's consent
        from .mandate_admin import _live_mandate  # pylint: disable=C0415
        mandate = _live_mandate(session, project_id, version_id=row.id)
        if mandate is not None and mandate.owner_user_id != owner_id:
            mandate = None   # another person's mandate never lends its authority to this schedule
        owner_ref = _person(owner_id) if owner_id != user_id else subject_ref
        binding, binding_state, uses, configurations = _version_items(
            result, module, session, project_id, row, user_id, subject_ref, permissions,
            mandate=mandate, owner_ref=owner_ref, with_grants=mandate is not None)
        title = f"{schedule.name}"
        is_owner = owner_id == user_id
        if mandate is None:
            result.add("mandate", "mandate", title, "missing",
                       requestable=is_owner and REQUEST_PERMISSION in permissions and binding_state == "active",
                       version_id=row.id, owner_user_id=owner_id,
                       trigger=schedule.principal.get("trigger") if isinstance(schedule.principal, dict) else None)
            # the mandate's grants, requested with it: one per credential the version uses
            for use in uses:
                config = configurations.get(use.configuration_uuid) or {}
                if use.selector == "uuid" and config.get("section") == CREDENTIAL_SECTION:
                    result.add(f"grant:{use.toolkit_instance_id}:{use.configuration_uuid}", "grant",
                               f"{config.get('title') or 'Credential'} for the schedule", "missing",
                               requestable=is_owner and REQUEST_PERMISSION in permissions and binding_state == "active",
                               toolkit_id=int(use.toolkit_instance_id) if str(use.toolkit_instance_id).isdecimal()
                               else None, configuration_uuid=use.configuration_uuid,
                               reference_ref=result.plan.get("_refs", {}).get(use.configuration_uuid))
        elif mandate.state != "active":
            own_draft = mandate.state == "draft" and mandate.created_by == user_id
            result.add("mandate", "mandate", title, mandate.state, ref=str(mandate.id),
                       requestable=own_draft and REQUEST_PERMISSION in permissions, mandate_id=str(mandate.id))
        if mandate is not None and mandate.identity_source == "owner_delegated" and \
                mandate.identity_material_ref is None and mandate.state in LIVE_MANDATE_STATES:
            result.add("consent", "consent", f"Your consent for {title}", "missing", ref=str(mandate.id),
                       for_="me" if is_owner else "shared")
    from .schedule_risk import scan_schedule  # pylint: disable=C0415
    for item in scan_schedule(project_id, row.id, owner_id)["blocking"]:
        result.add(f"blocking:{item['code']}:{item.get('toolkit') or ''}", "blocking", item["message"], item["code"])
    return result


def public(result: Readiness) -> list:
    """The items as the API returns them."""
    return [dict(item) for item in result.items]
