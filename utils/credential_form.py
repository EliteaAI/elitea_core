"""The chat's credential form (#7228, C-1009): a credential a governed agent needs is typed by the person, never by the
model.

The SDK's ``request_credential`` tool pauses the run with a shape-only interrupt (``guardrail_type: credential_form``,
the credential type, a title, a purpose). The chat renders the form inline; **Create** posts the typed values here,
under the person's own session (``complete``):

1. read only: governance enforced, the interrupt pending on that message with the same credential type, the type a
   credential type whose fields the body names, the ownership the person may choose;
2. the configuration is created by the configurations create service the REST route uses (author = the person,
   ``shared`` only for a team credential);
3. one transaction (``_OneTransaction`` as ``governance_requests``): the governed reference (personal, custodian = the
   person; a team one ``pending`` and ``created_by`` the person, which a project admin approves like every request;
   an Entra sign-in type always personal and delegated), filed by secrets' own lifecycle;
4. the value through the store's own staged write (personal: the owner's value write; team: the import); a failure
   retires the reference and deletes the configuration, so nothing is kept;
5. the person's own check of it (best effort: a failed probe leaves it pending, the state says so);
6. what the paused run's agent version still needs (identity binding, grants) filed for the person through
   ``governance_requests.file_requests`` (``caller_ref run:<conversation>``); a refusal there does not undo the credential.

The answer, the interrupt record it leaves on the message (``credential_form_result``, read by the resume) and every
event hold ids, titles, types and states only: never a value and never a ``{{secret.…}}`` placeholder. The endpoint does
not resume the run: the chat does, with ``{action: created, value: {reference_id}}``, which ``check_resume`` replaces by
the server's own record before anything is dispatched.
"""
import re
import secrets as _random
import uuid
from datetime import datetime, timezone

from pylon.core.tools import log
from sqlalchemy.orm.attributes import flag_modified
from tools import auth, db, this

from .governance import is_governed
from .governance_requests import RequestRefused, _OneTransaction, file_requests
from .parallel_hitl import interrupt_identity, pending_interrupts

GUARDRAIL = "credential_form"
ACTIONS = ("created", "cancel")
RESULT_KEY = "credential_form_result"
REASON = "CREDENTIAL_FORM_COMPLETED"
FAILED_REASON = "CREDENTIAL_FORM_FAILED"
CLAIM_KEY = "credential_form_in_progress"
CLAIM_TTL_S = 300   # a claim older than this (a crashed Create) may be taken over
PERSONAL, SHARED = "personal", "project_shared"
DELEGATED_AUTH_MODE = "entra_delegated"
CREATE_PERMISSION = "configurations.configuration.create"
CREDENTIAL_SECTION = "credentials"
NOT_TYPED = frozenset({"refresh_token"})   # a stored sign-in is written by the sign-in flow, never typed into a form
TITLE_MAX = 64

STATUS = {"PROJECT_GOVERNANCE_OFF": 409, "NOT_FOUND": 404, "INTERRUPT_MISMATCH": 409, "INVALID_REQUEST": 400,
          "NOT_RUN_USER": 403, "FORM_IN_PROGRESS": 409,
          "OWNERSHIP_INVALID": 400, "FORBIDDEN": 403, "DEPENDENCY_UNAVAILABLE": 503, "HITL_VALUE_INVALID": 400}
MESSAGES = {
    "PROJECT_GOVERNANCE_OFF": "The credential form applies only while credential governance is enforced; nothing was "
                              "created.",
    "NOT_FOUND": "This credential request is no longer waiting; nothing was created.",
    "INTERRUPT_MISMATCH": "The form does not match what the agent asked for; nothing was created.",
    "INVALID_REQUEST": "The form is not valid for this credential type; nothing was created.",
    "OWNERSHIP_INVALID": "A sign-in credential, or a credential in a personal project, is always private; nothing was "
                         "created.",
    "FORBIDDEN": "You may not create credentials in this project; nothing was created.",
    "DEPENDENCY_UNAVAILABLE": "The credential could not be stored right now; nothing was kept. Try again in a moment.",
    "HITL_VALUE_INVALID": "The answer to the credential request is not valid; nothing was resumed.",
    "NOT_RUN_USER": "Only the person whose run asked for this credential can create it; nothing was created.",
    "FORM_IN_PROGRESS": "This credential is being created already; nothing more was created.",
}


class FormRefused(Exception):
    """Nothing was created (or resumed). ``code`` is stable; the message is fixed text, never a value."""

    def __init__(self, code, field=None):
        super().__init__(code)
        self.code, self.field = code, field
        self.status = STATUS.get(code, 409)
        self.message = MESSAGES.get(code, MESSAGES["INVALID_REQUEST"])

    def body(self):
        found = {"error": self.message, "code": self.code}
        if self.field:
            found["field"] = self.field
        return found


# --- reads ------------------------------------------------------------------------------------------------------

def _message(session, conversation_uuid, message_id):
    from ..models.conversation import Conversation  # pylint: disable=C0415
    from ..models.message_group import ConversationMessageGroup  # pylint: disable=C0415
    return session.query(ConversationMessageGroup).join(
        Conversation, Conversation.id == ConversationMessageGroup.conversation_id
    ).filter(ConversationMessageGroup.uuid == str(message_id),
             Conversation.uuid == str(conversation_uuid)).with_for_update(of=ConversationMessageGroup).first()


def _form_interrupt(meta, interrupt_id):
    for item in pending_interrupts(meta):
        if interrupt_identity(item) == interrupt_id:
            return item
    return None


def _type_entry(credential_type):
    """A credential type a person may create here: registered, of the credentials section, not owned by a plugin."""
    from plugins.configurations.models.pd.registry import CONFIG_TYPE_REGISTRY  # pylint: disable=C0415,E0401
    entry = CONFIG_TYPE_REGISTRY.get(credential_type)
    if entry is None or getattr(entry, "section", None) != CREDENTIAL_SECTION or getattr(entry, "managed_by", None):
        raise FormRefused("INVALID_REQUEST", "credential_type")
    return entry


def _secret_names(credential_type):
    from .credential_slots import configuration_secret_fields  # pylint: disable=C0415
    try:
        paths = configuration_secret_fields([credential_type]).get(credential_type)
    except Exception:  # pylint: disable=W0703
        raise FormRefused("DEPENDENCY_UNAVAILABLE") from None
    if not isinstance(paths, (list, tuple)):
        raise FormRefused("INVALID_REQUEST", "credential_type")
    # top-level fields only: the form shows a type's top-level fields (no credential type has nested secrets today)
    return {path for path in paths if isinstance(path, str) and "." not in path}


def _is_personal_project(project_id, user_id):
    from tools import rpc_tools  # pylint: disable=C0415
    try:
        project = rpc_tools.RpcMixin().rpc.timeout(5).admin_get_user_private_project(user_id)
    except Exception:  # pylint: disable=W0703
        raise FormRefused("DEPENDENCY_UNAVAILABLE") from None
    return project is not None and int(getattr(project, "id", 0)) == int(project_id)


def _may_create(project_id, user_id):
    try:
        return CREATE_PERMISSION in set(auth.get_user_permissions(user_id, mode="default", project_id=project_id))
    except Exception:  # pylint: disable=W0703
        raise FormRefused("DEPENDENCY_UNAVAILABLE") from None


def _run_user(session, message):
    """The user whose run paused on this answer: the author of the question it replies to (None otherwise)."""
    from ..models.enums.all import ParticipantTypes  # pylint: disable=C0415
    from ..models.message_group import ConversationMessageGroup  # pylint: disable=C0415
    question = session.get(ConversationMessageGroup, message.reply_to_id) if message.reply_to_id else None
    participant = question.author_participant if question is not None else None
    if participant is None or participant.entity_name != ParticipantTypes.user:
        return None
    user_id = (participant.entity_meta or {}).get("id")
    return user_id if isinstance(user_id, int) and not isinstance(user_id, bool) else None


def _run_version(session, message):
    """``(project_id, version_id)`` of the agent whose run paused on this message, or None (not an agent's message)."""
    from ..models.enums.all import ParticipantTypes  # pylint: disable=C0415
    from ..models.participants import ParticipantMapping  # pylint: disable=C0415
    participant = message.author_participant
    if participant is None or participant.entity_name != ParticipantTypes.application:
        return None
    mapping = session.query(ParticipantMapping.entity_settings).filter(
        ParticipantMapping.participant_id == participant.id,
        ParticipantMapping.conversation_id == message.conversation_id).first()
    version_id = ((mapping.entity_settings or {}) if mapping else {}).get("version_id")
    owner = (participant.entity_meta or {}).get("project_id")
    return (owner, version_id) if isinstance(version_id, int) else None


# --- the steps --------------------------------------------------------------------------------------------------

def _elitea_title(title):
    base = re.sub(r"[^a-z0-9]+", "_", title.lower()).strip("_")[:48] or "credential"
    return f"{base}_{_random.token_hex(3)}"


def _create_configuration(project_id, body, actor, shared, delegated=False):
    from plugins.configurations.exceptions import ConfigurationError  # pylint: disable=C0415,E0401
    from plugins.configurations.utils import create_configuration  # pylint: disable=C0415,E0401
    data = {name: value.get_secret_value() for name, value in body.fields.items()}
    if delegated:   # a container of the person's Microsoft sign-in (#6756): the type's own marker, never typed
        data["auth_mode"] = DELEGATED_AUTH_MODE
    payload = {"elitea_title": _elitea_title(body.title), "label": body.title, "type": body.credential_type,
               "shared": shared, "project_id": project_id, "author_id": actor, "data": data}
    try:
        return create_configuration(payload)
    except ConfigurationError as exc:
        raise FormRefused("INVALID_REQUEST", exc.field if exc.field in body.fields else None) from None
    except Exception as exc:  # pylint: disable=W0703
        log.error("credential form: the configuration was not created (%s)", type(exc).__name__)
        raise FormRefused("DEPENDENCY_UNAVAILABLE") from None


def _stored_references(node, found=None, depth=0):
    """The ``{{secret.…}}`` references the configuration create stored the typed values under."""
    found = [] if found is None else found
    if isinstance(node, str) and node.startswith("{{secret.") and node.endswith("}}"):
        found.append(node)
    elif isinstance(node, dict) and depth < 8:
        for value in node.values():
            _stored_references(value, found, depth + 1)
    return found


def _delete_configuration(project_id, created):
    """The configuration and the Vault secrets its create stored (the form refuses typed references, so every
    reference in it is one of those)."""
    try:
        from plugins.configurations.utils import delete_configuration, purge_secrets  # pylint: disable=C0415,E0401
        references = sorted(set(_stored_references(created.get("data"))))
        if references:
            purge_secrets(project_id, references)
        delete_configuration(project_id, created.get("id"))
    except Exception as exc:  # pylint: disable=W0703
        log.error("credential form: a configuration was left after a failure (%s)", type(exc).__name__)


def _retire(project_id, ref_id, actor):
    from plugins.secrets.utils.credential_onboarding import run_reference_action  # pylint: disable=C0415,E0401
    answer = run_reference_action({"project_id": project_id, "action": "retire", "ref": ref_id, "dry_run": False},
                                  actor, audit_code=True)
    if answer.get("error") is not None:
        log.error("credential form: a reference was left after a failure (%s)", answer.get("code"))


def _file_reference(project_id, configuration_uuid, actor, ownership, delegation):
    """The governed reference, in one transaction: secrets' own lifecycle in this session, one commit."""
    item = {"kind": "reference", "key": f"reference:{configuration_uuid}", "configuration_uuid": configuration_uuid,
            "ownership_class": ownership}
    if delegation is not None:
        item.update(auth_mode=DELEGATED_AUTH_MODE, delegation=delegation)
    with db.get_session(project_id) as raw:
        try:
            [made] = this.for_module("secrets").module.file_credential_requests(
                _OneTransaction(raw), project_id, [item], actor)
            raw.commit()
        except Exception as exc:  # pylint: disable=W0703
            raw.rollback()
            code = getattr(exc, "code", None)
            log.error("credential form: the reference was not filed (%s)", code or type(exc).__name__)
            raise FormRefused("DEPENDENCY_UNAVAILABLE") from None
    return made["id"]


def _store_value(project_id, ref_id, secret_fields, ownership, actor):
    """The value through the store's staged write: the owner's value of a personal reference, the import of a team
    one (from the configuration just created). ``{"error", "code"}`` or the store's answer."""
    if ownership == PERSONAL:
        from plugins.secrets.utils.credential_material import write_material  # pylint: disable=C0415,E0401
        return write_material(project_id, ref_id, secret_fields, 0, actor)
    from plugins.secrets.utils.credential_onboarding import run_reference_action  # pylint: disable=C0415,E0401
    return run_reference_action({"project_id": project_id, "action": "import", "ref": ref_id, "dry_run": False},
                                actor, audit_code=True)


def _probe(project_id, ref_id, ownership, delegated, actor):
    """The person's own check of their personal credential (as the card's Check): ``(state, code)``, ``validated`` or
    ``pending`` with the check's code (a sign-in that is not a Microsoft one says so). A team credential is probed by
    the admin who approves it."""
    if ownership != PERSONAL:
        return "pending", None
    from plugins.secrets.utils.credential_onboarding import run_reference_action  # pylint: disable=C0415,E0401
    values = {"project_id": project_id, "action": "probe", "ref": ref_id, "dry_run": False}
    if not delegated:
        values["check_connection"] = True
    try:
        answer = run_reference_action(values, actor, audit_code=True)
    except Exception as exc:  # pylint: disable=W0703
        log.warning("credential form: the check did not run (%s)", type(exc).__name__)
        return "pending", "CHECK_NOT_RUN"
    if answer.get("error") is None and answer.get("state"):
        return answer["state"], None
    return "pending", answer.get("reason") or answer.get("code")


def _file_run_requests(module, project_id, run_version, actor, conversation_uuid):
    if run_version is None or run_version[0] not in (None, project_id):
        return [], None
    try:
        return file_requests(module, project_id, "version", run_version[1], actor, run_id=str(conversation_uuid)), None
    except RequestRefused as exc:
        return [], exc.code
    except Exception as exc:  # pylint: disable=W0703
        log.error("credential form: the run's requests were not filed (%s)", type(exc).__name__)
        return [], "DEPENDENCY_UNAVAILABLE"


def _record(project_id, actor, ref_id, configuration_id, interrupt_id, conversation_uuid, reason=REASON,  # pylint: disable=R0913
            step=None):
    from .governance_readiness import _person  # pylint: disable=C0415
    try:
        subject = _person(actor)
    except Exception:  # pylint: disable=W0703
        subject = None
    event = {"event_type": "lifecycle", "reason_code": reason, "outcome": "success" if reason == REASON else "failed",
             "project_id": project_id, "caller_ref": f"run:{conversation_uuid}", "user_ref": subject,
             "credential_ref": ref_id, "target_ref": f"configuration:{configuration_id}" if configuration_id else None,
             "operation_id": (f"credential_form:{interrupt_id}" + (f":{step}" if step else ""))[:256],
             "run_id": str(conversation_uuid)}
    try:
        this.for_module("tracing").module.security_audit_record_autocommit(
            {key: value for key, value in event.items() if value is not None})
    except Exception as exc:  # pylint: disable=W0703
        log.error("credential form: the completion was not recorded (%s)", type(exc).__name__)


def _remember(meta, interrupt_id, value, field=RESULT_KEY):
    """The interrupt record of this message with ``field`` set to ``value`` (removed for None): the server's answer
    (the resume reads it back) or the claim of a Create in progress."""
    def put(item):
        found = {key: entry for key, entry in item.items() if key != field}
        return found if value is None else {**found, field: value}

    updated = dict(meta or {})
    for key in ("hitl_interrupt", "hitl_interrupts"):
        current = updated.get(key)
        if isinstance(current, dict) and interrupt_identity(current) == interrupt_id:
            updated[key] = put(current)
        elif isinstance(current, list):
            updated[key] = [put(item) if isinstance(item, dict) and interrupt_identity(item) == interrupt_id else item
                            for item in current]
    return updated


def public_result(result):
    """What the reply and the model get: ids, titles, type, state and the check's code (never ``created_by``)."""
    found = {key: result.get(key) for key in ("reference_id", "configuration_uuid", "configuration_title", "title",
                                              "credential_type", "state")}
    if result.get("check"):
        found["check"] = result["check"]
    return found


def _check_body(body):
    """``(delegated, secret_fields)`` of a body that fits its credential type (read only)."""
    entry = _type_entry(body.credential_type)
    delegated = body.credential_type in _delegated_types()
    if body.auth_mode is not None and not delegated:
        raise FormRefused("INVALID_REQUEST", "auth_mode")
    if delegated and body.ownership != PERSONAL:
        raise FormRefused("OWNERSHIP_INVALID")
    properties = set(((getattr(entry, "config_schema", None) or {}).get("properties") or {}))
    unknown = sorted(name for name in body.fields if name not in properties or name in NOT_TYPED)
    # typed values only: a ``{{secret.…}}`` placeholder would make an existing project secret the person's credential
    if unknown or any("{{secret." in value.get_secret_value() for value in body.fields.values()):
        raise FormRefused("INVALID_REQUEST", "fields")
    secret_names = _secret_names(body.credential_type)
    secret_fields = {name: value for name, value in body.fields.items() if name in secret_names}
    if delegated and secret_fields:
        raise FormRefused("INVALID_REQUEST", "fields")   # a sign-in credential holds no typed secret
    if not delegated and not any(value.get_secret_value() for value in secret_fields.values()):
        raise FormRefused("INVALID_REQUEST", "fields")
    return delegated, secret_fields


def _claim_is_live(claim, now) -> bool:
    try:
        taken = datetime.fromisoformat(str(claim.get("at")))
    except (TypeError, ValueError, AttributeError):
        return False
    return (now - taken).total_seconds() < CLAIM_TTL_S


def _claim(project_id, conversation_uuid, body, actor):
    """Under the interrupt row's lock: the checks of the pause, then a claim (``CLAIM_KEY``) committed before any
    effect, so a second Create meanwhile is ``FORM_IN_PROGRESS``. Returns ``(record or None, run version)``: the
    record when this form was completed already (the same answer again, nothing created twice)."""
    now = datetime.now(timezone.utc)
    with db.get_session(project_id) as session:
        message = _message(session, conversation_uuid, body.message_id)
        interrupt = _form_interrupt(message.meta if message else None, body.interrupt_id)
        if interrupt is None:
            raise FormRefused("NOT_FOUND")
        if interrupt.get("guardrail_type") != GUARDRAIL or interrupt.get("credential_type") != body.credential_type:
            raise FormRefused("INTERRUPT_MISMATCH")
        if _run_user(session, message) != actor:   # the person whose run paused, and nobody else
            raise FormRefused("NOT_RUN_USER")
        done = interrupt.get(RESULT_KEY)
        if isinstance(done, dict):
            if done.get("created_by") != actor:
                raise FormRefused("NOT_RUN_USER")
            return done, None
        claim = interrupt.get(CLAIM_KEY)
        if isinstance(claim, dict) and _claim_is_live(claim, now):
            raise FormRefused("FORM_IN_PROGRESS")
        run_version = _run_version(session, message)
        message.meta = _remember(message.meta, body.interrupt_id, {"by": actor, "at": now.isoformat()}, CLAIM_KEY)
        flag_modified(message, "meta")
        session.commit()
    return None, run_version


def _settle(project_id, conversation_uuid, body, result):
    """The claim released; on success the record written in its place (one write)."""
    with db.get_session(project_id) as session:
        message = _message(session, conversation_uuid, body.message_id)
        if message is not None:
            meta = _remember(message.meta, body.interrupt_id, None, CLAIM_KEY)
            message.meta = _remember(meta, body.interrupt_id, result) if result is not None else meta
            flag_modified(message, "meta")
            session.commit()


def complete(module, project_id, conversation_uuid, body, actor):  # pylint: disable=R0914
    """Create the credential the paused run asked for. Returns the reply; ``FormRefused`` with nothing usable kept."""
    if not is_governed(module):
        raise FormRefused("PROJECT_GOVERNANCE_OFF")
    delegated, secret_fields = _check_body(body)
    if not _may_create(project_id, actor):
        raise FormRefused("FORBIDDEN")
    if body.ownership == SHARED and _is_personal_project(project_id, actor):
        raise FormRefused("OWNERSHIP_INVALID")
    delegation = _default_delegation(actor) if delegated else None
    done, run_version = _claim(project_id, conversation_uuid, body, actor)
    if done is not None:   # the same form sent again: the record, nothing created twice
        return {**public_result(done), "configuration_id": done.get("configuration_id"), "requests": []}
    step, created, ref_id = "configuration", None, None
    try:
        created = _create_configuration(project_id, body, actor, body.ownership == SHARED, delegated)
        step = "reference"
        ref_id = _file_reference(project_id, str(created.get("uuid")), actor, body.ownership, delegation)
        if not delegated:
            step = "value"
            stored = _store_value(project_id, ref_id, secret_fields, body.ownership, actor)
            if stored.get("error") is not None:
                log.error("credential form: the value was not stored (%s)", stored.get("code"))
                raise FormRefused("DEPENDENCY_UNAVAILABLE")
    except Exception as exc:   # nothing usable is kept: reference retired, configuration and its Vault secrets gone
        if ref_id is not None:
            _retire(project_id, ref_id, actor)
        if created is not None:
            _delete_configuration(project_id, created)
        _settle(project_id, conversation_uuid, body, None)
        code = exc.code if isinstance(exc, FormRefused) else "DEPENDENCY_UNAVAILABLE"
        _record(project_id, actor, ref_id, created.get("id") if created else None, body.interrupt_id,
                conversation_uuid, reason=FAILED_REASON, step=step)
        if isinstance(exc, FormRefused):
            raise
        log.error("credential form: failed at %s (%s)", step, type(exc).__name__)
        raise FormRefused(code) from None
    state, check_code = _probe(project_id, ref_id, body.ownership, delegated, actor)
    filed, refused = _file_run_requests(module, project_id, run_version, actor, conversation_uuid)
    result = {"reference_id": ref_id, "configuration_id": created.get("id"),
              "configuration_uuid": str(created.get("uuid")), "configuration_title": created.get("elitea_title"),
              "title": body.title, "credential_type": body.credential_type, "state": state, "created_by": actor}
    if check_code:
        result["check"] = check_code
    _settle(project_id, conversation_uuid, body, result)
    _record(project_id, actor, ref_id, created.get("id"), body.interrupt_id, conversation_uuid)
    reply = {**public_result(result), "configuration_id": created.get("id"), "requests": filed}
    if refused:
        reply["requests_refused"] = refused
    return reply


def _delegated_types():
    from .credential_slots import DELEGATED_TYPES  # pylint: disable=C0415
    return DELEGATED_TYPES


GRAPH_RESOURCE = "https://graph.microsoft.com"


def _default_delegation(actor):
    """The descriptor of a person's own Microsoft sign-in credential: the platform's app, the person's own tenant, the
    Graph default scope, their login as the assertion (the administration create takes the same keys)."""
    try:
        user = auth.get_user(actor) or {}
    except Exception:  # pylint: disable=W0703
        raise FormRefused("DEPENDENCY_UNAVAILABLE") from None
    tenant = user.get("idp_tid")
    if not tenant:
        raise FormRefused("INVALID_REQUEST", "delegation")
    return {"oauth_app_id": "platform", "tenant_id": str(tenant).lower(), "resource": GRAPH_RESOURCE,
            "scopes": [f"{GRAPH_RESOURCE}/.default"], "assertion_source": "login"}


# --- the resume (item 4) ----------------------------------------------------------------------------------------

def _reference(project_id, ref_id):
    from plugins.secrets.utils.credential_queries import reference_detail  # pylint: disable=C0415,E0401
    try:
        return reference_detail(project_id, ref_id)
    except Exception as exc:  # pylint: disable=W0703
        if getattr(exc, "code", None) == "REFERENCE_NOT_FOUND":
            return None
        raise FormRefused("DEPENDENCY_UNAVAILABLE") from None


def _user_id(value):
    return value.get("id") if isinstance(value, dict) else None


def server_value(project_id, interrupt, value, actor):
    """The value the model gets for ``created``: the server's own record of this interrupt (``RESULT_KEY``), after the
    caller's ``{reference_id}`` was shown to be it, and the reference to be in the project with the caller as its
    custodian or creator. ``FormRefused(HITL_VALUE_INVALID)`` for anything else."""
    from ..models.pd.continue_predict import CredentialFormValue  # pylint: disable=C0415
    from pydantic import ValidationError  # pylint: disable=C0415
    try:
        parsed = CredentialFormValue.model_validate(value)
    except ValidationError:
        raise FormRefused("HITL_VALUE_INVALID") from None
    done = interrupt.get(RESULT_KEY) if isinstance(interrupt, dict) else None
    if not isinstance(done, dict) or done.get("reference_id") != str(parsed.reference_id) \
            or done.get("created_by") != actor:
        raise FormRefused("HITL_VALUE_INVALID")
    reference = _reference(project_id, str(parsed.reference_id))
    if reference is None or actor not in (_user_id(reference.get("owner")), _user_id(reference.get("created_by"))):
        raise FormRefused("HITL_VALUE_INVALID")
    return {**public_result(done), "state": reference.get("state") or done.get("state")}


def check_resume(module, project_id, message_id, action, value, decisions, actor):
    """Before a resume dispatches anything: a pending ``credential_form`` interrupt is answered only with ``created``
    (its value replaced by ``server_value``) or ``cancel`` (no value). Returns ``(value, decisions)`` to dispatch (the
    caller's own when no credential form is pending). Under off and audit only nothing is read."""
    if not is_governed(module):
        return value, decisions
    from ..models.message_group import ConversationMessageGroup  # pylint: disable=C0415
    with db.get_session(project_id) as session:
        message = session.query(ConversationMessageGroup).filter(
            ConversationMessageGroup.uuid == str(message_id)).first()
        meta = message.meta if message is not None else None
        pending = pending_interrupts(meta)
        forms = {interrupt_identity(item): item for item in pending if item.get("guardrail_type") == GUARDRAIL}
        run_user = _run_user(session, message) if forms else None
    if not forms:
        return value, decisions
    from .internal_obo import current_claims  # pylint: disable=C0415
    if current_claims() is not None or run_user != actor:
        # only the person answers their credential form: never a run (its token), never another member
        raise FormRefused("HITL_VALUE_INVALID")
    if decisions:
        answered = []
        for decision in decisions:
            target = forms.get(str((decision or {}).get("interrupt_id") or "")) if isinstance(decision, dict) else None
            if target is None:
                answered.append(decision)
                continue
            answered.append({**decision, "value": _answer(project_id, target, decision.get("action"),
                                                          decision.get("value"), actor)})
        return value, answered
    if len(pending) != 1:
        return value, decisions   # the scalar rule (exactly one pending interrupt) refuses it later
    return _answer(project_id, next(iter(forms.values())), action, value, actor), decisions


def _answer(project_id, interrupt, action, value, actor):
    if action not in ACTIONS:
        raise FormRefused("HITL_VALUE_INVALID")
    if action == "cancel":
        if value not in (None, "", {}):
            raise FormRefused("HITL_VALUE_INVALID")
        return {}
    return server_value(project_id, interrupt, value, actor)
