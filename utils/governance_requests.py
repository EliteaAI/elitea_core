"""A member's request: file what readiness says an entity still needs, in one transaction (#7269, C-1009).

A request is the existing rows in their existing waiting states, made by the member (``created_by`` the member), never
an approval: a binding draft submitted for approval (or a rebind of the version's own binding), a personal credential
reference of the member's own (``pending``; the value through the member's own value page), a mandate draft for the
member's own schedule submitted for approval, use grants in ``pending_approval`` for the member (or for that mandate).
No request table: the project inbox lists those rows. Each item goes through the lifecycle function its admin task and
REST route use (four-eyes rules and state machines unchanged), and the batch is all or nothing: the lifecycle functions
share one session whose commit is a flush, and the one commit is at the end. Every item adds its own lifecycle audit
event and a ``GOVERNANCE_REQUEST_FILED`` event naming where the request came from (``caller_ref`` ``user:<id>``, or
``run:<id>`` for what a governed run created through the internal MCP, attributed to the run's subject).

The keys a caller names must be requestable now (``governance_readiness``); anything else refuses the whole batch
(``STALE_READINESS``) with nothing written.
"""
import uuid
from datetime import datetime, timedelta, timezone

from pylon.core.tools import log
from tools import db, this

from .binding_admin import BindingActionError, _create as create_binding, _transition as change_binding
from .binding_state import InvalidTransition
from .definition_digest import DigestError
from .governance import is_governed
from .governance_readiness import REQUESTED_MODE, readiness
from .mandate_admin import MandateTaskError, _change as change_mandate, _create as create_mandate, parse_mandate_param
from .mandate_identity import allowed_sources

REASON = "GOVERNANCE_REQUEST_FILED"
MANDATE_RUNTIME_S = 3600
MANDATE_VALIDITY_DAYS = 90
MAX_KEYS = 128
ORDER = ("binding", "reference", "mandate", "grant")


class RequestRefused(Exception):
    """Nothing was filed. ``code`` is stable; ``key`` names the item that refused, when one did."""

    def __init__(self, code, key=None):
        super().__init__(code)
        self.code, self.key = code, key


class _OneTransaction:
    """The batch's session as the lifecycle functions see it: their commit is a flush, so every item lands in the
    transaction committed once at the end. Anything else (a rollback included) is the session's own."""

    def __init__(self, session):
        self._session = session

    def commit(self):
        self._session.flush()

    def __getattr__(self, name):
        return getattr(self._session, name)


def _request_event(project_id, item, target, actor_subject, origin, run_id=None):
    event = {"event_type": "lifecycle", "reason_code": REASON, "outcome": "success", "project_id": project_id,
             "caller_ref": origin, "user_ref": actor_subject, "run_id": run_id,
             "target_ref": f"{item['kind']}:{target}"[:256]}
    return {key: value for key, value in event.items() if value is not None}


def _secrets_file(session, project_id, requests, actor):
    """The references and grants of the batch, by secrets' own lifecycle, in this session."""
    try:
        return this.for_module("secrets").module.file_credential_requests(session, project_id, requests, actor)
    except Exception as exc:  # pylint: disable=W0703
        code = getattr(exc, "code", None)
        raise RequestRefused(code if isinstance(code, str) else "DEPENDENCY_UNAVAILABLE",
                             getattr(exc, "key", None)) from None


def _file_binding(module, session, tracing, project_id, plan, actor):
    values = {"project_id": project_id, "application_id": plan["application_id"], "version_id": plan["version_id"],
              "mode": REQUESTED_MODE, "dry_run": False}
    from .admission import version_application_id  # pylint: disable=C0415
    if version_application_id(session, plan["version_id"]) != plan["application_id"]:
        raise RequestRefused("VERSION_NOT_FOUND", "binding")
    binding_id = plan.get("binding_id")
    if plan["action"] == "create":
        binding_id = create_binding(session, tracing, {**values, "action": "create"}, actor)["binding_ref"]
    action = "rebind" if plan["action"] == "rebind" else "submit"
    change_binding(module, session, tracing, {**values, "action": action, "binding_id": uuid.UUID(str(binding_id)),
                                              "expected_state": None}, actor)
    return str(binding_id)


def _file_mandate(module, session, tracing, project_id, plan, actor, delegated):
    until = (datetime.now(timezone.utc) + timedelta(days=MANDATE_VALIDITY_DAYS)).replace(microsecond=0)
    param = (f"project_id={project_id};action=create;version_id={plan['version_id']};owner_user_id={actor};"
             f"operations=run:schedule;max_runtime_s={MANDATE_RUNTIME_S};valid_until={until.isoformat()}")
    if delegated and "owner_delegated" in allowed_sources(module):
        param += ";identity=owner_delegated"   # the owner consents afterwards, on the schedule card
    values, error = parse_mandate_param(param)
    if error:
        raise RequestRefused("INVALID_REQUEST", "mandate")
    created = create_mandate(module, session, tracing, values, actor)
    mandate_id = created["mandate_ref"]
    change_mandate(module, session, tracing, {"project_id": project_id, "action": "submit",
                                              "mandate": uuid.UUID(mandate_id), "dry_run": False}, actor)
    return mandate_id


def file_requests(module, project_id, kind, entity_id, actor, keys=None, run_id=None):  # pylint: disable=R0912,R0913,R0914,R0915
    """File the requestable items of ``kind`` ``entity_id`` for ``actor`` (``keys``: those only), all or nothing.
    Returns ``[{key, kind, id}]`` of what was filed. ``RequestRefused`` with nothing written otherwise."""
    if not is_governed(module):
        raise RequestRefused("PROJECT_GOVERNANCE_OFF")
    if keys is not None and (not isinstance(keys, list) or len(keys) > MAX_KEYS
                             or not all(isinstance(key, str) and 0 < len(key) <= 256 for key in keys)):
        raise RequestRefused("INVALID_REQUEST")
    found = readiness(module, project_id, kind, entity_id, actor)
    requestable = {item["key"]: item for item in found.items if item["requestable"]}
    if keys is not None:
        stale = [key for key in keys if key not in requestable]
        if stale:
            raise RequestRefused("STALE_READINESS", stale[0])
        requestable = {key: requestable[key] for key in keys}
    if not requestable:
        return []
    items = sorted(requestable.values(), key=lambda item: ORDER.index(item["kind"]))
    subject = found.plan.get("_subject")
    origin = f"run:{run_id}" if run_id else f"user:{actor}"
    tracing = this.for_module("tracing").module
    filed = []
    with db.get_session(project_id) as raw:
        session = _OneTransaction(raw)
        try:
            binding_id = found.plan.get("_binding_id")
            references, mandate_id = {}, None
            for item in items:
                plan = found.plan.get(item["key"], {})
                if item["kind"] == "binding":
                    binding_id = _file_binding(module, session, tracing, project_id, plan, actor)
                    target = binding_id
                elif item["kind"] == "reference":
                    [made] = _secrets_file(session, project_id, [
                        {"kind": "reference", "key": item["key"], "configuration_uuid": plan["configuration_uuid"]}],
                        actor)
                    references[plan["configuration_uuid"]] = target = made["id"]
                elif item["kind"] == "mandate":
                    if plan.get("mandate_id"):   # the member's own draft: submitted
                        change_mandate(module, session, tracing, {
                            "project_id": project_id, "action": "submit", "mandate": uuid.UUID(plan["mandate_id"]),
                            "dry_run": False}, actor)
                        mandate_id = target = plan["mandate_id"]
                    else:
                        mandate_id = target = _file_mandate(module, session, tracing, project_id, plan, actor,
                                                            found.delegated)
                else:
                    ref = plan.get("reference_ref") or references.get(plan.get("configuration_uuid"))
                    if not ref or not binding_id or plan.get("toolkit_id") is None:
                        raise RequestRefused("STALE_READINESS", item["key"])
                    if kind == "schedule":
                        if mandate_id is None:
                            raise RequestRefused("STALE_READINESS", item["key"])
                        grant_subject = {"subject_ref": f"mandate:{mandate_id}", "subject_user_id": None}
                    else:
                        grant_subject = {"subject_ref": subject, "subject_user_id": actor}
                    [made] = _secrets_file(session, project_id, [
                        {"kind": "grant", "key": item["key"], "binding_ref": binding_id,
                         "toolkit_id": plan["toolkit_id"], "ref": ref, **grant_subject}], actor)
                    target = made["id"]
                tracing.security_audit_record(raw, _request_event(project_id, item, target, subject, origin, run_id))
                filed.append({"key": item["key"], "kind": item["kind"], "id": target})
            raw.commit()
        except RequestRefused:
            raw.rollback()
            raise
        except (BindingActionError, MandateTaskError) as exc:
            raw.rollback()
            raise RequestRefused(exc.code) from None
        except (InvalidTransition, DigestError) as exc:
            raw.rollback()
            raise RequestRefused(exc.reason_code) from None
        except Exception as exc:  # pylint: disable=W0703
            raw.rollback()
            log.error("governance requests: the batch failed (%s)", type(exc).__name__)
            raise RequestRefused("AUDIT_UNAVAILABLE") from None
    return filed


# --- what a governed run creates through the internal MCP (#7269 item 13) -------------------------------------------

# The internal MCP's creating writes, by method and path template (``internal_obo.ENDPOINT_OPERATIONS``), and what their
# answer names: the new agent's first version, the new toolkit, the new credential configuration
CREATIONS = {
    ("POST", "elitea_core/applications/{mode}/{project_id}"): "version",
    ("POST", "elitea_core/tools/{mode}/{project_id}"): "toolkit",
    ("POST", "configurations/configurations/{project_id}"): "configuration",
}


def _created_id(kind, answer):
    answer = answer if isinstance(answer, dict) else {}
    if kind == "version":
        details = answer.get("version_details") if isinstance(answer.get("version_details"), dict) else {}
        value = details.get("id")
    elif kind == "toolkit":
        value = answer.get("id")
    else:
        value = answer.get("uuid")
        return str(uuid.UUID(str(value))) if value else None
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _file_reference(project_id, configuration_uuid, actor, subject, run_id):
    """A credential a run created is its subject's own: their personal reference, requested (#7228 owns the choice
    of a shared one). Nothing for a configuration that is not a credential of this project."""
    from .credential_decision import CREDENTIAL_SECTION, read_configuration_facts  # pylint: disable=C0415
    facts = read_configuration_facts([(configuration_uuid, project_id)]).get(configuration_uuid) or {}
    if facts.get("section") != CREDENTIAL_SECTION or facts.get("project_id") != project_id:
        return []
    tracing = this.for_module("tracing").module
    item = {"key": f"reference:{configuration_uuid}", "kind": "reference"}
    with db.get_session(project_id) as raw:
        try:
            [made] = _secrets_file(_OneTransaction(raw), project_id, [
                {"kind": "reference", "key": item["key"], "configuration_uuid": configuration_uuid}], actor)
            tracing.security_audit_record(raw, _request_event(project_id, item, made["id"], subject, f"run:{run_id}",
                                                              run_id))
            raw.commit()
        except Exception:
            raw.rollback()
            raise
    return [{**item, "id": made["id"]}]


def file_for_run(module, claims, method, path, project_id, answer):
    """After a governed run's internal MCP created an agent, a toolkit or a credential: its readiness for the run's
    subject (the user, never the agent) and the request of what that user may request, filed as theirs with the run as
    its origin. A run never approves. ``None`` for any other call, or when governance is not enforced; never raises."""
    from .internal_obo import endpoint_key  # pylint: disable=C0415
    kind = CREATIONS.get((str(method or "").upper(), endpoint_key(path)))
    if kind is None or not isinstance(claims, dict) or not is_governed(module):
        return None
    user_id, run_id = claims.get("uid"), claims.get("run")
    try:
        project_id = int(project_id)
        entity_id = _created_id(kind, answer)
        if entity_id is None or not isinstance(user_id, int):
            return None
        if kind == "configuration":
            from .governance_readiness import _person  # pylint: disable=C0415
            subject = _person(user_id)
            return {"filed": _file_reference(project_id, entity_id, user_id, subject, run_id) if subject else [],
                    "items": []}
        filed = file_requests(module, project_id, kind, entity_id, user_id, run_id=run_id)
        items = readiness(module, project_id, kind, entity_id, user_id).items
        return {"entity_kind": kind, "entity_id": entity_id, "filed": filed, "items": items}
    except RequestRefused as exc:
        return {"entity_kind": kind, "filed": [], "refused": exc.code}
    except Exception as exc:  # pylint: disable=W0703
        log.error("governance requests: the run's request was not filed (%s)", type(exc).__name__)
        return {"entity_kind": kind, "filed": [], "refused": "DEPENDENCY_UNAVAILABLE"}
