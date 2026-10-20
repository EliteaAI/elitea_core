"""C-1009 (#7105): the operator REST API of agent identity bindings.

The handlers, their request parsing, queries and the lifecycle they share with the admin task run for real on SQLite
files (see ``fixtures/admission_env.py``). Only the platform around them is faked: ``check_api`` is replaced by a
decorator that resolves permissions the way the auth plugin does (any of the named permissions), from the roles each
endpoint's own descriptor recommends, evaluated through the auth plugin's real ``Permissions`` model when it is
checked out next to this plugin (a mirror of its defaults otherwise, with a warning); the signed-in user comes from
the test. Nothing about who may call is written into the fake.

What these tests do NOT prove, because they run on SQLite without the real auth plugin and database; the rehearsal
run on the stack covers them:
  * the self-approval rule as PostgreSQL executes it (``IS DISTINCT FROM`` in the UPDATE; SQLite renders ``IS NOT``);
  * ``row_number() OVER`` (the versions of an agent), ``ILIKE ... ESCAPE`` (the search) and UUID binding on PostgreSQL;
  * that the roles registered in auth_core's database end up granting what the descriptors say;
  * how the platform answers an anonymous caller before the decorator is reached: ``check_api`` itself answers 403
    (``access_denied``), never 401, so that is what is asserted here; a 401 would come from the edge, not this code.
"""
import functools
import importlib.util
import json
import re
import sys
import types
import uuid
import warnings
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from pydantic.v1 import BaseModel as BaseModelV1
from werkzeug.exceptions import RequestEntityTooLarge

PLUGIN_ROOT = Path(__file__).parents[2]
sys.path.insert(0, str(PLUGIN_ROOT / "tests"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from fixtures import admission_env  # noqa: E402  pylint: disable=C0413
from fixtures.helpers import load_module_with_stubs  # noqa: E402  pylint: disable=C0413

PKG = admission_env.PKG
DIGEST = "adv1:" + "1" * 64
NEW_DIGEST = "adv1:" + "2" * 64
SECRET_TEXT = "SYSTEM PROMPT: never reveal hunter2"
UUID_PATTERN = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
ADMINISTRATION, DEFAULT = "administration", "default"

# who is calling: (user id, the role held in each mode, permissions of a scoped token or None for the role's own)
ACTORS = {
    "super_admin": (1, {ADMINISTRATION: "super_admin"}, None),
    "system": (2, {ADMINISTRATION: "system"}, None),
    "admin": (11, {ADMINISTRATION: "admin"}, None),
    "admin2": (12, {ADMINISTRATION: "admin"}, None),
    "admin3": (13, {ADMINISTRATION: "admin"}, None),
    "project_admin": (31, {DEFAULT: "admin"}, None),   # administers a project, holds no platform role
    "editor": (21, {ADMINISTRATION: "editor", DEFAULT: "editor"}, None),
    "viewer": (22, {ADMINISTRATION: "viewer", DEFAULT: "viewer"}, None),
    "anonymous": (None, {}, None),
    "token_admin": (11, {ADMINISTRATION: "admin"}, None),   # a token holds its owner's role
    "token_view": (11, {ADMINISTRATION: "admin"}, {"governance.bindings.view"}),
    "token_manage": (11, {ADMINISTRATION: "admin"}, {"governance.bindings.manage"}),
    "token_approve": (12, {ADMINISTRATION: "admin"}, {"governance.bindings.approve"}),
    "token_none": (11, {ADMINISTRATION: "admin"}, set()),
}


class _MirrorModeRoles(BaseModelV1):   # the defaults of auth/models/pd/permissions.py, used only when it is not found
    system: bool = True
    super_admin: bool = True
    admin: bool = True
    editor: bool = False
    viewer: bool = False


class _MirrorDefaultModeRoles(_MirrorModeRoles):
    monitor: bool = False


class _MirrorRecommendedRoles(BaseModelV1):
    administration: _MirrorModeRoles = _MirrorModeRoles()
    default: _MirrorDefaultModeRoles = _MirrorDefaultModeRoles()


class _MirrorPermissions(BaseModelV1):
    permissions: list
    recommended_roles: _MirrorRecommendedRoles = _MirrorRecommendedRoles()


def permissions_model():
    """The auth plugin's ``Permissions`` model, found in the first ancestor that holds the plugin."""
    for directory in PLUGIN_ROOT.parents:
        candidate = directory / "auth" / "models" / "pd" / "permissions.py"
        if candidate.is_file():
            spec = importlib.util.spec_from_file_location("auth_permissions_7105", candidate)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            return module.Permissions, True
    warnings.warn("the auth plugin is not checked out next to this one: roles are evaluated with a mirror of its "
                  "defaults, not its model", stacklevel=1)
    return _MirrorPermissions, False


def effective_roles(descriptor):
    """``{mode: {role: bool}}`` the way the auth plugin turns a descriptor into registered rows."""
    return permissions_model()[0].parse_obj(descriptor).recommended_roles.dict()


class FakeRequest:
    args, content_length, body, raw, max_content_length = {}, None, None, None, None

    def get_data(self):
        data = self.raw if self.raw is not None else json.dumps(self.body).encode()
        if self.max_content_length is not None and len(data) > self.max_content_length:
            raise RequestEntityTooLarge()
        return data

    def get_json(self, silent=False):   # pylint: disable=W0613
        return self.body


@pytest.fixture(scope="module")
def ns(tmp_path_factory):   # pylint: disable=R0915
    saved = dict(sys.modules)
    ns = admission_env.install(PLUGIN_ROOT, tmp_path_factory.mktemp("binding_api"))
    tools = ns.tools

    class Application(ns.Base):   # the one table of the project schema the list reads names from
        __tablename__ = "applications"
        __table_args__ = {"schema": "tenant"}
        id = ns.sa.Column(ns.sa.Integer, primary_key=True)
        name = ns.sa.Column(ns.sa.String, nullable=False)

    ns.Application = Application
    sys.modules[f"{PKG}.models.all"].Application = Application
    ns.request, ns.actor, ns.descriptors = FakeRequest(), "admin", []
    ns.flask.request = ns.request

    def permissions_of(actor, mode=ADMINISTRATION):
        user_id, roles, token = ACTORS[actor]
        if user_id is None:
            return set()
        if token is not None:
            return set(token)
        granted = set()
        for descriptor in ns.descriptors:
            if effective_roles(descriptor)[mode].get(roles.get(mode)):
                granted.update(descriptor["permissions"])
        return granted

    def check_api(descriptor):
        ns.descriptors.append(descriptor)

        def decorator(func):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                if not set(descriptor["permissions"]) & permissions_of(ns.actor):
                    return {"ok": False, "error": "access_denied"}, 403
                return func(*args, **kwargs)
            wrapper.descriptor = descriptor
            return wrapper
        return decorator

    tools.auth.decorators = types.SimpleNamespace(check_api=check_api)
    tools.auth.current_user = lambda: {"id": ACTORS[ns.actor][0]}
    tools.auth.resolve_permissions = lambda mode, project_id=None: permissions_of(ns.actor, mode)
    tools.register_openapi = lambda **meta: (lambda func: func)
    tools.api_tools = types.SimpleNamespace(
        APIModeHandler=type("APIModeHandler", (), {"__init__": lambda self, module: setattr(self, "module", module)}),
        APIBase=type("APIBase", (), {}), with_modes=lambda params: params)
    for name in (f"{PKG}.api", f"{PKG}.api.v2"):
        package = types.ModuleType(name)
        package.__path__ = []
        sys.modules[name] = package
    ns.endpoints = types.SimpleNamespace(**{
        name: load_module_with_stubs(PLUGIN_ROOT / "api" / "v2" / f"{name}.py", f"{PKG}.api.v2.{name}")
        for name in ("agent_identities", "agent_identity", "agent_identity_action", "governance_agents")})
    yield ns
    sys.modules.clear()
    sys.modules.update(saved)


@pytest.fixture
def env(ns):
    ns.env.reset()
    ns.actor = "admin"
    ns.request.args, ns.request.body, ns.request.content_length, ns.request.raw = {}, None, None, None
    ns.env.users.update({11: "admin1@example.com", 12: "admin2@example.com", 13: "admin3@example.com"})
    seed_agent(ns.env, 1, 7, "Jira helper", 70)
    ns.env.digests[70] = DIGEST
    return ns.env


def seed_agent(env, project_id, application_id, name, version_id, version_name="v1"):
    env.add_application(project_id, application_id, name)
    env.add_version(project_id, version_id, application_id, version_name)


def hit(ns, endpoint, method, *, actor="admin", args=None, body=None, size=None, raw_size=None, project_id=1, **path):
    """Call one handler as ``actor``; returns ``(json body, status)``.

    ``size``: the Content-Length header ("unbounded": none). ``raw_size``: the bytes that really arrive."""
    ns.actor = actor
    ns.request.args = args or {}
    ns.request.body = body
    ns.request.raw = None if raw_size is None else b"x" * raw_size
    ns.request.max_content_length = None   # the handler sets the limit on the request itself
    unbounded = size == "unbounded"   # a body that does not say how long it is
    ns.request.content_length = None if unbounded else (size if size is not None else (
        None if body is None else len(json.dumps(body))))
    handler = getattr(ns.endpoints, endpoint).AdminAPI(ns.env.module(governed=False))
    response = getattr(handler, method)(project_id=project_id, **path)
    payload, status = response if isinstance(response, tuple) else (response, 200)
    json.dumps(payload)   # every response is plain JSON
    return payload, status


def rows(env, ns, project_id=1):
    return env.query(project_id, ns.models.AgentIdentityBinding)


def binding(env, ns):
    [row] = rows(env, ns)
    return row


def lifecycle(env):
    return [(event["reason_code"], event.get("caller_ref")) for _, event in env.recorded if event["event_type"] == "lifecycle"]


def create(ns, **over):
    return hit(ns, "agent_identities", "post", body={"application_id": 7, "version_id": 70,
                                                    "identity_mode": "elitea_native", **over}, **{})


def act(ns, binding_id, action, expected_state, actor="admin", **more):
    return hit(ns, "agent_identity_action", "post", actor=actor, binding_id=uuid.UUID(str(binding_id)),
               body={"action": action, "expected_state": expected_state, **more})


def reach(ns, env, state):
    """A binding of version 70 in ``state`` (written directly, with no creator on record)."""
    binding_id = env.add_binding(1, 7, 70, state=state, digest=DIGEST)
    if state in ("suspended", "retiring"):   # a binding that was once approved keeps its approval
        with ns.sa_orm.Session(env.engine(1)) as session:
            session.get(ns.models.AgentIdentityBinding, binding_id).approval_ref = "appr-1"
            session.get(ns.models.AgentIdentityBinding, binding_id).policy_revision = "rev-1"
            session.commit()
    return str(binding_id)


# --- create -------------------------------------------------------------------------------------

def test_create_makes_a_draft_with_the_digest_and_audits_it_in_the_same_transaction(ns, env):
    payload, status = create(ns)
    row = binding(env, ns)
    assert status == 200 and payload == {"ok": True, "id": str(row.id), "state": "draft", "digest": DIGEST}
    assert (row.state, row.created_by, row.definition_digest, row.project_id) == ("draft", 11, DIGEST, 1)
    [(session, event)] = env.recorded
    assert (event["reason_code"], event["caller_ref"], event["agent_binding_ref"]) == (
        "BINDING_DRAFT", "operator:11", str(row.id))
    assert session.commits == 1


@pytest.mark.parametrize("body", [
    {"application_id": 7, "version_id": 70},
    {"application_id": 7, "version_id": 70, "identity_mode": "root"},
    {"application_id": "7", "version_id": 70, "identity_mode": "elitea_native"},
    {"application_id": True, "version_id": 70, "identity_mode": "elitea_native"},
    {"application_id": 0, "version_id": 70, "identity_mode": "elitea_native"},
    {"application_id": 7, "version_id": -1, "identity_mode": "elitea_native"},
    {"application_id": 7, "version_id": 70, "identity_mode": "elitea_native", "created_by": 1},
    {"application_id": 7, "version_id": 70, "identity_mode": "elitea_native", "state": "active"},
])
def test_create_refuses_an_invalid_body_and_writes_nothing(ns, env, body):
    payload, status = hit(ns, "agent_identities", "post", body=body)
    assert status == 400 and payload["code"] == "INVALID_REQUEST" and payload["error"].startswith("the request body")
    assert rows(env, ns) == [] and env.recorded == []


@pytest.mark.parametrize("raw", [None, [], "text", 5])
def test_create_needs_a_json_object(ns, env, raw):
    ns.actor = "admin"
    ns.request.args, ns.request.body, ns.request.content_length = {}, raw, 10
    payload, status = ns.endpoints.agent_identities.AdminAPI(env.module(governed=False)).post(project_id=1)
    assert (status, payload["code"]) == (400, "INVALID_REQUEST")


GOOD_BODY = {"application_id": 7, "version_id": 70, "identity_mode": "elitea_native"}


@pytest.mark.parametrize("size", [8 * 1024 + 1, 10 ** 7])
def test_a_body_that_says_it_is_too_long_is_refused_before_it_is_read(ns, env, size):
    payload, status = hit(ns, "agent_identities", "post", size=size, body=GOOD_BODY)
    assert (status, payload["code"]) == (413, "PAYLOAD_TOO_LARGE") and rows(env, ns) == []


def test_a_body_that_does_not_say_its_length_is_read_up_to_the_limit_and_cut_off_beyond_it(ns, env):
    payload, status = hit(ns, "agent_identities", "post", size="unbounded", raw_size=8 * 1024 + 1, body=GOOD_BODY)
    assert (status, payload["code"]) == (413, "PAYLOAD_TOO_LARGE") and rows(env, ns) == []
    assert ns.request.max_content_length == 8 * 1024
    payload, status = hit(ns, "agent_identities", "post", size="unbounded", body=GOOD_BODY)   # no Content-Length is not an error
    assert status == 200 and len(rows(env, ns)) == 1


@pytest.mark.parametrize("size, raw_size", [(8 * 1024, None), ("unbounded", 8 * 1024)])
def test_a_body_of_exactly_the_limit_is_parsed(ns, env, size, raw_size):
    payload, status = hit(ns, "agent_identities", "post", size=size, raw_size=raw_size, body=GOOD_BODY)
    assert status == 200 and len(rows(env, ns)) == 1


def test_the_invalid_body_message_names_fields_never_values(ns, env):
    payload, _ = hit(ns, "agent_identities", "post", body={
        "application_id": "eyJhbGciOi.secret-looking.value", "version_id": 70, "identity_mode": "x", "extra": 1})
    assert payload["error"] == "the request body is not valid: invalid application_id, identity_mode; unknown field"
    assert "eyJ" not in json.dumps(payload)


def test_create_twice_is_a_conflict(ns, env):
    create(ns)
    payload, status = create(ns)
    assert (status, payload["code"]) == (409, "BINDING_EXISTS") and len(rows(env, ns)) == 1


def test_create_for_a_version_that_is_not_in_the_application_or_project_is_not_found(ns, env):
    seed_agent(env, 2, 8, "Other", 80)
    for body in ({"application_id": 8, "version_id": 70}, {"application_id": 7, "version_id": 80},
                 {"application_id": 7, "version_id": 999}):
        payload, status = create(ns, **body)
        assert (status, payload["code"]) == (404, "VERSION_NOT_FOUND"), body
    assert rows(env, ns) == [] and rows(env, ns, 2) == [] and env.recorded == []


def test_a_digest_that_cannot_be_computed_is_a_readable_not_found(ns, env):
    env.digests.pop(70)
    payload, status = create(ns)
    assert (status, payload["code"], payload["error"]) == (
        404, "VERSION_NOT_FOUND", "the version does not exist in this application")
    assert rows(env, ns) == []


# --- the state matrix through the API ---------------------------------------------------------------

STATES = ("draft", "provisioning", "pending_approval", "active", "suspended", "retiring", "retired")
ACTION_RESULT = {   # action -> {state it applies to: state it ends in}
    "submit": {"draft": "pending_approval"},
    "activate": {"pending_approval": "active"},
    "suspend": {"active": "suspended"},
    "resume": {"suspended": "active"},
    "retire": {"draft": "retired", "provisioning": "retired", "pending_approval": "retired", "active": "retired",
               "suspended": "retired", "retiring": "retired"},
    "rebind": {"active": "pending_approval", "suspended": "pending_approval"},
}


@pytest.mark.parametrize("action", sorted(ACTION_RESULT))
@pytest.mark.parametrize("state", STATES)
def test_every_action_in_every_state(ns, env, action, state):
    binding_id = reach(ns, env, state)
    before = len(env.recorded)
    approval = {"approval_ref": "CHG-12", "policy_revision": "rev-1"} if action == "activate" else {}
    payload, status = act(ns, binding_id, action, state, **approval)
    target = ACTION_RESULT[action].get(state)
    if target is None:
        assert (status, payload["code"]) == (409, "INVALID_STATE")
        assert payload["error"] == f"cannot {action} a binding in state '{state}'"
        assert binding(env, ns).state == state and len(env.recorded) == before
    else:
        assert (status, payload) == (200, {"ok": True, "id": binding_id, "state": target})
        assert binding(env, ns).state == target and len(env.recorded) > before


def test_the_whole_life_of_a_binding_through_two_operators(ns, env):
    binding_id = create(ns)[0]["id"]
    assert act(ns, binding_id, "submit", "draft")[0]["state"] == "pending_approval"
    payload, status = act(ns, binding_id, "activate", "pending_approval", actor="admin2",
                          approval_ref="CHG-12", policy_revision="rev-1")
    assert (status, payload["state"]) == (200, "active")
    row = binding(env, ns)
    assert (row.approval_ref, row.policy_revision, row.created_by, row.state_changed_by) == ("CHG-12", "rev-1", 11, 12)
    assert act(ns, binding_id, "suspend", "active")[0]["state"] == "suspended"
    assert act(ns, binding_id, "resume", "suspended")[0]["state"] == "active"
    assert act(ns, binding_id, "retire", "active")[0]["state"] == "retired"
    assert [reason for reason, _ in lifecycle(env)] == [
        "BINDING_DRAFT", "BINDING_PROVISIONING", "BINDING_PENDING_APPROVAL", "BINDING_ACTIVE", "BINDING_SUSPENDED",
        "BINDING_ACTIVE", "BINDING_RETIRING", "BINDING_RETIRED"]


def test_activation_needs_an_approval_and_a_policy_revision(ns, env):
    binding_id = reach(ns, env, "pending_approval")
    for more in ({}, {"approval_ref": "CHG-12"}, {"policy_revision": "rev-1"}):
        payload, status = act(ns, binding_id, "activate", "pending_approval", **more)
        assert (status, payload["code"]) == (400, "APPROVAL_REQUIRED"), more
    assert binding(env, ns).state == "pending_approval"


def test_activation_of_a_definition_that_changed_is_a_conflict(ns, env):
    binding_id = reach(ns, env, "pending_approval")
    env.digests[70] = NEW_DIGEST
    payload, status = act(ns, binding_id, "activate", "pending_approval", approval_ref="a", policy_revision="r")
    assert (status, payload["code"]) == (409, "BINDING_DIGEST_MISMATCH")
    assert DIGEST not in json.dumps(payload) and NEW_DIGEST not in json.dumps(payload)


def test_a_runtime_domain_binding_is_activated_only_when_the_platform_allows_it(ns, env):
    binding_id = str(env.add_binding(1, 7, 70, state="pending_approval", mode="runtime_domain_binding", digest=DIGEST))
    payload, status = act(ns, binding_id, "activate", "pending_approval", approval_ref="a", policy_revision="r")
    assert (status, payload["code"]) == (403, "DENIED_BINDING")


@pytest.mark.parametrize("body", [
    {"action": "create", "expected_state": "draft"},
    {"action": "delete", "expected_state": "draft"},
    {"action": "submit"},
    {"action": "submit", "expected_state": "nowhere"},
    {"action": "activate", "expected_state": "pending_approval", "approval_ref": "  "},
    {"action": "activate", "expected_state": "pending_approval", "approval_ref": "x" * 129},
    {"action": "activate", "expected_state": "pending_approval", "policy_revision": "line\nbreak"},
    {"action": "submit", "expected_state": "draft", "dry_run": True},
    {"action": "submit", "expected_state": "draft", "binding_id": "x"},
])
def test_an_invalid_action_body_is_refused_and_nothing_is_written(ns, env, body):
    binding_id = reach(ns, env, "draft")
    before = len(env.recorded)
    payload, status = hit(ns, "agent_identity_action", "post", binding_id=uuid.UUID(binding_id), body=body)
    assert (status, payload["code"]) == (400, "INVALID_REQUEST")
    assert binding(env, ns).state == "draft" and len(env.recorded) == before


def test_a_stale_view_is_a_conflict_and_nothing_is_written(ns, env):
    binding_id = reach(ns, env, "active")
    before = len(env.recorded)
    payload, status = act(ns, binding_id, "suspend", "pending_approval")
    assert (status, payload["code"]) == (409, "CONCURRENT_CHANGE")
    assert binding(env, ns).state == "active" and len(env.recorded) == before


def test_a_lost_race_is_a_conflict_too(ns, env, monkeypatch):
    binding_id = reach(ns, env, "active")
    monkeypatch.setattr(ns.binding_admin, "transition_binding", lambda *args, **kwargs: False)
    payload, status = act(ns, binding_id, "suspend", "active")
    assert (status, payload["code"]) == (409, "CONCURRENT_CHANGE") and binding(env, ns).state == "active"


def test_an_audit_trail_that_is_down_is_a_503_and_nothing_changes(ns, env):
    binding_id = reach(ns, env, "active")
    env.record_raises = RuntimeError("outbox full")
    payload, status = act(ns, binding_id, "suspend", "active")
    assert (status, payload["code"]) == (503, "AUDIT_UNAVAILABLE")
    assert "outbox" not in json.dumps(payload) and (binding(env, ns).state, binding(env, ns).revocation_epoch) == ("active", 0)


# --- self-approval ----------------------------------------------------------------------------------

def test_the_creator_cannot_approve_and_another_admin_can(ns, env):
    binding_id = create(ns)[0]["id"]
    act(ns, binding_id, "submit", "draft", actor="admin2")
    before = len(env.recorded)
    for actor in ("admin", "admin2"):   # the creator, and whoever submitted it
        payload, status = act(ns, binding_id, "activate", "pending_approval", actor=actor,
                              approval_ref="CHG-12", policy_revision="rev-1")
        assert (status, payload["code"]) == (403, "SELF_APPROVAL"), actor
        assert payload["error"] == "another operator must approve this binding"
    assert (binding(env, ns).state, len(env.recorded)) == ("pending_approval", before)
    payload, status = act(ns, binding_id, "activate", "pending_approval", actor="admin3",
                          approval_ref="CHG-12", policy_revision="rev-1")
    assert (status, payload["state"]) == (200, "active")


def test_a_super_admin_is_held_to_the_same_rule(ns, env):
    ns.actor = "super_admin"
    binding_id = hit(ns, "agent_identities", "post", actor="super_admin", body={
        "application_id": 7, "version_id": 70, "identity_mode": "elitea_native"})[0]["id"]
    act(ns, binding_id, "submit", "draft", actor="super_admin")
    payload, status = act(ns, binding_id, "activate", "pending_approval", actor="super_admin",
                          approval_ref="a", policy_revision="r")
    assert (status, payload["code"]) == (403, "SELF_APPROVAL")


# --- rebind -----------------------------------------------------------------------------------------

def test_rebind_through_the_api(ns, env):
    binding_id = create(ns)[0]["id"]
    act(ns, binding_id, "submit", "draft")
    act(ns, binding_id, "activate", "pending_approval", actor="admin2", approval_ref="CHG-1", policy_revision="rev-1")
    context = ns.adm.admit_execution(env.module(), env.request(), run_id="run-1").context_ref
    env.digests[70] = NEW_DIGEST   # the agent was edited
    payload, status = act(ns, binding_id, "rebind", "active", actor="admin3")
    row = binding(env, ns)
    assert (status, payload) == (200, {"ok": True, "id": binding_id, "state": "pending_approval"})
    assert (row.definition_digest, row.approval_ref, row.policy_revision, row.revocation_epoch, row.state_changed_by) == (
        NEW_DIGEST, None, None, 1, 13)
    [revoked] = env.query(1, ns.models.ExecutionSecurityContext)
    assert str(revoked.id) == context and revoked.state == "revoked"
    event = env.recorded[-1][1]
    assert (event["reason_code"], event["revocation_epoch"], event["caller_ref"]) == (
        "BINDING_REBIND_PENDING_APPROVAL", 1, "operator:13")
    # the rebound binding admits nothing until another operator approves the new definition
    with pytest.raises(ns.adm.AdmissionDenied):
        ns.adm.admit_execution(env.module(), env.request(), run_id="run-2")
    assert act(ns, binding_id, "activate", "pending_approval", actor="admin3",
               approval_ref="CHG-2", policy_revision="rev-2")[0]["code"] == "SELF_APPROVAL"
    assert act(ns, binding_id, "activate", "pending_approval", actor="admin2",
               approval_ref="CHG-2", policy_revision="rev-2")[0]["state"] == "active"


def test_the_api_writes_the_same_audit_events_as_the_task(ns, env):
    def flow_by_task():
        run = lambda action, actor: ns.binding_admin.manage_binding(  # noqa: E731
            env.module(governed=False),
            f"project_id=1;application_id=7;version_id=70;mode=elitea_native;action={action}"
            + (";approval_ref=CHG-1;policy_revision=r1" if action == "activate" else ""), actor)
        for action, actor in (("create", 11), ("submit", 11), ("activate", 12), ("suspend", 11), ("resume", 11),
                              ("rebind", 11), ("activate", 12), ("retire", 11)):
            assert "error" not in run(action, actor), action
        return [event for _, event in env.recorded]

    task_events = flow_by_task()
    env.reset()
    env.add_application(1, 7, "Jira helper")
    env.add_version(1, 70, 7, "v1")
    env.digests[70] = DIGEST
    binding_id = create(ns)[0]["id"]
    steps = (("submit", "draft", "admin", {}), ("activate", "pending_approval", "admin2", {"approval_ref": "CHG-1", "policy_revision": "r1"}),
             ("suspend", "active", "admin", {}), ("resume", "suspended", "admin", {}), ("rebind", "active", "admin", {}),
             ("activate", "pending_approval", "admin2", {"approval_ref": "CHG-1", "policy_revision": "r1"}),
             ("retire", "active", "admin", {}))
    for action, expected, actor, more in steps:
        assert act(ns, binding_id, action, expected, actor=actor, **more)[1] == 200, action
    api_events = [event for _, event in env.recorded]

    def shape(events):
        return [{key: value for key, value in event.items() if key not in ("event_id", "occurred_at", "agent_binding_ref")}
                for event in events]

    assert shape(api_events) == shape(task_events)


# --- permissions --------------------------------------------------------------------------------------

def test_activating_needs_the_approve_permission_and_the_rest_the_manage_one(ns, env):
    binding_id = reach(ns, env, "pending_approval")
    payload, status = act(ns, binding_id, "activate", "pending_approval", actor="token_manage",
                          approval_ref="a", policy_revision="r")
    assert (status, payload["code"]) == (403, "FORBIDDEN") and binding(env, ns).state == "pending_approval"
    for action in ("submit", "suspend", "resume", "retire", "rebind"):
        payload, status = act(ns, binding_id, action, "pending_approval", actor="token_approve")
        assert (status, payload["code"]) == (403, "FORBIDDEN"), action
    assert binding(env, ns).state == "pending_approval"
    payload, status = act(ns, binding_id, "activate", "pending_approval", actor="token_approve",
                          approval_ref="a", policy_revision="r")
    assert (status, payload["state"]) == (200, "active")


ENDPOINTS = {   # name -> how to call it, the permission it needs
    "list": lambda ns, actor, bid: hit(ns, "agent_identities", "get", actor=actor),
    "detail": lambda ns, actor, bid: hit(ns, "agent_identity", "get", actor=actor, binding_id=uuid.UUID(bid)),
    "agents": lambda ns, actor, bid: hit(ns, "governance_agents", "get", actor=actor),
    "create": lambda ns, actor, bid: hit(ns, "agent_identities", "post", actor=actor, body={
        "application_id": 7, "version_id": 70, "identity_mode": "dedicated_agent"}),
    "submit": lambda ns, actor, bid: act(ns, bid, "submit", "draft", actor=actor),
}
VIEWS, WRITES = ("list", "detail", "agents"), ("create", "submit")


@pytest.mark.parametrize("endpoint", sorted(ENDPOINTS))
@pytest.mark.parametrize("actor, may_view, may_write", [
    ("super_admin", True, True), ("admin", True, True), ("token_admin", True, True),
    ("system", False, False), ("project_admin", False, False), ("editor", False, False), ("viewer", False, False),
    ("anonymous", False, False), ("token_view", True, False), ("token_manage", False, True),
    ("token_approve", False, False), ("token_none", False, False),
])
def test_the_authorization_matrix(ns, env, endpoint, actor, may_view, may_write):
    binding_id = reach(ns, env, "draft")
    snapshot = ([(row.id, row.state) for row in rows(env, ns)], len(env.recorded))
    payload, status = ENDPOINTS[endpoint](ns, actor, binding_id)
    allowed = may_view if endpoint in VIEWS else may_write
    if allowed:
        assert status == 200, (endpoint, actor, payload)
    else:
        # check_api answers every caller it refuses, an anonymous one included, with 403 access_denied (never 401)
        assert status == 403 and payload.get("error") in ("access_denied", "you are not allowed to do this")
        # a refused call changed nothing
        assert ([(row.id, row.state) for row in rows(env, ns)], len(env.recorded)) == snapshot


def test_the_decorators_ask_for_the_designed_permissions(ns):
    e = ns.endpoints
    assert e.agent_identities.AdminAPI.get.descriptor["permissions"] == ["governance.bindings.view"]
    assert e.agent_identity.AdminAPI.get.descriptor["permissions"] == ["governance.bindings.view"]
    assert e.governance_agents.AdminAPI.get.descriptor["permissions"] == ["governance.bindings.view"]
    assert e.agent_identities.AdminAPI.post.descriptor["permissions"] == ["governance.bindings.manage"]
    assert e.agent_identity_action.AdminAPI.post.descriptor["permissions"] == [
        "governance.bindings.manage", "governance.bindings.approve"]
    for handler in (e.agent_identities, e.agent_identity, e.agent_identity_action, e.governance_agents):
        assert list(handler.API.mode_handlers) == ["administration"]   # project (default) mode: 404, fail closed
        assert handler.API.url_params


ADMINISTRATION_ROLES = {"system": False, "super_admin": True, "admin": True, "editor": False, "viewer": False}
DEFAULT_ROLES = {"system": False, "super_admin": True, "admin": False, "editor": False, "viewer": False, "monitor": False}


def test_what_the_module_registers_and_what_each_endpoint_recommends_grant_the_designed_roles(ns):
    """Through the auth plugin's own model: an unnamed role would silently default to True there (system, super_admin),
    and an unnamed mode would grant ``admin`` in every project."""
    descriptors = [handler.descriptor for handler in (
        ns.endpoints.agent_identities.AdminAPI.get, ns.endpoints.agent_identities.AdminAPI.post,
        ns.endpoints.agent_identity.AdminAPI.get, ns.endpoints.agent_identity_action.AdminAPI.post,
        ns.endpoints.governance_agents.AdminAPI.get)]
    descriptors += [ns.binding_api.registration(permission) for permission in ns.binding_api.PERMISSIONS]
    assert sorted(permission for item in descriptors[-3:] for permission in item["permissions"]) == sorted([
        "governance.bindings.approve", "governance.bindings.manage", "governance.bindings.view"])
    for descriptor in descriptors:
        effective = effective_roles(descriptor)
        assert effective == {"administration": ADMINISTRATION_ROLES, "default": DEFAULT_ROLES}, descriptor["permissions"]


def test_the_module_registers_each_binding_permission_through_that_helper():
    import ast
    tree = ast.parse((PLUGIN_ROOT / "module.py").read_text())
    calls = [node for node in ast.walk(tree) if isinstance(node, ast.Call)
             and getattr(node.func, "attr", None) == "register_permissions" and node.args
             and isinstance(node.args[0], ast.Call) and getattr(node.args[0].func, "id", None) == "binding_registration"]
    assert len(calls) == 1


# --- a foreign project's id ---------------------------------------------------------------------------

def test_an_id_of_another_project_is_not_found_and_nothing_is_written(ns, env):
    mine = reach(ns, env, "active")
    seed_agent(env, 2, 7, "Same ids elsewhere", 70)
    theirs = str(env.add_binding(2, 7, 70, state="active", digest=DIGEST))
    snapshot = ([(r.id, r.state, r.revocation_epoch) for r in rows(env, ns)],
                [(r.id, r.state, r.revocation_epoch) for r in rows(env, ns, 2)], len(env.recorded))
    for action in ("suspend", "retire", "rebind", "submit"):
        payload, status = hit(ns, "agent_identity_action", "post", project_id=1, binding_id=uuid.UUID(theirs),
                              body={"action": action, "expected_state": "active"})
        assert (status, payload["code"]) == (404, "NOT_FOUND"), action
        assert payload["error"] == "no such record"
    payload, status = hit(ns, "agent_identity", "get", project_id=1, binding_id=uuid.UUID(theirs))
    assert (status, payload["code"]) == (404, "NOT_FOUND")
    assert hit(ns, "agent_identity", "get", binding_id=uuid.uuid4())[1] == 404
    assert snapshot == ([(r.id, r.state, r.revocation_epoch) for r in rows(env, ns)],
                        [(r.id, r.state, r.revocation_epoch) for r in rows(env, ns, 2)], len(env.recorded))
    assert [str(r.id) for r in rows(env, ns)] == [mine]
    # the same ids under their own project work
    assert hit(ns, "agent_identity_action", "post", project_id=2, binding_id=uuid.UUID(theirs),
               body={"action": "suspend", "expected_state": "active"})[1] == 200


def test_listing_a_project_shows_that_projects_bindings_only(ns, env):
    reach(ns, env, "active")
    seed_agent(env, 2, 9, "Elsewhere", 90)
    env.add_binding(2, 9, 90, state="active")
    payload, _ = hit(ns, "agent_identities", "get")
    assert payload["total"] == 1 and [r["application"]["name"] for r in payload["rows"]] == ["Jira helper"]
    payload, _ = hit(ns, "agent_identities", "get", project_id=2)
    assert [r["application"]["name"] for r in payload["rows"]] == ["Elsewhere"]


def test_a_refusal_names_no_id(ns, env):
    binding_id = reach(ns, env, "retired")
    texts = []
    texts.append(act(ns, binding_id, "suspend", "retired")[0])
    texts.append(act(ns, binding_id, "suspend", "active")[0])
    texts.append(act(ns, str(uuid.uuid4()), "suspend", "active")[0])
    texts.append(hit(ns, "agent_identities", "post", body={"application_id": 7, "version_id": 999, "identity_mode": "elitea_native"})[0])
    for text in texts:
        assert not UUID_PATTERN.search(json.dumps(text)) and not re.search(r"\b(70|999)\b", text["error"]), text


# --- the list: pages, sorting, filters, digest status ---------------------------------------------------

def many(env, count, project_id=1):
    for index in range(count):
        seed_agent(env, project_id, 100 + index, f"Agent {index:02d}", 1000 + index)
        env.add_binding(project_id, 100 + index, 1000 + index, state="active")


def stamp(env, ns, ids_to_times):
    with ns.sa_orm.Session(env.engine(1)) as session:
        for row in session.query(ns.models.AgentIdentityBinding):
            moment = ids_to_times(row)
            row.updated_at, row.created_at = moment, moment
        session.commit()


def test_a_page_is_twenty_by_default_and_never_more_than_fifty(ns, env):
    many(env, 60)
    payload, status = hit(ns, "agent_identities", "get")
    assert (status, payload["total"], len(payload["rows"])) == (200, 60, 20)
    for asked in ("500", "51", "1000000000000"):
        assert len(hit(ns, "agent_identities", "get", args={"limit": asked})[0]["rows"]) == 50
    assert len(hit(ns, "agent_identities", "get", args={"limit": "7"})[0]["rows"]) == 7
    assert len(hit(ns, "agent_identities", "get", args={"limit": "0"})[0]["rows"]) == 1
    for junk in ("x", "-5", "1.5", ""):
        assert len(hit(ns, "agent_identities", "get", args={"limit": junk})[0]["rows"]) in (1, 20), junk
    assert len(hit(ns, "agent_identities", "get", args={"limit": "50", "offset": "55"})[0]["rows"]) == 5
    assert hit(ns, "agent_identities", "get", args={"offset": "-3"})[1] == 200


def test_the_digest_is_computed_for_the_rows_of_the_page_only(ns, env):
    many(env, 30)
    env.digest_calls = 0
    payload, _ = hit(ns, "agent_identities", "get", args={"limit": "5"})
    assert len(payload["rows"]) == 5 and env.digest_calls == 5


def test_only_whitelisted_sort_names_sort_and_anything_else_is_the_default(ns, env):
    for index, name in enumerate(("Beta", "Alpha", "Gamma")):
        seed_agent(env, 1, 200 + index, name, 2000 + index)
        env.add_binding(1, 200 + index, 2000 + index, state="active")
    base = datetime(2026, 10, 1, tzinfo=timezone.utc)
    stamp(env, ns, lambda row: base + timedelta(days=row.application_id))
    names = lambda args: [r["application"]["name"] for r in hit(ns, "agent_identities", "get", args=args)[0]["rows"]]  # noqa: E731
    default = names({})
    assert default == ["Gamma", "Alpha", "Beta"]   # updated_at, newest first
    assert names({"sort_by": "application_name", "sort_order": "asc"}) == ["Alpha", "Beta", "Gamma"]
    assert names({"sort_by": "application_name", "sort_order": "desc"}) == ["Gamma", "Beta", "Alpha"]
    assert names({"sort_by": "updated_at", "sort_order": "asc"}) == ["Beta", "Alpha", "Gamma"]
    assert names({"sort_by": "created_at"}) == default
    assert names({"sort_by": "state"})   # a text column sorts without error
    for hostile in ("definition_digest", "created_by", "id", "state; DROP TABLE x", "application_name)--", "", "UPDATED_AT"):
        assert names({"sort_by": hostile}) == default, hostile
    assert names({"sort_order": "sideways"}) == default


def test_the_search_matches_the_agent_name_and_wildcards_are_literal(ns, env):
    for index, name in enumerate(("Jira triage", "Jira_helper", "100% done", "Other")):
        seed_agent(env, 1, 300 + index, name, 3000 + index)
        env.add_binding(1, 300 + index, 3000 + index, state="active")
    names = lambda text: sorted(r["application"]["name"] for r in hit(ns, "agent_identities", "get", args={"search": text})[0]["rows"])  # noqa: E731
    assert names("jira") == ["Jira triage", "Jira_helper"]
    assert names("%") == ["100% done"] and names("_") == ["Jira_helper"]
    assert names("nothing like it") == []
    payload, status = hit(ns, "agent_identities", "get", args={"search": "x" * 129})
    assert (status, payload["code"]) == (400, "INVALID_REQUEST")
    assert hit(ns, "agent_identities", "get", args={"search": "x" * 128})[1] == 200


def test_the_filters(ns, env):
    reach(ns, env, "active")
    seed_agent(env, 1, 8, "Second", 80)
    env.add_binding(1, 8, 80, state="draft", mode="dedicated_agent")
    get = lambda **args: [r["application"]["name"] for r in hit(ns, "agent_identities", "get", args=args)[0]["rows"]]  # noqa: E731
    assert get(state="draft") == ["Second"] and get(identity_mode="elitea_native") == ["Jira helper"]
    assert get(application_id="8") == ["Second"] and get(state="active", application_id="8") == []
    for bad in ({"state": "nope"}, {"identity_mode": "root"}, {"application_id": "x"}, {"application_id": "99999999999"}):
        payload, status = hit(ns, "agent_identities", "get", args=bad)
        assert (status, payload["code"]) == (400, "INVALID_REQUEST"), bad


def test_the_digest_status_of_each_row_is_against_the_live_version(ns, env):
    reach(ns, env, "active")                                 # 70: unchanged
    for version_id, application_id in ((71, 8), (72, 9)):
        seed_agent(env, 1, application_id, f"App {application_id}", version_id)
    env.add_binding(1, 8, 71, state="active", digest=DIGEST)
    env.digests[71] = NEW_DIGEST                             # 71: edited since the approval
    env.add_binding(1, 9, 72, state="active", digest=DIGEST)
    env.digests.pop(72, None)                                # 72: cannot be computed any more
    payload, _ = hit(ns, "agent_identities", "get", args={"sort_by": "application_name", "sort_order": "asc"})
    status = {r["application"]["id"]: (r["digest_status"], r["definition_digest"]) for r in payload["rows"]}
    assert status == {7: ("match", "1" * 12), 8: ("changed", "1" * 12), 9: ("unknown", "1" * 12)}


def test_a_row_has_the_designed_fields_and_no_secret(ns, env):
    binding_id = create(ns)[0]["id"]
    env.digest_inputs[70] = {"instructions": {"length": 1, "sha256": "0" * 64}}
    [row] = hit(ns, "agent_identities", "get")[0]["rows"]
    assert row == {
        "id": binding_id, "application": {"id": 7, "name": "Jira helper"}, "version": {"id": 70, "name": "v1"},
        "identity_mode": "elitea_native", "state": "draft", "definition_digest": "1" * 12, "digest_status": "match",
        "policy_revision": None, "approval_ref": None, "revocation_epoch": 0,
        "created_by": {"id": 11, "email": "admin1@example.com"}, "state_changed_by": None,
        "created_at": row["created_at"], "updated_at": row["updated_at"],
    }
    assert "external_agent_identity_ref" not in row and "owner_user_ref" not in row


def test_users_are_resolved_with_one_call_per_page_and_a_failure_only_hides_the_emails(ns, env):
    many(env, 6)
    env.list_users_calls.clear()
    payload, _ = hit(ns, "agent_identities", "get")
    assert len(env.list_users_calls) <= 1
    env.users.clear()
    payload, status = hit(ns, "agent_identities", "get")
    assert status == 200 and all(r["created_by"] is None for r in payload["rows"])   # these rows have no creator
    binding_id = create(ns, version_id=70)[0]["id"]
    env.list_users_raises = RuntimeError("auth down")
    row = hit(ns, "agent_identities", "get", args={"search": "Jira"})[0]["rows"][0]
    assert row["created_by"] == {"id": 11, "email": None} and row["id"] == binding_id


def test_a_database_that_cannot_answer_is_a_503_without_detail(ns, env, monkeypatch):
    def boom(project_id):
        raise RuntimeError("relation \"tenant.agent_identity_binding\" does not exist; password=hunter2")
    monkeypatch.setattr(ns.tools.db, "get_session", boom)
    for call in (lambda: hit(ns, "agent_identities", "get"), lambda: hit(ns, "governance_agents", "get"),
                 lambda: hit(ns, "agent_identity", "get", binding_id=uuid.uuid4())):
        payload, status = call()
        assert (status, payload["code"]) == (503, "DEPENDENCY_UNAVAILABLE") and "hunter2" not in json.dumps(payload)


# --- the detail ---------------------------------------------------------------------------------------

def test_the_detail_shows_the_digest_as_stored_and_as_it_is_now_with_the_inputs(ns, env):
    binding_id = reach(ns, env, "active")
    env.digests[70] = NEW_DIGEST
    env.digest_inputs[70] = {
        "agent_type": "openai", "model": "gpt-x", "instructions": {"length": 36, "sha256": "a" * 64},
        "tools": [{"id": 3, "type": "jira", "name": "Jira", "credential_configuration_uuid": None}]}
    payload, status = hit(ns, "agent_identity", "get", binding_id=uuid.UUID(binding_id))
    assert status == 200
    assert (payload["definition_digest"], payload["digest_now"], payload["digest_status"]) == (DIGEST, NEW_DIGEST, "changed")
    assert payload["digest_inputs"] == env.digest_inputs[70] and payload["grants_count"] is None
    assert (payload["id"], payload["state"], payload["approval_ref"], payload["policy_revision"]) == (
        binding_id, "active", "appr-1", "rev-1")
    assert SECRET_TEXT not in json.dumps(payload)


def test_a_detail_whose_version_is_gone_still_answers(ns, env):
    binding_id = reach(ns, env, "active")
    env.digests.pop(70)
    payload, status = hit(ns, "agent_identity", "get", binding_id=uuid.UUID(binding_id))
    assert (status, payload["digest_now"], payload["digest_status"], payload["digest_inputs"]) == (
        200, None, "unknown", None)


# --- the agents for the create dialog -----------------------------------------------------------------

def test_the_agents_come_with_their_versions_and_the_bindings_each_has(ns, env):
    seed_agent(env, 1, 8, "Alpha", 80, "v1")
    env.add_version(1, 81, 8, "v2")
    env.add_binding(1, 8, 80, state="active")
    env.add_binding(1, 8, 80, state="draft", mode="dedicated_agent")
    payload, status = hit(ns, "governance_agents", "get")
    assert status == 200 and payload["total"] == 2
    alpha, jira = payload["rows"]
    assert (alpha["id"], alpha["name"], jira["name"]) == (8, "Alpha", "Jira helper")
    assert [(v["id"], v["name"]) for v in alpha["versions"]] == [(81, "v2"), (80, "v1")]
    assert [(b["identity_mode"], b["state"]) for b in alpha["versions"][1]["bindings"]] == [
        ("dedicated_agent", "draft"), ("elitea_native", "active")]
    assert alpha["versions"][0]["bindings"] == [] and jira["versions"][0]["bindings"] == []
    assert hit(ns, "governance_agents", "get", args={"search": "alp"})[0]["rows"][0]["name"] == "Alpha"


def test_the_agents_are_paged_and_their_versions_capped(ns, env):
    seed_agent(env, 1, 8, "Many versions", 800)
    for index in range(1, 70):
        env.add_version(1, 800 + index, 8, f"v{index}")
    many(env, 30)
    assert len(hit(ns, "governance_agents", "get", args={"limit": "500"})[0]["rows"]) == 32
    assert len(hit(ns, "governance_agents", "get", args={"limit": "5"})[0]["rows"]) == 5
    [row] = hit(ns, "governance_agents", "get", args={"search": "many"})[0]["rows"]
    assert len(row["versions"]) == 50 and row["versions"][0]["id"] == 869


# --- the files themselves ---------------------------------------------------------------------------

def test_the_api_files_say_what_they_are_for():
    for name in ("agent_identities", "agent_identity", "agent_identity_action", "governance_agents"):
        source = (PLUGIN_ROOT / "api" / "v2" / f"{name}.py").read_text()
        assert "mcp_tool=False" in source and "available_to_users=False" in source and "@register_openapi" in source
        assert source.index("@register_openapi") < source.index("@auth.decorators.check_api")


# --- only an activation carries an approval (review) --------------------------------------------------

@pytest.mark.parametrize("action, state", [("submit", "draft"), ("suspend", "active"), ("resume", "suspended"),
                                          ("retire", "active"), ("rebind", "active")])
@pytest.mark.parametrize("more", [{"approval_ref": "CHG-1"}, {"policy_revision": "rev-1"},
                                  {"approval_ref": "CHG-1", "policy_revision": "rev-1"}])
def test_any_action_but_activate_refuses_the_approval_fields_and_writes_nothing(ns, env, action, state, more):
    binding_id = reach(ns, env, state)
    before = (binding(env, ns).state, binding(env, ns).approval_ref, binding(env, ns).policy_revision,
              binding(env, ns).revocation_epoch, len(env.recorded))
    payload, status = act(ns, binding_id, action, state, **more)
    assert (status, payload["code"]) == (400, "INVALID_REQUEST")
    assert payload["error"] == "approval_ref and policy_revision belong to action activate only"
    row = binding(env, ns)
    assert (row.state, row.approval_ref, row.policy_revision, row.revocation_epoch, len(env.recorded)) == before


def test_a_manage_only_caller_cannot_put_text_into_the_approval_the_next_activation_would_show(ns, env):
    binding_id = create(ns)[0]["id"]
    assert act(ns, binding_id, "submit", "draft", actor="token_manage", approval_ref="forged")[1] == 400
    assert act(ns, binding_id, "submit", "draft", actor="token_manage")[1] == 200
    assert (binding(env, ns).approval_ref, binding(env, ns).policy_revision) == (None, None)
    payload, status = act(ns, binding_id, "activate", "pending_approval", actor="admin2")
    assert (status, payload["code"]) == (400, "APPROVAL_REQUIRED")
    payload, status = act(ns, binding_id, "activate", "pending_approval", actor="admin2",
                          approval_ref="MINE-7", policy_revision="pol-1")
    assert status == 200
    [event] = [event for _, event in env.recorded if event["reason_code"] == "BINDING_ACTIVE"]
    assert (event["approver_ref"], event["policy_revision"]) == ("MINE-7", "pol-1")
    assert all("forged" not in json.dumps(event) for _, event in env.recorded)


@pytest.mark.parametrize("value", ["has space", "a;b", "ü", "x" * 129, "", "tab\t", "<script>"])
def test_an_approval_must_be_reference_text(ns, env, value):
    binding_id = reach(ns, env, "pending_approval")
    for field in ("approval_ref", "policy_revision"):
        other = {"policy_revision": "r"} if field == "approval_ref" else {"approval_ref": "a"}
        payload, status = act(ns, binding_id, "activate", "pending_approval", actor="admin2", **{field: value}, **other)
        assert (status, payload["code"]) == (400, "INVALID_REQUEST"), (field, value)
    assert binding(env, ns).state == "pending_approval"


@pytest.mark.parametrize("value", ["CHG-12", "a.b_c:d/e@f-g", "x" * 128])
def test_reference_text_is_accepted(ns, env, value):
    binding_id = reach(ns, env, "pending_approval")
    payload, status = act(ns, binding_id, "activate", "pending_approval", actor="admin2",
                          approval_ref=value, policy_revision=value)
    assert (status, payload["state"]) == (200, "active") and binding(env, ns).approval_ref == value


def test_a_resume_keeps_the_stored_approval_in_the_row_and_in_the_audit(ns, env):
    binding_id = reach(ns, env, "suspended")
    assert act(ns, binding_id, "resume", "suspended")[1] == 200
    row = binding(env, ns)
    assert (row.approval_ref, row.policy_revision) == ("appr-1", "rev-1")
    event = env.recorded[-1][1]
    assert (event["reason_code"], event["approver_ref"]) == ("BINDING_ACTIVE", "appr-1")


# --- a binding whose version is gone, and two creates that race --------------------------------------------

def drop_version(ns, env, version_id=70):
    with ns.sa_orm.Session(env.engine(1)) as session:
        session.query(ns.ApplicationVersion).filter_by(id=version_id).delete()
        session.commit()


@pytest.mark.parametrize("action, state, target", [("suspend", "active", "suspended"), ("retire", "active", "retired"),
                                                  ("retire", "suspended", "retired"), ("submit", "draft", "pending_approval")])
def test_a_binding_without_its_version_can_still_be_taken_out_of_use(ns, env, action, state, target):
    binding_id = reach(ns, env, state)
    drop_version(ns, env)
    payload, status = act(ns, binding_id, action, state)
    assert (status, payload["state"]) == (200, target)


@pytest.mark.parametrize("action, state", [("activate", "pending_approval"), ("resume", "suspended"), ("rebind", "active")])
def test_what_reads_the_definition_needs_the_version(ns, env, action, state):
    binding_id = reach(ns, env, state)
    drop_version(ns, env)
    approval = {"approval_ref": "a", "policy_revision": "r"} if action == "activate" else {}
    payload, status = act(ns, binding_id, action, state, actor="admin2", **approval)
    assert (status, payload["code"]) == (404, "VERSION_NOT_FOUND") and binding(env, ns).state == state


def test_two_creates_that_race_are_a_conflict_not_a_503(ns, env, monkeypatch):
    create(ns)
    events = len(env.recorded)
    monkeypatch.setattr(ns.binding_admin, "_find", lambda values: ns.sa.select(ns.models.AgentIdentityBinding).where(ns.sa.false()))
    payload, status = create(ns)
    assert (status, payload["code"]) == (409, "BINDING_EXISTS") and len(rows(env, ns)) == 1 and len(env.recorded) == events


# --- the rebind event -------------------------------------------------------------------------------------

def test_the_rebind_event_names_the_new_and_the_previous_definition(ns, env):
    binding_id = reach(ns, env, "active")
    env.digests[70] = NEW_DIGEST
    act(ns, binding_id, "rebind", "active")
    event = env.recorded[-1][1]
    assert (event["reason_code"], event["target_ref"], event["operation_id"]) == (
        "BINDING_REBIND_PENDING_APPROVAL", NEW_DIGEST, f"previous:{DIGEST}")


# --- the application_id filter ---------------------------------------------------------------------------

@pytest.mark.parametrize("value", ["x", "-1", "0", "00", "1.5", "+3", "1e3", " 1", "1 ", "٣", "²", "99999999999", "2147483648",
                                   "1;DROP", "%"])
def test_application_id_must_be_a_positive_integer(ns, env, value):
    payload, status = hit(ns, "agent_identities", "get", args={"application_id": value})
    assert (status, payload["code"]) == (400, "INVALID_REQUEST") and payload["error"] == "application_id must be a positive integer"


@pytest.mark.parametrize("value", ["1", "007", "7", "2147483647"])
def test_a_positive_application_id_filters(ns, env, value):
    reach(ns, env, "active")
    payload, status = hit(ns, "agent_identities", "get", args={"application_id": value})
    assert status == 200 and payload["total"] == (1 if int(value) == 7 else 0)


# --- the digest status is read in bulk -----------------------------------------------------------------------

def statements(env, project_id=1):
    seen = []
    ns_sa = env.ns.sa
    engine = env.engine(project_id)
    listener = lambda conn, cursor, statement, params, context, many: seen.append((statement, params))   # noqa: E731
    ns_sa.event.listen(engine, "before_cursor_execute", listener)
    return seen, lambda: ns_sa.event.remove(engine, "before_cursor_execute", listener)


def seed_real(env, count, toolkits=2):
    """``count`` bindings over real versions (with definitions and toolkits), so the real digest code runs."""
    for index in range(count):
        version_id, application_id = 5000 + index, 500 + index
        env.add_application(1, application_id, f"Agent {index:03d}")
        env.add_version(1, version_id, application_id, "v1", agent_type="openai", instructions=f"Be agent {index}.",
                        llm_settings={"model_name": "m", "temperature": 0.5})
        for tool in range(toolkits):
            env.add_toolkit(1, version_id, 90000 + index * 10 + tool, "github", {"url": "x"}, selected=("run", "list"))


def test_a_page_reads_the_digests_in_two_queries_whatever_its_size(ns, env, monkeypatch):
    monkeypatch.setattr(ns.binding_queries, "digests_for_versions", ns.real_digests_for_versions)
    seed_real(env, 30)
    with ns.sa_orm.Session(env.engine(1)) as session:
        stored = ns.digest.digests_for_versions(session, range(5000, 5030))
    for index in range(30):
        env.add_binding(1, 500 + index, 5000 + index, state="active", digest=stored[5000 + index])
    counts = {}
    for limit in (3, 30):
        seen, stop = statements(env)
        payload, status = hit(ns, "agent_identities", "get", args={"limit": str(limit)})
        stop()
        assert status == 200 and len(payload["rows"]) == limit
        assert all(row["digest_status"] == "match" for row in payload["rows"])
        counts[limit] = len(seen)
    # count + page + the versions + their toolkits; the same for 3 rows and for 30
    assert counts == {3: 4, 30: 4}


def test_a_version_shared_by_two_bindings_is_digested_once(ns, env):
    reach(ns, env, "active")
    env.add_binding(1, 7, 70, state="draft", mode="dedicated_agent", digest=DIGEST)
    env.digest_calls = 0
    payload, _ = hit(ns, "agent_identities", "get")
    assert len(payload["rows"]) == 2 and env.digest_calls == 1


def test_the_bulk_digest_equals_the_single_one_and_is_none_for_what_cannot_be_digested(ns, env):
    seed_real(env, 4, toolkits=3)
    env.add_version(1, 5999, 599, "bad", agent_type="openai", instructions="x", llm_settings={"t": float("inf")})
    with ns.sa_orm.Session(env.engine(1)) as session:
        bulk = ns.digest.digests_for_versions(session, [5000, 5001, 5002, 5003, 5999, 123456, 5000])
        single = {version_id: ns.digest.digest_for_version(session, version_id) for version_id in range(5000, 5004)}
    assert {key: bulk[key] for key in range(5000, 5004)} == single
    assert bulk[123456] is None and bulk[5999] is None and set(bulk) == {5000, 5001, 5002, 5003, 5999, 123456}
    assert ns.digest.digests_for_versions(None, []) == {}


def test_the_bulk_digest_changes_with_the_definition(ns, env):
    seed_real(env, 1, toolkits=1)
    with ns.sa_orm.Session(env.engine(1)) as session:
        before = ns.digest.digests_for_versions(session, [5000])[5000]
        session.query(ns.ApplicationVersion).filter_by(id=5000).update({"instructions": "changed"})
        session.commit()
        assert ns.digest.digests_for_versions(session, [5000])[5000] != before


# --- the agents read bindings only for the versions they show ----------------------------------------------------

def test_the_agents_read_bindings_only_for_the_versions_that_are_listed(ns, env):
    seed_agent(env, 1, 8, "Many versions", 800)
    for index in range(1, 70):
        env.add_version(1, 800 + index, 8, f"v{index}")
    env.add_binding(1, 8, 800, state="active")     # the oldest version: 20 older than the 50 listed
    env.add_binding(1, 8, 869, state="draft")      # the newest
    seen, stop = statements(env)
    payload, _ = hit(ns, "governance_agents", "get", args={"search": "many"})
    stop()
    [row] = payload["rows"]
    assert len(row["versions"]) == 50
    listed_bindings = [b for ver in row["versions"] for b in ver["bindings"]]
    assert [b["state"] for b in listed_bindings] == ["draft"]
    [(_, params)] = [(sql, params) for sql, params in seen if "agent_identity_binding" in sql]
    assert 869 in params and 800 not in params and len(params) == 1 + 50   # the project, then the 50 listed versions
