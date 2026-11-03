"""C-1009 #7269 phase 1: governance for project admins (the project-mode handlers of the binding and mandate routes)
and the project inbox's escalation.

The handlers, the lifecycles they share with the admin tasks and the queries run for real on SQLite
(``fixtures/admission_env.py`` / ``mandate_env.py``). ``check_api`` is replaced by a decorator that grants what each
endpoint's own descriptor recommends for the caller's role *in the mode of the handler and the project of the call*
(through the auth plugin's real ``Permissions`` model when it is checked out next to this plugin), so a project role
counts in its own project only and a platform role in administration mode only, as auth_core resolves them.

Who calls (project 1 unless named): two project admins, an editor (the mandates' owner), a viewer, a member of project
2 only, a platform admin with no project role, a scoped API token and an unscoped one of the first admin, and nobody.
"""
import functools
import importlib.util
import json
import sys
import types
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

PLUGIN_ROOT = Path(__file__).parents[2]
sys.path.insert(0, str(PLUGIN_ROOT / "tests"))

from fixtures import admission_env, mandate_env  # noqa: E402  pylint: disable=C0413
from fixtures.helpers import load_module_with_stubs  # noqa: E402  pylint: disable=C0413

PKG = admission_env.PKG
ADMINISTRATION, DEFAULT = "administration", "default"
DIGEST = "adv1:" + "1" * 64
OWNER = mandate_env.OWNER
ADMIN_A, ADMIN_B, VIEWER, FOREIGN, PLATFORM = 31, 32, 34, 41, 11
SCOPED_TOKEN, UNSCOPED_TOKEN, OTHER_PROJECT_TOKEN, SYSTEM_TOKEN = 901, 902, 903, 904
SYSTEM = 5   # the project's system user
VALID_UNTIL = (datetime.now(timezone.utc) + timedelta(days=20)).replace(microsecond=0).isoformat()
# who calls: (user id, {mode or project id: role}, token id or None)
ACTORS = {
    "admin_a": (ADMIN_A, {1: "admin"}, None),
    "admin_b": (ADMIN_B, {1: "admin"}, None),
    "editor": (OWNER, {1: "editor"}, None),
    "viewer": (VIEWER, {1: "viewer"}, None),
    "foreign": (FOREIGN, {2: "admin"}, None),   # admin of another project
    "platform_admin": (PLATFORM, {ADMINISTRATION: "admin"}, None),
    "scoped_token": (ADMIN_A, {1: "admin"}, SCOPED_TOKEN),
    "unscoped_token": (ADMIN_A, {1: "admin"}, UNSCOPED_TOKEN),
    "anonymous": (None, {}, None),
    # (S-7 b) the project's system user: its role recommends nothing here; with the stale template rows a deployment may
    # still hold (C9-SEC-84 b, "admin"-like permissions) its unscoped token is refused by the gate
    "system": (SYSTEM, {1: "system"}, None),
    "system_token_with_stale_rows": (SYSTEM, {1: "admin"}, SYSTEM_TOKEN),
}


def permissions_model():
    for directory in PLUGIN_ROOT.parents:
        candidate = directory / "auth" / "models" / "pd" / "permissions.py"
        if candidate.is_file():
            spec = importlib.util.spec_from_file_location("auth_permissions_7269", candidate)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            return module.Permissions
    pytest.skip("the auth plugin's Permissions model is not next to this checkout")
    return None


def effective_roles(descriptor):
    return permissions_model().parse_obj(descriptor).recommended_roles.dict()


class FakeRequest:
    args, content_length, body, max_content_length = {}, None, None, None

    def get_data(self):
        return json.dumps(self.body).encode()

    def get_json(self, silent=False):   # pylint: disable=W0613
        return self.body


ENDPOINTS = ("agent_identities", "agent_identity", "agent_identity_action", "governance_agents", "governance_toolkits",
             "autonomous_mandates", "autonomous_mandate_action")


@pytest.fixture(scope="module")
def ns(tmp_path_factory):
    saved = dict(sys.modules)
    ns = mandate_env.install(PLUGIN_ROOT, tmp_path_factory.mktemp("project_governance"))
    tools = ns.tools

    class Application(ns.Base):
        __tablename__ = "applications"
        __table_args__ = {"schema": "tenant"}
        id = ns.sa.Column(ns.sa.Integer, primary_key=True)
        name = ns.sa.Column(ns.sa.String, nullable=False)

    ns.Application = Application
    sys.modules[f"{PKG}.models.all"].Application = Application
    ns.request, ns.actor, ns.descriptors = FakeRequest(), "admin_a", []
    ns.flask.request = ns.request
    ns.permission_asks = []

    def permissions_of(actor, mode, project_id):
        user_id, roles, _ = ACTORS[actor]
        if user_id is None:
            return set()
        role = roles.get(ADMINISTRATION) if mode == ADMINISTRATION else roles.get(project_id)
        key = ADMINISTRATION if mode == ADMINISTRATION else DEFAULT
        return {permission for descriptor in ns.descriptors for permission in descriptor["permissions"]
                if role and effective_roles(descriptor)[key].get(role)}

    def check_api(descriptor):
        ns.descriptors.append(descriptor)

        def decorator(func):
            @functools.wraps(func)
            def wrapper(handler, *args, **kwargs):
                granted = permissions_of(ns.actor, handler.mode, kwargs.get("project_id"))
                if not set(descriptor["permissions"]) & granted:
                    return {"ok": False, "error": "access_denied"}, 403
                return func(handler, *args, **kwargs)
            wrapper.descriptor = descriptor
            return wrapper
        return decorator

    def resolve_permissions(mode, project_id=None):
        ns.permission_asks.append((mode, project_id))
        return permissions_of(ns.actor, mode, project_id)

    tools.auth.decorators = types.SimpleNamespace(check_api=check_api)
    tools.auth.current_user = lambda: {"id": ACTORS[ns.actor][0]} if ACTORS[ns.actor][0] else {}
    tools.auth.resolve_permissions = resolve_permissions
    tools.register_openapi = lambda **meta: (lambda func: func)
    tools.api_tools = types.SimpleNamespace(
        APIModeHandler=type("APIModeHandler", (), {"__init__": lambda self, module: setattr(self, "module", module)}),
        APIBase=type("APIBase", (), {}), with_modes=lambda params: params)
    for name in (f"{PKG}.api", f"{PKG}.api.v2"):
        package = types.ModuleType(name)
        package.__path__ = []
        sys.modules[name] = package
    load_module_with_stubs(PLUGIN_ROOT / "models" / "pd" / "mandate_api.py", f"{PKG}.models.pd.mandate_api")
    ns.mandate_queries = load_module_with_stubs(PLUGIN_ROOT / "utils" / "mandate_queries.py",
                                                f"{PKG}.utils.mandate_queries")
    for name in ("constants", "schedule_access"):
        load_module_with_stubs(PLUGIN_ROOT / "utils" / f"{name}.py", f"{PKG}.utils.{name}")
    ns.endpoints = types.SimpleNamespace(**{
        name: load_module_with_stubs(PLUGIN_ROOT / "api" / "v2" / f"{name}.py", f"{PKG}.api.v2.{name}")
        for name in ENDPOINTS})
    yield ns
    sys.modules.clear()
    sys.modules.update(saved)


class Membership:
    """``admin_get_users_roles_in_project`` of the admin plugin: {user id: [role names]} without the system user."""

    def __init__(self):
        self.roles = {1: {ADMIN_A: ["admin", "editor"], ADMIN_B: ["admin"], OWNER: ["editor"], VIEWER: ["viewer"]},
                      2: {FOREIGN: ["admin"]}}
        self.calls, self.down = [], False

    def timeout(self, seconds):
        return self

    def admin_get_users_roles_in_project(self, project_id, filter_system_user=False):
        self.calls.append((project_id, filter_system_user))
        if self.down:
            raise TimeoutError("rpc down")
        return {user: list(names) for user, names in self.roles.get(project_id, {}).items()}


@pytest.fixture
def env(ns, monkeypatch):
    env = mandate_env.prepare(ns, ns.env)
    ns.mandate_admin._now = lambda: datetime.now(timezone.utc)  # pylint: disable=W0212
    ns.mandate_state._now = lambda: datetime.now(timezone.utc)  # pylint: disable=W0212
    env.users.update({uid: f"user{uid}@example.test" for uid in (OWNER, ADMIN_A, ADMIN_B, VIEWER, FOREIGN, PLATFORM)})
    env.add_application(1, 7, "Jira helper")
    env.add_version(1, 70, 7, "v1")
    env.digests[70] = DIGEST
    env.token_scopes[SCOPED_TOKEN] = {"user_id": ADMIN_A, "projects": [1], "operations": ["run", "admin"]}
    env.token_scopes[OTHER_PROJECT_TOKEN] = {"user_id": ADMIN_A, "projects": [2], "operations": ["admin"]}
    env.membership = Membership()
    monkeypatch.setattr(ns.tools, "rpc_tools", types.SimpleNamespace(
        RpcMixin=lambda: types.SimpleNamespace(rpc=env.membership)), raising=False)

    def holder():
        user_id, _, token = ACTORS[ns.actor]
        if token is not None:
            return types.SimpleNamespace(type="token", id=token, reference="-")
        return types.SimpleNamespace(type="user" if user_id else "public", id=user_id or "-", reference="session-ref")

    monkeypatch.setattr(ns.credential_probe, "_session_holder", holder)
    ns.permission_asks.clear()
    env.governed = True
    env.audit = False
    return env


def module(ns, env):
    built = ns.env.module(governed=env.governed, audit=env.audit)
    built.descriptor = types.SimpleNamespace(config={"mandate_identity_sources": []})
    return built


def hit(ns, env, endpoint, method, *, actor="admin_a", mode=DEFAULT, project_id=1, body=None, args=None, **path):
    ns.actor, ns.request.body, ns.request.args = actor, body, args or {}
    ns.request.content_length = None if body is None else len(json.dumps(body))
    ns.request.max_content_length = None
    endpoint_module = getattr(ns.endpoints, endpoint)
    handler = (endpoint_module.AdminAPI if mode == ADMINISTRATION else endpoint_module.ProjectAPI)(module(ns, env))
    handler.mode = mode
    payload, status = getattr(handler, method)(project_id=project_id, **path)
    json.dumps(payload)
    return payload, status


def bindings(env, ns):
    return env.query(1, ns.models.AgentIdentityBinding)


def create_binding(ns, env, actor="admin_a", mode=DEFAULT):
    return hit(ns, env, "agent_identities", "post", actor=actor, mode=mode,
               body={"application_id": 7, "version_id": 70, "identity_mode": "elitea_native"})


def act(ns, env, binding_id, action, expected, actor="admin_a", mode=DEFAULT, **more):
    if action == "activate" and "definition_digest" not in more:   # (#7228) the digest the approver reviewed
        with ns.sa_orm.Session(env.engine(1)) as session:
            row = session.get(ns.models.AgentIdentityBinding, uuid.UUID(str(binding_id)))
            more["definition_digest"] = row.definition_digest if row is not None else "adv1:" + "0" * 64
    return hit(ns, env, "agent_identity_action", "post", actor=actor, mode=mode,
               binding_id=uuid.UUID(str(binding_id)), body={"action": action, "expected_state": expected, **more})


def pending_binding(ns, env, creator="admin_a"):
    body, status = create_binding(ns, env, actor=creator)
    assert status == 200, body
    assert act(ns, env, body["id"], "submit", "draft", actor=creator)[1] == 200
    return body["id"]


def create_mandate(ns, env, actor="admin_a", mode=DEFAULT):
    body = {"version_id": 70, "owner_user_id": OWNER, "operations": ["run:schedule"], "max_runtime_s": 600,
            "valid_until": VALID_UNTIL}
    return hit(ns, env, "autonomous_mandates", "post", actor=actor, mode=mode, body=body)


def mandate_act(ns, env, mandate_id, action, actor="admin_a", mode=DEFAULT, **more):
    return hit(ns, env, "autonomous_mandate_action", "post", actor=actor, mode=mode,
               mandate_id=uuid.UUID(str(mandate_id)), body={"action": action, **more})


APPROVAL = {"approval_ref": "CHG-7269", "policy_revision": "rev-1"}


# --- the authorization matrix of every project-mode route ---------------------------------------------------------

def _routes(ns, env):
    binding_id = str(env.add_binding(1, 7, 70, state="draft", digest=DIGEST))
    mandate_binding = env.add_binding(1, 7, 71, state="active", digest=DIGEST)
    env.add_version(1, 71, 7, "v2")
    env.digests[71] = DIGEST
    return binding_id, mandate_binding


READS = ("binding_list", "binding_detail", "agents", "toolkits", "mandate_list")
CALLS = {
    "binding_list": lambda ns, env, actor, ids: hit(ns, env, "agent_identities", "get", actor=actor),
    "binding_detail": lambda ns, env, actor, ids: hit(ns, env, "agent_identity", "get", actor=actor,
                                                      binding_id=uuid.UUID(ids[0])),
    "agents": lambda ns, env, actor, ids: hit(ns, env, "governance_agents", "get", actor=actor),
    "toolkits": lambda ns, env, actor, ids: hit(ns, env, "governance_toolkits", "get", actor=actor),
    "mandate_list": lambda ns, env, actor, ids: hit(ns, env, "autonomous_mandates", "get", actor=actor),
    "binding_create": lambda ns, env, actor, ids: hit(ns, env, "agent_identities", "post", actor=actor, body={
        "application_id": 7, "version_id": 71, "identity_mode": "dedicated_agent"}),
    "binding_submit": lambda ns, env, actor, ids: act(ns, env, ids[0], "submit", "draft", actor=actor),
    "mandate_create": lambda ns, env, actor, ids: hit(ns, env, "autonomous_mandates", "post", actor=actor, body={
        "version_id": 71, "owner_user_id": OWNER, "operations": ["run:schedule"], "max_runtime_s": 600,
        "valid_until": VALID_UNTIL}),
}
# (actor, may read, may write); a refusal is check_api's 403 access_denied, or the gate's code
MATRIX = [
    ("admin_a", True, True), ("admin_b", True, True), ("scoped_token", True, True),
    ("editor", False, False), ("viewer", False, False), ("foreign", False, False), ("platform_admin", False, False),
    ("unscoped_token", "TOKEN_SCOPE_OPERATION", "TOKEN_SCOPE_OPERATION"), ("anonymous", False, False),
    ("system", False, False),
    ("system_token_with_stale_rows", "TOKEN_SCOPE_OPERATION", "TOKEN_SCOPE_OPERATION"),
]


@pytest.mark.parametrize("route", sorted(CALLS))
@pytest.mark.parametrize("actor, may_read, may_write", MATRIX)
def test_the_project_mode_authorization_matrix(ns, env, route, actor, may_read, may_write):
    ids = _routes(ns, env)
    before = ([(row.id, row.state) for row in bindings(env, ns)], len(env.recorded),
              len(env.query(1, ns.Mandate)))
    payload, status = CALLS[route](ns, env, actor, ids)
    allowed = may_read if route in READS else may_write
    if allowed is True:
        assert status == 200, (route, actor, payload)
        return
    if allowed is False:
        assert status == 403 and payload.get("error") == "access_denied", (route, actor, payload)
    else:
        assert (status, payload["code"]) == (403, allowed), (route, actor, payload)
    assert ([(row.id, row.state) for row in bindings(env, ns)], len(env.recorded),
            len(env.query(1, ns.Mandate))) == before


def _action_rows(ns, env):
    """A row in the state each lifecycle action starts from (bindings on their own versions, mandates on 70)."""
    rows = {}
    for version_id, state in ((80, "pending_approval"), (81, "active"), (82, "suspended"), (83, "active")):
        env.add_version(1, version_id, 7, f"v{version_id}")
        env.digests[version_id] = DIGEST if version_id != 83 else "adv1:" + "2" * 64   # 83: changed since approval
        rows[version_id] = str(env.add_binding(1, 7, version_id, state=state, digest=DIGEST))
    with ns.sa_orm.Session(env.engine(1)) as session:   # suspended by admin_a, a project admin
        row = session.get(ns.models.AgentIdentityBinding, uuid.UUID(rows[82]))
        row.created_by = row.state_changed_by = ADMIN_A
        row.approval_ref, row.policy_revision = "CHG-1", "rev-1"   # a resume carries the stored approval
        session.get(ns.models.AgentIdentityBinding, uuid.UUID(rows[80])).created_by = PLATFORM   # nobody who calls
        session.commit()
    binding = env.add_binding(1, 7, 70, state="active", digest=DIGEST)
    until = datetime.now(timezone.utc) + timedelta(days=20)
    rows["m_pending"] = str(mandate_env.add_mandate(env, ns, state="pending_approval", binding_ref=binding,
                                                    created_by=PLATFORM, valid_until=until))
    # (S-8) an active mandate to suspend, and one suspended by admin_a (a project admin) to resume
    rows["m_active"] = str(mandate_env.add_mandate(env, ns, version=81, state="active", binding_ref=uuid.UUID(rows[81]),
                                                   created_by=PLATFORM, valid_until=until))
    env.add_version(1, 84, 7, "v84")
    env.digests[84] = DIGEST
    suspended_binding = env.add_binding(1, 7, 84, state="active", digest=DIGEST)
    rows["m_suspended"] = _suspended_mandate(ns, env, ADMIN_A, version=84, binding=suspended_binding, until=until)
    return rows


def _suspended_mandate(ns, env, suspended_by, *, version=70, binding=None, until=None):
    until = until or datetime.now(timezone.utc) + timedelta(days=20)
    mandate_id = mandate_env.add_mandate(env, ns, version=version, state="suspended", created_by=PLATFORM,
                                         binding_ref=binding or env.add_binding(1, 7, version, state="active",
                                                                                digest=DIGEST),
                                         valid_until=until, suspended_reason="MANUAL")
    with ns.sa_orm.Session(env.engine(1)) as session:
        session.get(ns.Mandate, mandate_id).state_changed_by = suspended_by
        session.commit()
    return str(mandate_id)


ACTION_CALLS = {  # (S-7 a) every project-mode lifecycle action
    "binding_activate": lambda ns, env, actor, r: act(ns, env, r[80], "activate", "pending_approval", actor=actor,
                                                      **APPROVAL),
    "binding_suspend": lambda ns, env, actor, r: act(ns, env, r[81], "suspend", "active", actor=actor),
    "binding_resume": lambda ns, env, actor, r: act(ns, env, r[82], "resume", "suspended", actor=actor),
    "binding_rebind": lambda ns, env, actor, r: act(ns, env, r[83], "rebind", "active", actor=actor),
    "mandate_approve": lambda ns, env, actor, r: mandate_act(ns, env, r["m_pending"], "approve", actor=actor,
                                                             **APPROVAL),
    "mandate_reject": lambda ns, env, actor, r: mandate_act(ns, env, r["m_pending"], "reject", actor=actor),
    "mandate_revoke": lambda ns, env, actor, r: mandate_act(ns, env, r["m_pending"], "revoke", actor=actor),
    "mandate_suspend": lambda ns, env, actor, r: mandate_act(ns, env, r["m_active"], "suspend", actor=actor),
    "mandate_resume": lambda ns, env, actor, r: mandate_act(ns, env, r["m_suspended"], "resume", actor=actor),
}


@pytest.mark.parametrize("route", sorted(ACTION_CALLS))
@pytest.mark.parametrize("actor, refusal", [
    ("editor", "access_denied"), ("viewer", "access_denied"), ("foreign", "access_denied"),
    ("platform_admin", "access_denied"), ("anonymous", "access_denied"), ("system", "access_denied"),
    ("unscoped_token", "TOKEN_SCOPE_OPERATION"), ("system_token_with_stale_rows", "TOKEN_SCOPE_OPERATION"),
])
def test_every_lifecycle_action_refuses_who_is_not_an_admin_of_the_project(ns, env, route, actor, refusal):
    rows = _action_rows(ns, env)
    before = (sorted((str(row.id), row.state) for row in bindings(env, ns)), len(env.recorded),
              sorted((str(row.id), row.state) for row in env.query(1, ns.Mandate)))
    payload, status = ACTION_CALLS[route](ns, env, actor, rows)
    assert status == 403 and refusal in (payload.get("error"), payload.get("code")), (route, actor, payload)
    assert (sorted((str(row.id), row.state) for row in bindings(env, ns)), len(env.recorded),
            sorted((str(row.id), row.state) for row in env.query(1, ns.Mandate))) == before


@pytest.mark.parametrize("route", sorted(ACTION_CALLS))
@pytest.mark.parametrize("actor", ["admin_b", "scoped_token"])
def test_every_lifecycle_action_is_open_to_the_projects_admins(ns, env, route, actor):
    rows = _action_rows(ns, env)
    payload, status = ACTION_CALLS[route](ns, env, actor, rows)
    assert status == 200, (route, actor, payload)


# --- resume of what a platform operator suspended (D-3) ----------------------------------------------------------

def _suspended_by_operator(ns, env):
    binding_id = str(env.add_binding(1, 7, 70, state="active", digest=DIGEST))
    payload, status = act(ns, env, binding_id, "suspend", "active", actor="platform_admin", mode=ADMINISTRATION)
    assert status == 200, payload
    return binding_id


@pytest.mark.parametrize("action, expected", [("resume", "suspended"), ("rebind", "suspended")])
def test_a_project_admin_cannot_lift_what_a_platform_operator_suspended(ns, env, action, expected):
    binding_id = _suspended_by_operator(ns, env)
    events = len(env.recorded)
    payload, status = act(ns, env, binding_id, action, expected, actor="admin_b")
    assert (status, payload["code"]) == (403, "RESUME_NEEDS_OPERATOR"), payload
    assert [row.state for row in bindings(env, ns)] == ["suspended"] and len(env.recorded) == events
    # administration mode keeps resume
    payload, status = act(ns, env, binding_id, "resume", "suspended", actor="platform_admin", mode=ADMINISTRATION)
    assert (status, payload["state"]) == (200, "active")


def test_a_project_admin_resumes_what_an_admin_of_the_project_suspended(ns, env):
    binding_id = str(env.add_binding(1, 7, 70, state="active", digest=DIGEST))
    assert act(ns, env, binding_id, "suspend", "active", actor="admin_a")[1] == 200
    payload, status = act(ns, env, binding_id, "resume", "suspended", actor="admin_b")
    assert (status, payload["state"]) == (200, "active")


def test_the_resume_rule_reads_the_membership_and_fails_closed(ns, env):
    binding_id = _suspended_by_operator(ns, env)
    env.membership.down = True
    payload, status = act(ns, env, binding_id, "resume", "suspended", actor="admin_b")
    assert (status, payload["code"]) == (503, "DEPENDENCY_UNAVAILABLE")
    assert [row.state for row in bindings(env, ns)] == ["suspended"]


def _mandates(env, ns):
    return sorted((str(row.id), row.state) for row in env.query(1, ns.Mandate))


def test_a_project_admin_cannot_resume_a_mandate_a_platform_operator_suspended(ns, env):
    """(S-8, D-3) Who suspended counts, not only who owns it: admin_b is neither the owner nor the creator."""
    mandate_id = _suspended_mandate(ns, env, PLATFORM)
    before = (_mandates(env, ns), len(env.recorded))
    payload, status = mandate_act(ns, env, mandate_id, "resume", actor="admin_b")
    assert (status, payload["code"]) == (403, "RESUME_NEEDS_OPERATOR"), payload
    assert (_mandates(env, ns), len(env.recorded)) == before
    payload, status = mandate_act(ns, env, mandate_id, "resume", actor="platform_admin", mode=ADMINISTRATION)
    assert (status, payload["state"]) == (200, "active"), payload


def test_a_project_admin_resumes_a_mandate_another_admin_of_the_project_suspended(ns, env):
    env.add_binding(1, 7, 70, state="active", digest=DIGEST)
    mandate_id = create_mandate(ns, env, actor="admin_a")[0]["id"]
    assert mandate_act(ns, env, mandate_id, "submit", actor="admin_a")[1] == 200
    assert mandate_act(ns, env, mandate_id, "approve", actor="admin_b", **APPROVAL)[1] == 200
    assert mandate_act(ns, env, mandate_id, "suspend", actor="admin_a")[1] == 200
    payload, status = mandate_act(ns, env, mandate_id, "resume", actor="admin_b")
    assert (status, payload["state"]) == (200, "active"), payload


def test_the_mandate_resume_rule_reads_the_membership_and_fails_closed(ns, env):
    mandate_id = _suspended_mandate(ns, env, ADMIN_A)
    env.membership.down = True
    payload, status = mandate_act(ns, env, mandate_id, "resume", actor="admin_b")
    assert (status, payload["code"]) == (503, "DEPENDENCY_UNAVAILABLE")
    assert [state for _, state in _mandates(env, ns)] == ["suspended"]


# --- a project mandate's owner is a member (N-11) ----------------------------------------------------------------

def test_a_project_mandate_for_an_owner_outside_the_project_is_refused_at_create(ns, env):
    env.add_binding(1, 7, 70, state="active", digest=DIGEST)
    body = {"version_id": 70, "owner_user_id": FOREIGN, "operations": ["run:schedule"], "max_runtime_s": 600,
            "valid_until": VALID_UNTIL}
    payload, status = hit(ns, env, "autonomous_mandates", "post", actor="admin_a", body=body)
    assert (status, payload["code"]) == (400, "OWNER_NOT_MEMBER") and env.query(1, ns.Mandate) == []
    assert create_mandate(ns, env, actor="admin_a")[1] == 200   # the editor, a member


def test_a_token_scoped_to_another_project_holds_its_owners_role_and_the_hook_refuses_the_project(ns, env):
    """The route's own check: a scoped token passes the gate as its owner (the auth hook already refused it with
    TOKEN_SCOPE_PROJECT for a project outside its scope, ``token_scope.decide``)."""
    assert ns.token_scope.decide({"projects": [2], "operations": ["admin"]},
                                 "api.v2.elitea_core.agent_identities", "GET", {"mode": "default"}, "", 1) \
        == "TOKEN_SCOPE_PROJECT"
    for endpoint in ("api.v2.elitea_core.agent_identities", "api.v2.elitea_core.agent_identity_action",
                     "api.v2.elitea_core.autonomous_mandates", "api.v2.elitea_core.autonomous_mandate_action",
                     "api.v2.elitea_core.governance_agents", "api.v2.elitea_core.governance_toolkits"):
        for method in ("GET", "POST"):   # every project-mode governance route is admin, its reads included
            assert ns.token_scope.decide({"projects": [1], "operations": ["run", "build"]}, endpoint, method,
                                         {"mode": "default"}, "", 1) == "TOKEN_SCOPE_OPERATION"


def test_a_platform_admin_keeps_administration_mode(ns, env):
    _routes(ns, env)
    assert hit(ns, env, "agent_identities", "get", actor="platform_admin", mode=ADMINISTRATION)[1] == 200
    assert hit(ns, env, "agent_identities", "get", actor="admin_a", mode=ADMINISTRATION)[1] == 403   # no platform role


# --- four-eyes with two project admins -------------------------------------------------------------------------

def test_two_project_admins_close_a_binding_and_its_creator_cannot(ns, env):
    binding_id = pending_binding(ns, env, "admin_a")
    payload, status = act(ns, env, binding_id, "activate", "pending_approval", actor="admin_a", **APPROVAL)
    assert (status, payload["code"]) == (403, "SELF_APPROVAL")
    payload, status = act(ns, env, binding_id, "activate", "pending_approval", actor="admin_b", **APPROVAL)
    assert (status, payload["state"]) == (200, "active")
    [row] = bindings(env, ns)
    assert (row.created_by, row.state_changed_by, row.approval_ref) == (ADMIN_A, ADMIN_B, "CHG-7269")
    events = [(event["reason_code"], event["caller_ref"]) for _, event in env.recorded]
    assert events[-1] == ("BINDING_ACTIVE", f"operator:{ADMIN_B}")


def test_the_approve_permission_is_asked_in_the_project_not_in_administration(ns, env):
    binding_id = pending_binding(ns, env, "admin_a")
    ns.permission_asks.clear()
    act(ns, env, binding_id, "activate", "pending_approval", actor="admin_b", **APPROVAL)
    assert ns.permission_asks == [(DEFAULT, 1)]


def test_two_project_admins_close_a_mandate_and_neither_its_creator_nor_its_owner_can(ns, env):
    env.add_binding(1, 7, 70, state="active", digest=DIGEST)
    payload, status = create_mandate(ns, env, actor="admin_a")
    assert status == 200, payload
    mandate_id = payload["id"]
    assert mandate_act(ns, env, mandate_id, "submit", actor="admin_a")[1] == 200
    assert mandate_act(ns, env, mandate_id, "approve", actor="admin_a", **APPROVAL)[0]["code"] == "SELF_APPROVAL"
    assert mandate_act(ns, env, mandate_id, "approve", actor="editor", **APPROVAL)[1] == 403   # the owner, a member
    payload, status = mandate_act(ns, env, mandate_id, "approve", actor="admin_b", **APPROVAL)
    assert (status, payload["state"]) == (200, "active")


def test_the_project_mandate_route_checks_the_mandate_permissions(ns, env):
    e = ns.endpoints
    assert e.autonomous_mandates.ProjectAPI.get.descriptor["permissions"] == ["governance.mandates.view"]
    assert e.autonomous_mandates.ProjectAPI.post.descriptor["permissions"] == ["governance.mandates.manage"]
    assert e.autonomous_mandate_action.ProjectAPI.post.descriptor["permissions"] == [
        "governance.mandates.manage", "governance.mandates.approve"]
    # administration mode keeps the binding permissions it always had
    assert e.autonomous_mandates.AdminAPI.get.descriptor["permissions"] == ["governance.bindings.view"]
    assert e.autonomous_mandate_action.AdminAPI.post.descriptor["permissions"] == [
        "governance.bindings.view", "governance.bindings.manage", "governance.bindings.approve"]


def test_the_consent_actions_stay_the_owners_in_prompt_lib(ns, env):
    env.add_binding(1, 7, 70, state="active", digest=DIGEST)
    mandate_id = create_mandate(ns, env, actor="admin_a")[0]["id"]
    for action in ("consent", "consent_preview", "withdraw_consent"):
        assert mandate_act(ns, env, mandate_id, action, actor="admin_b") == (
            {"error": "you are not allowed to do this", "code": "FORBIDDEN"}, 403)


# --- escalation ----------------------------------------------------------------------------------------------------

def test_a_binding_with_a_second_admin_is_not_escalated_and_its_list_says_so(ns, env):
    pending_binding(ns, env, "admin_a")
    page, status = hit(ns, env, "agent_identities", "get", actor="admin_b")
    assert status == 200 and [row["escalated"] for row in page["rows"]] == [False]
    assert env.membership.calls == [(1, True)]   # one membership read for the page, the system user left out


def test_the_only_admin_as_creator_escalates(ns, env):
    del env.membership.roles[1][ADMIN_B]
    pending_binding(ns, env, "admin_a")
    page, _ = hit(ns, env, "agent_identities", "get", actor="admin_a")
    assert [row["escalated"] for row in page["rows"]] == [True]


def test_a_project_without_an_admin_escalates_everything_waiting(ns, env):
    env.membership.roles[1] = {OWNER: ["editor"], VIEWER: ["viewer"]}   # a private project: no admin, by design
    env.add_binding(1, 7, 70, state="pending_approval", digest=DIGEST)
    env.add_binding(1, 7, 70, state="active", digest=DIGEST, mode="dedicated_agent")
    page, _ = hit(ns, env, "agent_identities", "get", actor="platform_admin", mode=ADMINISTRATION,
                  args={"escalated": "true"})
    assert [(row["state"], row["escalated"]) for row in page["rows"]] == [("pending_approval", True)]
    assert page["total"] == 1


def test_escalated_true_filters_in_sql_so_the_page_and_its_total_agree(ns, env):
    first = pending_binding(ns, env, "admin_a")   # admin_b may approve: not escalated
    env.add_version(1, 72, 7, "v3")
    env.digests[72] = DIGEST
    other = str(env.add_binding(1, 7, 72, state="pending_approval", digest=DIGEST))
    with ns.sa_orm.Session(env.engine(1)) as session:   # created by admin_a, last changed by admin_b: nobody left
        row = session.get(ns.models.AgentIdentityBinding, uuid.UUID(other))
        row.created_by, row.state_changed_by = ADMIN_A, ADMIN_B
        session.commit()
    page, _ = hit(ns, env, "agent_identities", "get", actor="platform_admin", mode=ADMINISTRATION,
                  args={"escalated": "true"})
    assert [row["id"] for row in page["rows"]] == [other] and page["total"] == 1
    page, _ = hit(ns, env, "agent_identities", "get", actor="admin_a", args={"escalated": "true"})
    assert [row["id"] for row in page["rows"]] == [other]
    assert first not in [row["id"] for row in page["rows"]]


def test_a_mandate_escalates_when_the_admins_are_its_creator_and_its_owner(ns, env):
    env.membership.roles[1] = {ADMIN_A: ["admin"], OWNER: ["admin", "editor"]}
    env.add_binding(1, 7, 70, state="active", digest=DIGEST)
    mandate_id = create_mandate(ns, env, actor="admin_a")[0]["id"]
    mandate_act(ns, env, mandate_id, "submit", actor="admin_a")
    page, _ = hit(ns, env, "autonomous_mandates", "get", actor="admin_a")
    assert [row["escalated"] for row in page["rows"]] == [True]
    page, _ = hit(ns, env, "autonomous_mandates", "get", actor="platform_admin", mode=ADMINISTRATION,
                  args={"escalated": "true"})
    assert [row["id"] for row in page["rows"]] == [mandate_id]
    env.membership.roles[1][ADMIN_B] = ["admin"]
    page, _ = hit(ns, env, "autonomous_mandates", "get", actor="admin_a")
    assert [row["escalated"] for row in page["rows"]] == [False]


def test_an_unreadable_membership_refuses_the_page_rather_than_marking_it_wrong(ns, env):
    env.membership.down = True
    assert hit(ns, env, "agent_identities", "get", actor="admin_a")[0]["code"] == "DEPENDENCY_UNAVAILABLE"


def test_the_escalation_rule(ns):
    inbox = ns.governance_inbox
    assert inbox.escalated(frozenset(), 3) is True
    assert inbox.escalated(frozenset({3}), 3, None) is True
    assert inbox.escalated(frozenset({3, 4}), 3) is False
    assert inbox.escalated(frozenset({3, 4}), 3, 4) is True


# --- administration mode is unchanged ----------------------------------------------------------------------------

def test_without_escalated_the_administration_list_reads_no_membership_and_has_no_new_field(ns, env):
    pending_binding(ns, env, "admin_a")
    page, status = hit(ns, env, "agent_identities", "get", actor="platform_admin", mode=ADMINISTRATION)
    assert status == 200 and "escalated" not in page["rows"][0] and env.membership.calls == []
    page, status = hit(ns, env, "autonomous_mandates", "get", actor="platform_admin", mode=ADMINISTRATION)
    assert status == 200 and env.membership.calls == []


def test_the_handlers_of_each_route(ns):
    for name in ENDPOINTS:
        handlers = getattr(ns.endpoints, name).API.mode_handlers
        assert handlers[ADMINISTRATION].__name__ == "AdminAPI" and handlers[DEFAULT].__name__ == "ProjectAPI"
    assert set(ns.endpoints.autonomous_mandates.API.mode_handlers) == {ADMINISTRATION, DEFAULT, "prompt_lib"}
    assert set(ns.endpoints.agent_identities.API.mode_handlers) == {ADMINISTRATION, DEFAULT}


# --- off and audit only: nothing is read --------------------------------------------------------------------------

@pytest.mark.parametrize("audit", [False, True])
@pytest.mark.parametrize("route", sorted(CALLS))
def test_off_and_audit_only_refuse_every_project_route_with_nothing_read(ns, env, route, audit):
    ids = _routes(ns, env)
    env.governed, env.audit = False, audit
    opened, calls = len(env.sessions), list(env.membership.calls)
    payload, status = CALLS[route](ns, env, "admin_a", ids)
    assert (status, payload["code"]) == (409, "PROJECT_GOVERNANCE_OFF")
    assert len(env.sessions) == opened and env.membership.calls == calls and env.token_scope_calls == []


def test_administration_mode_works_under_off_as_before(ns, env):
    env.governed = False
    payload, status = create_binding(ns, env, actor="platform_admin", mode=ADMINISTRATION)
    assert status == 200 and payload["state"] == "draft"


# --- registration ------------------------------------------------------------------------------------------------

def test_the_project_roles_and_the_request_permission(ns):
    api = ns.binding_api
    project = effective_roles(api.project_check_spec(api.VIEW))
    assert project["default"] == {"system": False, "super_admin": True, "admin": True, "editor": False,
                                  "viewer": False, "monitor": False}
    assert project["administration"] == effective_roles(api.check_spec(api.VIEW))["administration"]
    request = effective_roles({"permissions": [api.REQUEST], "recommended_roles": api.REQUEST_ROLES})
    assert {role for role, granted in request["default"].items() if granted} == {"admin", "editor"}
    assert not any(request["administration"].values())


def test_the_module_registers_the_request_permission():
    text = (PLUGIN_ROOT / "module.py").read_text()
    assert '{"permissions": [BINDING_REQUEST], "recommended_roles": BINDING_REQUEST_ROLES}' in text
    assert text.index("BINDING_REQUEST_ROLES}") < text.index("self.task_node.start()")


def stored_operations(ns, env):
    """The binding's ``allowed_platform_operations`` as stored (a deferred column, read on its own)."""
    model = ns.models.AgentIdentityBinding
    with ns.sa_orm.Session(env.engine(1)) as session:
        return session.execute(ns.sa.select(model.allowed_platform_operations)).scalar_one()


# --- #7228 fix round 2: the operations an approval keeps -------------------------------------------------------

def proposed_binding(ns, env, operations):
    body, status = hit(ns, env, "agent_identities", "post", actor="admin_a", mode=DEFAULT,
                       body={"application_id": 7, "version_id": 70, "identity_mode": "elitea_native",
                             "platform_operations": operations})
    assert status == 200, body
    assert act(ns, env, body["id"], "submit", "draft", actor="admin_a")[1] == 200
    return body["id"]


def test_an_approval_keeps_part_of_the_proposed_operations_and_the_run_token_gets_that(ns, env):
    binding_id = proposed_binding(ns, env, ["read", "build", "credentials", "run"])
    payload, status = act(ns, env, binding_id, "activate", "pending_approval", actor="admin_b",
                          platform_operations=["read", "run"], **APPROVAL)
    assert (status, payload["state"]) == (200, "active"), payload
    assert stored_operations(ns, env) == ["read", "run"]
    [row] = bindings(env, ns)
    assert ns.internal_obo.binding_operations(1, str(row.id)) == ("read", "run")   # what a run token is minted from
    listed, _ = hit(ns, env, "agent_identities", "get", actor="admin_b", mode=DEFAULT)
    assert listed["rows"][0]["platform_operations"] == ["read", "run"]


def test_an_approval_without_operations_keeps_the_proposal(ns, env):
    binding_id = proposed_binding(ns, env, ["read", "build", "credentials", "run"])
    assert act(ns, env, binding_id, "activate", "pending_approval", actor="admin_b", **APPROVAL)[1] == 200
    assert stored_operations(ns, env) == ["read", "build", "credentials", "run"]


@pytest.mark.parametrize("proposed, kept", [(["read", "build"], ["read", "credentials"]), (None, ["read", "build"])])
def test_an_approval_never_widens_what_was_proposed(ns, env, proposed, kept):
    binding_id = proposed_binding(ns, env, proposed)
    payload, status = act(ns, env, binding_id, "activate", "pending_approval", actor="admin_b",
                          platform_operations=kept, **APPROVAL)
    assert (status, payload["code"]) == (400, "INVALID_REQUEST")
    [row] = bindings(env, ns)
    assert row.state == "pending_approval"


@pytest.mark.parametrize("action, expected", [("suspend", "active"), ("submit", "draft")])
def test_operations_belong_to_an_activation_only(ns, env, action, expected):
    body, _ = create_binding(ns, env)
    payload, status = act(ns, env, body["id"], action, expected, actor="admin_a", platform_operations=["read"])
    assert (status, payload["code"]) == (400, "INVALID_REQUEST")


def test_an_unknown_operation_is_refused_by_the_body(ns, env):
    binding_id = proposed_binding(ns, env, ["read"])
    payload, status = act(ns, env, binding_id, "activate", "pending_approval", actor="admin_b",
                          platform_operations=["admin"], **APPROVAL)
    assert status == 400
