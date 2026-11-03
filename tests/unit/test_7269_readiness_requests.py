"""C-1009 #7269 phase 2: readiness (what an entity still needs to run governed, for one person), the member's batch
request, what a governed run creates through the internal MCP, and the MCP server's exposed version (#7341).

Readiness, the request batch and the binding and mandate lifecycles run for real on SQLite (``fixtures/admission_env.py``,
``mandate_env.py``); the configuration and credential facts are the harness's fakes, and so is the secrets plugin's
``file_credential_requests`` (its real code is tested in the secrets suite): here it records what it was asked, in which
session, and answers ids.
"""
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
from fixtures.governed_credentials import Credential  # noqa: E402  pylint: disable=C0413
from fixtures.helpers import load_module_with_stubs  # noqa: E402  pylint: disable=C0413

PKG = admission_env.PKG
DIGEST = "adv1:" + "1" * 64
MEMBER, VIEWER, OTHER = 21, 22, 23
OWNER = mandate_env.OWNER
TID = "tid-1"
REQUEST = "governance.request"
EDIT = "models.applications.version.update"
MEMBER_RIGHTS = {REQUEST, EDIT, "credentials.use", "models.applications.version.details"}


@pytest.fixture(scope="module")
def ns(tmp_path_factory):
    saved = dict(sys.modules)
    ns = mandate_env.install(PLUGIN_ROOT, tmp_path_factory.mktemp("readiness"))

    class Application(ns.Base):
        __tablename__ = "applications"
        __table_args__ = {"schema": "tenant"}
        id = ns.sa.Column(ns.sa.Integer, primary_key=True)
        name = ns.sa.Column(ns.sa.String, nullable=False)

    ns.Application = Application
    sys.modules[f"{PKG}.models.all"].Application = Application

    def load(name):
        return load_module_with_stubs(PLUGIN_ROOT / "utils" / f"{name}.py", f"{PKG}.utils.{name}")

    for name in ("constants", "schedule_risk"):
        try:
            load(name)
        except Exception:  # pylint: disable=W0703   the risk scan needs the application tools; stubbed below
            pass
    try:   # the real schedule model, on the harness's base (``::jsonb`` defaults are not SQLite)
        models = sys.modules[f"{PKG}.models"]
        for name in ("config", "db", "db_tools"):
            if not hasattr(models, name):
                setattr(models, name, getattr(ns.tools, name))
        load_module_with_stubs(PLUGIN_ROOT / "models" / "enums" / "all.py", f"{PKG}.models.enums.all")
        ns.EntitySchedule = load_module_with_stubs(PLUGIN_ROOT / "models" / "entity_schedule.py",
                                                   f"{PKG}.models.entity_schedule").EntitySchedule
        ns.EntitySchedule.__table__.c.variables.server_default = None
    except Exception:  # pylint: disable=W0703
        ns.EntitySchedule = None
    ns.mcp_exposed_version = load("mcp_exposed_version")
    ns.governance_readiness = load("governance_readiness")
    ns.governance_requests = load("governance_requests")
    # the two routes, behind a check_api that grants the caller's own permission set (any of the named ones)
    tools = ns.tools
    ns.request, ns.actor = FakeRequest(), "member"
    ns.flask.request = ns.request

    def check_api(descriptor):
        def decorator(func):
            def wrapper(*args, **kwargs):
                if not set(descriptor["permissions"]) & permissions_of(ns.actor):
                    return {"ok": False, "error": "access_denied"}, 403
                return func(*args, **kwargs)
            wrapper.descriptor = descriptor
            return wrapper
        return decorator

    tools.auth.decorators = types.SimpleNamespace(check_api=check_api)
    tools.auth.current_user = lambda: {"id": CALLERS[ns.actor][0]} if CALLERS[ns.actor][0] else {}
    tools.auth.resolve_permissions = lambda mode, project_id=None: permissions_of(ns.actor) if project_id == 1 else set()
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
        for name in ("governance_readiness", "governance_requests")})
    yield ns
    sys.modules.clear()
    sys.modules.update(saved)


class FakeRequest:
    args, content_length, body, max_content_length = {}, None, None, None

    def get_data(self):
        return json.dumps(self.body).encode()

    def get_json(self, silent=False):   # pylint: disable=W0613
        return self.body


# who calls the routes: (user id, their permissions in project 1, a token id or None)
CALLERS = {
    "member": (MEMBER, MEMBER_RIGHTS, None),
    "viewer": (VIEWER, {"credentials.use", "models.applications.version.details"}, None),
    "foreign": (OTHER, set(), None),   # a member of another project only
    "scoped_token": (MEMBER, MEMBER_RIGHTS, 901),
    "unscoped_token": (MEMBER, MEMBER_RIGHTS, 902),
    "anonymous": (None, set(), None),
}


def permissions_of(actor):
    return set(CALLERS[actor][1])


class SecretsFake:
    """``file_credential_requests`` of the secrets plugin: what it was asked and in which session."""

    def __init__(self):
        self.calls, self.refuse = [], None

    def file_credential_requests(self, session, project_id, items, actor):
        self.calls.append((type(session).__name__, project_id, [dict(item) for item in items], actor))
        if self.refuse and any(item["kind"] == self.refuse for item in items):
            error = RuntimeError("refused")
            error.code, error.key = "GRANT_EXISTS", items[0]["key"]
            raise error
        return [{"key": item["key"], "id": str(uuid.uuid4())} for item in items]


@pytest.fixture
def env(ns, monkeypatch):
    env = mandate_env.prepare(ns, ns.env)
    ns.mandate_admin._now = lambda: datetime.now(timezone.utc)  # pylint: disable=W0212
    ns.mandate_state._now = lambda: datetime.now(timezone.utc)  # pylint: disable=W0212
    env.add_application(1, 7, "Jira helper")
    env.add_version(1, 70, 7, "base")
    env.digests[70] = DIGEST
    monkeypatch.setattr(ns.governance_readiness, "digest_for_version", lambda session, version_id, loaded=None:
                        env.digests[version_id])
    for user_id in (MEMBER, VIEWER, OTHER, OWNER):
        env.user_rows[user_id] = {"id": user_id, "idp_issuer": "https://issuer.test", "idp_tid": TID,
                                  "idp_oid": f"oid-{user_id}"}
        ns.users[user_id] = env.user_rows[user_id]
    env.permissions[MEMBER] = set(MEMBER_RIGHTS)
    env.permissions[OWNER] = set(MEMBER_RIGHTS)
    env.permissions[VIEWER] = {"credentials.use", "models.applications.version.details"}
    env.facts_version = 5
    env.secrets = SecretsFake()
    env.secrets_extra = {"file_credential_requests": env.secrets.file_credential_requests}
    monkeypatch.setattr(ns.governance_readiness, "scan_schedule", None, raising=False)
    env.token_scopes[901] = {"user_id": MEMBER, "projects": [1], "operations": ["run", "admin"]}

    def holder():
        user_id, _, token = CALLERS[ns.actor]
        if token is not None:
            return types.SimpleNamespace(type="token", id=token, reference="-")
        return types.SimpleNamespace(type="user" if user_id else "public", id=user_id or "-", reference="s")

    monkeypatch.setattr(ns.credential_probe, "_session_holder", holder)
    return env


def module(ns, governed=True, audit=False):
    built = ns.env.module(governed=governed, audit=audit)
    built.descriptor = types.SimpleNamespace(config={"mandate_identity_sources": ["owner_delegated"]})
    return built


def credential(env, binding=None, **over):
    """A toolkit of version 70 that selects a stored credential (no reference, no grant unless asked)."""
    return Credential(env, binding or uuid.uuid4(), reference=over.pop("reference", False),
                      grants=over.pop("grants", False), **over)


def items(ns, env, user=MEMBER, kind="version", entity=70, **kw):
    found = ns.governance_readiness.readiness(module(ns, **kw), 1, kind, entity, user)
    return {item["key"]: item for item in found.items}


def subject(user):
    return f"idp:{TID}:oid-{user}"


# --- readiness ---------------------------------------------------------------------------------------------------

@pytest.mark.parametrize("governed, audit", [(False, False), (False, True)])
def test_off_and_audit_only_answer_nothing_with_nothing_read(ns, env, governed, audit):
    credential(env)
    found = ns.governance_readiness.readiness(module(ns, governed, audit), 1, "version", 70, MEMBER)
    assert found.items == [] and env.sessions == [] and env.configuration_calls == [] and env.secrets_calls == []
    assert env.permission_calls == []


def test_a_new_agent_needs_its_binding_a_reference_and_a_grant_all_requestable_by_its_editor(ns, env):
    cred = credential(env)
    found = items(ns, env)
    assert set(found) == {"binding", f"reference:{cred.uuid}", f"grant:1:{cred.uuid}"}
    assert found["binding"] | {"ref": None} == {"key": "binding", "kind": "binding", "ref": None, "title": "Jira helper base",
                                                "state": "missing", "requestable": True, "for": "shared"}
    assert (found[f"reference:{cred.uuid}"]["state"], found[f"reference:{cred.uuid}"]["for"]) == ("missing", "me")
    assert found[f"grant:1:{cred.uuid}"]["requestable"] is True and found[f"grant:1:{cred.uuid}"]["for"] == "me"
    assert env.recorded == [] and env.denied == []   # no audit row, no decision


def test_a_member_without_the_request_permission_sees_the_same_items_none_requestable(ns, env):
    credential(env)
    found = items(ns, env, user=VIEWER)
    assert {key: item["requestable"] for key, item in found.items()} == {key: False for key in found}


def test_an_active_binding_and_reference_leave_only_the_callers_grant(ns, env):
    binding = env.add_binding(1, 7, 70, digest=DIGEST)
    cred = credential(env, binding, reference=True)
    found = items(ns, env)
    assert list(found) == [f"grant:1:{cred.uuid}"]
    # another member has their own grant: their card is empty, the first member's still names theirs
    env.grants[(1, cred.key(subject(OTHER)))] = {"grant_id": "g", "state": "active", "valid_from": None,
                                                 "valid_until": None}
    env.permissions[OTHER] = set(MEMBER_RIGHTS)
    assert items(ns, env, user=OTHER) == {}
    assert list(items(ns, env)) == [f"grant:1:{cred.uuid}"]


def test_a_grant_waiting_for_approval_is_shown_and_not_requested_again(ns, env):
    binding = env.add_binding(1, 7, 70, digest=DIGEST)
    cred = credential(env, binding, reference=True)
    env.grants[(1, cred.key(subject(MEMBER)))] = {"grant_id": "g1", "state": "pending_approval", "valid_from": None,
                                                  "valid_until": None}
    grant = items(ns, env)[f"grant:1:{cred.uuid}"]
    assert (grant["state"], grant["requestable"], grant["ref"]) == ("pending_approval", False, "g1")


def test_a_changed_agent_asks_for_its_binding_again(ns, env):
    env.add_binding(1, 7, 70, digest="adv1:" + "9" * 64)
    found = items(ns, env)
    assert (found["binding"]["state"], found["binding"]["requestable"]) == ("changed", True)


def test_an_inactive_reference_is_shown_by_its_state(ns, env):
    binding = env.add_binding(1, 7, 70, digest=DIGEST)
    cred = credential(env, binding, reference=True, ref_state="validated")
    found = items(ns, env)
    assert (found[f"reference:{cred.uuid}"]["state"], found[f"reference:{cred.uuid}"]["requestable"]) == (
        "validated", False)


def test_a_person_with_no_linked_sign_in_can_request_nothing(ns, env):
    credential(env)
    env.user_rows[MEMBER] = ns.users[MEMBER] = {"id": MEMBER}
    found = items(ns, env)
    assert found["identity"]["state"] == "unlinked"
    assert not any(item["requestable"] for item in found.values())


def test_a_toolkit_needs_its_reference_for_its_tester(ns, env):
    cred = credential(env, version_id=70)
    found = items(ns, env, kind="toolkit", entity=1)
    assert list(found) == [f"reference:{cred.uuid}"] and found[f"reference:{cred.uuid}"]["requestable"] is True


def test_an_unknown_entity_is_not_found(ns, env):
    with pytest.raises(ns.governance_readiness.EntityNotFound):
        items(ns, env, entity=999)
    with pytest.raises(ns.governance_readiness.EntityNotFound):
        items(ns, env, kind="toolkit", entity=999)


# --- the member's batch ------------------------------------------------------------------------------------------

def file(ns, env, user=MEMBER, keys=None, **kw):
    return ns.governance_requests.file_requests(module(ns), 1, "version", 70, user, keys=keys, **kw)


def bindings(env, ns):
    return env.query(1, ns.models.AgentIdentityBinding)


def test_one_batch_files_the_binding_the_reference_and_the_grant_as_the_members_own_requests(ns, env):
    cred = credential(env)
    filed = file(ns, env)
    assert [item["kind"] for item in filed] == ["binding", "reference", "grant"]
    [row] = bindings(env, ns)
    assert (row.state, row.created_by, row.state_changed_by, row.identity_mode) == (
        "pending_approval", MEMBER, MEMBER, "elitea_native")
    # the secrets plugin got the batch's own session (whose commit is a flush), the reference first, then the grant
    assert [(session, items[0]["kind"]) for session, _, items, _ in env.secrets.calls] == [
        ("_OneTransaction", "reference"), ("_OneTransaction", "grant")]
    grant = env.secrets.calls[1][2][0]
    assert (grant["binding_ref"], grant["subject_ref"], grant["subject_user_id"], grant["toolkit_id"]) == (
        str(row.id), subject(MEMBER), MEMBER, 1)
    assert grant["ref"] == filed[1]["id"] and env.secrets.calls[0][2][0]["configuration_uuid"] == cred.uuid
    events = [(event["reason_code"], event.get("caller_ref")) for _, event in env.recorded]
    assert events.count(("GOVERNANCE_REQUEST_FILED", f"user:{MEMBER}")) == 3
    assert ("BINDING_PENDING_APPROVAL", f"operator:{MEMBER}") in events
    # every event in the one transaction: the batch's session, or the lifecycle's view of it
    assert len({id(getattr(session, "_session", session)) for session, _ in env.recorded}) == 1
    batch = getattr(env.recorded[0][0], "_session", env.recorded[0][0])
    assert batch.commits == 1   # one commit for the whole batch
    assert not any(event["reason_code"] in ("BINDING_ACTIVE", "GRANT_ACTIVE") for _, event in env.recorded)


def test_the_batch_is_all_or_nothing(ns, env):
    credential(env)
    env.secrets.refuse = "grant"
    with pytest.raises(ns.governance_requests.RequestRefused) as refused:
        file(ns, env)
    assert refused.value.code == "GRANT_EXISTS"
    assert bindings(env, ns) == []   # the binding made first is rolled back with it


def test_a_key_that_is_not_requestable_now_refuses_the_batch_with_nothing_written(ns, env):
    credential(env)
    with pytest.raises(ns.governance_requests.RequestRefused) as refused:
        file(ns, env, keys=["binding", "mandate"])
    assert (refused.value.code, refused.value.key) == ("STALE_READINESS", "mandate")
    assert bindings(env, ns) == [] and env.secrets.calls == [] and env.recorded == []


def test_a_member_without_the_request_permission_files_nothing(ns, env):
    credential(env)
    assert file(ns, env, user=VIEWER) == []
    with pytest.raises(ns.governance_requests.RequestRefused):
        file(ns, env, user=VIEWER, keys=["binding"])
    assert bindings(env, ns) == []


def test_a_request_never_approves_and_the_requester_cannot_approve_it(ns, env):
    credential(env)
    file(ns, env, keys=["binding"])
    [row] = bindings(env, ns)
    values = {"project_id": 1, "application_id": 7, "version_id": 70, "mode": "elitea_native", "binding_id": row.id,
              "action": "activate", "approval_ref": "A", "policy_revision": "B", "dry_run": False}
    with pytest.raises(ns.binding_admin.BindingActionError) as refused:
        ns.binding_admin.run_binding_action(module(ns), values, MEMBER)
    assert refused.value.code == "SELF_APPROVAL"


def test_a_changed_agent_is_rebound_by_its_editor(ns, env):
    binding = env.add_binding(1, 7, 70, digest="adv1:" + "9" * 64)
    file(ns, env, keys=["binding"])
    [row] = bindings(env, ns)
    assert (row.id, row.state, row.definition_digest, row.approval_ref) == (binding, "pending_approval", DIGEST, None)


def test_off_refuses_the_batch(ns, env):
    credential(env)
    with pytest.raises(ns.governance_requests.RequestRefused) as refused:
        ns.governance_requests.file_requests(module(ns, governed=False), 1, "version", 70, MEMBER)
    assert refused.value.code == "PROJECT_GOVERNANCE_OFF" and env.sessions == []


# --- what a governed run creates through the internal MCP --------------------------------------------------------

def test_what_a_run_created_is_requested_for_its_user_with_the_run_as_origin(ns, env):
    credential(env)
    claims = {"uid": MEMBER, "run": "run-77", "sub": subject(MEMBER), "act": "binding-of-the-agent"}
    answer = {"version_details": {"id": 70}}
    result = ns.governance_requests.file_for_run(module(ns), claims, "POST", "/api/v2/elitea_core/applications/{mode}/{project_id}",
                                                 "1", answer)
    assert [item["kind"] for item in result["filed"]] == ["binding", "reference", "grant"]
    [row] = bindings(env, ns)
    assert row.created_by == MEMBER and row.state == "pending_approval"   # the user's, never the agent's
    filed = [event for _, event in env.recorded if event["reason_code"] == "GOVERNANCE_REQUEST_FILED"]
    assert {(event["caller_ref"], event["run_id"], event["user_ref"]) for event in filed} == {
        ("run:run-77", "run-77", subject(MEMBER))}


def test_other_internal_mcp_calls_and_off_file_nothing(ns, env):
    claims = {"uid": MEMBER, "run": "r"}
    assert ns.governance_requests.file_for_run(module(ns), claims, "PUT", "elitea_core/tool/{mode}/{project_id}/{tool_id}",
                                               1, {}) is None
    assert ns.governance_requests.file_for_run(module(ns, governed=False), claims, "POST",
                                               "elitea_core/applications/{mode}/{project_id}", 1,
                                               {"version_details": {"id": 70}}) is None
    assert env.recorded == []


def test_a_credential_a_run_created_is_requested_as_its_users_personal_reference(ns, env):
    cred = credential(env)
    claims = {"uid": MEMBER, "run": "run-9"}
    result = ns.governance_requests.file_for_run(module(ns), claims, "POST", "configurations/configurations/{project_id}",
                                                 1, {"uuid": cred.uuid})
    assert [item["kind"] for item in result["filed"]] == ["reference"]
    [(session, _, [asked], actor)] = env.secrets.calls
    assert (asked["kind"], asked["configuration_uuid"], actor) == ("reference", cred.uuid, MEMBER)


# --- the MCP server's exposed version (#7341) -------------------------------------------------------------------

def _version(version_id, status, day):
    return types.SimpleNamespace(id=version_id, status=status, created_at=datetime(2026, 11, day))


@pytest.mark.parametrize("governed, bound, expected", [
    (True, {2}, (2, "bound")), (True, {1, 3}, (3, "bound")), (True, {1}, (1, "bound")),
    (True, set(), (3, "published")),
    # (D-1) off and audit only: the first stored version, exactly as before (no rule, nothing bound read)
    (False, {2}, (1, "stored")), (False, set(), (1, "stored")),
])
def test_the_exposed_version_rule(ns, governed, bound, expected):
    versions = [_version(1, "draft", 1), _version(2, "draft", 2), _version(3, "published", 3)]
    version, rule = ns.mcp_exposed_version.exposed_version(versions, bound, governed)
    assert (version.id, rule) == expected


def test_with_no_published_version_the_first_stored_one_as_before(ns):
    versions = [_version(5, "draft", 3), _version(4, "draft", 1)]
    assert ns.mcp_exposed_version.exposed_version(versions, set(), False) == (versions[0], "stored")
    assert ns.mcp_exposed_version.exposed_version([], set(), True) == (None, None)


# --- a schedule: its owner's mandate, the mandate's grants, the owner's consent -----------------------------------

def schedule(ns, env, owner=OWNER):
    if ns.EntitySchedule is None:
        pytest.skip("the schedule model could not be loaded in this harness")
    with ns.sa_orm.Session(env.engine(1)) as session:
        row = ns.EntitySchedule(entity_type="agent", application_id=7, version_id=70, name="Nightly", cron="0 9 * * *",
                                timezone="Etc/UTC", prompt="Say hi", variables=[], enabled=True, created_by=owner,
                                principal={"kind": "delegation", "ref": owner})
        session.add(row)
        session.commit()
        return row.id


def no_risks(monkeypatch, ns):
    fake = types.ModuleType(f"{PKG}.utils.schedule_risk")
    fake.scan_schedule = lambda project_id, version_id, owner_id: {"blocking": [], "sensitive": [], "notices": []}
    monkeypatch.setitem(sys.modules, f"{PKG}.utils.schedule_risk", fake)


def test_a_schedule_needs_its_owners_mandate_and_the_mandates_grants(ns, env, monkeypatch):
    no_risks(monkeypatch, ns)
    binding = env.add_binding(1, 7, 70, digest=DIGEST)
    cred = credential(env, binding, reference=True)
    schedule_id = schedule(ns, env)
    found = items(ns, env, user=OWNER, kind="schedule", entity=schedule_id)
    assert (found["mandate"]["state"], found["mandate"]["requestable"]) == ("missing", True)
    assert found[f"grant:1:{cred.uuid}"]["requestable"] is True and "you" not in found[f"grant:1:{cred.uuid}"]["title"]
    filed = ns.governance_requests.file_requests(module(ns), 1, "schedule", schedule_id, OWNER)
    assert [item["kind"] for item in filed] == ["mandate", "grant"]
    [mandate] = env.query(1, ns.Mandate)
    assert (mandate.state, mandate.owner_user_id, mandate.created_by) == ("pending_approval", OWNER, OWNER)
    [(_, _, [grant], _)] = env.secrets.calls
    assert grant["subject_ref"] == f"mandate:{mandate.id}" and grant["subject_user_id"] is None


def test_a_schedule_is_its_owners_and_its_mandate_waits_for_an_active_binding(ns, env, monkeypatch):
    no_risks(monkeypatch, ns)
    schedule_id = schedule(ns, env)
    with pytest.raises(ns.governance_readiness.EntityNotFound):
        items(ns, env, user=MEMBER, kind="schedule", entity=schedule_id)
    found = items(ns, env, user=OWNER, kind="schedule", entity=schedule_id)
    assert found["binding"]["requestable"] is True and found["mandate"]["requestable"] is False


# --- the two routes ---------------------------------------------------------------------------------------------

def route(ns, env, name, method, actor="member", body=None, governed=True, **path):
    ns.actor, ns.request.body, ns.request.args = actor, body, {}
    ns.request.content_length = None if body is None else len(json.dumps(body))
    ns.request.max_content_length = None
    handler = getattr(ns.endpoints, name).PromptLibAPI(module(ns, governed))
    payload, status = getattr(handler, method)(project_id=1, **path)
    json.dumps(payload)
    return payload, status


def readiness_of(ns, env, actor="member", **kw):
    return route(ns, env, "governance_readiness", "get", actor=actor, entity_kind="version", entity_id=70, **kw)


def request_for(ns, env, actor="member", governed=True, **body):
    return route(ns, env, "governance_requests", "post", actor=actor, governed=governed,
                 body={"entity_kind": "version", "entity_id": 70, **body})


@pytest.mark.parametrize("actor, expected", [
    ("member", 200), ("viewer", 200), ("scoped_token", 200), ("foreign", "access_denied"),
    ("unscoped_token", "TOKEN_SCOPE_OPERATION"), ("anonymous", "access_denied"),
])
def test_who_may_read_readiness(ns, env, actor, expected):
    credential(env)
    payload, status = readiness_of(ns, env, actor)
    if expected == 200:
        assert status == 200 and payload["items"]
    else:
        assert status == 403 and expected in (payload.get("error"), payload.get("code"))


@pytest.mark.parametrize("actor, expected", [
    ("member", 200), ("scoped_token", 200), ("viewer", "access_denied"), ("foreign", "access_denied"),
    ("unscoped_token", "TOKEN_SCOPE_OPERATION"), ("anonymous", "access_denied"),
])
def test_who_may_file_a_request(ns, env, actor, expected):
    credential(env)
    payload, status = request_for(ns, env, actor)
    if expected == 200:
        assert status == 200 and [item["kind"] for item in payload["filed"]] == ["binding", "reference", "grant"]
        assert {item["state"] for item in payload["items"]} >= {"pending_approval"}
    else:
        assert status == 403 and expected in (payload.get("error"), payload.get("code"))
        assert bindings(env, ns) == []


def test_off_readiness_is_empty_with_nothing_read_and_a_request_is_refused(ns, env):
    credential(env)
    assert readiness_of(ns, env, governed=False) == ({"items": []}, 200)
    assert env.sessions == [] and env.token_scope_calls == []
    payload, status = request_for(ns, env, governed=False)
    assert (status, payload["code"]) == (409, "PROJECT_GOVERNANCE_OFF") and env.sessions == []


def test_a_stale_or_malformed_request_is_refused_with_nothing_written(ns, env):
    credential(env)
    payload, status = request_for(ns, env, keys=["mandate"])
    assert (status, payload["code"], payload["key"]) == (409, "STALE_READINESS", "mandate")
    payload, status = request_for(ns, env, approve=True)
    assert (status, payload["code"]) == (400, "INVALID_REQUEST")
    assert bindings(env, ns) == [] and env.recorded == []


def test_an_entity_of_another_kind_or_none_is_refused(ns, env):
    payload, status = route(ns, env, "governance_readiness", "get", entity_kind="mandate", entity_id=1)
    assert (status, payload["code"]) == (400, "INVALID_REQUEST")
    payload, status = route(ns, env, "governance_readiness", "get", entity_kind="version", entity_id=999)
    assert (status, payload["code"]) == (404, "NOT_FOUND")


def test_a_scoped_token_reads_readiness_with_any_operation_and_requests_only_with_admin(ns):
    scope = {"projects": [1], "operations": ["run"]}
    assert ns.token_scope.decide(scope, "api.v2.elitea_core.governance_readiness", "GET", {"mode": "prompt_lib"}, "", 1) \
        is None
    assert ns.token_scope.decide(scope, "api.v2.elitea_core.governance_requests", "POST", {"mode": "prompt_lib"}, "", 1) \
        == "TOKEN_SCOPE_OPERATION"
    assert ns.token_scope.decide({"projects": [1], "operations": ["admin"]}, "api.v2.elitea_core.governance_requests",
                                 "POST", {"mode": "prompt_lib"}, "", 1) is None


@pytest.mark.parametrize("admins, expected", [(frozenset({MEMBER}), True), (frozenset(), True),
                                              (frozenset({MEMBER, 31}), False)])
def test_a_waiting_item_says_whether_it_needs_a_platform_operator(ns, env, monkeypatch, admins, expected):
    binding = env.add_binding(1, 7, 70, digest=DIGEST)
    cred = credential(env, binding, reference=True)
    env.grants[(1, cred.key(subject(MEMBER)))] = {"grant_id": "g1", "state": "pending_approval", "valid_from": None,
                                                  "valid_until": None}
    monkeypatch.setattr(ns.governance_inbox, "project_admins", lambda project_id: admins)
    assert items(ns, env)[f"grant:1:{cred.uuid}"]["escalated"] is expected


def test_nothing_waiting_reads_no_membership(ns, env, monkeypatch):
    credential(env)
    monkeypatch.setattr(ns.governance_inbox, "project_admins", lambda project_id: pytest.fail("read"))
    assert all("escalated" not in item for item in items(ns, env).values())


def test_the_facts_are_asked_with_well_formed_keys_when_there_is_no_binding_or_no_sign_in(ns, env):
    """The secrets facts refuse a malformed binding id or an empty subject (ValueError): readiness never sends one."""
    credential(env)
    items(ns, env)
    env.user_rows[MEMBER] = ns.users[MEMBER] = {"id": MEMBER}
    items(ns, env)
    items(ns, env, kind="toolkit", entity=1)
    for asked in env.grants_for_calls:
        for binding, _toolkit, _config, _target, subject in asked or ():
            uuid.UUID(str(binding))
            assert subject
    for asked in env.subject_grant_calls:
        assert all(subject for _credential, subject in asked or ())


# --- fix round (review of 2026-11-03) ------------------------------------------------------------------------------

def _set_binding_state(ns, env, binding_id, state):
    with ns.sa_orm.Session(env.engine(1)) as session:
        session.get(ns.models.AgentIdentityBinding, binding_id).state = state
        session.commit()


def test_a_suspended_binding_stays_suspended_whatever_its_definition_and_a_member_cannot_lift_it(ns, env):
    """(S-1 b) A member's rebind never lifts a suspension: readiness says suspended, not requestable, and a request
    naming the binding is stale with the row unchanged."""
    changed = "adv1:" + "9" * 64
    binding = env.add_binding(1, 7, 70, state="suspended", digest=changed)
    found = items(ns, env)
    assert (found["binding"]["state"], found["binding"]["requestable"]) == ("suspended", False)
    with pytest.raises(ns.governance_requests.RequestRefused) as refused:
        file(ns, env, keys=["binding"])
    assert (refused.value.code, refused.value.key) == ("STALE_READINESS", "binding")
    [row] = bindings(env, ns)
    assert (row.id, row.state, row.definition_digest) == (binding, "suspended", changed) and env.recorded == []


def test_a_binding_suspended_after_readiness_is_not_rebound(ns, env, monkeypatch):
    binding = env.add_binding(1, 7, 70, digest="adv1:" + "9" * 64)
    real = ns.governance_requests.readiness

    def then_suspended(*args, **kwargs):
        found = real(*args, **kwargs)   # "changed": requestable
        _set_binding_state(ns, env, binding, "suspended")   # an operator suspends it meanwhile
        return found

    monkeypatch.setattr(ns.governance_requests, "readiness", then_suspended)
    with pytest.raises(ns.governance_requests.RequestRefused) as refused:
        file(ns, env, keys=["binding"])
    assert refused.value.code == "CONCURRENT_CHANGE"
    [row] = bindings(env, ns)
    assert row.state == "suspended" and env.recorded == []


def test_another_members_live_mandate_on_the_version_is_said_and_not_offered(ns, env, monkeypatch):
    """(S-3) One live mandate per version: while another member's holds it, nothing is requestable for this schedule,
    and the item says so without naming the holder."""
    no_risks(monkeypatch, ns)
    binding = env.add_binding(1, 7, 70, digest=DIGEST)
    cred = credential(env, binding, reference=True)
    mandate_env.add_mandate(env, ns, owner=MEMBER, state="active", binding_ref=binding,
                            valid_until=datetime.now(timezone.utc) + timedelta(days=30))
    schedule_id = schedule(ns, env, owner=OWNER)
    found = items(ns, env, user=OWNER, kind="schedule", entity=schedule_id)
    assert (found["mandate"]["state"], found["mandate"]["requestable"], found["mandate"]["ref"]) == (
        "held_by_another", False, None)
    assert found[f"grant:1:{cred.uuid}"]["requestable"] is False
    assert str(MEMBER) not in json.dumps(found["mandate"])
    assert ns.governance_requests.file_requests(module(ns), 1, "schedule", schedule_id, OWNER) == []


def test_the_mandate_exists_refusal_reads_as_such(ns):
    exc = ns.governance_requests.RequestRefused("MANDATE_EXISTS")
    body, status = ns.endpoints.governance_requests._refused(exc)  # pylint: disable=W0212
    assert status == 409 and "Another member's schedule" in body["error"]


def test_a_second_submit_of_the_same_batch_files_nothing_more(ns, env, monkeypatch):
    """(S-7 e) Two submits of one readiness: the second finds the first's rows and is refused, nothing duplicated."""
    credential(env)
    real = ns.governance_requests.readiness
    calls = []

    def first_one_wins(*args, **kwargs):
        found = real(*args, **kwargs)
        if not calls:
            calls.append(1)
            monkeypatch.setattr(ns.governance_requests, "readiness", real)
            file(ns, env)   # the other submit, with the same readiness, commits first
            monkeypatch.setattr(ns.governance_requests, "readiness", first_one_wins)
        return found

    monkeypatch.setattr(ns.governance_requests, "readiness", first_one_wins)
    recorded = None
    with pytest.raises(ns.governance_requests.RequestRefused) as refused:
        recorded = file(ns, env)
    assert recorded is None and refused.value.code in ("BINDING_EXISTS", "STALE_READINESS")
    assert len(bindings(env, ns)) == 1
    assert [event["reason_code"] for _, event in env.recorded].count("GOVERNANCE_REQUEST_FILED") == 3


def test_a_unique_constraint_lost_to_a_concurrent_submit_is_stale_readiness(ns, env, monkeypatch):
    """(N-3) The race both sides pass the existence check in: the database's unique constraint answers, read as stale
    (never as an unavailable audit trail)."""
    from sqlalchemy.exc import IntegrityError  # pylint: disable=C0415
    credential(env)

    def lost(*args, **kwargs):
        raise IntegrityError("insert", {}, Exception("unique"))

    monkeypatch.setattr(ns.governance_requests, "create_binding", lost)
    with pytest.raises(ns.governance_requests.RequestRefused) as refused:
        file(ns, env)
    assert refused.value.code == "STALE_READINESS" and bindings(env, ns) == [] and env.recorded == []


def test_ids_beyond_the_database_range_are_refused_before_it_is_asked(ns, env):
    """(N-12)"""
    payload, status = route(ns, env, "governance_readiness", "get", entity_kind="version", entity_id=2**31)
    assert (status, payload["code"]) == (400, "INVALID_REQUEST")
    payload, status = route(ns, env, "governance_requests", "post",
                            body={"entity_kind": "version", "entity_id": 2**31})
    assert (status, payload["code"]) == (400, "INVALID_REQUEST")
    assert env.sessions == []


def test_the_platform_settings_say_governance_is_enforced_only_while_it_is(ns, monkeypatch):
    """(S-4) The project Governance tab shows only under enforce; under off and audit only the platform settings
    answer exactly as before (no new field)."""
    for name, attrs in (("mcp_config", ("get_mcp_category_name", "is_mcp_exposure_enabled", "is_mcp_in_menu_enabled")),
                        ("midturn_injection_utils", ("get_midturn_injection_blocked", "get_midturn_injection_whitelist")),
                        ("skill_publish_utils", ("get_skill_publish_blocked", "get_skill_publish_whitelist")),
                        ("skill_utils", ("get_max_instructions_length",))):
        stub = types.ModuleType(f"{PKG}.utils.{name}")
        for attr in attrs:
            setattr(stub, attr, lambda *a, **k: [])
        monkeypatch.setitem(sys.modules, f"{PKG}.utils.{name}", stub)
    monkeypatch.setattr(ns.tools, "api_tools", types.SimpleNamespace(
        APIModeHandler=ns.tools.api_tools.APIModeHandler, APIBase=ns.tools.api_tools.APIBase,
        with_modes=lambda params: params, endpoint_metrics=lambda func: func), raising=False)
    monkeypatch.setattr(ns.tools, "rpc_tools", types.SimpleNamespace(), raising=False)
    settings = load_module_with_stubs(PLUGIN_ROOT / "api" / "v2" / "platform_settings.py",
                                      f"{PKG}.api.v2.platform_settings")
    monkeypatch.setattr(settings, "_usage_mode_settings", lambda: (None, True))
    monkeypatch.setattr(settings, "_tool_spend_availability", lambda: {})
    answers = {}
    for governed in (False, True):
        monkeypatch.setattr(settings, "this", types.SimpleNamespace(module=module(ns, governed=governed)))
        answers[governed] = settings.PromptLibAPI(None).get()[0]
    assert "governance_enforced" not in answers[False]
    assert answers[True] == {**answers[False], "governance_enforced": True}


# --- a stored selector by title (the index's pgvector as the UI saves it) ------------------------------------------

PGVECTOR = {"elitea_title": "elitea-pgvector", "private": False}


def _title_toolkit(env, monkeypatch, ns, answers, tool_id=2, field="pgvector_configuration", value=PGVECTOR):
    """A second toolkit of version 70 whose ``field`` selects a configuration by title only; ``answers``: what the
    run's resolver (``configurations_expand``) gives per title (an exception is raised)."""
    env.add_toolkit(1, 70, tool_id, "jira", {"url": "https://jira.example", field: dict(value)})
    calls = []

    def expand(project_id, settings, user_id):
        calls.append((project_id, dict(settings), user_id))
        answer = answers[settings["elitea_title"]]
        if isinstance(answer, Exception):
            raise answer
        return {**settings, "configuration_type": "pgvector", "connection_string": "{{secret.ref}}", **answer}

    monkeypatch.setattr(ns.governance_readiness, "_expand", expand)
    return calls


def test_a_title_only_index_store_is_resolved_as_the_run_resolves_it_and_is_not_a_credential(ns, env, monkeypatch):
    binding = env.add_binding(1, 7, 70, digest=DIGEST)
    cred = credential(env, binding)   # the jira credential by uuid: no reference yet
    store = str(uuid.uuid4())
    env.configurations[(1, store)] = {"id": 9, "project_id": 1, "type": "pgvector", "section": "vectorstorage",
                                      "generation": 1}
    calls = _title_toolkit(env, monkeypatch, ns, {"elitea-pgvector": {"configuration_uuid": store,
                                                                      "configuration_project_id": 1}})
    found = items(ns, env)
    assert [key for key in found if key.startswith(("reference:", "grant:"))] == [
        f"reference:{cred.uuid}", f"grant:1:{cred.uuid}"]
    assert found[f"reference:{cred.uuid}"]["state"] == "missing"
    assert not any(item["state"] == "not_registrable" for item in found.values())
    assert calls == [(1, {"elitea_title": "elitea-pgvector", "private": False}, MEMBER)]   # one call, as the run asks


def test_a_title_that_does_not_resolve_stays_not_registrable(ns, env, monkeypatch):
    env.add_binding(1, 7, 70, digest=DIGEST)
    _title_toolkit(env, monkeypatch, ns, {"elitea-pgvector": LookupError("not found")})
    found = items(ns, env)
    [item] = [item for key, item in found.items() if key.startswith("reference:")]
    assert (item["state"], item["requestable"]) == ("not_registrable", False)


def test_a_title_only_credential_is_treated_like_its_uuid(ns, env, monkeypatch):
    env.add_binding(1, 7, 70, digest=DIGEST)
    jira = str(uuid.uuid4())
    env.configurations[(1, jira)] = {"id": 11, "project_id": 1, "type": "jira", "section": "credentials",
                                     "generation": 1}
    _title_toolkit(env, monkeypatch, ns, {"jira-by-title": {"configuration_uuid": jira, "configuration_project_id": 1}},
                   field="jira_configuration", value={"elitea_title": "jira-by-title", "private": False})
    found = items(ns, env)
    assert (found[f"reference:{jira}"]["state"], found[f"reference:{jira}"]["requestable"]) == ("missing", True)
    assert f"grant:2:{jira}" in found


def test_an_unreachable_resolver_makes_readiness_unavailable_never_a_wrong_list(ns, env, monkeypatch):
    env.add_binding(1, 7, 70, digest=DIGEST)
    _title_toolkit(env, monkeypatch, ns, {"elitea-pgvector": TimeoutError("rpc")})
    with pytest.raises(ns.governance_readiness.ReadinessUnavailable):
        items(ns, env)
