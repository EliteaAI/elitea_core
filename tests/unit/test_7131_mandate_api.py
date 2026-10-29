"""C-1009 #7131: the operator REST API of autonomous mandates, and the owner's consent through it.

The handlers, the admin task they share and the consent run for real on SQLite (``fixtures/mandate_env.py``). The
platform around them is faked: ``check_api`` grants the permissions each endpoint's own descriptor recommends for the
caller's administration role (any of them, as the auth plugin does); auth_oidc's offline grant and the secrets plugin's
offline material are fakes. The consent's session check is the real one (``credential_probe._probe_person``, the
identity verifier faked as in every admission test). Every value is a made-up test value.
"""
import functools
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
TID = "11111111-2222-3333-4444-555555555555"
OID = "oid-owner-5"
ENTRA = f"https://login.microsoftonline.com/{TID}/v2.0"
OWNER = mandate_env.OWNER
CUSTODY_KEY = "custody-request-key-made-up-for-tests-0123"
REFRESH = "offline-refresh-token-made-up-0001"
VALID_UNTIL = (datetime.now(timezone.utc) + timedelta(days=20)).replace(microsecond=0).isoformat()
# who calls: user id, administration role, a token's permissions (None: a session with the role's own)
ACTORS = {
    "admin": (11, "admin", None), "approver": (12, "admin", None), "owner": (OWNER, "admin", None),
    "viewer": (22, "viewer", None), "token_view": (11, "admin", {"governance.bindings.view"}),
    "token_manage": (11, "admin", {"governance.bindings.manage"}),
}


class FakeRequest:
    args, content_length, body, max_content_length = {}, None, None, None

    def get_data(self):
        return json.dumps(self.body).encode()

    def get_json(self, silent=False):   # pylint: disable=W0613
        return self.body


@pytest.fixture(scope="module")
def ns(tmp_path_factory):
    saved = dict(sys.modules)
    ns = mandate_env.install(PLUGIN_ROOT, tmp_path_factory.mktemp("mandate_api"))
    tools = ns.tools

    class Application(ns.Base):
        __tablename__ = "applications"
        __table_args__ = {"schema": "tenant"}
        id = ns.sa.Column(ns.sa.Integer, primary_key=True)
        name = ns.sa.Column(ns.sa.String, nullable=False)

    sys.modules[f"{PKG}.models.all"].Application = Application
    ns.Application = Application
    ns.request, ns.actor, ns.descriptors = FakeRequest(), "admin", []
    ns.flask.request = ns.request

    def permissions_of(actor):
        _, role, token = ACTORS[actor]
        if token is not None:
            return set(token)
        return {permission for descriptor in ns.descriptors for permission in descriptor["permissions"]
                if descriptor["recommended_roles"]["administration"].get(role)}

    def check_api(descriptor):
        ns.descriptors.append(descriptor)

        def decorator(func):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                if not set(descriptor["permissions"]) & permissions_of(ns.actor):
                    return {"ok": False, "error": "access_denied"}, 403
                return func(*args, **kwargs)
            return wrapper
        return decorator

    tools.auth.decorators = types.SimpleNamespace(check_api=check_api)
    tools.auth.current_user = lambda: {"id": ACTORS[ns.actor][0]}
    tools.auth.resolve_permissions = lambda mode, project_id=None: permissions_of(ns.actor)
    tools.register_openapi = lambda **meta: (lambda func: func)
    tools.api_tools = types.SimpleNamespace(
        APIModeHandler=type("APIModeHandler", (), {"__init__": lambda self, module: setattr(self, "module", module)}),
        APIBase=type("APIBase", (), {}), with_modes=lambda params: params)
    for name in (f"{PKG}.api", f"{PKG}.api.v2"):
        package = types.ModuleType(name)
        package.__path__ = []
        sys.modules[name] = package
    ns.pd_mandate_api = load_module_with_stubs(PLUGIN_ROOT / "models" / "pd" / "mandate_api.py",
                                               f"{PKG}.models.pd.mandate_api")
    ns.mandate_queries = load_module_with_stubs(PLUGIN_ROOT / "utils" / "mandate_queries.py",
                                                f"{PKG}.utils.mandate_queries")
    ns.endpoints = types.SimpleNamespace(**{
        name: load_module_with_stubs(PLUGIN_ROOT / "api" / "v2" / f"{name}.py", f"{PKG}.api.v2.{name}")
        for name in ("autonomous_mandates", "autonomous_mandate_action")})
    yield ns
    sys.modules.clear()
    sys.modules.update(saved)


class Fakes:
    """auth_oidc's offline grant and the secrets plugin's offline material."""

    def __init__(self, obo):
        self.obo, self.granted, self.kept, self.retired = obo, [], [], []

    def timeout(self, seconds):
        return self

    def auth_oidc_obo_offline_grant(self, tid, oid, holder, iat, nonce, signature):
        assert signature == self.obo.sign_fields(CUSTODY_KEY, ("v1-offline-grant", tid, oid, holder), iat, nonce)
        self.granted.append((tid, oid, holder))
        return {"ok": True, "refresh_token": REFRESH, "assertion_source": "login"}

    def keep_offline_material(self, project_id, **kwargs):
        self.kept.append({key: value for key, value in kwargs.items() if key != "refresh_token"})
        return {"ok": True, "material_ref": str(uuid.uuid4()), "generation": 1}

    def retire_offline_material(self, project_id, material_ref, **kwargs):
        self.retired.append((str(material_ref), kwargs["reason"]))
        return {"ok": True, "retired": True}


@pytest.fixture
def env(ns, monkeypatch):
    env = mandate_env.prepare(ns, ns.env)
    ns.adm._now = ns.real_now  # pylint: disable=W0212
    ns.mandate_admin._now = lambda: datetime.now(timezone.utc)  # pylint: disable=W0212
    ns.mandate_state._now = lambda: datetime.now(timezone.utc)  # pylint: disable=W0212
    ns.users[OWNER] = {"id": OWNER, "idp_issuer": ENTRA, "idp_tid": TID, "idp_oid": OID}
    env.idp_links[(ENTRA, TID, OID)] = {"id": OWNER}
    env.users.update({uid: f"user{uid}@example.test" for uid in (OWNER, 11, 12, 22)})
    env.add_application(1, 7, "SharePoint reader")
    env.seed_agent()
    env.digests[70] = env.digests.get(70) or "adv1:" + "1" * 64
    env.fakes = Fakes(ns.entra_obo)
    env.secrets_extra = {"keep_offline_material": env.fakes.keep_offline_material,
                         "retire_offline_material": env.fakes.retire_offline_material}
    monkeypatch.setenv("ELITEA_OBO_CUSTODY_KEY", CUSTODY_KEY)
    env.sources = ["owner_delegated"]
    env.holder = types.SimpleNamespace(type="user", id=OWNER, reference="session-reference-made-up")
    monkeypatch.setattr(ns.credential_probe, "_session_holder", lambda: env.holder)
    principal = admission_env.principal(user_id=OWNER, tid=TID, oid=OID)
    principal["evidence"]["issuer"] = ENTRA
    env.verify_result = {"ok": True, "principal": principal}
    ns.request.body = None
    return env


def module(ns, env):
    built = ns.env.module(governed=True)
    built.descriptor = types.SimpleNamespace(config={"mandate_identity_sources": list(env.sources)})
    built.context = types.SimpleNamespace(rpc_manager=env.fakes)
    return built


def hit(ns, env, endpoint, method, *, actor="admin", body=None, args=None, **path):
    ns.actor, ns.request.body, ns.request.args = actor, body, args or {}
    ns.request.content_length = None if body is None else len(json.dumps(body))
    handler = getattr(ns.endpoints, endpoint).AdminAPI(module(ns, env))
    payload, status = getattr(handler, method)(project_id=1, **path)
    json.dumps(payload)
    return payload, status


def create(ns, env, actor="admin", **over):
    body = {"version_id": 70, "owner_user_id": OWNER, "operations": ["run:schedule"], "max_runtime_s": 600,
            "valid_until": VALID_UNTIL, "identity": "owner_delegated", **over}
    return hit(ns, env, "autonomous_mandates", "post", actor=actor, body=body)


def act(ns, env, mandate_id, action, actor="admin", **more):
    return hit(ns, env, "autonomous_mandate_action", "post", actor=actor, mandate_id=uuid.UUID(str(mandate_id)),
               body={"action": action, **more})


def active_mandate(ns, env):
    payload, status = create(ns, env)
    assert status == 200, payload
    assert act(ns, env, payload["id"], "submit")[1] == 200
    assert act(ns, env, payload["id"], "approve", actor="approver", approval_ref="CHG-1",
               policy_revision="rev-1") == ({"ok": True, "id": payload["id"], "state": "active"}, 200)
    return payload["id"]


def test_an_operator_creates_submits_and_another_approves_through_the_api(ns, env):
    mandate_id = active_mandate(ns, env)
    page, status = hit(ns, env, "autonomous_mandates", "get")
    assert status == 200 and page["total"] == 1 and page["identity_sources"] == ["owner_delegated"]
    [row] = page["rows"]
    assert (row["id"], row["state"], row["identity_source"], row["consented"]) == (
        mandate_id, "active", "owner_delegated", False)
    assert row["owner"] == {"id": OWNER, "email": f"user{OWNER}@example.test"}
    assert (row["application"]["name"], row["version"]["id"]) == ("SharePoint reader", 70)
    assert OID not in json.dumps(page) and TID not in json.dumps(page)   # owners by name, never by subject


@pytest.mark.parametrize("actor, endpoint, method, status", [
    ("viewer", "autonomous_mandates", "get", 403),
    ("viewer", "autonomous_mandates", "post", 403),
    ("token_view", "autonomous_mandates", "post", 403),
])
def test_who_may_list_and_create(ns, env, actor, endpoint, method, status):
    body = None if method == "get" else {"version_id": 70, "owner_user_id": OWNER, "operations": ["run:schedule"],
                                         "max_runtime_s": 600, "valid_until": VALID_UNTIL}
    assert hit(ns, env, endpoint, method, actor=actor, body=body)[1] == status


def test_approval_needs_the_approve_permission_and_another_person(ns, env):
    payload, _ = create(ns, env)
    act(ns, env, payload["id"], "submit")
    assert act(ns, env, payload["id"], "approve", actor="token_manage", approval_ref="C", policy_revision="r")[1] == 403
    refused, status = act(ns, env, payload["id"], "approve", approval_ref="C", policy_revision="r")   # its creator
    assert status == 403 and refused["code"] == "SELF_APPROVAL"


def test_creating_with_an_identity_the_platform_does_not_allow_is_refused(ns, env):
    env.sources = []
    refused, status = create(ns, env)
    assert status == 403 and refused["code"] == "IDENTITY_SOURCE_NOT_ALLOWED"
    assert hit(ns, env, "autonomous_mandates", "get")[0]["rows"] == []


def test_an_unknown_field_or_identity_is_refused(ns, env):
    assert create(ns, env, refresh_token="x")[1] == 400
    assert create(ns, env, identity="robot")[1] == 400


def test_the_owner_consents_in_their_own_session_and_the_list_says_so(ns, env):
    mandate_id = active_mandate(ns, env)
    answer, status = act(ns, env, mandate_id, "consent", actor="owner")
    assert status == 200 and answer["ok"] is True and answer["id"] == mandate_id
    assert env.fakes.granted == [(TID, OID, f"mandate:{mandate_id}")]
    assert env.fakes.kept[0]["custodian_subject_ref"] == f"idp:{TID}:{OID}"
    [row] = hit(ns, env, "autonomous_mandates", "get")[0]["rows"]
    assert row["consented"] is True and row["consented_at"]
    assert REFRESH not in json.dumps([answer, row])


@pytest.mark.parametrize("case, status, code", [
    ("operator", 403, "CONSENT_OWNER_ONLY"),                       # an operator cannot consent for the owner
    ("token", 403, "DENIED_DELEGATION_UNSUPPORTED_CALLER"),        # the owner's API token is not their session
    ("other_session", 403, "DENIED_DELEGATION_UNSUPPORTED_CALLER"),  # a session of someone else
    ("other_subject", 403, "DENIED_SUBJECT_MISMATCH"),             # signed in, but as another identity
    ("unverified", 409, "DENIED_ASSERTION_UNAVAILABLE"),           # the identity service does not vouch for it
])
def test_a_consent_that_is_not_the_owners_own_verified_session_is_refused(ns, env, case, status, code):
    mandate_id = active_mandate(ns, env)
    actor = "admin" if case == "operator" else "owner"
    if case == "token":
        env.holder = types.SimpleNamespace(type="token", id=OWNER, reference="token-reference")
    if case == "other_session":
        env.holder = types.SimpleNamespace(type="user", id=11, reference="session-of-someone-else")
    if case == "other_subject":
        env.verify_result["principal"]["evidence"]["oid"] = "someone-else"
    if case == "unverified":
        env.verify_result = {"ok": False, "reason_code": "IDENTITY_EXPIRED"}
    refused, got = act(ns, env, mandate_id, "consent", actor=actor)
    assert (got, refused["code"]) == (status, code)
    assert env.fakes.granted == [] and env.fakes.kept == []


def test_withdrawing_needs_manage_and_retires_the_consent(ns, env):
    mandate_id = active_mandate(ns, env)
    act(ns, env, mandate_id, "consent", actor="owner")
    assert act(ns, env, mandate_id, "withdraw_consent", actor="viewer")[1] == 403
    answer, status = act(ns, env, mandate_id, "withdraw_consent")
    assert status == 200 and answer == {"ok": True, "id": mandate_id}
    assert [reason for _, reason in env.fakes.retired] == ["CONSENT_WITHDRAWN"]
    [row] = hit(ns, env, "autonomous_mandates", "get")[0]["rows"]
    assert row["consented"] is False


def test_another_projects_mandate_is_not_found(ns, env):
    mandate_id = active_mandate(ns, env)
    ns.actor = "owner"
    handler = ns.endpoints.autonomous_mandate_action.AdminAPI(module(ns, env))
    ns.request.body = {"action": "consent"}
    payload, status = handler.post(project_id=2, mandate_id=uuid.UUID(mandate_id))
    assert status == 404 and payload["code"] == "MANDATE_NOT_FOUND"


def test_with_no_identity_allowed_the_list_names_none_and_reads_no_identity_column(ns, env):
    env.sources = []
    payload, _ = create(ns, env, identity=None)
    seen = []
    engine = env.engine(1)
    listener = lambda conn, cursor, statement, params, context, many: seen.append(statement)   # noqa: E731
    ns.sa.event.listen(engine, "before_cursor_execute", listener)
    try:
        page, _ = hit(ns, env, "autonomous_mandates", "get")
    finally:
        ns.sa.event.remove(engine, "before_cursor_execute", listener)
    assert page["identity_sources"] == [] and page["rows"][0]["identity_source"] is None
    assert not [s for s in seen if "identity_source" in s or "identity_material_ref" in s]
    assert payload["ok"] is True
