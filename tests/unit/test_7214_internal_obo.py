"""C-1009 (#7214): the internal Elitea MCP as a governed resource - the run token Main mints at admission, its
verification on the MCP side, the scope and the explicit operation map, the target projects, the audit of platform
writes, audit only and governance off.

Real models, admission and token code on SQLite files (``fixtures/admission_env.py``); the Pylon runtime, the identity
verifier and the audit methods are fakes, and audit events are validated against the tracing model when it is checked
out next to this plugin.
"""
import base64
import copy
import json
import sys
import time
import uuid
from pathlib import Path

import pytest

PLUGIN_ROOT = Path(__file__).parents[2]
sys.path.insert(0, str(PLUGIN_ROOT / "tests"))

from fixtures import admission_env  # noqa: E402  pylint: disable=C0413

RUN = "run-1"
KEY = "test-master-key-not-a-secret"
BUILD_PERMISSION = "models.applications.application.update"
CREDENTIAL_PERMISSION = "configurations.configuration.create"
PREDICT_PERMISSION = "models.applications.predict.post"
INTERNAL_URL = "http://pylon_main:8080/app/5/mcp/elitea_core/applications?scope_project_id=1"
MCP_ENDPOINT = "elitea_core.client_tag_filtered_streamable_http"
MCP_VIEW = {"project_id": 5, "openapi_tag": "elitea_core/applications"}
MCP_ARGS = {"scope_project_id": "1"}


@pytest.fixture(scope="module")
def ns(tmp_path_factory):
    saved = dict(sys.modules)
    yield admission_env.install(PLUGIN_ROOT, tmp_path_factory.mktemp("internal_obo"))
    sys.modules.clear()
    sys.modules.update(saved)


@pytest.fixture
def env(ns):
    ns.env.reset()
    ns.tools.config.SECRETS_MASTER_KEY = KEY
    ns.tools.config.APP_HOST = "http://localhost"
    ns.tools.this.module = ns.env.module()
    ns.tools.auth.epoch_ttl = 5.0
    ns.internal_obo._VERDICTS.clear()   # pylint: disable=W0212
    yield ns.env
    for name in ("SECRETS_MASTER_KEY", "APP_HOST"):
        vars(ns.tools.config).pop(name, None)


def obo(ns):
    return ns.internal_obo


def internal_tool(url=INTERNAL_URL, headers=None, kind="mcp", **settings):
    return {"type": kind, "id": None, "name": "Elitea Applications",
            "settings": {"url": url, "headers": {} if headers is None else headers, **settings}}


def call_kwargs(run_id=RUN, internal_mcp=False, tools=None, **extra):
    payload = {"project_id": 1, "message_id": 5, "_elitea_predict_run_id": run_id,
               "envelope": {"schema_version": 1, "security_profile": "governed",
                            "required_capabilities": ["task_envelope.v1"], "context_ref": None}}
    if internal_mcp:
        payload["tools"] = [internal_tool()]
    if tools is not None:
        payload["tools"] = tools
    payload.update(extra)
    return {"args": ["s", "m"], "kwargs": payload, "pool": "agents", "meta": {"project_id": 1}}


def seed(env, operations=None):
    binding_id = env.seed_agent()
    if operations is not None:
        with env.ns.sa_orm.Session(env.engine(1)) as session:
            session.execute(env.ns.sa.update(env.ns.models.AgentIdentityBinding)
                            .where(env.ns.models.AgentIdentityBinding.id == binding_id)
                            .values(allowed_platform_operations=operations))
            session.commit()
    return binding_id


def admitted(ns, env, operations=None, permissions=(BUILD_PERMISSION,), internal_mcp=True, **extra):
    binding_id = seed(env, operations)
    env.permissions[42] = {"credentials.use", *permissions}
    call = call_kwargs(internal_mcp=internal_mcp, **extra)
    context_ref = ns.adm.admit_dispatch(env.module(), "indexer_agent", call, env.request())
    return binding_id, context_ref, call


def claims_of(token):
    body = token[len("eio1."):].split(".")[0]
    return json.loads(base64.urlsafe_b64decode(body + "=" * (-len(body) % 4)))


def token_of(ns, call):
    return call["kwargs"][obo(ns).TOKEN_KWARG]["token"]


def verify(ns, env, token, endpoint=MCP_ENDPOINT, environ=None, now=None, view_args=None, args=None):
    return obo(ns).verify_request(token, environ or {}, endpoint=endpoint, module=env.module(), now=now,
                                  view_args=MCP_VIEW if view_args is None else view_args,
                                  args=MCP_ARGS if args is None else args)


def mint(ns, **over):
    values = dict(sub="idp:t:o", uid=42, act=str(uuid.uuid4()), scope=["read"], ctx=str(uuid.uuid4()), prj=1,
                  tgt=[1, 5], run=RUN, tsk="indexer_agent", exp=int(time.time()) + 600)
    values.update(over)
    return obo(ns).mint(**values)


# --- minting at admission ----------------------------------------------------------------------------------------

def test_admission_mints_one_token_with_the_claims_of_the_run(ns, env):
    binding_id, context_ref, call = admitted(ns, env, operations=["read", "build"])
    delivered = call["kwargs"][obo(ns).TOKEN_KWARG]
    assert set(delivered) == {"token", "origins", "identity"}
    claims = claims_of(delivered["token"])
    [row] = env.query(1, ns.models.ExecutionSecurityContext)
    assert set(claims) == set(obo(ns).CLAIM_KEYS)
    assert claims["sub"] == row.subject["ref"] == "idp:tid-1:oid-42"
    assert (claims["uid"], claims["act"], claims["ctx"], claims["prj"], claims["run"], claims["tsk"]) == (
        42, str(binding_id), context_ref, 1, RUN, "indexer_agent")
    assert claims["aud"] == "elitea:internal" and claims["iss"] == "elitea:main"
    assert claims["scope"] == ["read", "build"]
    assert claims["tgt"] == [1, 5]   # the URL's project and its scope_project_id
    assert claims["exp"] == int(ns.adm._aware(row.expires_at).timestamp())   # the context's own expiry
    assert delivered["origins"] == ["http://localhost", "http://pylon_main:8080"]
    assert obo(ns).TOKEN_KWARG not in call["meta"] and "eio1." not in json.dumps(call["kwargs"]["envelope"])


def test_the_token_never_reaches_a_log_line_or_an_audit_event(ns, env):
    _, _, call = admitted(ns, env, operations=["read", "build"])
    token = token_of(ns, call)
    assert not [line for line in env.log.lines if token in line or "eio1." in line]
    assert not [event for _, event in env.recorded if "eio1." in json.dumps(event)]


@pytest.mark.parametrize("stored", [None, []])
def test_a_binding_without_a_list_or_with_an_empty_one_allows_read_only(ns, env, stored):
    _, _, call = admitted(ns, env, operations=stored)
    assert claims_of(token_of(ns, call))["scope"] == ["read"]


def test_the_scope_is_the_binding_list_intersected_with_the_users_rbac(ns, env):
    _, _, call = admitted(ns, env, operations=["read", "build", "credentials", "run"], permissions=(BUILD_PERMISSION,))
    assert claims_of(token_of(ns, call))["scope"] == ["read", "build"]


def test_an_operation_the_user_may_do_is_still_out_of_scope_when_the_binding_does_not_allow_it(ns, env):
    _, _, call = admitted(ns, env, operations=["read", "build"],
                          permissions=(BUILD_PERMISSION, CREDENTIAL_PERMISSION))
    assert "credentials" not in claims_of(token_of(ns, call))["scope"]


def test_a_development_run_is_scoped_by_the_authors_rbac_alone(ns, env):
    env.permissions[42] = {CREDENTIAL_PERMISSION}
    assert obo(ns).scope_for(1, "development:adv1:" + "0" * 64, 42) == ["read", "credentials"]


def _statements(env):
    seen = []
    engine = env.engine(1)
    listener = lambda conn, cursor, statement, params, context, many: seen.append(statement)   # noqa: E731
    env.ns.sa.event.listen(engine, "before_cursor_execute", listener)
    return seen, lambda: env.ns.sa.event.remove(engine, "before_cursor_execute", listener)


def _dispatch_cost(ns, env, internal_mcp, run_id):
    env.permission_calls.clear()
    seen, stop = _statements(env)
    ns.adm.admit_dispatch(env.module(), "indexer_agent", call_kwargs(run_id=run_id, internal_mcp=internal_mcp),
                          env.request())
    stop()
    return seen, list(env.permission_calls)


def test_the_token_costs_one_select_and_one_permission_lookup_on_the_real_dispatch_path(ns, env):
    """Measured through ``admit_dispatch`` (admission, seal, token): with an internal toolkit the dispatch reads the
    binding's operations once and asks for the user's permissions once more; without one it reads exactly what it read
    before #7214."""
    seed(env, ["read", "build"])
    env.permissions[42] = {"credentials.use", BUILD_PERMISSION}
    plain, plain_permissions = _dispatch_cost(ns, env, False, "run-a")
    with_tools, tool_permissions = _dispatch_cost(ns, env, True, "run-b")
    extra = [statement for statement in with_tools if "allowed_platform_operations" in statement]
    assert len(with_tools) == len(plain) + 1 and len(extra) == 1 and extra[0].lstrip().upper().startswith("SELECT")
    assert len(tool_permissions) == len(plain_permissions) + 1
    assert not [statement for statement in plain if "allowed_platform_operations" in statement]


def test_a_run_without_an_internal_toolkit_gets_no_token_and_mints_nothing(ns, env):
    seed(env, ["read", "build"])
    signed = []
    real = obo(ns).mint
    obo(ns).mint = lambda **kw: signed.append(kw) or real(**kw)
    try:
        call = call_kwargs()
        assert ns.adm.admit_dispatch(env.module(), "indexer_agent", call, env.request())
    finally:
        obo(ns).mint = real
    assert obo(ns).TOKEN_KWARG not in call["kwargs"] and signed == []


def test_a_sub_agent_the_runtime_may_fetch_itself_still_gets_a_token_with_the_private_project(ns, env, monkeypatch):
    """The prefetch is not authoritative (it has a size and time budget): a sub-agent it did not cover is fetched by the
    SDK, with Main-injected internal toolkits in the user's private project and the run's own project."""
    seed(env, ["read", "build"])
    monkeypatch.setattr(obo(ns), "_private_project", lambda user_id: 9)
    child = {"type": "application", "settings": {"application_id": 3, "application_version_id": 30}}
    call = call_kwargs(tools=[child], application={"subagent_version_details": {}})
    ns.adm.admit_dispatch(env.module(), "indexer_agent", call, env.request())
    assert claims_of(token_of(ns, call))["tgt"] == [1, 9]
    covered = call_kwargs(run_id="run-2", tools=[child],
                          application={"subagent_version_details": {"3:30": {"tools": []}}})
    ns.adm.admit_dispatch(env.module(), "indexer_agent", covered, env.request())
    assert obo(ns).TOKEN_KWARG not in covered["kwargs"]


def test_without_a_key_a_run_with_internal_tools_is_refused_and_its_context_ended(ns, env):
    vars(ns.tools.config).pop("SECRETS_MASTER_KEY")
    seed(env)
    call = call_kwargs(internal_mcp=True)
    with pytest.raises(ns.exceptions.AdmissionDenied) as err:
        ns.adm.admit_dispatch(env.module(), "indexer_agent", call, env.request())
    assert err.value.reason_code == "INTERNAL_TOKEN_UNAVAILABLE" and "Try again later" in err.value.message
    [row] = env.query(1, ns.models.ExecutionSecurityContext)
    assert (row.state, row.revoked_reason) == ("revoked", "NOT_DISPATCHED")
    assert [e["reason_code"] for e in env.denied] == ["INTERNAL_TOKEN_UNAVAILABLE"]


def test_a_token_the_caller_put_on_the_payload_is_dropped(ns, env):
    seed(env)
    call = call_kwargs(**{"internal_run_token": {"token": "eio1.forged.x", "origins": ["https://evil.example"]}})
    ns.adm.admit_dispatch(env.module(), "indexer_agent", call, env.request())
    assert "internal_run_token" not in call["kwargs"]


def test_a_child_dispatch_gets_a_token_of_its_parents_context(ns, env):
    binding_id, context_ref, _ = admitted(ns, env, operations=["read", "build"])
    child = call_kwargs(internal_mcp=True)
    child["kwargs"]["envelope"]["context_ref"] = context_ref
    assert ns.adm.admit_dispatch(env.module(), "indexer_agent", child, None) is None
    claims = claims_of(token_of(ns, child))
    assert (claims["ctx"], claims["act"], claims["uid"], claims["run"], claims["scope"], claims["tgt"]) == (
        context_ref, str(binding_id), 42, RUN, ["read", "build"], [1, 5])


def test_a_child_of_an_unknown_context_with_internal_tools_is_refused(ns, env):
    seed(env)
    child = call_kwargs(internal_mcp=True)
    child["kwargs"]["envelope"]["context_ref"] = str(uuid.uuid4())
    with pytest.raises(ns.exceptions.AdmissionDenied):
        ns.adm.admit_dispatch(env.module(), "indexer_agent", child, None)


# --- an internal-shaped toolkit reaches the platform with the run token only (C9-SEC-25) ---------------------------

@pytest.mark.parametrize("headers", [
    {"Authorization": "Bearer pat-value"}, {"authorization": "Bearer {personal_token}"}, {"Cookie": "session=x"},
    json.dumps({"Authorization": "Bearer pat-value", "X-Trace": "1"}),
])
def test_under_enforce_an_internal_toolkits_own_auth_headers_are_removed_quietly_on_the_plain_path(ns, env, headers):
    seed(env)
    tool = internal_tool(headers=headers, kind="mcp_elitea", personal_token="pat-value")
    call = call_kwargs(tools=[tool])
    ns.adm.admit_dispatch(env.module(), "indexer_agent", call, env.request())
    settings = call["kwargs"]["tools"][0]["settings"]
    assert "personal_token" not in settings
    assert not [key for key in settings["headers"] if key.lower() in ("authorization", "cookie")]
    assert obo(ns).TOKEN_KWARG in call["kwargs"]


@pytest.mark.parametrize("url", [
    "http://10.0.0.7:8080/app/5/mcp/elitea_core/applications", "http://pylon-main.svc:8080/app/5/mcp",
    "https://elitea.example/app/5/mcp/secrets",
])
def test_under_enforce_an_internal_toolkit_on_another_origin_refuses_the_run(ns, env, url):
    seed(env)
    call = call_kwargs(tools=[internal_tool(url=url, headers={"Authorization": "Bearer pat-value"})])
    with pytest.raises(ns.exceptions.AdmissionDenied) as err:
        ns.adm.admit_dispatch(env.module(), "indexer_agent", call, env.request())
    assert err.value.reason_code == "INTERNAL_MCP_ORIGIN_NOT_ALLOWED"
    assert env.query(1, ns.models.ExecutionSecurityContext) == []   # refused before any admission


@pytest.mark.parametrize("url,internal", [
    ("http://pylon_main:8080/app/5/mcp", True), ("http://pylon_main:8080/app/5/mcp/", True),
    ("http://pylon_main:8080/app/{project_id}/mcp/secrets", True), ("http://pylon_main:8080/app/5/mcpx", False),
    ("http://pylon_main:8080/x/app/5/mcp/secrets", False), ("https://mcp.example.com/mcp", False),
])
def test_the_internal_shape_is_matched_on_the_path_with_or_without_a_tag(ns, url, internal):
    assert obo(ns).has_internal_mcp({"tools": [{"type": "mcp", "settings": {"url": url}}]}) is internal


# --- the token itself --------------------------------------------------------------------------------------------

@pytest.mark.parametrize("change", ["body", "mac", "prefix", "truncate", "huge"])
def test_a_tampered_token_is_invalid(ns, env, change):
    token, _ = mint(ns)
    body, mac = token[len("eio1."):].split(".")
    forged = {
        "body": "eio1." + base64.urlsafe_b64encode(json.dumps({**claims_of(token), "uid": 1}).encode()).decode().rstrip("=")
                + "." + mac,
        "mac": f"eio1.{body}.{mac[:-2]}AA",
        "prefix": "eio2." + token[5:],
        "truncate": f"eio1.{body}",
        "huge": "eio1." + "A" * 5000 + "." + mac,
    }[change]
    with pytest.raises(ValueError, match="INTERNAL_TOKEN_INVALID"):
        obo(ns).parse(forged, obo(ns).signing_key())


def _signed(ns, claims):
    key = obo(ns).signing_key()
    body = base64.urlsafe_b64encode(json.dumps(claims).encode()).decode().rstrip("=")
    return f"eio1.{body}.{obo(ns)._mac(key, body)}"   # pylint: disable=W0212


def test_a_token_signed_with_another_key_or_for_another_audience_is_invalid(ns, env):
    token, claims = mint(ns)
    ns.tools.config.SECRETS_MASTER_KEY = "another-key"
    with pytest.raises(ValueError, match="INTERNAL_TOKEN_INVALID"):
        obo(ns).parse(token, obo(ns).signing_key())
    ns.tools.config.SECRETS_MASTER_KEY = KEY
    with pytest.raises(ValueError, match="INTERNAL_TOKEN_INVALID"):
        obo(ns).parse(_signed(ns, {**claims, "aud": "elitea:other"}), obo(ns).signing_key())


def test_a_token_issued_in_the_future_or_living_longer_than_a_context_is_invalid(ns, env):
    now = int(time.time())
    _, claims = mint(ns)
    future = _signed(ns, {**claims, "iat": now + 120, "exp": now + 600})
    assert verify(ns, env, future)["code"] == "INTERNAL_TOKEN_INVALID"
    ttl = env.module().admission_settings.context_ttl_s
    long_lived = _signed(ns, {**claims, "iat": now, "exp": now + ttl + 3600})
    assert verify(ns, env, long_lived)["code"] == "INTERNAL_TOKEN_INVALID"
    assert [e["reason_code"] for e in env.denied] == ["INTERNAL_TOKEN_INVALID", "INTERNAL_TOKEN_INVALID"]


def test_an_expired_token_says_so_and_is_audited(ns, env):
    token, claims = mint(ns, exp=int(time.time()) + 1)
    reply = verify(ns, env, token, now=int(time.time()) + 5)
    assert (reply["ok"], reply["code"], reply["status"]) == (False, "INTERNAL_TOKEN_EXPIRED", 401)
    assert "security_metric name=internal_token_verification_total result=deny reason_code=INTERNAL_TOKEN_EXPIRED" \
        in env.log.lines
    [event] = env.denied
    assert (event["event_type"], event["decision"], event["reason_code"], event["runtime_identity_ref"]) == (
        "platform_operation", "deny", "INTERNAL_TOKEN_EXPIRED", f"context:{claims['ctx']}")


def test_a_bad_signature_is_counted_not_audited(ns, env):
    obo(ns)._UNVERIFIED_LOG.update(at=0.0, suppressed=0)   # pylint: disable=W0212
    token, _ = mint(ns)
    assert verify(ns, env, token[:-2] + "AA")["code"] == "INTERNAL_TOKEN_INVALID"
    assert env.denied == []   # its claims are not Main's: nothing of them is written
    assert "security_metric name=internal_token_verification_total result=deny reason_code=INTERNAL_TOKEN_INVALID " \
           "suppressed=0" in env.log.lines


def test_no_key_refuses_every_token(ns, env):
    token, _ = mint(ns)
    vars(ns.tools.config).pop("SECRETS_MASTER_KEY")
    assert verify(ns, env, token)["code"] == "INTERNAL_TOKEN_INVALID"


# --- verification on the MCP side ---------------------------------------------------------------------------------

def test_a_live_runs_token_is_accepted_on_the_mcp_route_as_its_user(ns, env):
    _, context_ref, call = admitted(ns, env, operations=["read", "build"])
    env.in_request = True
    assert verify(ns, env, token_of(ns, call)) == {"ok": True, "user_id": 42}
    assert env.g.internal_obo["ctx"] == context_ref and env.g.internal_obo["act"]


@pytest.mark.parametrize("endpoint", [
    "api.v2.elitea_core.applications", "elitea_core.client_connected", None,
    "other_plugin.client_tag_filtered_streamable_http",   # the same function name elsewhere is not the MCP route
])
def test_the_token_is_refused_on_any_other_route_and_audited(ns, env, endpoint):
    _, _, call = admitted(ns, env, operations=["read", "build"])
    env.denied.clear()
    reply = verify(ns, env, token_of(ns, call), endpoint=endpoint)
    assert (reply["ok"], reply["code"], reply["status"]) == (False, "INTERNAL_TOKEN_INVALID", 403)
    [event] = env.denied
    assert (event["event_type"], event["decision"], event["reason_code"]) == (
        "platform_operation", "deny", "INTERNAL_TOKEN_INVALID")


@pytest.mark.parametrize("view_args,args,ok", [
    ({"project_id": 5}, {"scope_project_id": "1"}, True),     # the injected private + current project
    ({"project_id": 1}, {}, True),
    ({"project_id": 7}, {"scope_project_id": "1"}, False),    # another project the user belongs to
    ({"project_id": 5}, {"scope_project_id": "7"}, False),
    ({"project_id": 5}, {"scope_project_id": "x"}, False),
])
def test_the_token_acts_only_in_the_projects_it_was_minted_for(ns, env, view_args, args, ok):
    _, _, call = admitted(ns, env, operations=["read", "build"])
    reply = verify(ns, env, token_of(ns, call), view_args=view_args, args=args)
    assert reply["ok"] is ok
    if not ok:
        assert (reply["code"], reply["status"]) == ("INTERNAL_TARGET_DENIED", 403)


def test_the_in_process_api_call_of_a_verified_endpoint_is_accepted_without_a_second_validation(ns, env):
    _, _, call = admitted(ns, env, operations=["read", "build"])
    token = token_of(ns, call)
    claims = claims_of(token)
    env.epoch_calls.clear()
    environ = {"elitea.internal_mcp_request": True, obo(ns).VERIFIED_ENVIRON_KEY: claims}
    assert verify(ns, env, token, endpoint="api.v2.elitea_core.applications", environ=environ,
                  view_args={"project_id": 1}, args={})["ok"]
    assert env.epoch_calls == []
    # but not in a project outside the token's targets (e.g. a project_id the model chose for a credential tool)
    assert verify(ns, env, token, endpoint="api.v2.configurations", environ=environ,
                  view_args={"project_id": 9}, args={})["code"] == "INTERNAL_TARGET_DENIED"
    other = {"elitea.internal_mcp_request": True, obo(ns).VERIFIED_ENVIRON_KEY: {**claims, "jti": "x"}}
    assert verify(ns, env, token, endpoint="api.v2.x", environ=other)["code"] == "INTERNAL_TOKEN_INVALID"


def test_the_mcp_path_does_not_write_the_context_on_every_request_and_reuses_a_positive_verdict(ns, env):
    _, context_ref, call = admitted(ns, env, operations=["read", "build"])
    token = token_of(ns, call)
    with env.get_session(1) as session:   # the worker validated: the run is running
        ns.adm._set_state(session, uuid.UUID(context_ref), "running")   # pylint: disable=W0212
        session.commit()
    seen, stop = _statements(env)
    env.epoch_calls.clear()
    for _ in range(5):
        assert verify(ns, env, token)["ok"]
    stop()
    assert not [s for s in seen if not s.lstrip().upper().startswith("SELECT")]   # no UPDATE, no commit of one
    assert len(env.epoch_calls) == 1   # validated once, then reused within the verdict TTL


def test_the_verdict_is_reused_no_longer_than_the_auth_epoch_ttl(ns, env):
    ns.tools.auth.epoch_ttl = 0
    _, _, call = admitted(ns, env, operations=["read", "build"])
    env.epoch_calls.clear()
    verify(ns, env, token_of(ns, call))
    verify(ns, env, token_of(ns, call))
    assert len(env.epoch_calls) == 2


def test_a_token_of_a_revoked_context_is_refused(ns, env):
    binding_id, _, call = admitted(ns, env, operations=["read", "build"])
    with env.get_session(1) as session:
        ns.adm.revoke_binding_contexts(session, binding_id)
        session.commit()
    reply = verify(ns, env, token_of(ns, call))
    assert (reply["ok"], reply["code"], reply["status"]) == (False, "CONTEXT_NOT_LIVE", 403)


def test_a_context_that_cannot_be_checked_is_503_not_a_refusal(ns, env):
    _, _, call = admitted(ns, env, operations=["read", "build"])
    env.epochs_raise = True
    reply = verify(ns, env, token_of(ns, call))
    assert (reply["ok"], reply["code"], reply["status"]) == (False, "DEPENDENCY_UNAVAILABLE", 503)


def test_a_token_of_another_context_or_run_is_refused(ns, env):
    _, _, call = admitted(ns, env, operations=["read", "build"])
    claims = claims_of(token_of(ns, call))
    base = {k: claims[k] for k in ("sub", "uid", "act", "scope", "prj", "tgt", "run", "tsk", "exp", "ctx")}
    other, _ = obo(ns).mint(**{**base, "ctx": str(uuid.uuid4())})
    assert verify(ns, env, other)["code"] == "CONTEXT_NOT_LIVE"
    other_run, _ = obo(ns).mint(**{**base, "run": "another-run"})
    assert verify(ns, env, other_run)["code"] == "CONTEXT_NOT_LIVE"


def test_a_token_after_the_user_was_suspended_is_refused(ns, env):
    _, _, call = admitted(ns, env, operations=["read", "build"])
    env.epochs[42] = {"active": False, "user": 3, "token": 0}
    assert verify(ns, env, token_of(ns, call))["code"] == "CONTEXT_NOT_LIVE"


# --- the explicit operation map (C9-SEC-21) -----------------------------------------------------------------------

# Every write the internal MCP exposes as an API tool on 2026-10-26 (enumerated from the live registry: tools/list of
# every MCP tag, matched to the OpenAPI paths), with the operation it must be.
EXPOSED_WRITES = [
    ("POST", "artifacts/s3_credentials/{mode}/{project_id}", "credentials"),
    ("PUT", "artifacts/s3_credentials/{mode}/{project_id}/{access_key_id}", "credentials"),
    ("POST", "configurations/configurations/{project_id}", "credentials"),
    ("PUT", "configurations/configuration/{project_id}/{config_id}", "credentials"),
    ("POST", "configurations/models/{project_id}", "credentials"),
    ("POST", "secrets/secrets/{mode}/{project_id}", "credentials"),
    ("PUT", "secrets/secret/{mode}/{project_id}/{secret}", "credentials"),
    ("POST", "elitea_core/messages/{mode}/{project_id}/{conversation_uuid}", "run"),
    ("POST", "elitea_core/continue_predict/{mode}/{project_id}/{conversation_uuid}", "run"),
    ("POST", "elitea_core/skill_predict/{mode}/{project_id}/{skill_id}", "run"),
    ("POST", "elitea_core/public_skill_predict/{mode}/{project_id}/{public_skill_id}", "run"),
    ("POST", "elitea_core/generate_project_context_draft/{mode}/{project_id}", "run"),
    ("POST", "elitea_core/generate_skill_draft/{mode}/{project_id}", "run"),
    ("POST", "elitea_core/applications/{mode}/{project_id}", "build"),
    ("POST", "elitea_core/versions/{mode}/{project_id}/{application_id}", "build"),
    ("PUT", "elitea_core/version/{mode}/{project_id}/{application_id}/{version_id}", "build"),
    ("POST", "elitea_core/version_instruction_patch/{mode}/{project_id}/{application_id}/{version_id}", "build"),
    ("PATCH", "elitea_core/application_relation/{mode}/{project_id}/{application_id}/{version_id}", "build"),
    ("POST", "elitea_core/tools/{mode}/{project_id}", "build"),
    ("PUT", "elitea_core/tool/{mode}/{project_id}/{tool_id}", "build"),
    ("PATCH", "elitea_core/tool/{mode}/{project_id}/{tool_id}", "build"),
    ("POST", "elitea_core/skills/{mode}/{project_id}", "build"),
    ("PUT", "elitea_core/skill/{mode}/{project_id}/{skill_id}", "build"),
    ("PATCH", "elitea_core/skill/{mode}/{project_id}/{skill_id}", "build"),
    ("PUT", "elitea_core/project_context/{mode}/{project_id}/project-context", "build"),
    ("DELETE", "elitea_core/project_context/{mode}/{project_id}/project-context", "build"),
    ("POST", "elitea_core/folder/{mode}/{project_id}", "build"),
    ("PUT", "elitea_core/folder/{mode}/{project_id}/{folder_id}", "build"),
    ("POST", "elitea_core/conversations/{mode}/{project_id}", "build"),
    ("PUT", "elitea_core/conversation/{mode}/{project_id}/{conversation_id}", "build"),
    ("POST", "elitea_core/participants/{mode}/{project_id}/{conversation_id}", "build"),
    ("DELETE", "elitea_core/participant/{mode}/{project_id}/{conversation_id}/{participant_id}", "build"),
    ("PATCH", "elitea_core/entity_settings/{mode}/{project_id}/{conversation_id}/{participant_id}", "build"),
    ("POST", "artifacts/artifacts/{mode}/{project_id}/{bucket}", "build"),
    ("DELETE", "artifacts/artifacts/{mode}/{project_id}/{bucket}", "build"),
    ("POST", "artifacts/buckets/{mode}/{project_id}", "build"),
    ("PUT", "artifacts/buckets/{mode}/{project_id}", "build"),
    ("PATCH", "artifacts/buckets/{mode}/{project_id}", "build"),
    ("POST", "social/folders/{mode}/{project_id}", "build"),
    ("PUT", "social/folder/{mode}/{project_id}/{folder_id}", "build"),
    ("PUT", "social/move_to_folder/{mode}/{project_id}", "build"),
    ("PUT", "notifications/notification/{mode}/{project_id}/{notification_id}", "build"),
]


@pytest.mark.parametrize("method,path,operation", EXPOSED_WRITES, ids=[f"{m} {p}" for m, p, _ in EXPOSED_WRITES])
def test_every_mcp_exposed_write_has_its_explicit_operation(ns, method, path, operation):
    assert obo(ns).operation_of(method, "/api/v2/" + path) == operation
    assert obo(ns).operation_of(method, path) == operation


def test_the_map_holds_exactly_the_exposed_writes(ns):
    assert set(obo(ns).ENDPOINT_OPERATIONS) == {(m, p) for m, p, _ in EXPOSED_WRITES}


@pytest.mark.parametrize("method,path", [
    ("POST", "/api/v2/projects/project/{mode}"), ("DELETE", "/api/v2/projects/project/{mode}/{project_id}"),
    ("DELETE", "/api/v2/artifacts/s3_credentials/{mode}/{project_id}/{access_key_id}"),
    ("POST", "/api/v2/elitea_core/new_thing/{mode}/{project_id}"),
])
def test_a_write_the_map_does_not_know_is_unknown(ns, method, path):
    assert obo(ns).operation_of(method, path) is None


def test_reads_are_read(ns):
    assert obo(ns).operation_of("GET", "/api/v2/artifacts/s3_credentials/{mode}/{project_id}") == "read"


def under_token(ns, env, scope):
    env.in_request = True
    _, claims = mint(ns, scope=scope)
    env.g.internal_obo = claims
    return claims


def test_a_write_in_scope_is_recorded_before_it_is_made_with_subject_actor_and_context(ns, env):
    claims = under_token(ns, env, ["read", "build"])
    obo(ns).check_operation("build", "POST /api/v2/elitea_core/applications/prompt_lib/1", "post_prompt_lib")
    [event] = env.denied
    assert event["event_type"] == "platform_operation" and event["decision"] == "allow"
    assert event["reason_code"] == "PLATFORM_OPERATION"
    assert (event["user_ref"], event["agent_binding_ref"], event["runtime_identity_ref"], event["run_id"]) == (
        claims["sub"], claims["act"], f"context:{claims['ctx']}", RUN)
    assert event["operation_id"] == "build:post_prompt_lib" and event["target_ref"].startswith("POST /api/v2/")


def test_a_read_is_not_recorded(ns, env):
    under_token(ns, env, ["read"])
    obo(ns).check_operation("read", "GET /api/v2/x/1", "get_x")
    assert env.denied == []


def test_an_operation_outside_the_scope_is_refused_readably_and_recorded(ns, env):
    under_token(ns, env, ["read", "build"])
    with pytest.raises(obo(ns).OperationRefused) as err:
        obo(ns).check_operation(obo(ns).operation_of("POST", "/api/v2/artifacts/s3_credentials/{mode}/{project_id}"),
                                "POST /api/v2/artifacts/s3_credentials/prompt_lib/1", "post_s3_credentials")
    assert err.value.code == "INTERNAL_SCOPE_DENIED"
    assert obo(ns).refusal_text(err.value).startswith("INTERNAL_SCOPE_DENIED: This agent is not allowed")
    [event] = env.denied
    assert (event["decision"], event["reason_code"], event["operation_id"]) == (
        "deny", "INTERNAL_SCOPE_DENIED", "credentials:post_s3_credentials")


def test_a_write_the_map_does_not_know_is_refused_whatever_the_scope(ns, env):
    under_token(ns, env, ["read", "build", "credentials", "run"])
    with pytest.raises(obo(ns).OperationRefused) as err:
        obo(ns).check_operation(None, "POST /api/v2/projects/project/prompt_lib", "post_projects_project")
    assert err.value.code == "INTERNAL_OPERATION_UNKNOWN"
    [event] = env.denied
    assert (event["decision"], event["reason_code"]) == ("deny", "INTERNAL_OPERATION_UNKNOWN")


def test_a_write_whose_record_cannot_be_written_is_not_made(ns, env):
    under_token(ns, env, ["read", "build"])
    env.autocommit_raises = RuntimeError("outbox down")
    with pytest.raises(obo(ns).OperationRefused) as err:
        obo(ns).check_operation("build", "POST /api/v2/x/1", "post_x")
    assert err.value.code == "AUDIT_UNAVAILABLE"


def test_a_request_without_a_run_token_is_not_touched(ns, env):
    env.in_request = True
    obo(ns).check_operation(None, "POST /api/v2/configurations/configurations/1", "post")
    assert env.denied == [] and env.tracing_calls == 0


# --- audit only and off -------------------------------------------------------------------------------------------

def test_audit_only_records_the_pat_path_of_the_internal_mcp_as_a_would_deny(ns, env):
    seed(env)
    call = call_kwargs(internal_mcp=True)
    call["kwargs"]["tools"][0]["settings"]["headers"] = {"Authorization": "Bearer pat-value"}
    before = copy.deepcopy(call)
    assert ns.adm.admit_dispatch(env.module(governed=False, audit=True), "indexer_agent", call, env.request()) is None
    legacy = [e for e in env.denied if e["reason_code"] == "LEGACY_PAT_INTERNAL_MCP"]
    assert len(legacy) == 1 and legacy[0]["would_deny"] is True and legacy[0]["decision"] == "allow"
    assert call["kwargs"].get("tools") == before["kwargs"]["tools"] and obo(ns).TOKEN_KWARG not in call["kwargs"]


def test_audit_only_without_internal_tools_records_no_such_event(ns, env):
    seed(env)
    ns.adm.admit_dispatch(env.module(governed=False, audit=True), "indexer_agent", call_kwargs(), env.request())
    assert not [e for e in env.denied if e["reason_code"] == "LEGACY_PAT_INTERNAL_MCP"]


def test_governance_off_changes_nothing_and_mints_nothing(ns, env):
    env.poison()
    call = call_kwargs(internal_mcp=True)
    call["kwargs"]["envelope"]["security_profile"] = "legacy"
    call["kwargs"]["tools"][0]["settings"]["headers"] = {"Authorization": "Bearer pat-value"}
    before = copy.deepcopy(call)
    assert ns.adm.admit_dispatch(env.module(governed=False), "indexer_agent", call, env.request()) is None
    assert call == before


# --- the binding's operations -------------------------------------------------------------------------------------

def test_the_admin_task_sets_the_operations_at_create_and_rebind_only(ns, env):
    values, error = ns.binding_admin.parse_binding_param(
        "project_id=1;application_id=7;version_id=70;mode=elitea_native;action=create;platform_operations=build,read")
    assert error is None and values["platform_operations"] == ["read", "build"]
    _, error = ns.binding_admin.parse_binding_param(
        "project_id=1;application_id=7;version_id=70;mode=elitea_native;action=activate;platform_operations=read")
    assert "create or action=rebind" in error
    _, error = ns.binding_admin.parse_binding_param(
        "project_id=1;application_id=7;version_id=70;mode=elitea_native;action=create;platform_operations=admin")
    assert error.startswith("invalid platform_operations")


def test_a_created_binding_keeps_its_operations_and_the_lifecycle_event_names_them(ns, env):
    env.add_version(1, 70, 7)
    env.digests[70] = "adv1:" + "1" * 64
    result = ns.binding_admin.manage_binding(
        env.module(), "project_id=1;application_id=7;version_id=70;mode=elitea_native;action=create;"
                      "platform_operations=read,build", 3)
    assert "error" not in result
    assert obo(ns).binding_operations(1, result["binding_ref"]) == ("read", "build")
    assert env.recorded[-1][1]["operation_id"] == "platform_operations:read,build"


def test_the_column_is_deferred_so_an_old_schema_is_still_read(ns):
    column = ns.models.AgentIdentityBinding.__mapper__.attrs["allowed_platform_operations"]
    assert column.deferred is True


def test_the_context_ttl_bounds_the_token(ns, env):
    _, _, call = admitted(ns, env, operations=["read"])
    claims = claims_of(token_of(ns, call))
    assert claims["exp"] - claims["iat"] <= env.module().admission_settings.context_ttl_s + 1


# --- development runs (#7213): runs once both are on the same tree --------------------------------------------------

def test_a_development_run_gets_the_token_with_the_author_and_its_digest(ns, env):
    if not hasattr(ns.pd_admission, "ENTRY_EDITOR_CHAT"):
        pytest.skip("development runs (#7213) are not on this tree")
    digest = "adv1:" + "7" * 64
    env.add_version(1, 70, 7)
    env.digests[70] = digest
    env.facts_version = 5
    env.permissions[42] = {"credentials.use", BUILD_PERMISSION, "models.applications.application.update"}
    call = call_kwargs(internal_mcp=True)
    context_ref = ns.adm.admit_dispatch(env.module(allow_development_runs=True), "indexer_agent", call,
                                        env.request(entry_point="editor_chat"))
    claims = claims_of(token_of(ns, call))
    assert (claims["sub"], claims["uid"], claims["act"], claims["ctx"]) == (
        "idp:tid-1:oid-42", 42, f"development:{digest}", context_ref)
    assert claims["scope"] == ["read", "build"]   # the author's RBAC alone: no credentials, no predict permission
    env.in_request = True
    assert verify(ns, env, token_of(ns, call)) == {"ok": True, "user_id": 42}


# --- round 2: the path as the client sends it (C9-SEC-26) ---------------------------------------------------------

DISGUISED = [
    "http://pylon_main:8080/x/../app/2/mcp/elitea_core/applications",
    "http://pylon_main:8080/./app/2/mcp/elitea_core/applications",
    "http://pylon_main:8080/app/2/%6Dcp/elitea_core/applications",
    "http://pylon_main:8080//app//2/mcp/elitea_core/applications",
    "http://pylon_main:8080/app/2/%256Dcp",
]


@pytest.mark.parametrize("url", DISGUISED)
def test_a_disguised_internal_path_is_recognised(ns, url):
    assert obo(ns).has_internal_mcp({"tools": [{"type": "mcp", "settings": {"url": url}}]})


@pytest.mark.parametrize("path,expected", [
    ("/x/../app/2/mcp", "/app/2/mcp"), ("/./app/2/mcp/", "/app/2/mcp/"), ("/app/2/%6Dcp", "/app/2/mcp"),
    ("//app///2/mcp", "/app/2/mcp"), ("/app/2/mcp/../../../etc", "/etc"), ("", "/"), ("\\app\\2\\mcp", "/app/2/mcp"),
])
def test_the_client_path_resolves_what_the_client_resolves(ns, path, expected):
    assert obo(ns).client_path(path) == expected


@pytest.mark.parametrize("url", DISGUISED)
def test_under_enforce_a_disguised_internal_toolkit_loses_the_pasted_token(ns, env, url):
    seed(env)
    call = call_kwargs(tools=[internal_tool(url=url, headers={"Authorization": "Bearer pat-value"})])
    ns.adm.admit_dispatch(env.module(), "indexer_agent", call, env.request())
    assert call["kwargs"]["tools"][0]["settings"]["headers"] == {}
    assert obo(ns).TOKEN_KWARG in call["kwargs"]
    stripped = [e for e in env.denied if e["reason_code"] == "INTERNAL_MCP_OWN_AUTH"]
    assert len(stripped) == 1 and stripped[0]["event_type"] == "decision" and not stripped[0].get("would_deny")


def test_under_enforce_any_mcp_toolkit_on_the_platforms_origin_loses_its_own_auth_whatever_the_path(ns, env):
    seed(env)
    other_route = {"type": "mcp", "id": 3, "settings": {
        "url": "http://pylon_main:8080/api/v2/elitea_core/applications/prompt_lib/2",
        "headers": {"Authorization": "Bearer pat-value", "Cookie": "session=x", "Proxy-Authorization": "Basic x",
                    "X-Trace": "1"}}}
    external = {"type": "mcp", "id": 4, "settings": {"url": "https://mcp.example.com/mcp",
                                                     "headers": {"Authorization": "Bearer external-token"}}}
    call = call_kwargs(tools=[other_route, external])
    ns.adm.admit_dispatch(env.module(), "indexer_agent", call, env.request())
    assert call["kwargs"]["tools"][0]["settings"]["headers"] == {"X-Trace": "1"}
    assert call["kwargs"]["tools"][1]["settings"]["headers"] == {"Authorization": "Bearer external-token"}


def test_under_enforce_a_disguised_internal_path_on_another_origin_refuses_the_run(ns, env):
    seed(env)
    call = call_kwargs(tools=[internal_tool(url="http://10.0.0.7:8080/x/../app/2/mcp/secrets",
                                            headers={"Authorization": "Bearer pat-value"})])
    with pytest.raises(ns.exceptions.AdmissionDenied) as err:
        ns.adm.admit_dispatch(env.module(), "indexer_agent", call, env.request())
    assert err.value.reason_code == "INTERNAL_MCP_ORIGIN_NOT_ALLOWED"


def test_audit_only_records_what_enforce_would_strip_or_refuse(ns, env):
    seed(env)
    tools = [internal_tool(url="http://pylon_main:8080/x/../app/2/mcp/secrets", headers={"Authorization": "Bearer p"}),
             internal_tool(url="http://10.0.0.7:8080/app/2/mcp/secrets")]
    call = call_kwargs(tools=tools)
    before = copy.deepcopy(call)
    ns.adm.admit_dispatch(env.module(governed=False, audit=True), "indexer_agent", call, env.request())
    codes = sorted(e["reason_code"] for e in env.denied if e.get("would_deny"))
    assert codes == ["INTERNAL_MCP_ORIGIN_NOT_ALLOWED", "INTERNAL_MCP_OWN_AUTH", "LEGACY_PAT_INTERNAL_MCP"]
    assert call["kwargs"]["tools"] == before["kwargs"]["tools"]   # audit only changes nothing


def test_audit_only_does_not_flag_the_plain_injected_toolkit_twice(ns, env):
    seed(env)
    call = call_kwargs(tools=[internal_tool(headers={"Authorization": "Bearer injected-pat"})])
    ns.adm.admit_dispatch(env.module(governed=False, audit=True), "indexer_agent", call, env.request())
    assert sorted(e["reason_code"] for e in env.denied if e.get("would_deny")) == ["LEGACY_PAT_INTERNAL_MCP"]


# --- round 2: projects named in the arguments (C9-SEC-22 residual) ------------------------------------------------

PARTICIPANT = {"entity_name": "application", "entity_meta": {"id": 7, "project_id": 9}, "entity_settings": {}}


@pytest.mark.parametrize("arguments,ok", [
    ({"project_id": 1, "participants": [dict(PARTICIPANT, entity_meta={"id": 7, "project_id": 5})]}, True),
    ({"project_id": 1, "participants": [PARTICIPANT]}, False),                       # an agent of project 9
    ({"participants": json.dumps([PARTICIPANT])}, False),                            # the same as a JSON string
    ({"participants": [{"entity_meta": {"project_id": "9"}}]}, False),
    ({"participants": [{"entity_meta": {"project_id": "nine"}}]}, False),            # unreadable: refused
    ({"model_project_id": 9, "llm_settings": {"model_project_id": 1}}, True),        # other keys are not projects
])
def test_a_participant_of_a_project_outside_the_targets_is_refused(ns, env, arguments, ok):
    under_token(ns, env, ["read", "build", "run"])
    target = "POST /api/v2/elitea_core/participants/prompt_lib/1/12"
    if ok:
        obo(ns).check_targets(arguments, target, "post_elitea_core_participants")
        assert env.denied == []
        return
    with pytest.raises(obo(ns).OperationRefused) as err:
        obo(ns).check_targets(arguments, target, "post_elitea_core_participants")
    assert err.value.code == "INTERNAL_TARGET_DENIED"
    [event] = env.denied
    assert (event["decision"], event["reason_code"], event["operation_id"]) == (
        "deny", "INTERNAL_TARGET_DENIED", "target:post_elitea_core_participants")


def test_arguments_too_deep_or_too_large_are_refused_not_half_checked(ns, env):
    under_token(ns, env, ["read", "build"])
    deep = {}
    node = deep
    for _ in range(20):
        node["a"] = {}
        node = node["a"]
    with pytest.raises(obo(ns).OperationRefused):
        obo(ns).check_targets(deep, "POST x", "t")
    with pytest.raises(obo(ns).OperationRefused):
        obo(ns).check_targets({"items": list(range(5000))}, "POST x", "t")


def test_targets_are_not_read_without_a_run_token(ns, env):
    env.in_request = True
    obo(ns).check_targets({"participants": [PARTICIPANT]}, "POST x", "t")
    assert env.denied == []


# --- round 2: the map against the live registry and at startup ------------------------------------------------------

REGISTRY = json.loads((PLUGIN_ROOT / "tests" / "fixtures" / "mcp_registry_writes_20261026.json").read_text())


def test_every_write_the_live_registry_exposes_is_in_the_map(ns):
    """``mcp_registry_writes_20261026.json`` is the live registry of the local stack (tools/list of every MCP tag with a
    run token, each name matched to its OpenAPI path). A write tool there without its line in the map would be refused
    at run time; this fails instead. Regenerate the snapshot when an MCP-exposed write is added."""
    exposed = {name for names in REGISTRY["tools_per_tag"].values() for name in names if not name.startswith("get_")}
    assert exposed == set(REGISTRY["write_tools"])
    unmapped = []
    for name in sorted(exposed):
        candidates = [entry.split(" ", 1) for entry in REGISTRY["write_tools"][name]]
        if not any(obo(ns).operation_of(method, path) for method, path in candidates):
            unmapped.append(name)
    assert unmapped == []
    stale = set(obo(ns).ENDPOINT_OPERATIONS) - {
        tuple(entry.split(" ", 1)) for entries in REGISTRY["write_tools"].values() for entry in entries}
    assert stale == set()


def test_the_startup_check_names_unmapped_writes(ns):
    tools = [{"method": "post", "path": "/api/v2/elitea_core/applications/{mode}/{project_id}"},
             {"method": "get", "path": "/api/v2/x/{project_id}"},
             {"method": "post", "path": "/api/v2/elitea_core/new_thing/{mode}/{project_id}"}]
    assert obo(ns).unmapped_writes(tools) == ["POST elitea_core/new_thing/{mode}/{project_id}"]


# --- round 2: log volume ------------------------------------------------------------------------------------------

def test_forged_tokens_are_counted_but_logged_at_most_once_per_window(ns, env):
    obo(ns)._UNVERIFIED_LOG.update(at=0.0, suppressed=0)   # pylint: disable=W0212
    token, _ = mint(ns)
    for _ in range(50):
        verify(ns, env, token[:-2] + "AA")
    lines = [line for line in env.log.lines if "internal_token_verification_total" in line]
    assert len(lines) == 1
    assert obo(ns)._COUNTER[("deny", "INTERNAL_TOKEN_INVALID")] >= 50   # pylint: disable=W0212


def test_a_verified_request_writes_no_log_line(ns, env):
    _, _, call = admitted(ns, env, operations=["read", "build"])
    env.log.lines.clear()
    assert verify(ns, env, token_of(ns, call))["ok"]
    assert not [line for line in env.log.lines if "internal_token_verification_total" in line]
