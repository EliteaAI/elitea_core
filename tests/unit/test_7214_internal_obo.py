"""C-1009 (#7214): the internal Elitea MCP as a governed resource - the run token Main mints at admission, its
verification on the MCP side, the scope, the audit of platform writes, audit only and governance off.

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
INTERNAL_URL = "http://pylon_main:8080/app/5/mcp/elitea_core/applications?scope_project_id=1"
MCP_ENDPOINT = "elitea_core.client_tag_filtered_streamable_http"


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
    yield ns.env
    for name in ("SECRETS_MASTER_KEY", "APP_HOST"):
        vars(ns.tools.config).pop(name, None)


def obo(ns):
    return ns.internal_obo


def call_kwargs(run_id=RUN, internal_mcp=False, **extra):
    payload = {"project_id": 1, "message_id": 5, "_elitea_predict_run_id": run_id,
               "envelope": {"schema_version": 1, "security_profile": "governed",
                            "required_capabilities": ["task_envelope.v1"], "context_ref": None}}
    if internal_mcp:
        payload["tools"] = [{"type": "mcp", "id": None, "name": "Elitea Applications",
                             "settings": {"url": INTERNAL_URL, "headers": {}}}]
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


def admitted(ns, env, operations=None, permissions=(BUILD_PERMISSION,), **extra):
    binding_id = seed(env, operations)
    env.permissions[42] = {"credentials.use", *permissions}
    call = call_kwargs(**extra)
    context_ref = ns.adm.admit_dispatch(env.module(), "indexer_agent", call, env.request())
    return binding_id, context_ref, call


def claims_of(token):
    body = token[len("eio1."):].split(".")[0]
    return json.loads(base64.urlsafe_b64decode(body + "=" * (-len(body) % 4)))


def verify(ns, env, token, endpoint=MCP_ENDPOINT, environ=None, now=None):
    return obo(ns).verify_request(token, environ or {}, endpoint=endpoint, module=env.module(), now=now)


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
    assert claims["exp"] == int(ns.adm._aware(row.expires_at).timestamp())   # the context's own expiry
    assert delivered["origins"] == ["http://localhost", "http://pylon_main:8080"]
    # the token is in the task kwargs only: never in the meta, the envelope or a toolkit's settings
    assert obo(ns).TOKEN_KWARG not in call["meta"] and "eio1." not in json.dumps(call["kwargs"]["envelope"])


def test_the_token_never_reaches_a_log_line_or_an_audit_event(ns, env):
    _, _, call = admitted(ns, env, operations=["read", "build"])
    token = call["kwargs"][obo(ns).TOKEN_KWARG]["token"]
    assert not [line for line in env.log.lines if token in line or "eio1." in line]
    assert not [event for _, event in env.recorded if "eio1." in json.dumps(event)]


def test_a_binding_without_a_list_allows_read_only(ns, env):
    _, _, call = admitted(ns, env, operations=None)
    assert claims_of(call["kwargs"][obo(ns).TOKEN_KWARG]["token"])["scope"] == ["read"]


def test_the_scope_is_the_binding_list_intersected_with_the_users_rbac(ns, env):
    _, _, call = admitted(ns, env, operations=["read", "build", "credentials", "run"], permissions=(BUILD_PERMISSION,))
    # the user may build but has neither a credential nor the predict permission: those stay out
    assert claims_of(call["kwargs"][obo(ns).TOKEN_KWARG]["token"])["scope"] == ["read", "build"]


def test_an_operation_the_user_may_do_is_still_out_of_scope_when_the_binding_does_not_allow_it(ns, env):
    _, _, call = admitted(ns, env, operations=["read", "build"],
                          permissions=(BUILD_PERMISSION, CREDENTIAL_PERMISSION))
    assert "credentials" not in claims_of(call["kwargs"][obo(ns).TOKEN_KWARG]["token"])["scope"]


def test_a_development_run_is_scoped_by_the_authors_rbac_alone(ns, env):
    env.permissions[42] = {CREDENTIAL_PERMISSION}
    assert obo(ns).scope_for(1, "development:adv1:" + "0" * 64, 42) == ["read", "credentials"]


def test_minting_costs_one_select_of_the_binding_and_one_signature(ns, env):
    seed(env, ["read", "build"])
    env.permissions[42] = {"credentials.use", BUILD_PERMISSION}
    seen = []
    engine = env.engine(1)
    listener = lambda conn, cursor, statement, params, context, many: seen.append(statement)   # noqa: E731
    view = ns.adm.admit_execution(env.module(), env.request(), run_id=RUN)
    ns.sa.event.listen(engine, "before_cursor_execute", listener)
    started = time.perf_counter()
    obo(ns).mint_for_admission(view, "indexer_agent")
    elapsed = time.perf_counter() - started
    ns.sa.event.remove(engine, "before_cursor_execute", listener)
    assert len(seen) == 1 and "allowed_platform_operations" in seen[0] and seen[0].lstrip().upper().startswith("SELECT")
    assert elapsed < 0.05


def test_without_a_key_nothing_is_read_and_a_run_without_internal_tools_goes_on_without_a_token(ns, env):
    vars(ns.tools.config).pop("SECRETS_MASTER_KEY")
    seed(env)
    call = call_kwargs()
    assert ns.adm.admit_dispatch(env.module(), "indexer_agent", call, env.request())
    assert obo(ns).TOKEN_KWARG not in call["kwargs"]


def test_a_run_with_internal_tools_and_no_token_is_refused_and_its_context_ended(ns, env):
    vars(ns.tools.config).pop("SECRETS_MASTER_KEY")
    seed(env)
    call = call_kwargs(internal_mcp=True)
    with pytest.raises(ns.exceptions.AdmissionDenied) as err:
        ns.adm.admit_dispatch(env.module(), "indexer_agent", call, env.request())
    assert err.value.reason_code == "INTERNAL_TOKEN_UNAVAILABLE" and "Try again later" in err.value.message
    [row] = env.query(1, ns.models.ExecutionSecurityContext)
    assert (row.state, row.revoked_reason) == ("revoked", "NOT_DISPATCHED")
    assert [e["reason_code"] for e in env.denied] == ["INTERNAL_TOKEN_UNAVAILABLE"]
    assert "envelope" in call["kwargs"] and call["kwargs"]["envelope"]["context_ref"] is None


def test_a_token_the_caller_put_on_the_payload_is_dropped(ns, env):
    seed(env)
    vars(ns.tools.config).pop("SECRETS_MASTER_KEY")
    call = call_kwargs(**{"internal_run_token": {"token": "eio1.forged.x", "origins": ["https://evil.example"]}})
    ns.adm.admit_dispatch(env.module(), "indexer_agent", call, env.request())
    assert "internal_run_token" not in call["kwargs"]


def test_a_child_dispatch_gets_a_token_of_its_parents_context(ns, env):
    binding_id, context_ref, _ = admitted(ns, env, operations=["read", "build"])
    child = call_kwargs()
    child["kwargs"]["envelope"]["context_ref"] = context_ref
    assert ns.adm.admit_dispatch(env.module(), "indexer_agent", child, None) is None
    claims = claims_of(child["kwargs"][obo(ns).TOKEN_KWARG]["token"])
    assert (claims["ctx"], claims["act"], claims["uid"], claims["run"], claims["scope"]) == (
        context_ref, str(binding_id), 42, RUN, ["read", "build"])


def test_a_child_of_an_unknown_context_with_internal_tools_is_refused(ns, env):
    seed(env)
    child = call_kwargs(internal_mcp=True)
    child["kwargs"]["envelope"]["context_ref"] = str(uuid.uuid4())
    with pytest.raises(ns.exceptions.AdmissionDenied):
        ns.adm.admit_dispatch(env.module(), "indexer_agent", child, None)


# --- the token itself --------------------------------------------------------------------------------------------

def mint(ns, **over):
    values = dict(sub="idp:t:o", uid=42, act=str(uuid.uuid4()), scope=["read"], ctx=str(uuid.uuid4()), prj=1,
                  run=RUN, tsk="indexer_agent", exp=int(time.time()) + 600)
    values.update(over)
    return obo(ns).mint(**values)


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


def test_a_token_signed_with_another_key_or_for_another_audience_is_invalid(ns, env):
    token, _ = mint(ns)
    ns.tools.config.SECRETS_MASTER_KEY = "another-key"
    with pytest.raises(ValueError, match="INTERNAL_TOKEN_INVALID"):
        obo(ns).parse(token, obo(ns).signing_key())
    ns.tools.config.SECRETS_MASTER_KEY = KEY
    key = obo(ns).signing_key()
    claims = {**claims_of(token), "aud": "elitea:other"}
    body = base64.urlsafe_b64encode(json.dumps(claims).encode()).decode().rstrip("=")
    with pytest.raises(ValueError, match="INTERNAL_TOKEN_INVALID"):
        obo(ns).parse(f"eio1.{body}.{obo(ns)._mac(key, body)}", key)   # pylint: disable=W0212


def test_an_expired_token_says_so(ns, env):
    token, _ = mint(ns, exp=int(time.time()) - 1)
    reply = verify(ns, env, token)
    assert (reply["ok"], reply["code"], reply["status"]) == (False, "INTERNAL_TOKEN_EXPIRED", 401)


def test_no_key_refuses_every_token(ns, env):
    token, _ = mint(ns)
    vars(ns.tools.config).pop("SECRETS_MASTER_KEY")
    assert verify(ns, env, token)["code"] == "INTERNAL_TOKEN_INVALID"


# --- verification on the MCP side ---------------------------------------------------------------------------------

def test_a_live_runs_token_is_accepted_on_the_mcp_route_as_its_user(ns, env):
    _, context_ref, call = admitted(ns, env, operations=["read", "build"])
    env.in_request = True
    reply = verify(ns, env, call["kwargs"][obo(ns).TOKEN_KWARG]["token"])
    assert reply == {"ok": True, "user_id": 42}
    assert env.g.internal_obo["ctx"] == context_ref and env.g.internal_obo["act"]


def test_the_token_is_refused_on_any_other_route(ns, env):
    _, _, call = admitted(ns, env, operations=["read", "build"])
    token = call["kwargs"][obo(ns).TOKEN_KWARG]["token"]
    for endpoint in ("api.v2.elitea_core.applications", "elitea_core.client_connected", None):
        reply = verify(ns, env, token, endpoint=endpoint)
        assert (reply["ok"], reply["code"], reply["status"]) == (False, "INTERNAL_TOKEN_INVALID", 403)


def test_the_in_process_api_call_of_a_verified_endpoint_is_accepted_without_a_second_validation(ns, env):
    _, _, call = admitted(ns, env, operations=["read", "build"])
    token = call["kwargs"][obo(ns).TOKEN_KWARG]["token"]
    claims = claims_of(token)
    env.epoch_calls.clear()
    environ = {"elitea.internal_mcp_request": True, obo(ns).VERIFIED_ENVIRON_KEY: claims}
    assert verify(ns, env, token, endpoint="api.v2.elitea_core.applications", environ=environ)["ok"]
    assert env.epoch_calls == []   # no validate_context
    # a marker of another token (or none) does not open the API to this one
    other = {"elitea.internal_mcp_request": True, obo(ns).VERIFIED_ENVIRON_KEY: {**claims, "jti": "x"}}
    assert verify(ns, env, token, endpoint="api.v2.x", environ=other)["code"] == "INTERNAL_TOKEN_INVALID"
    assert verify(ns, env, token, endpoint="api.v2.x", environ={"elitea.internal_mcp_request": True})["ok"] is False


def test_a_token_of_a_revoked_context_is_refused(ns, env):
    binding_id, context_ref, call = admitted(ns, env, operations=["read", "build"])
    with env.get_session(1) as session:
        ns.adm.revoke_binding_contexts(session, binding_id)
        session.commit()
    reply = verify(ns, env, call["kwargs"][obo(ns).TOKEN_KWARG]["token"])
    assert (reply["ok"], reply["code"], reply["status"]) == (False, "CONTEXT_NOT_LIVE", 403)
    assert "no longer admitted" in reply["message"]


def test_a_token_of_another_context_is_refused(ns, env):
    _, context_ref, call = admitted(ns, env, operations=["read", "build"])
    claims = claims_of(call["kwargs"][obo(ns).TOKEN_KWARG]["token"])
    other, _ = obo(ns).mint(**{**{k: claims[k] for k in ("sub", "uid", "act", "scope", "prj", "run", "tsk", "exp")},
                               "ctx": str(uuid.uuid4())})
    assert verify(ns, env, other)["code"] == "CONTEXT_NOT_LIVE"
    other_run, _ = obo(ns).mint(**{**{k: claims[k] for k in ("sub", "uid", "act", "scope", "prj", "tsk", "exp", "ctx")},
                                   "run": "another-run"})
    assert verify(ns, env, other_run)["code"] == "CONTEXT_NOT_LIVE"


def test_a_token_after_the_user_was_suspended_is_refused(ns, env):
    _, _, call = admitted(ns, env, operations=["read", "build"])
    env.epochs[42] = {"active": False, "user": 3, "token": 0}
    assert verify(ns, env, call["kwargs"][obo(ns).TOKEN_KWARG]["token"])["code"] == "CONTEXT_NOT_LIVE"


# --- scope and audit ----------------------------------------------------------------------------------------------

@pytest.mark.parametrize("method,path,operation", [
    ("GET", "/api/v2/secrets/secret/2", "read"),
    ("POST", "/api/v2/elitea_core/applications/prompt_lib/2", "build"),
    ("PUT", "/api/v2/elitea_core/toolkits/prompt_lib/2", "build"),
    ("POST", "/api/v2/configurations/configurations/2", "credentials"),
    ("DELETE", "/api/v2/secrets/secret/2/name", "credentials"),
])
def test_operations_are_classified_by_method_and_plugin(ns, method, path, operation):
    assert obo(ns).operation_of(method, path) == operation


def under_token(ns, env, scope):
    env.in_request = True
    token, claims = mint(ns, scope=scope)
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
        obo(ns).check_operation("credentials", "POST /api/v2/configurations/configurations/1", "post_configurations")
    assert err.value.code == "INTERNAL_SCOPE_DENIED"
    assert obo(ns).refusal_text(err.value).startswith("INTERNAL_SCOPE_DENIED: This agent is not allowed")
    [event] = env.denied
    assert (event["decision"], event["reason_code"]) == ("deny", "INTERNAL_SCOPE_DENIED")


def test_a_write_whose_record_cannot_be_written_is_not_made(ns, env):
    under_token(ns, env, ["read", "build"])
    env.autocommit_raises = RuntimeError("outbox down")
    with pytest.raises(obo(ns).OperationRefused) as err:
        obo(ns).check_operation("build", "POST /api/v2/x/1", "post_x")
    assert err.value.code == "AUDIT_UNAVAILABLE"


def test_a_request_without_a_run_token_is_not_touched(ns, env):
    env.in_request = True
    obo(ns).check_operation("credentials", "POST /api/v2/configurations/configurations/1", "post")
    assert env.denied == [] and env.tracing_calls == 0


# --- audit only and off -------------------------------------------------------------------------------------------

def test_audit_only_records_the_pat_path_of_the_internal_mcp_as_a_would_deny(ns, env):
    seed(env)
    call = call_kwargs(internal_mcp=True)
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


# --- what a payload carries ---------------------------------------------------------------------------------------

@pytest.mark.parametrize("tool,expected", [
    ({"type": "mcp", "settings": {"url": INTERNAL_URL}}, True),
    ({"type": "mcp_elitea", "settings": {"url": "https://x.example/app/2/mcp/secrets"}}, True),
    ({"type": "mcp", "settings": {"url": "https://mcp.example.com/mcp"}}, False),
    ({"type": "github", "settings": {"url": INTERNAL_URL}}, False),
])
def test_internal_mcp_toolkits_are_recognised_by_their_endpoint(ns, tool, expected):
    assert obo(ns).has_internal_mcp({"tools": [tool]}) is expected
    assert obo(ns).has_internal_mcp({"application": {"version_details": {"tools": [tool]}}}) is expected


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
    claims = claims_of(call["kwargs"][obo(ns).TOKEN_KWARG]["token"])
    ttl = env.module().admission_settings.context_ttl_s
    assert claims["exp"] - claims["iat"] <= ttl + 1
