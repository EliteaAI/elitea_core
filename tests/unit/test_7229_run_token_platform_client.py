"""C-1009 (#7229 item 1, C9-SEC-30, matrix row D15): the run token is the run's one platform identity.

- Main accepts the run token on the routes of the run's own platform client only (``internal_obo.RUN_ROUTES``), in the
  run's project, while its context lives: the authorization matrix below.
- Under enforce an admitted interactive run of a signed-in person carries no personal or system token or session: Main
  strips them and delivers a run token minted with nothing read. PAT callers, mandates and governance off are untouched.
- A sub-agent version the runtime fetches on its run token is sealed as a dispatch under the run would be (C9-SEC-30).
- A plain LLM chat whose toolkits carry a credential is a development run of the person (D15).

Real admission and token code on SQLite (``fixtures/admission_env.py``), as test_7214 and test_7227.
"""
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
from fixtures.governed_credentials import (  # noqa: E402  pylint: disable=C0413
    CALLER, RUN, SECRET, SUBJECT, Credential, call_kwargs, dumped, payload_of,
)

KEY = "test-master-key-not-a-secret"
EDIT = "models.applications.application.update"
DIGEST = "adv1:" + "7" * 64
CHILD_DIGEST = "adv1:" + "8" * 64
LLM_TASK = "indexer_predict_agent"
TOOLKIT_TASK = "indexer_test_toolkit_tool"
PAT = "pat-value-not-a-secret"
SESSION = "session-reference-not-a-secret"


@pytest.fixture(scope="module")
def ns(tmp_path_factory):
    saved = dict(sys.modules)
    yield admission_env.install(PLUGIN_ROOT, tmp_path_factory.mktemp("run_token_client"))
    sys.modules.clear()
    sys.modules.update(saved)


@pytest.fixture
def env(ns):
    ns.env.reset()
    ns.env.facts_version = 5
    ns.env.permissions[42] = {"credentials.use", EDIT}
    ns.tools.config.SECRETS_MASTER_KEY = KEY
    ns.tools.config.APP_HOST = "http://localhost"
    ns.tools.this.module = ns.env.module()
    ns.tools.auth.epoch_ttl = 5.0
    ns.internal_obo._VERDICTS.clear()   # pylint: disable=W0212
    ns.internal_obo._EXPIRED_AUDITED.clear()   # pylint: disable=W0212
    yield ns.env
    for name in ("SECRETS_MASTER_KEY", "APP_HOST"):
        vars(ns.tools.config).pop(name, None)


def obo(ns):
    return ns.internal_obo


def with_tokens(payload):
    """What the payload builders put on a run today: a PAT in the LLM settings and on top, and a session."""
    payload = copy.deepcopy(payload)
    payload["api_token"] = PAT
    payload["llm"] = {"kwargs": {"model": "m", "api_key": PAT, "auth_session": SESSION, "project_id": 1}}
    return payload


def admitted_run(ns, env, payload=None, task="indexer_agent", **request):
    env.seed_agent()
    call = call_kwargs(with_tokens(payload or {"application": {"version_details": {"tools": []}}}),
                       project_auth_token=PAT)
    ref = ns.adm.admit_dispatch(env.module(), task, call, env.request(**request))
    return call, ref


def token_of(call):
    return call["kwargs"]["internal_run_token"]["token"]


def claims_of(token):
    import base64  # pylint: disable=C0415
    body = token[len("eio1."):].split(".")[0]
    return json.loads(base64.urlsafe_b64decode(body + "=" * (-len(body) % 4)))


def verify(ns, env, token, endpoint, method="GET", view_args=None, args=None, headers=None):
    environ = {"REQUEST_METHOD": method, **{f"HTTP_{k.upper().replace('-', '_')}": v for k, v in (headers or {}).items()}}
    return obo(ns).verify_request(token, environ, endpoint=endpoint, module=env.module(),
                                  view_args={} if view_args is None else view_args, args={} if args is None else args)


def mint(ns, **over):
    values = dict(sub="idp:t:o", uid=42, act=str(uuid.uuid4()), scope=[], ctx=str(uuid.uuid4()), prj=1, tgt=[],
                  run=RUN, tsk="indexer_agent", exp=int(time.time()) + 600)
    values.update(over)
    return obo(ns).mint(**values)


# --- the authorization matrix of the run token on the platform's own routes ----------------------------------------

RUN_REQUESTS = [   # (endpoint, method, view_args, args, headers): what the run's own client sends, in project 1
    ("api.v2.elitea_core.version", "PATCH", {"mode": "prompt_lib", "project_id": 1, "application_id": 8, "version_id": 80},
     {}, {}),
    ("api.v2.elitea_core.application", "GET", {"mode": "prompt_lib", "project_id": 1, "application_id": 8}, {}, {}),
    ("api.v2.elitea_core.tools_list", "GET", {"project_id": 1}, {}, {}),
    ("api.v2.elitea_core.tools_call", "POST", {"project_id": 1}, {}, {}),
    ("api.v2.elitea_core.tool_spend_check", "POST", {"mode": "prompt_lib", "project_id": 1}, {}, {}),
    ("api.v2.elitea_core.tool_spend_record", "POST", {"mode": "prompt_lib", "project_id": 1}, {}, {}),
    ("api.v2.configurations.models", "GET", {"project_id": 1}, {"include_shared": "true"}, {}),
    ("api.v2.artifacts.buckets", "POST", {"project_id": 1}, {}, {}),
    ("api.v2.artifacts.artifacts", "POST", {"mode": "default", "project_id": 1, "bucket": "b"}, {}, {}),
    ("api.v2.artifacts.artifact", "GET", {"mode": "default", "project_id": 1, "bucket": "b", "filename": "f"}, {}, {}),
    ("artifacts.s3_object_operations", "PUT", {"bucket": "b", "key": "k"}, {"project_id": "1"}, {}),
    ("artifacts.s3_bucket_operations", "GET", {"bucket": "b"}, {"project_id": "1", "format": "json"}, {}),
    ("runtime_interface_litellm.litellm_route_http__url", "POST", {"url": "v1/chat/completions"}, {},
     {"X-Project-Id": "1"}),
    ("runtime_interface_litellm.litellm_route_http__url", "POST", {"url": "v1/messages"}, {},
     {"OpenAI-Organization": "1"}),
    ("runtime_interface_litellm.litellm_route_http__url", "POST", {"url": "v1/embeddings"}, {},
     {"OpenAI-Organization": "1"}),
    ("runtime_interface_litellm.litellm_route_http__url", "POST", {"url": "v1/images/generations"}, {},
     {"X-Project-Id": "1"}),
    ("api.v2.secrets.secret", "GET", {"project_id": 1, "secret": "pgvector_project_connstr"}, {}, {}),
]


@pytest.mark.parametrize("endpoint,method,view_args,args,headers", RUN_REQUESTS)
def test_the_run_token_is_accepted_on_the_runs_own_routes_in_its_project_as_its_user(
        ns, env, endpoint, method, view_args, args, headers):
    call, ref = admitted_run(ns, env)
    env.in_request = True
    assert verify(ns, env, token_of(call), endpoint, method, view_args, args, headers) == {"ok": True, "user_id": 42}
    assert env.g.internal_obo["ctx"] == ref


@pytest.mark.parametrize("endpoint,method,view_args,args,headers", RUN_REQUESTS)
def test_the_run_token_acts_in_no_other_project(ns, env, endpoint, method, view_args, args, headers):
    call, _ = admitted_run(ns, env)

    def elsewhere(values):
        return {key: (7 if isinstance(value, int) else "7") if key in ("project_id", "X-Project-Id",
                                                                       "OpenAI-Organization") else value
                for key, value in values.items()}
    reply = verify(ns, env, token_of(call), endpoint, method, elsewhere(view_args), elsewhere(args), elsewhere(headers))
    assert (reply["ok"], reply["code"], reply["status"]) == (False, "INTERNAL_TARGET_DENIED", 403)


@pytest.mark.parametrize("headers", [{}, {"X-Project-Id": "x"}, {"X-Project-Id": "7", "OpenAI-Organization": "1"}])
def test_a_model_call_must_name_the_runs_project_as_the_proxy_reads_it(ns, env, headers):
    call, _ = admitted_run(ns, env)
    reply = verify(ns, env, token_of(call), obo(ns).LLM_ENDPOINT, "POST", {"url": "v1/chat/completions"}, {}, headers)
    assert reply["code"] == "INTERNAL_TARGET_DENIED"


@pytest.mark.parametrize("endpoint,method,view_args", [
    ("api.v2.auth.tokens", "POST", {"project_id": 1}),                         # creating or rotating a token
    ("api.v2.auth.token", "DELETE", {"project_id": 1}),
    ("api.v2.elitea_core.governance_settings", "PUT", {"mode": "administration"}),   # governance settings
    ("api.v2.admin.users", "GET", {"mode": "administration"}),                 # admin endpoints
    ("api.v2.secrets.secret", "GET", {"project_id": 1, "secret": "s"}),        # no secret is returned to a run
    ("api.v2.secrets.secret", "GET", {"project_id": 1, "secret": "pgvector_project_connstr_x"}),
    ("api.v2.secrets.secret", "PUT", {"project_id": 1, "secret": "pgvector_project_connstr"}),
    ("api.v2.configurations.configurations", "GET", {"project_id": 1}),
    ("api.v2.elitea_core.applications", "GET", {"mode": "prompt_lib", "project_id": 1}),   # listings
    ("api.v2.elitea_core.tool", "GET", {"mode": "prompt_lib", "project_id": 1, "tool_id": 2}),
    ("api.v2.elitea_core.public_application", "GET", {"application_id": 8}),   # another project's agent
    ("api.v2.elitea_core.version", "GET", {"mode": "prompt_lib", "project_id": 1, "application_id": 8, "version_id": 80}),
    ("api.v2.elitea_core.version", "PUT", {"mode": "prompt_lib", "project_id": 1, "application_id": 8, "version_id": 80}),
    ("api.v2.elitea_core.version", "PATCH", {"mode": "administration", "project_id": 1, "application_id": 8,
                                            "version_id": 80}),
    ("api.v2.artifacts.artifact", "PUT", {"mode": "default", "project_id": 1, "bucket": "b"}),
    ("runtime_interface_litellm.litellm_route_http__url", "POST", {"url": "key/generate"}),   # LiteLLM admin
    ("runtime_interface_litellm.litellm_route_http__url", "GET", {"url": "v1/chat/completions"}),
    ("artifacts.s3_move_objects", "POST", {"bucket": "b"}),
    (None, "GET", {}),
])
def test_the_run_token_is_refused_on_every_other_route_with_one_answer(ns, env, endpoint, method, view_args):
    call, _ = admitted_run(ns, env)
    env.denied.clear()
    reply = verify(ns, env, token_of(call), endpoint, method, view_args, {"project_id": "1"}, {"X-Project-Id": "1"})
    assert (reply["ok"], reply["code"], reply["status"]) == (False, "INTERNAL_TOKEN_INVALID", 403)
    assert reply["message"] == obo(ns).MESSAGES["INTERNAL_TOKEN_INVALID"]   # the same text whatever the route
    assert [e["reason_code"] for e in env.denied] == ["INTERNAL_TOKEN_INVALID"]


def test_an_expired_a_forged_and_a_revoked_run_token_are_refused(ns, env):
    route = RUN_REQUESTS[0][:3]
    expired, _ = mint(ns, now=time.time() - 600, exp=int(time.time()) - 1)
    assert (verify(ns, env, expired, *route)["code"], verify(ns, env, expired, *route)["status"]) == (
        "INTERNAL_TOKEN_EXPIRED", 401)
    call, ref = admitted_run(ns, env)
    token = token_of(call)
    forged = token[:-4] + ("AAAA" if not token.endswith("AAAA") else "BBBB")
    assert (verify(ns, env, forged, *route)["code"], verify(ns, env, forged, *route)["status"]) == (
        "INTERNAL_TOKEN_INVALID", 401)
    assert verify(ns, env, "eio1.not-a-token", *route)["code"] == "INTERNAL_TOKEN_INVALID"
    with env.get_session(1) as session:   # the run ended or was revoked
        ns.adm._set_state(session, uuid.UUID(ref), "revoked", "EPOCH_MISMATCH")  # pylint: disable=W0212
        session.commit()
    reply = verify(ns, env, token, *route)
    assert (reply["code"], reply["status"]) == ("CONTEXT_NOT_LIVE", 403)


def test_the_run_route_validates_the_context_once_per_verdict_and_writes_nothing(ns, env):
    call, ref = admitted_run(ns, env)
    with env.get_session(1) as session:   # the worker validated: the run is running
        ns.adm._set_state(session, uuid.UUID(ref), "running")  # pylint: disable=W0212
        session.commit()
    env.epoch_calls.clear()
    for _ in range(5):
        assert verify(ns, env, token_of(call), *RUN_REQUESTS[0][:3])["ok"]
    assert len(env.epoch_calls) == 1


def test_a_client_only_token_reaches_no_internal_mcp_route(ns, env):
    call, _ = admitted_run(ns, env)
    claims = claims_of(token_of(call))
    assert (claims["scope"], claims["tgt"]) == ([], [])
    reply = obo(ns).verify_request(token_of(call), {"REQUEST_METHOD": "POST"},
                                   endpoint="elitea_core.client_tag_filtered_streamable_http", module=env.module(),
                                   view_args={"project_id": 1}, args={})
    assert reply["code"] == "INTERNAL_TARGET_DENIED"


# --- reference-only identity: no long-lived token in an admitted run's payload ---------------------------------------

def token_shaped(value) -> list:
    """Every token-shaped value in a payload or meta: a PAT, a session, a bearer that is not the run token."""
    text = dumped(value)
    return [marker for marker in (PAT, SESSION) if marker in text]


@pytest.mark.parametrize("task,request_over", [
    ("indexer_agent", {}),
    ("indexer_agent", {"entry_point": "predict_api"}),   # a REST predict of a signed-in person (a session)
    (LLM_TASK, {"entry_point": "chat_sio_llm", "application_id": None, "version_id": None}),
])
def test_an_admitted_interactive_run_carries_no_platform_token_only_the_run_token(ns, env, task, request_over):
    call, ref = admitted_run(ns, env, task=task, **request_over)
    assert ref and token_shaped(call["kwargs"]) == [] and token_shaped(call["meta"]) == []
    delivered = call["kwargs"]["internal_run_token"]
    assert claims_of(delivered["token"])["ctx"] == ref
    assert delivered["origins"] == ["http://localhost", "http://pylon_main:8080"]


def test_a_toolkit_run_carries_no_platform_token_in_its_payload_or_meta(ns, env):
    cred = Credential(env, uuid.uuid4())
    tool = cred.tool()
    call = call_kwargs({"toolkit_config": tool, "tool_name": "t", "tool_params": {}, "project_auth_token": PAT,
                        "auth_session": SESSION}, toolkit_config=copy.deepcopy(tool), project_auth_token=PAT,
                       auth_session=SESSION)
    ref = ns.adm.admit_dispatch(env.module(allow_development_runs=True), TOOLKIT_TASK, call, env.request(
        entry_point="toolkit_test", application_id=None, version_id=None, toolkit_id=1))
    assert ref and token_shaped(call["kwargs"]) == [] and token_shaped(call["meta"]) == []
    assert claims_of(token_of(call))["tsk"] == TOOLKIT_TASK


def test_the_run_token_of_a_run_without_internal_tools_reads_nothing(ns, env):
    """The governed dispatch reads what it read before #7229: the client token is a signature, no read."""
    def cost(payload_tokens):
        env.reset()
        env.permissions[42] = {"credentials.use", EDIT}
        env.seed_agent()
        payload = {"application": {"version_details": {"tools": []}}}
        call = call_kwargs(with_tokens(payload) if payload_tokens else payload)
        env.permission_calls.clear()
        before = len(env.sessions)
        ns.adm.admit_dispatch(env.module(), "indexer_agent", call, env.request(caller_type=payload_tokens or "user"))
        return len(env.sessions) - before, list(env.permission_calls), "internal_run_token" in call["kwargs"]

    with_token = cost(None)            # a session user: stripped, a client token is minted
    kept = cost("token")               # an API token caller keeps its token: nothing is minted (as on main)
    assert with_token[2] is True and kept[2] is False
    assert with_token[:2] == kept[:2]   # the same sessions and permission lookups


def test_an_api_token_caller_keeps_its_token_and_gets_no_client_token(ns, env):
    env.seed_agent()
    call = call_kwargs(with_tokens({"application": {"version_details": {"tools": []}}}))
    ns.adm.admit_dispatch(env.module(), "indexer_agent", call, env.request(caller_type="token", entry_point="predict_api"))
    assert token_shaped(call["kwargs"]) == [PAT, SESSION] and "internal_run_token" not in call["kwargs"]


@pytest.mark.parametrize("governed,audit", [(False, False), (False, True)])
def test_governance_off_and_audit_only_leave_the_payload_as_it_is(ns, env, governed, audit):
    env.seed_agent()
    call = call_kwargs(with_tokens({"application": {"version_details": {"tools": []}}}), project_auth_token=PAT)
    before = copy.deepcopy(call)
    ns.adm.admit_dispatch(env.module(governed=governed, audit=audit), "indexer_agent", call, env.request())
    assert call["kwargs"] == before["kwargs"] and call["meta"]["project_auth_token"] == PAT


def test_a_child_with_no_platform_token_gets_a_client_token_of_its_context(ns, env):
    _call, ref = admitted_run(ns, env)
    child = call_kwargs({"application": {"version_details": {"tools": []}}}, context_ref=ref)
    assert ns.adm.admit_dispatch(env.module(), "indexer_agent", child, None) is None
    claims = claims_of(token_of(child))
    assert (claims["ctx"], claims["uid"], claims["scope"], claims["tgt"]) == (ref, 42, [], [])
    # a child that still carries a token of its own (a mandate's system token) is minted nothing
    kept = call_kwargs(with_tokens({"application": {"version_details": {"tools": []}}}), context_ref=ref)
    ns.adm.admit_dispatch(env.module(), "indexer_agent", kept, None)
    assert "internal_run_token" not in kept["kwargs"]


def _context_reads(env):
    """Statements that read ``execution_security_context`` from now on, and the function that stops counting."""
    seen = []
    engine = env.engine(1)
    listener = lambda conn, cursor, statement, params, context, many: seen.append(statement)   # noqa: E731
    env.ns.sa.event.listen(engine, "before_cursor_execute", listener)
    stop = lambda: env.ns.sa.event.remove(engine, "before_cursor_execute", listener)   # noqa: E731
    return seen, stop


def test_a_credential_child_mints_its_token_from_the_read_its_seal_made(ns, env):
    """#7294 P-11: the seal of a child that carries a credential reads the parent context; its run token is minted from
    that row, so the child reads the context once (main: twice, one session each). In a fan-out, once for all."""
    cred = Credential(env, env.seed_agent())
    call = call_kwargs(payload_of(cred))
    ref = ns.adm.admit_dispatch(env.module(), "indexer_agent", call, env.request())
    seen, stop = _context_reads(env)
    before = len(env.sessions)
    child = call_kwargs(payload_of(cred), context_ref=ref)
    assert ns.adm.admit_dispatch(env.module(), "indexer_agent", child, None) is None
    assert len(env.sessions) - before == 1
    assert len([text for text in seen if "execution_security_context" in text]) == 1
    claims = claims_of(token_of(child))
    assert (claims["ctx"], claims["uid"], claims["sub"], claims["run"]) == (ref, 42, SUBJECT, RUN)
    assert claims["exp"] == claims_of(token_of(call))["exp"] or claims["exp"] > time.time()
    before = len(env.sessions)
    with ns.adm.parent_entries_scope():
        children = [call_kwargs(payload_of(cred), context_ref=ref) for _ in range(4)]
        for item in children:
            ns.adm.admit_dispatch(env.module(), "indexer_agent", item, None)
    stop()
    assert len(env.sessions) - before == 1
    assert {claims_of(token_of(item))["ctx"] for item in children} == {ref}
    # a credential-free child is not sealed against the parent: its token still reads the context row itself
    before = len(env.sessions)
    free = call_kwargs({"application": {"version_details": {"tools": []}}}, context_ref=ref)
    ns.adm.admit_dispatch(env.module(), "indexer_agent", free, None)
    assert len(env.sessions) - before == 1 and claims_of(token_of(free))["ctx"] == ref


def test_without_a_signing_key_the_payload_keeps_its_token_and_nothing_is_minted(ns, env):
    """A deployment without ``SECRETS_MASTER_KEY`` has no run token: its runs keep the identity they had (main)."""
    vars(ns.tools.config).pop("SECRETS_MASTER_KEY")
    call, ref = admitted_run(ns, env)
    assert ref and token_shaped(call["kwargs"]) == [PAT, SESSION] and "internal_run_token" not in call["kwargs"]


def test_the_predict_and_toolkit_builders_look_no_token_up_for_a_run_that_will_carry_the_run_token():
    """The builders skip the lookup (no ``list_tokens``, no ``ensure_system_token``) for the same runs admission strips
    (``run_identity_applies``); the MCP connection test and the attachment read still get the user's token."""
    source = (PLUGIN_ROOT / "utils" / "predict_utils.py").read_text()
    assert source.count("run_identity=True") == 2   # the predict payload and the test-tool payload
    application = (PLUGIN_ROOT / "rpc" / "application.py").read_text()
    test = application[application.index("def test_toolkit_tool_sio("):application.index("def test_mcp_connection_sio(")]
    mcp_test = application[application.index("def test_mcp_connection_sio("):]
    assert "run_identity=not platform_read" in test and "run_identity" not in mcp_test.split("def ", 2)[1]


def test_get_predict_token_and_session_skips_the_lookup_when_the_run_token_applies(ns, monkeypatch):
    import ast  # pylint: disable=C0415
    source = (PLUGIN_ROOT / "utils" / "predict_utils.py").read_text()
    node = next(n for n in ast.parse(source).body
                if isinstance(n, ast.FunctionDef) and n.name == "get_predict_token_and_session")
    looked_up, applies = [], {"value": True}
    namespace = {"Optional": __import__("typing").Optional, "PredictPayloadError": RuntimeError,
                 "get_user_token": lambda user_id: looked_up.append(user_id) or PAT,
                 "resolve_auth_session": lambda user_id, sid: SESSION,
                 "run_identity_applies": lambda sid: applies["value"]}
    exec(compile(ast.Module(body=[node], type_ignores=[]), "predict_utils.py", "exec"), namespace)  # pylint: disable=W0122
    fn = namespace["get_predict_token_and_session"]
    assert fn(1, 42, "sid", run_identity=True) == (None, None) and looked_up == []
    assert fn(1, 42, "sid") == (PAT, None) and looked_up == [42]   # a caller that does not ask: as before
    applies["value"] = False
    assert fn(1, 42, "sid", run_identity=True) == (PAT, None)


# --- C9-SEC-30: a sub-agent version the runtime fetches on its run token --------------------------------------------

def app_tool(application_id, version_id, tool_id):
    return {"id": tool_id, "type": "application",
            "settings": {"application_id": application_id, "application_version_id": version_id}}


def prefetch(payload, key, *tools):
    payload["application"].setdefault("subagent_version_details", {})[key] = {
        "name": f"agent {key}", "description": "", "version_details": {"tools": list(tools)}}
    return payload


def draft(env, application_id, version_id, digest):
    env.add_version(1, version_id, application_id)
    env.digests[version_id] = digest


def draft_tree(env):
    """A draft parent 7:70 → draft child 8:80 (credential granted elsewhere) → bound grandchild 9:90."""
    draft(env, 7, 70, DIGEST)
    draft(env, 8, 80, CHILD_DIGEST)
    child = Credential(env, uuid.uuid4(), tool_id=2, config_id=6, version_id=80)
    env.add_version(1, 90, 9)
    binding = env.add_binding(1, 9, 90)
    grandchild = Credential(env, binding, tool_id=3, config_id=7, version_id=90)
    payload = prefetch({"application": {"version_details": {"tools": [app_tool(8, 80, 802)]}}}, "8:80",
                       child.tool(), app_tool(9, 90, 902))
    prefetch(payload, "9:90", grandchild.tool())
    return payload, child, grandchild


def dev_dispatch(env, ns, payload, **request):
    request.setdefault("entry_point", "editor_chat")
    call = call_kwargs(payload)
    ref = ns.adm.admit_dispatch(env.module(allow_development_runs=True), "indexer_agent", call, env.request(**request))
    return call, ref


def fetched(ns, env, claims, application_id, version_id, details):
    return ns.adm.seal_fetched_version(env.module(allow_development_runs=True), claims, 1, application_id, version_id,
                                       details)


def child_claims(ns, env, ref):
    """The claims of the run token a dispatched fan-out child of 8:80 runs on (its own context, #7227)."""
    rows = sorted(env.query(1, ns.models.ExecutionSecurityContext), key=lambda row: row.attempt)
    child = next(row for row in rows if row.version_id == 80)
    return {"ctx": str(child.id), "run": RUN, "prj": 1}, ref


def test_a_grandchild_fetched_by_a_dispatched_child_is_sealed_against_its_own_admitted_decision(ns, env):
    payload, _child, grandchild = draft_tree(env)
    _call, ref = dev_dispatch(env, ns, payload)
    claims, _ = child_claims(ns, env, ref)
    details = {"tools": [grandchild.tool()]}   # what PATCH version expands: the material inlined
    assert SECRET in dumped(details)
    fetched(ns, env, claims, 9, 90, details)
    assert SECRET not in dumped(details) and "credential_slot" in dumped(details)


def test_a_credential_bearing_grandchild_nothing_was_decided_for_is_refused_readably(ns, env):
    payload, _child, _grandchild = draft_tree(env)
    _call, ref = dev_dispatch(env, ns, payload)
    claims, _ = child_claims(ns, env, ref)
    stray = Credential(env, uuid.uuid4(), tool_id=4, config_id=8, version_id=91)
    env.denied.clear()
    with pytest.raises(ns.exceptions.AdmissionDenied) as err:
        fetched(ns, env, claims, 9, 91, {"tools": [stray.tool()]})
    assert err.value.reason_code == "DENIED_DEVELOPMENT_CHILD"
    assert [e["reason_code"] for e in env.denied] == ["DENIED_DEVELOPMENT_CHILD"]


def test_under_a_governed_parent_a_fetched_version_is_sealed_against_the_runs_decisions_or_refused(ns, env):
    binding = env.seed_agent()
    own = Credential(env, binding)                      # the run's own toolkit, decided at admission
    call = call_kwargs(payload_of(own))
    ref = ns.adm.admit_dispatch(env.module(), "indexer_agent", call, env.request())
    claims = {"ctx": ref, "run": RUN, "prj": 1}
    shared = {"tools": [own.tool()]}                    # a sub-agent that uses the run's own toolkit
    fetched(ns, env, claims, 8, 80, shared)
    assert SECRET not in dumped(shared)
    other = Credential(env, uuid.uuid4(), tool_id=4, config_id=8, version_id=81)
    with pytest.raises(ns.exceptions.AdmissionDenied) as err:
        fetched(ns, env, claims, 8, 81, {"tools": [other.tool()]})
    assert err.value.reason_code == "CREDENTIAL_REFERENCE_REQUIRED"


def test_a_credential_free_fetched_version_reads_nothing_and_is_returned_as_it_is(ns, env):
    payload, _child, _grandchild = draft_tree(env)
    _call, ref = dev_dispatch(env, ns, payload)
    claims, _ = child_claims(ns, env, ref)
    details = {"tools": [{"id": 5, "type": "artifact", "settings": {"bucket": "b"}}], "instructions": "x"}
    before, sessions = copy.deepcopy(details), len(env.sessions)
    assert fetched(ns, env, claims, 9, 92, details) == before
    assert len(env.sessions) == sessions


def test_a_fetched_version_with_a_free_form_secret_or_a_foreign_internal_mcp_is_refused(ns, env):
    payload, _child, _grandchild = draft_tree(env)
    _call, ref = dev_dispatch(env, ns, payload)
    claims, _ = child_claims(ns, env, ref)
    with pytest.raises(ns.exceptions.AdmissionDenied) as err:
        fetched(ns, env, claims, 9, 92, {"tools": [{"id": 5, "type": "jira", "settings": {"token": "{{secret.x}}"}}]})
    assert err.value.reason_code == "CREDENTIAL_REFERENCE_REQUIRED"
    foreign = {"type": "mcp", "settings": {"url": "https://evil.example/app/1/mcp", "headers": {}}}
    with pytest.raises(ns.exceptions.AdmissionDenied) as err:
        fetched(ns, env, claims, 9, 92, {"tools": [foreign]})
    assert err.value.reason_code == "INTERNAL_MCP_ORIGIN_NOT_ALLOWED"


def test_the_version_fetch_seals_only_under_a_run_token():
    text = (PLUGIN_ROOT / "api" / "v2" / "version.py").read_text()
    patch = text[text.index("def patch("):text.index("@register_openapi", text.index("def patch("))]
    assert "current_claims()" in patch and "seal_fetched_version(" in patch and "403" in patch
    assert patch.index("if claims is not None") < patch.index("seal_fetched_version(")


# --- D15: a credential-bearing toolkit in a plain LLM chat ----------------------------------------------------------

def chat(env, ns, payload, allow=True):
    call = call_kwargs(payload)
    ref = ns.adm.admit_dispatch(env.module(allow_development_runs=allow), LLM_TASK, call, env.request(
        entry_point="chat_sio_llm", application_id=None, version_id=None))
    return call, ref


def chat_refusal(env, ns, payload, allow=True):
    with pytest.raises(ns.exceptions.AdmissionDenied) as err:
        chat(env, ns, payload, allow)
    return err.value


def admissions(env):
    return [event for _, event in env.recorded if event["event_type"] == "admission"]


def test_a_chat_toolkit_with_the_authors_personal_credential_runs_as_a_development_run(ns, env):
    cred = Credential(env, uuid.uuid4(), grants=False)
    cred.reference.update(ownership_class="personal", custodian_subject_ref=SUBJECT, custodian_user_id=42)
    call, ref = chat(env, ns, {"tools": [cred.tool()]})
    [row] = env.query(1, ns.models.ExecutionSecurityContext)
    assert str(row.id) == ref and row.profile == "development" and row.entry_point == "chat_sio_llm"
    assert len(row.allowed_bindings) == 1 and row.definition_digest.startswith("adv1:")
    assert [e["reason_code"] for e in admissions(env)] == ["ADMITTED_DEVELOPMENT"]
    assert SECRET not in dumped(call["kwargs"]) and "credential_slot" in dumped(call["kwargs"])


def test_a_chat_toolkit_with_a_granted_project_credential_runs_and_an_ungranted_one_names_the_toolkit(ns, env):
    granted = Credential(env, uuid.uuid4())
    assert chat(env, ns, {"tools": [granted.tool()]})[1]
    env.reset()
    env.permissions[42] = {"credentials.use", EDIT}
    env.facts_version = 5
    ungranted = Credential(env, uuid.uuid4(), grants=False)
    tool = {**ungranted.tool(), "toolkit_name": "my jira"}
    refusal = chat_refusal(env, ns, {"tools": [tool]})
    assert refusal.reason_code == "DENIED_DEVELOPMENT_CREDENTIAL" and "toolkit “my jira”" in refusal.message
    assert "in a chat" in refusal.message and "draft" not in refusal.message


def test_with_development_runs_off_a_chat_toolkit_credential_is_refused_as_switched_off(ns, env):
    cred = Credential(env, uuid.uuid4())
    refusal = chat_refusal(env, ns, {"tools": [cred.tool()]}, allow=False)
    assert refusal.reason_code == "DEVELOPMENT_RUNS_DISABLED"
    assert env.permission_calls == []   # nothing was decided
    assert [e["reason_code"] for e in env.denied][-1] == "DEVELOPMENT_RUNS_DISABLED"


def test_a_chat_toolkit_that_is_not_saved_in_the_project_declares_nothing(ns, env):
    cred = Credential(env, uuid.uuid4())
    tool = {**cred.tool(), "id": 999}
    refusal = chat_refusal(env, ns, {"tools": [tool]})
    assert refusal.reason_code != "ADMITTED"   # refused: no stored toolkit selects that credential


@pytest.mark.parametrize("allow", [False, True])
def test_a_plain_chat_without_configured_toolkits_still_decides_and_reads_nothing(ns, env, allow):
    _call, ref = chat(env, ns, {"tools": [{"id": 5, "type": "artifact", "settings": {"bucket": "b"}}]}, allow=allow)
    [row] = env.query(1, ns.models.ExecutionSecurityContext)
    assert ref == str(row.id) and row.allowed_bindings == []
    assert env.permission_calls == [] and env.secrets_calls == []


@pytest.mark.parametrize("bad_id", ["²", "99999999999", 2 ** 40, -3, "1e3", True])
def test_a_chat_toolkit_id_that_names_no_stored_toolkit_is_refused_typed(ns, env, bad_id):
    cred = Credential(env, uuid.uuid4())
    refusal = chat_refusal(env, ns, {"tools": [{**cred.tool(), "id": bad_id}]})
    assert isinstance(refusal, ns.exceptions.AdmissionDenied) and refusal.reason_code != "DEPENDENCY_UNAVAILABLE"


def test_a_chat_toolkit_with_only_a_platform_internal_configuration_takes_no_development_read(ns, env):
    cfg_uuid = str(uuid.uuid4())
    env.configurations[(1, cfg_uuid)] = {"id": 9, "project_id": 1, "type": "pgvector", "section": "vectorstore",
                                         "generation": 1}
    tool = {"id": 31, "type": "datasource", "settings": {"pgvector_configuration": {
        "elitea_title": "vectors", "configuration_uuid": cfg_uuid, "configuration_project_id": 1,
        "configuration_type": "pgvector", "connection_string": "postgresql://internal"}}}
    _call, ref = chat(env, ns, {"tools": [tool]})
    [row] = env.query(1, ns.models.ExecutionSecurityContext)
    assert ref == str(row.id) and row.allowed_bindings == [] and env.permission_calls == []
    assert len(env.configuration_calls) == 1   # the facts the seal reads anyway, once


# --- the run's lifetime: a sliding idle deadline under a cap (#7229, review item 1) ----------------------------------

from datetime import timedelta  # noqa: E402  pylint: disable=C0413

from fixtures.governed_credentials import run_to_running  # noqa: E402  pylint: disable=C0413

TTL = 3600


@pytest.fixture
def clock(ns, monkeypatch):
    """Admission's clock, moved by the test (the run token's own clock is the real one)."""
    real = ns.adm._now   # pylint: disable=W0212
    state = {"now": real()}
    monkeypatch.setattr(ns.adm, "_now", lambda: state["now"])
    return state


def context_row(env, ns, ref):
    return next(row for row in env.query(1, ns.models.ExecutionSecurityContext) if str(row.id) == ref)


def updates_during(env, action):
    seen = []
    engine = env.engine(1)
    listener = lambda conn, cursor, statement, params, context, many: seen.append(statement)   # noqa: E731
    env.ns.sa.event.listen(engine, "before_cursor_execute", listener)
    try:
        action()
    finally:
        env.ns.sa.event.remove(engine, "before_cursor_execute", listener)
    return [s for s in seen if s.lstrip().upper().startswith("UPDATE") and "expires_at" in s]


def verdict(ns, env, ref, touch=False):
    return ns.adm.validate_context(env.module(), 1, ref, CALLER, touch=touch)


def test_an_idle_run_expires_after_its_window_and_its_token_with_it(ns, env, clock):
    call, ref = admitted_run(ns, env)
    run_to_running(env, ns, ref)
    clock["now"] = ns.adm._aware(context_row(env, ns, ref).expires_at) + timedelta(seconds=1)  # pylint: disable=W0212
    assert verdict(ns, env, ref)["reason_code"] == "STALE_CONTEXT"
    reply = verify(ns, env, token_of(call), *RUN_REQUESTS[0][:3])
    assert (reply["code"], reply["status"]) == ("CONTEXT_NOT_LIVE", 403)


def test_a_run_in_use_slides_past_its_first_deadline_with_one_write_per_half_window(ns, env, clock):
    _call, ref = admitted_run(ns, env)
    run_to_running(env, ns, ref)
    start = clock["now"]
    first = ns.adm._aware(context_row(env, ns, ref).expires_at)  # pylint: disable=W0212
    writes = []
    for step in range(1, 7):   # a verdict every third of the window, for two windows
        clock["now"] = start + timedelta(seconds=step * TTL // 3)
        writes.append(len(updates_during(env, lambda: verdict(ns, env, ref))))
        assert ns.adm._aware(context_row(env, ns, ref).expires_at) > clock["now"]  # pylint: disable=W0212
    assert clock["now"] > first and writes == [0, 1, 0, 1, 0, 1]
    assert verdict(ns, env, ref)["ok"] is True


def test_an_ended_run_is_never_revived(ns, env, clock):
    call, ref = admitted_run(ns, env)
    run_to_running(env, ns, ref)
    for state in ("completed", "revoked"):
        with env.get_session(1) as session:
            ns.adm._set_state(session, uuid.UUID(ref), state, "EPOCH_MISMATCH")  # pylint: disable=W0212
            session.commit()
        before = context_row(env, ns, ref).expires_at
        assert verdict(ns, env, ref)["ok"] is False
        assert ns.adm.slide_for_resume(env.module(), 1, ref) == 0
        clock["now"] = ns.adm._aware(before) + timedelta(seconds=10)  # pylint: disable=W0212
        assert ns.adm.reopen_contexts(env.module(), 1, [ref]) == 0
        assert context_row(env, ns, ref).expires_at == before
        assert verify(ns, env, token_of(call), *RUN_REQUESTS[0][:3])["ok"] is False


def test_the_run_cap_holds_however_long_the_run_is_in_use(ns, env, clock):
    call, ref = admitted_run(ns, env)
    run_to_running(env, ns, ref)
    cap = ns.adm._aware(context_row(env, ns, ref).created_at) + timedelta(seconds=ns.adm.MAX_RUN_LIFETIME_S)  # pylint: disable=W0212
    while True:
        clock["now"] += timedelta(seconds=TTL // 2 + 1)
        if not verdict(ns, env, ref)["ok"]:
            break
        assert ns.adm._aware(context_row(env, ns, ref).expires_at) <= cap  # pylint: disable=W0212
    assert clock["now"] >= cap - timedelta(seconds=TTL // 2 + 1) and verdict(ns, env, ref)["reason_code"] == "STALE_CONTEXT"
    claims = claims_of(token_of(call))
    reply = obo(ns).verify_request(token_of(call), {"REQUEST_METHOD": "PATCH"}, endpoint=RUN_REQUESTS[0][0],
                                   module=env.module(), now=claims["exp"] + 1, view_args=RUN_REQUESTS[0][2], args={})
    assert reply["code"] == "INTERNAL_TOKEN_EXPIRED"
    assert claims["exp"] <= cap.timestamp() + 5


def test_near_its_cap_a_run_in_use_writes_nothing_more(ns, env, clock):
    _call, ref = admitted_run(ns, env)
    run_to_running(env, ns, ref)
    cap = ns.adm._aware(context_row(env, ns, ref).created_at) + timedelta(seconds=ns.adm.MAX_RUN_LIFETIME_S)  # pylint: disable=W0212
    with env.get_session(1) as session:   # the run slid all the way to its cap
        session.execute(env.ns.sa.update(ns.models.ExecutionSecurityContext)
                        .where(ns.models.ExecutionSecurityContext.id == uuid.UUID(ref)).values(expires_at=cap))
        session.commit()
    for seconds in (TTL // 3, TTL // 4, 60):   # verdicts in its last half window: the deadline cannot move
        clock["now"] = cap - timedelta(seconds=seconds)
        assert not updates_during(env, lambda: verdict(ns, env, ref))
    assert ns.adm._aware(context_row(env, ns, ref).expires_at) == cap  # pylint: disable=W0212


def test_a_resume_never_moves_any_context_past_the_runs_own_cap(ns, env, clock):
    root_ref, child_ref, _details = draft_fan_out(env, ns)
    run_to_running(env, ns, root_ref)
    root_created = ns.adm._aware(context_row(env, ns, root_ref).created_at)  # pylint: disable=W0212
    with env.get_session(1) as session:   # the child was admitted an hour after its root
        session.execute(env.ns.sa.update(ns.models.ExecutionSecurityContext)
                        .where(ns.models.ExecutionSecurityContext.id == uuid.UUID(child_ref))
                        .values(created_at=root_created + timedelta(hours=1)))
        session.commit()
    cap = root_created + timedelta(seconds=ns.adm.MAX_RUN_LIFETIME_S)
    clock["now"] = cap - timedelta(seconds=60)   # the person decides a minute before the run's cap
    assert ns.adm.slide_for_resume(env.module(), 1, child_ref) > 0
    for row in env.query(1, ns.models.ExecutionSecurityContext):
        assert ns.adm._aware(row.expires_at) <= cap  # pylint: disable=W0212


def clone_context(env, ns, ref, **values):
    """Another context row of the same run (same run_id), with the given columns changed; its id."""
    table = ns.models.ExecutionSecurityContext.__table__
    with env.get_session(1) as session:
        source = session.execute(env.ns.sa.select(table).where(table.c.id == uuid.UUID(ref))).mappings().one()
        clone = dict(source, id=uuid.uuid4(), attempt=source["attempt"] + 100, **values)
        session.execute(table.insert().values(**clone))
        session.commit()
    return str(clone["id"])


def test_an_older_live_context_of_the_run_never_makes_its_verdicts_write_for_nothing(ns, env, clock):
    _call, ref = admitted_run(ns, env)
    run_to_running(env, ns, ref)
    row = context_row(env, ns, ref)
    created = ns.adm._aware(row.created_at)  # pylint: disable=W0212
    # a context of the same run admitted a day and an hour earlier is still marked live (never ended, inherited item 8)
    older = clone_context(env, ns, ref, created_at=created - timedelta(hours=25),
                          expires_at=created + timedelta(seconds=TTL))
    clock["now"] = ns.adm._aware(row.expires_at) - timedelta(seconds=TTL // 4)  # pylint: disable=W0212
    first = updates_during(env, lambda: verdict(ns, env, ref))
    moved = ns.adm._aware(context_row(env, ns, ref).expires_at)  # pylint: disable=W0212
    second = updates_during(env, lambda: verdict(ns, env, ref))
    assert second == []   # the next verdict writes nothing (before the fix: an UPDATE that moved nothing, every time)
    assert len(first) == 1 and moved > clock["now"] + timedelta(seconds=TTL // 2)   # the verdict's own run moved
    older_deadline = ns.adm._aware(context_row(env, ns, older).expires_at)  # pylint: disable=W0212
    assert older_deadline == created + timedelta(seconds=TTL)   # an older admission is never moved past its own cap


def test_a_mandate_context_of_the_run_is_never_moved_by_another_contexts_verdict(ns, env, clock):
    _call, ref = admitted_run(ns, env)
    run_to_running(env, ns, ref)
    row = context_row(env, ns, ref)
    mandate = clone_context(env, ns, ref, subject={"kind": "mandate", "ref": "mandate:" + str(uuid.uuid4())})
    clock["now"] = ns.adm._aware(row.expires_at) - timedelta(seconds=TTL // 4)  # pylint: disable=W0212
    assert verdict(ns, env, ref)["ok"] is True
    assert ns.adm._aware(context_row(env, ns, ref).expires_at) > ns.adm._aware(row.expires_at)  # pylint: disable=W0212
    assert context_row(env, ns, mandate).expires_at == row.expires_at   # its deadline is the mandate's
    clock["now"] += timedelta(seconds=60)
    assert ns.adm.slide_for_resume(env.module(), 1, ref) == 1   # the resume decision moves the run, not the mandate
    assert context_row(env, ns, mandate).expires_at == row.expires_at


def test_a_mandate_context_never_slides(ns, env, clock):
    _call, ref = admitted_run(ns, env)
    with env.get_session(1) as session:   # the row as a mandate run has it
        session.execute(env.ns.sa.update(ns.models.ExecutionSecurityContext)
                        .where(ns.models.ExecutionSecurityContext.id == uuid.UUID(ref))
                        .values(subject={"kind": "mandate", "ref": "mandate:" + str(uuid.uuid4())}))
        session.commit()
    before = context_row(env, ns, ref).expires_at
    clock["now"] = ns.adm._aware(before) - timedelta(seconds=60)  # pylint: disable=W0212
    assert ns.adm.slide_for_resume(env.module(), 1, ref) == 0
    assert not updates_during(env, lambda: verdict(ns, env, ref))   # whatever the verdict, no deadline write
    assert context_row(env, ns, ref).expires_at == before


def draft_fan_out(env, ns):
    payload, _child, _grandchild = draft_tree(env)
    call, ref = dev_dispatch(env, ns, payload)
    details = copy.deepcopy(call["kwargs"]["application"]["subagent_version_details"]["8:80"]["version_details"])
    child = call_kwargs({"application": {"id": 8, "version_id": 80, "version_details": details}}, context_ref=ref)
    ns.adm.admit_dispatch(env.module(allow_development_runs=True), "indexer_agent", child, None)
    draft_fan_out.root_call = call
    return ref, child["kwargs"]["envelope"]["context_ref"], details


def test_a_fan_out_child_resumed_after_a_long_pause_runs_on_with_its_root(ns, env, clock):
    root_ref, child_ref, details = draft_fan_out(env, ns)
    assert child_ref != root_ref
    run_to_running(env, ns, root_ref)
    paused = ns.adm._aware(context_row(env, ns, child_ref).expires_at)  # pylint: disable=W0212
    clock["now"] = paused + timedelta(seconds=2 * TTL)   # the person decides two hours later
    assert verdict(ns, env, child_ref)["reason_code"] == "STALE_CONTEXT"
    # the whole run slides in one statement: the child, its root and the sub-agents admitted with it
    assert ns.adm.slide_for_resume(env.module(), 1, child_ref) == len(env.query(1, ns.models.ExecutionSecurityContext))
    for row in env.query(1, ns.models.ExecutionSecurityContext):
        assert ns.adm._aware(row.expires_at) > clock["now"]  # pylint: disable=W0212
    # the replay of the stashed child is admitted under the child's own context again, and the worker validates it
    resumed = call_kwargs({"application": {"id": 8, "version_id": 80, "version_details": copy.deepcopy(details)}},
                          context_ref=child_ref)
    ns.adm.admit_dispatch(env.module(allow_development_runs=True), "indexer_agent", resumed, None)
    assert resumed["kwargs"]["envelope"]["context_ref"] == child_ref
    assert verdict(ns, env, child_ref, touch=True)["ok"] is True
    assert child_ref in [str(row.id) for row in ns.adm.descendant_contexts(1, RUN, root_ref)]
    # the parked parent's reconcile still finds the decisions of the sub-agents that finished before the pause
    reconcile = copy.deepcopy(draft_fan_out.root_call)
    assert ns.adm.admit_dispatch(env.module(allow_development_runs=True), "indexer_agent", reconcile, None) is None
    assert reconcile["kwargs"]["envelope"]["context_ref"] == root_ref


def test_the_release_of_a_child_resumed_after_a_long_pause_honours_the_slid_deadline(ns, env, clock):
    ns.need_transport()   # sealing a release needs the shared transport (absent when this plugin is checked out alone)
    root_ref, child_ref, details = draft_fan_out(env, ns)
    run_to_running(env, ns, root_ref)
    clock["now"] = ns.adm._aware(context_row(env, ns, child_ref).expires_at) + timedelta(seconds=2 * TTL)  # pylint: disable=W0212
    assert ns.adm.slide_for_resume(env.module(), 1, child_ref) > 0
    resumed = call_kwargs({"application": {"id": 8, "version_id": 80, "version_details": copy.deepcopy(details)}},
                          context_ref=child_ref)
    ns.adm.admit_dispatch(env.module(allow_development_runs=True), "indexer_agent", resumed, None)
    assert verdict(ns, env, child_ref, touch=True)["ok"] is True
    _private, public = ns.keypair()
    slot = resumed["kwargs"]["application"]["version_details"]["tools"][0]["settings"]["credentials"]["credential_slot"]
    released = ns.credential_release.release_credential(
        env.module(), project_id=1, context_ref=child_ref, probe_ref=None, caller=dict(CALLER), toolkit_instance_id="2",
        field="credentials", slot=dict(slot), recipient_key=public)
    assert released.get("ok") is True, released.get("reason_code")


def test_the_child_resume_extends_the_run_only_behind_its_requester_check():
    text = (PLUGIN_ROOT / "rpc" / "chat_all.py").read_text()
    resume = text[text.index("def _continue_child_resume("):]
    resume = resume[:resume.index("\n    def ", 10)]
    assert resume.index("Child interrupt ownership validation failed") < resume.index("slide_for_resume(") \
        < resume.index("start_task(")



def test_with_development_runs_off_only_a_saved_chat_toolkit_is_refused_as_switched_off(ns, env):
    cred = Credential(env, uuid.uuid4())
    unsaved = {**cred.tool(), "id": None}   # switching development runs on would not help it
    assert chat_refusal(env, ns, {"tools": [unsaved]}, allow=False).reason_code == "CREDENTIAL_REFERENCE_REQUIRED"

