"""C-1009 (#7336): a scoped, expiring API token is governed evidence ``api_token``.

- Admission: a token caller whose row is scoped is verified as ``api_token`` (admitted under enforce with no switch) and
  held to its scope: the run's project in its projects (``TOKEN_SCOPE_PROJECT``), the ``run`` operation
  (``TOKEN_SCOPE_OPERATION``). It then runs as its owner's interactive run: subject = the owner's linked IdP subject,
  toolkit runs and a schedule's Run now admitted as the owner's, the run token minted and every platform token stripped.
  The audit names the token (``caller_ref=token:<id>``). An unscoped token is ``legacy_pat`` exactly as before.
- Audit only: the same refusals are ``would_deny`` rows; nothing is refused.
- The HTTP boundary (``token_scope``): the route table and its decision, per entry; refusals audited once per window.
- ``run_identity_applies``: a scoped token holder gets the run identity, so its owner's personal token is never looked up.

Real admission and token code on SQLite (``fixtures/admission_env.py``), as test_7229.
"""
import ast
import copy
import json
import sys
import types
import uuid
from pathlib import Path

import pytest

PLUGIN_ROOT = Path(__file__).parents[2]
sys.path.insert(0, str(PLUGIN_ROOT / "tests"))

from fixtures import admission_env  # noqa: E402  pylint: disable=C0413
from fixtures.governed_credentials import SUBJECT, Credential, call_kwargs, dumped  # noqa: E402  pylint: disable=C0413

RUN = "run-1"
TOKEN = 21
OWNER = 42
KEY = "test-master-key-not-a-secret"
EDIT = "models.applications.application.update"
PREDICT = "models.applications.predict.post"
PAT, SESSION = "pat-value-not-a-secret", "session-reference-not-a-secret"
TOOLKIT_TASK = "indexer_test_toolkit_tool"


@pytest.fixture(scope="module")
def ns(tmp_path_factory):
    saved = dict(sys.modules)
    yield admission_env.install(PLUGIN_ROOT, tmp_path_factory.mktemp("api_token"))
    sys.modules.clear()
    sys.modules.update(saved)


@pytest.fixture
def env(ns):
    ns.env.reset()
    env = ns.env
    env.facts_version = 5
    env.permissions[OWNER] = {"credentials.use", EDIT, PREDICT}
    env.user_rows[OWNER] = {"id": OWNER, "idp_issuer": "https://issuer", "idp_tid": "tid-1", "idp_oid": "oid-42"}
    scoped(env)
    ns.tools.config.SECRETS_MASTER_KEY = KEY
    ns.tools.config.APP_HOST = "http://localhost"
    ns.tools.this.module = env.module()
    ns.tools.auth.epoch_ttl = 5.0
    ns.token_scope._AUDITED.clear()   # pylint: disable=W0212
    yield env
    for name in ("SECRETS_MASTER_KEY", "APP_HOST"):
        vars(ns.tools.config).pop(name, None)


def scoped(env, projects=(1,), operations=("run",)):
    """Token 21 of user 42 is scoped; the verifier answers as the auth plugin does for it."""
    env.token_scopes[TOKEN] = {"projects": list(projects), "operations": list(operations), "user_id": OWNER}
    env.verify_result = {"ok": True, "principal": {
        "kind": "api_token", "user_id": OWNER, "subject_ref": f"user:{OWNER}",
        "evidence": {"type": "asserted_user", "issuer": "elitea:api_token", "subject": f"user:{OWNER}"},
        "expires_at": None, "epochs": {"user": 3, "token": 4}}}


def legacy(env):
    env.token_scopes.clear()
    env.verify_result = {"ok": False, "reason_code": "IDENTITY_KIND_NOT_ADMITTED", "message": "fixed"}


def token_request(env, **over):
    values = dict(caller_type="token", caller_id=TOKEN, caller_reference=None, entry_point="predict_api")
    values.update(over)
    return env.request(**values)


def admit(ns, env, module=None, **over):
    return ns.adm.admit_execution(module or env.module(), token_request(env, **over), run_id=RUN)


def refused(ns, env, module=None, **over):
    with pytest.raises(ns.exceptions.AdmissionDenied) as err:
        admit(ns, env, module, **over)
    return err.value


def contexts(env, ns, project_id=1):
    return env.query(project_id, ns.models.ExecutionSecurityContext)


def with_tokens(payload):
    payload = copy.deepcopy(payload)
    payload["api_token"] = PAT
    payload["llm"] = {"kwargs": {"model": "m", "api_key": PAT, "auth_session": SESSION, "project_id": 1}}
    return payload


def token_shaped(value) -> list:
    text = dumped(value)
    return [marker for marker in (PAT, SESSION) if marker in text]


def claims_of(delivered):
    import base64  # pylint: disable=C0415
    body = delivered["token"][len("eio1."):].split(".")[0]
    return json.loads(base64.urlsafe_b64decode(body + "=" * (-len(body) % 4)))


# --- admission: the authorization matrix -------------------------------------------------------

def test_a_token_in_scope_is_admitted_as_its_owner_and_the_audit_names_the_token(ns, env):
    env.seed_agent()
    view = admit(ns, env)

    [(evidence, kinds)] = env.verify_calls
    assert evidence == {"kind": "api_token", "token_id": TOKEN, "user_id": OWNER, "projects": [1], "operations": ["run"]}
    assert kinds == frozenset({"session_user", "api_token"})   # no switch: legacy tokens stay off
    [row] = contexts(env, ns)
    assert row.caller == {"kind": "api_token", "ref": f"token:{TOKEN}", "user_id": OWNER}
    assert row.subject == {"kind": "api_token", "ref": SUBJECT, "delegation_ref": None,
                           "evidence": {"type": "asserted_user", "issuer": "elitea:api_token",
                                        "subject": f"user:{OWNER}"}}
    assert row.tenant == "tid-1" and row.revocation_epochs["user"] == 3 and row.revocation_epochs["token"] == 4
    assert (view.subject_ref, view.subject_user_id, view.caller_kind) == (SUBJECT, OWNER, "api_token")
    [(_, allow)] = env.recorded
    assert (allow["decision"], allow["reason_code"], allow["caller_ref"], allow["user_ref"]) == (
        "allow", "ADMITTED", f"token:{TOKEN}", SUBJECT)
    assert env.token_scope_calls == [TOKEN]   # the token row is read once


@pytest.mark.parametrize("over, scope, code", [
    ({"project_id": 2, "binding_project_id": 2}, {}, "TOKEN_SCOPE_PROJECT"),
    ({"binding_project_id": 2}, {}, "TOKEN_SCOPE_PROJECT"),      # the agent's own project too
    ({}, {"projects": (2, 3)}, "TOKEN_SCOPE_PROJECT"),
    ({}, {"operations": ("build",)}, "TOKEN_SCOPE_OPERATION"),
    ({}, {"operations": ("build", "admin")}, "TOKEN_SCOPE_OPERATION"),
])
def test_a_token_out_of_scope_is_refused_with_one_deny_row(ns, env, over, scope, code):
    env.seed_agent()
    scoped(env, **scope)

    error = refused(ns, env, **over)
    assert error.reason_code == code and error.message == ns.adm.MESSAGES[code]
    assert contexts(env, ns) == [] and env.recorded == []
    [deny] = env.denied
    assert (deny["decision"], deny["reason_code"], deny["caller_ref"]) == ("deny", code, f"token:{TOKEN}")
    assert env.digest_calls == 0   # refused before the binding is read


def test_an_owner_with_no_linked_identity_cannot_be_a_subject(ns, env):
    env.seed_agent()
    env.user_rows[OWNER] = {"id": OWNER, "idp_issuer": None, "idp_tid": None, "idp_oid": None}
    assert refused(ns, env).reason_code == "IDENTITY_SUBJECT_UNLINKED"
    del env.user_rows[OWNER]
    assert refused(ns, env).reason_code == "DEPENDENCY_UNAVAILABLE"


def test_an_unreadable_token_row_refuses_the_run(ns, env):
    env.seed_agent()
    env.token_scope_raises = RuntimeError("auth down")
    assert refused(ns, env).reason_code == "DEPENDENCY_UNAVAILABLE" and env.verify_calls == []


@pytest.mark.parametrize("code", ["IDENTITY_REVOKED", "IDENTITY_EVIDENCE_MISSING", "IDENTITY_SUBJECT_UNLINKED"])
def test_the_verifiers_refusal_of_a_scoped_token_is_the_runs(ns, env, code):
    env.seed_agent()
    env.verify_result = {"ok": False, "reason_code": code, "message": "fixed"}
    assert refused(ns, env).reason_code == code


def test_a_legacy_token_is_unchanged(ns, env):
    env.seed_agent()
    legacy(env)
    assert refused(ns, env).reason_code == "IDENTITY_KIND_NOT_ADMITTED"
    [(evidence, kinds)] = env.verify_calls
    assert evidence == {"kind": "legacy_pat", "token_id": TOKEN} and "legacy_pat" not in kinds
    # admitted as a legacy token, it still needs a mandate (its subject is only asserted)
    env.verify_result = {"ok": True, "principal": {
        "kind": "legacy_pat", "user_id": OWNER, "subject_ref": f"token:{TOKEN}",
        "evidence": {"type": "asserted_user", "issuer": "elitea:pat", "subject": f"user:{OWNER}"}, "epochs": {}}}
    assert refused(ns, env, env.module(admit_legacy_pat=True)).reason_code == "MANDATE_REQUIRED"


@pytest.mark.parametrize("entry_point, code", [
    ("toolkit_test", "TOOLKIT_RUN_TOKEN_NOT_ADMITTED"), ("index_on_demand", "TOOLKIT_RUN_TOKEN_NOT_ADMITTED"),
    ("schedule_test", "SCHEDULE_RUN_TOKEN_NOT_ADMITTED")])
def test_a_legacy_token_still_gets_the_toolkit_and_schedule_codes_before_verification(ns, env, entry_point, code):
    legacy(env)
    toolkit = {"application_id": None, "version_id": None, "toolkit_id": 1} if entry_point != "schedule_test" else {}
    assert refused(ns, env, env.module(admit_legacy_pat=True), entry_point=entry_point, **toolkit).reason_code == code
    assert env.verify_calls == []


def test_a_toolkit_run_with_the_run_operation_is_admitted_as_the_owners_toolkit_run(ns, env):
    cred = Credential(env, uuid.uuid4())
    tool = cred.tool()
    call = call_kwargs({"toolkit_config": tool, "tool_name": "t", "tool_params": {}, "project_auth_token": PAT},
                       toolkit_config=copy.deepcopy(tool), project_auth_token=PAT)
    ref = ns.adm.admit_dispatch(env.module(), TOOLKIT_TASK, call, token_request(
        env, entry_point="toolkit_test", application_id=None, version_id=None, toolkit_id=1))

    [row] = contexts(env, ns)
    assert ref == str(row.id) and row.profile == "development" and row.caller["kind"] == "api_token"
    assert row.caller["user_id"] == OWNER and row.entry_point == "toolkit_test"
    assert len(row.allowed_bindings) == 1   # the owner's grant on the toolkit's credential, as their own Test run
    assert token_shaped(call["kwargs"]) == [] and token_shaped(call["meta"]) == []   # stripped: the run token only
    assert claims_of(call["kwargs"]["internal_run_token"])["uid"] == OWNER
    assert (OWNER, "default", 1) in env.permission_calls   # the owner's rights, not the token id's
    assert TOKEN not in [user for user, _, _ in env.permission_calls]


def test_a_toolkit_run_without_the_run_operation_is_refused(ns, env):
    scoped(env, operations=("build",))
    error = refused(ns, env, entry_point="toolkit_test", application_id=None, version_id=None, toolkit_id=1)
    assert error.reason_code == "TOKEN_SCOPE_OPERATION"


def test_an_admitted_agent_run_carries_the_run_token_and_no_platform_token(ns, env):
    env.seed_agent()
    call = call_kwargs(with_tokens({"application": {"version_details": {"tools": []}}}), project_auth_token=PAT)
    ref = ns.adm.admit_dispatch(env.module(), "indexer_agent", call, token_request(env))

    assert ref and token_shaped(call["kwargs"]) == [] and token_shaped(call["meta"]) == []
    claims = claims_of(call["kwargs"]["internal_run_token"])
    assert (claims["sub"], claims["uid"], claims["ctx"]) == (SUBJECT, OWNER, ref)


def test_a_child_dispatched_under_the_token_run_mints_from_its_context(ns, env):
    env.seed_agent()
    parent = call_kwargs({"application": {"version_details": {"tools": []}}})
    ref = ns.adm.admit_dispatch(env.module(), "indexer_agent", parent, token_request(env))
    child = call_kwargs({"application": {"version_details": {"tools": []}}}, context_ref=ref)
    ns.adm.admit_dispatch(env.module(), "indexer_agent", child, None)

    claims = claims_of(child["kwargs"]["internal_run_token"])
    assert (claims["sub"], claims["uid"], claims["ctx"]) == (SUBJECT, OWNER, ref)


def test_a_delegated_credential_under_a_token_run_is_refused_at_release_with_the_existing_code(ns):
    with pytest.raises(ns.entra_obo.DelegationRefused) as err:
        ns.entra_obo.run_subject({"kind": "api_token", "ref": SUBJECT}, "tid-1",
                                 {"kind": "api_token", "ref": f"token:{TOKEN}", "user_id": OWNER})
    assert err.value.code == "DENIED_DELEGATION_UNSUPPORTED_CALLER" == ns.entra_obo.CALLER_UNSUPPORTED
    assert ns.adm.MESSAGES[err.value.code]


def test_audit_only_records_would_deny_for_the_same_cases_and_refuses_nothing(ns, env):
    env.seed_agent()
    scoped(env, projects=(2,))
    call = call_kwargs(with_tokens({"application": {"version_details": {"tools": []}}}))
    before = copy.deepcopy(call)
    assert ns.adm.admit_dispatch(env.module(audit=True), "indexer_agent", call, token_request(env)) is None
    assert call == before   # the run goes on as a legacy run
    [row] = [event for event in env.denied if event.get("would_deny")]
    assert (row["decision"], row["reason_code"], row["caller_ref"]) == ("allow", "TOKEN_SCOPE_PROJECT", f"token:{TOKEN}")
    env.denied.clear()
    scoped(env, operations=("admin",))
    ns.adm.admit_dispatch(env.module(audit=True), "indexer_agent", call_kwargs({}), token_request(env))
    assert [event["reason_code"] for event in env.denied if event.get("would_deny")] == ["TOKEN_SCOPE_OPERATION"]


def test_audit_only_records_the_allow_of_a_token_in_scope(ns, env):
    env.seed_agent()
    ns.adm.admit_dispatch(env.module(audit=True), "indexer_agent",
                          call_kwargs({"application": {"version_details": {"tools": []}}}), token_request(env))
    assert not [event for event in env.denied if event.get("would_deny")]
    assert [(event["reason_code"], event["would_deny"], event["caller_ref"]) for _, event in env.recorded] == [
        ("ADMITTED", False, f"token:{TOKEN}")]


def test_the_scope_check_comes_after_verification_and_before_any_binding_or_person_read():
    text = (PLUGIN_ROOT / "utils" / "admission.py").read_text()
    body = text[text.index("def _admit("):text.index("def admit_execution")]
    order = [body.index(marker) for marker in (
        "evidence = _token_evidence(request)", "TOOLKIT_RUN_TOKEN_NOT_ADMITTED", "_verify_identity(request, settings, evidence)",
        'raise _denied("TOKEN_SCOPE_PROJECT")', 'raise _denied("TOKEN_SCOPE_OPERATION")', "request = request.model_copy(",
        "_author_permissions(request)", "_find_binding(request, settings)")]
    assert order == sorted(order)


# --- the HTTP boundary -------------------------------------------------------------------------

SCOPE = {"projects": [1], "operations": ["run"], "user_id": OWNER}


def route_table(ns):
    return ns.token_scope.ROUTE_OPERATIONS


def test_every_route_of_the_table_needs_its_operation_and_nothing_less(ns):
    for key, operation in route_table(ns).items():
        endpoint, path = key if isinstance(key, tuple) else (key, "")
        view_args = {"project_id": 1, "url": "v1/chat/completions"}
        assert ns.token_scope.operation_of(endpoint, "POST", view_args, path) == operation, key
        for operations in (["run"], ["build"], ["admin"], ["run", "build", "admin"]):
            scope = {"projects": [1], "operations": operations, "user_id": OWNER}
            expected = None if operation in operations else "TOKEN_SCOPE_OPERATION"
            assert ns.token_scope.decide(scope, endpoint, "POST", view_args, path, 1) == expected, (key, operations)
            # its project only
            assert ns.token_scope.decide(scope, endpoint, "POST", view_args, path, 2) in (
                "TOKEN_SCOPE_PROJECT", "TOKEN_SCOPE_OPERATION")


@pytest.mark.parametrize("endpoint, method, view_args, path, operation", [
    ("api.v2.elitea_core.applications", "POST", {"project_id": 1}, "", "build"),   # unclassified write: build
    ("api.v2.configurations.configurations", "PUT", {"project_id": 1}, "", "admin"),   # a credential write (C9-SEC-78)
    ("api.v2.artifacts.s3_credentials", "POST", {"project_id": 1}, "", "admin"),
    ("api.v2.secrets.secret", "GET", {"project_id": 1}, "", "admin"),             # returns a value (C9-SEC-77)
    ("api.v2.elitea_core.pipeline_trigger", "GET", {"project_id": 1}, "", "admin"),
    ("api.v2.artifacts.artifacts", "POST", {"project_id": 1}, "", "build"),
    ("api.v2.elitea_core.schedules", "POST", {"project_id": 1}, "/api/v2/elitea_core/schedules/prompt_lib/1/4/run", "run"),
    ("api.v2.elitea_core.schedules", "POST", {"project_id": 1}, "/api/v2/elitea_core/schedules/prompt_lib/1/4/pause", "build"),
    ("api.v2.elitea_core.applications", "GET", {"project_id": 1}, "", "read"),
    ("api.v2.elitea_core.predict", "GET", {"project_id": 1}, "", "read"),
    ("api.v2.elitea_core.applications", "GET", {"project_id": 1, "mode": "administration"}, "", "admin"),
    ("api.v2.admin.users", "GET", {}, "", "admin"),
    ("api.v2.elitea_core.governance_settings", "GET", {}, "", "admin"),
    ("api.v2.auth.token", "GET", {}, "", "admin"),
    ("runtime_interface_litellm.litellm_route_http__url", "POST", {"url": "v1/chat/completions"}, "", "run"),
    ("runtime_interface_litellm.litellm_route_http__url", "POST", {"url": "key/generate"}, "", "admin"),
    (None, "POST", {}, "", "build"),
])
def test_how_a_request_is_classified(ns, endpoint, method, view_args, path, operation):
    assert ns.token_scope.operation_of(endpoint, method, view_args, path) == operation


@pytest.mark.parametrize("endpoint, method, project, scope, code", [
    ("api.v2.elitea_core.applications", "GET", 1, SCOPE, None),                      # a read in scope
    ("api.v2.elitea_core.applications", "GET", 2, SCOPE, "TOKEN_SCOPE_PROJECT"),     # a read of another project
    ("api.v2.elitea_core.applications", "POST", 1, SCOPE, "TOKEN_SCOPE_OPERATION"),  # a write needs build
    ("api.v2.elitea_core.applications", "POST", 1, dict(SCOPE, operations=["build"]), None),
    ("api.v2.elitea_core.predict", "POST", 1, SCOPE, None),
    ("api.v2.elitea_core.predict", "POST", 2, SCOPE, "TOKEN_SCOPE_PROJECT"),
    ("elitea_core.client_streamable_http", "POST", 1, SCOPE, None),                 # Elitea as an MCP server
    ("elitea_core.client_streamable_http", "POST", 2, SCOPE, "TOKEN_SCOPE_PROJECT"),
    ("elitea_core.client_parametrized_streamable_http", "POST", 1, dict(SCOPE, operations=["build"]),
     "TOKEN_SCOPE_OPERATION"),
    ("api.v2.projects.project", "GET", None, SCOPE, None),                          # a read with no project (D2)
    (None, "GET", None, SCOPE, None),                                               # e.g. the caller's own user
    ("api.v2.projects.project", "POST", None, SCOPE, "TOKEN_SCOPE_OPERATION"),      # a write with no project: admin
    ("api.v2.projects.project", "POST", None, dict(SCOPE, operations=["admin"]), None),
    ("api.v2.admin.users", "GET", None, SCOPE, "TOKEN_SCOPE_OPERATION"),            # admin routes stay admin
    ("api.v2.elitea_core.governance_settings", "GET", None, SCOPE, "TOKEN_SCOPE_OPERATION"),
    ("sio.connect", "POST", None, SCOPE, "TOKEN_SCOPE_OPERATION"),                  # a socket: admin only (C9-SEC-75)
    ("sio.connect", "POST", None, dict(SCOPE, operations=["run", "admin"]), None),
    ("sio.project", "GET", 1, SCOPE, None),                                         # ... and only in its projects
    ("sio.project", "GET", 5, dict(SCOPE, operations=["run", "admin"]), "TOKEN_SCOPE_PROJECT"),
    ("api.v2.elitea_core.applications", "POST", None, dict(SCOPE, operations=["build"]), "TOKEN_SCOPE_OPERATION"),
    ("api.v2.auth.token", "GET", None, dict(SCOPE, operations=["run", "build", "admin"]), "TOKEN_SCOPE_OPERATION"),
    ("api.v2.auth.token", "POST", None, dict(SCOPE, operations=["run", "build", "admin"]), "TOKEN_SCOPE_OPERATION"),
    ("api.v2.auth.token", "DELETE", None, None, "TOKEN_SCOPE_OPERATION"),           # a legacy token neither
    ("api.v2.auth.token", "POST", None, None, "TOKEN_SCOPE_OPERATION"),
    ("api.v2.auth.token", "GET", None, None, None),                                 # a legacy token may list
    ("api.v2.elitea_core.applications", "POST", None, None, None),                  # legacy: as before
    ("api.v2.admin.users", "POST", None, None, None),
])
def test_what_a_token_may_do(ns, endpoint, method, project, scope, code):
    assert ns.token_scope.decide(scope, endpoint, method, {"project_id": project}, "", project) == code


# Secrets and long-lived credentials (C9-SEC-77, C9-SEC-78): one by one, every method is admin
SECRET_ROUTES = ["api.v2.secrets.secret", "api.v2.secrets.secrets", "api.v2.secrets.private_secret",
                 "api.v2.secrets.hide", "api.v2.secrets.credential_material", "api.v2.artifacts.s3_credentials",
                 "api.v2.elitea_core.pipeline_trigger"]
CREDENTIAL_WRITES = ["api.v2.configurations.configurations", "api.v2.configurations.configuration",
                     "api.v2.configurations.models"]


@pytest.mark.parametrize("endpoint", SECRET_ROUTES)
@pytest.mark.parametrize("method", ["GET", "HEAD", "POST", "PUT", "PATCH", "DELETE"])
def test_a_route_that_returns_or_mints_a_secret_needs_admin(ns, endpoint, method):
    assert ns.token_scope.operation_of(endpoint, method, {"project_id": 1}) == "admin"
    for operations in (["run"], ["build"], ["run", "build"]):
        scope = {"projects": [1], "operations": operations, "user_id": OWNER}
        assert ns.token_scope.decide(scope, endpoint, method, {"project_id": 1}, "", 1) == "TOKEN_SCOPE_OPERATION"
        assert ns.token_scope.decide(scope, endpoint, method, {}, "", None) == "TOKEN_SCOPE_OPERATION"
    admin = {"projects": [1], "operations": ["admin"], "user_id": OWNER}
    assert ns.token_scope.decide(admin, endpoint, method, {"project_id": 1}, "", 1) is None


@pytest.mark.parametrize("endpoint", CREDENTIAL_WRITES)
def test_a_configuration_write_needs_admin_and_its_read_stays_a_read(ns, endpoint):
    for method in ("POST", "PUT", "PATCH", "DELETE"):
        assert ns.token_scope.operation_of(endpoint, method, {"project_id": 1}) == "admin"
        assert ns.token_scope.decide(dict(SCOPE, operations=["build"]), endpoint, method, {"project_id": 1}, "", 1) \
            == "TOKEN_SCOPE_OPERATION"
    assert ns.token_scope.operation_of(endpoint, "GET", {"project_id": 1}) == "read"
    assert ns.token_scope.decide(SCOPE, endpoint, "GET", {"project_id": 1}, "", 1) is None


class Holder:
    def __init__(self, endpoint, method="POST", view_args=None, path="", args=None, environ=None):
        self.parts = (endpoint, method, view_args or {}, path, args or {}, environ or {})


def check(ns, env, holder, module=None):
    return ns.token_scope.check(module or env.module(), TOKEN, holder.parts)


def test_the_checker_refuses_with_a_readable_403_and_one_audit_row_per_window(ns, env):
    request = Holder("api.v2.elitea_core.predict", view_args={"project_id": 2})
    first = check(ns, env, request)
    again = check(ns, env, request)
    assert first == again == {"status": 403, "code": "TOKEN_SCOPE_PROJECT",
                              "message": ns.token_scope.MESSAGES["TOKEN_SCOPE_PROJECT"]}
    [deny] = env.denied   # rate-bounded: the second refusal is counted, not written
    assert (deny["event_type"], deny["decision"], deny["reason_code"], deny["caller_ref"], deny["project_id"],
            deny["target_ref"]) == ("admission", "deny", "TOKEN_SCOPE_PROJECT", f"token:{TOKEN}", 2,
                                    "POST api.v2.elitea_core.predict")
    assert check(ns, env, Holder("api.v2.elitea_core.predict", view_args={"project_id": 3}))["code"] == \
        "TOKEN_SCOPE_PROJECT"
    assert len(env.denied) == 2   # another project is another row
    check(ns, env, Holder("api.v2.elitea_core.predict", "GET", view_args={"project_id": 2}))
    assert len(env.denied) == 3   # and another method


def test_the_checker_reads_the_llm_proxys_project_from_its_header(ns, env):
    llm = "runtime_interface_litellm.litellm_route_http__url"
    ok = Holder(llm, view_args={"url": "v1/chat/completions"}, environ={"HTTP_X_PROJECT_ID": "1"})
    other = Holder(llm, view_args={"url": "v1/chat/completions"}, environ={"HTTP_X_PROJECT_ID": "2"})
    assert check(ns, env, ok) is None and check(ns, env, other)["code"] == "TOKEN_SCOPE_PROJECT"


def test_the_checker_passes_a_token_in_scope_and_a_legacy_token(ns, env):
    assert check(ns, env, Holder("api.v2.elitea_core.predict", view_args={"project_id": 1})) is None
    env.token_scopes.clear()
    assert check(ns, env, Holder("api.v2.elitea_core.applications", view_args={"project_id": 9})) is None
    assert env.denied == []


def test_a_token_management_refusal_says_tokens_do_not_manage_tokens(ns, env):
    refusal = check(ns, env, Holder("api.v2.auth.token", "POST"))
    assert (refusal["code"], refusal["message"]) == ("TOKEN_SCOPE_OPERATION", ns.token_scope.TOKEN_MANAGEMENT_MESSAGE)
    assert check(ns, env, Holder("api.v2.admin.users", "GET"))["message"] == \
        ns.token_scope.MESSAGES["TOKEN_SCOPE_OPERATION"]


def test_an_unreadable_token_is_a_503_without_an_audit_row(ns, env):
    env.token_scope_raises = RuntimeError("auth down")
    assert check(ns, env, Holder("api.v2.elitea_core.predict", view_args={"project_id": 1}))["status"] == 503
    assert env.denied == []


@pytest.mark.parametrize("module_kwargs", [{"governed": False}, {"audit": True}])
def test_off_and_audit_only_the_checker_reads_nothing_and_allows(ns, env, module_kwargs):
    env.token_scope_raises = AssertionError("read under off")
    assert check(ns, env, Holder("api.v2.auth.token", "DELETE"), env.module(**module_kwargs)) is None
    assert env.token_scope_calls == [] and env.denied == []


# --- run_identity_applies ----------------------------------------------------------------------

def _function(path, name, namespace):
    source = (PLUGIN_ROOT / path).read_text()
    node = next(n for n in ast.parse(source).body if isinstance(n, ast.FunctionDef) and n.name == name)
    exec(compile(ast.Module(body=[node], type_ignores=[]), path, "exec"), namespace)  # pylint: disable=W0122
    return namespace[name]


@pytest.fixture
def identity(monkeypatch):
    state = {"governed": True, "key": b"k", "holder": None, "scopes": {}}
    modules = {name: types.ModuleType(name) for name in (
        "c7336", "c7336.utils", "c7336.utils.internal_obo", "c7336.models", "c7336.models.pd",
        "c7336.models.pd.admission")}
    for name in ("c7336", "c7336.utils", "c7336.models", "c7336.models.pd"):
        modules[name].__path__ = []
    modules["c7336.utils.internal_obo"].signing_key = lambda: state["key"]
    modules["c7336.models.pd.admission"]._holder = lambda sid: state["holder"]  # pylint: disable=W0212
    modules["c7336.utils"].internal_obo = modules["c7336.utils.internal_obo"]
    for name, module in modules.items():
        monkeypatch.setitem(sys.modules, name, module)
    auth = types.SimpleNamespace(token_scope=lambda token_id: state["scopes"].get(token_id))
    namespace = {"__package__": "c7336.utils", "__name__": "c7336.utils.internal_tools", "auth": auth,
                 "run_token_applies": lambda: state["governed"]}
    return _function("utils/internal_tools.py", "run_identity_applies", namespace), state


def test_a_scoped_token_holder_has_the_run_identity_and_a_legacy_one_does_not(identity):
    applies, state = identity
    state["holder"] = types.SimpleNamespace(type="token", id=TOKEN)
    assert applies(None) is False   # legacy: its owner's token is looked up as before
    state["scopes"][TOKEN] = SCOPE
    assert applies(None) is True    # scoped: the run token only, get_user_token is not consulted
    state["governed"] = False
    assert applies(None) is False   # off / audit only: unchanged
    state["governed"], state["holder"] = True, types.SimpleNamespace(type="user", id=OWNER)
    assert applies(None) is True


def test_the_run_identity_means_the_owners_token_is_never_looked_up():
    looked_up = []
    namespace = {"Optional": __import__("typing").Optional, "PredictPayloadError": RuntimeError,
                 "get_user_token": lambda user_id: looked_up.append(user_id) or PAT,
                 "resolve_auth_session": lambda user_id, sid: SESSION,
                 "run_identity_applies": lambda sid, mandate=False: True}
    fn = _function("utils/predict_utils.py", "get_predict_token_and_session", namespace)
    assert fn(1, OWNER, None, run_identity=True) == (None, None) and looked_up == []


# --- fix round: no run token key (C9-SEC-76), sockets (C9-SEC-75), the legacy token pickers -----------------------

def test_without_the_run_token_key_a_scoped_token_run_is_refused_readably(ns, env):
    env.seed_agent()
    vars(ns.tools.config).pop("SECRETS_MASTER_KEY")
    assert ns.internal_obo.signing_key() is None
    call = call_kwargs(with_tokens({"application": {"version_details": {"tools": []}}}), project_auth_token=PAT)
    with pytest.raises(ns.exceptions.AdmissionDenied) as err:
        ns.adm.admit_dispatch(env.module(), "indexer_agent", call, token_request(env))
    assert err.value.reason_code == "API_TOKEN_RUN_KEY_UNAVAILABLE"
    assert err.value.message == ns.adm.MESSAGES["API_TOKEN_RUN_KEY_UNAVAILABLE"]
    assert contexts(env, ns) == [] and env.recorded == []
    [deny] = env.denied
    assert (deny["reason_code"], deny["caller_ref"]) == ("API_TOKEN_RUN_KEY_UNAVAILABLE", f"token:{TOKEN}")
    # a session run without the key is admitted as before (it keeps what the payload builders put on it)
    env.denied.clear()
    env.verify_result = {"ok": True, "principal": admission_env.principal()}
    assert ns.adm.admit_execution(env.module(), env.request(), run_id=RUN)


def test_without_the_run_token_key_audit_only_records_the_refusal(ns, env):
    env.seed_agent()
    vars(ns.tools.config).pop("SECRETS_MASTER_KEY")
    ns.adm.admit_dispatch(env.module(audit=True), "indexer_agent", call_kwargs({}), token_request(env))
    assert [event["reason_code"] for event in env.denied if event.get("would_deny")] == ["API_TOKEN_RUN_KEY_UNAVAILABLE"]


def test_a_run_started_from_a_tokens_socket_is_held_to_its_scope(ns, env):
    """A run-starting event (chat_predict and the others) builds its request from the socket's holder: a token socket
    is a token caller, refused out of its projects at admission, admitted in them."""
    env.seed_agent()
    env.seed_agent(project_id=2)
    env.sio_users["sid-1"] = types.SimpleNamespace(type="token", id=TOKEN, reference="-")
    module = env.module()

    def request(project_id):
        return ns.pd_admission.admission_request_from(
            module, sid="sid-1", project_id=project_id, binding_project_id=project_id,
            version_row=types.SimpleNamespace(id=70, application_id=7), run_kind="interactive", entry_point="chat_sio")

    assert (request(1).caller_type, request(1).caller_id) == ("token", TOKEN)
    with pytest.raises(ns.exceptions.AdmissionDenied) as err:
        ns.adm.admit_execution(module, request(2), run_id=RUN)
    assert err.value.reason_code == "TOKEN_SCOPE_PROJECT"
    view = ns.adm.admit_execution(module, request(1), run_id="run-3")
    assert view.caller_kind == "api_token" and view.subject_user_id == OWNER


@pytest.mark.parametrize("parts, scope, code", [
    (("sio.connect", "POST", {}, "", {}, {}), SCOPE, "TOKEN_SCOPE_OPERATION"),
    (("sio.connect", "POST", {}, "", {}, {}), dict(SCOPE, operations=["run", "admin"]), None),
    (("sio.project", "GET", {"project_id": 1}, "", {}, {}), SCOPE, None),
    (("sio.project", "GET", {"project_id": 5}, "", {}, {}), SCOPE, "TOKEN_SCOPE_PROJECT"),
])
def test_the_checker_answers_a_sockets_synthetic_requests(ns, env, parts, scope, code):
    env.token_scopes[TOKEN] = scope
    refusal = ns.token_scope.check(env.module(), TOKEN, parts)
    assert (refusal or {}).get("code") == code


def test_off_the_checker_answers_a_socket_with_nothing_read(ns, env):
    env.token_scope_raises = AssertionError("read under off")
    assert ns.token_scope.check(env.module(governed=False), TOKEN, ("sio.connect", "POST", {}, "", {}, {})) is None
    assert env.token_scope_calls == []


@pytest.fixture
def pickers():
    """``get_user_token`` and ``resolve_user_token_state`` with a token list of user 42: a scoped token first."""
    import datetime as dt   # pylint: disable=C0415
    future = dt.datetime.now() + dt.timedelta(days=5)
    rows = [{"id": 21, "expires": future, "scope_projects": [1], "scope_operations": ["run"]},
            {"id": 22, "expires": future, "scope_projects": None, "scope_operations": None},
            {"id": 23, "expires": None}]
    state = {"rows": rows}
    auth = types.SimpleNamespace(list_tokens=lambda user_id: list(state["rows"]),
                                 encode_token=lambda token_id: f"encoded-{token_id}",
                                 ensure_system_token=lambda user_id: "system")
    namespace = {"Optional": __import__("typing").Optional, "datetime": dt.datetime, "auth": auth,
                 "log": types.SimpleNamespace(warning=lambda *a, **k: None)}
    get_user_token = _function("utils/predict_utils.py", "get_user_token", dict(namespace))
    resolve = _function("utils/internal_tools.py", "resolve_user_token_state", dict(namespace))
    return get_user_token, resolve, state


def test_the_legacy_token_pickers_never_pick_a_scoped_token(pickers):
    get_user_token, resolve, state = pickers
    assert get_user_token(OWNER) == "encoded-22" and resolve(OWNER) == ("VALID", "encoded-22")
    state["rows"] = state["rows"][:1]   # only a scoped token: the system token, as with no PAT
    assert get_user_token(OWNER) == "system" and resolve(OWNER) == ("VALID", "system")
    state["rows"] = [{"id": 30, "expires": None}]   # tokens from before #7336 have no scope keys: unchanged
    assert get_user_token(OWNER) == "encoded-30" and resolve(OWNER) == ("VALID", "encoded-30")

