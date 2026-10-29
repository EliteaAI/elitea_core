"""C-1009 #6756 step 4: Entra-delegated credentials, released as a token minted on behalf of the run's person.

The admission, the seal and the release run for real against the SQLite harness (``fixtures/admission_env.py``); the
secrets plugin, auth_oidc's custody RPC and Entra's token endpoint are fakes that record their calls. Every value below
is a test value made up here.
"""
import base64
import json
import sys
import time
import types
import uuid
from pathlib import Path

import pytest

PLUGIN_ROOT = Path(__file__).parents[2]
sys.path.insert(0, str(PLUGIN_ROOT / "tests"))

from fixtures import admission_env, mandate_env  # noqa: E402  pylint: disable=C0413
from fixtures.governed_credentials import (  # noqa: E402  pylint: disable=C0413
    CALLER, Credential, call_kwargs, dispatch, payload_of, run_to_running, sealed_tool_settings, seeded,
)

TID = "11111111-2222-3333-4444-555555555555"
OID = "oid-42"
ENTRA = f"https://login.microsoftonline.com/{TID}/v2.0"
KEYCLOAK = "https://keycloak.example/realms/elitea"
PERSON_REF = f"idp:{TID}:{OID}"
GRAPH = "https://graph.microsoft.com"
CLIENT_SECRET = "test-client-secret-not-real"
CUSTODY_KEY = "custody-request-key-made-up-for-tests-0123"
GRAPH_TOKEN = "graph-token-made-up-for-tests"
SITE = "https://contoso.sharepoint.com/sites/sandbox"


def jwt(**claims):
    def part(value):
        return base64.urlsafe_b64encode(json.dumps(value).encode()).decode().rstrip("=")
    return f"{part({'alg': 'none'})}.{part(claims)}.sig"


ASSERTION = jwt(tid=TID, oid=OID, aud="api-client")


def _claims(token):
    try:
        body = token.split(".")[1]
        return json.loads(base64.urlsafe_b64decode(body + "=" * (-len(body) % 4)))
    except Exception:  # pylint: disable=W0703
        return None


@pytest.fixture(scope="module")
def ns(tmp_path_factory):
    saved = dict(sys.modules)
    yield mandate_env.install(PLUGIN_ROOT, tmp_path_factory.mktemp("external_obo"))
    sys.modules.clear()
    sys.modules.update(saved)


class Entra:
    """The token endpoint: records every POST, answers what the test queued (default: a token)."""

    def __init__(self):
        self.posts, self.answers, self.timeouts = [], [], []

    def __call__(self, url, data, timeout=None):
        self.posts.append((url, dict(data)))
        self.timeouts.append(timeout)
        oid = (_claims(data.get("assertion")) or {}).get("oid")   # each person gets a token of their own
        status, body = self.answers.pop(0) if self.answers else (
            200, {"access_token": GRAPH_TOKEN if oid in (None, OID) else f"graph-token-of-{oid}", "expires_in": 3600,
                  "token_type": "Bearer"})
        return types.SimpleNamespace(status_code=status, content=b"x", json=lambda: body)


class Custody:
    """auth_oidc's RPC ``auth_oidc_obo_assertion``."""

    def __init__(self, obo):
        self.obo, self.calls, self.answer, self.timeouts, self.nonces = obo, [], {"ok": True, "assertion": ASSERTION}, [], set()

    def timeout(self, seconds):
        self.timeouts.append(seconds)
        return self

    def auth_oidc_obo_assertion(self, tid, oid, refresh=False, iat=None, nonce=None, signature=None):
        assert signature == self.obo.sign_request(CUSTODY_KEY, tid, oid, refresh, iat, nonce)   # what auth_oidc checks
        assert abs(iat - time.time()) < 5 and nonce not in self.nonces
        self.nonces.add(nonce)
        self.calls.append((tid, oid, refresh))
        if self.answer.get("assertion") == ASSERTION and oid != OID:   # custody holds each person's own sign-in
            return {"ok": True, "assertion": jwt(tid=tid, oid=oid, aud="api-client")}
        return self.answer


@pytest.fixture
def env(ns, monkeypatch):
    mandate_env.prepare(ns, ns.env)
    ns.adm._now = ns.real_now  # pylint: disable=W0212   # tokens are minted in real time
    ns.entra_obo.forget()
    principal = admission_env.principal(tid=TID, oid=OID)
    principal["evidence"]["issuer"] = ENTRA
    ns.env.verify_result = {"ok": True, "principal": principal}
    ns.env.entra, ns.env.custody = Entra(), Custody(ns.entra_obo)
    monkeypatch.setattr(ns.entra_obo, "_post", ns.env.entra)
    monkeypatch.setenv("ELITEA_API_CLIENT_SECRET", CLIENT_SECRET)
    monkeypatch.setenv("ELITEA_OBO_CUSTODY_KEY", CUSTODY_KEY)
    real_module = ns.env.module

    def module(**kwargs):
        built = real_module(**kwargs)
        built.descriptor = types.SimpleNamespace(config={"entra_obo_client_id": "api-client"})
        built.context = types.SimpleNamespace(rpc_manager=ns.env.custody)
        return built

    monkeypatch.setattr(ns.env, "module", module)
    return ns.env


def descriptor(**over):
    found = {"oauth_app_id": "platform", "tenant_id": TID, "resource": GRAPH,
             "scopes": [f"{GRAPH}/Sites.Selected"], "assertion_source": "login"}
    found.update(over)
    return found


class Delegated(Credential):
    """A SharePoint configuration governed by an ``entra_delegated`` reference: no secret value anywhere."""

    def __init__(self, env, binding, *, personal=False, custodian=PERSON_REF, inline=None, **kwargs):
        env.secret_fields.setdefault("sharepoint", ["client_secret"])
        super().__init__(env, binding, ctype="sharepoint", material=False, grants=False, **kwargs)
        self.inline = inline or {}
        row = self.reference
        row["auth_mode"] = "entra_delegated"
        if personal:
            row["ownership_class"], row["custodian_subject_ref"] = "personal", custodian
        env.delegations[self.ref_id] = descriptor()
        env.grants[(1, self.key(PERSON_REF))] = {
            "grant_id": self.grant_id, "state": "active", "revocation_epoch": 4, "valid_from": None,
            "valid_until": None, "operations": ["*"]}

    def expanded(self):
        config = {"elitea_title": "sp", "configuration_uuid": self.uuid, "configuration_project_id": 1,
                  "configuration_type": "sharepoint", "configuration_generation": 3, "site_url": SITE}
        config.update(self.inline)
        return config


def admitted(ns, env, *creds):
    call, ref = dispatch(env, ns, *creds)
    run_to_running(env, ns, ref)
    private, public = ns.keypair()
    slots = [sealed_tool_settings(call, index)["credential_slot"] for index in range(len(creds))]
    for recorded in (env.recorded, env.denied, env.secrets_calls, env.batch_release_calls, env.release_calls):
        recorded.clear()

    def release():
        return ns.credential_release.release_credentials(env.module(), project_id=1, context_ref=ref, probe_ref=None,
                                                         caller=dict(CALLER), recipient_key=public, slots=[
            {"toolkit_instance_id": str(c.tool_id), "field": "credentials", "slot": dict(s)} for c, s in zip(creds, slots)])

    def opened(item):
        slot = next(s for c, s in zip(creds, slots) if str(c.tool_id) == item["toolkit_instance_id"])
        aad = "|".join(("v1", "1", ref, item["toolkit_instance_id"], item["field"], slot["decision_id"],
                        item["release_id"])).encode()
        return json.loads(ns.transport.open_sealed(item["sealed"], private, aad))

    return types.SimpleNamespace(call=call, ref=ref, slots=slots, release=release, opened=opened)


def context_row(ns, env, ref):
    [row] = env.query(1, ns.models.ExecutionSecurityContext, ns.models.ExecutionSecurityContext.id == uuid.UUID(ref))
    return row


# --- readiness gap 1: the subject's own fields at admission ---------------------------------------------------------

@pytest.mark.parametrize("issuer", [ENTRA, KEYCLOAK])
def test_admission_stores_the_issuer_and_oid_of_the_person_as_fields_of_their_own(ns, env, issuer):
    env.verify_result["principal"]["evidence"]["issuer"] = issuer
    cred = seeded(env)
    env.grants[(1, cred.key(PERSON_REF))] = env.grants.pop((1, cred.key()))
    _call, ref = dispatch(env, ns, cred)
    subject = context_row(ns, env, ref).subject
    assert (subject["issuer"], subject["oid"], subject["ref"]) == (issuer, OID, PERSON_REF)
    assert context_row(ns, env, ref).tenant == TID


# --- Deliverable A at admission: a container with no value ----------------------------------------------------------

def test_a_delegated_credential_is_sealed_as_a_slot_for_the_token_alone(ns, env):
    cred = Delegated(env, env.seed_agent())
    call, ref = dispatch(env, ns, cred)
    config = sealed_tool_settings(call)
    assert config["credential_slot"]["fields"] == ["access_token"]
    assert config["auth_mode"] == "entra_delegated" and config["site_url"] == SITE
    [entry] = context_row(ns, env, ref).allowed_bindings
    assert entry["auth_mode"] == "entra_delegated"


def test_a_stored_secret_keeps_its_entry_and_slot_byte_for_byte(ns, env):
    cred = seeded(env)
    env.grants[(1, cred.key(PERSON_REF))] = env.grants.pop((1, cred.key()))
    call, ref = dispatch(env, ns, cred)
    [entry] = context_row(ns, env, ref).allowed_bindings
    assert "auth_mode" not in entry and "auth_mode" not in sealed_tool_settings(call)
    assert sealed_tool_settings(call)["credential_slot"]["fields"] == ["token"]


@pytest.mark.parametrize("inline", [{"access_token": "eyJ.inline.token"}, {"client_secret": "inline-secret"}])
def test_a_delegated_credential_carrying_any_value_inline_is_refused(ns, env, inline):
    cred = Delegated(env, env.seed_agent(), inline=inline)
    with pytest.raises(ns.exceptions.AdmissionDenied) as refused:
        dispatch(env, ns, cred)
    assert refused.value.reason_code == "CREDENTIAL_REFERENCE_REQUIRED"


def test_a_configuration_that_says_delegated_over_a_stored_secret_is_refused(ns, env):
    cred = seeded(env)
    env.grants[(1, cred.key(PERSON_REF))] = env.grants.pop((1, cred.key()))
    payload = payload_of(cred)
    payload["application"]["version_details"]["tools"][0]["settings"]["credentials"]["auth_mode"] = "entra_delegated"
    with pytest.raises(ns.exceptions.AdmissionDenied) as refused:
        dispatch(env, ns, cred, payload=payload)
    assert refused.value.reason_code == "CREDENTIAL_REFERENCE_REQUIRED"


def test_has_free_form_secret_is_false_for_a_delegated_configuration(ns, env):
    cred = Delegated(env, env.seed_agent())
    assert ns.credential_slots.has_free_form_secret(env.module(), payload_of(cred)) is False


# --- Deliverable B: the release ------------------------------------------------------------------------------------

def test_a_delegated_release_hands_over_a_token_minted_on_behalf_of_the_person(ns, env):
    ns.need_transport()
    run = admitted(ns, env, Delegated(env, env.seed_agent()))
    reply = run.release()
    assert reply["ok"] is True, reply
    [item] = reply["releases"]
    body = run.opened(item)
    assert body["fields"] == {"access_token": GRAPH_TOKEN}
    [(url, form)] = env.entra.posts
    assert url == f"https://login.microsoftonline.com/{TID}/oauth2/v2.0/token"
    assert sorted(form) == ["assertion", "client_id", "client_secret", "grant_type", "requested_token_use", "scope"]
    assert (form["grant_type"], form["requested_token_use"], form["assertion"], form["client_id"], form["scope"]) == (
        "urn:ietf:params:oauth:grant-type:jwt-bearer", "on_behalf_of", ASSERTION, "api-client", f"{GRAPH}/Sites.Selected")
    assert env.custody.calls == [(TID, OID, False)]
    [(_, event)] = env.recorded
    assert (event["reason_code"], event["target_ref"], event["decision"]) == ("CREDENTIAL_RELEASED_DELEGATED", GRAPH, "allow")
    left = (ns.adm._aware(__import__("datetime").datetime.fromisoformat(item["expires_at"])).timestamp()   # pylint: disable=W0212
            - time.time())
    assert 0 < left <= 300
    for text in (json.dumps(reply), json.dumps([e for _, e in env.recorded], default=str), "\n".join(env.log.lines)):
        assert GRAPH_TOKEN not in text and ASSERTION not in text and CLIENT_SECRET not in text


def test_a_delegated_release_writes_nothing_anywhere_and_reads_what_a_stored_release_reads(ns, env, monkeypatch):
    """Roman, 2026-10-28 (binding): the exchanged token is never written to Vault, the secrets store or a table."""
    ns.need_transport()
    run = admitted(ns, env, Delegated(env, env.seed_agent()))
    store_calls = []
    monkeypatch.setattr(ns.tools.secret_stores, "get_store", lambda *a, **k: store_calls.append(a) or
                        (_ for _ in ()).throw(AssertionError("the store was used")), raising=False)
    seen = []
    engine = env.engine(1)
    listener = lambda conn, cursor, statement, params, context, many: seen.append(statement)   # noqa: E731
    ns.sa.event.listen(engine, "before_cursor_execute", listener)
    try:
        assert run.release()["ok"] is True
    finally:
        ns.sa.event.remove(engine, "before_cursor_execute", listener)
    assert store_calls == []
    assert [text.lstrip().split()[0].upper() for text in seen] == ["SELECT", "SELECT"]   # the context row, its binding
    assert len(env.batch_release_calls) + len(env.release_calls) == 1                  # the reference re-read, as today
    assert GRAPH_TOKEN not in json.dumps([e for _, e in env.recorded], default=str)
    assert all(GRAPH_TOKEN not in str(value) for value in env.redis.values.values())


def test_the_second_release_is_served_from_memory(ns, env):
    ns.need_transport()
    run = admitted(ns, env, Delegated(env, env.seed_agent()))
    assert run.release()["ok"] is True and run.release()["ok"] is True
    assert len(env.entra.posts) == 1 and len(env.custody.calls) == 1


def test_an_expired_assertion_is_refreshed_once_and_exchanged_once_more(ns, env):
    ns.need_transport()
    run = admitted(ns, env, Delegated(env, env.seed_agent()))
    env.entra.answers.append((400, {"error": "invalid_grant", "error_codes": [500133]}))
    assert run.release()["ok"] is True
    assert env.custody.calls == [(TID, OID, False), (TID, OID, True)] and len(env.entra.posts) == 2
    assert any("refreshed once" in line for line in env.log.lines)


def test_a_second_invalid_grant_is_a_readable_refusal(ns, env):
    run = admitted(ns, env, Delegated(env, env.seed_agent()))
    env.entra.answers += [(400, {"error": "invalid_grant"}), (400, {"error": "invalid_grant", "error_codes": [65001]})]
    reply = run.release()
    assert reply["reason_code"] == "DELEGATION_EXCHANGE_FAILED" and "consent" in reply["message"]
    assert len(env.entra.posts) == 2
    assert {event["reason_code"] for event in [e for _, e in env.recorded] + env.denied} == {"DELEGATION_EXCHANGE_FAILED"}


@pytest.mark.parametrize("answer, code", [
    ({"ok": False, "code": "ASSERTION_UNAVAILABLE"}, "DENIED_ASSERTION_UNAVAILABLE"),
    ({"ok": True, "assertion": jwt(tid=TID, oid="someone-else")}, "DENIED_SUBJECT_MISMATCH"),
    ({"ok": True, "assertion": "not-a-jwt"}, "DENIED_ASSERTION_UNAVAILABLE"),
])
def test_the_custody_must_hold_the_persons_own_assertion(ns, env, answer, code):
    run = admitted(ns, env, Delegated(env, env.seed_agent()))
    env.custody.answer = answer
    reply = run.release()
    assert reply["ok"] is False and reply["reason_code"] == code
    assert env.entra.posts == []
    events = [event for _, event in env.recorded] + env.denied
    assert events and {event["reason_code"] for event in events} == {code}


def test_a_descriptor_of_another_tenant_is_a_subject_mismatch(ns, env):
    cred = Delegated(env, env.seed_agent())
    env.delegations[cred.ref_id] = descriptor(tenant_id="99999999-2222-3333-4444-555555555555")
    reply = admitted(ns, env, cred).release()
    assert reply["reason_code"] == "DENIED_SUBJECT_MISMATCH" and env.custody.calls == []


def test_a_context_without_the_explicit_subject_fields_is_refused_not_parsed(ns, env):
    run = admitted(ns, env, Delegated(env, env.seed_agent()))
    table = ns.models.ExecutionSecurityContext.__table__
    with env.engine(1).begin() as connection:
        row = connection.execute(ns.sa.select(table.c.subject).where(table.c.id == uuid.UUID(run.ref))).scalar_one()
        connection.execute(table.update().where(table.c.id == uuid.UUID(run.ref)).values(
            subject={key: value for key, value in row.items() if key not in ("oid", "issuer")}))
    assert run.release()["reason_code"] == "STALE_CONTEXT" and env.custody.calls == []


def test_a_keycloak_login_is_told_a_connected_account_is_needed(ns, env):
    env.verify_result["principal"]["evidence"]["issuer"] = KEYCLOAK
    reply = admitted(ns, env, Delegated(env, env.seed_agent())).release()
    assert reply["reason_code"] == "DENIED_ASSERTION_SOURCE_UNSUPPORTED" and "connected Microsoft account" in reply["message"]
    assert env.custody.calls == [] and env.entra.posts == []


def test_a_connected_account_source_is_not_delivered_yet(ns, env):
    cred = Delegated(env, env.seed_agent())
    env.delegations[cred.ref_id] = descriptor(assertion_source="connected_account")
    assert admitted(ns, env, cred).release()["reason_code"] == "DENIED_ASSERTION_SOURCE_UNSUPPORTED"


def test_a_personal_delegated_credential_of_its_custodian_is_released(ns, env):
    ns.need_transport()
    run = admitted(ns, env, Delegated(env, env.seed_agent(), personal=True))
    assert run.release()["ok"] is True


def test_a_mixed_batch_reads_the_store_once_and_mints_the_delegated_slot(ns, env):
    ns.need_transport()
    env.batch_store = True
    static = seeded(env)
    env.grants[(1, static.key(PERSON_REF))] = env.grants.pop((1, static.key()))
    delegated = Delegated(env, static.binding, tool_id=2, config_id=6)
    run = admitted(ns, env, static, delegated)
    reply = run.release()
    assert reply["ok"] is True, reply
    bodies = {item["toolkit_instance_id"]: run.opened(item)["fields"] for item in reply["releases"]}
    assert bodies == {"1": {"token": f"{admission_env_secret()}-token"}, "2": {"access_token": GRAPH_TOKEN}}
    assert len(env.batch_release_calls) == 1
    assert [e["reason_code"] for _, e in env.recorded] == ["CREDENTIAL_RELEASED", "CREDENTIAL_RELEASED_DELEGATED"]


def admission_env_secret():
    from fixtures.governed_credentials import SECRET  # pylint: disable=C0415
    return SECRET


# --- the app client seam ------------------------------------------------------------------------------------------

def test_the_platform_client_needs_its_id_and_secret(ns, env, monkeypatch):
    monkeypatch.delenv("ELITEA_API_CLIENT_SECRET")
    reply = admitted(ns, env, Delegated(env, env.seed_agent())).release()
    assert reply["reason_code"] == "DENIED_OAUTH_APP_UNAVAILABLE" and env.entra.posts == []


def test_an_admin_app_resolves_through_the_7149_seam_and_its_secret_goes_to_entra_only(ns, env, monkeypatch):
    app_id = str(uuid.uuid4())
    asked = []

    def resolve(app, project_id, credential_type):
        asked.append((app, project_id, credential_type))
        return {"client_id": "admin-app", "client_secret": "admin-secret-made-up",
                "token_endpoint": f"https://login.microsoftonline.com/{TID}/oauth2/v2.0/token"}

    monkeypatch.setattr(ns.entra_obo, "_resolver", lambda: resolve)
    assert ns.entra_obo.app_client(env.module(), descriptor(oauth_app_id=app_id), 1) == ("admin-app", "admin-secret-made-up")
    assert asked == [(app_id, 1, "sharepoint")]
    resolve_elsewhere = lambda *a: {"client_id": "x", "client_secret": "y", "token_endpoint": "https://evil.example/t"}  # noqa: E731
    monkeypatch.setattr(ns.entra_obo, "_resolver", lambda: resolve_elsewhere)
    with pytest.raises(ns.entra_obo.DelegationRefused) as refused:
        ns.entra_obo.app_client(env.module(), descriptor(oauth_app_id=app_id), 1)
    assert refused.value.code == "DENIED_OAUTH_APP_UNAVAILABLE"
    monkeypatch.setattr(ns.entra_obo, "_resolver", lambda: None)   # #7149 not installed
    with pytest.raises(ns.entra_obo.DelegationRefused):
        ns.entra_obo.app_client(env.module(), descriptor(oauth_app_id=app_id), 1)


# --- readiness gap 2: the paths that hold a long-lived token never serve a delegated credential --------------------

def test_a_mandate_is_refused_a_delegated_credential_at_decision_time(ns, env):
    decision = ns.credential_decision
    ctx = decision.CredentialDecisionContext(
        binding_project_id=1, agent_binding_ref="b", subject_ref="mandate:m", policy_revision="r",
        now=ns.real_now(), expires_at=ns.real_now(), can_use=lambda: True, subject_kind="mandate", subject_user_id=5)
    use = decision.CredentialUse("1", "credentials", "cfg", "uuid", 1)
    facts = decision.CredentialFacts(
        {"cfg": {"id": 5, "project_id": 1, "section": "credentials", "generation": 3}},
        {5: {"ref_id": "r", "state": "active", "generation": 1, "configuration_generation": 3, "store_kind": "database",
             "ownership_class": "project_shared", "revocation_epoch": 1, "auth_mode": "entra_delegated"}}, {})
    verdict = decision.authorize_credential_use(ctx, use, facts)
    assert (verdict.allow, verdict.reason_code) == (False, "DENIED_MANDATE_DELEGATION_UNSUPPORTED")


def test_a_mandate_context_is_refused_at_release_too(ns):
    with pytest.raises(ns.entra_obo.DelegationRefused) as refused:
        ns.entra_obo.run_subject({"kind": "mandate", "ref": "mandate:m"}, TID, {"kind": "trigger"})
    assert refused.value.code == "DENIED_MANDATE_DELEGATION_UNSUPPORTED"


@pytest.mark.parametrize("caller", [{"kind": "token", "user_id": 42}, {"kind": "trigger"}, None])
def test_a_run_that_is_not_a_signed_in_persons_is_refused_at_release(ns, caller):
    with pytest.raises(ns.entra_obo.DelegationRefused) as refused:
        ns.entra_obo.run_subject({"kind": "delegation", "issuer": ENTRA, "oid": OID}, TID, caller)
    assert refused.value.code == "DENIED_DELEGATION_UNSUPPORTED_CALLER"


def test_a_probe_ticket_names_no_person_so_a_delegated_slot_is_refused_there(ns, env):
    scope = ns.credential_release._Scope(1, True, ns.real_now(), [])   # pylint: disable=W0212
    request = types.SimpleNamespace(slot={"fields": ["access_token"]}, toolkit_instance_id="probe", field="configuration")
    with pytest.raises(ns.exceptions.AdmissionDenied) as refused:
        ns.credential_release._delegated(env.module(), request, scope, descriptor())  # pylint: disable=W0212
    assert refused.value.reason_code == "DENIED_DELEGATION_UNSUPPORTED_CALLER"


def test_the_attachment_read_seals_a_delegated_credential_as_a_refusal(ns, env):
    cred = Delegated(env, env.seed_agent())
    payload = payload_of(cred)
    classify = lambda pairs: {cfg_uuid: {"section": "credentials"} for cfg_uuid, _ in pairs}  # noqa: E731
    with pytest.raises(ns.credential_decision.CredentialDecisionError) as refused:
        ns.credential_slots.seal_payload(payload, lambda: [], classify=classify)   # CREDENTIAL_FREE: no decisions
    assert refused.value.reason_code == "CREDENTIAL_REFERENCE_REQUIRED"
    assert "credential_slot" not in sealed_tool_settings(call_kwargs(payload))


def test_an_api_token_toolkit_run_is_not_admitted_with_a_delegated_credential(ns, env):
    cred = Delegated(env, env.seed_agent())
    call = call_kwargs({"toolkit_config": {"id": cred.tool_id, "type": "sharepoint", "settings": cred.tool()["settings"]}})
    with pytest.raises(ns.exceptions.AdmissionDenied) as refused:
        ns.adm.admit_dispatch(env.module(), ns.adm.TOOLKIT_TASK, call,
                              env.request(entry_point="toolkit_test", caller_type="token", caller_id=9))
    assert refused.value.reason_code == "TOOLKIT_RUN_TOKEN_NOT_ADMITTED"


def test_a_legacy_api_token_caller_is_never_a_delegation_subject(ns, env):
    env.verify_result = {"ok": True, "principal": admission_env.principal(verified=False)}
    cred = Delegated(env, env.seed_agent())
    with pytest.raises(ns.exceptions.AdmissionDenied) as refused:
        dispatch(env, ns, cred, caller_type="token", caller_id=9, module=env.module(admit_legacy_pat=True))
    assert refused.value.reason_code == "MANDATE_REQUIRED"


# --- readiness gap 3: release never keys on a token's scope ----------------------------------------------------------

def test_release_decides_from_the_context_row_and_the_slot_kind_only(ns):
    person = ns.entra_obo.run_subject({"kind": "delegation", "issuer": ENTRA, "oid": OID, "scope": ""}, TID,
                                      {"kind": "user", "user_id": 42, "scope": ""})
    assert person == {"issuer": ENTRA, "tid": TID, "oid": OID}


# --- the exchange's own guards -------------------------------------------------------------------------------------

def test_the_token_endpoint_is_reached_only_over_https_on_the_authority_without_redirects(ns, monkeypatch):
    sent = []

    class Session:
        trust_env, max_redirects, proxies = True, 30, None

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def post(self, url, **kwargs):
            sent.append((url, kwargs, self.trust_env, self.max_redirects, self.proxies))
            return "response"

    monkeypatch.setattr(ns.entra_obo.requests, "Session", Session)
    post = ns.entra_obo._post   # pylint: disable=W0212
    for url in ("http://login.microsoftonline.com/t/oauth2/v2.0/token", "https://evil.example/t",
                "https://login.microsoftonline.com:8443/t"):
        with pytest.raises(ns.entra_obo.DelegationRefused):
            post(url, {})
    assert post("https://login.microsoftonline.com/t/oauth2/v2.0/token", {"a": 1}) == "response"
    [(_, kwargs, trust_env, redirects, proxies)] = sent
    assert (kwargs["allow_redirects"], kwargs["verify"], kwargs["timeout"]) == (False, True, ns.entra_obo.HTTP_TIMEOUT_S)
    assert (trust_env, redirects, proxies) == (False, 0, {})


def test_the_cache_is_bounded_and_serves_only_tokens_with_life_left(ns):
    obo = ns.entra_obo
    obo.forget()
    now = time.time()
    obo._keep(("a",), "t1", now + 30, None, now)                  # pylint: disable=W0212
    assert obo._cached(("a",), now) is None                       # pylint: disable=W0212
    for index in range(obo.MAX_CACHE_ENTRIES + 5):
        obo._keep((index,), "t", now + 3600, None, now)           # pylint: disable=W0212
    assert len(obo._CACHE) <= obo.MAX_CACHE_ENTRIES               # pylint: disable=W0212
    obo.forget()


def test_a_probe_keeps_its_outcome_a_minute_and_never_the_token(ns, env):
    person = {"issuer": ENTRA, "tid": TID, "oid": OID}
    delegation = {**descriptor(), "ownership_class": "project_shared", "custodian_subject_ref": None}
    assert ns.entra_obo.probe(env.module(), "ref-1", person, delegation, 1) is None
    env.custody.answer = {"ok": False, "code": "ASSERTION_UNAVAILABLE"}
    ns.entra_obo._CACHE.clear()                                   # pylint: disable=W0212
    assert ns.entra_obo.probe(env.module(), "ref-1", person, delegation, 1) is None   # kept
    assert all(GRAPH_TOKEN not in str(value) for value in ns.entra_obo._PROBES.values())   # pylint: disable=W0212
    ns.entra_obo._PROBES.clear()                                  # pylint: disable=W0212
    assert ns.entra_obo.probe(env.module(), "ref-1", person, delegation, 1) == "DENIED_ASSERTION_UNAVAILABLE"


def test_without_the_custody_request_key_nothing_is_asked_for(ns, env, monkeypatch):
    monkeypatch.setenv("ELITEA_OBO_CUSTODY_KEY", "short")
    reply = admitted(ns, env, Delegated(env, env.seed_agent())).release()
    assert reply["reason_code"] == "DEPENDENCY_UNAVAILABLE" and env.custody.calls == [] and env.entra.posts == []


def test_the_exchange_fits_within_the_workers_wait(ns, env, monkeypatch):
    run = admitted(ns, env, Delegated(env, env.seed_agent()))
    env.entra.answers.append((400, {"error": "invalid_grant"}))
    clock = [1000.0]
    monkeypatch.setattr(ns.entra_obo.time, "monotonic", lambda: clock[0])
    real = env.entra.__call__

    def slow(url, data, timeout=None):   # the first exchange takes most of the budget
        clock[0] += ns.entra_obo.BUDGET_S - 1
        return real(url, data, timeout)

    monkeypatch.setattr(ns.entra_obo, "_post", slow)
    assert run.release()["reason_code"] == "DEPENDENCY_UNAVAILABLE"
    assert len(env.entra.posts) == 1 and env.custody.calls == [(TID, OID, False)]   # no refresh round started
    assert all(seconds <= ns.entra_obo.RPC_TIMEOUT_S for seconds in env.custody.timeouts)
    assert all(read <= ns.entra_obo.HTTP_TIMEOUT_S for _connect, read in env.entra.timeouts)
    assert ns.entra_obo.BUDGET_S < 15   # the worker's release wait (credential_resolver DEFAULT_TIMEOUT_S)


def test_a_cached_token_is_served_only_with_a_quarter_hour_left(ns):
    assert ns.entra_obo.MIN_REMAINING_S >= 900


def test_the_probe_outcomes_are_bounded(ns, env):
    ns.entra_obo.forget()
    person = {"issuer": ENTRA, "tid": TID, "oid": OID}
    delegation = {**descriptor(assertion_source="connected_account"), "ownership_class": "project_shared",
                  "custodian_subject_ref": None}
    for index in range(ns.entra_obo.MAX_CACHE_ENTRIES + 3):
        ns.entra_obo.probe(env.module(), f"ref-{index}", person, delegation, 1)
    assert len(ns.entra_obo._PROBES) <= ns.entra_obo.MAX_CACHE_ENTRIES   # pylint: disable=W0212
    ns.entra_obo.forget()


def test_the_onboarding_can_ask_whether_an_app_resolves_without_seeing_its_secret(ns, env, monkeypatch):
    assert ns.entra_obo.app_available(env.module(), descriptor(), 1) is True
    assert ns.entra_obo.app_available(env.module(), descriptor(oauth_app_id=str(uuid.uuid4())), 1) is False
    monkeypatch.delenv("ELITEA_API_CLIENT_SECRET")
    assert ns.entra_obo.app_available(env.module(), descriptor(), 1) is False


def test_every_new_code_has_a_readable_message(ns):
    for code in ns.entra_obo.CODES:
        assert ns.adm.MESSAGES[code] != ns.adm.GENERIC_MESSAGE


def test_a_real_mandate_admission_with_a_delegated_credential_is_refused(ns, env):
    """Keyed on the mandate admission itself (its subject), whatever the schedule starter or the owner token do."""
    binding = env.seed_agent()
    mandate = mandate_env.add_mandate(env, ns, state="active", binding_ref=binding, operations=("run:schedule",))
    cred = Delegated(env, binding)
    env.grants[(1, cred.key(f"mandate:{mandate}"))] = {
        "grant_id": str(uuid.uuid4()), "state": "active", "revocation_epoch": 4, "valid_from": None,
        "valid_until": None, "operations": ["*"]}
    request = ns.AdmissionRequest(
        project_id=1, binding_project_id=1, application_id=7, version_id=70, run_kind="mandate",
        entry_point="schedule", caller_type="trigger", mandate_ref=str(mandate), trigger_ref="schedule:version:70")
    with pytest.raises(ns.exceptions.AdmissionDenied) as refused:
        ns.adm.admit_dispatch(env.module(), "indexer_agent", call_kwargs(payload_of(cred)), request)
    assert refused.value.reason_code == "DENIED_MANDATE_DELEGATION_UNSUPPORTED"
    assert env.custody.calls == [] and env.entra.posts == []



# --- delta round (independent review, 2026-10-28) -----------------------------------------------------------------

def test_another_persons_personal_credential_is_refused_at_release_on_its_own(ns, env, monkeypatch):
    """Admission refuses it earlier; the release layer refuses it too, without asking for any assertion."""
    run = admitted(ns, env, Delegated(env, env.seed_agent(), personal=True))
    real = env.delegated_value

    def someone_elses(row, generation):
        value = real(row, generation)
        value.delegation["custodian_subject_ref"] = f"idp:{TID}:oid-someone-else"
        return value

    monkeypatch.setattr(env, "delegated_value", someone_elses)
    assert run.release()["reason_code"] == "DENIED_SUBJECT_MISMATCH"
    assert env.custody.calls == [] and env.entra.posts == []


def test_a_person_is_never_served_another_persons_cached_token(ns, env):
    ns.need_transport()
    binding = env.seed_agent()
    cred = Delegated(env, binding)
    first = admitted(ns, env, cred)
    assert first.opened(first.release()["releases"][0])["fields"] == {"access_token": GRAPH_TOKEN}
    other = admission_env.principal(user_id=43, tid=TID, oid="oid-43", epochs=dict(env.epochs[43]))
    other["evidence"]["issuer"] = ENTRA
    env.verify_result = {"ok": True, "principal": other}
    env.grants[(1, cred.key(f"idp:{TID}:oid-43"))] = dict(env.grants[(1, cred.key(PERSON_REF))])
    call, ref = dispatch(env, ns, cred, caller_id=43)
    run_to_running(env, ns, ref)
    private, public = ns.keypair()
    slot = sealed_tool_settings(call)["credential_slot"]
    reply = ns.credential_release.release_credentials(env.module(), project_id=1, context_ref=ref, probe_ref=None,
                                                      caller=dict(CALLER), recipient_key=public, slots=[
        {"toolkit_instance_id": str(cred.tool_id), "field": "credentials", "slot": dict(slot)}])
    [item] = reply["releases"]
    aad = "|".join(("v1", "1", ref, item["toolkit_instance_id"], item["field"], slot["decision_id"],
                    item["release_id"])).encode()
    assert json.loads(ns.transport.open_sealed(item["sealed"], private, aad))["fields"] == {
        "access_token": "graph-token-of-oid-43"}
    assert env.custody.calls == [(TID, OID, False), (TID, "oid-43", False)] and len(env.entra.posts) == 2


def test_another_exchanging_client_misses_the_cache(ns, env, monkeypatch):
    person = {"issuer": ENTRA, "tid": TID, "oid": OID}
    delegation = {**descriptor(), "ownership_class": "project_shared", "custodian_subject_ref": None}
    module = env.module()
    ns.entra_obo.acquire(module, person, delegation, 1)
    module.descriptor.config["entra_obo_client_id"] = "another-client"
    env.custody.answer = {"ok": True, "assertion": jwt(tid=TID, oid=OID, aud="another-client")}
    ns.entra_obo.acquire(module, person, delegation, 1)
    assert len(env.entra.posts) == 2 and [form["client_id"] for _, form in env.entra.posts] == ["api-client",
                                                                                               "another-client"]


@pytest.mark.parametrize("aud", ["https://graph.microsoft.com", "another-client", None])
def test_an_assertion_for_another_app_is_never_exchanged(ns, env, aud):
    env.custody.answer = {"ok": True, "assertion": jwt(tid=TID, oid=OID, aud=aud)}
    reply = admitted(ns, env, Delegated(env, env.seed_agent())).release()
    assert reply["reason_code"] == "DENIED_OAUTH_APP_UNAVAILABLE" and env.entra.posts == []


def test_a_v1_audience_of_the_exchanging_app_is_accepted(ns, env):
    ns.need_transport()
    env.custody.answer = {"ok": True, "assertion": jwt(tid=TID, oid=OID, aud="api://api-client")}
    assert admitted(ns, env, Delegated(env, env.seed_agent())).release()["ok"] is True


def test_the_release_budget_of_two_slots_stays_within_the_workers_wait(ns):
    assert ns.entra_obo.request_budget(2) == 13 < 15 + 0.5 * 2   # the worker still waits for the reply


def test_two_delegated_slots_share_one_budget_within_the_workers_wait(ns, env, monkeypatch):
    ns.need_transport()   # the first slot must seal before the second one runs out of budget
    binding = env.seed_agent()
    first = Delegated(env, binding)
    second = Delegated(env, binding, tool_id=2, config_id=6)
    env.delegations[second.ref_id] = descriptor(scopes=[f"{GRAPH}/Files.Read.All"])
    run = admitted(ns, env, first, second)
    clock = [1000.0]
    monkeypatch.setattr(ns.entra_obo.time, "monotonic", lambda: clock[0])
    real = env.entra.__call__

    def slow(url, data, timeout=None):   # Entra answers in 8 s; a read timeout below that times out
        connect, read = timeout
        if read < 8:
            clock[0] += read
            raise ns.entra_obo.requests.exceptions.Timeout()
        clock[0] += 8
        return real(url, data, timeout)

    monkeypatch.setattr(ns.entra_obo, "_post", slow)
    monkeypatch.setattr(ns.entra_obo, "HTTP_TIMEOUT_S", 10)
    started = clock[0]
    reply = run.release()
    assert reply["reason_code"] == "DEPENDENCY_UNAVAILABLE" and (reply["toolkit_instance_id"], reply["field"]) == (
        "2", "credentials")
    assert clock[0] - started <= ns.entra_obo.request_budget(2)


def test_the_authority_path_is_built_from_the_checked_tenant(ns, env):
    person = {"issuer": ENTRA, "tid": "../evil", "oid": OID}
    delegation = {**descriptor(tenant_id="../evil"), "ownership_class": "project_shared", "custodian_subject_ref": None}
    # custody would vouch for that tenant, so only the GUID check in acquire stands between it and the authority path
    env.custody.answer = {"ok": True, "assertion": jwt(tid="../evil", oid=OID, aud="api-client")}
    with pytest.raises(ns.entra_obo.DelegationRefused) as refused:
        ns.entra_obo.acquire(env.module(), {**person, "issuer": None}, delegation, 1)
    assert refused.value.code == "DENIED_SUBJECT_MISMATCH" and env.entra.posts == [] and env.custody.calls == []


def test_signing_out_drops_the_persons_cached_tokens_only(ns, env):
    delegation = {**descriptor(), "ownership_class": "project_shared", "custodian_subject_ref": None}
    module = env.module()
    ns.entra_obo.acquire(module, {"issuer": ENTRA, "tid": TID, "oid": OID}, delegation, 1)
    ns.entra_obo.acquire(module, {"issuer": ENTRA, "tid": TID, "oid": "oid-43"}, delegation, 1)
    assert ns.entra_obo.forget_subject(TID.upper(), OID) == 1
    assert [key[1] for key in ns.entra_obo._CACHE] == ["oid-43"]   # pylint: disable=W0212
    ns.entra_obo.acquire(module, {"issuer": ENTRA, "tid": TID, "oid": OID}, delegation, 1)
    assert len(env.entra.posts) == 3   # signed out: the next release asks custody again


def test_the_logout_event_drops_the_persons_tokens(ns, env, monkeypatch):
    from fixtures.helpers import load_module_with_stubs  # pylint: disable=C0415
    monkeypatch.setattr(sys.modules["pylon.core.tools"], "web",
                        types.SimpleNamespace(event=lambda name: (lambda func: func)), raising=False)
    events = load_module_with_stubs(PLUGIN_ROOT / "events" / "obo_custody.py", f"{admission_env.PKG}.events.obo_custody")
    dropped = []
    monkeypatch.setattr(ns.entra_obo, "forget_subject", lambda tid, oid: dropped.append((tid, oid)))
    events.Event().entra_obo_custody_dropped(None, "auth_oidc_obo_custody_dropped", {"tid": TID, "oid": OID})
    events.Event().entra_obo_custody_dropped(None, "auth_oidc_obo_custody_dropped", "not-a-dict")
    assert dropped == [(TID, OID), (None, None)]


def test_startup_says_so_when_the_platform_client_has_no_custody_key(ns, env, monkeypatch):
    module = env.module()
    assert ns.entra_obo.check_config(module) is True
    monkeypatch.delenv("ELITEA_OBO_CUSTODY_KEY")
    env.log.lines.clear()
    assert ns.entra_obo.check_config(module) is False
    assert any("custody request key" in line for line in env.log.lines)
    module.descriptor.config.pop("entra_obo_client_id")
    assert ns.entra_obo.check_config(module) is True   # nothing configured, nothing to say


# --- step 4b: a login brokered to Entra by a Keycloak realm ----------------------------------------------------------

@pytest.fixture
def brokered(env, monkeypatch):
    """The run's person signed in through the realm KEYCLOAK, which maps Entra's tid/oid into its tokens and is listed
    as a broker; auth_oidc's custody (the fake) holds the brokered Entra sign-in under that Entra subject."""
    inner = env.module

    def module(**kwargs):
        built = inner(**kwargs)
        built.descriptor.config["entra_obo_broker_issuers"] = [KEYCLOAK]
        return built

    monkeypatch.setattr(env, "module", module)
    env.verify_result["principal"]["evidence"]["issuer"] = KEYCLOAK
    return env


def test_the_brokered_subject_is_the_entra_identity_with_the_realm_as_issuer(ns, brokered):
    run = admitted(ns, brokered, Delegated(brokered, brokered.seed_agent()))
    row = context_row(ns, brokered, run.ref)
    assert (row.subject["issuer"], row.tenant, row.subject["oid"], row.subject["ref"]) == (KEYCLOAK, TID, OID, PERSON_REF)
    assert ns.entra_obo.run_subject(row.subject, row.tenant, {"kind": "user"}) == {"issuer": KEYCLOAK, "tid": TID,
                                                                                    "oid": OID}


def test_a_brokered_person_gets_the_same_exchange_as_a_direct_microsoft_sign_in(ns, brokered):
    ns.need_transport()
    run = admitted(ns, brokered, Delegated(brokered, brokered.seed_agent(), personal=True))
    reply = run.release()
    assert reply["ok"] is True, reply
    assert run.opened(reply["releases"][0])["fields"] == {"access_token": GRAPH_TOKEN}
    assert brokered.custody.calls == [(TID, OID, False)]                     # the custody key is the Entra (tid, oid)
    [(url, form)] = brokered.entra.posts
    assert url == f"https://login.microsoftonline.com/{TID}/oauth2/v2.0/token" and form["assertion"] == ASSERTION


def test_a_brokered_person_of_another_tenant_is_refused(ns, brokered):
    cred = Delegated(brokered, brokered.seed_agent())
    brokered.delegations[cred.ref_id] = descriptor(tenant_id="99999999-2222-3333-4444-555555555555")
    assert admitted(ns, brokered, cred).release()["reason_code"] == "DENIED_SUBJECT_MISMATCH"
    assert brokered.custody.calls == [] and brokered.entra.posts == []


def test_a_brokered_person_without_a_kept_sign_in_is_told_to_sign_in_again(ns, brokered):
    run = admitted(ns, brokered, Delegated(brokered, brokered.seed_agent()))
    brokered.custody.answer = {"ok": False, "code": "ASSERTION_UNAVAILABLE"}
    assert run.release()["reason_code"] == "DENIED_ASSERTION_UNAVAILABLE" and brokered.entra.posts == []


@pytest.mark.parametrize("listed", [[], [KEYCLOAK + "/"], [KEYCLOAK + "2"], [KEYCLOAK.upper()],
                                    f"{KEYCLOAK} https://other.example"])   # a string, not a list: no substring match
def test_only_a_listed_realm_issuer_counts_as_a_broker_exactly(ns, env, monkeypatch, listed):
    inner = env.module

    def module(**kwargs):
        built = inner(**kwargs)
        built.descriptor.config["entra_obo_broker_issuers"] = listed
        return built

    monkeypatch.setattr(env, "module", module)
    env.verify_result["principal"]["evidence"]["issuer"] = KEYCLOAK
    reply = admitted(ns, env, Delegated(env, env.seed_agent())).release()
    assert reply["reason_code"] == "DENIED_ASSERTION_SOURCE_UNSUPPORTED" and env.custody.calls == []


# --- #7281: a toolkit Test run and on-demand indexing act with the person's own sign-in -----------------------------

@pytest.mark.parametrize("entry_point", ["toolkit_test", "index_on_demand"])
def test_a_toolkit_run_releases_a_delegated_credential_for_the_signed_in_person_with_development_runs_off(
        ns, env, entry_point):
    ns.need_transport()
    env.facts_version = 5
    cred = Delegated(env, env.seed_agent(), personal=True)
    cred.reference["custodian_user_id"] = 42
    tool = {"id": cred.tool_id, "type": "sharepoint", "settings": {"credentials": cred.expanded()}}
    call = call_kwargs({"toolkit_config": tool, "tool_name": "get_files", "tool_params": {}}, toolkit_config=tool)
    ref = ns.adm.admit_dispatch(env.module(allow_development_runs=False), ns.adm.TOOLKIT_TASK, call, env.request(
        entry_point=entry_point, application_id=None, version_id=None, toolkit_id=cred.tool_id))
    row = context_row(ns, env, ref)
    assert (row.profile, row.caller["kind"], row.subject["kind"]) == ("development", "user", "delegation")
    caller = {"kind": "indexer_worker", "task_name": ns.adm.TOOLKIT_TASK, "run_id": "run-1"}
    run_to_running(env, ns, ref, caller=caller)
    slot = call["kwargs"]["toolkit_config"]["settings"]["credentials"]["credential_slot"]
    assert slot["fields"] == ["access_token"]
    private, public = ns.keypair()
    env.custody.calls.clear()
    reply = ns.credential_release.release_credentials(
        env.module(allow_development_runs=False), project_id=1, context_ref=ref, probe_ref=None, caller=caller,
        recipient_key=public, slots=[{"toolkit_instance_id": str(cred.tool_id), "field": "credentials", "slot": slot}])
    assert reply["ok"] is True, reply
    assert env.custody.calls == [(TID, OID, False)]   # the person of the run: the signed-in session that started it
    assert [event["reason_code"] for _, event in env.recorded][-1] == "CREDENTIAL_RELEASED_DELEGATED"


# --- #7281 A1: the governed probe acts with the tester's own Microsoft sign-in ------------------------------------

PROBE_CALLER = {"kind": "indexer_worker", "task_name": "indexer_governed_check_connection", "run_id": None}


class Probe:
    """Check connection on a saved Entra-delegated credential: the route's in-process call, then the worker's release
    of the probe slot (the task itself is the harness's fake)."""

    def __init__(self, ns, env, monkeypatch, *, personal=True, holder=None, **over):
        self.ns, self.env = ns, env
        env.facts_version = 5
        env.permissions[42] = {"credentials.probe", "credentials.use"}
        self.cred = Delegated(env, env.seed_agent(), personal=personal, **over)
        self.cred.reference["custodian_user_id"] = 42 if personal else None
        self.holder = holder if holder is not None else types.SimpleNamespace(
            type="user", id=42, reference="session-ref")
        monkeypatch.setattr(ns.credential_probe, "_session_holder", lambda: self.holder)
        monkeypatch.setattr(ns.credential_probe, "_stored_configuration",
                            lambda project_id, configuration_uuid, user_id: self.cred.expanded())

    def check(self):
        return self.ns.credential_probe.governed_check_connection(
            self.env.module(), 1, self.cred.uuid, "sharepoint", 42)

    def ticket(self):
        prefix = self.ns.credential_release.PROBE_KEY
        [(key, raw)] = [(k, v) for k, v in self.env.at_start[0].items() if k.startswith(prefix)]
        return key[len(prefix):], json.loads(raw)

    def release(self):
        probe_ref, _ = self.ticket()
        [(_, kwargs, _, _)] = self.env.started
        self.env.redis.values.update(self.env.at_start[0])   # the ticket as the task found it
        private, public = self.ns.keypair()
        return self.ns.credential_release.release_credential(
            self.env.module(), project_id=1, context_ref=None, probe_ref=probe_ref, caller=dict(PROBE_CALLER),
            toolkit_instance_id="probe", field="configuration", slot=kwargs["settings"]["credential_slot"],
            recipient_key=public), private


@pytest.mark.parametrize("issuer", [ENTRA, "http://localhost:8082/realms/elitea"])   # direct Entra, brokered (4b)
def test_a_probe_of_a_delegated_credential_is_released_for_the_verified_person(ns, env, monkeypatch, issuer):
    ns.need_transport()
    env.verify_result["principal"]["evidence"]["issuer"] = issuer
    probe = Probe(ns, env, monkeypatch)
    probe.env.module = (lambda real: lambda **kw: _with_brokers(real(**kw), issuer))(env.module)
    assert probe.check() == {"success": True}
    [(_, kwargs, pool, _)] = env.started
    settings = kwargs["settings"]
    assert pool == "governed_ops" and settings["auth_mode"] == "entra_delegated" and settings["site_url"] == SITE
    assert settings["credential_slot"]["fields"] == ["access_token"] and "access_token" not in settings
    _, ticket = probe.ticket()
    assert ticket["person"] == {"issuer": issuer, "tid": TID, "oid": OID}   # the verified subject, not the user id
    env.recorded.clear()
    reply, _ = probe.release()
    assert reply["ok"] is True, reply
    assert env.custody.calls == [(TID, OID, False)]
    [(_, event)] = env.recorded
    assert (event["reason_code"], event["caller_ref"], event["target_ref"]) == (
        "CREDENTIAL_RELEASED_DELEGATED", "indexer_worker:indexer_governed_check_connection", GRAPH)
    assert GRAPH_TOKEN not in json.dumps(reply)


def _with_brokers(module, issuer):
    if issuer != ENTRA:
        module.descriptor.config["entra_obo_broker_issuers"] = [issuer]
    return module


def test_a_probe_without_a_kept_sign_in_says_sign_in_again(ns, env, monkeypatch):
    ns.need_transport()
    probe = Probe(ns, env, monkeypatch)
    env.custody.answer = {"ok": False, "code": "ASSERTION_UNAVAILABLE"}
    assert probe.check() == {"success": True}   # the fake task; the worker's release is what refuses
    reply, _ = probe.release()
    assert (reply["ok"], reply["reason_code"]) == (False, "DENIED_ASSERTION_UNAVAILABLE")
    # the worker hands the code back; the route says it with the step-4 text, not as a worker problem
    shaped = ns.credential_probe.shape_result({"success": False, "reason_code": "DENIED_ASSERTION_UNAVAILABLE"})
    assert shaped == {"success": False, "code": "DENIED_ASSERTION_UNAVAILABLE",
                      "message": ns.adm.MESSAGES["DENIED_ASSERTION_UNAVAILABLE"]}


def test_a_probe_of_another_persons_personal_credential_is_refused(ns, env, monkeypatch):
    probe = Probe(ns, env, monkeypatch, custodian="idp:{}:someone-else".format(TID))
    probe.cred.reference["custodian_user_id"] = 7
    answer = probe.check()
    assert (answer["success"], answer["code"]) == (False, "CREDENTIAL_CLASS_NOT_ALLOWED")
    assert env.started == [] and env.custody.calls == []


def test_a_personal_reference_whose_custodian_is_another_identity_is_refused_at_release(ns, env, monkeypatch):
    """The user id matches but the identity does not (a relinked account): the release binds to the verified one."""
    ns.need_transport()
    probe = Probe(ns, env, monkeypatch, custodian="idp:{}:someone-else".format(TID))
    assert probe.check() == {"success": True}
    reply, _ = probe.release()
    assert (reply["ok"], reply["reason_code"]) == (False, "DENIED_SUBJECT_MISMATCH")
    assert env.custody.calls == []


def test_a_probe_of_a_shared_delegated_credential_needs_a_grant_to_the_person(ns, env, monkeypatch):
    probe = Probe(ns, env, monkeypatch, personal=False)
    env.grants.clear()
    answer = probe.check()
    assert answer == {"success": False, "code": "DENIED_GRANT",
                      "message": ns.credential_probe.PROBE_MESSAGES["DENIED_GRANT"]}
    assert env.started == []
    [event] = env.denied
    assert (event["event_type"], event["reason_code"]) == ("probe", "DENIED_GRANT")


def test_a_probe_of_a_shared_delegated_credential_granted_to_the_person_runs(ns, env, monkeypatch):
    probe = Probe(ns, env, monkeypatch, personal=False)
    assert probe.check() == {"success": True}
    assert probe.ticket()[1]["person"]["oid"] == OID


@pytest.mark.parametrize("holder, code", [
    (types.SimpleNamespace(type="token", id=42, reference=None), "DENIED_DELEGATION_UNSUPPORTED_CALLER"),   # API token
    (None, "DENIED_DELEGATION_UNSUPPORTED_CALLER"),                                       # no request behind it
    (types.SimpleNamespace(type="user", id=43, reference="session-ref"), "DENIED_DELEGATION_UNSUPPORTED_CALLER"),
])
def test_a_probe_that_no_signed_in_session_of_the_tester_stands_behind_is_refused(ns, env, monkeypatch, holder, code):
    probe = Probe(ns, env, monkeypatch, holder=holder or types.SimpleNamespace())
    answer = probe.check()
    assert (answer["success"], answer["code"]) == (False, code)
    assert env.started == [] and env.custody.calls == []


@pytest.mark.parametrize("result", [
    {"ok": True, "principal": admission_env.principal(verified=False)},   # an asserted identity
    {"ok": True, "principal": admission_env.principal(tid=None, oid=None)},   # no Microsoft identity
    {"ok": False, "reason_code": "IDENTITY_EXPIRED"},
])
def test_a_session_without_a_verifiable_microsoft_identity_is_told_to_sign_in_again(ns, env, monkeypatch, result):
    probe = Probe(ns, env, monkeypatch)
    env.verify_result = result
    answer = probe.check()
    assert (answer["success"], answer["code"]) == (False, "DENIED_ASSERTION_UNAVAILABLE")
    assert env.started == []


def test_the_ticket_person_cannot_come_from_the_request(ns, env, monkeypatch):
    """The route hands over ids only; the stored configuration may carry anything, the ticket names the session's
    verified person."""
    probe = Probe(ns, env, monkeypatch)
    probe.cred.inline = {"person": {"issuer": ENTRA, "tid": TID, "oid": "someone-else"}, "oid": "someone-else"}
    assert probe.check() == {"success": True}
    assert probe.ticket()[1]["person"] == {"issuer": ENTRA, "tid": TID, "oid": OID}


@pytest.mark.parametrize("person", [None, {"issuer": ENTRA, "tid": TID}, {"issuer": ENTRA, "tid": TID, "oid": ""},
                                    {"issuer": ENTRA, "tid": TID, "oid": OID, "extra": 1}, "idp:x:y"])
def test_a_ticket_without_a_well_formed_person_releases_no_delegated_slot(ns, env, monkeypatch, person):
    ns.need_transport()
    probe = Probe(ns, env, monkeypatch)
    assert probe.check() == {"success": True}
    probe_ref, ticket = probe.ticket()
    ticket["person"] = person
    env.at_start[0][ns.credential_release.PROBE_KEY + probe_ref] = json.dumps(ticket)
    reply, _ = probe.release()
    assert (reply["ok"], reply["reason_code"]) == (False, "DENIED_DELEGATION_UNSUPPORTED_CALLER")
    assert env.custody.calls == []


def test_a_batch_probe_verifies_the_person_once_on_the_requests_own_greenlet(ns, env, monkeypatch):
    probe = Probe(ns, env, monkeypatch)
    calls = []
    real = ns.credential_probe._probe_person   # pylint: disable=W0212
    monkeypatch.setattr(ns.credential_probe, "_probe_person", lambda *a: calls.append(a) or real(*a))
    answers = ns.credential_probe.governed_check_connection_many(
        env.module(), 1, [{"configuration_uuid": probe.cred.uuid, "config_type": "sharepoint"}] * 2, 42)
    assert answers == [{"success": True}, {"success": True}] and len(calls) == 1


# --- #7281 A3: a token minted from a custody never outlives it --------------------------------------------------------

def test_a_cached_token_is_not_served_past_the_end_of_the_custody_it_came_from(ns, env):
    ns.need_transport()
    env.custody.answer = {"ok": True, "assertion": ASSERTION, "custody_expires_at": int(time.time()) + 3600}
    run = admitted(ns, env, Delegated(env, env.seed_agent()))
    assert run.release()["ok"] is True and run.release()["ok"] is True
    assert len(env.custody.calls) == 1   # the second from the cache, the custody still in force
    [key] = list(ns.entra_obo._CACHE)                                                  # pylint: disable=W0212
    token, expires_at, _ends = ns.entra_obo._CACHE[key]                                # pylint: disable=W0212
    ns.entra_obo._CACHE[key] = (token, expires_at, int(time.time()) - 1)               # pylint: disable=W0212
    env.custody.answer = {"ok": False, "code": "ASSERTION_UNAVAILABLE"}                # the sign-in has expired
    reply = run.release()
    assert (reply["ok"], reply["reason_code"]) == (False, "DENIED_ASSERTION_UNAVAILABLE")   # at once, not an hour later
    assert len(env.custody.calls) == 2 and ns.entra_obo._CACHE == {}                  # pylint: disable=W0212


def test_no_custody_drops_every_token_cached_for_the_person(ns, env):
    obo, now = ns.entra_obo, time.time()
    obo.forget()
    obo._keep((TID, OID, "api-client", GRAPH, ("a",)), "t1", now + 3600, None, now)        # pylint: disable=W0212
    obo._keep((TID, "oid-43", "api-client", GRAPH, ("a",)), "t2", now + 3600, None, now)   # pylint: disable=W0212
    env.custody.answer = {"ok": False, "code": "ASSERTION_UNAVAILABLE"}
    with pytest.raises(obo.DelegationRefused) as refused:
        obo._assertion(                                                                 # pylint: disable=W0212
            env.module(), {"tid": TID, "oid": OID}, False, lambda: 10, "api-client")
    assert refused.value.code == "DENIED_ASSERTION_UNAVAILABLE"
    assert [key[1] for key in obo._CACHE] == ["oid-43"]                                 # pylint: disable=W0212
    obo.forget()


def test_an_auth_oidc_that_does_not_say_when_custody_ends_keeps_the_cache_as_before(ns, env):
    ns.need_transport()
    run = admitted(ns, env, Delegated(env, env.seed_agent()))   # the fake answers no custody_expires_at
    assert run.release()["ok"] is True and run.release()["ok"] is True
    assert len(env.custody.calls) == 1


def test_a_probe_of_a_stored_secret_asks_nothing_more_than_before(ns, env, monkeypatch):
    """No identity check, no grant read, no person in the ticket: the delegated branch is not entered (#7281)."""
    env.permissions[42] = {"credentials.probe", "credentials.use"}
    cred = seeded(env)
    monkeypatch.setattr(ns.credential_probe, "_probe_person", lambda *a: pytest.fail("asked for a person"))
    monkeypatch.setattr(ns.credential_probe, "_stored_configuration", lambda *a: {
        "configuration_uuid": cred.uuid, "configuration_project_id": 1, "configuration_type": "jira",
        "configuration_generation": 3, "base_url": "https://jira.example", "token": "{{secret.jira_token}}"})
    env.secrets_calls.clear()
    assert ns.credential_probe.governed_check_connection(env.module(), 1, cred.uuid, "jira", 42) == {"success": True}
    assert len(env.secrets_calls) == 1 and env.subject_grant_calls[-1] is None   # the facts read of before, only
    [ticket] = [json.loads(v) for k, v in env.at_start[0].items() if k.startswith(ns.credential_release.PROBE_KEY)]
    assert set(ticket) == {"project_id", "credential_ref", "credential_generation", "task_name", "user_id"}


# --- #7281 fix round: the probe release checks the shared reference's grant again --------------------------------

def test_a_probe_of_a_shared_reference_names_its_grant_and_is_released_while_it_stands(ns, env, monkeypatch):
    ns.need_transport()
    probe = Probe(ns, env, monkeypatch, personal=False)
    assert probe.check() == {"success": True}
    assert probe.ticket()[1]["grant"] == {"grant_id": probe.cred.grant_id, "epoch": 4}
    reply, _ = probe.release()
    assert reply["ok"] is True, reply


@pytest.mark.parametrize("change, code", [
    (lambda grant: grant.update(state="revoked"), "DENIED_GRANT"),        # revoked while the probe was open
    (lambda grant: grant.update(revocation_epoch=5), "EPOCH_MISMATCH"),    # re-issued at another epoch
])
def test_a_grant_revoked_between_the_probe_and_the_release_refuses_the_token(ns, env, monkeypatch, change, code):
    ns.need_transport()
    probe = Probe(ns, env, monkeypatch, personal=False)
    assert probe.check() == {"success": True}
    change(env.grants[(1, probe.cred.key(PERSON_REF))])
    reply, _ = probe.release()
    assert (reply["ok"], reply["reason_code"]) == (False, code)
    assert env.custody.calls == [] and env.entra.posts == []   # no token minted
    assert env.denied[-1]["reason_code"] == code


def test_a_shared_reference_probe_ticket_without_its_grant_mints_nothing(ns, env, monkeypatch):
    ns.need_transport()
    probe = Probe(ns, env, monkeypatch, personal=False)
    assert probe.check() == {"success": True}
    probe_ref, ticket = probe.ticket()
    del ticket["grant"]
    env.at_start[0][ns.credential_release.PROBE_KEY + probe_ref] = json.dumps(ticket)
    reply, _ = probe.release()
    assert (reply["ok"], reply["reason_code"]) == (False, "DENIED_GRANT")
    assert env.custody.calls == []


# --- #7281 fix round: the validators' checks are answered from a kept outcome for a short while ------------------

def _validator_check(ns, env, cred, ctype="sharepoint"):
    return ns.credential_probe.governed_check_connection(env.module(), 1, cred.uuid, ctype, 42, reuse=True)


def test_a_validator_asking_again_gets_the_kept_outcome_without_a_probe(ns, env, monkeypatch):
    ns.credential_probe.forget_outcomes()
    probe = Probe(ns, env, monkeypatch)
    assert _validator_check(ns, env, probe.cred) == {"success": True}
    started, recorded = len(env.started), len(env.recorded)
    assert _validator_check(ns, env, probe.cred) == {"success": True}
    assert (len(env.started), len(env.recorded)) == (started, recorded)   # no task, no audit row, no ticket
    assert probe.check() == {"success": True} and len(env.started) == started + 1   # Check connection: always fresh
    probe.cred.reference["generation"] = 2   # the reference changed: a new key
    _validator_check(ns, env, probe.cred)
    assert len(env.started) == started + 2
    ns.credential_probe.forget_outcomes()


def test_a_signed_out_person_loses_the_kept_outcome_and_a_missing_sign_in_is_never_kept(ns, env, monkeypatch):
    ns.credential_probe.forget_outcomes()
    probe = Probe(ns, env, monkeypatch)
    _validator_check(ns, env, probe.cred)
    ns.credential_probe.forget_outcomes(TID.upper(), OID)   # auth_oidc's drop event
    started = len(env.started)
    _validator_check(ns, env, probe.cred)
    assert len(env.started) == started + 1
    env.task_result = {"success": False, "reason_code": "DENIED_ASSERTION_UNAVAILABLE"}
    ns.credential_probe.forget_outcomes()
    for _ in range(2):
        assert _validator_check(ns, env, probe.cred)["code"] == "DENIED_ASSERTION_UNAVAILABLE"
    assert len(env.started) == started + 3   # asked again each time: a fresh sign-in clears the marker at once
    ns.credential_probe.forget_outcomes()


def test_a_kept_outcome_needs_the_same_grant(ns, env, monkeypatch):
    ns.credential_probe.forget_outcomes()
    probe = Probe(ns, env, monkeypatch, personal=False)
    _validator_check(ns, env, probe.cred)
    env.grants.clear()
    answer = _validator_check(ns, env, probe.cred)
    assert answer["code"] == "DENIED_GRANT"   # the grant read precedes the kept outcome
    ns.credential_probe.forget_outcomes()


def test_a_grant_reissued_during_the_check_is_said_as_such_not_as_a_worker_problem(ns):
    shaped = ns.credential_probe.shape_result({"success": False, "reason_code": "EPOCH_MISMATCH"})
    assert shaped == {"success": False, "code": "EPOCH_MISMATCH",
                      "message": ns.credential_probe.PROBE_MESSAGES["EPOCH_MISMATCH"]}
