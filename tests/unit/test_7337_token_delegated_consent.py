"""C-1009 #7337: a scoped API token's run acting with its owner's Microsoft sign-in, through the owner's consent.

The token admission (#7336), the credential decision, the seal and the release run for real against the SQLite harness
(``fixtures/admission_env.py``); the consent table is the real model on the harness's shared-schema database. auth_oidc's
offline RPCs, the secrets plugin's offline material and Entra's token endpoint are the fakes of the #7131 tests, which
record their calls. Every value below is a made-up test value.
"""
import json
import sys
import types
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pytest

PLUGIN_ROOT = Path(__file__).parents[2]
sys.path.insert(0, str(PLUGIN_ROOT / "tests"))
sys.path.insert(0, str(PLUGIN_ROOT / "tests" / "unit"))

from fixtures import admission_env  # noqa: E402  pylint: disable=C0413
from fixtures.governed_credentials import (  # noqa: E402  pylint: disable=C0413
    CALLER, Credential, call_kwargs, payload_of, run_to_running, sealed_tool_settings,
)
from test_7131_mandate_identity import (  # noqa: E402  pylint: disable=C0413
    ASSERTION, CLIENT_SECRET, CUSTODY_KEY, ENTRA, GRAPH, GRAPH_TOKEN, OID, REFRESH, TID, AuthOidc, Entra, Offline,
)

TOKEN = 21
OWNER = 42
OTHER = 43
OWNER_REF = f"idp:{TID}:{OID}"
KEY = "test-master-key-not-a-secret"
PREDICT = "models.applications.predict.post"
SOURCES = ["owner_delegated", "token_owner_delegated"]


@pytest.fixture(scope="module")
def ns(tmp_path_factory):
    saved = dict(sys.modules)
    yield admission_env.install(PLUGIN_ROOT, tmp_path_factory.mktemp("token_identity"))
    sys.modules.clear()
    sys.modules.update(saved)


@pytest.fixture
def env(ns, monkeypatch):
    ns.env.reset()
    env = ns.env
    env.facts_version = 5
    env.permissions[OWNER] = {"credentials.use", PREDICT}
    env.user_rows[OWNER] = {"id": OWNER, "idp_issuer": ENTRA, "idp_tid": TID, "idp_oid": OID}
    env.user_rows[OTHER] = {"id": OTHER, "idp_issuer": ENTRA, "idp_tid": TID, "idp_oid": "oid-other"}
    scoped(env)
    env.epochs[TOKEN] = {"active": True, "user": 3, "token": 4, "user_id": OWNER, "reason": None}
    ns.tools.config.SECRETS_MASTER_KEY = KEY
    ns.tools.config.APP_HOST = "http://localhost"
    ns.tools.this.module = env.module()
    ns.tools.auth.epoch_ttl = 5.0
    ns.entra_obo.forget()
    env.entra, env.oidc, env.offline = Entra(), AuthOidc(ns.entra_obo), Offline()
    env.secrets_extra = {name: getattr(env.offline, name) for name in (
        "keep_offline_material", "release_offline_material", "rotate_offline_material", "retire_offline_material")}
    monkeypatch.setattr(ns.entra_obo, "_post", env.entra)
    monkeypatch.setenv("ELITEA_API_CLIENT_SECRET", CLIENT_SECRET)
    monkeypatch.setenv("ELITEA_OBO_CUSTODY_KEY", CUSTODY_KEY)
    env.sources = list(SOURCES)
    real_module = env.module

    def module(**kwargs):
        built = real_module(**kwargs)
        built.descriptor = types.SimpleNamespace(config={
            "entra_obo_client_id": "api-client", "mandate_identity_sources": list(env.sources)})
        built.context = types.SimpleNamespace(rpc_manager=env.oidc)
        return built

    monkeypatch.setattr(env, "module", module)
    ns.token_identity.ensure_table()
    with ns.sa_orm.Session(env.engine(None)) as session:
        session.execute(ns.sa.delete(ns.token_models.ApiTokenIdentityConsent))
        session.commit()
    yield env
    for name in ("SECRETS_MASTER_KEY", "APP_HOST"):
        vars(ns.tools.config).pop(name, None)


def scoped(env, projects=(1,), operations=("run",), owner=OWNER):
    env.token_scopes[TOKEN] = {"projects": list(projects), "operations": list(operations), "user_id": owner}
    env.verify_result = {"ok": True, "principal": {
        "kind": "api_token", "user_id": owner, "subject_ref": f"user:{owner}",
        "evidence": {"type": "asserted_user", "issuer": "elitea:api_token", "subject": f"user:{owner}"},
        "expires_at": None, "epochs": {"user": 3, "token": 4}}}


class Delegated(Credential):
    """A SharePoint configuration governed by an ``entra_delegated`` reference, the project's or the owner's personal
    one, granted to the owner on the binding (as a session run of theirs uses it)."""

    def __init__(self, env, binding, *, personal=False, tool_id=1, **kwargs):
        env.secret_fields.setdefault("sharepoint", ["client_secret"])
        super().__init__(env, binding, ctype="sharepoint", material=False, grants=False, tool_id=tool_id,
                         config_id=10 + tool_id, **kwargs)
        row = self.reference
        row["auth_mode"] = "entra_delegated"
        if personal:
            row["ownership_class"], row["custodian_subject_ref"], row["custodian_user_id"] = "personal", OWNER_REF, OWNER
        env.grants[(1, self.key(OWNER_REF))] = {   # a governed run's grant on the binding, to the owner
            "grant_id": self.grant_id, "state": "active", "revocation_epoch": 4, "valid_from": None,
            "valid_until": None, "operations": ["*"]}
        env.delegations[self.ref_id] = {"oauth_app_id": "platform", "tenant_id": TID, "resource": GRAPH,
                                        "scopes": [f"{GRAPH}/Sites.Selected"], "assertion_source": "login"}

    def expanded(self):
        return {"elitea_title": "sp", "configuration_uuid": self.uuid, "configuration_project_id": 1,
                "configuration_type": "sharepoint", "configuration_generation": 3,
                "site_url": "https://contoso.sharepoint.com/sites/sandbox"}


def consent_row(ns, env, token_id=TOKEN):
    with ns.sa_orm.Session(env.engine(None)) as session:
        return session.execute(ns.sa.select(ns.token_models.ApiTokenIdentityConsent).where(
            ns.token_models.ApiTokenIdentityConsent.token_id == token_id)).scalar_one_or_none()


def consented(ns, env, holder=f"token:{TOKEN}", custodian=OWNER_REF, owner=OWNER, project=1):
    """A consent as ``consent`` records it: the material at the secrets plugin, the row naming it."""
    material = env.offline.add(holder, custodian)
    with ns.sa_orm.Session(env.engine(None)) as session:
        session.add(ns.token_models.ApiTokenIdentityConsent(
            token_id=TOKEN, owner_user_id=owner, material_project_id=project, material_ref=uuid.UUID(material),
            consented_at=datetime.now(timezone.utc)))
        session.commit()
    return material


def token_request(env, **over):
    values = dict(caller_type="token", caller_id=TOKEN, caller_reference=None, entry_point="predict_api")
    values.update(over)
    return env.request(**values)


def admit(ns, env, *creds, module=None):
    call = call_kwargs(payload_of(*creds))
    ref = ns.adm.admit_dispatch(module or env.module(), "indexer_agent", call, token_request(env))
    return call, ref


def refused(ns, env, *creds):
    with pytest.raises(ns.exceptions.AdmissionDenied) as denied:
        admit(ns, env, *creds)
    return denied.value.reason_code


def admitted(ns, env, *creds, module=None):
    call, ref = admit(ns, env, *creds, module=module)
    run_to_running(env, ns, ref)
    private, public = ns.keypair()
    slots = [sealed_tool_settings(call, index)["credential_slot"] for index in range(len(creds))]
    for recorded in (env.recorded, env.denied):
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

    return types.SimpleNamespace(call=call, ref=ref, release=release, opened=opened)


def context_row(ns, env, ref):
    [row] = env.query(1, ns.models.ExecutionSecurityContext, ns.models.ExecutionSecurityContext.id == uuid.UUID(ref))
    return row


def everything_said(env, *more):
    return "\n".join([json.dumps([e for _, e in env.recorded], default=str),
                      json.dumps(env.denied, default=str), "\n".join(env.log.lines), *map(str, more)])


def statements_of(env, ns, action):
    seen = []
    engines = [env.engine(1), env.engine(None)]
    listener = lambda conn, cursor, statement, params, context, many: seen.append(statement)   # noqa: E731
    for engine in engines:
        ns.sa.event.listen(engine, "before_cursor_execute", listener)
    try:
        action()
    finally:
        for engine in engines:
            ns.sa.event.remove(engine, "before_cursor_execute", listener)
    return seen


def signed_in_as(ns, monkeypatch, person=None, code=None):
    def verified(module, actor_user_id):
        if code:
            raise ns.exceptions.AdmissionDenied(code, "refused", {})
        return dict(person or {"issuer": ENTRA, "tid": TID, "oid": OID})
    monkeypatch.setattr(ns.credential_probe, "_probe_person", verified)


# --- the release: a consented token in scope ---------------------------------------------------------------------------

@pytest.mark.parametrize("personal", [False, True])
def test_a_consented_token_run_is_released_a_token_minted_from_the_owners_offline_consent(ns, env, personal):
    ns.need_transport()
    binding = env.seed_agent()
    material = consented(ns, env)
    run = admitted(ns, env, Delegated(env, binding, personal=personal))
    subject = context_row(ns, env, run.ref).subject
    assert (subject["kind"], subject["ref"], subject["issuer"], subject["oid"]) == ("api_token", OWNER_REF, ENTRA, OID)
    reply = run.release()
    assert reply["ok"] is True, reply
    assert run.opened(reply["releases"][0])["fields"] == {"access_token": GRAPH_TOKEN}
    assert env.offline.calls == [("release", 1, material, f"token:{TOKEN}", OWNER_REF)]
    assert env.oidc.redeemed == [(TID, OID, f"token:{TOKEN}", "login", REFRESH)]
    [(_, event)] = env.recorded
    assert (event["reason_code"], event["caller_ref"], event["approver_ref"], event["target_ref"], event["user_ref"]) == (
        "CREDENTIAL_RELEASED_DELEGATED", f"token:{TOKEN}", f"consent:{material}", GRAPH, OWNER_REF)
    assert "mandate_ref" not in event
    for secret in (GRAPH_TOKEN, ASSERTION, REFRESH, CLIENT_SECRET):
        assert secret not in everything_said(env, reply, subject)
    assert REFRESH not in "".join(env.oidc.bus)


def test_the_consent_is_read_at_every_release_and_a_withdrawal_stops_a_cached_token(ns, env):
    ns.need_transport()
    binding = env.seed_agent()
    consented(ns, env)
    run = admitted(ns, env, Delegated(env, binding))
    assert run.release()["ok"] is True and run.release()["ok"] is True
    assert len(env.oidc.redeemed) == 1 and [c[0] for c in env.offline.calls] == ["release", "release"]
    assert ns.token_identity.withdraw(TOKEN, OWNER) == {"token_id": TOKEN}
    assert run.release()["reason_code"] == "DENIED_TOKEN_CONSENT_REQUIRED"
    assert len(env.oidc.redeemed) == 1
    assert refused(ns, env, Delegated(env, binding, tool_id=2)) == "DENIED_TOKEN_CONSENT_REQUIRED"


# --- the decision ------------------------------------------------------------------------------------------------------

def test_without_a_consent_the_run_is_refused_readably_before_it_starts(ns, env):
    binding = env.seed_agent()
    assert refused(ns, env, Delegated(env, binding)) == "DENIED_TOKEN_CONSENT_REQUIRED"
    assert "Personal tokens" in ns.adm.MESSAGES["DENIED_TOKEN_CONSENT_REQUIRED"]
    assert "Personal tokens" in ns.adm.MESSAGES["DENIED_TOKEN_CONSENT_EXPIRED"]
    [deny] = env.denied
    assert (deny["reason_code"], deny["caller_ref"]) == ("DENIED_TOKEN_CONSENT_REQUIRED", f"token:{TOKEN}")
    assert env.oidc.redeemed == [] and env.offline.calls == []


def test_a_stored_secret_needs_no_consent_and_reads_nothing_about_one(ns, env):
    binding = env.seed_agent()
    cred = Credential(env, binding)
    env.grants[(1, cred.key(OWNER_REF))] = env.grants.pop((1, cred.key()))
    spy = []
    real = ns.token_identity._read  # pylint: disable=W0212
    ns.token_identity._read = lambda token_id: spy.append(token_id) or real(token_id)  # pylint: disable=W0212
    try:
        admit(ns, env, cred)
    finally:
        ns.token_identity._read = real  # pylint: disable=W0212
    assert spy == []   # the reader is lazy: asked only by a delegated credential


def test_another_users_consent_on_the_token_does_not_count(ns, env):
    binding = env.seed_agent()
    consented(ns, env, owner=OTHER)
    assert refused(ns, env, Delegated(env, binding)) == "DENIED_TOKEN_CONSENT_REQUIRED"


def test_a_consent_row_that_is_not_the_owners_at_release_is_refused(ns, env):
    binding = env.seed_agent()
    consented(ns, env)
    run = admitted(ns, env, Delegated(env, binding))
    with ns.sa_orm.Session(env.engine(None)) as session:
        session.execute(ns.sa.update(ns.token_models.ApiTokenIdentityConsent).values(owner_user_id=OTHER))
        session.commit()
    assert run.release()["reason_code"] == "DENIED_TOKEN_CONSENT_REQUIRED"
    assert env.offline.calls == [] and env.oidc.redeemed == []


# --- the switch: without the token source everything is as after #7336 -------------------------------------------------

@pytest.mark.parametrize("sources", [[], ["owner_delegated"], ["owner_delegated", "application"], "token_owner_delegated"])
def test_without_the_token_source_a_delegated_credential_is_refused_at_release_exactly_as_before(ns, env, sources):
    ns.need_transport()
    binding = env.seed_agent()
    consented(ns, env)   # even with a consent on record, nothing reads it
    env.sources = sources
    run = admitted(ns, env, Delegated(env, binding))
    assert "issuer" not in context_row(ns, env, run.ref).subject
    assert run.release()["reason_code"] == "DENIED_DELEGATION_UNSUPPORTED_CALLER"
    assert env.offline.calls == [] and env.oidc.redeemed == [] and env.entra.posts == []


def test_without_the_token_source_the_admission_statements_are_those_of_7336(ns, env):
    """Byte identity: the statements a token run with a delegated credential issues are the same with the switch empty
    and with the mandate sources only, and none of them names the consent table."""
    binding = env.seed_agent()
    cred = Delegated(env, binding)
    runs = []
    for sources in ([], ["owner_delegated", "application"]):
        env.sources = sources
        runs.append(statements_of(env, ns, lambda: admit(ns, env, cred)))
    off, on = runs
    assert off == on
    assert [statement for statement in off if "api_token_identity_consent" in statement] == []
    env.sources = list(SOURCES)
    with_source = statements_of(env, ns, lambda: refused(ns, env, cred))
    assert [statement for statement in with_source if "api_token_identity_consent" in statement]


def test_governance_off_reads_nothing_of_a_consent(ns, env, monkeypatch):
    calls = []
    monkeypatch.setattr(ns.token_identity, "_read", lambda token_id: calls.append(token_id))
    monkeypatch.setattr(ns.token_identity, "_linked_subject", lambda user_id: calls.append(user_id))
    status = ns.token_identity.status(env.module(governed=False), OWNER)
    assert status == {"allowed": False, "linked": False, "consents": {}, "mandates": False}
    env.sources = []
    assert ns.token_identity.status(env.module(), OWNER)["allowed"] is False
    assert ns.token_identity.identity_reader(env.module(), TOKEN, OWNER) is None
    assert ns.token_identity.on_auth_invalidate(env.module(), {"kind": "token", "id": TOKEN, "epoch": None}) is False
    assert calls == []
    off = ns.token_scope.check(env.module(governed=False), TOKEN, ("api.v2.elitea_core.token_identity", "POST", {}, "/",
                                                                     {}, {}))
    assert off is None


def test_a_plain_string_switch_is_no_list(ns):
    """``"token_owner_delegated"`` as a string would match by substring: it names nothing."""
    module = types.SimpleNamespace(descriptor=types.SimpleNamespace(config={
        "mandate_identity_sources": "token_owner_delegated"}))
    assert ns.token_identity.allowed(module) is False


# --- token lifecycle: deleted, expired, relinked -----------------------------------------------------------------------

@pytest.mark.parametrize("reason", ["token_missing", "token_expired"])
def test_a_deleted_or_expired_token_releases_nothing_and_its_consent_is_retired(ns, env, reason):
    binding = env.seed_agent()
    material = consented(ns, env)
    run = admitted(ns, env, Delegated(env, binding))
    env.epochs[TOKEN] = {"active": False, "user": 3, "token": None, "user_id": None, "reason": reason}
    assert run.release()["reason_code"] == "DENIED_TOKEN_CONSENT_REQUIRED"
    assert consent_row(ns, env) is None
    assert ("retire", material, "HOLDER_REVOKED") in env.offline.calls
    assert "TOKEN_IDENTITY_RETIRED" in [e["reason_code"] for _, e in env.recorded]
    assert env.oidc.redeemed == []


def test_deleting_a_token_retires_its_consent_from_the_event(ns, env):
    material = consented(ns, env)
    assert ns.token_identity.on_auth_invalidate(env.module(), {"kind": "token", "id": TOKEN, "epoch": 5}) is False
    assert consent_row(ns, env) is not None   # a bump, not a delete
    assert ns.token_identity.on_auth_invalidate(env.module(), {"kind": "user", "id": OWNER, "epoch": None}) is False
    assert ns.token_identity.on_auth_invalidate(env.module(), {"kind": "token", "id": TOKEN, "epoch": None}) is True
    assert consent_row(ns, env) is None
    assert ("retire", material, "HOLDER_REVOKED") in env.offline.calls
    [(_, event)] = env.recorded
    assert (event["event_type"], event["reason_code"], event["target_ref"]) == (
        "withdrawal", "TOKEN_IDENTITY_RETIRED", f"token:{TOKEN}")


def test_a_material_retired_before_the_row_is_swept_is_refused(ns, env):
    binding = env.seed_agent()
    material = consented(ns, env)
    run = admitted(ns, env, Delegated(env, binding))
    env.offline.rows[material]["state"] = "retired"
    assert run.release()["reason_code"] == "DENIED_TOKEN_CONSENT_REQUIRED"
    assert env.oidc.redeemed == []


def test_a_relinked_owner_needs_a_new_consent(ns, env):
    """The consent's material is bound to the subject the owner had when consenting: a run admitted on the new link
    is refused at release (the secrets plugin's custodian check)."""
    binding = env.seed_agent()
    consented(ns, env, custodian=f"idp:{TID}:oid-before-the-relink")
    run = admitted(ns, env, Delegated(env, binding, personal=False))
    assert run.release()["reason_code"] == "DENIED_TOKEN_CONSENT_REQUIRED"
    assert env.oidc.redeemed == []


def test_a_token_now_someone_elses_releases_nothing(ns, env):
    binding = env.seed_agent()
    consented(ns, env)
    run = admitted(ns, env, Delegated(env, binding))
    env.epochs[TOKEN] = {"active": True, "user": 3, "token": 4, "user_id": OTHER, "reason": None}
    assert run.release()["reason_code"] == "DENIED_TOKEN_CONSENT_REQUIRED"
    assert consent_row(ns, env) is not None and env.oidc.redeemed == []


# --- holders: a mandate's consent is never a token's, and the other way round -----------------------------------------

def test_a_mandates_material_never_satisfies_a_token_holder(ns, env):
    binding = env.seed_agent()
    consented(ns, env, holder=f"mandate:{uuid.uuid4()}")
    run = admitted(ns, env, Delegated(env, binding))
    assert run.release()["reason_code"] == "DENIED_TOKEN_CONSENT_REQUIRED"
    assert env.oidc.redeemed == []


def test_a_tokens_material_never_satisfies_a_mandate_holder(ns, env):
    material = env.offline.add(f"token:{TOKEN}")
    answer = env.offline.release_offline_material(1, material, holder_ref=f"mandate:{uuid.uuid4()}",
                                                  custodian_subject_ref=OWNER_REF, release_id="r", actor_ref="a")
    assert answer == {"ok": False, "reason_code": "OFFLINE_MATERIAL_MISMATCH"}
    assert ns.token_identity.holder_of(TOKEN) == "token:21" and ns.mandate_identity.holder_of(TOKEN) == "mandate:21"
    assert ns.token_identity.token_id_of({"kind": "api_token", "ref": "mandate:21"}) is None
    assert ns.token_identity.token_id_of({"kind": "trigger", "ref": "token:21"}) is None


# --- caches: the owner's interactive tokens and the token's offline ones never mix -------------------------------------

def test_the_owners_interactive_token_is_never_served_to_the_token_run_and_back(ns, env):
    ns.need_transport()
    binding = env.seed_agent()
    material = consented(ns, env)
    cred = Delegated(env, binding)
    person = {"issuer": ENTRA, "tid": TID, "oid": OID}
    delegation = {**env.delegations[cred.ref_id], "ownership_class": "project_shared", "custodian_subject_ref": None}
    client = ns.entra_obo.app_client(env.module(), delegation, 1)
    interactive = (TID.lower(), OID, client[0], GRAPH, (f"{GRAPH}/Sites.Selected",))
    ns.entra_obo._keep(interactive, "interactive-token-made-up", 9_999_999_999, None, 0)  # pylint: disable=W0212
    run = admitted(ns, env, cred)
    reply = run.release()
    assert reply["ok"] is True and run.opened(reply["releases"][0])["fields"] == {"access_token": GRAPH_TOKEN}
    assert len(env.oidc.redeemed) == 1   # redeemed from the consent, not served the owner's own token
    assert ns.entra_obo.forget_material(material) == 1   # withdrawing drops the token's only
    assert ns.entra_obo._cached(interactive, 0) is not None  # pylint: disable=W0212
    del person


# --- Microsoft refuses the consent -------------------------------------------------------------------------------------

def test_a_consent_microsoft_refuses_is_withdrawn_and_the_run_refused_readably(ns, env):
    binding = env.seed_agent()
    material = consented(ns, env)
    run = admitted(ns, env, Delegated(env, binding))
    env.oidc.answer = {"ok": False, "code": "CONSENT_REFUSED"}
    assert run.release()["reason_code"] == "DENIED_TOKEN_CONSENT_EXPIRED"
    assert consent_row(ns, env) is None
    assert ("retire", material, "CONSENT_REFUSED") in env.offline.calls
    assert "TOKEN_IDENTITY_CONSENT_REFUSED" in [e["reason_code"] for _, e in env.recorded]


@pytest.mark.parametrize("answer", [{"ok": False, "code": "ASSERTION_UNAVAILABLE"}, None])
def test_a_redemption_that_may_work_later_keeps_the_consent(ns, env, answer):
    binding = env.seed_agent()
    consented(ns, env)
    run = admitted(ns, env, Delegated(env, binding))
    env.oidc.answer = answer
    assert run.release()["reason_code"] == "DEPENDENCY_UNAVAILABLE"
    assert consent_row(ns, env) is not None


# --- consent and withdrawal --------------------------------------------------------------------------------------------

def test_the_owner_consents_in_their_own_session_and_only_a_reference_is_kept(ns, env, monkeypatch):
    signed_in_as(ns, monkeypatch)
    scoped(env, projects=(3, 1))
    answer = ns.token_identity.consent(env.module(), TOKEN, OWNER)
    assert answer["token_id"] == TOKEN and answer["consented_at"]
    assert env.oidc.granted == [(TID, OID, f"token:{TOKEN}")]
    [keep] = env.offline.calls
    assert keep == ("keep", 1, f"token:{TOKEN}", OWNER, OWNER_REF, "login", OWNER)   # kept in the lowest project
    row = consent_row(ns, env)
    assert (row.owner_user_id, row.material_project_id, str(row.material_ref) in env.offline.rows) == (OWNER, 1, True)
    [(_, event)] = env.recorded
    assert (event["event_type"], event["reason_code"], event["target_ref"], event["user_ref"], event["caller_ref"]) == (
        "approval", "TOKEN_IDENTITY_CONSENTED", f"token:{TOKEN}", OWNER_REF, f"user:{OWNER}")
    assert REFRESH not in everything_said(env, answer) and REFRESH not in "".join(env.oidc.bus)
    status = ns.token_identity.status(env.module(), OWNER)
    assert status["allowed"] is True and status["linked"] is True and list(status["consents"]) == [str(TOKEN)]
    assert ns.token_identity.status(env.module(), OTHER)["consents"] == {}


def test_a_consent_given_again_replaces_and_retires_the_earlier_one(ns, env, monkeypatch):
    earlier = consented(ns, env)
    signed_in_as(ns, monkeypatch)
    ns.token_identity.consent(env.module(), TOKEN, OWNER)
    assert str(consent_row(ns, env).material_ref) != earlier
    assert ("retire", earlier, "CONSENT_REPLACED") in env.offline.calls


def test_two_consents_at_once_one_wins_and_the_others_material_is_retired(ns, env, monkeypatch):
    signed_in_as(ns, monkeypatch)
    real_grant = ns.token_identity._grant  # pylint: disable=W0212
    winner = []

    def racing(module, person, holder):   # the other consent completes while this one asks for its grant
        granted = real_grant(module, person, holder)
        if not winner:
            monkeypatch.setattr(ns.token_identity, "_grant", real_grant)
            winner.append(ns.token_identity.consent(env.module(), TOKEN, OWNER))
        return granted

    monkeypatch.setattr(ns.token_identity, "_grant", racing)
    with pytest.raises(ns.token_identity.IdentityRefused) as lost:
        ns.token_identity.consent(env.module(), TOKEN, OWNER)
    assert lost.value.code == "CONCURRENT_CHANGE"
    kept = [call for call in env.offline.calls if call[0] == "keep"]
    retired = [call for call in env.offline.calls if call[0] == "retire"]
    row = consent_row(ns, env)
    assert len(kept) == 2 and [reason for _, _, reason in retired] == ["CONSENT_REPLACED"]
    assert retired[0][1] != str(row.material_ref)   # the loser's material went, the winner's stays
    assert env.offline.rows[str(row.material_ref)]["state"] == "active"


@pytest.mark.parametrize("case, code", [
    ("switch_off", "IDENTITY_SOURCE_NOT_ALLOWED"),
    ("token_caller", "DENIED_DELEGATION_UNSUPPORTED_CALLER"),   # the token itself, or no session
    ("unverified", "DENIED_ASSERTION_UNAVAILABLE"),
    ("other_subject", "DENIED_SUBJECT_MISMATCH"),
    ("legacy_token", "TOKEN_NOT_SCOPED"),
    ("other_users_token", "TOKEN_NOT_FOUND"),
    ("other_users_legacy_token", "TOKEN_NOT_FOUND"),   # not "not scoped": nothing is said of someone else's token
    ("unknown_token", "TOKEN_NOT_FOUND"),
    ("expired_token", "INVALID_STATE"),
    ("owner_unlinked", "OWNER_NOT_LINKED"),
    ("no_custody", "DENIED_ASSERTION_UNAVAILABLE"),
])
def test_a_consent_that_is_not_the_owners_own_live_sign_in_is_refused_with_nothing_kept(ns, env, monkeypatch, case, code):
    signed_in_as(ns, monkeypatch, person={"issuer": ENTRA, "tid": TID, "oid": "someone-else"}
                 if case == "other_subject" else None,
                 code={"unverified": "DENIED_ASSERTION_UNAVAILABLE",
                       "token_caller": "DENIED_DELEGATION_UNSUPPORTED_CALLER"}.get(case))
    actor = OWNER
    if case == "switch_off":
        env.sources = ["owner_delegated"]
    if case == "legacy_token":
        env.token_scopes.clear()
    if case in ("other_users_token", "other_users_legacy_token"):
        actor = OTHER
    if case == "other_users_legacy_token":
        env.token_scopes.clear()
    if case == "unknown_token":
        env.token_scope_raises = RuntimeError("no such token")
    if case == "expired_token":
        env.epochs[TOKEN] = {"active": False, "user": 3, "token": 4, "user_id": OWNER, "reason": "token_expired"}
    if case == "owner_unlinked":
        env.user_rows[OWNER] = {"id": OWNER, "idp_issuer": None, "idp_tid": None, "idp_oid": None}
    if case == "no_custody":
        env.oidc.grant_answer = {"ok": False, "code": "ASSERTION_UNAVAILABLE"}
    with pytest.raises(ns.token_identity.IdentityRefused) as refusal:
        ns.token_identity.consent(env.module(), TOKEN, actor)
    assert refusal.value.code == code and code in ns.token_identity.MESSAGES
    assert env.offline.calls == [] and consent_row(ns, env) is None


def test_withdrawing_is_the_owners_and_works_with_the_source_removed(ns, env):
    material = consented(ns, env)
    with pytest.raises(ns.token_identity.IdentityRefused) as other:
        ns.token_identity.withdraw(TOKEN, OTHER)
    assert other.value.code == "CONSENT_NOT_GIVEN"   # another user's token reads as none
    env.sources = []
    assert ns.token_identity.withdraw(TOKEN, OWNER) == {"token_id": TOKEN}
    assert consent_row(ns, env) is None
    assert ("retire", material, "CONSENT_WITHDRAWN") in env.offline.calls
    [(_, event)] = env.recorded
    assert (event["event_type"], event["reason_code"], event["target_ref"], event["caller_ref"]) == (
        "withdrawal", "TOKEN_IDENTITY_WITHDRAWN", f"token:{TOKEN}", f"user:{OWNER}")
    with pytest.raises(ns.token_identity.IdentityRefused) as again:
        ns.token_identity.withdraw(TOKEN, OWNER)
    assert again.value.code == "CONSENT_NOT_GIVEN"


@pytest.mark.parametrize("scope", [None, {"projects": [1], "operations": ["run", "build", "admin"]}])
@pytest.mark.parametrize("method", ["GET", "POST"])
def test_an_api_token_never_reaches_the_consent_route(ns, scope, method):
    code = ns.token_scope.decide(scope, "api.v2.elitea_core.token_identity", method)
    if scope is None and method == "GET":
        assert code is None   # a legacy token's read is the handler's to refuse (signed-in sessions only)
    else:
        assert code == "TOKEN_SCOPE_OPERATION"
    for endpoint in ("api.v2.elitea_core.autonomous_mandate_action", "api.v2.elitea_core.autonomous_mandates"):
        if scope is not None:
            assert ns.token_scope.decide(scope, endpoint, method, {"mode": "prompt_lib"}, project=1) == (
                None if "admin" in scope["operations"] else "TOKEN_SCOPE_OPERATION")
