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
PERSONAL = 7   # the owner's personal project: where their consent material is kept
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
    env.delegated, env.offered_calls, env.offer_projects = [], [], {1}

    def delegated_references_of(project_id, subject_ref, user_id):   # the secrets plugin (its own tests: secrets)
        env.offered_calls.append((project_id, subject_ref, user_id))
        return [cred.offered() for cred in env.delegated if project_id in env.offer_projects and subject_ref == OWNER_REF
                and user_id == OWNER and cred.reference["state"] == "active"]

    env.secrets_extra["delegated_references_of"] = delegated_references_of
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
    env.personal_calls = []
    env.oidc.projects_get_personal_project_id = lambda user_id: env.personal_calls.append(user_id) or PERSONAL
    ns.token_identity._SWEEP_CURSOR[0] = 0  # pylint: disable=W0212
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
        env.configurations[(1, self.uuid)]["title"] = f"sp-{tool_id}"
        env.delegated.append(self)

    def scope_item(self):
        """As a consent records it (``credential_refs``)."""
        return {"project_id": 1, "credential_ref": self.ref_id, "credential_generation": 1,
                "configuration_generation": 3, "title": f"sp-{self.tool_id}"}

    def offered(self):
        """As the secrets plugin's ``delegated_references_of`` lists it."""
        return {"ref_id": self.ref_id, "configuration_id": self.config_id, "configuration_uuid": self.uuid,
                "generation": 1, "configuration_generation": 3, "ownership_class": self.reference["ownership_class"]}

    def expanded(self):
        return {"elitea_title": "sp", "configuration_uuid": self.uuid, "configuration_project_id": 1,
                "configuration_type": "sharepoint", "configuration_generation": 3,
                "site_url": "https://contoso.sharepoint.com/sites/sandbox"}


def consent_row(ns, env, token_id=TOKEN):
    with ns.sa_orm.Session(env.engine(None)) as session:
        return session.execute(ns.sa.select(ns.token_models.ApiTokenIdentityConsent).where(
            ns.token_models.ApiTokenIdentityConsent.token_id == token_id)).scalar_one_or_none()


def consented(ns, env, holder=f"token:{TOKEN}", custodian=OWNER_REF, owner=OWNER, project=PERSONAL, token_id=TOKEN,
              covers=None):
    """A consent as ``consent`` records it: the material at the secrets plugin, the row naming it and the delegated
    credentials it covers (``covers``; by default every one made so far)."""
    material = env.offline.add(holder, custodian)
    with ns.sa_orm.Session(env.engine(None)) as session:
        session.add(ns.token_models.ApiTokenIdentityConsent(
            token_id=token_id, owner_user_id=owner, material_project_id=project, material_ref=uuid.UUID(material),
            consented_at=datetime.now(timezone.utc),
            credential_refs=[cred.scope_item() for cred in (env.delegated if covers is None else covers)]))
        session.commit()
    return material


def chosen(env, *creds):
    """What the owner picks in the consent: the given credentials, else every one made so far (one is made when none
    is: a consent always names at least one)."""
    if not creds and not env.delegated:
        Delegated(env, env.seed_agent())
    return [{"project_id": 1, "credential_ref": cred.ref_id} for cred in (creds or env.delegated)]


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
    cred = Delegated(env, binding, personal=personal)
    material = consented(ns, env)
    run = admitted(ns, env, cred)
    subject = context_row(ns, env, run.ref).subject
    assert (subject["kind"], subject["ref"], subject["issuer"], subject["oid"]) == ("api_token", OWNER_REF, ENTRA, OID)
    reply = run.release()
    assert reply["ok"] is True, reply
    assert run.opened(reply["releases"][0])["fields"] == {"access_token": GRAPH_TOKEN}
    assert env.offline.calls == [("release", PERSONAL, material, f"token:{TOKEN}", OWNER_REF)]
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
    cred = Delegated(env, binding)
    consented(ns, env)
    run = admitted(ns, env, cred)
    assert run.release()["ok"] is True and run.release()["ok"] is True
    assert len(env.oidc.redeemed) == 1 and [c[0] for c in env.offline.calls] == ["release", "release"]
    assert ns.token_identity.withdraw(env.module(), TOKEN, OWNER) == {"token_id": TOKEN}
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
    cred = Delegated(env, binding)
    consented(ns, env)
    run = admitted(ns, env, cred)
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
    cred = Delegated(env, binding)
    material = consented(ns, env)
    run = admitted(ns, env, cred)
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
    cred = Delegated(env, binding)
    material = consented(ns, env)
    run = admitted(ns, env, cred)
    env.offline.rows[material]["state"] = "retired"
    assert run.release()["reason_code"] == "DENIED_TOKEN_CONSENT_REQUIRED"
    assert env.oidc.redeemed == []


def test_a_relinked_owner_needs_a_new_consent(ns, env):
    """The consent's material is bound to the subject the owner had when consenting: a run admitted on the new link
    is refused at release (the secrets plugin's custodian check)."""
    binding = env.seed_agent()
    cred = Delegated(env, binding, personal=False)
    consented(ns, env, custodian=f"idp:{TID}:oid-before-the-relink")
    run = admitted(ns, env, cred)
    assert run.release()["reason_code"] == "DENIED_TOKEN_CONSENT_REQUIRED"
    assert env.oidc.redeemed == []


def test_a_token_now_someone_elses_releases_nothing(ns, env):
    binding = env.seed_agent()
    cred = Delegated(env, binding)
    consented(ns, env)
    run = admitted(ns, env, cred)
    env.epochs[TOKEN] = {"active": True, "user": 3, "token": 4, "user_id": OTHER, "reason": None}
    assert run.release()["reason_code"] == "DENIED_TOKEN_CONSENT_REQUIRED"
    assert consent_row(ns, env) is not None and env.oidc.redeemed == []


# --- holders: a mandate's consent is never a token's, and the other way round -----------------------------------------

def test_a_mandates_material_never_satisfies_a_token_holder(ns, env):
    binding = env.seed_agent()
    cred = Delegated(env, binding)
    consented(ns, env, holder=f"mandate:{uuid.uuid4()}")
    run = admitted(ns, env, cred)
    assert run.release()["reason_code"] == "DENIED_TOKEN_CONSENT_REQUIRED"
    assert env.oidc.redeemed == []


def test_holder_strings_never_collide(ns):
    """The holder strings elitea_core sends. That the secrets plugin never releases one holder's material to another is
    proven against its real code there (secrets ``test_a_mandate_holder_never_matches_a_token_holder_and_back``)."""
    assert ns.token_identity.holder_of(TOKEN) == "token:21" and ns.mandate_identity.holder_of(TOKEN) == "mandate:21"
    assert ns.token_identity.token_id_of({"kind": "api_token", "ref": "mandate:21"}) is None
    assert ns.token_identity.token_id_of({"kind": "trigger", "ref": "token:21"}) is None
    assert ns.token_identity.token_id_of({"kind": "api_token", "ref": "token:21"}) == 21


# --- caches: the owner's interactive tokens and the token's offline ones never mix -------------------------------------

def test_the_owners_interactive_token_is_never_served_to_the_token_run_and_back(ns, env):
    ns.need_transport()
    binding = env.seed_agent()
    cred = Delegated(env, binding)
    material = consented(ns, env)
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
    cred = Delegated(env, binding)
    material = consented(ns, env)
    run = admitted(ns, env, cred)
    env.oidc.answer = {"ok": False, "code": "CONSENT_REFUSED"}
    assert run.release()["reason_code"] == "DENIED_TOKEN_CONSENT_EXPIRED"
    assert consent_row(ns, env) is None
    assert ("retire", material, "CONSENT_REFUSED") in env.offline.calls
    assert "TOKEN_IDENTITY_CONSENT_REFUSED" in [e["reason_code"] for _, e in env.recorded]


@pytest.mark.parametrize("answer", [{"ok": False, "code": "ASSERTION_UNAVAILABLE"}, None])
def test_a_redemption_that_may_work_later_keeps_the_consent(ns, env, answer):
    binding = env.seed_agent()
    cred = Delegated(env, binding)
    consented(ns, env)
    run = admitted(ns, env, cred)
    env.oidc.answer = answer
    assert run.release()["reason_code"] == "DEPENDENCY_UNAVAILABLE"
    assert consent_row(ns, env) is not None


# --- consent and withdrawal --------------------------------------------------------------------------------------------

def test_the_owner_consents_in_their_own_session_and_only_a_reference_is_kept(ns, env, monkeypatch):
    signed_in_as(ns, monkeypatch)
    scoped(env, projects=(3, 1))
    answer = ns.token_identity.consent(env.module(), TOKEN, OWNER, chosen(env))
    assert answer["token_id"] == TOKEN and answer["consented_at"]
    assert env.oidc.granted == [(TID, OID, f"token:{TOKEN}")]
    [keep] = env.offline.calls
    # kept in the owner's personal project, whatever the token's scope (no scope project can strand it)
    assert keep == ("keep", PERSONAL, f"token:{TOKEN}", OWNER, OWNER_REF, "login", OWNER)
    assert env.personal_calls == [OWNER]
    row = consent_row(ns, env)
    assert (row.owner_user_id, row.material_project_id, str(row.material_ref) in env.offline.rows) == (
        OWNER, PERSONAL, True)
    [(_, event), (_, covered)] = env.recorded
    assert (event["event_type"], event["reason_code"], event["target_ref"], event["user_ref"], event["caller_ref"]) == (
        "approval", "TOKEN_IDENTITY_CONSENTED", f"token:{TOKEN}", OWNER_REF, f"user:{OWNER}")
    [cred] = env.delegated   # the audit names each credential the consent covers, in its project
    assert (covered["reason_code"], covered["credential_ref"], covered["project_id"], covered["target_ref"]) == (
        "TOKEN_IDENTITY_CREDENTIAL_COVERED", cred.ref_id, 1, f"token:{TOKEN}")
    assert row.credential_refs == [cred.scope_item()]
    assert answer["credentials"] == [{"project_id": 1, "credential_ref": cred.ref_id, "title": "sp-1"}]
    assert REFRESH not in everything_said(env, answer) and REFRESH not in "".join(env.oidc.bus)
    status = ns.token_identity.status(env.module(), OWNER)
    assert status["allowed"] is True and status["linked"] is True and list(status["consents"]) == [str(TOKEN)]
    assert status["consents"][str(TOKEN)]["credentials"] == answer["credentials"]
    assert ns.token_identity.status(env.module(), OTHER)["consents"] == {}


def test_a_consent_given_again_replaces_and_retires_the_earlier_one(ns, env, monkeypatch):
    earlier = consented(ns, env)
    signed_in_as(ns, monkeypatch)
    ns.token_identity.consent(env.module(), TOKEN, OWNER, chosen(env))
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
            winner.append(ns.token_identity.consent(env.module(), TOKEN, OWNER, chosen(env)))
        return granted

    monkeypatch.setattr(ns.token_identity, "_grant", racing)
    with pytest.raises(ns.token_identity.IdentityRefused) as lost:
        ns.token_identity.consent(env.module(), TOKEN, OWNER, chosen(env))
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
        ns.token_identity.consent(env.module(), TOKEN, actor, chosen(env))
    assert refusal.value.code == code and code in ns.token_identity.MESSAGES
    assert env.offline.calls == [] and consent_row(ns, env) is None


def test_withdrawing_is_the_owners_and_works_with_the_source_removed(ns, env):
    material = consented(ns, env)
    with pytest.raises(ns.token_identity.IdentityRefused) as other:
        ns.token_identity.withdraw(env.module(), TOKEN, OTHER)
    assert other.value.code == "CONSENT_NOT_GIVEN"   # another user's token reads as none
    env.sources = []
    assert ns.token_identity.withdraw(env.module(), TOKEN, OWNER) == {"token_id": TOKEN}
    assert consent_row(ns, env) is None
    assert ("retire", material, "CONSENT_WITHDRAWN") in env.offline.calls
    [(_, event)] = env.recorded
    assert (event["event_type"], event["reason_code"], event["target_ref"], event["caller_ref"]) == (
        "withdrawal", "TOKEN_IDENTITY_WITHDRAWN", f"token:{TOKEN}", f"user:{OWNER}")
    with pytest.raises(ns.token_identity.IdentityRefused) as again:
        ns.token_identity.withdraw(env.module(), TOKEN, OWNER)
    assert again.value.code == "CONSENT_NOT_GIVEN"


@pytest.mark.parametrize("scope", [None, {"projects": [1], "operations": ["run", "build", "admin"]}])
@pytest.mark.parametrize("method", ["GET", "POST"])
def test_the_hook_closes_the_token_consent_route_to_api_tokens(ns, scope, method):
    """The hook's half. That every owner-mode consent route refuses every API token, whatever its scope, in the
    handler as well, is pinned in test_7131_mandate_api (``test_no_api_token_reaches_an_owner_consent_route``)."""
    code = ns.token_scope.decide(scope, "api.v2.elitea_core.token_identity", method)
    if scope is None and method == "GET":
        assert code is None   # a legacy token's read is the handler's to refuse (signed-in sessions only)
    else:
        assert code == "TOKEN_SCOPE_OPERATION"


# --- fix round (#7337 review) ------------------------------------------------------------------------------------------

def test_a_token_run_admitted_without_the_source_is_stale_when_released_with_it(ns, env):
    binding = env.seed_agent()
    consented(ns, env)
    env.sources = ["owner_delegated"]
    run = admitted(ns, env, Delegated(env, binding))   # admitted as after #7336: no issuer/oid in the context
    env.sources = list(SOURCES)
    assert run.release()["reason_code"] == "STALE_CONTEXT"
    assert env.offline.calls == [] and env.oidc.redeemed == []


def test_a_consent_racing_a_replacement_of_an_earlier_one_loses_and_its_material_is_retired(ns, env, monkeypatch):
    earlier = consented(ns, env)
    signed_in_as(ns, monkeypatch)
    real_grant = ns.token_identity._grant  # pylint: disable=W0212
    winner = []

    def racing(module, person, holder):   # another consent replaces the earlier one while this one asks
        granted = real_grant(module, person, holder)
        if not winner:
            monkeypatch.setattr(ns.token_identity, "_grant", real_grant)
            winner.append(ns.token_identity.consent(env.module(), TOKEN, OWNER, chosen(env)))
        return granted

    monkeypatch.setattr(ns.token_identity, "_grant", racing)
    with pytest.raises(ns.token_identity.IdentityRefused) as lost:
        ns.token_identity.consent(env.module(), TOKEN, OWNER, chosen(env))
    assert lost.value.code == "CONCURRENT_CHANGE"
    row = consent_row(ns, env)
    kept = [call for call in env.offline.calls if call[0] == "keep"]
    retired = [(ref, reason) for name, ref, reason in (c for c in env.offline.calls if c[0] == "retire")]
    assert len(kept) == 2 and str(row.material_ref) not in (earlier, *[ref for ref, _ in retired])
    assert sorted(reason for _, reason in retired) == ["CONSENT_REPLACED", "CONSENT_REPLACED"]   # earlier + loser
    assert earlier in [ref for ref, _ in retired]


def test_the_owners_personal_project_is_asked_and_a_failure_keeps_nothing(ns, env, monkeypatch):
    signed_in_as(ns, monkeypatch)
    for answer in (RuntimeError("down"), None, 0, True):
        def personal(user_id, answer=answer):
            if isinstance(answer, Exception):
                raise answer
            return answer
        env.oidc.projects_get_personal_project_id = personal
        with pytest.raises(ns.token_identity.IdentityRefused) as refusal:
            ns.token_identity.consent(env.module(), TOKEN, OWNER, chosen(env))
        assert refusal.value.code == "DEPENDENCY_UNAVAILABLE"
    assert env.offline.calls == [] and env.oidc.granted == [] and consent_row(ns, env) is None


def test_a_consent_kept_in_the_personal_project_releases_whatever_project_the_run_is_in(ns, env):
    """The token's scope project (1) is not where the material is: leaving or deleting a scope project cannot strand
    the consent, and the release reads the material from the personal project the row names."""
    ns.need_transport()
    binding = env.seed_agent()
    cred = Delegated(env, binding)
    material = consented(ns, env)
    run = admitted(ns, env, cred)
    assert run.release()["ok"] is True
    assert env.offline.calls == [("release", PERSONAL, material, f"token:{TOKEN}", OWNER_REF)]


@pytest.mark.parametrize("reason, audit, retire_reason", [
    ("user_suspended", "TOKEN_IDENTITY_OWNER_RETIRED", "OWNER_RETIRED"),
    ("user_missing", "TOKEN_IDENTITY_OWNER_RETIRED", "OWNER_RETIRED"),
])
def test_a_release_for_a_gone_owner_is_refused_and_retires_the_consent(ns, env, reason, audit, retire_reason):
    binding = env.seed_agent()
    cred = Delegated(env, binding)
    material = consented(ns, env)
    run = admitted(ns, env, cred)
    env.epochs[TOKEN] = {"active": False, "user": 3, "token": 4, "user_id": OWNER, "reason": reason}
    assert run.release()["reason_code"] == "DENIED_TOKEN_CONSENT_REQUIRED"
    assert consent_row(ns, env) is None and ("retire", material, retire_reason) in env.offline.calls
    assert audit in [e["reason_code"] for _, e in env.recorded]


def _verdicts(env, by_token):
    for token_id, reason in by_token.items():
        env.epochs[token_id] = {"active": reason is None, "user": 3, "token": 4, "user_id": OWNER, "reason": reason}


def test_the_hourly_sweep_retires_the_consents_of_gone_tokens_and_owners_and_keeps_live_ones(ns, env):
    materials = {token_id: consented(ns, env, token_id=token_id) for token_id in (31, 32, 33, 34, 35)}
    _verdicts(env, {31: None, 32: "token_missing", 33: "token_expired", 34: "user_suspended", 35: "user_missing"})
    assert ns.token_identity.sweep(env.module()) == {"seen": 5, "retired": 4}
    retired = {ref: reason for name, ref, reason in (c for c in env.offline.calls if c[0] == "retire")}
    assert retired == {materials[32]: "HOLDER_REVOKED", materials[33]: "HOLDER_REVOKED",
                       materials[34]: "OWNER_RETIRED", materials[35]: "OWNER_RETIRED"}
    assert consent_row(ns, env, 31) is not None and all(consent_row(ns, env, t) is None for t in (32, 33, 34, 35))
    assert sorted(e["reason_code"] for _, e in env.recorded) == [
        "TOKEN_IDENTITY_OWNER_RETIRED", "TOKEN_IDENTITY_OWNER_RETIRED", "TOKEN_IDENTITY_RETIRED", "TOKEN_IDENTITY_RETIRED"]
    assert ns.token_identity.sweep(env.module()) == {"seen": 1, "retired": 0}   # idempotent


def test_the_sweep_is_bounded_per_run_and_goes_round(ns, env):
    for token_id in range(41, 46):
        consented(ns, env, token_id=token_id)
    _verdicts(env, {token_id: None for token_id in range(41, 46)})
    seen = [ns.token_identity.sweep(env.module(), limit=2)["seen"] for _ in range(4)]
    assert seen == [2, 2, 1, 2]   # 41-42, 43-44, 45, then from the start again
    assert env.epoch_calls[:5] == [("token", t) for t in range(41, 46)]


def test_the_sweep_reads_nothing_under_off_or_without_the_source(ns, env, monkeypatch):
    consented(ns, env, token_id=51)
    _verdicts(env, {51: "token_missing"})
    reads = []
    real = env.get_session
    monkeypatch.setattr(ns.tools.db, "get_session", lambda project_id: reads.append(project_id) or real(project_id))
    assert ns.token_identity.sweep(env.module(governed=False)) == {"seen": 0, "retired": 0}
    env.sources = ["owner_delegated"]
    assert ns.token_identity.sweep(env.module()) == {"seen": 0, "retired": 0}
    assert reads == [] and env.offline.calls == []


def test_the_sweep_skips_a_token_it_cannot_read_and_goes_on(ns, env):
    first, second = consented(ns, env, token_id=61), consented(ns, env, token_id=62)
    env.epochs[62] = {"active": False, "user": 3, "token": None, "user_id": None, "reason": "token_missing"}
    env.epochs.pop(61, None)   # unreadable: None from the auth plugin
    assert ns.token_identity.sweep(env.module()) == {"seen": 2, "retired": 1}
    assert consent_row(ns, env, 61) is not None and ("retire", second, "HOLDER_REVOKED") in env.offline.calls
    assert first in env.offline.rows and env.offline.rows[first]["state"] == "active"


@pytest.mark.parametrize("call", ["consent", "withdraw"])
def test_consent_and_withdraw_refuse_under_off_and_audit_only_with_nothing_read(ns, env, monkeypatch, call):
    consented(ns, env)
    signed_in_as(ns, monkeypatch)
    reads = []
    real = env.get_session
    monkeypatch.setattr(ns.tools.db, "get_session", lambda project_id: reads.append(project_id) or real(project_id))
    for module in (env.module(governed=False), env.module(audit=True)):
        with pytest.raises(ns.token_identity.IdentityRefused) as refusal:
            if call == "consent":
                ns.token_identity.consent(module, TOKEN, OWNER, chosen(env))
            else:
                ns.token_identity.withdraw(module, TOKEN, OWNER)
        assert refusal.value.code == "GOVERNANCE_NOT_ENFORCED" and refusal.value.code in ns.token_identity.MESSAGES
    assert reads == [] and env.offline.calls == [] and env.oidc.granted == []
    assert consent_row(ns, env) is not None


def test_the_hourly_pat_task_sweeps_first_and_a_failing_sweep_does_not_stop_the_notifications(ns, env, monkeypatch):
    from fixtures.helpers import load_module_with_stubs  # pylint: disable=C0415
    pkg = admission_env.PKG
    enums = sys.modules[f"{pkg}.models.enums.all"]
    monkeypatch.setattr(enums, "NotificationEventTypes",
                        types.SimpleNamespace(personal_access_token_expiring="pat_expiring"), raising=False)
    sys.modules.setdefault(f"{pkg}.rpc", types.ModuleType(f"{pkg}.rpc")).__path__ = []
    pylon_tools = sys.modules["pylon.core.tools"]
    monkeypatch.setattr(pylon_tools, "web", types.SimpleNamespace(rpc=lambda *names: (lambda func: func)), raising=False)
    task = load_module_with_stubs(PLUGIN_ROOT / "rpc" / "pat_expiration.py", f"{pkg}.rpc.pat_expiration")
    order, fired = [], []
    rpc = types.SimpleNamespace(
        auth_list_tokens_expiring_soon=lambda: order.append("expiring") or [{"user_id": OWNER, "uuid": "u", "name": "n"}],
        projects_get_personal_project_id=lambda user_id: PERSONAL)
    module = types.SimpleNamespace(context=types.SimpleNamespace(
        rpc_manager=types.SimpleNamespace(timeout=lambda seconds: rpc),
        event_manager=types.SimpleNamespace(fire_event=lambda name, payload: fired.append(name))))
    for outcome in ({"seen": 1, "retired": 1}, RuntimeError("store down")):
        def sweep(passed, outcome=outcome):
            order.append("sweep")
            assert passed is module
            if isinstance(outcome, Exception):
                raise outcome
            return outcome
        monkeypatch.setattr(ns.token_identity, "sweep", sweep)
        task.RPC.check_pat_expiration(module)
    assert order == ["sweep", "expiring", "sweep", "expiring"] and fired == ["notifications_stream"] * 2


# --- the consent names its credentials (C9-SEC-81, option 1) -----------------------------------------------------------

def test_a_listed_credential_is_released_and_an_unlisted_one_is_refused(ns, env):
    """The version uses two delegated credentials; a consent naming one of them does not admit the run, one naming both
    does, and each is released."""
    ns.need_transport()
    binding = env.seed_agent()
    listed, unlisted = Delegated(env, binding), Delegated(env, binding, tool_id=2)
    consented(ns, env, covers=[listed])
    assert refused(ns, env, listed, unlisted) == "DENIED_TOKEN_CONSENT_REQUIRED"
    [deny] = env.denied
    assert deny["credential_ref"] == unlisted.ref_id   # the refusal names the credential the consent does not cover
    assert "does not cover this credential" in ns.adm.MESSAGES["DENIED_TOKEN_CONSENT_REQUIRED"]
    with ns.sa_orm.Session(env.engine(None)) as session:
        session.execute(ns.sa.update(ns.token_models.ApiTokenIdentityConsent).values(
            credential_refs=[listed.scope_item(), unlisted.scope_item()]))
        session.commit()
    run = admitted(ns, env, listed, unlisted)
    reply = run.release()
    assert reply["ok"] is True and len(reply["releases"]) == 2


def test_a_credential_changed_since_the_consent_is_not_covered(ns, env):
    binding = env.seed_agent()
    cred = Delegated(env, binding)
    item = {**cred.scope_item(), "credential_generation": 2}   # consented to an earlier/later generation
    consented(ns, env, covers=[])
    with ns.sa_orm.Session(env.engine(None)) as session:
        session.execute(ns.sa.update(ns.token_models.ApiTokenIdentityConsent).values(credential_refs=[item]))
        session.commit()
    assert refused(ns, env, cred) == "DENIED_TOKEN_CONSENT_REQUIRED"


def test_a_release_outside_the_consent_is_refused_even_after_admission(ns, env):
    binding = env.seed_agent()
    cred = Delegated(env, binding)
    consented(ns, env)
    run = admitted(ns, env, cred)
    with ns.sa_orm.Session(env.engine(None)) as session:   # the consent was given again for something else since
        session.execute(ns.sa.update(ns.token_models.ApiTokenIdentityConsent).values(credential_refs=[]))
        session.commit()
    assert run.release()["reason_code"] == "DENIED_TOKEN_CONSENT_REQUIRED"
    assert env.offline.calls == [] and env.oidc.redeemed == []


@pytest.mark.parametrize("case", ["empty", "none", "outside_scope", "not_delegated", "other_custodian", "unknown",
                                  "malformed", "too_many"])
def test_a_consent_naming_a_credential_it_may_not_name_is_refused_readably_with_nothing_kept(
        ns, env, monkeypatch, case):
    signed_in_as(ns, monkeypatch)
    binding = env.seed_agent()
    mine = Delegated(env, binding)
    picks = [{"project_id": 1, "credential_ref": mine.ref_id}]
    if case == "empty":
        picks = []
    elif case == "none":
        picks = None
    elif case == "outside_scope":
        env.offer_projects = {1, 2}   # the owner may use it there too, but the token is scoped to project 1 only
        picks = [{"project_id": 2, "credential_ref": mine.ref_id}]
    elif case == "not_delegated":
        stored = Credential(env, binding, tool_id=3, config_id=30)   # a stored secret, never offered
        picks.append({"project_id": 1, "credential_ref": stored.ref_id})
    elif case == "other_custodian":
        theirs = Delegated(env, binding, personal=True, tool_id=4)
        theirs.reference["custodian_subject_ref"] = f"idp:{TID}:someone-else"
        env.delegated.remove(theirs)   # the secrets plugin lists only the owner's own and granted ones
        picks.append({"project_id": 1, "credential_ref": theirs.ref_id})
    elif case == "unknown":
        picks.append({"project_id": 1, "credential_ref": str(uuid.uuid4())})
    elif case == "malformed":
        picks = [{"project_id": 1, "credential_ref": "not-a-uuid"}]
    elif case == "too_many":
        picks = picks * 65
    with pytest.raises(ns.token_identity.IdentityRefused) as refusal:
        ns.token_identity.consent(env.module(), TOKEN, OWNER, picks)
    expected = "CREDENTIALS_REQUIRED" if case in ("empty", "none") else "CREDENTIAL_NOT_ELIGIBLE"
    assert refusal.value.code == expected and expected in ns.token_identity.MESSAGES
    assert env.offline.calls == [] and env.oidc.granted == [] and consent_row(ns, env) is None
    if case in ("unknown", "other_custodian", "not_delegated"):   # one text whatever the reason: no existence leak
        assert ns.token_identity.MESSAGES[expected].count("cannot be used") == 1


def test_a_consent_given_again_replaces_the_list(ns, env, monkeypatch):
    signed_in_as(ns, monkeypatch)
    binding = env.seed_agent()
    first, second = Delegated(env, binding), Delegated(env, binding, tool_id=2)
    ns.token_identity.consent(env.module(), TOKEN, OWNER, chosen(env, first, second))
    earlier = str(consent_row(ns, env).material_ref)
    ns.token_identity.consent(env.module(), TOKEN, OWNER, chosen(env, second))
    row = consent_row(ns, env)
    assert [item["credential_ref"] for item in row.credential_refs] == [second.ref_id]
    assert ("retire", earlier, "CONSENT_REPLACED") in env.offline.calls
    assert refused(ns, env, first) == "DENIED_TOKEN_CONSENT_REQUIRED"


def test_the_candidates_are_the_owners_delegated_ones_in_the_asked_projects_only(ns, env, monkeypatch):
    binding = env.seed_agent()
    cred = Delegated(env, binding)
    assert ns.token_identity.candidates(env.module(), OWNER, [1, 1, 2]) == [
        {"project_id": 1, "credential_ref": cred.ref_id, "title": "sp-1"}]
    assert env.offered_calls == [(1, OWNER_REF, OWNER), (2, OWNER_REF, OWNER)]
    assert ns.token_identity.candidates(env.module(), OTHER, [1]) == []   # someone else sees none of the owner's
    for module, code in ((env.module(governed=False), "GOVERNANCE_NOT_ENFORCED"),):
        with pytest.raises(ns.token_identity.IdentityRefused) as refusal:
            ns.token_identity.candidates(module, OWNER, [1])
        assert refusal.value.code == code
    env.sources = ["owner_delegated"]
    with pytest.raises(ns.token_identity.IdentityRefused) as refusal:
        ns.token_identity.candidates(env.module(), OWNER, [1])
    assert refusal.value.code == "IDENTITY_SOURCE_NOT_ALLOWED"
