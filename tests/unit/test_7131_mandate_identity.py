"""C-1009 #7131: autonomous runs acting with an Entra-delegated credential through a mandate that names its identity.

The mandate admission, the credential decision, the seal and the release run for real against the SQLite harness
(``fixtures/admission_env.py`` + ``fixtures/mandate_env.py``). auth_oidc's offline RPCs, the secrets plugin's offline
material and Entra's token endpoint are fakes that record their calls. Every value below is a made-up test value.
"""
import base64
import json
import sys
import time
import types
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from pydantic import SecretStr

PLUGIN_ROOT = Path(__file__).parents[2]
sys.path.insert(0, str(PLUGIN_ROOT / "tests"))

from fixtures import mandate_env  # noqa: E402  pylint: disable=C0413
from fixtures.governed_credentials import (  # noqa: E402  pylint: disable=C0413
    CALLER, Credential, call_kwargs, payload_of, run_to_running, sealed_tool_settings,
)

TID = "11111111-2222-3333-4444-555555555555"
OWNER = mandate_env.OWNER
OID = "oid-owner-5"
ENTRA = f"https://login.microsoftonline.com/{TID}/v2.0"
OWNER_REF = f"idp:{TID}:{OID}"
GRAPH = "https://graph.microsoft.com"
CLIENT_SECRET = "test-client-secret-not-real"
CUSTODY_KEY = "custody-request-key-made-up-for-tests-0123"
GRAPH_TOKEN = "graph-token-made-up-for-tests"
REFRESH = "offline-refresh-token-made-up-0001"
ROTATED = "offline-refresh-token-made-up-0002"
SITE = "https://contoso.sharepoint.com/sites/sandbox"
IDENTITY_COLUMNS = ("identity_source", "identity_material_ref", "identity_consented_at")


def jwt(**claims):
    def part(value):
        return base64.urlsafe_b64encode(json.dumps(value).encode()).decode().rstrip("=")
    return f"{part({'alg': 'none'})}.{part(claims)}.sig"


ASSERTION = jwt(tid=TID, oid=OID, aud="api-client")


@pytest.fixture(scope="module")
def ns(tmp_path_factory):
    saved = dict(sys.modules)
    ns = mandate_env.install(PLUGIN_ROOT, tmp_path_factory.mktemp("mandate_identity"))
    _install_schedules(ns)
    yield ns
    sys.modules.clear()
    sys.modules.update(saved)


def _install_schedules(ns):
    """The real schedule tables and the scheduler's own claim and run writes, on the harness database (#7131 S1)."""
    from fixtures.helpers import load_module_with_stubs  # pylint: disable=C0415
    pkg = mandate_env.admission_env.PKG
    models = sys.modules[f"{pkg}.models"]
    models.config, models.db, models.db_tools = ns.tools.config, ns.tools.db, ns.tools.db_tools
    ns.tools.config.POSTGRES_TENANT_SCHEMA = "tenant"
    ns.Application = type("Application", (ns.Base,), {
        "__tablename__": "applications", "__table_args__": {"schema": "tenant"},
        "id": ns.sa.Column(ns.sa.Integer, primary_key=True)})
    ns.schedules = load_module_with_stubs(PLUGIN_ROOT / "models" / "entity_schedule.py", f"{pkg}.models.entity_schedule")
    for table in ns.Base.metadata.tables.values():   # PostgreSQL-only defaults do not exist on SQLite
        for column in table.columns:
            if column.server_default is not None and "jsonb" in str(getattr(column.server_default, "arg", "")):
                column.server_default = None
    ns.schedule_claim = load_module_with_stubs(PLUGIN_ROOT / "utils" / "schedule_claim.py",
                                               f"{pkg}.utils.schedule_claim")


class Entra:
    def __init__(self):
        self.posts, self.answers = [], []

    def __call__(self, url, data, timeout=None):
        self.posts.append((url, dict(data)))
        status, body = self.answers.pop(0) if self.answers else (
            200, {"access_token": GRAPH_TOKEN, "expires_in": 3600, "token_type": "Bearer"})
        return types.SimpleNamespace(status_code=status, content=b"x", json=lambda: body)


class AuthOidc:
    """auth_oidc's offline RPCs: each checks the signature the real one checks, and the refresh token travels sealed
    both ways (``bus`` keeps every request and reply as sent, to show no token crosses in plain text)."""

    def __init__(self, obo):
        self.obo, self.redeemed, self.granted, self.nonces, self.bus = obo, [], [], set(), []
        self.answer = {"ok": True, "assertion": ASSERTION, "expires_at": int(time.time()) + 3600, "refresh_token": None}
        self.grant_answer = {"ok": True, "refresh_token": REFRESH, "assertion_source": "login"}
        self.key = obo.seal_key(CUSTODY_KEY)
        self.reply_binding = b""   # a test makes the rotated token's binding another request's

    def timeout(self, seconds):
        return self

    def _check(self, fields, iat, nonce, signature):
        assert signature == self.obo.sign_fields(CUSTODY_KEY, fields, iat, nonce)
        assert abs(iat - time.time()) < 5 and nonce not in self.nonces
        self.nonces.add(nonce)

    def auth_oidc_obo_offline_assertion(self, tid, oid, holder, source, sealed_refresh_token, iat, nonce, signature):
        digest = __import__("hashlib").sha256(sealed_refresh_token.encode()).hexdigest()
        self._check(("v1-offline", tid, oid, holder, source, digest), iat, nonce, signature)
        token = self.obo.open_sealed(self.key, sealed_refresh_token, self.obo.seal_aad("redeem-req", tid, oid, holder, nonce))
        assert token is not None
        self.redeemed.append((tid, oid, holder, source, token))
        answer = dict(self.answer) if isinstance(self.answer, dict) else self.answer
        if isinstance(answer, dict) and answer.get("ok") is True:
            rotated = answer.pop("refresh_token", None)
            answer["sealed_refresh_token"] = self.obo.seal(
                self.key, rotated, self.obo.seal_aad("redeem-reply", tid, oid, holder, nonce) + self.reply_binding) \
                if rotated else None
        self.bus.append(json.dumps([tid, oid, holder, source, sealed_refresh_token, answer]))
        return answer

    def auth_oidc_obo_offline_grant(self, tid, oid, holder, iat, nonce, signature):
        self._check(("v1-offline-grant", tid, oid, holder), iat, nonce, signature)
        self.granted.append((tid, oid, holder))
        answer = dict(self.grant_answer)
        if answer.get("ok") is True:
            answer["sealed_refresh_token"] = self.obo.seal(
                self.key, answer.pop("refresh_token"), self.obo.seal_aad("grant-reply", tid, oid, holder, nonce))
        self.bus.append(json.dumps(answer))
        return answer


class Offline:
    """The secrets plugin's offline material: rows by id, the token kept apart."""

    def __init__(self):
        self.rows, self.calls = {}, []

    def add(self, holder, custodian=OWNER_REF, state="active", source="login", token=REFRESH):
        ref = str(uuid.uuid4())
        self.rows[ref] = {"holder": holder, "custodian": custodian, "state": state, "source": source, "generation": 1,
                          "token": token}
        return ref

    def keep_offline_material(self, project_id, *, holder_ref, custodian_user_id, custodian_subject_ref, source,
                              refresh_token, actor_user_id):
        self.calls.append(("keep", project_id, holder_ref, custodian_user_id, custodian_subject_ref, source,
                           actor_user_id))
        ref = self.add(holder_ref, custodian_subject_ref, source=source, token=refresh_token.get_secret_value())
        return {"ok": True, "material_ref": ref, "generation": 1}

    def release_offline_material(self, project_id, material_ref, *, holder_ref, custodian_subject_ref, release_id,
                                 actor_ref):
        self.calls.append(("release", project_id, material_ref, holder_ref, custodian_subject_ref))
        row = self.rows.get(material_ref)
        if row is None or row["state"] != "active":
            return {"ok": False, "reason_code": "OFFLINE_MATERIAL_INACTIVE"}
        if (row["holder"], row["custodian"]) != (holder_ref, custodian_subject_ref):
            return {"ok": False, "reason_code": "OFFLINE_MATERIAL_MISMATCH"}
        return {"ok": True, "refresh_token": SecretStr(row["token"]), "source": row["source"],
                "generation": row["generation"]}

    def rotate_offline_material(self, project_id, material_ref, *, expected_generation, refresh_token, caller_ref):
        self.calls.append(("rotate", material_ref, expected_generation))
        row = self.rows[material_ref]
        row["token"], row["generation"] = refresh_token.get_secret_value(), row["generation"] + 1
        return {"ok": True, "generation": row["generation"]}

    def retire_offline_material(self, project_id, material_ref, *, reason, caller_ref, actor_user_id=None):
        self.calls.append(("retire", str(material_ref), reason))
        if str(material_ref) in self.rows:
            self.rows[str(material_ref)]["state"] = "retired"
        return {"ok": True, "retired": True}


@pytest.fixture
def env(ns, monkeypatch):
    env = mandate_env.prepare(ns, ns.env)
    ns.adm._now = ns.real_now  # pylint: disable=W0212   # tokens are minted in real time
    ns.entra_obo.forget()
    ns.users[OWNER] = {"id": OWNER, "idp_issuer": ENTRA, "idp_tid": TID, "idp_oid": OID}
    env.idp_links[(ENTRA, TID, OID)] = {"id": OWNER}
    env.entra, env.oidc, env.offline = Entra(), AuthOidc(ns.entra_obo), Offline()
    env.secrets_extra = {name: getattr(env.offline, name) for name in (
        "keep_offline_material", "release_offline_material", "rotate_offline_material", "retire_offline_material")}
    monkeypatch.setattr(ns.entra_obo, "_post", env.entra)
    monkeypatch.setenv("ELITEA_API_CLIENT_SECRET", CLIENT_SECRET)
    monkeypatch.setenv("ELITEA_OBO_CUSTODY_KEY", CUSTODY_KEY)
    env.sources = ["owner_delegated", "application"]
    env.facts_version = 5   # the owner's own grants come with the decision's facts
    real_module = env.module

    def module(**kwargs):
        built = real_module(**kwargs)
        built.descriptor = types.SimpleNamespace(config={
            "entra_obo_client_id": "api-client", "mandate_identity_sources": list(env.sources)})
        built.context = types.SimpleNamespace(rpc_manager=env.oidc)
        return built

    monkeypatch.setattr(env, "module", module)
    return env


class Delegated(Credential):
    """A SharePoint configuration governed by an ``entra_delegated`` reference, granted to the mandate (or personal),
    the owner's own grant on a shared one (``owner_grant``) and covered by the mandate's consent (``covered``)."""

    def __init__(self, env, binding, mandate, *, personal=False, custodian_user_id=OWNER, grant=True, tool_id=1,
                 owner_grant=True, covered=True, **kwargs):
        env.secret_fields.setdefault("sharepoint", ["client_secret"])
        super().__init__(env, binding, ctype="sharepoint", material=False, grants=False, tool_id=tool_id,
                         config_id=10 + tool_id, **kwargs)
        row = self.reference
        row["auth_mode"] = "entra_delegated"
        if personal:
            row["ownership_class"], row["custodian_subject_ref"] = "personal", OWNER_REF
            row["custodian_user_id"] = custodian_user_id
        env.delegations[self.ref_id] = {"oauth_app_id": "platform", "tenant_id": TID, "resource": GRAPH,
                                        "scopes": [f"{GRAPH}/Sites.Selected"], "assertion_source": "login"}
        if grant:
            env.grants[(1, self.key(f"mandate:{mandate}"))] = {
                "grant_id": self.grant_id, "state": "active", "revocation_epoch": 4, "valid_from": None,
                "valid_until": None, "operations": ["*"], "subject_kind": "mandate"}
        if owner_grant and not personal:   # the owner may use it themselves (any agent): a grant to their subject
            self.owner_grant_id = str(uuid.uuid4())
            env.grants[(1, self.key(OWNER_REF))] = {
                "grant_id": self.owner_grant_id, "state": "active", "revocation_epoch": 7, "valid_from": None,
                "valid_until": None, "operations": ["*"], "subject_kind": "user_delegation"}
        if covered:
            cover(env, env.ns, mandate, self)

    def expanded(self):
        return {"elitea_title": "sp", "configuration_uuid": self.uuid, "configuration_project_id": 1,
                "configuration_type": "sharepoint", "configuration_generation": 3, "site_url": SITE}


def cover(env, ns, mandate_id, cred, generation=1, configuration_generation=3):
    """Add ``cred`` to what the mandate's consent covers (as a consent records it)."""
    scope = list(mandate_columns_full(env, ns, mandate_id).identity_scope or [])
    scope.append({"credential_ref": cred.ref_id, "credential_generation": generation,
                  "configuration_generation": configuration_generation, "toolkits": ["sharepoint"]})
    set_mandate(env, ns, mandate_id, identity_scope=scope)


def mandate_columns_full(env, ns, mandate_id):
    with ns.sa_orm.Session(env.engine(1)) as session:
        return session.execute(ns.sa.select(ns.Mandate.identity_scope).where(ns.Mandate.id == mandate_id)).one()


def set_mandate(env, ns, mandate_id, **values):
    with ns.sa_orm.Session(env.engine(1)) as session:
        session.execute(ns.sa.update(ns.Mandate).where(ns.Mandate.id == mandate_id).values(**values))
        session.commit()


def mandate_columns(env, ns, mandate_id):
    with ns.sa_orm.Session(env.engine(1)) as session:
        return session.execute(ns.sa.select(
            ns.Mandate.identity_source, ns.Mandate.identity_material_ref, ns.Mandate.identity_consented_at,
        ).where(ns.Mandate.id == mandate_id)).one()


def new_mandate(env, ns, binding, *, identity="owner_delegated", consented=True, operations=("run:schedule",),
                version=70, application=7):
    mandate = mandate_env.add_mandate(env, ns, state="active", binding_ref=binding, operations=operations,
                                      version=version, application=application)
    values = {"owner_user_ref": {"user_id": OWNER, "issuer": ENTRA, "tid": TID, "oid": OID}}
    if identity is not None:
        values["identity_source"] = identity
    if consented:
        values["identity_material_ref"] = uuid.UUID(env.offline.add(f"mandate:{mandate}"))
        values["identity_consented_at"] = datetime.now(timezone.utc)
    set_mandate(env, ns, mandate, **values)
    return mandate


def request_for(ns, mandate, trigger_ref="schedule:version:70", entry_point="schedule"):
    return ns.AdmissionRequest(
        project_id=1, binding_project_id=1, application_id=7, version_id=70, run_kind="mandate",
        entry_point=entry_point, caller_type="trigger", mandate_ref=str(mandate), trigger_ref=trigger_ref)


def admit(ns, env, mandate, *creds, **request):
    call = call_kwargs(payload_of(*creds))
    ref = ns.adm.admit_dispatch(env.module(), "indexer_agent", call, request_for(ns, mandate, **request))
    return call, ref


def admitted(ns, env, mandate, *creds, **request):
    call, ref = admit(ns, env, mandate, *creds, **request)
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


def refused(ns, env, mandate, *creds, **request):
    with pytest.raises(ns.exceptions.AdmissionDenied) as denied:
        admit(ns, env, mandate, *creds, **request)
    return denied.value.reason_code


def everything_said(env, *more):
    return "\n".join([json.dumps([e for _, e in env.recorded], default=str),
                      json.dumps(env.denied, default=str), "\n".join(env.log.lines), *map(str, more)])


def statements_of(env, ns, action):
    seen = []
    engine = env.engine(1)
    listener = lambda conn, cursor, statement, params, context, many: seen.append(statement)   # noqa: E731
    ns.sa.event.listen(engine, "before_cursor_execute", listener)
    try:
        action()
    finally:
        ns.sa.event.remove(engine, "before_cursor_execute", listener)
    return seen


# --- nothing changes unless the deployment and the mandate both name an identity ------------------------------------

@pytest.mark.parametrize("sources, identity", [
    ([], "owner_delegated"),                    # the deployment allows none (the default)
    (["application"], "owner_delegated"),       # it allows only the application identity
    (["owner_delegated"], None),                # the mandate names none
    (["owner_delegated"], "application"),       # it names the application identity
])
def test_a_delegated_credential_is_refused_unless_the_mandate_acts_with_its_owners_consent(ns, env, sources, identity):
    env.sources = sources
    binding = env.seed_agent()
    mandate = new_mandate(env, ns, binding, identity=identity)
    assert refused(ns, env, mandate, Delegated(env, binding, mandate)) == "DENIED_MANDATE_DELEGATION_UNSUPPORTED"
    assert env.oidc.redeemed == [] and env.entra.posts == [] and env.offline.calls == []


def test_with_no_identity_allowed_nothing_about_one_is_read_and_a_stored_secret_run_is_unchanged(ns, env):
    """Byte-identity with the base: the statements of a mandate run with a stored secret (an App-only credential is one)
    are the same whatever the switch says, and none of them names an identity column."""
    binding = env.seed_agent()
    mandate = new_mandate(env, ns, binding, identity=None, consented=False)
    cred = Credential(env, binding)
    env.grants[(1, cred.key(f"mandate:{mandate}"))] = env.grants.pop((1, cred.key()))
    runs = []
    for sources in ([], ["owner_delegated", "application"]):
        env.sources = sources
        runs.append(statements_of(env, ns, lambda: admit(ns, env, mandate, cred, trigger_ref=f"t:{len(runs)}")))
    off, on = runs
    named = [statement for statement in off if any(column in statement for column in IDENTITY_COLUMNS)]
    assert off == on and named == []


def test_a_mandate_naming_the_application_identity_runs_an_app_only_credential_as_before(ns, env):
    """Option 1 (Roman, 2026-10-29): an App-only SharePoint credential is an ordinary governed secret reference; a
    mandate run uses it as it always did."""
    ns.need_transport()
    binding = env.seed_agent()
    mandate = new_mandate(env, ns, binding, identity="application", consented=False)
    env.secret_fields["sharepoint"] = ["client_secret"]   # App-only: client id and secret (or a certificate)
    cred = Credential(env, binding, ctype="sharepoint")
    env.grants[(1, cred.key(f"mandate:{mandate}"))] = env.grants.pop((1, cred.key()))
    run = admitted(ns, env, mandate, cred)
    reply = run.release()
    assert reply["ok"] is True
    assert run.opened(reply["releases"][0])["fields"] == {"client_secret": f"{cred.secret}-client_secret"}
    [(_, event)] = env.recorded
    assert event["reason_code"] == "CREDENTIAL_RELEASED" and event["mandate_ref"] == str(mandate)
    assert "issuer" not in context_row(ns, env, run.ref).subject
    assert env.oidc.redeemed == [] and env.offline.calls == []


# --- the decision -----------------------------------------------------------------------------------------------------

def test_an_owner_delegated_mandate_without_consent_is_refused_readably_before_it_starts(ns, env):
    binding = env.seed_agent()
    mandate = new_mandate(env, ns, binding, consented=False)
    assert refused(ns, env, mandate, Delegated(env, binding, mandate)) == "DENIED_MANDATE_CONSENT_REQUIRED"
    assert "Governance" in ns.adm.MESSAGES["DENIED_MANDATE_CONSENT_REQUIRED"]


def test_a_consented_mandate_is_admitted_and_its_context_names_the_owners_subject(ns, env):
    binding = env.seed_agent()
    mandate = new_mandate(env, ns, binding)
    call, ref = admit(ns, env, mandate, Delegated(env, binding, mandate))
    row = context_row(ns, env, ref)
    assert (row.subject["kind"], row.subject["ref"]) == ("mandate", f"mandate:{mandate}")
    assert (row.subject["issuer"], row.subject["oid"], row.tenant) == (ENTRA, OID, TID)
    assert row.caller["kind"] == "trigger" and row.caller["user_id"] == OWNER
    [entry] = row.allowed_bindings
    assert entry["auth_mode"] == "entra_delegated"
    assert sealed_tool_settings(call)["credential_slot"]["fields"] == ["access_token"]
    assert env.oidc.redeemed == [] and env.entra.posts == []   # nothing is minted at admission


def test_a_shared_delegated_credential_needs_a_grant_to_the_mandate(ns, env):
    binding = env.seed_agent()
    mandate = new_mandate(env, ns, binding)
    assert refused(ns, env, mandate, Delegated(env, binding, mandate, grant=False)) == "DENIED_GRANT"


def test_a_personal_delegated_credential_is_the_owners_own_or_nobodys(ns, env):
    binding = env.seed_agent()
    mandate = new_mandate(env, ns, binding)
    admit(ns, env, mandate, Delegated(env, binding, mandate, personal=True))
    cred = Delegated(env, binding, mandate, personal=True, custodian_user_id=OWNER + 1, tool_id=2)
    assert refused(ns, env, mandate, cred) == "CREDENTIAL_CLASS_NOT_ALLOWED"


def _schedule(env, ns, updated_by):
    """A real schedule row of the agent version, owned by the mandate's owner, last changed by ``updated_by``."""
    row = ns.schedules.EntitySchedule(
        entity_type="agent", application_id=7, version_id=70, name="nightly", cron="* * * * *", timezone="UTC",
        prompt="list the files", created_by=OWNER, updated_by=updated_by, principal={"kind": "delegation"},
        next_run=datetime.now(timezone.utc) - timedelta(minutes=1))
    with ns.sa_orm.Session(env.engine(1)) as session:
        session.add(row)
        session.commit()
        return row.id, row.next_run


def _scheduler_writes(env, ns, schedule_id, seen):
    """What the tick writes before a run is admitted: the claim of the slot and the run row."""
    with ns.sa_orm.Session(env.engine(1)) as session:
        assert ns.schedule_claim.claim_schedule(session, schedule_id, seen, seen + timedelta(minutes=1))
        assert ns.schedule_claim.insert_run(session, schedule_id, "cron") is not None
        session.commit()


def test_a_schedule_someone_else_changed_last_needs_the_owner_to_save_it(ns, env):
    """C9-SEC-46: a schedule an operator changed last does not run with the owner's sign-in until the owner saves it;
    the scheduler's own writes (claim, run row) change nothing about that, in either direction."""
    binding = env.seed_agent()
    mandate = new_mandate(env, ns, binding)
    with ns.sa_orm.Session(env.engine(1)) as session:
        session.add(ns.Application(id=7))
        session.commit()
    schedule_id, seen = _schedule(env, ns, updated_by=77)                # an operator's edit
    _scheduler_writes(env, ns, schedule_id, seen)
    trigger = {"trigger_ref": f"schedule:entity:{schedule_id}"}
    assert refused(ns, env, mandate, Delegated(env, binding, mandate), **trigger) == "DENIED_MANDATE_CONSENT_REQUIRED"
    assert "must save the schedule" in ns.adm.MESSAGES["DENIED_MANDATE_CONSENT_REQUIRED"]
    with ns.sa_orm.Session(env.engine(1)) as session:                    # the owner saves it
        session.get(ns.schedules.EntitySchedule, schedule_id).updated_by = OWNER
        session.commit()
    for slot, tool_id in enumerate((2, 3)):                                # two consecutive slots, each claimed first
        with ns.sa_orm.Session(env.engine(1)) as session:
            row = session.get(ns.schedules.EntitySchedule, schedule_id)
            seen_now = row.next_run
            for run in session.scalars(ns.sa.select(ns.schedules.EntityScheduleRun)):
                run.status = "success"
            session.commit()
        _scheduler_writes(env, ns, schedule_id, seen_now)
        admit(ns, env, mandate, Delegated(env, binding, mandate, tool_id=tool_id), **trigger)
    assert slot == 1
    assert refused(ns, env, mandate, Delegated(env, binding, mandate, tool_id=4), trigger_ref="schedule:entity:999") \
        == "DENIED_MANDATE_CONSENT_REQUIRED"   # no such schedule: fail closed


# --- the release ------------------------------------------------------------------------------------------------------

def test_a_mandate_run_is_released_a_token_minted_from_the_owners_consent(ns, env):
    ns.need_transport()
    binding = env.seed_agent()
    mandate = new_mandate(env, ns, binding)
    run = admitted(ns, env, mandate, Delegated(env, binding, mandate))
    reply = run.release()
    assert reply["ok"] is True, reply
    assert run.opened(reply["releases"][0])["fields"] == {"access_token": GRAPH_TOKEN}
    material = str(mandate_columns(env, ns, mandate).identity_material_ref)
    assert env.offline.calls == [("release", 1, material, f"mandate:{mandate}", OWNER_REF)]
    assert env.oidc.redeemed == [(TID, OID, f"mandate:{mandate}", "login", REFRESH)]
    [(url, form)] = env.entra.posts
    assert url == f"https://login.microsoftonline.com/{TID}/oauth2/v2.0/token"
    assert (form["assertion"], form["requested_token_use"], form["client_id"]) == (ASSERTION, "on_behalf_of", "api-client")
    [(_, event)] = env.recorded
    assert (event["reason_code"], event["mandate_ref"], event["target_ref"]) == (
        "CREDENTIAL_RELEASED_DELEGATED", str(mandate), GRAPH)
    assert event["caller_ref"].startswith("indexer_worker") or event["caller_ref"]
    for secret in (GRAPH_TOKEN, ASSERTION, REFRESH, CLIENT_SECRET):
        assert secret not in everything_said(env, reply, context_row(ns, env, run.ref).subject)
    assert REFRESH not in "".join(env.oidc.bus)   # the refresh token crossed the bus sealed only


def test_a_second_release_is_served_from_the_cache_after_the_consent_is_read_again(ns, env):
    ns.need_transport()   # the release seals its reply (the shared transport; absent in a lone checkout)
    binding = env.seed_agent()
    mandate = new_mandate(env, ns, binding)
    run = admitted(ns, env, mandate, Delegated(env, binding, mandate))
    assert run.release()["ok"] is True and run.release()["ok"] is True
    assert len(env.oidc.redeemed) == 1 and len(env.entra.posts) == 1
    assert [call[0] for call in env.offline.calls] == ["release", "release"]   # the consent is checked every time


def test_a_mandate_run_with_two_slots_of_one_descriptor_redeems_the_consent_once(ns, env, monkeypatch):
    """#7294 review M1: a mandate run's slots stay on the sequential loop (they redeem one consent); the second slot is
    served from the token the first one minted. The consent is still read for each slot."""
    ns.need_transport()
    binding = env.seed_agent()
    mandate = new_mandate(env, ns, binding)
    run = admitted(ns, env, mandate, Delegated(env, binding, mandate), Delegated(env, binding, mandate, tool_id=2))
    real = env.oidc.auth_oidc_obo_offline_assertion
    monkeypatch.setattr(env.oidc, "auth_oidc_obo_offline_assertion",   # a redemption takes real time
                        lambda *a, **k: time.sleep(0.05) or real(*a, **k))
    reply = run.release()
    assert reply["ok"] is True, reply
    assert len(reply["releases"]) == 2
    assert [call[0] for call in env.offline.calls] == ["release", "release"]
    assert len(env.oidc.redeemed) == 1 and len(env.entra.posts) == 1


def test_a_withdrawn_consent_stops_the_next_release_even_with_a_cached_token(ns, env):
    ns.need_transport()   # the release seals its reply (the shared transport; absent in a lone checkout)
    binding = env.seed_agent()
    mandate = new_mandate(env, ns, binding)
    run = admitted(ns, env, mandate, Delegated(env, binding, mandate))
    assert run.release()["ok"] is True
    material = str(mandate_columns(env, ns, mandate).identity_material_ref)
    env.offline.rows[material]["state"] = "retired"   # withdrawn at the secrets plugin
    assert run.release()["reason_code"] == "DENIED_MANDATE_CONSENT_REQUIRED"
    set_mandate(env, ns, mandate, identity_material_ref=None, identity_consented_at=None)   # and on the mandate
    assert run.release()["reason_code"] == "DENIED_MANDATE_CONSENT_REQUIRED"
    assert len(env.oidc.redeemed) == 1


def test_a_consent_of_another_holder_or_person_is_never_redeemed(ns, env):
    binding = env.seed_agent()
    mandate = new_mandate(env, ns, binding)
    run = admitted(ns, env, mandate, Delegated(env, binding, mandate))
    material = str(mandate_columns(env, ns, mandate).identity_material_ref)
    for change in ({"holder": f"mandate:{uuid.uuid4()}"}, {"custodian": f"idp:{TID}:someone-else"}):
        saved = dict(env.offline.rows[material])
        env.offline.rows[material].update(change)
        assert run.release()["reason_code"] == "DENIED_MANDATE_CONSENT_REQUIRED"
        env.offline.rows[material] = saved
    assert env.oidc.redeemed == [] and env.entra.posts == []


def test_a_context_whose_subject_is_not_the_mandates_owner_is_refused(ns, env):
    binding = env.seed_agent()
    mandate = new_mandate(env, ns, binding)
    run = admitted(ns, env, mandate, Delegated(env, binding, mandate))
    with ns.sa_orm.Session(env.engine(1)) as session:
        row = session.get(ns.models.ExecutionSecurityContext, uuid.UUID(run.ref))
        row.subject = {**row.subject, "oid": "someone-else"}
        session.commit()
    assert run.release()["reason_code"] == "DENIED_SUBJECT_MISMATCH"
    with ns.sa_orm.Session(env.engine(1)) as session:
        row = session.get(ns.models.ExecutionSecurityContext, uuid.UUID(run.ref))
        row.subject = {key: value for key, value in row.subject.items() if key not in ("issuer", "oid")}
        session.commit()
    assert run.release()["reason_code"] == "STALE_CONTEXT"   # admitted before the identity was stored
    assert env.offline.calls == [] and env.oidc.redeemed == []


def test_a_consent_microsoft_refuses_is_withdrawn_and_the_run_refused_readably(ns, env):
    binding = env.seed_agent()
    mandate = new_mandate(env, ns, binding)
    run = admitted(ns, env, mandate, Delegated(env, binding, mandate))
    material = str(mandate_columns(env, ns, mandate).identity_material_ref)
    env.oidc.answer = {"ok": False, "code": "CONSENT_REFUSED"}
    assert run.release()["reason_code"] == "DENIED_MANDATE_CONSENT_EXPIRED"
    assert mandate_columns(env, ns, mandate).identity_material_ref is None   # needs a new consent
    assert ("retire", material, "CONSENT_REFUSED") in env.offline.calls
    assert "MANDATE_IDENTITY_CONSENT_REFUSED" in [e["reason_code"] for _, e in env.recorded]
    assert "Governance" in ns.adm.MESSAGES["DENIED_MANDATE_CONSENT_EXPIRED"]


@pytest.mark.parametrize("answer", [{"ok": False, "code": "ASSERTION_UNAVAILABLE"}, {"ok": False, "code": "REQUEST_REFUSED"},
                                    None])
def test_a_redemption_that_may_work_later_keeps_the_consent(ns, env, answer):
    binding = env.seed_agent()
    mandate = new_mandate(env, ns, binding)
    run = admitted(ns, env, mandate, Delegated(env, binding, mandate))
    env.oidc.answer = answer
    assert run.release()["reason_code"] == "DEPENDENCY_UNAVAILABLE"
    assert mandate_columns(env, ns, mandate).identity_material_ref is not None


def test_an_assertion_of_another_person_or_app_is_never_exchanged(ns, env):
    binding = env.seed_agent()
    mandate = new_mandate(env, ns, binding)
    run = admitted(ns, env, mandate, Delegated(env, binding, mandate))
    env.oidc.answer = {"ok": True, "assertion": jwt(tid=TID, oid="someone-else", aud="api-client")}
    assert run.release()["reason_code"] == "DENIED_SUBJECT_MISMATCH"
    env.oidc.answer = {"ok": True, "assertion": jwt(tid=TID, oid=OID, aud="another-app")}
    assert run.release()["reason_code"] == "DENIED_OAUTH_APP_UNAVAILABLE"
    assert env.entra.posts == []


def test_a_rotated_refresh_token_is_kept_as_the_next_generation(ns, env):
    ns.need_transport()   # the release seals its reply (the shared transport; absent in a lone checkout)
    binding = env.seed_agent()
    mandate = new_mandate(env, ns, binding)
    run = admitted(ns, env, mandate, Delegated(env, binding, mandate))
    env.oidc.answer = {**env.oidc.answer, "refresh_token": ROTATED}
    assert run.release()["ok"] is True
    material = str(mandate_columns(env, ns, mandate).identity_material_ref)
    assert ("rotate", material, 1) in env.offline.calls and env.offline.rows[material]["token"] == ROTATED
    assert ROTATED not in everything_said(env) and ROTATED not in "".join(env.oidc.bus)


def test_a_rotated_token_that_does_not_open_is_not_kept(ns, env):
    ns.need_transport()
    binding = env.seed_agent()
    mandate = new_mandate(env, ns, binding)
    run = admitted(ns, env, mandate, Delegated(env, binding, mandate))
    env.oidc.answer = {**env.oidc.answer, "refresh_token": ROTATED}
    env.oidc.reply_binding = b"|another-request"
    assert run.release()["ok"] is True
    assert not [call for call in env.offline.calls if call[0] == "rotate"]


def test_a_shared_credential_needs_the_owners_own_grant_at_admission_and_at_release(ns, env):
    ns.need_transport()
    binding = env.seed_agent()
    mandate = new_mandate(env, ns, binding)
    cred = Delegated(env, binding, mandate, owner_grant=False)
    assert refused(ns, env, mandate, cred) == "DENIED_GRANT"   # granted to the mandate, not to the owner
    cred.owner_grant_id = str(uuid.uuid4())
    env.grants[(1, cred.key(OWNER_REF))] = {
        "grant_id": cred.owner_grant_id, "state": "active", "revocation_epoch": 7, "valid_from": None,
        "valid_until": None, "operations": ["*"], "subject_kind": "user_delegation"}
    run = admitted(ns, env, mandate, cred)
    [entry] = context_row(ns, env, run.ref).allowed_bindings
    assert (entry["owner_grant_id"], entry["owner_grant_epoch"]) == (cred.owner_grant_id, 7)
    assert run.release()["ok"] is True
    env.grants[(1, cred.key(OWNER_REF))]["state"] = "revoked"   # revoked between admission and release
    ns.entra_obo.forget()
    assert run.release()["reason_code"] == "DENIED_GRANT"
    env.grants[(1, cred.key(OWNER_REF))].update(state="active", revocation_epoch=8)   # re-issued at another epoch
    assert run.release()["reason_code"] == "EPOCH_MISMATCH"


def test_a_personal_delegated_credential_needs_no_grant_of_the_owner(ns, env):
    binding = env.seed_agent()
    mandate = new_mandate(env, ns, binding)
    _, ref = admit(ns, env, mandate, Delegated(env, binding, mandate, personal=True))
    assert "owner_grant_id" not in context_row(ns, env, ref).allowed_bindings[0]


def test_a_credential_the_consent_does_not_cover_is_refused(ns, env):
    """The consent covers the credentials (and their generations) the version used when the owner gave it."""
    binding = env.seed_agent()
    mandate = new_mandate(env, ns, binding)
    assert refused(ns, env, mandate, Delegated(env, binding, mandate, covered=False)) == "DENIED_MANDATE_CONSENT_REQUIRED"
    reapproved = Delegated(env, binding, mandate, tool_id=2, covered=False)
    cover(env, ns, mandate, reapproved, configuration_generation=2)   # consented to an earlier approval of it
    assert refused(ns, env, mandate, reapproved) == "DENIED_MANDATE_CONSENT_REQUIRED"


def test_a_release_outside_the_consent_is_refused_even_after_admission(ns, env):
    binding = env.seed_agent()
    mandate = new_mandate(env, ns, binding)
    run = admitted(ns, env, mandate, Delegated(env, binding, mandate))
    set_mandate(env, ns, mandate, identity_scope=[])   # a consent given again for something else
    assert run.release()["reason_code"] == "DENIED_MANDATE_CONSENT_REQUIRED"
    assert env.oidc.redeemed == []


def test_a_revoked_grant_to_the_mandate_stops_the_release(ns, env):
    binding = env.seed_agent()
    mandate = new_mandate(env, ns, binding)
    cred = Delegated(env, binding, mandate)
    run = admitted(ns, env, mandate, cred)
    env.grants[(1, cred.key(f"mandate:{mandate}"))]["state"] = "revoked"
    assert run.release()["reason_code"] == "DENIED_GRANT"
    assert env.oidc.redeemed == [] and env.offline.calls == []


def test_the_owners_own_sign_in_and_the_mandate_never_share_a_token(ns, env):
    """A Graph token minted from the owner's live sign-in is not served to the mandate (it would outlive a withdrawal),
    and one minted from the consent is dropped with the consent."""
    ns.need_transport()   # the release seals its reply (the shared transport; absent in a lone checkout)
    binding = env.seed_agent()
    mandate = new_mandate(env, ns, binding)
    run = admitted(ns, env, mandate, Delegated(env, binding, mandate))
    key = (TID, OID, "api-client", GRAPH, (f"{GRAPH}/Sites.Selected",))   # the owner's interactive cache entry
    now = time.time()
    ns.entra_obo._keep(key, "token-of-the-owners-session", now + 3600, now + 3600, now)  # pylint: disable=W0212
    reply = run.release()
    assert reply["ok"] is True and len(env.oidc.redeemed) == 1   # redeemed from the consent, not the session's token
    material = str(mandate_columns(env, ns, mandate).identity_material_ref)
    assert ns.entra_obo.forget_material(material) == 1
    assert ns.entra_obo._cached(key, time.time()) is not None   # pylint: disable=W0212  # the session's stays its own


def test_the_switch_turned_off_after_admission_refuses_the_release_as_before(ns, env):
    binding = env.seed_agent()
    mandate = new_mandate(env, ns, binding)
    run = admitted(ns, env, mandate, Delegated(env, binding, mandate))
    env.sources = []
    assert run.release()["reason_code"] == "DENIED_MANDATE_DELEGATION_UNSUPPORTED"
    assert env.offline.calls == []


# --- consent and withdrawal -------------------------------------------------------------------------------------------

def signed_in_as(ns, monkeypatch, person=None, code=None):
    def verified(module, actor_user_id):
        if code:
            raise ns.exceptions.AdmissionDenied(code, "refused", {})
        return dict(person or {"issuer": ENTRA, "tid": TID, "oid": OID})
    monkeypatch.setattr(ns.credential_probe, "_probe_person", verified)


def test_the_owner_consents_once_signed_in_and_the_mandate_keeps_a_reference_only(ns, env, monkeypatch):
    binding = env.seed_agent()
    mandate = new_mandate(env, ns, binding, consented=False)
    cred = Delegated(env, binding, mandate, covered=False)
    signed_in_as(ns, monkeypatch)
    preview = ns.mandate_identity.consent_preview(env.module(), 1, mandate, OWNER)
    answer = ns.mandate_identity.consent(env.module(), 1, mandate, OWNER)
    assert answer["mandate_ref"] == str(mandate) and answer["covered"] == preview["covered"] == [{"toolkits": ["sharepoint"]}]
    assert [(item["credential_ref"], item["credential_generation"], item["configuration_generation"])
            for item in mandate_columns_full(env, ns, mandate).identity_scope] == [(cred.ref_id, 1, 3)]
    assert REFRESH not in "".join(env.oidc.bus)   # sealed on the bus
    assert env.oidc.granted == [(TID, OID, f"mandate:{mandate}")]
    [keep] = env.offline.calls
    assert keep == ("keep", 1, f"mandate:{mandate}", OWNER, OWNER_REF, "login", OWNER)
    columns = mandate_columns(env, ns, mandate)
    assert str(columns.identity_material_ref) in env.offline.rows and columns.identity_consented_at is not None
    [(_, event)] = env.recorded
    assert (event["event_type"], event["reason_code"], event["mandate_ref"], event["user_ref"]) == (
        "approval", "MANDATE_IDENTITY_CONSENTED", str(mandate), OWNER_REF)
    with ns.sa_orm.Session(env.engine(1)) as session:
        stored = json.dumps([str(value) for value in session.execute(
            ns.sa.select(ns.Mandate.__table__)).one()], default=str)
    assert REFRESH not in stored and REFRESH not in everything_said(env, answer)


def test_a_consent_given_again_replaces_and_retires_the_earlier_one(ns, env, monkeypatch):
    binding = env.seed_agent()
    mandate = new_mandate(env, ns, binding)
    Delegated(env, binding, mandate)
    earlier = str(mandate_columns(env, ns, mandate).identity_material_ref)
    signed_in_as(ns, monkeypatch)
    ns.mandate_identity.consent(env.module(), 1, mandate, OWNER)
    assert str(mandate_columns(env, ns, mandate).identity_material_ref) != earlier
    assert ("retire", earlier, "CONSENT_REPLACED") in env.offline.calls


@pytest.mark.parametrize("case, code", [
    ("not_owner", "CONSENT_OWNER_ONLY"),
    ("other_subject", "DENIED_SUBJECT_MISMATCH"),
    ("no_sign_in", "DENIED_ASSERTION_UNAVAILABLE"),
    ("token_caller", "DENIED_DELEGATION_UNSUPPORTED_CALLER"),
    ("switch_off", "IDENTITY_SOURCE_NOT_ALLOWED"),
    ("application", "IDENTITY_NOT_DELEGATED"),
    ("revoked", "INVALID_STATE"),
    ("unknown", "MANDATE_NOT_FOUND"),
    ("no_custody", "DENIED_ASSERTION_UNAVAILABLE"),
    ("nothing_to_consent", "NOTHING_TO_CONSENT"),
    ("sealed_with_another_key", "DENIED_ASSERTION_UNAVAILABLE"),
])
def test_a_consent_that_is_not_the_owners_own_live_sign_in_is_refused_with_nothing_kept(
        ns, env, monkeypatch, case, code):
    binding = env.seed_agent()
    mandate = new_mandate(env, ns, binding, consented=False, identity="application" if case == "application"
                          else "owner_delegated")
    if case != "nothing_to_consent":
        Delegated(env, binding, mandate, covered=False)
    actor = OWNER + 1 if case == "not_owner" else OWNER
    signed_in_as(ns, monkeypatch, person={"issuer": ENTRA, "tid": TID, "oid": "someone-else"}
                 if case == "other_subject" else None,
                 code={"no_sign_in": "DENIED_ASSERTION_UNAVAILABLE",
                       "token_caller": "DENIED_DELEGATION_UNSUPPORTED_CALLER"}.get(case))
    if case == "switch_off":
        env.sources = ["application"]
    if case == "revoked":
        set_mandate(env, ns, mandate, state="revoked", revocation_epoch=1)
    if case == "no_custody":
        env.oidc.grant_answer = {"ok": False, "code": "ASSERTION_UNAVAILABLE"}
    if case == "sealed_with_another_key":   # a reply Main cannot open is never kept
        env.oidc.key = ns.entra_obo.seal_key("another-request-key-made-up-0123456789")
    target = uuid.uuid4() if case == "unknown" else mandate
    with pytest.raises(ns.mandate_identity.IdentityRefused) as refusal:
        ns.mandate_identity.consent(env.module(), 1, target, actor)
    assert refusal.value.code == code and refusal.value.code in ns.mandate_identity.MESSAGES
    assert env.offline.calls == [] and mandate_columns(env, ns, mandate).identity_material_ref is None


def test_withdrawing_retires_the_consent_and_the_next_run_is_refused(ns, env):
    binding = env.seed_agent()
    mandate = new_mandate(env, ns, binding)
    material = str(mandate_columns(env, ns, mandate).identity_material_ref)
    assert ns.mandate_identity.withdraw(env.module(), 1, mandate, 1, operator=True) == {"mandate_ref": str(mandate)}
    assert mandate_columns(env, ns, mandate).identity_material_ref is None
    assert ("retire", material, "CONSENT_WITHDRAWN") in env.offline.calls
    assert [e["reason_code"] for _, e in env.recorded] == ["MANDATE_IDENTITY_WITHDRAWN"]
    assert refused(ns, env, mandate, Delegated(env, binding, mandate)) == "DENIED_MANDATE_CONSENT_REQUIRED"
    with pytest.raises(ns.mandate_identity.IdentityRefused) as again:
        ns.mandate_identity.withdraw(env.module(), 1, mandate, 1, operator=True)
    assert again.value.code == "CONSENT_NOT_GIVEN"


def test_the_owner_withdraws_their_own_consent_and_nobody_else_without_manage(ns, env):
    binding = env.seed_agent()
    mandate = new_mandate(env, ns, binding)
    with pytest.raises(ns.mandate_identity.IdentityRefused) as other:
        ns.mandate_identity.withdraw(env.module(), 1, mandate, OWNER + 1)
    assert other.value.code == "WITHDRAW_NOT_ALLOWED"
    assert ns.mandate_identity.withdraw(env.module(), 1, mandate, OWNER) == {"mandate_ref": str(mandate)}


def test_with_the_switch_emptied_a_consent_can_still_be_withdrawn_and_revoking_retires_it(ns, env):
    binding = env.seed_agent()
    first = new_mandate(env, ns, binding)
    second = new_mandate(env, ns, env.seed_agent(application_id=8, version_id=80), version=80, application=8)
    env.sources = []
    assert ns.mandate_identity.withdraw(env.module(), 1, first, OWNER) == {"mandate_ref": str(first)}
    material = str(mandate_columns(env, ns, second).identity_material_ref)
    assert task(ns, env, f"project_id=1;action=revoke;mandate={second}", actor=1)["state"] == "revoked"
    assert ("retire", material, "HOLDER_REVOKED") in env.offline.calls


def test_an_expired_mandate_retires_its_consent(ns, env):
    binding = env.seed_agent()
    mandate = new_mandate(env, ns, binding)
    material = str(mandate_columns(env, ns, mandate).identity_material_ref)
    set_mandate(env, ns, mandate, valid_until=datetime.now(timezone.utc) - timedelta(minutes=1))
    assert refused(ns, env, mandate, Delegated(env, binding, mandate)) == "MANDATE_EXPIRED"
    assert ("retire", material, "HOLDER_REVOKED") in env.offline.calls
    assert mandate_columns(env, ns, mandate).identity_material_ref is None


def test_an_inactive_owner_takes_their_consents_with_them(ns, env):
    binding = env.seed_agent()
    mandate = new_mandate(env, ns, binding)
    material = str(mandate_columns(env, ns, mandate).identity_material_ref)
    env.epochs[OWNER] = {"active": False, "user": 2, "token": 0}
    assert refused(ns, env, mandate, Delegated(env, binding, mandate)) == "MANDATE_INACTIVE"
    assert ("retire", material, "HOLDER_REVOKED") in env.offline.calls


# --- the operator task ------------------------------------------------------------------------------------------------

def task(ns, env, param, actor=3):
    return ns.mandate_admin.manage_mandate(env.module(), param, actor_user_id=actor)


CREATE = "project_id=1;action=create;version_id=70;owner_user_id=5;operations=run:schedule;max_runtime_s=600;" \
         "valid_until=2026-11-15T00:00:00+00:00"


def test_the_task_creates_a_mandate_naming_an_identity_the_deployment_allows(ns, env):
    env.seed_agent()
    env.sources = ["application"]
    assert task(ns, env, CREATE + ";identity=owner_delegated")["code"] == "IDENTITY_SOURCE_NOT_ALLOWED"
    assert "invalid identity" in task(ns, env, CREATE + ";identity=robot")["error"]
    env.sources = ["owner_delegated"]
    created = task(ns, env, CREATE + ";identity=owner_delegated")
    assert created["identity"] == "owner_delegated"
    assert mandate_columns(env, ns, uuid.UUID(created["mandate_ref"])).identity_source == "owner_delegated"


def test_a_mandate_created_without_an_identity_names_none_and_costs_the_same_statements(ns, env):
    """The INSERT names the new columns as NULL (like #7214's binding column; the startup safety net adds them to an
    older schema at boot); nothing else is read or written, whatever the switch says."""
    env.seed_agent()
    runs = []
    for sources in ([], ["owner_delegated", "application"]):
        env.sources = sources
        seen = statements_of(env, ns, lambda: runs.append(task(ns, env, CREATE)))
        task(ns, env, f"project_id=1;action=revoke;mandate={runs[-1]['mandate_ref']}", actor=1)
        runs[-1] = (runs[-1], seen)
    (first, off), (second, on) = runs
    assert len(off) == len(on) and [s.split()[0] for s in off] == [s.split()[0] for s in on]
    for created in (first, second):
        assert "identity" not in created
        assert tuple(mandate_columns(env, ns, uuid.UUID(created["mandate_ref"]))) == (None, None, None)


def test_revoking_a_mandate_retires_its_consent(ns, env):
    binding = env.seed_agent()
    mandate = new_mandate(env, ns, binding)
    material = str(mandate_columns(env, ns, mandate).identity_material_ref)
    result = task(ns, env, f"project_id=1;action=revoke;mandate={mandate}", actor=1)
    assert result["state"] == "revoked"
    assert ("retire", material, "HOLDER_REVOKED") in env.offline.calls
    assert mandate_columns(env, ns, mandate).identity_material_ref is None


def test_the_run_token_of_a_consented_mandate_run_names_the_mandate_not_the_person(ns, env):
    """The run's own platform identity stays the mandate (#7229/#603): acting with the owner's Microsoft sign-in on
    Graph does not make the run the owner on the platform."""
    ns.tools.config.SECRETS_MASTER_KEY = "master-key-made-up-for-tests-0123456789"
    ns.tools.config.APP_HOST = "http://localhost"
    try:
        binding = env.seed_agent()
        mandate = new_mandate(env, ns, binding)
        call, _ = admit(ns, env, mandate, Delegated(env, binding, mandate))
        token = call["kwargs"]["internal_run_token"]["token"]
        body = token[len("eio1."):].split(".")[0]
        claims = json.loads(base64.urlsafe_b64decode(body + "=" * (-len(body) % 4)))
        assert (claims["sub"], claims["uid"]) == (f"mandate:{mandate}", OWNER)
    finally:
        for name in ("SECRETS_MASTER_KEY", "APP_HOST"):
            vars(ns.tools.config).pop(name, None)
