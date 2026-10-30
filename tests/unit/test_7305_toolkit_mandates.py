"""C-1009 #7305: scheduled indexing under a toolkit-scoped mandate.

A mandate covers an agent version or the index schedules of one toolkit (``toolkit_id``). The table constraints, the
operator task, the admission of a scheduled index run on the toolkit runner, the re-check at the worker and the consent
scope run for real on the SQLite harness (``fixtures/admission_env.py`` + ``fixtures/mandate_env.py``); only the Pylon
runtime, the identity service and the audit methods are fakes. Every value is a made-up test value.
"""
import copy
import sys
import types
import uuid
from datetime import timedelta
from pathlib import Path

import pytest

PLUGIN_ROOT = Path(__file__).parents[2]
sys.path.insert(0, str(PLUGIN_ROOT / "tests"))

from fixtures import mandate_env  # noqa: E402  pylint: disable=C0413
from fixtures.governed_credentials import (  # noqa: E402  pylint: disable=C0413
    SECRET, Credential, call_kwargs, dumped, run_to_running,
)
from fixtures.mandate_env import NOW, OWNER, mandate_row  # noqa: E402  pylint: disable=C0413

PKG = mandate_env.admission_env.PKG
OWNER_REF = "idp:tid-1:oid-5"
TOOLKIT_TASK = "indexer_test_toolkit_tool"
RUN = "run-1"
TOOLKIT_CALLER = {"kind": "indexer_worker", "task_name": TOOLKIT_TASK, "run_id": RUN}
TRIGGER = f"index_schedule:1:idx:{OWNER}"
INDEX = "models.applications.tool.patch"
CREATOR, APPROVER = 3, 1
UNTIL = (NOW + timedelta(days=30)).isoformat()
CREATE = (f"project_id=1;action=create;toolkit_id=1;owner_user_id={OWNER};operations=run:index_schedule;"
          f"max_runtime_s=600;valid_until={UNTIL}")
APPROVE = "approval_ref=CHG-7305;policy_revision=rev-1"


@pytest.fixture(scope="module")
def ns(tmp_path_factory):
    saved = dict(sys.modules)
    yield mandate_env.install(PLUGIN_ROOT, tmp_path_factory.mktemp("toolkit_mandates"))
    sys.modules.clear()
    sys.modules.update(saved)


@pytest.fixture
def env(ns):
    env = mandate_env.prepare(ns, ns.env)
    env.facts_version = 5
    env.permissions[OWNER] = {INDEX, "credentials.use"}
    ns.mandate_admin.revoke_mandate_contexts = lambda session, mandate_id, reason="MANDATE_INACTIVE": 0
    return env


def stored_digest(env, ns, toolkit_id):
    """The digest of the toolkit row as it is now (what an approval stamps), or None when there is no such row."""
    [tool] = env.query(1, ns.EliteATool, ns.EliteATool.id == toolkit_id) or [None]
    return None if tool is None else ns.adm.toolkit_digest(toolkit_id, tool)


def add_toolkit_mandate(env, ns, *, toolkit_id=1, owner=OWNER, state="active", created_by=CREATOR, approved_by=None,
                        operations=("run:index_schedule",), valid_until=None, revision=1, runtime=600, identity=None,
                        digest="now"):
    """One toolkit mandate row; ``digest`` "now" stamps the toolkit as it is (as an approval does)."""
    if state in ("active", "suspended") and approved_by is None:
        approved_by = APPROVER
    if digest == "now":
        digest = stored_digest(env, ns, toolkit_id)
    mandate_id = uuid.uuid4()
    row = ns.Mandate(
        id=mandate_id, project_id=1, application_id=None, agent_version=None, agent_binding_ref=None,
        toolkit_id=toolkit_id, owner_user_id=owner,
        owner_user_ref={"user_id": owner, "issuer": "https://issuer.example", "tid": "tid-1", "oid": f"oid-{owner}"},
        allowed_operations=list(operations), allowed_targets=["*"], max_runtime_s=runtime,
        valid_until=valid_until or NOW + timedelta(days=30), state=state,
        approval_ref="CHG-1" if approved_by else None, policy_revision="rev-1" if approved_by else None,
        created_by=created_by, approved_by=approved_by, revision=revision, identity_source=identity,
        toolkit_digest=digest)
    with ns.sa_orm.Session(env.engine(1)) as session:
        session.add(row)
        session.commit()
    return mandate_id


class SharePoint(Credential):
    """A SharePoint configuration governed by an ``entra_delegated`` reference: no stored value to carry."""

    def __init__(self, env, **kwargs):
        env.secret_fields.setdefault("sharepoint", ["client_secret"])
        super().__init__(env, uuid.uuid4(), ctype="sharepoint", material=False, grants=False, **kwargs)
        self.reference["auth_mode"] = "entra_delegated"

    def expanded(self):
        return {"elitea_title": "sp", "configuration_uuid": self.uuid, "configuration_project_id": 1,
                "configuration_type": "sharepoint", "configuration_generation": 3,
                "site_url": "https://contoso.example/sites/sandbox"}


def owner_credential(env, *, tool_id=1, granted=True, personal=False, custodian=OWNER, delegated=False):
    """A credential of toolkit ``tool_id``: a project one the owner holds a grant on (any binding), or a personal one;
    ``delegated``: an Entra-delegated SharePoint one."""
    kind = SharePoint if delegated else (lambda env_, **kw: Credential(env_, uuid.uuid4(), grants=False, **kw))
    cred = kind(env, tool_id=tool_id, config_id=100 + tool_id)
    if personal:
        cred.reference.update(ownership_class="personal", custodian_user_id=custodian,
                              custodian_subject_ref=f"idp:tid-1:oid-{custodian}")
    elif granted:
        env.grants[(1, cred.key(OWNER_REF))] = {
            "grant_id": cred.grant_id, "state": "active", "revocation_epoch": 4, "valid_from": None,
            "valid_until": None, "operations": ["*"]}
    return cred


def request(ns, mandate_id, **over):
    values = dict(project_id=1, binding_project_id=1, run_kind="mandate", entry_point="index_schedule",
                  caller_type="trigger", caller_id=None, mandate_ref=str(mandate_id), toolkit_id=1)
    values.update(over)
    values.setdefault("trigger_ref", f"index_schedule:{values['toolkit_id']}:idx:{OWNER}")
    return ns.AdmissionRequest(**values)


def index_call(cred):
    tool = cred.tool()
    payload = {"toolkit_config": tool, "tool_name": "index_data", "tool_params": {"index_name": "idx"}}
    return call_kwargs(payload, toolkit_config=copy.deepcopy(tool))


def dispatch(ns, env, cred, mandate_id, module=None, task=TOOLKIT_TASK, **over):
    call = index_call(cred)
    ref = ns.adm.admit_dispatch(module or env.module(), task, call, request(ns, mandate_id, **over))
    return call, ref


def refused(ns, env, cred, mandate_id, **kwargs):
    with pytest.raises(ns.exceptions.AdmissionDenied) as err:
        dispatch(ns, env, cred, mandate_id, **kwargs)
    return err.value


def contexts(env, ns):
    return env.query(1, ns.models.ExecutionSecurityContext)


def admissions(env):
    return [event for _, event in env.recorded if event["event_type"] == "admission"]


# --- the table -----------------------------------------------------------------------------------------------------

def test_a_mandate_is_an_agent_version_mandate_or_a_toolkit_mandate_never_both_or_neither(ns, env):
    add_toolkit_mandate(env, ns)
    for over in (dict(agent_version=70, application_id=7, agent_binding_ref=uuid.uuid4()),   # both
                 dict(toolkit_id=None)):                                                     # neither
        row = dict(id=uuid.uuid4(), project_id=1, application_id=None, agent_version=None, agent_binding_ref=None,
                   toolkit_id=2, owner_user_id=OWNER, owner_user_ref={"user_id": OWNER}, allowed_operations=["x"],
                   allowed_targets=["*"], max_runtime_s=600, valid_until=NOW + timedelta(days=3), state="draft",
                   created_by=CREATOR, revision=1)
        row.update(over)
        with ns.sa_orm.Session(env.engine(1)) as session:
            session.add(ns.Mandate(**row))
            with pytest.raises(ns.sa.exc.IntegrityError):
                session.commit()


def test_one_live_toolkit_mandate_per_toolkit_and_owner(ns, env):
    add_toolkit_mandate(env, ns, state="draft")
    add_toolkit_mandate(env, ns, state="draft", owner=6)                # another owner: its own
    add_toolkit_mandate(env, ns, state="draft", toolkit_id=2)           # another toolkit: its own
    add_toolkit_mandate(env, ns, state="revoked", revision=2)           # an ended one does not count
    with pytest.raises(ns.sa.exc.IntegrityError):
        add_toolkit_mandate(env, ns, state="pending_approval", revision=3)


# --- the operator task -----------------------------------------------------------------------------------------------

def run(ns, env, param, actor=CREATOR):
    return ns.mandate_admin.manage_mandate(env.module(governed=False), param, actor)


def seed_toolkit(env, tool_id=1):
    env.add_toolkit(1, 70, tool_id, "github", {"url": "https://github.example"})


def test_a_toolkit_mandate_is_created_as_a_draft_with_no_version_and_no_binding(ns, env):
    seed_toolkit(env)
    result = run(ns, env, CREATE)
    assert (result["action"], result["state"], result["revision"]) == ("create", "draft", 1)
    with ns.sa_orm.Session(env.engine(1)) as session:
        row = session.execute(ns.sa.select(
            ns.Mandate.toolkit_id, ns.Mandate.application_id, ns.Mandate.agent_version, ns.Mandate.agent_binding_ref,
            ns.Mandate.allowed_operations, ns.Mandate.owner_user_ref,
        ).where(ns.Mandate.id == uuid.UUID(result["mandate_ref"]))).one()
    assert (row.toolkit_id, row.application_id, row.agent_version, row.agent_binding_ref) == (1, None, None, None)
    assert row.allowed_operations == ["run:index_schedule"] and row.owner_user_ref["oid"] == "oid-5"
    [(_, event)] = env.recorded
    assert (event["reason_code"], event["toolkit_instance_id"], event["mandate_ref"]) == (
        "MANDATE_DRAFT", "1", result["mandate_ref"])
    assert "agent_binding_ref" not in event and "application_id" not in event


def test_the_four_eyes_rule_and_the_states_are_those_of_an_agent_mandate(ns, env):
    seed_toolkit(env)
    mandate_id = run(ns, env, CREATE)["mandate_ref"]
    assert run(ns, env, f"project_id=1;action=submit;mandate={mandate_id}")["state"] == "pending_approval"
    for actor in (CREATOR, OWNER):   # neither its creator nor its owner approves it
        assert run(ns, env, f"project_id=1;action=approve;mandate={mandate_id};{APPROVE}", actor)["code"] == "SELF_APPROVAL"
    assert run(ns, env, f"project_id=1;action=approve;mandate={mandate_id};{APPROVE}", APPROVER)["state"] == "active"
    assert run(ns, env, f"project_id=1;action=suspend;mandate={mandate_id}")["state"] == "suspended"
    assert run(ns, env, f"project_id=1;action=resume;mandate={mandate_id}", OWNER)["code"] == "SELF_APPROVAL"
    assert run(ns, env, f"project_id=1;action=resume;mandate={mandate_id}", APPROVER)["state"] == "active"
    assert run(ns, env, f"project_id=1;action=revoke;mandate={mandate_id}")["state"] == "revoked"
    assert [e["reason_code"] for _, e in env.recorded] == [
        "MANDATE_DRAFT", "MANDATE_PENDING_APPROVAL", "MANDATE_ACTIVE", "MANDATE_SUSPENDED", "MANDATE_ACTIVE",
        "MANDATE_REVOKED"]
    assert all(e.get("toolkit_instance_id") == "1" for _, e in env.recorded)


def test_a_second_live_one_for_the_same_toolkit_and_owner_is_refused_and_revisions_count_per_pair(ns, env):
    seed_toolkit(env)
    first = run(ns, env, CREATE)["mandate_ref"]
    assert run(ns, env, CREATE)["code"] == "MANDATE_EXISTS"
    ns.users[6] = {"id": 6, "idp_issuer": "https://issuer.example", "idp_tid": "tid-1", "idp_oid": "oid-6"}
    assert run(ns, env, CREATE.replace(f"owner_user_id={OWNER}", "owner_user_id=6"))["revision"] == 1
    run(ns, env, f"project_id=1;action=revoke;mandate={first}")
    assert run(ns, env, CREATE)["revision"] == 2


def test_a_toolkit_that_does_not_exist_or_is_gone_is_refused(ns, env):
    assert run(ns, env, CREATE)["code"] == "TOOLKIT_NOT_FOUND"
    mandate_id = add_toolkit_mandate(env, ns, state="pending_approval", toolkit_id=9)
    assert run(ns, env, f"project_id=1;action=approve;mandate={mandate_id};{APPROVE}", APPROVER)["code"] == \
        "TOOLKIT_NOT_FOUND"
    assert mandate_row(env, ns, mandate_id).state == "pending_approval"


@pytest.mark.parametrize("param, error", [
    (CREATE.replace("run:index_schedule", "run:schedule"), "invalid operations"),
    (CREATE.replace("run:index_schedule", "run:index_schedule,run:webhook"), "invalid operations"),
    (CREATE.replace("toolkit_id=1", "toolkit_id=x"), "invalid toolkit_id"),
])
def test_a_toolkit_mandate_allows_index_schedules_only(ns, env, param, error):
    values, message = ns.mandate_admin.parse_mandate_param(param)
    assert values is None and error in message


# --- admission of a scheduled index run --------------------------------------------------------------------------------

def test_a_covered_run_is_admitted_on_the_mandate_on_the_toolkit_runner_with_a_reference_only_payload(ns, env):
    cred = owner_credential(env)
    mandate_id = add_toolkit_mandate(env, ns)
    call, ref = dispatch(ns, env, cred, mandate_id)
    [row] = contexts(env, ns)
    assert str(row.id) == ref and (row.entry_point, row.profile, row.application_id, row.version_id) == (
        "index_schedule", "development", None, None)
    assert row.agent_binding_ref is None and row.caller == {"kind": "trigger", "ref": TRIGGER, "user_id": OWNER}
    assert row.subject["kind"] == "mandate" and row.subject["ref"] == f"mandate:{mandate_id}"
    assert row.subject["owner"] == OWNER_REF and row.revocation_epochs["mandate"] == 0
    assert row.allowed_bindings[0]["grant_id"] == cred.grant_id   # the owner's own grant
    [event] = admissions(env)
    assert (event["reason_code"], event["mandate_ref"], event["caller_ref"], event["toolkit_instance_id"]) == (
        "ADMITTED", str(mandate_id), TRIGGER, "1")   # a mandate run, never a development one
    assert call["meta"]["context_deadline"].code == "RUNTIME_LIMIT_EXCEEDED"   # Main stops it (the wrapper takes it)
    assert SECRET not in dumped(call["kwargs"]) and SECRET not in dumped(call["meta"])
    assert env.verify_calls == []   # nobody is verified: the mandate is the authority


def test_the_run_deadline_is_the_mandates_longest_run(ns, env):
    cred = owner_credential(env)
    mandate_id = add_toolkit_mandate(env, ns, runtime=900)
    call = index_call(cred)
    ns.adm.admit_dispatch(env.module(), TOOLKIT_TASK, call, request(ns, mandate_id))
    deadline = call["meta"]["context_deadline"]
    assert (deadline.at, deadline.code) == (NOW + timedelta(seconds=900), "RUNTIME_LIMIT_EXCEEDED")


def test_the_owners_personal_credential_needs_no_grant_and_someone_elses_is_refused(ns, env):
    mine = owner_credential(env, personal=True)
    assert dispatch(ns, env, mine, add_toolkit_mandate(env, ns))[1] is not None
    theirs = owner_credential(env, tool_id=2, personal=True, custodian=6)
    other = add_toolkit_mandate(env, ns, toolkit_id=2)
    assert refused(ns, env, theirs, other, toolkit_id=2).reason_code == "CREDENTIAL_CLASS_NOT_ALLOWED"


def test_a_project_credential_the_owner_holds_no_grant_on_is_refused(ns, env):
    cred = owner_credential(env, granted=False)
    env.grants[(1, cred.key("mandate:" + str(uuid.uuid4())))] = {   # a grant to someone else does not count
        "grant_id": "g-other", "state": "active", "revocation_epoch": 1, "valid_from": None, "valid_until": None}
    assert refused(ns, env, cred, add_toolkit_mandate(env, ns)).reason_code == "DENIED_GRANT"
    assert contexts(env, ns) == []


@pytest.mark.parametrize("permissions", [{"credentials.use"}, {INDEX}])
def test_an_owner_who_may_not_index_the_toolkit_or_use_credentials_is_refused(ns, env, permissions):
    cred = owner_credential(env)
    env.permissions[OWNER] = permissions
    assert refused(ns, env, cred, add_toolkit_mandate(env, ns)).reason_code == "DENIED_RBAC"


@pytest.mark.parametrize("state, over, code", [
    ("suspended", {}, "MANDATE_INACTIVE"), ("revoked", {}, "MANDATE_INACTIVE"), ("draft", {}, "MANDATE_INACTIVE"),
    ("pending_approval", {}, "MANDATE_INACTIVE"), ("expired", {}, "MANDATE_EXPIRED"),
    ("active", {"operations": ("run:schedule",)}, "DENIED_OPERATION"),
])
def test_a_mandate_that_cannot_carry_the_run_is_refused_with_its_code(ns, env, state, over, code):
    cred = owner_credential(env)
    error = refused(ns, env, cred, add_toolkit_mandate(env, ns, state=state, **over))
    assert error.reason_code == code and error.message == ns.adm.MESSAGES[code]
    [deny] = env.denied
    assert (deny["reason_code"], deny["caller_ref"]) == (code, TRIGGER)


def test_an_overdue_mandate_expires_on_the_spot(ns, env):
    cred = owner_credential(env)
    mandate_id = add_toolkit_mandate(env, ns, valid_until=NOW - timedelta(minutes=1))
    assert refused(ns, env, cred, mandate_id).reason_code == "MANDATE_EXPIRED"
    assert mandate_row(env, ns, mandate_id).state == "expired"


def test_the_mandate_must_be_for_this_toolkit_and_a_toolkit_mandate_carries_no_agent_run(ns, env):
    cred = owner_credential(env)
    other_toolkit = add_toolkit_mandate(env, ns, toolkit_id=2)
    assert refused(ns, env, cred, other_toolkit).reason_code == "MANDATE_REQUIRED"
    binding = env.seed_agent()
    agent = mandate_env.add_mandate(env, ns, state="active", binding_ref=binding)
    assert refused(ns, env, cred, agent).reason_code == "MANDATE_REQUIRED"   # an agent mandate never covers indexing
    toolkit = add_toolkit_mandate(env, ns)
    error = refused(ns, env, cred, toolkit, entry_point="schedule", toolkit_id=None, application_id=7, version_id=70)
    assert error.reason_code == "DENIED_ENTRY_POINT"   # an agent entry point on the toolkit runner
    with pytest.raises(ns.exceptions.AdmissionDenied) as err:
        ns.adm.admit_execution(env.module(), request(ns, toolkit, entry_point="schedule", toolkit_id=None,
                                                     application_id=7, version_id=70), run_id=RUN, payload={})
    assert err.value.reason_code == "MANDATE_REQUIRED"   # and a toolkit mandate never covers an agent run


def test_an_index_schedule_request_runs_only_on_the_toolkit_runner(ns, env):
    cred = owner_credential(env)
    error = refused(ns, env, cred, add_toolkit_mandate(env, ns), task="indexer_agent")
    assert error.reason_code == "DENIED_ENTRY_POINT"
    assert ns.adm.task_fits_entry(TOOLKIT_TASK, "index_schedule")
    assert not ns.adm.task_fits_entry("indexer_agent", "index_schedule")
    assert "index_schedule" not in ns.adm.DEVELOPMENT_ENTRY_POINTS   # never a development run


def test_an_unlinked_or_inactive_owner_is_refused(ns, env):
    cred = owner_credential(env)
    mandate_id = add_toolkit_mandate(env, ns)
    env.idp_links.clear()
    assert refused(ns, env, cred, mandate_id).reason_code == "IDENTITY_SUBJECT_UNLINKED"
    env.epochs[OWNER] = {"active": False, "user": 1, "token": 0}
    assert refused(ns, env, cred, mandate_id).reason_code == "MANDATE_INACTIVE"
    assert mandate_row(env, ns, mandate_id).state == "suspended"


def module_with_identity(env, sources):
    module = env.module()
    module.descriptor = types.SimpleNamespace(config={"mandate_identity_sources": list(sources)})
    return module


def test_a_delegated_credential_needs_the_owners_consent(ns, env):
    cred = owner_credential(env, personal=True, delegated=True)
    plain = add_toolkit_mandate(env, ns)
    assert refused(ns, env, cred, plain, module=module_with_identity(env, ())).reason_code == \
        "DENIED_MANDATE_DELEGATION_UNSUPPORTED"
    with ns.sa_orm.Session(env.engine(1)) as session:   # the same mandate, naming the owner's sign-in, no consent yet
        session.execute(ns.sa.update(ns.Mandate).where(ns.Mandate.id == plain).values(identity_source="owner_delegated"))
        session.commit()
    error = refused(ns, env, cred, plain, module=module_with_identity(env, ("owner_delegated",)))
    assert error.reason_code == "DENIED_MANDATE_CONSENT_REQUIRED"
    with ns.sa_orm.Session(env.engine(1)) as session:   # consented, for this credential at these generations
        session.execute(ns.sa.update(ns.Mandate).where(ns.Mandate.id == plain).values(
            identity_material_ref=uuid.uuid4(), identity_consented_at=NOW, identity_scope=[{
                "credential_ref": cred.ref_id, "credential_generation": 1, "configuration_generation": 3}]))
        session.commit()
    _call, ref = dispatch(ns, env, cred, plain, module=module_with_identity(env, ("owner_delegated",)))
    [row] = contexts(env, ns)
    assert str(row.id) == ref and row.subject["oid"] == "oid-5" and row.allowed_bindings[0]["auth_mode"] == \
        "entra_delegated"


# --- at the worker -----------------------------------------------------------------------------------------------------

def test_the_worker_validates_the_run_and_a_revoked_owner_grant_revokes_it(ns, env):
    cred = owner_credential(env)
    mandate_id = add_toolkit_mandate(env, ns)
    _call, ref = dispatch(ns, env, cred, mandate_id)
    answer = run_to_running(env, ns, ref, caller=TOOLKIT_CALLER)
    assert answer["ok"] is True, answer
    env.grants[(1, cred.key(OWNER_REF))]["state"] = "revoked"
    answer = ns.adm.validate_context(env.module(), 1, ref, dict(TOOLKIT_CALLER))
    assert (answer["ok"], answer["reason_code"]) == (False, "DENIED_GRANT")
    [row] = contexts(env, ns)
    assert (row.state, row.revoked_reason) == ("revoked", "DENIED_GRANT")


def release(ns, env, ref, slot):
    _private, public = ns.keypair()
    return ns.credential_release.release_credential(
        env.module(), project_id=1, context_ref=ref, probe_ref=None, caller=dict(TOOLKIT_CALLER),
        toolkit_instance_id="1", field="credentials", slot=dict(slot), recipient_key=public)


def test_the_worker_gets_the_credential_released_on_the_mandate_and_not_after_the_grant_is_gone(ns, env):
    ns.need_transport()
    cred = owner_credential(env)
    mandate_id = add_toolkit_mandate(env, ns)
    call, ref = dispatch(ns, env, cred, mandate_id)
    assert run_to_running(env, ns, ref, caller=TOOLKIT_CALLER)["ok"] is True
    slot = call["kwargs"]["toolkit_config"]["settings"]["credentials"]["credential_slot"]
    reply = release(ns, env, ref, slot)
    assert reply["ok"] is True and SECRET not in str(reply)
    [(_, event)] = env.recorded[-1:]
    assert (event["reason_code"], event["mandate_ref"], event["user_ref"]) == (
        "CREDENTIAL_RELEASED", str(mandate_id), OWNER_REF)
    env.grants[(1, cred.key(OWNER_REF))]["state"] = "revoked"
    reply = release(ns, env, ref, slot)
    assert (reply["ok"], reply["reason_code"]) == (False, "DENIED_GRANT")


def test_a_mandate_suspended_after_admission_stops_the_run_at_the_worker(ns, env):
    cred = owner_credential(env)
    mandate_id = add_toolkit_mandate(env, ns)
    _call, ref = dispatch(ns, env, cred, mandate_id)
    with ns.sa_orm.Session(env.engine(1)) as session:
        session.execute(ns.sa.update(ns.Mandate).where(ns.Mandate.id == mandate_id).values(state="suspended"))
        session.commit()
    answer = ns.adm.validate_context(env.module(), 1, ref, dict(TOOLKIT_CALLER))
    assert (answer["ok"], answer["reason_code"]) == (False, "MANDATE_INACTIVE")


# --- coverage, the readable refusal and the consent scope ------------------------------------------------------------

@pytest.mark.parametrize("key, created_by, owner", [
    (str(OWNER), OWNER, OWNER), (str(OWNER), None, None), (str(OWNER), 77, None),   # NULL: saved again first (C9-SEC-72)
    ("-1", OWNER, OWNER), ("-1", None, None), ("x", OWNER, None),
])
def test_whose_index_schedule_it_is(ns, key, created_by, owner):
    assert ns.trigger_principal.index_schedule_owner(key, created_by) == owner
    mandates = {(1, OWNER): "mine"}
    assert ns.trigger_principal.covering_toolkit_mandate(mandates, 1, key, created_by) == (
        "mine" if owner == OWNER else None)


def test_the_latest_revision_per_toolkit_and_owner_is_read_in_one_query(ns, env):
    add_toolkit_mandate(env, ns, state="revoked", revision=1)
    latest = add_toolkit_mandate(env, ns, state="suspended", revision=2)
    other = add_toolkit_mandate(env, ns, owner=6, state="active")
    binding = env.seed_agent()
    mandate_env.add_mandate(env, ns, state="active", binding_ref=binding)   # an agent mandate is not listed
    with env.get_session(1) as session:
        found = ns.trigger_principal.latest_toolkit_mandates(session, 1)
    assert {key: row.id for key, row in found.items()} == {(1, OWNER): latest, (1, 6): other}
    assert found[(1, OWNER)].state == "suspended"


def test_the_index_history_names_the_toolkit_mandate_as_the_remedy(ns):
    for code in ("MANDATE_REQUIRED", "AUTHORIZATION_REQUIRED"):
        text = ns.adm.index_schedule_refusal_text(code)
        assert "index-schedule mandate for the schedule owner" in text and "Governance → Mandates" in text
    assert ns.adm.index_schedule_refusal_text("MANDATE_INACTIVE") == ns.adm.MESSAGES["MANDATE_INACTIVE"]
    assert ns.adm.MESSAGES["MANDATE_REQUIRED"] == "No approved mandate or delegation covers this run; it was not started."


def test_the_consent_scope_is_the_toolkits_delegated_credentials_and_the_owners_schedule_overrides(ns, env, monkeypatch):
    stored = owner_credential(env, personal=True, delegated=True)
    override = Credential(env, uuid.uuid4(), tool_id=9, config_id=109, grants=False)   # another configuration
    override.reference.update(auth_mode="entra_delegated", ownership_class="personal", custodian_user_id=OWNER)
    plain = Credential(env, uuid.uuid4(), tool_id=8, config_id=108, grants=False)   # a stored secret: not consented to
    with env.get_session(1) as session:
        session.execute(ns.sa.update(ns.EliteATool).where(ns.EliteATool.id == 1).values(name="obo-sharepoint", meta={
            "indexes_meta": {"idx": {"schedules": {
                str(OWNER): {"created_by": OWNER, "credentials": {"elitea_title": "o"}},     # the owner's: counted
                "77": {"created_by": 77, "credentials": {"elitea_title": "x"}},             # someone else's: not
            }}}}))
        session.commit()
    seen = []

    def settings_of(project_id, tool, schedule, key, owner_user_id):
        seen.append((key, owner_user_id))
        return {"credentials": override.expanded(), "other": plain.expanded()}

    monkeypatch.setitem(sys.modules, f"{PKG}.utils.index_scheduling", types.SimpleNamespace(
        schedule_credential_settings=settings_of))
    scope = ns.mandate_identity.toolkit_consent_scope(1, 1, OWNER)
    assert seen == [(str(OWNER), OWNER)]
    assert sorted(item["credential_ref"] for item in scope) == sorted([stored.ref_id, override.ref_id])
    assert all(item["toolkits"] == ["obo-sharepoint"] for item in scope)
    assert ns.mandate_identity.toolkit_consent_scope(1, 99, OWNER) == []   # no such toolkit


def test_the_list_names_the_toolkit_of_a_toolkit_mandate(ns, env):
    from fixtures.helpers import load_module_with_stubs  # pylint: disable=C0415
    models_all = sys.modules[f"{PKG}.models.all"]
    if not hasattr(models_all, "Application"):
        models_all.Application = type("Application", (ns.Base,), {
            "__tablename__": "applications", "__table_args__": {"schema": "tenant", "extend_existing": True},
            "id": ns.sa.Column(ns.sa.Integer, primary_key=True), "name": ns.sa.Column(ns.sa.String)})
        ns.Base.metadata.create_all(env.engine(1))
    queries = load_module_with_stubs(PLUGIN_ROOT / "utils" / "mandate_queries.py", f"{PKG}.utils.mandate_queries")
    queries.users_by_id = lambda ids: {uid: {"id": uid, "email": f"u{uid}@example.test"} for uid in ids}
    seed_toolkit(env)
    with env.get_session(1) as session:
        session.execute(ns.sa.update(ns.EliteATool).where(ns.EliteATool.id == 1).values(name="obo-sharepoint"))
        session.commit()
    mandate_id = add_toolkit_mandate(env, ns)
    page = queries.list_page(env.module(), 1)
    [row] = page["rows"]
    assert row["id"] == str(mandate_id) and row["toolkit"] == {"id": 1, "name": "obo-sharepoint"}
    assert row["version"] == {"id": None, "name": None} and row["operations"] == ["run:index_schedule"]


# --- the credential a schedule names, in a governed run ---------------------------------------------------------------

@pytest.fixture(scope="module")
def as_reference():
    """``as_credential_reference`` and ``resolve_credential_config_key`` lifted from ``utils/index_scheduling.py`` (the
    module itself needs the Pylon runtime)."""
    import ast  # pylint: disable=C0415
    tree = ast.parse((PLUGIN_ROOT / "utils" / "index_scheduling.py").read_text())
    wanted = {"as_credential_reference", "resolve_credential_config_key", "_NON_CREDENTIAL_CONFIG_KEYS"}
    nodes = [node for node in tree.body if getattr(node, "name", None) in wanted or (
        isinstance(node, ast.Assign) and any(getattr(t, "id", None) in wanted for t in node.targets))]
    space = {}
    exec(compile(ast.Module(nodes, []), "index_scheduling.py", "exec"), space)  # pylint: disable=W0122
    return space["as_credential_reference"]


def test_the_row_a_schedule_names_becomes_a_reference_to_it(as_reference):
    row = {"id": 5, "uuid": "8b1d2c3e-0000-4000-8000-000000000005", "project_id": 2, "elitea_title": "qtest",
           "type": "qtest", "section": "credentials", "data": {"qtest_api_token": "{{secret.made_up}}"}, "private": True}
    settings = {"qtest_configuration": row, "pgvector_configuration": {"elitea_title": "pgv", "private": True}}
    as_reference(settings, "qtest")
    assert settings["qtest_configuration"] == {
        "elitea_title": "qtest", "private": True, "configuration_uuid": "8b1d2c3e-0000-4000-8000-000000000005",
        "configuration_project_id": 2}
    assert settings["pgvector_configuration"] == {"elitea_title": "pgv", "private": True}   # untouched


def test_a_stored_reference_is_left_as_it_is(as_reference):
    stored = {"elitea_title": "qtest", "private": False, "configuration_uuid": "u-1"}
    settings = {"qtest_configuration": dict(stored)}
    as_reference(settings, "qtest")
    assert settings == {"qtest_configuration": stored}
    as_reference({}, "qtest")   # no slot: nothing to do


# --- fix round: a schedule's own credential (M2), the toolkit as approved (M3, C9-SEC-73), who writes schedules (M1) --

@pytest.fixture
def schedule_lookup(ns, monkeypatch, as_reference):
    """``utils.index_scheduling`` as admission imports it: the real ``as_credential_reference`` and a
    ``resolve_credentials`` that answers like the configurations lookup (the row found for the schedule's title)."""
    found, calls = {}, []

    def resolve_credentials(project_settings, toolkit_type, user_config, project_id, **context):
        calls.append((user_config.get("credentials"), context.get("creator_id")))
        if found.get("raise"):
            return False, "could not look up", True
        row = found.get((user_config.get("credentials") or {}).get("elitea_title"))
        if row is None:
            return False, "no longer exists", False
        project_settings[f"{toolkit_type}_configuration"] = dict(row)
        return True, None, False

    module = types.SimpleNamespace(resolve_credentials=resolve_credentials, as_credential_reference=as_reference)
    monkeypatch.setitem(sys.modules, f"{PKG}.utils.index_scheduling", module)
    return types.SimpleNamespace(found=found, calls=calls)


def set_schedules(env, ns, schedules, toolkit_id=1):
    with env.get_session(1) as session:
        session.execute(ns.sa.update(ns.EliteATool).where(ns.EliteATool.id == toolkit_id).values(
            meta={"indexes_meta": {"idx": {"schedules": schedules}}}))
        session.commit()


def two_credentials(env):
    """The toolkit (1) stored on a project credential the owner holds a grant on; the owner's personal credential of
    the same type (configuration 109), which a schedule can name instead."""
    stored = Credential(env, uuid.uuid4(), tool_id=1, config_id=101, field="jira_configuration", grants=False)
    env.grants[(1, stored.key(OWNER_REF))] = {"grant_id": stored.grant_id, "state": "active", "revocation_epoch": 4,
                                             "valid_from": None, "valid_until": None, "operations": ["*"]}
    mine = Credential(env, uuid.uuid4(), tool_id=9, config_id=109, field="jira_configuration", grants=False)
    mine.reference.update(ownership_class="personal", custodian_user_id=OWNER, custodian_subject_ref=OWNER_REF)
    return stored, mine


def tick_built_call(stored, credential):
    """The payload the index tick builds: the stored toolkit with ``credential`` applied and expanded."""
    tool = stored.tool()
    tool["settings"]["jira_configuration"] = credential.expanded()
    payload = {"toolkit_config": tool, "tool_name": "index_data", "tool_params": {"index_name": "idx"}}
    return call_kwargs(payload, toolkit_config=copy.deepcopy(tool))


def test_a_schedule_on_the_owners_own_credential_is_admitted_on_a_toolkit_stored_with_another(
        ns, env, schedule_lookup):
    stored, mine = two_credentials(env)
    set_schedules(env, ns, {str(OWNER): {"created_by": OWNER, "credentials": {"elitea_title": "mine"}}})
    schedule_lookup.found["mine"] = {"id": 109, "uuid": mine.uuid, "project_id": 1, "elitea_title": "mine",
                                     "type": "jira", "data": {"token": "{{secret.made_up}}"}, "private": False}
    mandate_id = add_toolkit_mandate(env, ns)
    call = tick_built_call(stored, mine)
    ref = ns.adm.admit_dispatch(env.module(), TOOLKIT_TASK, call, request(ns, mandate_id))
    [row] = contexts(env, ns)
    assert str(row.id) == ref and [entry["credential_ref"] for entry in row.allowed_bindings] == [mine.ref_id]
    assert schedule_lookup.calls == [({"elitea_title": "mine"}, OWNER)]   # read from the toolkit row, as its saver
    assert SECRET not in dumped(call["kwargs"])


def test_a_credential_the_schedule_does_not_name_is_still_refused(ns, env, schedule_lookup):
    stored, mine = two_credentials(env)
    set_schedules(env, ns, {str(OWNER): {"created_by": OWNER, "credentials": {"elitea_title": "something-else"}}})
    mandate_id = add_toolkit_mandate(env, ns)
    with pytest.raises(ns.exceptions.AdmissionDenied) as err:   # the lookup finds nothing: the stored one only
        ns.adm.admit_dispatch(env.module(), TOOLKIT_TASK, tick_built_call(stored, mine), request(ns, mandate_id))
    assert err.value.reason_code == "CREDENTIAL_REFERENCE_REQUIRED"
    set_schedules(env, ns, {"-1": {"created_by": OWNER}})   # a team schedule names none: the stored one only
    with pytest.raises(ns.exceptions.AdmissionDenied) as err:
        ns.adm.admit_dispatch(env.module(), TOOLKIT_TASK, tick_built_call(stored, mine),
                              request(ns, mandate_id, trigger_ref="index_schedule:1:idx:-1"))
    assert err.value.reason_code == "CREDENTIAL_REFERENCE_REQUIRED"
    assert ns.adm.admit_dispatch(env.module(), TOOLKIT_TASK, tick_built_call(stored, stored),
                                 request(ns, mandate_id, trigger_ref="index_schedule:1:idx:-1")) is not None


def test_a_schedule_lookup_outage_is_unavailable_and_a_foreign_trigger_is_no_mandate_run(ns, env, schedule_lookup):
    stored, mine = two_credentials(env)
    set_schedules(env, ns, {str(OWNER): {"created_by": OWNER, "credentials": {"elitea_title": "mine"}}})
    schedule_lookup.found["raise"] = True
    mandate_id = add_toolkit_mandate(env, ns)
    with pytest.raises(ns.exceptions.AdmissionDenied) as err:
        ns.adm.admit_dispatch(env.module(), TOOLKIT_TASK, tick_built_call(stored, mine), request(ns, mandate_id))
    assert err.value.reason_code == "DEPENDENCY_UNAVAILABLE"
    with pytest.raises(ns.exceptions.AdmissionDenied) as err:   # the reference names another toolkit
        ns.adm.admit_dispatch(env.module(), TOOLKIT_TASK, tick_built_call(stored, stored),
                              request(ns, mandate_id, trigger_ref="index_schedule:2:idx:5"))
    assert err.value.reason_code == "MANDATE_REQUIRED"


def test_a_toolkit_changed_after_approval_is_refused_readably(ns, env):
    cred = owner_credential(env)
    mandate_id = add_toolkit_mandate(env, ns)
    with env.get_session(1) as session:
        session.execute(ns.sa.update(ns.EliteATool).where(ns.EliteATool.id == 1).values(settings={
            **cred.tool()["settings"], "url": "https://elsewhere.example"}))
        session.commit()
    error = refused(ns, env, cred, mandate_id)
    assert error.reason_code == "TOOLKIT_CHANGED" and "approve a new revision" in error.message
    assert contexts(env, ns) == []
    never = add_toolkit_mandate(env, ns, toolkit_id=2, digest=None)   # approved before the digest was kept
    other = owner_credential(env, tool_id=2)
    assert refused(ns, env, other, never, toolkit_id=2).reason_code == "TOOLKIT_CHANGED"


def test_the_worker_holds_the_run_to_the_toolkit_the_mandate_approved(ns, env):
    cred = owner_credential(env)
    mandate_id = add_toolkit_mandate(env, ns)
    _call, ref = dispatch(ns, env, cred, mandate_id)
    with ns.sa_orm.Session(env.engine(1)) as session:   # re-approved meanwhile for another toolkit definition
        session.execute(ns.sa.update(ns.Mandate).where(ns.Mandate.id == mandate_id).values(toolkit_digest="adv1:other"))
        session.commit()
    answer = ns.adm.validate_context(env.module(), 1, ref, dict(TOOLKIT_CALLER))
    assert (answer["ok"], answer["reason_code"]) == (False, "TOOLKIT_CHANGED")
    assert "TOOLKIT_CHANGED" in ns.adm.REVOKE_CODES


def digest_of(env, ns, mandate_id):
    with ns.sa_orm.Session(env.engine(1)) as session:
        return session.execute(ns.sa.select(ns.Mandate.toolkit_digest).where(ns.Mandate.id == mandate_id)).scalar_one()


def test_approve_stamps_the_toolkit_and_resume_stamps_it_again(ns, env):
    seed_toolkit(env)
    mandate_id = run(ns, env, CREATE)["mandate_ref"]
    assert digest_of(env, ns, uuid.UUID(mandate_id)) is None
    run(ns, env, f"project_id=1;action=submit;mandate={mandate_id}")
    run(ns, env, f"project_id=1;action=approve;mandate={mandate_id};{APPROVE}", APPROVER)
    approved = digest_of(env, ns, uuid.UUID(mandate_id))
    assert approved == stored_digest(env, ns, 1)
    run(ns, env, f"project_id=1;action=suspend;mandate={mandate_id}")
    with env.get_session(1) as session:
        session.execute(ns.sa.update(ns.EliteATool).where(ns.EliteATool.id == 1).values(settings={"url": "https://x"}))
        session.commit()
    assert digest_of(env, ns, uuid.UUID(mandate_id)) == approved   # suspend leaves it
    assert run(ns, env, f"project_id=1;action=resume;mandate={mandate_id}", APPROVER)["state"] == "active"
    assert digest_of(env, ns, uuid.UUID(mandate_id)) == stored_digest(env, ns, 1) != approved


def test_whose_schedule_needs_saving_again(ns):
    mandates = {(1, OWNER): "mine"}
    assert ns.trigger_principal.resave_needed(mandates, 1, str(OWNER), None)
    assert ns.trigger_principal.resave_needed(mandates, 1, str(OWNER), 77)
    assert not ns.trigger_principal.resave_needed(mandates, 1, str(OWNER), OWNER)   # covered
    assert not ns.trigger_principal.resave_needed(mandates, 1, "77", None)          # no mandate of 77
    assert not ns.trigger_principal.resave_needed(mandates, 1, "-1", None)          # a team schedule


def test_a_toolkit_update_keeps_the_stored_schedules():
    from fixtures.helpers import load_module_with_stubs  # pylint: disable=C0415
    meta = load_module_with_stubs(PLUGIN_ROOT / "utils" / "toolkit_meta.py", f"{PKG}.utils.toolkit_meta")
    stored = {"indexes_meta": {"idx": {"schedules": {"5": {"created_by": 5}}}}, "shared": False}
    planted = {"indexes_meta": {"idx": {"schedules": {"5": {"created_by": 5, "cron": "* * * * *"}}}}, "mcp": True}
    assert meta.keep_stored_index_schedules(planted, stored) == {"mcp": True, "indexes_meta": stored["indexes_meta"]}
    assert meta.keep_stored_index_schedules(planted, {}) == {"mcp": True}   # none stored: none planted
    assert meta.keep_stored_index_schedules(None, None) == {}
    assert meta.keep_stored_index_schedules({"x": 1}, stored)["indexes_meta"] is stored["indexes_meta"]   # not dropped
    put = (PLUGIN_ROOT / "api" / "v2" / "tool.py").read_text()
    put = put[put.index("def put("):]
    locked = put.index(".with_for_update().first()")
    kept = put.index("update_data.meta = keep_stored_index_schedules(update_data.meta, current.meta")
    assert locked < kept < put.index("tool_query.update(")   # under the row lock, before the write
