"""C-1009 (#7119, PR 6): personal credential references in the credential-use decision.

A configuration can have a shared reference and one personal reference per owner. The decision reads the facts for the
caller (so it finds the caller's own personal reference first, else the shared one), and a personal reference is usable
only by its custodian: ``custodian_subject_ref`` equals the subject of the run, or, for a mandate, the mandate's owner.
Real models and statements on SQLite files (see ``fixtures/admission_env.py``); the facts methods are keyed-data fakes.
"""
import copy
import json
import types
import re
import sys
import uuid
from datetime import timedelta
from pathlib import Path

import pytest

PLUGIN_ROOT = Path(__file__).parents[2]
sys.path.insert(0, str(PLUGIN_ROOT / "tests"))

from fixtures import admission_env  # noqa: E402  pylint: disable=C0413

RUN = "run-1"
SUBJECT = "idp:tid-1:oid-42"
OTHER_SUBJECT = "idp:tid-1:oid-77"
CALLER_USER = 42
TITLE = "jira-main-credential"
CONFIG_ID = 5


@pytest.fixture(scope="module")
def ns(tmp_path_factory):
    saved = dict(sys.modules)
    yield admission_env.install(PLUGIN_ROOT, tmp_path_factory.mktemp("personal_decision"))
    sys.modules.clear()
    sys.modules.update(saved)


@pytest.fixture
def env(ns):
    ns.env.reset()
    ns.env.__dict__["caller_calls"] = []
    return ns.env


class Setup:
    """One toolkit with one credential configuration, and the references that exist for it."""

    def __init__(self, env):
        self.env = env
        self.binding = env.seed_agent()
        self.uuid = str(uuid.uuid4())
        env.add_toolkit(1, 70, 1, "jira", {"url": "https://jira.example", "credentials": {
            "elitea_title": TITLE, "private": False, "configuration_uuid": self.uuid, "configuration_project_id": 1}})
        env.configurations[(1, self.uuid)] = {
            "id": CONFIG_ID, "project_id": 1, "type": "jira", "section": "credentials", "generation": 3}
        self.refs = {}

    def reference(self, name, *, ownership_class="project_shared", custodian_subject_ref=None, custodian_user_id=None,
                  state="active", grant_subject=SUBJECT, grant=True):
        ref_id = str(uuid.uuid4())
        self.env.references.append({
            "project_id": 1, "configuration_id": CONFIG_ID, "ref_id": ref_id, "state": state, "generation": 1,
            "configuration_generation": 3, "store_kind": "database", "ownership_class": ownership_class,
            "revocation_epoch": 2, "custodian_user_id": custodian_user_id,
            "custodian_subject_ref": custodian_subject_ref})
        if grant:
            self.env.grants[(1, "|".join((str(self.binding), "1", ref_id, "*", grant_subject)))] = {
                "grant_id": str(uuid.uuid4()), "state": "active", "revocation_epoch": 4, "valid_from": None,
                "valid_until": None, "operations": ["*"]}
        self.refs[name] = ref_id
        return ref_id

    @property
    def payload(self):
        expanded = {"elitea_title": TITLE, "configuration_uuid": self.uuid, "configuration_project_id": 1,
                    "configuration_type": "jira", "configuration_generation": 3, "token": "plain-token-value"}
        return {"application": {"version_details": {"tools": [
            {"id": 1, "type": "jira", "settings": {"url": "https://jira.example", "credentials": expanded}}]}}}


@pytest.fixture
def cfg(env):
    return Setup(env)


def admit(ns, env, cfg, **request):
    return ns.adm.admit_execution(env.module(), env.request(**request), run_id=RUN, payload=cfg.payload)


def refusal(ns, env, cfg, **request):
    with pytest.raises(ns.exceptions.AdmissionDenied) as err:
        admit(ns, env, cfg, **request)
    return err.value


def allowed_ref(ns, env):
    [context] = env.query(1, ns.models.ExecutionSecurityContext)
    [binding] = context.allowed_bindings
    return binding["credential_ref"]


# --- the decision matrix ----------------------------------------------------------------------------------------

def test_the_callers_own_personal_credential_is_allowed_and_is_the_one_recorded(ns, env, cfg):
    cfg.reference("shared")
    mine = cfg.reference("mine", ownership_class="personal", custodian_subject_ref=SUBJECT, custodian_user_id=CALLER_USER)
    cfg.reference("theirs", ownership_class="personal", custodian_subject_ref=OTHER_SUBJECT, custodian_user_id=77,
                  grant_subject=OTHER_SUBJECT)
    admit(ns, env, cfg)
    assert allowed_ref(ns, env) == mine
    assert env.caller_calls[0] == (SUBJECT, None)   # the facts were read for this caller


def test_another_users_personal_credential_is_refused_by_class(ns, env, cfg):
    cfg.reference("theirs", ownership_class="personal", custodian_subject_ref=OTHER_SUBJECT, custodian_user_id=77,
                  grant_subject=SUBJECT)   # even a grant to the caller does not make it theirs
    error = refusal(ns, env, cfg)
    assert error.reason_code == "CREDENTIAL_CLASS_NOT_ALLOWED"
    assert error.message == ns.adm.MESSAGES["CREDENTIAL_CLASS_NOT_ALLOWED"]
    assert env.query(1, ns.models.ExecutionSecurityContext) == []


def test_a_personal_credential_that_names_no_custodian_is_nobodys(ns, env, cfg):
    cfg.reference("orphan", ownership_class="personal")
    assert refusal(ns, env, cfg).reason_code == "CREDENTIAL_CLASS_NOT_ALLOWED"


def test_the_shared_credential_is_used_when_the_caller_has_none_of_their_own(ns, env, cfg):
    shared = cfg.reference("shared")
    cfg.reference("theirs", ownership_class="personal", custodian_subject_ref=OTHER_SUBJECT, custodian_user_id=77,
                  grant_subject=OTHER_SUBJECT)
    admit(ns, env, cfg)
    assert allowed_ref(ns, env) == shared


def test_a_personal_credential_without_a_grant_is_not_replaced_by_the_shared_one(ns, env, cfg):
    cfg.reference("shared")
    cfg.reference("mine", ownership_class="personal", custodian_subject_ref=SUBJECT, custodian_user_id=CALLER_USER,
                  grant=False)
    assert refusal(ns, env, cfg).reason_code == "DENIED_GRANT"


def test_a_personal_credential_that_is_not_active_is_not_replaced_by_the_shared_one(ns, env, cfg):
    cfg.reference("shared")
    cfg.reference("mine", ownership_class="personal", custodian_subject_ref=SUBJECT, custodian_user_id=CALLER_USER,
                  state="suspended")
    assert refusal(ns, env, cfg).reason_code == "CREDENTIAL_REFERENCE_INACTIVE"


def test_the_grant_that_counts_is_the_one_on_the_chosen_reference(ns, env, cfg):
    cfg.reference("shared", grant=False)   # the shared reference has no grant; the personal one has
    mine = cfg.reference("mine", ownership_class="personal", custodian_subject_ref=SUBJECT, custodian_user_id=CALLER_USER)
    admit(ns, env, cfg)
    assert allowed_ref(ns, env) == mine
    [(_, _, keys, _)] = [call for call in env.secrets_calls if call[2]]
    assert [key.split("|")[2] for key in ("|".join(item) for item in keys)] == [mine]


def test_a_shared_credential_is_unaffected(ns, env, cfg):
    shared = cfg.reference("shared", ownership_class="service")
    admit(ns, env, cfg)
    assert allowed_ref(ns, env) == shared


# --- the pure rule ----------------------------------------------------------------------------------------------

def pure(ns, reference, **ctx_over):
    cd = ns.credential_decision
    now = ns.adm._now()   # pylint: disable=W0212
    ctx = cd.CredentialDecisionContext(
        binding_project_id=1, agent_binding_ref="b" * 8, subject_ref=ctx_over.pop("subject_ref", SUBJECT),
        policy_revision="rev-1", now=now, expires_at=now + timedelta(hours=1), can_use=lambda: True, **ctx_over)
    base = {"ref_id": "r-1", "state": "active", "generation": 1, "configuration_generation": 3,
            "store_kind": "database", "ownership_class": "project_shared", "revocation_epoch": 2,
            "custodian_user_id": None}
    use = cd.CredentialUse("1", "credentials", "u-1", "uuid", 1)
    grant = {"grant_id": "g-1", "state": "active", "subject_kind": ctx_over.get("subject_kind", "user_delegation"),
             "revocation_epoch": 4, "valid_from": None, "valid_until": None}
    facts = cd.CredentialFacts(
        {"u-1": {"id": 5, "project_id": 1, "section": "credentials", "generation": 3}}, {5: {**base, **reference}},
        {cd.grant_key("b" * 8, "1", "r-1", ctx.subject_ref): grant})
    return cd.authorize_credential_use(ctx, use, facts)


@pytest.mark.parametrize("reference, ctx, allow", [
    # a user delegation: the custodian's subject is the run's
    ({"ownership_class": "personal", "custodian_subject_ref": SUBJECT}, {}, True),
    ({"ownership_class": "personal", "custodian_subject_ref": OTHER_SUBJECT}, {}, False),
    ({"ownership_class": "personal", "custodian_subject_ref": None}, {}, False),
    ({"ownership_class": "personal"}, {}, False),
    ({"ownership_class": "personal", "custodian_subject_ref": ""}, {}, False),
    ({"ownership_class": "personal", "custodian_subject_ref": 42}, {}, False),
    # the user id alone is not the subject
    ({"ownership_class": "personal", "custodian_user_id": CALLER_USER}, {}, False),
    # a mandate acts for its owner
    ({"ownership_class": "personal", "custodian_user_id": 6},
     {"subject_kind": "mandate", "subject_user_id": 6, "subject_ref": "mandate:m-1"}, True),
    ({"ownership_class": "personal", "custodian_user_id": 7},
     {"subject_kind": "mandate", "subject_user_id": 6, "subject_ref": "mandate:m-1"}, False),
    ({"ownership_class": "personal", "custodian_user_id": None},
     {"subject_kind": "mandate", "subject_user_id": None, "subject_ref": "mandate:m-1"}, False),
    ({"ownership_class": "personal", "custodian_subject_ref": "mandate:m-1"},
     {"subject_kind": "mandate", "subject_user_id": 6, "subject_ref": "mandate:m-1"}, False),
    # every other class does not look at a custodian
    ({"ownership_class": "service", "custodian_subject_ref": OTHER_SUBJECT}, {}, True),
])
def test_a_personal_reference_is_usable_only_by_its_custodian(ns, reference, ctx, allow):
    decision = pure(ns, reference, **ctx)
    assert decision.allow is allow
    assert decision.reason_code == ("CREDENTIAL_USE_ALLOWED" if allow else "CREDENTIAL_CLASS_NOT_ALLOWED")


def test_the_class_check_comes_before_the_grant(ns):
    cd = ns.credential_decision
    now = ns.adm._now()   # pylint: disable=W0212
    ctx = cd.CredentialDecisionContext(1, "b" * 8, SUBJECT, "rev-1", now, now + timedelta(hours=1), lambda: True)
    reference = {"ref_id": "r-1", "state": "active", "generation": 1, "configuration_generation": 3,
                 "store_kind": "database", "ownership_class": "personal", "revocation_epoch": 2,
                 "custodian_subject_ref": OTHER_SUBJECT}
    facts = cd.CredentialFacts({"u-1": {"id": 5, "project_id": 1, "section": "credentials", "generation": 3}},
                               {5: reference}, {})
    decision = cd.authorize_credential_use(ctx, cd.CredentialUse("1", "credentials", "u-1", "uuid", 1), facts)
    assert decision.reason_code == "CREDENTIAL_CLASS_NOT_ALLOWED"


# --- the caller the facts are read for ----------------------------------------------------------------------------

def test_a_mandate_reads_the_facts_for_its_owner(ns):
    cd = ns.credential_decision
    now = ns.adm._now()   # pylint: disable=W0212
    mandate = cd.CredentialDecisionContext(1, "b", "mandate:m-1", "r", now, now, lambda: True,
                                           subject_kind="mandate", subject_user_id=6)
    assert cd._caller_of(mandate) == {"personal_user_id": 6}   # pylint: disable=W0212
    ownerless = cd.CredentialDecisionContext(1, "b", "mandate:m-1", "r", now, now, lambda: True, subject_kind="mandate")
    assert cd._caller_of(ownerless) == {}   # pylint: disable=W0212
    person = cd.CredentialDecisionContext(1, "b", SUBJECT, "r", now, now, lambda: True)
    assert cd._caller_of(person) == {"personal_subject_ref": SUBJECT}   # pylint: disable=W0212


# --- the re-check of a queued run ---------------------------------------------------------------------------------

def entry(cfg, ref_id):
    return {"toolkit_instance_id": "1", "field": "credentials", "configuration_uuid": cfg.uuid, "credential_ref": ref_id,
            "credential_generation": 1, "reference_epoch": 2, "grant_id": "g", "grant_epoch": 4,
            "configuration_generation": 3, "decision_id": "d"}


def recheck(ns, env, cfg, ref_id, subject=SUBJECT):
    return ns.credential_decision.recheck_credentials(
        1, [entry(cfg, ref_id)], subject, str(cfg.binding), ns.adm._now())   # pylint: disable=W0212


def test_the_recheck_of_a_personal_reference_holds_for_its_custodian_only(ns, env, cfg):
    mine = cfg.reference("mine", ownership_class="personal", custodian_subject_ref=SUBJECT, custodian_user_id=CALLER_USER)
    assert recheck(ns, env, cfg, mine) is None
    theirs = cfg.reference("theirs", ownership_class="personal", custodian_subject_ref=OTHER_SUBJECT, custodian_user_id=77)
    assert recheck(ns, env, cfg, theirs) == "CREDENTIAL_CLASS_NOT_ALLOWED"
    shared = cfg.reference("shared")
    assert recheck(ns, env, cfg, shared) is None


def test_the_recheck_of_a_mandate_run_leaves_the_owner_rule_to_admission(ns, env, cfg):
    mandate = f"mandate:{uuid.uuid4()}"
    ref = cfg.reference("mine", ownership_class="personal", custodian_user_id=6, grant_subject=mandate)
    env.grants[(1, "|".join((str(cfg.binding), "1", ref, "*", mandate)))]["subject_kind"] = "mandate"
    assert recheck(ns, env, cfg, ref, subject=mandate) is None


# --- messages -----------------------------------------------------------------------------------------------------

def test_the_class_message_is_readable_and_names_nothing(ns):
    message = ns.adm.MESSAGES["CREDENTIAL_CLASS_NOT_ALLOWED"]
    assert message.endswith(".") and not re.search(r"\d", message) and "uuid" not in message.lower()
    assert "someone else" in message


# --- the governed test of a saved credential ------------------------------------------------------------------------

def test_a_probe_tests_the_testers_own_personal_reference_when_there_is_one(ns, env):
    from fixtures.governed_credentials import seeded   # pylint: disable=C0415
    cred = seeded(env)
    env.permissions[CALLER_USER] = {"credentials.probe", "credentials.use"}
    mine = str(uuid.uuid4())
    env.references.append({
        "project_id": 1, "configuration_id": cred.config_id, "ref_id": mine, "state": "active", "generation": 1,
        "configuration_generation": 3, "store_kind": "database", "ownership_class": "personal", "revocation_epoch": 2,
        "custodian_user_id": CALLER_USER, "custodian_subject_ref": SUBJECT})
    env.material[mine] = {"token": "personal-token"}
    ns.credential_probe._stored_configuration = lambda *a: copy.deepcopy({   # pylint: disable=W0212
        "configuration_uuid": cred.uuid, "configuration_project_id": 1, "configuration_type": "jira",
        "configuration_generation": 3, "elitea_title": TITLE, "token": "{{secret.jira_token}}"})
    assert ns.credential_probe.governed_check_connection(env.module(), 1, cred.uuid, "jira", CALLER_USER) == {"success": True}
    [(_, kwargs, _, _)] = env.started
    assert kwargs["settings"]["credential_slot"]["credential_ref"] == mine
    assert env.caller_calls[-1] == (None, CALLER_USER)


# --- the operator tests a personal reference for its custodian ------------------------------------------------------

OPERATOR = 9


def personal_probe(ns, env, personal_state="active"):
    from fixtures.governed_credentials import seeded   # pylint: disable=C0415
    cred = seeded(env)
    mine = str(uuid.uuid4())
    env.references.append({
        "project_id": 1, "configuration_id": cred.config_id, "ref_id": mine, "state": personal_state,
        "generation": 1, "configuration_generation": 3, "store_kind": "database", "ownership_class": "personal",
        "revocation_epoch": 2, "custodian_user_id": CALLER_USER, "custodian_subject_ref": SUBJECT})
    env.material[mine] = {"token": "personal-token"}
    ns.credential_probe._stored_configuration = lambda *a: (setattr(env, "stored_read", a) or copy.deepcopy({   # pylint: disable=W0212
        "configuration_uuid": cred.uuid, "configuration_project_id": 1, "configuration_type": "jira",
        "configuration_generation": 3, "elitea_title": TITLE, "token": "{{secret.jira_token}}"}))
    return cred, mine


def run_probe(ns, env, cred, **kwargs):
    return ns.credential_probe.governed_check_connection(env.module(), 1, cred.uuid, "jira", CALLER_USER, **kwargs)


def test_the_operators_permission_is_checked_not_the_custodians(ns, env):
    cred, mine = personal_probe(ns, env)
    env.permissions[OPERATOR] = {"credentials.probe"}
    env.permissions[CALLER_USER] = {"credentials.use"}   # an editor-owner: may not probe
    env.permission_calls.clear()
    assert run_probe(ns, env, cred, actor_user_id=OPERATOR) == {"success": True}
    assert [call[0] for call in env.permission_calls] == [OPERATOR]
    [(_, kwargs, _, _)] = env.started
    assert kwargs["settings"]["credential_slot"]["credential_ref"] == mine   # the custodian's reference was probed
    assert env.stored_read[2] == OPERATOR


def test_an_actor_without_credentials_probe_is_refused_whatever_the_custodian_holds(ns, env):
    cred, _ = personal_probe(ns, env)
    env.permissions[OPERATOR] = {"credentials.use"}
    env.permissions[CALLER_USER] = {"credentials.probe"}
    answer = run_probe(ns, env, cred, actor_user_id=OPERATOR)
    assert answer["success"] is False and answer["code"] == "DENIED_RBAC" and env.started == []
    [event] = env.denied
    assert (event["caller_ref"], event["user_ref"], event["reason_code"]) == (
        f"user:{OPERATOR}", f"user:{CALLER_USER}", "DENIED_RBAC")


def test_without_an_actor_the_tester_is_the_actor_as_before(ns, env):
    cred, _ = personal_probe(ns, env)
    env.permissions[CALLER_USER] = {"credentials.probe"}
    env.permission_calls.clear()
    assert run_probe(ns, env, cred) == {"success": True}
    assert [call[0] for call in env.permission_calls] == [CALLER_USER]


def test_an_operator_asking_for_a_user_without_a_personal_reference_probes_the_shared_one(ns, env):
    cred, mine = personal_probe(ns, env)
    env.permissions[OPERATOR] = {"credentials.probe"}
    answer = ns.credential_probe.governed_check_connection(env.module(), 1, cred.uuid, "jira", 77, actor_user_id=OPERATOR)
    assert answer == {"success": True}
    [(_, kwargs, _, _)] = env.started
    assert kwargs["settings"]["credential_slot"]["credential_ref"] == cred.ref_id != mine   # never someone else's


@pytest.mark.parametrize("actor", [True, "9", 1.5])
def test_an_actor_that_is_not_an_integer_is_refused(ns, env, actor):
    cred, _ = personal_probe(ns, env)
    assert run_probe(ns, env, cred, actor_user_id=actor)["code"] == "CREDENTIAL_REFERENCE_REQUIRED"


# --- the onboarding probe of a value that is still pending ---------------------------------------------------------

def test_a_pending_value_is_probed_only_for_the_onboarding(ns, env):
    cred, mine = personal_probe(ns, env, "pending")
    env.permissions[OPERATOR] = {"credentials.probe"}
    answer = run_probe(ns, env, cred, actor_user_id=OPERATOR)
    assert answer["code"] == "CREDENTIAL_REFERENCE_INACTIVE" and env.started == []
    env.denied.clear()
    assert run_probe(ns, env, cred, actor_user_id=OPERATOR, onboarding=True) == {"success": True}
    [ticket] = [value for key, value in env.at_start[0].items() if key.startswith(ns.credential_release.PROBE_KEY)]
    assert json.loads(ticket)["onboarding"] is True


def test_a_pending_reference_that_holds_no_value_is_never_probed(ns, env):
    cred, mine = personal_probe(ns, env, "pending")
    next(row for row in env.references if row["ref_id"] == mine)["generation"] = 0
    env.permissions[OPERATOR] = {"credentials.probe"}
    assert run_probe(ns, env, cred, actor_user_id=OPERATOR, onboarding=True)["code"] == "CREDENTIAL_REFERENCE_INACTIVE"


def test_an_ordinary_probe_ticket_does_not_open_a_pending_reference_to_the_release(ns, env):
    cred, mine = personal_probe(ns, env)
    env.permissions[OPERATOR] = {"credentials.probe"}
    run_probe(ns, env, cred, actor_user_id=OPERATOR)
    [ticket] = [value for key, value in env.at_start[0].items() if key.startswith(ns.credential_release.PROBE_KEY)]
    assert "onboarding" not in json.loads(ticket)


def test_the_release_scope_of_a_probe_follows_the_tickets_onboarding_flag(ns, env):
    release = ns.credential_release
    for flag, expected in ((True, True), (None, False), ("yes", False)):
        probe_ref = str(uuid.uuid4())
        ticket = {"project_id": 1, "credential_ref": "r-1", "credential_generation": 1,
                  "task_name": release.PROBE_TASK, "user_id": OPERATOR}
        if flag is not None:
            ticket["onboarding"] = flag
        env.redis.values[release.PROBE_KEY + probe_ref] = json.dumps(ticket)
        request = types.SimpleNamespace(
            slot={"credential_ref": "r-1", "credential_generation": 1}, toolkit_instance_id="probe", field="configuration",
            task_name=release.PROBE_TASK, caller={"run_id": None}, probe_ref=probe_ref, project_id=1)
        request.known = {"caller_ref": "indexer_worker:x"}
        scope = release._check_probe(env.module(), request)   # pylint: disable=W0212
        assert (scope.allow_validated, scope.allow_pending) == (True, expected)
