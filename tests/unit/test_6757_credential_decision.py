"""C-1009 (#6757): the credential-use decision in governed admission, its re-check for a queued run, the
inherited-dispatch refusal and the revocation of the runs that hold a credential.

Real models and statements on SQLite files (see ``fixtures/admission_env.py``); the two facts methods of
``configurations`` and ``secrets`` are keyed-data fakes that record their calls.
"""
import copy
import itertools
import json
import re
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

PLUGIN_ROOT = Path(__file__).parents[2]
sys.path.insert(0, str(PLUGIN_ROOT / "tests"))

from fixtures import admission_env  # noqa: E402  pylint: disable=C0413

RUN = "run-1"
SUBJECT = "idp:tid-1:oid-42"
OTHER_SUBJECT = "idp:tid-1:oid-77"
TITLE = "jira-main-credential"
CALLER = {"kind": "indexer_worker", "task_name": "indexer_agent", "run_id": RUN}
UUID_PATTERN = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-")
ALLOWED = "CREDENTIAL_USE_ALLOWED"


@pytest.fixture(scope="module")
def ns(tmp_path_factory):
    saved = dict(sys.modules)
    yield admission_env.install(PLUGIN_ROOT, tmp_path_factory.mktemp("credential_decision"))
    sys.modules.clear()
    sys.modules.update(saved)


@pytest.fixture
def env(ns):
    ns.env.reset()
    return ns.env


class Credential:
    """One governed credential of a toolkit: its rows, the facts that describe them and the payload that carries it."""

    def __init__(self, env, binding, *, tool_id=1, field="credentials", config_id=5, project_id=1, section="credentials",
                 selector="uuid", ref_state="active", store_kind="database", ownership_class="project_shared",
                 config_generation=3, approved_generation=3, grant_state="active", grant_subject=SUBJECT, grants=True,
                 reference=True, cfg_uuid=None, version_id=70):
        self.env, self.binding, self.tool_id, self.field, self.config_id = env, binding, tool_id, field, config_id
        self.uuid, self.ref_id, self.grant_id = cfg_uuid or str(uuid.uuid4()), str(uuid.uuid4()), str(uuid.uuid4())
        self.project_id = project_id
        stored = {"elitea_title": TITLE, "private": False}
        if selector == "uuid":
            stored.update(configuration_uuid=self.uuid, configuration_project_id=project_id)
        env.add_toolkit(1, version_id, tool_id, "jira", {"url": "https://jira.example", field: stored})
        env.configurations[(project_id, self.uuid)] = {
            "id": config_id, "project_id": project_id, "type": "jira", "section": section, "generation": config_generation}
        if reference:
            env.references.append({
                "project_id": 1, "configuration_id": config_id, "ref_id": self.ref_id, "state": ref_state, "generation": 1,
                "configuration_generation": approved_generation, "store_kind": store_kind,
                "ownership_class": ownership_class, "revocation_epoch": 2})
        if grants:
            env.grants[(1, self.key(grant_subject))] = {
                "grant_id": self.grant_id, "state": grant_state, "revocation_epoch": 4, "valid_from": None,
                "valid_until": None, "operations": ["*"]}

    def key(self, subject=SUBJECT):
        return "|".join((str(self.binding), str(self.tool_id), self.ref_id, "*", subject))

    @property
    def grant(self):
        return self.env.grants[(1, self.key())]

    @property
    def reference(self):
        return next(row for row in self.env.references if row["ref_id"] == self.ref_id)

    def expanded(self):
        """What expansion leaves in the dispatched settings: the dict with the configuration merged in."""
        return {"elitea_title": TITLE, "configuration_uuid": self.uuid, "configuration_project_id": self.project_id,
                "configuration_type": "jira", "configuration_generation": 3, "token": "plain-token-value"}

    def tool(self):
        return {"id": self.tool_id, "type": "jira", "settings": {"url": "https://jira.example", self.field: self.expanded()}}


def payload_of(*credentials, **extra):
    payload = {"application": {"version_details": {"tools": [credential.tool() for credential in credentials]}}}
    payload.update(extra)
    return payload


def seeded(env, **over):
    binding = env.seed_agent()
    return Credential(env, binding, **over)


def admit(env, ns, payload, module=None, **request):
    return ns.adm.admit_execution(module or env.module(), env.request(**request), run_id=RUN, payload=payload)


def refusal(env, ns, payload, **kwargs):
    with pytest.raises(ns.exceptions.AdmissionDenied) as err:
        admit(env, ns, payload, **kwargs)
    return err.value


def contexts(env, ns):
    return env.query(1, ns.models.ExecutionSecurityContext)


# --- the happy path and what it records --------------------------------------------------------

def test_an_allowed_use_is_recorded_on_the_context_and_in_one_decision_event(ns, env):
    cred = seeded(env)
    view = admit(env, ns, payload_of(cred))
    [row] = contexts(env, ns)
    [entry] = row.allowed_bindings
    assert entry == {
        "toolkit_instance_id": "1", "field": "credentials", "configuration_uuid": cred.uuid, "credential_ref": cred.ref_id,
        "credential_generation": 1, "reference_epoch": 2, "grant_id": cred.grant_id, "grant_epoch": 4,
        "configuration_generation": 3, "decision_id": entry["decision_id"]}
    assert row.revocation_epochs == {"user": 3, "token": 0, "binding": 0, "project": None, "credential": {cred.ref_id: 2}}
    [(decision_session, decision), (admission_session, admission)] = env.recorded
    assert decision == {
        "event_type": "decision", "decision": "allow", "reason_code": ALLOWED, "event_id": entry["decision_id"],
        "tenant": "tid-1", "project_id": 1, "application_id": 7, "caller_ref": "user:42", "user_ref": SUBJECT,
        "agent_binding_ref": str(cred.binding), "run_id": RUN, "attempt": 1, "credential_ref": cred.ref_id,
        "credential_generation": 1, "toolkit_instance_id": "1", "target_ref": "*", "policy_revision": "rev-1",
        "revocation_epoch": 2}
    assert admission["event_type"] == "admission" and decision_session is admission_session
    assert str(row.id) == view.context_ref and env.denied == []


def test_the_decision_events_and_the_context_row_are_one_transaction_with_one_commit(ns, env):
    cred_a = seeded(env)
    cred_b = Credential(env, cred_a.binding, tool_id=2, config_id=6)
    admit(env, ns, payload_of(cred_a, cred_b))
    sessions = {id(session) for session, _ in env.recorded}
    assert len(env.recorded) == 3 and len(sessions) == 1
    [(_, writer)] = [entry for entry in env.sessions if id(entry[1]) in sessions]
    assert writer.commits == 1
    [row] = contexts(env, ns)
    assert [entry["toolkit_instance_id"] for entry in row.allowed_bindings] == ["1", "2"]
    assert {event["event_id"] for _, event in env.recorded[:2]} == {entry["decision_id"] for entry in row.allowed_bindings}


def test_a_failing_decision_event_leaves_no_row_and_a_dependency_refusal(ns, env):
    cred = seeded(env)
    env.record_raises = RuntimeError("outbox full")
    assert refusal(env, ns, payload_of(cred)).reason_code == "DEPENDENCY_UNAVAILABLE"
    assert contexts(env, ns) == [] and env.recorded == []
    [event] = env.denied
    assert (event["decision"], event["reason_code"]) == ("deny", "DEPENDENCY_UNAVAILABLE")


def test_a_failing_commit_leaves_no_row_either(ns, env):
    cred = seeded(env)
    env.fail_commit_once = RuntimeError("io")
    assert refusal(env, ns, payload_of(cred)).reason_code == "DEPENDENCY_UNAVAILABLE"
    assert contexts(env, ns) == []


def test_allowed_uses_are_counted_once_each_and_the_agent_without_credentials_is_not_counted(ns, env):
    cred = seeded(env)
    before = ns.adm.CREDENTIAL_DECISION.total("allow", ALLOWED)
    admit(env, ns, payload_of(cred))
    assert ns.adm.CREDENTIAL_DECISION.total("allow", ALLOWED) == before + 1
    assert f"security_metric name=credential_decision_total decision=allow reason_code={ALLOWED}" in env.log.lines
    env.reset()
    env.seed_agent()
    admit(env, ns, {})
    assert not [line for line in env.log.lines if "credential_decision_total" in line]


def test_an_agent_without_governed_credentials_calls_nothing_outside_admission(ns, env):
    env.seed_agent()
    env.add_toolkit(1, 70, 1, "github", {"url": "https://x", "note": "plain"})
    admit(env, ns, payload_of())
    assert env.configuration_calls == [] and env.secrets_calls == [] and env.permission_calls == []
    [row] = contexts(env, ns)
    assert row.allowed_bindings == [] and row.revocation_epochs["credential"] is None


def test_the_limit_of_governed_credentials_per_run_passes_and_one_more_is_refused(ns, env):
    binding = env.seed_agent()
    limit = ns.credential_decision.MAX_GOVERNED_CREDENTIALS_PER_RUN
    creds = [Credential(env, binding, tool_id=index + 1, config_id=100 + index) for index in range(limit)]
    admit(env, ns, payload_of(*creds))
    assert len(contexts(env, ns)[0].allowed_bindings) == limit
    # the facts methods were called within their own bound
    assert all(len(call[1]) <= limit and len(call[2]) <= limit for call in env.secrets_calls)
    env.reset()
    binding = env.seed_agent()
    creds = [Credential(env, binding, tool_id=index + 1, config_id=100 + index) for index in range(limit + 1)]
    assert refusal(env, ns, payload_of(*creds)).reason_code == "CREDENTIAL_REFERENCE_REQUIRED"
    assert contexts(env, ns) == []


# --- the decision matrix -----------------------------------------------------------------------

def expected_code(ref_active, grant_active, rbac, subject_match):
    if not ref_active:
        return "CREDENTIAL_REFERENCE_INACTIVE"
    if not rbac:
        return "DENIED_RBAC"
    if not (grant_active and subject_match):
        return "DENIED_GRANT"
    return None


@pytest.mark.parametrize("ref_active, grant_active, rbac, subject_match", list(itertools.product([True, False], repeat=4)))
def test_sixteen_cell_matrix_only_the_all_true_cell_is_allowed(ns, env, ref_active, grant_active, rbac, subject_match):
    cred = seeded(env, ref_state="active" if ref_active else "suspended",
                  grant_state="active" if grant_active else "suspended",
                  grant_subject=SUBJECT if subject_match else OTHER_SUBJECT)
    env.permissions[42] = {"credentials.use"} if rbac else {"projects.read"}
    code = expected_code(ref_active, grant_active, rbac, subject_match)
    if code is None:
        admit(env, ns, payload_of(cred))
        assert len(contexts(env, ns)) == 1
        return
    error = refusal(env, ns, payload_of(cred))
    assert (error.reason_code, error.message) == (code, ns.adm.MESSAGES[code])
    assert contexts(env, ns) == []
    [event] = env.denied
    assert (event["event_type"], event["decision"], event["reason_code"]) == ("admission", "deny", code)
    assert ns.adm.CREDENTIAL_DECISION.total("deny", code) >= 1


def test_a_denied_use_audits_the_references_it_knows_and_never_puts_them_in_the_message(ns, env):
    cred = seeded(env, ref_state="suspended")
    error = refusal(env, ns, payload_of(cred))
    [event] = env.denied
    assert (event["credential_ref"], event["credential_generation"], event["toolkit_instance_id"]) == (cred.ref_id, 1, "1")
    text = "\n".join([error.message, str(error), *env.log.lines])
    for secret in (cred.ref_id, cred.uuid, TITLE, cred.grant_id, "plain-token-value"):
        assert secret not in text


STEP_FAILURES = {
    "title only": (dict(selector="title"), "CREDENTIAL_REFERENCE_REQUIRED"),
    "no reference": (dict(reference=False), "CREDENTIAL_REFERENCE_REQUIRED"),
    "reference pending": (dict(ref_state="pending"), "CREDENTIAL_REFERENCE_INACTIVE"),
    "reference retired": (dict(ref_state="retired"), "CREDENTIAL_REFERENCE_REQUIRED"),
    "store kind refused": (dict(store_kind="managed_vault"), "CREDENTIAL_REFERENCE_INACTIVE"),
    "personal class": (dict(ownership_class="personal"), "CREDENTIAL_CLASS_NOT_ALLOWED"),
    "generation moved": (dict(config_generation=4), "CREDENTIAL_GENERATION_MISMATCH"),
    "never imported generation": (dict(approved_generation=None), "CREDENTIAL_GENERATION_MISMATCH"),
    "cross project": (dict(project_id=9), "DENIED_GRANT"),
    "no grant": (dict(grants=False), "DENIED_GRANT"),
    "grant pending": (dict(grant_state="pending_approval"), "DENIED_GRANT"),
}


@pytest.mark.parametrize("over, code", STEP_FAILURES.values(), ids=STEP_FAILURES.keys())
def test_each_remaining_step_failing_alone_refuses_with_its_code(ns, env, over, code):
    cred = seeded(env, **over)
    assert refusal(env, ns, payload_of(cred)).reason_code == code
    assert contexts(env, ns) == []


def test_a_title_only_selection_is_refused_before_any_fact_is_read(ns, env):
    cred = seeded(env, selector="title")
    assert refusal(env, ns, payload_of(cred)).reason_code == "CREDENTIAL_REFERENCE_REQUIRED"
    assert env.secrets_calls == [] and env.permission_calls == []
    # the configuration is looked up only to learn its section
    assert env.configuration_calls == [(1, [cred.uuid])]


def test_a_configuration_that_is_not_in_the_agents_project_and_unknown_is_refused(ns, env):
    cred = seeded(env)
    env.configurations.clear()   # a public or personal configuration the facts do not know
    assert refusal(env, ns, payload_of(cred)).reason_code == "CREDENTIAL_REFERENCE_REQUIRED"


def test_an_inline_provider_secret_is_refused(ns, env):
    env.seed_agent()
    env.schemas["hub_toolkit"] = {"properties": {"api_key": {"secret": True}, "url": {"type": "string"}}}
    env.add_toolkit(1, 70, 1, "hub_toolkit", {"url": "https://hub.example", "api_key": "{{secret.hub_key}}"})
    assert refusal(env, ns, payload_of()).reason_code == "CREDENTIAL_REFERENCE_REQUIRED"
    assert env.schema_calls == [("hub_toolkit", 1, 42)]


def test_an_empty_inline_secret_and_a_toolkit_of_plain_values_are_not_a_credential(ns, env):
    env.seed_agent()
    env.schemas["hub_toolkit"] = {"properties": {"api_key": {"secret": True}}}
    env.add_toolkit(1, 70, 1, "hub_toolkit", {"api_key": ""})
    env.add_toolkit(1, 70, 2, "github", {"count": 3, "flag": True})
    admit(env, ns, payload_of())
    assert env.schema_calls == [] and len(contexts(env, ns)) == 1   # nothing to read a schema for


def test_the_user_permission_is_asked_once_and_only_when_a_use_reaches_it(ns, env):
    binding = env.seed_agent()
    first, second = Credential(env, binding, tool_id=1, config_id=5), Credential(env, binding, tool_id=2, config_id=6)
    admit(env, ns, payload_of(first, second))
    assert env.permission_calls == [(42, "default", 1)]
    env.reset()
    cred = seeded(env, ref_state="suspended")
    refusal(env, ns, payload_of(cred))
    assert env.permission_calls == []


def test_the_permission_is_the_exact_string_not_a_prefix_or_a_wildcard(ns, env):
    cred = seeded(env)
    for held in ({"credentials.*"}, {"credentials.use.extra"}, {"credentials"}, {"credentials.reveal"}):
        env.permissions[42] = held
        assert refusal(env, ns, payload_of(cred)).reason_code == "DENIED_RBAC"


@pytest.mark.parametrize("name, break_it", [
    ("secrets facts raise", lambda env: setattr(env, "secrets_raises", RuntimeError("db down"))),
    ("configuration facts raise", lambda env: setattr(env, "configurations_raises", RuntimeError("db down"))),
    ("secrets module absent", lambda env: env.missing_modules.add("secrets")),
    ("configurations module absent", lambda env: env.missing_modules.add("configurations")),
    ("permission lookup raises", lambda env: setattr(env, "permissions_raise", RuntimeError("auth down"))),
])
def test_an_unreadable_dependency_refuses_with_dependency_unavailable(ns, env, name, break_it):
    cred = seeded(env)
    break_it(env)
    error = refusal(env, ns, payload_of(cred))
    assert error.reason_code == "DEPENDENCY_UNAVAILABLE", name
    assert contexts(env, ns) == []
    assert not [line for line in env.log.lines if cred.ref_id in line or cred.uuid in line or "down" in line]


def test_the_schema_lookup_of_a_toolkit_type_failing_refuses_as_well(ns, env):
    env.seed_agent()
    env.add_toolkit(1, 70, 1, "hub_toolkit", {"url": "https://hub.example"})
    ns.application_tools.find_toolkit_schema_by_type_everywhere = lambda *a: (_ for _ in ()).throw(RuntimeError("rpc down"))
    assert refusal(env, ns, payload_of()).reason_code == "DEPENDENCY_UNAVAILABLE"
    env.unpoison()   # reinstall the fakes for the following tests


def test_the_first_refusal_wins_in_the_order_of_toolkits(ns, env):
    binding = env.seed_agent()
    Credential(env, binding, tool_id=1, config_id=5, ownership_class="personal")
    other = Credential(env, binding, tool_id=2, config_id=6, ref_state="suspended")
    error = refusal(env, ns, payload_of(other))
    assert error.reason_code == "CREDENTIAL_CLASS_NOT_ALLOWED"


# --- the stored definition against the dispatched payload -------------------------------------

def foreign_configuration(env, *, section="credentials", cfg_uuid=None, project_id=1, config_id=77):
    cfg_uuid = cfg_uuid or str(uuid.uuid4())
    env.configurations[(project_id, cfg_uuid)] = {
        "id": config_id, "project_id": project_id, "type": "jira", "section": section, "generation": 1}
    return {"elitea_title": "other", "configuration_uuid": cfg_uuid, "configuration_project_id": project_id,
            "configuration_type": "jira", "configuration_generation": 1}


def test_a_conversation_participant_toolkit_with_a_credential_is_refused(ns, env):
    cred = seeded(env)
    payload = payload_of(cred)
    payload["application"]["version_details"]["tools"].append(
        {"id": 99, "type": "jira", "settings": {"credentials": foreign_configuration(env)}})
    assert refusal(env, ns, payload).reason_code == "CREDENTIAL_REFERENCE_REQUIRED"


def test_a_request_level_tool_with_a_credential_is_refused(ns, env):
    cred = seeded(env)
    payload = payload_of(cred, tools=[{"id": 98, "type": "jira", "settings": {"credentials": foreign_configuration(env)}}])
    assert refusal(env, ns, payload).reason_code == "CREDENTIAL_REFERENCE_REQUIRED"


def test_a_nested_toolkit_reference_with_a_credential_is_refused(ns, env):
    cred = seeded(env)
    payload = payload_of(cred)
    payload["application"]["version_details"]["tools"][0]["settings"]["sub_toolkit"] = {
        "id": 55, "settings": {"credentials": foreign_configuration(env)}}
    assert refusal(env, ns, payload).reason_code == "CREDENTIAL_REFERENCE_REQUIRED"


def test_a_configuration_swapped_in_the_payload_for_the_stored_one_is_refused(ns, env):
    cred = seeded(env)
    payload = payload_of(cred)
    payload["application"]["version_details"]["tools"][0]["settings"]["credentials"] = foreign_configuration(env)
    assert refusal(env, ns, payload).reason_code == "CREDENTIAL_REFERENCE_REQUIRED"


def test_a_credential_in_a_declared_toolkit_at_another_field_is_undeclared(ns, env):
    cred = seeded(env)
    payload = payload_of(cred)
    payload["application"]["version_details"]["tools"][0]["settings"]["extra"] = foreign_configuration(env)
    assert refusal(env, ns, payload).reason_code == "CREDENTIAL_REFERENCE_REQUIRED"


def test_an_undeclared_configuration_without_a_uuid_or_an_unknown_one_is_refused(ns, env):
    cred = seeded(env)
    for stray in ({"configuration_type": "jira"},
                  {"configuration_type": "jira", "configuration_uuid": str(uuid.uuid4()), "configuration_project_id": 1}):
        payload = payload_of(cred)
        payload["application"]["version_details"]["tools"][0]["settings"]["extra"] = stray
        assert refusal(env, ns, payload).reason_code == "CREDENTIAL_REFERENCE_REQUIRED"


def test_platform_internal_configurations_are_outside_the_decision_whatever_their_selector(ns, env):
    cred = seeded(env)
    pgvector = {"elitea_title": "shared-pgvector"}   # stored by title, like most platform-internal configurations
    with ns.sa_orm.Session(env.engine(1)) as session:
        tool = session.get(ns.EliteATool, 1)
        tool.settings = {**tool.settings, "pgvector_configuration": pgvector}
        session.commit()
    payload = payload_of(cred)
    internal = foreign_configuration(env, section="vector_storage", project_id=3)
    payload["application"]["version_details"]["tools"][0]["settings"]["pgvector_configuration"] = internal
    payload["application"]["version_details"]["tools"].append(
        {"id": 99, "type": "x", "settings": {"embedding": foreign_configuration(env, section="embedding")}})
    admit(env, ns, payload)
    [row] = contexts(env, ns)
    assert [entry["toolkit_instance_id"] for entry in row.allowed_bindings] == ["1"]
    # a configuration of another project is looked up in that project, in groups of at most 64
    assert (3, [internal["configuration_uuid"]]) in env.configuration_calls


def test_a_title_selected_credential_is_resolved_from_the_payload_to_learn_its_section(ns, env):
    cred = seeded(env, selector="title")
    assert refusal(env, ns, payload_of(cred)).reason_code == "CREDENTIAL_REFERENCE_REQUIRED"


def test_a_title_selected_platform_internal_configuration_is_not_a_credential(ns, env):
    cred = seeded(env, selector="title", section="llm")
    admit(env, ns, payload_of(cred))
    assert contexts(env, ns)[0].allowed_bindings == [] and env.secrets_calls == []


def test_a_payload_with_the_credential_in_the_request_tools_only_still_decides_the_stored_one(ns, env):
    cred = seeded(env)
    admit(env, ns, {"tools": [cred.tool()]})
    assert len(contexts(env, ns)[0].allowed_bindings) == 1


def test_a_stored_credential_the_payload_does_not_carry_is_still_decided(ns, env):
    cred = seeded(env, ref_state="suspended")
    assert refusal(env, ns, {}).reason_code == "CREDENTIAL_REFERENCE_INACTIVE"
    assert cred.ref_id not in "".join(env.log.lines)


# --- pure decision -----------------------------------------------------------------------------

def pure(ns, **over):
    cd = ns.credential_decision
    now = ns.adm._now()   # pylint: disable=W0212
    ctx = cd.CredentialDecisionContext(
        binding_project_id=1, agent_binding_ref="b" * 8, subject_ref=SUBJECT, policy_revision="rev-1", now=now,
        expires_at=now + timedelta(hours=1), can_use=over.pop("can_use", lambda: True))
    use = cd.CredentialUse("1", "credentials", "u-1", over.pop("selector", "uuid"), 1)
    reference = {"ref_id": "r-1", "state": "active", "generation": 1, "configuration_generation": 3,
                 "store_kind": "database", "ownership_class": "project_shared", "revocation_epoch": 2,
                 "custodian_user_id": None}
    grant = {"grant_id": "g-1", "state": "active", "subject_kind": "user_delegation", "revocation_epoch": 4,
             "valid_from": None, "valid_until": None}
    grant.update(over.pop("grant", {}))
    facts = cd.CredentialFacts(
        {"u-1": {"id": 5, "project_id": 1, "section": "credentials", "generation": 3}}, {5: reference},
        {cd.grant_key("b" * 8, "1", "r-1", SUBJECT): grant})
    return cd.authorize_credential_use(ctx, use, facts), now


def test_the_pure_decision_allows_and_carries_the_epochs_and_the_context_expiry(ns):
    decision, now = pure(ns)
    assert decision.allow and decision.reason_code == ALLOWED
    assert (decision.credential_ref, decision.credential_generation, decision.reference_epoch, decision.grant_id,
            decision.grant_epoch, decision.configuration_generation) == ("r-1", 1, 2, "g-1", 4, 3)
    assert decision.expires_at == now + timedelta(hours=1) and decision.policy_revision == "rev-1"
    uuid.UUID(decision.decision_id)
    with pytest.raises(Exception):
        decision.allow = False   # frozen


def test_the_grant_window_is_closed_at_the_end_and_open_at_the_start(ns):
    now = ns.adm._now()   # pylint: disable=W0212
    for window, allowed in (
            ({"valid_from": (now - timedelta(minutes=1)).isoformat(), "valid_until": (now + timedelta(minutes=1)).isoformat()}, True),
            ({"valid_until": (now - timedelta(seconds=1)).isoformat()}, False),
            ({"valid_from": (now + timedelta(minutes=1)).isoformat()}, False),
            ({"valid_until": (now + timedelta(days=1)).replace(tzinfo=None).isoformat()}, True),   # naive means UTC
            ({"valid_until": "not a time"}, False)):
        decision, _ = pure(ns, grant=window)
        assert decision.allow is allowed, window


def test_the_pure_decision_never_asks_the_permission_for_an_earlier_refusal(ns):
    def boom():
        raise AssertionError("asked")
    for selector in ("title", "inline", "undeclared"):
        decision, _ = pure(ns, selector=selector, can_use=boom)
        assert decision.reason_code == "CREDENTIAL_REFERENCE_REQUIRED"


# --- the re-check of a queued run --------------------------------------------------------------

def validate(ns, env, ref, caller=CALLER):
    return ns.adm.validate_context(env.module(), 1, ref, caller)


def admitted(ns, env, **over):
    cred = seeded(env, **over)
    view = admit(env, ns, payload_of(cred))
    env.denied.clear()
    env.log.lines.clear()
    return cred, view.context_ref


def suspend_reference(cred):
    cred.reference.update(state="suspended", revocation_epoch=3)


def retire_reference(cred):
    cred.reference["state"] = "retired"


def expire_grant(cred):
    cred.grant["valid_until"] = (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()


RECHECKS = {
    "reference suspended": (suspend_reference, "CREDENTIAL_REFERENCE_INACTIVE"),
    "reference retired": (retire_reference, "CREDENTIAL_REFERENCE_INACTIVE"),
    "reference epoch moved": (lambda cred: cred.reference.update(revocation_epoch=9), "EPOCH_MISMATCH"),
    "reference generation moved": (lambda cred: cred.reference.update(generation=2), "CREDENTIAL_GENERATION_MISMATCH"),
    "configuration edited": (lambda cred: cred.env.configurations[(1, cred.uuid)].update(generation=4),
                             "CREDENTIAL_GENERATION_MISMATCH"),
    "configuration deleted": (lambda cred: cred.env.configurations.pop((1, cred.uuid)), "CREDENTIAL_REFERENCE_INACTIVE"),
    "grant revoked": (lambda cred: cred.env.grants.pop((1, cred.key())), "DENIED_GRANT"),
    "grant suspended": (lambda cred: cred.grant.update(state="suspended"), "DENIED_GRANT"),
    "grant expired": (expire_grant, "DENIED_GRANT"),
    "grant epoch moved": (lambda cred: cred.grant.update(revocation_epoch=9), "EPOCH_MISMATCH"),
}


@pytest.mark.parametrize("change, code", RECHECKS.values(), ids=RECHECKS.keys())
def test_a_queued_run_whose_credential_decision_no_longer_holds_is_revoked_with_the_code(ns, env, change, code):
    cred, ref = admitted(ns, env)
    change(cred)
    result = validate(ns, env, ref)
    assert (result["ok"], result["reason_code"], result["message"]) == (False, code, ns.adm.MESSAGES[code])
    [row] = contexts(env, ns)
    assert (row.state, row.revoked_reason) == ("revoked", code)
    assert validate(ns, env, ref)["reason_code"] == code   # the worker is told the same reason again
    assert not [line for line in env.log.lines if cred.ref_id in line or cred.uuid in line]


def test_a_queued_run_whose_decisions_still_hold_is_accepted_and_moves_to_running(ns, env):
    cred, ref = admitted(ns, env)
    result = validate(ns, env, ref)
    assert result["ok"] is True and contexts(env, ns)[0].state == "running"
    # one read of the facts for the references and the grants (M5), then the configurations
    assert env.secrets_calls[-1:] == [(1, [], [("%s" % cred.binding, "1", cred.ref_id, "*", SUBJECT)], [cred.ref_id])]
    assert env.configuration_calls[-1] == (1, [cred.uuid])


def test_unreadable_credential_facts_leave_the_run_unrevoked_and_retryable(ns, env):
    cred, ref = admitted(ns, env)
    env.secrets_raises = RuntimeError("db down")
    assert validate(ns, env, ref)["reason_code"] == "DEPENDENCY_UNAVAILABLE"
    assert contexts(env, ns)[0].state == "admitted"
    env.secrets_raises = None
    assert validate(ns, env, ref)["ok"] is True
    assert cred


def test_a_run_without_credential_decisions_is_validated_without_a_single_facts_call(ns, env):
    env.seed_agent()
    ref = admit(env, ns, {}).context_ref
    env.secrets_calls.clear()
    env.configuration_calls.clear()
    assert validate(ns, env, ref)["ok"] is True
    assert env.secrets_calls == [] and env.configuration_calls == []


# --- revoking the runs that hold a credential --------------------------------------------------

def test_revoking_a_credential_revokes_only_the_live_runs_that_hold_it(ns, env):
    first_binding = env.seed_agent(application_id=7, version_id=70)
    second_binding = env.seed_agent(application_id=8, version_id=71)
    env.seed_agent(application_id=9, version_id=72)
    first = Credential(env, first_binding, tool_id=1, config_id=5, version_id=70)
    second = Credential(env, second_binding, tool_id=2, config_id=6, version_id=71)
    held = {}
    for run, agent, creds in (("run-a", (7, 70), [first]), ("run-b", (7, 70), [first]),
                              ("run-c", (8, 71), [second]), ("run-d", (9, 72), [])):
        request = env.request(application_id=agent[0], version_id=agent[1])
        held[run] = ns.adm.admit_execution(env.module(), request, run_id=run, payload=payload_of(*creds)).context_ref
    with ns.sa_orm.Session(env.engine(1)) as session:
        assert ns.adm.revoke_credential_contexts(session, uuid.UUID(first.ref_id)) == 2
        session.commit()
    states = {row.run_id: (row.state, row.revoked_reason) for row in contexts(env, ns)}
    assert states == {"run-a": ("revoked", "CREDENTIAL_REFERENCE_INACTIVE"), "run-b": ("revoked", "CREDENTIAL_REFERENCE_INACTIVE"),
                      "run-c": ("admitted", None), "run-d": ("admitted", None)}
    # the worker of a revoked run is told why
    assert validate(ns, env, held["run-a"], {**CALLER, "run_id": "run-a"})["reason_code"] == "CREDENTIAL_REFERENCE_INACTIVE"
    # a text id works, an already revoked run is not counted again, a reference nobody holds changes nothing
    with ns.sa_orm.Session(env.engine(1)) as session:
        assert ns.adm.revoke_credential_contexts(session, second.ref_id) == 1
        assert ns.adm.revoke_credential_contexts(session, first.ref_id) == 0
        assert ns.adm.revoke_credential_contexts(session, str(uuid.uuid4())) == 0
        session.commit()
    assert validate(ns, env, held["run-c"], {**CALLER, "run_id": "run-c"})["reason_code"] == "CREDENTIAL_REFERENCE_INACTIVE"


def test_revoking_does_not_commit_the_callers_transaction(ns, env):
    cred = seeded(env)
    admit(env, ns, payload_of(cred))
    with ns.sa_orm.Session(env.engine(1)) as session:
        assert ns.adm.revoke_credential_contexts(session, cred.ref_id) == 1
        session.rollback()
    assert contexts(env, ns)[0].state == "admitted"


def test_the_method_for_the_secrets_plugin_is_a_module_method_over_the_same_function():
    text = (PLUGIN_ROOT / "methods" / "restricted.py").read_text()
    assert "def revoke_credential_contexts(self, session, credential_ref):" in text
    assert "return revoke_credential_contexts(session, credential_ref)" in text


# --- an inherited dispatch ---------------------------------------------------------------------

def call_kwargs(payload, context_ref=None):
    body = {"project_id": 1, "message_id": 5, "_elitea_predict_run_id": RUN, **payload}
    body["envelope"] = {"schema_version": 1, "security_profile": "governed",
                        "required_capabilities": ["task_envelope.v1"], "context_ref": context_ref}
    return {"args": ["s", "m"], "kwargs": body, "pool": "agents", "meta": {"project_id": 1}}


def parent_context(ns, env, **over):
    cred = seeded(env, **over)
    call = call_kwargs(payload_of(cred))
    return cred, ns.adm.admit_dispatch(env.module(), "indexer_agent", call, env.request())


def test_a_child_dispatch_carrying_a_credential_the_parent_was_not_admitted_for_is_refused(ns, env):
    _, parent = parent_context(ns, env)
    child = call_kwargs(payload_of(), parent)
    child["kwargs"]["application"] = {"version_details": {"tools": [
        {"id": 31, "type": "jira", "settings": {"credentials": foreign_configuration(env)}}]}}
    env.denied.clear()
    before = ns.adm.CREDENTIAL_DECISION.total("deny", "CREDENTIAL_REFERENCE_REQUIRED")
    with pytest.raises(ns.exceptions.AdmissionDenied) as err:
        ns.adm.admit_dispatch(env.module(), "indexer_agent", child, None)
    assert err.value.reason_code == "CREDENTIAL_REFERENCE_REQUIRED"
    assert "context_ref" not in child["meta"]
    [event] = env.denied
    assert (event["decision"], event["reason_code"], event["run_id"]) == ("deny", "CREDENTIAL_REFERENCE_REQUIRED", RUN)
    assert ns.adm.CREDENTIAL_DECISION.total("deny", "CREDENTIAL_REFERENCE_REQUIRED") == before + 1
    assert len(contexts(env, ns)) == 1   # no new admission


def test_a_child_dispatch_with_the_parents_own_credentials_and_internal_configurations_inherits(ns, env):
    cred, parent = parent_context(ns, env)
    child = call_kwargs(payload_of(cred), parent)
    child["kwargs"]["tools"] = [{"id": 8, "type": "x", "settings": {"pgvector": foreign_configuration(env, section="vector_storage")}}]
    assert ns.adm.admit_dispatch(env.module(), "indexer_agent", child, None) is None
    assert child["meta"]["context_ref"] == parent


def test_a_child_dispatch_with_no_configuration_at_all_reads_nothing(ns, env):
    _, parent = parent_context(ns, env)
    calls = (len(env.configuration_calls), len(env.secrets_calls))
    sessions = len(env.sessions)
    child = call_kwargs({"tools": [{"id": 8, "type": "x", "settings": {"note": "plain"}}]}, parent)
    assert ns.adm.admit_dispatch(env.module(), "indexer_agent", child, None) is None
    assert (len(env.configuration_calls), len(env.secrets_calls)) == calls and len(env.sessions) == sessions


@pytest.mark.parametrize("name, setup, code", [
    ("parent unknown", lambda env, parent: str(uuid.uuid4()), "STALE_CONTEXT"),
    ("facts unreadable", lambda env, parent: (setattr(env, "configurations_raises", RuntimeError("down")), parent)[1],
     "DEPENDENCY_UNAVAILABLE"),
])
def test_a_child_dispatch_whose_parent_or_facts_cannot_be_read_is_refused(ns, env, name, setup, code):
    cred, parent = parent_context(ns, env)
    child = call_kwargs(payload_of(), setup(env, parent))
    child["kwargs"]["tools"] = [{"id": 31, "type": "jira", "settings": {"credentials": foreign_configuration(env)}}]
    with pytest.raises(ns.exceptions.AdmissionDenied) as err:
        ns.adm.admit_dispatch(env.module(), "indexer_agent", child, None)
    assert err.value.reason_code == code, name


# --- flag off ----------------------------------------------------------------------------------

@pytest.mark.parametrize("with_request", [False, True])
def test_flag_off_the_payload_is_untouched_and_nothing_credential_related_is_called(ns, env, with_request):
    cred = seeded(env)
    call = call_kwargs(payload_of(cred))
    call["kwargs"]["envelope"] = {"schema_version": 1, "security_profile": "legacy", "required_capabilities": [],
                                  "context_ref": None}
    before = copy.deepcopy(call)
    env.poison()
    assert ns.adm.admit_dispatch(env.module(governed=False), "indexer_agent", call,
                                 env.request() if with_request else None) is None
    env.unpoison()
    assert call == before
    assert json.dumps(call, sort_keys=True) == json.dumps(before, sort_keys=True)
    assert env.configuration_calls == [] and env.secrets_calls == [] and env.permission_calls == []
    assert env.schema_calls == [] and contexts(env, ns) == []


def test_flag_off_a_dispatch_with_a_legacy_envelope_does_not_look_at_credentials(ns, env):
    """Off stays free. (A child of a run admitted under enforce carries a governed envelope: it is sealed against its
    parent whatever the mode is now, see test_7120_audit_only.)"""
    cred = seeded(env)
    call = call_kwargs(payload_of(cred), "11111111-1111-1111-1111-111111111111")
    call["kwargs"]["envelope"]["security_profile"] = "legacy"
    env.poison()
    assert ns.adm.admit_dispatch(env.module(governed=False), "indexer_agent", call, None) is None
    env.unpoison()
    assert env.configuration_calls == [] and env.secrets_calls == []


def test_a_governed_dispatch_passes_the_payload_to_admission_and_the_context_carries_the_decision(ns, env):
    cred = seeded(env)
    call = call_kwargs(payload_of(cred))
    context_ref = ns.adm.admit_dispatch(env.module(), "indexer_agent", call, env.request())
    [row] = contexts(env, ns)
    assert str(row.id) == context_ref and row.allowed_bindings[0]["credential_ref"] == cred.ref_id


# --- startup, messages, source order -----------------------------------------------------------

def failing(ns, env):
    return sorted(check.id for check in ns.adm.governance_preflight(env.module()) if check.status == "fail")


def test_the_preflight_needs_the_facts_methods_and_the_master_key_for_the_database_store(ns, env):
    assert failing(ns, env) == []
    for missing in ("secrets", "configurations"):
        env.missing_modules = {missing}
        assert failing(ns, env) == ["credential_facts"]
    env.missing_modules = set()
    ns.tools.config.SECRETS_ENGINE = "database"
    try:
        assert failing(ns, env) == ["master_key"]
        ns.tools.config.SECRETS_MASTER_KEY = "set"
        assert failing(ns, env) == []
    finally:
        for name in ("SECRETS_ENGINE", "SECRETS_MASTER_KEY"):
            vars(ns.tools.config).pop(name, None)


def test_the_preflight_never_raises_however_much_is_missing(ns, env):
    env.missing_modules = {"secrets", "configurations", "tracing"}
    assert "credential_facts" in failing(ns, env)   # a failing preflight no longer stops the plugin from starting (#7120)


NEW_CODES = ("CREDENTIAL_REFERENCE_REQUIRED", "CREDENTIAL_REFERENCE_INACTIVE", "CREDENTIAL_GENERATION_MISMATCH",
             "CREDENTIAL_CLASS_NOT_ALLOWED", "DENIED_GRANT", "DENIED_RBAC")


def test_the_new_messages_are_readable_fixed_text_without_identifiers(ns):
    for code in NEW_CODES:
        message = ns.adm.MESSAGES[code]
        assert message.endswith(".") and len(message) < 120
        assert not UUID_PATTERN.search(message) and not re.search(r"\d", message)
        for noise in ("{", "}", "_id", "uuid", "toolkit", "field"):
            assert noise not in message.lower()
    assert len({ns.adm.MESSAGES[code] for code in NEW_CODES}) == len(NEW_CODES)
    assert {"CREDENTIAL_REFERENCE_INACTIVE", "CREDENTIAL_GENERATION_MISMATCH", "DENIED_GRANT"} <= ns.adm.REVOKE_CODES


def test_a_refusal_never_leaks_what_it_refused_in_any_log_line_or_event_value(ns, env):
    cred = seeded(env, ownership_class="personal")
    error = refusal(env, ns, payload_of(cred))
    values = [error.message, str(error), *env.log.lines]
    values += [str(value) for event in env.denied for key, value in event.items() if key not in ("credential_ref",)]
    for secret in (cred.uuid, TITLE, cred.grant_id, "plain-token-value", "https://jira.example"):
        assert not [value for value in values if secret in value]


def test_the_decision_runs_after_binding_and_subject_and_before_the_context_row():
    text = (PLUGIN_ROOT / "utils" / "admission.py").read_text()
    body = text[text.index("def _admit("):text.index("def admit_execution")]
    order = [body.index(marker) for marker in (
        "_verify_identity(request, settings)", "_find_binding(request, settings)", "_subject_of(principal)",
        "_decide_credentials(module, request, subject, binding, payload, expires_at, shadow=shadow, rows=rows,",
        "_store_admission(")]
    assert order == sorted(order)
    store = text[text.index("def _store_admission("):text.index("# --- autonomous mandates")]
    assert store.index("session.add(ExecutionSecurityContext(") < store.index("security_audit_record(session")
    assert "payload=payload" in text[text.index("def admit_dispatch"):text.index("def _parent_entries")]
