"""C-1009 (#7213): development runs under enforce - the editor's test chat of an unbound version.

The entry point ``editor_chat`` (set by ``chat_all`` from the conversation row), the ``allow_development_runs`` setting,
the development admission (no binding, the author's own credentials only), its audit shape, the worker's validation and
release against a development context, the inherited dispatch of its children, and the setting's defaults. Real models
and statements on SQLite (see ``fixtures/admission_env.py``); the facts of ``secrets`` are the keyed fake at version 5.
"""
import sys
import types
import uuid
from pathlib import Path

import pytest

PLUGIN_ROOT = Path(__file__).parents[2]
sys.path.insert(0, str(PLUGIN_ROOT / "tests"))

from fixtures import admission_env  # noqa: E402  pylint: disable=C0413
from fixtures.governed_credentials import (  # noqa: E402  pylint: disable=C0413
    CALLER, RUN, SECRET, SUBJECT, Credential, call_kwargs, dispatch, payload_of, run_to_running, sealed_tool_settings,
)

OTHER_SUBJECT = "idp:tid-1:oid-77"
EDIT = "models.applications.application.update"
DIGEST = "adv1:" + "7" * 64
DEV_CODE = "DENIED_DEVELOPMENT_CREDENTIAL"


@pytest.fixture(scope="module")
def ns(tmp_path_factory):
    saved = dict(sys.modules)
    yield admission_env.install(PLUGIN_ROOT, tmp_path_factory.mktemp("development_runs"))
    sys.modules.clear()
    sys.modules.update(saved)


@pytest.fixture
def env(ns):
    ns.env.reset()
    ns.env.facts_version = 5
    ns.env.permissions[42] = {"credentials.use", EDIT}
    return ns.env


def unbound_version(env, version_id=70, application_id=7, digest=DIGEST):
    """A saved version with no binding at all: what the editor tests before an operator binds it."""
    env.add_version(1, version_id, application_id)
    env.digests[version_id] = digest


def module(env, **settings):
    settings.setdefault("allow_development_runs", True)
    return env.module(**settings)


def admit(env, ns, payload=None, module_=None, **request):
    request.setdefault("entry_point", "editor_chat")
    return ns.adm.admit_execution(module_ or module(env), env.request(**request), run_id=RUN, payload=payload)


def refusal(env, ns, payload=None, module_=None, **request):
    with pytest.raises(ns.exceptions.AdmissionDenied) as err:
        admit(env, ns, payload, module_, **request)
    return err.value


def contexts(env, ns):
    return env.query(1, ns.models.ExecutionSecurityContext)


def granted_elsewhere(env, **over):
    """A project credential the author holds a grant on, for another binding and toolkit than the draft's."""
    return Credential(env, uuid.uuid4(), **over)   # the grant key names that other binding


def personal(env, custodian=SUBJECT, **over):
    cred = Credential(env, uuid.uuid4(), grants=False, **over)
    cred.reference.update(ownership_class="personal", custodian_subject_ref=custodian, custodian_user_id=42)
    return cred


# --- the entry point: who may say "editor" ---------------------------------------------------------------------------

def test_the_editor_entry_point_is_a_request_value_and_the_only_one_that_can_run_development(ns):
    request = ns.env.request(entry_point="editor_chat")
    assert request.entry_point == "editor_chat" == ns.pd_admission.ENTRY_EDITOR_CHAT
    settings = ns.adm.AdmissionSettings(allow_development_runs=True)
    assert ns.adm._is_development(request, settings)  # pylint: disable=W0212
    for other in ("chat_sio", "chat_sio_llm", "predict_api", "schedule", "webhook"):
        assert not ns.adm._is_development(ns.env.request(entry_point=other), settings)  # pylint: disable=W0212
    assert not ns.adm._is_development(request, ns.adm.AdmissionSettings(allow_development_runs=False))  # pylint: disable=W0212
    assert not ns.adm._is_development(request.model_copy(update={"run_kind": "system"}), settings)  # pylint: disable=W0212
    assert not ns.adm._is_development(request.model_copy(update={"caller_type": "token"}), settings)  # pylint: disable=W0212


def test_chat_all_says_editor_from_the_conversation_row_and_application_predict_maps_it_to_the_entry_point():
    chat_all = (PLUGIN_ROOT / "rpc" / "chat_all.py").read_text()
    assert chat_all.count("**authoring_kwargs(conversation,") == 2   # the first turn and a continued one
    helper = chat_all[chat_all.index("def authoring_kwargs("):chat_all.index("def _get_agent_type")]
    assert "is_authoring_conversation(conversation, participant, chat_project_id)" in helper
    assert "parsed" not in helper and "data" not in helper   # nothing of the client's turn decides it
    application = (PLUGIN_ROOT / "rpc" / "application.py").read_text()
    predict = application[application.index("def predict_sio("):application.index("def predict_sio_llm(")]
    assert "authoring: bool = False" in predict
    assert 'entry_point=ENTRY_EDITOR_CHAT if authoring and sid else "chat_sio"' in predict
    # the SIO handler of a direct application predict does not pass it: a client cannot reach the argument
    sio = (PLUGIN_ROOT / "sio" / "all.py").read_text()
    assert "authoring" not in sio
    assert "authoring" not in (PLUGIN_ROOT / "models" / "pd" / "chat.py").read_text()


def conversation(**over):
    values = dict(is_private=True, source="agent", meta={"single_participant": {
        "entity_name": "application", "entity_meta": {"id": 7, "project_id": 1}}})
    values.update(over)
    return types.SimpleNamespace(**values)


def participant(**over):
    values = dict(entity_name="application", entity_meta={"id": 7, "project_id": 1})
    values.update(over)
    return types.SimpleNamespace(**values)


def test_an_authoring_conversation_is_the_private_editor_one_of_this_application_in_its_own_project(ns):
    authoring = ns.authoring.is_authoring_conversation
    assert authoring(conversation(), participant(), 1)
    assert authoring(conversation(source="pipeline"), participant(), 1)
    assert not authoring(conversation(is_private=False), participant(), 1)            # a shared chat
    assert not authoring(conversation(source="elitea"), participant(), 1)              # the chat page
    assert not authoring(conversation(meta={}), participant(), 1)                      # no single participant
    assert not authoring(conversation(), participant(entity_meta={"id": 8, "project_id": 1}), 1)   # another agent
    assert not authoring(conversation(), participant(), 2)                             # chatted from another project
    assert not authoring(conversation(), participant(entity_name="skill"), 1)
    assert not authoring(conversation(meta={"single_participant": {"entity_name": "application"}}), participant(), 1)
    assert not authoring(None, participant(), 1) and not authoring(conversation(), None, 1)


# --- the development admission -----------------------------------------------------------------------------------------

def test_an_unbound_version_is_admitted_from_the_editor_as_a_development_run_with_its_digest(ns, env):
    unbound_version(env)
    view = admit(env, ns)
    [row] = contexts(env, ns)
    assert (row.profile, row.agent_binding_ref, row.definition_digest, row.entry_point) == (
        "development", None, DIGEST, "editor_chat")
    assert row.subject["ref"] == SUBJECT and row.caller == {"kind": "user", "ref": "user:42", "user_id": 42}
    assert row.revocation_epochs == {"user": 3, "token": 0, "binding": None, "project": None, "credential": None}
    assert view.agent_binding_ref == f"development:{DIGEST}" and view.context_ref == str(row.id)
    [(_, event)] = env.recorded
    assert (event["event_type"], event["decision"], event["reason_code"], event["agent_binding_ref"]) == (
        "admission", "allow", "ADMITTED_DEVELOPMENT", f"development:{DIGEST}")
    assert event["user_ref"] == SUBJECT and env.denied == []
    assert "security_metric name=admission_decision_total decision=allow reason_code=ADMITTED" in env.log.lines


def test_the_same_unbound_version_from_any_other_entry_point_is_refused_as_before(ns, env):
    unbound_version(env)
    for entry_point in ("chat_sio", "predict_api"):
        assert refusal(env, ns, entry_point=entry_point).reason_code == "DENIED_BINDING"
    assert contexts(env, ns) == []
    assert [event["reason_code"] for event in env.denied] == ["DENIED_BINDING", "DENIED_BINDING"]


def test_with_the_setting_off_the_editor_is_refused_with_the_readable_reason(ns, env):
    unbound_version(env)
    error = refusal(env, ns, module_=module(env, allow_development_runs=False))
    # the same card as a toolkit test or a reindex with the setting off (#7227 D10), not "no active binding"
    assert (error.reason_code, error.message) == (
        "DEVELOPMENT_RUNS_DISABLED", ns.adm.MESSAGES["DEVELOPMENT_RUNS_DISABLED"])
    assert "Admin \u2192 Governance" in error.message
    assert contexts(env, ns) == []


def test_a_bound_version_from_the_editor_is_also_a_development_run_the_profile_follows_the_entry_point(ns, env):
    env.seed_agent()   # bound and active
    admit(env, ns)
    [row] = contexts(env, ns)
    assert row.profile == "development" and row.agent_binding_ref is None


def test_the_governed_path_reads_nothing_more_than_before_the_profile_is_decided_from_the_request(ns, env):
    env.seed_agent()
    governed = env.module(allow_development_runs=True)
    before = len(env.sessions)
    ns.adm.admit_execution(governed, env.request(entry_point="chat_sio"), run_id=RUN, payload=None)
    opened = len(env.sessions) - before
    env.reset()
    env.permissions[42] = {"credentials.use", EDIT}
    env.seed_agent()
    ns.adm.admit_execution(env.module(), env.request(entry_point="chat_sio"), run_id=RUN, payload=None)
    assert len(env.sessions) == opened and env.permission_calls == []
    text = (PLUGIN_ROOT / "utils" / "admission.py").read_text()
    body = text[text.index("def _admit("):text.index("def _is_development(")]
    # the governed path's binding read comes after the development decision (an owner's Run now of a schedule reads its
    # binding first on purpose, #7254: a bound version runs on it)
    assert body.index("elif _is_development(request, settings):") < body.rindex("_find_binding(request, settings)")


def test_a_system_user_or_a_token_cannot_author(ns, env):
    unbound_version(env)
    assert refusal(env, ns, run_kind="system").reason_code == "MANDATE_REQUIRED"
    env.verify_result = {"ok": False, "reason_code": "IDENTITY_KIND_NOT_ADMITTED"}
    assert refusal(env, ns, caller_type="token", caller_reference=None).reason_code == "IDENTITY_KIND_NOT_ADMITTED"
    assert "#7214" in ns.adm.MESSAGES["IDENTITY_KIND_NOT_ADMITTED"]
    env.verify_result = {"ok": True, "principal": admission_env.principal(verified=False)}   # a PAT that verified
    assert refusal(env, ns).reason_code == "MANDATE_REQUIRED"   # asserted, not verified: never an author
    assert contexts(env, ns) == []


def test_only_someone_who_may_edit_the_application_may_author_it(ns, env):
    unbound_version(env)
    env.permissions[42] = {"credentials.use"}
    assert refusal(env, ns).reason_code == "DENIED_RBAC"
    assert env.permission_calls == [(42, "default", 1)]
    env.permissions_raise = RuntimeError("auth down")
    assert refusal(env, ns).reason_code == "DEPENDENCY_UNAVAILABLE"
    assert contexts(env, ns) == []


def test_a_version_that_does_not_exist_or_belongs_to_another_application_is_refused(ns, env):
    assert refusal(env, ns).reason_code == "DENIED_BINDING"
    unbound_version(env, application_id=8)
    assert refusal(env, ns).reason_code == "DENIED_BINDING"
    env.add_version(1, 71, 7)   # exists, no digest readable
    assert refusal(env, ns, version_id=71).reason_code == "DENIED_BINDING"


# --- the credential rule: the author's own right, decided per turn -------------------------------------------------------

def test_a_toolkit_without_a_credential_is_admitted_without_a_single_facts_call(ns, env):
    unbound_version(env)
    env.add_toolkit(1, 70, 1, "jira", {"url": "https://jira.example"})
    admit(env, ns, {"application": {"version_details": {"tools": [
        {"id": 1, "type": "jira", "settings": {"url": "https://jira.example"}}]}}})
    assert env.secrets_calls == [] and contexts(env, ns)[0].allowed_bindings == []


def test_a_project_credential_granted_to_the_author_on_another_binding_is_released_to_the_draft(ns, env):
    unbound_version(env)
    cred = granted_elsewhere(env)
    view = admit(env, ns, payload_of(cred))
    [row] = contexts(env, ns)
    [entry] = row.allowed_bindings
    assert (entry["credential_ref"], entry["grant_id"], entry["grant_epoch"], entry["reference_epoch"]) == (
        cred.ref_id, cred.grant_id, 4, 2)
    assert view.allowed_bindings == [entry]
    # one facts read: the references and the author's grants on them, keyed by configuration id and subject
    assert env.subject_grant_calls[-1] == [(5, SUBJECT)]
    assert env.secrets_calls[-1] == (1, [5], [], None)
    assert env.permission_calls == [(42, "default", 1)]   # the edit right and credentials.use in one read
    [(_, decision), (_, admission)] = env.recorded
    assert decision["reason_code"] == "CREDENTIAL_USE_ALLOWED" and decision["agent_binding_ref"] == f"development:{DIGEST}"
    assert admission["reason_code"] == "ADMITTED_DEVELOPMENT"


def test_a_project_credential_the_author_holds_no_grant_on_is_refused_readably(ns, env):
    unbound_version(env)
    cred = granted_elsewhere(env, grants=False)
    error = refusal(env, ns, payload_of(cred))
    assert (error.reason_code, error.message) == (DEV_CODE, "This credential needs a grant before a draft can use it.")
    [event] = env.denied
    assert (event["reason_code"], event["credential_ref"], event["decision"]) == (DEV_CODE, cred.ref_id, "deny")
    assert contexts(env, ns) == []
    assert f"credential_decision_total decision=deny reason_code={DEV_CODE}" in "\n".join(env.log.lines)


def test_a_grant_to_someone_else_or_one_that_is_not_in_force_does_not_count(ns, env):
    unbound_version(env)
    cred = granted_elsewhere(env, grants=False)
    env.grants[(1, cred.key(OTHER_SUBJECT))] = {"grant_id": str(uuid.uuid4()), "state": "active", "revocation_epoch": 0,
                                               "valid_from": None, "valid_until": None, "operations": ["*"]}
    assert refusal(env, ns, payload_of(cred)).reason_code == DEV_CODE
    env.grants[(1, cred.key())] = {"grant_id": str(uuid.uuid4()), "state": "suspended", "revocation_epoch": 0,
                                   "valid_from": None, "valid_until": None, "operations": ["*"]}
    assert refusal(env, ns, payload_of(cred)).reason_code == DEV_CODE
    env.grants[(1, cred.key())].update(state="active", subject_kind="mandate")   # a mandate's grant is not a person's
    assert refusal(env, ns, payload_of(cred)).reason_code == DEV_CODE


def test_the_authors_personal_credential_needs_no_grant_and_someone_elses_is_refused(ns, env):
    unbound_version(env)
    cred = personal(env)
    admit(env, ns, payload_of(cred))
    [entry] = contexts(env, ns)[0].allowed_bindings
    assert entry["credential_ref"] == cred.ref_id and entry["grant_id"] is None and entry["grant_epoch"] is None
    assert env.subject_grant_calls[-1] == [(5, SUBJECT)]   # asked in the same read; nothing is held and nothing needed
    env.reset()
    env.facts_version, env.permissions[42] = 5, {"credentials.use", EDIT}
    unbound_version(env)
    other = personal(env, custodian=OTHER_SUBJECT)
    assert refusal(env, ns, payload_of(other)).reason_code == "CREDENTIAL_CLASS_NOT_ALLOWED"


def test_the_other_checks_of_a_credential_stand_before_the_development_rule(ns, env):
    unbound_version(env)
    env.permissions[42] = {EDIT}   # may edit, may not use credentials
    cred = granted_elsewhere(env)
    assert refusal(env, ns, payload_of(cred)).reason_code == "DENIED_RBAC"
    env.permissions[42] = {"credentials.use", EDIT}
    cred.reference["state"] = "suspended"
    assert refusal(env, ns, payload_of(cred)).reason_code == "CREDENTIAL_REFERENCE_INACTIVE"


def test_a_secrets_plugin_that_cannot_answer_the_authors_grants_is_a_dependency_refusal_not_a_pass(ns, env):
    unbound_version(env)
    env.facts_version = 4
    cred = granted_elsewhere(env)
    assert refusal(env, ns, payload_of(cred)).reason_code == "DEPENDENCY_UNAVAILABLE"
    assert env.secrets_calls == []


def test_each_turn_is_decided_anew_the_draft_may_swap_its_toolkits_between_turns(ns, env):
    unbound_version(env)
    granted = granted_elsewhere(env, tool_id=1, config_id=5)
    admit(env, ns, payload_of(granted))                                              # turn 1: the granted toolkit
    ungranted = granted_elsewhere(env, tool_id=2, config_id=6, grants=False)         # the draft gains a toolkit
    env.digests[70] = "adv1:" + "8" * 64
    assert refusal(env, ns, payload_of(granted, ungranted)).reason_code == DEV_CODE   # turn 2: refused
    with ns.sa_orm.Session(env.engine(1)) as session:                                # the author takes it out again
        session.execute(ns.sa.delete(ns.EntityToolMapping).where(ns.EntityToolMapping.tool_id == 2))
        session.commit()
    env.digests[70] = DIGEST
    ns.adm.admit_execution(module(env), env.request(entry_point="editor_chat"), run_id="run-3", payload=payload_of(granted))
    rows = sorted(contexts(env, ns), key=lambda row: row.run_id)
    assert [(row.run_id, row.definition_digest) for row in rows] == [(RUN, DIGEST), ("run-3", DIGEST)]
    admissions = [event for _, event in env.recorded if event["event_type"] == "admission"]
    assert [event["agent_binding_ref"] for event in admissions] == [f"development:{DIGEST}"] * 2
    assert [event["reason_code"] for event in env.denied] == [DEV_CODE]


# --- audit only: rehearsed, never refused ------------------------------------------------------------------------------

def test_audit_only_rehearses_the_editor_as_a_development_run_and_records_a_would_allow(ns, env):
    unbound_version(env)
    cred = granted_elsewhere(env)
    call = call_kwargs(payload_of(cred))
    result = ns.adm.admit_dispatch(env.module(governed=False, audit=True, allow_development_runs=True), "indexer_agent",
                                   call, env.request(entry_point="editor_chat"))
    assert result is None and contexts(env, ns) == [] and env.denied == []
    [(_, decision), (_, admission)] = env.recorded
    assert (admission["reason_code"], admission["would_deny"], admission["agent_binding_ref"]) == (
        "ADMITTED_DEVELOPMENT", False, f"development:{DIGEST}")
    assert decision["would_deny"] is False
    assert "credential_slot" not in str(call)   # the real payload is not sealed in audit only


def test_audit_only_with_the_setting_off_records_the_readable_would_deny(ns, env):
    unbound_version(env)
    ns.adm.admit_dispatch(env.module(governed=False, audit=True, allow_development_runs=False), "indexer_agent",
                          call_kwargs({}), env.request(entry_point="editor_chat"))
    [event] = env.denied
    assert (event["reason_code"], event["would_deny"]) == ("DEVELOPMENT_RUNS_DISABLED", True)


# --- the worker: validation and release against a development context ------------------------------------------------------

def running_development(ns, env, *credentials):
    """A development run dispatched (sealed), then validated to ``running``: ``(call, context_ref)``."""
    unbound_version(env)
    call, ref = dispatch(env, ns, *credentials, module=module(env), entry_point="editor_chat")
    run_to_running(env, ns, ref)
    env.denied.clear()
    env.recorded.clear()
    env.log.lines.clear()
    return call, ref


def test_the_worker_validates_a_development_context_without_a_binding_and_is_told_what_it_runs_on(ns, env):
    cred = granted_elsewhere(env)
    _, ref = running_development(ns, env, cred)
    [row] = contexts(env, ns)
    assert row.state == "running" and row.profile == "development"
    result = ns.adm.validate_context(env.module(), 1, ref, CALLER)
    assert result["ok"] is True and result["context"]["agent_binding_ref"] == f"development:{DIGEST}"
    # the recheck read the author's grant on the credential, not a grant of a binding
    assert env.secrets_calls[-1] == (1, [], [], [cred.ref_id]) and env.subject_grant_calls[-1] == [(cred.ref_id, SUBJECT)]


def revoke_grant(cred):
    cred.env.grants.pop((1, cred.key()))


RECHECKS = {
    "the author's grant revoked": (revoke_grant, DEV_CODE),
    "the author's grant suspended": (lambda cred: cred.env.grants[(1, cred.key())].update(state="suspended"), DEV_CODE),
    "the grant epoch moved": (lambda cred: cred.env.grants[(1, cred.key())].update(revocation_epoch=9), "EPOCH_MISMATCH"),
    "the reference suspended": (lambda cred: cred.reference.update(state="suspended"), "CREDENTIAL_REFERENCE_INACTIVE"),
    "the reference epoch moved": (lambda cred: cred.reference.update(revocation_epoch=9), "EPOCH_MISMATCH"),
    "the configuration edited": (lambda cred: cred.env.configurations[(1, cred.uuid)].update(generation=4),
                                 "CREDENTIAL_GENERATION_MISMATCH"),
}


@pytest.mark.parametrize("change, code", RECHECKS.values(), ids=RECHECKS.keys())
def test_a_development_run_whose_decision_no_longer_holds_is_revoked_with_the_code(ns, env, change, code):
    cred = granted_elsewhere(env)
    _, ref = running_development(ns, env, cred)
    change(cred)
    result = ns.adm.validate_context(env.module(), 1, ref, CALLER)
    assert (result["ok"], result["reason_code"], result["message"]) == (False, code, ns.adm.MESSAGES[code])
    [row] = contexts(env, ns)
    assert (row.state, row.revoked_reason) == ("revoked", code)
    assert ns.adm.validate_context(env.module(), 1, ref, CALLER)["reason_code"] == code


def test_a_personal_credential_that_changed_custodian_revokes_the_development_run(ns, env):
    cred = personal(env)
    _, ref = running_development(ns, env, cred)
    cred.reference["custodian_subject_ref"] = OTHER_SUBJECT
    assert ns.adm.validate_context(env.module(), 1, ref, CALLER)["reason_code"] == "CREDENTIAL_CLASS_NOT_ALLOWED"


def test_a_release_to_a_development_run_records_the_digest_it_was_released_under(ns, env):
    ns.need_transport()
    cred = granted_elsewhere(env)
    call, ref = running_development(ns, env, cred)
    slot = dict(sealed_tool_settings(call)["credential_slot"])
    _private, public = ns.keypair()
    reply = ns.credential_release.release_credential(
        env.module(), project_id=1, context_ref=ref, probe_ref=None, caller=dict(CALLER), toolkit_instance_id="1",
        field="credentials", slot=slot, recipient_key=public)
    assert reply["ok"] is True and SECRET not in str(reply)
    [(_, event)] = env.recorded
    assert (event["event_type"], event["reason_code"], event["agent_binding_ref"], event["credential_ref"]) == (
        "credential_acquired", "CREDENTIAL_RELEASED", f"development:{DIGEST}", cred.ref_id)
    assert event["user_ref"] == SUBJECT
    # the batch handler names the same thing
    env.recorded.clear()
    reply = ns.credential_release.release_credentials(
        env.module(), project_id=1, context_ref=ref, probe_ref=None, caller=dict(CALLER), recipient_key=public,
        slots=[{"toolkit_instance_id": "1", "field": "credentials", "slot": slot}])
    assert reply["ok"] is True and env.recorded[0][1]["agent_binding_ref"] == f"development:{DIGEST}"


def test_a_release_after_the_authors_grant_went_is_refused_and_ends_the_run(ns, env):
    cred = granted_elsewhere(env)
    call, ref = running_development(ns, env, cred)
    slot = dict(sealed_tool_settings(call)["credential_slot"])
    _private, public = ns.keypair()
    revoke_grant(cred)
    reply = ns.credential_release.release_credential(
        env.module(), project_id=1, context_ref=ref, probe_ref=None, caller=dict(CALLER), toolkit_instance_id="1",
        field="credentials", slot=slot, recipient_key=public)
    assert (reply["ok"], reply["reason_code"]) == (False, DEV_CODE)
    assert contexts(env, ns)[0].state == "revoked" and env.release_calls == [] and env.batch_release_calls == []
    [event] = env.denied
    assert (event["event_type"], event["reason_code"], event["agent_binding_ref"]) == (
        "credential_acquired", DEV_CODE, f"development:{DIGEST}")


# --- children: the author's identity is not lent beyond the draft's decisions --------------------------------------------

def test_a_child_of_a_development_run_inherits_its_decisions_and_nothing_more(ns, env):
    cred = granted_elsewhere(env)
    _, ref = running_development(ns, env, cred)
    # a child dispatch carrying the parent's credential: sealed against the parent's decision
    child = call_kwargs(payload_of(cred), context_ref=ref)
    assert ns.adm.admit_dispatch(env.module(), "indexer_agent", child, None) is None
    assert sealed_tool_settings(child)["credential_slot"]["credential_ref"] == cred.ref_id
    # a child asking for a credential the draft was not admitted for: refused with the development code
    stray = Credential(env, uuid.uuid4(), tool_id=2, config_id=6, version_id=71)
    env.add_version(1, 71, 9)
    other = call_kwargs(payload_of(cred, stray), context_ref=ref)
    with pytest.raises(ns.exceptions.AdmissionDenied) as err:
        ns.adm.admit_dispatch(env.module(), "indexer_agent", other, None)
    assert (err.value.reason_code, err.value.message) == (
        "DENIED_DEVELOPMENT_CHILD", ns.adm.MESSAGES["DENIED_DEVELOPMENT_CHILD"])
    assert env.denied[-1]["reason_code"] == "DENIED_DEVELOPMENT_CHILD"


def test_a_child_of_a_governed_run_keeps_the_governed_refusal(ns, env):
    binding = env.seed_agent()
    cred = Credential(env, binding)
    call, ref = dispatch(env, ns, cred)
    run_to_running(env, ns, ref)
    stray = Credential(env, uuid.uuid4(), tool_id=2, config_id=6, version_id=71)
    env.add_version(1, 71, 9)
    with pytest.raises(ns.exceptions.AdmissionDenied) as err:
        ns.adm.admit_dispatch(env.module(), "indexer_agent", call_kwargs(payload_of(cred, stray), context_ref=ref), None)
    assert err.value.reason_code == "CREDENTIAL_REFERENCE_REQUIRED"


# --- the setting: default by mode, explicit under enforce ------------------------------------------------------------------

def test_the_setting_defaults_on_under_audit_only_and_off_under_enforce_until_chosen(ns):
    gs = ns.governance_settings

    def stored(mode, chosen=None):
        return gs.Stored(mode=mode, policy_revision="rev-1" if mode != "off" else None, admit_legacy_pat=False,
                         allow_runtime_domain_binding=False, environment=None, revision=1, updated_by=1, updated_at=None,
                         allow_development_runs=chosen)

    def effective(mode, chosen=None, ceiling="enforce"):
        return gs.resolve(stored(mode, chosen), ceiling=ceiling, env_environment=None, preflight=[], read="ok")

    # on under audit only, off under enforce and off: an enforce deployment keeps what it has (#7227 D14)
    assert gs.development_runs_default("audit_only") and not gs.development_runs_default("enforce")
    assert not gs.development_runs_default("off")
    assert effective("audit_only").allow_development_runs is True
    assert effective("enforce").allow_development_runs is False
    assert effective("enforce", chosen=True).allow_development_runs is True
    assert effective("off").allow_development_runs is False
    assert effective("enforce", chosen=True).allow_development_runs is True
    assert effective("audit_only", chosen=False).allow_development_runs is False
    assert effective("enforce", ceiling="audit_only").allow_development_runs is True   # the default follows the mode in effect
    assert gs.resolve(None, ceiling="enforce", env_environment=None, preflight=[], read="ok").allow_development_runs is False
    assert gs.SettingsUpdate.model_fields["allow_development_runs"].default is None
    assert "allow_development_runs" in gs.IGNORED_CONFIG_KEYS
    assert "ELITEA_CREDENTIAL_GOVERNANCE_ALLOW_DEVELOPMENT_RUNS" in gs.IGNORED_ENV
    assert gs.DESCRIPTIONS["fields"]["allow_development_runs"]["label"] == "Allow development runs"


def test_the_module_hands_the_setting_to_admission_as_of_one_dispatch(ns, env):
    effective = ns.governance_settings.Effective(
        mode="enforce", policy_revision="rev-1", admit_legacy_pat=False, allow_runtime_domain_binding=False,
        environment=None, capped=False, unavailable=False, read="ok", allow_development_runs=True)
    as_of = ns.adm._AsOf(env.module(), effective)  # pylint: disable=W0212
    assert as_of.admission_settings.allow_development_runs is True
    text = (PLUGIN_ROOT / "module.py").read_text()
    assert "allow_development_runs=effective.allow_development_runs" in text


# --- schedules and webhooks saved while developing are not development runs: the author is told -------------------------------

def test_an_unattended_trigger_saved_on_an_unbound_version_under_governance_carries_a_notice(ns, env):
    notice_for, text = ns.trigger_governance.governance_notice_for, ns.trigger_governance.UNBOUND_NOTICE
    unbound_version(env)
    with env.get_session(1) as session:
        for mode in ({"governed": True}, {"governed": False, "audit": True}):
            assert notice_for(env.module(**mode), session, 1, 7, 70, "schedule") == text
            assert notice_for(env.module(**mode), session, 1, 7, 70, "webhook") == text
            assert notice_for(env.module(**mode), session, 1, 7, 70, "chat_message") is None
        before = len(env.sessions)
        assert notice_for(env.module(governed=False), session, 1, 7, 70, "schedule") is None   # off: nothing is read
        assert len(env.sessions) == before
        env.add_binding(1, 7, 70)
        assert notice_for(env.module(), session, 1, 7, 70, "schedule") is None
        env.add_version(1, 71, 7)
        env.add_binding(1, 7, 71, state="suspended")
        assert notice_for(env.module(), session, 1, 7, 71, "schedule") == text
    assert "binds the version and approves a mandate" in text and "test chat is not affected" in text
    api = (PLUGIN_ROOT / "api" / "v2" / "pipeline_trigger.py").read_text()
    assert "governance_notice=governance_notice" in api and "governance_notice_for(" in api
    assert "governance_notice: Optional[str] = None" in (PLUGIN_ROOT / "models" / "pd" / "pipeline_trigger.py").read_text()


# --- the schema: a development context has no binding reference ---------------------------------------------------------------

def test_the_context_table_accepts_a_null_binding_reference_and_older_schemas_are_relaxed(ns):
    column = ns.models.ExecutionSecurityContext.__table__.c.agent_binding_ref
    assert column.nullable is True
    schema = (PLUGIN_ROOT / "utils" / "agent_identity_schema.py").read_text()
    assert "is_nullable = 'NO'" in schema and "DROP NOT NULL" in schema
    assert 'ALTER TABLE "p_{int(pid)}".{CONTEXT_TABLE} ALTER COLUMN {column} DROP NOT NULL' in schema
    assert 'NULLABLE_COLUMNS = (BINDING_REF_COLUMN, "application_id", "version_id")' in schema   # #7229: toolkit runs
    module_text = (PLUGIN_ROOT / "module.py").read_text()
    guard = module_text[module_text.index("def _ensure_agent_identity_schema"):module_text.index("    def init(self):")]
    assert "project_ids_with_not_null_binding_ref()" in guard and "apply_nullable_binding_ref(strict)" in guard
    tasks = (PLUGIN_ROOT / "methods" / "admin_tasks.py").read_text()
    task = tasks[tasks.index("def create_agent_identity_tables"):tasks.index("def manage_agent_identity_binding")]
    assert "apply_nullable_binding_ref(" in task and '"relaxed": len(relaxed)' in task
