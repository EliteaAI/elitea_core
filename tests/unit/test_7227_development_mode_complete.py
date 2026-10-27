"""C-1009 (#7227, #7229 item 2): development mode complete.

#7227: the sub-agents a run calls are admitted with it, each on its own decision: a bound one on its own binding and
grants, a draft one (under a draft parent) as a development child of the same author; their contexts are linked to the
caller's, the worker's releases and a fan-out child's dispatch are checked against them, and a governed parent reads
nothing more than before. #7229: a toolkit Test run and on-demand indexing are development runs of the caller, with a
reference-only payload. Real models and statements on SQLite (``fixtures/admission_env.py``), secrets facts version 5.
"""
import copy
import sys
import uuid
from pathlib import Path

import pytest

PLUGIN_ROOT = Path(__file__).parents[2]
sys.path.insert(0, str(PLUGIN_ROOT / "tests"))

from fixtures import admission_env  # noqa: E402  pylint: disable=C0413
from fixtures.governed_credentials import (  # noqa: E402  pylint: disable=C0413
    CALLER, RUN, SECRET, SUBJECT, Credential, call_kwargs, dumped, payload_of, run_to_running, sealed_tool_settings,
)

EDIT = "models.applications.application.update"
DIGEST = "adv1:" + "7" * 64
CHILD_DIGEST = "adv1:" + "8" * 64
DEV_CODE = "DENIED_DEVELOPMENT_CREDENTIAL"
TOOLKIT_TASK = "indexer_test_toolkit_tool"
TOOLKIT_CALLER = {"kind": "indexer_worker", "task_name": TOOLKIT_TASK, "run_id": RUN}


@pytest.fixture(scope="module")
def ns(tmp_path_factory):
    saved = dict(sys.modules)
    yield admission_env.install(PLUGIN_ROOT, tmp_path_factory.mktemp("development_mode_complete"))
    sys.modules.clear()
    sys.modules.update(saved)


@pytest.fixture
def env(ns):
    ns.env.reset()
    ns.env.facts_version = 5
    ns.env.permissions[42] = {"credentials.use", EDIT}
    return ns.env


def module(env, **settings):
    settings.setdefault("allow_development_runs", True)
    return env.module(**settings)


def contexts(env, ns):
    return sorted(env.query(1, ns.models.ExecutionSecurityContext), key=lambda row: row.attempt)


def admissions(env):
    return [event for _, event in env.recorded if event["event_type"] == "admission"]


def app_tool(application_id, version_id, tool_id):
    return {"id": tool_id, "type": "application",
            "settings": {"application_id": application_id, "application_version_id": version_id}}


def prefetch(payload, key, *tools):
    """A sub-agent as Main prefetches it: ``{name, description, version_details: {tools}}`` (``subagent_prefetch``)."""
    payload["application"].setdefault("subagent_version_details", {})[key] = {
        "name": f"agent {key}", "description": "", "version_details": {"tools": list(tools)}}
    return payload


def prefetched_settings(call, key, index=0, field="credentials"):
    return call["kwargs"]["application"]["subagent_version_details"][key]["version_details"]["tools"][index][
        "settings"][field]


def draft(env, application_id, version_id, digest):
    env.add_version(1, version_id, application_id)
    env.digests[version_id] = digest


class Tree:  # pylint: disable=R0903
    """A draft parent (7:70) calling a draft child (8:80, a project credential the author holds a grant on elsewhere),
    which calls a bound grandchild (9:90, a credential granted to the author on its own binding)."""

    def __init__(self, env, child_grants=True, grandchild=True):
        draft(env, 7, 70, DIGEST)
        draft(env, 8, 80, CHILD_DIGEST)
        self.child = Credential(env, uuid.uuid4(), tool_id=2, config_id=6, version_id=80, grants=child_grants)
        child_tools = [self.child.tool()]
        if grandchild:
            env.add_version(1, 90, 9)
            self.binding = env.add_binding(1, 9, 90)
            self.grandchild = Credential(env, self.binding, tool_id=3, config_id=7, version_id=90)
            child_tools.append(app_tool(9, 90, 902))
        self.payload = prefetch({"application": {"version_details": {"tools": [app_tool(8, 80, 802)]}}}, "8:80",
                                *child_tools)
        if grandchild:
            prefetch(self.payload, "9:90", self.grandchild.tool())


def dispatch(env, ns, payload, module_=None, **request):
    request.setdefault("entry_point", "editor_chat")
    call = call_kwargs(payload)
    ref = ns.adm.admit_dispatch(module_ or module(env), "indexer_agent", call, env.request(**request))
    return call, ref


def release(env, ns, ref, toolkit_id, slot, caller=CALLER):
    _private, public = ns.keypair()
    return ns.credential_release.release_credential(
        env.module(), project_id=1, context_ref=ref, probe_ref=None, caller=dict(caller),
        toolkit_instance_id=str(toolkit_id), field="credentials", slot=dict(slot), recipient_key=public)


# --- #7227: each sub-agent admitted on its own ---------------------------------------------------------------------

def test_a_draft_parent_admits_each_sub_agent_on_its_own_decision_linked_to_its_caller(ns, env):
    tree = Tree(env)
    call, ref = dispatch(env, ns, tree.payload)
    root, child, grandchild = contexts(env, ns)
    assert str(root.id) == ref and (root.profile, root.parent_context_ref, root.call_lineage) == ("development", None, [])
    assert (child.profile, child.application_id, child.version_id, child.agent_binding_ref, child.definition_digest) == (
        "development", 8, 80, None, CHILD_DIGEST)
    assert (child.parent_context_ref, child.call_lineage) == (root.id, [ref])
    assert (grandchild.profile, grandchild.agent_binding_ref, grandchild.application_id) == ("governed", tree.binding, 9)
    assert (grandchild.parent_context_ref, grandchild.call_lineage) == (child.id, [ref, str(child.id)])
    assert [row.attempt for row in (root, child, grandchild)] == [1, 2, 3]
    assert {row.subject["ref"] for row in (root, child, grandchild)} == {SUBJECT}   # the author, never a mandate
    assert [row.entry_point for row in (root, child, grandchild)] == ["editor_chat"] * 3
    # one admission event each, with its own label, and the caller's context as the parent call
    events = admissions(env)
    assert [(e["reason_code"], e["application_id"], e["agent_binding_ref"], e.get("parent_call_id")) for e in events] == [
        ("ADMITTED_DEVELOPMENT", 7, f"development:{DIGEST}", None),
        ("ADMITTED_DEVELOPMENT", 8, f"development:{CHILD_DIGEST}", ref),
        ("ADMITTED", 9, str(tree.binding), str(child.id)),
    ]
    # each sub-agent's toolkits are sealed against its own decision; nothing leaves Main in clear
    assert prefetched_settings(call, "8:80")["credential_slot"]["decision_id"] == child.allowed_bindings[0]["decision_id"]
    assert prefetched_settings(call, "9:90")["credential_slot"]["decision_id"] == \
        grandchild.allowed_bindings[0]["decision_id"]
    assert child.allowed_bindings[0]["grant_id"] == tree.child.grant_id   # the author's grant, on another binding
    assert grandchild.allowed_bindings[0]["grant_id"] == tree.grandchild.grant_id   # the grant of its own binding
    assert SECRET not in dumped(call)


def test_a_bound_sub_agent_is_decided_on_its_own_binding_not_on_the_authors_development_rule(ns, env):
    tree = Tree(env)
    env.grants.pop((1, tree.grandchild.key()))   # the author holds a grant on the credential, but not for this binding
    env.grants[(1, "|".join((str(uuid.uuid4()), "3", tree.grandchild.ref_id, "*", SUBJECT)))] = {
        "grant_id": "g-elsewhere", "state": "active", "revocation_epoch": 1, "valid_from": None, "valid_until": None}
    with pytest.raises(ns.exceptions.AdmissionDenied) as err:
        dispatch(env, ns, tree.payload)
    assert err.value.reason_code == "DENIED_GRANT" and contexts(env, ns) == []


def test_a_mandate_is_never_lent_to_a_sub_agent_of_a_draft(ns, env):
    tree = Tree(env)
    grant = env.grants.pop((1, tree.grandchild.key()))
    env.grants[(1, tree.grandchild.key(subject=f"mandate:{uuid.uuid4()}"))] = grant   # granted to a mandate only
    with pytest.raises(ns.exceptions.AdmissionDenied) as err:
        dispatch(env, ns, tree.payload)
    assert err.value.reason_code == "DENIED_GRANT"


def test_a_draft_sub_agent_with_a_credential_the_author_holds_no_grant_on_refuses_the_run(ns, env):
    tree = Tree(env, child_grants=False, grandchild=False)
    with pytest.raises(ns.exceptions.AdmissionDenied) as err:
        dispatch(env, ns, tree.payload)
    assert (err.value.reason_code, err.value.message) == (DEV_CODE, ns.adm.MESSAGES[DEV_CODE])
    assert contexts(env, ns) == [] and env.denied[-1]["reason_code"] == DEV_CODE


def test_a_sub_agent_that_is_not_a_version_of_its_application_refuses_a_draft(ns, env):
    tree = Tree(env, grandchild=False)
    tree.payload["application"]["subagent_version_details"]["8:999"] = {"version_details": {"tools": []}}
    with pytest.raises(ns.exceptions.AdmissionDenied) as err:
        dispatch(env, ns, tree.payload)
    assert err.value.reason_code == "DENIED_BINDING"


def test_a_governed_parent_admits_a_bound_sub_agent_on_its_own_binding(ns, env):
    parent = env.seed_agent()
    tree = Tree.__new__(Tree)
    env.add_version(1, 90, 9)
    binding = env.add_binding(1, 9, 90)
    own = Credential(env, binding, tool_id=3, config_id=7, version_id=90)
    payload = prefetch({"application": {"version_details": {"tools": [app_tool(9, 90, 902)]}}}, "9:90", own.tool())
    call, ref = dispatch(env, ns, payload, module_=env.module(), entry_point="chat_sio")
    root, child = contexts(env, ns)
    assert (root.profile, root.agent_binding_ref) == ("governed", parent)
    assert (child.profile, child.agent_binding_ref, child.parent_context_ref) == ("governed", binding, root.id)
    assert prefetched_settings(call, "9:90")["credential_slot"]["decision_id"] == child.allowed_bindings[0]["decision_id"]
    assert ref == str(root.id) and tree is not None


def test_a_governed_parent_runs_no_draft_and_its_credential_is_refused_as_before(ns, env):
    env.seed_agent()
    draft(env, 8, 80, CHILD_DIGEST)
    child = Credential(env, uuid.uuid4(), tool_id=2, config_id=6, version_id=80)   # the author holds a grant elsewhere
    payload = prefetch({"application": {"version_details": {"tools": [app_tool(8, 80, 802)]}}}, "8:80", child.tool())
    with pytest.raises(ns.exceptions.AdmissionDenied) as err:
        dispatch(env, ns, payload, module_=env.module(), entry_point="chat_sio")
    assert err.value.reason_code == "CREDENTIAL_REFERENCE_REQUIRED"   # no development inside a governed run
    [row] = contexts(env, ns)
    assert (row.state, row.revoked_reason) == ("revoked", "NOT_DISPATCHED")


def sessions_of(env, ns, payload, **request):
    before = len(env.sessions)
    dispatch(env, ns, payload, module_=env.module(), entry_point="chat_sio", **request)
    return len(env.sessions) - before


def test_the_governed_path_reads_nothing_more_when_its_sub_agents_need_no_decision_of_their_own(ns, env):
    def fresh():
        env.reset()
        env.facts_version = 5
        return Credential(env, env.seed_agent())

    cred = fresh()
    baseline = sessions_of(env, ns, payload_of(cred))
    cred = fresh()
    no_configuration = prefetch(payload_of(cred), "8:80", {"id": 55, "type": "x", "settings": {"url": "u"}})
    assert sessions_of(env, ns, no_configuration) == baseline
    cred = fresh()
    shares_the_toolkit = prefetch(payload_of(cred), "8:80", cred.tool())   # sealed with the run's own decision
    assert sessions_of(env, ns, shares_the_toolkit) == baseline
    assert len(contexts(env, ns)) == 1


def test_the_profile_of_a_sub_agent_is_decided_before_the_context_rows_are_written_in_one_transaction(ns, env):
    tree = Tree(env)
    before = len(env.sessions)
    dispatch(env, ns, tree.payload)
    writes = [session for _, session in env.sessions[before:] if session.commits]
    assert len(writes) == 1 and writes[0].commits == 1   # the run and its sub-agents: one transaction


# --- the worker: releases of an in-process sub-agent ----------------------------------------------------------------

def test_the_worker_releases_a_sub_agents_slot_against_the_sub_agents_own_context(ns, env):
    ns.need_transport()
    tree = Tree(env)
    call, ref = dispatch(env, ns, tree.payload)
    run_to_running(env, ns, ref)
    env.recorded.clear()
    reply = release(env, ns, ref, 2, prefetched_settings(call, "8:80")["credential_slot"])
    assert reply["ok"] is True and SECRET not in str(reply)
    [(_, event)] = env.recorded
    assert (event["agent_binding_ref"], event["application_id"], event["attempt"]) == (
        f"development:{CHILD_DIGEST}", 8, 2)
    env.recorded.clear()
    reply = release(env, ns, ref, 3, prefetched_settings(call, "9:90")["credential_slot"])
    assert reply["ok"] is True
    assert (env.recorded[0][1]["agent_binding_ref"], env.recorded[0][1]["attempt"]) == (str(tree.binding), 3)


def test_a_sub_agent_whose_binding_was_suspended_meanwhile_is_refused_and_ends_the_run(ns, env):
    tree = Tree(env)
    call, ref = dispatch(env, ns, tree.payload)
    run_to_running(env, ns, ref)
    with env.get_session(1) as session:
        session.execute(ns.sa.update(ns.models.AgentIdentityBinding).values(state="suspended"))
        session.commit()
    reply = release(env, ns, ref, 3, prefetched_settings(call, "9:90")["credential_slot"])
    assert (reply["ok"], reply["reason_code"]) == (False, "DENIED_BINDING")
    assert contexts(env, ns)[0].state == "revoked"   # the run, not only the sub-agent


def test_the_release_of_the_runs_own_slot_does_not_look_for_sub_agents(ns, env, monkeypatch):
    ns.need_transport()
    draft(env, 7, 70, DIGEST)
    cred = Credential(env, uuid.uuid4())
    call, ref = dispatch(env, ns, payload_of(cred))
    run_to_running(env, ns, ref)
    monkeypatch.setattr(ns.adm, "descendant_contexts", lambda *a: pytest.fail("looked for sub-agents"))
    assert release(env, ns, ref, 1, sealed_tool_settings(call)["credential_slot"])["ok"] is True


def test_a_slot_no_context_of_the_run_holds_is_refused(ns, env):
    tree = Tree(env)
    call, ref = dispatch(env, ns, tree.payload)
    run_to_running(env, ns, ref)
    slot = dict(prefetched_settings(call, "8:80")["credential_slot"], decision_id=str(uuid.uuid4()))
    assert release(env, ns, ref, 2, slot)["reason_code"] == "CREDENTIAL_REFERENCE_REQUIRED"


# --- a fan-out child dispatched as its own task, its HITL resume, the reconcile ------------------------------------------

def fan_out_child(call):
    """The payload the worker builds for a fan-out child of 8:80 from the parent's (sealed) prefetch."""
    details = copy.deepcopy(call["kwargs"]["application"]["subagent_version_details"]["8:80"]["version_details"])
    return {"application": {"id": 8, "version_id": 80, "version_details": details}}


def test_a_fan_out_child_of_a_draft_runs_under_its_own_context_with_its_own_token(ns, env, monkeypatch):
    tree = Tree(env)
    call, ref = dispatch(env, ns, tree.payload)
    child_ref = str(contexts(env, ns)[1].id)
    minted = []
    monkeypatch.setattr(ns.adm.internal_obo, "mint_for_context",
                        lambda project, context, task, payload: minted.append(context) or {"token": "t"})
    child = call_kwargs(fan_out_child(call), context_ref=ref)
    assert ns.adm.admit_dispatch(module(env), "indexer_agent", child, None) is None
    assert child["kwargs"]["envelope"]["context_ref"] == child["meta"]["context_ref"] == child_ref
    assert minted == [child_ref]   # minted for the child's context, not the parent's
    assert ns.adm.validate_context(env.module(), 1, child_ref, CALLER)["ok"] is True
    # the HITL resume replays the stash written from the sealed child payload: it names the child's context already
    resumed = copy.deepcopy(child)
    resumed["kwargs"].pop("internal_run_token", None)
    assert ns.adm.admit_dispatch(module(env), "indexer_agent", resumed, None) is None
    assert resumed["kwargs"]["envelope"]["context_ref"] == child_ref


def test_the_reconcile_of_a_draft_runs_under_the_parents_context_with_its_sub_agents_sealed(ns, env):
    tree = Tree(env)
    call, ref = dispatch(env, ns, tree.payload)
    reconcile = copy.deepcopy(call)
    assert ns.adm.admit_dispatch(module(env), "indexer_agent", reconcile, None) is None
    assert reconcile["kwargs"]["envelope"]["context_ref"] == ref


def test_a_fan_out_child_nothing_was_decided_for_is_refused_with_the_reason(ns, env):
    tree = Tree(env, grandchild=False)
    _, ref = dispatch(env, ns, tree.payload)
    stray = Credential(env, uuid.uuid4(), tool_id=4, config_id=8, version_id=81)
    child = call_kwargs({"application": {"id": 8, "version_id": 81, "version_details": {"tools": [stray.tool()]}}},
                        context_ref=ref)
    with pytest.raises(ns.exceptions.AdmissionDenied) as err:
        ns.adm.admit_dispatch(module(env), "indexer_agent", child, None)
    assert err.value.reason_code == "DENIED_DEVELOPMENT_CHILD"
    assert "checked for it when the draft started" in err.value.message


def test_a_fan_out_child_holds_its_context_only_while_its_run_does(ns, env):
    tree = Tree(env)
    call, ref = dispatch(env, ns, tree.payload)
    child = call_kwargs(fan_out_child(call), context_ref=ref)
    ns.adm.admit_dispatch(module(env), "indexer_agent", child, None)
    child_ref = child["kwargs"]["envelope"]["context_ref"]
    run_to_running(env, ns, ref)
    assert ns.adm.validate_context(env.module(), 1, child_ref, CALLER)["ok"] is True
    with env.get_session(1) as session:   # the run ends (stopped, completed or revoked)
        ns.adm._set_state(session, uuid.UUID(ref), "revoked", "EPOCH_MISMATCH")  # pylint: disable=W0212
        session.commit()
    assert ns.adm.validate_context(env.module(), 1, child_ref, CALLER)["reason_code"] == "STALE_CONTEXT"
    slot = child["kwargs"]["application"]["version_details"]["tools"][0]["settings"]["credentials"]["credential_slot"]
    assert release(env, ns, child_ref, 2, slot)["reason_code"] == "STALE_CONTEXT"


def test_a_credential_nothing_decided_for_in_a_fan_out_childs_sub_agent_is_refused(ns, env):
    tree = Tree(env)
    call, ref = dispatch(env, ns, tree.payload)
    reconcile = copy.deepcopy(call)
    stray = Credential(env, uuid.uuid4(), tool_id=4, config_id=8, version_id=81)
    prefetched = reconcile["kwargs"]["application"]["subagent_version_details"]["9:90"]["version_details"]["tools"]
    prefetched.append(stray.tool())   # a credential the bound leaf was never decided for
    with pytest.raises(ns.exceptions.AdmissionDenied) as err:
        ns.adm.admit_dispatch(module(env), "indexer_agent", reconcile, None)
    assert err.value.reason_code in ("CREDENTIAL_REFERENCE_REQUIRED", "DENIED_DEVELOPMENT_CHILD")


def test_audit_only_rehearses_each_sub_agent_and_writes_no_context(ns, env):
    tree = Tree(env)
    call, ref = dispatch(env, ns, tree.payload, module_=module(env, governed=False, audit=True))
    assert ref is None and contexts(env, ns) == [] and env.denied == []
    assert [(e["reason_code"], e["application_id"], e["would_deny"]) for e in admissions(env)] == [
        ("ADMITTED_DEVELOPMENT", 7, False), ("ADMITTED_DEVELOPMENT", 8, False), ("ADMITTED", 9, False)]
    assert "credential_slot" not in dumped(call)   # audit only never changes the real payload


# --- #7229: a toolkit Test run and on-demand indexing ---------------------------------------------------------------------

def toolkit_call(cred_or_tool, context_ref=None):
    tool = cred_or_tool.tool() if isinstance(cred_or_tool, Credential) else cred_or_tool
    payload = {"toolkit_config": tool, "tool_name": "get_issues", "tool_params": {}}
    return call_kwargs(payload, context_ref=context_ref, toolkit_config=copy.deepcopy(tool))


def toolkit_request(env, **over):
    values = dict(entry_point="toolkit_test", application_id=None, version_id=None, toolkit_id=1)
    values.update(over)
    return env.request(**values)


def admit_toolkit(env, ns, call, module_=None, **over):
    return ns.adm.admit_dispatch(module_ or module(env), TOOLKIT_TASK, call, toolkit_request(env, **over))


def refused_toolkit(env, ns, call, module_=None, **over):
    with pytest.raises(ns.exceptions.AdmissionDenied) as err:
        admit_toolkit(env, ns, call, module_, **over)
    return err.value


@pytest.mark.parametrize("entry_point", ["toolkit_test", "index_on_demand"])
def test_a_toolkit_run_is_a_development_run_of_the_caller_with_a_reference_only_payload(ns, env, entry_point):
    cred = Credential(env, uuid.uuid4())   # a project credential the caller holds a grant on
    call = toolkit_call(cred)
    ref = admit_toolkit(env, ns, call, entry_point=entry_point)
    [row] = contexts(env, ns)
    assert str(row.id) == ref and (row.profile, row.entry_point, row.application_id, row.version_id) == (
        "development", entry_point, None, None)
    assert row.agent_binding_ref is None and row.definition_digest.startswith("adv1:")
    assert row.allowed_bindings[0]["grant_id"] == cred.grant_id and row.subject["ref"] == SUBJECT
    [event] = admissions(env)
    assert (event["reason_code"], event["agent_binding_ref"], event["toolkit_instance_id"]) == (
        "ADMITTED_DEVELOPMENT", f"development:{row.definition_digest}", "1")
    assert call["kwargs"]["toolkit_config"]["settings"]["credentials"]["credential_slot"]["credential_ref"] == cred.ref_id
    assert SECRET not in dumped(call["kwargs"]) and SECRET not in dumped(call["meta"])   # the meta is the sealed copy
    assert call["meta"]["context_ref"] == ref


def test_the_toolkit_digest_is_of_the_stored_toolkit_and_changes_with_it(ns, env):
    cred = Credential(env, uuid.uuid4())
    admit_toolkit(env, ns, toolkit_call(cred))
    first = contexts(env, ns)[0].definition_digest
    with env.get_session(1) as session:
        session.execute(ns.sa.update(ns.EliteATool).where(ns.EliteATool.id == 1).values(type="jira", settings={
            "url": "https://other.example", "credentials": {"elitea_title": "t", "configuration_uuid": cred.uuid}}))
        session.commit()
    admit_toolkit(env, ns, toolkit_call(cred))
    assert contexts(env, ns)[-1].definition_digest != first


def test_a_toolkit_run_needs_development_runs_switched_on_and_a_signed_in_person(ns, env):
    cred = Credential(env, uuid.uuid4())
    error = refused_toolkit(env, ns, toolkit_call(cred), module_=module(env, allow_development_runs=False))
    assert (error.reason_code, error.message) == ("DEVELOPMENT_RUNS_DISABLED", ns.adm.MESSAGES["DEVELOPMENT_RUNS_DISABLED"])
    assert refused_toolkit(env, ns, toolkit_call(cred), run_kind="system").reason_code == "MANDATE_REQUIRED"
    env.verify_result = {"ok": True, "principal": admission_env.principal()}
    error = refused_toolkit(env, ns, toolkit_call(cred), module_=module(env, admit_legacy_pat=True),
                            caller_type="token", caller_reference=None)
    assert error.reason_code == "DEVELOPMENT_RUNS_DISABLED"   # a token is not a person testing a toolkit
    assert contexts(env, ns) == [] and env.denied[0]["reason_code"] == "DEVELOPMENT_RUNS_DISABLED"


def test_a_toolkit_run_with_a_project_credential_the_caller_holds_no_grant_on_is_refused(ns, env):
    cred = Credential(env, uuid.uuid4(), grants=False)
    assert refused_toolkit(env, ns, toolkit_call(cred)).reason_code == DEV_CODE


def test_the_callers_personal_credential_needs_no_grant(ns, env):
    cred = Credential(env, uuid.uuid4(), grants=False)
    cred.reference.update(ownership_class="personal", custodian_subject_ref=SUBJECT, custodian_user_id=42)
    assert admit_toolkit(env, ns, toolkit_call(cred)) is not None


def test_a_toolkit_run_is_held_to_the_credential_its_stored_toolkit_selects(ns, env):
    stored = Credential(env, uuid.uuid4())
    other = Credential(env, uuid.uuid4(), tool_id=2, config_id=6)
    tool = stored.tool()
    tool["settings"]["credentials"] = other.expanded()   # the dispatch swaps in another credential
    assert refused_toolkit(env, ns, toolkit_call(tool)).reason_code == "CREDENTIAL_REFERENCE_REQUIRED"


def test_a_toolkit_that_is_not_saved_declares_nothing(ns, env):
    cred = Credential(env, uuid.uuid4())
    assert refused_toolkit(env, ns, toolkit_call(cred), toolkit_id=999).reason_code == "CREDENTIAL_REFERENCE_REQUIRED"
    plain = {"id": None, "type": "x", "settings": {"url": "https://example"}}
    assert admit_toolkit(env, ns, toolkit_call(plain), toolkit_id=None) is not None


def test_the_attachment_read_is_not_admitted_and_may_carry_no_credential(ns, env):
    plain = {"id": None, "type": "artifact", "settings": {"bucket": "b"}}
    call = toolkit_call(plain)
    assert ns.adm.admit_dispatch(module(env), TOOLKIT_TASK, call, ns.adm.CREDENTIAL_FREE) is None
    assert contexts(env, ns) == [] and call["kwargs"][ns.adm.UNADMITTED_KWARG] is True   # the worker's marker
    # a caller cannot bring the marker: any other dispatch has it taken out
    forged = toolkit_call(plain)
    forged["kwargs"][ns.adm.UNADMITTED_KWARG] = True
    ns.adm.admit_dispatch(module(env), TOOLKIT_TASK, forged, toolkit_request(env, toolkit_id=None))
    assert ns.adm.UNADMITTED_KWARG not in forged["kwargs"] and forged["kwargs"]["envelope"]["context_ref"]
    cred = Credential(env, uuid.uuid4())
    with pytest.raises(ns.exceptions.AdmissionDenied) as err:
        ns.adm.admit_dispatch(module(env), TOOLKIT_TASK, toolkit_call(cred), ns.adm.CREDENTIAL_FREE)
    assert err.value.reason_code == "CREDENTIAL_REFERENCE_REQUIRED"


def test_a_toolkit_run_without_an_admission_request_is_refused_under_enforce(ns, env):
    cred = Credential(env, uuid.uuid4())
    with pytest.raises(ns.exceptions.AdmissionDenied) as err:
        ns.adm.admit_dispatch(module(env), TOOLKIT_TASK, toolkit_call(cred), None)
    assert err.value.reason_code == "IDENTITY_EVIDENCE_MISSING"


def test_off_reads_and_changes_nothing(ns, env):
    cred = Credential(env, uuid.uuid4())
    call = toolkit_call(cred)
    sent = copy.deepcopy(call)
    off = env.module(governed=False)
    env.poison()
    try:
        assert ns.adm.admit_dispatch(off, TOOLKIT_TASK, call, None) is None
    finally:
        env.unpoison()
    assert call == sent


def test_audit_only_rehearses_a_toolkit_run(ns, env):
    cred = Credential(env, uuid.uuid4())
    call = toolkit_call(cred)
    assert admit_toolkit(env, ns, call, module_=module(env, governed=False, audit=True)) is None
    assert contexts(env, ns) == [] and SECRET in dumped(call["kwargs"])
    [event] = admissions(env)
    assert (event["reason_code"], event["would_deny"]) == ("ADMITTED_DEVELOPMENT", False)


def test_the_worker_validates_and_releases_a_toolkit_run(ns, env):
    ns.need_transport()
    cred = Credential(env, uuid.uuid4())
    call = toolkit_call(cred)
    ref = admit_toolkit(env, ns, call)
    result = ns.adm.validate_context(env.module(), 1, ref, TOOLKIT_CALLER)
    assert result["ok"] is True and result["context"]["application_id"] is None
    slot = call["kwargs"]["toolkit_config"]["settings"]["credentials"]["credential_slot"]
    env.recorded.clear()
    reply = release(env, ns, ref, 1, slot, caller=TOOLKIT_CALLER)
    assert reply["ok"] is True
    assert env.recorded[0][1]["agent_binding_ref"] == f"development:{contexts(env, ns)[0].definition_digest}"


def test_the_toolkit_task_needs_a_worker_that_applies_slots():
    caps = (PLUGIN_ROOT / "utils" / "worker_capabilities.py").read_text()
    assert '"indexer_test_toolkit_tool": (CAPABILITY_CREDENTIAL_SLOTS_V1,)' in caps


# --- the trusted entries -----------------------------------------------------------------------------------------------

def function(text, name, until):
    start = text.index(f"def {name}(")
    return text[start:text.index(until, start + 1)]


def test_the_test_entry_says_which_kind_of_toolkit_run_from_the_tool_and_passes_its_admission():
    text = (PLUGIN_ROOT / "rpc" / "application.py").read_text()
    test = function(text, "test_toolkit_tool_sio", "def test_mcp_connection_sio(")
    assert "entry_point=ENTRY_INDEX_ON_DEMAND if tool_name == 'index_data' else ENTRY_TOOLKIT_TEST" in test
    assert "admission = CREDENTIAL_FREE if platform_read else admission_request_from(" in test
    assert test.count("admission=admission") == 2   # the test run and start_index_task
    assert "if not is_governed(self):" in test and test.index("if not is_governed(self):") < test.index(".unsecret(")
    assert "admitted_run=False" not in test   # admitted: its internal MCP gets the run token
    mcp_test = function(text, "test_mcp_connection_sio", "@web.rpc(")
    assert "admitted_run=False" in mcp_test   # not admitted, out of scope: keeps the user's token
    assert "except AdmissionDenied as denied:" in test and "_toolkit_run_refused(" in test
    assert "platform_read" not in (PLUGIN_ROOT / "sio" / "all.py").read_text()   # a client cannot say it
    assert "platform_read=True" in (PLUGIN_ROOT / "utils" / "attachments.py").read_text()
    index = (PLUGIN_ROOT / "utils" / "application_tools.py").read_text()
    assert "admission=admission," in function(index, "start_index_task", "def ")
    runtool = (PLUGIN_ROOT / "methods" / "runtool.py").read_text()
    assert "entry_point=ENTRY_TOOLKIT_TEST" in runtool and "admission=admission" in runtool and "sid=None" in runtool
    api = (PLUGIN_ROOT / "api" / "v2" / "test_toolkit_tool.py").read_text()
    assert "'message': result.get('error')}, 403" in api and "'message': e.error.get('error')}, 403" in api
    # the socket an async REST test streams to is the caller's own: its holder is who the run is admitted as
    assert "auth.current_user(auth_data=holder).get('id') != caller_id" in api
    # no payload-wide unsecret under governance: a template in the test parameters is refused, not passed on
    assert "is_governed(self) and has_secret_template(data.get('tool_params'))" in test


def test_scheduled_indexing_stays_refused_before_any_dispatch():
    text = (PLUGIN_ROOT / "rpc" / "index_scheduling.py").read_text()
    assert text.index("if is_governed(self):") < text.index("start_index_task(")


def test_the_internal_mcp_checks_see_the_toolkits_of_a_sub_agent_as_main_prefetches_it(ns):
    tool = {"id": 5, "type": "mcp", "settings": {"url": "https://x"}}
    payload = prefetch({"application": {"version_details": {"tools": []}}}, "8:80", tool)
    assert any(tools and tool in tools for tools in ns.internal_obo._tool_lists(payload))  # pylint: disable=W0212
