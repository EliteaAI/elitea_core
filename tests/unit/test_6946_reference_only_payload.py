"""C-1009 (#6946): a governed payload leaves Main with credential slots, never material.

Sealing at the dispatch chokepoint (``admit_dispatch``), the inherited dispatch, the free-form secret pre-check, the
worker capability the sealed payload needs, and the stash of a parallel child. Real models and statements on SQLite
files (see ``fixtures/admission_env.py``); the facts methods of ``configurations`` and ``secrets`` are keyed-data fakes.
"""
import copy
import importlib.util
import json
import sys
import types
import uuid
from pathlib import Path

import pytest

PLUGIN_ROOT = Path(__file__).parents[2]
sys.path.insert(0, str(PLUGIN_ROOT / "tests"))

from fixtures import admission_env  # noqa: E402  pylint: disable=C0413
from fixtures.governed_credentials import (  # noqa: E402  pylint: disable=C0413
    RUN, SECRET, Credential, call_kwargs, dispatch, dumped, payload_of, sealed_tool_settings, seeded,
)

@pytest.fixture(scope="module")
def parallel():
    """The parallel dispatch methods, loaded standalone against a minimal ``pylon.core.tools`` (they use only
    ``web.method`` and ``log``); whatever other suites left in ``sys.modules`` is put back."""
    names = ("pylon", "pylon.core", "pylon.core.tools")
    saved = {name: sys.modules.get(name) for name in names}
    quiet = types.SimpleNamespace(info=lambda *a, **k: None, warning=lambda *a, **k: None, error=lambda *a, **k: None,
                                  exception=lambda *a, **k: None, debug=lambda *a, **k: None)
    tools = types.ModuleType("pylon.core.tools")
    tools.log, tools.web = quiet, types.SimpleNamespace(method=lambda *a, **k: (lambda function: function))
    sys.modules.update({"pylon": types.ModuleType("pylon"), "pylon.core": types.ModuleType("pylon.core"),
                        "pylon.core.tools": tools})
    try:
        spec = importlib.util.spec_from_file_location("parallel_dispatch_6946", PLUGIN_ROOT / "methods" / "parallel_dispatch.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    finally:
        for name, previous in saved.items():
            if previous is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = previous
    return module


@pytest.fixture(scope="module")
def ns(tmp_path_factory):
    saved = dict(sys.modules)
    yield admission_env.install(PLUGIN_ROOT, tmp_path_factory.mktemp("reference_only"))
    sys.modules.clear()
    sys.modules.update(saved)


@pytest.fixture
def env(ns):
    ns.env.reset()
    return ns.env


def contexts(env, ns):
    return env.query(1, ns.models.ExecutionSecurityContext)


def entries_of(env, ns):
    [row] = contexts(env, ns)
    return row.allowed_bindings


# --- sealing at the dispatch chokepoint --------------------------------------------------------

def test_one_credential_is_replaced_by_its_slot_and_no_value_is_left_in_the_dispatch(ns, env):
    cred = seeded(env)
    call, context_ref = dispatch(env, ns, cred)
    config = sealed_tool_settings(call)
    [entry] = entries_of(env, ns)
    assert config["credential_slot"] == {
        "v": 1, "credential_ref": cred.ref_id, "credential_generation": 1, "decision_id": entry["decision_id"],
        "fields": ["token"]}
    assert "token" not in config
    # the non-secret fields stay as expansion left them
    assert config["base_url"] == "https://jira.example" and config["configuration_uuid"] == cred.uuid
    assert config["configuration_type"] == "jira" and config["configuration_generation"] == 3
    assert SECRET not in dumped(call)
    # and the envelope / meta carry the context as before
    assert call["kwargs"]["envelope"]["context_ref"] == context_ref == call["meta"]["context_ref"]


def test_two_toolkits_of_one_configuration_get_their_own_decision_each(ns, env):
    first = seeded(env)
    second = Credential(env, first.binding, tool_id=2, config_id=first.config_id, cfg_uuid=first.uuid,
                        reference=False, grants=False)
    env.grants[(1, "|".join((str(first.binding), "2", first.ref_id, "*", "idp:tid-1:oid-42")))] = {
        "grant_id": str(uuid.uuid4()), "state": "active", "revocation_epoch": 1, "valid_from": None, "valid_until": None,
        "operations": ["*"]}
    call, _ = dispatch(env, ns, first, second)
    slots = [sealed_tool_settings(call, index)["credential_slot"] for index in (0, 1)]
    by_toolkit = {entry["toolkit_instance_id"]: entry["decision_id"] for entry in entries_of(env, ns)}
    assert [slot["decision_id"] for slot in slots] == [by_toolkit["1"], by_toolkit["2"]]
    assert len({slot["decision_id"] for slot in slots}) == 2 and SECRET not in dumped(call)


def test_the_request_tools_and_the_version_tools_share_their_dicts_and_are_sealed_once(ns, env):
    cred = seeded(env)
    tool = cred.tool()
    payload = {"tools": [tool], "application": {"version_details": {"tools": [tool]}}}
    call, _ = dispatch(env, ns, payload=payload)
    config = tool["settings"]["credentials"]
    assert config["credential_slot"]["fields"] == ["token"] and "token" not in config
    assert SECRET not in dumped(call)


def test_an_empty_secret_field_is_removed_but_not_asked_for(ns, env):
    cred = seeded(env)
    env.secret_fields["jira"] = ["token", "password"]
    payload = payload_of(cred)
    config = payload["application"]["version_details"]["tools"][0]["settings"]["credentials"]
    config["password"] = ""
    call, _ = dispatch(env, ns, payload=payload)
    sealed = sealed_tool_settings(call)
    assert "password" not in sealed and "token" not in sealed
    assert sealed["credential_slot"]["fields"] == ["token"]


def test_a_nested_secret_path_is_removed_and_listed_with_its_dots(ns, env):
    cred = seeded(env, ctype="github")
    call, _ = dispatch(env, ns, cred)
    sealed = sealed_tool_settings(call)
    assert sealed["credential_slot"]["fields"] == ["auth.token", "app_key"]
    assert sealed["auth"] == {} and "app_key" not in sealed
    assert SECRET not in dumped(call)


def test_a_configuration_that_is_not_a_credential_keeps_its_values_and_is_not_asked_about(ns, env):
    cred = seeded(env)
    payload = payload_of(cred)
    pgvector = {"configuration_uuid": str(uuid.uuid4()), "configuration_project_id": 1,
                "configuration_type": "pgvector", "connection_string": "postgres://internal"}
    payload["application"]["version_details"]["tools"].append({"id": 9, "type": "x", "settings": {"store": pgvector}})
    env.configurations[(1, pgvector["configuration_uuid"])] = {
        "id": 99, "project_id": 1, "type": "pgvector", "section": "vector_storage", "generation": 1}
    call, _ = dispatch(env, ns, payload=payload)
    assert pgvector == payload["application"]["version_details"]["tools"][1]["settings"]["store"]
    assert "credential_slot" not in pgvector and pgvector["connection_string"] == "postgres://internal"
    assert env.secret_field_calls == [["jira"]]   # only the credential's type was looked up


def test_an_agent_without_credentials_seals_nothing_and_reads_nothing_more(ns, env):
    env.seed_agent()
    env.add_toolkit(1, 70, 1, "github", {"url": "https://x", "note": "plain"})
    call, _ = dispatch(env, ns, payload={"tools": [{"id": 1, "type": "github", "settings": {"note": "plain"}}]})
    assert env.secret_field_calls == [] and env.configuration_calls == [] and env.secrets_calls == []
    assert call["kwargs"]["tools"][0]["settings"] == {"note": "plain"}


def test_the_context_view_carries_the_decisions_and_nothing_secret(ns, env):
    cred = seeded(env)
    payload = payload_of(cred)
    view = ns.adm.admit_execution(env.module(), env.request(), run_id=RUN, payload=payload)
    assert [entry["credential_ref"] for entry in view.allowed_bindings] == [cred.ref_id]
    assert SECRET not in view.model_dump_json()


def test_sealing_the_sealed_payload_again_changes_nothing(ns, env):
    cred = seeded(env)
    call, _ = dispatch(env, ns, cred)
    sealed = copy.deepcopy(call["kwargs"])
    ns.credential_slots.seal_payload(call["kwargs"], lambda: entries_of(env, ns))
    assert call["kwargs"] == sealed


# --- the sub-agents the payload carries ready-expanded ------------------------------------------

def with_prefetched(payload, *tools):
    payload["application"]["subagent_version_details"] = {"7:71": {"tools": list(tools)}}
    return payload


def test_a_prefetched_sub_agent_toolkit_of_the_run_is_sealed_with_the_runs_decision(ns, env):
    cred = seeded(env)
    payload = with_prefetched(payload_of(cred), cred.tool())   # the same toolkit, expanded again for the sub-agent
    call, _ = dispatch(env, ns, payload=payload)
    prefetched = call["kwargs"]["application"]["subagent_version_details"]["7:71"]["tools"][0]["settings"]["credentials"]
    [entry] = entries_of(env, ns)
    assert prefetched["credential_slot"]["decision_id"] == entry["decision_id"] and "token" not in prefetched
    assert SECRET not in dumped(call)


def test_a_prefetched_sub_agent_with_a_credential_nobody_admitted_is_refused(ns, env):
    cred = seeded(env)
    foreign = Credential(env, cred.binding, tool_id=41, config_id=77, reference=False, grants=False, version_id=71)
    call = call_kwargs(with_prefetched(payload_of(cred), foreign.tool()))
    with pytest.raises(ns.exceptions.AdmissionDenied) as err:
        ns.adm.admit_dispatch(env.module(), "indexer_agent", call, env.request())
    assert err.value.reason_code == "CREDENTIAL_REFERENCE_REQUIRED"
    [row] = contexts(env, ns)
    assert (row.state, row.revoked_reason) == ("revoked", "NOT_DISPATCHED")


def test_a_prefetched_sub_agent_with_only_internal_configurations_is_left_alone(ns, env):
    cred = seeded(env)
    internal = {"configuration_uuid": str(uuid.uuid4()), "configuration_project_id": 1,
                "configuration_type": "pgvector", "connection_string": "postgres://internal"}
    env.configurations[(1, internal["configuration_uuid"])] = {
        "id": 97, "project_id": 1, "type": "pgvector", "section": "vector_storage", "generation": 1}
    payload = with_prefetched(payload_of(cred), {"id": 55, "type": "x", "settings": {"store": internal}})
    call, _ = dispatch(env, ns, payload=payload)
    assert internal["connection_string"] == "postgres://internal" and "credential_slot" not in internal
    assert SECRET not in dumped(call)


def test_a_secret_template_in_a_prefetched_sub_agent_is_found_before_the_unsecret(ns, env):
    payload = with_prefetched(
        {"application": {"version_details": {"tools": []}}},
        {"id": 55, "type": "x", "settings": {"headers": {"X-Key": "{{secret.api_key}}"}}})
    assert ns.credential_slots.has_free_form_secret(env.module(), payload) is True


# --- seal_payload on its own: the refusals -----------------------------------------------------

def entry_for(cred, **over):
    entry = {"toolkit_instance_id": str(cred.tool_id), "field": cred.field, "configuration_uuid": cred.uuid,
             "credential_ref": cred.ref_id, "credential_generation": 1, "decision_id": str(uuid.uuid4())}
    entry.update(over)
    return entry


def seal(ns, env, payload, entries, classify=None, **kwargs):
    return ns.credential_slots.seal_payload(
        payload, lambda: entries, secret_fields_of=lambda types: {t: env.secret_fields.get(t, []) for t in types
                                                                   if t not in env.unregistered_types},
        classify=classify, **kwargs)


def refused(ns, env, payload, entries, classify=None, code="CREDENTIAL_REFERENCE_REQUIRED"):
    with pytest.raises(ns.credential_decision.CredentialDecisionError) as err:
        seal(ns, env, payload, entries, classify)
    assert err.value.reason_code == code
    return err.value


def test_a_credential_without_a_decision_is_refused_when_it_is_a_credential(ns, env):
    cred = seeded(env)
    payload = payload_of(cred)
    facts = lambda pairs: {cfg_uuid: {"section": "credentials"} for cfg_uuid, _ in pairs}   # noqa: E731
    refused(ns, env, payload, [], classify=facts)
    # a decision for another toolkit, another field or another configuration does not count
    for wrong in ({"toolkit_instance_id": "99"}, {"field": "other"}, {"configuration_uuid": str(uuid.uuid4())}):
        refused(ns, env, payload_of(cred), [entry_for(cred, **wrong)], classify=facts)


def test_a_configuration_whose_section_is_unknown_counts_as_a_credential(ns, env):
    cred = seeded(env)
    refused(ns, env, payload_of(cred), [], classify=lambda pairs: {})
    refused(ns, env, payload_of(cred), [], classify=lambda pairs: {cred.uuid: None})


def test_a_nested_credential_is_refused_it_cannot_match_a_decision(ns, env):
    cred = seeded(env)
    payload = {"tools": [{"id": 1, "type": "x", "settings": {"wrapper": {"inner": cred.expanded()}}}]}
    facts = lambda pairs: {cfg_uuid: {"section": "credentials"} for cfg_uuid, _ in pairs}   # noqa: E731
    refused(ns, env, payload, [entry_for(cred, field="wrapper")], classify=facts)
    assert SECRET in dumped(payload)   # nothing was sealed, nothing was dispatched


def test_a_configuration_without_a_uuid_cannot_be_shown_to_be_internal(ns, env):
    payload = {"tools": [{"id": 1, "type": "x", "settings": {"store": {"configuration_type": "pgvector", "dsn": "x"}}}]}
    refused(ns, env, payload, [], classify=lambda pairs: pytest.fail("asked"))


def test_a_type_the_configurations_plugin_does_not_know_is_refused(ns, env):
    cred = seeded(env)
    env.unregistered_types.add("jira")
    refused(ns, env, payload_of(cred), [entry_for(cred)])


def test_the_secret_fields_lookup_failing_is_a_dependency_refusal_and_never_a_pass(ns, env):
    cred = seeded(env)

    def boom(types):
        raise RuntimeError("configurations down: " + SECRET)

    payload = payload_of(cred)
    with pytest.raises(ns.credential_decision.CredentialDecisionError) as err:
        ns.credential_slots.seal_payload(payload, lambda: [entry_for(cred)], secret_fields_of=boom)
    assert err.value.reason_code == "DEPENDENCY_UNAVAILABLE" and SECRET not in str(err.value)


def test_a_slot_that_is_not_the_decisions_is_refused_and_the_decisions_own_is_kept(ns, env):
    cred = seeded(env)
    entry = entry_for(cred)
    payload = payload_of(cred)
    seal(ns, env, payload, [entry])
    for field, value in (("decision_id", str(uuid.uuid4())), ("credential_generation", 9),
                         ("credential_ref", str(uuid.uuid4()))):
        forged = copy.deepcopy(payload)
        forged["application"]["version_details"]["tools"][0]["settings"]["credentials"]["credential_slot"][field] = value
        refused(ns, env, forged, [entry])
    again = copy.deepcopy(payload)
    assert seal(ns, env, again, [entry]) == 1 and again == payload


def test_a_configuration_shared_by_two_toolkits_cannot_name_two_decisions(ns, env):
    cred = seeded(env)
    shared = cred.expanded()
    payload = {"tools": [{"id": 1, "type": "x", "settings": {"credentials": shared}},
                         {"id": 2, "type": "x", "settings": {"credentials": shared}}]}
    refused(ns, env, payload, [entry_for(cred), entry_for(cred, toolkit_instance_id="2")])


def test_a_secret_path_that_survives_removal_is_refused(ns, env, monkeypatch):
    cred = seeded(env)
    monkeypatch.setattr(ns.credential_slots, "_remove_path", lambda node, path: True)
    refused(ns, env, payload_of(cred), [entry_for(cred)])


def test_the_decisions_are_asked_only_when_the_payload_carries_a_configuration(ns, env):
    payload = {"tools": [{"id": 1, "type": "x", "settings": {"note": "plain"}}]}
    assert ns.credential_slots.seal_payload(payload, lambda: pytest.fail("asked")) == 0
    with pytest.raises(ns.credential_decision.CredentialDecisionError) as err:
        ns.credential_slots.seal_payload(payload_of(seeded(env)), lambda: "not a list")
    assert err.value.reason_code == "STALE_CONTEXT"


# --- a seal refusal after admission ends the run it admitted -----------------------------------

def test_a_refusal_at_the_seal_revokes_the_context_audits_and_dispatches_nothing(ns, env):
    cred = seeded(env)
    env.secret_fields_raises = RuntimeError("configurations down")
    call = call_kwargs(payload_of(cred))
    before = ns.adm.CREDENTIAL_DECISION.total("deny", "DEPENDENCY_UNAVAILABLE")
    with pytest.raises(ns.exceptions.AdmissionDenied) as err:
        ns.adm.admit_dispatch(env.module(), "indexer_agent", call, env.request())
    assert err.value.reason_code == "DEPENDENCY_UNAVAILABLE"
    [row] = contexts(env, ns)
    assert (row.state, row.revoked_reason) == ("revoked", "NOT_DISPATCHED")
    assert call["kwargs"]["envelope"]["context_ref"] is None and "context_ref" not in call["meta"]
    [event] = env.denied
    assert (event["decision"], event["reason_code"], event["run_id"]) == ("deny", "DEPENDENCY_UNAVAILABLE", RUN)
    assert ns.adm.CREDENTIAL_DECISION.total("deny", "DEPENDENCY_UNAVAILABLE") == before + 1


# --- the inherited dispatch ---------------------------------------------------------------------

def child_of(cred_payload, parent):
    return call_kwargs(cred_payload, parent)


def test_a_child_dispatch_with_the_parents_credential_is_sealed_against_the_parents_decision(ns, env):
    cred = seeded(env)
    _, parent = dispatch(env, ns, cred)
    [entry] = entries_of(env, ns)
    child = child_of(payload_of(cred), parent)   # a child payload built from an unsealed source
    assert ns.adm.admit_dispatch(env.module(), "indexer_agent", child, None) is None
    config = sealed_tool_settings(child)
    assert config["credential_slot"]["decision_id"] == entry["decision_id"] and "token" not in config
    assert SECRET not in dumped(child) and child["meta"]["context_ref"] == parent


def test_a_reconcile_dispatch_of_an_already_sealed_payload_passes_unchanged(ns, env):
    cred = seeded(env)
    first, parent = dispatch(env, ns, cred)
    reconcile = call_kwargs(copy.deepcopy(first["kwargs"]), parent)
    expected = copy.deepcopy(reconcile["kwargs"])
    assert ns.adm.admit_dispatch(env.module(), "indexer_agent", reconcile, None) is None
    assert reconcile["kwargs"] == expected


def test_the_parents_credential_in_another_toolkit_is_no_longer_covered(ns, env):
    cred = seeded(env)
    _, parent = dispatch(env, ns, cred)
    other = Credential(env, cred.binding, tool_id=31, config_id=cred.config_id, cfg_uuid=cred.uuid, reference=False,
                       grants=False)
    child = child_of(payload_of(other), parent)
    with pytest.raises(ns.exceptions.AdmissionDenied) as err:
        ns.adm.admit_dispatch(env.module(), "indexer_agent", child, None)
    assert err.value.reason_code == "CREDENTIAL_REFERENCE_REQUIRED"
    assert "context_ref" not in child["meta"]


def test_a_child_carrying_a_foreign_credential_is_refused_before_anything_is_sealed(ns, env):
    cred = seeded(env)
    _, parent = dispatch(env, ns, cred)
    foreign = Credential(env, cred.binding, tool_id=41, config_id=77, reference=False, grants=False)
    child = child_of(payload_of(foreign), parent)
    with pytest.raises(ns.exceptions.AdmissionDenied):
        ns.adm.admit_dispatch(env.module(), "indexer_agent", child, None)
    assert SECRET in dumped(child)   # and nothing was dispatched


def test_a_child_with_an_internal_configuration_inherits_it_untouched(ns, env):
    cred = seeded(env)
    _, parent = dispatch(env, ns, cred)
    payload = payload_of(cred)
    internal = {"configuration_uuid": str(uuid.uuid4()), "configuration_project_id": 1,
                "configuration_type": "pgvector", "connection_string": "postgres://internal"}
    env.configurations[(1, internal["configuration_uuid"])] = {
        "id": 98, "project_id": 1, "type": "pgvector", "section": "vector_storage", "generation": 1}
    payload["application"]["version_details"]["tools"].append({"id": 8, "type": "x", "settings": {"store": internal}})
    child = child_of(payload, parent)
    assert ns.adm.admit_dispatch(env.module(), "indexer_agent", child, None) is None
    assert internal["connection_string"] == "postgres://internal" and "credential_slot" not in internal


# --- free-form secrets, resolved by the payload-wide unsecret -----------------------------------

def test_a_secret_template_outside_a_configuration_is_found_and_one_inside_is_not(ns, env):
    module = env.module()
    cred = seeded(env)
    plain = {"tools": [{"id": 1, "type": "x", "settings": {"headers": {"X-Key": "{{secret.api_key}}"}}}]}
    assert ns.credential_slots.has_free_form_secret(module, plain) is True
    inside = {"tools": [{"id": 1, "type": "x", "settings": {"credentials": {**cred.expanded(), "token": "{{secret.t}}"}}}]}
    assert ns.credential_slots.has_free_form_secret(module, inside) is False
    assert ns.credential_slots.has_free_form_secret(module, payload_of(cred)) is False


def test_a_payload_too_large_or_deep_to_scan_counts_as_holding_one(ns, env):
    deep = node = {}
    for _ in range(ns.credential_slots.MAX_SCAN_DEPTH + 2):
        node["n"] = {}
        node = node["n"]
    assert ns.credential_slots.has_free_form_secret(
        env.module(), {"tools": [{"id": 1, "settings": {"deep": deep}}]}) is True


def test_the_scan_reads_nothing_when_governance_is_off(ns, env):
    env.poison()
    payload = {"tools": [{"id": 1, "type": "x", "settings": {"headers": {"X-Key": "{{secret.api_key}}"}}}]}
    assert ns.credential_slots.has_free_form_secret(env.module(governed=False), payload) is False
    env.unpoison()


def test_a_marked_payload_is_refused_at_the_gate_and_the_mark_never_travels(ns, env):
    cred = seeded(env)
    payload = payload_of(cred)
    ns.credential_slots.mark_free_form_secret(payload, True)
    call = call_kwargs(payload)
    before = ns.adm.CREDENTIAL_DECISION.total("deny", "CREDENTIAL_REFERENCE_REQUIRED")
    with pytest.raises(ns.exceptions.AdmissionDenied) as err:
        ns.adm.admit_dispatch(env.module(), "indexer_agent", call, env.request())
    assert err.value.reason_code == "CREDENTIAL_REFERENCE_REQUIRED"
    assert ns.credential_slots.FREE_FORM_SECRET_MARK not in call["kwargs"]
    assert contexts(env, ns) == []   # refused before an admission was made
    assert ns.adm.CREDENTIAL_DECISION.total("deny", "CREDENTIAL_REFERENCE_REQUIRED") == before + 1
    # an unmarked payload goes through, and marking nothing leaves the payload as it was
    clean = payload_of(cred)
    ns.credential_slots.mark_free_form_secret(clean, False)
    assert ns.credential_slots.FREE_FORM_SECRET_MARK not in clean


def test_the_predict_entry_points_check_before_they_unsecret_and_mark_after():
    for path in ("rpc/application.py", "methods/predict.py"):
        text = (PLUGIN_ROOT / path).read_text()
        for site in [i for i in range(len(text)) if text.startswith("has_free_form_secret(self, payload)", i)]:
            after = text[site:site + 600]
            assert after.index(".unsecret(payload)") < after.index("mark_free_form_secret(payload, free_form_secret)")
    assert (PLUGIN_ROOT / "rpc/application.py").read_text().count("mark_free_form_secret(payload, free_form_secret)") == 2
    assert (PLUGIN_ROOT / "methods/predict.py").read_text().count("mark_free_form_secret(payload, free_form_secret)") == 1


# --- the worker capability a sealed payload needs ----------------------------------------------

GOVERNED = types.SimpleNamespace(enabled=True, policy_revision="r1", profile="governed")


def registry(ns, *capabilities):
    reg = ns.caps.WorkerCapabilityRegistry(ttl_s=120)
    reg.on_capabilities("e", {"ident": "w1", "pool": "agents", "capabilities": list(capabilities), "ts": 1.0})
    return reg


def test_a_governed_agent_task_needs_a_worker_that_applies_credential_slots(ns):
    call = {"kwargs": {}}
    with pytest.raises(ns.exceptions.WorkerCapabilityUnavailable) as err:
        ns.caps.apply_envelope_gate("indexer_agent", call, GOVERNED, registry(ns, "task_envelope.v1"))
    assert err.value.reason_code == "WORKER_CAPABILITY_UNAVAILABLE" and "credential_slots.v1" in str(err.value)
    call = {"kwargs": {}}
    ns.caps.apply_envelope_gate("indexer_predict_agent", call, GOVERNED, registry(ns, "task_envelope.v1", "credential_slots.v1"))
    assert call["kwargs"]["envelope"]["required_capabilities"] == ["credential_slots.v1", "task_envelope.v1"]


def test_the_probe_also_needs_the_governed_ops_pool(ns):
    base = ("task_envelope.v1", "credential_slots.v1")
    with pytest.raises(ns.exceptions.WorkerCapabilityUnavailable):
        ns.caps.apply_envelope_gate("indexer_governed_check_connection", {"kwargs": {}}, GOVERNED, registry(ns, *base))
    call = {"kwargs": {}}
    ns.caps.apply_envelope_gate("indexer_governed_check_connection", call, GOVERNED, registry(ns, *base, "governed_ops_pool.v1"))
    assert call["kwargs"]["envelope"]["required_capabilities"] == [
        "credential_slots.v1", "governed_ops_pool.v1", "task_envelope.v1"]


def test_other_enveloped_tasks_and_the_legacy_profile_are_not_asked_for_credential_slots(ns):
    call = {"kwargs": {}}
    ns.caps.apply_envelope_gate("indexer_test_toolkit_tool", call, GOVERNED, registry(ns, "task_envelope.v1"))
    assert call["kwargs"]["envelope"]["required_capabilities"] == ["task_envelope.v1"]
    legacy = types.SimpleNamespace(enabled=False, policy_revision=None, profile="legacy")
    call = {"kwargs": {}}
    ns.caps.apply_envelope_gate("indexer_agent", call, legacy, registry(ns))
    assert call["kwargs"]["envelope"] == {"schema_version": 1, "security_profile": "legacy",
                                          "required_capabilities": [], "context_ref": None}


def test_the_probe_task_carries_an_envelope(ns):
    assert "indexer_governed_check_connection" in ns.caps.ENVELOPED_TASKS


# --- the parallel child stash -----------------------------------------------------------------

def make_launcher(parallel, ns, env, governed):
    return type("Launcher", (Launcher, parallel.Method), {})(ns, env, governed)


class Launcher:
    """The parallel dispatch methods over a recording stash, with ``start_task`` through the real gate."""

    def __init__(self, ns, env, governed):
        self.module = env.module(governed=governed)
        self.governance_profile = self.module.governance_profile
        self.stashed, self.order, self.dispatched = [], [], []
        self.parent_context = None
        self.refuse = False
        self.task_node = types.SimpleNamespace(start_task=self.start_task)
        self.ns, self.env = ns, env
        self.redis = types.SimpleNamespace(set=lambda *a, **k: True, hset=lambda *a, **k: 1)

    def get_redis_client(self):
        return self.redis

    def _parallel_reconcile_stash(self, *_args, **_kwargs):
        return None

    def _parallel_prime_gate(self, *_args, **_kwargs):
        return None

    def _parallel_set_child_task_ids(self, *_args, **_kwargs):
        return None

    def _parallel_settle_child(self, parent_thread_id, epoch, child_thread_id, terminal_error=None):
        self.order.append("settle")

    def _parallel_child_stash(self, child_thread_id, child_payload, child_meta, *_args):
        self.order.append("stash")
        self.stashed.append((child_thread_id, json.dumps(child_payload, sort_keys=True), dict(child_meta)))

    def start_task(self, name, **kwargs):
        self.order.append("start")
        if self.refuse:
            raise self.ns.exceptions.AdmissionDenied("CREDENTIAL_REFERENCE_REQUIRED", "refused")
        self.ns.adm.admit_dispatch(self.module, name, kwargs, None)
        self.dispatched.append(kwargs)
        return "child-task"


def launch(launcher, child_payload):
    launcher.parallel_dispatch_launch_children(
        "parent-task", {"task_name": "indexer_agent", "project_id": 1},
        {"thread_id": "root", "dispatch_epoch": "e1", "parallel_dispatch": [{
            "dispatch_id": "e1:c", "tool_call_id": "c", "child_thread_id": "root:B:c", "child_payload": child_payload,
            "name": "B", "display_name": "B"}]})


def test_a_governed_child_is_stashed_after_start_task_and_the_stash_holds_the_sealed_dict(ns, env, parallel):
    cred = seeded(env)
    _, parent = dispatch(env, ns, cred)
    launcher = make_launcher(parallel, ns, env, governed=True)
    launch(launcher, call_kwargs(payload_of(cred), parent)["kwargs"])
    assert launcher.order == ["start", "stash"]
    [(_, stashed, _)] = launcher.stashed
    assert SECRET not in stashed and '"credential_slot"' in stashed


def test_a_governed_child_the_gate_refused_is_never_stashed(ns, env, parallel):
    cred = seeded(env)
    _, parent = dispatch(env, ns, cred)
    launcher = make_launcher(parallel, ns, env, governed=True)
    launcher.refuse = True
    launch(launcher, call_kwargs(payload_of(cred), parent)["kwargs"])
    assert launcher.order == ["start", "settle"] and launcher.stashed == []


def test_a_legacy_child_is_stashed_before_start_task_as_it_always_was(ns, env, parallel):
    launcher = make_launcher(parallel, ns, env, governed=False)
    payload = {"thread_id": "root:B:c", "tools": [{"id": 1, "settings": {"token": "legacy-token-value"}}]}
    launch(launcher, payload)
    assert launcher.order == ["stash", "start"]
    assert "legacy-token-value" in launcher.stashed[0][1]


# --- flag off: the dispatch is what it was -------------------------------------------------------

def legacy_envelope():
    return {"schema_version": 1, "security_profile": "legacy", "required_capabilities": [], "context_ref": None}


def flag_off_dispatch(ns, env, payload, task="indexer_agent"):
    """The seams of the start_task wrapper in order, as module.py runs them."""
    module = env.module(governed=False)
    call = {"args": ["s", "m"], "kwargs": copy.deepcopy(payload), "pool": "agents", "meta": {"project_id": 1}}
    ns.caps.apply_envelope_gate(task, call, module.governance_profile, module.worker_capabilities)
    ns.run_id.stamp_predict_run_id(call)
    assert ns.adm.admit_dispatch(module, task, call, env.request()) is None
    return call


@pytest.mark.parametrize("task", [
    "indexer_agent",                 # chat, pipeline schedule and webhook, agent predict, parallel child and reconcile
    "indexer_predict_agent",         # plain LLM chat
    "indexer_test_toolkit_tool",     # the test tool (enveloped, never admitted)
    "indexer_configuration_check_connection",   # the ungoverned check-connection task
])
def test_flag_off_the_dispatch_payload_is_deep_equal_to_what_it_was_and_nothing_is_called(ns, env, task):
    cred = seeded(env)
    payload = payload_of(cred, project_id=1, message_id=5)
    payload["tools"] = [{"id": 8, "type": "x", "settings": {"headers": {"X-Key": "resolved-secret-value"}}}]
    payload["application"]["version_details"]["tools"][0]["settings"]["credentials"]["private_key"] = "pk-secret"
    before = copy.deepcopy(payload)
    env.poison()
    env.redis.raises = AssertionError("redis was used")
    call = flag_off_dispatch(ns, env, payload, task)
    env.unpoison()
    # exactly the payload that went in, plus the legacy envelope and the run id the wrapper always stamped
    expected = {**before, "envelope": legacy_envelope(), "_elitea_predict_run_id": call["kwargs"]["_elitea_predict_run_id"]}
    assert call["kwargs"] == expected
    assert dumped(call["kwargs"]) == dumped(expected)
    assert "credential_slot" not in dumped(call) and ns.credential_slots.FREE_FORM_SECRET_MARK not in call["kwargs"]
    assert env.secret_field_calls == [] and env.release_calls == [] and env.configuration_calls == []
    assert env.secrets_calls == [] and env.redis.calls == [] and env.tracing_calls == 0 and env.sessions == []


def test_flag_off_the_entry_points_do_not_mark_and_the_security_modules_are_not_loaded(ns, env):
    payload = {"tools": [{"id": 1, "type": "x", "settings": {"headers": {"X-Key": "{{secret.api_key}}"}}}]}
    env.poison()
    found = ns.credential_slots.has_free_form_secret(env.module(governed=False), payload)
    env.unpoison()
    ns.credential_slots.mark_free_form_secret(payload, found)
    assert found is False and ns.credential_slots.FREE_FORM_SECRET_MARK not in payload
    assert "shared_secret_stores_6946.corporate_vault" not in sys.modules


def test_flag_off_the_release_handler_and_the_probe_refuse_and_touch_nothing(ns, env):
    module = env.module(governed=False)
    env.poison()
    env.redis.raises = AssertionError("redis was used")
    reply = ns.credential_release.release_credential(module, project_id=1)
    probe = ns.credential_probe.governed_check_connection(module, 1, str(uuid.uuid4()), "jira", 42)
    env.unpoison()
    assert reply["ok"] is False and probe["success"] is False
    assert env.release_calls == [] and env.redis.calls == [] and env.started == []


def test_the_release_handler_is_registered_only_when_governed_and_unregistered_with_it():
    text = (PLUGIN_ROOT / "module.py").read_text()
    assert text.count('name="restricted_release_credential"') == 2
    ready = text[text.index("if self.governance_profile.enabled:\n            try:"):text.index("def _ensure_share_token_schema")]
    assert "self.release_credential" in ready and ready.count("rpc_node.register(") == 2
    deinit = text[text.index("rpc_node.unregister(\n                        self.validate_execution_context"):]
    assert "self.release_credential" in deinit[:900]
    methods = (PLUGIN_ROOT / "methods" / "governed_credentials.py").read_text()
    assert "def release_credential(" in methods and "def governed_check_connection(" in methods
    assert methods.count("@web.method()") == 2


def test_governed_startup_needs_the_release_methods_and_the_transport(ns, env):
    ns.adm.check_governed_startup(env.module())
    for missing in ("secrets", "configurations"):
        env.missing_modules = {missing}
        with pytest.raises(ValueError, match=missing):
            ns.adm.check_governed_startup(env.module())
    env.missing_modules = set()
    ns.tools.secret_stores = types.ModuleType("shared_without_transport")
    try:
        with pytest.raises(ValueError, match="secret_stores.transport"):
            ns.adm.check_governed_startup(env.module())
    finally:
        sys.modules.pop("shared_without_transport", None)
    ns.tools.secret_stores = sys.modules["shared_secret_stores_6946"]
    ns.adm.check_governed_startup(env.module())
