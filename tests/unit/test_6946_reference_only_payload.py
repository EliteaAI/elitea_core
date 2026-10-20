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
    RUN, SECRET, Credential, build_wrapper, call_kwargs, dispatch, dumped, payload_of, run_to_running,
    sealed_tool_settings, seeded,
)

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


@pytest.fixture(scope="module")
def parallel(ns):
    """The parallel dispatch methods, loaded as part of the plugin package (their relative imports resolve to the
    harness's modules) against a minimal ``pylon.core.tools`` that has ``web.method``."""
    package = types.ModuleType(f"{admission_env.PKG}.methods")
    package.__path__ = []
    sys.modules[package.__name__] = package
    tools = sys.modules["pylon.core.tools"]
    tools.web = types.SimpleNamespace(method=lambda *a, **k: (lambda function: function))
    spec = importlib.util.spec_from_file_location(
        f"{admission_env.PKG}.methods.parallel_dispatch", PLUGIN_ROOT / "methods" / "parallel_dispatch.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


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


def calls_in(path, function_name_of):
    """``{function: [(line, called name)]}`` of the calls in a source file, from its syntax tree."""
    import ast
    tree = ast.parse((PLUGIN_ROOT / path).read_text())
    found = {}
    for function in ast.walk(tree):
        if isinstance(function, (ast.FunctionDef, ast.AsyncFunctionDef)):
            for call in ast.walk(function):
                if isinstance(call, ast.Call):
                    name = function_name_of(call.func)
                    if name:
                        found.setdefault(function.name, []).append((call.lineno, name))
    return found


def called(node):
    import ast
    return node.attr if isinstance(node, ast.Attribute) else node.id if isinstance(node, ast.Name) else None


@pytest.mark.parametrize("path, count", [("rpc/application.py", 2), ("methods/predict.py", 1)])
def test_every_predict_entry_point_checks_before_it_unsecrets_and_marks_after(path, count):
    """The entry points need the Pylon runtime and cannot run here; their order is read from the syntax tree."""
    sites = [lines for lines in calls_in(path, called).values() if any(name == "has_free_form_secret" for _, name in lines)]
    assert len(sites) == count
    for lines in sites:
        at = {name: [line for line, other in lines if other == name] for name in
              ("has_free_form_secret", "unsecret", "mark_free_form_secret", "start_task")}
        assert len(at["has_free_form_secret"]) == len(at["mark_free_form_secret"]) == 1
        check, mark = at["has_free_form_secret"][0], at["mark_free_form_secret"][0]
        payload_wide = [line for line in at["unsecret"] if line > check]   # (the webhook secret is unsecreted earlier)
        assert payload_wide and check < payload_wide[0] < mark < min(line for line in at["start_task"] if line > mark)


def test_the_release_handler_is_registered_with_the_validate_handler_on_the_first_enforce_and_unregistered_with_it():
    text = (PLUGIN_ROOT / "module.py").read_text()
    ensure = text[text.index("def _ensure_governed_handlers_registered"):text.index("def _unregister_governed_handlers")]
    unregister = text[text.index("def _unregister_governed_handlers"):text.index("def _ensure_share_token_schema")]
    for part, call in ((ensure, "rpc_node.register(handler, name=name)"), (unregister, "rpc_node.unregister(handler, name=name)")):
        assert call in part
        assert ("self.validate_execution_context" in part and "self.release_credential" in part
                and '"restricted_validate_execution_context"' in part and '"restricted_release_credential"' in part)
    # nothing registers them at boot: the switch does, when the mode reaches enforce (utils/governance_settings.py)
    assert text.count("rpc_node.register(") == text.count("rpc_node.register(handler, name=name)") + 1   # + provider api
    settings = (PLUGIN_ROOT / "utils" / "governance_settings.py").read_text()
    assert "_ensure_governed_handlers_registered" in settings and "MODE_ENFORCE and self._on_enforce" in settings


def test_the_two_module_methods_are_web_methods_so_they_bind():
    import ast
    tree = ast.parse((PLUGIN_ROOT / "methods" / "governed_credentials.py").read_text())
    methods = {function.name: [ast.unparse(d) for d in function.decorator_list]
               for function in ast.walk(tree) if isinstance(function, ast.FunctionDef)}
    assert methods["release_credential"] == ["web.method()"] == methods["governed_check_connection"]


def test_the_bus_handler_reads_only_the_request_keys_and_never_raises(ns, env, parallel):
    """``parallel`` gives the harness a ``web.method``; a ``module`` key from the bus is not an argument."""
    name = f"{admission_env.PKG}.methods.governed_credentials"
    spec = importlib.util.spec_from_file_location(name, PLUGIN_ROOT / "methods" / "governed_credentials.py")
    loaded = importlib.util.module_from_spec(spec)
    sys.modules[name] = loaded
    spec.loader.exec_module(loaded)
    method = loaded.Method.release_credential
    reply = method(env.module(), module="evil", project_id=1, context_ref=str(uuid.uuid4()), unknown=1)
    assert reply["ok"] is False and reply["reason_code"] == "STALE_CONTEXT"
    assert method(env.module(governed=False), project_id=object())["ok"] is False


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
        self._context_by_task, self._context_lock = self.module._context_by_task, self.module._context_lock
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


class Original:
    """The real ``start_task`` behind the wrapper: records what it was handed (a copy, as Redis would hold it)."""

    def __init__(self):
        self.calls = []

    def __call__(self, *args, **kwargs):
        self.calls.append((args, copy.deepcopy(kwargs)))
        return "task-1"


def flag_off_dispatch(ns, env, payload, task="indexer_agent"):
    """What reaches the real ``start_task`` through the real ``_maintenance_gated_start_task`` of module.py."""
    original = Original()
    wrapper = build_wrapper(ns, env.module(governed=False), original)
    kwargs = {"args": ["s", "m"], "kwargs": copy.deepcopy(payload), "pool": "agents", "meta": {"project_id": 1},
              "admission": env.request()}
    assert wrapper(task, **kwargs) == "task-1"
    [(args, sent)] = original.calls
    assert args == (task,) and "admission" not in sent
    return sent


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


def test_the_module_registers_the_release_handler_lazily_and_the_methods_are_web_methods():
    text = (PLUGIN_ROOT / "module.py").read_text()
    assert text.count('"restricted_release_credential"') == 2   # the register and the unregister table
    ensure = text[text.index("def _ensure_governed_handlers_registered"):text.index("def _unregister_governed_handlers")]
    assert "self.release_credential" in ensure and ensure.count("rpc_node.register(") == 1
    methods = (PLUGIN_ROOT / "methods" / "governed_credentials.py").read_text()
    assert "def release_credential(" in methods and "def governed_check_connection(" in methods
    assert methods.count("@web.method()") == 2


def test_the_preflight_needs_the_release_methods_and_the_transport(ns, env):
    def failing():
        return sorted(check.id for check in ns.adm.governance_preflight(env.module()) if check.status == "fail")

    assert failing() == []
    for missing in ("secrets", "configurations"):
        env.missing_modules = {missing}
        assert failing() == ["credential_facts"]
    env.missing_modules = set()
    ns.tools.secret_stores = types.ModuleType("shared_without_transport")
    try:
        assert failing() == ["credential_facts"]
    finally:
        sys.modules.pop("shared_without_transport", None)
    ns.tools.secret_stores = sys.modules["shared_secret_stores_6946"]
    assert failing() == []


# --- secret fields outside ``properties``: header maps ------------------------------------------

def openapi(env, headers, walker_paths=("api_key",)):
    """A credential of an OpenAPI-like type whose configuration holds a header map (``Dict[str, SecretStr]``)."""
    cred = seeded(env, ctype="openapi")
    env.secret_fields["openapi"] = list(walker_paths)
    payload = payload_of(cred)
    config = payload["application"]["version_details"]["tools"][0]["settings"]["credentials"]
    config["headers"] = headers
    return cred, payload, config


def test_a_header_map_the_walker_does_not_list_is_still_taken_out_and_asked_for(ns, env):
    _, payload, config = openapi(env, {"X-Api-Key": f"{SECRET}-h1", "X-Other": f"{SECRET}-h2", "X-Empty": ""})
    call, _ = dispatch(env, ns, payload=payload)
    sealed = sealed_tool_settings(call)
    assert sealed["headers"] == {}
    assert sealed["credential_slot"]["fields"] == ["api_key", "headers.X-Api-Key", "headers.X-Other"]
    assert SECRET not in dumped(call)


def test_a_wildcard_path_removes_every_key_under_the_dict_and_asks_for_the_concrete_ones(ns, env):
    _, payload, _ = openapi(env, {"X-Api-Key": f"{SECRET}-h1", "X-Empty": ""}, walker_paths=("api_key", "headers.*"))
    call, _ = dispatch(env, ns, payload=payload)
    sealed = sealed_tool_settings(call)
    assert sealed["headers"] == {} and sealed["credential_slot"]["fields"] == ["api_key", "headers.X-Api-Key"]
    assert SECRET not in dumped(call)


def test_a_header_map_given_as_one_string_is_taken_out_whole(ns, env):
    _, payload, _ = openapi(env, f"{SECRET}-as-text")
    call, _ = dispatch(env, ns, payload=payload)
    sealed = sealed_tool_settings(call)
    assert "headers" not in sealed and sealed["credential_slot"]["fields"] == ["api_key", "headers"]
    assert SECRET not in dumped(call)


def test_a_key_a_release_request_cannot_name_and_a_slot_asking_for_too_much_are_refused(ns, env):
    for headers in ({"X Key": "v"}, {"X.Key": "v"}, {f"K{n}": "v" for n in range(40)}):
        env.reset()
        _, payload, _ = openapi(env, headers)
        with pytest.raises(ns.exceptions.AdmissionDenied) as err:
            dispatch(env, ns, payload=payload)
        assert err.value.reason_code == "CREDENTIAL_REFERENCE_REQUIRED"
        [row] = contexts(env, ns)
        assert row.revoked_reason == "NOT_DISPATCHED"


def test_a_header_map_of_a_slot_already_sealed_stays_sealed(ns, env):
    _, payload, _ = openapi(env, {"X-Api-Key": f"{SECRET}-h1"})
    call, ref = dispatch(env, ns, payload=payload)
    sealed = copy.deepcopy(call["kwargs"])
    child = call_kwargs(copy.deepcopy(call["kwargs"]), ref)
    assert ns.adm.admit_dispatch(env.module(), "indexer_agent", child, None) is None
    assert child["kwargs"]["application"] == sealed["application"]


def test_strip_secret_fields_on_its_own(ns):
    strip = ns.credential_slots.strip_secret_fields
    config = {"a": "1", "auth": {"token": "t", "keep": "k"}, "headers": {"X": "x"}, "extra_headers": {"Y": "y"}}
    assert strip(config, ["a", "auth.token"]) == ["a", "auth.token", "headers.X", "extra_headers.Y"]
    assert config == {"auth": {"keep": "k"}, "headers": {}, "extra_headers": {}}
    assert strip({"h": {"k": "v"}}, ["h.*"]) == ["h.k"]
    assert strip({"headers": {"X": "x"}}, ["headers.*"]) == ["headers.X"]
    assert strip({}, ["a", "b.*"]) == []


def schemas_of_the_sdk():
    """The JSON schema of every configuration the SDK ships, read in a subprocess (importing the SDK here would put
    its dependencies into the other suites); skipped when the SDK checkout is not there."""
    import subprocess
    sdk = Path("/Users/romanmitusov/Documents/Work/.claude/projects/elitea-sdk/wt/elitea-sdk-integration")
    if not (sdk / "elitea_sdk" / "configurations" / "__init__.py").is_file():
        pytest.skip("the SDK checkout is not available")
    code = ("import json, sys; sys.path.insert(0, sys.argv[1]); "
            "from elitea_sdk.configurations import AVAILABLE_CONFIGURATIONS as A; print(json.dumps(A))")
    done = subprocess.run([sys.executable, "-c", code, str(sdk)], capture_output=True, text=True, timeout=120, check=False)
    if done.returncode != 0 or not done.stdout.strip():
        pytest.skip("the SDK cannot be imported here")
    return json.loads(done.stdout.strip().splitlines()[-1])


def _ref(schema, defs):
    return defs.get(schema["$ref"].split("/")[-1], {}) if "$ref" in schema else schema


def _is_password(schema, defs):
    schema = _ref(schema, defs)
    return schema.get("format") == "password" or any(
        _ref(option, defs).get("format") == "password" for option in schema.get("anyOf", []) if isinstance(option, dict))


def instance_of(schema, defs, marks, depth=0):
    """A value for a schema with a distinct marker in every password leaf, at any depth (properties,
    additionalProperties, items, anyOf)."""
    schema = _ref(schema, defs)
    if depth > 6 or not isinstance(schema, dict):
        return None
    if _is_password(schema, defs):
        marks.append(f"MARK{len(marks)}-{SECRET}")
        return marks[-1]
    for option in schema.get("anyOf", []):
        if isinstance(option, dict) and option.get("type") not in (None, "null"):
            value = instance_of(option, defs, marks, depth + 1)
            if value is not None:
                return value
    if isinstance(schema.get("properties"), dict):
        return {name: value for name, sub in schema["properties"].items()
                if (value := instance_of(sub, defs, marks, depth + 1)) is not None}
    if isinstance(schema.get("additionalProperties"), dict):
        value = instance_of(schema["additionalProperties"], defs, marks, depth + 1)
        return {"X-Key-1": value, "X-Key-2": value} if value is not None else {}
    if isinstance(schema.get("items"), dict):
        value = instance_of(schema["items"], defs, marks, depth + 1)
        return [value] if value is not None else []
    return None


def walker_paths(schema, wildcards):
    """What ``configuration_secret_fields`` lists today (``properties`` only), or with the ``.*`` paths of dicts whose
    every value is a password (what the walker is being taught)."""
    defs, found = schema.get("$defs", {}), []

    def walk(sub, prefix):
        for name, spec in (_ref(sub, defs).get("properties") or {}).items():
            path = f"{prefix}{name}"
            if _is_password(spec, defs):
                found.append(path)
            elif wildcards and isinstance(_ref(spec, defs).get("additionalProperties"), dict) \
                    and _is_password(_ref(spec, defs)["additionalProperties"], defs):
                found.append(f"{path}.*")
            elif _ref(spec, defs).get("properties"):
                walk(spec, f"{path}.")

    walk(schema, "")
    return found


@pytest.mark.parametrize("wildcards", [False, True])
def test_after_sealing_no_password_of_any_sdk_configuration_remains_at_any_depth(ns, env, wildcards):
    schemas = schemas_of_the_sdk()
    assert len(schemas) > 20
    sealed_types = 0
    for name, schema in sorted(schemas.items()):
        marks = []
        data = instance_of(schema, schema.get("$defs", {}), marks)
        if not isinstance(data, dict) or not marks:
            continue   # nothing secret in this configuration: a slot would ask for nothing (refused, tested elsewhere)
        config = {**data, "configuration_type": name, "configuration_uuid": str(uuid.uuid4()),
                  "configuration_project_id": 1}
        entry = {"toolkit_instance_id": "1", "field": "credentials", "configuration_uuid": config["configuration_uuid"],
                 "credential_ref": str(uuid.uuid4()), "credential_generation": 1, "decision_id": str(uuid.uuid4())}
        payload = {"tools": [{"id": 1, "type": "x", "settings": {"credentials": config}}]}
        paths = walker_paths(schema, wildcards)
        ns.credential_slots.seal_payload(
            payload, lambda: [entry], secret_fields_of=lambda types, paths=paths: {name: paths})
        text = dumped(payload)
        left = [mark for mark in marks if mark in text]
        assert not left, f"{name} (walker wildcards={wildcards}) leaves {left}"
        assert config["credential_slot"]["fields"], name
        sealed_types += 1
    assert sealed_types > 20


# --- a credential configuration holding no secret at all --------------------------------------

def test_a_credential_configuration_with_no_secret_value_is_refused_not_sealed_with_an_empty_slot(ns, env):
    cred = seeded(env)
    payload = payload_of(cred)
    payload["application"]["version_details"]["tools"][0]["settings"]["credentials"]["token"] = ""
    with pytest.raises(ns.exceptions.AdmissionDenied) as err:
        dispatch(env, ns, payload=payload)
    assert err.value.reason_code == "CREDENTIAL_REFERENCE_REQUIRED"
    [row] = contexts(env, ns)
    assert row.revoked_reason == "NOT_DISPATCHED"
    env.reset()
    cred = seeded(env)
    env.secret_fields["jira"] = []   # a type without secret fields at all
    with pytest.raises(ns.exceptions.AdmissionDenied):
        dispatch(env, ns, cred)


# --- free-form secrets in the non-secret fields of a configuration --------------------------------

def test_a_template_in_a_non_secret_field_of_a_credential_is_found_one_in_its_secret_fields_is_not(ns, env):
    module, cred = env.module(), seeded(env)
    payload = payload_of(cred)
    config = payload["application"]["version_details"]["tools"][0]["settings"]["credentials"]
    assert ns.credential_slots.has_free_form_secret(module, payload) is False
    config["token"] = "{{secret.unresolved}}"           # a secret field: sealed, not scanned
    assert ns.credential_slots.has_free_form_secret(module, payload) is False
    config["base_url"] = "https://{{secret.host}}.example"   # a plain field: the payload-wide unsecret would resolve it
    assert ns.credential_slots.has_free_form_secret(module, payload) is True


def test_the_secret_fields_of_all_types_are_asked_once_per_payload_and_unknown_or_failing_is_read_whole(ns, env):
    module = env.module()
    first, second = seeded(env), None
    second = Credential(env, first.binding, tool_id=2, config_id=6, field="other")
    payload = payload_of(first, second)
    assert ns.credential_slots.has_free_form_secret(module, payload) is False
    assert env.secret_field_calls == [["jira"]]
    env.unregistered_types.add("jira")   # the walker does not know the type: everything is read, secrets included
    env.secret_field_calls.clear()
    assert ns.credential_slots.has_free_form_secret(module, payload) is False
    payload["application"]["version_details"]["tools"][0]["settings"]["credentials"]["token"] = "{{secret.x}}"
    assert ns.credential_slots.has_free_form_secret(module, payload) is True
    env.secret_fields_raises = RuntimeError("down")   # cannot tell: counts as found
    assert ns.credential_slots.has_free_form_secret(module, payload_of(first)) is True


def test_a_header_map_is_not_scanned_for_templates_it_is_sealed(ns, env):
    _, payload, _ = openapi(env, {"X-Api-Key": "{{secret.k}}"})
    assert ns.credential_slots.has_free_form_secret(env.module(), payload) is False


# --- a fan-out: the parent's context lives until the epoch reconciles -----------------------------

def test_launching_the_children_of_a_governed_parked_parent_reopens_its_completed_context(ns, env, parallel):
    cred = seeded(env)
    _, parent = dispatch(env, ns, cred)
    run_to_running(env, ns, parent)
    launcher = make_launcher(parallel, ns, env, governed=True)
    launcher.module._context_by_task["parent-task"] = (1, parent)
    ns.adm.complete_context(launcher.module, "parent-task")
    assert contexts(env, ns)[0].state == "completed"
    child = call_kwargs(payload_of(cred), parent)["kwargs"]
    launch(launcher, child)
    assert contexts(env, ns)[0].state == "running"
    assert ns.adm.validate_context(launcher.module, 1, parent, {
        "kind": "indexer_worker", "task_name": "indexer_agent", "run_id": RUN})["ok"] is True


def test_a_legacy_launch_does_not_touch_any_context(ns, env, parallel):
    launcher = make_launcher(parallel, ns, env, governed=False)
    env.poison()
    launch(launcher, {"thread_id": "root:B:c", "envelope": {"context_ref": str(uuid.uuid4())}})
    env.unpoison()
    assert launcher.order == ["stash", "start"]


def test_the_reconcile_run_is_tracked_so_it_closes_the_context_when_it_stops(ns, env, parallel):
    cred = seeded(env)
    _, parent = dispatch(env, ns, cred)
    launcher = make_launcher(parallel, ns, env, governed=True)
    launcher._parallel_track_reconcile("reconcile-task", 1, {"envelope": {"context_ref": parent}})
    assert launcher.module._context_by_task["reconcile-task"] == (1, parent)
    ns.adm.complete_context(launcher.module, "reconcile-task")
    assert contexts(env, ns)[0].state == "completed"
    legacy = make_launcher(parallel, ns, env, governed=False)
    legacy._parallel_track_reconcile("other-task", 1, {"envelope": {"context_ref": parent}})
    assert "other-task" not in legacy.module._context_by_task
