"""C-1009 #7228 item 1: credential material never on the model path.

Every MCP-exposed route of the live registry is classified (``mcp_registry_material_20261103.json``: tools/list of the
local stack matched to /shared/openapi/?all=true, schemas inlined) and pinned by name; a route whose schema gains a
secret-typed field is material on its own; a run token never executes a material call, whatever its scope
(``CREDENTIALS_VIA_FORM``, recorded); the internal MCP toolkits of a run whose scope holds ``credentials`` carry the
``credential_form`` stamp, any other run's none; under off nothing is stamped and nothing is read. Real token and
admission code on SQLite (``fixtures/admission_env.py``, as ``test_7214_internal_obo``)."""
import copy
import json
import sys
import types
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from test_7214_internal_obo import (  # noqa: E402  pylint: disable=W0611
    BUILD_PERMISSION, CREDENTIAL_PERMISSION, admitted, call_kwargs, env, mint, ns, obo, seed)

PLUGIN_ROOT = Path(__file__).parents[2]
SNAPSHOT = json.loads((PLUGIN_ROOT / "tests" / "fixtures" / "mcp_registry_material_20261103.json").read_text())
EXPECTED = {   # every MCP-exposed route whose class is not ``none``, by tool name
    "post_secrets_secrets": "material_in",
    "put_secrets_secret": "material_in",
    "post_artifacts_s3_credentials": "material_out",
    "put_artifacts_s3_credentials": "material_out",
    "post_configurations_configurations": "per_request",
    "put_configurations_configuration": "per_request",
    "post_elitea_core_applications": "per_request",   # its webhook secret
}


def spec_of(routes):
    paths = {}
    for entries in routes.values():
        for entry in entries:
            operation = {}
            if entry["request"] is not None:
                operation["requestBody"] = {"content": {"application/json": {"schema": entry["request"]}}}
            if entry["response"] is not None:
                operation["responses"] = {"200": {"content": {"application/json": {"schema": entry["response"]}}}}
            paths.setdefault(entry["path"], {})[entry["method"].lower()] = operation
    return {"paths": paths, "components": {"schemas": {}}}


@pytest.fixture
def registry(ns, monkeypatch):
    found = types.SimpleNamespace(spec=spec_of(SNAPSHOT["routes"]))
    found.get_combined_spec = lambda full=True: found.spec
    monkeypatch.setattr(ns.tools, "openapi_registry", found, raising=False)
    obo(ns)._MATERIALS.clear()   # pylint: disable=W0212
    yield found
    obo(ns)._MATERIALS.clear()   # pylint: disable=W0212


def test_every_mcp_exposed_route_of_the_live_registry_has_its_class(ns, registry):
    classes = {}
    for name, entries in SNAPSHOT["routes"].items():
        [entry] = entries
        classes[name] = obo(ns).material_of(entry["method"], entry["path"])
    assert len(classes) == 90
    assert {name: found for name, found in classes.items() if found != "none"} == EXPECTED


def test_reading_a_secret_value_is_the_credentials_operation_and_every_other_read_stays_read(ns, registry):
    assert obo(ns).operation_of("GET", "/api/v2/secrets/secret/{mode}/{project_id}/{secret}") == "credentials"
    reads = [(entry["method"], entry["path"]) for entries in SNAPSHOT["routes"].values() for entry in entries
             if entry["method"] == "GET"]
    assert reads and {obo(ns).operation_of(method, path) for method, path in reads} == {"read"}


def test_the_platform_clients_secret_read_keeps_its_one_fixed_value(ns):
    """``RUN_ROUTES``: the worker's checkpointer fallback reads ``pgvector_project_connstr`` and nothing else, as before."""
    assert obo(ns).RUN_ROUTES["api.v2.secrets.secret"] == frozenset({"GET"})
    assert obo(ns).RUN_ROUTE_VALUES == {"api.v2.secrets.secret": ("secret", frozenset({"pgvector_project_connstr"}))}


@pytest.mark.parametrize("field", [
    {"type": "string", "format": "password", "writeOnly": True},          # SecretStr
    {"anyOf": [{"type": "string", "format": "password"}, {"type": "null"}]},
    {"type": "object", "additionalProperties": {"type": "string", "writeOnly": True}},
    {"$ref": "#/components/schemas/Secret"},
])
def test_a_new_route_with_a_secret_typed_field_is_material_by_its_own_schema(ns, registry, field):
    registry.spec = copy.deepcopy(registry.spec)
    registry.spec["components"]["schemas"]["Secret"] = {"type": "string", "format": "password"}
    registry.spec["paths"]["/api/v2/elitea_core/new_thing/{mode}/{project_id}"] = {
        "post": {"requestBody": {"content": {"application/json": {"schema": {
            "type": "object", "properties": {"name": {"type": "string"}, "token": field}}}}}},
        "get": {"responses": {"200": {"content": {"application/json": {"schema": {
            "type": "object", "properties": {"token": field}}}}}}}}
    assert obo(ns).material_of("POST", "/api/v2/elitea_core/new_thing/{mode}/{project_id}") == "material_in"
    assert obo(ns).material_of("GET", "/api/v2/elitea_core/new_thing/{mode}/{project_id}") == "material_out"
    assert obo(ns).operation_of("GET", "/api/v2/elitea_core/new_thing/{mode}/{project_id}") == "credentials"
    tools = [{"method": "post", "path": "/api/v2/elitea_core/new_thing/{mode}/{project_id}"},
             {"method": "get", "path": "/api/v2/elitea_core/new_thing/{mode}/{project_id}"},
             {"method": "post", "path": "/api/v2/secrets/secrets/{mode}/{project_id}"}]
    # the startup guard: a write carrying material that the operation map files elsewhere (here: unknown) is named
    assert obo(ns).unclassified_material(tools) == ["POST elitea_core/new_thing/{mode}/{project_id}"]


def test_the_startup_guard_finds_nothing_on_the_live_registry(ns, registry):
    tools = [{"method": entry["method"].lower(), "path": entry["path"]}
             for entries in SNAPSHOT["routes"].values() for entry in entries]
    assert obo(ns).unclassified_material(tools) == []
    # pre-existing (#7185, not this change): the skill file writes are not in the operation map, so a run is refused them
    assert obo(ns).unmapped_writes(tools) == [
        "DELETE elitea_core/skill_file/{mode}/{project_id}/{skill_id}/{version_id}/{path}",
        "PUT elitea_core/skill_file/{mode}/{project_id}/{skill_id}/{version_id}/{path}"]


def test_a_schema_too_large_to_walk_counts_as_material(ns):
    deep = {"type": "object", "properties": {f"f{i}": {"type": "string"} for i in range(5000)}}
    assert obo(ns).schema_material(deep, None) == "material_in"


# --- the run token never executes material ------------------------------------------------------------------------

def under(ns, env, scope):
    env.in_request = True
    _, claims = mint(ns, scope=scope)
    env.g.internal_obo = claims
    return claims


def test_a_material_call_is_refused_whatever_the_scope_and_recorded_like_a_scope_refusal(ns, env):
    under(ns, env, ["read", "build", "credentials", "run"])
    with pytest.raises(obo(ns).OperationRefused) as refused:
        obo(ns).check_operation("credentials", "POST /api/v2/secrets/secrets/prompt_lib/1", "post_secrets_secrets",
                                material=True)
    assert refused.value.code == "CREDENTIALS_VIA_FORM"
    assert "credential form" in refused.value.message and "Nothing was written" in refused.value.message
    [event] = env.denied
    assert (event["event_type"], event["decision"], event["reason_code"]) == (
        "platform_operation", "deny", "CREDENTIALS_VIA_FORM")
    assert event["operation_id"] == "credentials:post_secrets_secrets" and "value" not in json.dumps(event)


def test_out_of_scope_stays_a_scope_refusal(ns, env):
    under(ns, env, ["read"])
    with pytest.raises(obo(ns).OperationRefused) as refused:
        obo(ns).check_operation("credentials", "POST x", "t", material=True)
    assert refused.value.code == "INTERNAL_SCOPE_DENIED"


def test_a_model_definition_is_not_material_and_runs_under_the_credentials_scope(ns, env):
    under(ns, env, ["read", "credentials"])
    obo(ns).check_operation("credentials", "POST /api/v2/configurations/configurations/1", "post", material=False)
    [event] = env.denied
    assert (event["decision"], event["reason_code"]) == ("allow", "PLATFORM_OPERATION")


@pytest.mark.parametrize("config_type, data, paths, expected", [
    ("llm_model", {"name": "gpt", "ai_credentials": {"elitea_title": "azure", "private": False}}, [], False),
    ("github", {"base_url": "https://api.github.com"}, ["access_token"], False),
    ("github", {"access_token": "made-up-not-a-secret"}, ["access_token"], True),
    ("openapi", {"headers": {"X-Key": "made-up"}}, ["headers.*"], True),
    ("unknown_type", {"anything": "x"}, None, True),   # a type the configurations plugin does not know
])
def test_a_configuration_body_is_material_only_with_a_value_at_a_secret_field(ns, monkeypatch, config_type, data,
                                                                               paths, expected):
    known = {} if paths is None else {config_type: paths}
    slots = sys.modules[obo(ns).__name__.rsplit(".", 1)[0] + ".credential_slots"]
    monkeypatch.setattr(slots, "configuration_secret_fields", lambda types_: known)
    assert obo(ns).material_of_call("POST", "/api/v2/configurations/configurations/{project_id}", {"project_id": 1},
                                    {"type": config_type, "data": data}) is expected


def test_an_agent_create_is_material_only_with_a_webhook_secret(ns):
    path = "/api/v2/elitea_core/applications/{mode}/{project_id}"
    assert obo(ns).material_of_call("POST", path, {}, {"name": "a"}) is False
    assert obo(ns).material_of_call("POST", path, {}, {"name": "a", "webhook_secret": "made-up"}) is True


def test_the_run_token_listing_omits_what_is_only_ever_material(ns, registry):
    hidden = sorted(name for name, entries in SNAPSHOT["routes"].items()
                    if not obo(ns).listed_for_run(entries[0]["method"], entries[0]["path"]))
    assert hidden == ["post_artifacts_s3_credentials", "post_secrets_secrets", "put_artifacts_s3_credentials",
                      "put_secrets_secret"]


def test_audit_only_records_a_would_deny_and_never_raises(ns, env):
    module = env.module(governed=False, audit=True)
    obo(ns).record_would_deny_material(module, 1, 42, "POST /api/v2/secrets/secrets/prompt_lib/1", "post_secrets")
    [event] = env.denied
    assert (event["decision"], event["reason_code"], event["would_deny"]) == ("allow", "CREDENTIALS_VIA_FORM", True)
    assert event["caller_ref"] == "user:42"


# --- the credential_form stamp ---------------------------------------------------------------------------------

def stamp_of(call):
    return [tool["settings"].get("credential_form") for tool in call["kwargs"]["tools"]]


def test_a_run_whose_scope_holds_credentials_gets_the_stamp_on_its_internal_toolkits(ns, env):
    _, _, call = admitted(ns, env, operations=["read", "credentials"], permissions=(CREDENTIAL_PERMISSION,))
    assert stamp_of(call) == [True]


def test_a_run_without_the_credentials_scope_gets_no_stamp_and_loses_a_stored_one(ns, env):
    tools = [{"type": "mcp", "id": None, "name": "Elitea Applications",
              "settings": {"url": "http://pylon_main:8080/app/5/mcp/elitea_core/applications?scope_project_id=1",
                           "headers": {}, "credential_form": True}},
             {"type": "mcp", "id": 3, "name": "external",
              "settings": {"url": "https://mcp.example.com/mcp", "credential_form": True}}]
    _, _, call = admitted(ns, env, operations=["read", "build"], permissions=(BUILD_PERMISSION,), internal_mcp=False,
                          tools=tools)
    assert [("credential_form" in tool["settings"]) for tool in call["kwargs"]["tools"]] == [False, False]


def test_an_external_mcp_toolkit_never_gets_the_stamp(ns):
    payload = {"tools": [{"type": "mcp", "settings": {"url": "https://mcp.example.com/app/1/mcp"}},
                         {"type": "mcp", "settings": {"url": "http://pylon_main:8080/app/1/mcp/elitea_core/applications"}}]}
    obo(ns).mark_credential_form(payload, True)
    assert [tool["settings"].get("credential_form") for tool in payload["tools"]] == [None, True]


def test_governance_off_stamps_nothing_and_reads_nothing(ns, env):
    env.poison()
    call = call_kwargs(internal_mcp=True)
    call["kwargs"]["envelope"]["security_profile"] = "legacy"
    before = copy.deepcopy(call)
    assert ns.adm.admit_dispatch(env.module(governed=False), "indexer_agent", call, env.request()) is None
    assert call == before and "credential_form" not in call["kwargs"]["tools"][0]["settings"]
