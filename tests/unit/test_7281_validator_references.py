"""#7281 (C-1009): under enforce a settings-validator task gets references, not values.

``toolkit_settings_validator`` is the one place an ``indexer_validator`` task is started; the toolkit and agent
validators, every run payload built through ``ToolValidatedDetails`` and the toolkit Test / on-demand indexing path all
go through it (pinned below). Under enforce the secret fields of every ``credentials`` configuration are replaced by a
fixed placeholder before the task and before the result cache; off and audit only send the settings as before. The RPC
is lifted out of ``rpc/application.py`` with ``ast`` (the module cannot be imported without Pylon); the masking is the
real ``credential_slots`` code. Every value below is made up for the test.
"""
import ast
import copy
import importlib.util
import json
import pathlib
import sys
import textwrap
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PLUGIN_ROOT / "tests"))

from fixtures import admission_env  # noqa: E402  pylint: disable=C0413

STORED = "stored-value-made-up-for-the-test"
HEADER = "header-value-made-up-for-the-test"
SECRET_FIELDS = {"github": ["access_token", "password", "app_private_key"], "openapi": ["api_key", "headers.*"]}
SCHEMA = {"title": "github", "properties": {}}


def expanded():
    return {
        "repository": "o/r",
        "github_configuration": {"configuration_type": "github", "elitea_title": "gh", "private": False,
                                 "configuration_uuid": "6a3c4e7e-9b51-4cf5-8f86-1f0e8c3c1d10",
                                 "configuration_project_id": 2, "configuration_generation": 3,
                                 "base_url": "https://api.github.com", "access_token": STORED, "password": ""},
        "openapi_configuration": {"configuration_type": "openapi", "configuration_uuid": "u2", "api_key": STORED,
                                  "headers": {"X-Api-Key": HEADER}},
        "pgvector_configuration": {"configuration_type": "pgvector", "connection_string": "postgresql://made-up"},
    }


@pytest.fixture(scope="module")
def ns(tmp_path_factory):
    saved = dict(sys.modules)
    yield admission_env.install(PLUGIN_ROOT, tmp_path_factory.mktemp("validator_references"))
    sys.modules.clear()
    sys.modules.update(saved)


def _lift(name):
    source = (PLUGIN_ROOT / "rpc" / "application.py").read_text()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return textwrap.dedent("\n".join(source.splitlines()[node.lineno - 1:node.end_lineno]))
    raise AssertionError(name)


class Node:
    def __init__(self):
        self.started = []

    def start_task(self, task_name, kwargs=None, pool=None, meta=None):
        self.started.append((task_name, copy.deepcopy(kwargs), pool))
        return "task-1"

    def join_task(self, task_id, timeout=None):
        return {"result": copy.deepcopy(self.started[-1][1]["settings"])}


def validator(ns, enabled, secret_fields=None):
    spec = importlib.util.spec_from_file_location("vc_7281", PLUGIN_ROOT / "utils" / "validator_cache.py")
    cache_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(cache_module)
    cache = cache_module.ValidatorResultCache()
    fields = secret_fields if secret_fields is not None else SECRET_FIELDS
    namespace = {
        "find_toolkit_schema_by_type_everywhere": lambda *a, **k: (SCHEMA, False),
        "make_validator_cache_key": cache_module.make_validator_cache_key, "toolkit_validator_cache": cache,
        "log": Mock(),
        "is_credential_type": lambda name: name != "pgvector",
        "mask_credential_values": lambda settings, is_credential: ns.credential_slots.mask_credential_values(
            settings, is_credential, lambda types: {name: fields[name] for name in types if name in fields}),
    }
    exec(_lift("toolkit_settings_validator"), namespace)  # noqa: S102
    node = Node()
    module = SimpleNamespace(task_node=node, governance_profile=SimpleNamespace(enabled=enabled))
    call = lambda settings: namespace["toolkit_settings_validator"](module, settings, "github", 2, 9)  # noqa: E731
    return call, node, cache


def test_under_enforce_the_validator_task_gets_references_not_values(ns):
    call, node, cache = validator(ns, enabled=True)
    assert call(expanded())["ok"] is True
    [(name, kwargs, pool)] = node.started
    sent = kwargs["settings"]
    assert (name, pool) == ("indexer_validator", "indexer")
    assert STORED not in json.dumps(kwargs) and HEADER not in json.dumps(kwargs)
    github = sent["github_configuration"]
    assert github["access_token"] == ns.credential_slots.GOVERNED_VALUE   # present, so the settings check still holds
    assert github["password"] == ""                                       # an empty secret stays empty
    assert {key: github[key] for key in ("configuration_uuid", "configuration_project_id", "base_url")} == {
        key: expanded()["github_configuration"][key] for key in ("configuration_uuid", "configuration_project_id",
                                                                   "base_url")}   # the selector and its shape kept
    assert sent["openapi_configuration"]["headers"] == {"X-Api-Key": ns.credential_slots.GOVERNED_VALUE}
    assert sent["pgvector_configuration"] == expanded()["pgvector_configuration"]   # not a credential: unchanged
    assert STORED not in json.dumps(list(cache._entries.values()), default=str)   # pylint: disable=W0212


def test_the_callers_settings_are_not_changed(ns):
    call, _node, _cache = validator(ns, enabled=True)
    settings = expanded()
    call(settings)
    assert settings == expanded()   # the run payload keeps its own expansion for the governed seal


@pytest.mark.parametrize("profile", [False, None])   # off / audit only (enforce only masks), no profile at all
def test_without_enforce_the_task_gets_the_settings_byte_for_byte(ns, profile):
    call, node, _cache = validator(ns, enabled=profile)
    call(expanded())
    assert node.started == [("indexer_validator", {"toolkit_type": "github", "settings": expanded()}, "indexer")]


def test_when_the_secret_paths_are_unknown_nothing_is_sent(ns):
    call, node, _cache = validator(ns, enabled=True, secret_fields={})   # the type is not known to the registry
    assert call(expanded())["ok"] is True
    assert node.started == []


# --- every path that validates toolkit settings goes through that one RPC ----------------------------------------

def _calls_in(path, function):
    tree = ast.parse((PLUGIN_ROOT / path).read_text())
    found = [node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef) and node.name == function]
    assert found, (path, function)
    return {call.func.attr for fn in found for call in ast.walk(fn)
            if isinstance(call, ast.Call) and isinstance(call.func, ast.Attribute)}


@pytest.mark.parametrize("path, function", [
    ("models/pd/tool.py", "validate_settings"),                 # the validators and every ToolValidatedDetails
    ("rpc/application.py", "test_toolkit_tool_sio"),            # toolkit Test / on-demand indexing
    ("rpc/application.py", "test_mcp_connection_sio"),          # an MCP toolkit's connection test
])
def test_the_validating_paths_go_through_the_masking_rpc(path, function):
    assert "toolkit_settings_validator" in _calls_in(path, function)


def test_a_run_payload_validates_its_tools_through_the_validated_model():
    """``get_application_version_details_expanded`` (chat, sub-agent prefetch, evaluations) builds its tools with
    ``ToolValidatedDetails`` (``validate_settings`` above)."""
    source = (PLUGIN_ROOT / "models" / "pd" / "version.py").read_text()
    assert "tools: Optional[List[ToolValidatedDetails]]" in source
    assert "ApplicationVersionDetailToolValidatedModel.model_validate" in (
        PLUGIN_ROOT / "utils" / "application_utils.py").read_text()


def test_the_validator_task_is_started_in_one_place_only():
    hits = []
    for path in PLUGIN_ROOT.rglob("*.py"):
        parts = path.relative_to(PLUGIN_ROOT).parts
        if parts[0] in ("tests", "wt", "static", "node_modules"):
            continue
        tree = ast.parse(path.read_text())
        for fn in [n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)]:
            for call in ast.walk(fn):
                if isinstance(call, ast.Call) and getattr(call.func, "attr", None) == "start_task" and call.args \
                        and isinstance(call.args[0], ast.Constant) and call.args[0].value == "indexer_validator":
                    hits.append((path.relative_to(PLUGIN_ROOT).as_posix(), fn.name))
    assert sorted(set(hits)) == [("rpc/application.py", "toolkit_settings_validator")]
