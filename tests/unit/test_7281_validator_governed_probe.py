"""#7281 (C-1009): under enforce the toolkit and agent validators check a saved credential through the governed probe.

The connection check of ``ToolValidatedDetails`` ran on settings that ``expand_toolkit_settings`` had already
resolved, and sent them to the indexer (``applications_configuration_check_connection``): the stored value of a
governed credential travelled outside the trusted release. Under enforce it is now the governed probe (ids only, the
worker asks for the value through the release); governance off and every configuration outside the ``credentials``
section keep the old path byte for byte. The helpers are lifted out of the module with ``ast`` (the module itself
cannot be imported without Pylon). Every value below is made up for the test.
"""
import ast
import pathlib
import sys
import textwrap
import types

import pytest

pytestmark = pytest.mark.unit

SOURCE_PATH = pathlib.Path(__file__).resolve().parents[2] / "utils" / "application_utils.py"
SOURCE = SOURCE_PATH.read_text()
TREE = ast.parse(SOURCE)
NAMES = ("_find_configurations_in_settings", "_inject_oauth_tokens", "_governed_connection_error",
         "_check_configurations_connection_from_expanded_settings")
SECRET = "stored-value-made-up-for-the-test"
CFG = "0c780530-1311-4f01-be16-1f6ad2d6d284"


class Rpc:
    def __init__(self):
        self.calls = []

    def timeout(self, _seconds):
        return self

    def applications_configuration_check_connection(self, type_, settings):
        self.calls.append((type_, settings))
        return None


class Module:
    def __init__(self, enabled, answer=None):
        self.governance_profile = types.SimpleNamespace(enabled=enabled)
        self.answer, self.probes = answer if answer is not None else {"success": True}, []

    def governed_check_connection(self, project_id, configuration_uuid, config_type, user_id):
        self.probes.append((project_id, configuration_uuid, config_type, user_id))
        return dict(self.answer)


def load(module, rpc):
    registry = types.ModuleType("plugins.configurations.models.pd.registry")
    def entry(section):
        return types.SimpleNamespace(section=section, check_connection_func="rpc", model=None)

    registry.CONFIG_TYPE_REGISTRY = {"github": entry("credentials"), "pgvector": entry("vectorstorage")}
    sys.modules["plugins.configurations.models.pd.registry"] = registry
    namespace = {"this": types.SimpleNamespace(module=module), "context": types.SimpleNamespace(rpc_manager=rpc),
                 "log": types.SimpleNamespace(debug=lambda *a, **k: None, warning=lambda *a, **k: None)}
    for node in TREE.body:
        if isinstance(node, ast.FunctionDef) and node.name in NAMES:
            exec(textwrap.dedent(ast.get_source_segment(SOURCE, node)), namespace)  # noqa: S102
    return namespace["_check_configurations_connection_from_expanded_settings"]


def settings():
    """What ``expand_toolkit_settings`` leaves: the credential resolved, a vector store next to it."""
    return {"repository": "o/r",
            "github_configuration": {"configuration_type": "github", "elitea_title": "gh", "configuration_uuid": CFG,
                                     "configuration_project_id": 2, "access_token": SECRET},
            "pgvector_configuration": {"configuration_type": "pgvector", "elitea_title": "pg",
                                       "connection_string": "postgresql://made-up"}}


@pytest.fixture(autouse=True)
def _registry():
    saved = sys.modules.get("plugins.configurations.models.pd.registry")
    yield
    if saved is None:
        sys.modules.pop("plugins.configurations.models.pd.registry", None)
    else:
        sys.modules["plugins.configurations.models.pd.registry"] = saved


def test_under_enforce_a_saved_credential_is_checked_through_the_governed_probe_with_ids_only():
    module, rpc = Module(enabled=True), Rpc()
    assert load(module, rpc)(settings(), {}, project_id=5, user_id=9) is None
    assert module.probes == [(2, CFG, "github", 9)]
    assert [type_ for type_, _ in rpc.calls] == ["pgvector"]   # not a credential: the old path
    assert SECRET not in repr(rpc.calls)                        # the resolved value is sent nowhere


def test_a_refusal_of_the_probe_is_the_connection_error_with_its_code_and_text():
    answer = {"success": False, "code": "DENIED_ASSERTION_UNAVAILABLE",
              "message": "Your Microsoft sign-in is not available to this run. Sign in with Microsoft again, "
                         "then retry."}
    module, rpc = Module(enabled=True, answer=answer), Rpc()
    [error] = load(module, rpc)({"github_configuration": settings()["github_configuration"]}, {}, 5, 9)
    assert error == {"configuration_title": "gh", "configuration_type": "github", "message": answer["message"],
                     "requires_authorization": False, "auth_metadata": None, "code": "DENIED_ASSERTION_UNAVAILABLE"}
    assert rpc.calls == []


def test_a_user_who_may_not_test_credentials_sees_no_marker():
    module, rpc = Module(enabled=True, answer={"success": False, "code": "DENIED_RBAC", "message": "x"}), Rpc()
    assert load(module, rpc)(settings(), {}, 5, 9) is None
    assert [type_ for type_, _ in rpc.calls] == ["pgvector"]


@pytest.mark.parametrize("profile", [Module(enabled=False), types.SimpleNamespace()])   # off, audit only, no profile
def test_without_enforce_every_configuration_takes_the_old_path_unchanged(profile):
    rpc = Rpc()
    assert load(profile, rpc)(settings(), {}, 5, 9) is None
    expanded = settings()
    assert rpc.calls == [("github", expanded["github_configuration"]), ("pgvector", expanded["pgvector_configuration"])]
    assert getattr(profile, "probes", []) == []


def _helpers():
    namespace = {"json": __import__("json"), "ValidationError": __import__("pydantic").ValidationError}
    for node in TREE.body:
        if isinstance(node, ast.FunctionDef) and node.name in ("governed_refusal_of", "_nested_governed_refusal"):
            exec(textwrap.dedent(ast.get_source_segment(SOURCE, node)), namespace)  # noqa: S102
    return namespace["governed_refusal_of"], namespace["_nested_governed_refusal"]


def test_an_agents_validation_carries_the_probes_refusal_as_the_refusal_shape_up_to_its_parent():
    """The agent's toolkit card, and the card of a parent agent that nests it, say what the probe said."""
    from pydantic import ValidationError
    refusal_of, nested = _helpers()
    text = "Your Microsoft sign-in is not available to this run. Sign in with Microsoft again, then retry."
    refusal = refusal_of([{"configuration_title": "sp", "message": text, "code": "DENIED_ASSERTION_UNAVAILABLE"}])
    assert __import__("json").loads(refusal) == {"code": "DENIED_ASSERTION_UNAVAILABLE", "error": text}
    assert refusal_of([{"configuration_title": "gh", "message": "Authentication failed"}]) is None   # not governed
    assert refusal_of(None) is None
    child = ValidationError.from_exception_data("M", [
        {"type": "value_error", "loc": ("tools", 1, "__root__"), "input": None, "ctx": {"error": refusal}}])
    assert nested(child) == refusal
    other = ValidationError.from_exception_data("M", [
        {"type": "value_error", "loc": ("tools", 1, "__root__"), "input": None, "ctx": {"error": "boom"}}])
    assert nested(other) is None and nested(RuntimeError("x")) is None
