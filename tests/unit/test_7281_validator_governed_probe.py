"""#7281 (C-1009): under enforce the toolkit and agent validators check a saved credential through the governed probe.

Validator probes go through the governed path: the probe gets the configuration's ids, and its outcome becomes the
connection error the cards show. Governance off and every configuration outside the ``credentials`` section keep the
path they had. The helpers are lifted out of the module with ``ast`` (the module itself cannot be imported without
Pylon). Every value below is made up for the test.
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
         "_check_configurations_connection_from_expanded_settings", "_connection_error_of", "_resolved",
         "_toolkit_connection_entry", "governed_refusal_of", "_DeferredProbe", "_ProbeBatch")
ASSIGNED = ("NOT_CHECKED", "_PROBE_BATCH", "PROBE_BATCH_SIZE")
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
        self.batches = []

    def governed_check_connection(self, project_id, configuration_uuid, config_type, user_id, reuse=False):
        assert reuse is True   # the validators may be answered from a kept outcome
        self.probes.append((project_id, configuration_uuid, config_type, user_id))
        return dict(self.answer)

    def governed_check_connection_many(self, project_id, items, user_id, reuse=False):
        assert reuse is True
        self.batches.append((project_id, [item["configuration_uuid"] for item in items], user_id))
        answers = self.answers if hasattr(self, "answers") else {}
        return [dict(answers.get(item["configuration_uuid"], self.answer)) for item in items]


def load(module, rpc):
    registry = types.ModuleType("plugins.configurations.models.pd.registry")
    def entry(section):
        return types.SimpleNamespace(section=section, check_connection_func="rpc", model=None)

    registry.CONFIG_TYPE_REGISTRY = {"github": entry("credentials"), "pgvector": entry("vectorstorage")}
    sys.modules["plugins.configurations.models.pd.registry"] = registry
    namespace = {"this": types.SimpleNamespace(module=module), "context": types.SimpleNamespace(rpc_manager=rpc),
                 "log": types.SimpleNamespace(debug=lambda *a, **k: None, warning=lambda *a, **k: None),
                 "contextvars": __import__("contextvars"), "json": __import__("json")}
    for node in TREE.body:
        named = isinstance(node, ast.Assign) and any(getattr(t, "id", None) in ASSIGNED for t in node.targets)
        if named or (isinstance(node, (ast.FunctionDef, ast.ClassDef)) and node.name in NAMES):
            exec(textwrap.dedent(ast.get_source_segment(SOURCE, node)), namespace)  # noqa: S102
    LOADED.clear()
    LOADED.update(namespace)
    return namespace["_check_configurations_connection_from_expanded_settings"]


LOADED = {}


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


@pytest.mark.parametrize("code", ["DENIED_RBAC", "DEPENDENCY_UNAVAILABLE", "RATE_LIMITED",
                                  "WORKER_CAPABILITY_UNAVAILABLE", "AUDIT_UNAVAILABLE"])
def test_nothing_checked_is_no_marker(code):
    """The user may not test saved credentials, or the governed pool is busy: not a fact about the credential."""
    module, rpc = Module(enabled=True, answer={"success": False, "code": code, "message": "x"}), Rpc()
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


def test_a_parent_agents_card_names_the_sub_agent_whose_credential_was_refused():
    from pydantic import ValidationError
    refusal_of, nested = _helpers()
    refusal = refusal_of([{"message": "Sign in with Microsoft again, then retry.", "code": "DENIED_ASSERTION_UNAVAILABLE"}])

    def child_validation(*_a, **_k):
        raise ValidationError.from_exception_data("M", [
            {"type": "value_error", "loc": ("tools", 1, "__root__"), "input": None, "ctx": {"error": refusal}}])

    namespace = {"json": __import__("json"), "_nested_governed_refusal": nested,
                 "application_ids_to_names": lambda *_a: ("obo-sharepoint-reader", "v1"),
                 "validate_application_version_details": child_validation}
    [node] = [n for n in TREE.body if isinstance(n, ast.FunctionDef) and n.name == "_validate_toolkit_type_application"]
    exec(textwrap.dedent(ast.get_source_segment(SOURCE, node)), namespace)  # noqa: S102
    tool = {"settings": {"application_id": 1, "application_version_id": 2}}
    with pytest.raises(ValueError) as raised:
        namespace["_validate_toolkit_type_application"](None, tool, 5, 9, set())
    assert __import__("json").loads(str(raised.value)) == {
        "code": "DENIED_ASSERTION_UNAVAILABLE", "error": "obo-sharepoint-reader: Sign in with Microsoft again, then retry."}


# --- #7294 P-01: one batch per version ---------------------------------------------------------------------------

def configuration(index, project_id=2):
    return {"configuration_type": "github", "elitea_title": f"gh{index}", "configuration_uuid": f"uuid-{index}",
            "configuration_project_id": project_id, "access_token": SECRET}


def test_inside_a_validation_the_probe_is_deferred_and_asked_with_the_others_in_batches():
    """100 credential toolkits: no probe while the toolkits are walked, then two batch calls (64 + 36), where main asked
    100 single probes one after the other. A credential two toolkits use is probed once."""
    module, rpc = Module(enabled=True), Rpc()
    check = load(module, rpc)
    batch = LOADED["_ProbeBatch"]()
    token = LOADED["_PROBE_BATCH"].set(batch)
    try:
        per_toolkit = [check({"github_configuration": configuration(index)}, {}, 5, 9) for index in range(100)]
        repeat = check({"github_configuration": configuration(7)}, {}, 5, 9)
    finally:
        LOADED["_PROBE_BATCH"].reset(token)
    assert module.probes == [] and module.batches == []
    assert all(len(errors) == 1 and isinstance(errors[0], LOADED["_DeferredProbe"]) for errors in per_toolkit + [repeat])
    batch.run()
    assert [(project, len(uuids), user) for project, uuids, user in module.batches] == [(2, 64, 9), (2, 36, 9)]
    assert sorted(uuid for _, uuids, _ in module.batches for uuid in uuids) == sorted(f"uuid-{i}" for i in range(100))
    assert LOADED["_resolved"](repeat) == [] and all(LOADED["_resolved"](errors) == [] for errors in per_toolkit)


def test_a_batch_answer_becomes_the_same_entry_a_single_probe_gave():
    refused = {"success": False, "code": "DENIED_GRANT", "message": "You hold no grant."}
    failed = {"success": False, "message": "Authentication failed"}
    module, rpc = Module(enabled=True), Rpc()
    module.answers = {"uuid-1": refused, "uuid-2": failed, "uuid-3": {"success": False, "code": "RATE_LIMITED"}}
    check = load(module, rpc)
    batch = LOADED["_ProbeBatch"]()
    token = LOADED["_PROBE_BATCH"].set(batch)
    try:
        deferred = {index: check({"github_configuration": configuration(index)}, {}, 5, 9) for index in (1, 2, 3, 4)}
    finally:
        LOADED["_PROBE_BATCH"].reset(token)
    batch.run()
    entry = LOADED["_toolkit_connection_entry"]
    resolved = {index: LOADED["_resolved"](errors) for index, errors in deferred.items()}
    # what the single-probe path gives for the same answers
    single = {}
    for index, answer in ((1, refused), (2, failed), (3, module.answers["uuid-3"]), (4, {"success": True})):
        one = Module(enabled=True, answer=answer)
        single[index] = load(one, Rpc())({"github_configuration": configuration(index)}, {}, 5, 9) or []
    assert resolved == single
    assert entry(11, None, resolved[1])["type"] == "value_error"            # the governed refusal shape (#7281)
    assert entry(12, None, resolved[2])["type"] == "connection_error"
    assert entry(13, None, resolved[3]) is None and entry(14, None, resolved[4]) is None   # not checked / passed


def test_the_version_validation_opens_and_always_closes_the_batch_and_skips_the_author_lookup():
    body = ast.get_source_segment(SOURCE, next(node for node in TREE.body if isinstance(node, ast.FunctionDef)
                                               and node.name == "validate_application_version_details"))
    assert "batch_token = _PROBE_BATCH.set(batch)" in body and "finally:\n            _PROBE_BATCH.reset(batch_token)" in body
    assert "batch.run()" in body.split("finally:", 1)[1]
    loop = ast.get_source_segment(SOURCE, next(node for node in TREE.body if isinstance(node, ast.FunctionDef)
                                               and node.name == "_validate_version_toolkits"))
    # an empty authors map: ToolDetails.add_author_data skips its per-toolkit identity-service call
    assert "'authors_map': {}" in loop
