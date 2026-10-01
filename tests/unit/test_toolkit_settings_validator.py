"""Behavioural tests for ``RPC.toolkit_settings_validator`` and its validation-result reuse.

The RPC lives in a Pylon ``RPC`` class next to dozens of DB/SIO imports, so the module cannot be
imported in isolation. Instead the function's own source is lifted out of ``rpc/application.py``
with ``ast`` and executed against stubbed collaborators, with the real ``validator_cache``
module wired in. That keeps the test on the production code path: a missing local, a renamed
helper or a changed return shape fails here, which a source-text assertion would not catch.
"""

import ast
import importlib.util
import pathlib
import textwrap
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

TYPE_ = "artifact"
SCHEMA = {"title": "artifact", "properties": {"bucket": {"type": "string"}}}
SETTINGS = {"bucket": "reminder-queue", "selected_tools": ["list_files", "read_file"]}
VALIDATED = {"bucket": "reminder-queue", "selected_tools": ["list_files", "read_file"], "extra": 1}


def _load_cache_module(plugin_root: pathlib.Path):
    spec = importlib.util.spec_from_file_location(
        "validator_cache_under_test", plugin_root / "utils" / "validator_cache.py",
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _lift_function(plugin_root: pathlib.Path, name: str):
    source = (plugin_root / "rpc" / "application.py").read_text()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.FunctionDef) and node.name == name:
            lines = source.splitlines()[node.lineno - 1:node.end_lineno]
            return textwrap.dedent("\n".join(lines))
    raise AssertionError(f"{name} not found in rpc/application.py")


class FakeTaskNode:
    """Records dispatches and replays scripted indexer results in order."""

    def __init__(self, results):
        self.results = list(results)
        self.started = []

    def start_task(self, task_name, kwargs=None, pool=None, meta=None):
        self.started.append({"task_name": task_name, "kwargs": kwargs, "pool": pool, "meta": meta})
        return f"task-{len(self.started)}"

    def join_task(self, task_id, timeout=None):
        return self.results.pop(0)


class Harness:
    def __init__(self, plugin_root: pathlib.Path):
        cache_module = _load_cache_module(plugin_root)
        self.cache = cache_module.ValidatorResultCache()
        self.schema_lookup = Mock(return_value=(SCHEMA, False))
        namespace = {
            "find_toolkit_schema_by_type_everywhere": lambda *a, **kw: self.schema_lookup(*a, **kw),
            "make_validator_cache_key": cache_module.make_validator_cache_key,
            "toolkit_validator_cache": self.cache,
            "log": Mock(),
        }
        exec(_lift_function(plugin_root, "toolkit_settings_validator"), namespace)
        self._fn = namespace["toolkit_settings_validator"]
        self.cache_module = cache_module
        self.node = FakeTaskNode([])
        self.self_ = SimpleNamespace(task_node=self.node)

    def script(self, *results):
        self.node.results = list(results)

    def call(self, settings=None, type_=TYPE_):
        return self._fn(self.self_, SETTINGS if settings is None else settings, type_, 3, 7)


@pytest.fixture
def harness(plugin_root):
    return Harness(plugin_root)


class TestValidationResultReuse:
    @staticmethod
    def test_first_call_dispatches_and_returns_the_indexer_result(harness):
        harness.script({"result": VALIDATED})

        assert harness.call() == {"ok": True, "result": VALIDATED}
        assert len(harness.node.started) == 1
        dispatch = harness.node.started[0]
        assert dispatch["task_name"] == "indexer_validator"
        assert dispatch["pool"] == "indexer"
        assert dispatch["kwargs"] == {"toolkit_type": TYPE_, "settings": SETTINGS}

    @staticmethod
    def test_identical_second_call_is_served_without_a_dispatch(harness):
        harness.script({"result": VALIDATED})
        first = harness.call()

        second = harness.call()

        assert second == first == {"ok": True, "result": VALIDATED}
        assert len(harness.node.started) == 1

    @staticmethod
    def test_a_reused_result_cannot_be_corrupted_by_an_earlier_caller(harness):
        # Callers assign the validated dict into pydantic values and may mutate it in place;
        # that must never leak into what the next predict is handed.
        harness.script({"result": VALIDATED})
        harness.call()
        served = harness.call()
        served["result"]["selected_tools"].append("mutated")

        assert harness.call()["result"]["selected_tools"] == ["list_files", "read_file"]

    @staticmethod
    @pytest.mark.parametrize("changed", [
        {"bucket": "other-bucket", "selected_tools": ["list_files", "read_file"]},
        {"bucket": "reminder-queue", "selected_tools": ["list_files"]},
        {**SETTINGS, "token": "rotated-secret"},
    ])
    def test_any_settings_change_is_validated_afresh(harness, changed):
        harness.script({"result": VALIDATED}, {"result": {**VALIDATED, "fresh": True}})
        harness.call()

        result = harness.call(settings=changed)

        assert result["result"].get("fresh") is True
        assert len(harness.node.started) == 2

    @staticmethod
    def test_a_changed_toolkit_schema_is_validated_afresh(harness):
        # An SDK upgrade that alters the schema must not be answered with the old verdict.
        harness.script({"result": VALIDATED}, {"result": {**VALIDATED, "fresh": True}})
        harness.call()
        harness.schema_lookup.return_value = ({**SCHEMA, "properties": {"new_field": {}}}, False)

        assert harness.call()["result"].get("fresh") is True
        assert len(harness.node.started) == 2

    @staticmethod
    def test_different_toolkit_types_do_not_share_results(harness):
        harness.script({"result": VALIDATED}, {"result": {"other": True}})
        harness.call(type_="artifact")

        assert harness.call(type_="memory") == {"ok": True, "result": {"other": True}}
        assert len(harness.node.started) == 2

    @staticmethod
    def test_clearing_the_cache_forces_revalidation(harness):
        harness.script({"result": VALIDATED}, {"result": VALIDATED})
        harness.call()
        harness.cache.clear()

        harness.call()

        assert len(harness.node.started) == 2


class TestOutcomesThatMustNotBeReused:
    @staticmethod
    def test_a_rejection_is_returned_but_never_stored(harness):
        # Only successes are reusable: a user fixing their settings back to the same text
        # after a transient indexer failure must get a real verdict, not a remembered error.
        errors = [{"loc": ["bucket"], "msg": "field required", "type": "missing"}]
        harness.script({"error": errors}, {"result": VALIDATED})

        assert harness.call() == {"ok": False, "error": errors}
        assert len(harness.cache) == 0

        assert harness.call() == {"ok": True, "result": VALIDATED}
        assert len(harness.node.started) == 2

    @staticmethod
    def test_settings_that_cannot_be_keyed_are_validated_every_time(harness):
        unkeyable = {"bucket": "b", "handle": object()}
        harness.script({"result": VALIDATED}, {"result": VALIDATED})

        harness.call(settings=unkeyable)
        harness.call(settings=unkeyable)

        assert len(harness.node.started) == 2
        assert len(harness.cache) == 0

    @staticmethod
    def test_toolkits_with_large_sdk_schemas_are_still_reused(harness):
        # github / jira / confluence schemas are 30-42KB; a cap on the whole key input would
        # silently disable reuse for exactly the most common toolkits.
        harness.schema_lookup.return_value = ({**SCHEMA, "description": "x" * 45_000}, False)
        harness.script({"result": VALIDATED})

        harness.call()
        harness.call()

        assert len(harness.node.started) == 1

    @staticmethod
    def test_oversized_settings_bypass_the_cache(harness):
        huge = {"spec": "x" * harness.cache_module.MAX_CACHEABLE_SETTINGS_BYTES}
        harness.script({"result": VALIDATED}, {"result": VALIDATED})

        harness.call(settings=huge)
        harness.call(settings=huge)

        assert len(harness.node.started) == 2
        assert len(harness.cache) == 0


class TestPassthroughsDoNotTouchTheIndexer:
    @staticmethod
    def test_unknown_toolkit_type_passes_settings_through(harness):
        harness.schema_lookup.return_value = (None, False)

        assert harness.call() == {"ok": True, "result": SETTINGS}
        assert harness.node.started == []
        assert len(harness.cache) == 0

    @staticmethod
    def test_external_toolkits_pass_through_and_are_not_cached(harness):
        harness.schema_lookup.return_value = (SCHEMA, True)

        assert harness.call() == {"ok": True, "result": SETTINGS}
        assert harness.node.started == []
        assert len(harness.cache) == 0
