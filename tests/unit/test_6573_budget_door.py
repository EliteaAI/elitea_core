"""Unit tests for the advisory predict-door budget pre-check (utils/budget_door.py).

The door is deliberately weaker than the inference-plane gate: it may only ever turn a
call away when the usage plugin says a budget is already full, and every other outcome —
an unreachable RPC, a malformed answer, a missing project id — has to let the dispatch
through. These tests pin that asymmetry, plus the fact that a start_task call carries its
project id in `meta`/`kwargs` rather than at the top level.

Run standalone: python3 tests/unit/test_6573_budget_door.py
"""

import os
import sys
import types
import unittest


def _load_module(rpc_manager):
    """Load utils/budget_door.py with pylon/tools stubbed and a fake rpc_manager."""
    plugin_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    #
    pylon_pkg = types.ModuleType("pylon")
    pylon_core = types.ModuleType("pylon.core")
    pylon_tools = types.ModuleType("pylon.core.tools")
    pylon_tools.log = types.SimpleNamespace(
        debug=lambda *a, **kw: None,
        info=lambda *a, **kw: None,
        exception=lambda *a, **kw: None,
    )
    #
    tools_stub = types.ModuleType("tools")
    tools_stub.context = types.SimpleNamespace(rpc_manager=rpc_manager)
    #
    saved = {
        name: sys.modules.get(name)
        for name in ("pylon", "pylon.core", "pylon.core.tools", "tools")
    }
    sys.modules["pylon"] = pylon_pkg
    sys.modules["pylon.core"] = pylon_core
    sys.modules["pylon.core.tools"] = pylon_tools
    sys.modules["tools"] = tools_stub
    #
    try:
        import importlib.util  # pylint: disable=C0415
        #
        spec = importlib.util.spec_from_file_location(
            "budget_door_under_test",
            os.path.join(plugin_root, "utils", "budget_door.py"),
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        #
        return module
    finally:
        for name, value in saved.items():
            if value is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = value


class FakeGate:
    """Stands in for the usage plugin's read-only usage_gate_check RPC."""

    def __init__(self, verdict=None, raises=None):
        self.verdict = verdict
        self.raises = raises
        self.calls = []

    def timeout(self, _seconds):
        return self

    def usage_gate_check(self, project_id=None, user_id=None):
        self.calls.append((project_id, user_id))
        #
        if self.raises is not None:
            raise self.raises
        #
        return self.verdict


def door(verdict=None, raises=None):
    gate = FakeGate(verdict, raises)
    #
    return _load_module(gate), gate


class TestClosedScope(unittest.TestCase):
    """What the door reports back for each answer the gate can give."""

    def test_a_full_project_budget_closes_the_door(self):
        module, _ = door({"closed": True, "scope": "project", "healthy": True})
        #
        self.assertEqual(module.closed_budget_scope(42, 7), "project")

    def test_a_full_member_slice_closes_on_the_member_scope(self):
        module, _ = door({"closed": True, "scope": "member", "healthy": True})
        #
        self.assertEqual(module.closed_budget_scope(42, 7), "member")

    def test_headroom_leaves_the_door_open(self):
        module, _ = door({"closed": False, "scope": None, "healthy": True})
        #
        self.assertIsNone(module.closed_budget_scope(42, 7))

    def test_the_user_id_is_passed_through_so_member_limits_are_seen(self):
        module, gate = door({"closed": False, "scope": None, "healthy": True})
        module.closed_budget_scope(42, 7)
        #
        self.assertEqual(gate.calls, [(42, 7)])


class TestFailsOpen(unittest.TestCase):
    """Anything short of a definite "budget is full" has to let the dispatch through."""

    def test_an_unreachable_gate_never_blocks(self):
        module, _ = door(raises=RuntimeError("no usage plugin"))
        #
        self.assertIsNone(module.closed_budget_scope(42, 7))

    def test_a_missing_project_id_skips_the_check_entirely(self):
        module, gate = door({"closed": True, "scope": "project", "healthy": True})
        #
        self.assertIsNone(module.closed_budget_scope(None))
        self.assertEqual(gate.calls, [])

    def test_an_empty_answer_never_blocks(self):
        module, _ = door(None)
        #
        self.assertIsNone(module.closed_budget_scope(42))

    def test_an_unknown_verdict_never_blocks(self):
        module, _ = door({"closed": False, "scope": None, "healthy": False})
        #
        self.assertIsNone(module.closed_budget_scope(42))

    def test_closed_without_a_scope_still_blocks_but_reports_nothing_specific(self):
        module, _ = door({"closed": True, "scope": None, "healthy": True})
        #
        self.assertIsNone(module.closed_budget_scope(42))


class TestDispatchOwner(unittest.TestCase):
    """start_task kwargs nest the payload, so the ids are never at the top level."""

    def test_meta_carries_the_ids(self):
        module, _ = door()
        #
        self.assertEqual(
            module.dispatch_owner({"meta": {"project_id": 3, "user_id": 9}, "kwargs": {}}),
            (3, 9),
        )

    def test_the_task_payload_is_the_fallback(self):
        module, _ = door()
        #
        self.assertEqual(
            module.dispatch_owner({"kwargs": {"project_id": 3, "user_id": 9}}),
            (3, 9),
        )

    def test_meta_wins_over_the_payload(self):
        module, _ = door()
        #
        self.assertEqual(
            module.dispatch_owner({"meta": {"project_id": 3}, "kwargs": {"project_id": 4}}),
            (3, None),
        )

    def test_a_dispatch_with_neither_yields_nothing_to_check(self):
        module, _ = door()
        #
        self.assertEqual(module.dispatch_owner({"pool": "indexer"}), (None, None))


class TestErrorContract(unittest.TestCase):
    """The refusal has to be recognisable as the same budget refusal the proxy returns."""

    def test_the_error_carries_the_wire_triple(self):
        plugin_root = os.path.dirname(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        )
        namespace = {}
        source = open(  # pylint: disable=R1732,W1514
            os.path.join(plugin_root, "utils", "exceptions.py"),
        ).read()
        exec(compile(source, "exceptions.py", "exec"), namespace)  # pylint: disable=W0122
        #
        error = namespace["BudgetDoorClosedError"](scope="member", project_id=42)
        #
        self.assertEqual(error.type, "budget_exceeded")
        self.assertEqual(error.code, "member_budget_exceeded")
        self.assertEqual(error.message, namespace["BUDGET_ERROR_MESSAGE"])
        self.assertEqual(str(error), namespace["BUDGET_ERROR_MESSAGE"])

    def test_an_unknown_scope_falls_back_to_the_project_code(self):
        plugin_root = os.path.dirname(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        )
        namespace = {}
        source = open(  # pylint: disable=R1732,W1514
            os.path.join(plugin_root, "utils", "exceptions.py"),
        ).read()
        exec(compile(source, "exceptions.py", "exec"), namespace)  # pylint: disable=W0122
        #
        self.assertEqual(
            namespace["BudgetDoorClosedError"](scope="whatever").code,
            "project_budget_exceeded",
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
