"""Unit tests for the admin budgets listing's pure helpers.

Covers the limit-source labelling an admin reads to understand why a limit applies, and
the owner-id resolution that makes a personal project findable by its owner's identity.

Run standalone: python3 tests/unit/test_project_budgets_helpers.py
"""

import os
import sys
import types
import unittest


def _load_module():
    """Load api/v2/project_budgets.py with the pylon/tools imports stubbed out."""
    plugin_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    #
    tools_stub = types.ModuleType("tools")
    tools_stub.api_tools = types.SimpleNamespace(
        APIModeHandler=object,
        APIBase=object,
        with_modes=lambda params: params,
        endpoint_metrics=lambda func: func,
    )
    tools_stub.auth = types.SimpleNamespace(
        current_user=lambda: {"id": 1},
        decorators=types.SimpleNamespace(check_api=lambda *a, **kw: (lambda func: func)),
    )
    tools_stub.config = types.SimpleNamespace(
        DEFAULT_MODE="default", ADMINISTRATION_MODE="administration",
    )
    tools_stub.rpc_tools = types.SimpleNamespace(RpcMixin=object)
    #
    flask_stub = types.ModuleType("flask")
    flask_stub.request = types.SimpleNamespace(args={})
    #
    # Overwrite rather than setdefault: the shared harness installs its own "tools" stub
    # without api_tools, and it would otherwise win here
    saved = {name: sys.modules.get(name) for name in ("tools", "flask")}
    sys.modules["tools"] = tools_stub
    sys.modules["flask"] = flask_stub
    #
    try:
        import importlib.util  # pylint: disable=C0415
        #
        spec = importlib.util.spec_from_file_location(
            "project_budgets_under_test",
            os.path.join(plugin_root, "api", "v2", "project_budgets.py"),
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    finally:
        for name, original in saved.items():
            if original is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = original


budgets = _load_module()


class FakeRpc:
    """Minimal stand-in for the RPC manager's timeout(...).auth_search_users chain."""

    def __init__(self, users=None, raises=False):
        self.users = users or []
        self.raises = raises
        self.calls = 0

    def timeout(self, _seconds):
        return self

    def auth_search_users(self, search):  # pylint: disable=W0613
        self.calls += 1
        #
        if self.raises:
            raise RuntimeError("auth is down")
        #
        return self.users


class TestOwnerIdsForSearch(unittest.TestCase):
    """A personal project is named project_user_N, so identity has to be resolved to ids."""

    def test_matching_users_become_ids(self):
        rpc = FakeRpc([{"id": 4}, {"id": 7}])
        self.assertEqual(budgets._owner_ids_for_search(rpc, "admin"), [4, 7])

    def test_no_match_returns_none_rather_than_empty_list(self):
        # An empty list would be OR'd in as "owner_id IN ()" and match nothing extra,
        # but None keeps the query identical to the no-owner-search case
        rpc = FakeRpc([])
        self.assertIsNone(budgets._owner_ids_for_search(rpc, "nobody"))

    def test_empty_search_makes_no_rpc_call(self):
        rpc = FakeRpc([{"id": 4}])
        self.assertIsNone(budgets._owner_ids_for_search(rpc, ""))
        self.assertIsNone(budgets._owner_ids_for_search(rpc, None))
        self.assertEqual(rpc.calls, 0)

    def test_rpc_failure_degrades_to_no_owner_search(self):
        # Name and id search must keep working when the auth lookup is unavailable
        rpc = FakeRpc(raises=True)
        self.assertIsNone(budgets._owner_ids_for_search(rpc, "admin"))


class TestLimitSource(unittest.TestCase):
    """The source label explains why a limit applies, so an admin isn't surprised by it."""

    def test_explicit_row_wins(self):
        self.assertEqual(
            budgets._limit_source({"enabled": True, "monthly_limit": 10.0}, 10.0), "explicit",
        )

    def test_disabled_row_is_unlimited_even_with_a_stored_limit(self):
        # An explicit row with enabled=false means deliberately exempt
        self.assertEqual(
            budgets._limit_source({"enabled": False, "monthly_limit": 10.0}, None), "unlimited",
        )

    def test_no_row_but_effective_limit_is_a_platform_default(self):
        self.assertEqual(budgets._limit_source({}, 25.0), "default")
        self.assertEqual(budgets._limit_source(None, 25.0), "default")

    def test_no_row_and_no_limit_is_unlimited(self):
        self.assertEqual(budgets._limit_source({}, None), "unlimited")
        self.assertEqual(budgets._limit_source(None, None), "unlimited")

    def test_row_without_explicit_limit_falls_through_to_default(self):
        self.assertEqual(
            budgets._limit_source({"enabled": True, "monthly_limit": None}, 5.0), "default",
        )

    def test_zero_limit_is_explicit_not_unlimited(self):
        # Zero is a real, very restrictive limit rather than "unset"
        self.assertEqual(
            budgets._limit_source({"enabled": True, "monthly_limit": 0.0}, 0.0), "explicit",
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
