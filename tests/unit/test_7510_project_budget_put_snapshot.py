"""Saving a project budget records the month's history (api/v2/project_budget.py, #7510).

The write-through lives in the PUT handler, not in rpc/project_budgets.py: that file is pinned
byte-for-byte by the #6980 tests, and this handler is the only caller of set_project_budget.
What matters is the order and the failure mode: the history is written after the save, and a
failing history write never turns a saved budget into an error.

Run standalone: python3 tests/unit/test_7510_project_budget_put_snapshot.py
"""

import importlib.util
import os
import sys
import types
import unittest

PLUGIN_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
PKG = "elitea_pkg_7510_put"

LOGGED = []


class FakeRequest:
    args = {}
    json = {"monthly_limit": 40.0, "enabled": True}


def _load_module():
    pylon = types.ModuleType("pylon")
    pylon_core = types.ModuleType("pylon.core")
    pylon_tools = types.ModuleType("pylon.core.tools")
    pylon_tools.log = types.SimpleNamespace(exception=lambda *a, **kw: LOGGED.append(a))
    #
    tools_stub = types.ModuleType("tools")
    tools_stub.api_tools = types.SimpleNamespace(
        APIModeHandler=object, APIBase=object, with_modes=lambda params: params,
        endpoint_metrics=lambda func: func,
    )
    tools_stub.auth = types.SimpleNamespace(
        decorators=types.SimpleNamespace(check_api=lambda *a, **kw: (lambda func: func)),
    )
    tools_stub.config = types.SimpleNamespace(DEFAULT_MODE="default", ADMINISTRATION_MODE="administration")
    tools_stub.rpc_tools = types.SimpleNamespace(RpcMixin=object)
    tools_stub.register_openapi = lambda *a, **kw: (lambda func: func)
    #
    flask_stub = types.ModuleType("flask")
    flask_stub.request = FakeRequest
    #
    constants = types.ModuleType(f"{PKG}.utils.constants")
    constants.PROMPT_LIB_MODE = "prompt_lib"
    features = types.ModuleType(f"{PKG}.utils.feature_budget_admin")
    features.request_feature = lambda request: None
    features.feature_state = lambda *a, **kw: {}
    features.feature_write = lambda *a, **kw: {}
    #
    names = ["pylon", "pylon.core", "pylon.core.tools", "tools", "flask", PKG, f"{PKG}.api",
             f"{PKG}.api.v2", f"{PKG}.utils", f"{PKG}.utils.constants", f"{PKG}.utils.feature_budget_admin",
             f"{PKG}.api.v2.project_budget"]
    saved = {name: sys.modules.get(name) for name in names}
    sys.modules.update({
        "pylon": pylon, "pylon.core": pylon_core, "pylon.core.tools": pylon_tools,
        "tools": tools_stub, "flask": flask_stub,
        constants.__name__: constants, features.__name__: features,
    })
    for name in (PKG, f"{PKG}.api", f"{PKG}.api.v2", f"{PKG}.utils"):
        package = types.ModuleType(name)
        package.__path__ = []
        sys.modules[name] = package
    sys.modules[constants.__name__] = constants
    sys.modules[features.__name__] = features
    #
    try:
        spec = importlib.util.spec_from_file_location(
            f"{PKG}.api.v2.project_budget", os.path.join(PLUGIN_ROOT, "api", "v2", "project_budget.py"),
        )
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        #
        return module
    finally:
        for name, original in saved.items():
            if original is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = original


module = _load_module()


class FakeRpc:
    def __init__(self, snapshot_raises=False):
        self.calls = []
        self.snapshot_raises = snapshot_raises

    def timeout(self, _seconds):
        return self

    def elitea_core_set_project_budget(self, **kwargs):
        self.calls.append(("set", kwargs["project_id"]))

    def elitea_core_snapshot_project_budget(self, **kwargs):
        self.calls.append(("snapshot", kwargs["project_id"]))
        #
        if self.snapshot_raises:
            raise RuntimeError("history is down")

    def elitea_core_get_project_budget(self, **kwargs):
        return {"monthly_limit": 40.0, "enabled": True}

    def usage_get_project_spend(self, **kwargs):
        return {"spend": 5.0, "available": True}

    def elitea_core_get_effective_project_limit(self, **kwargs):
        return 40.0


def _put(rpc):
    module.rpc_tools.RpcMixin = lambda: types.SimpleNamespace(rpc=rpc)
    #
    return module.AdminAPI().put(project_id=7)


class TestPutRecordsHistory(unittest.TestCase):
    def setUp(self):
        LOGGED.clear()

    def test_the_month_is_recorded_after_the_budget_is_saved(self):
        rpc = FakeRpc()
        #
        payload, status = _put(rpc)
        #
        self.assertEqual(status, 200)
        self.assertEqual(rpc.calls, [("set", 7), ("snapshot", 7)])
        self.assertEqual(payload["effective_limit"], 40.0)

    def test_a_failing_history_write_does_not_fail_the_saved_budget(self):
        rpc = FakeRpc(snapshot_raises=True)
        #
        payload, status = _put(rpc)
        #
        self.assertEqual(status, 200)
        self.assertEqual(payload["effective_limit"], 40.0)
        self.assertTrue(LOGGED)

    def test_the_budget_is_saved_before_the_history_is_attempted(self):
        rpc = FakeRpc(snapshot_raises=True)
        #
        _put(rpc)
        #
        self.assertEqual([name for name, _ in rpc.calls], ["set", "snapshot"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
