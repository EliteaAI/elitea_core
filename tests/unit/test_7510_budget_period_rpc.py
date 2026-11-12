"""Writers of the monthly budget history (rpc/budget_periods.py, #7510).

Two things matter here. The write-through that follows an admin's budget edit must never
raise, because the edit has already been saved. And the hourly job must write only the rows
that differ, so an hour with nothing to record costs no writes.

Run standalone: python3 tests/unit/test_7510_budget_period_rpc.py
"""

import contextlib
import importlib.util
import os
import sys
import types
import unittest

PLUGIN_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
PKG = "elitea_pkg_7510_rpc"

LOGGED = []
SESSION = object()
UPSERTS = []
READS = {}


def _load_module():
    pylon = types.ModuleType("pylon")
    pylon_core = types.ModuleType("pylon.core")
    pylon_tools = types.ModuleType("pylon.core.tools")
    pylon_tools.web = types.SimpleNamespace(rpc=lambda *a, **kw: (lambda func: func))
    pylon_tools.log = types.SimpleNamespace(
        exception=lambda *a, **kw: LOGGED.append(("exception", a)),
        info=lambda *a, **kw: None,
        warning=lambda *a, **kw: None,
    )
    #
    @contextlib.contextmanager
    def session_scope(_project_id):
        yield SESSION
    #
    tools_stub = types.ModuleType("tools")
    tools_stub.db = types.SimpleNamespace(with_project_schema_session=session_scope)
    #
    names = ["pylon", "pylon.core", "pylon.core.tools", "tools",
             PKG, f"{PKG}.rpc", f"{PKG}.utils", f"{PKG}.utils.budget_periods"]
    saved = {name: sys.modules.get(name) for name in names}
    sys.modules.update({
        "pylon": pylon, "pylon.core": pylon_core, "pylon.core.tools": pylon_tools, "tools": tools_stub,
    })
    for name in (PKG, f"{PKG}.rpc", f"{PKG}.utils"):
        package = types.ModuleType(name)
        package.__path__ = []
        sys.modules[name] = package
    #
    try:
        for name, path in (
            (f"{PKG}.utils.budget_periods", os.path.join(PLUGIN_ROOT, "utils", "budget_periods.py")),
            (f"{PKG}.rpc.budget_periods", os.path.join(PLUGIN_ROOT, "rpc", "budget_periods.py")),
        ):
            spec = importlib.util.spec_from_file_location(name, path)
            module = importlib.util.module_from_spec(spec)
            sys.modules[name] = module
            spec.loader.exec_module(module)
        #
        # Database access is replaced on the helper module the RPC class calls into
        helpers = sys.modules[f"{PKG}.utils.budget_periods"]
        helpers.read_snapshots = lambda session, period, project_ids=None: dict(READS.get(period, {}))
        helpers.upsert_snapshots = lambda session, period, rows: UPSERTS.append((period, dict(rows)))
        #
        return module, helpers
    finally:
        for name, original in saved.items():
            if original is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = original


module, helpers = _load_module()


class FakeRpcManager:
    def __init__(self, projects, raises=False):
        self.projects = projects
        self.raises = raises

    def timeout(self, _seconds):
        return self

    def project_list(self):
        if self.raises:
            raise RuntimeError("projects is down")
        return self.projects


class Instance(module.RPC):
    """The RPC class bound to stand-ins for the methods that live in other rpc files."""

    def __init__(self, projects=(), budgets=None, limits=None, raises=False, read_raises=False):
        self.context = types.SimpleNamespace(rpc_manager=FakeRpcManager(list(projects), raises))
        self._budgets = budgets or {}
        self._limits = limits or {}
        self._read_raises = read_raises

    def list_project_budgets(self):
        return self._budgets

    def get_effective_project_limits(self, project_ids):
        return {pid: self._limits.get(pid) for pid in project_ids}

    def _read_project_budget(self, project_id):
        if self._read_raises:
            raise RuntimeError("db is down")
        return self._budgets.get(project_id)

    def get_effective_project_limit(self, project_id, project_budget=None):
        return self._limits.get(project_id)


class Base(unittest.TestCase):
    def setUp(self):
        UPSERTS.clear()
        READS.clear()
        LOGGED.clear()


class TestSnapshotProjectBudget(Base):
    def test_it_records_the_current_months_limit_and_source(self):
        instance = Instance(budgets={1: {"monthly_limit": 50.0, "enabled": True}}, limits={1: 50.0})
        #
        self.assertTrue(instance.snapshot_project_budget(1))
        #
        period, rows = UPSERTS[-1]
        self.assertEqual(period, helpers.current_period())
        self.assertEqual(rows[1]["effective_limit"], 50.0)
        self.assertEqual(rows[1]["limit_source"], "explicit")

    def test_removing_an_explicit_limit_records_the_default_that_now_applies(self):
        instance = Instance(budgets={1: {"monthly_limit": None, "enabled": True}}, limits={1: 20.0})
        #
        instance.snapshot_project_budget(1)
        #
        self.assertEqual(UPSERTS[-1][1][1]["limit_source"], "default")
        self.assertEqual(UPSERTS[-1][1][1]["effective_limit"], 20.0)

    def test_a_failed_history_write_never_raises_into_the_budget_edit(self):
        instance = Instance(read_raises=True)
        #
        self.assertFalse(instance.snapshot_project_budget(1))
        self.assertTrue(LOGGED)
        self.assertEqual(UPSERTS, [])


class TestSnapshotAllProjectBudgets(Base):
    PROJECTS = [{"id": 1}, {"id": 2}]

    def test_first_run_of_a_month_writes_every_project(self):
        instance = Instance(self.PROJECTS, limits={1: 50.0, 2: None})
        #
        result = instance.snapshot_all_project_budgets()
        #
        self.assertEqual(result, {"ok": True, "written": 2})
        self.assertEqual(sorted(UPSERTS[-1][1]), [1, 2])

    def test_nothing_is_written_when_nothing_changed(self):
        instance = Instance(self.PROJECTS, limits={1: 50.0, 2: None})
        READS[helpers.current_period()] = {
            1: {"project_id": 1, "period": helpers.current_period(),
                "effective_limit": 50.0, "limit_source": "default", "currency": "USD"},
            2: {"project_id": 2, "period": helpers.current_period(),
                "effective_limit": None, "limit_source": "unlimited", "currency": "USD"},
        }
        #
        result = instance.snapshot_all_project_budgets()
        #
        self.assertEqual(result, {"ok": True, "written": 0})
        self.assertEqual(UPSERTS, [])

    def test_only_the_changed_project_is_written(self):
        instance = Instance(self.PROJECTS, limits={1: 75.0, 2: None})
        period = helpers.current_period()
        READS[period] = {
            1: {"project_id": 1, "period": period,
                "effective_limit": 50.0, "limit_source": "default", "currency": "USD"},
            2: {"project_id": 2, "period": period,
                "effective_limit": None, "limit_source": "unlimited", "currency": "USD"},
        }
        #
        instance.snapshot_all_project_budgets()
        #
        self.assertEqual(sorted(UPSERTS[-1][1]), [1])
        self.assertEqual(UPSERTS[-1][1][1]["effective_limit"], 75.0)

    def test_only_the_current_month_is_ever_written(self):
        instance = Instance(self.PROJECTS, limits={1: 75.0})
        #
        instance.snapshot_all_project_budgets()
        #
        self.assertEqual({period for period, _ in UPSERTS}, {helpers.current_period()})

    def test_an_unreachable_projects_plugin_reports_failure_not_an_exception(self):
        instance = Instance(self.PROJECTS, raises=True)
        #
        self.assertEqual(instance.snapshot_all_project_budgets(), {"ok": False, "written": 0})
        self.assertEqual(UPSERTS, [])

    def test_no_projects_is_a_clean_no_op(self):
        self.assertEqual(Instance([]).snapshot_all_project_budgets(), {"ok": True, "written": 0})


if __name__ == "__main__":
    unittest.main(verbosity=2)
