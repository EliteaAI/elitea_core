"""The hourly budget-history job is registered on its own (module.py, #7510).

It is deliberately outside the platform schedule inventory that the #6556 tests pin, so these
tests cover what that inventory no longer does for it: ready() registers the job, and a
missing or failing scheduling plugin skips only this job instead of breaking startup.

Run standalone: python3 tests/unit/test_7510_budget_period_schedule.py
"""

import types
import unittest
from pathlib import Path
from queue import Empty

PLUGIN_ROOT = Path(__file__).parents[2]
SOURCE = (PLUGIN_ROOT / "module.py").read_text()


def _method(name, next_marker):
    start = SOURCE.index(f"    def {name}(")
    return SOURCE[start:SOURCE.index(next_marker, start)]


def _bind_register(error=None):
    body = _method("_register_budget_period_snapshot", "    # Provider Hub Methods")
    created, logs = [], []
    #
    class Rpc:
        def timeout(self, _seconds):
            return self

        def scheduling_create_if_not_exists(self, payload):
            if error is not None:
                raise error
            created.append(payload)
    #
    namespace = {
        "Empty": Empty,
        "log": types.SimpleNamespace(
            warning=lambda message, *a: logs.append(("warning", message % a if a else message)),
            exception=lambda message, *a: logs.append(("exception", message % a if a else message)),
        ),
    }
    exec(compile("class _M:\n" + body, "module.py", "exec"), namespace)
    holder = types.SimpleNamespace(context=types.SimpleNamespace(rpc_manager=Rpc()))
    #
    return namespace["_M"], holder, created, logs


class TestRegistration(unittest.TestCase):
    def test_it_creates_the_hourly_job_for_the_snapshot_rpc(self):
        cls, holder, created, _ = _bind_register()
        #
        cls._register_budget_period_snapshot(holder)
        #
        self.assertEqual(len(created), 1)
        self.assertEqual(created[0]["name"], "project_budget_period_snapshot")
        self.assertEqual(created[0]["rpc_func"], "elitea_core_snapshot_all_project_budgets")
        self.assertTrue(created[0]["active"])
        # Hourly, off the :00 mark every other job lands on
        minute, hour, *_ = created[0]["cron"].split()
        self.assertEqual(hour, "*")
        self.assertNotIn(minute, ("0", "*"))

    def test_the_rpc_it_names_exists(self):
        rpc_source = (PLUGIN_ROOT / "rpc" / "budget_periods.py").read_text()
        #
        self.assertIn('@web.rpc("elitea_core_snapshot_all_project_budgets"', rpc_source)

    def test_a_missing_scheduling_plugin_only_skips_this_job(self):
        cls, holder, created, logs = _bind_register(error=Empty())
        #
        cls._register_budget_period_snapshot(holder)
        #
        self.assertEqual(created, [])
        self.assertEqual([level for level, _ in logs], ["warning"])
        self.assertIn("project_budget_period_snapshot", logs[0][1])

    def test_any_other_failure_is_logged_not_raised(self):
        cls, holder, _, logs = _bind_register(error=RuntimeError("boom"))
        #
        cls._register_budget_period_snapshot(holder)
        #
        self.assertEqual([level for level, _ in logs], ["exception"])


class TestReadyRunsIt(unittest.TestCase):
    def test_ready_registers_the_job_after_the_platform_schedules(self):
        start = SOURCE.index("    def ready(self):")
        ready = SOURCE[start:SOURCE.index("\n    def ", start + 1)]
        #
        self.assertIn("self._bootstrap_platform_schedules()", ready)
        self.assertIn("self._register_budget_period_snapshot()", ready)
        self.assertLess(
            ready.index("self._bootstrap_platform_schedules()"),
            ready.index("self._register_budget_period_snapshot()"),
        )


class TestPinnedAreasUntouched(unittest.TestCase):
    """The existing inventories must not learn about this job."""

    def test_the_platform_schedule_inventory_does_not_list_it(self):
        bootstrap = _method("_bootstrap_platform_schedules", "    def get_managed_schedules")
        #
        self.assertNotIn("project_budget_period_snapshot", bootstrap)

    def test_reconfig_does_not_start_extra_work(self):
        reconfig = _method("reconfig", "    def _bootstrap_platform_schedules")
        #
        self.assertNotIn("snapshot", reconfig)
        self.assertNotIn("Thread", reconfig)


if __name__ == "__main__":
    unittest.main(verbosity=2)
