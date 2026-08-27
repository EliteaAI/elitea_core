"""Issue #6389 - an armed schedule must retry a run that stopped reporting.

A worker killed without a terminal write leaves its index_meta row `in_progress`
forever, and the scheduler's gate skipped such rows silently on every tick — the
schedule starved with no log line. The gate now admits an in_progress row once it
is stale by the same rule the index list GET applies (`is_index_stale`), so both
surfaces agree on when a run stopped counting as alive.

Run via:
    python tests/run_tests.py integration/test_6389_schedule_stale_retry.py -v
"""

import importlib.util
import pathlib
import sys
import time
import types

import pytest


PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def application_tools():
    """Load application_tools standalone, per the test_6163 scaffold."""
    for name in (
        "plugins",
        "plugins.elitea_core",
        "plugins.elitea_core.models",
        "plugins.elitea_core.utils",
    ):
        mod = sys.modules.setdefault(name, types.ModuleType(name))
        mod.__path__ = []

    pylon_tools = types.ModuleType("pylon.core.tools")
    pylon_tools.log = types.SimpleNamespace(
        info=lambda *a, **k: None, warning=lambda *a, **k: None,
        error=lambda *a, **k: None, debug=lambda *a, **k: None,
        exception=lambda *a, **k: None,
    )
    sys.modules.setdefault("pylon", types.ModuleType("pylon"))
    sys.modules.setdefault("pylon.core", types.ModuleType("pylon.core"))
    sys.modules["pylon.core.tools"] = pylon_tools

    tools_pkg = types.ModuleType("tools")
    tools_pkg.auth = types.SimpleNamespace(decorators=types.SimpleNamespace())
    tools_pkg.db = types.SimpleNamespace(get_session=lambda pid: None)
    tools_pkg.this = types.SimpleNamespace(descriptor=types.SimpleNamespace(config={}))
    tools_pkg.serialize = types.SimpleNamespace()
    tools_pkg.context = types.SimpleNamespace()
    sys.modules["tools"] = tools_pkg

    models_all = types.ModuleType("plugins.elitea_core.models.all")
    models_all.EliteATool = type("EliteATool", (), {})
    models_all.EntityToolMapping = type("EntityToolMapping", (), {})
    models_all.ApplicationVersion = type("ApplicationVersion", (), {})
    sys.modules["plugins.elitea_core.models.all"] = models_all

    models_indexer = types.ModuleType("plugins.elitea_core.models.indexer")
    models_indexer.EmbeddingStore = type("EmbeddingStore", (), {})
    sys.modules["plugins.elitea_core.models.indexer"] = models_indexer

    enums = types.ModuleType("plugins.elitea_core.models.enums.all")
    enums.ToolEntityTypes = type("ToolEntityTypes", (), {})
    enums.AgentTypes = type("AgentTypes", (), {})
    enums.InitiatorType = type("InitiatorType", (), {"user": "user"})
    enums.IndexDataStatus = type("IndexDataStatus", (), {
        "in_progress": types.SimpleNamespace(value="in_progress"),
        "cancelled": types.SimpleNamespace(value="cancelled"),
    })
    sys.modules["plugins.elitea_core.models.enums.all"] = enums

    exceptions = types.ModuleType("plugins.elitea_core.utils.exceptions")
    exceptions.PoolSaturationError = type("PoolSaturationError", (Exception,), {})
    sys.modules["plugins.elitea_core.utils.exceptions"] = exceptions

    utils_utils = types.ModuleType("plugins.elitea_core.utils.utils")
    utils_utils.parse_ids_filter = lambda *a, **k: None
    sys.modules["plugins.elitea_core.utils.utils"] = utils_utils

    spec = importlib.util.spec_from_file_location(
        "plugins.elitea_core.utils.application_tools",
        PLUGIN_ROOT / "utils" / "application_tools.py",
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


TIMEOUT = 7200


class TestIsIndexStale:

    def test_a_terminal_state_is_never_stale(self, application_tools):
        long_ago = time.time() - TIMEOUT * 10
        for state in ("completed", "failed", "cancelled", "created", None):
            assert application_tools.is_index_stale(long_ago, state, TIMEOUT) is False

    def test_a_fresh_in_progress_run_is_not_stale(self, application_tools):
        assert application_tools.is_index_stale(time.time() - 10, "in_progress", TIMEOUT) is False

    def test_an_in_progress_run_past_the_timeout_is_stale(self, application_tools):
        assert application_tools.is_index_stale(time.time() - TIMEOUT * 1.5, "in_progress", TIMEOUT) is True

    def test_the_boundary_is_strictly_past_the_timeout(self, application_tools):
        # `>` not `>=`: exactly-at-timeout still counts as alive.
        now = time.time()
        assert application_tools.is_index_stale(now - TIMEOUT + 5, "in_progress", TIMEOUT) is False

    def test_a_row_with_no_updated_on_reads_as_stale(self, application_tools):
        # The GET defaults a missing updated_on to 0 before calling this; the epoch
        # is always past any timeout, so such a row is immediately reclaim-eligible.
        assert application_tools.is_index_stale(0, "in_progress", TIMEOUT) is True


class TestScheduleTriggerGate:
    """Exercises the production predicate the scheduler RPC calls — not a mirror of
    it, so a change there fails here."""

    def test_a_terminal_state_still_triggers(self, application_tools):
        gate = application_tools.should_trigger_scheduled_index
        assert gate("completed", stale_retry=False) is True
        assert gate("failed", stale_retry=False) is True

    def test_a_fresh_in_progress_run_still_skips(self, application_tools):
        assert application_tools.should_trigger_scheduled_index("in_progress", stale_retry=False) is False

    def test_a_stale_in_progress_run_now_triggers(self, application_tools):
        # The fix: the schedule no longer starves forever behind a dead run's row.
        assert application_tools.should_trigger_scheduled_index("in_progress", stale_retry=True) is True

    def test_a_missing_state_still_skips(self, application_tools):
        assert application_tools.should_trigger_scheduled_index(None, stale_retry=True) is False
        assert application_tools.should_trigger_scheduled_index("", stale_retry=True) is False

    def test_case_drift_in_the_stored_state_does_not_reopen_the_gate(self, application_tools):
        assert application_tools.should_trigger_scheduled_index("In_Progress", stale_retry=False) is False
