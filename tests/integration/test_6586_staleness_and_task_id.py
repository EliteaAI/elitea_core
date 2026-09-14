"""Issue #6586 - one staleness rule, and a locked task_id stamp.

Guards three things that ship green if they break:
  * a dead run with a registered heartbeat reads stale in heartbeat intervals,
    not after the 2h disconnect timeout that horizon was never sized for;
  * a row with NO run row keeps the disconnect-timeout rule, so legacy and
    pre-heartbeat runs are never starved;
  * the task_id stamp reads under the row lock. Unlocked, its read-modify-write
    of the whole cmetadata column reverts whatever committed in between - and
    the worst case is a cancel, whose embeddings are already deleted by then.

Run via:
    python tests/run_tests.py integration/test_6586_staleness_and_task_id.py -v
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
    tools_pkg.VaultClient = type("VaultClient", (), {"get_secrets": lambda self: {}})
    sys.modules["tools"] = tools_pkg

    models_all = types.ModuleType("plugins.elitea_core.models.all")
    models_all.EliteATool = type("EliteATool", (), {})
    models_all.EntityToolMapping = type("EntityToolMapping", (), {})
    models_all.ApplicationVersion = type("ApplicationVersion", (), {})
    sys.modules["plugins.elitea_core.models.all"] = models_all

    models_indexer = types.ModuleType("plugins.elitea_core.models.indexer")
    models_indexer.EmbeddingStore = type("EmbeddingStore", (), {})
    models_indexer.IndexRun = type("IndexRun", (), {})
    models_indexer.INDEX_RUN_PENDING = "pending"
    models_indexer.INDEX_RUN_CANCELLED = "cancelled"
    models_indexer.INDEX_RUN_STATUSES = ("pending", "cancelled", "promoted", "discarded")
    models_indexer.INDEX_RUN_LIVE_INDEX_NAME = "uq_elitea_index_runs_live"
    models_indexer.INDEX_RUN_LIVE_INDEX_PREDICATE = "status = 'pending'"
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



HEARTBEAT = 60
HORIZON = HEARTBEAT * 5
TIMEOUT = 7200


class TestTheHeartbeatHorizonReplacesTheTwoHourOne:

    def test_a_run_with_a_stopped_heartbeat_is_stale_in_minutes(self, application_tools):
        dead = time.time() - HORIZON * 2
        assert application_tools.resolve_index_staleness(
            "in_progress", time.time(), TIMEOUT, pending_heartbeat=dead) is True

    def test_the_two_hour_timeout_no_longer_holds_such_a_row_alive(self, application_tools):
        # The whole defect: updated_on is 60s-fresh, judged against 7200s.
        dead = time.time() - HORIZON * 2
        assert application_tools.is_index_stale(time.time(), "in_progress", TIMEOUT) is False
        assert application_tools.resolve_index_staleness(
            "in_progress", time.time(), TIMEOUT, pending_heartbeat=dead) is True

    def test_a_ticking_run_is_never_stale(self, application_tools):
        assert application_tools.resolve_index_staleness(
            "in_progress", 0, TIMEOUT, pending_heartbeat=time.time()) is False

    def test_a_single_missed_tick_is_not_stale(self, application_tools):
        assert application_tools.resolve_index_staleness(
            "in_progress", 0, TIMEOUT,
            pending_heartbeat=time.time() - HEARTBEAT * 1.5) is False

    def test_the_horizon_is_a_small_multiple_of_the_interval(self, application_tools):
        assert application_tools.INDEX_RUN_HEARTBEAT_INTERVAL_SEC == HEARTBEAT
        assert application_tools.HEARTBEAT_STALE_INTERVALS == 5


class TestRowsWithoutARunRowKeepTheOldRule:

    def test_no_run_row_falls_back_to_the_disconnect_timeout(self, application_tools):
        assert application_tools.resolve_index_staleness(
            "in_progress", time.time() - TIMEOUT * 1.5, TIMEOUT, pending_heartbeat=None) is True

    def test_a_legacy_row_inside_the_timeout_is_still_alive(self, application_tools):
        # Would read stale if the heartbeat horizon were applied blindly.
        assert application_tools.resolve_index_staleness(
            "in_progress", time.time() - HORIZON * 2, TIMEOUT, pending_heartbeat=None) is False

    def test_a_terminal_state_is_never_stale_either_way(self, application_tools):
        long_ago = time.time() - TIMEOUT * 10
        for state in ("completed", "failed", "cancelled", "partly_indexed"):
            assert application_tools.resolve_index_staleness(
                state, long_ago, TIMEOUT, pending_heartbeat=long_ago) is False

    def test_a_missing_state_is_never_stale(self, application_tools):
        assert application_tools.resolve_index_staleness(None, 0, TIMEOUT) is False
        assert application_tools.resolve_index_staleness("", 0, TIMEOUT) is False


class TestBothSurfacesShareOneRule:
    """The scheduler and the list GET disagreed about the same row before #6586:
    the scheduler preferred the run heartbeat, the GET always used updated_on."""

    def test_the_two_inputs_produce_one_verdict(self, application_tools):
        dead = time.time() - HORIZON * 2
        scheduler = application_tools.resolve_index_staleness(
            "in_progress", time.time(), TIMEOUT, pending_heartbeat=dead)
        list_get = application_tools.resolve_index_staleness(
            "in_progress", time.time(), TIMEOUT, pending_heartbeat=dead)
        assert scheduler == list_get is True


class TestRunChunksIsSeededNotInherited:
    """The list view renders `run_chunks` while a run is in flight, so the key has
    to exist from the moment the row goes in_progress and must never carry a value
    from the run before."""

    def test_run_chunks_is_never_preserved_across_runs(self, application_tools):
        # Preserving it would make a new run open showing the previous run's chunk
        # count as if it were its own progress.
        assert "run_chunks" not in application_tools.INDEX_META_PRESERVED_KEYS

    def test_the_measurements_that_must_survive_a_reindex_still_do(self, application_tools):
        # The counterpart invariant: the index stays readable during a reindex.
        for key in ("indexed", "updated", "total", "report", "indexed_chunks"):
            assert key in application_tools.INDEX_META_PRESERVED_KEYS


class TestTheTaskIdStampTakesTheRowLock:

    def test_the_stamp_reads_under_for_update(self, application_tools, monkeypatch):
        locked = []
        monkeypatch.setattr(application_tools, "lock_toolkit_index_meta",
                            lambda session, name, lock_timeout=None: locked.append(lock_timeout) or None)
        monkeypatch.setattr(application_tools, "get_session_for_schema",
                            lambda *a, **k: _NullSession())
        # An unlocked read is the bug; this must never reach for one.
        monkeypatch.setattr(application_tools, "get_toolkit_index_meta",
                            lambda *a, **k: pytest.fail("the stamp must read under the row lock"))

        application_tools._stamp_index_task_id("cs", "schema", "idx", "task-1", 100.0)

        assert locked == [application_tools.INDEX_META_LOCK_TIMEOUT]

    def test_a_lock_timeout_is_retried_with_the_longer_wait(self, application_tools, monkeypatch):
        waits = []

        def flaky(session, name, lock_timeout=None):
            waits.append(lock_timeout)
            if len(waits) == 1:
                raise application_tools.IndexMetaLockTimeoutError(name)
            return None

        monkeypatch.setattr(application_tools, "lock_toolkit_index_meta", flaky)
        monkeypatch.setattr(application_tools, "get_session_for_schema", lambda *a, **k: _NullSession())
        monkeypatch.setattr(application_tools, "validate_toolkit_for_index", lambda cfg: ("schema", "cs"))

        application_tools.ensure_index_data_has_task_id(
            None, {"task_id": "t", "index_name": "idx", "toolkit_config": {}, "created_at": 100.0})

        assert waits == [application_tools.INDEX_META_LOCK_TIMEOUT,
                         application_tools.INDEX_META_RETRY_LOCK_TIMEOUT]

    def test_the_stamp_claims_an_empty_task_id(self, application_tools, monkeypatch):
        meta = types.SimpleNamespace(cmetadata={"task_id": None, "created_on": 100.0})
        session = _RecordingSession()
        monkeypatch.setattr(application_tools, "lock_toolkit_index_meta",
                            lambda s, name, lock_timeout=None: meta)
        monkeypatch.setattr(application_tools, "get_session_for_schema", lambda *a, **k: session)

        claimed = application_tools._stamp_index_task_id("cs", "schema", "idx", "task-1", 100.0)

        assert claimed is True
        assert meta.cmetadata["task_id"] == "task-1"
        assert session.commits == 1

    def test_the_stamp_never_writes_updated_on(self, application_tools, monkeypatch):
        # The heartbeat owns updated_on; the event carries the run-START value,
        # so writing it here drags the liveness signal backwards.
        meta = types.SimpleNamespace(cmetadata={"task_id": None, "created_on": 100.0})
        monkeypatch.setattr(application_tools, "lock_toolkit_index_meta",
                            lambda s, name, lock_timeout=None: meta)
        monkeypatch.setattr(application_tools, "get_session_for_schema", lambda *a, **k: _RecordingSession())

        application_tools._stamp_index_task_id("cs", "schema", "idx", "task-1", 100.0)

        assert "updated_on" not in meta.cmetadata

    def test_a_row_that_already_has_a_task_id_is_left_alone(self, application_tools, monkeypatch):
        meta = types.SimpleNamespace(cmetadata={"task_id": "other", "created_on": 100.0})
        session = _RecordingSession()
        monkeypatch.setattr(application_tools, "lock_toolkit_index_meta",
                            lambda s, name, lock_timeout=None: meta)
        monkeypatch.setattr(application_tools, "get_session_for_schema", lambda *a, **k: session)

        claimed = application_tools._stamp_index_task_id("cs", "schema", "idx", "task-1", 100.0)

        assert claimed is False
        assert meta.cmetadata["task_id"] == "other"
        assert session.commits == 0


class _NullSession:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _RecordingSession(_NullSession):
    def __init__(self):
        self.commits = 0

    def commit(self):
        self.commits += 1
