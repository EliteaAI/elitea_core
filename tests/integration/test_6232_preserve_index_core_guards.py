"""Issue #6232 - core-side guards that preserve the active index across reindex failures.

Covers the dispatch-reset merge (preserved measurements), the identity-free failed-state
writer guards, the scheduler start-failure caller gating, the notify fence polarity, the
advisory dispatch guard, and the one-transaction run-scoped cancel. SQL-level physics
(TOCTOU interleavings, ON CONFLICT arbitration, predicate semantics against real jsonb
rows) live in test_6232_live_sql_run_coordination.py.

Run via:
    python tests/run_tests.py integration/test_6232_preserve_index_core_guards.py -v
"""

import ast
import importlib.util
import json
import pathlib
import sys
import time
import types

import pytest
from sqlalchemy.dialects import postgresql
from sqlalchemy.exc import OperationalError, ProgrammingError


PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def application_tools():
    """Load application_tools standalone, per the test_6389 scaffold, but with the REAL
    models/indexer.py (the cancel path builds ORM predicates the tests compile)."""
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
    pylon_tools.web = types.SimpleNamespace(
        method=lambda *a, **k: (lambda f: f), rpc=lambda *a, **k: (lambda f: f),
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
    tools_pkg.rpc_tools = types.SimpleNamespace()
    sys.modules["tools"] = tools_pkg

    models_all = types.ModuleType("plugins.elitea_core.models.all")
    models_all.EliteATool = type("EliteATool", (), {})
    models_all.EntityToolMapping = type("EntityToolMapping", (), {})
    models_all.ApplicationVersion = type("ApplicationVersion", (), {})
    sys.modules["plugins.elitea_core.models.all"] = models_all

    indexer_spec = importlib.util.spec_from_file_location(
        "plugins.elitea_core.models.indexer",
        PLUGIN_ROOT / "models" / "indexer.py",
    )
    models_indexer = importlib.util.module_from_spec(indexer_spec)
    sys.modules[indexer_spec.name] = models_indexer
    indexer_spec.loader.exec_module(models_indexer)

    enums = types.ModuleType("plugins.elitea_core.models.enums.all")
    enums.ToolEntityTypes = type("ToolEntityTypes", (), {})
    enums.AgentTypes = type("AgentTypes", (), {})
    enums.InitiatorType = type("InitiatorType", (), {"user": "user"})
    enums.IndexDataStatus = type("IndexDataStatus", (), {
        "in_progress": types.SimpleNamespace(value="in_progress"),
        "cancelled": types.SimpleNamespace(value="cancelled"),
        "failed": types.SimpleNamespace(value="failed"),
    })
    sys.modules["plugins.elitea_core.models.enums.all"] = enums

    exceptions = types.ModuleType("plugins.elitea_core.utils.exceptions")
    exceptions.PoolSaturationError = type("PoolSaturationError", (Exception,), {})
    exceptions.MaintenanceInProgressError = type("MaintenanceInProgressError", (Exception,), {})
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


class _FakeQuery:
    def __init__(self, session, entity):
        self.session = session
        self.entity = entity
        self.clauses = []

    def filter(self, *clauses):
        self.clauses.extend(clauses)
        return self

    def delete(self, synchronize_session=False):
        self.session.ops.append(("delete", self.entity, list(self.clauses)))
        return 0


class _FakeSession:
    def __init__(self):
        self.ops = []

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False

    def execute(self, statement):
        self.ops.append(("execute", str(statement)))

    def query(self, entity):
        return _FakeQuery(self, entity)

    def add(self, obj):
        self.ops.append(("add", obj))

    def commit(self):
        self.ops.append(("commit",))

    @property
    def committed(self):
        return any(op[0] == "commit" for op in self.ops)

    @property
    def deletes(self):
        return [op for op in self.ops if op[0] == "delete"]


def _compiled(clause):
    return str(clause.compile(dialect=postgresql.dialect()))


def _bind_values(clauses):
    values = []
    for clause in clauses:
        values.extend(clause.compile(dialect=postgresql.dialect()).params.values())
    return values


def _run_row(run_id, status, heartbeat=None):
    return types.SimpleNamespace(
        run_id=run_id, status=status,
        heartbeat=heartbeat if heartbeat is not None else time.time(),
    )


def _arrange(application_tools, monkeypatch, cmetadata, run_rows="unset"):
    meta = types.SimpleNamespace(cmetadata=cmetadata) if cmetadata is not None else None
    session = _FakeSession()
    monkeypatch.setattr(application_tools, "get_session_for_schema", lambda *a: session)
    monkeypatch.setattr(application_tools, "get_toolkit_index_meta", lambda *a, **kw: meta)
    if run_rows != "unset":
        def _query(_session, _index_name, statuses=None, for_update=False):
            if run_rows is None:
                return None
            if statuses:
                return [row for row in run_rows if row.status in statuses]
            return list(run_rows)
        monkeypatch.setattr(application_tools, "query_index_runs", _query)
    return meta, session


class TestFindLastSuccessfulRun:

    def test_first_index_success_then_failed_reindex_is_the_flagship_case(self, application_tools):
        history = [
            {"state": "completed", "updated_on": 100.0, "indexed": 12},
            {"state": "failed", "updated_on": 200.0, "indexed": 12},
        ]
        found = application_tools.find_last_successful_run(history)
        assert found == history[0]

    def test_the_newest_successful_entry_wins(self, application_tools):
        history = [
            {"state": "completed", "updated_on": 100.0},
            {"state": "scheduled_reindex", "updated_on": 200.0},
            {"state": "failed", "updated_on": 300.0},
        ]
        assert application_tools.find_last_successful_run(history)["updated_on"] == 200.0

    def test_partly_indexed_counts_as_successful(self, application_tools):
        history = [{"state": "partly_indexed", "updated_on": 100.0}]
        assert application_tools.find_last_successful_run(history) is not None

    def test_no_successful_entry_returns_none(self, application_tools):
        assert application_tools.find_last_successful_run(
            [{"state": "failed"}, {"state": "cancelled"}, {"state": "in_progress"}]
        ) is None

    def test_empty_and_none_histories_return_none(self, application_tools):
        assert application_tools.find_last_successful_run([]) is None
        assert application_tools.find_last_successful_run(None) is None

    def test_non_dict_entries_are_ignored(self, application_tools):
        assert application_tools.find_last_successful_run(["garbage", 42]) is None


class TestResetPreservesMeasurements:
    """The dispatch reset replaces every platform-owned key from the default dict but keeps
    the previous run's promoted measurements readable, and never carries a stale error."""

    def _stored(self):
        return {
            "collection": "docs",
            "type": "index_meta",
            "state": "failed",
            "indexed": 120,
            "updated": 7,
            "total": 130,
            "report": '{"documents": 120}',
            "indexed_chunks": 4321,
            "error": "boom from last run",
            "task_id": "old-task",
            "created_on": 100.0,
            "updated_on": 200.0,
            "history": json.dumps([{"state": "completed", "indexed": 120}]),
        }

    def _default(self):
        return {
            "collection": "docs",
            "type": "index_meta",
            "indexed": 0,
            "updated": 0,
            "state": "in_progress",
            "index_configuration": {"index_name": "docs"},
            "created_on": 300.0,
            "updated_on": 300.0,
            "task_id": "new-task",
            "conversation_id": None,
            "toolkit_id": 42,
            "initiator": "user",
            "task_disconnected_timeout_sec": 7200,
        }

    def test_measurements_survive_the_reset_with_their_exact_values(self, application_tools, monkeypatch):
        meta, session = _arrange(application_tools, monkeypatch, self._stored())
        application_tools.reset_or_create_toolkit_index_meta("postgresql://", "42", "docs", self._default())
        assert meta.cmetadata["indexed"] == 120
        assert meta.cmetadata["updated"] == 7
        assert meta.cmetadata["total"] == 130
        assert meta.cmetadata["report"] == '{"documents": 120}'
        assert meta.cmetadata["indexed_chunks"] == 4321
        assert session.committed

    def test_platform_owned_keys_reset_from_the_default(self, application_tools, monkeypatch):
        meta, _ = _arrange(application_tools, monkeypatch, self._stored())
        application_tools.reset_or_create_toolkit_index_meta("postgresql://", "42", "docs", self._default())
        assert meta.cmetadata["state"] == "in_progress"
        assert meta.cmetadata["task_id"] == "new-task"
        assert meta.cmetadata["created_on"] == 300.0
        assert meta.cmetadata["initiator"] == "user"
        assert meta.cmetadata["task_disconnected_timeout_sec"] == 7200

    def test_a_stale_error_never_survives_into_the_in_progress_row(self, application_tools, monkeypatch):
        meta, _ = _arrange(application_tools, monkeypatch, self._stored())
        application_tools.reset_or_create_toolkit_index_meta("postgresql://", "42", "docs", self._default())
        assert "error" not in meta.cmetadata

    def test_the_history_entry_stays_the_reset_stub(self, application_tools, monkeypatch):
        meta, _ = _arrange(application_tools, monkeypatch, self._stored())
        application_tools.reset_or_create_toolkit_index_meta("postgresql://", "42", "docs", self._default())
        history = json.loads(meta.cmetadata["history"])
        assert history[-1]["state"] == "in_progress"
        assert history[-1]["indexed"] == 0
        assert history[-1]["updated"] == 0
        assert "report" not in history[-1]
        assert "history" not in history[-1]

    def test_a_missing_row_is_created_from_the_default_alone(self, application_tools, monkeypatch):
        _, session = _arrange(application_tools, monkeypatch, None)
        application_tools.reset_or_create_toolkit_index_meta("postgresql://", "42", "docs", self._default())
        added = [op[1] for op in session.ops if op[0] == "add"]
        assert len(added) == 1
        assert added[0].cmetadata["indexed"] == 0
        assert session.committed


TIMEOUT = 7200


class TestDispatchGuard:

    def test_a_fresh_pending_run_refuses_dispatch(self, application_tools, monkeypatch):
        _arrange(application_tools, monkeypatch, None,
                 run_rows=[_run_row("r1", "pending")])
        with pytest.raises(application_tools.IndexRunInProgressError):
            application_tools.reject_index_dispatch_when_run_live(
                "postgresql://", "42", "docs", TIMEOUT
            )

    def test_a_stale_pending_run_never_blocks(self, application_tools, monkeypatch):
        _arrange(application_tools, monkeypatch, None,
                 run_rows=[_run_row("r1", "pending", heartbeat=time.time() - TIMEOUT * 2)])
        application_tools.reject_index_dispatch_when_run_live(
            "postgresql://", "42", "docs", TIMEOUT
        )

    def test_a_missing_runs_table_allows_dispatch(self, application_tools, monkeypatch):
        _arrange(application_tools, monkeypatch, None, run_rows=None)
        application_tools.reject_index_dispatch_when_run_live(
            "postgresql://", "42", "docs", TIMEOUT
        )

    def test_no_rows_allow_dispatch(self, application_tools, monkeypatch):
        _arrange(application_tools, monkeypatch, None, run_rows=[])
        application_tools.reject_index_dispatch_when_run_live(
            "postgresql://", "42", "docs", TIMEOUT
        )


class TestFailedStateWriterGuard:
    """update_toolkit_index_meta_failed_state: identity-free entry guard + cancelled-meta skip."""

    def _row(self, state="in_progress"):
        return {
            "collection": "docs", "type": "index_meta", "state": state,
            "created_on": 100.0, "updated_on": 100.0,
            "history": json.dumps([{"state": "in_progress", "created_on": 100.0}]),
        }

    def test_a_live_registered_run_suppresses_the_flip(self, application_tools, monkeypatch):
        meta, session = _arrange(application_tools, monkeypatch, self._row(),
                                 run_rows=[_run_row("r1", "pending")])
        application_tools.update_toolkit_index_meta_failed_state("postgresql://", "42", "docs", "boom")
        assert meta.cmetadata["state"] == "in_progress"
        assert not session.committed

    def test_a_cancelled_row_keeps_cancels_snapshot(self, application_tools, monkeypatch):
        meta, session = _arrange(application_tools, monkeypatch, self._row(state="cancelled"),
                                 run_rows=[_run_row("r1", "cancelled")])
        application_tools.update_toolkit_index_meta_failed_state("postgresql://", "42", "docs", "boom")
        assert meta.cmetadata["state"] == "cancelled"
        assert not session.committed

    def test_a_row_with_no_registered_run_still_flips(self, application_tools, monkeypatch):
        meta, session = _arrange(application_tools, monkeypatch, self._row(), run_rows=[])
        application_tools.update_toolkit_index_meta_failed_state("postgresql://", "42", "docs", "boom")
        assert meta.cmetadata["state"] == "failed"
        assert meta.cmetadata["error"] == "boom"
        assert session.committed

    def test_a_terminal_run_row_never_blocks_a_late_callback(self, application_tools, monkeypatch):
        meta, session = _arrange(application_tools, monkeypatch, self._row(),
                                 run_rows=[_run_row("r1", "discarded")])
        application_tools.update_toolkit_index_meta_failed_state("postgresql://", "42", "docs", "boom")
        assert meta.cmetadata["state"] == "failed"
        assert session.committed


class TestSchedulerFailedStateWriter:
    """update_toolkit_index_meta_history_with_failed_state: pending-only guard, outcome
    return for caller gating, initiator/reindex stamping, conversation_id/task_id clearing."""

    def _row(self, history=None):
        return {
            "collection": "docs", "type": "index_meta", "state": "completed",
            "indexed": 55, "updated": 3,
            "task_id": "prev-task", "conversation_id": "prev-conv",
            "created_on": 100.0, "updated_on": 100.0,
            "history": json.dumps(history if history is not None
                                  else [{"state": "completed", "indexed": 55}]),
        }

    def test_a_live_run_returns_skipped_and_writes_nothing(self, application_tools, monkeypatch):
        meta, session = _arrange(application_tools, monkeypatch, self._row(),
                                 run_rows=[_run_row("r1", "pending")])
        outcome = application_tools.update_toolkit_index_meta_history_with_failed_state(
            "postgresql://", 42, "docs", "creds broke"
        )
        assert outcome == {"flipped": False, "skipped_live_run": True}
        assert meta.cmetadata["state"] == "completed"
        assert not session.committed

    def test_a_cancelled_meta_row_still_flips_here(self, application_tools, monkeypatch):
        row = self._row()
        row["state"] = "cancelled"
        meta, session = _arrange(application_tools, monkeypatch, row, run_rows=[])
        outcome = application_tools.update_toolkit_index_meta_history_with_failed_state(
            "postgresql://", 42, "docs", "creds broke"
        )
        assert outcome["flipped"] is True
        assert meta.cmetadata["state"] == "failed"

    def test_the_flip_stamps_initiator_reindex_and_clears_run_identity(self, application_tools, monkeypatch):
        meta, session = _arrange(application_tools, monkeypatch, self._row(), run_rows=[])
        outcome = application_tools.update_toolkit_index_meta_history_with_failed_state(
            "postgresql://", 42, "docs", "creds broke", initiator="schedule"
        )
        assert outcome["flipped"] is True
        assert outcome["reindex"] is True
        assert outcome["indexed"] == 55
        assert outcome["updated"] == 3
        entry = json.loads(meta.cmetadata["history"])[-1]
        assert entry["state"] == "failed"
        assert entry["error"] == "creds broke"
        assert entry["initiator"] == "schedule"
        assert entry["reindex"] is True
        assert entry["task_id"] is None
        assert entry["conversation_id"] is None
        assert session.committed

    def test_reindex_is_false_without_a_successful_history_entry(self, application_tools, monkeypatch):
        meta, _ = _arrange(application_tools, monkeypatch,
                           self._row(history=[{"state": "failed"}]), run_rows=[])
        outcome = application_tools.update_toolkit_index_meta_history_with_failed_state(
            "postgresql://", 42, "docs", "creds broke"
        )
        assert outcome["reindex"] is False

    def test_a_missing_row_reports_not_flipped_not_skipped(self, application_tools, monkeypatch):
        _arrange(application_tools, monkeypatch, None, run_rows=[])
        outcome = application_tools.update_toolkit_index_meta_history_with_failed_state(
            "postgresql://", 42, "docs", "creds broke"
        )
        assert outcome == {"flipped": False, "skipped_live_run": False}


class TestNotifyFence:
    """should_suppress_index_failure_notification: suppress only on a positively-observed
    cancel for a resolved schema+collection; everything else, including everything
    unresolvable, notifies."""

    def _config(self):
        return {"id": "42", "settings": {"pgvector_configuration": {"connection_string": "postgresql://"}}}

    def test_the_scheduler_start_failure_payload_notifies(self, application_tools):
        assert application_tools.should_suppress_index_failure_notification(
            {"index_name": "docs", "state": "failed"}
        ) is False

    def test_a_resolution_error_notifies(self, application_tools, monkeypatch):
        def _boom(*a, **kw):
            raise RuntimeError("lookup failed")
        monkeypatch.setattr(application_tools, "get_session_for_schema", _boom)
        assert application_tools.should_suppress_index_failure_notification(
            {"index_name": "docs", "toolkit_config": self._config()}
        ) is False

    def test_a_missing_runs_table_and_plain_failed_row_notifies(self, application_tools, monkeypatch):
        _arrange(application_tools, monkeypatch, {"state": "failed"}, run_rows=None)
        assert application_tools.should_suppress_index_failure_notification(
            {"index_name": "docs", "toolkit_config": self._config()}
        ) is False

    def test_a_failure_alongside_a_foreign_live_run_notifies(self, application_tools, monkeypatch):
        _arrange(application_tools, monkeypatch, {"state": "in_progress"},
                 run_rows=[_run_row("r1", "pending")])
        assert application_tools.should_suppress_index_failure_notification(
            {"index_name": "docs", "toolkit_config": self._config()}
        ) is False

    def test_a_failure_whose_own_discard_left_the_row_pending_notifies(self, application_tools, monkeypatch):
        _arrange(application_tools, monkeypatch, {"state": "failed"},
                 run_rows=[_run_row("r1", "pending")])
        assert application_tools.should_suppress_index_failure_notification(
            {"index_name": "docs", "toolkit_config": self._config()}
        ) is False

    def test_a_cancelled_meta_row_suppresses_the_stop_survivor_event(self, application_tools, monkeypatch):
        _arrange(application_tools, monkeypatch, {"state": "cancelled"},
                 run_rows=[_run_row("r1", "cancelled")])
        assert application_tools.should_suppress_index_failure_notification(
            {"index_name": "docs", "toolkit_config": self._config()}
        ) is True

    def test_a_genuine_failure_after_discard_notifies(self, application_tools, monkeypatch):
        _arrange(application_tools, monkeypatch, {"state": "failed"},
                 run_rows=[_run_row("r1", "discarded")])
        assert application_tools.should_suppress_index_failure_notification(
            {"index_name": "docs", "toolkit_config": self._config()}
        ) is False

    def test_a_missing_meta_row_notifies(self, application_tools, monkeypatch):
        _arrange(application_tools, monkeypatch, None, run_rows=[])
        assert application_tools.should_suppress_index_failure_notification(
            {"index_name": "docs", "toolkit_config": self._config()}
        ) is False

    def test_a_cancelled_run_row_under_a_running_index_notifies(self, application_tools, monkeypatch):
        """Run A was stopped and run B re-reset the row; B's genuine failure must reach its
        initiator even though A's tombstone is still there."""
        _arrange(application_tools, monkeypatch, {"state": "failed"},
                 run_rows=[_run_row("a", "cancelled"), _run_row("b", "discarded")])
        assert application_tools.should_suppress_index_failure_notification(
            {"index_name": "docs", "toolkit_config": self._config()}
        ) is False


class TestDeriveIndexReindexFlag:
    """derive_index_reindex_flag: server-side 'reindex' for events without the key (the
    worker-synthesized failed event); unresolvable events read as first-index."""

    def _config(self):
        return {"id": "42", "settings": {"pgvector_configuration": {"connection_string": "postgresql://"}}}

    def test_a_successful_history_entry_makes_it_a_reindex(self, application_tools, monkeypatch):
        _arrange(application_tools, monkeypatch,
                 {"history": json.dumps([{"state": "completed"}, {"state": "failed"}])})
        assert application_tools.derive_index_reindex_flag(
            {"index_name": "docs", "toolkit_config": self._config()}
        ) is True

    def test_no_successful_entry_reads_as_first_index(self, application_tools, monkeypatch):
        _arrange(application_tools, monkeypatch,
                 {"history": json.dumps([{"state": "failed"}, {"state": "cancelled"}])})
        assert application_tools.derive_index_reindex_flag(
            {"index_name": "docs", "toolkit_config": self._config()}
        ) is False

    def test_a_missing_toolkit_config_reads_as_first_index(self, application_tools):
        assert application_tools.derive_index_reindex_flag({"index_name": "docs"}) is False

    def test_a_resolution_error_reads_as_first_index(self, application_tools, monkeypatch):
        def _boom(*a, **kw):
            raise RuntimeError("lookup failed")
        monkeypatch.setattr(application_tools, "get_session_for_schema", _boom)
        assert application_tools.derive_index_reindex_flag(
            {"index_name": "docs", "toolkit_config": self._config()}
        ) is False

    def test_a_missing_meta_row_reads_as_first_index(self, application_tools, monkeypatch):
        _arrange(application_tools, monkeypatch, None)
        assert application_tools.derive_index_reindex_flag(
            {"index_name": "docs", "toolkit_config": self._config()}
        ) is False

    def test_an_already_decoded_history_list_is_accepted(self, application_tools, monkeypatch):
        _arrange(application_tools, monkeypatch, {"history": [{"state": "partly_indexed"}]})
        assert application_tools.derive_index_reindex_flag(
            {"index_name": "docs", "toolkit_config": self._config()}
        ) is True


class _FakePgError(Exception):
    pass


class TestQueryIndexRunsErrorRouting:
    """Only UndefinedTable (42P01) means 'runs table not provisioned'; every other
    ProgrammingError must reach the caller — cancel treats None as pre-P2 and would
    fall back to the legacy collection-wide delete over a promoted corpus."""

    class _Savepoint:
        def __init__(self):
            self.committed = False
            self.rolled_back = False

        def commit(self):
            self.committed = True

        def rollback(self):
            self.rolled_back = True

    class _BrokenSession:
        def __init__(self, error):
            self.error = error
            self.savepoint = None

        def begin_nested(self):
            self.savepoint = TestQueryIndexRunsErrorRouting._Savepoint()
            return self.savepoint

        def query(self, entity):
            raise self.error

    def _error(self, sqlstate, attr="pgcode"):
        orig = _FakePgError("boom")
        setattr(orig, attr, sqlstate)
        return ProgrammingError("SELECT 1", {}, orig)

    def test_undefined_table_reads_as_not_provisioned(self, application_tools):
        session = self._BrokenSession(self._error("42P01"))
        assert application_tools.query_index_runs(session, "docs") is None
        assert session.savepoint.rolled_back

    def test_the_psycopg3_sqlstate_attribute_is_recognized(self, application_tools):
        session = self._BrokenSession(self._error("42P01", attr="sqlstate"))
        assert application_tools.query_index_runs(session, "docs") is None

    @pytest.mark.parametrize("sqlstate", ["42703", "42501"])
    def test_any_other_programming_error_aborts_loudly(self, application_tools, sqlstate):
        session = self._BrokenSession(self._error(sqlstate))
        with pytest.raises(ProgrammingError):
            application_tools.query_index_runs(session, "docs")
        assert session.savepoint.rolled_back

    def test_a_non_undefined_table_error_aborts_the_cancel_instead_of_the_legacy_wipe(
            self, application_tools, monkeypatch):
        meta, session = _arrange(application_tools, monkeypatch, {
            "collection": "docs", "type": "index_meta", "state": "in_progress",
            "task_id": "task-1", "created_on": 100.0, "updated_on": 100.0,
            "history": json.dumps([{"state": "in_progress", "created_on": 100.0}]),
        })

        def _broken(*a, **kw):
            raise self._error("42703")
        monkeypatch.setattr(application_tools, "query_index_runs", _broken)
        with pytest.raises(ProgrammingError):
            application_tools._cancel_index_meta_in_session(
                session, "docs", None, True, False, None,
            )
        assert session.deletes == []
        assert not session.committed


class TestCancelRunScoped:
    """_cancel_index_meta_in_session: one transaction, mixed-deploy probe states, run-scoped
    chunk delete with no collection conjunct, tombstoned run rows."""

    def _row(self):
        return {
            "collection": "docs", "type": "index_meta", "state": "in_progress",
            "task_id": "task-1", "created_on": 100.0, "updated_on": 100.0,
            "history": json.dumps([{"state": "in_progress", "created_on": 100.0}]),
        }

    def _cancel(self, application_tools, session, delete_embeddings=True):
        return application_tools._cancel_index_meta_in_session(
            session, "docs", None, delete_embeddings, False, None,
        )

    def test_pending_runs_are_tombstoned_and_their_chunks_deleted_run_scoped(self, application_tools, monkeypatch):
        pending = _run_row("r1", "pending")
        terminal = _run_row("r0", "promoted")
        meta, session = _arrange(application_tools, monkeypatch, self._row(),
                                 run_rows=[pending, terminal])
        assert self._cancel(application_tools, session) is True
        assert pending.status == "cancelled"
        assert terminal.status == "promoted"
        assert meta.cmetadata["state"] == "cancelled"
        assert meta.cmetadata["task_id"] is None
        assert len(session.deletes) == 1
        compiled = " ".join(_compiled(clause) for clause in session.deletes[0][2])
        assert "@>" in compiled
        assert "IS NULL" in compiled
        values = _bind_values(session.deletes[0][2])
        assert {"_elitea_run_id": "r1"} in values
        assert "collection" not in values

    def test_none_pending_deletes_nothing(self, application_tools, monkeypatch):
        meta, session = _arrange(application_tools, monkeypatch, self._row(),
                                 run_rows=[_run_row("r0", "promoted"), _run_row("rX", "discarded")])
        assert self._cancel(application_tools, session) is True
        assert session.deletes == []
        assert meta.cmetadata["state"] == "cancelled"

    def test_no_rows_of_any_status_falls_back_to_the_legacy_collection_clean(self, application_tools, monkeypatch):
        _, session = _arrange(application_tools, monkeypatch, self._row(), run_rows=[])
        assert self._cancel(application_tools, session) is True
        assert len(session.deletes) == 1
        values = _bind_values(session.deletes[0][2])
        assert "collection" in values
        assert "docs" in values
        assert "index_meta" in values

    def test_a_missing_runs_table_falls_back_to_the_legacy_collection_clean(self, application_tools, monkeypatch):
        _, session = _arrange(application_tools, monkeypatch, self._row(), run_rows=None)
        assert self._cancel(application_tools, session) is True
        assert len(session.deletes) == 1
        values = _bind_values(session.deletes[0][2])
        assert "collection" in values
        assert "docs" in values

    def test_the_cancel_is_one_transaction_with_the_commit_last(self, application_tools, monkeypatch):
        _, session = _arrange(application_tools, monkeypatch, self._row(),
                              run_rows=[_run_row("r1", "pending")])
        self._cancel(application_tools, session)
        commits = [i for i, op in enumerate(session.ops) if op[0] == "commit"]
        assert len(commits) == 1
        assert commits[0] == len(session.ops) - 1

    def test_the_reconcile_path_tombstones_without_deleting(self, application_tools, monkeypatch):
        pending = _run_row("r1", "pending")
        meta, session = _arrange(application_tools, monkeypatch, self._row(), run_rows=[pending])
        assert self._cancel(application_tools, session, delete_embeddings=False) is True
        assert pending.status == "cancelled"
        assert session.deletes == []
        assert meta.cmetadata["state"] == "cancelled"


class TestEnsureDdlTwin:
    """The elitea_index_runs DDL twin must build (its classes only execute inside the
    ensure call) and compile to the arbitration shape the SDK's ON CONFLICT names."""

    class _StubEngine:
        def __init__(self):
            self.metadata = None

        def begin(self):
            import contextlib

            @contextlib.contextmanager
            def _ctx():
                yield types.SimpleNamespace(execute=lambda *a, **k: None)
            return _ctx()

        def _run_ddl_visitor(self, visitorcallable, element, **kwargs):
            self.metadata = element

    def _captured_ddl(self, application_tools, monkeypatch):
        from sqlalchemy.schema import CreateTable, CreateIndex
        engine = self._StubEngine()
        monkeypatch.setattr(application_tools, "_get_pgvector_engine", lambda conn: engine)
        application_tools.ensure_pgvector_schema_and_tables("postgresql://ignored", "sch_42")
        table = engine.metadata.tables["sch_42.elitea_index_runs"]
        statements = [_compiled(CreateTable(table))]
        statements.extend(_compiled(CreateIndex(index)) for index in table.indexes)
        return "\n".join(statements)

    def test_the_twin_builds_and_compiles_the_partial_unique_arbitration_index(self, application_tools, monkeypatch):
        ddl = self._captured_ddl(application_tools, monkeypatch)
        assert "CREATE UNIQUE INDEX uq_elitea_index_runs_live" in ddl
        assert "WHERE status = 'pending'" in ddl
        assert "run_id VARCHAR NOT NULL" in ddl
        assert "heartbeat DOUBLE PRECISION NOT NULL" in ddl
        for status in ("pending", "cancelled", "promoted", "discarded"):
            assert f"'{status}'" in ddl


@pytest.fixture(scope="module")
def index_scheduling(application_tools):
    """Load utils/index_scheduling.py on top of the application_tools scaffold."""
    spec = importlib.util.spec_from_file_location(
        "plugins.elitea_core.utils.index_scheduling",
        PLUGIN_ROOT / "utils" / "index_scheduling.py",
    )
    enums_pkg = types.ModuleType("plugins.elitea_core.models.enums")
    enums_pkg.InitiatorType = types.SimpleNamespace(schedule="schedule", user="user")
    sys.modules["plugins.elitea_core.models.enums"] = enums_pkg
    tools_pkg = sys.modules["tools"]
    tools_pkg.rpc_tools = types.SimpleNamespace(
        RpcMixin=lambda: types.SimpleNamespace(
            rpc=types.SimpleNamespace(
                timeout=lambda t: types.SimpleNamespace(
                    configurations_expand=lambda **kw: {"connection_string": "postgresql://"}
                )
            )
        )
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class TestHandleFailedIndexSchedule:
    """The caller gates the failure notification on the writer's locked-read outcome."""

    def _toolkit(self):
        return types.SimpleNamespace(id=42, type="confluence")

    def _capture_notify(self, index_scheduling, monkeypatch):
        sent = []
        monkeypatch.setattr(
            index_scheduling, "this",
            types.SimpleNamespace(module=types.SimpleNamespace(
                notify_index_data_status=lambda payload: sent.append(payload)
            )),
        )
        return sent

    def test_a_skipped_live_run_suppresses_the_notification(self, index_scheduling, monkeypatch):
        sent = self._capture_notify(index_scheduling, monkeypatch)
        monkeypatch.setattr(
            index_scheduling, "update_toolkit_index_meta_history_with_failed_state",
            lambda *a, **kw: {"flipped": False, "skipped_live_run": True},
        )
        index_scheduling.handle_failed_index_schedule(
            1, {}, 7, self._toolkit(), "docs", "creds broke",
        )
        assert sent == []

    def test_a_flip_notifies_with_the_derived_reindex_and_preserved_counts(self, index_scheduling, monkeypatch):
        sent = self._capture_notify(index_scheduling, monkeypatch)
        monkeypatch.setattr(
            index_scheduling, "update_toolkit_index_meta_history_with_failed_state",
            lambda *a, **kw: {"flipped": True, "skipped_live_run": False,
                              "reindex": True, "indexed": 55, "updated": 3},
        )
        index_scheduling.handle_failed_index_schedule(
            1, {}, 7, self._toolkit(), "docs", "creds broke",
        )
        assert len(sent) == 1
        assert sent[0]["reindex"] is True
        assert sent[0]["indexed"] == 55
        assert sent[0]["updated"] == 3
        assert sent[0]["state"] == "failed"
        assert sent[0]["initiator"] == "schedule"

    def test_a_missing_row_still_notifies(self, index_scheduling, monkeypatch):
        sent = self._capture_notify(index_scheduling, monkeypatch)
        monkeypatch.setattr(
            index_scheduling, "update_toolkit_index_meta_history_with_failed_state",
            lambda *a, **kw: {"flipped": False, "skipped_live_run": False},
        )
        index_scheduling.handle_failed_index_schedule(
            1, {}, 7, self._toolkit(), "docs", "creds broke",
        )
        assert len(sent) == 1
        assert sent[0]["reindex"] is False

    def test_a_lock_timeout_neither_notifies_nor_raises(self, index_scheduling, application_tools, monkeypatch):
        sent = self._capture_notify(index_scheduling, monkeypatch)

        def _locked(*a, **kw):
            raise application_tools.IndexMetaLockTimeoutError("docs")
        monkeypatch.setattr(
            index_scheduling, "update_toolkit_index_meta_history_with_failed_state", _locked,
        )
        index_scheduling.handle_failed_index_schedule(
            1, {}, 7, self._toolkit(), "docs", "creds broke",
        )
        assert sent == []


class TestCancelLegacyCollectionClean:
    """The run-unaware fallback is the only cancel path that empties a collection outright,
    and when it runs it must leave no count claiming the wiped data is still searchable."""

    def _row(self, history_states=("in_progress",), indexed_chunks=4321):
        return {
            "collection": "docs", "type": "index_meta", "state": "in_progress",
            "task_id": "task-1", "created_on": 100.0, "updated_on": 100.0,
            "indexed_chunks": indexed_chunks,
            "history": json.dumps([{"state": state, "created_on": 100.0}
                                   for state in history_states]),
        }

    def _cancel(self, application_tools, session, delete_embeddings=True):
        return application_tools._cancel_index_meta_in_session(
            session, "docs", None, delete_embeddings, False, None,
        )

    def test_the_legacy_clean_zeroes_the_retention_count(self, application_tools, monkeypatch):
        meta, session = _arrange(application_tools, monkeypatch, self._row(), run_rows=[])
        assert self._cancel(application_tools, session) is True
        assert len(session.deletes) == 1
        assert meta.cmetadata["indexed_chunks"] == 0

    def test_the_history_entry_agrees_with_the_zeroed_count(self, application_tools, monkeypatch):
        meta, session = _arrange(application_tools, monkeypatch, self._row(), run_rows=[])
        self._cancel(application_tools, session)
        assert json.loads(meta.cmetadata["history"])[-1]["indexed_chunks"] == 0

    def test_a_remembered_success_never_spares_the_clean_nor_the_count(self, application_tools, monkeypatch):
        """A run-unaware SDK empties the collection before it writes, so a 'completed' entry
        proves nothing about what is on disk when the Stop lands."""
        meta, session = _arrange(application_tools, monkeypatch,
                                 self._row(history_states=("completed", "in_progress")),
                                 run_rows=[])
        assert self._cancel(application_tools, session) is True
        assert len(session.deletes) == 1
        assert meta.cmetadata["indexed_chunks"] == 0
        assert meta.cmetadata["state"] == "cancelled"
        assert session.committed

    def test_a_missing_runs_table_cleans_and_zeroes_the_same_way(self, application_tools, monkeypatch):
        meta, session = _arrange(application_tools, monkeypatch,
                                 self._row(history_states=("partly_indexed", "in_progress")),
                                 run_rows=None)
        assert self._cancel(application_tools, session) is True
        assert len(session.deletes) == 1
        assert meta.cmetadata["indexed_chunks"] == 0

    def test_an_index_that_never_completed_still_gets_the_collection_clean(self, application_tools, monkeypatch):
        _, session = _arrange(application_tools, monkeypatch,
                              self._row(history_states=("failed", "in_progress")), run_rows=[])
        assert self._cancel(application_tools, session) is True
        values = _bind_values(session.deletes[0][2])
        assert "collection" in values
        assert "docs" in values

    def test_the_run_scoped_delete_leaves_the_count_alone(self, application_tools, monkeypatch):
        meta, session = _arrange(application_tools, monkeypatch, self._row(),
                                 run_rows=[_run_row("r1", "pending")])
        self._cancel(application_tools, session)
        assert meta.cmetadata["indexed_chunks"] == 4321

    def test_the_reconcile_path_never_zeroes_the_count(self, application_tools, monkeypatch):
        meta, session = _arrange(application_tools, monkeypatch, self._row(), run_rows=[])
        self._cancel(application_tools, session, delete_embeddings=False)
        assert session.deletes == []
        assert meta.cmetadata["indexed_chunks"] == 4321


class TestLiveRunFreshness:
    """has_live_index_run suppresses the failed-state writes, so its liveness must expire:
    an OOM-killed worker's pending row is only reclaimed when a NEXT run starts."""

    def _config(self):
        return {"id": "42", "settings": {"pgvector_configuration": {"connection_string": "postgresql://"}}}

    def _row(self, state="in_progress", timeout=None):
        row = {
            "collection": "docs", "type": "index_meta", "state": state,
            "created_on": 100.0, "updated_on": 100.0,
            "history": json.dumps([{"state": "in_progress", "created_on": 100.0}]),
        }
        if timeout is not None:
            row["task_disconnected_timeout_sec"] = timeout
        return row

    def test_a_fresh_pending_row_is_live(self, application_tools, monkeypatch):
        _, session = _arrange(application_tools, monkeypatch, None,
                              run_rows=[_run_row("r1", "pending")])
        assert application_tools.has_live_index_run(session, "docs", TIMEOUT) is True

    def test_a_stale_pending_row_is_not_live(self, application_tools, monkeypatch):
        _, session = _arrange(application_tools, monkeypatch, None,
                              run_rows=[_run_row("r1", "pending", heartbeat=time.time() - TIMEOUT * 2)])
        assert application_tools.has_live_index_run(session, "docs", TIMEOUT) is False

    def test_one_fresh_row_among_stale_ones_is_still_live(self, application_tools, monkeypatch):
        _, session = _arrange(application_tools, monkeypatch, None, run_rows=[
            _run_row("r0", "pending", heartbeat=time.time() - TIMEOUT * 2),
            _run_row("r1", "pending"),
        ])
        assert application_tools.has_live_index_run(session, "docs", TIMEOUT) is True

    def test_the_rows_own_timeout_is_read_from_the_meta_row(self, application_tools):
        meta = types.SimpleNamespace(cmetadata={"task_disconnected_timeout_sec": 60})
        assert application_tools.resolve_index_run_timeout(meta) == 60

    @pytest.mark.parametrize("cmetadata", [{}, {"task_disconnected_timeout_sec": None},
                                           {"task_disconnected_timeout_sec": "nope"}])
    def test_an_unusable_timeout_falls_back_to_the_default(self, application_tools, cmetadata):
        meta = types.SimpleNamespace(cmetadata=cmetadata)
        assert (application_tools.resolve_index_run_timeout(meta)
                == application_tools.DEFAULT_TASK_DISCONNECTED_TIMEOUT_SEC)

    def test_a_missing_meta_row_falls_back_to_the_default(self, application_tools):
        assert (application_tools.resolve_index_run_timeout(None)
                == application_tools.DEFAULT_TASK_DISCONNECTED_TIMEOUT_SEC)

    def test_a_stale_row_no_longer_suppresses_the_failed_state_write(self, application_tools, monkeypatch):
        meta, session = _arrange(
            application_tools, monkeypatch, self._row(),
            run_rows=[_run_row("r1", "pending", heartbeat=time.time() - TIMEOUT * 2)],
        )
        application_tools.update_toolkit_index_meta_failed_state("postgresql://", "42", "docs", "boom")
        assert meta.cmetadata["state"] == "failed"
        assert session.committed

    def test_the_meta_rows_timeout_decides_staleness_for_the_failed_state_write(self, application_tools, monkeypatch):
        meta, session = _arrange(
            application_tools, monkeypatch, self._row(timeout=60),
            run_rows=[_run_row("r1", "pending", heartbeat=time.time() - 600)],
        )
        application_tools.update_toolkit_index_meta_failed_state("postgresql://", "42", "docs", "boom")
        assert meta.cmetadata["state"] == "failed"

    def test_a_fresh_row_still_suppresses_the_failed_state_write(self, application_tools, monkeypatch):
        meta, session = _arrange(application_tools, monkeypatch, self._row(),
                                 run_rows=[_run_row("r1", "pending")])
        application_tools.update_toolkit_index_meta_failed_state("postgresql://", "42", "docs", "boom")
        assert meta.cmetadata["state"] == "in_progress"
        assert not session.committed

    def test_a_stale_row_no_longer_suppresses_the_scheduler_history_write(self, application_tools, monkeypatch):
        meta, session = _arrange(
            application_tools, monkeypatch, self._row(state="completed"),
            run_rows=[_run_row("r1", "pending", heartbeat=time.time() - TIMEOUT * 2)],
        )
        outcome = application_tools.update_toolkit_index_meta_history_with_failed_state(
            "postgresql://", 42, "docs", "creds broke"
        )
        assert outcome["flipped"] is True
        assert meta.cmetadata["state"] == "failed"

    def test_a_fresh_row_still_suppresses_the_scheduler_history_write(self, application_tools, monkeypatch):
        meta, session = _arrange(application_tools, monkeypatch, self._row(state="completed"),
                                 run_rows=[_run_row("r1", "pending")])
        outcome = application_tools.update_toolkit_index_meta_history_with_failed_state(
            "postgresql://", 42, "docs", "creds broke"
        )
        assert outcome == {"flipped": False, "skipped_live_run": True}

    def test_run_freshness_does_not_reach_the_failure_notification(self, application_tools, monkeypatch):
        _arrange(application_tools, monkeypatch, self._row(), run_rows=[_run_row("r1", "pending")])
        assert application_tools.should_suppress_index_failure_notification(
            {"index_name": "docs", "toolkit_config": self._config()}
        ) is False


class TestDeriveIndexIndexedChunks:
    """derive_index_indexed_chunks: server-side retention count for events from a worker
    that drops the field; unresolvable events, and collections no run-aware SDK owns,
    claim nothing."""

    def _config(self):
        return {"id": "42", "settings": {"pgvector_configuration": {"connection_string": "postgresql://"}}}

    def test_the_meta_rows_count_is_returned_for_a_run_aware_collection(self, application_tools, monkeypatch):
        _arrange(application_tools, monkeypatch, {"indexed_chunks": 4321},
                 run_rows=[_run_row("r0", "promoted")])
        assert application_tools.derive_index_indexed_chunks(
            {"index_name": "docs", "toolkit_config": self._config()}
        ) == 4321

    def test_a_collection_with_no_run_row_claims_nothing(self, application_tools, monkeypatch):
        """Old SDK + new core: the reset preserves the last success's count while the
        run-unaware worker empties the collection, so the stored number proves nothing."""
        _arrange(application_tools, monkeypatch, {"indexed_chunks": 4321}, run_rows=[])
        assert application_tools.derive_index_indexed_chunks(
            {"index_name": "docs", "toolkit_config": self._config()}
        ) == 0

    def test_a_missing_runs_table_claims_nothing(self, application_tools, monkeypatch):
        _arrange(application_tools, monkeypatch, {"indexed_chunks": 4321}, run_rows=None)
        assert application_tools.derive_index_indexed_chunks(
            {"index_name": "docs", "toolkit_config": self._config()}
        ) == 0

    def test_a_row_without_the_count_reads_as_no_retained_data(self, application_tools, monkeypatch):
        _arrange(application_tools, monkeypatch, {"state": "failed"},
                 run_rows=[_run_row("r0", "promoted")])
        assert application_tools.derive_index_indexed_chunks(
            {"index_name": "docs", "toolkit_config": self._config()}
        ) == 0

    def test_a_missing_meta_row_reads_as_no_retained_data(self, application_tools, monkeypatch):
        _arrange(application_tools, monkeypatch, None, run_rows=[_run_row("r0", "promoted")])
        assert application_tools.derive_index_indexed_chunks(
            {"index_name": "docs", "toolkit_config": self._config()}
        ) == 0

    def test_a_missing_toolkit_config_reads_as_no_retained_data(self, application_tools):
        assert application_tools.derive_index_indexed_chunks({"index_name": "docs"}) == 0

    def test_a_resolution_error_reads_as_no_retained_data(self, application_tools, monkeypatch):
        def _boom(*a, **kw):
            raise RuntimeError("lookup failed")
        monkeypatch.setattr(application_tools, "get_session_for_schema", _boom)
        assert application_tools.derive_index_indexed_chunks(
            {"index_name": "docs", "toolkit_config": self._config()}
        ) == 0


class TestDispatchOutlivesTheMetaWrite:
    """start_index_task dispatches before it writes the meta row, and that write can fail —
    on the lock, or anywhere after it: the caller's refusal must never leave a worker
    reindexing behind it."""

    @pytest.fixture
    def dispatch(self, application_tools, monkeypatch):
        state = types.SimpleNamespace(stopped=[], writes=[])
        monkeypatch.setattr(application_tools, "validate_toolkit_for_index",
                            lambda config: ("42", "postgresql://"))
        monkeypatch.setattr(application_tools, "reject_index_dispatch_when_run_live",
                            lambda *a, **kw: None)
        state.task_node = types.SimpleNamespace(
            start_task=lambda *a, **kw: "task-9",
            stop_task=lambda task_id: state.stopped.append(task_id),
        )
        state.data = {
            "toolkit_config": {"id": 42},
            "project_id": 1,
            "tool_name": "index_data",
            "tool_params": {"index_name": "docs"},
        }
        return state

    def _run(self, application_tools, dispatch):
        return application_tools.start_index_task(dispatch.task_node, dispatch.data, None)

    def test_a_clean_write_never_touches_the_task(self, application_tools, monkeypatch, dispatch):
        monkeypatch.setattr(application_tools, "reset_or_create_toolkit_index_meta",
                            lambda *a, **kw: dispatch.writes.append(kw.get("lock_timeout")))
        assert self._run(application_tools, dispatch) == "task-9"
        assert dispatch.writes == [None]
        assert dispatch.stopped == []

    def test_a_transient_lock_hold_is_retried_on_a_longer_wait(self, application_tools, monkeypatch, dispatch):
        def _write(*a, **kw):
            dispatch.writes.append(kw.get("lock_timeout"))
            if len(dispatch.writes) == 1:
                raise application_tools.IndexMetaLockTimeoutError("docs")
        monkeypatch.setattr(application_tools, "reset_or_create_toolkit_index_meta", _write)
        assert self._run(application_tools, dispatch) == "task-9"
        assert dispatch.writes == [None, application_tools.INDEX_META_RETRY_LOCK_TIMEOUT]
        assert dispatch.stopped == []

    def test_a_permanent_lock_hold_stops_the_dispatched_task_before_refusing(
            self, application_tools, monkeypatch, dispatch):
        def _write(*a, **kw):
            raise application_tools.IndexMetaLockTimeoutError("docs")
        monkeypatch.setattr(application_tools, "reset_or_create_toolkit_index_meta", _write)
        with pytest.raises(application_tools.IndexMetaLockTimeoutError):
            self._run(application_tools, dispatch)
        assert dispatch.stopped == ["task-9"]

    def test_a_failing_stop_does_not_mask_the_refusal(self, application_tools, monkeypatch, dispatch):
        def _write(*a, **kw):
            raise application_tools.IndexMetaLockTimeoutError("docs")
        monkeypatch.setattr(application_tools, "reset_or_create_toolkit_index_meta", _write)

        def _stop(task_id):
            raise RuntimeError("arbiter down")
        dispatch.task_node.stop_task = _stop
        with pytest.raises(application_tools.IndexMetaLockTimeoutError):
            self._run(application_tools, dispatch)

    def test_a_dropped_connection_at_the_commit_also_stops_the_task(
            self, application_tools, monkeypatch, dispatch):
        """Only the FOR UPDATE is converted to IndexMetaLockTimeoutError; the commit that
        follows it raises raw, and strands the worker exactly the same way."""
        def _write(*a, **kw):
            raise OperationalError("COMMIT", {}, Exception("server closed the connection"))
        monkeypatch.setattr(application_tools, "reset_or_create_toolkit_index_meta", _write)
        with pytest.raises(OperationalError):
            self._run(application_tools, dispatch)
        assert dispatch.stopped == ["task-9"]

    def test_a_commit_failure_on_the_retry_also_stops_the_task(
            self, application_tools, monkeypatch, dispatch):
        def _write(*a, **kw):
            dispatch.writes.append(kw.get("lock_timeout"))
            if len(dispatch.writes) == 1:
                raise application_tools.IndexMetaLockTimeoutError("docs")
            raise OperationalError("COMMIT", {}, Exception("server closed the connection"))
        monkeypatch.setattr(application_tools, "reset_or_create_toolkit_index_meta", _write)
        with pytest.raises(OperationalError):
            self._run(application_tools, dispatch)
        assert dispatch.stopped == ["task-9"]


@pytest.fixture(scope="module")
def sio_utils():
    spec = importlib.util.spec_from_file_location(
        "plugins.elitea_core.utils.sio_utils", PLUGIN_ROOT / "utils" / "sio_utils.py",
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _compile_index_dispatch_branch(namespace):
    """Compile the `tool_name == 'index_data'` branch of test_toolkit_tool_sio on its own:
    its module needs the whole plugin to import, and that branch is the last place a
    dispatch failure can still be reported to the socket caller."""
    tree = ast.parse((PLUGIN_ROOT / "rpc" / "application.py").read_text())
    method = next(node for node in ast.walk(tree)
                  if isinstance(node, ast.FunctionDef) and node.name == "test_toolkit_tool_sio")
    branch = next(node for node in method.body
                  if isinstance(node, ast.If) and ast.unparse(node.test) == "tool_name == 'index_data'")
    template = ast.parse(
        "def dispatch(self, sid, data, sio_event, project_id, tool_name='index_data'):\n"
        "    task_id = None\n"
        "    return task_id\n"
    )
    template.body[0].body.insert(1, branch)
    exec(compile(ast.fix_missing_locations(template), "rpc/application.py", "exec"), namespace)
    return namespace["dispatch"]


class TestSocketPathSurfacesEveryDispatchFailure:
    """The dispatch stops the worker it already started before it refuses, so a failure that
    reaches this caller is the whole run — and nothing outside this branch reports it."""

    @pytest.fixture
    def dispatch(self, application_tools, sio_utils):
        exceptions = sys.modules["plugins.elitea_core.utils.exceptions"]
        state = types.SimpleNamespace(emitted=[], raised=None,
                                      MaintenanceInProgressError=exceptions.MaintenanceInProgressError)
        state.sio = types.SimpleNamespace(emit=lambda **kwargs: state.emitted.append(kwargs))
        state.self = types.SimpleNamespace(
            context=types.SimpleNamespace(sio=state.sio),
            task_node=types.SimpleNamespace(),
        )
        state.data = {"stream_id": "s1", "message_id": "m1"}
        state.start_event_content = {}

        def _start(*args, **kwargs):
            if state.raised:
                raise state.raised
            return "task-9"

        state.call = _compile_index_dispatch_branch({
            "start_index_task": _start,
            "SioValidationError": sio_utils.SioValidationError,
            "IndexRunInProgressError": application_tools.IndexRunInProgressError,
            "IndexMetaLockTimeoutError": application_tools.IndexMetaLockTimeoutError,
            "PoolSaturationError": application_tools.PoolSaturationError,
            "start_event_content": state.start_event_content,
            "MaintenanceInProgressError": state.MaintenanceInProgressError,
            "log": types.SimpleNamespace(info=lambda *a, **k: None, exception=lambda *a, **k: None),
        })
        return state

    def _run(self, dispatch, sid="sid-1"):
        return dispatch.call(dispatch.self, sid, dispatch.data, "test_toolkit_tool", 1)

    def test_a_clean_dispatch_returns_the_task_and_emits_nothing(self, dispatch):
        assert self._run(dispatch) == "task-9"
        assert dispatch.emitted == []

    def test_a_run_in_progress_refusal_reaches_the_socket_with_its_own_wording(
            self, dispatch, application_tools, sio_utils):
        dispatch.raised = application_tools.IndexRunInProgressError("docs", "already in progress")
        with pytest.raises(sio_utils.SioValidationError):
            self._run(dispatch)
        assert dispatch.emitted[0]["data"]["content"] == "already in progress"

    def test_an_unexpected_failure_also_reaches_the_socket(self, dispatch, sio_utils):
        dispatch.raised = OperationalError("COMMIT", {}, Exception("server closed the connection"))
        with pytest.raises(sio_utils.SioValidationError) as raised:
            self._run(dispatch)
        assert dispatch.emitted[0]["data"]["content"] == "Failed to start indexing, please try again"
        assert dispatch.emitted[0]["data"]["stream_id"] == "s1"
        assert raised.value.__cause__ is dispatch.raised

    def test_the_socket_message_never_carries_the_database_error(self, dispatch, sio_utils):
        dispatch.raised = OperationalError("COMMIT", {}, Exception("server closed the connection"))
        with pytest.raises(sio_utils.SioValidationError):
            self._run(dispatch)
        assert "connection" not in dispatch.emitted[0]["data"]["content"]

    def test_a_caller_without_a_socket_keeps_the_original_exception(self, dispatch):
        dispatch.raised = OperationalError("COMMIT", {}, Exception("server closed the connection"))
        with pytest.raises(OperationalError):
            self._run(dispatch, sid=None)
        assert dispatch.emitted == []

    def test_pool_saturation_keeps_its_own_type_and_retry_hint_without_a_socket(
        self, dispatch, application_tools
    ):
        saturated = application_tools.PoolSaturationError("agents")
        saturated.retry_after = 5
        dispatch.raised = saturated
        with pytest.raises(application_tools.PoolSaturationError) as raised:
            self._run(dispatch, sid=None)
        assert raised.value.retry_after == 5
        assert dispatch.emitted == []

    def test_pool_saturation_still_reaches_a_socket_caller_as_its_own_type(
        self, dispatch, application_tools
    ):
        dispatch.raised = application_tools.PoolSaturationError("agents")
        with pytest.raises(application_tools.PoolSaturationError):
            self._run(dispatch)

    def test_pool_saturation_notifies_the_socket_without_naming_the_pool(
        self, dispatch, application_tools
    ):
        dispatch.raised = application_tools.PoolSaturationError("agents")
        with pytest.raises(application_tools.PoolSaturationError):
            self._run(dispatch)
        assert len(dispatch.emitted) == 1
        content = str(dispatch.emitted[0])
        assert "busy processing other requests" in content
        assert "agents" not in content

    def test_a_chat_hosted_caller_is_answered_on_its_own_placeholder(
        self, dispatch, application_tools, sio_utils
    ):
        dispatch.start_event_content["question_id"] = "q-42"
        dispatch.raised = application_tools.IndexRunInProgressError("busy")
        with pytest.raises(sio_utils.SioValidationError):
            self._run(dispatch)
        assert dispatch.emitted[0]["data"]["message_id"] == "q-42"

    def test_without_a_question_id_the_response_uuid_is_used(
        self, dispatch, application_tools, sio_utils
    ):
        dispatch.raised = application_tools.IndexRunInProgressError("busy")
        with pytest.raises(sio_utils.SioValidationError):
            self._run(dispatch)
        assert dispatch.emitted[0]["data"]["message_id"] == "m1"

    def test_maintenance_keeps_its_own_type_without_a_socket(self, dispatch):
        dispatch.raised = dispatch.MaintenanceInProgressError("indexing")
        with pytest.raises(dispatch.MaintenanceInProgressError):
            self._run(dispatch, sid=None)
        assert dispatch.emitted == []

    def test_maintenance_notifies_the_socket_without_naming_the_task(self, dispatch):
        dispatch.raised = dispatch.MaintenanceInProgressError("indexer_test_toolkit_tool")
        with pytest.raises(dispatch.MaintenanceInProgressError):
            self._run(dispatch)
        assert len(dispatch.emitted) == 1
        content = str(dispatch.emitted[0])
        assert "maintenance is in progress" in content
        assert "indexer_test_toolkit_tool" not in content


@pytest.fixture(scope="module")
def notifications(application_tools):
    """Load methods/notifications.py on top of the application_tools scaffold, with the real
    indexing_report so the rendered message is the one a reader would receive."""
    report_spec = importlib.util.spec_from_file_location(
        "plugins.elitea_core.utils.indexing_report",
        PLUGIN_ROOT / "utils" / "indexing_report.py",
    )
    indexing_report = importlib.util.module_from_spec(report_spec)
    sys.modules[report_spec.name] = indexing_report
    report_spec.loader.exec_module(indexing_report)

    class _State(str):
        @property
        def value(self):
            return str(self)

    enums = types.ModuleType("plugins.elitea_core.models.enums.all")
    enums.NotificationEventTypes = types.SimpleNamespace(index_data_changed="index_data_changed")
    enums.IndexDataStatus = types.SimpleNamespace(
        completed=_State("completed"), failed=_State("failed"), created=_State("created"),
        scheduled_reindex=_State("scheduled_reindex"), partly_indexed=_State("partly_indexed"),
    )
    sys.modules["plugins.elitea_core.models.enums.all"] = enums

    methods_pkg = sys.modules.setdefault(
        "plugins.elitea_core.methods", types.ModuleType("plugins.elitea_core.methods")
    )
    methods_pkg.__path__ = []
    spec = importlib.util.spec_from_file_location(
        "plugins.elitea_core.methods.notifications",
        PLUGIN_ROOT / "methods" / "notifications.py",
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class TestNotificationRetentionCount:
    """A worker older than the event-field whitelist drops indexed_chunks, and the retention
    predicate reads its absence as an emptied index."""

    def _config(self):
        return {"id": "42", "settings": {"pgvector_configuration": {"connection_string": "postgresql://"}}}

    def _notify(self, notifications, payload):
        fired = []
        method = notifications.Method()
        method.context = types.SimpleNamespace(
            event_manager=types.SimpleNamespace(
                fire_event=lambda name, event: fired.append((name, event))
            )
        )
        method.notify_index_data_status(payload)
        return fired

    def _payload(self, **overrides):
        payload = {
            "index_name": "docs", "state": "failed", "error": "boom", "reindex": True,
            "project_id": 1, "user_id": 7, "initiator": "user",
            "toolkit_config": self._config(),
        }
        payload.update(overrides)
        return payload

    def test_an_absent_count_is_derived_from_the_meta_row(self, notifications, application_tools,
                                                          monkeypatch):
        _arrange(application_tools, monkeypatch, {"state": "failed", "indexed_chunks": 4321},
                 run_rows=[_run_row("r0", "promoted")])
        fired = self._notify(notifications, self._payload())
        meta = fired[0][1]["meta"]
        assert meta["indexed_chunks"] == 4321
        assert meta["message"] == (
            "Index [docs]() reindex failed: boom."
            " Previously indexed data remains available for search."
        )

    def test_a_count_on_the_event_is_never_overridden(self, notifications, application_tools,
                                                      monkeypatch):
        _arrange(application_tools, monkeypatch, {"state": "failed", "indexed_chunks": 4321},
                 run_rows=[_run_row("r0", "promoted")])
        fired = self._notify(notifications, self._payload(indexed_chunks=0))
        assert fired[0][1]["meta"]["indexed_chunks"] == 0
        assert "remains available for search" not in fired[0][1]["meta"]["message"]

    def test_a_run_unaware_collection_never_claims_the_preserved_count(
            self, notifications, application_tools, monkeypatch):
        """The old-SDK rollout window: the meta row still carries the last success's count
        over a collection the run-unaware worker emptied."""
        _arrange(application_tools, monkeypatch, {"state": "failed", "indexed_chunks": 4321},
                 run_rows=[])
        fired = self._notify(notifications, self._payload())
        assert fired[0][1]["meta"]["indexed_chunks"] == 0
        assert fired[0][1]["meta"]["message"] == "Index [docs]() reindex failed: boom."

    def test_an_underivable_count_claims_no_retention(self, notifications, application_tools,
                                                      monkeypatch):
        _arrange(application_tools, monkeypatch, None, run_rows=[_run_row("r0", "promoted")])
        fired = self._notify(notifications, self._payload())
        assert fired[0][1]["meta"]["indexed_chunks"] == 0
        assert fired[0][1]["meta"]["message"] == "Index [docs]() reindex failed: boom."


class TestFailureNotificationReachesItsInitiator:
    """The notify path must never lose a genuine failure: for a scheduled run the
    notification is the only push channel, so a suppressed one surfaces to nobody until
    someone opens the page."""

    def _config(self):
        return {"id": "42", "settings": {"pgvector_configuration": {"connection_string": "postgresql://"}}}

    def _notify(self, notifications, payload):
        fired = []
        method = notifications.Method()
        method.context = types.SimpleNamespace(
            event_manager=types.SimpleNamespace(
                fire_event=lambda name, event: fired.append((name, event))
            )
        )
        method.notify_index_data_status(payload)
        return fired

    def _payload(self, **overrides):
        payload = {
            "index_name": "docs", "state": "failed", "error": "boom", "reindex": True,
            "indexed_chunks": 4321, "project_id": 1, "user_id": 7, "initiator": "user",
            "toolkit_config": self._config(),
        }
        payload.update(overrides)
        return payload

    def test_a_failure_alongside_a_foreign_live_run_notifies(self, notifications, application_tools,
                                                             monkeypatch):
        _arrange(application_tools, monkeypatch, {"state": "in_progress", "indexed_chunks": 4321},
                 run_rows=[_run_row("b", "pending")])
        fired = self._notify(notifications, self._payload())
        assert fired[0][1]["meta"]["error"] == "boom"

    def test_a_scheduled_failure_alongside_a_foreign_live_run_notifies(self, notifications,
                                                                       application_tools, monkeypatch):
        _arrange(application_tools, monkeypatch, {"state": "in_progress", "indexed_chunks": 4321},
                 run_rows=[_run_row("b", "pending")])
        fired = self._notify(notifications, self._payload(initiator="schedule"))
        assert fired[0][1]["meta"]["initiator"] == "schedule"

    def test_a_failure_whose_own_discard_left_the_row_pending_notifies(self, notifications,
                                                                       application_tools, monkeypatch):
        _arrange(application_tools, monkeypatch, {"state": "failed", "indexed_chunks": 4321},
                 run_rows=[_run_row("a", "pending")])
        fired = self._notify(
            notifications,
            self._payload(error="pgvector down; additionally failed to discard staged rows for the run"),
        )
        assert "failed to discard staged rows" in fired[0][1]["meta"]["error"]

    def test_the_post_stop_worker_fallback_event_stays_silent(self, notifications, application_tools,
                                                              monkeypatch):
        _arrange(application_tools, monkeypatch, {"state": "cancelled", "indexed_chunks": 4321},
                 run_rows=[_run_row("a", "cancelled")])
        assert self._notify(notifications, self._payload()) == []

    def test_a_successful_run_is_never_fenced(self, notifications, application_tools, monkeypatch):
        _arrange(application_tools, monkeypatch, {"state": "cancelled", "indexed_chunks": 4321},
                 run_rows=[_run_row("a", "cancelled")])
        fired = self._notify(notifications, self._payload(state="completed", error=None))
        assert fired[0][1]["meta"]["state"] == "completed"
