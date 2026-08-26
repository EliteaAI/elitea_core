"""Issue #6389 - reclaim abandoned index runs to a persisted 'interrupted' state.

A run killed hard (OOM, container restart) or hung never writes a terminal
state to its index_meta row, and nothing reconciled the leftover 'in_progress'
row - the read-time 'stale' flag was never persisted. These tests pin the
reclaim decision predicate (`should_reclaim_index_meta`) and the generalized
guarded write (`_finalize_index_meta_in_session`), including the behaviours the
reclaim must NOT break: the Stop path still lands 'cancelled', ownership guards
still protect a newer run on the same index_name, and the write is idempotent.

Run via:
    python tests/run_tests.py integration/test_6389_index_reclaim.py -v
"""

import importlib.util
import json
import pathlib
import sys
import time
import types

import pytest


PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]


@pytest.fixture
def application_tools_module():
    """Load application_tools with minimal stubs."""
    for name in (
        "plugins",
        "plugins.elitea_core",
        "plugins.elitea_core.models",
        "plugins.elitea_core.utils",
    ):
        mod = sys.modules.setdefault(name, types.ModuleType(name))
        mod.__path__ = []

    pylon = types.ModuleType("pylon")
    core = types.ModuleType("pylon.core")
    tools_mod = types.ModuleType("pylon.core.tools")
    tools_mod.log = types.SimpleNamespace(
        info=lambda *a, **k: None,
        warning=lambda *a, **k: None,
        error=lambda *a, **k: None,
        debug=lambda *a, **k: None,
        exception=lambda *a, **k: None,
    )
    sys.modules.setdefault("pylon", pylon)
    sys.modules.setdefault("pylon.core", core)
    sys.modules.setdefault("pylon.core.tools", tools_mod)

    tools_pkg = types.ModuleType("tools")
    tools_pkg.auth = types.SimpleNamespace(decorators=types.SimpleNamespace())
    tools_pkg.db = types.SimpleNamespace(get_session=lambda pid: None)
    tools_pkg.this = types.SimpleNamespace(
        descriptor=types.SimpleNamespace(config={})
    )
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
    enums.IndexDataStatus = type(
        "IndexDataStatus",
        (),
        {
            "in_progress": types.SimpleNamespace(value="in_progress"),
            "cancelled": types.SimpleNamespace(value="cancelled"),
            "interrupted": types.SimpleNamespace(value="interrupted"),
        },
    )
    sys.modules["plugins.elitea_core.models.enums.all"] = enums

    exceptions = types.ModuleType("plugins.elitea_core.utils.exceptions")
    exceptions.PoolSaturationError = type("PoolSaturationError", (Exception,), {})
    sys.modules["plugins.elitea_core.utils.exceptions"] = exceptions

    spec = importlib.util.spec_from_file_location(
        "plugins.elitea_core.utils.application_tools",
        PLUGIN_ROOT / "utils" / "application_tools.py",
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    yield module


TIMEOUT = 7200


def _row(state="in_progress", age=TIMEOUT * 1.5, task_id="task-1", now=1_000_000.0, **extra):
    cmetadata = {
        "collection": "idx",
        "type": "index_meta",
        "state": state,
        "task_id": task_id,
        "created_on": now - age - 60,
        "updated_on": now - age,
    }
    cmetadata.update(extra)
    return cmetadata


class TestShouldReclaimPredicate:
    NOW = 1_000_000.0

    def _call(self, m, cmetadata, status="running"):
        calls = []

        def resolver(task_id):
            calls.append(task_id)
            return status

        return m.should_reclaim_index_meta(cmetadata, self.NOW, TIMEOUT, resolver), calls

    def test_terminal_states_are_never_reclaimed(self, application_tools_module):
        m = application_tools_module
        for state in ("completed", "failed", "cancelled", "created", "scheduled_reindex", "interrupted"):
            decision, calls = self._call(m, _row(state=state))
            assert decision is False
            assert calls == []

    def test_young_in_progress_is_left_alone_without_liveness_probe(self, application_tools_module):
        decision, calls = self._call(application_tools_module, _row(age=TIMEOUT * 0.5))
        assert decision is False
        assert calls == []

    def test_old_run_with_live_task_is_left_alone(self, application_tools_module):
        decision, calls = self._call(application_tools_module, _row(), status="running")
        assert decision is False
        assert calls == ["task-1"]

    def test_running_verdict_stops_protecting_past_the_hard_ceiling(self, application_tools_module):
        m = application_tools_module
        ceiling = m.RECLAIM_HARD_CEILING_FACTOR * TIMEOUT
        below, _ = self._call(m, _row(age=ceiling - 1), status="running")
        above, calls = self._call(m, _row(age=ceiling + 1), status="running")
        assert below is False
        assert above is True
        assert calls == []

    def test_hard_ceiling_factor_is_overridable(self, application_tools_module):
        m = application_tools_module
        stale_for_four_timeouts = _row(age=TIMEOUT * 4)
        widened = m.should_reclaim_index_meta(
            stale_for_four_timeouts, self.NOW, TIMEOUT, lambda _: 'running', False, 6,
        )
        tightened = m.should_reclaim_index_meta(
            stale_for_four_timeouts, self.NOW, TIMEOUT, lambda _: 'running', False, 2,
        )
        assert widened is False
        assert tightened is True

    def test_hard_ceiling_never_applies_to_a_young_row(self, application_tools_module):
        m = application_tools_module
        decision, calls = self._call(m, _row(age=TIMEOUT * 0.5), status="running")
        assert decision is False
        assert calls == []

    def test_old_run_with_stopped_task_is_reclaimed(self, application_tools_module):
        decision, _ = self._call(application_tools_module, _row(), status="stopped")
        assert decision is True

    def test_old_run_with_lost_task_is_reclaimed(self, application_tools_module):
        m = application_tools_module
        decision, _ = self._call(m, _row(), status=m.TASK_LOST)
        assert decision is True

    def test_unresolvable_task_status_is_conservative(self, application_tools_module):
        decision, _ = self._call(application_tools_module, _row(), status="unknown")
        assert decision is False

    def test_untracked_row_is_never_reclaimed_by_default(self, application_tools_module):
        # An agent-inline run carries no task_id for its entire life.
        m = application_tools_module
        for age in (TIMEOUT * 1.5, TIMEOUT * 2.5, TIMEOUT * 100):
            decision, calls = self._call(m, _row(task_id=None, age=age))
            assert decision is False
            assert calls == []

    def test_untracked_row_opt_in_still_needs_double_age(self, application_tools_module):
        m = application_tools_module

        def opted_in(cmetadata):
            return m.should_reclaim_index_meta(
                cmetadata, self.NOW, TIMEOUT, lambda _: 'running', reclaim_untracked=True,
            )

        assert opted_in(_row(task_id=None, age=TIMEOUT * 1.5)) is False
        assert opted_in(_row(task_id=None, age=TIMEOUT * 2.5)) is True

    def test_unparseable_updated_on_is_never_reclaimed(self, application_tools_module):
        decision, _ = self._call(application_tools_module, _row(updated_on="garbage"), status="stopped")
        assert decision is False


class _FakeSession:
    def __init__(self):
        self.commits = 0

    def commit(self):
        self.commits += 1


def _meta_row(cmetadata):
    row = dict(cmetadata)
    row["history"] = json.dumps([dict(cmetadata)])
    return types.SimpleNamespace(cmetadata=row)


def _finalize(m, meta, target_state="interrupted", **kwargs):
    session = _FakeSession()
    m.get_toolkit_index_meta = lambda s, index_name, for_update=False: meta
    kwargs.setdefault("expected_task_id", meta.cmetadata.get("task_id") if meta else None)
    kwargs.setdefault("expected_created_on", meta.cmetadata.get("created_on") if meta else None)
    kwargs.setdefault("delete_embeddings", False)
    kwargs.setdefault("require_in_progress", True)
    result = m._finalize_index_meta_in_session(
        session, "idx", target_state,
        kwargs.pop("expected_task_id"), kwargs.pop("delete_embeddings"),
        kwargs.pop("require_in_progress"), kwargs.pop("expected_created_on"),
        **kwargs,
    )
    return result, session


class TestFinalizeWrite:

    def test_finalize_writes_state_error_and_history_consistently(self, application_tools_module):
        m = application_tools_module
        meta = _meta_row(_row())
        result, session = _finalize(m, meta, error="interrupted by the platform")
        assert result is True
        assert session.commits == 1
        assert meta.cmetadata["state"] == "interrupted"
        assert meta.cmetadata["task_id"] is None
        assert meta.cmetadata["error"] == "interrupted by the platform"
        history = json.loads(meta.cmetadata["history"])
        assert history[-1]["state"] == "interrupted"
        assert history[-1]["error"] == "interrupted by the platform"

    def test_second_pass_is_a_noop(self, application_tools_module):
        m = application_tools_module
        meta = _meta_row(_row())
        first, _ = _finalize(m, meta)
        second, session = _finalize(m, meta)
        assert first is True
        assert second is False
        assert session.commits == 0

    def test_newer_run_on_same_index_is_protected(self, application_tools_module):
        m = application_tools_module
        meta = _meta_row(_row(task_id="task-B", created_on=999.0))
        result, session = _finalize(
            m, meta, expected_task_id="task-A", expected_created_on=500.0,
        )
        assert result is False
        assert session.commits == 0
        assert meta.cmetadata["state"] == "in_progress"

    def test_untracked_row_created_on_mismatch_is_protected(self, application_tools_module):
        m = application_tools_module
        meta = _meta_row(_row(task_id=None, created_on=999.0))
        result, _ = _finalize(m, meta, expected_task_id=None, expected_created_on=500.0)
        assert result is False
        assert meta.cmetadata["state"] == "in_progress"

    def test_heartbeat_between_scan_and_write_stands_down(self, application_tools_module):
        m = application_tools_module
        meta = _meta_row(_row())
        meta.cmetadata["updated_on"] = time.time() - 10
        result, _ = _finalize(m, meta, min_updated_age=TIMEOUT)
        assert result is False
        assert meta.cmetadata["state"] == "in_progress"

    def test_stale_beyond_min_age_is_still_reclaimed(self, application_tools_module):
        m = application_tools_module
        meta = _meta_row(_row())
        meta.cmetadata["updated_on"] = time.time() - TIMEOUT * 1.5
        result, _ = _finalize(m, meta, min_updated_age=TIMEOUT)
        assert result is True
        assert meta.cmetadata["state"] == "interrupted"

    def test_cancel_wrapper_still_lands_cancelled(self, application_tools_module):
        m = application_tools_module
        meta = _meta_row(_row())
        session = _FakeSession()
        m.get_toolkit_index_meta = lambda s, index_name, for_update=False: meta
        result = m.cancel_toolkit_index_meta(
            "conn", "42", "idx",
            expected_task_id="task-1",
            expected_created_on=meta.cmetadata["created_on"],
            session=session,
        )
        assert result is True
        assert meta.cmetadata["state"] == "cancelled"
        assert meta.cmetadata["task_id"] is None
        history = json.loads(meta.cmetadata["history"])
        assert history[-1]["state"] == "cancelled"

    def test_ensure_task_id_resolves_from_ids_on_the_agent_path(self, application_tools_module):
        # The agent/chat path emits no toolkit_config.
        m = application_tools_module
        meta = _meta_row(_row(task_id=None))
        created_on = meta.cmetadata["created_on"]
        session = _FakeSession()
        resolved_with = []

        class _Ctx:
            def __enter__(self):
                return session

            def __exit__(self, *exc):
                return False

        def fake_resolve(project_id, toolkit_id, user_id=None):
            resolved_with.append((project_id, toolkit_id, user_id))
            return "conn", "56"

        m.resolve_toolkit_index_connection = fake_resolve
        m.get_session_for_schema = lambda conn, schema: _Ctx()
        m.get_toolkit_index_meta = lambda s, index_name, for_update=False: meta
        m.ensure_index_data_has_task_id(None, {
            "task_id": "task-new",
            "index_name": "idx",
            "project_id": 2,
            "toolkit_id": 56,
            "user_id": 3,
            "created_at": created_on,
            "updated_on": created_on + 5,
        })
        assert resolved_with == [(2, 56, 3)]
        assert meta.cmetadata["task_id"] == "task-new"
        assert session.commits == 1

    def test_ensure_task_id_gives_up_when_ids_cannot_resolve(self, application_tools_module):
        m = application_tools_module
        meta = _meta_row(_row(task_id=None))
        m.resolve_toolkit_index_connection = lambda *a, **k: (None, None)
        m.get_toolkit_index_meta = lambda *a, **k: (_ for _ in ()).throw(AssertionError("must not be reached"))
        m.ensure_index_data_has_task_id(None, {
            "task_id": "task-new",
            "index_name": "idx",
            "project_id": 2,
            "toolkit_id": 56,
        })
        assert meta.cmetadata["task_id"] is None

    def test_reclaim_wrapper_locks_the_row_and_writes_interrupted(self, application_tools_module):
        m = application_tools_module
        meta = _meta_row(_row())
        meta.cmetadata["updated_on"] = time.time() - TIMEOUT * 1.5
        meta.cmetadata["created_on"] = meta.cmetadata["updated_on"] - 60
        seen_for_update = []

        def fake_get(session, index_name, for_update=False):
            seen_for_update.append(for_update)
            return meta

        class _Ctx:
            def __enter__(self):
                return _FakeSession()

            def __exit__(self, *exc):
                return False

        m.get_toolkit_index_meta = fake_get
        m.get_session_for_schema = lambda conn, schema: _Ctx()
        result = m.reclaim_toolkit_index_meta(
            "conn", "42", "idx",
            expected_task_id="task-1",
            expected_created_on=meta.cmetadata["created_on"],
            min_updated_age=TIMEOUT,
        )
        assert result is True
        assert seen_for_update == [True]
        assert meta.cmetadata["state"] == "interrupted"
        assert meta.cmetadata["task_id"] == "task-1"
        assert f"over {TIMEOUT}s" in meta.cmetadata["error"]
