"""Issue #6422 - saving an index configuration without starting a reindex.

`build_index_configuration` is the single definition of what a persisted index configuration
looks like, shared by `start_index_task` and the config-only save endpoint. Scheduled runs feed
`cmetadata['index_configuration']` straight into `start_index_task`, which subscripts it - so the
two invariants under test are that the result is always a dict and always carries `index_name`.

Run via:
    python tests/run_tests.py integration/test_6422_index_configuration_save.py -v
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
    """Load application_tools standalone, per the test_6389 scaffold."""
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


class TestBuildIndexConfiguration:

    def test_a_dict_payload_passes_through(self, application_tools):
        payload = {"index_name": "docs", "chunk_size": 1000}
        assert application_tools.build_index_configuration(payload) == payload

    def test_the_result_is_a_copy(self, application_tools):
        payload = {"index_name": "docs"}
        result = application_tools.build_index_configuration(payload)
        result["chunk_size"] = 1000
        assert "chunk_size" not in payload

    def test_a_json_string_payload_is_coerced_to_a_dict(self, application_tools):
        result = application_tools.build_index_configuration('{"index_name": "docs", "chunk_size": 500}')
        assert result == {"index_name": "docs", "chunk_size": 500}

    def test_undecodable_input_degrades_to_an_empty_dict(self, application_tools):
        assert application_tools.build_index_configuration("not json at all") == {}

    def test_none_yields_an_empty_dict(self, application_tools):
        assert application_tools.build_index_configuration(None) == {}

    def test_index_name_is_stamped_when_the_client_omits_it(self, application_tools):
        # The UI filters index_name out as immutable, so a save payload never carries it.
        result = application_tools.build_index_configuration({"chunk_size": 500}, "docs")
        assert result == {"chunk_size": 500, "index_name": "docs"}

    def test_a_blank_index_name_is_replaced(self, application_tools):
        result = application_tools.build_index_configuration({"index_name": ""}, "docs")
        assert result["index_name"] == "docs"

    def test_a_client_supplied_index_name_is_overridden(self, application_tools):
        # A payload naming another index would aim the next scheduled run at that index's
        # collection and reset its metadata, so the caller's name always wins.
        result = application_tools.build_index_configuration({"index_name": "other"}, "docs")
        assert result["index_name"] == "docs"

    def test_no_authoritative_name_leaves_the_payload_alone(self, application_tools):
        assert application_tools.build_index_configuration({"chunk_size": 500}) == {"chunk_size": 500}


TIMEOUT = 7200


def _stored(**overrides):
    base = {
        "collection": "docs",
        "type": "index_meta",
        "state": "completed",
        "history": '[{"state": "completed"}]',
        "task_id": "task-1",
        "report": {"documents": 12},
        "error": None,
        "created_on": 100.0,
        "updated_on": 200.0,
        "index_configuration": {"index_name": "docs", "chunk_size": 1000},
    }
    base.update(overrides)
    return base


def _arrange(application_tools, monkeypatch, cmetadata):
    meta = types.SimpleNamespace(cmetadata=cmetadata) if cmetadata is not None else None
    session = _FakeSession()
    monkeypatch.setattr(application_tools, "get_session_for_schema", lambda *a: session)
    monkeypatch.setattr(application_tools, "get_toolkit_index_meta", lambda *a, **kw: meta)
    return meta, session


class TestSaveTouchesOnlyTheConfiguration:
    """The save must not read as a run: state, history, task_id, report and the timestamps all
    survive, and updated_on in particular drives both list ordering and is_index_stale."""

    def test_every_other_metadata_key_survives(self, application_tools, monkeypatch):
        stored = _stored()
        meta, session = _arrange(application_tools, monkeypatch, stored.copy())

        returned = application_tools.save_toolkit_index_configuration(
            "postgresql://", "42", "docs", {"chunk_size": 500}, TIMEOUT
        )

        assert returned == {"chunk_size": 500, "index_name": "docs"}
        assert meta.cmetadata["index_configuration"] == {"chunk_size": 500, "index_name": "docs"}
        assert session.committed is True
        for key in ("state", "history", "task_id", "report", "created_on", "updated_on"):
            assert meta.cmetadata[key] == stored[key]

    def test_the_route_index_name_overrides_the_payload(self, application_tools, monkeypatch):
        meta, _ = _arrange(application_tools, monkeypatch, _stored())

        application_tools.save_toolkit_index_configuration(
            "postgresql://", "42", "docs", {"index_name": "other"}, TIMEOUT
        )

        assert meta.cmetadata["index_configuration"]["index_name"] == "docs"

    def test_a_missing_index_returns_none_and_does_not_commit(self, application_tools, monkeypatch):
        _, session = _arrange(application_tools, monkeypatch, None)

        assert application_tools.save_toolkit_index_configuration(
            "postgresql://", "42", "missing", {"chunk_size": 500}, TIMEOUT
        ) is None
        assert session.committed is False

    def test_the_row_is_locked_for_the_check_and_the_write(self, application_tools, monkeypatch):
        """Without the lock a run committing between the check and this write would be reverted."""
        seen = {}
        session = _FakeSession()
        meta = types.SimpleNamespace(cmetadata=_stored())

        def _capture(_session, _index_name, for_update=False):
            seen["for_update"] = for_update
            return meta

        monkeypatch.setattr(application_tools, "get_session_for_schema", lambda *a: session)
        monkeypatch.setattr(application_tools, "get_toolkit_index_meta", _capture)

        application_tools.save_toolkit_index_configuration(
            "postgresql://", "42", "docs", {"chunk_size": 500}, TIMEOUT
        )

        assert seen["for_update"] is True


class TestSaveRefusesToRaceALiveRun:

    def test_a_live_run_blocks_the_save(self, application_tools, monkeypatch):
        _, session = _arrange(
            application_tools, monkeypatch,
            _stored(state="in_progress", updated_on=time.time()),
        )

        with pytest.raises(application_tools.IndexRunInProgressError):
            application_tools.save_toolkit_index_configuration(
                "postgresql://", "42", "docs", {"chunk_size": 500}, TIMEOUT
            )
        assert session.committed is False

    def test_a_run_that_stopped_reporting_does_not_lock_edits_out(self, application_tools, monkeypatch):
        meta, session = _arrange(
            application_tools, monkeypatch,
            _stored(state="in_progress", updated_on=time.time() - TIMEOUT * 2),
        )

        application_tools.save_toolkit_index_configuration(
            "postgresql://", "42", "docs", {"chunk_size": 500}, TIMEOUT
        )

        assert session.committed is True
        assert meta.cmetadata["state"] == "in_progress"

    def test_a_terminal_state_never_blocks(self, application_tools, monkeypatch):
        for state in ("completed", "failed", "cancelled", "scheduled_reindex"):
            _, session = _arrange(application_tools, monkeypatch, _stored(state=state))
            application_tools.save_toolkit_index_configuration(
                "postgresql://", "42", "docs", {"chunk_size": 500}, TIMEOUT
            )
            assert session.committed is True


class _FakeSession:
    def __init__(self):
        self.committed = False
        self.executed = []

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False

    def execute(self, statement):
        self.executed.append(str(statement))

    def commit(self):
        self.committed = True
