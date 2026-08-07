"""Issue #6163 - bound the pgvector engine cache so it cannot grow for the
process lifetime.

`_get_pgvector_engine` caches one SQLAlchemy engine per connection string,
reused across requests. These tests exercise the idle reaper and LRU hard-cap
that reclaim it, and prove (against a real SQLAlchemy engine, not a mock)
that disposing an evicted engine does not break a session that is still
using it - only checked-in connections are closed on dispose().

Run via:
    python tests/run_tests.py integration/test_6163_bound_pgvector_engine_cache.py -v
"""

import importlib.util
import pathlib
import sys
import types

import pytest
from sqlalchemy import create_engine as _real_create_engine, text
from sqlalchemy.pool import QueuePool


PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]


@pytest.fixture
def application_tools_module():
    """Load application_tools with minimal stubs, and a real create_engine."""
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

    config = {}
    tools_pkg = types.ModuleType("tools")
    tools_pkg.auth = types.SimpleNamespace(decorators=types.SimpleNamespace())
    tools_pkg.db = types.SimpleNamespace(get_session=lambda pid: None)
    tools_pkg.this = types.SimpleNamespace(
        descriptor=types.SimpleNamespace(config=config)
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

    module._pgvector_engine_cache_config = config
    # sqlite's default SingletonThreadPool rejects pool_size/max_overflow; force
    # QueuePool so the test exercises the same pool class postgres uses.
    module.create_engine = lambda conn_str, **kw: _real_create_engine(
        conn_str, poolclass=QueuePool, **kw
    )
    yield module

    module._PGVECTOR_ENGINE_CACHE.clear()
    module._PGVECTOR_ENGINE_LAST_USED.clear()
    module._PGVECTOR_ENGINE_LAST_REAP[0] = 0.0


def _connstr(n):
    # Distinct sqlite in-memory DBs behave like distinct pgvector connstrs for
    # cache-identity purposes; each gets its own engine and pool.
    return f"sqlite:///file:cache{n}?mode=memory&cache=shared&uri=true"


class TestIdleReclamation:
    def test_engine_past_max_idle_is_reaped_and_disposed(self, application_tools_module, monkeypatch):
        m = application_tools_module
        m._pgvector_engine_cache_config.update(
            {"pgvector_engine_cache": {"max_idle_seconds": 10, "reap_interval_seconds": 0}}
        )

        clock = [0.0]
        monkeypatch.setattr(m.time, "monotonic", lambda: clock[0])

        engine = m._get_pgvector_engine(_connstr(1))
        assert m._PGVECTOR_ENGINE_CACHE[_connstr(1)] is engine

        clock[0] = 11.0
        m._reap_idle_pgvector_engines(clock[0])

        assert _connstr(1) not in m._PGVECTOR_ENGINE_CACHE
        assert _connstr(1) not in m._PGVECTOR_ENGINE_LAST_USED

    def test_reap_is_a_no_op_before_the_interval_elapses(self, application_tools_module, monkeypatch):
        m = application_tools_module
        m._pgvector_engine_cache_config.update(
            {"pgvector_engine_cache": {"max_idle_seconds": 1, "reap_interval_seconds": 300}}
        )

        clock = [0.0]
        monkeypatch.setattr(m.time, "monotonic", lambda: clock[0])

        m._get_pgvector_engine(_connstr(2))
        clock[0] = 5.0
        m._get_pgvector_engine(_connstr(2))

        assert _connstr(2) in m._PGVECTOR_ENGINE_CACHE


class TestHardCap:
    def test_cache_never_exceeds_max_cached_engines(self, application_tools_module, monkeypatch):
        m = application_tools_module
        m._pgvector_engine_cache_config.update(
            {"pgvector_engine_cache": {"max_cached_engines": 2, "reap_interval_seconds": 0}}
        )

        clock = [0.0]
        monkeypatch.setattr(m.time, "monotonic", lambda: clock[0])

        for i in range(5):
            clock[0] += 1.0
            m._get_pgvector_engine(_connstr(10 + i))

        assert len(m._PGVECTOR_ENGINE_CACHE) <= 2

    def test_hard_cap_evicts_the_least_recently_used_first(self, application_tools_module, monkeypatch):
        m = application_tools_module
        m._pgvector_engine_cache_config.update(
            {"pgvector_engine_cache": {"max_cached_engines": 2, "reap_interval_seconds": 0}}
        )

        clock = [0.0]
        monkeypatch.setattr(m.time, "monotonic", lambda: clock[0])

        m._get_pgvector_engine(_connstr(20))
        clock[0] = 1.0
        m._get_pgvector_engine(_connstr(21))
        clock[0] = 2.0
        m._get_pgvector_engine(_connstr(22))

        assert _connstr(20) not in m._PGVECTOR_ENGINE_CACHE
        assert _connstr(21) in m._PGVECTOR_ENGINE_CACHE
        assert _connstr(22) in m._PGVECTOR_ENGINE_CACHE


class TestLiveSessionSurvivesEviction:
    """The main regression test: an evicted engine's dispose() must not break
    a session that already checked out a connection from it."""

    def test_checked_out_connection_keeps_working_after_dispose(self, application_tools_module, monkeypatch):
        m = application_tools_module
        m._pgvector_engine_cache_config.update(
            {"pgvector_engine_cache": {"max_idle_seconds": 0, "reap_interval_seconds": 0}}
        )

        clock = [0.0]
        monkeypatch.setattr(m.time, "monotonic", lambda: clock[0])

        connstr = _connstr(30)
        engine = m._get_pgvector_engine(connstr)
        held_conn = engine.connect()
        held_conn.execute(text("select 1"))

        # Force a reap from elsewhere while held_conn is still checked out.
        clock[0] = 100.0
        m._reap_idle_pgvector_engines(clock[0])
        assert connstr not in m._PGVECTOR_ENGINE_CACHE

        # The already-checked-out connection is untouched by dispose().
        result = held_conn.execute(text("select 1")).scalar()
        assert result == 1
        held_conn.close()

        # A fresh lookup creates a new engine, not the disposed one.
        new_engine = m._get_pgvector_engine(connstr)
        assert new_engine is not engine


class TestReuseUnaffected:
    def test_repeat_access_to_the_same_connstr_reuses_one_engine(self, application_tools_module):
        m = application_tools_module

        first = m._get_pgvector_engine(_connstr(40))
        second = m._get_pgvector_engine(_connstr(40))

        assert first is second
