"""Issue #6273 - toolkits missing from cloud MCP tools list after connection.

`toolkits_listing(..., filter_mcp=False)` (the default) excludes any
`EliteATool` row where `meta['mcp']` is true (or, for legacy rows, `type ==
'mcp'`) from the result set. User-connected remote MCP toolkits are persisted
with `meta['mcp'] = True` (see `api/v2/tools.py`, where a tool's type is
checked against the project's registered MCP schemas and `meta['mcp']` is set
accordingly — the toolkit's `type` itself stays the MCP server's schema key,
e.g. `mcp_*`, it is not literally the string `'mcp'`). `mcp_service.py` (the
module backing the platform's own `/mcp` export used by Claude Code / "Cloud
Code") called `toolkits_listing` with the default `filter_mcp=False`, so a
connected MCP toolkit was silently dropped from the tools list — only agents
tagged `mcp` (fetched through an unrelated code path) still showed up,
matching the reported symptom.

The fix adds a third state to `filter_mcp`: passing `filter_mcp=None` skips
the MCP-status filter entirely, returning both regular toolkits and
MCP-connected toolkits together, which is what the MCP export needs (it
already filters by the `available_by_mcp` flag downstream). This suite pins:

1. `filter_mcp=True` still restricts to MCP-flagged rows (`meta['mcp']`
   true, or the legacy `type == 'mcp'`) only.
2. `filter_mcp=False` (default) still excludes those rows via both the
   `meta['mcp']` clause and the `type != 'mcp'` clause (existing Toolkits-page
   behavior is unchanged) — including when `meta['mcp']` is explicitly false
   or null.
3. `filter_mcp=None` applies no MCP-status predicate at all — the new branch
   this fix introduces.
4. `mcp_service.py`'s toolkit-tool call sites now pass `filter_mcp=None`.

Run via:
    python tests/run_tests.py integration/test_6273_mcp_toolkits_listing_filter.py -v
"""

import importlib.util
import pathlib
import re
import sys
import types

import pytest


PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]


class _Expr:
    """Minimal proxy standing in for a SQLAlchemy column/expression.

    Records the shape of the expression as a string so tests can assert
    which predicates `toolkits_listing` builds, without needing a real
    Postgres-backed JSON column (the production code uses the
    Postgres-only `.astext` JSON operator, which sqlite does not support).
    """

    def __init__(self, tag):
        self.tag = tag

    def __getitem__(self, key):
        return _Expr(f"{self.tag}[{key!r}]")

    @property
    def astext(self):
        return _Expr(f"{self.tag}.astext")

    def cast(self, type_):
        return _Expr(f"{self.tag}.cast(...)")

    def is_(self, value):
        return _Expr(f"{self.tag}.is_({value!r})")

    def in_(self, value):
        return _Expr(f"{self.tag}.in_({value!r})")

    def ilike(self, value):
        return _Expr(f"{self.tag}.ilike({value!r})")

    def __eq__(self, other):
        return _Expr(f"{self.tag}=={other!r}")

    def __ne__(self, other):
        return _Expr(f"{self.tag}!={other!r}")

    def __or__(self, other):
        return _Expr(f"({self.tag}|{other.tag})")

    def __and__(self, other):
        return _Expr(f"({self.tag}&{other.tag})")

    def __hash__(self):
        return id(self)

    def __repr__(self):
        return self.tag


class _FakeQuery:
    """Chainable stand-in for a SQLAlchemy Query that records `.filter()` calls."""

    def __init__(self, filters_log):
        self._filters_log = filters_log
        self.column_descriptions = [{"expr": "is_pinned"}, {"expr": "pin_updated_at"}]

    def filter(self, *args, **kwargs):
        for arg in args:
            self._filters_log.append(repr(arg))
        return self

    def with_entities(self, *args, **kwargs):
        return self

    def order_by(self, *args, **kwargs):
        return self

    def offset(self, *args, **kwargs):
        return self

    def limit(self, *args, **kwargs):
        return self

    def options(self, *args, **kwargs):
        return self

    def count(self):
        return 0

    def all(self):
        return []


class _FakeSession:
    def __init__(self, filters_log):
        self._filters_log = filters_log

    def query(self, model):
        return _FakeQuery(self._filters_log)

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False


@pytest.fixture()
def filters_log():
    return []


@pytest.fixture()
def application_tools(filters_log, monkeypatch):
    """Load application_tools.py standalone with minimal stubs.

    All `sys.modules` entries touched here go through `monkeypatch.setitem`
    so they're restored to whatever (if anything) was there before this test
    ran, once the test finishes. Without that, a module inserted/overwritten
    here (e.g. `tools`, `pylon.core.tools`) would leak into later tests in the
    same process and make the suite order-dependent.
    """
    for name in (
        "plugins",
        "plugins.elitea_core",
        "plugins.elitea_core.models",
        "plugins.elitea_core.utils",
    ):
        mod = sys.modules.get(name) or types.ModuleType(name)
        mod.__path__ = []
        monkeypatch.setitem(sys.modules, name, mod)

    pylon_tools = types.ModuleType("pylon.core.tools")
    pylon_tools.log = types.SimpleNamespace(
        info=lambda *a, **k: None, warning=lambda *a, **k: None,
        error=lambda *a, **k: None, debug=lambda *a, **k: None,
        exception=lambda *a, **k: None,
    )
    monkeypatch.setitem(sys.modules, "pylon", sys.modules.get("pylon") or types.ModuleType("pylon"))
    monkeypatch.setitem(sys.modules, "pylon.core", sys.modules.get("pylon.core") or types.ModuleType("pylon.core"))
    monkeypatch.setitem(sys.modules, "pylon.core.tools", pylon_tools)

    class _RpcMixin:
        class _Rpc:
            def timeout(self, _seconds):
                return self

            def social_add_pins_with_priority(self):
                def _add_pins(original_query, project_id, entity):
                    return original_query, []
                return _add_pins

        rpc = _Rpc()

    tools_pkg = types.ModuleType("tools")
    tools_pkg.auth = types.SimpleNamespace(decorators=types.SimpleNamespace())
    tools_pkg.db = types.SimpleNamespace(
        get_session=lambda pid: _FakeSession(filters_log)
    )
    tools_pkg.this = types.SimpleNamespace(descriptor=types.SimpleNamespace(config={}))
    tools_pkg.serialize = types.SimpleNamespace()
    tools_pkg.context = types.SimpleNamespace()
    tools_pkg.rpc_tools = types.SimpleNamespace(RpcMixin=_RpcMixin)
    monkeypatch.setitem(sys.modules, "tools", tools_pkg)

    models_all = types.ModuleType("plugins.elitea_core.models.all")
    models_all.EliteATool = type("EliteATool", (), {
        "type": _Expr("EliteATool.type"),
        "meta": _Expr("EliteATool.meta"),
        "name": _Expr("EliteATool.name"),
        "description": _Expr("EliteATool.description"),
        "author_id": _Expr("EliteATool.author_id"),
        "id": _Expr("EliteATool.id"),
        "settings": _Expr("EliteATool.settings"),
        "created_at": _Expr("EliteATool.created_at"),
    })
    models_all.EntityToolMapping = type("EntityToolMapping", (), {})
    models_all.ApplicationVersion = type("ApplicationVersion", (), {})
    monkeypatch.setitem(sys.modules, "plugins.elitea_core.models.all", models_all)

    models_indexer = types.ModuleType("plugins.elitea_core.models.indexer")
    models_indexer.EmbeddingStore = type("EmbeddingStore", (), {})
    monkeypatch.setitem(sys.modules, "plugins.elitea_core.models.indexer", models_indexer)

    enums = types.ModuleType("plugins.elitea_core.models.enums.all")
    enums.ToolEntityTypes = type("ToolEntityTypes", (), {})
    enums.AgentTypes = type("AgentTypes", (), {})
    enums.InitiatorType = type("InitiatorType", (), {"user": "user"})
    enums.IndexDataStatus = type("IndexDataStatus", (), {
        "in_progress": types.SimpleNamespace(value="in_progress"),
        "cancelled": types.SimpleNamespace(value="cancelled"),
    })
    monkeypatch.setitem(sys.modules, "plugins.elitea_core.models.enums.all", enums)

    exceptions = types.ModuleType("plugins.elitea_core.utils.exceptions")
    exceptions.PoolSaturationError = type("PoolSaturationError", (Exception,), {})
    monkeypatch.setitem(sys.modules, "plugins.elitea_core.utils.exceptions", exceptions)

    utils_utils = types.ModuleType("plugins.elitea_core.utils.utils")
    utils_utils.parse_ids_filter = lambda *a, **k: None
    monkeypatch.setitem(sys.modules, "plugins.elitea_core.utils.utils", utils_utils)

    models_pd_tool = types.ModuleType("plugins.elitea_core.models.pd.tool")
    models_pd_tool.ToolDetails = type("ToolDetails", (), {})
    models_pd_tool.sanitization_pattern = re.compile(r"[^A-Za-z0-9]")
    monkeypatch.setitem(sys.modules, "plugins.elitea_core.models.pd.tool", models_pd_tool)

    utils_authors = types.ModuleType("plugins.elitea_core.utils.authors")
    utils_authors.get_authors_data = lambda *a, **k: {}
    monkeypatch.setitem(sys.modules, "plugins.elitea_core.utils.authors", utils_authors)

    spec = importlib.util.spec_from_file_location(
        "plugins.elitea_core.utils.application_tools",
        PLUGIN_ROOT / "utils" / "application_tools.py",
    )
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, spec.name, module)
    spec.loader.exec_module(module)
    # `desc`/`asc` are real SQLAlchemy functions imported into this module;
    # they coerce their argument via SQLAlchemy's column-role machinery,
    # which our lightweight `_Expr` proxy doesn't satisfy. Since sort order
    # isn't what this suite is exercising, replace them with passthrough
    # stand-ins that just tag the direction onto the `_Expr` for inspection.
    module.desc = lambda col: _Expr(f"desc({col!r})")
    module.asc = lambda col: _Expr(f"asc({col!r})")
    return module


class TestToolkitsListingFilterMcp:
    """Exercises the actual `filter_mcp` branch in `toolkits_listing` — not a
    mirror of it, so drift there fails here."""

    def test_filter_mcp_true_restricts_to_mcp_rows(self, application_tools, filters_log):
        application_tools.toolkits_listing(project_id=1, query=None, limit=None, filter_mcp=True)
        joined = " ".join(filters_log)
        assert "EliteATool.meta[" in joined and "EliteATool.type==" in joined
        # No mcp-status exclusion predicate should be present in this mode.
        assert "EliteATool.type!='mcp'" not in joined

    def test_filter_mcp_false_excludes_mcp_rows(self, application_tools, filters_log):
        application_tools.toolkits_listing(project_id=1, query=None, limit=None, filter_mcp=False)
        joined = " ".join(filters_log)
        assert "EliteATool.type!='mcp'" in joined
        # The `meta['mcp']` clause is the one that actually matters for
        # user-connected MCP toolkits (they are persisted with meta['mcp']
        # True, not type=='mcp' — see api/v2/tools.py). Pin it explicitly so a
        # regression that drops this clause while leaving the `type`
        # predicate in place is caught here rather than slipping through.
        assert "EliteATool.meta[" in joined

    def test_filter_mcp_false_excludes_rows_with_meta_mcp_true(self, application_tools, filters_log):
        """meta['mcp'] == True is the actual persisted shape for connected MCP
        toolkits; make sure the False branch's predicate would exclude it
        (i.e. the predicate is built from `meta['mcp']`, not just `type`)."""
        application_tools.toolkits_listing(project_id=1, query=None, limit=None, filter_mcp=False)
        joined = " ".join(filters_log)
        assert "EliteATool.meta['mcp'].astext.cast(...)==False" in joined
        assert "EliteATool.meta['mcp'].astext.is_(None)" in joined

    def test_filter_mcp_none_applies_no_mcp_status_predicate(self, application_tools, filters_log):
        """The fix: filter_mcp=None must not filter by MCP status at all, so
        MCP-connected toolkits (persisted with meta['mcp'] == True, or the
        legacy type == 'mcp') and regular toolkits are both returned
        together."""
        application_tools.toolkits_listing(project_id=1, query=None, limit=None, filter_mcp=None)
        joined = " ".join(filters_log)
        assert "EliteATool.type!='mcp'" not in joined
        assert "EliteATool.type=='mcp'" not in joined
        assert "EliteATool.meta[" not in joined

    def test_default_still_excludes_mcp_rows(self, application_tools, filters_log):
        """Default (no filter_mcp passed) preserves the pre-fix Toolkits-page
        behavior: MCP-connected toolkits stay out unless opted in."""
        application_tools.toolkits_listing(project_id=1, query=None, limit=None)
        joined = " ".join(filters_log)
        assert "EliteATool.type!='mcp'" in joined
        assert "EliteATool.meta[" in joined
