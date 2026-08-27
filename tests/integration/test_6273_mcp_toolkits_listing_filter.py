"""Issue #6273 - toolkits missing from cloud MCP tools list after connection.

`toolkits_listing(..., filter_mcp=False)` (the default) excludes any
`EliteATool` row with `type == 'mcp'` from the result set. User-connected
remote MCP toolkits are persisted with exactly that type. `mcp_service.py`
(the module backing the platform's own `/mcp` export used by Claude Code /
"Cloud Code") called `toolkits_listing` with the default `filter_mcp=False`,
so a connected MCP toolkit was silently dropped from the tools list — only
agents tagged `mcp` (fetched through an unrelated code path) still showed up,
matching the reported symptom.

The fix adds a third state to `filter_mcp`: passing `filter_mcp=None` skips
the MCP-status filter entirely, returning both regular toolkits and
MCP-connected toolkits together, which is what the MCP export needs (it
already filters by the `available_by_mcp` flag downstream). This suite pins:

1. `filter_mcp=True` still restricts to MCP-flagged/typed rows only.
2. `filter_mcp=False` (default) still excludes MCP-typed rows (existing
   Toolkits-page behavior is unchanged).
3. `filter_mcp=None` applies no MCP-status predicate at all — the new branch
   this fix introduces.
4. `mcp_service.py`'s toolkit-tool call sites now pass `filter_mcp=None`.

Run via:
    python tests/run_tests.py integration/test_6273_mcp_toolkits_listing_filter.py -v
"""

import importlib.util
import pathlib
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

    def __getattr__(self, name):
        def _fallback(*args, **kwargs):
            return self
        return _fallback


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
def application_tools(filters_log):
    """Load application_tools.py standalone with minimal stubs."""
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
    sys.modules["tools"] = tools_pkg

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

    models_pd_tool = types.ModuleType("plugins.elitea_core.models.pd.tool")
    models_pd_tool.ToolDetails = type("ToolDetails", (), {})
    import re
    models_pd_tool.sanitization_pattern = re.compile(r"[^A-Za-z0-9]")
    sys.modules["plugins.elitea_core.models.pd.tool"] = models_pd_tool

    utils_authors = types.ModuleType("plugins.elitea_core.utils.authors")
    utils_authors.get_authors_data = lambda *a, **k: {}
    sys.modules["plugins.elitea_core.utils.authors"] = utils_authors

    spec = importlib.util.spec_from_file_location(
        "plugins.elitea_core.utils.application_tools",
        PLUGIN_ROOT / "utils" / "application_tools.py",
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
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

    def test_filter_mcp_none_applies_no_mcp_status_predicate(self, application_tools, filters_log):
        """The fix: filter_mcp=None must not filter by MCP status at all, so
        MCP-connected toolkits (type == 'mcp') and regular toolkits are both
        returned together."""
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


class TestMcpServiceUsesUnfilteredListing:
    """`mcp_service.py`'s toolkit-tool call sites must pass filter_mcp=None so
    MCP-connected toolkits are included in the platform's own MCP tool export."""

    def test_all_toolkits_listing_calls_pass_filter_mcp_none(self):
        source = (PLUGIN_ROOT / "utils" / "mcp_service.py").read_text()
        calls = [
            line for line in source.splitlines()
            if "toolkits_listing(" in line or (line.strip().startswith("project_id=self.session.project_id")
                                                and "toolkits_listing" not in line)
        ]
        # Every toolkits_listing(...) invocation site in this file must be
        # accompanied by filter_mcp=None somewhere in its call (possibly on a
        # following line, since calls are multi-line formatted).
        import re
        call_blocks = re.findall(r"toolkits_listing\((?:[^()]|\([^()]*\))*\)", source, re.DOTALL)
        assert call_blocks, "expected at least one toolkits_listing(...) call in mcp_service.py"
        for block in call_blocks:
            assert "filter_mcp=None" in block, f"missing filter_mcp=None in call: {block}"
