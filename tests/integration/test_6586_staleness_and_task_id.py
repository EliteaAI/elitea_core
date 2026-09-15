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

import ast
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


class TestTheDisplayHorizonReplacesTheTwoHourOne:
    """Callers that only decide what the card SAYS pass the heartbeat horizon."""

    def _display(self, application_tools, state, updated_on, heartbeat):
        return application_tools.resolve_index_staleness(
            state, updated_on, TIMEOUT, pending_heartbeat=heartbeat,
            heartbeat_horizon=application_tools.HEARTBEAT_STALE_HORIZON_SEC,
        )

    def test_a_run_with_a_stopped_heartbeat_is_stale_in_minutes(self, application_tools):
        dead = time.time() - HORIZON * 2
        assert self._display(application_tools, "in_progress", time.time(), dead) is True

    def test_the_two_hour_timeout_no_longer_holds_such_a_row_alive(self, application_tools):
        # The whole defect: updated_on is 60s-fresh, judged against 7200s.
        dead = time.time() - HORIZON * 2
        assert application_tools.is_index_stale(time.time(), "in_progress", TIMEOUT) is False
        assert self._display(application_tools, "in_progress", time.time(), dead) is True

    def test_a_ticking_run_is_never_stale(self, application_tools):
        assert self._display(application_tools, "in_progress", 0, time.time()) is False

    def test_a_single_missed_tick_is_not_stale(self, application_tools):
        assert self._display(
            application_tools, "in_progress", 0, time.time() - HEARTBEAT * 1.5) is False

    def test_the_horizon_is_a_small_multiple_of_the_interval(self, application_tools):
        assert application_tools.INDEX_RUN_HEARTBEAT_INTERVAL_SEC == HEARTBEAT
        assert application_tools.HEARTBEAT_STALE_INTERVALS == 5
        assert application_tools.HEARTBEAT_STALE_HORIZON_SEC == HORIZON


class TestRowsWithoutARunRowKeepTheOldRule:

    def test_the_heartbeat_read_degrades_instead_of_failing_the_list(self, application_tools, monkeypatch):
        # The list GET calls this unconditionally inside its single try; re-raising
        # anything but UndefinedTable 400s the whole page over one unreadable table.
        class _Boom(_NullSession):
            def begin_nested(self):
                return types.SimpleNamespace(commit=lambda: None, rollback=lambda: None)

            def query(self, *a, **k):
                raise RuntimeError("permission denied for table elitea_index_runs")

        assert application_tools.get_pending_index_run_heartbeats(_Boom()) == {}

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


class TestControlDecisionsKeepTheDisconnectHorizon:
    """Superseding, stopping or deleting a run is NOT a display decision.

    The horizon that authorizes them has to agree with `has_live_index_run`, which
    the dispatch guard inside `start_index_task` uses. When it did not, the
    scheduler read a run mid-promote as stale, killed the worker, and was then
    refused the dispatch it killed it for — leaving the schedule due and re-firing
    every tick. The default (no `heartbeat_horizon`) is the safe one on purpose."""

    def test_a_run_inside_the_disconnect_window_is_not_reclaimable(self, application_tools):
        # The band that caused the kill-then-refuse loop: past the display horizon,
        # nowhere near the timeout the dispatch guard applies.
        mid_promote = time.time() - HORIZON * 2
        assert application_tools.resolve_index_staleness(
            "in_progress", time.time(), TIMEOUT, pending_heartbeat=mid_promote) is False

    def test_the_default_matches_what_has_live_index_run_would_say(self, application_tools):
        mid_promote = time.time() - HORIZON * 2
        row = types.SimpleNamespace(heartbeat=mid_promote)
        still_live = (time.time() - row.heartbeat) <= TIMEOUT

        reclaimable = application_tools.resolve_index_staleness(
            "in_progress", time.time(), TIMEOUT, pending_heartbeat=mid_promote)

        assert still_live is True
        assert reclaimable is False, "control horizon must not outrun the dispatch guard"

    def test_a_genuinely_dead_run_is_still_reclaimable(self, application_tools):
        dead = time.time() - TIMEOUT * 1.5
        assert application_tools.resolve_index_staleness(
            "in_progress", time.time(), TIMEOUT, pending_heartbeat=dead) is True

    def test_display_and_control_disagree_only_inside_the_band(self, application_tools):
        mid_promote = time.time() - HORIZON * 2
        display = application_tools.resolve_index_staleness(
            "in_progress", time.time(), TIMEOUT, pending_heartbeat=mid_promote,
            heartbeat_horizon=application_tools.HEARTBEAT_STALE_HORIZON_SEC)
        control = application_tools.resolve_index_staleness(
            "in_progress", time.time(), TIMEOUT, pending_heartbeat=mid_promote)

        assert (display, control) == (True, False)


class TestTheHorizonWiringAtEachCallSite:
    """resolve_index_staleness is safe by default and dangerous by keyword, so the
    behaviour lives in how each call site spells it — which no behavioural test
    reaches. Parsed, not grepped: a comment mentioning the keyword must not pass, and
    a real keyword must not be missed.

    Matching is by bare name, and a companion test refuses any indirection that would
    route around it, so the pair is total rather than best-effort. That is still the
    weaker instrument: prefer extracting the decision into a pure function and
    asserting its VALUES wherever the call site allows it. These two sit inside deep
    request/tick bodies that resist it, which is why they are guarded this way.

    The scheduler passing `heartbeat_horizon` reintroduces the kill-then-refuse loop:
    it supersedes a run mid-promote, calls stop_task on a live worker, and is then
    refused the dispatch by reject_index_dispatch_when_run_live on the disconnect
    rule, leaving the schedule due and re-firing every tick."""

    HORIZON_POSITION = 4  # state, updated_on, timeout, pending_heartbeat, heartbeat_horizon

    TARGET = "resolve_index_staleness"

    @staticmethod
    def _tree(relative_path):
        return ast.parse((PLUGIN_ROOT / relative_path).read_text())

    def _calls(self, relative_path):
        tree = self._tree(relative_path)
        return [
            node for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            # `.id` for a bare name, `.attr` for a module-qualified call — matching
            # only the first turns a qualified call into a silent zero-match.
            and (getattr(node.func, "id", None) or getattr(node.func, "attr", None))
            == self.TARGET
        ]

    def test_neither_file_reaches_the_rule_under_another_name(self):
        """Refuse indirection instead of chasing it.

        The previous version resolved aliases and rebindings, which made the checks
        below best-effort in a way the docstring overstated: a single non-iterated
        walk misses a chain whose first link is visited later, and it only understood
        plain assignment — not AnnAssign, not walrus, not a try/except import
        fallback. Worse, feeding a short alias into the `.attr` arm made any
        same-named attribute call match.

        Asserting that no indirection EXISTS makes the bare-name matching total
        instead. It costs a legitimate rename in two files, and says so loudly."""
        for path in ("rpc/index_scheduling.py", "api/v2/index_meta.py"):
            tree = self._tree(path)
            aliases = [
                alias.asname for node in ast.walk(tree)
                if isinstance(node, ast.ImportFrom)
                for alias in node.names
                if alias.name == self.TARGET and alias.asname
            ]
            assert aliases == [], f"{path} imports {self.TARGET} as {aliases}"

            # Any mention that is not the direct callee of a call — an assignment, an
            # annotation, a walrus, passing it as an argument — is an indirection the
            # matching below cannot follow.
            callees = {id(node.func) for node in ast.walk(tree) if isinstance(node, ast.Call)}
            indirect = [
                node for node in ast.walk(tree)
                if isinstance(node, ast.Name) and node.id == self.TARGET
                and id(node) not in callees
            ]
            assert indirect == [], (
                f"{path} refers to {self.TARGET} without calling it directly "
                f"(line {indirect[0].lineno if indirect else '?'})"
            )

    def _asks_for_the_horizon(self, call):
        """Dangerous by keyword AND by position — and a **splat hides both."""
        if len(call.args) > self.HORIZON_POSITION:
            return True
        return any(kw.arg in ("heartbeat_horizon", None) for kw in call.keywords)

    def test_the_scheduler_never_asks_for_the_display_horizon(self):
        calls = self._calls("rpc/index_scheduling.py")

        assert calls, "the scheduler must still decide staleness through the shared rule"
        assert not any(self._asks_for_the_horizon(c) for c in calls)

    def test_the_list_get_asks_for_it_for_display_only(self):
        calls = self._calls("api/v2/index_meta.py")
        asking = [c for c in calls if self._asks_for_the_horizon(c)]

        # Counted by role, not by total: a third legitimate call site should not turn
        # this red, but losing either role must.
        assert len(asking) >= 1, "display must ask for the heartbeat horizon"
        assert len(calls) - len(asking) >= 1, "control must not ask for it"

    def test_the_get_still_returns_the_control_flag(self):
        source = (PLUGIN_ROOT / "api/v2/index_meta.py").read_text()
        # Walk to the response dict itself; scanning every Constant in the file passes
        # as long as the word survives anywhere, including in a docstring.
        keys = set()
        for node in ast.walk(ast.parse(source)):
            if isinstance(node, ast.Call) and getattr(node.func, "attr", None) == "append":
                for argument in node.args:
                    if isinstance(argument, ast.Dict):
                        keys.update(k.value for k in argument.keys if isinstance(k, ast.Constant))

        # Dropping `reclaimable` from the payload silently re-arms Delete on a live
        # run, because the UI falls back to `stale` when the field is absent.
        assert "reclaimable" in keys
        assert "stale" in keys


class TestTheSqlstateDiagnosticWorksOnThisDriver:
    """The degrade path logs the SQLSTATE so a real permission/schema regression is
    still visible. Reading `.pgcode` alone prints None on psycopg3, which is the
    driver the PGVector path actually uses."""

    def test_a_psycopg3_style_error_reports_its_code(self, application_tools):
        error = types.SimpleNamespace(orig=types.SimpleNamespace(sqlstate="42501"))

        assert application_tools.error_sqlstate(error) == "42501"

    def test_a_psycopg2_style_error_still_reports_its_code(self, application_tools):
        error = types.SimpleNamespace(orig=types.SimpleNamespace(pgcode="42501"))

        assert application_tools.error_sqlstate(error) == "42501"

    def test_the_undefined_table_probe_uses_the_same_read(self, application_tools):
        error = types.SimpleNamespace(orig=types.SimpleNamespace(sqlstate="42P01"))

        assert application_tools._is_undefined_table_error(error) is True


class TestTheDisplayHorizonCanNeverOutrunControl:
    """`task_disconnected_timeout_sec` is an unclamped vault secret. Set it below the
    display horizon and the flags invert: a row becomes reclaimable — Delete enabled —
    while still rendering a live spinner with no error styling, which is the one
    combination the split exists to prevent. This stack ships that config: the local
    project secret is 60."""

    def test_a_short_disconnect_timeout_does_not_arm_control_early(self, application_tools):
        short = 60  # this stack's value, below the display horizon
        age = time.time() - 200  # past `short`, inside the 300s display horizon

        control = application_tools.resolve_index_staleness(
            "in_progress", time.time(), short, pending_heartbeat=age)

        assert control is False, "Delete/supersede must not arm against a live run"

    def test_a_short_disconnect_timeout_does_not_make_display_flicker(self, application_tools):
        # Ceiling-ing display onto a 60s timeout would put the horizon at the heartbeat
        # interval itself, so a healthy run reads stale for the tail of every cycle.
        healthy = time.time() - 90  # one missed tick on a 60s interval

        display = application_tools.resolve_index_staleness(
            "in_progress", time.time(), 60, pending_heartbeat=healthy,
            heartbeat_horizon=application_tools.HEARTBEAT_STALE_HORIZON_SEC)

        assert display is False

    def test_reclaimable_always_implies_stale(self, application_tools):
        for timeout in (30, 60, 299, 300, 301, 7200):
            for age in (10, 61, 200, 301, 8000):
                heartbeat = time.time() - age
                display = application_tools.resolve_index_staleness(
                    "in_progress", time.time(), timeout, pending_heartbeat=heartbeat,
                    heartbeat_horizon=application_tools.HEARTBEAT_STALE_HORIZON_SEC)
                control = application_tools.resolve_index_staleness(
                    "in_progress", time.time(), timeout, pending_heartbeat=heartbeat)
                assert not (control and not display), (
                    f"inverted at timeout={timeout} age={age}"
                )


class TestTheDispatchSeedsRunChunks:
    """The list renders `run_chunks` while a run is in flight. Core flips the row to
    in_progress at dispatch but the worker needs tens of seconds to boot, so without a
    seed here the card falls back to the PREVIOUS run's counts in the meantime."""

    @pytest.fixture
    def dispatch(self, application_tools, monkeypatch):
        state = types.SimpleNamespace(cmetadata=[])
        monkeypatch.setattr(application_tools, "validate_toolkit_for_index",
                            lambda config: ("42", "postgresql://"))
        monkeypatch.setattr(application_tools, "reject_index_dispatch_when_run_live",
                            lambda *a, **kw: None)
        monkeypatch.setattr(application_tools, "reset_or_create_toolkit_index_meta",
                            lambda cs, schema, name, meta, **kw: state.cmetadata.append(meta))
        state.task_node = types.SimpleNamespace(
            start_task=lambda *a, **kw: "task-9", stop_task=lambda task_id: None)
        state.data = {
            "toolkit_config": {"id": 42},
            "project_id": 1,
            "tool_name": "index_data",
            "tool_params": {"index_name": "docs"},
        }
        return state

    def test_the_dispatched_row_carries_a_zero_chunk_count(self, application_tools, dispatch):
        application_tools.start_index_task(dispatch.task_node, dispatch.data, None)

        assert dispatch.cmetadata[-1]["run_chunks"] == 0

    def test_it_does_not_carry_a_previous_runs_count(self, application_tools, dispatch):
        application_tools.start_index_task(dispatch.task_node, dispatch.data, None)

        assert dispatch.cmetadata[-1]["state"] == "in_progress"
        assert dispatch.cmetadata[-1]["indexed"] == 0


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
