"""Issue #6583 - the scheduler tick, executed rather than inspected.

Every other guard on this behaviour reads `rpc/index_scheduling.py` as an AST. That has
now traded one covered shape for another three times running: a global source count broke
on a legitimate write; an ExceptHandler sweep missed a path that is not a handler; an
`orelse` check passed vacuously because the gate it named has no `else`; and an allow-list
of blocks silently permitted the `else` arms *inside* those blocks. Each fix was correct
and each left a differently-shaped hole, because the shape is not the property.

The property is behavioural: how many times does a broken schedule report, and when does
its cursor move. This file runs `check_index_scheduling` against fakes and counts. It does
not care where in the function the calls live, so it survives refactors that break every
AST assertion, and it fails for any arrangement that reports twice in a cron period or
consumes a cron slot on a contention path.

Run via:
    python tests/run_tests.py integration/test_6583c_tick_behaviour.py -v
"""

import contextlib
import importlib.util
import time
import pathlib
import sys
import types
from datetime import datetime, timedelta, timezone

import pytest


PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
UTC = timezone.utc


def _install_stubs():
    for name in ("plugins", "plugins.elitea_core", "plugins.elitea_core.models",
                 "plugins.elitea_core.models.enums", "plugins.elitea_core.models.pd",
                 "plugins.elitea_core.utils", "plugins.elitea_core.rpc"):
        mod = sys.modules.setdefault(name, types.ModuleType(name))
        mod.__path__ = []

    pylon_tools = types.ModuleType("pylon.core.tools")
    pylon_tools.log = types.SimpleNamespace(
        info=lambda *a, **k: None, warning=lambda *a, **k: None,
        error=lambda *a, **k: None, debug=lambda *a, **k: None,
        exception=lambda *a, **k: None)
    pylon_tools.web = types.SimpleNamespace(
        method=lambda *a, **k: (lambda f: f), rpc=lambda *a, **k: (lambda f: f))
    sys.modules.setdefault("pylon", types.ModuleType("pylon"))
    sys.modules.setdefault("pylon.core", types.ModuleType("pylon.core"))
    sys.modules["pylon.core.tools"] = pylon_tools

    tools_pkg = types.ModuleType("tools")
    tools_pkg.auth = types.SimpleNamespace(decorators=types.SimpleNamespace())
    tools_pkg.db = types.SimpleNamespace(get_session=lambda pid: None)
    tools_pkg.this = types.SimpleNamespace(descriptor=types.SimpleNamespace(config={}))
    tools_pkg.serialize = types.SimpleNamespace()
    tools_pkg.context = types.SimpleNamespace()
    tools_pkg.VaultClient = lambda pid: types.SimpleNamespace(get_secrets=lambda: {})
    tools_pkg.rpc_tools = types.SimpleNamespace()
    sys.modules["tools"] = tools_pkg
    return tools_pkg


def _load(name, relpath):
    spec = importlib.util.spec_from_file_location(name, PLUGIN_ROOT / relpath)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def tick():
    """Load rpc/index_scheduling.py with the REAL cron, model and scheduling utils.

    Only the leaves are faked. The interaction between the due check, the classification,
    the escalation clock and the cursor write is exactly what is under test, so none of
    those may be stubbed.
    """
    tools_pkg = _install_stubs()

    models_all = types.ModuleType("plugins.elitea_core.models.all")
    for attr in ("EliteATool", "EntityToolMapping", "ApplicationVersion"):
        setattr(models_all, attr, type(attr, (), {}))
    sys.modules["plugins.elitea_core.models.all"] = models_all
    _load("plugins.elitea_core.models.indexer", "models/indexer.py")

    enums_all = types.ModuleType("plugins.elitea_core.models.enums.all")
    enums_all.ToolEntityTypes = type("ToolEntityTypes", (), {})
    enums_all.AgentTypes = type("AgentTypes", (), {})
    enums_all.InitiatorType = type("InitiatorType", (), {"user": "user"})
    enums_all.IndexDataStatus = type("IndexDataStatus", (), {
        "in_progress": types.SimpleNamespace(value="in_progress"),
        "cancelled": types.SimpleNamespace(value="cancelled"),
        "failed": types.SimpleNamespace(value="failed")})
    sys.modules["plugins.elitea_core.models.enums.all"] = enums_all

    exceptions = types.ModuleType("plugins.elitea_core.utils.exceptions")
    exceptions.PoolSaturationError = type("PoolSaturationError", (Exception,), {})
    exceptions.MaintenanceInProgressError = type("MaintenanceInProgressError", (Exception,), {})
    sys.modules["plugins.elitea_core.utils.exceptions"] = exceptions
    utils_utils = types.ModuleType("plugins.elitea_core.utils.utils")
    utils_utils.parse_ids_filter = lambda *a, **k: None
    sys.modules["plugins.elitea_core.utils.utils"] = utils_utils

    _load("plugins.elitea_core.utils.application_tools", "utils/application_tools.py")

    enums_pkg = sys.modules["plugins.elitea_core.models.enums"]
    enums_pkg.InitiatorType = types.SimpleNamespace(schedule="schedule", user="user")
    tools_pkg.rpc_tools = types.SimpleNamespace(RpcMixin=lambda: types.SimpleNamespace(
        rpc=types.SimpleNamespace(timeout=lambda t: types.SimpleNamespace(
            configurations_expand=lambda **kw: {"connection_string": "postgresql://x"},
            project_list=lambda **kw: [{"id": 2}]))))
    sched_utils = _load("plugins.elitea_core.utils.index_scheduling", "utils/index_scheduling.py")
    _load("plugins.elitea_core.models.pd.index", "models/pd/index.py")
    _load("plugins.elitea_core.utils.cron_utils", "utils/cron_utils.py")

    # utils/utils.py and predict_utils reach further than this test needs.
    utils_utils.make_yield_to_hub = lambda runtime: (lambda: None)
    utils_utils.end_ambient_transaction = lambda: None
    predict = types.ModuleType("plugins.elitea_core.utils.predict_utils")
    predict.get_predict_base_url = lambda pid: "http://predict"
    predict.get_system_user_token = lambda pid: "token"
    sys.modules["plugins.elitea_core.utils.predict_utils"] = predict
    gate = types.ModuleType("plugins.elitea_core.utils.maintenance_gate")
    gate.is_maintenance_active = lambda: False
    sys.modules["plugins.elitea_core.utils.maintenance_gate"] = gate
    enums_idx = types.ModuleType("plugins.elitea_core.models.enums.indexer")
    enums_idx.IndexingSchedule = type("IndexingSchedule", (), {})
    sys.modules["plugins.elitea_core.models.enums.indexer"] = enums_idx
    elitea_tools = types.ModuleType("plugins.elitea_core.models.elitea_tools")
    elitea_tools.EliteATool = type("EliteATool", (), {"meta": None, "id": None})
    sys.modules["plugins.elitea_core.models.elitea_tools"] = elitea_tools

    rpc = _load("plugins.elitea_core.rpc.index_scheduling", "rpc/index_scheduling.py")
    rpc._sched_utils = sched_utils
    return rpc


class Recorder:
    """One tick environment. Counts what reached the owner and what moved the cursor."""

    def __init__(self, tick, monkeypatch, *, schedule, index_state="completed",
                 credentials=(True, None, False), lock_timeout=False,
                 connection_string="postgresql://x", index_fresh=True,
                 resolve_raises=False, pgvector_raises=False, stop_raises=False):
        self.tick, self.notifications, self.history_writes = tick, [], []
        self.dispatched = 0
        # Witnesses. Every "never consumes the cron slot" assertion is an absence, and an
        # absence is also what you get from maintenance mode, a not-due schedule, or a raise
        # before the branch. These prove the tick actually arrived. pgvector_connects counts
        # session entries rather than completed reads: it increments before a connect that
        # then raises, which is exactly what makes it usable as a witness on that path.
        self.credential_checks = 0
        self.pgvector_connects = 0
        self.toolkit = types.SimpleNamespace(
            id=42, type="confluence", settings={"pgvector_configuration": {}},
            meta={"indexes_meta": {"docs": {"schedules": {"7": dict(schedule)}}}})
        self._index_state = index_state
        self._credentials = credentials
        self._lock_timeout = lock_timeout
        self._conn = connection_string
        # One ordered log, not separate counters: the supersede has to happen BEFORE the
        # dispatch, or the straggler keeps writing the collection alongside the new run.
        # Two counters cannot express that, and a deferred stop passes them both.
        self.events = []
        self.schedule_parses = 0
        self._resolve_raises = resolve_raises
        self._pgvector_raises = pgvector_raises
        self._stop_raises = stop_raises
        # A row is only "fresh" if its heartbeat is recent; updated_on=0 makes
        # is_index_stale true and silently routes the test down the STALE branch instead.
        self._updated_on = time.time() if index_fresh else 0
        self._wire(monkeypatch)

    @property
    def entry(self):
        return self.toolkit.meta["indexes_meta"]["docs"]["schedules"]["7"]

    def _wire(self, monkeypatch):
        tick, sched = self.tick, self.tick._sched_utils
        rec = self

        class _Q:
            def __init__(self, rows): self._rows = rows
            def filter(self, *a, **k): return self
            def all(self): return self._rows
            def first(self): return self._rows[0] if self._rows else None

        class _Session:
            def query(self, *a, **k):
                if a and getattr(a[0], "__name__", "") == "EliteATool":
                    return _Q([rec.toolkit])
                return _Q([types.SimpleNamespace(
                    id=1, cmetadata={"state": rec._index_state,
                                     "updated_on": rec._updated_on,
                                     # Without a task_id the supersede branch is skipped,
                                     # so a stale row never exercises stop_task at all.
                                     "task_id": "stale-task-1"})])
            def refresh(self, obj): pass
            def commit(self): pass
            def rollback(self): pass

        @contextlib.contextmanager
        def _session(*a, **k):
            yield _Session()

        @contextlib.contextmanager
        def _pgvector(*a, **k):
            rec.pgvector_connects += 1
            if rec._pgvector_raises:
                raise RuntimeError("pgvector unreachable")
            yield _Session()

        monkeypatch.setattr(tick, "db", types.SimpleNamespace(get_session=_session))
        monkeypatch.setattr(tick, "get_session_for_schema", _pgvector)
        class _Col:
            def __getitem__(self, k): return self
            def isnot(self, other): return self
            def astext(self): return self
        monkeypatch.setattr(tick, "EliteATool",
                            type("EliteATool", (), {"meta": _Col(), "id": _Col()}))
        monkeypatch.setattr(tick, "flag_modified", lambda *a, **k: None, raising=False)
        monkeypatch.setattr(sched, "flag_modified", lambda *a, **k: None)
        def _resolve(**kw):
            rec.credential_checks += 1
            if rec._resolve_raises:
                raise RuntimeError("vault unavailable")
            return rec._credentials
        monkeypatch.setattr(tick, "resolve_credentials", _resolve)
        monkeypatch.setattr(tick, "get_system_user_token", lambda pid: "token")

        # Per-schedule on purpose. Counting the EliteATool query instead sits above
        # indexes_meta -> schedules, so a `continue` at either loop head left the
        # absence-only tests green while nothing descended to the schedule at all.
        # Wrap the parse entry points ON the real class rather than substituting a stand-in
        # for it. A stand-in only forwards attribute access: `__call__` and `isinstance` are
        # looked up on the type, never through `__getattr__`, so the equally natural v2 idiom
        # `ToolkitIndexingSchedule(**user_config)` would raise TypeError inside the parse try.
        # The tick swallows that, and every schedule would look unparseable while these
        # witnesses reported that it never descended.
        _model = tick.ToolkitIndexingSchedule
        # Outermost only: parse_obj delegates to model_validate in v2, so wrapping both
        # double-counts a single parse.
        _depth = {"n": 0}

        def _counting(orig, method=False):
            def _wrapped(*a, **k):
                if _depth["n"] == 0:
                    rec.schedule_parses += 1
                _depth["n"] += 1
                try:
                    return orig(*a, **k)
                finally:
                    _depth["n"] -= 1
            return _wrapped if method else staticmethod(_wrapped)

        for _entry in ("parse_obj", "model_validate"):
            _orig = getattr(_model, _entry, None)
            if _orig is not None:
                monkeypatch.setattr(_model, _entry, _counting(_orig))
        # The constructor is a third idiom and does not route through either of the above.
        monkeypatch.setattr(_model, "__init__", _counting(_model.__init__, method=True))
        # No registered run row, so staleness falls to the updated_on rule above.
        monkeypatch.setattr(tick, "get_pending_index_run_heartbeat", lambda *a, **k: None)
        monkeypatch.setattr(tick, "VaultClient",
                            lambda pid: types.SimpleNamespace(get_secrets=lambda: {}))
        monkeypatch.setattr(tick, "start_index_task",
                            lambda *a, **k: rec._count_dispatch())
        monkeypatch.setattr(tick, "rpc_tools", types.SimpleNamespace(
            RpcMixin=lambda: types.SimpleNamespace(rpc=types.SimpleNamespace(
                timeout=lambda t: types.SimpleNamespace(
                    project_list=lambda **kw: [{"id": 2}],
                    configurations_expand=lambda **kw: {
                        "pgvector_configuration": {"connection_string": rec._conn},
                        "connection_string": rec._conn})))))

        def _writer(*a, **k):
            if rec._lock_timeout:
                raise sched.IndexMetaLockTimeoutError("docs")
            rec.history_writes.append(a)
            return {"flipped": True, "skipped_live_run": False,
                    "reindex": False, "indexed": 0, "updated": 0}
        monkeypatch.setattr(sched, "update_toolkit_index_meta_history_with_failed_state", _writer)
        monkeypatch.setattr(sched, "rpc_tools", types.SimpleNamespace(
            RpcMixin=lambda: types.SimpleNamespace(rpc=types.SimpleNamespace(
                timeout=lambda t: types.SimpleNamespace(
                    configurations_expand=lambda **kw: {"connection_string": "postgresql://x"})))))
        monkeypatch.setattr(sched, "this", types.SimpleNamespace(
            module=types.SimpleNamespace(
                notify_index_data_status=lambda p: rec.notifications.append(p))))

    def _stop_task(self, task_id):
        if self._stop_raises:
            # A distinct event: asserting only that a stop was attempted cannot tell a
            # raising stop from a working one, so this test would quietly become a copy of
            # the ordering test if the wiring drifted.
            self.events.append(("stop_failed", task_id))
            raise RuntimeError("task node unreachable")
        self.events.append(("stopped", task_id))

    def _count_dispatch(self):
        self.dispatched += 1
        self.events.append(("dispatched",))
        return "task-1"

    def run(self, ticks=1):
        for _ in range(ticks):
            self.tick.RPC.check_index_scheduling(
                types.SimpleNamespace(context=types.SimpleNamespace(web_runtime="gevent"),
                                      task_node=types.SimpleNamespace(
                                          stop_task=self._stop_task)))
        return self


def _due_schedule(**over):
    base = {"cron": "0 3 * * *", "enabled": True, "created_by": 7,
            "timezone": "UTC", "last_run": "2026-01-01T00:00:00+00:00",
            "credentials": {"private": False, "elitea_title": "cred"}}
    base.update(over)
    return base


TERMINAL = (False, "credential 'cred' no longer exists", False)
RETRYABLE = (False, "could not look up credential 'cred': TimeoutError", True)


class TestReportingCadence:
    def test_a_terminal_failure_reports_once_not_once_per_tick(self, tick, monkeypatch):
        """#6583 itself, measured rather than inspected."""
        rec = Recorder(tick, monkeypatch, schedule=_due_schedule(),
                       credentials=TERMINAL).run(ticks=5)
        assert len(rec.notifications) == 1, rec.notifications
        assert len(rec.history_writes) == 1
        assert rec.entry["last_run"] != "2026-01-01T00:00:00+00:00"

    def test_contention_never_consumes_the_cron_slot(self, tick, monkeypatch):
        """A row held by a live run's lock must be retried within a minute, not next period.

        This is the assertion the AST allow-list kept failing to express: whatever shape the
        code takes, a schedule blocked by contention keeps its cursor.
        """
        rec = Recorder(tick, monkeypatch, schedule=_due_schedule(),
                       credentials=TERMINAL, lock_timeout=True).run(ticks=3)
        assert rec.credential_checks == 3, "the tick never reached the credential check"
        assert rec.notifications == []
        assert rec.entry["last_run"] == "2026-01-01T00:00:00+00:00", \
            "contention moved the cursor, so the schedule lost its whole cron period"

    def test_a_live_and_fresh_run_never_consumes_the_cron_slot(self, tick, monkeypatch):
        rec = Recorder(tick, monkeypatch, schedule=_due_schedule(),
                       index_state="in_progress").run(ticks=3)
        assert rec.pgvector_connects == 3, "the tick never opened the pgvector session"
        assert rec.dispatched == 0
        assert rec.events == [], \
            "a healthy, heartbeating run must not be stopped — only a stale one is superseded"
        assert rec.entry["last_run"] == "2026-01-01T00:00:00+00:00"

    def test_a_missing_connection_string_never_consumes_the_cron_slot(self, tick,
                                                                       monkeypatch):
        """A project whose pgvector config resolves to nothing cannot be reported ON, so
        this path is log-only and keeps the fast retry. It is also the path deferred for a
        possible cursor write later — if that lands, this test is what has to be revisited,
        deliberately."""
        rec = Recorder(tick, monkeypatch, schedule=_due_schedule(),
                       connection_string=None).run(ticks=3)
        assert rec.credential_checks == 3, "the tick never reached the credential check"
        assert rec.dispatched == 0
        assert rec.notifications == []
        assert rec.entry["last_run"] == "2026-01-01T00:00:00+00:00"

    def test_a_healthy_schedule_dispatches_once_and_moves_on(self, tick, monkeypatch):
        rec = Recorder(tick, monkeypatch, schedule=_due_schedule()).run(ticks=4)
        assert rec.dispatched == 1, "a dispatched schedule must leave the due set"
        assert rec.notifications == []


class TestRetryableOutage:
    def test_a_blip_is_recorded_but_never_reported(self, tick, monkeypatch):
        rec = Recorder(tick, monkeypatch, schedule=_due_schedule(),
                       credentials=RETRYABLE).run(ticks=3)
        assert rec.notifications == [], "a transient lookup failure must not alarm the owner"
        assert rec.entry["last_run"] == "2026-01-01T00:00:00+00:00", \
            "a blip must keep the 60s retry, not consume the period"
        assert rec.entry.get("retry_since"), "the outage must be recorded for a later tick"

    def test_an_outage_past_the_grace_reports_once_and_moves_the_cursor(self, tick,
                                                                        monkeypatch):
        old = (datetime.now(UTC) - timedelta(hours=2)).isoformat()
        rec = Recorder(tick, monkeypatch, credentials=RETRYABLE,
                       schedule=_due_schedule(retry_since=old)).run(ticks=4)
        assert len(rec.notifications) == 1, rec.notifications
        assert "still failing for over" in rec.notifications[0]["error"]
        assert rec.entry["last_run"] != "2026-01-01T00:00:00+00:00"
        assert rec.entry["retry_since"] is None, "reporting ends the outage"

    def test_a_recovered_lookup_clears_the_outage(self, tick, monkeypatch):
        old = (datetime.now(UTC) - timedelta(hours=2)).isoformat()
        rec = Recorder(tick, monkeypatch, schedule=_due_schedule(retry_since=old),
                       index_state="in_progress").run(ticks=1)
        assert rec.credential_checks == 1
        assert rec.entry["retry_since"] is None, \
            "a successful lookup must clear the outage even when the tick goes on to skip " \
            "the dispatch because a live, fresh run holds the row"


class TestTransientFailuresKeepTheFastRetry:
    """The paths the retired allow-list guarded and the first version of this suite missed.

    A stamp on any of these turns a transient blip — an unreachable vault, a pgvector
    hiccup — into a lost cron period, which is the exact inverse of the property the whole
    fix rests on, and what handle_failed_index_schedule's own lock-timeout comment relies on.
    """

    def test_a_settings_resolution_failure_keeps_the_cursor(self, tick, monkeypatch):
        """Vault and cross-project config expansion raise here; both self-heal."""
        rec = Recorder(tick, monkeypatch, schedule=_due_schedule(),
                       resolve_raises=True).run(ticks=3)
        assert rec.credential_checks == 3, "the tick never reached the credential check"
        assert rec.notifications == []
        assert rec.entry["last_run"] == "2026-01-01T00:00:00+00:00"

    def test_a_pgvector_failure_keeps_the_cursor(self, tick, monkeypatch):
        """The per-toolkit catch-all around the connect, the staleness checks and dispatch."""
        rec = Recorder(tick, monkeypatch, schedule=_due_schedule(),
                       pgvector_raises=True).run(ticks=3)
        assert rec.pgvector_connects == 3, "the tick never opened the pgvector session"
        assert rec.notifications == []
        assert rec.entry["last_run"] == "2026-01-01T00:00:00+00:00"

    def test_an_unparseable_schedule_keeps_the_cursor(self, tick, monkeypatch):
        """It fails before the due check, so it is not gated on the cursor at all; moving it
        would be inert at best and would rewrite a row that could not be validated."""
        rec = Recorder(tick, monkeypatch,
                       schedule=_due_schedule(cron="not a cron")).run(ticks=3)
        assert rec.schedule_parses == 3, "the tick never descended to the schedule"
        assert rec.credential_checks == 0, "an unparseable schedule must not be resolved"
        assert rec.entry["last_run"] == "2026-01-01T00:00:00+00:00"

    def test_a_schedule_that_is_not_due_is_left_alone(self, tick, monkeypatch):
        """The baseline the other cases are measured against: no read, no write, no report.

        The cursor assertion is the load-bearing one. Moving it here would re-stamp every
        healthy schedule on every tick, so its next firing would always be a period away and
        nothing on the platform would ever run again.
        """
        cursor = datetime.now(UTC).isoformat()
        rec = Recorder(tick, monkeypatch,
                       schedule=_due_schedule(last_run=cursor)).run(ticks=3)
        assert rec.schedule_parses == 3, "the tick never descended to the schedule"
        assert (rec.credential_checks, rec.pgvector_connects, rec.dispatched) == (0, 0, 0)
        assert rec.notifications == []
        assert rec.entry["last_run"] == cursor, "a not-due schedule must not be re-stamped"
        assert "retry_since" not in rec.entry or rec.entry["retry_since"] is None


class TestStaleRunIsSuperseded:
    """index_fresh=False was added and never used, so the stale arm — including the
    stop_task supersede — stopped being executed at all when freshness was made real."""

    def test_a_stale_in_progress_row_is_retried_and_consumes_the_slot(self, tick,
                                                                      monkeypatch):
        rec = Recorder(tick, monkeypatch, schedule=_due_schedule(),
                       index_state="in_progress", index_fresh=False).run(ticks=3)
        assert rec.dispatched == 1, "a stale run must be retried, exactly once"
        assert rec.entry["last_run"] != "2026-01-01T00:00:00+00:00"

    def test_the_straggler_is_stopped_before_the_retry_overwrites_its_row(self, tick,
                                                                          monkeypatch):
        """The supersede itself, which the fixture previously skipped: with no task_id on
        the row, `if stale_task_id:` was False and stop_task was never called, so deleting
        it left the suite green."""
        rec = Recorder(tick, monkeypatch, schedule=_due_schedule(),
                       index_state="in_progress", index_fresh=False).run(ticks=1)
        assert rec.events == [("stopped", "stale-task-1"), ("dispatched",)], \
            "the straggler must be stopped BEFORE the retry, or the two write together"

    def test_a_straggler_that_cannot_be_stopped_does_not_block_the_retry(self, tick,
                                                                        monkeypatch):
        """The supersede is best-effort. If a raising stop_task escaped, it would reach the
        per-toolkit catch-all, skipping both the dispatch and the cursor write, and the
        schedule would re-enter every 60s forever — #6583's own symptom."""
        rec = Recorder(tick, monkeypatch, schedule=_due_schedule(), stop_raises=True,
                       index_state="in_progress", index_fresh=False)

        # Precondition, asserted rather than assumed: the fake must actually raise. Recording
        # an event just before the raise proves intent, not failure — deleting the raise, or
        # flipping the flag, would leave the event and turn this into a copy of the ordering
        # test while still claiming the except arm.
        with pytest.raises(RuntimeError):
            rec._stop_task("probe")
        rec.events.clear()

        rec.run(ticks=1)
        assert ("stop_failed", "stale-task-1") in rec.events, \
            "production never reached the failing stop"
        assert rec.dispatched == 1, "a stale run must still be retried"
        assert rec.entry["last_run"] != "2026-01-01T00:00:00+00:00", \
            "the cursor must still move, or the schedule re-enters every tick"
