"""Issue #6583 follow-up - a lookup that keeps failing must stop failing silently.

75b7edc split credential failures into terminal and retryable, and made the retryable
branch report nothing at all: no notification, no history entry, no cursor movement. That
is right for a three-second RPC blip. For a lookup that keeps raising it means the index
stops updating indefinitely with nothing on any screen saying so - strictly less visible
than the 1440-a-day flood that commit set out to fix, because the durable history entry it
would have written is three navigations deep and nothing pulls anyone toward it.

The repair records when the current run of retryable failures began. Below the grace window
nothing changes; past it the failure is reclassified terminal, and the path that already
reports once per cron period does the work. The retryable branch itself stays out of the
reporting business entirely, so the tripwires 75b7edc left behind still hold.

Run via:
    python tests/run_tests.py integration/test_6583b_persistent_lookup_failure.py -v
"""

import ast
import importlib.util
import pathlib
import sys
import types
from datetime import datetime, timedelta, timezone

import pytest


PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
UTC = timezone.utc


@pytest.fixture(scope="module")
def application_tools():
    for name in ("plugins", "plugins.elitea_core",
                 "plugins.elitea_core.models", "plugins.elitea_core.utils"):
        mod = sys.modules.setdefault(name, types.ModuleType(name))
        mod.__path__ = []

    pylon_tools = types.ModuleType("pylon.core.tools")
    pylon_tools.log = types.SimpleNamespace(
        info=lambda *a, **k: None, warning=lambda *a, **k: None,
        error=lambda *a, **k: None, debug=lambda *a, **k: None,
        exception=lambda *a, **k: None,
    )
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
    tools_pkg.VaultClient = type("VaultClient", (), {"get_secrets": lambda self: {}})
    sys.modules["tools"] = tools_pkg

    models_all = types.ModuleType("plugins.elitea_core.models.all")
    for attr in ("EliteATool", "EntityToolMapping", "ApplicationVersion"):
        setattr(models_all, attr, type(attr, (), {}))
    sys.modules["plugins.elitea_core.models.all"] = models_all

    indexer_spec = importlib.util.spec_from_file_location(
        "plugins.elitea_core.models.indexer", PLUGIN_ROOT / "models" / "indexer.py")
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
        PLUGIN_ROOT / "utils" / "application_tools.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def index_scheduling(application_tools):
    spec = importlib.util.spec_from_file_location(
        "plugins.elitea_core.utils.index_scheduling",
        PLUGIN_ROOT / "utils" / "index_scheduling.py")
    enums_pkg = types.ModuleType("plugins.elitea_core.models.enums")
    enums_pkg.InitiatorType = types.SimpleNamespace(schedule="schedule", user="user")
    sys.modules["plugins.elitea_core.models.enums"] = enums_pkg
    sys.modules["tools"].rpc_tools = types.SimpleNamespace(
        RpcMixin=lambda: types.SimpleNamespace(rpc=types.SimpleNamespace(
            timeout=lambda t: types.SimpleNamespace(
                configurations_expand=lambda **kw: {"connection_string": "postgresql://"}))))
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _toolkit(retry_since=None, index="docs"):
    entry = {"cron": "0 3 * * *", "enabled": True,
             "last_run": "2026-01-01T00:00:00+00:00"}
    if retry_since is not None:
        entry["retry_since"] = retry_since
    return types.SimpleNamespace(
        id=42, type="confluence",
        meta={"indexes_meta": {index: {"schedules": {"7": entry}}}})


def _entry(toolkit, index="docs"):
    return toolkit.meta["indexes_meta"][index]["schedules"]["7"]


class FakeSession:
    def __init__(self, on_refresh=None, commit_raises=False):
        self.commits = self.rollbacks = self.refreshes = 0
        self._on_refresh, self._commit_raises = on_refresh, commit_raises

    def refresh(self, toolkit):
        import copy
        self.refreshes += 1
        toolkit.meta = copy.deepcopy(toolkit.meta)
        if self._on_refresh:
            self._on_refresh(toolkit)

    def commit(self):
        if self._commit_raises:
            raise RuntimeError("connection lost at COMMIT")
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


class TestRetryEscalationDue:
    def test_no_retry_run_never_escalates(self, index_scheduling):
        """None means 'not currently failing', never 'failing since forever'.

        Reading None as an epoch would escalate on the very first tick and restore the
        #6583 flood through the new field.
        """
        assert index_scheduling.retry_escalation_due(None) is False
        assert index_scheduling.retry_escalation_due("") is False

    def test_an_unreadable_stamp_never_escalates(self, index_scheduling):
        for bad in ("not a date", "2026-13-45T99:99:99"):
            assert index_scheduling.retry_escalation_due(bad) is False

    def test_a_future_stamp_never_escalates(self, index_scheduling):
        ahead = (datetime.now(UTC) + timedelta(days=1)).isoformat()
        assert index_scheduling.retry_escalation_due(ahead) is False

    def test_a_blip_does_not_escalate_but_an_outage_does(self, index_scheduling):
        now = datetime(2026, 9, 10, 12, 0, tzinfo=UTC)
        grace = index_scheduling.RETRYABLE_REPORT_GRACE
        assert index_scheduling.retry_escalation_due(
            (now - timedelta(seconds=3)).isoformat(), now=now) is False
        assert index_scheduling.retry_escalation_due(
            (now - grace + timedelta(seconds=1)).isoformat(), now=now) is False
        assert index_scheduling.retry_escalation_due(
            (now - grace).isoformat(), now=now) is True

    def test_a_naive_stamp_is_read_as_utc(self, index_scheduling):
        now = datetime(2026, 9, 10, 12, 0, tzinfo=UTC)
        naive = (now - timedelta(hours=2)).replace(tzinfo=None).isoformat()
        assert index_scheduling.retry_escalation_due(naive, now=now) is True


class TestOwnerFacingText:
    """This text lands in the pgvector `error` field and the notification body, so it is
    read by the schedule's owner in the UI, not by an operator in a log."""

    def test_the_grace_reads_as_a_duration_not_a_clock_time(self, index_scheduling):
        from datetime import timedelta as _td
        assert index_scheduling.describe_grace(_td(hours=1)) == "an hour"
        assert index_scheduling.describe_grace(_td(hours=2)) == "2 hours"
        assert index_scheduling.describe_grace(_td(minutes=30)) == "30 minutes"
        assert index_scheduling.describe_grace(_td(minutes=1)) == "a minute"

    def test_no_rendering_of_the_grace_looks_like_a_clock(self, index_scheduling):
        """`str(timedelta(hours=1))` is '1:00:00', which reads as a time of day."""
        from datetime import timedelta as _td
        for td in (_td(hours=1), _td(hours=2), _td(minutes=30), _td(minutes=90)):
            assert ":" not in index_scheduling.describe_grace(td)

    def test_sub_minute_durations_do_not_render_as_zero(self, index_scheduling):
        """Unreachable at the current 1h grace, but "0 minutes" is what a naive
        floor-to-minutes gives if the grace is ever retuned below a minute."""
        from datetime import timedelta as _td
        assert index_scheduling.describe_grace(_td(seconds=30)) == "30 seconds"
        assert index_scheduling.describe_grace(_td(seconds=1)) == "a second"
        assert "0 minutes" not in index_scheduling.describe_grace(_td(seconds=45))

    def test_the_default_tracks_the_constant(self, index_scheduling):
        """A future change to the grace must not leave the owner-facing text behind."""
        assert index_scheduling.describe_grace() == index_scheduling.describe_grace(
            index_scheduling.RETRYABLE_REPORT_GRACE)


class TestFailedWriteNamesItsOwnConsequence:
    """One shared sentence would be wrong for at least one caller: a dropped cursor write
    leaves the schedule due every tick, a dropped outage stamp means it is never reported,
    and a dropped clear runs on the RECOVERY path, where the tick goes on to dispatch."""

    def _logged(self, index_scheduling, monkeypatch):
        out = []
        monkeypatch.setattr(index_scheduling, "flag_modified", lambda *a, **k: None)
        monkeypatch.setattr(index_scheduling, "log", types.SimpleNamespace(
            info=lambda m: None, warning=lambda m: None, debug=lambda m: None,
            error=lambda m: out.append(m), exception=lambda m: out.append(m)))
        return out

    def test_a_dropped_cursor_write_says_the_schedule_stays_due(self, index_scheduling,
                                                                monkeypatch):
        out = self._logged(index_scheduling, monkeypatch)
        index_scheduling.stamp_schedule_last_run(
            FakeSession(commit_raises=True), _toolkit(), "docs", "7", "[ctx]")
        assert any("stays due" in m and "every tick" in m for m in out), out

    def test_a_dropped_outage_stamp_says_it_will_not_be_reported(self, index_scheduling,
                                                                 monkeypatch):
        out = self._logged(index_scheduling, monkeypatch)
        index_scheduling.stamp_schedule_retry_since(
            FakeSession(commit_raises=True), _toolkit(), "docs", "7", "[ctx]")
        assert any("not be reported" in m for m in out), out
        assert not any("stays due" in m for m in out), \
            "the cursor consequence is wrong here - nothing was due-blocked by this write"

    def test_a_dropped_clear_says_the_stale_stamp_may_escalate(self, index_scheduling,
                                                               monkeypatch):
        """This one fires on recovery, where the tick goes on to dispatch and stamp - so
        'the schedule stays due and will be retried on every tick' is simply false."""
        out = self._logged(index_scheduling, monkeypatch)
        index_scheduling.clear_schedule_retry_since(
            FakeSession(commit_raises=True), _toolkit(retry_since="2026-01-01T00:00:00+00:00"),
            "docs", "7", "[ctx]")
        assert any("stale stamp" in m and "no grace" in m for m in out), out
        assert not any("stays due" in m for m in out), \
            "the recovery path does not leave the schedule due"

    def test_the_three_consequences_are_distinct(self, index_scheduling, monkeypatch):
        """Guards the collapse: extracting a shared writer must not merge these."""
        seen = []
        for call in (
            lambda: index_scheduling.stamp_schedule_last_run(
                FakeSession(commit_raises=True), _toolkit(), "docs", "7", "[c]"),
            lambda: index_scheduling.stamp_schedule_retry_since(
                FakeSession(commit_raises=True), _toolkit(), "docs", "7", "[c]"),
            lambda: index_scheduling.clear_schedule_retry_since(
                FakeSession(commit_raises=True), _toolkit(), "docs", "7", "[c]"),
        ):
            out = self._logged(index_scheduling, monkeypatch)
            call()
            seen.append(out[-1].split("; ", 1)[-1])
        assert len(set(seen)) == 3, seen


class TestRetrySinceWrites:
    def test_the_stamp_is_recorded_without_moving_the_cursor(self, index_scheduling,
                                                             monkeypatch):
        """Recording an outage must not consume the schedule's cron slot."""
        monkeypatch.setattr(index_scheduling, "flag_modified", lambda *a, **k: None)
        toolkit, session = _toolkit(), FakeSession()
        before = _entry(toolkit)["last_run"]
        written = index_scheduling.stamp_schedule_retry_since(
            session, toolkit, "docs", "7", "[ctx]")
        assert written is not None
        assert _entry(toolkit)["retry_since"] == written
        assert _entry(toolkit)["last_run"] == before

    def test_concluding_clears_the_retry_run_in_the_same_write(self, index_scheduling,
                                                              monkeypatch):
        """Otherwise the NEXT outage inherits a stale stamp and escalates with no grace."""
        monkeypatch.setattr(index_scheduling, "flag_modified", lambda *a, **k: None)
        toolkit = _toolkit(retry_since="2026-09-09T00:00:00+00:00")
        session = FakeSession()
        written = index_scheduling.stamp_schedule_last_run(
            session, toolkit, "docs", "7", "[ctx]")
        assert _entry(toolkit)["last_run"] == written
        assert _entry(toolkit)["retry_since"] is None
        assert session.commits == 1

    def test_clearing_leaves_the_cursor_alone(self, index_scheduling, monkeypatch):
        monkeypatch.setattr(index_scheduling, "flag_modified", lambda *a, **k: None)
        toolkit = _toolkit(retry_since="2026-09-09T00:00:00+00:00")
        before = _entry(toolkit)["last_run"]
        index_scheduling.clear_schedule_retry_since(
            FakeSession(), toolkit, "docs", "7", "[ctx]")
        assert _entry(toolkit)["retry_since"] is None
        assert _entry(toolkit)["last_run"] == before

    def test_the_written_stamp_is_accepted_by_the_schedule_model(self, index_scheduling,
                                                                 monkeypatch):
        monkeypatch.setattr(index_scheduling, "flag_modified", lambda *a, **k: None)
        toolkit = _toolkit()
        written = index_scheduling.stamp_schedule_retry_since(
            FakeSession(), toolkit, "docs", "7", "[ctx]")
        spec = importlib.util.spec_from_file_location(
            "elitea_core_models_pd_index_6583b", PLUGIN_ROOT / "models" / "pd" / "index.py")
        index_pd = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(index_pd)
        model = index_pd.ToolkitIndexingSchedule.parse_obj(_entry(toolkit))
        assert model.retry_since == written

    def test_a_schedule_deleted_mid_tick_is_not_resurrected(self, index_scheduling,
                                                            monkeypatch):
        monkeypatch.setattr(index_scheduling, "flag_modified", lambda *a, **k: None)

        def _drop(toolkit):
            toolkit.meta["indexes_meta"]["docs"]["schedules"].pop("7", None)
        session = FakeSession(on_refresh=_drop)
        assert index_scheduling.stamp_schedule_retry_since(
            session, _toolkit(), "docs", "7", "[ctx]") is None
        assert session.commits == 0

    def test_a_failed_write_never_escapes(self, index_scheduling, monkeypatch):
        monkeypatch.setattr(index_scheduling, "flag_modified", lambda *a, **k: None)
        session = FakeSession(commit_raises=True)
        assert index_scheduling.stamp_schedule_retry_since(
            session, _toolkit(), "docs", "7", "[ctx]") is None
        assert (session.commits, session.rollbacks) == (0, 1)


class TestReportSurvivesAConfigurationsOutage:
    """The report path reaches pgvector through the plugin whose outage it usually reports."""

    @pytest.fixture(scope="class")
    def tick_tree(self):
        return ast.parse((PLUGIN_ROOT / "rpc" / "index_scheduling.py").read_text())

    def test_an_expand_failure_concludes_instead_of_escaping(self, index_scheduling,
                                                             monkeypatch):
        """Escaping lands in the tick's settings catch-all, which does not move the cursor,
        so every broken schedule would re-enter this 2s RPC on every tick forever."""
        sent = []
        monkeypatch.setattr(index_scheduling, "this", types.SimpleNamespace(
            module=types.SimpleNamespace(
                notify_index_data_status=lambda p: sent.append(p))))

        def _boom(**kw):
            raise TimeoutError("configurations plugin is down")
        monkeypatch.setattr(index_scheduling, "rpc_tools", types.SimpleNamespace(
            RpcMixin=lambda: types.SimpleNamespace(rpc=types.SimpleNamespace(
                timeout=lambda t: types.SimpleNamespace(configurations_expand=_boom)))))

        recorded = index_scheduling.handle_failed_index_schedule(
            1, {}, 7, _toolkit(), "docs", "lookup broke")
        assert recorded, "an unreportable failure must still consume the cron slot"
        assert sent == []

    def test_a_failed_notification_does_not_undo_the_conclusion(self, index_scheduling,
                                                                monkeypatch):
        """By this point the row is flipped and the history entry committed.

        An escape here leaves the caller unable to advance the cursor, so the next tick
        re-flips and re-appends — the #6583 flood again, conditional on the notify path.
        The payload is built in this frame (the message renderer runs here), so a raise
        propagates whether or not the event bus is queued.
        """
        monkeypatch.setattr(index_scheduling, "update_toolkit_index_meta_history_with_failed_state",
                            lambda *a, **kw: {"flipped": True, "skipped_live_run": False,
                                              "reindex": True, "indexed": 5, "updated": 0})

        def _boom(payload):
            raise RuntimeError("notifications plugin exploded")
        monkeypatch.setattr(index_scheduling, "this", types.SimpleNamespace(
            module=types.SimpleNamespace(notify_index_data_status=_boom)))

        recorded = index_scheduling.handle_failed_index_schedule(
            1, {}, 7, _toolkit(), "docs", "creds broke")
        assert recorded, "a failed notification must not un-reach the conclusion"


class TestTickWiring:
    @pytest.fixture(scope="class")
    def tick_source(self):
        return (PLUGIN_ROOT / "rpc" / "index_scheduling.py").read_text()

    @pytest.fixture(scope="class")
    def tick_tree(self, tick_source):
        return ast.parse(tick_source)

    def _init_issue_block(self, tick_tree):
        for node in ast.walk(tick_tree):
            if isinstance(node, ast.If) and isinstance(node.test, ast.Name) \
                    and node.test.id == "init_issue":
                return node
        pytest.fail("could not find the `if init_issue:` block")

    def test_a_persistent_failure_is_demoted_to_terminal(self, tick_tree):
        """The escalation must clear the retryable flag rather than report from the
        retryable branch, so the path that already reports once per cron period does it."""
        demotions = [
            n for n in ast.walk(tick_tree)
            if isinstance(n, ast.If) and "retry_escalation_due" in ast.dump(n.test)]
        assert demotions, "nothing consults retry_escalation_due"
        gate = demotions[0]

        # Polarity, not vocabulary: nothing here executes the tick, so an inverted
        # condition would otherwise pass on the strength of the right names appearing.
        assert isinstance(gate.test, ast.BoolOp) and isinstance(gate.test.op, ast.And), \
            "escalation must require BOTH a retryable failure and an outlived grace"
        assert not [n for n in ast.walk(gate.test)
                    if isinstance(n, ast.UnaryOp) and isinstance(n.op, ast.Not)], \
            "a negated escalation test would escalate blips and never escalate outages"

        demoted = [n for n in ast.walk(gate)
                   if isinstance(n, ast.Assign)
                   and any(getattr(t, "id", None) == "init_issue_retryable"
                           for t in n.targets)]
        assert demoted, "escalation must demote the classification"
        assert all(n.value.value is False for n in demoted), \
            "escalation must set init_issue_retryable False, not True"

    def test_the_owner_facing_text_renders_the_grace_as_a_duration(self, tick_tree):
        """Pins the fix at the call site, not just in the helper.

        This string lands in the pgvector `error` field and the notification body. A raw
        timedelta renders as "1:00:00", which reads as a time of day — and reverting to it
        left the whole suite green, because only describe_grace itself was tested.
        """
        demotions = [n for n in ast.walk(tick_tree)
                     if isinstance(n, ast.If) and "retry_escalation_due" in ast.dump(n.test)]
        assert demotions, "nothing consults retry_escalation_due"
        body = ast.dump(ast.Module(body=demotions[0].body, type_ignores=[]))
        assert "describe_grace" in body, \
            "the owner-facing escalation text must render the grace as a duration"
        assert "RETRYABLE_REPORT_GRACE" not in body, \
            "a raw timedelta in owner-facing text renders as a clock time"

    def test_the_retryable_warning_keeps_naming_its_consequence(self, tick_tree):
        """The only trace of a young outage. Stripping the consequence left the suite green.

        It has to say what is happening to the index, not just why — and when it will stop
        being silent about it.
        """
        block = self._init_issue_block(tick_tree)
        branch = next(n for n in ast.walk(block)
                      if isinstance(n, ast.If) and "init_issue_retryable" in ast.dump(n.test))
        warnings = [n for n in ast.walk(branch)
                    if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
                    and n.func.attr == "warning"]
        assert warnings, "the retryable branch must log"
        text = ast.dump(warnings[0])
        assert "not being updated" in text, "the warning must name the consequence"
        assert "describe_grace" in text, "the warning must say when it stops being silent"

    def test_the_retryable_branch_still_reports_nothing(self, tick_tree):
        """The 75b7edc tripwire, restated here so deleting it there still trips.

        Recording an outage is not reporting one: the branch may write retry_since and
        nothing else.
        """
        block = self._init_issue_block(tick_tree)
        branch = next(n for n in ast.walk(block)
                      if isinstance(n, ast.If) and "init_issue_retryable" in ast.dump(n.test))

        # Hoisting the reporter above this branch leaves the branch clean while every
        # retryable tick still notifies. That is caught by running the tick
        # (test_6583c_tick_behaviour.py::test_a_blip_is_recorded_but_never_reported); a
        # positional scan here also failed whenever the block gained a `with` or a guard.
        body = ast.dump(ast.Module(body=branch.body, type_ignores=[]))
        assert "handle_failed_index_schedule" not in body
        assert "stamp_schedule_last_run" not in body

    # Which paths may consume the cron slot is asserted by running the tick, in
    # test_6583c_tick_behaviour.py. Four successive AST shapes each permitted a different
    # arm — a source count, an ExceptHandler sweep, an `orelse` check that was vacuous, and
    # a block allow-list that swept in the blocks' own else arms. The property is what the
    # tick does, not where the call sits.

    def test_the_outage_stamp_is_written_once_not_per_tick(self, tick_tree):
        """Re-stamping would pin its age at one tick interval so it never escalates, and
        would cost a write on every tick of the outage."""
        guards = [n for n in ast.walk(tick_tree)
                  if isinstance(n, ast.If)
                  and "stamp_schedule_retry_since" in ast.dump(n)
                  and "retry_since" in ast.dump(n.test)]
        assert guards, "the outage stamp must be guarded on retry_since being unset"

    def test_a_recovered_lookup_clears_the_outage_stamp(self, tick_tree):
        """Without this a lookup that recovers into contention never reaches a cursor
        write, so the stale stamp escalates the next unrelated blip with no grace."""
        clears = [n for n in ast.walk(tick_tree)
                  if isinstance(n, ast.If)
                  and "clear_schedule_retry_since" in ast.dump(n)
                  and "credentials_ok" in ast.dump(n.test)]
        assert clears, "nothing clears retry_since when the lookup succeeds"
