"""Issue #4997 - a scheduled index must stop running by itself unless somebody renews it.

A schedule created once ran forever. Nobody notices an index nobody looks at, so the
platform kept paying for embeddings of abandoned collections indefinitely. The repair gives
every schedule a deadline, warns its author twice before it lands, and switches the schedule
off rather than deleting it, so renewal is one toggle.

Three properties carry the whole feature and each has a failure mode that is silent:

* Pricing must classify by the SHORTEST gap and size by the MEAN gap. Collapsing them to one
  statistic is invisible on a daily cron, which is nearly every schedule.
* An unpriceable cron must leave the schedule running. Guessing a deadline there disables it;
  raising strands it forever (#6526).
* Every notification must be tied to its dedup write landing, or a database under contention
  turns two warnings into 1440 notifications a day.

Run via:
    python tests/run_tests.py integration/test_4997_schedule_expiration.py -v
"""

import ast
import copy
import importlib.util
import pathlib
import sys
import types
from datetime import datetime, timedelta, timezone

import pytest

from fixtures.helpers import register_index_pd_module

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
UTC = timezone.utc


@pytest.fixture(scope="module")
def index_pd():
    """The real models/pd/index.py — it has no plugin-relative imports."""
    spec = importlib.util.spec_from_file_location(
        "elitea_core_models_pd_index_4997", PLUGIN_ROOT / "models" / "pd" / "index.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def index_scheduling():
    """utils/index_scheduling.py with its plugin imports stubbed, but the pd model real.

    The expiry helper and the PATCH endpoint have to agree on the window, so a stubbed
    calculator here would let the two drift while the suite stayed green.
    """
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

    tools_pkg = sys.modules.get("tools") or types.ModuleType("tools")
    tools_pkg.this = types.SimpleNamespace(module=types.SimpleNamespace())
    tools_pkg.rpc_tools = types.SimpleNamespace(RpcMixin=lambda: types.SimpleNamespace())
    tools_pkg.db = getattr(tools_pkg, "db", types.SimpleNamespace(get_session=lambda pid: None))
    tools_pkg.VaultClient = getattr(tools_pkg, "VaultClient", type("VaultClient", (), {}))
    sys.modules["tools"] = tools_pkg

    enums_pkg = types.ModuleType("plugins.elitea_core.models.enums")
    enums_pkg.InitiatorType = types.SimpleNamespace(schedule="schedule", user="user")
    sys.modules["plugins.elitea_core.models.enums"] = enums_pkg

    # Another suite in the same interpreter may already have loaded the real module; fill in
    # only what is missing so this one does not overwrite it with a thinner stub.
    app_tools = sys.modules.setdefault(
        "plugins.elitea_core.utils.application_tools",
        types.ModuleType("plugins.elitea_core.utils.application_tools"))
    if not hasattr(app_tools, "IndexMetaLockTimeoutError"):
        app_tools.IndexMetaLockTimeoutError = type("IndexMetaLockTimeoutError", (Exception,), {})
    if not hasattr(app_tools, "update_toolkit_index_meta_history_with_failed_state"):
        app_tools.update_toolkit_index_meta_history_with_failed_state = lambda *a, **k: None

    register_index_pd_module(PLUGIN_ROOT)
    spec = importlib.util.spec_from_file_location(
        "plugins.elitea_core.utils.index_scheduling",
        PLUGIN_ROOT / "utils" / "index_scheduling.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _toolkit(index="docs", user="7", **entry_overrides):
    entry = {"cron": "0 3 * * *", "enabled": True,
             "created_by": 7, "last_run": "2026-01-01T00:00:00+00:00"}
    entry.update(entry_overrides)
    return types.SimpleNamespace(
        id=42, type="confluence",
        meta={"indexes_meta": {index: {"schedules": {user: entry}}}})


def _entry(toolkit, index="docs", user="7"):
    return toolkit.meta["indexes_meta"][index]["schedules"][user]


class FakeSession:
    def __init__(self, on_refresh=None, commit_raises=False):
        self.commits = self.rollbacks = self.refreshes = 0
        self._on_refresh, self._commit_raises = on_refresh, commit_raises

    def refresh(self, toolkit):
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


@pytest.fixture
def expiry(index_scheduling, monkeypatch):
    """handle_schedule_expiry with the ORM flag and the notifier captured.

    Returns (call, sent) where `sent` accumulates the notification payloads.
    """
    monkeypatch.setattr(index_scheduling, "flag_modified", lambda *a, **k: None)
    sent = []
    monkeypatch.setattr(index_scheduling, "this", types.SimpleNamespace(
        module=types.SimpleNamespace(
            notify_index_schedule_expiry=lambda payload: sent.append(payload))))

    def call(toolkit, session=None, now=None, index="docs", user="7", model=None):
        pd = sys.modules["plugins.elitea_core.models.pd.index"]
        schedule_model = model or pd.ToolkitIndexingSchedule.parse_obj(
            _entry(toolkit, index, user))
        return index_scheduling.handle_schedule_expiry(
            session if session is not None else FakeSession(),
            toolkit, index, user, "[ctx]", schedule_model, 1, now=now)

    return call, sent


class TestPricingClassifiesByShortestGapAndSizesByMean:
    """Two statistics, two jobs. One value cannot do both.

    Using the mean to classify makes a cron firing on the 1st and 2nd of every month
    low-frequency, so a twice-monthly schedule gets 180+ days. Using the min to size makes a
    quarterly cron's window 6 x 1 day. Neither shows up on a daily cron.
    """

    def test_a_daily_cron_gets_the_high_frequency_window(self, index_pd):
        now = datetime(2026, 5, 1, tzinfo=UTC)
        assert index_pd.compute_schedule_expiration("0 3 * * *", now) == now + timedelta(days=90)

    def test_a_weekly_cron_gets_the_high_frequency_window(self, index_pd):
        now = datetime(2026, 5, 1, tzinfo=UTC)
        assert index_pd.compute_schedule_expiration("0 3 * * 1", now) == now + timedelta(days=90)

    def test_a_monthly_cron_gets_at_least_the_low_frequency_window(self, index_pd):
        """28 days (February) is the shortest gap a monthly cron produces, and the boundary
        is exclusive, so every monthly pattern lands on the low-frequency side.

        Six months of a ~30.4-day mean is a shade over the 180-day floor, so the floor is
        the guarantee here rather than the answer.
        """
        now = datetime(2026, 5, 1, tzinfo=UTC)
        window = index_pd.compute_schedule_expiration("0 3 1 * *", now) - now
        assert window >= timedelta(days=180), window
        assert window < timedelta(days=190), window

    def test_a_twice_monthly_cron_is_priced_as_high_frequency(self, index_pd):
        """The 1st and the 2nd of each month: one 1-day gap and one ~29-day gap. It is a
        frequently-firing schedule that merely happens to have a long gap, and the min is
        what says so."""
        now = datetime(2026, 5, 1, tzinfo=UTC)
        assert index_pd.compute_schedule_expiration(
            "0 3 1,2 * *", now) == now + timedelta(days=90)

    def test_a_burst_cadence_is_classified_by_its_shortest_gap(self, index_pd):
        """Pairs of consecutive days, once a quarter: min 1 day, mean ~45 days.

        This is the case that separates the two statistics. Classifying on the mean puts it
        past the monthly boundary and hands it a 270-day window, even though it fires twice
        in 24 hours. The `0 3 1,2 * *` case above cannot catch that — its mean is under a
        month too.
        """
        now = datetime(2026, 5, 1, tzinfo=UTC)
        assert index_pd.compute_schedule_expiration(
            "0 3 1,2 1,4,7,10 *", now) == now + timedelta(days=90)

    def test_an_uneven_low_frequency_cadence_is_sized_by_its_mean_gap(self, index_pd):
        """January and March: a 59-day gap and a 306-day one.

        Sizing on the min gives 6 x 59 days and retires it after its 12th firing; sizing on
        the mean gives the intended six firings. The quarterly case above cannot catch that
        — its gaps are nearly uniform.
        """
        now = datetime(2026, 5, 1, tzinfo=UTC)
        window = index_pd.compute_schedule_expiration("0 3 1 1,3 *", now) - now
        assert window > timedelta(days=1000), window

    def test_a_quarterly_cron_gets_six_firings_worth_of_time(self, index_pd):
        """Six firings of a ~91-day cadence is ~18 months, so the floor does not apply.

        Capping this at 180 days would retire a quarterly schedule after its second run.
        """
        now = datetime(2026, 5, 1, tzinfo=UTC)
        window = index_pd.compute_schedule_expiration("0 3 1 1,4,7,10 *", now) - now
        assert window > timedelta(days=500), window
        assert window < timedelta(days=600), window

    def test_a_yearly_cron_gets_roughly_six_years(self, index_pd):
        now = datetime(2026, 5, 1, tzinfo=UTC)
        window = index_pd.compute_schedule_expiration("0 3 1 1 *", now) - now
        assert timedelta(days=6 * 364) < window < timedelta(days=6 * 367), window

    def test_the_window_does_not_depend_on_when_the_schedule_was_saved(self, index_pd):
        """Gaps are probed from a fixed instant. Probing from `now` would make a monthly
        schedule saved in January (31-day gaps) outlive one saved in February."""
        january = datetime(2026, 1, 15, tzinfo=UTC)
        february = datetime(2026, 2, 15, tzinfo=UTC)
        assert (index_pd.compute_schedule_expiration("0 3 1 * *", january) - january
                == index_pd.compute_schedule_expiration("0 3 1 * *", february) - february)

    def test_the_deadline_keeps_the_caller_s_timezone_awareness(self, index_pd):
        """A naive deadline compared against an aware `now` raises inside the tick."""
        deadline = index_pd.compute_schedule_expiration("0 3 * * *", datetime.now(UTC))
        assert deadline.tzinfo is not None
        assert deadline > datetime.now(UTC)

    def test_an_unwalkable_cron_raises_rather_than_guessing(self, index_pd):
        """`0 0 30 2 *` constructs fine and only fails on iteration. A silent fallback
        window here is a deadline nobody can predict; the caller's job is to leave such a
        schedule unpriced."""
        with pytest.raises(ValueError):
            index_pd.compute_schedule_expiration("0 0 30 2 *", datetime.now(UTC))


class TestLegacySchedulesStillParse:
    """Rows written before this feature carry neither field. Rejecting them would strand
    every existing schedule on the tick's "invalid schedule configuration" path (#6526)."""

    def _base(self):
        return {"cron": "0 3 * * *", "enabled": True, "created_by": 7,
                "last_run": "2026-01-01T00:00:00+00:00"}

    def test_a_schedule_without_the_new_fields_parses_unpriced(self, index_pd):
        model = index_pd.ToolkitIndexingSchedule.parse_obj(self._base())
        assert model.expires_at is None
        assert model.notified_expiry_warnings == []

    def test_an_unreadable_deadline_degrades_to_unpriced(self, index_pd):
        """Not a raise: that strands the schedule forever. Not the epoch: that disables it
        on the next tick. None means "price it again", which costs one more window."""
        for bad in ("not a date", "2026-13-45T99:99:99", 12345, {"a": 1}, []):
            model = index_pd.ToolkitIndexingSchedule.parse_obj(
                {**self._base(), "expires_at": bad})
            assert model.expires_at is None, bad

    def test_a_naive_deadline_is_read_as_utc(self, index_pd):
        model = index_pd.ToolkitIndexingSchedule.parse_obj(
            {**self._base(), "expires_at": "2026-12-01T03:00:00"})
        assert datetime.fromisoformat(model.expires_at) == datetime(2026, 12, 1, 3, tzinfo=UTC)

    def test_an_offset_deadline_is_normalized_to_utc(self, index_pd):
        model = index_pd.ToolkitIndexingSchedule.parse_obj(
            {**self._base(), "expires_at": "2026-12-01T06:00:00+03:00"})
        assert datetime.fromisoformat(model.expires_at) == datetime(2026, 12, 1, 3, tzinfo=UTC)

    def test_a_malformed_warning_list_degrades_to_empty(self, index_pd):
        for bad in ("7d", None, 7, {"7d": True}):
            model = index_pd.ToolkitIndexingSchedule.parse_obj(
                {**self._base(), "notified_expiry_warnings": bad})
            assert model.notified_expiry_warnings == [], bad


class TestBackfillPricesOnceAndLetsTheScheduleRun:
    def test_an_unpriced_schedule_is_priced_and_not_skipped(self, expiry, index_pd):
        call, sent = expiry
        toolkit, session = _toolkit(), FakeSession()
        now = datetime(2026, 5, 1, tzinfo=UTC)

        assert call(toolkit, session, now=now) is False, "pricing must not skip the run"
        written = datetime.fromisoformat(_entry(toolkit)["expires_at"])
        assert written == index_pd.compute_schedule_expiration("0 3 * * *", now)
        assert sent == [], "a fresh deadline is not news"

    def test_the_deadline_is_a_full_window_from_first_sight_not_from_creation(self, expiry):
        """Backdating to `created_at` would disable most of the platform's schedules in the
        first tick after deploy — the outcome this whole design exists to avoid."""
        call, _ = expiry
        now = datetime(2026, 5, 1, tzinfo=UTC)
        toolkit = _toolkit(last_run="2020-01-01T00:00:00+00:00")
        call(toolkit, now=now)
        assert datetime.fromisoformat(_entry(toolkit)["expires_at"]) > now

    def test_pricing_arms_both_warnings(self, expiry):
        call, _ = expiry
        toolkit = _toolkit(notified_expiry_warnings=["7d", "24h"])
        call(toolkit, now=datetime(2026, 5, 1, tzinfo=UTC))
        assert _entry(toolkit)["notified_expiry_warnings"] == [], \
            "a new deadline inherits no sent-warning state, or its warnings never fire"

    def test_a_disabled_schedule_is_left_alone(self, expiry):
        """A schedule nobody has turned on is not consuming anything, and pricing it would
        start a clock that its owner cannot see running."""
        call, sent = expiry
        toolkit = _toolkit(enabled=False)
        assert call(toolkit) is False
        assert "expires_at" not in _entry(toolkit)
        assert sent == []

    def test_an_unpriceable_cron_leaves_the_schedule_running(self, expiry):
        """Fail open. A cron the calculator cannot walk is a pricing bug, and the cost of
        guessing is a disabled schedule its owner never asked to disable."""
        call, sent = expiry
        toolkit, session = _toolkit(cron="0 0 30 2 *"), FakeSession()
        assert call(toolkit, session) is False
        assert _entry(toolkit).get("expires_at") is None
        assert (session.commits, sent) == (0, [])

    def test_a_failed_pricing_write_leaves_the_schedule_running(self, expiry):
        call, sent = expiry
        toolkit, session = _toolkit(), FakeSession(commit_raises=True)
        assert call(toolkit, session) is False
        assert (session.rollbacks, sent) == (1, [])


class TestExpiryDisablesRatherThanDeletes:
    def _expired(self, **overrides):
        return _toolkit(expires_at="2026-05-01T00:00:00+00:00", **overrides)

    def test_a_past_deadline_switches_the_schedule_off_and_skips_the_run(self, expiry):
        call, sent = expiry
        toolkit, session = self._expired(), FakeSession()
        assert call(toolkit, session, now=datetime(2026, 5, 2, tzinfo=UTC)) is True
        assert _entry(toolkit)["enabled"] is False
        assert session.commits == 1

    def test_the_schedule_survives_expiry_so_renewal_is_one_toggle(self, expiry):
        call, _ = expiry
        toolkit = self._expired()
        call(toolkit, now=datetime(2026, 5, 2, tzinfo=UTC))
        entry = _entry(toolkit)
        assert entry["cron"] == "0 3 * * *"
        assert entry["expires_at"], "the deadline is what the UI reads to say 'Expired'"

    def test_the_author_is_told_and_told_how_to_renew(self, expiry):
        call, sent = expiry
        call(self._expired(), now=datetime(2026, 5, 2, tzinfo=UTC))
        assert len(sent) == 1
        payload = sent[0]
        assert payload["expired"] is True
        assert payload["user_id"] == 7, "notices go to the author, not to the -1 team key"
        assert payload["project_id"] == 1
        assert payload["index_name"] == "docs"
        assert "switched off" in payload["message"]
        assert "back on" in payload["message"], "a notice with no way out is a dead end"

    def test_the_deadline_reads_as_a_date_not_an_iso_timestamp(self, expiry):
        """This lands in a notification read by the schedule's author."""
        call, sent = expiry
        call(self._expired(), now=datetime(2026, 5, 2, tzinfo=UTC))
        assert "2026-05-01 00:00 UTC" in sent[0]["message"]
        assert "T00:00:00+00:00" not in sent[0]["message"]

    def test_the_run_is_skipped_even_when_the_disable_write_is_lost(self, expiry):
        """An expired schedule must never dispatch. Returning False on a failed write would
        run it for as long as the database stays unhappy."""
        call, sent = expiry
        session = FakeSession(commit_raises=True)
        assert call(self._expired(), session, now=datetime(2026, 5, 2, tzinfo=UTC)) is True
        assert sent == [], "nothing was retired, so there is nothing to announce"

    def test_a_deadline_exactly_now_expires(self, expiry):
        call, _ = expiry
        toolkit = self._expired()
        assert call(toolkit, now=datetime(2026, 5, 1, tzinfo=UTC)) is True

    def test_a_schedule_deleted_mid_tick_is_not_resurrected(self, expiry):
        call, sent = expiry

        def _drop(toolkit):
            toolkit.meta["indexes_meta"]["docs"]["schedules"].pop("7", None)
        session = FakeSession(on_refresh=_drop)
        assert call(self._expired(), session, now=datetime(2026, 5, 2, tzinfo=UTC)) is True
        assert (session.commits, sent) == (0, [])

    def test_a_team_schedule_notifies_its_creator(self, expiry):
        """Team schedules are stored under user_id -1. Nobody owns that number."""
        call, sent = expiry
        toolkit = _toolkit(user="-1", expires_at="2026-05-01T00:00:00+00:00", created_by=59)
        assert call(toolkit, user="-1", now=datetime(2026, 5, 2, tzinfo=UTC)) is True
        assert sent[0]["user_id"] == 59

    def test_an_authorless_schedule_is_still_retired(self, expiry):
        """Rows predating `created_by` have no addressee, but they still expire."""
        call, sent = expiry
        toolkit = _toolkit(expires_at="2026-05-01T00:00:00+00:00")
        del _entry(toolkit)["created_by"]
        assert call(toolkit, now=datetime(2026, 5, 2, tzinfo=UTC)) is True
        assert _entry(toolkit)["enabled"] is False
        assert sent[0]["user_id"] is None, "the delivery method drops it and logs"

    def test_a_broken_notifier_does_not_undo_the_retirement(self, index_scheduling,
                                                            monkeypatch, index_pd):
        """The disable is committed before the push. An escape here leaves the tick unable
        to conclude, so the next tick re-disables and re-notifies every minute."""
        monkeypatch.setattr(index_scheduling, "flag_modified", lambda *a, **k: None)

        def _boom(payload):
            raise RuntimeError("notifications plugin exploded")
        monkeypatch.setattr(index_scheduling, "this", types.SimpleNamespace(
            module=types.SimpleNamespace(notify_index_schedule_expiry=_boom)))

        toolkit = _toolkit(expires_at="2026-05-01T00:00:00+00:00")
        pd = sys.modules["plugins.elitea_core.models.pd.index"]
        assert index_scheduling.handle_schedule_expiry(
            FakeSession(), toolkit, "docs", "7", "[ctx]",
            pd.ToolkitIndexingSchedule.parse_obj(_entry(toolkit)), 1,
            now=datetime(2026, 5, 2, tzinfo=UTC)) is True
        assert _entry(toolkit)["enabled"] is False


class TestWarningsFireOnceEach:
    """The tick re-reads every schedule every minute and a warning window is days wide, so
    "send when inside the window" is 1440 notifications a day per schedule."""

    def _due(self, remaining, **overrides):
        now = datetime(2026, 5, 1, tzinfo=UTC)
        return now, _toolkit(expires_at=(now + remaining).isoformat(), **overrides)

    def test_no_warning_outside_the_first_window(self, expiry):
        call, sent = expiry
        now, toolkit = self._due(timedelta(days=30))
        assert call(toolkit, now=now) is False
        assert sent == []
        assert _entry(toolkit).get("notified_expiry_warnings", []) == [], \
            "nothing is written until a window is actually crossed"

    def test_the_week_warning_fires_inside_its_window(self, expiry):
        call, sent = expiry
        now, toolkit = self._due(timedelta(days=6))
        assert call(toolkit, now=now) is False, "a warning is not a reason to skip the run"
        assert len(sent) == 1
        assert sent[0]["expired"] is False
        assert "7 days" in sent[0]["message"]
        assert "Reschedule" in sent[0]["message"]
        assert _entry(toolkit)["notified_expiry_warnings"] == ["7d"]

    def test_the_week_warning_does_not_repeat_on_the_next_tick(self, expiry):
        call, sent = expiry
        now, toolkit = self._due(timedelta(days=6))
        call(toolkit, now=now)
        call(toolkit, now=now + timedelta(minutes=1))
        assert len(sent) == 1, sent

    def test_the_day_warning_fires_after_the_week_warning(self, expiry):
        call, sent = expiry
        now, toolkit = self._due(timedelta(days=6))
        call(toolkit, now=now)
        call(toolkit, now=now + timedelta(days=5, hours=12))
        assert [p["expired"] for p in sent] == [False, False]
        assert "24 hours" in sent[-1]["message"]
        assert sorted(_entry(toolkit)["notified_expiry_warnings"]) == ["24h", "7d"]

    def test_a_schedule_first_seen_inside_the_last_day_gets_one_warning(self, expiry):
        """A schedule priced late, or a tick outage spanning the 7-day window, must not fire
        two notifications at once — and must not fire the stale 7-day text."""
        call, sent = expiry
        now, toolkit = self._due(timedelta(hours=12))
        assert call(toolkit, now=now) is False
        assert len(sent) == 1
        assert "24 hours" in sent[0]["message"]
        assert "7 days" not in sent[0]["message"]
        assert sorted(_entry(toolkit)["notified_expiry_warnings"]) == ["24h", "7d"], \
            "the skipped window must be marked sent, or it fires after the closer one"

    def test_a_missed_week_window_never_warns_late(self, expiry):
        call, sent = expiry
        now, toolkit = self._due(timedelta(hours=12))
        call(toolkit, now=now)
        call(toolkit, now=now + timedelta(hours=1))
        assert len(sent) == 1, sent

    def test_a_failed_dedup_write_suppresses_the_warning(self, expiry):
        """The write is the only thing that stops the next tick from repeating. Notifying
        without it turns a database blip into a notification storm."""
        call, sent = expiry
        now, toolkit = self._due(timedelta(days=6))
        assert call(toolkit, FakeSession(commit_raises=True), now=now) is False
        assert sent == [], "no dedup record, no notification"

    def test_a_renewed_schedule_re_arms_both_warnings(self, expiry, index_pd):
        """The PATCH endpoint rewrites expires_at and clears the sent list. Simulated here
        because the endpoint is not importable under the test stubs; the pairing is what
        matters — a new deadline with a stale list never warns again."""
        call, sent = expiry
        now = datetime(2026, 5, 1, tzinfo=UTC)
        toolkit = _toolkit(
            expires_at=index_pd.compute_schedule_expiration("0 3 * * *", now).isoformat(),
            notified_expiry_warnings=[])
        assert call(toolkit, now=now) is False
        assert sent == []
        assert call(toolkit, now=now + timedelta(days=89)) is False
        assert len(sent) == 1


class TestSaveIsARenewal:
    """The PATCH endpoint is the only writer of schedules, so it is the only place a deadline
    can be granted. It is not importable under these stubs (Flask, auth, ORM), so the shape
    of the one statement that matters is asserted from source."""

    @pytest.fixture(scope="class")
    def patch_source(self):
        return (PLUGIN_ROOT / "api" / "v2" / "index_meta.py").read_text()

    def test_the_deadline_is_computed_from_the_saved_cron(self, patch_source):
        assert "expires_at=compute_schedule_expiration(update_data.cron, saved_at)" \
            in patch_source, "every save must re-price, or an edit inherits the old deadline"

    def test_a_client_supplied_deadline_is_never_honoured(self, patch_source):
        """The UI round-trips the stored schedule back on edit and on the enable toggle, so
        reading expires_at from the payload would let any caller grant itself forever."""
        tree = ast.parse(patch_source)
        reads = [n for n in ast.walk(tree)
                 if isinstance(n, ast.Attribute) and n.attr in
                 ("expires_at", "notified_expiry_warnings")
                 and "update_data" in ast.dump(n.value)]
        assert reads == [], "the payload's expiry fields must be ignored"

    def test_the_payload_model_has_no_expiry_fields_to_read(self, index_pd):
        fields = index_pd.UpdateIndexingSchedule.model_fields
        assert "expires_at" not in fields
        assert "notified_expiry_warnings" not in fields

    def test_a_saved_schedule_starts_with_no_warnings_sent(self, index_pd):
        """Carrying the sent list across a renewal is silent: the schedule is renewed, both
        windows are already marked sent, and it is then retired with no warning at all."""
        saved = index_pd.ToolkitIndexingSchedule(
            cron="0 3 * * *", enabled=True, created_by=7,
            last_run=datetime.now(UTC),
            expires_at=index_pd.compute_schedule_expiration("0 3 * * *", datetime.now(UTC)),
        )
        assert saved.notified_expiry_warnings == []

    def test_a_cron_the_calculator_cannot_price_is_rejected_at_the_boundary(self, index_pd):
        """Why the endpoint needs no guard around pricing: the payload model walks the same
        expression for its daily-frequency floor, so an unwalkable cron is a 400 long before
        compute_schedule_expiration could raise a 500.
        """
        with pytest.raises(Exception):
            index_pd.UpdateIndexingSchedule(user_id=1, cron="0 0 30 2 *", enabled=True)


class TestTickWiring:
    @pytest.fixture(scope="class")
    def tick_tree(self):
        return ast.parse((PLUGIN_ROOT / "rpc" / "index_scheduling.py").read_text())

    def test_expiry_is_checked_before_the_cron_due_check(self, tick_tree):
        """A monthly schedule's 7-day warning must not have to coincide with a firing.
        Behind the due check, a warning would only be sent if a run happened to land inside
        the window — which for a monthly cron is most often never.
        """
        source = (PLUGIN_ROOT / "rpc" / "index_scheduling.py").read_text()
        assert source.index("handle_schedule_expiry") < source.index("should_trigger_by_time"), \
            "the expiry check must run before the cron-due check"

    def test_an_expired_schedule_short_circuits_the_tick(self, tick_tree):
        """The helper returning True means "do not run this schedule". Ignoring the return
        value would dispatch an index run for a schedule that was just switched off.
        """
        gates = [n for n in ast.walk(tick_tree)
                 if isinstance(n, ast.If) and "handle_schedule_expiry" in ast.dump(n.test)]
        assert gates, "the tick must branch on the expiry helper's return value"
        assert any(isinstance(n, ast.Continue) for n in ast.walk(gates[0])), \
            "an expired schedule must skip the rest of the loop body"
        assert not [n for n in ast.walk(gates[0].test)
                    if isinstance(n, ast.UnaryOp) and isinstance(n.op, ast.Not)], \
            "a negated test skips every live schedule and runs every expired one"

    def test_the_tick_reports_how_many_schedules_it_retired(self, tick_tree):
        """Every per-schedule diagnostic is debug-level, which is off in production. Without
        this counter a deploy that retires thousands of schedules leaves no trace."""
        source = (PLUGIN_ROOT / "rpc" / "index_scheduling.py").read_text()
        assert "'expired': 0" in source, "the counter must be initialised, not conditional"
        assert "expired={stats['expired']}" in source, \
            "the per-tick summary must report retirements"
