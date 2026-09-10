"""Issue #6583 - a failed schedule must consume its cron slot, not re-fire every tick.

`last_run` is the scheduler's cron cursor: `is_cron_due` asks only whether a firing has
elapsed since it. The single write of it used to sit after a successful `start_index_task`,
so any tick that concluded the schedule *could not* run left the cursor where it was and
the schedule stayed due on every 60s scan - appending a `failed` history entry and
notifying the owner once a minute, forever, while indexing nothing.

The fix has two halves, both pinned here:

  1. `handle_failed_index_schedule` now reports whether it reached a conclusion about the
     schedule. Contention (a lock, or a live registered run) returns False and must keep
     today's one-minute retry; a recorded failure, or an index that does not exist in this
     project, returns True and consumes the slot.
  2. `stamp_schedule_last_run` performs the write for both the dispatch and the failure
     paths, so the delete-wins re-read and the refresh()-rebinding trap live in one place.

Run via:
    python tests/run_tests.py integration/test_6583_schedule_failure_cron_cadence.py -v
"""

import ast
import importlib.util
import pathlib
import sys
import types

import pytest


PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]


# The scaffolds below mirror test_6232_preserve_index_core_guards.py, which loads the same
# two modules; kept local so this file can be run on its own.
@pytest.fixture(scope="module")
def application_tools():
    for name in (
        "plugins", "plugins.elitea_core",
        "plugins.elitea_core.models", "plugins.elitea_core.utils",
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
    sys.modules["tools"] = tools_pkg

    models_all = types.ModuleType("plugins.elitea_core.models.all")
    models_all.EliteATool = type("EliteATool", (), {})
    models_all.EntityToolMapping = type("EntityToolMapping", (), {})
    models_all.ApplicationVersion = type("ApplicationVersion", (), {})
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
    exceptions.MaintenanceInProgressError = type(
        "MaintenanceInProgressError", (Exception,), {})
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


@pytest.fixture(scope="module")
def index_scheduling(application_tools):
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


def _toolkit(schedules=None, index="docs"):
    """A toolkit whose meta carries one schedule, shaped as the DB stores it."""
    if schedules is None:
        schedules = {"7": {"cron": "0 3 * * *", "enabled": True,
                           "last_run": "2026-01-01T00:00:00+00:00"}}
    return types.SimpleNamespace(
        id=42, type="confluence",
        meta={"indexes_meta": {index: {"schedules": schedules}}},
    )


class FakeSession:
    """Records commits/rollbacks. refresh() rebinds toolkit.meta to a NEW dict, exactly as
    SQLAlchemy's does - the trap the helper exists to contain."""

    def __init__(self, on_refresh=None, commit_raises=False):
        self.commits = 0
        self.rollbacks = 0
        self.refreshes = 0
        self._on_refresh = on_refresh
        self._commit_raises = commit_raises

    def refresh(self, toolkit):
        self.refreshes += 1
        import copy
        toolkit.meta = copy.deepcopy(toolkit.meta)
        if self._on_refresh is not None:
            self._on_refresh(toolkit)

    def commit(self):
        if self._commit_raises:
            raise RuntimeError("connection lost at COMMIT")
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


class TestFailureOutcomeContract:
    """Which exits mean 'we concluded something about this schedule'."""

    def _capture_notify(self, index_scheduling, monkeypatch):
        sent = []
        monkeypatch.setattr(
            index_scheduling, "this",
            types.SimpleNamespace(module=types.SimpleNamespace(
                notify_index_data_status=lambda payload: sent.append(payload))),
        )
        return sent

    def _run(self, index_scheduling, monkeypatch, writer):
        self._capture_notify(index_scheduling, monkeypatch)
        monkeypatch.setattr(
            index_scheduling, "update_toolkit_index_meta_history_with_failed_state", writer)
        return index_scheduling.handle_failed_index_schedule(
            1, {}, 7, _toolkit(), "docs", "creds broke")

    def test_a_lock_timeout_reports_no_conclusion(self, index_scheduling, application_tools,
                                                  monkeypatch):
        """The row is held by a live run's promote/registration. Nothing was learned about
        the schedule, so the cursor must not move and the next scan retries in a minute."""
        def _locked(*a, **kw):
            raise application_tools.IndexMetaLockTimeoutError("docs")
        assert self._run(index_scheduling, monkeypatch, _locked) is False

    def test_a_live_registered_run_reports_no_conclusion(self, index_scheduling, monkeypatch):
        assert self._run(
            index_scheduling, monkeypatch,
            lambda *a, **kw: {"flipped": False, "skipped_live_run": True}) is False

    def test_a_missing_index_row_reports_a_conclusion(self, index_scheduling, monkeypatch):
        """Only a run creates an index_meta row and this schedule cannot run, so retrying
        in a minute can never change the answer. This is the #6544 orphan that logged a
        WARNING every 60s forever."""
        assert self._run(
            index_scheduling, monkeypatch,
            lambda *a, **kw: {"flipped": False, "skipped_live_run": False}) is True

    def test_a_recorded_failure_reports_a_conclusion(self, index_scheduling, monkeypatch):
        """The reported bug: this exit notified the owner and appended history every tick."""
        assert self._run(
            index_scheduling, monkeypatch,
            lambda *a, **kw: {"flipped": True, "skipped_live_run": False,
                              "reindex": True, "indexed": 5, "updated": 0}) is True

    def test_the_conclusion_and_the_notification_agree(self, index_scheduling, monkeypatch):
        """Stamp gating and notification gating must stay locked together: every exit that
        suppresses the notification for contention also suppresses the stamp."""
        for writer, expected_recorded, expected_sent in (
            (lambda *a, **kw: {"flipped": False, "skipped_live_run": True}, False, 0),
            (lambda *a, **kw: {"flipped": False, "skipped_live_run": False}, True, 0),
            (lambda *a, **kw: {"flipped": True, "skipped_live_run": False,
                               "reindex": False, "indexed": 0, "updated": 0}, True, 1),
        ):
            sent = self._capture_notify(index_scheduling, monkeypatch)
            monkeypatch.setattr(
                index_scheduling, "update_toolkit_index_meta_history_with_failed_state", writer)
            recorded = index_scheduling.handle_failed_index_schedule(
                1, {}, 7, _toolkit(), "docs", "creds broke")
            assert (recorded, len(sent)) == (expected_recorded, expected_sent)


class TestStampScheduleLastRun:
    def test_it_advances_the_cursor_and_commits_once(self, index_scheduling, monkeypatch):
        monkeypatch.setattr(index_scheduling, "flag_modified", lambda *a, **k: None)
        toolkit, session = _toolkit(), FakeSession()
        written = index_scheduling.stamp_schedule_last_run(
            session, toolkit, "docs", "7", "[ctx]")
        assert written is not None
        assert toolkit.meta["indexes_meta"]["docs"]["schedules"]["7"]["last_run"] == written
        assert (session.commits, session.rollbacks) == (1, 0)

    def test_the_written_stamp_is_accepted_by_the_schedule_model(self, index_scheduling,
                                                                 monkeypatch):
        """The cursor is read back through ToolkitIndexingSchedule on the next tick, so the
        format the helper writes must survive normalize_last_run."""
        monkeypatch.setattr(index_scheduling, "flag_modified", lambda *a, **k: None)
        written = index_scheduling.stamp_schedule_last_run(
            FakeSession(), _toolkit(), "docs", "7", "[ctx]")
        spec = importlib.util.spec_from_file_location(
            "elitea_core_models_pd_index_6583", PLUGIN_ROOT / "models" / "pd" / "index.py")
        index_pd = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(index_pd)
        model = index_pd.ToolkitIndexingSchedule.parse_obj(
            {"cron": "0 3 * * *", "enabled": True, "last_run": written})
        assert model.last_run.startswith(written[:19])

    def test_it_writes_into_the_dict_refresh_returned(self, index_scheduling, monkeypatch):
        """refresh() rebinds toolkit.meta, so any implementation that mutates the dict the
        tick loop captured before the refresh writes into a detached object and the stamp
        is silently lost. Assert the stamp is visible on the post-refresh meta."""
        monkeypatch.setattr(index_scheduling, "flag_modified", lambda *a, **k: None)
        toolkit = _toolkit()
        captured_before_refresh = toolkit.meta["indexes_meta"]["docs"]["schedules"]
        session = FakeSession()
        written = index_scheduling.stamp_schedule_last_run(
            session, toolkit, "docs", "7", "[ctx]")
        assert session.refreshes == 1
        assert toolkit.meta["indexes_meta"]["docs"]["schedules"]["7"]["last_run"] == written
        assert toolkit.meta["indexes_meta"]["docs"]["schedules"] is not captured_before_refresh
        assert "last_run" not in captured_before_refresh["7"] or \
            captured_before_refresh["7"]["last_run"] != written

    def test_a_schedule_deleted_mid_tick_is_not_resurrected(self, index_scheduling, monkeypatch):
        """Delete wins: the re-read is the whole point of doing it before the write."""
        monkeypatch.setattr(index_scheduling, "flag_modified", lambda *a, **k: None)

        def _drop(toolkit):
            toolkit.meta["indexes_meta"]["docs"]["schedules"].pop("7", None)
        toolkit, session = _toolkit(), FakeSession(on_refresh=_drop)
        assert index_scheduling.stamp_schedule_last_run(
            session, toolkit, "docs", "7", "[ctx]") is None
        assert (session.commits, session.rollbacks) == (0, 0)
        assert "7" not in toolkit.meta["indexes_meta"]["docs"]["schedules"]

    def test_an_index_deleted_mid_tick_does_not_raise(self, index_scheduling, monkeypatch):
        monkeypatch.setattr(index_scheduling, "flag_modified", lambda *a, **k: None)

        def _drop_index(toolkit):
            toolkit.meta["indexes_meta"].pop("docs", None)
        session = FakeSession(on_refresh=_drop_index)
        assert index_scheduling.stamp_schedule_last_run(
            session, _toolkit(), "docs", "7", "[ctx]") is None
        assert session.commits == 0

    def test_a_failed_commit_rolls_back_and_never_escapes(self, index_scheduling, monkeypatch):
        """A session left in a failed transaction would take every later schedule in the
        tick down with it, so the helper absorbs its own failure."""
        monkeypatch.setattr(index_scheduling, "flag_modified", lambda *a, **k: None)
        session = FakeSession(commit_raises=True)
        assert index_scheduling.stamp_schedule_last_run(
            session, _toolkit(), "docs", "7", "[ctx]") is None
        assert (session.commits, session.rollbacks) == (0, 1)

    def test_a_failed_write_logs_the_consequence_not_just_the_cause(
            self, index_scheduling, monkeypatch):
        """By the time a failure path calls this, the notification and history entry are
        already written, so a cursor that did not move means the #6583 flood resumes while
        the tick summary still reads as one healthy failure. The log has to say that."""
        logged = []
        monkeypatch.setattr(index_scheduling, "flag_modified", lambda *a, **k: None)
        monkeypatch.setattr(
            index_scheduling, "log",
            types.SimpleNamespace(
                info=lambda m: logged.append(("info", m)),
                warning=lambda m: logged.append(("warning", m)),
                debug=lambda m: logged.append(("debug", m)),
                error=lambda m: logged.append(("error", m)),
                exception=lambda m: logged.append(("exception", m))))
        index_scheduling.stamp_schedule_last_run(
            FakeSession(commit_raises=True), _toolkit(), "docs", "7", "[ctx]")
        loud = [m for level, m in logged if level in ("exception", "error", "warning")]
        assert loud, "a failed cursor write must be logged loudly"
        assert any("every tick" in m for m in loud), \
            f"the log must name the consequence, got: {loud}"

    def test_a_mid_tick_deletion_is_not_reported_as_a_failure(
            self, index_scheduling, monkeypatch):
        """Delete-wins is benign - the schedule is gone and cannot retry, so it must not
        claim the schedule 'will retry on every tick'."""
        logged = []
        monkeypatch.setattr(index_scheduling, "flag_modified", lambda *a, **k: None)
        monkeypatch.setattr(
            index_scheduling, "log",
            types.SimpleNamespace(
                info=lambda m: logged.append(("info", m)),
                warning=lambda m: logged.append(("warning", m)),
                debug=lambda m: logged.append(("debug", m)),
                error=lambda m: logged.append(("error", m)),
                exception=lambda m: logged.append(("exception", m))))

        def _drop(toolkit):
            toolkit.meta["indexes_meta"]["docs"]["schedules"].pop("7", None)
        index_scheduling.stamp_schedule_last_run(
            FakeSession(on_refresh=_drop), _toolkit(), "docs", "7", "[ctx]")
        assert not [m for lvl, m in logged if lvl in ("exception", "error", "warning")]
        # all(), not any(): with any() this passes as soon as a single line lacks the
        # phrase, so it cannot fail and the claim it encodes is not actually checked.
        assert all("every tick" not in m for _, m in logged), \
            f"a benign deletion must not claim the schedule will retry: {logged}"

    def test_an_explicit_stamp_is_used_verbatim(self, index_scheduling, monkeypatch):
        """The dispatch path reuses its own timestamp for the trigger-finished log line."""
        monkeypatch.setattr(index_scheduling, "flag_modified", lambda *a, **k: None)
        toolkit = _toolkit()
        stamp = "2026-06-01T12:00:00+00:00"
        assert index_scheduling.stamp_schedule_last_run(
            FakeSession(), toolkit, "docs", "7", "[ctx]", when=stamp) == stamp
        assert toolkit.meta["indexes_meta"]["docs"]["schedules"]["7"]["last_run"] == stamp


class TestTickWiring:
    """The helper and the contract are useless if the tick does not call them. Read the
    scheduler's source: the fix is which blocks stamp and which deliberately do not."""

    @pytest.fixture(scope="class")
    def tick_source(self):
        return (PLUGIN_ROOT / "rpc" / "index_scheduling.py").read_text()

    @pytest.fixture(scope="class")
    def tick_tree(self, tick_source):
        return ast.parse(tick_source)

    def test_the_credential_failure_block_stamps_only_when_a_failure_was_recorded(
            self, tick_tree):
        """Path E - the reported runaway. The stamp must sit behind the outcome test, or
        contention loses its one-minute retry."""
        for node in ast.walk(tick_tree):
            if not isinstance(node, ast.If):
                continue
            if not (isinstance(node.test, ast.Name) and node.test.id == "init_issue"):
                continue
            body = ast.dump(ast.Module(body=node.body, type_ignores=[]))
            assert "handle_failed_index_schedule" in body
            assert "stamp_schedule_last_run" in body

            # The stamp must never sit unguarded in the block: contention (recorded=False)
            # would then consume the cron slot and lose its one-minute retry.
            for stmt in node.body:
                assert "stamp_schedule_last_run" not in ast.dump(stmt) \
                    or isinstance(stmt, ast.If), \
                    "the stamp must be guarded, not a bare statement"

            # The conditional *directly* wrapping the call must test `recorded`. Matching
            # on any ancestor instead would be satisfied by an unrelated `if recorded:`
            # somewhere else in the block while the stamp itself sat unguarded.
            def _directly_stamps(if_node):
                return any(
                    isinstance(st, ast.Expr) and isinstance(st.value, ast.Call)
                    and getattr(st.value.func, "id", None) == "stamp_schedule_last_run"
                    for st in if_node.body)

            wrappers = [n for n in ast.walk(node)
                        if isinstance(n, ast.If) and _directly_stamps(n)]
            assert wrappers, "no conditional directly wraps the stamp"
            assert all("recorded" in ast.dump(n.test) for n in wrappers), \
                "the stamp must be gated on the recorded outcome"
            return
        pytest.fail("could not find the `if init_issue:` block")

    def test_the_missing_index_row_block_stamps(self, tick_tree):
        """Path H - the same physical condition as the missing-metadata outcome, reached by
        a read instead of a write; it must not be left looping."""
        for node in ast.walk(tick_tree):
            if isinstance(node, ast.If) and isinstance(node.test, ast.UnaryOp) \
                    and isinstance(node.test.op, ast.Not) \
                    and isinstance(node.test.operand, ast.Name) \
                    and node.test.operand.id == "index":
                assert "stamp_schedule_last_run" in ast.dump(
                    ast.Module(body=node.body, type_ignores=[]))
                return
        pytest.fail("could not find the `if not index:` block")

    def test_contention_and_transient_paths_do_not_stamp(self, tick_source):
        """The paths that must keep the 60s retry: a live-but-fresh run, the settings
        resolution exception, the missing connection string, and the dispatch catch-all
        (which swallows PoolSaturationError and IndexRunInProgressError). Exactly three
        call sites: credential failure, missing index row, successful dispatch."""
        assert tick_source.count("stamp_schedule_last_run(") == 3


    def _init_issue_block(self, tick_tree):
        for node in ast.walk(tick_tree):
            if isinstance(node, ast.If) and isinstance(node.test, ast.Name) \
                    and node.test.id == "init_issue":
                return node
        pytest.fail("could not find the `if init_issue:` block")

    def _retryable_branch(self, tick_tree):
        block = self._init_issue_block(tick_tree)
        branches = [n for n in ast.walk(block) if isinstance(n, ast.If)
                    and "init_issue_retryable" in ast.dump(n.test)]
        assert branches, "no branch keys on init_issue_retryable"
        return branches[0]

    def test_a_transient_lookup_failure_reports_nothing_at_all(self, tick_tree):
        """The retryable branch must REPORT nothing, not merely skip the stamp.

        It is no longer inert: it records when the outage began so a later tick can tell an
        outage from a blip. Recording is not reporting, and the assertions below name the
        two calls that would be.

        Skipping only the stamp is the #6583 runaway through a narrower door: the failure
        would still be flipped onto the index row, appended to its unbounded history and
        notified to the owner on every 60s tick for as long as the configurations RPC is
        down — while the un-advanced cursor guarantees the tick repeats. The lock and
        live-run exits avoid this by returning *before* notifying, and this branch has to
        do the same by not calling the handler at all.
        """
        block = self._init_issue_block(tick_tree)
        branch = self._retryable_branch(tick_tree)

        # Not enough to check the branch body: hoisting the handler ABOVE the branch leaves
        # the branch itself clean while every retryable tick still notifies. So require that
        # nothing reporting a failure runs before the retryable test is made.
        for stmt in block.body:
            if stmt is branch:
                break
            assert "handle_failed_index_schedule" not in ast.dump(stmt), \
                "the retryable test must come BEFORE anything that reports the failure"
            assert "stamp_schedule_last_run" not in ast.dump(stmt), \
                "the retryable test must come BEFORE anything that moves the cursor"
        else:
            pytest.fail("the retryable branch is not a statement of the init_issue block")

        body = ast.dump(ast.Module(body=branch.body, type_ignores=[]))
        assert "handle_failed_index_schedule" not in body, \
            "a retryable failure must not notify or append history"
        assert "stamp_schedule_last_run" not in body, \
            "a retryable failure must not consume the cron slot"

    def test_a_terminal_failure_still_reports_and_stamps(self, tick_tree):
        """The other half of the same branch: everything not retryable must still be
        recorded and must still consume the slot, or #6583 is simply un-fixed."""
        branch = self._retryable_branch(tick_tree)
        orelse = ast.dump(ast.Module(body=branch.orelse, type_ignores=[]))
        assert "handle_failed_index_schedule" in orelse
        assert "stamp_schedule_last_run" in orelse


    def test_the_dispatch_path_goes_through_the_same_helper(self, tick_source):
        """One writer for the cursor, so delete-wins and the refresh() rebinding cannot
        drift apart between the success and failure paths."""
        assert "['last_run'] = " not in tick_source
        assert "flag_modified" not in tick_source


class TestRetryableCredentialLookup:
    """A failed *lookup* is not a failed credential.

    `resolve_credentials` collapses any exception from the configurations RPCs (each on a
    3s timeout) into a failure string. Consuming the cron slot for that would mean a daily
    schedule loses a whole day to a three-second blip, where before it self-healed on the
    next 60s scan. Only that one exit is retryable; everything else names a property of the
    stored schedule or the credential catalogue and cannot fix itself.
    """

    _DEFAULT = object()

    def _resolve(self, index_scheduling, monkeypatch, *, rpc,
                 settings=_DEFAULT, creds=_DEFAULT):
        # resolve_credentials binds rpc_tools at import (`from tools import ... rpc_tools`),
        # so the module attribute is what has to be replaced, not tools.rpc_tools.
        monkeypatch.setattr(
            index_scheduling, "rpc_tools",
            types.SimpleNamespace(RpcMixin=lambda: types.SimpleNamespace(
                rpc=types.SimpleNamespace(timeout=lambda t: rpc))))
        if settings is self._DEFAULT:
            settings = {"confluence_configuration": {"elitea_title": "x"}}
        if creds is self._DEFAULT:
            creds = {"private": False, "elitea_title": "cred"}
        return index_scheduling.resolve_credentials(
            project_settings=settings,
            toolkit_type="confluence",
            user_config={"credentials": creds},
            project_id=1, creator_id=3, toolkit_id=42, index_name="docs", user_id="3",
        )

    def test_a_raising_lookup_is_retryable(self, index_scheduling, monkeypatch):
        def _boom(**kw):
            raise TimeoutError("configurations RPC timed out")
        ok, issue, retryable = self._resolve(
            index_scheduling, monkeypatch,
            rpc=types.SimpleNamespace(configurations_get_first_filtered_project=_boom))
        assert ok is False
        assert "could not look up credential" in issue
        assert retryable is True

    def test_a_credential_that_does_not_exist_is_not_retryable(self, index_scheduling,
                                                               monkeypatch):
        """The lookup succeeded and said no. Retrying in a minute cannot change that."""
        ok, issue, retryable = self._resolve(
            index_scheduling, monkeypatch,
            rpc=types.SimpleNamespace(
                configurations_get_first_filtered_project=lambda **kw: None))
        assert (ok, retryable) == (False, False)
        assert "no longer exists" in issue

    @pytest.mark.parametrize("settings,creds,fragment", [
        ({"confluence_configuration": {}}, None, "no credentials selected"),
        ({"confluence_configuration": {}}, "not-a-dict", "malformed"),
        ({"confluence_configuration": {}}, {"private": False}, "do not name a credential"),
    ])
    def test_schedule_shaped_failures_are_not_retryable(self, index_scheduling, monkeypatch,
                                                        settings, creds, fragment):
        ok, issue, retryable = self._resolve(
            index_scheduling, monkeypatch, rpc=types.SimpleNamespace(),
            settings=settings, creds=creds)
        assert (ok, retryable) == (False, False)
        assert fragment in issue

    def test_success_is_not_retryable(self, index_scheduling, monkeypatch):
        ok, issue, retryable = self._resolve(
            index_scheduling, monkeypatch,
            rpc=types.SimpleNamespace(
                configurations_get_first_filtered_project=lambda **kw: {"id": 1}))
        assert (ok, issue, retryable) == (True, None, False)
