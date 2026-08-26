"""Issue #6389 - the reclaim sweep's own logic, over injectable fakes.

The predicate and the guarded write are covered in test_6389_index_reclaim.py; this
covers rpc/index_reclaim.py, which decides how much work a tick does and in what order.
Nothing here touches a database or a task network - every collaborator is replaced on
the loaded module.

Run via:
    python tests/run_tests.py integration/test_6389_index_reclaim_sweep.py -v
"""

import importlib.util
import pathlib
import sys
import time
import types

import pytest


PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]


class _FakeTaskNode:
    """Stands in for the arbiter node: a status map plus the shared state cache."""

    def __init__(self, statuses=None, cached=None, query_wait=5, raises=None):
        self.query_wait = query_wait
        self.lock = _NullLock()
        self.global_task_state = dict(cached or {})
        self._statuses = statuses or {}
        self._raises = raises or {}
        self.queried = []

    def get_task_status(self, task_id):
        self.queried.append(task_id)
        if task_id in self._raises:
            raise self._raises[task_id]
        if task_id not in self._statuses:
            raise RuntimeError("Unknown task")
        return self._statuses[task_id]


class _NullLock:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


@pytest.fixture
def reclaim():
    """Load rpc/index_reclaim.py with its pylon and plugin imports stubbed."""
    for name in ("plugins", "plugins.elitea_core", "plugins.elitea_core.models",
                 "plugins.elitea_core.utils", "plugins.elitea_core.rpc"):
        mod = sys.modules.setdefault(name, types.ModuleType(name))
        mod.__path__ = []

    pylon_tools = types.ModuleType("pylon.core.tools")
    pylon_tools.log = types.SimpleNamespace(
        info=lambda *a, **k: None, warning=lambda *a, **k: None,
        error=lambda *a, **k: None, debug=lambda *a, **k: None,
        exception=lambda *a, **k: None,
    )
    pylon_tools.web = types.SimpleNamespace(rpc=lambda *a, **k: (lambda fn: fn))
    sys.modules.setdefault("pylon", types.ModuleType("pylon"))
    sys.modules.setdefault("pylon.core", types.ModuleType("pylon.core"))
    sys.modules["pylon.core.tools"] = pylon_tools

    tools_pkg = types.ModuleType("tools")
    tools_pkg.db = types.SimpleNamespace(get_session=lambda pid: None)
    tools_pkg.rpc_tools = types.SimpleNamespace()
    sys.modules["tools"] = tools_pkg

    for name, attrs in (
        ("plugins.elitea_core.models.elitea_tools", {"EliteATool": type("EliteATool", (), {})}),
        ("plugins.elitea_core.models.indexer", {"EmbeddingStore": type("EmbeddingStore", (), {})}),
        ("plugins.elitea_core.models.enums.all", {
            "IndexDataStatus": type("IndexDataStatus", (), {
                "in_progress": types.SimpleNamespace(value="in_progress"),
            }),
        }),
        ("plugins.elitea_core.utils.application_tools", {
            "RECLAIM_HARD_CEILING_FACTOR": 3,
            "TASK_LOST": "task_lost",
            "UNTRACKED_RECLAIM_AGE_FACTOR": 2,
            "_expand_toolkit_settings": lambda *a, **k: {},
            "_get_pgvector_engine": lambda conn: None,
            "get_session_for_schema": lambda conn, schema: None,
            "read_task_disconnected_timeout": lambda pid: 7200,
            "reclaim_toolkit_index_meta": lambda *a, **k: True,
            "should_reclaim_index_meta": lambda *a, **k: True,
        }),
        ("plugins.elitea_core.utils.utils", {
            "make_yield_to_hub": lambda runtime: (lambda: None),
            "end_ambient_transaction": lambda: None,
        }),
        ("plugins.elitea_core.utils.maintenance_gate", {"is_maintenance_active": lambda: False}),
    ):
        module = types.ModuleType(name)
        for key, value in attrs.items():
            setattr(module, key, value)
        sys.modules[name] = module

    spec = importlib.util.spec_from_file_location(
        "plugins.elitea_core.rpc.index_reclaim", PLUGIN_ROOT / "rpc" / "index_reclaim.py",
    )
    loaded = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = loaded
    spec.loader.exec_module(loaded)
    yield loaded


TIMEOUT = 7200


def _candidate(collection="idx", task_id="task-1", age=TIMEOUT * 1.5, now=None, **over):
    # Anchored to the real clock: the sweep partitions candidates against time.time().
    now = time.time() if now is None else now
    candidate = {
        "project_id": 2,
        "toolkit_id": 56,
        "connection_string": "postgresql://db",
        "schema": "56",
        "cmetadata": {
            "collection": collection,
            "task_id": task_id,
            "created_on": now - age - 60,
            "updated_on": now - age,
        },
        "probed_task_id": task_id,
        "timeout": TIMEOUT,
    }
    candidate.update(over)
    return candidate


class TestProbeBudget:

    def test_derives_the_count_from_the_node_s_own_query_wait(self, reclaim):
        # Hard-coding the cost would drift silently the moment the node is reconfigured.
        slow = reclaim._max_confirms_per_tick(_FakeTaskNode(query_wait=5))
        fast = reclaim._max_confirms_per_tick(_FakeTaskNode(query_wait=1))

        assert slow == (reclaim.PROBE_BUDGET_SEC - reclaim.LIVENESS_CONFIRM_DELAY_SEC) // 10
        assert fast > slow

    def test_always_allows_at_least_one(self, reclaim):
        assert reclaim._max_confirms_per_tick(_FakeTaskNode(query_wait=10_000)) == 1

    def test_survives_a_node_without_the_attribute(self, reclaim):
        assert reclaim._max_confirms_per_tick(types.SimpleNamespace()) >= 1


class TestLivenessProbe:

    def test_evicts_the_cached_entry_before_asking(self, reclaim):
        # Without the eviction get_task_status answers from our own stale copy, and a
        # dead run's 'running' is never retracted.
        node = _FakeTaskNode(statuses={"task-1": "running"}, cached={"task-1": {"status": "running"}})

        assert reclaim._resolve_task_liveness(node, "task-1", {}) == "running"
        assert node.queried == ["task-1"]

    def test_reports_a_task_nobody_owns_as_lost(self, reclaim):
        node = _FakeTaskNode()

        assert reclaim._resolve_task_liveness(node, "gone", {}) == reclaim.TASK_LOST

    def test_restores_the_shared_entry_when_no_fresh_answer_arrives(self, reclaim):
        # The cache is read by the task views; a background probe must not cost them
        # an entry just because the network went quiet.
        cached = {"status": "running"}
        node = _FakeTaskNode(cached={"task-1": cached})

        assert reclaim._resolve_task_liveness(node, "task-1", {}) == reclaim.TASK_LOST
        assert node.global_task_state["task-1"] == cached

    def test_restores_it_on_a_transport_error_too(self, reclaim):
        cached = {"status": "running"}
        node = _FakeTaskNode(cached={"task-1": cached}, raises={"task-1": OSError("bus down")})

        assert reclaim._resolve_task_liveness(node, "task-1", {}) == "unknown"
        assert node.global_task_state["task-1"] == cached

    def test_asks_once_per_task_per_pass(self, reclaim):
        node = _FakeTaskNode(statuses={"task-1": "running"})
        verdicts = {}

        reclaim._resolve_task_liveness(node, "task-1", verdicts)
        reclaim._resolve_task_liveness(node, "task-1", verdicts)

        assert node.queried == ["task-1"]


class TestRegistryLookup:

    def _module(self, entries):
        return types.SimpleNamespace(
            active_index_tasks=entries, active_index_tasks_lock=_NullLock(),
        )

    def test_matches_across_int_and_str_drift(self, reclaim):
        # Registry keys come from event payloads, the sweep's ids from SQLAlchemy.
        module = self._module({"task-9": {("2", 56, "idx"): {}}})

        assert reclaim._find_registered_task(module, 2, "56", "idx") == "task-9"

    def test_reports_nothing_for_an_unknown_run(self, reclaim):
        module = self._module({"task-9": {(2, 56, "other"): {}}})

        assert reclaim._find_registered_task(module, 2, 56, "idx") is None

    def test_forgetting_drops_the_task_once_its_last_entry_goes(self, reclaim):
        module = self._module({"task-9": {(2, 56, "idx"): {}}})

        reclaim._forget_registered_task(module, 2, 56, "idx")

        assert module.active_index_tasks == {}

    def test_forgetting_keeps_a_task_that_still_has_other_indexes(self, reclaim):
        module = self._module({"task-9": {(2, 56, "idx"): {}, (2, 56, "other"): {}}})

        reclaim._forget_registered_task(module, 2, 56, "idx")

        assert list(module.active_index_tasks["task-9"]) == [(2, 56, "other")]


class TestAbandonedAt:

    def test_reads_the_last_progress_timestamp(self, reclaim):
        assert reclaim._read_abandoned_at(_candidate(age=100, now=500)) == 400

    def test_coerces_a_value_that_arrived_as_a_string(self, reclaim):
        # A mixed-type sort would raise out of the RPC and cost the schedule its
        # last_run update, silently promoting the sweep to every poll period.
        candidate = _candidate()
        candidate["cmetadata"]["updated_on"] = "1234.5"

        assert reclaim._read_abandoned_at(candidate) == 1234.5

    def test_sorts_unusable_values_first_rather_than_raising(self, reclaim):
        for value in (None, "", "garbage", {}):
            candidate = _candidate()
            candidate["cmetadata"]["updated_on"] = value
            assert reclaim._read_abandoned_at(candidate) == 0.0


class TestPgvectorCredential:

    def test_refuses_an_inline_connection_string(self, reclaim, monkeypatch):
        # Honouring one would let whoever wrote the toolkit choose where this sweep
        # connects; it must go through credential expansion like everything else.
        seen = []
        monkeypatch.setattr(reclaim, '_expand_toolkit_settings',
                            lambda ref, *a: seen.append(ref) or {})
        ref = {'connection_string': 'postgresql://attacker/db'}

        assert reclaim._resolve_pgvector_connection(2, ref, 3, {}) is None
        assert seen == [ref]

    def test_refuses_a_credential_of_another_type(self, reclaim, monkeypatch):
        monkeypatch.setattr(reclaim, '_expand_toolkit_settings', lambda *a: {
            'configuration_type': 'postgres', 'connection_string': 'postgresql://db',
        })

        assert reclaim._resolve_pgvector_connection(2, {'elitea_title': 'x'}, 3, {}) is None

    def test_expands_once_per_reference_including_failures(self, reclaim, monkeypatch):
        calls = []

        def boom(*a):
            calls.append(a)
            raise RuntimeError("vault down")

        monkeypatch.setattr(reclaim, '_expand_toolkit_settings', boom)
        memo = {}
        ref = {'elitea_title': 'shared', 'private': False}

        assert reclaim._resolve_pgvector_connection(2, ref, 3, memo) is None
        assert reclaim._resolve_pgvector_connection(2, ref, 3, memo) is None
        assert len(calls) == 1


def _drive_sweep(reclaim, monkeypatch, candidates, *, confirms=None, max_confirms=46,
                 statuses=None):
    """Run a whole tick with every collaborator replaced.

    `confirms` maps a collection name to the verdicts _confirm_dead should return, one
    per pass; anything absent confirms on both.
    """
    confirmed_calls = []
    reclaimed = []
    slept = []

    def fake_confirm(module, candidate, reclaim_untracked, ceiling, verdicts):
        collection = candidate['cmetadata']['collection']
        confirmed_calls.append(collection)
        verdicts_for = (confirms or {}).get(collection)
        if verdicts_for is None:
            return True
        return verdicts_for.pop(0) if verdicts_for else True

    monkeypatch.setattr(reclaim, '_collect_project_candidates',
                        lambda *a, **k: list(candidates))
    monkeypatch.setattr(reclaim, '_confirm_dead', fake_confirm)
    monkeypatch.setattr(reclaim, '_max_confirms_per_tick', lambda node: max_confirms)
    monkeypatch.setattr(reclaim, '_forget_registered_task', lambda *a, **k: None)
    monkeypatch.setattr(reclaim, 'reclaim_toolkit_index_meta',
                        lambda conn, schema, name, **kwargs: reclaimed.append((name, kwargs)) or True)
    monkeypatch.setattr(reclaim.time, 'sleep', lambda seconds: slept.append(seconds))
    monkeypatch.setattr(reclaim.rpc_tools, 'RpcMixin', lambda: types.SimpleNamespace(
        rpc=types.SimpleNamespace(timeout=lambda _t: types.SimpleNamespace(
            project_list=lambda **kw: [{'id': 2}],
        )),
    ), raising=False)

    module = types.SimpleNamespace(
        descriptor=types.SimpleNamespace(config={'scheduler': {'index_reclaim': {}}}),
        context=types.SimpleNamespace(web_runtime='flask'),
        task_node=_FakeTaskNode(statuses=statuses),
    )
    reclaim._sweep(module)
    return {'confirmed': confirmed_calls, 'reclaimed': reclaimed, 'slept': slept}


class TestTickBudget:

    def test_a_ceiling_expired_candidate_is_never_capped(self, reclaim, monkeypatch):
        # Past the ceiling no probe is paid, so capping those would ration free work —
        # and a mass restart ages every abandoned run past the ceiling together.
        past_ceiling = [
            _candidate(collection=f"old-{i}", age=TIMEOUT * 10) for i in range(20)
        ]
        result = _drive_sweep(reclaim, monkeypatch, past_ceiling, max_confirms=2)

        assert len(result['reclaimed']) == 20

    def test_probe_bound_candidates_are_capped_oldest_first(self, reclaim, monkeypatch):
        within_ceiling = [
            _candidate(collection=f"new-{i}", age=TIMEOUT * (1.1 + i * 0.1)) for i in range(5)
        ]
        result = _drive_sweep(reclaim, monkeypatch, list(reversed(within_ceiling)), max_confirms=2)

        # Oldest is the largest age, so the last two built.
        assert [name for name, _ in result['reclaimed']] == ['new-4', 'new-3']

    def test_free_and_probed_candidates_share_a_tick(self, reclaim, monkeypatch):
        mixed = [
            _candidate(collection='free', age=TIMEOUT * 10),
            _candidate(collection='probed', age=TIMEOUT * 1.2),
        ]
        result = _drive_sweep(reclaim, monkeypatch, mixed, max_confirms=1)

        assert sorted(name for name, _ in result['reclaimed']) == ['free', 'probed']


class TestTwoPassConfirm:

    def test_a_candidate_must_survive_both_passes(self, reclaim, monkeypatch):
        result = _drive_sweep(
            reclaim, monkeypatch, [_candidate(collection='flaky')],
            confirms={'flaky': [True, False]},
        )

        assert result['confirmed'] == ['flaky', 'flaky']
        assert result['reclaimed'] == []

    def test_the_confirm_delay_is_paid_once_per_tick(self, reclaim, monkeypatch):
        result = _drive_sweep(reclaim, monkeypatch, [
            _candidate(collection='a', age=TIMEOUT * 1.2),
            _candidate(collection='b', age=TIMEOUT * 1.3),
        ])

        assert result['slept'] == [reclaim.LIVENESS_CONFIRM_DELAY_SEC]

    def test_nothing_sleeps_when_the_first_pass_clears_everything(self, reclaim, monkeypatch):
        result = _drive_sweep(
            reclaim, monkeypatch, [_candidate(collection='alive', age=TIMEOUT * 1.2)],
            confirms={'alive': [False]},
        )

        assert result['slept'] == []
        assert result['reclaimed'] == []


class TestReclaimWrite:

    def test_a_tracked_row_must_be_stale_by_one_timeout(self, reclaim, monkeypatch):
        result = _drive_sweep(reclaim, monkeypatch, [_candidate(age=TIMEOUT * 10)])

        _, kwargs = result['reclaimed'][0]
        assert kwargs['min_updated_age'] == TIMEOUT

    def test_an_untracked_row_must_be_stale_by_two(self, reclaim, monkeypatch):
        untracked = _candidate(task_id=None, age=TIMEOUT * 10)
        result = _drive_sweep(reclaim, monkeypatch, [untracked])

        _, kwargs = result['reclaimed'][0]
        assert kwargs['min_updated_age'] == 2 * TIMEOUT

    def test_the_write_is_guarded_by_the_row_s_own_identity(self, reclaim, monkeypatch):
        result = _drive_sweep(reclaim, monkeypatch, [_candidate(age=TIMEOUT * 10)])

        _, kwargs = result['reclaimed'][0]
        assert kwargs['expected_task_id'] == 'task-1'
        assert 'expected_created_on' in kwargs


class TestDeferredRead:

    def test_does_not_produce_the_value_until_asked(self, reclaim):
        calls = []
        read = reclaim._memoize(lambda: calls.append(1) or 7200)

        assert calls == []
        assert read() == 7200
        assert read() == 7200
        assert calls == [1]
