"""EL-7011 US-08 R1.3, R1.4, R3.1 to R3.5: the tick RPC with its gates, flags and cap.

Contract under test: rpc/entity_schedules_check.py. The claim, due query and starter are faked,
so the test pins what the tick decides, not how Postgres answers.
"""
import contextlib
import sys
import types
from datetime import UTC, datetime

import pytest
from unit._tool_spend_loader import load

NOW = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
NEXT = datetime(2026, 1, 1, 13, 0, tzinfo=UTC)


class FakeSession:
    def __init__(self, project_id):
        self.project_id, self.commits, self.rollbacks = project_id, 0, 0

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


def _row(row_id, entity_type="pipeline"):
    return types.SimpleNamespace(
        id=row_id, entity_type=entity_type, cron="* * * * *", timezone="UTC", next_run=NOW,
    )


class World:
    """The fakes one tick talks to, and the calls it made on them."""

    def __init__(self):
        self.projects, self.rows = [1], {}  # project id -> due rows
        self.config = {"agent_scheduling_enabled": True, "dispatch_cap": 100}
        self.maintenance, self.fail_claim, self.lost, self.conflict = False, set(), set(), set()
        self.fail_projects, self.listed = set(), False
        self.selects, self.claims, self.started, self.sessions = [], [], [], []

    def select_due(self, session, entity_types, limit):
        self.selects.append((session.project_id, list(entity_types), limit))
        return [r for r in self.rows.get(session.project_id, []) if r.entity_type in entity_types][:limit]

    def claim(self, session, schedule_id, seen, new):
        if schedule_id in self.fail_claim:
            raise RuntimeError("db down")
        self.claims.append((schedule_id, seen, new))
        return schedule_id not in self.lost

    def insert_run(self, session, schedule_id):
        return None if schedule_id in self.conflict else schedule_id * 10

    def start(self, entity_type, project_id, schedule_id, run_id):
        self.started.append((project_id, schedule_id, run_id))
        return True

    @contextlib.contextmanager
    def get_session(self, project_id):
        if project_id in self.fail_projects:
            raise RuntimeError("no schema")
        session = FakeSession(project_id)
        self.sessions.append(session)
        yield session

    def project_list(self, **_):
        self.listed = True
        return [{"id": p} for p in self.projects]


@pytest.fixture
def world(monkeypatch):
    world = World()
    stubs = {
        "utils.maintenance_gate": {"is_maintenance_active": lambda: world.maintenance},
        "utils.utils": {"make_yield_to_hub": lambda _runtime: (lambda: None), "end_ambient_transaction": lambda: None},
        "utils.schedule_claim": {
            "select_due_schedules": world.select_due, "claim_schedule": world.claim,
            "insert_cron_run": world.insert_run,
        },
        "utils.schedule_dispatch": {
            "RUN_STARTERS": {"agent": print, "pipeline": print}, "start_schedule_run": world.start,
        },
        "utils.schedule_timing": {"compute_next_run": lambda cron, zone, after: NEXT},
    }
    for dotted, names in stubs.items():
        module = types.ModuleType("plugins.elitea_core." + dotted)
        vars(module).update(names)
        monkeypatch.setitem(sys.modules, module.__name__, module)
    # Other suites leave a narrower pylon stub behind, so this one brings the decorator it needs
    pylon_tools = types.ModuleType("pylon.core.tools")
    pylon_tools.web = types.SimpleNamespace(rpc=lambda *a, **k: (lambda f: f))
    pylon_tools.log = types.SimpleNamespace(info=print, exception=print)
    monkeypatch.setitem(sys.modules, "pylon.core.tools", pylon_tools)
    monkeypatch.setitem(sys.modules, "plugins.elitea_core.rpc.entity_schedules_check", None)
    tick = load("rpc/entity_schedules_check.py", fresh=True)
    monkeypatch.setattr(tick, "db", types.SimpleNamespace(get_session=world.get_session))
    rpc = types.SimpleNamespace(timeout=lambda _n: types.SimpleNamespace(project_list=world.project_list))
    monkeypatch.setattr(tick, "rpc_tools", types.SimpleNamespace(RpcMixin=lambda: types.SimpleNamespace(rpc=rpc)))
    descriptor = types.SimpleNamespace(config={"scheduler": {"schedules": world.config}})
    monkeypatch.setattr(tick, "this", types.SimpleNamespace(module=types.SimpleNamespace(descriptor=descriptor)))
    world.tick = lambda: tick.RPC().entity_schedules_check.__func__(
        types.SimpleNamespace(context=types.SimpleNamespace(web_runtime="flask")))
    world.starters = sys.modules["plugins.elitea_core.utils.schedule_dispatch"].RUN_STARTERS
    return world


def test_7011_r3_3_maintenance_skips_the_whole_tick(world):
    world.maintenance, world.rows = True, {1: [_row(1)]}
    world.tick()
    assert not world.listed and not world.selects and not world.started


def test_7011_r3_1_agent_flag_off_leaves_agent_rows_unclaimed(world):
    world.config["agent_scheduling_enabled"] = False
    world.rows = {1: [_row(1, "agent"), _row(2, "pipeline")]}
    world.tick()
    assert [claim[0] for claim in world.claims] == [2]
    assert world.selects[0][1] == ["pipeline"]


def test_7011_r3_2_agent_flag_off_still_starts_pipeline_rows(world):
    world.config["agent_scheduling_enabled"] = False
    world.rows = {1: [_row(2, "pipeline")]}
    world.tick()
    assert world.started == [(1, 2, 20)]


def test_7011_r3_1_agent_flag_on_selects_both_types(world):
    world.rows = {1: [_row(1, "agent"), _row(2, "pipeline")]}
    world.tick()
    assert sorted(world.selects[0][1]) == ["agent", "pipeline"]
    assert [started[1] for started in world.started] == [1, 2]


def test_7011_r3_5_type_without_starter_is_not_selected(world):
    del world.starters["agent"]
    world.rows = {1: [_row(1, "agent"), _row(2, "pipeline")]}
    world.tick()
    assert world.selects[0][1] == ["pipeline"]
    assert [claim[0] for claim in world.claims] == [2]


def test_7011_r3_5_no_starter_at_all_reads_no_rows(world):
    world.starters.clear()
    world.rows = {1: [_row(1)]}
    world.tick()
    assert not world.selects and not world.claims


def test_7011_r1_3_cap_counts_over_all_projects(world):
    world.config["dispatch_cap"] = 3
    world.projects = [1, 2, 3]
    world.rows = {1: [_row(1), _row(2)], 2: [_row(3), _row(4)], 3: [_row(5)]}
    world.tick()
    assert [started[1] for started in world.started] == [1, 2, 3]
    assert [(project, limit) for project, _, limit in world.selects] == [(1, 3), (2, 1)]


def test_7011_r1_3_leftover_rows_keep_their_next_run(world):
    world.config["dispatch_cap"] = 1
    world.rows = {1: [_row(1), _row(2)]}
    world.tick()
    assert [claim[0] for claim in world.claims] == [1]


def test_7011_r1_4_a_failing_row_does_not_stop_the_next_one(world):
    world.fail_claim = {1}
    world.rows = {1: [_row(1), _row(2)]}
    world.tick()
    assert [started[1] for started in world.started] == [2]
    assert world.sessions[0].rollbacks == 1


def test_7011_r1_4_a_failing_project_does_not_stop_the_next_one(world):
    world.projects, world.fail_projects = [1, 2], {1}
    world.rows = {2: [_row(3)]}
    world.tick()
    assert world.started == [(2, 3, 30)]


def test_7011_r1_4_project_list_failure_ends_the_tick_quietly(world, monkeypatch):
    def broken(**_):
        raise TimeoutError

    monkeypatch.setattr(world, "project_list", broken)
    assert world.tick() is None
    assert not world.selects


def test_7011_r1_3_claim_moves_next_run_and_commits_before_the_start(world):
    world.rows = {1: [_row(1)]}
    world.tick()
    assert world.claims == [(1, NOW, NEXT)]
    assert world.sessions[0].commits == 1


def test_7011_r1_3_lost_claim_and_active_run_start_nothing(world):
    world.lost, world.conflict = {1}, {2}
    world.rows = {1: [_row(1), _row(2), _row(3)]}
    world.tick()
    assert world.started == [(1, 3, 30)]
