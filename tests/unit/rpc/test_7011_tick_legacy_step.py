"""EL-7011 US-08 R4.1 to R4.3: the legacy pipeline check runs last, per project, under its advisory lock.

Contract under test: rpc/entity_schedules_check.py. The new-schedule steps find nothing due and the
lock connection is faked by key, so the test pins which projects the legacy step checks and when.
"""
import contextlib
import sys
import threading
import types

import pytest
from unit._tool_spend_loader import load


def _module(name, **attrs):
    module = types.ModuleType(name)
    vars(module).update(attrs)
    return module


class LockConnection:
    """One connection that answers pg_try_advisory_lock and pg_advisory_unlock by key."""

    def __init__(self, world):
        self.world, self.options = world, {}

    def execution_options(self, **options):
        self.options.update(options)
        return self

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, statement, params):
        key, sql = params["key"], str(statement)
        if "pg_try_advisory_lock" in sql:
            self.world.locks.append(("lock", key))
            return types.SimpleNamespace(scalar=lambda: key not in self.world.held)
        assert "pg_advisory_unlock" in sql
        self.world.locks.append(("unlock", key))
        return types.SimpleNamespace(scalar=lambda: True)


class QuerySession:
    """A project session whose version query returns every version it was given, unfiltered."""

    def __init__(self, project_id, versions):
        self.project_id, self.versions = project_id, versions

    def query(self, *_):
        return self

    def filter(self, *_):
        return self

    def all(self):
        return list(self.versions)

    def get(self, _id):
        return None

    def commit(self):
        pass

    def rollback(self):
        pass


class World:
    def __init__(self):
        self.projects, self.held, self.fail_legacy = [1, 2], set(), set()
        self.events, self.locks, self.versions, self.executed = [], [], [], []
        self.connection = LockConnection(self)
        self.legacy_lock = threading.Lock()

    def select_due(self, session, entity_types, limit):
        self.events.append(("select", session.project_id))
        return []

    def check_project(self, project_id, yield_to_hub):
        self.events.append(("legacy", project_id))
        if project_id in self.fail_legacy:
            raise RuntimeError("bad trigger")

    @contextlib.contextmanager
    def get_session(self, project_id):
        yield QuerySession(project_id, self.versions)

    def project_list(self, **_):
        return [{"id": p} for p in self.projects]


@pytest.fixture
def world(monkeypatch):
    world = World()
    quiet = lambda *a, **k: None
    stubs = {
        "utils.maintenance_gate": {"is_maintenance_active": lambda: False},
        "utils.utils": {"make_yield_to_hub": lambda _runtime: quiet, "end_ambient_transaction": quiet},
        "utils.schedule_claim": {
            "select_due_schedules": world.select_due, "claim_schedule": None, "insert_cron_run": None,
            "legacy_lock_key": lambda project_id: 1000 + project_id, "fail_run": None,
        },
        "utils.schedule_dispatch": {
            "RUN_STARTERS": {"pipeline": print}, "start_schedule_run": None, "START_FAILED": "",
        },
        "utils.schedule_timing": {"compute_next_run": None},
        "rpc.pipeline_scheduling": {
            "_check_project_pipelines": world.check_project, "_check_pipeline_scheduling_lock": world.legacy_lock,
        },
    }
    for dotted, names in stubs.items():
        name = "plugins.elitea_core." + dotted
        monkeypatch.setitem(sys.modules, name, _module(name, **names))
    pylon_tools = _module("pylon.core.tools", web=types.SimpleNamespace(rpc=lambda *a, **k: (lambda f: f)),
                          log=types.SimpleNamespace(info=quiet, warning=quiet, exception=quiet))
    monkeypatch.setitem(sys.modules, "pylon.core.tools", pylon_tools)
    monkeypatch.setitem(sys.modules, "plugins.elitea_core.rpc.entity_schedules_check", None)
    tick = load("rpc/entity_schedules_check.py", fresh=True)
    world.db = types.SimpleNamespace(get_session=world.get_session, engine=types.SimpleNamespace(
        connect=lambda: world.connection))
    monkeypatch.setattr(tick, "db", world.db)
    rpc = types.SimpleNamespace(timeout=lambda _n: types.SimpleNamespace(project_list=world.project_list))
    monkeypatch.setattr(tick, "rpc_tools", types.SimpleNamespace(RpcMixin=lambda: types.SimpleNamespace(rpc=rpc)))
    settings = {"scheduler": {"schedules": {"dispatch_cap": 100}}}
    monkeypatch.setattr(tick, "this", types.SimpleNamespace(
        module=types.SimpleNamespace(descriptor=types.SimpleNamespace(config=settings))))
    world.settings = settings["scheduler"]["schedules"]
    world.key = tick.legacy_lock_key
    world.tick = lambda: tick.RPC().entity_schedules_check.__func__(
        types.SimpleNamespace(context=types.SimpleNamespace(web_runtime="flask")))
    return world


def test_7011_r4_1_legacy_check_runs_per_project_after_the_schedule_steps(world):
    world.tick()
    assert world.events == [("select", 1), ("select", 2), ("legacy", 1), ("legacy", 2)]
    assert not world.legacy_lock.locked()


def test_7011_r4_1_legacy_step_runs_when_the_cap_is_spent(world):
    world.settings["dispatch_cap"] = 0
    world.tick()
    assert world.events == [("legacy", 1), ("legacy", 2)]


def test_7011_r4_1_legacy_step_skips_while_a_previous_one_runs(world):
    world.legacy_lock.acquire()
    world.tick()
    assert [e for e in world.events if e[0] == "legacy"] == []
    assert world.locks == []


def test_7011_r4_2_each_project_lock_is_taken_and_freed_on_one_autocommit_connection(world):
    world.tick()
    k1, k2 = world.key(1), world.key(2)
    assert world.locks == [("lock", k1), ("unlock", k1), ("lock", k2), ("unlock", k2)]
    assert world.connection.options == {"isolation_level": "AUTOCOMMIT"}
    assert k1 != k2


def test_7011_r4_2_held_lock_skips_only_that_project(world):
    world.held = {world.key(1)}
    world.tick()
    assert [e for e in world.events if e[0] == "legacy"] == [("legacy", 2)]
    assert ("unlock", world.key(1)) not in world.locks


def test_7011_r4_2_lock_is_freed_when_the_legacy_check_fails(world):
    world.fail_legacy = {1}
    world.tick()
    assert ("unlock", world.key(1)) in world.locks
    assert ("legacy", 2) in world.events


@pytest.fixture
def real_legacy(world, monkeypatch):
    """The real rpc/pipeline_scheduling.py; only its leaves are faked."""
    column = types.SimpleNamespace(isnot=lambda _v: True)
    column.op = lambda _op: (lambda _arg: column)
    enum = lambda **values: types.SimpleNamespace(
        **{k: types.SimpleNamespace(value=v) for k, v in values.items()})
    leaves = {
        "models.all": {"Application": object, "ApplicationVersion": types.SimpleNamespace(
            agent_type=column, pipeline_settings=column)},
        "models.enums.all": {"AgentTypes": enum(pipeline="pipeline")},
        "models.pd.pipeline_trigger": {
            "TriggerType": enum(schedule="schedule"),
            "PipelineTriggerSchedule": types.SimpleNamespace(parse_obj=lambda t: types.SimpleNamespace(**t)),
        },
        "utils.cron_utils": {"is_cron_due": lambda cron, last_run, zone: True},
        "utils.pipeline_execution": {
            "TriggerType": types.SimpleNamespace(SCHEDULED="scheduled"),
            "create_trigger_run_conversation": lambda **_: ("uuid", 1, "message"),
            "execute_pipeline_via_predict_sio": lambda **kw: world.executed.append(kw["version"].id),
            "finish_trigger_run_with_error": None,
        },
    }
    for dotted, names in leaves.items():
        name = "plugins.elitea_core." + dotted
        monkeypatch.setitem(sys.modules, name, _module(name, **names))
    for name in ("plugins.elitea_core.models.enums", "plugins.elitea_core.models.pd"):
        monkeypatch.setitem(sys.modules, name, _module(name, __path__=[]))
    monkeypatch.setitem(sys.modules, "sqlalchemy.orm.attributes",
                        _module("sqlalchemy.orm.attributes", flag_modified=lambda *_: None))
    monkeypatch.setitem(sys.modules, "plugins.elitea_core.rpc.pipeline_scheduling", None)
    legacy = load("rpc/pipeline_scheduling.py", fresh=True)
    monkeypatch.setattr(legacy, "db", world.db)
    return world


def test_7011_r4_3_legacy_step_never_starts_a_migrated_version(real_legacy):
    real_legacy.projects = [34]
    old_due = {"cron": "*/5 * * * *", "timezone": "UTC", "last_run": "2026-01-01T00:00:00+00:00", "created_by": 3}
    real_legacy.versions = [
        types.SimpleNamespace(id=1, application_id=10, pipeline_settings={"trigger": {"type": "schedule", **old_due}}),
        # Migrated: the trigger was reset in the transaction that inserted its entity_schedules row
        types.SimpleNamespace(id=2, application_id=20, pipeline_settings={
            "trigger": {"type": "chat_message", **old_due}}),
    ]
    real_legacy.tick()
    assert real_legacy.executed == [1]
