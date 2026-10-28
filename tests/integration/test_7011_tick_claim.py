"""The schedule tick on a real PostgreSQL schema (EL-7011 US-08): claim, active-run rule and legacy lock.

Covers R1.1, R1.2, R2.1, R2.2, R4.1, R4.2 (R4.3 is pinned by the unit test of the legacy step).
The real models, claim functions and tick run against PostgreSQL; the plugin around them is stubbed.
Set ELITEA_7011_PG_DSN to a PostgreSQL database, for example inside the local stack container:

    ELITEA_7011_PG_DSN="postgresql://$POSTGRES_USER:$POSTGRES_PASSWORD@$POSTGRES_HOST:$POSTGRES_PORT/$POSTGRES_DB" \
        python3 -m pytest -q integration/test_7011_tick_claim.py

The test creates its own schema `t7011_tick_<pid>` and drops it afterwards; it touches nothing else.
"""
import contextlib
import importlib.util
import os
import pathlib
import sys
import threading
import types
from datetime import UTC, datetime, timedelta

import pytest
from integration.test_7011_entity_schedule_tables import REAL_SQLALCHEMY

DSN = os.environ.get("ELITEA_7011_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="ELITEA_7011_PG_DSN is not set")
SCHEMA = f"t7011_tick_{os.getpid()}"  # the models' `tenant` schema maps to this one

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
PROJECT_ID = 34
PACKAGE = "plugins.elitea_core"


def _module(name, **attrs):
    module = types.ModuleType(name)
    module.__path__ = []
    vars(module).update(attrs)
    return module


@pytest.fixture
def world(monkeypatch, request):
    """The tick over a fresh schema; `world.tick()` runs one call, `world.events` records the order."""
    for name, module in REAL_SQLALCHEMY.items():
        monkeypatch.setitem(sys.modules, name, module)
    sa, orm = REAL_SQLALCHEMY["sqlalchemy"], REAL_SQLALCHEMY["sqlalchemy.orm"]
    engine = sa.create_engine(DSN).execution_options(schema_translate_map={"tenant": SCHEMA})
    with engine.begin() as conn:
        conn.execute(sa.text(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE"))
        conn.execute(sa.text(f"CREATE SCHEMA {SCHEMA}"))

    def drop_schema():  # also after a failed set-up, so no schema is left behind
        with engine.begin() as conn:
            conn.execute(sa.text(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE"))
        engine.dispose()

    request.addfinalizer(drop_schema)

    class Base(orm.DeclarativeBase):
        pass

    for parent in ("applications", "application_versions"):  # the foreign keys need their parents
        sa.Table(parent, Base.metadata, sa.Column("id", sa.Integer, primary_key=True), schema="tenant")

    world = types.SimpleNamespace(engine=engine, events=[], starts=[], legacy=[])
    quiet = lambda *a, **k: None
    stubs = {
        "pylon.core.tools": _module(
            "pylon.core.tools", web=types.SimpleNamespace(rpc=lambda *a, **k: (lambda f: f)),
            log=types.SimpleNamespace(info=quiet, warning=quiet, exception=quiet)),
        PACKAGE: _module(PACKAGE),
        f"{PACKAGE}.utils": _module(f"{PACKAGE}.utils"),
        f"{PACKAGE}.rpc": _module(f"{PACKAGE}.rpc"),
        f"{PACKAGE}.models": _module(
            f"{PACKAGE}.models", db=types.SimpleNamespace(Base=Base),
            db_tools=types.SimpleNamespace(AbstractBaseMixin=type("Mixin", (), {})),
            config=types.SimpleNamespace(POSTGRES_TENANT_SCHEMA="tenant")),
        f"{PACKAGE}.utils.maintenance_gate": _module("m", is_maintenance_active=lambda: False),
        f"{PACKAGE}.utils.utils": _module("u", make_yield_to_hub=lambda _runtime: quiet,
                                         end_ambient_transaction=quiet),
        f"{PACKAGE}.rpc.pipeline_scheduling": _module(
            "p", _check_pipeline_scheduling_lock=threading.Lock(),
            _check_project_pipelines=lambda project_id, yield_to_hub: (
                world.events.append("legacy"), world.legacy.append(project_id))),
    }
    for name, module in stubs.items():
        monkeypatch.setitem(sys.modules, name, module)

    def load(relative):
        name = f"{PACKAGE}.{relative[:-3].replace('/', '.')}"
        spec = importlib.util.spec_from_file_location(name, PLUGIN_ROOT / relative)
        module = importlib.util.module_from_spec(spec)
        monkeypatch.setitem(sys.modules, name, module)
        spec.loader.exec_module(module)
        return module

    models = load("models/entity_schedule.py")
    load("utils/schedule_timing.py")
    claim = load("utils/schedule_claim.py")
    load("utils/schedule_dispatch.py")
    tick = load("rpc/entity_schedules_check.py")
    Base.metadata.create_all(engine)
    with engine.begin() as conn:
        conn.execute(sa.text(f"INSERT INTO {SCHEMA}.applications (id) VALUES (1)"))

    @contextlib.contextmanager
    def get_session(_project_id):
        with orm.Session(engine) as session:
            yield session

    def start_schedule_run(entity_type, project_id, schedule_id, run_id):
        with engine.connect() as conn:  # a second connection: only a committed run row is visible
            status = conn.execute(sa.text(
                f"SELECT status FROM {SCHEMA}.entity_schedule_runs WHERE id = :id"), {"id": run_id}).scalar()
        world.events.append("start")
        world.starts.append((schedule_id, run_id, status))
        return True

    monkeypatch.setitem(tick.RUN_STARTERS, "pipeline", print)
    monkeypatch.setattr(tick, "start_schedule_run", start_schedule_run)
    monkeypatch.setattr(tick, "db", types.SimpleNamespace(get_session=get_session, engine=engine))
    project_list = lambda **_: [{"id": PROJECT_ID}]
    monkeypatch.setattr(tick, "rpc_tools", types.SimpleNamespace(RpcMixin=lambda: types.SimpleNamespace(
        rpc=types.SimpleNamespace(timeout=lambda _n: types.SimpleNamespace(project_list=project_list)))))
    monkeypatch.setattr(tick, "this", types.SimpleNamespace(module=types.SimpleNamespace(
        descriptor=types.SimpleNamespace(config={}))))
    runtime = types.SimpleNamespace(context=types.SimpleNamespace(web_runtime="flask"))

    world.models, world.claim, world.sa, world.orm, world.tick_module = models, claim, sa, orm, tick
    world.tick = lambda: tick.RPC().entity_schedules_check.__func__(runtime)

    def add_schedule(**overrides):
        values = {
            "entity_type": "pipeline", "application_id": 1, "name": "nightly", "cron": "*/5 * * * *",
            "timezone": "UTC", "created_by": 7, "principal": {"kind": "mandate"},
            "next_run": datetime.now(UTC) - timedelta(minutes=10),
        }
        with orm.Session(engine) as session:
            row = models.EntitySchedule(**{**values, **overrides})
            session.add(row)
            session.commit()
            return row.id

    def add_run(schedule_id, status):
        with orm.Session(engine) as session:
            session.add(models.EntityScheduleRun(schedule_id=schedule_id, trigger="cron", status=status))
            session.commit()

    def runs(schedule_id):
        with orm.Session(engine) as session:
            return [(r.trigger, r.status) for r in session.scalars(
                sa.select(models.EntityScheduleRun).where(models.EntityScheduleRun.schedule_id == schedule_id)
                .order_by(models.EntityScheduleRun.id))]

    def schedule(schedule_id):
        with orm.Session(engine) as session:
            return session.get(models.EntitySchedule, schedule_id)

    world.add_schedule, world.add_run, world.runs, world.schedule = add_schedule, add_run, runs, schedule
    return world


def _both_see_the_same_due_row(world, monkeypatch, parties=2):
    """Hold each tick after its due query until all have read, so every one holds the same `next_run`."""
    barrier = threading.Barrier(parties, timeout=10)
    select_due = world.tick_module.select_due_schedules

    def select_then_wait(*args):
        rows = select_due(*args)
        barrier.wait()
        return rows

    monkeypatch.setattr(world.tick_module, "select_due_schedules", select_then_wait)


# TC-US08-01, TC-US08-03 / R1.1, R2.2: two parallel ticks on one due row give one run
def test_7011_r1_1_parallel_ticks_start_a_due_schedule_once(world, monkeypatch):
    schedule_id = world.add_schedule()
    _both_see_the_same_due_row(world, monkeypatch)
    errors = []

    def tick():
        try:
            world.tick()
        except Exception as exc:  # noqa: BLE001 -- the assert below names it
            errors.append(exc)

    threads = [threading.Thread(target=tick) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=20)
    assert not errors
    assert world.runs(schedule_id) == [("cron", "running")]
    assert [(s, status) for s, _run, status in world.starts] == [(schedule_id, "running")]


# US-14 R3.1 (review round 1): a schedule past its stop date is picked even when its next run is far away
def test_7011_us14_r3_1_expired_schedule_is_due_although_its_next_run_is_far(world):
    now = datetime.now(UTC)
    expired = world.add_schedule(next_run=now + timedelta(days=365), expires_at=now - timedelta(minutes=1))
    later = world.add_schedule(next_run=now + timedelta(days=365))
    with world.orm.Session(world.engine) as session:
        due = [row.id for row in world.claim.select_due_schedules(session, ["pipeline"], 10)]
    assert expired in due and later not in due


# TC-US08-03 / R1.1: a claim from a stale `next_run` changes nothing
def test_7011_r1_1_claim_from_a_stale_next_run_loses(world):
    schedule_id = world.add_schedule()
    seen = world.schedule(schedule_id).next_run
    later = seen + timedelta(minutes=30)
    with world.orm.Session(world.engine) as session:
        assert world.claim.claim_schedule(session, schedule_id, seen, later) is True
        assert world.claim.claim_schedule(session, schedule_id, seen, later + timedelta(minutes=5)) is False
        session.commit()
    assert world.schedule(schedule_id).next_run == later


# TC-US08-02 / R1.2: a claim moves `next_run` past now and sets `last_run`
def test_7011_r1_2_claim_sets_next_run_after_now_and_last_run(world):
    schedule_id = world.add_schedule()
    before = datetime.now(UTC)
    world.tick()
    row = world.schedule(schedule_id)
    assert row.next_run > before
    assert row.next_run.minute % 5 == 0 and row.next_run.second == 0
    assert row.last_run is not None and row.last_run >= before - timedelta(seconds=1)


# TC-US08-06 / R2.2: a finished run does not block; the new run row exists before the starter call
def test_7011_r2_2_finished_run_does_not_block_the_next_run(world):
    schedule_id = world.add_schedule()
    world.add_run(schedule_id, "success")
    world.tick()
    assert world.runs(schedule_id) == [("cron", "success"), ("cron", "running")]
    assert [status for _s, _run, status in world.starts] == ["running"]


# TC-US08-07, TC-US08-08 / R2.1: an active run blocks a second run, yet the claim stays
@pytest.mark.parametrize("active", ["running", "waiting_for_approval"])
def test_7011_r2_1_active_run_blocks_a_second_run(world, active):
    schedule_id = world.add_schedule()
    world.add_run(schedule_id, active)
    before = world.schedule(schedule_id).next_run
    world.tick()
    assert world.runs(schedule_id) == [("cron", active)]
    assert world.starts == []
    assert world.schedule(schedule_id).next_run > before  # the due time is consumed, not retried


def _hold_project_lock(world):
    holder = world.engine.connect().execution_options(isolation_level="AUTOCOMMIT")
    key = {"key": world.tick_module.legacy_lock_key(PROJECT_ID)}
    assert holder.execute(world.sa.text("SELECT pg_try_advisory_lock(:key)"), key).scalar()
    return holder, key


# TC-US08-14 / R4.1: the legacy check runs once per tick, after the schedule steps, and frees its lock
def test_7011_r4_1_legacy_step_runs_last_and_frees_the_project_lock(world):
    world.add_schedule()
    world.tick()
    assert world.events == ["start", "legacy"]
    assert world.legacy == [PROJECT_ID]
    holder, _ = _hold_project_lock(world)  # a free lock: the migration can take it right after the tick
    holder.close()


# TC-US08-15 / R4.2: a project the migration holds is skipped; the next tick runs it
def test_7011_r4_2_held_project_lock_skips_the_legacy_step_for_one_tick(world):
    holder, key = _hold_project_lock(world)
    try:
        world.tick()
        assert world.legacy == []
        holder.execute(world.sa.text("SELECT pg_advisory_unlock(:key)"), key)
    finally:
        holder.close()
    world.tick()
    assert world.legacy == [PROJECT_ID]
