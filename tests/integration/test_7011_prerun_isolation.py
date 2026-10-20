"""EL-7011 US-09 R1.4, R1.5, R2.4, R2.5, R3.1: one owner failing the pre-run check leaves a shared agent's other schedule alone.

Real models, `run_prerun_check`, `check_owner_access` and the run writers on in-memory SQLite.
Only the auth RPC, the credential step and the notice helper are stood in for.
"""
import sys
import types
from contextlib import contextmanager

import pytest
from sqlalchemy import Column, Integer, Table, create_engine, select
from sqlalchemy.orm import DeclarativeBase, Session
from unit._tool_spend_loader import load

PKG = "plugins.elitea_core"
PROJECT_ID = 34
LOST, FAILED_OWNER, OK_OWNER = "owner lost access", 12, 13
CREDENTIAL = "credential jira-main not found"


class FakeAuth:
    """Auth RPC for several owners; `members` holds the project members, `users` the user records."""

    def __init__(self, members, users):
        self.members, self.users = members, users

    def timeout(self, _seconds):
        return self

    def auth_check_user_in_project(self, _project_id, user_id):
        return user_id in self.members

    def auth_get_user(self, user_id):
        if user_id not in self.users:
            raise RuntimeError("No such user found")
        return self.users[user_id]


@pytest.fixture
def world(monkeypatch):
    """Two schedules of agent 1: S1 (owner 12) and S2 (owner 13), each with one running run."""
    class Base(DeclarativeBase):
        pass

    for parent in ("applications", "application_versions"):
        Table(parent, Base.metadata, Column("id", Integer, primary_key=True), schema="tenant")
    models = types.ModuleType(f"{PKG}.models")
    models.__path__ = []
    models.db = types.SimpleNamespace(Base=Base)
    models.db_tools = types.SimpleNamespace(AbstractBaseMixin=type("AbstractBaseMixin", (), {}))
    models.config = types.SimpleNamespace(POSTGRES_TENANT_SCHEMA="tenant")
    monkeypatch.setitem(sys.modules, f"{PKG}.models", models)
    for name in ("db", "db_tools", "config"):
        monkeypatch.setitem(sys.modules, f"{PKG}.models.{name}", getattr(models, name))
    for dotted in ("models.entity_schedule", "utils.schedule_claim", "utils.schedule_runs", "utils.schedule_prerun"):
        monkeypatch.setitem(sys.modules, f"{PKG}.{dotted}", None)

    engine = create_engine("sqlite://", execution_options={"schema_translate_map": {"tenant": None}})
    tools = types.ModuleType("tools")
    tools.db = types.SimpleNamespace(get_session=None)
    tools.rpc_tools = types.SimpleNamespace()
    monkeypatch.setitem(sys.modules, "tools", tools)

    schedules = load("models/entity_schedule.py", fresh=True)
    load("utils/schedule_claim.py", fresh=True)
    load("utils/schedule_runs.py", fresh=True)
    prerun = load("utils/schedule_prerun.py", fresh=True)
    schedules.EntitySchedule.__table__.c.variables.server_default = None  # '[]'::jsonb is Postgres only
    Base.metadata.create_all(engine)

    notices = []
    monkeypatch.setitem(sys.modules, f"{PKG}.utils.schedule_notifications", types.SimpleNamespace(
        notify_schedule_event=lambda _project, owner_id, event, _schedule, run=None, extra=None: notices.append(
            (owner_id, event, run.reason))))
    monkeypatch.setitem(sys.modules, f"{PKG}.models.enums.all", types.SimpleNamespace(
        NotificationEventTypes=types.SimpleNamespace(schedule_disabled="schedule_disabled",
                                                     schedule_run_failed="schedule_run_failed")))

    @contextmanager
    def get_session(_project_id):
        with Session(engine) as session:
            yield session

    monkeypatch.setattr(tools.db, "get_session", get_session)
    auth = FakeAuth(members={OK_OWNER}, users={OK_OWNER: {"id": OK_OWNER, "suspended": False}})
    monkeypatch.setattr(prerun, "rpc_tools", types.SimpleNamespace(RpcMixin=lambda: types.SimpleNamespace(rpc=auth)))
    credential_errors = {}
    monkeypatch.setattr(prerun, "check_owner_credentials",
                        lambda _project, schedule: credential_errors.get(schedule.created_by))

    with Session(engine) as session:
        session.execute(Base.metadata.tables["tenant.applications"].insert().values(id=1))
        rows = {}
        for key, owner in (("S1", FAILED_OWNER), ("S2", OK_OWNER)):
            schedule = schedules.EntitySchedule(
                entity_type="agent", application_id=1, name=key, cron="0 * * * *", timezone="UTC",
                created_by=owner, principal={"kind": "delegation", "ref": owner})
            session.add(schedule)
            session.flush()
            run = schedules.EntityScheduleRun(schedule_id=schedule.id, trigger="cron", status="running")
            session.add(run)
            session.flush()
            rows[key] = (schedule.id, run.id)
        session.commit()

    def read(key):
        schedule_id, run_id = rows[key]
        with Session(engine) as session:
            schedule = session.get(schedules.EntitySchedule, schedule_id)
            run = session.scalar(select(schedules.EntityScheduleRun).where(schedules.EntityScheduleRun.id == run_id))
            return types.SimpleNamespace(
                enabled=schedule.enabled, disabled_reason=schedule.disabled_reason,
                last_run_status=schedule.last_run_status, run_status=run.status, run_reason=run.reason,
                owner=schedule.created_by)

    def check(key):
        schedule_id, run_id = rows[key]
        with Session(engine) as session:
            schedule = session.get(schedules.EntitySchedule, schedule_id)
            session.expunge(schedule)
        return prerun.run_prerun_check(PROJECT_ID, schedule, run_id)

    return types.SimpleNamespace(auth=auth, credential_errors=credential_errors, notices=notices,
                                 read=read, check=check)


def _non_member(world):
    pass  # owner 12 is not in `members`


def _suspended(world):
    world.auth.members.add(FAILED_OWNER)
    world.auth.users[FAILED_OWNER] = {"id": FAILED_OWNER, "suspended": True}


def _no_user_record(world):
    world.auth.members.add(FAILED_OWNER)


LOST_ACCESS = pytest.mark.parametrize("break_owner", [_non_member, _suspended, _no_user_record])


# TC-US09-10, TC-US09-04, TC-US09-05, TC-US09-06 / R1.4, R3.1
@LOST_ACCESS
def test_7011_r3_1_owner_lost_access_fails_s1_and_s2_is_admitted(world, break_owner):
    break_owner(world)

    failed, admitted = world.check("S1"), world.check("S2")

    assert (failed.ok, failed.owner_id, failed.reason) == (False, None, LOST)
    assert (admitted.ok, admitted.owner_id, admitted.reason) == (True, OK_OWNER, None)
    assert (world.read("S1").run_status, world.read("S1").run_reason) == ("failed", LOST)
    assert world.read("S2").run_status == "running"  # S2 starts its run


# TC-US09-11, TC-US09-04 / R1.5, R3.1
@LOST_ACCESS
def test_7011_r1_5_r3_1_lost_access_disables_only_s1_and_notifies_only_its_owner(world, break_owner):
    break_owner(world)
    before = world.read("S2")

    world.check("S1")

    assert (world.read("S1").enabled, world.read("S1").disabled_reason) == (False, "owner_lost_access")
    assert world.read("S2") == before
    assert world.notices == [(FAILED_OWNER, "schedule_disabled", LOST)]  # owner 13 gets nothing


# TC-US09-11 / R2.4, R2.5, R3.1: a credential failure keeps S1 enabled and still spares S2
def test_7011_r2_4_r2_5_r3_1_credential_failure_keeps_s1_enabled_and_leaves_s2_alone(world):
    world.auth.members.add(FAILED_OWNER)
    world.auth.users[FAILED_OWNER] = {"id": FAILED_OWNER, "suspended": False}
    world.credential_errors[FAILED_OWNER] = CREDENTIAL
    before = world.read("S2")

    failed, admitted = world.check("S1"), world.check("S2")

    s1 = world.read("S1")
    assert (failed.ok, failed.reason, admitted.ok, admitted.owner_id) == (False, CREDENTIAL, True, OK_OWNER)
    assert (s1.enabled, s1.disabled_reason, s1.run_status, s1.run_reason) == (True, None, "failed", CREDENTIAL)
    assert world.read("S2") == before
    assert world.notices == [(FAILED_OWNER, "schedule_run_failed", CREDENTIAL)]
