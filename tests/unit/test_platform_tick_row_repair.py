"""The admin task repair of the platform tick row: one global row drives the tick, dry run by default."""

import ast
import sys
from pathlib import Path

import pytest
import sqlalchemy as sa
from sqlalchemy import orm

PLUGIN_ROOT = Path(__file__).parents[2]
sys.path.insert(0, str(PLUGIN_ROOT / "tests"))

from fixtures.helpers import load_utils_module

OLD, NEW = "entity_schedules_check", "pipelines_check_scheduling"


class _Base(orm.DeclarativeBase):
    pass


class Schedule(_Base):
    """The columns of the scheduling plugin's model that the repair reads and writes."""
    __tablename__ = "schedule"
    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String(64), nullable=False)
    project_id = sa.Column(sa.Integer, nullable=True)
    cron = sa.Column(sa.String(64), nullable=False)
    active = sa.Column(sa.Boolean, default=True)
    rpc_func = sa.Column(sa.String(64), nullable=False)


@pytest.fixture(scope="module")
def tick_row():
    return load_utils_module(PLUGIN_ROOT / "utils", "platform_tick_row")


def _session(rows):
    engine = sa.create_engine("sqlite://")
    _Base.metadata.create_all(engine)
    session = orm.Session(engine)
    for name, project_id, rpc_func, active in rows:
        session.add(Schedule(name=name, project_id=project_id, cron="* * * * *", rpc_func=rpc_func, active=active))
    session.commit()
    return session


def _rows(session):
    return [(r.id, r.rpc_func, r.active) for r in session.query(Schedule).order_by(Schedule.id)]


def test_a_dry_run_reports_the_plan_and_writes_nothing(tick_row):
    session = _session([("pipeline_scheduling", None, OLD, True), ("pipeline_scheduling", None, OLD, True)])
    before = _rows(session)
    plan = tick_row.repair(session, Schedule)
    assert plan == {"found": [1, 2], "rename": [1], "switch_off": [2]}
    assert _rows(session) == before


def test_the_only_stray_row_takes_the_managed_handler(tick_row):
    session = _session([("pipeline_scheduling", None, OLD, True)])
    tick_row.repair(session, Schedule, dry_run=False)
    assert _rows(session) == [(1, NEW, True)]


def test_a_stray_row_beside_the_managed_row_is_switched_off(tick_row):
    session = _session([("pipeline_scheduling", None, OLD, True), ("pipeline_scheduling", None, NEW, True)])
    plan = tick_row.repair(session, Schedule, dry_run=False)
    assert plan == {"found": [1], "rename": [], "switch_off": [1]}
    assert _rows(session) == [(1, OLD, False), (2, NEW, True)]


def test_of_two_stray_rows_only_the_oldest_is_renamed(tick_row):
    session = _session([("pipeline_scheduling", None, OLD, True), ("pipeline_scheduling", None, OLD, True)])
    tick_row.repair(session, Schedule, dry_run=False)
    assert _rows(session) == [(1, NEW, True), (2, OLD, False)]


def test_a_paused_stray_row_stays_paused_when_renamed(tick_row):
    session = _session([("pipeline_scheduling", None, OLD, False)])
    tick_row.repair(session, Schedule, dry_run=False)
    assert _rows(session) == [(1, NEW, False)]


def test_project_rows_and_other_names_are_not_touched(tick_row):
    session = _session([
        ("pipeline_scheduling", 7, OLD, True),
        ("other", None, OLD, True),
        ("pipeline_scheduling", None, NEW, True),
    ])
    before = _rows(session)
    plan = tick_row.repair(session, Schedule, dry_run=False)
    assert plan == {"found": [], "rename": [], "switch_off": []}
    assert _rows(session) == before


def test_a_second_run_changes_nothing(tick_row):
    session = _session([("pipeline_scheduling", None, OLD, True), ("pipeline_scheduling", None, OLD, True)])
    tick_row.repair(session, Schedule, dry_run=False)
    after = _rows(session)
    plan = tick_row.repair(session, Schedule, dry_run=False)
    assert plan == {"found": [2], "rename": [], "switch_off": []}
    assert _rows(session) == after


@pytest.mark.parametrize("param, dry_run", [
    ("", True), (None, True), ("dry_run=true", True), ("dry_run=false", False), (" DRY_RUN = False ", False),
])
def test_only_dry_run_false_writes(tick_row, param, dry_run):
    assert tick_row.parse_params(param) is dry_run


def _registered_tasks():
    tree = ast.parse((PLUGIN_ROOT / "module.py").read_text())
    tasks = {}
    for node in ast.walk(tree):
        if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                and node.func.attr == "register_admin_task" and node.args):
            group = next((k.value.value for k in node.keywords if k.arg == "group"), None)
            tasks[node.args[0].value] = group
    return tasks


def test_the_task_is_registered_in_the_current_release_group():
    tasks = _registered_tasks()
    assert tasks["fix_platform_tick_row"] == tasks["migrate_legacy_pipeline_schedules"]


def test_the_boot_does_not_repair_schedule_rows():
    src = (PLUGIN_ROOT / "module.py").read_text()
    body = src[src.index("    def _bootstrap_platform_schedules"):src.index("    def get_managed_schedules")]
    assert "platform_tick_row" not in body and "rebind" not in body
