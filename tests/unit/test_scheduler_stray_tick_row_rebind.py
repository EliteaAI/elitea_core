"""A platform row with a handler of an earlier build runs the managed tick handler, and only one row runs it."""

import sys
import types
from pathlib import Path

import pytest
import sqlalchemy as sa

PLUGIN_ROOT = Path(__file__).parents[2]
sys.path.insert(0, str(PLUGIN_ROOT / "tests"))

from fixtures.helpers import load_utils_module

OLD, NEW = "entity_schedules_check", "pipelines_check_scheduling"


@pytest.fixture(scope="module")
def bindings():
    pylon = types.ModuleType("pylon")
    core = types.ModuleType("pylon.core")
    tools = types.ModuleType("pylon.core.tools")
    tools.log = types.SimpleNamespace(
        info=lambda *a, **k: None, error=lambda *a, **k: None,
        warning=lambda *a, **k: None, exception=lambda *a, **k: None,
    )
    return load_utils_module(
        PLUGIN_ROOT / "utils", "scheduler_bindings",
        extra_stubs={"pylon": pylon, "pylon.core": core, "pylon.core.tools": tools},
    )


def _rows(session):
    return session.execute(sa.text(
        "SELECT name, rpc_func, active FROM schedule ORDER BY id"
    )).all()


def _session(rows):
    engine = sa.create_engine("sqlite://")
    with engine.begin() as conn:
        conn.execute(sa.text(
            "CREATE TABLE schedule (id INTEGER PRIMARY KEY, name TEXT, "
            "project_id INTEGER, rpc_func TEXT, active BOOLEAN)"
        ))
        for name, project_id, rpc_func, active in rows:
            conn.execute(sa.text(
                "INSERT INTO schedule (name, project_id, rpc_func, active) "
                "VALUES (:n, :p, :r, :a)"
            ), {"n": name, "p": project_id, "r": rpc_func, "a": active})
    return sa.orm.Session(engine)


def test_a_stray_handler_row_is_bound_to_the_managed_handler(bindings):
    session = _session([("pipeline_scheduling", None, OLD, True)])
    bindings.rebind_legacy_rows(session)
    assert _rows(session) == [("pipeline_scheduling", NEW, True)]


def test_a_stray_row_beside_the_managed_row_is_switched_off(bindings):
    session = _session([
        ("pipeline_scheduling", None, OLD, True),
        ("pipeline_scheduling", None, NEW, True),
    ])
    bindings.rebind_legacy_rows(session)
    assert _rows(session) == [
        ("pipeline_scheduling", OLD, False),
        ("pipeline_scheduling", NEW, True),
    ]


def test_other_rows_are_not_touched(bindings):
    session = _session([
        ("pipeline_scheduling", 7, OLD, True),
        ("other", None, OLD, True),
        ("pipeline_scheduling", None, NEW, True),
    ])
    before = _rows(session)
    bindings.rebind_legacy_rows(session)
    assert _rows(session) == before


def test_a_paused_stray_row_stays_paused_after_the_rebind(bindings):
    session = _session([("pipeline_scheduling", None, OLD, False)])
    bindings.rebind_legacy_rows(session)
    assert _rows(session) == [("pipeline_scheduling", NEW, False)]


def _bootstrap():
    src = (PLUGIN_ROOT / "module.py").read_text()
    body = src[
        src.index("    def _bootstrap_platform_schedules"):
        src.index("    def get_managed_schedules")
    ]
    events = []
    log = types.SimpleNamespace(
        warning=lambda *a, **k: None, exception=lambda *a, **k: None,
    )
    namespace = {
        "Empty": type("Empty", (Exception,), {}),
        "log": log,
        "build_scheduler_sync_plan": lambda cfg: [
            {"name": n, "cron": "* * * * *", "active": True, "rpc_func": n}
            for n in ("index_scheduling", "pipeline_scheduling")
        ],
    }
    exec(compile("class _M:\n" + body, "module.py", "exec"), namespace)  # noqa: S102 - runs one method of module.py without the pylon plugin

    def _rebind():
        events.append("rebind")

    holder = types.SimpleNamespace(
        descriptor=types.SimpleNamespace(config={}),
        context=types.SimpleNamespace(rpc_manager=types.SimpleNamespace(
            timeout=lambda _s: types.SimpleNamespace(
                scheduling_create_if_not_exists=lambda p: events.append(p["name"]),
            ),
        )),
        _rebind_legacy_schedule_rows=_rebind,
        _apply_scheduler_runtime_config=lambda plan=None: events.append("apply"),
        _release_managed_schedules=lambda: events.append("released"),
    )
    namespace["_M"]._bootstrap_platform_schedules(holder)
    return events


def test_the_rebind_runs_before_the_rows_are_created():
    events = _bootstrap()
    assert events[0] == "rebind"
    assert "pipeline_scheduling" in events


def test_a_failed_rebind_does_not_stop_the_bootstrap():
    src = (PLUGIN_ROOT / "module.py").read_text()
    body = src[src.index("    def _rebind_legacy_schedule_rows"):src.index("    def _bootstrap_platform_schedules")]
    logged = []

    def _raise(_session):
        raise RuntimeError("no database")

    namespace = {
        "db": types.SimpleNamespace(get_session=lambda _p: __import__("contextlib").nullcontext(None)),
        "rebind_legacy_rows": _raise,
        "log": types.SimpleNamespace(exception=logged.append),
    }
    exec(compile("class _M:\n" + body, "module.py", "exec"), namespace)  # noqa: S102 - runs one method of module.py without the pylon plugin
    namespace["_M"]()._rebind_legacy_schedule_rows()
    assert logged == ["Failed to rebind legacy platform schedule rows"]
