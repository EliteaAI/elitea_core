"""EL-7011 US-13 R2.2, R2.3: the owner run starter blocks a run on a closed budget (TC-US13-09, -10, -11).

The real starter runs on faked neighbours. The budget gate is faked, except in the fail-open case
where the real `closed_budget_scope` meets a failing usage call.
"""
import sys
import types

import pytest
from unit._tool_spend_loader import load
from unit.utils.test_7011_run_starter_pipeline import OWNER, load_starter

PROJECT_ID, SCHEDULE_ID, RUN_ID = 34, 1, 3


@pytest.fixture
def world(monkeypatch):
    state = types.SimpleNamespace(calls=[], scope=None, predict_error=None)
    exceptions = load("utils/exceptions.py", fresh=True)

    def predict(**kwargs):
        state.calls.append(("predict", kwargs))
        if state.predict_error:
            raise state.predict_error

    def budget_scope(project_id, user_id=None):
        state.calls.append(("budget", project_id, user_id))
        return state.scope

    starter = load_starter(
        monkeypatch,
        create=lambda **kw: state.calls.append(("create",)) or ("uuid-1", 7, "resp-1"),
        predict=predict,
        prerun=lambda *a: types.SimpleNamespace(ok=True, owner_id=OWNER),
        finish_error=lambda *a: state.calls.append(("finish_error", *a)),
        fail=lambda *a: state.calls.append(("fail", *a)),
        block=lambda *a: state.calls.append(("block", *a)),
        budget_scope=budget_scope,
        budget_error=exceptions.BudgetDoorClosedError,
    )
    loaded = types.SimpleNamespace(
        schedule=types.SimpleNamespace(id=SCHEDULE_ID, entity_type="agent", run_mode=None),
        version=types.SimpleNamespace(id=9), entity_name="Daily report", prompt="Hi", variables=[])
    monkeypatch.setattr(starter, "_load", lambda *a: loaded)
    state.names = lambda: [call[0] for call in state.calls]
    state.run = lambda: starter.run_owner_schedule(PROJECT_ID, SCHEDULE_ID, RUN_ID)
    state.exceptions, state.starter = exceptions, starter
    state.monkeypatch = monkeypatch
    return state


def test_7011_r2_2_closed_scope_blocks_the_run_before_any_conversation_or_predict(world):
    world.scope = "monthly"
    world.run()
    assert world.calls == [("budget", PROJECT_ID, OWNER), ("block", PROJECT_ID, RUN_ID, "the monthly budget is used up")]


def test_7011_r2_3_budget_error_from_predict_blocks_the_run(world):
    world.predict_error = world.exceptions.BudgetDoorClosedError(scope="member", project_id=PROJECT_ID)
    world.run()
    assert world.names()[-2:] == ["finish_error", "block"]
    assert world.calls[-1] == ("block", PROJECT_ID, RUN_ID, "the member budget is used up")
    assert "fail" not in world.names()


def test_7011_r2_2_open_scope_starts_the_run(world):
    world.run()
    assert world.names() == ["budget", "create", "predict"]


def test_7011_r2_2_failing_budget_check_lets_the_run_start(world):
    def broken(**_):
        raise RuntimeError("usage plugin down")

    rpc = types.SimpleNamespace(timeout=lambda _s: types.SimpleNamespace(usage_gate_check=broken))
    world.monkeypatch.setattr(sys.modules["tools"], "context", types.SimpleNamespace(rpc_manager=rpc), raising=False)
    real = load("utils/budget_door.py", fresh=True)
    world.monkeypatch.setattr(world.starter, "closed_budget_scope", real.closed_budget_scope)
    world.run()
    assert world.names() == ["create", "predict"]
