"""The age cleanup keeps the checkpoint of a run for the whole configured approval wait."""
import sys
import types

import pytest
from unit._tool_spend_loader import load


@pytest.fixture
def cleanup(monkeypatch):
    emitted, state = [], {"waiting": True, "hours": 24}

    def stub(name, **attrs):
        module = types.ModuleType(name)
        vars(module).update(attrs)
        monkeypatch.setitem(sys.modules, name, module)

    stub("plugins.elitea_core.utils.project_utils", get_all_project_ids=lambda: [1])
    stub("plugins.elitea_core.utils.vectorstore", get_pgvector_connection_string=lambda _id: "conn")
    stub("plugins.elitea_core.utils.schedule_runs", has_waiting_runs=lambda _id: state["waiting"],
         approval_wait_hours=lambda: state["hours"])
    stub("pylon.core.tools", web=types.SimpleNamespace(rpc=lambda *a, **k: (lambda f: f)),
         log=types.SimpleNamespace(exception=lambda *a, **k: None))
    monkeypatch.setitem(sys.modules, "plugins.elitea_core.rpc.agent_state_cleanup", None)
    module = load("rpc/agent_state_cleanup.py", fresh=True)
    rpc = module.RPC()
    rpc.event_node = types.SimpleNamespace(emit=lambda _name, payload: emitted.append(payload))
    return rpc, emitted, state


@pytest.mark.parametrize("hours, days", [(1, 2), (24, 2), (25, 3), (720, 31)])
def test_a_waiting_run_raises_the_retention_to_the_wait_plus_a_day(cleanup, hours, days):
    rpc, emitted, state = cleanup
    state["hours"] = hours
    rpc.empty_state()
    assert emitted[0]["days_to_retain"] == days


def test_a_project_without_a_waiting_run_keeps_the_caller_retention(cleanup):
    rpc, emitted, state = cleanup
    state["waiting"] = False
    rpc.empty_state(days_to_retain=5)
    assert emitted[0]["days_to_retain"] == 5
