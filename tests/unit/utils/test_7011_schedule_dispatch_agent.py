"""EL-7011 US-11 R1.1: the agent starter sits in the run starter map."""
import sys
import types

import pytest
from unit._tool_spend_loader import load

DISPATCH = "utils/schedule_dispatch.py"
STARTER = "plugins.elitea_core.utils.schedule_run_starter"


def test_7011_r1_1_agent_and_pipeline_starters_are_registered():
    starters = load(DISPATCH, fresh=True).RUN_STARTERS
    assert callable(starters["agent"])
    assert callable(starters["pipeline"])


@pytest.mark.parametrize("entity_type", ["agent", "pipeline"])
def test_7011_r1_1_starter_calls_run_owner_schedule(monkeypatch, entity_type):
    calls = []
    stub = types.ModuleType(STARTER)
    stub.run_owner_schedule = lambda *args: calls.append(args)
    monkeypatch.setitem(sys.modules, STARTER, stub)
    load(DISPATCH, fresh=True).RUN_STARTERS[entity_type](5, 6, 7)
    assert calls == [(5, 6, 7)]


def test_7011_r1_1_dispatch_loads_without_the_starter_module(monkeypatch):
    monkeypatch.delitem(sys.modules, STARTER, raising=False)
    load(DISPATCH, fresh=True)
    assert STARTER not in sys.modules
