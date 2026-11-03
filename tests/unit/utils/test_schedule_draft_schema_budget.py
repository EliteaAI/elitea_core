"""apply_draft_column: the boot wait is capped and the projects left over are returned as failed."""
import sys
import types

import pytest
from unit._tool_spend_loader import load


@pytest.fixture
def schema(monkeypatch):
    """schedule_draft_schema with the schedule model and the db tools stubbed: only the retry loop is under test."""
    models = types.ModuleType("plugins.elitea_core.models.entity_schedule")
    models.EntitySchedule = types.SimpleNamespace(__tablename__="entity_schedules")
    monkeypatch.setitem(sys.modules, "plugins.elitea_core.models", types.ModuleType("plugins.elitea_core.models"))
    monkeypatch.setitem(sys.modules, "plugins.elitea_core.models.entity_schedule", models)
    monkeypatch.setitem(sys.modules, "tools", types.SimpleNamespace(db=types.SimpleNamespace(get_session=None)))
    monkeypatch.setitem(sys.modules, "plugins.elitea_core.utils.schedule_draft_schema", None)
    return load("utils/schedule_draft_schema.py", fresh=True)


@pytest.fixture
def always_failing(monkeypatch, schema):
    calls = []

    def fail(pid):
        calls.append(pid)
        raise RuntimeError("locked")

    monkeypatch.setattr(schema, "_apply_once", fail)
    return calls


def test_the_retry_loop_stops_at_the_budget_and_returns_every_project_as_failed(schema, always_failing):
    now = [0.0]

    def clock():
        now[0] += 10
        return now[0]

    migrated, failed = schema.apply_draft_column(
        [1, 2, 3, 4, 5], attempts=3, delay_s=5, sleep=lambda _: None, budget_s=30, clock=clock)

    assert migrated == []
    assert sorted(failed) == [1, 2, 3, 4, 5]
    assert len(always_failing) < 15


def test_a_project_that_works_on_the_second_try_is_migrated(schema, monkeypatch):
    seen = []

    def flaky(pid):
        seen.append(pid)
        if len(seen) == 1:
            raise RuntimeError("locked")

    monkeypatch.setattr(schema, "_apply_once", flaky)

    migrated, failed = schema.apply_draft_column([7], attempts=3, delay_s=0, sleep=lambda _: None)

    assert (migrated, failed) == ([7], [])
