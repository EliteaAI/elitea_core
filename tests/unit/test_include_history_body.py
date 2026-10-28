"""The delivery object of a schedule body keeps `include_history` between 0 and 20."""
import sys
import types

import pytest
from pydantic import ValidationError
from sqlalchemy import Column, Integer, Table
from sqlalchemy.orm import DeclarativeBase
from unit._tool_spend_loader import load

BASE = {"cron": "0 9 * * *", "timezone": "UTC", "name": "n", "entity_type": "agent",
        "application_id": 1, "version_id": 2, "prompt": "hi"}
CONVERSATION = {"mode": "conversation", "conversation_uuid": "3f1c9a0e-5b7d-4c2a-8e6f-0b1d3c5a7e9f",
                "participant_id": 5}


@pytest.fixture
def model(monkeypatch):
    """models/pd/entity_schedule.py with the plugin settings and the ORM table stubbed."""
    class Base(DeclarativeBase):
        pass

    for parent in ("applications", "application_versions"):
        Table(parent, Base.metadata, Column("id", Integer, primary_key=True), schema="tenant")
    package = types.ModuleType("plugins.elitea_core.models")
    package.__path__ = []
    monkeypatch.setitem(sys.modules, "plugins.elitea_core.models", package)
    for name, stub in {
        "db": types.SimpleNamespace(Base=Base),
        "db_tools": types.SimpleNamespace(AbstractBaseMixin=type("AbstractBaseMixin", (), {})),
        "config": types.SimpleNamespace(POSTGRES_TENANT_SCHEMA="tenant"),
    }.items():
        setattr(package, name, stub)
        monkeypatch.setitem(sys.modules, "plugins.elitea_core.models." + name, stub)
    monkeypatch.setitem(sys.modules, "plugins.elitea_core.models.entity_schedule", None)
    load("models/entity_schedule.py", fresh=True)
    descriptor = types.SimpleNamespace(config={})
    tools = types.ModuleType("tools")
    tools.this = types.SimpleNamespace(module=types.SimpleNamespace(descriptor=descriptor))
    monkeypatch.setitem(sys.modules, "tools", tools)
    for dotted in ("utils.schedule_timing", "models.pd.entity_schedule"):
        monkeypatch.setitem(sys.modules, "plugins.elitea_core." + dotted, None)
    load("utils/schedule_timing.py", fresh=True)
    load("models/pd/index.py")
    return load("models/pd/entity_schedule.py", fresh=True)


def _delivery(**extra):
    return {"delivery": {**CONVERSATION, **extra}}


def test_delivery_include_history_defaults_to_zero(model):
    assert model.ScheduleCreate(**BASE, delivery=CONVERSATION).delivery.include_history == 0


@pytest.mark.parametrize("value", [0, 1, 20])
def test_create_accepts_include_history_inside_range(model, value):
    body = model.ScheduleCreate(**BASE, **_delivery(include_history=value))
    assert body.delivery.include_history == value


@pytest.mark.parametrize("value", [-1, 21, 100])
def test_create_rejects_include_history_outside_range(model, value):
    with pytest.raises(ValidationError) as excinfo:
        model.ScheduleCreate(**BASE, **_delivery(include_history=value))
    assert excinfo.value.errors()[0]["loc"] == ("delivery", "include_history")


@pytest.mark.parametrize("value", [-1, 21])
def test_edit_rejects_include_history_outside_range(model, value):
    with pytest.raises(ValidationError) as excinfo:
        model.ScheduleEdit(**_delivery(include_history=value))
    assert excinfo.value.errors()[0]["loc"] == ("delivery", "include_history")


def test_edit_accepts_include_history_at_the_upper_bound(model):
    assert model.ScheduleEdit(**_delivery(include_history=20)).delivery.include_history == 20
