"""The delivery object of the schedule bodies: models/pd/entity_schedule.py."""
import sys
import types
from datetime import UTC, datetime

import pytest
from pydantic import ValidationError
from unit._tool_spend_loader import load

DAILY = "0 9 * * *"
CHAT = "3f1c9a0e-5b7d-4c2a-8e6f-0b1d3c5a7e9f"


@pytest.fixture
def model(monkeypatch):
    """The body models, loaded fresh with the plugin config stubbed."""
    stub = types.ModuleType("tools")
    stub.this = types.SimpleNamespace(module=types.SimpleNamespace(descriptor=types.SimpleNamespace(config={})))
    monkeypatch.setitem(sys.modules, "tools", stub)
    for dotted in ("utils.schedule_timing", "models.pd.entity_schedule"):
        monkeypatch.setitem(sys.modules, "plugins.elitea_core." + dotted, None)
    load("utils/schedule_timing.py", fresh=True)
    load("models/pd/index.py")
    return load("models/pd/entity_schedule.py", fresh=True)


def _create_body(**overrides):
    body = {
        "name": "Nightly report", "entity_type": "agent", "application_id": 15, "version_id": 10,
        "prompt": "Summarize", "cron": DAILY, "timezone": "UTC",
    }
    return {**body, **overrides}


def _details(model, **overrides):
    row = {
        "id": 1, "name": "n", "description": None, "entity_type": "agent", "application_id": 2,
        "version_id": 3, "run_mode": "prompt", "prompt": "p", "variables": [], "cron": DAILY,
        "timezone": "UTC", "expires_at": None, "enabled": True, "created_by": 4,
        "owner": {"id": 4}, "updated_by": None, "next_run": None, "last_run": datetime.now(UTC),
        "last_run_status": None, "disabled_reason": None, "approval_rule": "pause_and_ask",
        "allowed_actions": [],
    }
    return model.ScheduleDetails(**{**row, **overrides})


def test_conversation_mode_needs_chat_and_participant(model):
    for delivery in ({"mode": "conversation"},
                     {"mode": "conversation", "conversation_uuid": CHAT},
                     {"mode": "conversation", "participant_id": 5}):
        with pytest.raises(ValidationError):
            model.ScheduleCreate(**_create_body(delivery=delivery))
        with pytest.raises(ValidationError):
            model.ScheduleEdit(delivery=delivery)


def test_conversation_mode_accepts_both_targets(model):
    body = model.ScheduleCreate(**_create_body(
        delivery={"mode": "conversation", "conversation_uuid": CHAT, "participant_id": 5}))
    assert body.delivery.model_dump() == {"mode": "conversation", "conversation_uuid": CHAT, "participant_id": 5}


def test_run_history_mode_clears_targets(model):
    body = model.ScheduleEdit(delivery={"mode": "run_history", "conversation_uuid": CHAT, "participant_id": 5})
    assert body.delivery.model_dump() == {"mode": "run_history", "conversation_uuid": None, "participant_id": None}


def test_bad_participant_id_is_rejected(model):
    with pytest.raises(ValidationError):
        model.ScheduleEdit(delivery={"mode": "conversation", "conversation_uuid": CHAT, "participant_id": 0})


def test_edit_without_delivery_keeps_it_unset(model):
    body = model.ScheduleEdit(name="New")
    assert body.delivery is None
    assert "delivery" not in body.model_dump(exclude_unset=True)


def test_details_return_delivery_object(model):
    delivery = {"mode": "conversation", "conversation_uuid": CHAT, "participant_id": 5}
    assert _details(model, delivery=delivery).delivery.model_dump() == delivery
    assert _details(model).delivery.model_dump() == {
        "mode": "run_history", "conversation_uuid": None, "participant_id": None}
