"""EL-7011 US-16: execution_type on conversation list rows.

Contract under test: models/pd/conversation.py (ConversationListExtended).
Covers R1.1, R1.2, R1.3 (amended by BUG-12: any value of the shared trigger enum).
"""
import importlib.util
import sys
import types
from datetime import datetime

import pytest
from pydantic import BaseModel
from unit._tool_spend_loader import PLUGIN_ROOT
from unit.utils.test_7011_pipeline_execution_owner import world  # noqa: F401 -- the real TriggerType

PKG = "plugins.elitea_core"
SHARED_TRIGGERS = ["chat", "api", "mcp", "webhook", "scheduled", "schedule_manual"]


@pytest.fixture
def model(monkeypatch, world):  # noqa: F811 -- the fixture imported above
    """conversation.py loaded under its package name, sibling imports stubbed, pipeline_execution real."""
    execution = world.execution
    stubs = {
        f"{PKG}.models.pd.message": {"MessageGroupDetail": BaseModel},
        f"{PKG}.models.pd.participant": {"ParticipantBase": BaseModel, "ParticipantCreate": BaseModel},
        f"{PKG}.utils.chat_constants": {"CONVERSATION_NAME_MAX_LENGTH": 255},
    }
    for name in (PKG, f"{PKG}.models", f"{PKG}.models.pd", f"{PKG}.utils"):
        package = types.ModuleType(name)
        package.__path__ = []
        monkeypatch.setitem(sys.modules, name, package)
    for name, attrs in stubs.items():
        stub = types.ModuleType(name)
        stub.__dict__.update(attrs)
        monkeypatch.setitem(sys.modules, name, stub)
    monkeypatch.setitem(sys.modules, f"{PKG}.utils.pipeline_execution", execution)
    dotted = f"{PKG}.models.pd.conversation"
    spec = importlib.util.spec_from_file_location(dotted, PLUGIN_ROOT / "models/pd/conversation.py")
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, dotted, module)
    spec.loader.exec_module(module)
    return module


def _row(model, meta):
    return model.ConversationListExtended(
        id=1, name="run", is_private=True, author_id=1, created_at=datetime(2026, 1, 1), meta=meta,
        participants_count=1, message_groups_count=1, users_count=1,
    )


# TC-US16-01: R1.1 - stored value wins
@pytest.mark.parametrize("stored", ["manual", "scheduled"])
def test_el_7011_r1_1_stored_execution_type_is_returned(model, stored):
    """TC-US16-01: meta.execution_type is returned as is, even against scheduled_run."""
    assert _row(model, {"execution_type": stored, "scheduled_run": stored == "manual"}).execution_type == stored


# BUG-12: R1.1 - every shared trigger value is returned as stored
@pytest.mark.parametrize("stored", SHARED_TRIGGERS)
def test_el_7011_bug12_r1_1_shared_trigger_value_is_returned(model, stored):
    """BUG-12: meta.execution_type=schedule_manual (or any shared value) is returned, not turned into manual."""
    assert _row(model, {"execution_type": stored}).execution_type == stored


# TC-US16-03, TC-US16-04: R1.2 - legacy flag
def test_el_7011_r1_2_scheduled_run_flag_gives_scheduled(model):
    """TC-US16-03: no execution_type and scheduled_run true gives scheduled."""
    assert _row(model, {"scheduled_run": True}).execution_type == "scheduled"


def test_el_7011_r1_2_false_flag_gives_chat(model):
    """TC-US16-04: scheduled_run false gives chat (owner decision 2026-10-21, US-50 R3.1)."""
    assert _row(model, {"scheduled_run": False}).execution_type == "chat"


# TC-US16-02: R1.3 - default
@pytest.mark.parametrize("meta", [{}, {"execution_type": "other"}])
def test_el_7011_r1_3_no_usable_field_gives_chat(model, meta):
    """TC-US16-02: neither field (or an unknown value) gives chat; the list never fails (owner decision 2026-10-21)."""
    assert _row(model, meta).execution_type == "chat"


CLIENT_META = {"schedule_id": 3, "schedule_run_id": 9, "scheduled_run": True, "execution_type": "scheduled",
               "persona": "kept"}


def test_a_created_conversation_drops_the_run_keys_of_client_meta(model):
    created = model.ConversationCreate(name="chat", author_id=1, meta=dict(CLIENT_META))
    assert created.meta == {"persona": "kept"}


def test_a_conversation_update_drops_the_run_keys_of_client_meta(model):
    assert model.ConversationUpdate(meta=dict(CLIENT_META)).meta == {"persona": "kept"}


def test_an_update_without_meta_stays_without_meta(model):
    assert model.ConversationUpdate(name="renamed").meta is None
