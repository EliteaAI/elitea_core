"""EL-7011 US-16: execution_type on conversation list rows.

Contract under test: models/pd/conversation.py (ConversationListExtended).
Covers R1.1, R1.2, R1.3.
"""
import importlib.util
import sys
import types
from datetime import datetime

import pytest
from pydantic import BaseModel
from unit._tool_spend_loader import PLUGIN_ROOT

PKG = "plugins.elitea_core"


@pytest.fixture
def model(monkeypatch):
    """conversation.py loaded under its package name, sibling imports stubbed."""
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


# TC-US16-03, TC-US16-04: R1.2 - legacy flag
def test_el_7011_r1_2_scheduled_run_flag_gives_scheduled(model):
    """TC-US16-03: no execution_type and scheduled_run true gives scheduled."""
    assert _row(model, {"scheduled_run": True}).execution_type == "scheduled"


def test_el_7011_r1_2_false_flag_gives_manual(model):
    """TC-US16-04: scheduled_run false gives manual."""
    assert _row(model, {"scheduled_run": False}).execution_type == "manual"


# TC-US16-02: R1.3 - default
@pytest.mark.parametrize("meta", [{}, {"execution_type": "other"}])
def test_el_7011_r1_3_no_usable_field_gives_manual(model, meta):
    """TC-US16-02: neither field (or an unknown value) gives manual."""
    assert _row(model, meta).execution_type == "manual"
