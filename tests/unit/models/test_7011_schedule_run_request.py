"""EL-7011 US-15: ScheduleRunRequest model for run-now and test-run API.

Contract under test: models/pd/schedule_run.py (ScheduleRunRequest).
Covers R2.3, R2.4: request body validation.
"""
import sys
import types

import pytest
from pydantic import ValidationError
from unit._tool_spend_loader import load


@pytest.fixture
def model(monkeypatch):
    """The schedule run model, loaded fresh with the plugin config stubbed."""
    stub = types.ModuleType("tools")
    stub.this = types.SimpleNamespace(module=types.SimpleNamespace(descriptor=types.SimpleNamespace(config={})))
    monkeypatch.setitem(sys.modules, "tools", stub)
    monkeypatch.setitem(sys.modules, "plugins.elitea_core.models.pd.schedule_run", None)
    return load("models/pd/schedule_run.py", fresh=True)


def _error_fields(excinfo):
    return {error["loc"][0] for error in excinfo.value.errors() if error["loc"]}


# TC-US15-13, TC-US15-14: R2.3, R2.4 - body validation
def test_el_7011_r2_3_r2_4_trigger_manual_is_valid(model):
    """R2.3, R2.4: trigger='manual' is valid."""
    body = model.ScheduleRunRequest(trigger="manual")
    assert body.trigger == "manual"


def test_el_7011_r2_3_r2_4_trigger_test_is_valid(model):
    """R2.3, R2.4: trigger='test' is valid."""
    body = model.ScheduleRunRequest(trigger="test")
    assert body.trigger == "test"


def test_el_7011_r2_3_r2_4_missing_trigger_fails(model):
    """R2.4: missing trigger is rejected."""
    with pytest.raises(ValidationError) as excinfo:
        model.ScheduleRunRequest()
    assert _error_fields(excinfo) == {"trigger"}


def test_el_7011_r2_3_r2_4_invalid_trigger_value_fails(model):
    """R2.4: invalid trigger value is rejected."""
    with pytest.raises(ValidationError) as excinfo:
        model.ScheduleRunRequest(trigger="cron")
    assert _error_fields(excinfo) == {"trigger"}


def test_el_7011_r2_3_r2_4_extra_fields_are_forbidden(model):
    """R2.3: extra fields are forbidden (extra='forbid')."""
    with pytest.raises(ValidationError) as excinfo:
        model.ScheduleRunRequest(trigger="manual", user_input="some prompt")
    assert _error_fields(excinfo) == {"user_input"}


def test_el_7011_r2_3_r2_4_multiple_extra_fields_are_all_rejected(model):
    """R2.3: multiple extra fields are all rejected."""
    with pytest.raises(ValidationError) as excinfo:
        model.ScheduleRunRequest(trigger="test", user_input="prompt", extra_param="value")
    error_fields = _error_fields(excinfo)
    assert error_fields == {"user_input", "extra_param"}


def test_el_7011_r2_3_r2_4_trigger_null_is_rejected(model):
    """R2.4: null trigger is rejected."""
    with pytest.raises(ValidationError) as excinfo:
        model.ScheduleRunRequest(trigger=None)
    assert _error_fields(excinfo) == {"trigger"}


def test_el_7011_r2_3_r2_4_trigger_is_case_sensitive(model):
    """R2.4: trigger is case-sensitive (only 'manual' and 'test' are valid)."""
    with pytest.raises(ValidationError) as excinfo:
        model.ScheduleRunRequest(trigger="Manual")
    assert _error_fields(excinfo) == {"trigger"}

    with pytest.raises(ValidationError) as excinfo:
        model.ScheduleRunRequest(trigger="TEST")
    assert _error_fields(excinfo) == {"trigger"}
