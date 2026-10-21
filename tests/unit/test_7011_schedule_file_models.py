"""EL-7011 US-22: file models of the schedule YAML file (R1.3, R2.4, R3.2, R5.6).

Contract under test: models/pd/schedule_yaml.py (ScheduleFile, ScheduleFileItem).
"""
import sys
import types
import uuid
from datetime import date

import pytest
from pydantic import ValidationError
from unit._tool_spend_loader import load

WRONG_HEADER = "This is not a schedule file of schema version 1 or 2"


@pytest.fixture
def model(monkeypatch):
    """The file models, loaded fresh with the plugin config stubbed."""
    stub = types.ModuleType("tools")
    stub.this = types.SimpleNamespace(module=types.SimpleNamespace(descriptor=types.SimpleNamespace(config={})))
    monkeypatch.setitem(sys.modules, "tools", stub)
    load("utils/schedule_timing.py", fresh=True)
    load("models/pd/index.py")
    load("models/pd/entity_schedule.py", fresh=True)
    return load("models/pd/schedule_yaml.py", fresh=True)


def _item(**overrides):
    item = {
        "name": "Daily digest", "description": None, "entity_type": "agent", "entity_name": "Digest agent",
        "version_name": "base", "run_mode": None, "cron": "0 9 * * 1-5", "timezone": "Europe/Berlin",
        "prompt": "Summarise", "variables": [{"name": "team", "value": "support"}],
        "expires_at": None, "approval_rule": "pause_and_ask",
    }
    return {**item, **overrides}


@pytest.mark.parametrize("header", [
    {"kind": "elitea_entities", "schema_version": 1},
    {"kind": "elitea_schedules", "schema_version": 3},
    {"kind": "elitea_schedules", "schema_version": True},
])
def test_r3_2_el_7011_wrong_header_is_rejected(model, header):
    with pytest.raises(ValidationError) as excinfo:
        model.ScheduleFile(**header, schedules=[])
    assert WRONG_HEADER in str(excinfo.value)


def test_r3_2_el_7011_missing_header_is_rejected(model):
    with pytest.raises(ValidationError):
        model.ScheduleFile(kind="elitea_schedules", schedules=[])


def test_r3_2_el_7011_right_header_keeps_items_raw(model):
    raw = {"name": "x", "bogus": 1}
    parsed = model.ScheduleFile(kind="elitea_schedules", schema_version=1, schedules=[raw, _item()])
    assert parsed.schedules[0] == raw


@pytest.mark.parametrize("version", [1, 2])
def test_r3_2_el_7011_schema_versions_1_and_2_are_accepted(model, version):
    assert model.ScheduleFile(kind="elitea_schedules", schema_version=version, schedules=[]).schema_version == version


def test_r1_3_el_7011_item_reads_the_key_as_a_uuid(model):
    assert model.ScheduleFileItem(**_item(key=str(uuid.UUID(int=5)))).key == uuid.UUID(int=5)


def test_r1_3_el_7011_item_rejects_a_key_that_is_not_a_uuid(model):
    with pytest.raises(ValidationError):
        model.ScheduleFileItem(**_item(key="42"))


def test_r5_6_el_7011_item_without_a_key_has_none(model):
    assert model.ScheduleFileItem(**_item()).key is None


def test_r2_4_el_7011_item_reads_all_allow_listed_keys(model):
    item = model.ScheduleFileItem(**_item(expires_at="2027-01-31"))
    assert item.expires_at == date(2027, 1, 31)
    assert item.variables[0].name == "team"


def test_r2_4_el_7011_item_rejects_unknown_key(model):
    with pytest.raises(ValidationError) as excinfo:
        model.ScheduleFileItem(**_item(enabled=True))
    assert excinfo.value.errors()[0]["type"] == "extra_forbidden"


@pytest.mark.parametrize("field", ["name", "entity_type", "entity_name", "cron", "timezone"])
def test_r2_4_el_7011_item_requires_core_keys(model, field):
    body = _item()
    del body[field]
    with pytest.raises(ValidationError):
        model.ScheduleFileItem(**body)


def test_r2_4_el_7011_item_optional_keys_default(model):
    body = {k: v for k, v in _item().items() if k in {"name", "entity_type", "entity_name", "cron", "timezone"}}
    item = model.ScheduleFileItem(**body)
    assert (item.description, item.version_name, item.run_mode, item.prompt, item.expires_at) == (None,) * 5
    assert (item.variables, item.approval_rule) == ([], "pause_and_ask")
