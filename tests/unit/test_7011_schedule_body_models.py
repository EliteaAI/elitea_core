"""EL-7011 US-07: create, update and details bodies of a schedule.

Contract under test: models/pd/entity_schedule.py (ScheduleVariable, ScheduleCreate,
ScheduleUpdate, ScheduleDetails). The run input rules (R5.1, R5.4 to R5.7) live in the service.
"""
import sys
import types
from datetime import UTC, datetime

import pytest
from pydantic import ValidationError
from unit._tool_spend_loader import load

DAILY = "0 9 * * *"


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


def _error_fields(excinfo):
    return {error["loc"][0] for error in excinfo.value.errors() if error["loc"]}


# R2.2 the update body carries no owner field, so created_by and principal are ignored
def test_el_7011_r2_2_update_body_drops_created_by_and_principal(model):
    body = model.ScheduleUpdate(name="New", created_by=6, principal={"kind": "mandate", "ref": 6})
    assert body.model_dump(exclude_unset=True) == {"name": "New"}
    assert not hasattr(body, "created_by")
    assert not hasattr(body, "principal")


def test_el_7011_r2_2_create_body_has_no_owner_field(model):
    body = model.ScheduleCreate(**_create_body(created_by=6, principal={"kind": "mandate", "ref": 6}))
    assert "created_by" not in body.model_dump()
    assert "principal" not in body.model_dump()


# R5.3 variables are a list of {name, value}
def test_el_7011_r5_3_variables_are_name_value_pairs(model):
    pairs = [{"name": "region", "value": "EU"}, {"name": "limit", "value": "10"}]
    body = model.ScheduleCreate(**_create_body(run_mode="prompt", variables=pairs))
    assert [variable.model_dump() for variable in body.variables] == pairs


@pytest.mark.parametrize("variables", [{"region": "EU"}, [{"name": "region"}], [{"value": "EU"}], ["region"]])
def test_el_7011_r5_3_malformed_variables_are_rejected_on_variables(model, variables):
    with pytest.raises(ValidationError) as excinfo:
        model.ScheduleCreate(**_create_body(variables=variables))
    assert _error_fields(excinfo) == {"variables"}


def test_el_7011_r5_3_variables_default_to_an_empty_list(model):
    assert model.ScheduleCreate(**_create_body()).variables == []


# R5.4 run_mode is entry_point or prompt
def test_el_7011_r5_4_unknown_run_mode_is_rejected_on_run_mode(model):
    with pytest.raises(ValidationError) as excinfo:
        model.ScheduleCreate(**_create_body(entity_type="pipeline", run_mode="start"))
    assert _error_fields(excinfo) == {"run_mode"}
    with pytest.raises(ValidationError) as excinfo:
        model.ScheduleUpdate(run_mode="start")
    assert _error_fields(excinfo) == {"run_mode"}


def test_el_7011_r5_2_pipeline_entry_point_body_needs_no_prompt(model):
    body = model.ScheduleCreate(**_create_body(entity_type="pipeline", run_mode="entry_point", prompt=None))
    assert (body.run_mode, body.prompt, body.variables) == ("entry_point", None, [])


# R5.6 description holds at most 2304 characters
def test_el_7011_r5_6_description_over_2304_is_rejected_on_description(model):
    assert model.ScheduleCreate(**_create_body(description="x" * 2304)).description == "x" * 2304
    with pytest.raises(ValidationError) as excinfo:
        model.ScheduleCreate(**_create_body(description="x" * 2305))
    assert _error_fields(excinfo) == {"description"}
    with pytest.raises(ValidationError) as excinfo:
        model.ScheduleUpdate(description="x" * 2305)
    assert _error_fields(excinfo) == {"description"}


# Create body: required fields, timing rules inherited, enabled by default
def test_el_7011_create_body_requires_its_fields_and_enables_by_default(model):
    assert model.ScheduleCreate(**_create_body()).enabled is True
    with pytest.raises(ValidationError) as excinfo:
        model.ScheduleCreate(cron=DAILY, timezone="UTC")
    assert _error_fields(excinfo) == {"name", "entity_type", "application_id", "version_id"}


def test_el_7011_create_body_rejects_an_unknown_entity_type(model):
    with pytest.raises(ValidationError) as excinfo:
        model.ScheduleCreate(**_create_body(entity_type="toolkit"))
    assert _error_fields(excinfo) == {"entity_type"}


def test_el_7011_create_body_keeps_the_timing_rules(model):
    with pytest.raises(ValidationError) as excinfo:
        model.ScheduleCreate(**_create_body(cron="* * * * *"))
    assert _error_fields(excinfo) == {"cron"}


# Update body: every field optional, timing rules apply to a value that is sent
def test_el_7011_update_body_sends_only_what_it_sets(model):
    assert model.ScheduleUpdate().model_dump(exclude_unset=True) == {}
    body = model.ScheduleUpdate(enabled=True, version_id=None)
    assert body.model_dump(exclude_unset=True) == {"enabled": True, "version_id": None}


@pytest.mark.parametrize("field, value", [("cron", "* * * * *"), ("timezone", "Mars/Base"), ("name", "")])
def test_el_7011_update_body_validates_a_sent_value(model, field, value):
    with pytest.raises(ValidationError) as excinfo:
        model.ScheduleUpdate(**{field: value})
    assert _error_fields(excinfo) == {field}


def test_el_7011_update_body_accepts_a_valid_cron_and_timezone(model):
    body = model.ScheduleUpdate(cron=DAILY, timezone="Europe/Kyiv")
    assert (body.cron, body.timezone) == (DAILY, "Europe/Kyiv")


# Details body: serializes a stored row, past stop dates included
def test_el_7011_details_body_reads_a_stored_row(model):
    row = types.SimpleNamespace(
        id=7, name="Nightly", description=None, entity_type="agent", application_id=15, version_id=None,
        run_mode=None, prompt="Summarize", variables=[{"name": "a", "value": "b"}], cron="* * * * *",
        timezone="UTC", expires_at=datetime(2020, 1, 1, tzinfo=UTC), enabled=False, created_by=5,
        owner={"id": 5, "name": "Ann"}, updated_by=None, next_run=None, last_run=None,
        last_run_status=None, disabled_reason="owner_lost_access", approval_rule="pause_and_ask",
        allowed_actions=["edit", "delete"],
    )
    details = model.ScheduleDetails.model_validate(row)
    assert details.owner.model_dump() == {"id": 5, "name": "Ann"}
    assert details.variables[0].value == "b"
    assert details.model_dump()["allowed_actions"] == ["edit", "delete"]
