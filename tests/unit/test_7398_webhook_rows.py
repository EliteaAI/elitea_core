"""#7398 Core A: webhook rows of entity_schedules — body models, list filter, copy rules.

Contracts under test:
- models/pd/entity_schedule.py: `trigger_kind` discriminator; cron fields only on cron rows, webhook fields only
  on webhook rows; the min-interval check never runs for webhooks.
- models/pd/schedule_list.py: the list defaults to cron rows, so the Schedules page, MCP tools and chat
  drawer never see webhooks.
- utils/trigger_write_rule.py: a copied pipeline_settings never carries a legacy webhook secret ref (#6440).
"""
import pathlib
import sys
import types

import pytest
from pydantic import ValidationError
from unit._tool_spend_loader import load

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PLUGIN_ROOT / "tests"))

from fixtures.helpers import load_utils_module  # noqa: E402


@pytest.fixture
def model(monkeypatch):
    # min_interval 60 makes a 5-minute cron invalid, to show webhooks skip the interval rule
    stub = types.ModuleType("tools")
    config = {"scheduler": {"schedules": {"min_interval_minutes": 60}}}
    stub.this = types.SimpleNamespace(module=types.SimpleNamespace(descriptor=types.SimpleNamespace(config=config)))
    monkeypatch.setitem(sys.modules, "tools", stub)
    for dotted in ("utils.schedule_timing", "models.pd.entity_schedule"):
        monkeypatch.setitem(sys.modules, "plugins.elitea_core." + dotted, None)
    load("utils/schedule_timing.py", fresh=True)
    load("models/pd/index.py")
    return load("models/pd/entity_schedule.py", fresh=True)


@pytest.fixture
def list_model():
    return load("models/pd/schedule_list.py", fresh=True)


def _body(**overrides):
    body = {"name": "On push", "entity_type": "agent", "application_id": 15, "version_id": 10, "prompt": "Go"}
    return {**body, **overrides}


def _webhook(**overrides):
    return _body(**{"trigger_kind": "webhook", "webhook_type": "github", **overrides})


def test_7398_webhook_with_a_stop_date_is_refused(model):
    with pytest.raises(ValidationError, match="no cron, timezone or stop date"):
        model.ScheduleCreate(**_webhook(expires_at="2099-01-01"))


# AC10: a create without trigger_kind is a cron schedule and keeps every cron rule
def test_7398_cron_is_the_default_and_still_needs_cron_and_timezone(model):
    assert model.ScheduleCreate(**_body(cron="0 9 * * *", timezone="UTC")).trigger_kind == "cron"
    with pytest.raises(ValidationError, match="cron and timezone"):
        model.ScheduleCreate(**_body())
    with pytest.raises(ValidationError, match="60 minutes apart"):
        model.ScheduleCreate(**_body(cron="*/5 * * * *", timezone="UTC"))


def test_7398_cron_rows_refuse_webhook_fields(model):
    with pytest.raises(ValidationError, match="only allowed on webhooks"):
        model.ScheduleCreate(**_body(cron="0 9 * * *", timezone="UTC", webhook_type="github"))


def test_7398_webhook_needs_type_and_no_cron(model):
    assert model.ScheduleCreate(**_webhook()).cron is None
    with pytest.raises(ValidationError, match="needs webhook_type"):
        model.ScheduleCreate(**_body(trigger_kind="webhook"))
    for field, value in (("cron", "0 9 * * *"), ("timezone", "UTC")):
        with pytest.raises(ValidationError, match="no cron, timezone or stop date"):
            model.ScheduleCreate(**_webhook(**{field: value}))


def test_7398_gitlab_method_only_and_always_for_gitlab(model):
    # #7404: a signing-token row also carries the token GitLab issued
    assert model.ScheduleCreate(**_body(trigger_kind="webhook", webhook_type="gitlab", gitlab_auth_method="signing_token",
                                        signing_secret_value="whsec_x")).gitlab_auth_method == "signing_token"
    with pytest.raises(ValidationError, match="gitlab_auth_method"):
        model.ScheduleCreate(**_body(trigger_kind="webhook", webhook_type="gitlab"))
    with pytest.raises(ValidationError, match="gitlab_auth_method"):
        model.ScheduleCreate(**_webhook(gitlab_auth_method="secret_token"))


def test_7398_unknown_kind_or_type_is_refused(model):
    with pytest.raises(ValidationError):
        model.ScheduleCreate(**_body(trigger_kind="timer"))
    with pytest.raises(ValidationError):
        model.ScheduleCreate(**_webhook(webhook_type="bitbucket"))


def test_7398_edit_body_cannot_change_the_kind(model):
    assert "trigger_kind" not in model.ScheduleUpdate(trigger_kind="webhook").model_dump(exclude_unset=True)


# AC3: the list defaults to cron rows; webhooks are asked for explicitly
def test_7398_list_defaults_to_cron_rows(list_model):
    assert list_model.ScheduleListFilters().trigger_kind == "cron"
    filters = list_model.ScheduleListFilters(trigger_kind="webhook", webhook_type="gitlab")
    assert (filters.trigger_kind, filters.webhook_type) == ("webhook", "gitlab")
    with pytest.raises(ValidationError):
        list_model.ScheduleListFilters(trigger_kind="all")


def test_7398_list_item_of_a_webhook_has_no_cron(list_model):
    item = list_model.ScheduleListItem(
        id=1, name="On push", entity_type="agent", application_id=1, entity_name="A", enabled=True,
        owner={"id": 3}, allowed_actions=[], trigger_kind="webhook", webhook_type="github",
        webhook_url="/api/v2/elitea_core/hooks/prompt_lib/3/abc")
    assert item.cron is None and item.timezone is None
    assert item.previous_secret_until is None


# #7405: the Webhooks table marks a row whose previous secret is still accepted
def test_7405_list_item_carries_the_rotation_window(list_model):
    item = list_model.ScheduleListItem(
        id=1, name="On push", entity_type="agent", application_id=1, entity_name="A", enabled=True,
        owner={"id": 3}, allowed_actions=[], trigger_kind="webhook", webhook_type="github",
        gitlab_auth_method=None, previous_secret_until="2026-11-06T10:00:00+00:00")
    assert item.model_dump(mode="json")["previous_secret_until"].startswith("2026-11-06T10:00:00")


# #7405: a rotate from the list must know a signing row, which takes a new pasted token
def test_7405_list_item_carries_the_gitlab_method(list_model):
    item = list_model.ScheduleListItem(
        id=2, name="On MR", entity_type="agent", application_id=1, entity_name="A", enabled=True,
        owner={"id": 3}, allowed_actions=[], trigger_kind="webhook", webhook_type="gitlab",
        gitlab_auth_method="signing_token")
    assert item.model_dump(mode="json")["gitlab_auth_method"] == "signing_token"


# AC8 / #6440: a new version never takes another version's webhook secret refs
@pytest.fixture(scope="module")
def rule():
    return load_utils_module(PLUGIN_ROOT / "utils", "trigger_write_rule")


LEGACY_WEBHOOK = {"type": "webhook", "webhook_type": "gitlab", "gitlab_auth_method": "signing_token",
                  "created_by": 7, "webhook_secret": "{{secret.webhook_secret_v4}}",
                  "webhook_signing_secret": "{{secret.webhook_signing_secret_v4}}"}


def test_7398_copy_strips_legacy_webhook_secret_refs(rule):
    out = rule.sanitize_trigger_for_write({"trigger": dict(LEGACY_WEBHOOK), "nodes": 1}, None)
    assert out == {"trigger": {"type": "webhook", "webhook_type": "gitlab", "gitlab_auth_method": "signing_token",
                               "created_by": 7}, "nodes": 1}


def test_7398_save_of_the_same_version_keeps_its_secret_refs(rule):
    stored = dict(LEGACY_WEBHOOK)
    assert rule.sanitize_trigger_for_write({"trigger": dict(LEGACY_WEBHOOK)}, stored)["trigger"] == LEGACY_WEBHOOK


def test_7398_strip_leaves_settings_without_secrets_unchanged(rule):
    for settings in ({}, {"trigger": None}, {"trigger": {"type": "chat_message"}}):
        assert rule.strip_trigger_secrets(settings) == settings
    original = {"trigger": dict(LEGACY_WEBHOOK)}
    rule.strip_trigger_secrets(original)
    assert original == {"trigger": LEGACY_WEBHOOK}  # the input is not mutated
