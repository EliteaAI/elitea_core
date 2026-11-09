"""#7454: a webhook delivery through the owner run starter -- input shaping, preview, conversation, run log.

The starter and utils/webhook_input.py are real; the run-starter fakes are the #7011 ones
(test_7011_run_starter_agent), extended with a webhook row. The guardrail is a stand-in that masks
the word "SECRET", so the test can see that the preview is redacted before it is cut.
"""
import json
import sys
from types import SimpleNamespace

import pytest

from unit._tool_spend_loader import load
from unit.utils.test_7011_run_starter_agent import (  # noqa: F401 -- the shared fakes
    AGENT_ID, CONVERSATION_ID, OWNER, PKG, PROJECT_ID, RUN_ID, SCHEDULE_ID, world as base_world,
)

GUARDRAIL = SimpleNamespace(redacted_input=lambda text: text.replace("SECRET", "[REDACTED]"))


@pytest.fixture
def webhook_input(monkeypatch):
    monkeypatch.setitem(sys.modules, f"{PKG}.utils.input_guardrail", GUARDRAIL)
    return load("utils/webhook_input.py", fresh=True)


@pytest.fixture
def world(base_world, webhook_input, monkeypatch):
    base_world.schedule.trigger_kind, base_world.schedule.webhook_type = "webhook", "github"
    base_world.events = []
    base_world.module.TriggerType = SimpleNamespace(SCHEDULED="scheduled", SCHEDULE_MANUAL="schedule_manual",
                                                     WEBHOOK="webhook")
    base_world.prepared = []
    sys.modules[f"{PKG}.utils.mandate_admission"].prepare_schedule_run = (
        lambda **kwargs: base_world.prepared.append(kwargs) or {})
    base_world.module.run_event = lambda kind, text, *args, **fields: {"kind": kind, "text": text, **fields}
    base_world.module.record_run_events = lambda _pid, _rid, *events, **_kw: base_world.events.extend(events)
    base_world.module._load = _with_trigger(base_world.module._load, "webhook")
    return base_world


def _with_trigger(load_fn, trigger):
    def wrapped(*args):
        loaded = load_fn(*args)
        if loaded is not None:
            loaded.trigger = trigger
        return loaded
    return wrapped


def _deliver(world, body=b'{"action": "opened"}', delivery="gh-1"):
    world.module.run_owner_schedule(PROJECT_ID, SCHEDULE_ID, RUN_ID, payload=body, delivery=delivery)


# --- input shaping (pure) -----------------------------------------------------------------------------

@pytest.mark.parametrize("payload, parsed", [
    (b'{"a": 1}', {"a": 1}),
    (b'[1, 2]', [1, 2]),
    (b'"text"', "text"),
    (b'not json', {"raw_payload": "not json"}),
    (b'', {"raw_payload": ""}),
    (b'\xff{"a"', {"raw_payload": '�{"a"'}),   # not UTF-8: kept readable, never a crash
])
def test_a_body_is_parsed_or_wrapped_as_raw_payload(webhook_input, payload, parsed):
    assert webhook_input.parse_body(payload) == json.dumps(parsed, indent=2)


@pytest.mark.parametrize("entity_type, prompt, expected", [
    ("pipeline", "ignored", '{\n  "a": 1\n}'),
    ("agent", "", '{\n  "a": 1\n}'),
    ("agent", "  ", '{\n  "a": 1\n}'),
    ("agent", "Review it", 'Review it\n\n```json\n{\n  "a": 1\n}\n```'),
])
def test_the_input_per_entity_type(webhook_input, entity_type, prompt, expected):
    assert webhook_input.shape_input(entity_type, prompt, json.dumps({"a": 1}, indent=2)) == expected


def test_the_preview_is_redacted_before_it_is_cut_to_500(webhook_input):
    # The secret straddles the 500 mark: a cut first would leave "SEC" unmatched
    text = "x" * 497 + "SECRET" + "y" * 100
    preview = webhook_input.payload_preview(text)
    assert "SEC" not in preview and preview.endswith("...")
    assert len(preview) == 503 and preview.startswith("x" * 497 + "[RE")


def test_a_short_preview_is_not_cut(webhook_input):
    assert webhook_input.payload_preview("short") == "short"


# --- the run ------------------------------------------------------------------------------------------

def test_an_agent_delivery_gets_the_prompt_and_the_body_and_a_webhook_conversation(world):
    _deliver(world)
    created, predicted = world.call("create")[1], world.call("predict")[1]
    assert created["conversation_name"] == "Webhook run: Daily report"
    assert created["trigger_type"] == "webhook"
    assert created["extra_meta"] == {"schedule_id": SCHEDULE_ID, "schedule_run_id": RUN_ID,
                                     "execution_type": "webhook", "webhook_type": "github"}
    assert created["trigger_message"].startswith("[Webhook triggered: github]\n\nPayload:\n```json\n")
    assert predicted["user_input"] == 'Summarize open bugs\n\n```json\n{\n  "action": "opened"\n}\n```'
    assert predicted["trigger_source"] == "webhook"


@pytest.mark.parametrize("run_mode", [None, "entry_point"])
def test_a_pipeline_delivery_gets_the_whole_body_whatever_its_run_mode(world, run_mode):
    world.schedule.entity_type, world.schedule.run_mode = "pipeline", run_mode
    _deliver(world, body=b"plain text")
    assert json.loads(world.call("predict")[1]["user_input"]) == {"raw_payload": "plain text"}


def test_the_trigger_message_is_the_redacted_preview_and_the_input_is_whole(world):
    _deliver(world, body=json.dumps({"token": "SECRET", "pad": "z" * 900}).encode())
    message, user_input = world.call("create")[1]["trigger_message"], world.call("predict")[1]["user_input"]
    assert "SECRET" not in message and "[REDACTED]" in message and "...\n```" in message
    assert "SECRET" in user_input   # the run gets the body; only the stored copy is redacted


def test_the_started_event_names_trigger_type_and_delivery(world):
    _deliver(world)
    started = world.events[0]
    assert started["kind"] == "started"
    assert (started["trigger"], started["webhook_type"], started["delivery_id"]) == ("webhook", "github", "gh-1")
    assert "trigger: webhook · type: github · delivery gh-1" in started["text"]


def test_a_delivery_without_an_id_says_none(world):
    _deliver(world, delivery=None)
    assert "delivery none" in world.events[0]["text"]


def test_governance_is_prepared_as_a_webhook(world):
    _deliver(world)
    assert world.prepared[0]["trigger_kind"] == "webhook"


def test_a_resumed_webhook_run_without_its_body_fails_readably(world):
    # Approve after a pre-run park restarts the run without the body, which is never kept
    world.module.run_owner_schedule(PROJECT_ID, SCHEDULE_ID, RUN_ID)
    assert "predict" not in world.names() and "create" not in world.names()
    assert world.call("fail") == ("fail", PROJECT_ID, RUN_ID, world.module.BODY_GONE)


def test_a_cron_run_keeps_its_schedule_path(world):
    world.module._load = _with_trigger(world.module._load, "cron")
    world.module.run_owner_schedule(PROJECT_ID, SCHEDULE_ID, RUN_ID)
    created = world.call("create")[1]
    assert created["conversation_name"] == "Scheduled run: Daily report"
    assert created["trigger_type"] == "scheduled" and "webhook_type" not in created["extra_meta"]
    assert world.prepared[0]["trigger_kind"] == "cron"
    assert "delivery" not in world.events[0]["text"]


# A signed body may be hostile: nesting json.loads rejects, or accepts but json.dumps cannot write back
@pytest.mark.parametrize("depth", [3000, 50000])
def test_a_too_deeply_nested_body_is_kept_as_raw_payload(webhook_input, depth):
    payload = ("[" * depth + "]" * depth).encode()
    assert json.loads(webhook_input.parse_body(payload)) == {"raw_payload": payload.decode()}


def test_a_deeply_nested_delivery_runs_with_the_raw_body(world):
    _deliver(world, body=b"[" * 3000 + b"]" * 3000)
    fenced = world.call("predict")[1]["user_input"].split("```json\n", 1)[1].rsplit("\n```", 1)[0]
    assert json.loads(fenced) == {"raw_payload": "[" * 3000 + "]" * 3000}


def test_a_body_the_run_cannot_shape_fails_the_run_with_a_reason(world, monkeypatch):
    monkeypatch.setattr(world.module, "_webhook_input", lambda *args: (_ for _ in ()).throw(KeyError("name")))
    _deliver(world)
    assert "create" not in world.names() and "predict" not in world.names()
    assert world.call("fail") == ("fail", PROJECT_ID, RUN_ID, world.module.BODY_UNREADABLE)


def test_only_a_bounded_window_is_redacted_for_the_preview(webhook_input, monkeypatch):
    # A 512 KB body must not run the guardrail over the whole indented text just to keep 500 characters
    seen = []
    monkeypatch.setattr(webhook_input, "redacted_input", lambda text: seen.append(len(text)) or text)
    preview = webhook_input.payload_preview("x" * 1_000_000)
    assert seen == [webhook_input.REDACT_WINDOW] and preview == "x" * 500 + "..."
