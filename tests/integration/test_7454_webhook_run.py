"""#7454: a webhook delivery runs end to end through `run_owner_schedule`, pauses on a sensitive tool, and is paused
itself after three governance refusals.

Real: the starter, the run writers, `create_trigger_run_conversation`, the predict helper, the schedule models and the
decision writers, over SQLite tables (the `world` of the US-11 integration test, turned into a webhook row). Faked: the
predict RPC (records its call), the guardrail (masks "SECRET"), the governed preparation and the notice helper.

Run via:
    python tests/run_tests.py -m integration -v integration/test_7454_webhook_run.py
"""
import json
import sys
import types

import pytest
from integration.test_7011_agent_schedule_run import OWNER, PKG, PROJECT_ID, world  # noqa: F401 -- fixture reuse
from unit._tool_spend_loader import load

REFUSED = {"code": "MANDATE_REQUIRED", "error": "This scheduled run needs an approved mandate."}
SENSITIVE = {"hitl_interrupt": {"guardrail_type": "sensitive_tool", "toolkit_name": "github", "tool_name": "merge"}}
BODY = json.dumps({"action": "opened", "token": "SECRET"}).encode()


@pytest.fixture(autouse=True)
def _timing():
    if f"{PKG}.utils.schedule_timing" not in sys.modules:
        load("utils/schedule_timing.py")


@pytest.fixture
def hook(world, monkeypatch):  # noqa: F811 -- the fixture extends the imported one
    """Schedule 1 as a GitHub webhook of the agent; `deliver(answer)` claims a webhook run and starts it with BODY."""
    monkeypatch.setitem(sys.modules, f"{PKG}.utils.input_guardrail", types.SimpleNamespace(
        redacted_input=lambda text: text.replace("SECRET", "[REDACTED]")))
    load("utils/webhook_input.py", fresh=True)
    starter = sys.modules[f"{PKG}.utils.schedule_run_starter"]
    state = types.SimpleNamespace(governed=False, answers=[], prepared=[])
    mandate = types.ModuleType(f"{PKG}.utils.mandate_admission")
    mandate.prepare_schedule_run = lambda **kwargs: (
        state.prepared.append(kwargs), {"unattended": True} if state.governed else {})[1]
    monkeypatch.setitem(sys.modules, mandate.__name__, mandate)
    real_predict = starter.execute_pipeline_via_predict_sio
    monkeypatch.setattr(starter, "execute_pipeline_via_predict_sio", lambda **kwargs: (
        real_predict(**kwargs), state.answers.pop(0) if state.answers else {"task_id": "task-1"})[1])
    monkeypatch.setattr(starter, "finish_trigger_run_refused", lambda *_args: None)
    monkeypatch.setattr(starter, "run_prerun_check", lambda _project, schedule, _run: types.SimpleNamespace(
        ok=True, owner_id=schedule.created_by))
    with world.edit_schedule() as row:
        row.trigger_kind, row.webhook_type, row.cron, row.timezone, row.next_run = "webhook", "github", None, None, None
    world.set_run_status("success")
    # the pause reads the approval window; reject drops the run's checkpoints over the event node
    monkeypatch.setattr(sys.modules["tools"].this, "module", types.SimpleNamespace(
        descriptor=types.SimpleNamespace(config={}), event_node=types.SimpleNamespace(emit=lambda *_a, **_k: None)),
        raising=False)

    def deliver(answer=None, governed=False):
        state.governed = governed
        if answer is not None:
            state.answers.append(answer)
        run_id = world.claim("webhook")
        starter.run_owner_schedule(PROJECT_ID, 1, run_id, payload=BODY, delivery="gh-7")
        return run_id

    return types.SimpleNamespace(world=world, deliver=deliver, state=state,
                                 runs=sys.modules[f"{PKG}.utils.schedule_runs"])


def _run(world, run_id):
    schedules = sys.modules[f"{PKG}.models.entity_schedule"]
    with sys.modules["tools"].db.get_session(PROJECT_ID) as session:
        return session.get(schedules.EntityScheduleRun, run_id)


def test_a_delivery_runs_as_the_owner_with_a_webhook_conversation_and_redacted_preview(hook):
    run_id = hook.deliver()
    [conversation] = hook.world.conversations()
    assert conversation.name == "Webhook run: Daily report"
    assert conversation.meta["execution_type"] == "webhook" and conversation.meta["webhook_type"] == "github"
    message = hook.world.first_message()
    assert message.startswith("[Webhook triggered: github]") and "SECRET" not in message and "[REDACTED]" in message
    [predict] = hook.world.predictions
    assert predict["data"]["user_input"].startswith("Summarize open bugs\n\n```json\n")
    assert '"token": "SECRET"' in predict["data"]["user_input"]   # the run gets the whole body
    run = _run(hook.world, run_id)
    assert (run.trigger, run.status, run.conversation_id) == ("webhook", "running", conversation.id)
    started = run.events[0]
    assert (started["kind"], started["webhook_type"], started["delivery_id"]) == ("started", "github", "gh-7")
    assert hook.state.prepared[0]["trigger_kind"] == "webhook"


@pytest.mark.parametrize("action, status", [("approve", "running"), ("reject", "failed")])
def test_a_sensitive_tool_pauses_the_delivery_and_the_owner_decides(hook, action, status):
    run_id = hook.deliver()
    run = _run(hook.world, run_id)
    hook.runs.pause_schedule_run(PROJECT_ID, run_id, SENSITIVE, run.conversation_id, OWNER)
    assert _run(hook.world, run_id).status == "waiting_for_approval"
    decision = hook.runs.check_schedule_decision(PROJECT_ID, run_id, OWNER, [action])
    hook.runs.apply_schedule_decision(PROJECT_ID, run_id, decision)
    assert _run(hook.world, run_id).status == status


def test_three_refused_deliveries_in_a_row_pause_the_webhook(hook):
    for _ in range(3):
        hook.deliver(REFUSED, governed=True)
    row = hook.world.schedule()
    assert (row.enabled, row.disabled_reason) == (False, "governance_refused")
