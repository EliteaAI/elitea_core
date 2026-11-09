"""#7455: the owner's sample body runs end to end through the delivery path, with the predict RPC mocked.

Reuses the #7454 `hook` fixture (a GitHub webhook row of the agent over SQLite tables, real starter and run
writers). The run row is claimed with trigger `test`, as the run route does through `admit_webhook_run`.

Run via:
    python tests/run_tests.py -m integration -v integration/test_7455_sample_run.py
"""
import json
import sys

from integration.test_7011_agent_schedule_run import PKG, PROJECT_ID, world  # noqa: F401 -- fixture reuse
from integration.test_7454_webhook_run import _run, _timing, hook  # noqa: F401 -- fixture reuse

SAMPLE = json.dumps({"action": "opened", "token": "SECRET"}).encode()


def _sample(hook):  # noqa: F811
    run_id = hook.world.claim("test")
    starter = sys.modules[f"{PKG}.utils.schedule_run_starter"]
    starter.run_owner_schedule(PROJECT_ID, 1, run_id, payload=SAMPLE)
    return run_id


def test_a_sample_body_run_is_a_test_run_on_the_delivery_path(hook):  # noqa: F811
    run_id = _sample(hook)
    run = _run(hook.world, run_id)
    [conversation] = hook.world.conversations()
    assert (run.trigger, run.status, run.conversation_id) == ("test", "running", conversation.id)
    assert conversation.name == "Webhook run: Daily report"
    assert conversation.meta["execution_type"] == "webhook"   # trigger_source: analytics count it as automated
    [predict] = hook.world.predictions
    assert '"token": "SECRET"' in predict["data"]["user_input"]   # the run gets the whole body
    assert hook.state.prepared[0]["trigger_kind"] == "webhook"
    started = run.events[0]
    assert "Sample body by the owner" in started["text"] and started["sample"] is True


def test_the_sample_body_is_not_stored(hook):  # noqa: F811
    run_id = _sample(hook)
    run = _run(hook.world, run_id)
    # The run row keeps only its log; the conversation keeps only the redacted preview
    assert "SECRET" not in json.dumps(run.events) and "SECRET" not in (run.reason or "")
    message = hook.world.first_message()
    assert "SECRET" not in message and "[REDACTED]" in message
