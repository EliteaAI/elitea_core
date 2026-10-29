"""EL-7011 US-13: final status, pause, skip, block and stale writers of schedule runs, with their notices.

Contract under test: utils/schedule_runs.py (finish_schedule_run, pause_schedule_run, record_skipped_run,
block_schedule_run, fail_stale_runs) on the real models, in-memory SQLite. The notice helpers of US-06 are a
recording stand-in. The fixtures are the ones of the pre-run writer tests.
"""
import sys
import types
from datetime import UTC, datetime, timedelta

import pytest

# Shared fixtures of the fail_schedule_run tests: pytest injects them by name
from unit.utils.test_7011_schedule_runs_fail import _run, _schedule, runs, session  # noqa: F401

# ruff: noqa: F811

NOW = datetime(2026, 10, 19, 12, 0, tzinfo=UTC)
NOTICES = "plugins.elitea_core.utils.schedule_notifications"


@pytest.fixture
def sent(monkeypatch):
    """Everything the writers send: notices as (owner, event, run id) and room events as (owner, run id, status)."""
    calls = types.SimpleNamespace(notices=[], emitted=[])
    stand_in = types.ModuleType(NOTICES)
    stand_in.notify_schedule_event = lambda project_id, owner_id, event_type, schedule, run=None, extra=None: (
        calls.notices.append((owner_id, str(event_type), run.id)))
    stand_in.emit_schedule_run_finished = lambda owner_id, schedule_id, run_id, status, entity_id, conversation_id: (
        calls.emitted.append((owner_id, run_id, status, entity_id, conversation_id)))
    monkeypatch.setitem(sys.modules, NOTICES, stand_in)
    return calls


def test_el_7011_r1_2_finish_schedule_run_sets_success_and_the_end(runs, session, sent):
    schedule = _schedule(runs, session)
    run = _run(runs, session, schedule)

    assert runs.finish_schedule_run(1, run.id, "success", conversation_id=55) is True

    session.refresh(run)
    assert (run.status, run.reason, run.conversation_id) == ("success", None, 55)
    assert run.finished_at is not None


def test_el_7011_r1_3_finish_schedule_run_sets_failed_with_the_reason(runs, session, sent):
    schedule = _schedule(runs, session)
    run = _run(runs, session, schedule)

    runs.finish_schedule_run(1, run.id, "failed", "Tool failed", conversation_id=55)

    session.refresh(run)
    assert (run.status, run.reason) == ("failed", "Tool failed")


def test_el_7011_r1_5_finish_schedule_run_copies_status_and_conversation_to_the_schedule(runs, session, sent):
    schedule = _schedule(runs, session)
    run = _run(runs, session, schedule)

    runs.finish_schedule_run(1, run.id, "success", conversation_id=55)

    session.refresh(schedule)
    assert (schedule.last_run_status, schedule.last_conversation_id) == ("success", 55)


def test_el_7011_r1_4_finish_schedule_run_leaves_a_finished_run_and_sends_nothing(runs, session, sent):
    schedule = _schedule(runs, session, last_run_status="failed")
    run = _run(runs, session, schedule, status="failed")

    assert runs.finish_schedule_run(1, run.id, "success", conversation_id=55) is False

    session.refresh(run)
    session.refresh(schedule)
    assert (run.status, schedule.last_run_status, schedule.last_conversation_id) == ("failed", "failed", None)
    assert (sent.notices, sent.emitted) == ([], [])


def test_el_7011_r4_1_a_failed_run_notifies_the_owner_and_the_owner_room(runs, session, sent):
    schedule = _schedule(runs, session, created_by=12)
    run = _run(runs, session, schedule)

    runs.finish_schedule_run(1, run.id, "failed", "Tool failed", conversation_id=55)

    assert sent.notices == [(12, "schedule_run_failed", run.id)]
    assert sent.emitted == [(12, run.id, "failed", 1, 55)]


def test_el_7011_r4_2_a_successful_run_sends_the_success_notice(runs, session, sent):
    schedule = _schedule(runs, session, created_by=12)
    run = _run(runs, session, schedule)

    runs.finish_schedule_run(1, run.id, "success", conversation_id=55)

    assert sent.notices == [(12, "schedule_run_succeeded", run.id)]


def test_el_7011_r4_4_only_an_agent_run_emits_the_room_event(runs, session, sent):
    schedule = _schedule(runs, session, entity_type="pipeline")
    run = _run(runs, session, schedule)

    runs.finish_schedule_run(1, run.id, "success", conversation_id=55)

    assert len(sent.notices) == 1
    assert sent.emitted == []


def test_el_7011_r5_2_pause_schedule_run_fails_the_run_that_waits_for_input(runs, session, sent):
    schedule = _schedule(runs, session, created_by=12)
    run = _run(runs, session, schedule)

    runs.pause_schedule_run(1, run.id, {"type": "interrupt"})

    session.refresh(run)
    session.refresh(schedule)
    assert (run.status, run.reason) == ("failed", "the run needs a human answer")
    assert schedule.last_run_status == "failed"
    assert sent.notices == [(12, "schedule_run_failed", run.id)]


def test_el_7011_r2_4_record_skipped_run_inserts_a_finished_run_and_copies_the_status(runs, session, sent):
    schedule = _schedule(runs, session)
    active = _run(runs, session, schedule)

    skipped_id = runs.record_skipped_run(session, schedule.id, "the previous run is still running")

    skipped = session.get(runs.EntityScheduleRun, skipped_id)
    session.refresh(active)
    session.refresh(schedule)
    assert (skipped.trigger, skipped.status, skipped.conversation_id) == ("cron", "skipped", None)
    assert skipped.reason == "the previous run is still running"
    assert skipped.finished_at is not None
    assert schedule.last_run_status == "skipped"
    assert (active.status, active.reason, active.finished_at) == ("running", None, None)
    assert (sent.notices, sent.emitted) == ([], [])


def test_el_7011_r2_4_block_schedule_run_sets_blocked_and_copies_the_status(runs, session, sent):
    schedule = _schedule(runs, session)
    run = _run(runs, session, schedule)

    runs.block_schedule_run(1, run.id, "budget closed")

    session.refresh(run)
    session.refresh(schedule)
    assert (run.status, run.reason) == ("blocked", "budget closed")
    assert run.finished_at is not None
    assert schedule.last_run_status == "blocked"
    assert (sent.notices, sent.emitted) == ([], [])


def test_el_7011_r2_4_block_schedule_run_leaves_a_finished_run_as_it_is(runs, session, sent):
    schedule = _schedule(runs, session, last_run_status="success")
    run = _run(runs, session, schedule, status="success")

    runs.block_schedule_run(1, run.id, "budget closed")

    session.refresh(run)
    session.refresh(schedule)
    assert (run.status, run.reason, schedule.last_run_status) == ("success", None, "success")


def test_el_7011_r3_2_fail_stale_runs_fails_the_quiet_run_with_the_reason_and_a_notice(runs, session, sent):
    schedule = _schedule(runs, session, created_by=12)
    run = _run(runs, session, schedule)
    run.started_at = NOW - timedelta(hours=5)
    run.resumed_at = NOW - timedelta(hours=3)

    assert runs.fail_stale_runs(1, NOW, 120) == 1

    session.refresh(run)
    session.refresh(schedule)
    assert (run.status, run.reason) == ("failed", "the run did not finish in time")
    assert schedule.last_run_status == "failed"
    assert sent.notices == [(12, "schedule_run_failed", run.id)]


def test_el_7011_r3_2_fail_stale_runs_keeps_a_resumed_or_fresh_run_and_a_finished_one(runs, session, sent):
    resumed = _run(runs, session, _schedule(runs, session, name="S1", created_by=1))
    resumed.started_at, resumed.resumed_at = NOW - timedelta(hours=5), NOW - timedelta(minutes=10)
    fresh = _run(runs, session, _schedule(runs, session, name="S2", created_by=2))
    fresh.started_at = NOW - timedelta(minutes=119)
    done = _run(runs, session, _schedule(runs, session, name="S3", created_by=3), status="success")
    done.started_at = NOW - timedelta(hours=5)

    assert runs.fail_stale_runs(1, NOW, 120) == 0

    for row, status in ((resumed, "running"), (fresh, "running"), (done, "success")):
        session.refresh(row)
        assert row.status == status
    assert sent.notices == []


def test_el_7011_r4_3_announce_skips_a_run_whose_status_has_no_notice(runs, session, sent):
    """The status moved on after the commit (here: still running); no notice and no KeyError."""
    schedule = _schedule(runs, session, created_by=12)
    run = _run(runs, session, schedule)

    runs._announce(1, session, [run.id])

    assert (sent.notices, sent.emitted) == ([], [])


TOOL_ERROR = "GitHub \u00b7 create_pull_request: missing title"


def test_a_failed_run_with_a_short_tool_error_stores_the_reason_and_sends_the_failed_notice(runs, session, sent):
    schedule = _schedule(runs, session, created_by=12)
    run = _run(runs, session, schedule)

    runs.finish_schedule_run(1, run.id, "failed", runs.run_failure_reason(TOOL_ERROR), conversation_id=55)

    session.refresh(run)
    session.refresh(schedule)
    assert (run.status, schedule.last_run_status) == ("failed", "failed")
    assert run.reason == f"The run failed: {TOOL_ERROR}"
    assert sent.notices == [(12, "schedule_run_failed", run.id)]
    assert run.events[-1]["kind"] == "finished"
    assert run.events[-1]["text"] == f"Run finished: failed ({run.reason})"


def test_a_run_finished_without_a_reason_ends_success_and_sends_the_succeeded_notice(runs, session, sent):
    schedule = _schedule(runs, session, created_by=12)
    run = _run(runs, session, schedule)

    runs.finish_schedule_run(1, run.id, "success", conversation_id=55)

    session.refresh(run)
    session.refresh(schedule)
    assert (run.status, run.reason, schedule.last_run_status) == ("success", None, "success")
    assert sent.notices == [(12, "schedule_run_succeeded", run.id)]
    assert run.events[-1]["text"] == "Run finished: success"


@pytest.fixture
def reasons(runs):
    """schedule_runs with the real error_text helpers (a plain module, nothing to stub)."""
    return runs


TRACEBACK = (
    "Traceback (most recent call last):\n"
    '  File "/data/plugins/indexer_worker/agent_common.py", line 412, in execution_error\n'
    "    raise RuntimeError(text)\n"
    "RuntimeError: Pipeline node 'printer' failed\n"
)


GENERIC = "The run failed. Open the run log or ask an administrator for details."
WORKER_SENTENCE = "The step 'create_issue' of GitHub could not be completed. Open the run log or ask an administrator for details."


@pytest.mark.parametrize("error", [
    TRACEBACK,
    "OSError: cannot open /data/plugins/elitea_core/utils/x.py for reading",
    "langchain_core.tools.base.ToolException: Node `x` has tool 'y' which is not found",
    "ValueError: bad input",
    "see C:\\work\\elitea\\x.py",
    "failed in agent_common.py line 4",
    "create_pull_request: 4 validation errors for CreatePR title Field required [type=missing, input_value={}, input_type=dict]",
])
def test_run_failure_reason_hides_an_engineer_text_behind_a_generic_sentence(reasons, error):
    assert reasons.run_failure_reason(error) == GENERIC


def test_run_failure_reason_keeps_a_plain_sentence(reasons):
    assert reasons.run_failure_reason(WORKER_SENTENCE) == f"The run failed: {WORKER_SENTENCE}"
    assert reasons.run_failure_reason(TOOL_ERROR) == f"The run failed: {TOOL_ERROR}"


def test_run_failure_reason_removes_control_and_direction_characters(reasons):
    assert reasons.run_failure_reason("The step is bad\u202e now\x00 here\u200b") == "The run failed: The step is bad now here"


def test_run_failure_reason_neutralises_links_and_tags(reasons):
    reason = reasons.run_failure_reason("Click [here](https://evil.example) <img src=x onerror=alert(1)> `now`")
    assert not any(ch in reason for ch in "[]<>`")


def test_run_failure_reason_is_capped_at_300_characters(reasons):
    reason = reasons.run_failure_reason("The step is slow " + "x" * 1000)
    assert len(reason) == 300
    assert reason.startswith("The run failed: The step is slow ") and reason.endswith("...")


@pytest.mark.parametrize("error", [None, "", "   \n  "])
def test_run_failure_reason_without_error_text(reasons, error):
    assert reasons.run_failure_reason(error) == "The run failed"


@pytest.mark.parametrize(("error", "reason"), [
    ("The page https://host/a/b is gone", "The page https://host/a/b is gone"),
    ("The call to https://host/a?token=x&b=2 was refused", "The call to https://host/a?<query> was refused"),
    ("The date 12/05/2026 is not valid", "The date 12/05/2026 is not valid"),
])
def test_run_failure_reason_hides_only_query_strings(reasons, error, reason):
    assert reasons.run_failure_reason(error) == f"The run failed: {reason}"


from plugins.elitea_core.utils.error_text import public_event, public_reason  # noqa: E402

OLD_REASON = "The run failed: langchain_core.tools.base.ToolException: Node `Calltool` has tool 'get_issue' which is not found"


def test_a_reason_stored_before_the_generic_text_is_shown_as_the_generic_text():
    assert public_reason(OLD_REASON) == GENERIC
    assert public_reason(f"The run failed: {WORKER_SENTENCE}") == f"The run failed: {WORKER_SENTENCE}"
    assert public_reason(None) is None


def test_a_stored_finished_event_with_an_engineer_text_is_shown_as_the_generic_text():
    old = {"kind": "finished", "at": "2026-10-29T20:20:00Z", "text": f"Run finished: failed ({OLD_REASON})"}
    assert public_event(old)["text"] == f"Run finished: failed ({GENERIC})"
    assert public_event(old)["at"] == old["at"]
    plain = {"kind": "finished", "at": None, "text": "Run finished: success"}
    assert public_event(plain) == plain
    other = {"kind": "log", "at": None, "text": "ToolException in a step"}
    assert public_event(other) == other


def test_a_two_line_error_keeps_the_last_line_when_it_is_plain(reasons):
    assert reasons.run_failure_reason("The step stopped\nThe title is missing") == "The run failed: The title is missing"


def test_a_two_line_error_with_an_engineer_line_anywhere_in_the_last_line_is_generic(reasons):
    assert reasons.run_failure_reason("The step stopped\nValueError: boom") == GENERIC


def test_the_worker_sentence_goes_from_the_end_event_to_the_stored_run(runs, session, sent):
    schedule = _schedule(runs, session, created_by=12)
    run = _run(runs, session, schedule)

    runs.finish_schedule_run(1, run.id, "failed", runs.run_failure_reason(WORKER_SENTENCE), conversation_id=55)

    session.refresh(run)
    assert run.reason == f"The run failed: {WORKER_SENTENCE}"
    assert run.events[-1]["text"] == f"Run finished: failed ({run.reason})"
