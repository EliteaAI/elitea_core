"""EL-7011 owner CR 2026-10-21 (test panel log): the run writers keep a small event list on each run.

Contract under test: utils/schedule_claim.py (run_event, finished_event, append_run_events, fail_run) and
utils/schedule_runs.py (record_run_events, finish, block, skip, pause, decision, expiry) on the real
models, in-memory SQLite. The fixtures are the ones of the pre-run writer tests.
"""
import sys
from datetime import UTC, datetime, timedelta

import pytest

# Shared fixtures of the US-09 and US-13 writer tests: pytest injects them by name
from unit.utils.test_7011_schedule_runs_fail import _run, _schedule, runs, session  # noqa: F401
from unit.utils.test_7011_schedule_runs_outcome import sent  # noqa: F401

# ruff: noqa: F811

CLAIM = "plugins.elitea_core.utils.schedule_claim"
SENSITIVE = {"hitl_interrupt": {
    "type": "hitl", "guardrail_type": "sensitive_tool", "toolkit_name": "github", "tool_name": "create_issue",
    "tool_args": {"token": "secret"}}}


def _claim():
    return sys.modules[CLAIM]


def _events(session, run):
    session.refresh(run)
    return run.events or []


def _kinds(session, run):
    return [event["kind"] for event in _events(session, run)]


def test_cr_el7011_an_event_is_small_and_bounded(runs):
    event = _claim().run_event("tool_call", "x" * 1000, "error", toolkit="github", duration_ms=None)

    assert set(event) == {"at", "kind", "text", "level", "toolkit"}  # empty data is left out
    assert (event["kind"], event["level"], len(event["text"])) == ("tool_call", "error", _claim().TEXT_LIMIT)
    assert datetime.fromisoformat(event["at"]).tzinfo is not None


def test_cr_el7011_the_list_keeps_the_first_events_and_always_the_newest(runs, session):
    claim = _claim()
    run = _run(runs, session, _schedule(runs, session))
    claim.append_run_events(session, run.id, *(claim.run_event("note", str(i)) for i in range(250)))
    session.flush()

    events = _events(session, run)
    assert len(events) == claim.EVENT_LIMIT == 200
    assert [e["text"] for e in events[:3]] == ["0", "1", "2"]
    assert (events[-2]["text"], events[-1]["text"]) == ("198", "249")

    claim.append_run_events(session, run.id, claim.finished_event("success"))
    session.flush()
    events = _events(session, run)
    assert (len(events), events[-1]["kind"], events[-2]["text"]) == (200, "finished", "198")


def test_cr_el7011_record_run_events_sets_columns_and_never_raises(runs, session, monkeypatch):
    run = _run(runs, session, _schedule(runs, session))

    runs.record_run_events(1, run.id, _claim().run_event("account", "Account: Ann"), principal_label="Ann")

    assert (_kinds(session, run), run.principal_label) == (["account"], "Ann")

    def broken(_project_id):
        raise RuntimeError("database down")

    monkeypatch.setattr(sys.modules["tools"].db, "get_session", broken)
    runs.record_run_events(1, run.id, _claim().run_event("account", "lost"))  # logged, the run goes on


def test_cr_el7011_a_failed_run_ends_its_log_with_the_reason(runs, session):
    run = _run(runs, session, _schedule(runs, session))

    runs.fail_schedule_run(1, run.id, "owner lost access")

    (event,) = _events(session, run)
    assert (event["kind"], event["level"], event["status"]) == ("finished", "error", "failed")
    assert event["text"] == "Run finished: failed (owner lost access)"


def test_cr_el7011_success_blocked_and_skipped_runs_log_their_end(runs, session, sent):
    schedule = _schedule(runs, session)
    done = _run(runs, session, schedule)
    runs.finish_schedule_run(1, done.id, "success", conversation_id=55)
    blocked = _run(runs, session, schedule)
    runs.block_schedule_run(1, blocked.id, "the monthly budget is used up")
    skipped_id = runs.record_skipped_run(session, schedule.id, "a run is still active")

    assert [(e["text"], e["level"]) for e in _events(session, done)] == [("Run finished: success", "success")]
    assert _events(session, blocked)[-1]["level"] == "warning"
    skipped = session.get(runs.EntityScheduleRun, skipped_id)
    assert [e["text"] for e in _events(session, skipped)] == ["Run finished: skipped (a run is still active)"]


def test_cr_el7011_a_sensitive_pause_logs_the_toolkit_and_tool_but_never_the_arguments(runs, session, sent):
    run = _run(runs, session, _schedule(runs, session))

    runs.pause_schedule_run(1, run.id, SENSITIVE)

    (event,) = _events(session, run)
    assert (event["kind"], event["level"], event["toolkit"], event["tool"]) == (
        "paused", "warning", "github", "create_issue")
    assert event["text"] == "Paused for approval: github · create_issue"
    assert "secret" not in str(event)


def test_a_parallel_pause_names_every_tool_and_keeps_the_first_in_the_fields(runs, session, sent):
    run = _run(runs, session, _schedule(runs, session))
    second = {**SENSITIVE["hitl_interrupt"], "tool_name": "delete_issue"}
    third = {**SENSITIVE["hitl_interrupt"], "tool_name": "close_issue"}

    runs.pause_schedule_run(1, run.id, {"hitl_interrupts": [SENSITIVE["hitl_interrupt"], second, third]})

    (event,) = _events(session, run)
    assert event["text"] == "Paused for approval: github · create_issue, github · delete_issue, github · close_issue"
    assert (event["toolkit"], event["tool"]) == ("github", "create_issue")


def test_a_parallel_pause_names_the_toolkit_of_each_tool_without_arguments(runs, session, sent):
    run = _run(runs, session, _schedule(runs, session))
    second = {"guardrail_type": "sensitive_tool", "toolkit_name": "ADO", "tool_name": "list_indexes",
              "tool_args": {"token": "secret"}}

    runs.pause_schedule_run(1, run.id, {"hitl_interrupts": [SENSITIVE["hitl_interrupt"], second]})

    (event,) = _events(session, run)
    assert event["text"] == "Paused for approval: github · create_issue, ADO · list_indexes"
    assert "secret" not in str(event)


def test_a_parallel_pause_lists_the_first_tool_once_when_both_interrupt_shapes_hold_it(runs, session, sent):
    run = _run(runs, session, _schedule(runs, session))
    first = SENSITIVE["hitl_interrupt"]
    second = {**first, "tool_name": "delete_issue"}

    runs.pause_schedule_run(1, run.id, {"hitl_interrupt": first, "hitl_interrupts": [first, second]})

    (event,) = _events(session, run)
    assert event["text"] == "Paused for approval: github · create_issue, github · delete_issue"


def test_a_pause_with_one_interrupt_shape_only_still_names_its_tool(runs, session, sent):
    run = _run(runs, session, _schedule(runs, session))

    runs.pause_schedule_run(1, run.id, {"hitl_interrupts": [SENSITIVE["hitl_interrupt"]]})

    (event,) = _events(session, run)
    assert event["text"] == "Paused for approval: github · create_issue"


def test_a_long_tool_list_is_cut_by_whole_entries_with_a_count_of_the_hidden_ones(runs, session, sent):
    run = _run(runs, session, _schedule(runs, session))
    tools = [{**SENSITIVE["hitl_interrupt"], "tool_name": f"tool_number_{number:02d}"} for number in range(30)]

    runs.pause_schedule_run(1, run.id, {"hitl_interrupts": tools})

    (event,) = _events(session, run)
    text = event["text"]
    shown = text.removeprefix("Paused for approval: ").rsplit(" (+", 1)[0].split(", ")
    assert len(text) <= _claim().TEXT_LIMIT
    assert shown == [f"github · tool_number_{number:02d}" for number in range(len(shown))]
    assert text.endswith(f" (+{30 - len(shown)} more)") and len(shown) < 30
    assert (event["toolkit"], event["tool"]) == ("github", "tool_number_00")


def test_a_pause_with_tools_that_fit_shows_no_hidden_count(runs, session, sent):
    run = _run(runs, session, _schedule(runs, session))
    tools = [{**SENSITIVE["hitl_interrupt"], "tool_name": f"t{number}"} for number in range(5)]

    runs.pause_schedule_run(1, run.id, {"hitl_interrupts": tools})

    (event,) = _events(session, run)
    assert "more)" not in event["text"] and event["text"].count("github") == 5


def test_a_parallel_pause_skips_an_interrupt_that_is_not_a_sensitive_tool(runs, session, sent):
    run = _run(runs, session, _schedule(runs, session))
    other = {"type": "hitl", "toolkit_name": "other", "tool_name": "other_tool"}

    runs.pause_schedule_run(1, run.id, {"hitl_interrupts": [SENSITIVE["hitl_interrupt"], other]})

    (event,) = _events(session, run)
    assert event["text"] == "Paused for approval: github · create_issue"


def test_cr_el7011_a_human_step_pause_logs_only_the_failed_end(runs, session, sent):
    run = _run(runs, session, _schedule(runs, session))

    runs.pause_schedule_run(1, run.id, {"hitl_interrupt": {"type": "hitl"}, "node_name": "review"})

    assert _kinds(session, run) == ["finished"]


def test_cr_el7011_an_approve_logs_who_decided(runs, session, sent):
    schedule = _schedule(runs, session, created_by=12)
    run = _run(runs, session, schedule)
    runs.pause_schedule_run(1, run.id, SENSITIVE)

    runs.apply_schedule_decision(1, run.id, runs.check_schedule_decision(1, run.id, 12, ["approve"]))

    decision = _events(session, run)[-1]
    assert (run.status, decision["kind"], decision["action"], decision["by"]) == ("running", "decision", "approve", 12)
    assert decision["text"] == "Approved by the schedule owner"


def test_cr_el7011_a_reject_logs_the_decision_then_the_failed_end(runs, session, sent):
    schedule = _schedule(runs, session, created_by=12)
    run = _run(runs, session, schedule)
    runs.pause_schedule_run(1, run.id, SENSITIVE)

    runs.apply_schedule_decision(1, run.id, runs.check_schedule_decision(1, run.id, 12, ["reject"]))

    events = _events(session, run)
    assert [e["kind"] for e in events] == ["paused", "decision", "finished"]
    assert (events[1]["text"], events[2]["text"]) == (
        "Rejected by the schedule owner", "Run finished: failed (rejected by the owner)")
    assert run.status == "failed"


def test_cr_el7011_a_late_decision_logs_nothing(runs, session, sent):
    schedule = _schedule(runs, session, created_by=12)
    run = _run(runs, session, schedule, status="failed")

    with pytest.raises(runs.ScheduleRunNotWaiting):
        runs.apply_schedule_decision(1, run.id, "approve")
    assert _events(session, run) == []


def test_cr_el7011_an_expired_wait_logs_the_failed_end(runs, session, sent):
    run = _run(runs, session, _schedule(runs, session), status="waiting_for_approval")
    run.approval_expires_at = datetime.now(UTC) - timedelta(minutes=1)
    session.flush()

    assert runs.expire_waiting_runs(1, datetime.now(UTC)) == 1

    assert [e["text"] for e in _events(session, run)] == ["Run finished: failed (approval expired)"]
