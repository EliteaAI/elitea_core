"""EL-7011 US-19 (R2.1 to R2.4): a schedule run that pauses for a human answer fails.

Contract under test: utils/schedule_runs.py pause_schedule_run on the real models, in-memory SQLite.
Harnesses come from the sibling 7011 test files.
"""
import pytest
from unit.utils.test_7011_schedule_runs_fail import _run, _schedule, runs, session  # noqa: F401
from unit.utils.test_7011_schedule_runs_outcome import sent  # noqa: F401

# ruff: noqa: F811

PLAIN = "the run needs a human answer"


def _pause(runs, session, interrupt, status="running"):
    schedule = _schedule(runs, session, created_by=12)
    run = _run(runs, session, schedule, status=status)
    runs.pause_schedule_run(1, run.id, interrupt)
    session.refresh(run)
    session.refresh(schedule)
    return run, schedule


@pytest.mark.parametrize("interrupt, reason", [
    ({"hitl_interrupt": {"guardrail_type": "clarifying_question", "node_name": "Review"}},
     PLAIN + " (step 'Review')"),  # TC-US19-06
    ({"hitl_interrupt": {"guardrail_type": "hitl", "node_name": "Approve"}},
     PLAIN + " (step 'Approve')"),  # TC-US19-07
    ({"hitl_interrupt": {"guardrail_type": "sensitive_tool_off", "toolkit_name": "SharePoint",
                         "node_name": "mcp_auth_guard"}},
     PLAIN + " (toolkit 'SharePoint')"),  # TC-US19-08: the toolkit wins over the node
    ({"toolkit_name": "SharePoint"}, PLAIN + " (toolkit 'SharePoint')"),  # TC-US19-08: top level
    ({"hitl_interrupts": [{"guardrail_type": "hitl"}, {"node_name": "Second"}]},
     PLAIN + " (step 'Second')"),  # R2.4 over a parallel pause
    ({"type": "interrupt"}, PLAIN),  # TC-US19-11: no name
])
def test_el_7011_r2_1_to_r2_4_a_human_step_pause_fails_the_run_and_names_the_step(
        runs, session, sent, interrupt, reason):
    run, schedule = _pause(runs, session, interrupt)

    assert (run.status, run.reason) == ("failed", reason)
    assert run.finished_at is not None
    assert schedule.last_run_status == "failed"


def test_el_7011_r2_3_a_human_step_failure_sends_one_failed_notice(runs, session, sent):
    run, _ = _pause(runs, session, {"hitl_interrupt": {"guardrail_type": "clarifying_question"}})

    assert sent.notices == [(12, "schedule_run_failed", run.id)]


def test_el_7011_r2_1_a_sensitive_tool_pause_still_waits_for_approval(runs, session, sent):
    """TC-US19-09."""
    run, _ = _pause(runs, session, {"hitl_interrupt": {"guardrail_type": "sensitive_tool", "node_name": "N"}})

    assert (run.status, run.reason) == ("waiting_for_approval", None)
    assert sent.notices == [(12, "schedule_approval_needed", run.id)]


def test_el_7011_r2_1_a_pause_of_a_run_that_is_not_running_changes_nothing(runs, session, sent):
    """TC-US19-10."""
    run, _ = _pause(runs, session, {"hitl_interrupt": {"guardrail_type": "clarifying_question"}}, status="success")

    assert (run.status, run.reason) == ("success", None)
    assert sent.notices == []
