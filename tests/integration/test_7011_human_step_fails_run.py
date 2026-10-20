"""EL-7011 US-19 R2.1, R2.3: an Ask User pause fails the run once, tells the owner once and frees the slot.

Real: `pause_schedule_run`, `finish_schedule_run`, the run claim and the schedule models, over SQLite.
Faked: the notice helper (through the `world` fixture of the agent run test).

Run via:
    python tests/run_tests.py -m integration -v integration/test_7011_human_step_fails_run.py
"""
import sys

import pytest
from integration.test_7011_agent_schedule_run import OWNER, PKG, PROJECT_ID, world  # noqa: F401 -- pytest fixture

REASON = "the run needs a human answer (step 'Review')"
ASK_USER = {"hitl_interrupt": {"guardrail_type": "clarifying_question", "node_name": "Review"}}


@pytest.fixture
def pause(world, monkeypatch):  # noqa: F811 -- the fixture, extended
    """`world` plus the real pause writer; the one seeded run is running."""
    # The agent schedule also tells the owner's open chats; the world stub has only the notice helper
    monkeypatch.setattr(sys.modules[f"{PKG}.utils.schedule_notifications"], "emit_schedule_run_finished",
                        lambda *a, **k: None, raising=False)
    pause_schedule_run = sys.modules[f"{PKG}.utils.schedule_runs"].pause_schedule_run
    world.pause = lambda interrupt=ASK_USER: pause_schedule_run(PROJECT_ID, 1, interrupt)
    return world


def test_el_7011_r2_1_ask_user_pause_fails_the_run_with_the_step_name(pause):
    """TC-US19-06, TC-US19-07: the run fails at once, the reason names the step, the schedule copies the status."""
    pause.pause()
    run = pause.run_row()
    assert (run.status, run.reason) == ("failed", REASON)
    assert run.finished_at is not None and run.approval_expires_at is None
    assert pause.schedule().last_run_status == "failed"


def test_el_7011_r2_3_owner_gets_one_failed_notice(pause):
    """TC-US19-08, TC-US19-09: one `schedule_run_failed` notice with the reason, none for approval."""
    pause.pause()
    assert pause.notices == [(OWNER, "schedule_run_failed", REASON)]


def test_el_7011_r2_3_second_pause_changes_nothing_and_sends_no_notice(pause):
    """TC-US19-10: the second Ask User interrupt finds a run that no longer runs."""
    pause.pause()
    pause.pause()
    assert pause.run_row().status == "failed"
    assert len(pause.notices) == 1


def test_el_7011_r2_3_failed_run_frees_the_active_run_slot(pause):
    """TC-US19-11, TC-US19-12, TC-US19-13: the active slot is held while the run runs and free after the failure."""
    assert pause.claim("cron") is None
    pause.pause()
    assert pause.claim("cron") is not None
