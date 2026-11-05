"""The owner or a project admin decides on a run that waits for approval.

Real: `check_schedule_decision` and `apply_schedule_decision` of `utils/schedule_runs.py` and the schedule models, over SQLite tables.
The harness is the `world` fixture of the US-11 integration test.

Run via:
    python tests/run_tests.py -m integration -v integration/test_7011_approval_decision.py
"""
import sys
from datetime import UTC, datetime, timedelta

import pytest
from integration.test_7011_agent_schedule_run import OWNER, PKG, PROJECT_ID, world  # noqa: F401 -- fixture reuse

ADMIN = 99


@pytest.fixture
def waiting(world):  # noqa: F811 -- the fixture extends the imported one
    """Run 1 waits for approval with an expiry; the real decision writer."""
    world.set_run_status("waiting_for_approval")
    schedules = sys.modules[f"{PKG}.models.entity_schedule"]
    with sys.modules["tools"].db.get_session(PROJECT_ID) as session:
        session.get(schedules.EntityScheduleRun, 1).approval_expires_at = datetime.now(UTC) + timedelta(hours=24)
        session.commit()
    runs = sys.modules[f"{PKG}.utils.schedule_runs"]
    return type("Waiting", (), {"world": world, "runs": runs})


@pytest.fixture
def project_admins(waiting, monkeypatch):
    """The users the admin role lookup answers true for; the rest of the rights module is real."""
    admins = set()
    rights = sys.modules[f"{PKG}.utils.schedule_rights"]
    monkeypatch.setattr(rights, "is_project_admin", lambda project_id, user_id: user_id in admins)
    return admins


def _decide(runs, caller, action):
    """The decision path of continue_predict: the check, then the write."""
    runs.apply_schedule_decision(PROJECT_ID, 1, runs.check_schedule_decision(PROJECT_ID, 1, caller, [action]))


# R2.1: the owner approves, the run goes on and the expiry is cleared
def test_7011_r2_1_the_owner_approve_sets_running_and_clears_the_expiry(waiting):
    _decide(waiting.runs, OWNER, "approve")

    run = waiting.world.run_row()
    assert (run.status, run.approval_expires_at, run.finished_at) == ("running", None, None)
    assert run.resumed_at is not None


# R2.2: the owner rejects, the run fails with the reason
def test_7011_r2_2_the_owner_reject_fails_the_run_with_the_reason(waiting):
    _decide(waiting.runs, OWNER, "reject")

    run = waiting.world.run_row()
    assert (run.status, run.reason, run.finished_at is not None) == ("failed", "rejected", True)
    assert waiting.world.schedule().last_run_status == "failed"


# A user who is neither the owner nor a project admin is refused and the run keeps waiting
@pytest.mark.parametrize("caller", [ADMIN, OWNER + 1])
@pytest.mark.parametrize("action", ["approve", "reject"])
def test_another_user_is_refused_and_the_run_keeps_waiting(waiting, project_admins, caller, action):
    with pytest.raises(waiting.runs.ScheduleDecisionForbidden):
        _decide(waiting.runs, caller, action)

    assert waiting.world.run_row().status == "waiting_for_approval"


@pytest.mark.parametrize("action, status", [("approve", "running"), ("reject", "failed")])
def test_a_project_admin_who_is_not_the_owner_decides_and_the_log_names_the_admin(waiting, project_admins, action, status):
    project_admins.add(ADMIN)

    waiting.runs.apply_schedule_decision(
        PROJECT_ID, 1, waiting.runs.check_schedule_decision(PROJECT_ID, 1, ADMIN, [action]), ADMIN)

    run = waiting.world.run_row()
    assert run.status == status
    decision = next(event for event in run.events if event["kind"] == "decision")
    assert (decision["text"].endswith("by a project admin"), decision["by"]) == (True, ADMIN)


# R2.4: a second decision finds the run no longer waiting
@pytest.mark.parametrize("second", ["approve", "reject"])
def test_7011_r2_4_a_second_decision_gets_not_waiting(waiting, second):
    _decide(waiting.runs, OWNER, "approve")

    with pytest.raises(waiting.runs.ScheduleRunNotWaiting):
        _decide(waiting.runs, OWNER, second)

    assert waiting.world.run_row().status == "running"
