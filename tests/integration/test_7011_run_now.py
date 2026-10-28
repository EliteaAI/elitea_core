"""EL-7011 US-15 R1.2, R1.4, R2.1: Run now on a paused schedule and during a run.

Real: the run claim insert, `run_owner_schedule` and the schedule models over SQLite tables.
Faked: the predict RPC, the auth RPC and the credential step (the world of the agent schedule run tests).

Run via:
    python tests/run_tests.py -m integration -v integration/test_7011_run_now.py
"""
from datetime import datetime

import pytest
from integration.test_7011_agent_schedule_run import OWNER
from integration.test_7011_agent_schedule_run import world as _agent_world

world = _agent_world  # the agent run fixture, shared

NEXT_RUN = datetime(2030, 1, 1, 9, 0)
PAUSED = {"enabled": False, "next_run": NEXT_RUN}


# TC-US15-01, TC-US15-02 / R1.2
@pytest.mark.parametrize("trigger", ["manual", "test"])
def test_7011_r1_2_manual_run_starts_as_the_owner_with_manual_meta(world, trigger):
    world.run(trigger=trigger)

    (predicted,) = world.predictions
    assert (predicted["is_system_user"], predicted["user_id"]) == (False, OWNER)
    assert predicted["trigger_source"] == "schedule_manual"
    (conversation,) = world.conversations()
    assert conversation.meta["execution_type"] == "schedule_manual"
    assert "scheduled_run" not in conversation.meta
    assert world.run_row().conversation_id == conversation.id


# TC-US15-05, TC-US15-06, TC-US15-07 / R1.4
def test_7011_r1_4_manual_run_on_a_paused_schedule_keeps_enabled_next_run_and_cron(world):
    world.run(trigger="manual", **PAUSED)

    assert len(world.predictions) == 1
    schedule = world.schedule()
    assert (schedule.enabled, schedule.next_run, schedule.cron) == (False, NEXT_RUN, "0 * * * *")


# TC-US15-08, TC-US15-09, TC-US15-10 / R2.1
@pytest.mark.parametrize("status", ["running", "waiting_for_approval"])
@pytest.mark.parametrize("trigger", ["manual", "test"])
def test_7011_r2_1_a_run_during_an_active_run_gets_the_conflict_and_inserts_no_run(world, trigger, status):
    world.set_run_status(status)

    assert world.claim(trigger) is None
    assert world.run_count() == 1


# TC-US15-11, TC-US15-12 / R2.1: a closed run frees the schedule
def test_7011_r2_1_a_run_after_the_active_run_closed_is_accepted(world):
    world.set_run_status("success")

    run_id = world.claim("manual")

    assert run_id is not None
    assert world.run_count() == 2


# BUG-35 / R1.5: the list row shows Running at once; CR-24 / R4.1: Run now also moves the Last run time
def test_7011_bug35_cr24_manual_run_shows_running_in_list_and_sets_last_run(world):
    last_run = datetime(2026, 10, 20, 9, 0)
    world.set_run_status("success")
    with world.edit_schedule() as schedule:
        schedule.last_run_status, schedule.last_run = "success", last_run

    assert world.claim("manual") is not None

    schedule = world.schedule()
    assert schedule.last_run_status == "running"
    assert schedule.last_run is not None and schedule.last_run.replace(tzinfo=None) > last_run
