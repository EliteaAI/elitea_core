"""#7400 AC3: at most N webhook runs of one row wait for approval; the next pause fails its run with a notice.

Since #7454 a webhook run never waits before it starts, so a sensitive-tool pause during the run is the only
wait counted here. Fixtures are the ones of the US-09 / US-13 run writer tests (real models, SQLite).
"""
import pytest

from unit.utils.test_7011_schedule_runs_fail import _schedule, runs, session  # noqa: F401
from unit.utils.test_7011_schedule_runs_outcome import sent  # noqa: F401

# ruff: noqa: F811

pytestmark = pytest.mark.unit

SENSITIVE = {"hitl_interrupt": {"type": "hitl", "guardrail_type": "sensitive_tool", "toolkit_name": "github",
                                "tool_name": "create_issue"}}


@pytest.fixture(autouse=True)
def bound(runs):
    runs.config['scheduler'] = {'schedules': {'webhook_max_waiting_runs': 2}}


def _webhook_row(runs, session):
    return _schedule(runs, session, trigger_kind='webhook', webhook_type='custom', cron=None, timezone=None)


def _running(runs, session, schedule, trigger):
    row = runs.EntityScheduleRun(schedule_id=schedule.id, trigger=trigger, status='running')
    session.add(row)
    session.flush()
    return row


def _pause_all(runs, session, rows):
    for row in rows:
        runs.pause_schedule_run(1, row.id, SENSITIVE)
    for row in rows:
        session.refresh(row)


def test_a_webhook_pause_over_the_waiting_bound_fails_its_run_with_a_notice(runs, session, sent):
    schedule = _webhook_row(runs, session)
    rows = [_running(runs, session, schedule, 'webhook') for _ in range(3)]

    _pause_all(runs, session, rows)

    assert [row.status for row in rows[:2]] == ['waiting_for_approval', 'waiting_for_approval']
    assert (rows[2].status, rows[2].reason) == ('failed', 'too many runs waiting for approval')
    assert sent.notices[-1][1:] == ('schedule_run_failed', rows[2].id)


def test_a_sample_run_counts_as_a_webhook_run_for_the_waiting_bound(runs, session, sent):
    schedule = _webhook_row(runs, session)
    rows = [_running(runs, session, schedule, trigger) for trigger in ('webhook', 'webhook', 'test')]

    _pause_all(runs, session, rows)

    assert rows[-1].status == 'failed'


def test_the_bound_is_per_webhook(runs, session, sent):
    first, second = _webhook_row(runs, session), _webhook_row(runs, session)
    rows = [_running(runs, session, first, 'webhook') for _ in range(2)] + [_running(runs, session, second, 'webhook')]

    _pause_all(runs, session, rows)

    assert {row.status for row in rows} == {'waiting_for_approval'}


def test_a_cron_schedule_is_never_bounded_by_the_waiting_cap(runs, session, sent):
    runs.config['scheduler']['schedules']['webhook_max_waiting_runs'] = 1
    schedule = _schedule(runs, session)
    waiting = runs.EntityScheduleRun(schedule_id=schedule.id, trigger='manual', status='success')
    session.add(waiting)
    row = _running(runs, session, schedule, 'cron')  # one active cron run: the active-run index allows no more

    _pause_all(runs, session, [row])

    assert row.status == 'waiting_for_approval'


def test_the_waiting_limit_is_read_from_the_config(runs):
    assert runs.webhook_waiting_limit() == 2
    runs.config['scheduler']['schedules']['webhook_max_waiting_runs'] = None
    assert runs.webhook_waiting_limit() == 10


def _approved_after_a_pause(runs, session, schedule):
    run = _running(runs, session, schedule, 'webhook')
    _pause_all(runs, session, [run])
    runs.apply_schedule_decision(1, run.id, 'approve')
    session.refresh(run)
    assert run.status == 'running'
    return run


def test_a_second_pause_of_an_admitted_webhook_run_is_not_counted_against_the_bound_again(runs, session, sent):
    schedule = _webhook_row(runs, session)
    approved = _approved_after_a_pause(runs, session, schedule)
    others = [_running(runs, session, schedule, 'webhook') for _ in range(2)]
    _pause_all(runs, session, others)

    _pause_all(runs, session, [approved])

    assert [row.status for row in others] == ['waiting_for_approval', 'waiting_for_approval']
    assert (approved.status, approved.reason) == ('waiting_for_approval', None)


def test_the_second_pause_of_an_admitted_webhook_run_arms_the_expiry_again(runs, session, sent):
    schedule = _webhook_row(runs, session)
    approved = _approved_after_a_pause(runs, session, schedule)
    assert approved.approval_expires_at is None

    _pause_all(runs, session, [approved])

    assert approved.approval_expires_at is not None
    assert approved.resumed_at is None


def test_a_new_webhook_run_is_still_failed_over_the_bound_while_an_admitted_one_waits_again(runs, session, sent):
    schedule = _webhook_row(runs, session)
    approved = _approved_after_a_pause(runs, session, schedule)
    waiting = [_running(runs, session, schedule, 'webhook') for _ in range(2)]
    _pause_all(runs, session, waiting)
    late = _running(runs, session, schedule, 'webhook')

    _pause_all(runs, session, [approved, late])

    assert approved.status == 'waiting_for_approval'
    assert (late.status, late.reason) == ('failed', 'too many runs waiting for approval')
