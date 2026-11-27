"""#7400: webhook limits — running caps per webhook and per project, the skipped-row throttle, the waiting bound.

Reuses the #7453 door harness (SQLite schedule store, fake flask, stubbed limits). SQLite has no advisory lock,
so the race itself is proven by the Postgres suite `integration/test_7400_webhook_limits_pg.py`.
"""
import sys
from datetime import UTC, datetime, timedelta

import pytest
from unit.test_7453_webhook_door import BODY, PACKAGE, _github, _post, _runs, _webhook, door  # noqa: F401
from integration.test_7011_schedule_service import env  # noqa: F401

pytestmark = pytest.mark.unit


def _run_model():
    return sys.modules[f'{PACKAGE}.models.entity_schedule'].EntityScheduleRun


def _add_run(env, schedule_id, status='running', trigger='webhook'):
    with env.session_factory() as session:
        run = _run_model()(schedule_id=schedule_id, trigger=trigger, status=status)
        session.add(run)
        session.commit()
        return run.id


def _skipped(env, schedule_id):
    return [run for run in _runs(env) if run.schedule_id == schedule_id and run.status == 'skipped']


# --- the counting rule (pure)

def test_check_caps_counts_the_row_first_then_the_project(door):
    check = door.door.check_caps
    assert check({}, 1, 3, 10) is None
    assert check({1: 2, 2: 7}, 1, 3, 10) is None
    assert check({1: 3}, 1, 3, 10) == door.door.Capped('webhook_cap_reached', 3, 3)
    assert check({1: 1, 2: 9}, 1, 3, 10) == door.door.Capped('project_cap_reached', 10, 10)


# --- AC1: the fourth delivery is refused with 429 and Retry-After

def test_the_fourth_delivery_gets_429_with_retry_after_and_starts_nothing(door):
    door.limits.per_webhook = 3
    schedule_id, row_uuid = _webhook(door)
    assert [_post(door, row_uuid, _github(BODY))[0] for _ in range(3)] == [202, 202, 202]

    status, data, headers = _post(door, row_uuid, _github(BODY))

    assert (status, data, headers['Retry-After']) == (429, {'code': 'webhook_cap_reached', 'running': 3, 'cap': 3}, '30')
    assert len(door.started) == 3
    assert door.row(schedule_id).last_delivery_outcome == 'capped'


def test_a_capped_delivery_releases_its_claim_so_the_sender_retry_runs(door):
    door.limits.per_webhook = 1
    _, row_uuid = _webhook(door)
    assert _post(door, row_uuid, _github(BODY))[0] == 202
    retry = _github(BODY)
    assert _post(door, row_uuid, retry)[0] == 429
    with door.session_factory() as session:  # the first run ends; the sender retries the same delivery
        session.query(_run_model()).filter_by(status='running').update({'status': 'success'})
        session.commit()

    assert _post(door, row_uuid, retry)[0] == 202


# --- AC3: only running counts; waiting runs do not block

def test_waiting_and_finished_runs_do_not_count(door):
    door.limits.per_webhook = 1
    schedule_id, row_uuid = _webhook(door)
    for status in ('waiting_for_approval', 'waiting_for_approval', 'success', 'failed', 'skipped'):
        _add_run(door, schedule_id, status)

    assert _post(door, row_uuid, _github(BODY))[0] == 202


def test_a_running_sample_counts_against_the_webhook_cap(door):
    door.limits.per_webhook = 1
    schedule_id, row_uuid = _webhook(door)
    _add_run(door, schedule_id, trigger='test')

    assert _post(door, row_uuid, _github(BODY))[0] == 429


# --- AC4: the project cap bounds the sum over all webhooks

def test_the_project_cap_bounds_the_sum_over_all_webhooks(door):
    door.limits.per_webhook, door.limits.per_project = 3, 4
    first_id, first_uuid = _webhook(door)
    _, second_uuid = _webhook(door)
    for _ in range(3):
        _add_run(door, first_id)

    assert _post(door, second_uuid, _github(BODY))[0] == 202
    status, data, _ = _post(door, second_uuid, _github(BODY))
    assert (status, data['code'], data['running'], data['cap']) == (429, 'project_cap_reached', 4, 4)


# --- AC7: cron runs never count

def test_a_cron_schedule_with_running_runs_does_not_fill_the_project_cap(door):
    door.limits.per_project = 1
    cron_id = door.seed()
    _add_run(door, cron_id, trigger='cron')
    _, row_uuid = _webhook(door)

    assert _post(door, row_uuid, _github(BODY))[0] == 202


# --- AC5: the project's lower value

def test_a_lower_project_value_lowers_the_cap(door):
    door.limits.per_webhook, door.project_cap = 3, 1
    _, row_uuid = _webhook(door)
    assert _post(door, row_uuid, _github(BODY))[0] == 202

    status, data, _ = _post(door, row_uuid, _github(BODY))
    assert (status, data['cap']) == (429, 1)


# --- AC1 / AC6: one skipped row a minute, with a counter

def test_refusals_in_one_minute_write_one_skipped_row_with_a_counter(door):
    door.limits.per_webhook = 1
    schedule_id, row_uuid = _webhook(door)
    _post(door, row_uuid, _github(BODY))
    for _ in range(4):
        assert _post(door, row_uuid, _github(BODY))[0] == 429

    (row,) = _skipped(door, schedule_id)
    (event,) = row.events
    assert (row.trigger, row.reason, event['count']) == ('webhook', 'too many runs at once', 4)
    assert event['text'] == 'Run finished: skipped (4 deliveries in this minute, too many runs at once)'


def test_a_refusal_in_the_next_minute_writes_a_new_skipped_row(door):
    schedule_id, _ = _webhook(door)
    minute = datetime(2026, 11, 10, 9, 0, 30, tzinfo=UTC)
    with door.session_factory() as session:
        assert door.door.record_capped(session, schedule_id, minute) == 1
        assert door.door.record_capped(session, schedule_id, minute + timedelta(seconds=20)) == 2
        assert door.door.record_capped(session, schedule_id, minute + timedelta(seconds=40)) == 1
        session.commit()

    assert sorted(row.events[-1]['count'] for row in _skipped(door, schedule_id)) == [1, 2]


def test_a_bad_signature_writes_no_skipped_row_even_when_capped(door):
    door.limits.per_webhook = 1
    schedule_id, row_uuid = _webhook(door)
    _post(door, row_uuid, _github(BODY))

    assert _post(door, row_uuid, _github(BODY, secret='wrong'))[0] == 404
    assert _skipped(door, schedule_id) == []

