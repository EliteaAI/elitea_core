"""EL-7011 US-16: handler tests of the run records endpoint `GET .../{schedule_id}/runs` (R3.1 to R3.5).

The harness of the schedule resource tests is shared: the handler and the SQLite tables are real.

Run via:
    python tests/run_tests.py unit/test_7011_schedule_runs_api.py -v
"""
import datetime
import pathlib
import sys

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).parent))  # the shared harness lives beside this file
from test_7011_schedule_crud_api import (
    ADMIN,
    ADMIN_ID,
    NOT_FOUND,
    OTHER_ID,
    OWNER_ID,
    PACKAGE,
    PERMISSION_PREFIX,
)
from test_7011_schedule_crud_api import harness as _crud_harness

harness = _crud_harness  # the CRUD harness fixture, shared

FIELDS = {
    'id', 'trigger', 'status', 'reason', 'conversation_id', 'requested_by', 'started_at', 'finished_at',
    'version_id', 'duration_ms',  # owner CR 2026-10-21: the Version and Duration columns of the Runs table
}


def add_runs(harness, schedule_id, *specs):
    """Insert runs oldest first; each spec is a dict of columns, `started_at` rises with the order."""
    run_model = sys.modules[f'{PACKAGE}.models.entity_schedule'].EntityScheduleRun
    start = datetime.datetime(2026, 10, 1, tzinfo=datetime.UTC)
    with harness.session_factory() as session:
        for index, spec in enumerate(specs):
            session.add(run_model(
                schedule_id=schedule_id, started_at=start + datetime.timedelta(minutes=index),
                **{'trigger': 'cron', 'status': 'success', **spec},
            ))
        session.commit()


def get_runs(harness, schedule_id, **query):
    return harness.call('get', schedule_id=schedule_id, suffix='/runs', query=query)


def test_7011_r3_1_tc_us16_08_runs_are_listed_newest_first_with_every_field(harness):
    """TC-US16-08, TC-US16-11 (R3.1, R3.2): fields, order, a run without a conversation, no runs."""
    schedule_id = harness.seed()
    empty_id = harness.seed(name='Empty')
    add_runs(
        harness, schedule_id,
        {'trigger': 'manual', 'conversation_id': 9120, 'requested_by': OWNER_ID, 'finished_at': datetime.datetime(
            2026, 10, 1, 1, tzinfo=datetime.UTC)},
        {'status': 'skipped', 'reason': 'a run of this schedule is still active'},
    )
    body, status = get_runs(harness, schedule_id)
    assert (status, body['total'], [r['status'] for r in body['rows']]) == (200, 2, ['skipped', 'success'])
    assert all(set(row) == FIELDS for row in body['rows'])
    skipped, manual = body['rows']
    assert (skipped['conversation_id'], skipped['reason']) == (None, 'a run of this schedule is still active')
    assert (manual['conversation_id'], manual['reason'], manual['requested_by']) == (9120, None, OWNER_ID)
    assert manual['finished_at'].startswith('2026-10-01T01:00')
    assert get_runs(harness, empty_id) == ({'total': 0, 'rows': []}, 200)


def test_7011_cr_el7011_rows_have_the_version_and_the_duration_of_a_finished_run(harness):
    """Owner CR 2026-10-21: `version_id` and `duration_ms`; no duration while the run is active."""
    schedule_id = harness.seed()
    add_runs(
        harness, schedule_id,
        {'version_id': 10, 'finished_at': datetime.datetime(2026, 10, 1, 0, 0, 38, tzinfo=datetime.UTC)},
        {'status': 'running', 'version_id': 11},
    )
    running, finished = get_runs(harness, schedule_id)[0]['rows']
    assert (finished['version_id'], finished['duration_ms']) == (10, 38000)
    assert (running['version_id'], running['duration_ms']) == (11, None)


def test_7011_r3_3_tc_us16_09_limit_offset_and_status_page_and_filter(harness):
    """TC-US16-09 (R3.3): paging, the status filter and the cap of 100."""
    schedule_id = harness.seed()
    add_runs(harness, schedule_id, *([{'status': 'failed'}] * 30 + [{'status': 'success'}] * 100))
    page, _ = get_runs(harness, schedule_id, limit='20', offset='20')
    assert (len(page['rows']), page['total']) == (20, 130)
    failed, _ = get_runs(harness, schedule_id, status='failed')
    assert (failed['total'], {r['status'] for r in failed['rows']}) == (30, {'failed'})
    assert len(get_runs(harness, schedule_id, limit='500')[0]['rows']) == 100


def test_7011_r3_3_default_page_size_is_20_for_el_7011(harness):
    """EL-7011: without `limit` the page holds 20 runs (design: default 20, cap 100)."""
    schedule_id = harness.seed()
    add_runs(harness, schedule_id, *([{'status': 'success'}] * 25))
    body, _ = get_runs(harness, schedule_id)
    assert (len(body['rows']), body['total']) == (20, 25)


@pytest.mark.parametrize('query', [{'limit': 'x'}, {'limit': '0'}, {'offset': '-1'}])
def test_7011_r3_3_a_bad_limit_or_offset_is_400(harness, query):
    schedule_id = harness.seed()
    assert get_runs(harness, schedule_id, **query)[1] == 400


def test_7011_r3_5_tc_us16_10_an_unknown_status_is_400(harness):
    """TC-US16-10 (R3.5)."""
    schedule_id = harness.seed()
    add_runs(harness, schedule_id, {})
    body, status = get_runs(harness, schedule_id, status='done')
    assert (status, body['field']) == (400, 'status')


def test_7011_r3_1_tc_us16_12_an_admin_reads_the_runs_of_another_owner(harness):
    """TC-US16-12 (R3.1)."""
    schedule_id = harness.seed(created_by=OTHER_ID)
    add_runs(harness, schedule_id, {})
    harness.request_as(ADMIN_ID, ADMIN)
    body, status = get_runs(harness, schedule_id)
    assert (status, body['total']) == (200, 1)


def test_7011_r3_4_tc_us16_13_a_regular_user_gets_404_for_another_owners_runs(harness):
    """TC-US16-13 (R3.4): no run data in the answer."""
    schedule_id = harness.seed(created_by=OTHER_ID)
    add_runs(harness, schedule_id, {})
    assert get_runs(harness, schedule_id) == (NOT_FOUND, 404)


def test_7011_r3_4_tc_us16_14_unknown_schedule_is_404_and_missing_permission_is_403(harness):
    """TC-US16-14 (R3.4): the refusal does not tell whether a schedule exists."""
    assert get_runs(harness, 9999) == (NOT_FOUND, 404)
    schedule_id = harness.seed()
    harness.permissions.discard(PERMISSION_PREFIX + 'details')
    assert get_runs(harness, schedule_id)[1] == 403


@pytest.mark.parametrize('verb', ['post', 'put', 'delete'])
def test_7011_r3_1_other_verbs_on_the_runs_url_are_refused(harness, verb):
    schedule_id = harness.seed()
    assert harness.call(verb, {'name': 'changed'}, schedule_id, suffix='/runs')[1] == 405
    assert harness.row(schedule_id).name == 'Nightly'
