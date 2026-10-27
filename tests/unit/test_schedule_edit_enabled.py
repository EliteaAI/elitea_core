"""Edit of a schedule with the `enabled` field: false pauses, true resumes, other fields still edit.

The harness is the one of the schedule handler tests: real handlers and service, SQLite rows.

Run via:
    python tests/run_tests.py unit/test_schedule_edit_enabled.py -v
"""
import pytest
from unit import test_7011_schedule_crud_api as crud_api_tests
from unit.test_7011_schedule_crud_api import NOT_FOUND, PERMISSION_PREFIX

# Pytest finds a fixture by its module-level name; an assignment keeps the shared fixture without an unused import.
harness = crud_api_tests.harness


def test_enabled_false_pauses_the_schedule(harness):
    schedule_id = harness.seed()
    body, status = harness.call('put', {'enabled': False}, schedule_id)
    assert (status, body['enabled']) == (200, False)
    assert harness.row(schedule_id).enabled is False


def test_enabled_true_resumes_the_schedule_and_computes_the_next_run(harness):
    schedule_id = harness.seed(enabled=False, next_run=None)
    body, status = harness.call('put', {'enabled': True}, schedule_id)
    row = harness.row(schedule_id)
    assert (status, body['enabled'], row.enabled) == (200, True, True)
    assert row.next_run is not None


def test_enabled_with_other_fields_edits_then_pauses(harness):
    schedule_id = harness.seed()
    body, status = harness.call('put', {'name': 'Renamed', 'enabled': False}, schedule_id)
    row = harness.row(schedule_id)
    assert (status, body['name'], body['enabled']) == (200, 'Renamed', False)
    assert (row.name, row.enabled) == ('Renamed', False)


def test_failed_edit_leaves_the_enabled_state_alone(harness):
    schedule_id = harness.seed()
    body, status = harness.call('put', {'name': '', 'enabled': False}, schedule_id)
    assert (status, body['field']) == (400, 'name')
    assert harness.row(schedule_id).enabled is True


def test_edit_without_enabled_keeps_the_enabled_state(harness):
    schedule_id = harness.seed(enabled=False)
    _, status = harness.call('put', {'name': 'Renamed'}, schedule_id)
    row = harness.row(schedule_id)
    assert (status, row.name, row.enabled) == (200, 'Renamed', False)


@pytest.mark.parametrize('value', ['false', 0, None])
def test_enabled_that_is_not_a_boolean_is_400(harness, value):
    schedule_id = harness.seed()
    body, status = harness.call('put', {'enabled': value}, schedule_id)
    assert (status, body['field']) == (400, 'enabled')
    assert harness.row(schedule_id).enabled is True


def test_resume_past_the_project_cap_is_409_and_stays_paused(harness):
    harness.config['scheduler'] = {'schedules': {'max_enabled_per_project': 1}}
    harness.seed(name='Running')
    schedule_id = harness.seed(enabled=False)
    _, status = harness.call('put', {'enabled': True}, schedule_id)
    assert status == 409
    assert harness.row(schedule_id).enabled is False


def test_enabled_on_an_unknown_schedule_is_404_and_changes_nothing(harness):
    schedule_id = harness.seed()
    body, status = harness.call('put', {'enabled': False}, schedule_id + 100)
    assert (status, body['error']) == (404, NOT_FOUND['error'])
    assert harness.row(schedule_id).enabled is True


def test_enabled_needs_the_update_permission(harness):
    schedule_id = harness.seed()
    harness.permissions.discard(PERMISSION_PREFIX + 'update')
    assert harness.call('put', {'enabled': False}, schedule_id)[1] == 403
    assert harness.row(schedule_id).enabled is True
