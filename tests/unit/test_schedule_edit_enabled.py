"""Edit of a schedule with the `enabled` field: false pauses, true resumes, other fields still edit.

The harness is the one of the schedule handler tests: real handlers and service, SQLite rows.

Run via:
    python tests/run_tests.py unit/test_schedule_edit_enabled.py -v
"""
import pytest
from unit import test_7011_schedule_crud_api as crud_api_tests
from unit.test_7011_schedule_crud_api import (
    ADMIN,
    ADMIN_ID,
    NOT_FOUND,
    OTHER_ID,
    OWNER_ID,
    PERMISSION_PREFIX,
)

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


@pytest.mark.parametrize('enabled', [True, False])
def test_enabled_needs_the_update_permission(harness, enabled):
    schedule_id = harness.seed(enabled=not enabled)
    harness.request_as(OWNER_ID, {PERMISSION_PREFIX + 'list', PERMISSION_PREFIX + 'details'})
    assert harness.call('put', {'enabled': enabled}, schedule_id)[1] == 403
    assert harness.row(schedule_id).enabled is (not enabled)


@pytest.mark.parametrize('enabled', [True, False])
def test_enabled_on_another_owners_schedule_is_404_for_a_regular_user(harness, enabled):
    schedule_id = harness.seed(created_by=OTHER_ID, enabled=not enabled)
    body, status = harness.call('put', {'enabled': enabled}, schedule_id)
    assert (status, body['error']) == (404, NOT_FOUND['error'])
    assert harness.row(schedule_id).enabled is (not enabled)


def test_admin_enabled_change_of_another_owners_schedule_tells_the_owner(harness):
    schedule_id = harness.seed(created_by=OTHER_ID)
    harness.request_as(ADMIN_ID, ADMIN)
    _, status = harness.call('put', {'enabled': False}, schedule_id)
    assert (status, harness.row(schedule_id).enabled) == (200, False)
    assert harness.notices == [(OTHER_ID, 'schedule_edited_by_admin', schedule_id)]


def test_resume_with_other_fields_past_the_project_cap_saves_nothing(harness):
    harness.config['scheduler'] = {'schedules': {'max_enabled_per_project': 1}}
    harness.seed(name='Running')
    schedule_id = harness.seed(enabled=False)
    name_before = harness.row(schedule_id).name
    _, status = harness.call('put', {'name': 'Renamed', 'enabled': True}, schedule_id)
    row = harness.row(schedule_id)
    assert (status, row.name, row.enabled) == (409, name_before, False)


def test_resume_with_other_fields_of_a_schedule_without_a_version_saves_nothing(harness):
    schedule_id = harness.seed(enabled=False, version_id=None)
    name_before = harness.row(schedule_id).name
    body, status = harness.call('put', {'name': 'Renamed', 'enabled': True}, schedule_id)
    row = harness.row(schedule_id)
    assert (status, body['field'], row.name, row.enabled) == (400, 'version_id', name_before, False)


def test_enabled_with_other_fields_answers_the_findings_and_the_governance_notice_of_the_edit(harness):
    harness.risk_warnings = [{'code': 'scan_incomplete', 'message': 'Not every tool was checked'}]
    harness.governance_notice = 'The version has no active binding'
    schedule_id = harness.seed()
    body, status = harness.call('put', {'name': 'Renamed', 'enabled': False}, schedule_id)
    assert status == 200
    assert body['warnings'] == harness.risk_warnings
    assert body['governance_notice'] == harness.governance_notice


def test_enabled_with_other_fields_runs_one_risk_review(harness):
    schedule_id = harness.seed()
    harness.call('put', {'name': 'Renamed', 'enabled': False}, schedule_id)
    assert len(harness.scans) == 1


def test_enabled_alone_runs_no_edit_review_and_no_governance_check(harness):
    schedule_id = harness.seed()
    harness.call('put', {'enabled': False}, schedule_id)
    assert harness.governance_calls == []
