"""EL-7011 US-10: handler tests of the admin-only owner route of the schedule resource.

Covers R1.1, R1.3, R2.1, R2.2, R2.3, R3.1 and R3.2. The CRUD harness is shared: the handler, the owner
write and the SQLite tables are real. The eligibility check is replaced; it has its own tests (T1).

Run via:
    python tests/run_tests.py unit/api/test_7011_change_owner_api.py -v
"""
import pytest
from fixtures.helpers import load_module_with_stubs
from unit.test_7011_schedule_crud_api import (
    ADMIN,
    ADMIN_ID,
    EDITOR,
    NOT_FOUND,
    OTHER_ID,
    OWNER_ID,
    PACKAGE,
    PLUGIN_ROOT,
)
from unit.test_7011_schedule_crud_api import harness as _crud_harness

harness = _crud_harness  # the CRUD harness fixture, shared


@pytest.fixture
def owner(harness):
    """The real owner module with a fake eligibility check; `ineligible` maps an owner id to its reason."""
    load_module_with_stubs(PLUGIN_ROOT / 'utils/schedule_prerun.py', f'{PACKAGE}.utils.schedule_prerun')
    module = load_module_with_stubs(PLUGIN_ROOT / 'utils/schedule_owner.py', f'{PACKAGE}.utils.schedule_owner')
    module.ineligible, module.checked = {}, []
    module.validate_new_owner = lambda project_id, owner_id: module.checked.append(owner_id) or (
        module.ineligible.get(owner_id))
    harness.request_as(ADMIN_ID, ADMIN)
    return module


def change_owner(harness, schedule_id, body):
    return harness.call('post', body, schedule_id, suffix='/owner')


def unchanged(harness, schedule_id):
    row = harness.row(schedule_id)
    return (row.created_by, row.principal) == (OTHER_ID, {'kind': 'delegation', 'ref': OTHER_ID})


def test_7011_r1_1_admin_sets_the_new_owner_and_gets_the_schedule(harness, owner):
    """TC-US10-01, TC-US10-09: an admin passes the check; owner and principal move; 200 with the schedule."""
    schedule_id = harness.seed(created_by=OTHER_ID)
    body, status = change_owner(harness, schedule_id, {'owner_id': OWNER_ID})
    row = harness.row(schedule_id)
    assert status == 200
    assert (body['id'], body['created_by'], body['owner']['id']) == (schedule_id, OWNER_ID, OWNER_ID)
    assert (row.created_by, row.principal, row.updated_by) == (
        OWNER_ID, {'kind': 'delegation', 'ref': OWNER_ID}, ADMIN_ID)
    assert owner.checked == [OWNER_ID]


def test_7011_r1_1_unknown_schedule_is_404_and_writes_nothing(harness, owner):
    """TC-US10-04"""
    assert change_owner(harness, 404, {'owner_id': OWNER_ID}) == (NOT_FOUND, 404)
    assert (harness.notices, harness.span.attributes) == ([], {})


def test_7011_r1_1_same_owner_is_200_without_span_or_notice(harness, owner):
    """TC-US10-15"""
    schedule_id = harness.seed(created_by=OTHER_ID)
    body, status = change_owner(harness, schedule_id, {'owner_id': OTHER_ID})
    assert (status, body['created_by']) == (200, OTHER_ID)
    assert (harness.notices, harness.span.attributes) == ([], {})


@pytest.mark.parametrize('verb', ['get', 'put', 'delete'])
def test_7011_r1_1_only_post_serves_the_owner_url(harness, owner, verb):
    schedule_id = harness.seed(created_by=OTHER_ID)
    body = {'owner_id': OWNER_ID} if verb == 'put' else None
    assert harness.call(verb, body, schedule_id, suffix='/owner') == harness.module.NOT_ALLOWED
    assert unchanged(harness, schedule_id)


def test_7011_r1_3_caller_without_schedules_admin_gets_403_and_no_audit_mark(harness, owner):
    """TC-US10-10, TC-US10-06: the owner of the schedule is no admin; nothing is written, marked or sent."""
    schedule_id = harness.seed(created_by=OTHER_ID)
    harness.request_as(OTHER_ID, EDITOR)
    assert change_owner(harness, schedule_id, {'owner_id': OWNER_ID})[1] == 403
    assert unchanged(harness, schedule_id)
    assert (owner.checked, harness.notices, harness.span.attributes) == ([], [], {})


@pytest.mark.parametrize('reason_name', ['NOT_ACTIVE_MEMBER', 'CANNOT_USE_SCHEDULES'])
def test_7011_r2_1_r2_2_ineligible_new_owner_is_400_with_the_reason(harness, owner, reason_name):
    """TC-US10-12, TC-US10-13"""
    reason = getattr(owner, reason_name)
    owner.ineligible[OWNER_ID] = reason
    schedule_id = harness.seed(created_by=OTHER_ID)
    assert change_owner(harness, schedule_id, {'owner_id': OWNER_ID}) == (
        {'error': reason, 'field': 'owner_id'}, 400)
    assert unchanged(harness, schedule_id)
    assert harness.notices == []


def test_7011_r2_1_unavailable_access_check_is_503_not_400(harness, owner):
    """A timeout or an RPC error of the auth check answers 503 and writes nothing (design NFR)."""
    owner.ineligible[OWNER_ID] = owner.ACCESS_CHECK_UNAVAILABLE
    schedule_id = harness.seed(created_by=OTHER_ID)
    assert change_owner(harness, schedule_id, {'owner_id': OWNER_ID}) == (
        {'error': owner.ACCESS_CHECK_UNAVAILABLE}, 503)
    assert unchanged(harness, schedule_id)


@pytest.mark.parametrize('body', [
    {'owner_id': str(OWNER_ID)}, {'owner_id': None}, {}, {'owner_id': True}, {'owner_id': 43.0}, None, [OWNER_ID],
])
def test_7011_r2_3_body_without_an_integer_owner_id_is_400(harness, owner, body):
    """TC-US10-14"""
    schedule_id = harness.seed(created_by=OTHER_ID)
    response, status = change_owner(harness, schedule_id, body)
    assert (status, response['field']) == (400, 'owner_id')
    assert unchanged(harness, schedule_id)
    assert owner.checked == []


def test_7011_r3_1_owner_change_marks_the_span(harness, owner):
    """TC-US10-05"""
    schedule_id = harness.seed(created_by=OTHER_ID)
    change_owner(harness, schedule_id, {'owner_id': OWNER_ID})
    assert {key: str(value) for key, value in harness.span.attributes.items()} == {
        'entity.type': 'schedule', 'entity.id': str(schedule_id), 'entity.name': 'Nightly',
    }


def test_7011_r3_2_both_owners_get_the_notice_with_both_ids(harness, owner):
    """TC-US10-07"""
    schedule_id = harness.seed(created_by=OTHER_ID)
    change_owner(harness, schedule_id, {'owner_id': OWNER_ID})
    assert harness.notices == [
        (OTHER_ID, 'schedule_owner_changed', schedule_id), (OWNER_ID, 'schedule_owner_changed', schedule_id),
    ]
    assert harness.notice_extras == [{'old_owner_id': OTHER_ID, 'new_owner_id': OWNER_ID}] * 2


def test_7011_r3_2_failed_notice_keeps_the_change(harness, owner):
    """TC-US10-08"""
    harness.notice_error = RuntimeError('socket down')
    schedule_id = harness.seed(created_by=OTHER_ID)
    assert change_owner(harness, schedule_id, {'owner_id': OWNER_ID})[1] == 200
    assert harness.row(schedule_id).created_by == OWNER_ID
