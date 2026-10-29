"""Handler tests of Run now and the run list for a schedule that delivers its answer to a chat.

The harness of the schedule resource tests is shared: the route module, the handler, the claim insert and the
SQLite tables are real. The thread that starts the run is replaced by a recorder, so a test reads what the handler
hands to the run starter. The starter itself is tested in test_schedule_delivery_run_branch.py.

Run via:
    python tests/run_tests.py unit/test_schedule_delivery_run_api.py -v
"""
import pathlib
import sys
from uuid import uuid4

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).parent))  # the shared harness lives beside this file
from test_7011_schedule_crud_api import (
    ADMIN,
    ADMIN_ID,
    OWNER_ID,
    PACKAGE,
    PERMISSION_PREFIX,
    PROJECT_ID,
    assert_refusal,
)
from test_7011_schedule_crud_api import harness as _crud_harness
from test_7011_schedule_runs_api import add_runs, get_runs

harness = _crud_harness  # the CRUD harness fixture, shared

TARGET_CHAT_ID = 55
PARTICIPANT_ID = 8


@pytest.fixture
def started(harness):
    """Calls of the run starter; the starter answers true as a started thread does."""
    calls = []
    harness.module.start_schedule_run = lambda *args: calls.append(args) or True
    return calls


def seed_delivery(harness, **fields):
    return harness.seed(
        delivery_mode='conversation', delivery_conversation_uuid=uuid4(), delivery_participant_id=PARTICIPANT_ID,
        **fields)


def run_now(harness, schedule_id, trigger='manual'):
    return harness.call('post', {'trigger': trigger}, schedule_id, suffix='/run')


def stored_runs(harness):
    run_model = sys.modules[f'{PACKAGE}.models.entity_schedule'].EntityScheduleRun
    with harness.session_factory() as session:
        return [(r.id, r.schedule_id, r.trigger, r.status, r.requested_by) for r in session.query(run_model)]


@pytest.mark.parametrize('entity_type', ['agent', 'pipeline'])
def test_run_now_of_a_delivery_schedule_hands_the_run_to_the_run_starter(harness, started, entity_type):
    schedule_id = seed_delivery(harness, entity_type=entity_type)
    body, status = run_now(harness, schedule_id)
    assert status == 202
    assert started == [(entity_type, PROJECT_ID, schedule_id, body['run_id'])]


def test_run_now_of_a_delivery_schedule_inserts_one_manual_run_of_the_owner(harness, started):
    schedule_id = seed_delivery(harness)
    body, _ = run_now(harness, schedule_id)
    assert stored_runs(harness) == [(body['run_id'], schedule_id, 'manual', 'running', OWNER_ID)]


def test_run_now_leaves_the_delivery_target_of_the_schedule_unchanged(harness, started):
    schedule_id = seed_delivery(harness)
    before = harness.row(schedule_id)
    target = (before.delivery_mode, before.delivery_conversation_uuid, before.delivery_participant_id)
    run_now(harness, schedule_id)
    after = harness.row(schedule_id)
    assert (after.delivery_mode, after.delivery_conversation_uuid, after.delivery_participant_id) == target


def test_run_now_of_a_run_history_schedule_takes_the_same_handoff(harness, started):
    schedule_id = harness.seed()
    body, status = run_now(harness, schedule_id)
    assert (status, started) == (202, [('agent', PROJECT_ID, schedule_id, body['run_id'])])
    assert harness.row(schedule_id).delivery_mode == 'run_history'


@pytest.mark.parametrize('active', ['running', 'waiting_for_approval'])
def test_run_now_skips_a_delivery_schedule_with_an_active_run(harness, started, active):
    schedule_id = seed_delivery(harness)
    add_runs(harness, schedule_id, {'status': active})
    run_id = stored_runs(harness)[0][0]
    body, status = run_now(harness, schedule_id)
    assert (status, body['active_run_id']) == (409, run_id)
    assert (len(stored_runs(harness)), started) == (1, [])


def test_run_now_of_a_delivery_schedule_refuses_an_admin_who_is_not_the_owner(harness, started):
    schedule_id = seed_delivery(harness, created_by=OWNER_ID + 1)
    harness.request_as(ADMIN_ID, ADMIN)
    assert_refusal(run_now(harness, schedule_id), 403, code='not_owner')
    assert (stored_runs(harness), started) == ([], [])


def test_run_now_of_a_delivery_schedule_needs_the_run_permission(harness, started):
    schedule_id = seed_delivery(harness)
    harness.permissions.discard(PERMISSION_PREFIX + 'run')
    assert run_now(harness, schedule_id)[1] == 403
    assert (stored_runs(harness), started) == ([], [])


def test_run_now_of_a_delivery_schedule_with_a_start_failure_closes_the_run(harness):
    harness.module.start_schedule_run = lambda *args: False
    schedule_id = seed_delivery(harness)
    assert run_now(harness, schedule_id)[1] == 500
    assert [row[3] for row in stored_runs(harness)] == ['failed']


def test_run_list_of_a_delivery_schedule_shows_the_chat_the_run_wrote_to(harness):
    schedule_id = seed_delivery(harness)
    add_runs(harness, schedule_id, {'conversation_id': TARGET_CHAT_ID})
    body, status = get_runs(harness, schedule_id)
    assert status == 200
    assert [row['conversation_id'] for row in body['rows']] == [TARGET_CHAT_ID]
