"""EL-7011 US-15: handler tests of the run endpoint of the schedule resource (run now and test run).

Covers R1.1, R2.1, R2.2, R3.1, R3.2 and R3.3. The harness of the schedule resource tests is shared: the
handler, the claim insert and the SQLite tables are real. The thread that starts the run is replaced.

Run via:
    python tests/run_tests.py unit/test_7011_schedule_run_api.py -v
"""
import pathlib
import sys

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).parent))  # the shared harness lives beside this file
from test_7011_schedule_crud_api import (  # noqa: F401 -- `harness` is a fixture
    ADMIN,
    ADMIN_ID,
    EDITOR,
    NOT_FOUND,
    OTHER_ID,
    OWNER_ID,
    PACKAGE,
    PERMISSION_PREFIX,
    PROJECT_ID,
)
from test_7011_schedule_crud_api import harness as _crud_harness

harness = _crud_harness  # the CRUD harness fixture, shared


@pytest.fixture
def started(harness):
    """Calls of the run starter; the starter answers true as a started thread does."""
    calls = []
    harness.module.start_schedule_run = lambda *args: calls.append(args) or True
    return calls


def run(harness, schedule_id, trigger='manual', **kwargs):
    return harness.call('post', {'trigger': trigger}, schedule_id, suffix='/run', **kwargs)


def runs(harness):
    run_model = sys.modules[f'{PACKAGE}.models.entity_schedule'].EntityScheduleRun
    with harness.session_factory() as session:
        return [(r.id, r.schedule_id, r.trigger, r.status, r.requested_by) for r in session.query(run_model)]


def add_run(harness, schedule_id, status):
    run_model = sys.modules[f'{PACKAGE}.models.entity_schedule'].EntityScheduleRun
    with harness.session_factory() as session:
        session.add(run_model(schedule_id=schedule_id, trigger='cron', status=status))
        session.commit()


@pytest.mark.parametrize('trigger', ['manual', 'test'])
def test_7011_r1_1_owner_run_inserts_a_running_row_and_answers_202(harness, started, trigger):
    schedule_id = harness.seed()
    body, status = run(harness, schedule_id, trigger)
    assert status == 202
    assert runs(harness) == [(body['run_id'], schedule_id, trigger, 'running', OWNER_ID)]
    assert started == [('agent', PROJECT_ID, schedule_id, body['run_id'])]


def test_7011_r1_1_a_run_that_cannot_start_is_failed_and_answers_500(harness):
    harness.module.start_schedule_run = lambda *args: False
    schedule_id = harness.seed()
    assert run(harness, schedule_id)[1] == 500
    assert [row[3] for row in runs(harness)] == ['failed']


def test_7011_r1_1_a_bad_trigger_is_400_and_inserts_nothing(harness, started):
    schedule_id = harness.seed()
    assert run(harness, schedule_id, 'cron')[1] == 400
    assert harness.call('post', {'trigger': 'manual', 'user_input': 'x'}, schedule_id, suffix='/run')[1] == 400
    assert (runs(harness), started) == ([], [])


@pytest.mark.parametrize('verb', ['get', 'put', 'delete'])
def test_7011_r1_1_other_verbs_on_the_run_url_are_refused(harness, verb):
    """Review round 1: the router matches by verb only, so GET, PUT and DELETE on /run must not touch the schedule."""
    schedule_id = harness.seed()
    assert harness.call(verb, {'name': 'changed'}, schedule_id, suffix='/run')[1] == 405
    assert harness.row(schedule_id) is not None


def test_7011_r1_1_post_to_the_schedule_without_run_is_refused(harness, started):
    schedule_id = harness.seed()
    assert harness.call('post', {'trigger': 'manual'}, schedule_id)[1] == 405
    assert (runs(harness), started) == ([], [])


@pytest.mark.parametrize('active', ['running', 'waiting_for_approval'])
def test_7011_r2_1_active_run_is_409_with_its_id_and_inserts_nothing(harness, started, active):
    schedule_id = harness.seed()
    add_run(harness, schedule_id, active)
    body, status = run(harness, schedule_id)
    assert (status, body['active_run_id']) == (409, runs(harness)[0][0])
    assert (len(runs(harness)), started) == (1, [])


def test_7011_r2_1_a_finished_run_does_not_block(harness, started):
    schedule_id = harness.seed()
    add_run(harness, schedule_id, 'success')
    assert run(harness, schedule_id)[1] == 202


def test_7011_r2_2_deleted_version_is_409_and_inserts_nothing(harness, started):
    schedule_id = harness.seed(version_id=None, enabled=False, disabled_reason='version_deleted')
    assert run(harness, schedule_id) == ({'error': 'version_deleted'}, 409)
    assert (runs(harness), started) == ([], [])


def test_7011_r3_1_admin_who_is_not_the_owner_is_403_and_inserts_nothing(harness, started):
    schedule_id = harness.seed(created_by=OTHER_ID)
    harness.request_as(ADMIN_ID, ADMIN)
    assert run(harness, schedule_id) == ({'error': 'not_owner'}, 403)
    assert (runs(harness), started) == ([], [])


def test_7011_r3_2_regular_user_run_of_another_owners_schedule_is_404(harness, started):
    schedule_id = harness.seed(created_by=OTHER_ID)
    assert run(harness, schedule_id) == (NOT_FOUND, 404)
    assert (runs(harness), started) == ([], [])


def test_7011_r3_3_missing_run_permission_is_403_and_inserts_nothing(harness, started):
    schedule_id = harness.seed()
    harness.permissions.discard(PERMISSION_PREFIX + 'run')
    assert run(harness, schedule_id)[1] == 403
    assert (runs(harness), started) == ([], [])
