"""Handler tests of `POST .../test_run`: a run of the unsaved Save form.

The harness of the schedule resource tests is shared: handlers, service, draft helper and SQLite tables are real.
The thread that starts the run is replaced.
"""
import pathlib
import sys

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).parent))  # the shared harness lives beside this file
from test_7011_schedule_crud_api import (  # noqa: F401 -- `harness` is a fixture
    NOT_FOUND,
    OTHER_ID,
    OWNER_ID,
    PACKAGE,
    PERMISSION_PREFIX,
    PROJECT_ID,
    agent_body,
    assert_refusal,
    pipeline_body,
)
from test_7011_schedule_crud_api import harness as _crud_harness

harness = _crud_harness  # the CRUD harness fixture, shared

@pytest.fixture
def started(harness):
    calls = []
    harness.module.start_schedule_run = lambda *args: calls.append(args) or True
    return calls

def models():
    return sys.modules[f'{PACKAGE}.models.entity_schedule']

def test_run(harness, body=None, **kwargs):
    return harness.call('post', agent_body() if body is None else body, suffix='/test_run', **kwargs)

def drafts(harness):
    with harness.session_factory() as session:
        return session.query(models().EntitySchedule).filter_by(draft=True).all()

def runs(harness):
    with harness.session_factory() as session:
        return [(r.schedule_id, r.trigger, r.status, r.requested_by) for r in session.query(models().EntityScheduleRun)]

def test_a_test_run_stores_a_hidden_draft_and_starts_a_test_run(harness, started):
    body, status = test_run(harness)

    assert status == 202
    (draft,) = drafts(harness)
    assert (draft.enabled, draft.next_run, draft.last_run, draft.created_by) == (False, None, None, OWNER_ID)
    assert (draft.name, draft.prompt, draft.version_id) == ('Nightly', 'Say hi to {{ who }}', 10)
    assert runs(harness) == [(draft.id, 'test', 'running', OWNER_ID)]
    assert started == [('agent', PROJECT_ID, draft.id, body['run_id'], None)]
    assert draft.last_run_status == 'running'

def test_a_test_run_leaves_the_schedule_list_without_the_draft(harness, started):
    test_run(harness)
    saved_id = harness.seed(name='Saved')
    sql_rows = harness.call('get', None, saved_id)
    assert sql_rows[1] == 200
    with harness.session_factory() as session:
        visible = session.query(models().EntitySchedule).filter(models().not_draft()).all()
    assert [row.name for row in visible] == ['Saved']

def test_a_second_test_run_of_the_same_entity_reuses_the_draft_and_rewrites_it(harness, started):
    test_run(harness)
    (first,) = drafts(harness)
    with harness.session_factory() as session:
        session.query(models().EntityScheduleRun).update({'status': 'succeeded'})
        session.commit()

    test_run(harness, agent_body(prompt='Say bye to {{ who }}'))

    (second,) = drafts(harness)
    assert (second.id, second.prompt) == (first.id, 'Say bye to {{ who }}')
    assert [row[1] for row in runs(harness)] == ['test', 'test']

def test_a_second_test_run_while_one_is_active_is_409_with_the_active_run(harness, started):
    first, _ = test_run(harness)[0], None

    body, status = test_run(harness)

    assert status == 409
    assert (body['code'], body['active_run_id']) == ('run_active', first['run_id'])
    assert len(runs(harness)) == 1

def test_each_owner_and_each_entity_gets_its_own_draft(harness, started):
    test_run(harness)
    test_run(harness, pipeline_body())
    harness.user_id = OTHER_ID
    test_run(harness)

    assert sorted((row.created_by, row.application_id) for row in drafts(harness)) == [
        (OWNER_ID, 1), (OWNER_ID, 2), (OTHER_ID, 1)]

def test_the_test_run_uses_the_save_rules_and_writes_nothing_on_a_refusal(harness, started):
    body, status = test_run(harness, agent_body(prompt='  '))

    assert (status, body['code'], body['field']) == (400, 'invalid_input', 'prompt')
    assert drafts(harness) == [] and runs(harness) == [] and started == []

def test_the_test_run_refuses_an_unknown_version_with_the_save_field(harness, started):
    body, status = test_run(harness, agent_body(version_id=999))

    assert (status, body['field']) == (400, 'version_id')

def test_the_test_run_refuses_run_without_asking_when_the_project_does_not_allow_it(harness, started):
    body, status = test_run(harness, agent_body(approval_rule='run_without_asking'))

    assert (status, body['code']) == (400, 'approval_rule_not_allowed')
    assert drafts(harness) == []

def test_the_test_run_needs_the_run_permission(harness, started):
    harness.permissions.discard(PERMISSION_PREFIX + 'run')

    assert test_run(harness)[1] == 403
    assert drafts(harness) == []

def test_a_test_run_that_cannot_start_fails_the_run_and_answers_500(harness):
    harness.module.start_schedule_run = lambda *args: False

    assert_refusal(test_run(harness), 500)
    assert [row[2] for row in runs(harness)] == ['failed']

def test_a_test_run_that_cannot_build_the_owner_request_writes_no_draft(harness, started):
    harness.owner_request_error = RuntimeError('down')

    assert_refusal(test_run(harness), 500)
    assert drafts(harness) == [] and runs(harness) == []

def test_a_test_run_passes_the_owner_request_and_the_checks_of_a_saved_run(harness, started):
    harness.owner_request = 'ticket'

    body, _ = test_run(harness)

    assert started[0][-1] == 'ticket'
    assert harness.owner_requests[0][1]['version_id'] == 10

def test_a_draft_never_shows_last_run_or_next_run_after_a_test_run(harness, started):
    test_run(harness)
    (draft,) = drafts(harness)

    assert (draft.last_run, draft.next_run, draft.enabled) == (None, None, False)

def test_the_run_of_a_draft_is_not_reachable_through_the_runs_by_id_route(harness, started):
    body, _ = test_run(harness)
    (draft,) = drafts(harness)

    assert harness.call('get', None, draft.id, suffix=f"/runs/{body['run_id']}") == (NOT_FOUND, 404)
