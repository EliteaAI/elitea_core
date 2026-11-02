"""Handler tests of `POST .../test_run` and its Cancel `DELETE .../test_run`: a run of the unsaved Save form.

The harness of the schedule resource tests is shared: handlers, service, draft helper and SQLite tables are real.
The thread that starts the run is replaced.
"""
import pathlib
import sys
import types
from datetime import UTC, datetime, timedelta

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


def cancel(harness, application_id=1):
    return harness.call('delete', suffix='/test_run', query={'application_id': application_id})


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


def test_save_removes_the_draft_of_the_entity_when_it_has_no_run(harness, started):
    harness.module.start_schedule_run = lambda *args: False
    test_run(harness)
    with harness.session_factory() as session:
        session.query(models().EntityScheduleRun).delete()
        session.commit()

    body, status = harness.call('post', agent_body(enabled=False))

    assert status == 201
    assert drafts(harness) == []


def test_cancel_removes_a_draft_without_runs_and_is_safe_to_repeat(harness, started):
    harness.module.start_schedule_run = lambda *args: False
    test_run(harness)
    with harness.session_factory() as session:
        session.query(models().EntityScheduleRun).delete()
        session.commit()

    assert cancel(harness)[1] == 204
    assert cancel(harness)[1] == 204
    assert drafts(harness) == []


def test_cancel_keeps_a_draft_that_has_runs_and_never_removes_another_owners_draft(harness, started):
    test_run(harness)
    assert cancel(harness)[1] == 204
    assert len(drafts(harness)) == 1  # its test run stays in Run History
    harness.user_id = OTHER_ID
    assert cancel(harness)[1] == 204
    assert len(drafts(harness)) == 1


def test_cancel_needs_the_run_permission_and_a_valid_application_id(harness):
    assert harness.call('delete', suffix='/test_run', query={})[1] == 400
    assert cancel(harness, 'x')[1] == 400
    harness.permissions.discard(PERMISSION_PREFIX + 'run')
    assert cancel(harness)[1] == 403


def test_an_idle_draft_without_runs_is_removed_by_the_next_test_run_of_the_owner(harness, started):
    old = datetime.now(UTC) - timedelta(hours=25)
    with harness.session_factory() as session:
        session.add(models().EntitySchedule(
            draft=True, enabled=False, entity_type='pipeline', application_id=2, version_id=11, name='Old',
            cron='0 9 * * *', timezone='UTC', created_by=OWNER_ID, principal={'kind': 'delegation', 'ref': OWNER_ID},
            updated_at=old))
        session.add(models().EntitySchedule(
            draft=True, enabled=False, entity_type='pipeline', application_id=2, version_id=11, name='Other',
            cron='0 9 * * *', timezone='UTC', created_by=OTHER_ID, principal={'kind': 'delegation', 'ref': OTHER_ID},
            updated_at=old))
        session.commit()

    test_run(harness)

    assert sorted((row.name, row.created_by) for row in drafts(harness)) == [('Nightly', OWNER_ID), ('Other', OTHER_ID)]


def test_save_keeps_a_draft_that_has_runs_and_the_test_runs_stay_in_the_runs_table(harness, started):
    test_run(harness)

    assert harness.call('post', agent_body(enabled=False))[1] == 201

    assert len(drafts(harness)) == 1
    assert len(runs(harness)) == 1


@pytest.fixture
def reads(monkeypatch):
    """The read helpers the run routes import late; their own tests read real tables."""
    module = types.ModuleType(f'{PACKAGE}.utils.schedule_run_detail')
    module.version_labels = lambda session, ids: {i: 'v3' for i in ids}
    module.usage_summaries = lambda session, project_id, rows: {row.id: (10, 0.5) for row in rows}
    module.version_label = lambda session, version_id: 'v3'
    module.tool_call_events = lambda session, conversation_id: []
    module.usage_summary = lambda session, project_id, run: (10, 0.5)
    monkeypatch.setitem(sys.modules, module.__name__, module)


def read_runs(harness, application_id=1, **query):
    return harness.call('get', suffix='/test_run', query={'application_id': application_id, **query})


def read_run(harness, run_id, application_id=1):
    harness.body, harness.query = None, {'application_id': application_id}
    harness.path = f'/schedules/prompt_lib/{PROJECT_ID}/test_run/{run_id}'
    return harness.routes.PromptLibAPI().get(project_id=PROJECT_ID, run_id=run_id)


def test_the_post_answer_has_the_run_id_and_the_application_id(harness, started):
    assert test_run(harness)[0] == {'run_id': 1, 'application_id': 1}


def test_run_now_answers_with_the_same_shape(harness, started):
    schedule_id = harness.seed()

    body, status = harness.call('post', {'trigger': 'manual'}, schedule_id, suffix='/run')

    assert (status, body) == (202, {'run_id': 1, 'application_id': 1})


def test_the_test_run_list_has_the_rows_of_the_runs_list_newest_first(harness, started, reads):
    test_run(harness)
    with harness.session_factory() as session:
        session.query(models().EntityScheduleRun).update({'status': 'succeeded'})
        session.commit()
    test_run(harness)

    body, status = read_runs(harness)

    assert status == 200 and body['total'] == 2
    assert [row['id'] for row in body['rows']] == [2, 1]
    assert {'id', 'trigger', 'status', 'conversation_id', 'duration_ms', 'version_label', 'tokens', 'cost_usd'} <= set(body['rows'][0])
    assert read_runs(harness, limit=1, offset=1)[0]['rows'][0]['id'] == 1


def test_the_test_run_list_is_empty_without_a_draft_and_hides_other_owners_runs(harness, started, reads):
    assert read_runs(harness)[0] == {'total': 0, 'rows': []}
    harness.user_id = OTHER_ID
    test_run(harness)
    harness.user_id = OWNER_ID
    assert read_runs(harness)[0] == {'total': 0, 'rows': []}
    assert read_runs(harness, application_id=2)[0]['total'] == 0


def test_the_test_run_list_needs_the_run_permission_and_an_application_id(harness, reads):
    assert harness.call('get', suffix='/test_run', query={})[1] == 400
    harness.permissions.discard(PERMISSION_PREFIX + 'run')
    assert read_runs(harness)[1] == 403


def test_the_test_run_detail_has_the_body_of_the_run_detail(harness, started, reads):
    run_id = test_run(harness)[0]['run_id']

    body, status = read_run(harness, run_id)

    assert status == 200
    assert (body['id'], body['trigger'], body['version_label']) == (run_id, 'test', 'v3')
    assert body['summary']['tokens'] == 10 and body['summary']['cost_usd'] == 0.5
    assert {'events', 'status', 'conversation_id', 'approval_rule'} <= set(body)


def test_the_test_run_detail_is_404_for_a_foreign_a_saved_or_a_missing_run(harness, started, reads):
    own = test_run(harness)[0]['run_id']
    saved_run = harness.call('post', {'trigger': 'manual'}, harness.seed(name='Saved'), suffix='/run')[0]['run_id']
    harness.user_id = OTHER_ID
    assert read_run(harness, own)[1] == 404
    harness.user_id = OWNER_ID

    for run_id in (saved_run, 999):
        assert read_run(harness, run_id) == ({'code': 'run_not_found', 'error': 'Run not found'}, 404)
    assert read_run(harness, own, application_id=2)[1] == 404


def test_the_test_run_detail_needs_the_run_permission_and_an_application_id(harness, started, reads):
    run_id = test_run(harness)[0]['run_id']
    assert read_run(harness, run_id, application_id='x')[1] == 400
    harness.permissions.discard(PERMISSION_PREFIX + 'run')
    assert read_run(harness, run_id)[1] == 403
