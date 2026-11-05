"""The decision route of a waiting run: `POST .../runs/<id>/decision` and its dispatch.

Uses the schedule resource harness: real handlers, service, models and SQLite rows. The decision writers
of `utils/schedule_runs.py` are faked, and the run starter is replaced.
"""
import sys
import types

import pytest
from unit.test_7011_schedule_crud_api import (
    ADMIN,
    ADMIN_ID,
    OTHER_ID,
    OWNER_ID,
    PACKAGE,
    PROJECT_ID,
)
from unit.test_7011_schedule_crud_api import harness as _crud_harness

harness = _crud_harness  # the CRUD harness fixture, shared


class Decisions:
    """Fake decision writers; `check_error` and `apply_error` are raised when set."""

    def __init__(self, runs):
        self.checks, self.applied, self.applied_by, self.admin_flags = [], [], [], []
        self.check_error = None
        self.runs = runs

    def check(self, project_id, run_id, caller_id, actions, fan_out=False, sign_in=False, is_admin=None,
              conversation_id=None):
        self.checks.append((run_id, caller_id, actions))
        self.admin_flags.append(is_admin)
        if self.check_error:
            raise self.check_error
        return 'reject' if 'reject' in actions else 'approve'

    def apply(self, project_id, run_id, decision, decided_by=None):
        self.applied.append((run_id, decision))
        self.applied_by.append(decided_by)


@pytest.fixture
def decisions(harness, monkeypatch):
    runs = types.ModuleType(f'{PACKAGE}.utils.schedule_runs')

    class ScheduleDecisionForbidden(Exception):
        pass

    class ScheduleRunNotWaiting(Exception):
        pass

    state = Decisions(runs)
    runs.run_actor_id = lambda session, run: (
        run.requested_by if run.trigger in ('manual', 'test') and run.requested_by is not None
        else harness.row(run.schedule_id).created_by if run.schedule_id else run.requested_by)
    runs.ScheduleDecisionForbidden, runs.ScheduleRunNotWaiting = ScheduleDecisionForbidden, ScheduleRunNotWaiting
    runs.check_schedule_decision, runs.apply_schedule_decision = state.check, state.apply
    monkeypatch.setitem(sys.modules, runs.__name__, runs)
    state.started = []
    prerun = types.ModuleType(f'{PACKAGE}.utils.schedule_prerun')
    state.missing = None
    prerun.check_owner_credentials = lambda project_id, schedule: state.missing
    prerun.credential_wait = lambda reason: reason is not None
    state.checked, state.forgotten = [], []
    prerun.mark_credentials_checked = state.checked.append
    prerun.forget_credentials_checked = state.forgotten.append
    monkeypatch.setitem(sys.modules, prerun.__name__, prerun)
    harness.module.start_schedule_run = lambda *args: state.started.append(args) or state.start_ok
    state.start_ok = True
    return state


def add_run(harness, schedule_id, status='waiting_for_approval', **columns):
    run_model = sys.modules[f'{PACKAGE}.models.entity_schedule'].EntityScheduleRun
    with harness.session_factory() as session:
        run = run_model(schedule_id=schedule_id, status=status, **{'trigger': 'cron', **columns})
        session.add(run)
        session.commit()
        return run.id


def decide(harness, schedule_id, run_id, action='approve', verb='post'):
    harness.body, harness.query = {'action': action}, {}
    harness.path = f'/schedules/prompt_lib/{PROJECT_ID}/{schedule_id}/runs/{run_id}/decision'
    handler = harness.routes.PromptLibAPI()
    handler.module = object()
    return getattr(handler, verb)(project_id=PROJECT_ID, schedule_id=schedule_id, run_id=run_id)


def run_status(harness, run_id):
    run_model = sys.modules[f'{PACKAGE}.models.entity_schedule'].EntityScheduleRun
    with harness.session_factory() as session:
        run = session.get(run_model, run_id)
        return run.status, run.reason


def test_the_owner_approves_and_the_run_starts(harness, decisions):
    schedule_id = harness.seed()
    run_id = add_run(harness, schedule_id)

    assert decide(harness, schedule_id, run_id) == ({'run_id': run_id, 'status': 'running'}, 200)
    assert decisions.applied == [(run_id, 'approve')]
    assert decisions.started == [('agent', PROJECT_ID, schedule_id, run_id)]
    assert decisions.checks == [(run_id, OWNER_ID, ['approve'])]


def test_approve_while_the_credential_is_missing_is_409_and_the_run_keeps_waiting(harness, decisions):
    schedule_id = harness.seed()
    run_id = add_run(harness, schedule_id)
    decisions.missing = 'credential qa_missing not found'

    body, status = decide(harness, schedule_id, run_id)

    assert (status, body['code']) == (409, 'credential_missing')
    assert body['error'] == 'The credential is still missing. Add it, then approve again.'
    assert decisions.applied == [] and decisions.started == []
    assert run_status(harness, run_id) == ('waiting_for_approval', None)


def test_a_clean_credential_check_is_remembered_for_the_restarted_run(harness, decisions):
    schedule_id = harness.seed()
    run_id = add_run(harness, schedule_id)

    decide(harness, schedule_id, run_id)

    assert decisions.checked == [run_id]


def test_an_error_in_the_credential_check_is_logged_and_the_run_still_starts(harness, decisions, monkeypatch):
    schedule_id = harness.seed()
    run_id = add_run(harness, schedule_id)
    logged = []
    monkeypatch.setattr(harness.module.log, 'exception', lambda *args: logged.append(args), raising=False)
    prerun = sys.modules[f'{PACKAGE}.utils.schedule_prerun']

    def explode(project_id, schedule):
        raise RuntimeError('token service down')

    prerun.check_owner_credentials = explode

    assert decide(harness, schedule_id, run_id) == ({'run_id': run_id, 'status': 'running'}, 200)
    assert len(logged) == 1
    assert decisions.started == [('agent', PROJECT_ID, schedule_id, run_id)]
    assert decisions.checked == []  # the restart runs the check itself


def test_the_restart_fails_the_run_with_the_reason_when_the_credential_is_still_missing(harness, decisions):
    schedule_id = harness.seed()
    run_id = add_run(harness, schedule_id, status='running')
    harness.module.start_schedule_run = lambda *args: False

    body, status = decide(harness, schedule_id, run_id)

    assert (status, body['code']) == (500, 'run_start_failed')
    assert run_status(harness, run_id) == ('failed', 'the run could not start')
    assert decisions.forgotten == [run_id]


def test_reject_does_not_check_the_credential(harness, decisions):
    schedule_id = harness.seed()
    run_id = add_run(harness, schedule_id)
    decisions.missing = 'credential qa_missing not found'

    assert decide(harness, schedule_id, run_id, 'reject') == ({'run_id': run_id, 'status': 'failed'}, 200)
    assert decisions.applied == [(run_id, 'reject')]


def test_the_owner_rejects_and_the_run_does_not_start(harness, decisions):
    schedule_id = harness.seed()
    run_id = add_run(harness, schedule_id)

    assert decide(harness, schedule_id, run_id, 'reject') == ({'run_id': run_id, 'status': 'failed'}, 200)
    assert decisions.applied == [(run_id, 'reject')]
    assert decisions.started == []


@pytest.mark.parametrize('action', ['approve', 'reject'])
def test_a_project_admin_who_is_not_the_owner_decides_and_is_recorded(harness, decisions, action):
    harness.request_as(ADMIN_ID, ADMIN)
    schedule_id = harness.seed()
    run_id = add_run(harness, schedule_id)

    body, status = decide(harness, schedule_id, run_id, action)

    assert (status, body['status']) == (200, 'running' if action == 'approve' else 'failed')
    assert decisions.checks == [(run_id, ADMIN_ID, [action])]
    assert decisions.admin_flags == [True]
    assert decisions.applied == [(run_id, action)] and decisions.applied_by == [ADMIN_ID]


def test_the_owner_is_recorded_as_the_one_who_decided(harness, decisions):
    schedule_id = harness.seed()
    run_id = add_run(harness, schedule_id)

    decide(harness, schedule_id, run_id)

    assert decisions.applied_by == [OWNER_ID]


def test_a_caller_the_check_refuses_gets_403_not_owner(harness, decisions):
    harness.request_as(ADMIN_ID, ADMIN)
    schedule_id = harness.seed()
    run_id = add_run(harness, schedule_id)
    decisions.check_error = sys.modules[f'{PACKAGE}.utils.schedule_runs'].ScheduleDecisionForbidden()

    body, status = decide(harness, schedule_id, run_id)

    assert (status, body['code']) == (403, 'not_owner')
    assert decisions.applied == [] and decisions.started == []


def test_a_run_with_a_chat_is_409_run_has_chat(harness, decisions):
    schedule_id = harness.seed()
    run_id = add_run(harness, schedule_id, conversation_id=55)

    body, status = decide(harness, schedule_id, run_id)

    assert (status, body['code']) == (409, 'run_has_chat')
    assert decisions.checks == []


def test_a_run_that_is_not_waiting_is_409_run_not_waiting(harness, decisions):
    schedule_id = harness.seed()
    run_id = add_run(harness, schedule_id, status='success')
    decisions.check_error = sys.modules[f'{PACKAGE}.utils.schedule_runs'].ScheduleRunNotWaiting()

    body, status = decide(harness, schedule_id, run_id)

    assert (status, body['code']) == (409, 'run_not_waiting')
    assert decisions.applied == [] and decisions.started == []


def test_a_failed_decision_write_leaves_no_credential_mark(harness, decisions):
    schedule_id = harness.seed()
    run_id = add_run(harness, schedule_id)

    def lost_race(project_id, run_id, decision, decided_by=None):
        raise sys.modules[f'{PACKAGE}.utils.schedule_runs'].ScheduleRunNotWaiting()

    decisions.runs.apply_schedule_decision = lost_race

    body, status = decide(harness, schedule_id, run_id)

    assert (status, body['code']) == (409, 'run_not_waiting')
    assert decisions.checked == []


def test_a_missing_run_is_404(harness, decisions):
    schedule_id = harness.seed()

    body, status = decide(harness, schedule_id, 999)

    assert (status, body['code']) == (404, 'run_not_found')


def test_a_run_of_another_schedule_is_404(harness, decisions):
    schedule_id, other_id = harness.seed(), harness.seed(name='Other')
    run_id = add_run(harness, other_id)

    body, status = decide(harness, schedule_id, run_id)

    assert (status, body['code']) == (404, 'run_not_found')
    assert decisions.checks == []


def test_a_missing_schedule_is_404(harness, decisions):
    assert decide(harness, 999, 1)[1] == 404


def test_a_start_failure_after_approval_fails_the_run_and_answers_500(harness, decisions):
    schedule_id = harness.seed()
    run_id = add_run(harness, schedule_id, status='running')
    decisions.start_ok = False

    body, status = decide(harness, schedule_id, run_id)

    assert (status, body['code']) == (500, 'run_start_failed')
    assert run_status(harness, run_id)[0] == 'failed'


def test_an_error_while_starting_an_approved_run_fails_the_run_with_a_reason(harness, decisions):
    schedule_id = harness.seed()
    run_id = add_run(harness, schedule_id, status='running')

    def explode(*args):
        raise RuntimeError('no starter')

    harness.module.start_schedule_run = explode

    body, status = decide(harness, schedule_id, run_id)

    assert (status, body['code']) == (500, 'run_start_failed')
    assert run_status(harness, run_id) == ('failed', 'the run could not start')


def test_a_post_on_the_decision_path_runs_the_decision_handler(harness, decisions, monkeypatch):
    seen = []
    monkeypatch.setattr(harness.routes.PromptLibAPI, '_decide_run', lambda self, **ids: seen.append(ids) or ('ok', 200),
                        raising=False)

    assert decide(harness, 5, 6) == ('ok', 200)
    assert seen == [{'project_id': PROJECT_ID, 'schedule_id': 5, 'run_id': 6}]


def test_a_get_on_the_decision_path_does_not_run_the_decision_handler(harness, decisions, monkeypatch):
    seen = []
    monkeypatch.setattr(harness.routes.PromptLibAPI, '_decide_run', lambda self, **ids: seen.append(ids) or ('ok', 200),
                        raising=False)
    monkeypatch.setattr(harness.routes.PromptLibAPI, '_run_detail', lambda self, **ids: ('detail', 200), raising=False)

    assert decide(harness, 5, 6, verb='get') != ('ok', 200)
    assert seen == []


def test_the_tail_of_a_run_path_is_the_decision_only_when_it_ends_in_decision(harness):
    harness.path = f'/schedules/prompt_lib/{PROJECT_ID}/5/runs/6/decision/'
    assert harness.routes._tail(5, 6).endswith('/decision')
    harness.path = f'/schedules/prompt_lib/{PROJECT_ID}/5/runs/6'
    assert not harness.routes._tail(5, 6).endswith('/decision')


def add_test_run(harness, requested_by=OWNER_ID, status='waiting_for_approval', application_id=1, **columns):
    """A Test Run: no schedule, the form and the agent in the unmapped columns."""
    models = sys.modules[f'{PACKAGE}.models.entity_schedule']
    form = {'entity_type': 'agent', 'application_id': application_id, 'version_id': 10, 'approval_rule': 'pause_and_ask'}
    with harness.session_factory() as session:
        run_id = session.execute(models.EntityScheduleRun.__table__.insert().values(
            schedule_id=None, trigger='test', status=status, requested_by=requested_by,
            **{models.RUN_FORM.key: form, models.RUN_APPLICATION_ID.key: application_id}, **columns,
        )).inserted_primary_key[0]
        session.commit()
    return run_id


def decide_test_run(harness, run_id, action='approve'):
    harness.body, harness.query = {'action': action}, {}
    harness.path = f'/schedules/prompt_lib/{PROJECT_ID}/test_run/{run_id}/decision'
    handler = harness.routes.PromptLibAPI()
    handler.module = object()
    return handler.post(project_id=PROJECT_ID, run_id=run_id)


def test_the_owner_approves_a_waiting_test_run_and_the_run_starts(harness, decisions):
    run_id = add_test_run(harness)

    assert decide_test_run(harness, run_id) == ({'run_id': run_id, 'status': 'running'}, 200)
    assert decisions.applied == [(run_id, 'approve')]
    assert decisions.started == [('agent', PROJECT_ID, None, run_id)]
    assert decisions.checks == [(run_id, OWNER_ID, ['approve'])]


def test_approving_a_test_run_checks_the_credential_of_the_form_first(harness, decisions):
    run_id = add_test_run(harness)
    decisions.missing = 'credential qa_missing not found'
    seen = []
    prerun = sys.modules[f'{PACKAGE}.utils.schedule_prerun']
    prerun.check_owner_credentials = lambda project_id, owner: seen.append(owner) or decisions.missing

    body, status = decide_test_run(harness, run_id)

    assert (status, body['code']) == (409, 'credential_missing')
    assert (seen[0].created_by, seen[0].application_id, seen[0].version_id) == (OWNER_ID, 1, 10)
    assert decisions.applied == [] and decisions.started == []


def test_rejecting_a_test_run_does_not_start_it(harness, decisions):
    run_id = add_test_run(harness)

    assert decide_test_run(harness, run_id, 'reject') == ({'run_id': run_id, 'status': 'failed'}, 200)
    assert decisions.started == []


def test_a_test_run_with_a_chat_is_decided_in_that_chat(harness, decisions):
    run_id = add_test_run(harness, conversation_id=5)

    body, status = decide_test_run(harness, run_id)

    assert (status, body['code']) == (409, 'run_has_chat')


def test_another_owners_test_run_answers_404(harness, decisions):
    run_id = add_test_run(harness, requested_by=OTHER_ID)

    assert decide_test_run(harness, run_id)[1] == 404
    assert decisions.applied == []


def test_an_admin_gets_404_for_a_test_run_of_another_user(harness, decisions):
    harness.request_as(ADMIN_ID, ADMIN)
    run_id = add_test_run(harness)

    assert decide_test_run(harness, run_id)[1] == 404


def test_a_saved_run_answers_404_on_the_test_run_decision_route(harness, decisions):
    run_id = add_run(harness, harness.seed())

    assert decide_test_run(harness, run_id)[1] == 404
    assert decisions.applied == []


def test_a_test_run_that_cannot_restart_fails_with_the_reason(harness, decisions):
    run_id = add_test_run(harness, status='running')
    decisions.start_ok = False

    body, status = decide_test_run(harness, run_id)

    assert (status, body['code']) == (500, 'run_start_failed')
    assert run_status(harness, run_id) == ('failed', 'the run could not start')


def test_the_decision_tail_of_a_test_run_has_no_schedule_id(harness):
    harness.path = f'/schedules/prompt_lib/{PROJECT_ID}/test_run/6/decision'
    assert harness.routes._tail(None, 6) == 'test_run/<int:run_id>/decision'
    assert harness.routes.ROUTES[('post', harness.routes._tail(None, 6))] == '_decide_test_run'
