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
    NOT_FOUND,
    OWNER_ID,
    PACKAGE,
    PROJECT_ID,
)
from unit.test_7011_schedule_crud_api import harness as _crud_harness

harness = _crud_harness  # the CRUD harness fixture, shared


class Decisions:
    """Fake decision writers; `check_error` and `apply_error` are raised when set."""

    def __init__(self, runs):
        self.checks, self.applied = [], []
        self.check_error = None
        self.runs = runs

    def check(self, project_id, run_id, caller_id, actions):
        self.checks.append((run_id, caller_id, actions))
        if self.check_error:
            raise self.check_error
        return 'reject' if 'reject' in actions else 'approve'

    def apply(self, project_id, run_id, decision):
        self.applied.append((run_id, decision))


@pytest.fixture
def decisions(harness, monkeypatch):
    runs = types.ModuleType(f'{PACKAGE}.utils.schedule_runs')

    class ScheduleDecisionForbidden(Exception):
        pass

    class ScheduleRunNotWaiting(Exception):
        pass

    state = Decisions(runs)
    runs.ScheduleDecisionForbidden, runs.ScheduleRunNotWaiting = ScheduleDecisionForbidden, ScheduleRunNotWaiting
    runs.check_schedule_decision, runs.apply_schedule_decision = state.check, state.apply
    monkeypatch.setitem(sys.modules, runs.__name__, runs)
    state.started = []
    harness.module.start_schedule_run = lambda *args: state.started.append(args) or state.start_ok
    state.start_ok = True
    return state


def add_run(harness, schedule_id, status='waiting_for_approval', **columns):
    run_model = sys.modules[f'{PACKAGE}.models.entity_schedule'].EntityScheduleRun
    with harness.session_factory() as session:
        run = run_model(schedule_id=schedule_id, trigger='cron', status=status, **columns)
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


def test_the_owner_rejects_and_the_run_does_not_start(harness, decisions):
    schedule_id = harness.seed()
    run_id = add_run(harness, schedule_id)

    assert decide(harness, schedule_id, run_id, 'reject') == ({'run_id': run_id, 'status': 'failed'}, 200)
    assert decisions.applied == [(run_id, 'reject')]
    assert decisions.started == []


def test_an_admin_who_is_not_the_owner_gets_403_not_owner(harness, decisions):
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
    assert NOT_FOUND['code'] == 'not_found'


def test_a_start_failure_after_approval_fails_the_run_and_answers_500(harness, decisions):
    schedule_id = harness.seed()
    run_id = add_run(harness, schedule_id, status='running')
    decisions.start_ok = False

    body, status = decide(harness, schedule_id, run_id)

    assert (status, body['code']) == (500, 'run_start_failed')
    assert run_status(harness, run_id)[0] == 'failed'


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
