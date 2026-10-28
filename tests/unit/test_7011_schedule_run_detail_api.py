"""EL-7011 owner CR 2026-10-21 (test panel log): `GET .../{schedule_id}/runs/{run_id}`.

The harness of the schedule resource tests is shared: the route module, the handler and the SQLite
tables are real. The read helpers of `utils/schedule_run_detail.py` (version name, tool calls of the
conversation, usage) are stand-ins here; their own test is integration/test_7011_run_detail_read.py.

Run via:
    python tests/run_tests.py unit/test_7011_schedule_run_detail_api.py -v
"""
import datetime
import pathlib
import sys
import types

import pytest
from fixtures import schedule_sqlite as sqlite_db

sys.path.insert(0, str(pathlib.Path(__file__).parent))  # the shared harness lives beside this file
from test_7011_schedule_crud_api import (
    ADMIN,
    ADMIN_ID,
    NOT_FOUND,
    OTHER_ID,
    PACKAGE,
    PERMISSION_PREFIX,
    PROJECT_ID,
)
from test_7011_schedule_crud_api import harness as _crud_harness
from test_7011_schedule_runs_api import FIELDS, add_runs

harness = _crud_harness  # the CRUD harness fixture, shared

START = datetime.datetime(2026, 10, 1, tzinfo=datetime.UTC)
RUN_NOT_FOUND = {'code': 'run_not_found', 'error': 'Run not found'}
STARTED = {'at': '2026-10-01T00:00:00+00:00', 'kind': 'started', 'text': 'Test run started', 'level': 'info'}
FINISHED = {'at': '2026-10-01T00:00:38+00:00', 'kind': 'finished', 'text': 'Run finished: success',
            'level': 'success', 'status': 'success'}
TOOL = {'at': '2026-10-01T00:00:10+00:00', 'kind': 'tool_call', 'text': 'Tool call: github · list_issues',
        'level': 'info', 'toolkit': 'github', 'tool': 'list_issues', 'duration_ms': 1200}
DETAIL_FIELDS = FIELDS | {
    'version_label', 'principal_label', 'risk_outcome', 'approval_rule', 'approval_expires_at', 'summary', 'events',
}


@pytest.fixture
def reads(monkeypatch):
    """The read helpers the handler imports late; `usage` is what the usage store answers."""
    state = types.SimpleNamespace(usage=(2140, 0.03), conversations=[], tool_events=[TOOL])
    module = types.ModuleType(f'{PACKAGE}.utils.schedule_run_detail')
    module.version_label = lambda session, version_id: {10: 'v3'}.get(version_id)

    def tool_call_events(session, conversation_id):
        state.conversations.append(conversation_id)
        return state.tool_events if conversation_id else []

    module.tool_call_events = tool_call_events
    module.usage_summary = lambda session, project_id, run: state.usage
    monkeypatch.setitem(sys.modules, module.__name__, module)
    return state


def seed_run(harness, schedule_id, **columns):
    """One finished test run of 38 s with two stored events; returns its id."""
    add_runs(harness, schedule_id, {
        'trigger': 'test', 'conversation_id': 901, 'version_id': 10, 'principal_label': 'Ann Smith (schedule owner)',
        'risk_outcome': 'needs_approval', 'finished_at': START + datetime.timedelta(seconds=38),
        'events': [STARTED, FINISHED], **columns,
    })
    run_model = sys.modules[f'{PACKAGE}.models.entity_schedule'].EntityScheduleRun
    with harness.session_factory() as session:
        return session.scalar(sqlite_db.sa.select(sqlite_db.sa.func.max(run_model.id)))


def call_run(harness, verb, schedule_id, run_id):
    harness.body, harness.query = None, {}
    harness.path = f'/schedules/prompt_lib/{PROJECT_ID}/{schedule_id}/runs/{run_id}'
    handler = harness.routes.PromptLibAPI()
    handler.module = object()
    return getattr(handler, verb)(project_id=PROJECT_ID, schedule_id=schedule_id, run_id=run_id)


def test_cr_el7011_the_detail_has_the_run_version_account_risk_summary_and_events(harness, reads):
    schedule_id = harness.seed(approval_rule='pause_and_ask')
    run_id = seed_run(harness, schedule_id)

    body, status = call_run(harness, 'get', schedule_id, run_id)

    assert (status, set(body)) == (200, DETAIL_FIELDS)
    assert (body['id'], body['trigger'], body['status'], body['conversation_id']) == (run_id, 'test', 'success', 901)
    assert (body['version_id'], body['version_label']) == (10, 'v3')
    assert (body['principal_label'], body['risk_outcome'], body['approval_rule']) == (
        'Ann Smith (schedule owner)', 'needs_approval', 'pause_and_ask')
    assert body['summary'] == {'duration_ms': 38000, 'tokens': 2140, 'cost_usd': 0.03}
    assert body['duration_ms'] == 38000
    # The stored events and the tool calls of the conversation, in time order
    assert [event['kind'] for event in body['events']] == ['started', 'tool_call', 'finished']
    assert reads.conversations == [901]


def test_cr_el7011_the_merged_events_stay_at_the_event_limit_and_keep_the_last(harness, reads):
    schedule_id = harness.seed()
    start = datetime.datetime(2026, 10, 1, 1, tzinfo=datetime.UTC)
    reads.tool_events = [
        {**TOOL, 'at': (start + datetime.timedelta(seconds=n)).isoformat(), 'text': f'tool {n}'} for n in range(250)]
    run_id = seed_run(harness, schedule_id)

    body, _ = call_run(harness, 'get', schedule_id, run_id)

    assert len(body['events']) == 200
    assert (body['events'][0]['kind'], body['events'][-1]['text']) == ('started', 'tool 249')


def test_cr_el7011_a_missing_usage_row_gives_null_tokens_and_cost(harness, reads):
    reads.usage = (None, None)
    schedule_id = harness.seed()
    run_id = seed_run(harness, schedule_id, conversation_id=None, finished_at=None, status='running', events=None)

    body, status = call_run(harness, 'get', schedule_id, run_id)

    assert status == 200
    assert body['summary'] == {'duration_ms': None, 'tokens': None, 'cost_usd': None}
    assert (body['events'], body['finished_at']) == ([], None)


def test_cr_el7011_a_run_with_a_deleted_version_has_no_label(harness, reads):
    schedule_id = harness.seed()
    run_id = seed_run(harness, schedule_id, version_id=99)

    body, _ = call_run(harness, 'get', schedule_id, run_id)

    assert (body['version_id'], body['version_label']) == (99, None)


def test_cr_el7011_an_admin_reads_the_run_of_another_owner(harness, reads):
    schedule_id = harness.seed(created_by=OTHER_ID)
    run_id = seed_run(harness, schedule_id)
    harness.request_as(ADMIN_ID, ADMIN)

    assert call_run(harness, 'get', schedule_id, run_id)[1] == 200


def test_cr_el7011_a_regular_user_gets_404_for_the_run_of_another_owner(harness, reads):
    schedule_id = harness.seed(created_by=OTHER_ID)
    run_id = seed_run(harness, schedule_id)

    assert call_run(harness, 'get', schedule_id, run_id) == (NOT_FOUND, 404)


def test_cr_el7011_the_detail_needs_the_details_permission(harness, reads):
    schedule_id = harness.seed()
    run_id = seed_run(harness, schedule_id)
    harness.permissions.discard(PERMISSION_PREFIX + 'details')

    assert call_run(harness, 'get', schedule_id, run_id)[1] == 403


def test_cr_el7011_a_run_of_another_schedule_or_an_unknown_run_is_404(harness, reads):
    schedule_id = harness.seed()
    other_id = harness.seed(name='Other')
    run_id = seed_run(harness, other_id)

    assert call_run(harness, 'get', schedule_id, run_id) == (RUN_NOT_FOUND, 404)
    assert call_run(harness, 'get', schedule_id, 9999) == (RUN_NOT_FOUND, 404)


@pytest.mark.parametrize('verb', ['post', 'put', 'delete'])
def test_cr_el7011_other_verbs_on_the_run_url_are_refused(harness, reads, verb):
    schedule_id = harness.seed()
    run_id = seed_run(harness, schedule_id)

    assert call_run(harness, verb, schedule_id, run_id)[1] == 405
    assert harness.row(schedule_id).name == 'Nightly'
