"""EL-7011 US-17: save warnings and the approval rule on the schedule handlers (R2.4, R3.1-R3.4).

Uses the US-07 harness (real handler, service and models, SQLite rows). The risk scan and the
project setting "Allow Run without asking" are stubbed there; their own tests live in unit/utils.

Run via:
    python tests/run_tests.py unit/test_7011_approval_rule_api.py -v
"""
import pytest
from unit.test_7011_schedule_crud_api import OWNER_ID, PROJECT_ID, agent_body
from unit.test_7011_schedule_crud_api import harness as _crud_harness

harness = _crud_harness  # the CRUD harness fixture, shared

WARNING = {'code': 'token_may_expire', 'message': 'The token may expire'}  # a notice: never blocks a save


def count_rows(harness):
    with harness.session_factory() as session:
        return session.query(harness.EntitySchedule).count()


# R2.4 (TC-US17-10, TC-US17-11)
def test_7011_r2_4_save_with_warnings_keeps_enabled_true(harness):
    harness.risk_warnings = [WARNING]
    body, status = harness.call('post', agent_body(enabled=True))
    assert status == 201
    assert body['warnings'] == [WARNING]
    assert harness.row(body['id']).enabled is True
    assert harness.scans == [(PROJECT_ID, 10, OWNER_ID)]
    updated, status = harness.call('put', {'name': 'Renamed'}, schedule_id=body['id'])
    assert status == 200 and updated['warnings'] == [WARNING]
    assert harness.row(body['id']).enabled is True


def test_7011_r2_4_save_with_warnings_keeps_enabled_false(harness):
    harness.risk_warnings = [WARNING]
    body, status = harness.call('post', agent_body(enabled=False))
    assert status == 201 and body['warnings'] == [WARNING]
    assert harness.row(body['id']).enabled is False


def test_7011_r2_4_save_without_warnings_returns_an_empty_list(harness):
    body, _ = harness.call('post', agent_body())
    assert body['warnings'] == []


# R3.1 (TC-US17-16)
def test_7011_r3_1_create_without_a_rule_stores_pause_and_ask(harness):
    body, status = harness.call('post', agent_body())
    assert status == 201 and body['approval_rule'] == 'pause_and_ask'
    assert harness.row(body['id']).approval_rule == 'pause_and_ask'


# R3.2 (TC-US17-12, TC-US17-13, TC-US17-14)
def test_7011_r3_2_run_without_asking_is_stored_when_the_project_allows_it(harness):
    harness.run_without_asking_allowed = True
    body, status = harness.call('post', agent_body(approval_rule='run_without_asking'))
    assert status == 201 and body['approval_rule'] == 'run_without_asking'
    assert harness.row(body['id']).approval_rule == 'run_without_asking'


def test_7011_r3_2_run_without_asking_is_400_and_saves_nothing_when_not_allowed(harness):
    body, status = harness.call('post', agent_body(approval_rule='run_without_asking'))
    assert status == 400 and body['code'] == 'approval_rule_not_allowed' and body['field'] == 'approval_rule'
    assert count_rows(harness) == 0


def test_7011_r3_2_update_to_run_without_asking_is_400_and_keeps_the_rule(harness):
    schedule_id = harness.seed()
    body, status = harness.call('put', {'approval_rule': 'run_without_asking'}, schedule_id=schedule_id)
    assert status == 400 and body['code'] == 'approval_rule_not_allowed' and body['field'] == 'approval_rule'
    assert harness.row(schedule_id).approval_rule == 'pause_and_ask'


def test_7011_r3_2_update_to_run_without_asking_is_stored_when_allowed(harness):
    harness.run_without_asking_allowed = True
    schedule_id = harness.seed()
    body, status = harness.call('put', {'approval_rule': 'run_without_asking'}, schedule_id=schedule_id)
    assert status == 200 and body['approval_rule'] == 'run_without_asking'


# R3.3 (TC-US17-15)
@pytest.mark.parametrize('verb', ['post', 'put'])
def test_7011_r3_3_unknown_rule_is_400(harness, verb):
    schedule_id = harness.seed()
    body = agent_body(approval_rule='always') if verb == 'post' else {'approval_rule': 'always'}
    response, status = harness.call(verb, body, schedule_id=schedule_id if verb == 'put' else None)
    assert status == 400 and response['field'] == 'approval_rule'


# R3.4 (TC-US17-17, TC-US17-18)
@pytest.mark.parametrize('allowed', [True, False])
def test_7011_r3_4_create_get_and_update_return_the_flag_as_a_boolean(harness, allowed):
    harness.run_without_asking_allowed = allowed
    created, _ = harness.call('post', agent_body())
    fetched, _ = harness.call('get', schedule_id=created['id'])
    updated, _ = harness.call('put', {'name': 'Renamed'}, schedule_id=created['id'])
    assert [r['run_without_asking_allowed'] for r in (created, fetched, updated)] == [allowed] * 3
