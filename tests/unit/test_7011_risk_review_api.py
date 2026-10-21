"""EL-7011 owner CR 2026-10-21: the server refuses risky saves (US-17, US-18, US-33).

The dry-run `check`, the save refusals `schedule_blocked`, `risk_confirmation_required`, `risk_changed`
and the name check of `risk_ack`, and the record of who confirmed "Run without asking". Uses the
US-07 harness: real handlers, service, models and SQLite rows; the risk scan is a fake fed by
`harness.risk_warnings`.

Run via:
    python tests/run_tests.py unit/test_7011_risk_review_api.py -v
"""
import sys
import types

import pytest
from unit.test_7011_schedule_crud_api import (
    EDITOR,
    OWNER_ID,
    PACKAGE,
    PERMISSION_PREFIX,
    PROJECT_ID,
    agent_body,
)
from unit.test_7011_schedule_crud_api import harness as _crud_harness

harness = _crud_harness  # the CRUD harness fixture, shared

SIGNIN = {'code': 'personal_signin_required', 'message': 'Toolkit "GitHub" needs a personal sign-in',
          'toolkit': 'GitHub', 'toolkit_type': 'github', 'reason': 'needs a personal sign-in'}
HUMAN = {'code': 'human_input_step', 'message': 'Node "Review" waits for a person', 'step': 'Review'}
CREDENTIAL = {'code': 'missing_credential', 'message': 'Toolkit "Jira" has no usable credential', 'toolkit': 'Jira'}
SENSITIVE = {'code': 'sensitive_action', 'message': 'Tool "send_email" needs approval', 'toolkit': 'Outlook',
             'toolkit_type': 'outlook', 'tool': 'send_email', 'risk_level': 'needs_approval',
             'badge': 'sends_messages', 'effect': 'Sends e-mail'}
INCOMPLETE = {'code': 'scan_incomplete', 'message': 'Toolkit "X" could not be checked for risks'}
NOTICE = {'code': 'token_may_expire', 'message': 'The token may expire'}
CHECK_BODY = {'entity_type': 'agent', 'application_id': 1, 'version_id': 10}


def count_rows(harness):
    with harness.session_factory() as session:
        return session.query(harness.EntitySchedule).count()


def _rwa(**changes):
    return agent_body(approval_rule='run_without_asking', **changes)


def _ack(name='Nightly', fingerprint='fp-1'):
    return {'fingerprint': fingerprint, 'confirmed_name': name}


@pytest.fixture
def named(harness, monkeypatch):
    """The auth lookup names user OWNER_ID 'Anna K.'."""
    authors = types.ModuleType(f'{PACKAGE}.utils.authors')
    authors.get_authors_data = lambda ids: [{'id': OWNER_ID, 'name': 'Anna K.'}]
    monkeypatch.setitem(sys.modules, authors.__name__, authors)
    return harness


class TestCheck:
    def test_el7011_cr_check_answers_the_review_and_writes_nothing(self, harness):
        harness.risk_warnings = [SIGNIN, SENSITIVE, NOTICE]
        harness.run_without_asking_allowed = True
        body, status = harness.call('post', CHECK_BODY, suffix='/check')
        assert status == 200
        assert body == {'blocking': [SIGNIN], 'sensitive': [SENSITIVE], 'notices': [NOTICE],
                        'run_without_asking_allowed': True, 'fingerprint': 'fp-1'}
        assert harness.scans == [(PROJECT_ID, 10, OWNER_ID)]
        assert count_rows(harness) == 0

    def test_el7011_cr_check_refuses_run_without_asking_when_the_scan_is_incomplete(self, harness):
        harness.risk_warnings = [INCOMPLETE]
        harness.run_without_asking_allowed = True
        body, _ = harness.call('post', CHECK_BODY, suffix='/check')
        assert body['run_without_asking_allowed'] is False

    def test_el7011_cr_check_follows_the_project_setting(self, harness):
        body, _ = harness.call('post', CHECK_BODY, suffix='/check')
        assert body['run_without_asking_allowed'] is False

    def test_el7011_cr_check_needs_the_create_permission(self, harness):
        harness.request_as(OWNER_ID, EDITOR - {PERMISSION_PREFIX + 'create'})
        _, status = harness.call('post', CHECK_BODY, suffix='/check')
        assert status == 403

    @pytest.mark.parametrize('body, field', [
        ({'entity_type': 'agent', 'application_id': 1, 'version_id': 11}, 'version_id'),
        ({'entity_type': 'agent', 'application_id': 1}, 'version_id'),
        ({'entity_type': 'pipeline', 'application_id': 1, 'version_id': 10}, 'entity_type'),
    ])
    def test_el7011_cr_check_refuses_a_version_that_is_not_of_the_entity(self, harness, body, field):
        response, status = harness.call('post', body, suffix='/check')
        assert (status, response['field']) == (400, field)
        assert harness.scans == []


class TestBlocked:
    @pytest.mark.parametrize('finding', [SIGNIN, HUMAN, CREDENTIAL])
    def test_el7011_cr_a_blocking_finding_refuses_the_create(self, harness, finding):
        harness.risk_warnings = [finding, NOTICE]
        body, status = harness.call('post', agent_body())
        assert status == 409
        assert body['code'] == 'schedule_blocked' and body['error']
        assert body['blocking'] == [finding] and body['notices'] == [NOTICE]
        assert count_rows(harness) == 0

    def test_el7011_cr_a_blocking_finding_refuses_the_edit_and_keeps_the_row(self, harness):
        schedule_id = harness.seed()
        harness.risk_warnings = [HUMAN]
        body, status = harness.call('put', {'name': 'Renamed'}, schedule_id=schedule_id)
        assert (status, body['code']) == (409, 'schedule_blocked')
        assert harness.row(schedule_id).name == 'Nightly'

    def test_el7011_cr_an_acknowledgement_never_unblocks(self, harness):
        harness.risk_warnings = [SIGNIN, SENSITIVE]
        body, status = harness.call('post', agent_body(risk_ack=_ack()))
        assert (status, body['code']) == (409, 'schedule_blocked')


class TestConfirmation:
    @pytest.mark.parametrize('rule', ['pause_and_ask', 'run_without_asking'])
    def test_el7011_cr_sensitive_tools_without_an_ack_need_a_confirmation(self, harness, rule):
        harness.run_without_asking_allowed = True
        harness.risk_warnings = [SENSITIVE]
        body, status = harness.call('post', agent_body(approval_rule=rule))
        assert status == 409
        assert body['code'] == 'risk_confirmation_required' and body['error']
        assert (body['sensitive'], body['fingerprint']) == ([SENSITIVE], 'fp-1')
        assert count_rows(harness) == 0

    def test_el7011_cr_pause_and_ask_with_the_ack_saves_without_a_name_and_records_nothing(self, harness):
        harness.risk_warnings = [SENSITIVE]
        body, status = harness.call('post', agent_body(risk_ack=_ack(name='')))
        assert status == 201 and body['warnings'] == [SENSITIVE]
        row = harness.row(body['id'])
        assert (row.approval_confirmed_by, row.approval_confirmed_at, row.approval_fingerprint) == (None, None, None)
        assert harness.audit == []

    def test_el7011_cr_run_without_asking_with_the_ack_saves_and_records_who_confirmed(self, named):
        named.run_without_asking_allowed = True
        named.risk_warnings = [SENSITIVE]
        body, status = named.call('post', _rwa(risk_ack=_ack(name=' Nightly ')))
        assert status == 201 and body['approval_rule'] == 'run_without_asking'
        row = named.row(body['id'])
        assert (row.approval_confirmed_by, row.approval_fingerprint) == (OWNER_ID, 'fp-1')
        assert row.approval_confirmed_at is not None
        assert body['approval_confirmed_by'] == 'Anna K.' and body['approval_confirmed_at']
        [(name, attributes)] = named.audit
        assert name.startswith('schedule_run_without_asking_confirmed') and 'Outlook.send_email' in name
        assert attributes['entity.id'] == str(body['id']) and attributes['user.id'] == OWNER_ID
        assert attributes['project.id'] == PROJECT_ID
        details, _ = named.call('get', schedule_id=body['id'])
        assert (details['approval_confirmed_by'], details['approval_confirmed_at']) == (
            'Anna K.', body['approval_confirmed_at'])

    def test_el7011_cr_run_without_asking_with_a_wrong_name_is_400_on_risk_ack(self, harness):
        harness.run_without_asking_allowed = True
        harness.risk_warnings = [SENSITIVE]
        body, status = harness.call('post', _rwa(risk_ack=_ack(name='nightly')))
        assert status == 400
        assert (body['code'], body['field']) == ('risk_name_mismatch', 'risk_ack') and body['error']
        assert count_rows(harness) == 0

    @pytest.mark.parametrize('rule', ['pause_and_ask', 'run_without_asking'])
    def test_el7011_cr_a_changed_fingerprint_is_409_risk_changed(self, harness, rule):
        harness.run_without_asking_allowed = True
        harness.risk_warnings = [SENSITIVE]
        harness.fingerprint = 'fp-2'
        body, status = harness.call('post', agent_body(approval_rule=rule, risk_ack=_ack()))
        assert status == 409
        assert (body['code'], body['fingerprint'], body['sensitive']) == ('risk_changed', 'fp-2', [SENSITIVE])
        assert count_rows(harness) == 0

    def test_el7011_cr_run_without_asking_is_refused_when_the_scan_is_incomplete(self, harness):
        harness.run_without_asking_allowed = True
        harness.risk_warnings = [INCOMPLETE]
        body, status = harness.call('post', _rwa())
        assert status == 400
        assert (body['code'], body['field']) == ('approval_rule_not_allowed', 'approval_rule')
        assert body['notices'] == [INCOMPLETE]
        assert count_rows(harness) == 0

    def test_el7011_cr_the_ack_must_be_an_object_with_a_fingerprint(self, harness):
        body, status = harness.call('post', agent_body(risk_ack={'confirmed_name': 'Nightly'}))
        assert (status, body['field']) == (400, 'risk_ack')

    def test_el7011_cr_no_sensitive_tool_needs_no_ack(self, harness):
        harness.run_without_asking_allowed = True
        body, status = harness.call('post', _rwa())
        assert status == 201
        assert harness.row(body['id']).approval_confirmed_by is None


class TestEdit:
    def test_el7011_cr_an_edit_with_sensitive_tools_needs_the_ack(self, harness):
        schedule_id = harness.seed()
        harness.risk_warnings = [SENSITIVE]
        body, status = harness.call('put', {'name': 'Renamed'}, schedule_id=schedule_id)
        assert (status, body['code']) == (409, 'risk_confirmation_required')
        assert harness.row(schedule_id).name == 'Nightly'
        _, status = harness.call('put', {'name': 'Renamed', 'risk_ack': _ack()}, schedule_id=schedule_id)
        assert status == 200 and harness.row(schedule_id).name == 'Renamed'

    def test_el7011_cr_an_edit_checks_the_new_name_and_the_stored_rule(self, harness):
        harness.run_without_asking_allowed = True
        schedule_id = harness.seed(approval_rule='run_without_asking')
        harness.risk_warnings = [SENSITIVE]
        body, status = harness.call('put', {'name': 'Daily', 'risk_ack': _ack(name='Nightly')}, schedule_id=schedule_id)
        assert (status, body['field']) == (400, 'risk_ack')
        _, status = harness.call('put', {'name': 'Daily', 'risk_ack': _ack(name='Daily')}, schedule_id=schedule_id)
        assert status == 200 and harness.row(schedule_id).approval_confirmed_by == OWNER_ID

    def test_el7011_cr_back_to_pause_and_ask_clears_the_confirmation(self, harness):
        harness.run_without_asking_allowed = True
        harness.risk_warnings = [SENSITIVE]
        created, _ = harness.call('post', _rwa(risk_ack=_ack()))
        _, status = harness.call('put', {'approval_rule': 'pause_and_ask', 'risk_ack': _ack()}, schedule_id=created['id'])
        assert status == 200
        row = harness.row(created['id'])
        assert (row.approval_confirmed_by, row.approval_confirmed_at, row.approval_fingerprint) == (None, None, None)

    @pytest.mark.parametrize('action', ['/pause', '/resume'])
    def test_el7011_cr_pause_and_resume_need_no_ack(self, harness, action):
        schedule_id = harness.seed(enabled=action == '/pause')
        harness.risk_warnings = [SENSITIVE]
        _, status = harness.call('post', schedule_id=schedule_id, suffix=action)
        assert status == 200


def test_el7011_cr_a_save_scans_the_version_once(harness):
    harness.risk_warnings = [NOTICE]
    body, status = harness.call('post', agent_body())
    assert (status, body['warnings']) == (201, [NOTICE])
    assert harness.scans == [(PROJECT_ID, 10, OWNER_ID)]
