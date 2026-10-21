"""EL-7011 US-17 BUG-29: every Schedules refusal carries `code` and `error` (R8.1, R8.2).

One sweep over the refusals of the schedule resource, the run endpoint and the project settings. R2.4,
R3.2 and R6.2 pin the code of their own refusal; the others need only a non-empty `code`. `field`
names the one wrong input (R8.2). Extra keys stay. Uses the US-07 harness (real handlers and SQLite)
and the settings harness (real project handlers, fake store).

Run via:
    python tests/run_tests.py unit/test_7011_refusal_body.py -v
"""
import pathlib
import sys

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).parent))  # the shared harnesses live beside this file
from test_7011_schedule_crud_api import (  # noqa: F401 -- `harness` is a fixture
    ADMIN,
    ADMIN_ID,
    OTHER_ID,
    agent_body,
    assert_refusal,
)
from test_7011_schedule_crud_api import harness as _crud_harness
from test_7011_schedule_settings_api import _call as call_settings
from test_7011_schedule_settings_api import harness as _settings_harness

harness = _crud_harness
settings_harness = _settings_harness
SENSITIVE = {'code': 'sensitive_action', 'message': 'Tool "send_email" needs approval'}
BLOCKING = {'code': 'human_input_step', 'message': 'Node "Review" waits for a person'}


def run(harness, schedule_id):
    return harness.call('post', {'trigger': 'manual'}, schedule_id, suffix='/run')


# --- R8.1, R8.2: refusals of the schedule resource

@pytest.mark.parametrize('verb', ['get', 'put', 'delete'])
def test_7011_r8_1_a_schedule_out_of_scope_is_404_with_code_and_error(harness, verb):
    schedule_id = harness.seed(created_by=OTHER_ID)
    assert_refusal(harness.call(verb, {'name': 'x'}, schedule_id), 404, code='not_found')


def test_7011_r8_1_a_missing_schedule_is_404_with_code_and_error(harness):
    assert_refusal(harness.call('get', schedule_id=9999), 404, code='not_found')


def test_7011_r8_1_a_run_by_a_non_owner_admin_is_403_not_owner(harness):
    schedule_id = harness.seed(created_by=OTHER_ID)
    harness.request_as(ADMIN_ID, ADMIN)
    assert_refusal(run(harness, schedule_id), 403, code='not_owner')


def test_7011_r8_1_an_active_run_is_409_with_code_and_its_id(harness):
    harness.module.start_schedule_run = lambda *args: True
    schedule_id = harness.seed()
    body, _ = run(harness, schedule_id)
    assert_refusal(run(harness, schedule_id), 409, code='run_active', active_run_id=body['run_id'])


def test_7011_r8_1_a_deleted_version_run_is_409_with_code_and_error(harness):
    schedule_id = harness.seed(version_id=None, enabled=False, disabled_reason='version_deleted')
    assert_refusal(run(harness, schedule_id), 409, code='version_deleted')


def test_7011_r8_1_a_run_that_cannot_start_is_500_with_code_and_error(harness):
    harness.module.start_schedule_run = lambda *args: False
    assert_refusal(run(harness, harness.seed()), 500, code='run_start_failed')


def test_7011_r8_1_a_method_the_url_does_not_serve_is_405_with_code_and_error(harness):
    assert_refusal(harness.call('get', None, harness.seed(), suffix='/pause'), 405, code='method_not_allowed')


def test_7011_r8_1_a_folder_denial_is_returned_with_code_and_its_text(harness):
    harness.access_error = ({'ok': False, 'error': 'No access'}, 404)
    assert_refusal(harness.call('post', agent_body()), 404, 'No access')


def test_7011_r8_1_resume_past_the_project_cap_is_409_with_code_and_error(harness):
    harness.config['scheduler'] = {'schedules': {'max_enabled_per_project': 1}}
    harness.seed(name='Running')
    assert_refusal(harness.call('post', None, harness.seed(enabled=False), suffix='/resume'), 409, code='schedule_limit_reached')


@pytest.mark.parametrize('verb, body, field', [
    ('post', agent_body(application_id=99), 'application_id'),
    ('post', agent_body(version_id=11), 'version_id'),
    ('post', agent_body(entity_type='pipeline'), 'entity_type'),
    ('post', agent_body(prompt='Say {% if x %}a{% endif %}'), 'prompt'),
    ('post', agent_body(approval_rule='always'), 'approval_rule'),
    ('put', {'name': 'x', 'enabled': True}, 'enabled'),
])
def test_7011_r8_2_one_wrong_input_is_400_with_code_error_and_field(harness, verb, body, field):
    schedule_id = harness.seed() if verb == 'put' else None
    assert_refusal(harness.call(verb, body, schedule_id), 400, code='invalid_input', field=field)


def test_7011_r8_2_resume_without_a_version_is_400_with_code_and_field(harness):
    schedule_id = harness.seed(version_id=None, enabled=False, disabled_reason='version_deleted')
    assert_refusal(harness.call('post', None, schedule_id, suffix='/resume'), 400, code='invalid_input', field='version_id')


# --- R2.4, R3.2, R6.2: the save refusals pin their code

def test_7011_r2_4_a_blocking_finding_is_409_schedule_blocked(harness):
    harness.risk_warnings = [BLOCKING]
    assert_refusal(harness.call('post', agent_body()), 409, code='schedule_blocked', blocking=[BLOCKING])


def test_7011_r3_2_a_rule_the_project_does_not_allow_is_400_with_code_and_field(harness):
    response = harness.call('post', agent_body(approval_rule='run_without_asking'))
    assert_refusal(response, 400, code='approval_rule_not_allowed', field='approval_rule')


def test_7011_r6_2_a_changed_fingerprint_is_409_risk_changed_with_the_new_one(harness):
    harness.risk_warnings = [SENSITIVE]
    harness.fingerprint = 'fp-2'
    body = agent_body(risk_ack={'fingerprint': 'fp-1', 'confirmed_name': 'Nightly'})
    assert_refusal(harness.call('post', body), 409, code='risk_changed', fingerprint='fp-2')


# --- R4.3: the settings refusals

@pytest.mark.parametrize('body, field', [
    ({'allow_run_without_asking': True, 'enabled': True}, 'enabled'),
    ({'allow_run_without_asking': 'true'}, 'allow_run_without_asking'),
    ({}, 'allow_run_without_asking'),
])
def test_7011_r4_3_a_bad_settings_body_is_400_with_code_error_and_field(settings_harness, body, field):
    assert_refusal(call_settings(settings_harness, 'put', body), 400, code='invalid_input', field=field)


def test_7011_r4_3_a_settings_store_error_is_503_with_code_and_error(settings_harness):
    settings_harness.save_error = RuntimeError('configurations down')
    assert_refusal(call_settings(settings_harness, 'put', {'allow_run_without_asking': True}), 503, code='settings_unavailable')
