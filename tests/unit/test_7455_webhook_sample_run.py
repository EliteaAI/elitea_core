"""#7455: run a webhook row with the owner's sample body, and the row answer's warning data.

Uses the shared schedule resource harness (real handler, claim insert and SQLite tables). The run thread is
replaced, so a test sees exactly what the starter was given: the sample body as bytes and no owner ticket,
because a sample run is admitted like a delivery (webhook governance), not as the owner's own Run now.
"""
import pathlib
import sys

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import types

from test_7011_schedule_crud_api import (  # noqa: F401
    ADMIN, ADMIN_ID, OWNER_ID, PACKAGE, PROJECT_ID, harness as _crud_harness,
)
from test_7011_schedule_run_api import runs


@pytest.fixture
def harness(_crud_harness):
    # The door's admit (where #7400's caps go) as it is: the real claim insert with the run trigger.
    # The door module itself pulls the signing stack, which the door tests cover.
    door = types.ModuleType(f'{PACKAGE}.utils.webhook_door')
    insert_run = sys.modules[f'{PACKAGE}.utils.schedule_claim'].insert_run
    door.admitted = []
    door.admit_webhook_run = lambda session, row_id, trigger='webhook': (
        door.admitted.append(trigger) or insert_run(session, row_id, trigger))
    sys.modules[door.__name__] = door
    _crud_harness.door = door
    return _crud_harness

SENSITIVE = {'code': 'sensitive_action', 'toolkit': 'github', 'tool': 'delete_branch', 'message': 'deletes'}


@pytest.fixture
def started(harness):
    calls = []
    harness.module.start_schedule_run = lambda *args, **kwargs: calls.append((args, kwargs)) or True
    return calls


def _hook(harness, webhook_type='custom', gitlab_auth_method=None, **fields):
    return harness.seed(trigger_kind='webhook', webhook_type=webhook_type, gitlab_auth_method=gitlab_auth_method,
                        cron=None, timezone=None, next_run=None, **fields)


def _run(harness, schedule_id, **body):
    return harness.call('post', {'trigger': 'test', **body}, schedule_id, suffix='/run')


# --- the run route ------------------------------------------------------------------------------------

def test_a_sample_run_inserts_a_test_run_and_passes_the_body_in_memory(harness, started):
    hook = _hook(harness)
    body, status = _run(harness, hook, payload='{"action": "opened"}')
    assert status == 202
    assert runs(harness) == [(body['run_id'], hook, 'test', 'running', None)]
    assert harness.door.admitted == ['test']
    args, kwargs = started[0]
    assert args == ('agent', PROJECT_ID, hook, body['run_id'])   # no owner ticket: governed as a delivery
    assert kwargs == {'payload': b'{"action": "opened"}'}


def test_one_sample_run_at_a_time_but_beside_an_active_delivery(harness, started):
    hook = _hook(harness)
    run_model = sys.modules[f'{PACKAGE}.models.entity_schedule'].EntityScheduleRun
    with harness.session_factory() as session:
        session.add(run_model(schedule_id=hook, trigger='webhook', status='running'))
        session.commit()
    first, _ = _run(harness, hook, payload='{}')
    answer, status = _run(harness, hook, payload='{}')
    assert (status, answer['active_run_id']) == (409, first['run_id'])


@pytest.mark.parametrize('body, field', [
    ({'trigger': 'test'}, 'payload'),                       # a webhook run needs a body
    ({'trigger': 'manual', 'payload': '{}'}, 'trigger'),    # a webhook is only tried as a test
])
def test_a_webhook_run_without_a_sample_or_as_manual_is_400(harness, started, body, field):
    hook = _hook(harness)
    answer, status = harness.call('post', body, hook, suffix='/run')
    assert (status, answer.get('field')) == (400, field)
    assert (runs(harness), started) == ([], [])


@pytest.mark.parametrize('trigger', ['manual', 'test'])
def test_a_payload_on_a_cron_row_is_400(harness, started, trigger):
    schedule_id = harness.seed()
    answer, status = harness.call('post', {'trigger': trigger, 'payload': '{}'}, schedule_id, suffix='/run')
    assert (status, answer.get('field')) == (400, 'payload')
    assert runs(harness) == []


def test_a_payload_over_512_kb_is_400(harness, started):
    hook = _hook(harness)
    assert _run(harness, hook, payload='x' * (512 * 1024 + 1))[1] == 400
    assert runs(harness) == []


def test_a_sample_run_that_cannot_start_is_failed_and_500(harness):
    harness.module.start_schedule_run = lambda *args, **kwargs: False
    hook = _hook(harness)
    assert _run(harness, hook, payload='{}')[1] == 500
    assert [row[3] for row in runs(harness)] == ['failed']


def test_only_the_owner_runs_a_sample(harness, started):
    hook = _hook(harness)
    harness.request_as(ADMIN_ID, ADMIN)   # an admin sees the row but never runs it as the owner
    assert _run(harness, hook, payload='{}')[1] == 403
    assert runs(harness) == []


# --- the row answer -----------------------------------------------------------------------------------

@pytest.mark.parametrize('webhook_type, method, signed', [
    ('github', None, True),
    ('gitlab', 'signing_token', True),
    ('gitlab', 'secret_token', False),
    ('custom', None, False),
])
def test_signed_and_recommended_per_webhook_type(harness, webhook_type, method, signed):
    answer, _ = harness.call('get', None, _hook(harness, webhook_type, method))
    assert (answer['signed'], answer['recommended']) == (signed, signed)


def test_a_cron_row_has_no_signing_data(harness):
    answer, _ = harness.call('get', None, harness.seed())
    assert (answer['signed'], answer['recommended']) == (None, None)


def _confirmed_hook(harness, fingerprint='fp-1', confirmed_by=OWNER_ID):
    return _hook(harness, approval_rule='run_without_asking', approval_fingerprint=fingerprint,
                 approval_confirmed_by=confirmed_by)


def test_needs_reconfirm_after_the_entity_changed_and_not_after_a_new_confirmation(harness):
    harness.risk_warnings, harness.fingerprint = [SENSITIVE], 'fp-2'   # a tool was added since 'fp-1'
    assert harness.call('get', None, _confirmed_hook(harness))[0]['needs_reconfirm'] is True
    assert harness.call('get', None, _confirmed_hook(harness, 'fp-2'))[0]['needs_reconfirm'] is False


def test_needs_reconfirm_when_someone_else_confirmed(harness):
    # After an owner change the old owner's consent does not hold for the new owner (_may_auto_approve)
    harness.risk_warnings, harness.fingerprint = [SENSITIVE], 'fp-1'
    hook = _confirmed_hook(harness, confirmed_by=OWNER_ID + 1)
    assert harness.call('get', None, hook)[0]['needs_reconfirm'] is True


def test_pause_and_ask_never_needs_reconfirm_and_is_not_scanned_for_it(harness):
    hook = _hook(harness)
    harness.scans.clear()
    assert harness.call('get', None, hook)[0]['needs_reconfirm'] is False
    assert harness.scans == []


def test_a_failed_scan_reads_as_unknown_not_an_error(harness):
    hook = _confirmed_hook(harness)
    sys.modules[f'{PACKAGE}.utils.schedule_risk'].scan_schedule = lambda *a: (_ for _ in ()).throw(RuntimeError())
    answer, status = harness.call('get', None, hook)
    assert (status, answer['needs_reconfirm']) == (200, None)


def test_run_without_asking_on_a_webhook_is_refused_when_the_project_setting_is_off(harness):
    harness.run_without_asking_allowed = False
    hook = _hook(harness)
    answer, status = harness.call('put', {'approval_rule': 'run_without_asking'}, hook)
    assert (status, answer.get('code')) == (400, 'approval_rule_not_allowed')
