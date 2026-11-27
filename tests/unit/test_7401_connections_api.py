"""#7401: the Connections check over the Schedules API: the webhook save gate, the route and Re-enable.

The check itself has its own tests (unit/utils/test_7401_connections.py); here it is the CRUD harness
stand-in, which answers `harness.connections[version_id]` as the rows of that version.
"""
import pytest
from unit.test_7011_schedule_crud_api import ADMIN, ADMIN_ID, EDITOR, OWNER_ID, agent_body  # noqa: F401
from unit.test_7404_webhook_api import api, create, seed_hook, webhook_body  # noqa: F401
from unit.test_7011_schedule_crud_api import harness  # noqa: F401

OK = {'toolkit': 'Mail', 'status': 'ok', 'credential_uuid': 'uuid-o', 'login': 'ok'}
REMOTE_MCP = {'toolkit': 'Remote', 'status': 'unsupported', 'reason': 'Webhooks cannot wait for a sign-in'}
EXPIRED = {'toolkit': 'Mail', 'status': 'sign_in_required', 'credential_uuid': 'uuid-o', 'login': 'expired',
           'credential_title': 'team_outlook'}
CHECK = {'entity_type': 'agent', 'application_id': 1, 'version_id': 10}


def row(api, schedule_id):
    with api.session_factory() as session:
        return session.get(api.EntitySchedule, schedule_id)


@pytest.mark.parametrize('rule', ['pause_and_ask', 'run_without_asking'])
def test_7401_a_webhook_with_a_toolkit_that_cannot_run_unattended_is_refused(api, rule):
    api.request_as(OWNER_ID, EDITOR)
    api.run_without_asking_allowed = True
    api.connections[10] = [OK, REMOTE_MCP]

    body, status = create(api, approval_rule=rule)

    assert status == 409 and body['code'] == 'connections_not_ready' and body['rows'] == [OK, REMOTE_MCP]
    assert api.connection_calls == [(10, OWNER_ID, True)]  # a real refresh, as the owner


def test_7401_a_webhook_whose_rows_are_ok_saves_its_credential_refs(api):
    api.request_as(OWNER_ID, EDITOR)
    api.connections[10] = [OK, {'toolkit': 'GH', 'status': 'ok'}]

    body, status = create(api)

    assert status == 201, body
    assert row(api, body['id']).credential_refs == ['uuid-o']


def test_7401_a_cron_schedule_saves_its_refs_without_a_block_or_a_refresh(harness):
    harness.request_as(OWNER_ID, EDITOR)
    harness.connections[10] = [EXPIRED]

    body, status = harness.call('_create', agent_body())

    assert status == 201, body
    assert row(harness, body['id']).credential_refs == ['uuid-o']
    assert harness.connection_calls == [(10, OWNER_ID, False)]


def test_7401_an_edit_of_a_webhook_without_refs_rechecks_its_connections(api):
    schedule_id, _ = seed_hook(api, credential_refs=None)  # saved before #7401
    api.connections[10] = [REMOTE_MCP]
    api.request_as(OWNER_ID, EDITOR)

    body, status = api.call('_edit', {'name': 'Renamed'}, schedule_id)

    assert status == 409 and body['code'] == 'connections_not_ready'
    assert row(api, schedule_id).name == 'On push'


def test_7401_a_rename_of_a_checked_webhook_makes_no_token_call(api):
    # Its toolkits did not change; a transient token error must not refuse a rename
    api.connections[10] = [OK]
    schedule_id, _ = seed_hook(api)
    api.connections[10] = [{**EXPIRED, 'login': 'transient'}]
    api.connection_calls.clear()
    api.request_as(OWNER_ID, EDITOR)

    body, status = api.call('_edit', {'name': 'Renamed'}, schedule_id)

    assert status == 200, body
    assert api.connection_calls == []


# --- the route ------------------------------------------------------------------------------------------------------

def test_7401_the_route_answers_the_rows_and_blocks_a_webhook_only(harness):
    harness.request_as(OWNER_ID, EDITOR)
    harness.connections[10] = [OK, REMOTE_MCP]

    webhook, status = harness.call('_check_connections', {**CHECK, 'trigger_kind': 'webhook'}, suffix='/connections')
    cron, _ = harness.call('_check_connections', CHECK, suffix='/connections')

    assert status == 200 and webhook == {'rows': [OK, REMOTE_MCP], 'all_ok': False, 'blocking': True,
                                         'lost_login': False}
    assert cron['blocking'] is False and cron['all_ok'] is False
    assert harness.switched_off == []  # no schedule_id: nothing is switched off or written


def test_7401_the_owners_check_of_a_trigger_saves_refs_and_switches_off_on_a_lost_login(api):
    schedule_id, _ = seed_hook(api)
    api.request_as(OWNER_ID, EDITOR)
    api.connections[10] = [EXPIRED]

    body, status = api.call('_check_connections', {**CHECK, 'schedule_id': schedule_id}, suffix='/connections')

    assert status == 200 and body['blocking'] is True and body['lost_login'] is True
    assert api.switched_off == [(OWNER_ID, 'uuid-o')]
    assert row(api, schedule_id).credential_refs == ['uuid-o']


# --- Re-enable ------------------------------------------------------------------------------------------------------

def _switched_off(api, reason='credential_expired'):
    return seed_hook(api, enabled=False, disabled_reason=reason)[0]


def test_7401_re_enable_is_refused_while_the_login_still_fails(api):
    schedule_id = _switched_off(api)
    api.connections[10] = [EXPIRED]
    api.request_as(OWNER_ID, EDITOR)

    body, status = api.call('_resume', schedule_id=schedule_id, suffix='/resume')

    assert status == 409 and body['code'] == 'connections_not_ready' and body['rows'] == [EXPIRED]
    assert row(api, schedule_id).enabled is False and row(api, schedule_id).disabled_reason == 'credential_expired'


def test_7401_re_enable_succeeds_after_a_new_sign_in(api):
    schedule_id = _switched_off(api)
    api.connections[10] = [OK]
    api.request_as(OWNER_ID, EDITOR)

    body, status = api.call('_resume', schedule_id=schedule_id, suffix='/resume')

    assert status == 200, body
    stored = row(api, schedule_id)
    assert stored.enabled is True and stored.disabled_reason is None and stored.credential_refs == ['uuid-o']


@pytest.mark.parametrize('reason', ['prerun_refused', None])
def test_7401_other_switch_offs_resume_without_a_check(api, reason):
    schedule_id = _switched_off(api, reason)
    api.connections[10] = [EXPIRED]
    api.request_as(OWNER_ID, EDITOR)
    api.connection_calls.clear()

    assert api.call('_resume', schedule_id=schedule_id, suffix='/resume')[1] == 200
    assert api.connection_calls == []


def test_7401_a_schedule_resumes_with_a_browser_only_toolkit_once_its_login_is_back(harness):
    # a schedule under "Pause and ask me" may wait for a sign-in (#7304): only its saved logins must work again
    harness.request_as(OWNER_ID, EDITOR)
    body, _ = harness.call('_create', agent_body())
    with harness.session_factory() as session:
        stored = session.get(harness.EntitySchedule, body['id'])
        stored.enabled, stored.disabled_reason = False, 'credential_expired'
        session.commit()
    harness.connections[10] = [OK, REMOTE_MCP]

    assert harness.call('_resume', schedule_id=body['id'], suffix='/resume')[1] == 200


def test_7401_an_admins_check_of_a_members_trigger_writes_and_switches_nothing(api):
    schedule_id, _ = seed_hook(api)
    api.connections[10] = [EXPIRED]
    api.request_as(ADMIN_ID, ADMIN)

    body, status = api.call('_check_connections', {**CHECK, 'schedule_id': schedule_id}, suffix='/connections')

    assert status == 200 and body['blocking'] is True and body['lost_login'] is False
    assert api.switched_off == []
