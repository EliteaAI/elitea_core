"""#7404 (Core B of #7398): webhook rows through the real Schedules router, handlers, service and SQLite table.

Reuses the CRUD harness of test_7011_schedule_crud_api.py; the hidden vault store is a fake shared dict.
Covers #7398 AC 1 (create + secret shown once), 2 (owner rules), 4 (rotate / regenerate), 5 (change owner),
6 (delete removes keys) and 7 (one audit event per action).
"""
import sys

import pytest
from fixtures.helpers import load_module_with_stubs
from unit.test_7011_schedule_crud_api import (  # noqa: F401 -- `harness` is a fixture
    ADMIN, ADMIN_ID, EDITOR, OTHER_ID, OWNER_ID, PACKAGE, PLUGIN_ROOT, fake_connections, harness,
)

SIGNING_TOKEN = 'whsec_' + 'QUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVo='  # base64 of 26 letters


class FakeVault:
    store: dict = {}
    fail = False

    def __init__(self, project_id):
        pass

    def get_hidden_secrets(self):
        if FakeVault.fail:
            raise RuntimeError('vault down: token=hvs.SECRET')  # must never reach the answer
        return dict(FakeVault.store)

    def set_hidden_secrets(self, secrets):
        FakeVault.store = dict(secrets)


@pytest.fixture
def api(harness):
    FakeVault.store, FakeVault.fail = {}, False
    sys.modules['tools'].VaultClient = FakeVault
    trigger = sys.modules.setdefault(f'{PACKAGE}.utils.pipeline_trigger', type(sys)('pipeline_trigger'))
    trigger.validate_gitlab_signing_token_format = (
        lambda value: None if value.startswith('whsec_') else "GitLab signing token must start with 'whsec_'")
    load_module_with_stubs(PLUGIN_ROOT / 'utils/webhook_secrets.py', f'{PACKAGE}.utils.webhook_secrets')
    return harness


def webhook_body(**changes):
    body = {'name': 'On push', 'entity_type': 'agent', 'application_id': 1, 'version_id': 10,
            'prompt': 'Handle it', 'trigger_kind': 'webhook', 'webhook_type': 'github'}
    return {**body, **changes}


def create(api, **changes):
    return api.call('_create', webhook_body(**changes))


def audit_names(api):
    return [name for name, _attrs in api.audit]


def seed_hook(api, created_by=OWNER_ID, **fields):
    """Create through the API as `created_by`, so the row has a real secret; then patch fields directly."""
    api.request_as(created_by, EDITOR)
    body, status = create(api)
    assert status == 201, body
    if fields:
        with api.session_factory() as session:
            row = session.get(api.EntitySchedule, body['id'])
            for key, value in fields.items():
                setattr(row, key, value)
            session.commit()
    api.audit.clear()
    return body['id'], body['secret']


def secret_key(api, schedule_id, suffix=''):
    return f'webhook_secret_{api.row(schedule_id).uuid}{suffix}'


# --- AC 1: create, the secret is answered once


def test_7404_create_stores_a_hidden_secret_and_answers_it_once(api):
    body, status = create(api)
    assert status == 201
    key = secret_key(api, body['id'])
    assert FakeVault.store == {key: body['secret']}
    assert api.row(body['id']).secret_ref == '{{secret.%s}}' % key
    assert body['webhook_url'].endswith(str(api.row(body['id']).uuid))


def test_7404_reads_never_answer_the_secret(api):
    schedule_id, secret = seed_hook(api)
    detail, status = api.call('_read', schedule_id=schedule_id)
    assert status == 200 and 'secret' not in detail and secret not in str(detail)


def test_7404_two_webhooks_on_the_same_version_get_their_own_secrets(api):
    first, _ = seed_hook(api, OWNER_ID)
    second, _ = seed_hook(api, OTHER_ID)
    assert len(FakeVault.store) == 2
    assert api.row(first).secret_ref != api.row(second).secret_ref


def test_7404_gitlab_signing_row_stores_the_pasted_token_and_answers_no_secret(api):
    body, status = create(api, webhook_type='gitlab', gitlab_auth_method='signing_token',
                          signing_secret_value=SIGNING_TOKEN)
    assert status == 201 and 'secret' not in body
    row = api.row(body['id'])
    assert FakeVault.store == {f'webhook_signing_secret_{row.uuid}': SIGNING_TOKEN} and row.secret_ref is None


@pytest.mark.parametrize('changes, field', [
    ({'webhook_type': 'gitlab', 'gitlab_auth_method': 'signing_token'}, 'body'),
    ({'webhook_type': 'gitlab', 'gitlab_auth_method': 'signing_token', 'signing_secret_value': 'nope'},
     'signing_secret_value'),
    ({'signing_secret_value': SIGNING_TOKEN}, 'body'),
])
def test_7404_signing_token_value_is_required_valid_and_only_for_signing_rows(api, changes, field):
    body, status = create(api, **changes)
    assert status == 400 and body['field'] == field
    assert FakeVault.store == {}


def test_7404_vault_failure_on_create_leaves_no_row_and_no_detail(api):
    FakeVault.fail = True
    body, status = create(api)
    assert status == 503 and body['code'] == 'secret_store_unavailable'
    assert 'hvs' not in str(body)
    with api.session_factory() as session:
        assert session.query(api.EntitySchedule).count() == 0


# --- AC 2: owner rules


@pytest.mark.parametrize('change', [{'name': 'Renamed'}, {'approval_rule': 'pause_and_ask'}, {'prompt': 'x'}])
def test_7404_admin_cannot_edit_another_owners_webhook(api, change):
    schedule_id, _ = seed_hook(api)
    api.request_as(ADMIN_ID, ADMIN)
    body, status = api.call('_edit', change, schedule_id)
    assert (status, body['code']) == (403, 'not_owner')
    assert api.scans == [] or all(scan[0] != schedule_id for scan in api.scans)  # refused before the scan
    assert api.row(schedule_id).name == 'On push'


def test_7404_admin_edit_of_a_cron_schedule_still_works(api):
    schedule_id = api.seed(created_by=OWNER_ID)
    api.request_as(ADMIN_ID, ADMIN)
    _body, status = api.call('_edit', {'name': 'Renamed'}, schedule_id)
    assert status == 200 and api.row(schedule_id).name == 'Renamed'


def test_7404_admin_may_pause_resume_and_delete_a_webhook_and_the_owner_is_told(api):
    schedule_id, _ = seed_hook(api)
    api.request_as(ADMIN_ID, ADMIN)
    assert api.call('_pause', schedule_id=schedule_id, suffix='/pause')[1] == 200
    assert api.call('_resume', schedule_id=schedule_id, suffix='/resume')[1] == 200
    assert api.call('_delete', schedule_id=schedule_id)[1] == 204
    assert [notice[0] for notice in api.notices if notice[2] == schedule_id] == [OWNER_ID] * 3


def test_7404_another_member_cannot_see_or_rotate_a_webhook(api):
    schedule_id, _ = seed_hook(api)
    api.request_as(OTHER_ID, EDITOR)
    assert api.call('_read', schedule_id=schedule_id)[1] == 404
    assert api.call('_secret', {'mode': 'rotate'}, schedule_id, suffix='/secret')[1] == 404


def test_7404_details_answer_the_webhook_actions(api):
    schedule_id, _ = seed_hook(api)
    api.request_as(ADMIN_ID, ADMIN)
    detail, _ = api.call('_read', schedule_id=schedule_id)
    assert detail['allowed_actions'] == ['run', 'pause', 'rotate_secret', 'delete', 'change_owner']


def test_details_answer_the_webhook_actions_of_the_owner(api):
    schedule_id, _ = seed_hook(api)
    detail, _ = api.call('_read', schedule_id=schedule_id)
    assert detail['allowed_actions'][:2] == ['run', 'edit']


# --- AC 4: rotate and regenerate


def test_7404_rotate_answers_a_new_secret_and_keeps_the_old_one_24h(api):
    schedule_id, old = seed_hook(api)
    body, status = api.call('_secret', {'mode': 'rotate'}, schedule_id, suffix='/secret')
    assert status == 200 and body['secret'] != old
    assert FakeVault.store[secret_key(api, schedule_id)] == body['secret']
    assert FakeVault.store[secret_key(api, schedule_id, '_prev')] == old
    assert body['previous_secret_until'] is not None
    assert audit_names(api) == ['webhook_secret_rotated']


def test_7404_second_rotate_answers_409_with_the_window(api):
    schedule_id, old = seed_hook(api)
    api.call('_secret', {'mode': 'rotate'}, schedule_id, suffix='/secret')
    body, status = api.call('_secret', {'mode': 'rotate'}, schedule_id, suffix='/secret')
    assert (status, body['code']) == (409, 'rotation_in_progress') and body['previous_secret_until']
    assert FakeVault.store[secret_key(api, schedule_id, '_prev')] == old
    assert audit_names(api) == ['webhook_secret_rotated']


# Review fix: the secret is written before the commit; a failed commit must not leave it behind
def test_7404_failed_commit_on_create_drops_the_stored_secret(api, monkeypatch):
    session_class = type(api.session_factory())
    real_commit = session_class.commit
    calls = []

    def commit_once_failing(self):
        calls.append(1)
        if len(calls) == 1:
            raise sys.modules['sqlalchemy.exc'].OperationalError('INSERT', {}, Exception('db down'))
        return real_commit(self)

    monkeypatch.setattr(session_class, 'commit', commit_once_failing)
    api.request_as(OWNER_ID, EDITOR)
    with pytest.raises(sys.modules['sqlalchemy.exc'].OperationalError):
        create(api)
    assert FakeVault.store == {}


# Review fix: the handler and the service judge the same change set; a null is "not sent"
def test_7404_admin_pause_with_a_null_field_is_not_an_edit(api):
    schedule_id, _ = seed_hook(api)
    api.request_as(ADMIN_ID, ADMIN)
    _body, status = api.call('_edit', {'version_id': None, 'name': None}, schedule_id)
    assert status == 200


# Review fix: a 503 must mean "nothing changed": the working secret still works and no new one is lost
def test_7404_failed_regenerate_commit_keeps_the_working_secret(api, monkeypatch):
    schedule_id, old = seed_hook(api)
    session_class = type(api.session_factory())
    real_commit, calls = session_class.commit, []

    def fail_first(self):
        calls.append(1)
        if len(calls) == 1:
            raise RuntimeError('db down')
        return real_commit(self)

    monkeypatch.setattr(session_class, 'commit', fail_first)
    body, status = api.call('_secret', {'mode': 'regenerate'}, schedule_id, suffix='/secret')
    assert (status, body['code']) == (503, 'secret_store_unavailable')
    assert FakeVault.store[secret_key(api, schedule_id)] == old
    assert audit_names(api) == []


def test_7404_regenerate_replaces_at_once(api):
    schedule_id, _ = seed_hook(api)
    api.call('_secret', {'mode': 'rotate'}, schedule_id, suffix='/secret')
    body, status = api.call('_secret', {'mode': 'regenerate'}, schedule_id, suffix='/secret')
    assert status == 200
    assert FakeVault.store == {secret_key(api, schedule_id): body['secret']}
    row = api.row(schedule_id)
    assert (row.previous_secret_ref, row.previous_secret_until) == (None, None)
    assert audit_names(api) == ['webhook_secret_rotated', 'webhook_secret_regenerated']


def test_7404_admin_rotation_is_audited_and_the_owner_told(api):
    schedule_id, _ = seed_hook(api)
    api.request_as(ADMIN_ID, ADMIN)
    _body, status = api.call('_secret', {'mode': 'regenerate'}, schedule_id, suffix='/secret')
    assert status == 200
    assert (OWNER_ID, 'schedule_edited_by_admin', schedule_id) in api.notices
    assert api.audit[-1][1]['user.id'] == ADMIN_ID


def test_7404_secret_route_refuses_cron_rows_and_bad_modes(api):
    cron_id = api.seed(created_by=OWNER_ID)
    assert api.call('_secret', {'mode': 'rotate'}, cron_id, suffix='/secret')[0]['code'] == 'not_a_webhook'
    schedule_id, _ = seed_hook(api)
    assert api.call('_secret', {'mode': 'edit'}, schedule_id, suffix='/secret')[1] == 400


def test_7404_rotation_of_a_signing_row_takes_the_new_pasted_token(api):
    api.request_as(OWNER_ID, EDITOR)
    created, _ = create(api, webhook_type='gitlab', gitlab_auth_method='signing_token',
                        signing_secret_value=SIGNING_TOKEN)
    assert api.call('_secret', {'mode': 'rotate'}, created['id'], suffix='/secret')[1] == 400
    new_token = SIGNING_TOKEN.replace('QUJD', 'WFla')
    body, status = api.call('_secret', {'mode': 'rotate', 'signing_secret_value': new_token}, created['id'],
                            suffix='/secret')
    uuid = api.row(created['id']).uuid
    assert status == 200 and 'secret' not in body
    assert FakeVault.store[f'webhook_signing_secret_{uuid}'] == new_token
    assert FakeVault.store[f'webhook_secret_{uuid}_prev'] == SIGNING_TOKEN


def test_7404_vault_failure_on_rotate_keeps_the_current_secret(api):
    schedule_id, old = seed_hook(api)
    FakeVault.fail = True
    body, status = api.call('_secret', {'mode': 'rotate'}, schedule_id, suffix='/secret')
    assert status == 503 and 'hvs' not in str(body)
    FakeVault.fail = False
    assert FakeVault.store[secret_key(api, schedule_id)] == old
    assert api.row(schedule_id).previous_secret_until is None


# --- AC 5: change owner


@pytest.fixture
def owner_change(api):
    fake_connections(api, load_module_with_stubs(PLUGIN_ROOT / 'utils/schedule_prerun.py',
                                                  f'{PACKAGE}.utils.schedule_prerun'))
    module = load_module_with_stubs(PLUGIN_ROOT / 'utils/schedule_owner.py', f'{PACKAGE}.utils.schedule_owner')
    module.validate_new_owner = lambda *args, **kwargs: None
    return api


def test_7404_change_owner_pauses_and_resets_the_webhook(owner_change):
    api = owner_change
    schedule_id, _ = seed_hook(api, approval_rule='run_without_asking', approval_fingerprint='fp',
                               credential_refs=['cfg-1'])
    api.request_as(ADMIN_ID, ADMIN)
    body, status = api.call('_change_owner', {'owner_id': OTHER_ID}, schedule_id, suffix='/owner')
    assert status == 200
    row = api.row(schedule_id)
    assert (row.created_by, row.enabled, row.disabled_reason, row.approval_rule, row.approval_fingerprint,
            row.credential_refs) == (OTHER_ID, False, 'owner_changed', 'pause_and_ask', None, None)
    assert 'resume' not in body['allowed_actions']
    assert [extra.get('reason') is not None for extra in api.notice_extras[-2:]] == [True, True]
    assert audit_names(api) == [f'webhook_owner_changed: {OWNER_ID} -> {OTHER_ID}']


def test_7404_only_the_new_owner_switches_it_on_again(owner_change):
    api = owner_change
    schedule_id, _ = seed_hook(api)
    api.request_as(ADMIN_ID, ADMIN)
    api.call('_change_owner', {'owner_id': OTHER_ID}, schedule_id, suffix='/owner')
    body, status = api.call('_resume', schedule_id=schedule_id, suffix='/resume')
    assert (status, body['code']) == (403, 'not_owner')
    api.request_as(OTHER_ID, EDITOR)
    assert api.call('_resume', schedule_id=schedule_id, suffix='/resume')[1] == 200
    assert api.row(schedule_id).disabled_reason is None


def test_7404_change_owner_of_a_cron_schedule_keeps_it_enabled(owner_change):
    api = owner_change
    schedule_id = api.seed(created_by=OWNER_ID)
    api.request_as(ADMIN_ID, ADMIN)
    api.call('_change_owner', {'owner_id': OTHER_ID}, schedule_id, suffix='/owner')
    assert (api.row(schedule_id).enabled, api.row(schedule_id).disabled_reason) == (True, None)


# --- AC 6, 7: delete and audit


def test_7404_delete_removes_the_keys_and_is_audited(api):
    schedule_id, _ = seed_hook(api)
    api.call('_secret', {'mode': 'rotate'}, schedule_id, suffix='/secret')
    api.audit.clear()
    assert api.call('_delete', schedule_id=schedule_id)[1] == 204
    assert FakeVault.store == {}
    assert audit_names(api) == ['webhook_deleted']


def test_7404_delete_goes_on_when_the_vault_is_down(api):
    schedule_id, _ = seed_hook(api)
    FakeVault.fail = True
    assert api.call('_delete', schedule_id=schedule_id)[1] == 204
    assert api.row(schedule_id) is None


def test_7404_create_and_approval_rule_change_are_audited(api):
    api.request_as(OWNER_ID, EDITOR)
    body, _ = create(api)
    assert audit_names(api) == ['webhook_created: github']
    api.audit.clear()
    api.call('_edit', {'name': 'Renamed'}, body['id'])
    assert audit_names(api) == []  # a rename is no audited webhook action
    api.call('_edit', {'approval_rule': 'pause_and_ask'}, body['id'])
    assert audit_names(api) == ['webhook_approval_rule_changed: pause_and_ask']


def test_7404_audit_spans_carry_no_secret(api):
    schedule_id, secret = seed_hook(api)
    body, _ = api.call('_secret', {'mode': 'rotate'}, schedule_id, suffix='/secret')
    for name, attributes in api.audit:
        assert secret not in name + str(attributes) and body['secret'] not in name + str(attributes)
