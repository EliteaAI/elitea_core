"""The risk check of a Toolkit Index: the credential of its toolkit, no tools, no fingerprint.

The handler, the service and the SQLite tables are real. The read of the index store, the settings expansion and
the scan helpers are stand-ins with the parameters of the real functions.
"""
import json
import pathlib
import sys
import types

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).parent))  # the shared harness lives beside this file
from test_7011_schedule_crud_api import (  # noqa: E402,F401 -- `harness` is a fixture
    ADMIN,
    OTHER_ID,
    OWNER_ID,
    PACKAGE,
    PROJECT_ID,
    agent_body,
    assert_refusal,
)
from test_7011_schedule_crud_api import harness as _crud_harness  # noqa: E402

harness = _crud_harness  # the CRUD harness fixture, shared

TOOLKIT_ID = 3
INDEX = 'docs'
SAVED = {'chunk_size': 500}


class ConfigurationExpandError(Exception):
    def __init__(self, errors):
        super().__init__('expand failed')
        self.errors = errors


def credential_error(error_type='credential_not_found'):
    return ConfigurationExpandError([{'msg': json.dumps({'error_type': error_type})}])


class Env:
    """What the check did and what the stand-ins answer."""

    def __init__(self, harness):
        self.harness = harness
        self.expand_error = None
        self.expanded = []
        self.private = False
        self.configuration = SAVED
        self.governance = []
        self.schedule_id = None

    def check(self, toolkit_id=TOOLKIT_ID, index_name=INDEX, schedule_id=None):
        body = {'entity_type': 'toolkit_index', 'toolkit_id': toolkit_id, 'index_name': index_name,
                'schedule_id': schedule_id}
        body = {key: value for key, value in body.items() if value is not None}
        return self.harness.call('post', body, suffix='/check')

    def seed_index_schedule(self, created_by=OWNER_ID, index_name=INDEX):
        self.schedule_id = self.harness.seed(
            created_by, entity_type='toolkit_index', application_id=None, version_id=None, prompt=None,
            variables=[], toolkit_id=TOOLKIT_ID, index_name=index_name)
        return self.schedule_id


@pytest.fixture
def env(harness, monkeypatch):
    """An artifact toolkit with a saved index, folder rights granted and the stand-ins installed."""
    state = Env(harness)
    toolkit_model = sys.modules[f'{PACKAGE}.models.all'].EliteATool
    with harness.session_factory() as session:
        session.add(toolkit_model(id=TOOLKIT_ID, name='Docs toolkit', type='artifact',
                                  settings={'pgvector_configuration': {'elitea_title': 'store'}}))
        session.commit()
    sys.modules[f'{PACKAGE}.utils.folder_access'].TOOLKIT_ENTITY_TYPES = ('artifact',)
    service = sys.modules[f'{PACKAGE}.utils.schedule_service']
    monkeypatch.setattr(service, 'read_index_configuration',
                        lambda project_id, toolkit, index_name: state.configuration)

    application_tools = types.ModuleType(f'{PACKAGE}.utils.application_tools')
    application_tools.ConfigurationExpandError = ConfigurationExpandError

    def expand_toolkit_settings(type_, settings, project_id, user_id):
        state.expanded.append((type_, project_id, user_id))
        if state.expand_error:
            raise state.expand_error
        return settings

    application_tools.expand_toolkit_settings = expand_toolkit_settings
    monkeypatch.setitem(sys.modules, application_tools.__name__, application_tools)

    # The scan helpers keep the parameters of the real ones in utils/schedule_risk
    risk = sys.modules[f'{PACKAGE}.utils.schedule_risk']
    risk.CREDENTIAL_ERRORS = {'private_credential_not_found', 'credential_not_found', 'credential_type_mismatch'}
    risk.INCOMPLETE_CODE = 'scan_incomplete'

    def _warning(code, message, toolkit=None, toolkit_type=None, tool=None):
        return {'code': code, 'message': message, 'toolkit': toolkit, 'toolkit_type': toolkit_type, 'tool': tool}

    def _error_types(errors):
        return {json.loads(error['msg']).get('error_type') for error in errors}

    def _uses_private_credential(tool):
        return state.private

    def _personal_credential(tool, name):
        return _warning('personal_credential', f'Toolkit "{name}" runs with the owner\'s personal credential',
                        toolkit=name, toolkit_type=tool.type, tool=None)

    risk._warning, risk._error_types = _warning, _error_types
    risk._uses_private_credential, risk._personal_credential = _uses_private_credential, _personal_credential

    def schedule_governance_notice(module, session, project_id, application_id, version_id, owner_id, saver_id,
                                   entity_type=None):
        state.governance.append(entity_type)
        return None

    monkeypatch.setattr(harness.module, 'schedule_governance_notice', schedule_governance_notice)
    return state


def test_a_toolkit_with_a_usable_credential_has_no_findings(harness, env):
    body, status = env.check()
    assert status == 200
    assert body == {'blocking': [], 'sensitive': [], 'notices': [], 'fingerprint': None,
                    'run_without_asking_allowed': False}
    assert harness.scans == []  # an index has no version to scan
    assert env.expanded == [('artifact', PROJECT_ID, OWNER_ID)]


@pytest.mark.parametrize('error_type', ['credential_not_found', 'private_credential_not_found',
                                        'credential_type_mismatch'])
def test_a_credential_the_owner_cannot_use_blocks_the_save(harness, env, error_type):
    env.expand_error = credential_error(error_type)
    body, status = env.check()
    assert status == 200
    assert [item['code'] for item in body['blocking']] == ['credential_missing']
    assert body['blocking'][0]['toolkit'] == 'Docs toolkit'
    assert body['sensitive'] == [] and body['notices'] == [] and body['fingerprint'] is None


def test_a_personal_credential_is_a_notice_and_needs_no_confirmation(harness, env):
    env.private = True
    body, status = env.check()
    assert status == 200
    assert [item['code'] for item in body['notices']] == ['personal_credential']
    assert body['blocking'] == [] and body['sensitive'] == []


def test_a_failed_expansion_is_an_incomplete_notice_not_a_500(harness, env):
    env.expand_error = RuntimeError('vault down')
    body, status = env.check()
    assert status == 200
    assert [item['code'] for item in body['notices']] == ['scan_incomplete']
    assert body['blocking'] == []


def test_an_expansion_error_that_is_not_a_credential_is_an_incomplete_notice(harness, env):
    env.expand_error = credential_error('schema_error')
    body, status = env.check()
    assert [item['code'] for item in body['notices']] == ['scan_incomplete']
    assert body['blocking'] == []


def test_an_index_that_is_not_in_the_toolkit_is_400_on_index_name(harness, env):
    env.configuration = None
    body, status = env.check()
    assert status == 400
    assert body['field'] == 'index_name'
    assert env.expanded == []


def test_a_toolkit_outside_the_project_is_400_on_toolkit_id(harness, env):
    body, status = env.check(toolkit_id=99)
    assert status == 400
    assert body['field'] == 'toolkit_id'


def test_a_folder_the_caller_cannot_write_is_refused_before_the_index_is_read(harness, env):
    harness.access_error = ({'ok': False, 'error': 'No access'}, 404)
    assert_refusal(env.check(), 404, 'No access')
    assert env.expanded == []


@pytest.mark.parametrize('missing', ['toolkit_id', 'index_name'])
def test_a_check_without_the_toolkit_or_the_index_is_400(harness, env, missing):
    body = {'entity_type': 'toolkit_index', 'toolkit_id': TOOLKIT_ID, 'index_name': INDEX}
    del body[missing]
    response = harness.call('post', body, suffix='/check')
    assert response[1] == 400
    assert response[0]['field'] == missing


def test_a_check_of_an_edit_answers_confirmed_false_and_checks_as_the_owner(harness, env):
    schedule_id = env.seed_index_schedule(created_by=OWNER_ID)
    harness.request_as(OTHER_ID, ADMIN)
    body, status = env.check(schedule_id=schedule_id)
    assert status == 200
    assert body['confirmed'] is False
    assert env.expanded == [('artifact', PROJECT_ID, OWNER_ID)]  # the owner's credential, not the admin's


def test_the_index_of_a_new_form_may_already_hold_a_schedule(harness, env):
    env.seed_index_schedule()
    assert env.check()[1] == 200


def test_a_check_of_an_edit_must_name_the_index_of_the_schedule(harness, env):
    schedule_id = env.seed_index_schedule()
    body, status = env.check(schedule_id=schedule_id, index_name='other')
    assert status == 400
    assert body['field'] == 'index_name'


def test_a_check_of_an_edit_cannot_name_an_agent_schedule(harness, env):
    schedule_id = harness.seed()
    body, status = env.check(schedule_id=schedule_id)
    assert status == 400
    assert body['field'] == 'entity_type'


def test_another_owners_schedule_is_not_found_for_a_check(harness, env):
    schedule_id = env.seed_index_schedule(created_by=OTHER_ID)
    assert env.check(schedule_id=schedule_id)[1] == 404


def test_an_agent_check_still_scans_the_version(harness, env):
    body, status = harness.call('post', {'entity_type': 'agent', 'application_id': 1, 'version_id': 10},
                                suffix='/check')
    assert status == 200
    assert [scan[:2] for scan in harness.scans] == [(PROJECT_ID, 10)]
    assert env.expanded == []


def test_saving_an_index_schedule_names_its_type_to_the_governance_notice(harness, env):
    body = {'name': 'Nightly index', 'entity_type': 'toolkit_index', 'toolkit_id': TOOLKIT_ID, 'index_name': INDEX,
            'cron': '0 9 * * *', 'timezone': 'Etc/UTC'}
    response = harness.call('post', body)
    assert response[1] == 201
    assert env.governance == ['toolkit_index']


def test_saving_an_agent_schedule_does_not_name_a_type_to_the_governance_notice(harness, env):
    harness.module.schedule_governance_notice = lambda *args, **kwargs: env.governance.append(kwargs) or None
    assert harness.call('post', agent_body())[1] == 201
    assert env.governance == [{}]
