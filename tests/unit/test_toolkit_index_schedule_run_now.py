"""Run now of a Toolkit Index schedule: no version rule, the busy guard, the admission and the reply.

The handler, the claim insert and the SQLite tables are real. The thread that starts the run, the read of the index
row and the admission builder are replaced; the harness of the schedule resource tests is shared.
"""
import pathlib
import sys
import types
from unittest.mock import create_autospec

import pytest
from fixtures.helpers import load_module_with_stubs

sys.path.insert(0, str(pathlib.Path(__file__).parent))  # the shared harness lives beside this file
from test_7011_schedule_crud_api import (  # noqa: E402,F401 -- `harness` is a fixture
    ADMIN,
    ADMIN_ID,
    OTHER_ID,
    OWNER_ID,
    PACKAGE,
    PLUGIN_ROOT,
    PROJECT_ID,
    assert_refusal,
)
from test_7011_schedule_crud_api import harness as _crud_harness  # noqa: E402

harness = _crud_harness  # the CRUD harness fixture, shared

ENTRY_INDEX_ON_DEMAND = 'index_on_demand'
TOOLKIT_ID = 3
INDEX = 'docs'


class FakeRequest:
    """The admission request: `model_copy` returns the copy the starter would get."""

    def __init__(self, fields):
        self.fields = fields

    def model_copy(self, update):
        return FakeRequest({**self.fields, **update})


class Index:
    """What the run endpoint did for one index schedule."""

    def __init__(self, harness):
        self.harness = harness
        self.starts = []
        self.admissions = []
        self.live = False
        self.schedule_id = None

    def run(self, schedule_id=None):
        schedule_id = schedule_id or self.schedule_id
        return self.harness.call('post', {'trigger': 'manual'}, schedule_id, suffix='/run')

    def runs(self):
        run_model = sys.modules[f'{PACKAGE}.models.entity_schedule'].EntityScheduleRun
        with self.harness.session_factory() as session:
            return [(r.id, r.schedule_id, r.trigger, r.status) for r in session.query(run_model)]

    def add_test_run(self, index_name=INDEX, requested_by=OTHER_ID, status='running'):
        entity = sys.modules[f'{PACKAGE}.models.entity_schedule']
        statement = entity.EntityScheduleRun.__table__.insert().values(
            schedule_id=None, trigger='test', status=status, requested_by=requested_by,
            **{entity.RUN_FORM.key: {'toolkit_id': TOOLKIT_ID, 'index_name': index_name},
               entity.RUN_APPLICATION_ID.key: None})
        with self.harness.session_factory() as session:
            run_id = session.execute(statement).inserted_primary_key[0]
            session.commit()
        return run_id

    def add_schedule_run(self, status='running'):
        run_model = sys.modules[f'{PACKAGE}.models.entity_schedule'].EntityScheduleRun
        with self.harness.session_factory() as session:
            row = run_model(schedule_id=self.schedule_id, trigger='cron', status=status)
            session.add(row)
            session.commit()
            return row.id


@pytest.fixture
def index(harness, monkeypatch):
    """An indexed toolkit, its Toolkit Index schedule owned by OWNER_ID, and an index that is not running."""
    state = Index(harness)
    toolkit_model = sys.modules[f'{PACKAGE}.models.all'].EliteATool
    with harness.session_factory() as session:
        session.add(toolkit_model(id=TOOLKIT_ID, name='Docs toolkit', type='artifact'))
        session.commit()
    state.schedule_id = harness.seed(
        entity_type='toolkit_index', application_id=None, version_id=None, prompt=None, variables=[],
        toolkit_id=TOOLKIT_ID, index_name=INDEX)
    sys.modules[f'{PACKAGE}.utils.folder_access'].TOOLKIT_ENTITY_TYPES = ('artifact',)
    harness.permissions.add('models.applications.index_meta.edit')  # the owner may edit indexes
    harness.module.start_schedule_run = lambda *args: state.starts.append(args) or True
    monkeypatch.setattr(harness.module, '_index_row_is_live', lambda project_id, toolkit, name: state.live)

    admission = types.ModuleType(f'{PACKAGE}.models.pd.admission')
    admission.ENTRY_INDEX_ON_DEMAND = ENTRY_INDEX_ON_DEMAND

    # The real keyword-only signature, so a keyword the real builder does not take fails here
    def admission_request_from(module, *, sid, project_id, binding_project_id, version_row, run_kind, entry_point,
                               toolkit_id=None):
        state.admissions.append({'sid': sid, 'project_id': project_id, 'binding_project_id': binding_project_id,
                                 'version_row': version_row, 'run_kind': run_kind, 'entry_point': entry_point,
                                 'toolkit_id': toolkit_id})
        return harness.owner_request

    admission.admission_request_from = admission_request_from
    monkeypatch.setitem(sys.modules, admission.__name__, admission)
    return state


def test_run_now_of_an_index_answers_202_with_the_toolkit_and_the_index(harness, index):
    body, status = index.run()
    assert status == 202
    assert body == {'run_id': body['run_id'], 'toolkit_id': TOOLKIT_ID, 'index_name': INDEX}
    assert index.runs() == [(body['run_id'], index.schedule_id, 'manual', 'running')]
    assert index.starts == [('toolkit_index', PROJECT_ID, index.schedule_id, body['run_id'], None)]


def test_run_now_of_an_agent_still_names_the_application(harness, index):
    schedule_id = harness.seed()
    body, status = index.run(schedule_id)
    assert status == 202
    assert body == {'run_id': body['run_id'], 'application_id': 1}


def test_run_now_of_an_index_is_admitted_as_the_on_demand_index_run(harness, index):
    harness.owner_request = FakeRequest({'entry_point': ENTRY_INDEX_ON_DEMAND})
    body, status = index.run()
    assert status == 202
    assert index.admissions == [{
        'sid': None, 'project_id': PROJECT_ID, 'binding_project_id': PROJECT_ID,
        'version_row': types.SimpleNamespace(id=None, application_id=None), 'run_kind': 'interactive',
        'entry_point': ENTRY_INDEX_ON_DEMAND, 'toolkit_id': TOOLKIT_ID}]
    assert harness.owner_requests == []  # the agent builder needs a version and is not used
    sent = index.starts[0][4]
    assert sent.fields == {'entry_point': ENTRY_INDEX_ON_DEMAND, 'trigger_ref': f'schedule:entity:{index.schedule_id}'}


def test_run_now_of_an_index_with_governance_off_sends_no_admission(harness, index):
    harness.owner_request = None
    index.run()
    assert index.starts[0][4] is None


def test_run_now_over_a_live_index_row_answers_409_and_inserts_no_run(harness, index):
    index.live = True
    assert_refusal(index.run(), 409, 'A run of this index is active', code='run_active')
    assert index.runs() == []
    assert index.starts == []


def test_run_now_over_a_test_run_of_another_user_does_not_name_that_run(harness, index):
    index.add_test_run(requested_by=OTHER_ID)
    response = index.run()
    assert_refusal(response, 409, 'A run of this index is active', code='run_active')
    assert response[0]['active_run_id'] is None
    assert index.starts == []


def test_run_now_over_the_own_test_run_of_the_index_names_that_run(harness, index):
    test_run = index.add_test_run(requested_by=OWNER_ID)
    assert_refusal(index.run(), 409, 'A run of this index is active', code='run_active', active_run_id=test_run)
    assert index.starts == []


def test_run_now_ignores_the_test_run_of_another_index(harness, index):
    index.add_test_run(index_name='other')
    assert index.run()[1] == 202


def test_run_now_over_an_active_run_of_the_row_answers_409_with_that_run(harness, index):
    active = index.add_schedule_run()
    assert_refusal(index.run(), 409, code='run_active', active_run_id=active)
    assert index.starts == []


def test_run_now_of_an_index_is_refused_for_an_admin_who_is_not_the_owner(harness, index):
    harness.request_as(OTHER_ID, ADMIN)
    assert_refusal(index.run(), 403, code='not_owner')
    assert index.runs() == []


def test_run_now_of_an_index_is_refused_for_an_owner_without_the_index_edit_permission(harness, index):
    harness.permissions.discard('models.applications.index_meta.edit')
    assert_refusal(index.run(), 403, code='read_only')
    assert index.runs() == []
    assert index.starts == []


def test_run_now_of_an_index_is_refused_for_an_owner_who_cannot_write_the_toolkit_folder(harness, index):
    harness.access_error = ({'ok': False, 'error': 'No access'}, 404)
    assert_refusal(index.run(), 404, code='not_found')
    assert index.starts == []


CANNOT_USE = 'the new owner cannot use schedules'


@pytest.fixture
def owner_check(harness):
    """The real owner module with a fake eligibility check that, like the real one, needs the toolkit id of an index."""
    load_module_with_stubs(PLUGIN_ROOT / 'utils/schedule_prerun.py', f'{PACKAGE}.utils.schedule_prerun')
    module = load_module_with_stubs(PLUGIN_ROOT / 'utils/schedule_owner.py', f'{PACKAGE}.utils.schedule_owner')
    module.seen = []

    def validate_new_owner(project_id, owner_id, entity_type=None, entity_id=None):
        module.seen.append((entity_type, entity_id))
        return None if entity_id == TOOLKIT_ID and owner_id == OTHER_ID else CANNOT_USE

    module.validate_new_owner = validate_new_owner
    harness.request_as(ADMIN_ID, ADMIN)
    return module


def test_the_owner_change_of_an_index_row_checks_the_new_owner_against_the_toolkit(harness, index, owner_check):
    body, status = harness.call('post', {'owner_id': OTHER_ID}, index.schedule_id, suffix='/owner')
    assert (status, body['created_by']) == (200, OTHER_ID)
    assert owner_check.seen == [('toolkit_index', TOOLKIT_ID)]


def test_the_owner_change_of_an_index_row_refuses_a_new_owner_without_the_index_rights(harness, index, owner_check):
    response = harness.call('post', {'owner_id': OTHER_ID + 100}, index.schedule_id, suffix='/owner')
    assert_refusal(response, 400, CANNOT_USE, code='invalid_input', field='owner_id')
    assert harness.row(index.schedule_id).created_by == OWNER_ID


def test_an_agent_row_without_a_version_is_still_refused(harness, index):
    schedule_id = harness.seed(version_id=None)
    assert_refusal(index.run(schedule_id), 409, code='version_deleted')


def test_the_toolkit_row_is_locked_before_the_busy_read(harness, index):
    """The busy guard takes the toolkit row FOR UPDATE, so a second caller waits for the first one's commit."""
    statements = []
    factory = harness.session_factory

    def recording_session():
        session = factory()
        scalars = session.scalars
        session.scalars = lambda statement, *args, **kwargs: (
            statements.append(statement) or scalars(statement, *args, **kwargs))
        return session

    harness.session_factory = recording_session
    index.run()
    locked = [s for s in statements if s._for_update_arg is not None]
    assert len(locked) == 1
    assert 'elitea_tools' in str(locked[0])


class TestIndexRowLiveness:
    """`_index_row_is_live` reads the index row like the starter and fails open."""

    @pytest.fixture
    def read(self, harness, monkeypatch):
        calls = types.SimpleNamespace(meta={}, heartbeat=None, error=None, stale=False, staleness=[])
        starter = types.ModuleType(f'{PACKAGE}.utils.schedule_index_starter')
        starter._toolkit = create_autospec(lambda project_id, owner_id, toolkit_id: None)
        starter._toolkit.side_effect = lambda *args: ({}, 'postgresql://store')

        def read_index(connection_string, toolkit_id, index_name, with_heartbeat=False):
            if calls.error:
                raise calls.error
            return types.SimpleNamespace(meta=calls.meta, heartbeat=calls.heartbeat)

        starter._read_index = read_index
        tools = types.ModuleType(f'{PACKAGE}.utils.application_tools')
        tools.DEFAULT_TASK_DISCONNECTED_TIMEOUT_SEC = 900

        def resolve_index_staleness(state, updated_on, timeout, pending_heartbeat=None, heartbeat_horizon=None):
            calls.staleness.append((state, updated_on, timeout, pending_heartbeat, heartbeat_horizon))
            return calls.stale

        tools.resolve_index_staleness = resolve_index_staleness
        monkeypatch.setitem(sys.modules, starter.__name__, starter)
        monkeypatch.setitem(sys.modules, tools.__name__, tools)
        monkeypatch.setattr(harness.module, 'VaultClient',
                            lambda project_id: types.SimpleNamespace(get_secrets=lambda: {}))
        return calls

    def toolkit(self):
        return types.SimpleNamespace(id=TOOLKIT_ID, author_id=OWNER_ID)

    def test_an_index_that_is_not_in_progress_is_not_live(self, harness, read):
        read.meta = {'state': 'completed'}
        assert harness.module._index_row_is_live(PROJECT_ID, self.toolkit(), INDEX) is False
        assert read.staleness == []

    def test_an_in_progress_index_with_a_beating_run_is_live(self, harness, read):
        read.meta, read.heartbeat = {'state': 'in_progress', 'updated_on': 5}, 100.0
        assert harness.module._index_row_is_live(PROJECT_ID, self.toolkit(), INDEX) is True
        assert read.staleness == [('in_progress', 5, 900, 100.0, None)]  # the control rule: no display horizon

    def test_a_stale_in_progress_index_is_not_live(self, harness, read):
        read.meta, read.stale = {'state': 'in_progress'}, True
        assert harness.module._index_row_is_live(PROJECT_ID, self.toolkit(), INDEX) is False

    def test_an_unreadable_index_does_not_block_the_run(self, harness, read):
        read.error = RuntimeError('store down')
        assert harness.module._index_row_is_live(PROJECT_ID, self.toolkit(), INDEX) is False


def test_the_governance_notice_names_the_type_only_for_an_index(harness):
    seen = []

    def schedule_governance_notice(module, session, project_id, application_id, version_id, owner_id, saver_id,
                                   entity_type=None):
        seen.append(entity_type)
        return 'notice'

    harness.module.schedule_governance_notice = schedule_governance_notice
    index_row = types.SimpleNamespace(entity_type='toolkit_index', application_id=None, version_id=None, created_by=1)
    agent_row = types.SimpleNamespace(entity_type='agent', application_id=1, version_id=10, created_by=1)
    assert harness.module._governance_notice(object(), None, PROJECT_ID, index_row, 2) == 'notice'
    assert harness.module._governance_notice(object(), None, PROJECT_ID, agent_row, 2) == 'notice'
    assert seen == ['toolkit_index', None]
