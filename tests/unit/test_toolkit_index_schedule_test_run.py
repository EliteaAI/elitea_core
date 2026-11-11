"""Test Runs of a Toolkit Index: the start and its busy guard, the list, the detail and the trim, each by the
toolkit and index pair, and the default scope that keeps index runs out of every agent and pipeline read.

The handlers, the Test Run helpers and the SQLite tables are real. The thread that starts the run, the read of the
index store, the index row liveness and the admission builder are stand-ins with the real parameters.
"""
import pathlib
import sys
import types

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).parent))  # the shared harness lives beside this file
from fixtures import schedule_sqlite as sqlite_db  # noqa: E402
from test_7011_schedule_crud_api import (  # noqa: E402,F401 -- `harness` is a fixture
    OTHER_ID,
    OWNER_ID,
    PACKAGE,
    PERMISSION_PREFIX,
    PROJECT_ID,
    agent_body,
    assert_refusal,
)
from test_7011_schedule_crud_api import harness as _crud_harness  # noqa: E402

harness = _crud_harness  # the CRUD harness fixture, shared
sa = sqlite_db.sa

ENTRY_INDEX_ON_DEMAND = 'index_on_demand'
TOOLKIT_ID = 3
INDEX = 'docs'


class FakeRequest:
    """The admission request: `model_copy` returns the copy the starter would get."""

    def __init__(self, fields):
        self.fields = fields

    def model_copy(self, update):
        return FakeRequest({**self.fields, **update})


def models():
    return sys.modules[f'{PACKAGE}.models.entity_schedule']


def runs_module():
    return sys.modules[f'{PACKAGE}.utils.schedule_test_runs']


def index_body(index_name=INDEX, toolkit_id=TOOLKIT_ID):
    return {'name': 'Nightly index', 'entity_type': 'toolkit_index', 'toolkit_id': toolkit_id,
            'index_name': index_name, 'cron': '0 9 * * *', 'timezone': 'Etc/UTC'}


class Index:
    """What the Test Run routes did for an indexed toolkit."""

    def __init__(self, harness):
        self.harness = harness
        self.starts = []
        self.admissions = []
        self.live = False
        self.configuration = {'chunk_size': 500}

    def test_run(self, body=None):
        return self.harness.call('post', index_body() if body is None else body, suffix='/test_run')

    def add_run(self, requested_by=OWNER_ID, status='success', toolkit_id=TOOLKIT_ID, index_name=INDEX,
                application_id=None, **columns):
        """Insert a Test Run row, of an index when `application_id` is None, else of that agent."""
        form = ({'entity_type': 'agent', 'application_id': application_id, 'version_id': 10}
                if application_id is not None
                else {'entity_type': 'toolkit_index', 'toolkit_id': toolkit_id, 'index_name': index_name})
        with self.harness.session_factory() as session:
            run_id = session.execute(models().EntityScheduleRun.__table__.insert().values(
                schedule_id=None, trigger='test', status=status, requested_by=requested_by,
                **{models().RUN_FORM.key: form, models().RUN_APPLICATION_ID.key: application_id}, **columns,
            )).inserted_primary_key[0]
            session.commit()
        return run_id

    def run_ids(self):
        with self.harness.session_factory() as session:
            return sorted(row.id for row in session.query(models().EntityScheduleRun))

    def stored(self, run_id):
        table = models().EntityScheduleRun.__table__
        with self.harness.session_factory() as session:
            return session.execute(sa.select(
                table.c.schedule_id, table.c.trigger, table.c.requested_by, table.c.status,
                models().RUN_FORM, models().RUN_APPLICATION_ID).where(table.c.id == run_id)).one()

    def list(self, **query):
        return self.harness.call('get', suffix='/test_run', query=query)

    def detail(self, run_id, **query):
        self.harness.body, self.harness.query = None, query
        self.harness.path = f'/schedules/prompt_lib/{PROJECT_ID}/test_run/{run_id}'
        return self.harness.routes.PromptLibAPI().get(project_id=PROJECT_ID, run_id=run_id)


@pytest.fixture
def index(harness, monkeypatch):
    """An artifact toolkit with a saved index, folder rights granted and the stand-ins installed."""
    state = Index(harness)
    toolkit_model = sys.modules[f'{PACKAGE}.models.all'].EliteATool
    with harness.session_factory() as session:
        session.add(toolkit_model(id=TOOLKIT_ID, name='Docs toolkit', type='artifact'))
        session.commit()
    sys.modules[f'{PACKAGE}.utils.folder_access'].TOOLKIT_ENTITY_TYPES = ('artifact',)
    harness.permissions.add('models.applications.index_meta.edit')  # the caller may edit indexes
    service = sys.modules[f'{PACKAGE}.utils.schedule_service']
    monkeypatch.setattr(service, 'read_index_configuration',
                        lambda session, project_id, toolkit, index_name, user_id: state.configuration)
    harness.module.start_schedule_run = lambda *args: state.starts.append(args) or True
    harness.module.remove_trimmed_chats = lambda project_id, chats: None
    monkeypatch.setattr(harness.module, '_index_row_is_live', lambda project_id, toolkit, name: state.live)

    admission = types.ModuleType(f'{PACKAGE}.models.pd.admission')
    admission.ENTRY_INDEX_ON_DEMAND = ENTRY_INDEX_ON_DEMAND

    # The real keyword-only signature, so a keyword the real builder does not take fails here
    def admission_request_from(module, *, sid, project_id, binding_project_id, version_row, run_kind, entry_point,
                               toolkit_id=None):
        state.admissions.append({'sid': sid, 'project_id': project_id, 'entry_point': entry_point,
                                 'toolkit_id': toolkit_id, 'version_row': version_row})
        return FakeRequest({'entry_point': entry_point})

    admission.admission_request_from = admission_request_from
    monkeypatch.setitem(sys.modules, admission.__name__, admission)

    # The read helpers the run routes import late; their own tests read real tables
    reads = types.ModuleType(f'{PACKAGE}.utils.schedule_run_detail')
    reads.version_labels = lambda session, ids: {i: 'v3' for i in ids}
    reads.conversation_uuids = lambda session, ids: {i: f'uuid-{i}' for i in ids if i is not None}
    reads.usage_summaries = lambda project_id, rows: {row.id: (10, 0.5) for row in rows}
    reads.version_label = lambda session, version_id: 'v3'
    reads.tool_call_events = lambda session, conversation_id, with_output=True: []
    reads.usage_summary = lambda session, project_id, run: (10, 0.5)
    monkeypatch.setitem(sys.modules, reads.__name__, reads)
    deciders = types.ModuleType(f'{PACKAGE}.utils.schedule_runs')  # a draft Test run is decided by its starter
    deciders.run_decider_id = lambda session, run: run.requested_by
    # the run detail imports this name from the module, so the stub must carry it
    deciders.can_approve_all = lambda session, project_id, run, caller_id: False
    monkeypatch.setitem(sys.modules, deciders.__name__, deciders)
    return state


@pytest.fixture
def chats(harness):
    """Stub chat tables for the trim, which reads them late."""
    base = sqlite_db.orm.declarative_base()

    class Conversation(base):
        __tablename__ = 'conversations'
        id = sa.Column(sa.Integer, primary_key=True)
        uuid = sa.Column(sa.String)

    class ConversationMessageGroup(base):
        __tablename__ = 'message_groups'
        id = sa.Column(sa.Integer, primary_key=True)
        conversation_id = sa.Column(sa.Integer)
        meta = sa.Column(sa.JSON)

    base.metadata.create_all(harness.engine)
    for name, cls in (('conversation', Conversation), ('message_group', ConversationMessageGroup)):
        module = types.ModuleType(f'{PACKAGE}.models.{name}')
        setattr(module, cls.__name__, cls)
        sys.modules[module.__name__] = module


# --- Test run -------------------------------------------------------------------------------------------------

def test_a_test_run_of_an_index_stores_a_run_with_no_schedule_and_names_the_pair(harness, index):
    body, status = index.test_run()

    assert (status, body) == (202, {'run_id': 1, 'toolkit_id': TOOLKIT_ID, 'index_name': INDEX})
    schedule_id, trigger, requested_by, run_status, form, application_id = index.stored(1)
    assert (schedule_id, trigger, requested_by, run_status, application_id) == (None, 'test', OWNER_ID, 'running', None)
    assert (form['toolkit_id'], form['index_name'], form['entity_type']) == (TOOLKIT_ID, INDEX, 'toolkit_index')
    with harness.session_factory() as session:
        assert session.query(models().EntitySchedule).count() == 0


def test_a_test_run_of_an_index_starts_with_the_index_entity_and_no_schedule(harness, index):
    body, _ = index.test_run()
    assert [start[:4] for start in index.starts] == [('toolkit_index', PROJECT_ID, None, body['run_id'])]


def test_a_test_run_of_an_index_is_admitted_as_the_on_demand_index_run_named_after_the_run(harness, index):
    body, _ = index.test_run()
    assert index.admissions == [{
        'sid': None, 'project_id': PROJECT_ID, 'entry_point': ENTRY_INDEX_ON_DEMAND, 'toolkit_id': TOOLKIT_ID,
        'version_row': index.admissions[0]['version_row']}]
    started_request = index.starts[0][4]
    assert started_request.fields == {'entry_point': ENTRY_INDEX_ON_DEMAND,
                                      'trigger_ref': f'schedule:test:{body["run_id"]}'}


def test_a_test_run_of_an_index_is_admitted_with_no_governance_when_the_builder_answers_none(harness, index,
                                                                                                monkeypatch):
    admission = sys.modules[f'{PACKAGE}.models.pd.admission']
    monkeypatch.setattr(admission, 'admission_request_from', lambda *args, **kwargs: None)
    assert index.test_run()[1] == 202
    assert index.starts[0][4] is None


def test_the_active_test_run_of_an_index_refuses_the_next_one_whoever_started_it(harness, index):
    active = index.add_run(requested_by=OTHER_ID, status='running')

    response = index.test_run()

    assert_refusal(response, 409, 'A run of this index is active', code='run_active')
    assert response[0]['active_run_id'] is None  # the id of another user's run stays out of the answer
    assert index.run_ids() == [active] and index.starts == []


def test_the_own_active_test_run_of_an_index_is_named_in_the_refusal(harness, index):
    active = index.add_run(requested_by=OWNER_ID, status='running')

    response = index.test_run()

    assert_refusal(response, 409, 'A run of this index is active', code='run_active', active_run_id=active)


def test_a_waiting_test_run_of_an_index_also_refuses_the_next_one(harness, index):
    index.add_run(status='waiting_for_approval')
    assert index.test_run()[1] == 409


def test_a_finished_test_run_of_an_index_lets_the_next_one_start(harness, index):
    index.add_run(status='failed')
    assert index.test_run()[1] == 202


def test_the_active_test_run_of_another_index_or_toolkit_does_not_refuse(harness, index):
    index.add_run(status='running', index_name='other')
    index.add_run(status='running', toolkit_id=33)
    assert index.test_run()[1] == 202


def test_a_live_index_row_refuses_the_test_run_without_an_active_run_id(harness, index):
    index.live = True
    response = index.test_run()
    assert_refusal(response, 409, 'A run of this index is active', code='run_active')
    assert response[0]['active_run_id'] is None
    assert index.run_ids() == []


def test_an_active_test_run_of_an_index_does_not_refuse_an_agent_test_run(harness, index):
    index.add_run(status='running')
    body, status = harness.call('post', agent_body(), suffix='/test_run')
    assert (status, body) == (202, {'run_id': 2, 'application_id': 1})


def test_the_busy_guard_locks_the_toolkit_row_before_it_reads_the_active_run(harness, index):
    statements = []
    factory = harness.session_factory

    def recording_session():
        session = factory()
        scalars = session.scalars
        session.scalars = lambda statement, *args, **kwargs: (
            statements.append(statement) or scalars(statement, *args, **kwargs))
        return session

    harness.session_factory = recording_session
    index.test_run()
    locked = [s for s in statements if s._for_update_arg is not None]
    assert len(locked) == 1 and 'elitea_tools' in str(locked[0])


def test_a_test_run_of_an_index_the_toolkit_does_not_hold_is_400_and_stores_nothing(harness, index):
    index.configuration = None
    body, status = index.test_run()
    assert status == 400 and body['field'] == 'index_name'
    assert index.run_ids() == [] and index.starts == []


def test_a_test_run_of_a_toolkit_outside_the_project_is_400_on_toolkit_id(harness, index):
    body, status = index.test_run(index_body(toolkit_id=99))
    assert status == 400 and body['field'] == 'toolkit_id'
    assert index.run_ids() == []


def test_a_test_run_of_an_index_the_caller_cannot_write_is_refused_and_stores_nothing(harness, index):
    harness.access_error = ({'ok': False, 'error': 'No access'}, 404)
    assert_refusal(index.test_run(), 404, 'No access')
    assert index.run_ids() == []


def test_a_test_run_of_an_index_may_try_the_form_of_a_schedule_the_index_already_holds(harness, index):
    harness.seed(entity_type='toolkit_index', application_id=None, version_id=None, prompt=None, variables=[],
                 toolkit_id=TOOLKIT_ID, index_name=INDEX)
    assert index.test_run()[1] == 202


def test_a_test_run_of_an_index_needs_the_run_permission(harness, index):
    harness.permissions.discard(PERMISSION_PREFIX + 'run')
    assert index.test_run()[1] == 403
    assert index.run_ids() == []


def test_a_test_run_of_an_index_that_cannot_start_fails_the_run(harness, index):
    harness.module.start_schedule_run = lambda *args: False
    assert_refusal(index.test_run(), 500, 'The run could not start', code='run_start_failed')
    assert index.stored(1).status == 'failed'


# --- The list and the detail ----------------------------------------------------------------------------------

def test_the_list_of_an_index_has_the_own_runs_of_that_index_newest_first(harness, index):
    first = index.add_run()
    second = index.add_run(status='failed')

    body, status = index.list(toolkit_id=TOOLKIT_ID, index_name=INDEX)

    assert status == 200 and body['total'] == 2
    assert [row['id'] for row in body['rows']] == [second, first]
    assert {'id', 'trigger', 'status', 'conversation_id', 'version_label', 'tokens', 'cost_usd'} <= set(body['rows'][0])
    assert index.list(toolkit_id=TOOLKIT_ID, index_name=INDEX, status='failed')[0]['total'] == 1
    assert [row['id'] for row in index.list(toolkit_id=TOOLKIT_ID, index_name=INDEX, limit=1, offset=1)[0]['rows']] == [first]


def test_the_list_of_an_index_leaves_out_other_owners_other_indexes_and_agent_runs(harness, index):
    mine = index.add_run()
    index.add_run(requested_by=OTHER_ID)
    index.add_run(index_name='other')
    index.add_run(toolkit_id=33)
    index.add_run(application_id=1)
    with harness.session_factory() as session:
        saved = harness.seed(entity_type='toolkit_index', application_id=None, version_id=None, prompt=None,
                             variables=[], toolkit_id=TOOLKIT_ID, index_name=INDEX)
        session.add(models().EntityScheduleRun(schedule_id=saved, trigger='test', status='success'))
        session.commit()

    body, _ = index.list(toolkit_id=TOOLKIT_ID, index_name=INDEX)

    assert [row['id'] for row in body['rows']] == [mine]


def test_the_list_of_an_agent_leaves_out_the_runs_of_an_index(harness, index):
    index.add_run()
    agent_run = index.add_run(application_id=1)

    body, _ = index.list(application_id=1)

    assert [row['id'] for row in body['rows']] == [agent_run]


@pytest.mark.parametrize('query, field', [
    ({}, 'application_id'),
    ({'toolkit_id': TOOLKIT_ID}, 'index_name'),
    ({'index_name': INDEX}, 'toolkit_id'),
    ({'application_id': 1, 'toolkit_id': TOOLKIT_ID, 'index_name': INDEX}, 'application_id'),
    ({'application_id': 1, 'toolkit_id': TOOLKIT_ID}, 'application_id'),
])
def test_a_list_that_names_no_target_or_two_is_400_with_the_field(harness, index, query, field):
    body, status = index.list(**query)
    assert status == 400 and body['field'] == field


def test_a_list_of_an_index_needs_the_run_permission(harness, index):
    harness.permissions.discard(PERMISSION_PREFIX + 'run')
    assert index.list(toolkit_id=TOOLKIT_ID, index_name=INDEX)[1] == 403


def test_the_detail_of_an_own_index_run_has_the_body_of_the_run_detail(harness, index):
    run_id = index.add_run(status='running')
    table = models().EntityScheduleRun.__table__
    with harness.session_factory() as session:
        session.execute(table.update().where(table.c.id == run_id).values(
            {models().RUN_FORM.key: {'entity_type': 'toolkit_index', 'toolkit_id': TOOLKIT_ID,
                                     'index_name': INDEX, 'approval_rule': 'pause_and_ask'}}))
        session.commit()

    body, status = index.detail(run_id, toolkit_id=TOOLKIT_ID, index_name=INDEX)

    assert status == 200
    assert (body['id'], body['trigger']) == (run_id, 'test')
    assert body['approval_rule'] == 'pause_and_ask'
    assert {'events', 'status', 'conversation_id'} <= set(body)


def test_the_detail_is_404_for_another_owner_another_index_or_a_run_of_an_agent(harness, index):
    own = index.add_run()
    foreign = index.add_run(requested_by=OTHER_ID)
    agent_run = index.add_run(application_id=1)
    saved = harness.seed(entity_type='toolkit_index', application_id=None, version_id=None, prompt=None,
                         variables=[], toolkit_id=TOOLKIT_ID, index_name=INDEX)
    with harness.session_factory() as session:
        session.add(models().EntityScheduleRun(id=500, schedule_id=saved, trigger='test', status='success'))
        session.commit()

    for run_id, query in (
        (foreign, {'toolkit_id': TOOLKIT_ID, 'index_name': INDEX}),
        (own, {'toolkit_id': TOOLKIT_ID, 'index_name': 'other'}),
        (own, {'toolkit_id': 33, 'index_name': INDEX}),
        (agent_run, {'toolkit_id': TOOLKIT_ID, 'index_name': INDEX}),
        (own, {'application_id': 1}),  # an index run is not reachable as an agent run
        (500, {'toolkit_id': TOOLKIT_ID, 'index_name': INDEX}),  # a run of a saved schedule
        (999, {'toolkit_id': TOOLKIT_ID, 'index_name': INDEX}),
    ):
        assert index.detail(run_id, **query) == ({'code': 'run_not_found', 'error': 'Run not found'}, 404), (run_id, query)


@pytest.mark.parametrize('query, field', [
    ({}, 'application_id'),
    ({'toolkit_id': TOOLKIT_ID}, 'index_name'),
    ({'application_id': 1, 'index_name': INDEX}, 'application_id'),
])
def test_a_detail_that_names_no_target_or_two_is_400_with_the_field(harness, index, query, field):
    run_id = index.add_run()
    body, status = index.detail(run_id, **query)
    assert status == 400 and body['field'] == field


def test_the_detail_of_an_index_run_needs_the_run_permission(harness, index):
    run_id = index.add_run()
    harness.permissions.discard(PERMISSION_PREFIX + 'run')
    assert index.detail(run_id, toolkit_id=TOOLKIT_ID, index_name=INDEX)[1] == 403


# --- Trim by the pair -----------------------------------------------------------------------------------------

def trim(harness, keep, monkeypatch, **target):
    monkeypatch.setattr(runs_module(), 'KEEP_TEST_RUNS', keep)
    with harness.session_factory() as session:
        trimmed = runs_module().trim_test_runs(session, OWNER_ID, **target)
        session.commit()
    return trimmed


def test_trim_by_the_pair_keeps_the_newest_runs_of_that_index_only(harness, index, monkeypatch, chats):
    old = [index.add_run() for _ in range(3)]
    other_index = index.add_run(index_name='other')
    other_toolkit = index.add_run(toolkit_id=33)
    other_owner = index.add_run(requested_by=OTHER_ID)
    agent_run = index.add_run(application_id=1)

    trimmed = trim(harness, 2, monkeypatch, toolkit_id=TOOLKIT_ID, index_name=INDEX)

    assert trimmed == []  # an index run has no chat
    assert index.run_ids() == sorted([*old[1:], other_index, other_toolkit, other_owner, agent_run])


def test_trim_by_the_pair_keeps_an_active_run_even_when_it_is_old(harness, index, monkeypatch, chats):
    active = index.add_run(status='running')
    newer = [index.add_run() for _ in range(3)]

    trim(harness, 2, monkeypatch, toolkit_id=TOOLKIT_ID, index_name=INDEX)

    assert index.run_ids() == [active, *newer[1:]]


def test_trim_by_an_agent_leaves_the_runs_of_an_index(harness, index, monkeypatch, chats):
    index_runs = [index.add_run() for _ in range(3)]
    agent_runs = [index.add_run(application_id=1) for _ in range(3)]

    trim(harness, 1, monkeypatch, application_id=1)

    assert index.run_ids() == [*index_runs, agent_runs[-1]]


def test_trim_without_a_target_leaves_the_runs_of_an_index(harness, index, monkeypatch, chats):
    index_runs = [index.add_run() for _ in range(3)]
    agent_runs = [index.add_run(application_id=1) for _ in range(3)]

    trim(harness, 1, monkeypatch)

    assert index.run_ids() == [*index_runs, agent_runs[-1]]


def test_a_test_run_of_an_index_trims_the_older_runs_of_that_index_after_it_is_stored(harness, index, monkeypatch,
                                                                                    chats):
    monkeypatch.setattr(runs_module(), 'KEEP_TEST_RUNS', 1)
    old = [index.add_run() for _ in range(2)]
    other = index.add_run(index_name='other')

    body, status = index.test_run()

    assert status == 202
    assert index.run_ids() == [other, body['run_id']] and old[0] not in index.run_ids()


# --- The default scope ----------------------------------------------------------------------------------------

def test_the_default_scope_reaches_no_run_of_an_index(harness, index):
    index_run = index.add_run(status='running')
    agent_run = index.add_run(application_id=1, status='running')
    runs = runs_module()
    with harness.session_factory() as session:
        assert runs.load_test_run(session, OWNER_ID, index_run) is None
        assert runs.load_test_run(session, OWNER_ID, agent_run).id == agent_run
        assert runs.active_test_run_id(session, OWNER_ID) == agent_run
        assert runs.active_test_run_id(session, OWNER_ID, 1) == agent_run


def test_a_half_named_pair_falls_back_to_the_default_scope(harness, index):
    index_run = index.add_run(status='running')
    runs = runs_module()
    with harness.session_factory() as session:
        assert runs.load_test_run(session, OWNER_ID, index_run, toolkit_id=TOOLKIT_ID) is None
        assert runs.load_test_run(session, OWNER_ID, index_run, index_name=INDEX) is None
        assert runs.load_test_run(session, OWNER_ID, index_run, toolkit_id=TOOLKIT_ID, index_name=INDEX).id == index_run


def test_the_pair_scope_matches_the_toolkit_id_as_a_whole_number(harness, index):
    run_id = index.add_run(toolkit_id=33, status='running')
    runs = runs_module()
    with harness.session_factory() as session:
        assert runs.active_test_run_id(session, OWNER_ID, toolkit_id=3, index_name=INDEX) is None
        assert runs.active_test_run_id(session, OWNER_ID, toolkit_id=33, index_name=INDEX) == run_id


def test_the_active_index_run_read_ignores_the_owner_and_the_agent_runs(harness, index):
    index.add_run(application_id=1, status='running')
    foreign = index.add_run(requested_by=OTHER_ID, status='running')
    runs = runs_module()
    with harness.session_factory() as session:
        assert runs.active_index_test_run_id(session, TOOLKIT_ID, INDEX) == foreign
        assert runs.active_index_test_run_id(session, TOOLKIT_ID, 'other') is None


def test_the_decision_route_cannot_reach_the_test_run_of_an_index(harness, index):
    run_id = index.add_run(status='waiting_for_approval')
    harness.body, harness.query = {'action': 'approve'}, {}
    harness.path = f'/schedules/prompt_lib/{PROJECT_ID}/test_run/{run_id}/decision'
    body, status = harness.routes.PromptLibAPI().post(project_id=PROJECT_ID, run_id=run_id)
    assert status == 404 and body['code'] == 'run_not_found'
