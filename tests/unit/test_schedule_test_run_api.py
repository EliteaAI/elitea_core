"""Handler tests of the Test Run routes: a run of the unsaved Save form, stored as a run with no schedule.

The harness of the schedule resource tests is shared: handlers, service, the Test Run helper and SQLite tables are
real. The thread that starts the run is replaced.
"""
import pathlib
import sys
import types
from contextlib import nullcontext

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).parent))  # the shared harness lives beside this file
from fixtures import schedule_sqlite as sqlite_db
from test_7011_schedule_crud_api import (
    OTHER_ID,
    OWNER_ID,
    PACKAGE,
    PERMISSION_PREFIX,
    PROJECT_ID,
    agent_body,
    assert_refusal,
    pipeline_body,
)
from test_7011_schedule_crud_api import harness as _crud_harness

harness = _crud_harness  # the CRUD harness fixture, shared
sa = sqlite_db.sa


@pytest.fixture
def started(harness):
    calls = []
    harness.module.start_schedule_run = lambda *args: calls.append(args) or True
    return calls


@pytest.fixture
def removed(harness):
    """The chats the route hands to the cleanup after the commit."""
    calls = []
    harness.module.remove_trimmed_chats = lambda project_id, chats: calls.append((project_id, chats))
    return calls


@pytest.fixture
def chats(harness):
    """Stub chat tables for the trim: a chat has a uuid, a message group may carry a thread id."""
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
    return Conversation, ConversationMessageGroup


def models():
    return sys.modules[f'{PACKAGE}.models.entity_schedule']


def _create_model(values):
    return sys.modules[f'{PACKAGE}.models.pd.entity_schedule'].ScheduleCreate.model_validate(values)


def runs_module():
    return sys.modules[f'{PACKAGE}.utils.schedule_test_runs']


def post_test_run(harness, body=None, **kwargs):
    return harness.call('post', agent_body() if body is None else body, suffix='/test_run', **kwargs)


def schedules(harness):
    with harness.session_factory() as session:
        return session.query(models().EntitySchedule).all()


def runs(harness):
    with harness.session_factory() as session:
        return [(r.schedule_id, r.trigger, r.status, r.requested_by) for r in session.query(models().EntityScheduleRun)]


def stored_form(harness, run_id):
    table = models().EntityScheduleRun.__table__
    with harness.session_factory() as session:
        return session.execute(sa.select(models().RUN_FORM, models().RUN_APPLICATION_ID).where(
            table.c.id == run_id)).one()


def add_test_run(harness, requested_by=OWNER_ID, status='success', application_id=1, **columns):
    """Insert a Test Run row; the form is the one a Save body gives."""
    table = models().EntityScheduleRun.__table__
    form = {'entity_type': 'agent', 'application_id': application_id, 'version_id': 10, 'approval_rule': 'pause_and_ask'}
    with harness.session_factory() as session:
        run_id = session.execute(table.insert().values(
            schedule_id=None, trigger='test', status=status, requested_by=requested_by,
            **{models().RUN_FORM.key: form, models().RUN_APPLICATION_ID.key: application_id}, **columns,
        )).inserted_primary_key[0]
        session.commit()
    return run_id


def test_a_test_run_stores_a_run_with_no_schedule_and_writes_no_schedule_row(harness, started):
    body, status = post_test_run(harness)

    assert (status, body) == (202, {'run_id': 1, 'application_id': 1})
    assert schedules(harness) == []
    assert runs(harness) == [(None, 'test', 'running', OWNER_ID)]
    form, application_id = stored_form(harness, 1)
    assert application_id == 1
    assert (form['name'], form['prompt'], form['version_id'], form['entity_type']) == (
        'Nightly', 'Say hi to {{ who }}', 10, 'agent')
    assert 'risk_ack' not in form and 'enabled' not in form
    assert started == [('agent', PROJECT_ID, None, 1, None)]


def test_the_owner_request_is_named_after_the_test_run(harness, started):
    post_test_run(harness)

    user, kwargs = harness.owner_requests[0]
    assert (user, kwargs['version_id'], kwargs['trigger_ref']) == (OWNER_ID, 10, 'schedule:test:1')


def test_a_test_run_passes_the_owner_request_to_the_starter(harness, started):
    harness.owner_request = 'ticket'

    post_test_run(harness)

    assert started[0][-1] == 'ticket'


def test_a_test_run_leaves_the_schedule_table_and_the_last_run_of_a_saved_schedule_alone(harness, started):
    saved_id = harness.seed(name='Saved')
    before = harness.row(saved_id).last_run_status

    post_test_run(harness)

    assert [row.id for row in schedules(harness)] == [saved_id]
    assert harness.row(saved_id).last_run_status == before


def test_a_second_active_test_run_of_the_same_entity_is_409_with_the_active_run(harness, started):
    first = post_test_run(harness)[0]

    body, status = post_test_run(harness)

    assert status == 409
    assert (body['code'], body['active_run_id']) == ('run_active', first['run_id'])
    assert len(runs(harness)) == 1


def test_a_finished_test_run_lets_the_next_one_start(harness, started):
    post_test_run(harness)
    with harness.session_factory() as session:
        session.query(models().EntityScheduleRun).update({'status': 'success'})
        session.commit()

    assert post_test_run(harness)[1] == 202
    assert len(runs(harness)) == 2


def test_each_owner_and_each_entity_has_its_own_active_test_run(harness, started):
    assert post_test_run(harness)[1] == 202
    assert post_test_run(harness, pipeline_body())[1] == 202
    harness.user_id = OTHER_ID
    assert post_test_run(harness)[1] == 202


@pytest.mark.parametrize('pgcode', ['23502', '42703'])
def test_a_schema_without_the_test_run_columns_answers_503(harness, started, pgcode):
    module = runs_module()

    class Orig(Exception):
        pass

    Orig.pgcode = pgcode

    class Session:
        def begin_nested(self):
            return nullcontext()

        def execute(self, statement):
            raise sa.exc.DBAPIError('insert', {}, Orig())

    with pytest.raises(module.TestRunUnavailable):
        module.insert_test_run(Session(), OWNER_ID, _create_model(agent_body()))


def test_the_route_answers_503_test_run_unavailable_when_the_schema_is_old(harness, started, monkeypatch):
    def unavailable(*args):
        raise runs_module().TestRunUnavailable()

    monkeypatch.setattr(harness.module, 'insert_test_run', unavailable)

    body, status = post_test_run(harness)

    assert (status, body['code']) == (503, 'test_run_unavailable')
    assert started == []


def test_any_other_database_error_is_not_hidden_as_503(harness):
    class Orig(Exception):
        pgcode = '40001'

    class Session:
        def begin_nested(self):
            return nullcontext()

        def execute(self, statement):
            raise sa.exc.DBAPIError('insert', {}, Orig())

    with pytest.raises(sa.exc.DBAPIError):
        runs_module().insert_test_run(Session(), OWNER_ID, _create_model(agent_body()))


def test_the_test_run_uses_the_save_rules_and_writes_nothing_on_a_refusal(harness, started):
    body, status = post_test_run(harness, agent_body(prompt='  '))

    assert (status, body['code'], body['field']) == (400, 'invalid_input', 'prompt')
    assert runs(harness) == [] and started == []


def test_the_test_run_refuses_an_unknown_version_with_the_save_field(harness, started):
    body, status = post_test_run(harness, agent_body(version_id=999))

    assert (status, body['field']) == (400, 'version_id')


def test_the_test_run_refuses_run_without_asking_when_the_project_does_not_allow_it(harness, started):
    body, status = post_test_run(harness, agent_body(approval_rule='run_without_asking'))

    assert (status, body['code']) == (400, 'approval_rule_not_allowed')
    assert runs(harness) == []


def test_the_test_run_needs_the_run_permission(harness, started):
    harness.permissions.discard(PERMISSION_PREFIX + 'run')

    assert post_test_run(harness)[1] == 403
    assert runs(harness) == []


def test_a_test_run_that_cannot_start_fails_the_run_and_deletes_no_schedule(harness):
    harness.module.start_schedule_run = lambda *args: False
    saved_id = harness.seed(name='Saved')

    assert_refusal(post_test_run(harness), 500)

    assert [row[2] for row in runs(harness)] == ['failed']
    assert [row.id for row in schedules(harness)] == [saved_id]


def test_a_test_run_that_cannot_build_the_owner_request_does_not_start(harness, started):
    harness.owner_request_error = RuntimeError('down')

    assert_refusal(post_test_run(harness), 500)
    assert started == []


def test_the_cleanup_of_trimmed_chats_runs_after_the_commit(harness, started, removed):
    post_test_run(harness)

    assert removed == [(PROJECT_ID, [])]


def test_save_does_not_touch_test_runs(harness, started):
    post_test_run(harness)

    assert harness.call('post', agent_body(enabled=False))[1] == 201
    assert len(runs(harness)) == 1


def test_the_delete_route_of_a_test_run_is_gone(harness):
    harness.body, harness.query = None, {'application_id': 1}
    harness.path = f'/schedules/prompt_lib/{PROJECT_ID}/test_run'

    assert harness.routes.PromptLibAPI().delete(project_id=PROJECT_ID) == harness.routes.NOT_ALLOWED
    assert ('delete', 'test_run') not in harness.routes.ROUTES


def trim(harness, keep, application_id=1, user_id=OWNER_ID):
    module = runs_module()
    module.KEEP_TEST_RUNS = keep
    with harness.session_factory() as session:
        trimmed = module.trim_test_runs(session, user_id, application_id)
        session.commit()
    return trimmed


def test_trim_keeps_the_newest_runs_and_deletes_the_older_ones(harness, monkeypatch, chats):
    monkeypatch.setattr(runs_module(), 'KEEP_TEST_RUNS', 50)
    ids = [add_test_run(harness) for _ in range(53)]

    trim(harness, 50)

    with harness.session_factory() as session:
        left = sorted(row.id for row in session.query(models().EntityScheduleRun))
    assert left == ids[3:]


def test_trim_keeps_an_active_run_even_when_it_is_old(harness, monkeypatch, chats):
    monkeypatch.setattr(runs_module(), 'KEEP_TEST_RUNS', 2)
    active = add_test_run(harness, status='waiting_for_approval')
    newer = [add_test_run(harness) for _ in range(3)]

    trim(harness, 2)

    with harness.session_factory() as session:
        left = sorted(row.id for row in session.query(models().EntityScheduleRun))
    assert left == [active, *newer[1:]]


def test_trim_leaves_the_test_runs_of_other_owners_entities_and_saved_runs(harness, monkeypatch, chats):
    monkeypatch.setattr(runs_module(), 'KEEP_TEST_RUNS', 1)
    other_owner = add_test_run(harness, requested_by=OTHER_ID)
    other_entity = add_test_run(harness, application_id=2)
    saved = harness.seed()
    with harness.session_factory() as session:
        session.add(models().EntityScheduleRun(schedule_id=saved, trigger='test', status='success'))
        session.commit()
    mine = [add_test_run(harness) for _ in range(2)]

    trim(harness, 1)

    with harness.session_factory() as session:
        left = {row.id for row in session.query(models().EntityScheduleRun)}
    assert {other_owner, other_entity} <= left and mine[0] not in left and mine[1] in left
    assert len(left) == 4


def test_trim_deletes_the_chats_of_trimmed_runs_and_returns_them_for_the_cleanup(harness, monkeypatch, chats):
    conversation, message_group = chats
    with harness.session_factory() as session:
        session.add_all([conversation(id=5, uuid='aaaa'), conversation(id=6, uuid='bbbb')])
        session.add(message_group(id=1, conversation_id=5, meta={'thread_id': 'thread-1'}))
        session.commit()
    add_test_run(harness, conversation_id=5)
    kept = add_test_run(harness, conversation_id=6)

    trimmed = trim(harness, 1)

    assert [(chat.uuid, chat.thread_ids) for chat in trimmed] == [('aaaa', ['aaaa', 'thread-1'])]
    with harness.session_factory() as session:
        assert [row.id for row in session.query(conversation)] == [6]
        assert [row.id for row in session.query(models().EntityScheduleRun)] == [kept]


def test_a_failed_chat_cleanup_is_logged_and_never_raised(harness, monkeypatch):
    chat = runs_module().TrimmedChat('aaaa', ['aaaa'])
    logged = []
    monkeypatch.setattr(runs_module().log, 'exception', lambda *args: logged.append(args), raising=False)

    runs_module().remove_trimmed_chats(PROJECT_ID, [chat])

    assert len(logged) == 1


@pytest.fixture
def reads(monkeypatch):
    """The read helpers the run routes import late; their own tests read real tables."""
    module = types.ModuleType(f'{PACKAGE}.utils.schedule_run_detail')
    module.version_labels = lambda session, ids: {i: 'v3' for i in ids}
    module.conversation_uuids = lambda session, ids: {i: f'uuid-{i}' for i in ids if i is not None}
    module.usage_summaries = lambda project_id, rows: {row.id: (10, 0.5) for row in rows}
    module.version_label = lambda session, version_id: 'v3'
    module.tool_call_events = lambda session, conversation_id, with_output=True: []
    module.usage_summary = lambda session, project_id, run: (10, 0.5)
    monkeypatch.setitem(sys.modules, module.__name__, module)
    runs = types.ModuleType(f'{PACKAGE}.utils.schedule_runs')  # a draft Test run is decided by its starter
    runs.run_decider_id = lambda session, run: run.requested_by
    runs.can_approve_all = lambda session, project_id, run, viewer_id: viewer_id == OWNER_ID
    monkeypatch.setitem(sys.modules, runs.__name__, runs)


def read_runs(harness, application_id=1, **query):
    return harness.call('get', suffix='/test_run', query={'application_id': application_id, **query})


def read_run(harness, run_id, application_id=1):
    harness.body, harness.query = None, {'application_id': application_id}
    harness.path = f'/schedules/prompt_lib/{PROJECT_ID}/test_run/{run_id}'
    return harness.routes.PromptLibAPI().get(project_id=PROJECT_ID, run_id=run_id)


def test_the_test_run_list_has_the_rows_of_the_runs_list_newest_first(harness, started, reads):
    post_test_run(harness)
    with harness.session_factory() as session:
        session.query(models().EntityScheduleRun).update({'status': 'success'})
        session.commit()
    post_test_run(harness)

    body, status = read_runs(harness)

    assert status == 200 and body['total'] == 2
    assert [row['id'] for row in body['rows']] == [2, 1]
    assert {'id', 'trigger', 'status', 'conversation_id', 'duration_ms', 'version_label', 'tokens', 'cost_usd'} <= set(body['rows'][0])
    assert (body['rows'][0]['tokens'], body['rows'][0]['cost_usd']) == (10, 0.5)
    assert read_runs(harness, limit=1, offset=1)[0]['rows'][0]['id'] == 1


def test_the_test_run_list_takes_the_filters_of_the_runs_list(harness, reads):
    add_test_run(harness, status='failed')
    add_test_run(harness, status='success')

    body, _ = read_runs(harness, status='failed')

    assert [row['status'] for row in body['rows']] == ['failed']
    assert read_runs(harness, status='nonsense')[1] == 400


def test_the_test_run_list_of_another_owner_or_entity_is_empty(harness, started, reads):
    assert read_runs(harness)[0] == {'total': 0, 'rows': []}
    harness.user_id = OTHER_ID
    post_test_run(harness)
    harness.user_id = OWNER_ID
    assert read_runs(harness)[0] == {'total': 0, 'rows': []}
    assert read_runs(harness, application_id=2)[0]['total'] == 0


def test_the_test_run_list_does_not_hold_the_project_session_while_it_reads_usage(harness, reads, monkeypatch):
    add_test_run(harness)
    open_sessions = []
    real_factory = harness.session_factory

    def counting_factory():
        session = real_factory()
        open_sessions.append(session)
        return session

    harness.session_factory = counting_factory
    seen = []
    module = sys.modules[f'{PACKAGE}.utils.schedule_run_detail']
    module.usage_summaries = lambda project_id, rows: seen.append(
        [s.in_transaction() for s in open_sessions]) or {row.id: (None, None) for row in rows}

    assert read_runs(harness)[1] == 200
    assert seen == [[False]]


def test_the_test_run_list_needs_the_run_permission_and_an_application_id(harness, reads):
    assert harness.call('get', suffix='/test_run', query={})[1] == 400
    harness.permissions.discard(PERMISSION_PREFIX + 'run')
    assert read_runs(harness)[1] == 403


def test_the_test_run_list_answers_503_for_an_old_schema(harness, reads, monkeypatch):
    class Orig(Exception):
        pgcode = '42703'

    def old_schema(*args):
        raise sa.exc.DBAPIError('select', {}, Orig())

    monkeypatch.setattr(harness.module, '_runs_page', old_schema)

    body, status = read_runs(harness)

    assert (status, body['code']) == (503, 'test_run_unavailable')


def test_the_test_run_detail_has_the_body_of_the_run_detail_and_the_rule_of_the_form(harness, started, reads):
    run_id = post_test_run(harness, agent_body(approval_rule='pause_and_ask'))[0]['run_id']

    body, status = read_run(harness, run_id)

    assert status == 200
    assert (body['id'], body['trigger'], body['version_label']) == (run_id, 'test', 'v3')
    assert body['summary']['tokens'] == 10 and body['summary']['cost_usd'] == 0.5
    assert body['approval_rule'] == 'pause_and_ask'
    assert body['decider_id'] == OWNER_ID
    assert {'events', 'status', 'conversation_id'} <= set(body)
    assert body['can_approve_all'] is True


def test_the_test_run_detail_is_404_for_a_foreign_a_saved_or_a_missing_run(harness, started, reads):
    own = post_test_run(harness)[0]['run_id']
    saved_run = harness.call('post', {'trigger': 'manual'}, harness.seed(name='Saved'), suffix='/run')[0]['run_id']
    harness.user_id = OTHER_ID
    assert read_run(harness, own)[1] == 404
    harness.user_id = OWNER_ID

    for run_id in (saved_run, 999):
        assert read_run(harness, run_id) == ({'code': 'run_not_found', 'error': 'Run not found'}, 404)
    assert read_run(harness, own, application_id=2)[1] == 404


def test_the_test_run_detail_needs_the_run_permission_and_an_application_id(harness, started, reads):
    run_id = post_test_run(harness)[0]['run_id']
    assert read_run(harness, run_id, application_id='x')[1] == 400
    harness.permissions.discard(PERMISSION_PREFIX + 'run')
    assert read_run(harness, run_id)[1] == 403


def test_run_now_answers_with_the_same_shape(harness, started):
    schedule_id = harness.seed()

    body, status = harness.call('post', {'trigger': 'manual'}, schedule_id, suffix='/run')

    assert (status, body) == (202, {'run_id': 1, 'application_id': 1})


def test_a_saved_run_is_not_reachable_through_the_test_run_scope(harness):
    saved = harness.seed()
    with harness.session_factory() as session:
        session.add(models().EntityScheduleRun(schedule_id=saved, trigger='test', status='success', requested_by=OWNER_ID))
        session.commit()
        assert runs_module().load_test_run(session, OWNER_ID, 1) is None


def test_a_schedule_or_run_id_above_the_database_range_is_404(harness):
    huge = 2**31
    assert harness.call('get', None, huge) == ({'code': 'not_found', 'error': 'Schedule not found'}, 404)
    harness.body, harness.query = None, {'application_id': 1}
    harness.path = f'/schedules/prompt_lib/{PROJECT_ID}/test_run/{huge}'
    assert harness.routes.PromptLibAPI().get(project_id=PROJECT_ID, run_id=huge)[1] == 404


# #7404: a webhook form is not a Test Run (a webhook is tried with a sample delivery, #7399)
def test_7404_a_webhook_form_is_refused_and_stores_no_run(harness, started):
    body = agent_body(trigger_kind='webhook', webhook_type='gitlab', gitlab_auth_method='signing_token',
                      signing_secret_value='whsec_x', cron=None, timezone=None)
    answer, status = post_test_run(harness, body)
    assert (status, answer['field']) == (400, 'trigger_kind')
    assert runs(harness) == [] and started == []


def test_7404_the_stored_form_never_keeps_a_signing_token(harness):
    module = runs_module()
    form = _create_model(agent_body(trigger_kind='webhook', webhook_type='gitlab', gitlab_auth_method='signing_token',
                                    signing_secret_value='whsec_x', cron=None, timezone=None))
    statement = {}
    real_insert = module.EntityScheduleRun.__table__.insert

    class Insert:
        def values(self, **values):
            statement.update(values)
            raise RuntimeError('stop before the database')

    module.EntityScheduleRun.__table__.insert = lambda: Insert()
    try:
        with pytest.raises(RuntimeError):
            module.insert_test_run(types.SimpleNamespace(), OWNER_ID, form)
    finally:
        module.EntityScheduleRun.__table__.insert = real_insert
    assert 'signing_secret_value' not in statement[models().RUN_FORM.key]
