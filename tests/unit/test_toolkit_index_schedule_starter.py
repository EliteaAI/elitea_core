"""The Toolkit Index run starter: one claimed run, from the owner check to the index task (SQLite, real run writers).

The modules around the starter (tokens, admission, the index worker, the vector store) are stand-ins whose signatures
match the real ones, so a call with a wrong argument fails here. Each stand-in is set by the test through `Start`.
"""
import sys
import types
from unittest.mock import create_autospec

import pytest
from fixtures.helpers import load_module_with_stubs
from unit import test_toolkit_index_schedule_run_close as run_close
from unit.test_toolkit_index_schedule_run_close import OWNER_ID, PACKAGE, PLUGIN_ROOT, PROJECT_ID, TOOLKIT_ID

env = run_close.env  # the fixture of the close test, bound here so pytest finds it

CONNECTION = 'postgresql://vector-store'
OLD_TASK = 'task-old'
NEW_TASK = 'task-new'
SAVED = {'chunk_size': 500}
LLM = {'model_name': 'gpt-x', 'max_tokens': 100}
SETTINGS = {'pgvector_configuration': {'connection_string': CONNECTION}, 'expanded': True}


def fake(implementation):
    """A mock with the signature of `implementation`, which it also runs."""
    return create_autospec(implementation, side_effect=implementation)


def stand_in(name, **members):
    module = types.ModuleType(f'{PACKAGE}.utils.{name}')
    vars(module).update(members)
    sys.modules[module.__name__] = module
    return module


class FakeIndexSession:
    def __init__(self, row):
        self.row = row

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def query(self, *columns):
        return self

    def filter(self, *conditions):
        return self

    def first(self):
        return self.row


class Start:
    """Stand-ins for everything around the starter, with a default that lets a cron run reach the dispatch."""

    def __init__(self, env, monkeypatch):
        self.env, self.monkeypatch, self.order = env, monkeypatch, []
        self.owner, self.prerun_ok, self.budget_scope = OWNER_ID, True, None
        self.governed = self.auditing = False
        self.token, self.token_error = 'user-token', None
        self.config = {'id': TOOLKIT_ID, 'type': 'artifact', 'name': 'Docs', 'settings': SETTINGS}
        self.config_error = None
        self.meta = {'type': 'index_meta', 'collection': 'docs', 'state': 'completed', 'index_configuration': SAVED,
                     'task_id': OLD_TASK, 'updated_on': 100.0}
        self.index_missing, self.read_error, self.heartbeat, self.stale = False, None, None, False
        self.secrets, self.llm = {}, (LLM, None, False)
        self.dispatch_error, self.task_id, self.finish = None, NEW_TASK, None
        self.stop_error, self.free_form, self.template = None, False, False
        self.mandates, self.mandate, self.audit_code = {}, None, None
        self.mandate_error, self.refusal_code, self.resave = None, 'AUTHORIZATION_REQUIRED', False
        self.stopped = []
        self._install_core()
        self._install_late_imports()

    def _install_core(self):
        starter, start = self.env.starter, self

        def run_prerun_check(project_id, schedule, run_id):
            start.order.append('prerun')
            return types.SimpleNamespace(ok=start.prerun_ok, owner_id=start.owner, reason=None)

        def closed_budget_scope(project_id, user_id=None):
            return start.budget_scope

        def is_governed(module):
            return start.governed

        def is_auditing(module):
            return start.auditing

        def get_predict_token_and_session(project_id, user_id, sid=None, run_identity=False, mandate_run=False):
            if start.token_error is not None:
                raise start.token_error
            return start.token, 'auth-session'

        def get_toolkit_config(project_id, user_id, toolkit_id):
            start.order.append('toolkit_config')
            if start.config_error is not None:
                raise start.config_error
            return start.config

        def get_predict_base_url(project_id):
            return 'https://deployment'

        def get_session_for_schema(connection_string, schema):
            start.order.append('read_index')
            if start.read_error is not None:
                raise start.read_error
            return FakeIndexSession(None if start.index_missing else types.SimpleNamespace(id='row', cmetadata=start.meta))

        def get_pending_index_run_heartbeat(session, index_name):
            return start.heartbeat

        def resolve_index_staleness(index_data_state, updated_on, task_disconnected_timeout, pending_heartbeat=None,
                                    heartbeat_horizon=None):
            return start.stale

        def resolve_index_llm_settings(project_id):
            return start.llm

        def start_index_task(task_node, data, sio_event, initiator='user', admission=None):
            start.order.append('dispatch')
            if start.dispatch_error is not None:
                raise start.dispatch_error
            if start.finish is not None:  # the end event came before the task id was written
                start.meta = {**start.meta, 'task_id': start.task_id, **start.finish}
            return start.task_id

        def count_refusal(project_id, schedule, run_id, reason, code=None, disabled_reason='x', text='y'):
            start.order.append('count_refusal')

        class VaultClient:
            def __init__(self, project_id):
                self.project_id = project_id

            def get_secrets(self):
                return start.secrets

        def stop_task(task_id):
            start.stopped.append(task_id)
            if start.stop_error is not None:
                raise start.stop_error

        names = dict(run_prerun_check=run_prerun_check, closed_budget_scope=closed_budget_scope,
                     is_governed=is_governed, is_auditing=is_auditing,
                     get_predict_token_and_session=get_predict_token_and_session,
                     get_toolkit_config=get_toolkit_config, get_predict_base_url=get_predict_base_url,
                     get_session_for_schema=get_session_for_schema,
                     get_pending_index_run_heartbeat=get_pending_index_run_heartbeat,
                     resolve_index_staleness=resolve_index_staleness,
                     resolve_index_llm_settings=resolve_index_llm_settings, start_index_task=start_index_task,
                     count_refusal=count_refusal)
        self.mocks = {name: fake(implementation) for name, implementation in names.items()}
        for name, mock in self.mocks.items():
            self.monkeypatch.setattr(starter, name, mock)
        self.monkeypatch.setattr(starter, 'VaultClient', VaultClient)
        self.stop_task = fake(stop_task)
        self.monkeypatch.setattr(starter, 'this', types.SimpleNamespace(
            module=types.SimpleNamespace(task_node=types.SimpleNamespace(stop_task=self.stop_task))))

    def _install_late_imports(self):
        start = self

        def latest_toolkit_mandates(session, project_id):
            start.order.append('mandates')
            if start.mandate_error is not None:
                raise start.mandate_error
            return start.mandates

        def covering_toolkit_mandate(mandates, toolkit_id, schedule_key, created_by):
            return start.mandate

        def principal_of(trigger_kind, mandate=None, schedule_key=None):
            return f'principal:{trigger_kind}'

        def resave_needed(mandates, toolkit_id, schedule_key, created_by):
            return start.resave

        def audit_only_refusal(mandate, operation='run:index'):
            return start.audit_code

        def index_schedule_trigger_ref(toolkit_id, index_meta_id, schedule_key):
            return f'index:{toolkit_id}:{index_meta_id}:{schedule_key}'

        def toolkit_mandate_request(*, project_id, toolkit_id, mandate, trigger_ref):
            return f'request:{trigger_ref}'

        def index_schedule_refusal_code(principal):
            return start.refusal_code

        def index_schedule_refusal_text(code, resave=False):
            return f'Index run refused ({code}{", resave" if resave else ""})'

        def record_index_schedule_refusal(module, code, *, project_id, toolkit_id, mandate_ref=None):
            start.order.append('record_refusal')

        def record_index_schedule_would_deny(module, code, *, project_id, toolkit_id, trigger_ref):
            start.order.append('record_would_deny')

        def has_free_form_secret(module, payload):
            return start.free_form

        def has_secret_template(node):
            return start.template

        def mark_free_form_secret(payload, found):
            payload['free_form_secret'] = found

        self.trigger_principal = stand_in(
            'trigger_principal', OPERATION_OF={'index_schedule': 'run:index'},
            latest_toolkit_mandates=fake(latest_toolkit_mandates), covering_toolkit_mandate=fake(covering_toolkit_mandate),
            principal_of=fake(principal_of), resave_needed=fake(resave_needed))
        self.mandate_admission = stand_in(
            'mandate_admission', audit_only_refusal=fake(audit_only_refusal),
            index_schedule_trigger_ref=fake(index_schedule_trigger_ref),
            toolkit_mandate_request=fake(toolkit_mandate_request))
        self.admission = stand_in(
            'admission', index_schedule_refusal_code=fake(index_schedule_refusal_code),
            index_schedule_refusal_text=fake(index_schedule_refusal_text),
            record_index_schedule_refusal=fake(record_index_schedule_refusal),
            record_index_schedule_would_deny=fake(record_index_schedule_would_deny))
        self.credential_slots = stand_in(
            'credential_slots', has_free_form_secret=fake(has_free_form_secret),
            has_secret_template=fake(has_secret_template), mark_free_form_secret=fake(mark_free_form_secret))

    def schedule(self, **fields):
        return self.env.seed_schedule(**fields)

    def run(self, schedule_id, trigger='cron', owner_request=None, **run_fields):
        """Claim a run of the schedule (none for a Test run) and start it; returns the run id."""
        run_id = self.env.add_run(schedule_id, task_id=None, trigger=trigger, **run_fields)
        self.env.starter.run_index_schedule(PROJECT_ID, schedule_id, run_id, owner_request)
        return run_id

    def test_run(self, owner_request='request:test', **form):
        values = {'toolkit_id': TOOLKIT_ID, 'index_name': 'docs', 'name': 'Try it', **form}
        run_id = self.env.add_run(None, task_id=None, trigger='test', form=values)
        self.env.starter.run_index_schedule(PROJECT_ID, None, run_id, owner_request)
        return run_id

    @property
    def data(self):
        return self.mocks['start_index_task'].call_args.args[1]

    @property
    def dispatched(self):
        return 'dispatch' in self.order

    def events(self, run_id):
        with self.env.session_factory() as session:
            return session.get(self.env.EntityScheduleRun, run_id).events

    def enabled(self, schedule_id):
        with self.env.session_factory() as session:
            return session.get(self.env.EntitySchedule, schedule_id).enabled


@pytest.fixture
def start(env, monkeypatch):
    return Start(env, monkeypatch)


def test_the_dispatch_map_starts_toolkit_index_rows_with_the_index_starter(env, start):
    dispatch = load_module_with_stubs(PLUGIN_ROOT / 'utils/schedule_dispatch.py', f'{PACKAGE}.utils.schedule_dispatch')
    calls = []
    env.starter.run_index_schedule = lambda project_id, schedule_id, run_id, owner_request=None: calls.append(
        (project_id, schedule_id, run_id, owner_request))
    dispatch.RUN_STARTERS['toolkit_index'](PROJECT_ID, 5, 6, 'request')
    dispatch.RUN_STARTERS['toolkit_index'](PROJECT_ID, None, 7)
    assert calls == [(PROJECT_ID, 5, 6, 'request'), (PROJECT_ID, None, 7, None)]


def test_a_cron_run_dispatches_the_saved_index_settings_as_the_owner(env, start):
    schedule_id = start.schedule()
    run_id = start.run(schedule_id)
    data = start.data
    assert data['tool_name'] == 'index_data'
    assert data['tool_params'] == SAVED
    assert (data['project_id'], data['user_id'], data['project_auth_token']) == (PROJECT_ID, OWNER_ID, 'user-token')
    assert (data['llm_model'], data['llm_settings']) == ('gpt-x', LLM)
    assert data['deployment_url'] == 'https://deployment'
    call = start.mocks['start_index_task'].call_args
    assert call.kwargs == {'initiator': 'schedule', 'admission': None}
    assert env.run(run_id) == ('running', None)


def test_the_toolkit_config_names_the_toolkit_by_its_type_with_expanded_settings(env, start):
    start.run(start.schedule())
    assert start.data['toolkit_config'] == {'id': TOOLKIT_ID, 'toolkit_name': 'artifact', 'settings': SETTINGS}


def test_the_dispatched_task_id_is_written_into_the_run_log(env, start):
    run_id = start.run(start.schedule())
    events = [event for event in start.events(run_id) if event['kind'] == 'index_task']
    assert [event['task_id'] for event in events] == [NEW_TASK]


def test_the_settings_are_expanded_before_the_index_is_read(env, start):
    start.run(start.schedule())
    assert start.order.index('toolkit_config') < start.order.index('read_index') < start.order.index('dispatch')
    assert start.mocks['get_session_for_schema'].call_args_list[0].args == (CONNECTION, str(TOOLKIT_ID))


def test_a_failed_prerun_check_stops_the_starter_before_any_other_step(env, start):
    start.prerun_ok = False
    run_id = start.run(start.schedule())
    assert start.order == ['prerun']
    assert env.run(run_id)[0] == 'running'  # the pre-run check ends its own run


def test_a_closed_budget_blocks_the_run_without_a_dispatch(env, start):
    start.budget_scope = 'project'
    run_id = start.run(start.schedule())
    assert env.run(run_id) == ('blocked', 'the project budget is used up')
    assert not start.dispatched


def test_a_deleted_schedule_fails_the_run_with_not_found(env, start):
    run_id = start.run(999)
    assert env.run(run_id) == ('failed', env.starter.NOT_FOUND)
    assert start.order == []


def test_a_test_run_reads_its_target_and_owner_from_its_form(env, start):
    start.owner = OWNER_ID
    run_id = start.test_run(index_name='docs', name='Try it')
    target = start.mocks['run_prerun_check'].call_args.args[1]
    assert (target.id, target.toolkit_id, target.index_name, target.name) == (None, TOOLKIT_ID, 'docs', 'Try it')
    assert (target.created_by, target.principal, target.trigger) == (
        OWNER_ID, {'kind': 'delegation', 'ref': OWNER_ID}, 'test')
    assert start.data['user_id'] == OWNER_ID
    assert env.run(run_id)[0] == 'running'


def test_a_test_run_of_a_deleted_toolkit_fails_with_not_found(env, start):
    run_id = start.test_run(toolkit_id=99)
    assert env.run(run_id) == ('failed', env.starter.NOT_FOUND)
    assert not start.dispatched


def test_a_test_run_without_an_index_name_fails_with_not_found(env, start):
    run_id = start.test_run(index_name=None)
    assert env.run(run_id) == ('failed', env.starter.NOT_FOUND)


def test_a_missing_token_fails_the_run_with_no_token(env, start):
    start.token_error = sys.modules[f'{PACKAGE}.utils.predict_utils'].PredictPayloadError('User token not found')
    run_id = start.run(start.schedule())
    assert env.run(run_id) == ('failed', env.starter.NO_TOKEN)
    assert not start.dispatched


def test_a_sign_in_session_without_a_token_fails_the_run_with_no_token(env, start):
    start.token = None
    run_id = start.run(start.schedule())
    assert env.run(run_id) == ('failed', env.starter.NO_TOKEN)


def test_without_governance_the_token_is_asked_as_the_owner_without_a_run_identity(env, start):
    start.run(start.schedule())
    call = start.mocks['get_predict_token_and_session'].call_args
    assert call.args == (PROJECT_ID, OWNER_ID)
    assert (call.kwargs['sid'], call.kwargs['run_identity'], call.kwargs['mandate_run']) == (None, False, False)


def test_a_governed_cron_run_with_a_mandate_asks_for_a_mandate_run_token(env, start):
    start.governed, start.mandate = True, types.SimpleNamespace(id=11)
    start.run(start.schedule())
    kwargs = start.mocks['get_predict_token_and_session'].call_args.kwargs
    assert (kwargs['run_identity'], kwargs['mandate_run']) == (True, True)
    assert start.mocks['start_index_task'].call_args.kwargs['admission'] == f'request:index:{TOOLKIT_ID}:docs:{OWNER_ID}'


def test_the_mandate_lookup_and_the_trigger_reference_use_one_schedule_key(env, start):
    start.governed, start.mandate = True, types.SimpleNamespace(id=11)
    start.run(start.schedule())
    covering = start.trigger_principal.covering_toolkit_mandate.call_args.args
    reference = start.mandate_admission.index_schedule_trigger_ref.call_args.args
    assert covering[2] == reference[2] == str(OWNER_ID)
    assert covering[3] == OWNER_ID


def test_a_run_now_uses_the_request_of_its_endpoint_and_reads_no_mandate(env, start):
    start.governed = True
    start.run(start.schedule(), trigger='manual', owner_request='request:now')
    assert start.mocks['start_index_task'].call_args.kwargs['admission'] == 'request:now'
    kwargs = start.mocks['get_predict_token_and_session'].call_args.kwargs
    assert (kwargs['run_identity'], kwargs['mandate_run']) == (True, False)
    assert 'mandates' not in start.order


def test_a_test_run_uses_the_request_of_its_endpoint_and_reads_no_mandate(env, start):
    start.governed = True
    start.test_run(owner_request='request:test')
    assert start.mocks['start_index_task'].call_args.kwargs['admission'] == 'request:test'
    assert 'mandates' not in start.order


def test_a_cron_run_without_a_mandate_is_refused_before_the_dispatch(env, start):
    start.governed = True
    schedule_id = start.schedule()
    run_id = start.run(schedule_id)
    assert env.run(run_id) == ('failed', env.runs.run_failure_reason('Index run refused (AUTHORIZATION_REQUIRED)'))
    assert not start.dispatched
    assert start.order[-2:] == ['record_refusal', 'count_refusal']
    assert start.enabled(schedule_id) is True
    assert [event for event, _, _ in env.notices] == ['schedule_run_failed']


def test_a_refusal_asks_the_owner_to_save_again_when_the_mandate_is_not_confirmed(env, start):
    start.governed, start.resave = True, True
    run_id = start.run(start.schedule())
    assert 'resave' in env.run(run_id)[1]


def test_a_mandate_that_enforce_refuses_ends_the_governed_run(env, start):
    start.governed, start.mandate, start.audit_code = True, types.SimpleNamespace(id=11), 'MANDATE_REVOKED'
    run_id = start.run(start.schedule())
    assert env.run(run_id)[0] == 'failed'
    assert not start.dispatched


def test_audit_only_lets_a_refused_cron_run_go_on_and_records_the_would_deny_after_the_dispatch(env, start):
    start.auditing = True
    run_id = start.run(start.schedule())
    assert start.order.index('dispatch') < start.order.index('record_would_deny')
    assert start.mocks['start_index_task'].call_args.kwargs['admission'] is None
    assert env.run(run_id)[0] == 'running'
    assert 'count_refusal' not in start.order


def test_audit_only_records_nothing_when_the_mandate_covers_the_run(env, start):
    start.auditing, start.mandate = True, types.SimpleNamespace(id=11)
    start.run(start.schedule())
    assert 'record_would_deny' not in start.order


def test_a_failed_mandate_read_fails_the_run_with_the_fixed_text(env, start):
    start.governed, start.mandate_error = True, RuntimeError('database password leaked here')
    run_id = start.run(start.schedule())
    assert env.run(run_id) == ('failed', env.starter.START_FAILED)
    assert not start.dispatched


@pytest.mark.parametrize('free_form, template, marked', [(False, False, False), (True, False, True), (False, True, True)])
def test_a_governed_run_marks_a_free_form_secret_in_its_payload(env, start, free_form, template, marked):
    start.governed, start.mandate = True, types.SimpleNamespace(id=11)
    start.free_form, start.template = free_form, template
    start.run(start.schedule())
    assert start.data['free_form_secret'] is marked


def test_an_ungoverned_run_does_not_look_for_a_free_form_secret(env, start):
    start.run(start.schedule())
    assert 'free_form_secret' not in start.data


def test_a_live_index_run_ends_the_cron_run_as_skipped_without_a_notice(env, start):
    start.meta = {**start.meta, 'state': 'IN_PROGRESS'}
    start.heartbeat = 1234.0
    start.secrets = {'task_disconnected_timeout_sec': 60}
    schedule_id = start.schedule()
    run_id = start.run(schedule_id)
    assert env.run(run_id) == ('skipped', env.starter.INDEX_RUN_ACTIVE)
    assert not start.dispatched
    assert env.notices == []
    assert start.enabled(schedule_id) is True
    call = start.mocks['resolve_index_staleness'].call_args
    assert call.args == ('in_progress', 100.0, 60, 1234.0)  # the vault timeout and the run heartbeat; no horizon


def test_the_disconnect_timeout_defaults_to_the_platform_one(env, start):
    start.meta = {**start.meta, 'state': 'in_progress'}
    start.run(start.schedule())
    assert start.mocks['resolve_index_staleness'].call_args.args[2] == 7200


def test_a_stale_index_run_is_stopped_and_replaced(env, start):
    start.meta = {**start.meta, 'state': 'in_progress'}
    start.stale = True
    run_id = start.run(start.schedule())
    assert start.stopped == [OLD_TASK]
    assert start.order.index('dispatch') > 0
    assert env.run(run_id)[0] == 'running'


def test_a_stale_index_run_is_replaced_even_when_the_stop_fails(env, start):
    start.meta = {**start.meta, 'state': 'in_progress'}
    start.stale, start.stop_error = True, RuntimeError('task node down')
    start.run(start.schedule())
    assert start.dispatched


def test_a_stale_row_without_a_task_id_is_replaced_without_a_stop(env, start):
    start.meta = {**start.meta, 'state': 'in_progress', 'task_id': None}
    start.stale = True
    start.run(start.schedule())
    assert start.stopped == []
    assert start.dispatched


FAILURES = [
    ('index row missing', lambda s: setattr(s, 'index_missing', True), 'failed', 'NOT_FOUND'),
    ('index unreadable', lambda s: setattr(s, 'read_error', ConnectionError('down')), 'failed', 'INDEX_UNAVAILABLE'),
    ('no saved settings', lambda s: setattr(s, 'meta', {**s.meta, 'index_configuration': {}}), 'failed',
     'INDEX_NOT_SCHEDULABLE'),
    ('toolkit settings error', lambda s: setattr(s, 'config', {'error': 'No such toolkit'}), 'failed',
     'TOOLKIT_CONFIG_FAILED'),
    ('toolkit settings lookup', lambda s: setattr(s, 'config_error', LookupError('gone')), 'failed',
     'TOOLKIT_CONFIG_FAILED'),
    ('no connection string', lambda s: setattr(s, 'config', {**s.config, 'settings': {}}), 'failed',
     'CREDENTIAL_MISSING'),
    ('no llm', lambda s: setattr(s, 'llm', (None, 'secret traceback text', False)), 'failed', 'LLM_UNAVAILABLE'),
    ('index run in progress', lambda s: setattr(
        s, 'dispatch_error', sys.modules[f'{PACKAGE}.utils.application_tools'].IndexRunInProgressError('docs')),
     'skipped', 'INDEX_RUN_ACTIVE'),
    ('pool saturated', lambda s: setattr(
        s, 'dispatch_error', sys.modules[f'{PACKAGE}.utils.exceptions'].PoolSaturationError('index')), 'failed',
     'POOL_SATURATED'),
    ('unexpected error', lambda s: setattr(s, 'dispatch_error', RuntimeError('boom at /srv/app.py')), 'failed',
     'START_FAILED'),
]


@pytest.mark.parametrize('setup, status, reason_name', [case[1:] for case in FAILURES], ids=[c[0] for c in FAILURES])
def test_every_step_that_cannot_go_on_ends_the_run_with_a_fixed_text_and_keeps_the_schedule_on(
        env, start, setup, status, reason_name):
    setup(start)
    schedule_id = start.schedule()
    run_id = start.run(schedule_id)
    assert env.run(run_id) == (status, getattr(env.starter, reason_name))
    assert start.enabled(schedule_id) is True
    assert env.notices == []


def test_an_admission_refusal_at_the_dispatch_ends_the_cron_run_and_counts_it(env, start):
    start.governed, start.mandate = True, types.SimpleNamespace(id=11)
    start.dispatch_error = sys.modules[f'{PACKAGE}.utils.exceptions'].AdmissionDenied('DENIED_ENTRY_POINT', 'fixed text')
    run_id = start.run(start.schedule())
    assert env.run(run_id) == ('failed', env.runs.run_failure_reason('Index run refused (DENIED_ENTRY_POINT)'))
    assert 'count_refusal' in start.order


def test_an_admission_refusal_of_a_run_now_is_not_counted_against_the_schedule(env, start):
    start.governed = True
    start.dispatch_error = sys.modules[f'{PACKAGE}.utils.exceptions'].AdmissionDenied('DENIED_ENTRY_POINT', 'fixed text')
    run_id = start.run(start.schedule(), trigger='manual', owner_request='request:now')
    assert env.run(run_id)[0] == 'failed'
    assert 'count_refusal' not in start.order


def test_an_unexpected_error_text_never_reaches_the_run(env, start):
    start.dispatch_error = RuntimeError('password=hunter2 at /srv/app/index.py')
    run_id = start.run(start.schedule())
    assert 'hunter2' not in env.run(run_id)[1]


def test_an_index_that_ended_before_the_task_id_was_written_closes_the_run_once(env, start):
    start.finish = {'state': 'completed'}
    run_id = start.run(start.schedule())
    assert env.run(run_id)[0] == 'success'
    assert [event for event, _, _ in env.notices] == ['schedule_run_succeeded']


def test_an_index_that_failed_before_the_task_id_was_written_fails_the_run_with_its_error(env, start):
    start.finish = {'state': 'failed', 'error': 'The vector store refused the write'}
    run_id = start.run(start.schedule())
    status, reason = env.run(run_id)
    assert status == 'failed'
    assert 'The vector store refused the write' in reason


def test_a_row_still_in_progress_after_the_dispatch_leaves_the_run_running(env, start):
    start.finish = {'state': 'in_progress'}
    run_id = start.run(start.schedule())
    assert env.run(run_id)[0] == 'running'
