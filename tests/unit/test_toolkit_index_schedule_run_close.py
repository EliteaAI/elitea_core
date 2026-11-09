"""The end of a Toolkit Index schedule run: the status event closes the run that started the index task (SQLite).

The real schedule models, run writers and index starter run on one SQLite database. The modules around the starter
(index worker access, admission, tokens) are stand-ins: nothing here dispatches a task.
"""
import pathlib
import sys
import types
from datetime import UTC, datetime, timedelta
from enum import StrEnum

import pytest
from fixtures import schedule_sqlite as sqlite_db
from fixtures.helpers import load_module_with_stubs

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
PACKAGE = 'plugins.elitea_core'
PROJECT_ID = 7
OWNER_ID = 42
TOOLKIT_ID = 3
TASK_ID = 'task-1'
STALE_MINUTES = 30


class Env:
    """The loaded run writers and index starter, one SQLite database and the notices the writers sent."""

    def seed_schedule(self, index_name='docs', toolkit_id=TOOLKIT_ID):
        return sqlite_db.seed_schedule(
            self.session_factory, self.EntitySchedule, OWNER_ID,
            entity_type='toolkit_index', application_id=None, version_id=None, prompt=None,
            toolkit_id=toolkit_id, index_name=index_name)

    def add_run(self, schedule_id, task_id=TASK_ID, status='running', trigger='cron', form=None, **columns):
        """A run with an `index_task` event of `task_id` (none when `task_id` is None)."""
        events = [] if task_id is None else [{'kind': 'index_task', 'text': 'Index task started', 'task_id': task_id}]
        with self.session_factory() as session:
            run = self.EntityScheduleRun(
                schedule_id=schedule_id, trigger=trigger, status=status, events=events, requested_by=OWNER_ID,
                **columns)
            session.add(run)
            session.commit()
            run_id = run.id
            if form is not None:
                session.execute(self.EntityScheduleRun.__table__.update()
                                .where(self.EntityScheduleRun.id == run_id).values(form=form))
                session.commit()
            return run_id

    def run(self, run_id):
        with self.session_factory() as session:
            row = session.get(self.EntityScheduleRun, run_id)
            return row.status, row.reason

    def close(self, state, task_id=TASK_ID, error=None, index_name='docs', toolkit_id=TOOLKIT_ID):
        return self.starter.close_index_schedule_run(PROJECT_ID, toolkit_id, index_name, task_id, state, error)


class IndexState(StrEnum):
    """The shape of the platform's `IndexDataStatus` event state."""
    completed = 'completed'


def _restore(original):
    for key in set(sys.modules) - set(original):
        sys.modules.pop(key, None)
    sys.modules.update(original)


@pytest.fixture
def env():
    original = sys.modules.copy()
    for name, module in sqlite_db.REAL_SQLALCHEMY.items():
        sys.modules[name] = module
    env = Env()
    for name in ('plugins', PACKAGE, f'{PACKAGE}.utils', f'{PACKAGE}.models', f'{PACKAGE}.models.enums',
                 f'{PACKAGE}.models.pd', 'pylon', 'pylon.core'):
        sqlite_db.package(name)
    Base, Application, ApplicationVersion = sqlite_db.install_parent_models(PACKAGE)
    engine = sqlite_db.sqlite_engine()
    env.session_factory = lambda: sqlite_db.orm.Session(engine, expire_on_commit=False)
    tools = sqlite_db.LenientModule('tools')
    tools.db = types.SimpleNamespace(get_session=lambda project_id=None: env.session_factory(), Base=Base)
    tools.this = types.SimpleNamespace(module=types.SimpleNamespace(descriptor=types.SimpleNamespace(config={})))
    sys.modules['tools'] = tools

    env.notices, env.chat_events = [], []
    notifications = types.ModuleType(f'{PACKAGE}.utils.schedule_notifications')
    notifications.notify_schedule_event = lambda project_id, user_id, event, schedule, run: env.notices.append(
        (event, run.id, run.status))
    notifications.emit_schedule_run_finished = lambda *args: env.chat_events.append(args)
    sys.modules[notifications.__name__] = notifications
    # Modules the starter imports and the close never calls
    for name in ('application_tools', 'budget_door', 'governance', 'index_scheduling', 'predict_utils',
                 'schedule_prerun', 'schedule_run_starter'):
        sys.modules[f'{PACKAGE}.utils.{name}'] = sqlite_db.LenientModule(f'{PACKAGE}.utils.{name}')
    # Names the starter binds at import and compares or formats; the rest of these modules is a stand-in per test
    stand_ins = {
        'schedule_run_starter': {'NOT_FOUND': 'schedule or version not found', 'START_FAILED': 'the run could not start',
                                 'BUDGET_CLOSED': 'the {} budget is used up'},
        'schedule_prerun': {'NO_TOKEN': 'no token for the owner'},
        'predict_utils': {'PredictPayloadError': type('PredictPayloadError', (Exception,), {})},
        'application_tools': {
            'DEFAULT_TASK_DISCONNECTED_TIMEOUT_SEC': 7200,
            'IndexRunInProgressError': type('IndexRunInProgressError', (Exception,), {})},
    }
    for name, values in stand_ins.items():
        vars(sys.modules[f'{PACKAGE}.utils.{name}']).update(values)
    sys.modules[f'{PACKAGE}.models.indexer'] = sqlite_db.LenientModule(f'{PACKAGE}.models.indexer')
    elitea_tools = types.ModuleType(f'{PACKAGE}.models.elitea_tools')
    elitea_tools.EliteATool = sys.modules[f'{PACKAGE}.models.all'].EliteATool
    sys.modules[elitea_tools.__name__] = elitea_tools

    def load(relative_path, name):
        return load_module_with_stubs(PLUGIN_ROOT / relative_path, f'{PACKAGE}.{name}')

    env.EntitySchedule = sqlite_db.create_schedule_table(load, Base, engine)
    entity_schedule = sys.modules[f'{PACKAGE}.models.entity_schedule']
    env.EntityScheduleRun = entity_schedule.EntityScheduleRun
    load('utils/exceptions.py', 'utils.exceptions')
    load('utils/schedule_timing.py', 'utils.schedule_timing')
    load('utils/error_text.py', 'utils.error_text')
    load('utils/schedule_claim.py', 'utils.schedule_claim')
    env.runs = load('utils/schedule_runs.py', 'utils.schedule_runs')
    env.starter = load('utils/schedule_index_starter.py', 'utils.schedule_index_starter')
    sqlite_db.add_agent_and_pipeline(env.session_factory, Application, ApplicationVersion)
    with env.session_factory() as session:
        session.add_all([sys.modules[f'{PACKAGE}.models.all'].EliteATool(id=TOOLKIT_ID, name='Docs', type='artifact'),
                         sys.modules[f'{PACKAGE}.models.all'].EliteATool(id=4, name='Other', type='artifact')])
        session.commit()
    yield env
    _restore(original)


@pytest.fixture
def stream(env):
    """The stream method class with a stand-in `close_index_schedule_run` that records its calls."""
    for name in ('sio', 'sio.all', 'models.pd.index', 'utils.application_tools', 'utils.parallel_hitl'):
        sys.modules[f'{PACKAGE}.{name}'] = sqlite_db.LenientModule(f'{PACKAGE}.{name}')
    sqlite_db.package(f'{PACKAGE}.methods')
    sys.modules['pylon.core.tools'] = types.SimpleNamespace(
        log=sys.modules['pylon.core.tools'].log,
        web=types.SimpleNamespace(method=lambda *a, **k: (lambda f: f)))
    calls = []
    starter = sys.modules[f'{PACKAGE}.utils.schedule_index_starter']

    def close_index_schedule_run(project_id, toolkit_id, index_name, task_id, state, error=None):
        calls.append((project_id, toolkit_id, index_name, task_id, state, error))
        if isinstance(state, str) and state == 'raises':
            raise RuntimeError('boom')
        return False

    starter.close_index_schedule_run = close_index_schedule_run
    module = load_module_with_stubs(PLUGIN_ROOT / 'methods/stream.py', f'{PACKAGE}.methods.stream')
    handled = []
    fake = types.SimpleNamespace(
        context=None, notify_index_data_status=lambda metadata: handled.append(('notified', metadata)),
        _maintain_active_index_registry=lambda metadata: handled.append(('registry', metadata)))
    fake.calls, fake.handled = calls, handled
    fake.process = lambda metadata: module.Method.process_index_data_status_event(fake, metadata)
    return fake


@pytest.mark.parametrize('state', ['completed', 'scheduled_reindex'])
def test_a_completed_index_task_ends_its_run_as_success(env, state):
    run_id = env.add_run(env.seed_schedule())
    assert env.close(state) is True
    assert env.run(run_id) == ('success', None)


def test_a_partly_indexed_task_ends_its_run_as_success_with_the_reason(env):
    run_id = env.add_run(env.seed_schedule())
    env.close('partly_indexed')
    assert env.run(run_id) == ('success', 'partly indexed')


def test_a_failed_task_ends_its_run_with_the_readable_error(env):
    run_id = env.add_run(env.seed_schedule())
    env.close('failed', error='The vector store refused the write')
    status, reason = env.run(run_id)
    assert status == 'failed'
    assert 'The vector store refused the write' in reason


def test_a_failed_task_without_an_error_ends_its_run_with_the_plain_failure_text(env):
    run_id = env.add_run(env.seed_schedule())
    env.close('failed')
    assert env.run(run_id) == ('failed', env.runs.FAILED_REASON)


def test_a_cancelled_task_ends_its_run_as_failed_with_a_fallback_text(env):
    run_id = env.add_run(env.seed_schedule())
    env.close('cancelled')
    status, reason = env.run(run_id)
    assert status == 'failed'
    assert env.starter.CANCELLED in reason


@pytest.mark.parametrize('state', ['created', 'in_progress', None, 'unknown'])
def test_a_state_that_is_not_final_closes_no_run(env, state):
    run_id = env.add_run(env.seed_schedule())
    assert env.close(state) is False
    assert env.run(run_id) == ('running', None)


def test_a_state_enum_value_closes_the_run(env):
    run_id = env.add_run(env.seed_schedule())
    env.close(IndexState.completed)
    assert env.run(run_id)[0] == 'success'


def test_an_event_of_another_task_leaves_the_run_open(env):
    run_id = env.add_run(env.seed_schedule())
    assert env.close('completed', task_id='task-2') is False
    assert env.close('completed', task_id=None) is False
    assert env.run(run_id)[0] == 'running'


def test_a_run_without_an_index_task_event_stays_open(env):
    run_id = env.add_run(env.seed_schedule(), task_id=None)
    assert env.close('completed') is False
    assert env.run(run_id)[0] == 'running'


def test_the_run_of_another_index_or_toolkit_stays_open(env):
    docs = env.add_run(env.seed_schedule('docs'))
    other_index = env.add_run(env.seed_schedule('wiki'), task_id='task-2')
    other_toolkit = env.add_run(env.seed_schedule('docs', toolkit_id=4), task_id='task-3')
    env.close('completed')
    assert [env.run(docs)[0], env.run(other_index)[0], env.run(other_toolkit)[0]] == ['success', 'running', 'running']


def test_an_agent_run_with_the_same_task_id_is_not_closed(env):
    agent_schedule = sqlite_db.seed_schedule(env.session_factory, env.EntitySchedule, OWNER_ID)
    agent_run = env.add_run(agent_schedule)
    env.close('completed')
    assert env.run(agent_run)[0] == 'running'


def test_a_second_close_sends_no_second_notice(env):
    env.add_run(env.seed_schedule())
    assert env.close('completed') is True
    assert env.close('completed') is False
    assert [event for event, _, _ in env.notices] == ['schedule_run_succeeded']


def test_a_failed_run_sends_the_failed_notice_and_no_chat_event(env):
    run_id = env.add_run(env.seed_schedule())
    env.close('failed', error='The vector store refused the write')
    assert env.notices == [('schedule_run_failed', run_id, 'failed')]
    assert env.chat_events == []


def test_the_schedule_copies_the_final_status(env):
    schedule_id = env.seed_schedule()
    env.add_run(schedule_id)
    env.close('completed')
    with env.session_factory() as session:
        assert session.get(env.EntitySchedule, schedule_id).last_run_status == 'success'


def test_a_test_run_closes_on_its_form_and_sends_no_notice(env):
    form = {'toolkit_id': TOOLKIT_ID, 'index_name': 'docs', 'name': 'Try it'}
    run_id = env.add_run(None, trigger='test', form=form)
    env.close('completed')
    assert env.run(run_id)[0] == 'success'
    assert env.notices == []


def test_a_test_run_of_another_index_stays_open(env):
    run_id = env.add_run(None, trigger='test', form={'toolkit_id': TOOLKIT_ID, 'index_name': 'wiki'})
    env.close('completed')
    assert env.run(run_id)[0] == 'running'


def test_a_terminal_event_before_the_task_id_is_written_is_closed_by_the_reread(env):
    run_id = env.add_run(env.seed_schedule(), task_id=None)
    index_row = types.SimpleNamespace(meta={'task_id': TASK_ID, 'state': 'completed'})
    env.starter._read_index = lambda connection_string, toolkit_id, index_name, with_heartbeat=False: index_row
    with env.session_factory() as session:
        run = session.get(env.EntityScheduleRun, run_id)
        run.events = [{'kind': 'index_task', 'text': 'Index task started', 'task_id': TASK_ID}]
        session.commit()
    target = types.SimpleNamespace(toolkit_id=TOOLKIT_ID, index_name='docs')
    env.starter._close_if_ended(PROJECT_ID, 'postgresql://x', target, TASK_ID)
    assert env.run(run_id)[0] == 'success'
    assert len(env.notices) == 1


def test_the_reread_of_another_task_closes_nothing(env):
    run_id = env.add_run(env.seed_schedule())
    index_row = types.SimpleNamespace(meta={'task_id': 'task-2', 'state': 'completed'})
    env.starter._read_index = lambda *args, **kwargs: index_row
    target = types.SimpleNamespace(toolkit_id=TOOLKIT_ID, index_name='docs')
    env.starter._close_if_ended(PROJECT_ID, 'postgresql://x', target, TASK_ID)
    assert env.run(run_id)[0] == 'running'


def test_a_reread_failure_leaves_the_run_for_the_stale_sweep(env):
    run_id = env.add_run(env.seed_schedule())

    def unreadable(*args, **kwargs):
        raise ConnectionError('vector store down')

    env.starter._read_index = unreadable
    target = types.SimpleNamespace(toolkit_id=TOOLKIT_ID, index_name='docs')
    env.starter._close_if_ended(PROJECT_ID, 'postgresql://x', target, TASK_ID)
    assert env.run(run_id)[0] == 'running'


def test_a_killed_task_leaves_the_run_running_until_the_stale_sweep_fails_it(env):
    schedule_id = env.seed_schedule()
    long_ago = datetime.now(UTC) - timedelta(minutes=STALE_MINUTES * 2)
    run_id = env.add_run(schedule_id, started_at=long_ago)
    assert env.run(run_id)[0] == 'running'
    assert env.runs.fail_stale_runs(PROJECT_ID, datetime.now(UTC), STALE_MINUTES) == 1
    assert env.run(run_id) == ('failed', env.runs.STALE_REASON)


def test_the_stale_limit_counts_from_the_run_start_not_from_the_index_heartbeat(env):
    long_ago = datetime.now(UTC) - timedelta(minutes=STALE_MINUTES * 2)
    run_id = env.add_run(env.seed_schedule(), started_at=long_ago)
    fresh = datetime.now(UTC).isoformat()
    with env.session_factory() as session:
        run = session.get(env.EntityScheduleRun, run_id)
        run.events = [*run.events, {'kind': 'heartbeat', 'text': 'alive', 'at': fresh}]
        session.commit()
    env.runs.fail_stale_runs(PROJECT_ID, datetime.now(UTC), STALE_MINUTES)
    assert env.run(run_id)[0] == 'failed'


def test_a_status_event_asks_for_the_close_with_its_fields(stream):
    stream.process({'state': 'completed', 'task_id': TASK_ID, 'index_name': 'docs', 'toolkit_id': TOOLKIT_ID,
                    'project_id': PROJECT_ID, 'error': None})
    assert stream.calls == [(PROJECT_ID, TOOLKIT_ID, 'docs', TASK_ID, 'completed', None)]
    assert [kind for kind, _ in stream.handled] == ['notified', 'registry']


def test_a_close_that_raises_does_not_stop_the_status_event(stream):
    stream.process({'state': 'raises', 'task_id': TASK_ID, 'index_name': 'docs', 'toolkit_id': TOOLKIT_ID,
                    'project_id': PROJECT_ID})
    assert [kind for kind, _ in stream.handled] == ['notified', 'registry']
