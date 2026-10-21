"""Issue #7064 - a scheduled pipeline whose dispatch fails runs once per cron slot, not every tick.

Before the fix, `_process_pipeline_version` returned from its `except` before writing
`last_run`. The schedule therefore stayed due, so every scheduler tick created another
"Scheduled run" conversation and failed the same way. On dev one pipeline (cron
`0 5 * * 1-5`, failing with `llm_settings with model_name is required`) did this from
2026-10-06 onward at about 56 conversations per hour. Each of those conversations was also
left streaming forever.

#7064 moves agent expansion before dispatch, so more failures (missing credential, expansion
error, prefetch bound) surface at this point. The fix:
  * `last_run` is written whether dispatch succeeded or failed;
  * the run's placeholder message is closed with the error through `chat_message_stream_end`,
    the same event a finished run fires, so History shows why it failed;
  * the webhook path closes its placeholder the same way.

`rpc/pipeline_scheduling.py` and `utils/pipeline_execution.py` are loaded from source. Only
the leaves (DB session, cron check, predict RPC, event manager) are faked, and the test
counts what happens.

Run via:
    python tests/run_tests.py integration/test_7064_failed_dispatch_consumes_slot.py -v
"""
import importlib.util
import pathlib
import sys
import types

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]


class FakeSession:
    def __init__(self):
        self.commits = 0
        self.rollbacks = 0

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


class FakeVersion:
    def __init__(self, cron='0 5 * * 1-5'):
        self.id = 2711
        self.application_id = 15
        self.pipeline_settings = {'trigger': {
            'type': 'schedule', 'cron': cron, 'timezone': 'UTC',
            'last_run': '2026-10-06T07:36:22+00:00', 'created_by': 3,
        }}


def _module(name, **attrs):
    mod = types.ModuleType(name)
    mod.__dict__.update(attrs)
    return mod


@pytest.fixture
def env(monkeypatch):
    state = types.SimpleNamespace(
        dispatch_error=None, dispatches=[], events=[], conversations=[],
        logged=[], due=True,
    )
    for name in ('plugins', 'plugins.elitea_core', 'plugins.elitea_core.models',
                 'plugins.elitea_core.models.pd', 'plugins.elitea_core.models.enums',
                 'plugins.elitea_core.models.message_items', 'plugins.elitea_core.utils',
                 'plugins.elitea_core.rpc'):
        pkg = _module(name)
        pkg.__path__ = []
        monkeypatch.setitem(sys.modules, name, pkg)

    log = types.SimpleNamespace(
        info=lambda *a, **k: None, debug=lambda *a, **k: None, warning=lambda *a, **k: None,
        exception=lambda *a, **k: state.logged.append(a), error=lambda *a, **k: None,
    )
    monkeypatch.setitem(sys.modules, 'pylon', _module('pylon'))
    monkeypatch.setitem(sys.modules, 'pylon.core', _module('pylon.core'))
    monkeypatch.setitem(sys.modules, 'pylon.core.tools', _module(
        'pylon.core.tools', log=log,
        web=types.SimpleNamespace(rpc=lambda *a, **k: (lambda f: f), method=lambda *a, **k: (lambda f: f)),
    ))

    def fire_event(name, payload):
        state.events.append((name, payload))

    class SessionCtx:
        def __enter__(self):
            return types.SimpleNamespace(query=lambda *a: types.SimpleNamespace(get=lambda _id: None))

        def __exit__(self, *exc):
            return False

    def predict_sio(**kwargs):
        state.dispatches.append(kwargs)
        if state.dispatch_error:
            raise state.dispatch_error
        return {'task_id': 'task-1'}

    rpc = types.SimpleNamespace(applications_predict_sio=predict_sio)
    tools = _module(
        'tools',
        db=types.SimpleNamespace(get_session=lambda project_id: SessionCtx()),
        rpc_tools=types.SimpleNamespace(RpcMixin=lambda: types.SimpleNamespace(
            rpc=types.SimpleNamespace(timeout=lambda seconds: rpc))),
        this=types.SimpleNamespace(module=types.SimpleNamespace(
            context=types.SimpleNamespace(event_manager=types.SimpleNamespace(fire_event=fire_event)))),
    )
    monkeypatch.setitem(sys.modules, 'tools', tools)

    for name, attrs in {
        'plugins.elitea_core.models.all': {'Application': object, 'ApplicationVersion': object},
        'plugins.elitea_core.models.conversation': {'Conversation': object},
        'plugins.elitea_core.models.enums.all': {
            'ParticipantTypes': types.SimpleNamespace(application=types.SimpleNamespace(value='application'),
                                                      user='user'),
            'AgentTypes': types.SimpleNamespace(pipeline=types.SimpleNamespace(value='pipeline')),
        },
        'plugins.elitea_core.models.message_group': {'ConversationMessageGroup': object},
        'plugins.elitea_core.models.message_items.text': {'TextMessageItem': object},
        'plugins.elitea_core.models.pd.participant': {
            'ParticipantEntityApplication': object, 'ParticipantEntityUser': object},
        'plugins.elitea_core.utils.participant_utils': {'get_or_create_one': None},
        'plugins.elitea_core.utils.sio_utils': {
            'SioEvents': types.SimpleNamespace(chat_predict=types.SimpleNamespace(value='chat_predict'))},
        'plugins.elitea_core.utils.cron_utils': {'is_cron_due': lambda cron, last, tz: state.due},
        'plugins.elitea_core.utils.maintenance_gate': {'is_maintenance_active': lambda: False},
        # governance is off here: no mandate, no ticket, no sweep (the governed paths are test_6947_mandate_triggers)
        'plugins.elitea_core.utils.governance': {
            'is_governed': lambda module: False, 'governance_lifecycle_active': lambda module: False},
        'plugins.elitea_core.utils.admission': {'enforce_run_deadlines': lambda module: 0},
        'plugins.elitea_core.utils.mandate_admission': {
            'prepare_trigger_run': lambda **kwargs: (kwargs['default_user_id'], None)},
        'plugins.elitea_core.utils.utils': {
            'make_yield_to_hub': lambda runtime: (lambda: None), 'end_ambient_transaction': lambda: None},
    }.items():
        monkeypatch.setitem(sys.modules, name, _module(name, **attrs))

    schedule_model = type('PipelineTriggerSchedule', (), {
        'parse_obj': classmethod(lambda cls, trigger: types.SimpleNamespace(**trigger)),
    })
    monkeypatch.setitem(sys.modules, 'plugins.elitea_core.models.pd.pipeline_trigger', _module(
        'plugins.elitea_core.models.pd.pipeline_trigger',
        TriggerType=types.SimpleNamespace(schedule=types.SimpleNamespace(value='schedule'),
                                          webhook=types.SimpleNamespace(value='webhook')),
        PipelineTriggerSchedule=schedule_model,
    ))
    monkeypatch.setitem(sys.modules, 'sqlalchemy.orm.attributes', _module(
        'sqlalchemy.orm.attributes', flag_modified=lambda obj, key: None))

    execution = _load(monkeypatch, 'plugins.elitea_core.utils.pipeline_execution', 'utils/pipeline_execution.py')

    def create_conversation(**kwargs):
        state.conversations.append(kwargs)
        return 'conv-uuid', 55, f'response-{len(state.conversations)}'

    monkeypatch.setattr(execution, 'create_trigger_run_conversation', create_conversation)
    scheduling = _load(monkeypatch, 'plugins.elitea_core.rpc.pipeline_scheduling', 'rpc/pipeline_scheduling.py')
    monkeypatch.setattr(scheduling, 'create_trigger_run_conversation', create_conversation)
    state.scheduling = scheduling
    state.execution = execution
    return state


def _load(monkeypatch, name, relpath):
    spec = importlib.util.spec_from_file_location(name, PLUGIN_ROOT / relpath)
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, name, module)
    spec.loader.exec_module(module)
    return module


def _tick(env, version, session):
    env.scheduling._process_pipeline_version(121, version, session)


def test_a_failed_dispatch_still_moves_last_run(env):
    env.dispatch_error = RuntimeError('llm_settings with model_name is required')
    version, session = FakeVersion(), FakeSession()

    _tick(env, version, session)

    assert version.pipeline_settings['trigger']['last_run'] != '2026-10-06T07:36:22+00:00'
    assert session.commits == 1
    assert env.logged, 'the dispatch error is still logged'


def test_a_failed_slot_is_not_retried_on_the_next_tick(env, monkeypatch):
    # The dev loop: due on every tick because last_run never moved
    stale = '2026-10-06T07:36:22+00:00'
    monkeypatch.setattr(env.scheduling, 'is_cron_due', lambda cron, last_run, tz: last_run == stale)
    env.dispatch_error = RuntimeError('llm_settings with model_name is required')
    version, session = FakeVersion(), FakeSession()

    for _ in range(5):
        _tick(env, version, session)

    assert len(env.conversations) == 1
    assert len(env.dispatches) == 1


def test_the_failed_run_conversation_is_closed_with_the_error(env):
    env.dispatch_error = RuntimeError("Configuration 'tavily_live' not found")
    version, session = FakeVersion(), FakeSession()

    _tick(env, version, session)

    assert len(env.events) == 1
    name, payload = env.events[0]
    assert name == 'chat_message_stream_end'
    assert payload['message_id'] == 'response-1'
    assert "tavily_live" in payload['content']
    meta = payload['response_metadata']
    assert meta['is_error'] is True
    assert meta['chat_project_id'] == 121 and meta['project_id'] == 121


def test_a_successful_dispatch_is_unchanged(env):
    version, session = FakeVersion(), FakeSession()

    _tick(env, version, session)

    assert len(env.dispatches) == 1
    assert env.dispatches[0]['is_system_user'] is True
    assert env.events == []
    assert session.commits == 1


def test_a_schedule_that_is_not_due_does_nothing(env):
    env.due = False
    version, session = FakeVersion(), FakeSession()

    _tick(env, version, session)

    assert env.conversations == [] and env.dispatches == [] and session.commits == 0


def test_recording_the_failure_cannot_break_the_tick(env, monkeypatch):
    env.dispatch_error = RuntimeError('boom')

    def broken_fire_event(name, payload):
        raise RuntimeError('event manager down')

    monkeypatch.setattr(env.execution.this.module.context.event_manager, 'fire_event', broken_fire_event)
    version, session = FakeVersion(), FakeSession()

    _tick(env, version, session)

    assert session.commits == 1


def test_webhook_dispatch_failure_closes_its_run(env, monkeypatch):
    for name, attrs in {
        'plugins.elitea_core.utils.exceptions': {'PoolSaturationError': type('PoolSaturationError', (Exception,), {})},
        'plugins.elitea_core.utils.input_guardrail': {'redacted_input': lambda text: text},
    }.items():
        monkeypatch.setitem(sys.modules, name, _module(name, **attrs))
    webhook = _load(monkeypatch, 'plugins.elitea_core.rpc.pipeline_webhook', 'rpc/pipeline_webhook.py')
    monkeypatch.setattr(webhook, 'create_trigger_run_conversation',
                        lambda **kwargs: ('conv-uuid', 55, 'webhook-response'))
    env.dispatch_error = RuntimeError('Sub-agent 7/70 could not be loaded')

    with pytest.raises(RuntimeError):
        webhook.execute_pipeline_webhook(121, FakeVersion(), 3, 'github', {'action': 'opened'})

    assert [(n, p['message_id']) for n, p in env.events] == [('chat_message_stream_end', 'webhook-response')]
