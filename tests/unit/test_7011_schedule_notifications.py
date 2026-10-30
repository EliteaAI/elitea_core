"""EL-7011 US-06 - schedule notices: the sender, the "Get notified" gate and the socket event.

Covers R1.1-R1.4, R3.1, R3.2 and R4.1-R4.3 of utils/schedule_notifications.py. The module is
loaded alone behind stand-ins for `tools` and the models; the enums and `sio_utils` have no
dependencies and load for real, so the new enum values are checked as well.
"""
import pathlib
import sys
import types

import pytest
from fixtures.helpers import load_module_with_stubs

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
PKG = 'plugins.elitea_core'

ALWAYS_SENT = [
    'schedule_run_failed',
    'schedule_approval_needed',
    'schedule_approval_expired',
    'schedule_expired',
    'schedule_disabled',
    'schedule_edited_by_admin',
    'schedule_owner_changed',
]
SCHEDULE = types.SimpleNamespace(id=7, name='Daily report')
RUN = types.SimpleNamespace(id=301, conversation_id=55, reason='credential not found')


class Recorder:
    """Everything the module under test sends out, plus the switches that make it fail."""

    def __init__(self):
        self.fired = []
        self.emitted = []
        self.logged = []
        self.preference = None  # the owner's EntitySchedulePreference row, None when absent
        self.fire_error = None
        self.emit_error = None
        self.gate_error = None


class FakeSession:
    def __init__(self, recorder):
        self._recorder = recorder

    def __enter__(self):
        if self._recorder.gate_error:
            raise self._recorder.gate_error
        return self

    def __exit__(self, *exc):
        return False

    def get(self, _model, _user_id):
        return self._recorder.preference


def _load_real(name, relative):
    return load_module_with_stubs(PLUGIN_ROOT / relative, f'{PKG}.{name}')


@pytest.fixture
def recorder():
    return Recorder()


@pytest.fixture
def notifications(isolated_sys_modules, recorder):
    def log_to_recorder(*args, **_kwargs):
        recorder.logged.append(args[0] % args[1:] if len(args) > 1 else str(args[0]))

    pylon_tools = types.ModuleType('pylon.core.tools')
    pylon_tools.log = types.SimpleNamespace(
        info=lambda *a, **k: None, debug=lambda *a, **k: None,
        warning=log_to_recorder, error=log_to_recorder, exception=log_to_recorder,
    )
    sys.modules.setdefault('pylon', types.ModuleType('pylon'))
    sys.modules.setdefault('pylon.core', types.ModuleType('pylon.core'))
    sys.modules['pylon.core.tools'] = pylon_tools

    def fire_event(name, payload):
        if recorder.fire_error:
            raise recorder.fire_error
        recorder.fired.append((name, payload))

    def emit(event, data, **kwargs):
        if recorder.emit_error:
            raise recorder.emit_error
        recorder.emitted.append((event, data, kwargs))

    tools_pkg = types.ModuleType('tools')
    tools_pkg.db = types.SimpleNamespace(get_session=lambda project_id: FakeSession(recorder))
    tools_pkg.rpc_tools = types.SimpleNamespace(EventManagerMixin=lambda: types.SimpleNamespace(
        event_manager=types.SimpleNamespace(fire_event=fire_event)))
    tools_pkg.this = types.SimpleNamespace(module=types.SimpleNamespace(
        context=types.SimpleNamespace(sio=types.SimpleNamespace(emit=emit))))
    sys.modules['tools'] = tools_pkg

    for name in (PKG, f'{PKG}.models', f'{PKG}.models.enums', f'{PKG}.utils'):
        sys.modules[name] = types.ModuleType(name)
        sys.modules[name].__path__ = []

    _load_real('models.enums.all', 'models/enums/all.py')
    _load_real('utils.sio_utils', 'utils/sio_utils.py')
    schedule_models = types.ModuleType(f'{PKG}.models.entity_schedule')
    schedule_models.EntitySchedulePreference = type('EntitySchedulePreference', (), {'user_id': 'user_id'})
    sys.modules[schedule_models.__name__] = schedule_models
    return _load_real('utils.schedule_notifications', 'utils/schedule_notifications.py')


def _send(notifications, event_type, **kwargs):
    return notifications.notify_schedule_event(12, 42, event_type, SCHEDULE, **kwargs)


def test_the_expired_notice_names_the_configured_window(notifications, recorder, monkeypatch):
    runs = types.ModuleType(f'{PKG}.utils.schedule_runs')
    runs.approval_wait_hours = lambda: 48
    monkeypatch.setitem(sys.modules, runs.__name__, runs)

    _send(notifications, 'schedule_approval_expired')

    assert 'was not approved in 48 hours' in recorder.fired[0][1]['meta']['message']


def test_the_expired_notice_falls_back_to_24_hours_when_the_setting_cannot_be_read(notifications, recorder, monkeypatch):
    runs = types.ModuleType(f'{PKG}.utils.schedule_runs')

    def broken():
        raise RuntimeError('config down')

    runs.approval_wait_hours = broken
    monkeypatch.setitem(sys.modules, runs.__name__, runs)

    _send(notifications, 'schedule_approval_expired')

    assert 'was not approved in 24 hours' in recorder.fired[0][1]['meta']['message']


class TestEL7011EventTypes:
    def test_r1_1_all_eight_event_types_exist(self, notifications):
        enums = sys.modules[f'{PKG}.models.enums.all']
        values = {item.value for item in enums.NotificationEventTypes}
        assert set(ALWAYS_SENT) | {'schedule_run_succeeded'} <= values

    def test_r4_1_the_socket_event_exists(self, notifications):
        assert sys.modules[f'{PKG}.utils.sio_utils'].SioEvents.schedule_run_finished == 'schedule_run_finished'


class TestEL7011R1Sender:
    def test_r1_1_fires_notifications_stream_for_the_owner(self, notifications, recorder):
        assert _send(notifications, 'schedule_run_failed', run=RUN) is True

        [(name, payload)] = recorder.fired
        assert name == 'notifications_stream'
        assert payload['project_id'] == 12
        assert payload['user_id'] == 42
        assert payload['event_type'] == 'schedule_run_failed'

    def test_r1_2_meta_carries_message_and_ids(self, notifications, recorder):
        _send(notifications, 'schedule_run_failed', run=RUN)

        meta = recorder.fired[0][1]['meta']
        assert meta['schedule_id'] == 7
        assert meta['schedule_name'] == 'Daily report'
        assert meta['run_id'] == 301
        assert meta['conversation_id'] == 55
        assert meta['message'] == 'Schedule "[Daily report]()" failed: credential not found'

    @pytest.mark.parametrize('extra, message', [
        (None, 'Schedule "[Daily report]()" was switched off'),
        ({'reason': 'the agent was deleted'},
         'Schedule "[Daily report]()" was switched off: the agent was deleted'),
    ])
    def test_r1_2_a_notice_without_a_reason_has_no_dangling_colon(self, notifications, recorder, extra, message):
        _send(notifications, 'schedule_disabled', extra=extra)

        assert recorder.fired[0][1]['meta']['message'] == message

    @pytest.mark.parametrize('extra, message', [
        (None, 'An admin edited your schedule "[Daily report]()"'),
        ({'action': 'deleted'}, 'An admin deleted your schedule "[Daily report]()"'),
    ])
    def test_r1_2_the_admin_notice_names_the_action(self, notifications, recorder, extra, message):
        _send(notifications, 'schedule_edited_by_admin', extra=extra)

        assert recorder.fired[0][1]['meta']['message'] == message

    def test_r1_2_a_notice_without_a_run_has_null_run_fields(self, notifications, recorder):
        _send(notifications, 'schedule_expired')

        meta = recorder.fired[0][1]['meta']
        assert meta['run_id'] is None
        assert meta['conversation_id'] is None
        assert meta['message'] == 'Schedule "[Daily report]()" reached its stop date and is switched off'

    def test_r1_3_owner_change_adds_both_owner_ids(self, notifications, recorder):
        _send(notifications, 'schedule_owner_changed', extra={'old_owner_id': 42, 'new_owner_id': 43})

        meta = recorder.fired[0][1]['meta']
        assert meta['old_owner_id'] == 42
        assert meta['new_owner_id'] == 43

    def test_r1_3_other_events_carry_no_owner_ids(self, notifications, recorder):
        _send(notifications, 'schedule_run_failed', run=RUN)

        meta = recorder.fired[0][1]['meta']
        assert 'old_owner_id' not in meta
        assert 'new_owner_id' not in meta

    def test_r1_4_a_failed_fire_is_logged_with_the_schedule_id_and_returns_false(self, notifications, recorder):
        recorder.fire_error = RuntimeError('event bus down')

        assert _send(notifications, 'schedule_run_failed', run=RUN) is False

        assert any('7' in line and 'event bus down' in line for line in recorder.logged)


ALL_TYPES = ALWAYS_SENT + ['schedule_run_succeeded']
SCHEDULE_LINK = '[Daily report]()'


class TestEL7011NoticeLinks:
    """QA bug 2026-10-21 (BUG-21): the bell renders a link only for a markdown link in meta.message; the name
    is the empty-target link `[name]()` and the bell builds the target from event_type and meta (US-40 R2.1-R2.5)."""

    @pytest.mark.parametrize('event_type', ALL_TYPES)
    def test_every_notice_links_the_name_with_an_empty_target(self, notifications, recorder, event_type):
        _send(notifications, event_type)

        meta = recorder.fired[0][1]['meta']
        assert f'"{SCHEDULE_LINK}"' in meta['message']
        assert '/settings' not in meta['message']
        assert meta['schedule_name'] == 'Daily report'
        assert meta['schedule_id'] == 7

    @pytest.mark.parametrize('event_type, message', [
        ('schedule_approval_needed', f'Schedule "{SCHEDULE_LINK}" waits for your approval'),
        ('schedule_approval_expired',
         f'Schedule "{SCHEDULE_LINK}" was not approved in 24 hours and the run stopped'),  # the default window
        ('schedule_run_failed', f'Schedule "{SCHEDULE_LINK}" failed: credential not found'),
        ('schedule_run_succeeded', f'Schedule "{SCHEDULE_LINK}" ran successfully'),
    ])
    def test_a_run_notice_keeps_the_run_in_meta_not_in_the_message(
        self, notifications, recorder, event_type, message,
    ):
        _send(notifications, event_type, run=RUN)

        meta = recorder.fired[0][1]['meta']
        assert meta['message'] == message
        assert meta['conversation_id'] == 55
        assert meta['run_id'] == 301

    def test_owner_changed_embeds_nothing_and_keeps_the_owner_ids(self, notifications, recorder):
        _send(notifications, 'schedule_owner_changed', extra={'old_owner_id': 42, 'new_owner_id': 43})

        meta = recorder.fired[0][1]['meta']
        assert meta['message'] == f'The owner of schedule "{SCHEDULE_LINK}" changed'
        assert meta['schedule_id'] == 7

    def test_a_missing_name_gives_an_empty_link_text_and_still_sends(self, notifications, recorder):
        schedule = types.SimpleNamespace(id=7, name=None)

        assert notifications.notify_schedule_event(12, 42, 'schedule_expired', schedule) is True

        assert recorder.fired[0][1]['meta']['message'] == (
            'Schedule "[]()" reached its stop date and is switched off'
        )

    def test_brackets_in_the_name_cannot_break_the_link(self, notifications, recorder):
        schedule = types.SimpleNamespace(id=7, name='[Q1] report [draft]')

        notifications.notify_schedule_event(12, 42, 'schedule_expired', schedule)

        meta = recorder.fired[0][1]['meta']
        assert meta['message'] == (
            'Schedule "[［Q1］ report ［draft］]()" reached its stop date and is switched off'
        )
        assert meta['schedule_name'] == '[Q1] report [draft]'


class TestEL7011R3Gate:
    def test_r3_1_run_succeeded_is_dropped_when_the_owner_turned_it_off(self, notifications, recorder):
        recorder.preference = types.SimpleNamespace(get_notified=False)

        assert _send(notifications, 'schedule_run_succeeded', run=RUN) is False
        assert recorder.fired == []

    def test_r3_1_run_succeeded_is_sent_when_the_owner_has_no_row(self, notifications, recorder):
        assert _send(notifications, 'schedule_run_succeeded', run=RUN) is True
        assert recorder.fired[0][1]['event_type'] == 'schedule_run_succeeded'

    def test_r3_1_run_succeeded_is_sent_when_the_owner_turned_it_on(self, notifications, recorder):
        recorder.preference = types.SimpleNamespace(get_notified=True)

        assert _send(notifications, 'schedule_run_succeeded', run=RUN) is True

    def test_r3_1_run_succeeded_is_sent_when_the_gate_read_fails(self, notifications, recorder):
        recorder.gate_error = RuntimeError('database down')

        assert _send(notifications, 'schedule_run_succeeded', run=RUN) is True
        assert recorder.fired[0][1]['event_type'] == 'schedule_run_succeeded'
        assert any('7' in line and 'database down' in line for line in recorder.logged)

    def test_get_notified_for_is_true_without_a_row_and_follows_the_row_otherwise(self, notifications, recorder):
        session = FakeSession(recorder)
        assert notifications.get_notified_for(session, 42) is True
        recorder.preference = types.SimpleNamespace(get_notified=False)
        assert notifications.get_notified_for(session, 42) is False

    @pytest.mark.parametrize('event_type', ALWAYS_SENT)
    def test_r3_2_every_other_event_is_sent_whatever_the_owner_chose(self, notifications, recorder, event_type):
        recorder.preference = types.SimpleNamespace(get_notified=False)

        assert _send(notifications, event_type, run=RUN) is True
        assert recorder.fired[0][1]['event_type'] == event_type


FINISHED = {'owner_id': 42, 'schedule_id': 7, 'run_id': 301, 'status': 'success', 'entity_id': 9, 'conversation_id': 55}


class TestEL7011R4SocketEvent:
    def test_r4_1_emits_to_the_owners_notification_room(self, notifications, recorder):
        notifications.emit_schedule_run_finished(**FINISHED)

        [(event, _data, kwargs)] = recorder.emitted
        assert event == 'schedule_run_finished'
        assert kwargs['to'] == 'room_notifications_42'

    def test_r4_2_payload_carries_the_five_fields(self, notifications, recorder):
        notifications.emit_schedule_run_finished(**FINISHED)

        assert recorder.emitted[0][1] == {
            'schedule_id': 7, 'run_id': 301, 'status': 'success', 'entity_id': 9, 'conversation_id': 55,
        }

    def test_r4_3_a_failed_emit_is_logged_and_does_not_raise(self, notifications, recorder):
        recorder.emit_error = RuntimeError('sio down')

        assert notifications.emit_schedule_run_finished(**FINISHED) is None

        assert any('sio down' in line for line in recorder.logged)


def test_a_notice_other_than_the_expired_one_does_not_read_the_approval_window(notifications, recorder, monkeypatch):
    runs = types.ModuleType(f'{PKG}.utils.schedule_runs')
    reads = []
    runs.approval_wait_hours = lambda: reads.append(1) or 48
    monkeypatch.setitem(sys.modules, runs.__name__, runs)

    _send(notifications, 'schedule_expired')

    assert reads == []
    assert recorder.fired
