"""EL-7011 US-13 R1.1, R1.6, R5.1: the stream end and pause events close a schedule run after their commit.

Contract under test: events/message_stream.py. Every sibling import is faked; the schedule writers record
their calls, so the test pins when the step runs, with which arguments, and that other chats skip it.
"""
import sys
import types

import pytest
from unit._tool_spend_loader import load

PROJECT_ID, RUN_ID, CONVERSATION_ID, AUTHOR_ID = 5, 9, 42, 17
SCHEDULE_META = {"schedule_id": 3, "schedule_run_id": RUN_ID, "execution_type": "scheduled"}


def _module(name, **attrs):
    module = types.ModuleType(name)
    vars(module).update(attrs)
    return module


class Model:
    uuid = id = message_group_id = None

    def __init__(self, **kwargs):
        vars(self).update(kwargs)


class Session:
    """Answers every query with the one message group; records commits."""

    def __init__(self, world):
        self.world = world

    def __enter__(self):
        self.world.open_sessions += 1
        return self

    def __exit__(self, *exc):
        self.world.open_sessions -= 1
        return False

    def query(self, *_):
        return self

    filter = with_for_update = lambda self, *a, **k: self

    def first(self):
        return self.world.msg_group

    def scalar(self):
        return self.world.schedule_owner

    def count(self):
        return 0

    def add(self, _obj):
        pass

    def refresh(self, _obj):
        pass

    def commit(self):
        self.world.events.append("commit")


class World:
    schedule_owner = None

    def __init__(self, meta, group_meta=None):
        self.events = []
        self.open_sessions, self.open_at_step = 0, []
        self.msg_group = types.SimpleNamespace(
            id=1, meta=group_meta or {}, message_items=[], reply_to_id=None, is_streaming=True,
            conversation=types.SimpleNamespace(id=CONVERSATION_ID, uuid="c-1", meta=meta, author_id=AUTHOR_ID),
        )

    fail_step = False

    def finish(self, *args, author_id=None, message_group_id=None):
        self.open_at_step.append(self.open_sessions)
        self.events.append(("finish", *args, author_id, *([message_group_id] if message_group_id else [])))
        if self.fail_step:
            raise RuntimeError("db down")

    def pause(self, *args, message_group_id=None):
        self.open_at_step.append(self.open_sessions)
        self.events.append(("pause", *args, *([message_group_id] if message_group_id else [])))
        if self.fail_step:
            raise RuntimeError("db down")


@pytest.fixture
def make_world(monkeypatch):
    def make(meta, group_meta=None, schedule_owner=None):
        world = World(meta, group_meta)
        world.schedule_owner = schedule_owner
        same = lambda value, *a, **k: value
        none = lambda *a, **k: None
        stubs = {
            "models.participants": {"Participant": Model},
            "models.message_group": {"ConversationMessageGroup": Model},
            "models.entity_schedule": {"EntitySchedule": type("EntitySchedule", (), {"id": None, "created_by": None})},
            "models.message_items.text": {"TextMessageItem": Model},
            "models.message_items.base": {"MessageItem": Model},
            "models.pd.message": {"MessageGroupDetail": types.SimpleNamespace(model_validate=same)},
            "utils.chat_usage": {"accumulate_chat_usage": none},
            "utils.sio_utils": {"get_chat_room": same, "SioEvents": types.SimpleNamespace(chat_message_sync="sync")},
            "utils.message_stream": {
                "update_message_group_meta": same, "safe_decode_bytes_in_dict": same, "parse_content_chunks": same},
            "utils.parallel_hitl": {
                "complete_supervisor_decisions": none, "is_current_execution": lambda *a: True,
                "merge_interrupts": lambda _meta, metadata: [metadata["hitl_interrupt"]],
                "merge_supervisor_roster": none, "requires_plural_persistence": lambda *a: False,
                "retire_all_interrupts": same, "retire_interrupts": none, "update_supervisor_decision_phase": none},
            "utils.toolkit_authorization": {
                "merge_authorization_request": same, "retire_all_authorization_requests": same,
                "retire_authorization_requests": none},
            "utils.attachments": {
                "process_single_attachment_file": none, "is_multimodal_content": lambda _c: False,
                "process_multimodal_content": none, "update_attachment_thumbnails": none},
            "utils.token_limit_continuation": {"trim_continuation_overlap": same},
            "utils.pipeline_hitl_history": {"persist_pipeline_hitl_prompt": none},
            "utils.schedule_runs": {
                "finish_schedule_run": world.finish, "pause_schedule_run": world.pause,
                "run_failure_reason": lambda error: f"short({error})"},
        }
        for dotted, names in stubs.items():
            name = "plugins.elitea_core." + dotted
            monkeypatch.setitem(sys.modules, name, _module(name, **names))
        for name in ("plugins.elitea_core.events", "plugins.elitea_core.models.message_items"):
            package = _module(name)
            package.__path__ = []
            monkeypatch.setitem(sys.modules, name, package)
        quiet = types.SimpleNamespace(
            info=none, debug=none, warning=none, error=none, exception=lambda *a: world.events.append("logged"))
        monkeypatch.setitem(sys.modules, "pylon.core.tools", _module(
            "pylon.core.tools", web=types.SimpleNamespace(event=lambda *a: (lambda f: f)), log=quiet))
        monkeypatch.setitem(sys.modules, "tools", _module(
            "tools", db=types.SimpleNamespace(get_session=lambda _pid: Session(world)),
            serialize=lambda _v: {}, rpc_tools=None))
        monkeypatch.setitem(sys.modules, "plugins.elitea_core.events.message_stream", None)
        events = load("events/message_stream.py", fresh=True)
        monkeypatch.setattr(events, "flag_modified", none)
        sio = types.SimpleNamespace(emit=lambda **kw: world.events.append(("emit", kw["event"])))
        handler = types.SimpleNamespace(
            context=types.SimpleNamespace(sio=sio), is_chat_run_stopped=lambda _id: False)
        world.end = lambda metadata: events.Event.chat_message_stream_end(handler, None, "end", {
            "message_id": "m-1", "content": "the answer",
            "response_metadata": {"chat_project_id": PROJECT_ID, "project_id": PROJECT_ID, **metadata}})
        world.pause_event = lambda metadata: events.Event.chat_message_stream_pause(handler, None, "pause", {
            "message_id": "m-1", "response_metadata": {"chat_project_id": PROJECT_ID, **metadata}})
        return world
    return make


def test_7011_r1_1_stream_end_finishes_the_schedule_run_as_success_after_the_commit(make_world):
    world = make_world(dict(SCHEDULE_META))
    world.end({})
    finish = ("finish", PROJECT_ID, RUN_ID, "success", None, CONVERSATION_ID, AUTHOR_ID)
    assert finish in world.events
    assert world.events.index("commit") < world.events.index(finish)


def test_7011_r1_1_stream_end_with_an_error_finishes_the_run_as_failed_with_the_error(make_world):
    world = make_world(dict(SCHEDULE_META))
    world.end({"is_error": True, "error": "the model failed"})
    assert ("finish", PROJECT_ID, RUN_ID, "failed", "short(the model failed)", CONVERSATION_ID, AUTHOR_ID) in world.events


# BUG-37 (US-13 R1.3, R6.1): the stored reason is the short one, never the raw traceback
def test_7011_bug37_stream_end_stores_the_short_reason_not_the_traceback(make_world):
    world = make_world(dict(SCHEDULE_META))
    error = 'Traceback (most recent call last):\n  File "/data/plugins/x.py", line 3\nValueError: boom'
    world.end({"is_error": True, "error": error})
    assert ("finish", PROJECT_ID, RUN_ID, "failed", f"short({error})", CONVERSATION_ID, AUTHOR_ID) in world.events


@pytest.mark.parametrize("meta", [{}, None, {"scheduled_run": True}])
def test_7011_r1_6_stream_end_without_a_schedule_run_keeps_todays_path(make_world, meta):
    world = make_world(meta)
    world.end({"is_error": True, "error": "x"})
    assert world.events == ["commit", ("emit", "sync")]


def test_7011_r5_1_stream_pause_hands_the_interrupt_to_the_schedule_run_after_the_commit(make_world):
    world = make_world(dict(SCHEDULE_META))
    interrupt = {"hitl_interrupt": {"guardrail_type": "sensitive_tool"}}
    world.pause_event(interrupt)
    pause = ("pause", PROJECT_ID, RUN_ID, {"chat_project_id": PROJECT_ID, **interrupt}, CONVERSATION_ID, AUTHOR_ID)
    assert world.events == ["commit", pause]


@pytest.mark.parametrize("meta", [{}, None])
def test_7011_r1_6_stream_pause_without_a_schedule_run_keeps_todays_path(make_world, meta):
    world = make_world(meta)
    world.pause_event({"hitl_interrupt": {"guardrail_type": "sensitive_tool"}})
    assert world.events == ["commit"]


@pytest.mark.parametrize("metadata", [
    {"hitl_interrupt": {"resume_strategy": "aggregate_child"}},
    {"metadata": {"child_thread_id": "child-1"}},
], ids=["parked-child-hitl", "durable-child-mcp-auth"])
def test_7011_r5_1_a_pause_that_keeps_the_run_streaming_leaves_the_schedule_run_open(make_world, metadata):
    world = make_world(dict(SCHEDULE_META))
    world.pause_event(metadata)
    assert world.msg_group.is_streaming is True
    assert world.events == ["commit"]


def test_7011_r1_6_a_failing_finish_step_does_not_break_the_stream_end(make_world):
    world = make_world(dict(SCHEDULE_META))
    world.fail_step = True
    world.end({})
    assert world.events == ["commit", ("emit", "sync"), ("finish", PROJECT_ID, RUN_ID, "success", None,
                                                         CONVERSATION_ID, AUTHOR_ID), "logged"]


def test_7011_r1_6_a_failing_pause_step_does_not_break_the_pause(make_world):
    world = make_world(dict(SCHEDULE_META))
    world.fail_step = True
    world.pause_event({"hitl_interrupt": {"guardrail_type": "sensitive_tool"}})
    assert world.events[0] == "commit"
    assert world.events[-1] == "logged"


def test_stream_end_closes_its_session_before_the_schedule_writer_opens_another(make_world):
    world = make_world(dict(SCHEDULE_META))
    world.end({})
    assert world.open_at_step == [0]


def test_stream_pause_closes_its_session_before_the_schedule_writer_opens_another(make_world):
    world = make_world(dict(SCHEDULE_META))
    world.pause_event({"hitl_interrupt": {"guardrail_type": "sensitive_tool"}})
    assert world.open_at_step == [0]
