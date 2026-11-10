"""EL-7011 US-13 R1.1, R1.6, R5.1: the stream end and pause events close a schedule run after their commit.

Contract under test: events/message_stream.py. Every sibling import is faked; the schedule writers record
their calls, so the test pins when the step runs, with which arguments, and that other chats skip it.

The EL-7385 cases at the end use the same harness: the run status each event announces to the chat
lists.
"""
import sys
import types

import pytest
from unit._tool_spend_loader import load

# Fixtures of the schedule writer tests: the chain test runs the real writer on an in-memory database
from unit.utils import test_7011_schedule_runs_fail as schedule_writer_fixtures

runs, session = schedule_writer_fixtures.runs, schedule_writer_fixtures.session
_run, _schedule = schedule_writer_fixtures._run, schedule_writer_fixtures._schedule

provider_error_answer = load("utils/error_text.py").provider_error_answer

PROJECT_ID, RUN_ID, CONVERSATION_ID, AUTHOR_ID = 5, 9, 42, 17
CHILD_PAUSE_REASON = "a step inside a parallel branch needs an answer"
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

    def count(self):
        return 0

    def add(self, _obj):
        pass

    def refresh(self, _obj):
        pass

    def commit(self):
        self.world.events.append("commit")


class World:
    def __init__(self, meta):
        self.events = []
        self.run = []
        self.open_sessions, self.open_at_step = 0, []
        self.msg_group = types.SimpleNamespace(
            id=1, meta={}, message_items=[], reply_to_id=None, is_streaming=True,
            conversation=types.SimpleNamespace(id=CONVERSATION_ID, uuid="c-1", meta=meta, author_id=AUTHOR_ID),
        )

    fail_step = False
    stopped = False
    writer = None  # a test sets the real finish_schedule_run to run the whole chain
    reason = staticmethod(lambda error: f"short({error})")

    def finish(self, *args, author_id=None):
        self.open_at_step.append(self.open_sessions)
        self.events.append(("finish", *args, author_id))
        if self.writer:
            self.writer(*args, author_id=author_id)
        if self.fail_step:
            raise RuntimeError("db down")

    def pause(self, *args):
        self.open_at_step.append(self.open_sessions)
        self.events.append(("pause", *args))
        if self.fail_step:
            raise RuntimeError("db down")


@pytest.fixture
def make_world(monkeypatch):
    def make(meta):
        world = World(meta)
        same = lambda value, *a, **k: value
        none = lambda *a, **k: None
        stubs = {
            "models.participants": {"Participant": Model},
            "models.message_group": {"ConversationMessageGroup": Model},
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
            "utils.error_text": {"provider_error_answer": provider_error_answer},
            "utils.schedule_runs": {
                "finish_schedule_run": world.finish, "pause_schedule_run": world.pause,
                "CHILD_PAUSE_REASON": CHILD_PAUSE_REASON,
                "run_failure_reason": lambda error: world.reason(error)},
            # EL-7385 run state: recorded apart from world.events so the schedule-step orderings stay exact
            "utils.run_status": {
                "RunStatus": types.SimpleNamespace(
                    running="running", waiting="waiting", finished="finished", failed="failed", stopped="stopped"),
                "build_notice": lambda _session, project_id, _group, status: (project_id, status),
                # Like the real one: a None notice sends nothing
                "send_notice": lambda _sio, notice: notice is not None and world.run.append(("notice", notice)),
            },
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
            context=types.SimpleNamespace(sio=sio), is_chat_run_stopped=lambda _id: world.stopped)
        world.task_id = lambda: events.Event.applications_predict_task_id(handler, None, "task", {
            "task_id": "t-1", "project_id": PROJECT_ID, "message_group_id": "m-1"})
        world.end = lambda metadata, content="the answer": events.Event.chat_message_stream_end(handler, None, "end", {
            "message_id": "m-1", "content": content,
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


WORKER_ERROR = "GitHub \u00b7 create_pull_request: missing title"


def test_stream_end_with_a_short_tool_error_finishes_the_run_as_failed_with_that_text(make_world):
    world = make_world(dict(SCHEDULE_META))
    world.end({"is_error": True, "error": WORKER_ERROR})
    finish = ("finish", PROJECT_ID, RUN_ID, "failed", f"short({WORKER_ERROR})", CONVERSATION_ID, AUTHOR_ID)
    assert finish in world.events


# The reason code is stubbed here: these tests pin the is_error mapping and the call arguments only.
# Core trusts the flag, so a tool that returns its error as plain text still ends as success.
def test_stream_end_ignores_error_text_without_the_error_flag(make_world):
    world = make_world(dict(SCHEDULE_META))
    world.end({"error": WORKER_ERROR})
    finish = ("finish", PROJECT_ID, RUN_ID, "success", None, CONVERSATION_ID, AUTHOR_ID)
    assert finish in world.events


PROVIDER_400 = "Error: Error code: 400 - {'error': {'message': 'The prompt is too long', 'type': 'invalid_request_error'}}"
PROVIDER_400_REASON = "The model provider rejected the request (HTTP 400)."


def test_stream_end_with_a_provider_400_answer_and_no_flag_finishes_the_run_as_failed(make_world):
    world = make_world(dict(SCHEDULE_META))
    world.end({}, content=PROVIDER_400)
    finish = ("finish", PROJECT_ID, RUN_ID, "failed", f"short({PROVIDER_400_REASON})", CONVERSATION_ID, AUTHOR_ID)
    assert finish in world.events, world.events


@pytest.mark.parametrize(("content", "reason"), [
    ("Error: LLM stream stalled: no data received for 120s. The model provider stopped responding; please retry.",
     "The model provider stopped responding."),
    ("Error: LLM stream interrupted: the connection closed before the response finished; please retry.",
     "The connection to the model provider closed before the answer finished."),
], ids=["stalled", "interrupted"])
def test_stream_end_with_a_stalled_or_cut_stream_answer_and_no_flag_finishes_the_run_as_failed(make_world, content, reason):
    world = make_world(dict(SCHEDULE_META))
    world.end({}, content=content)
    finish = ("finish", PROJECT_ID, RUN_ID, "failed", f"short({reason})", CONVERSATION_ID, AUTHOR_ID)
    assert finish in world.events, world.events


def test_stream_end_keeps_the_worker_error_over_the_answer_text(make_world):
    world = make_world(dict(SCHEDULE_META))
    world.end({"is_error": True, "error": "the model failed"}, content=PROVIDER_400)
    finish = ("finish", PROJECT_ID, RUN_ID, "failed", "short(the model failed)", CONVERSATION_ID, AUTHOR_ID)
    assert finish in world.events, world.events


@pytest.mark.parametrize("content", [
    "The check found Error: Error code: 400 in the log",
    "Error: the file is missing",
    "Error code: 400 was returned by the API, here is how to fix it",
    [{"type": "text", "text": PROVIDER_400}],
    "",
], ids=["inside-text", "other-error-prefix", "no-answer-prefix", "chunk-list", "empty"])
def test_stream_end_with_a_normal_answer_finishes_the_run_as_success(make_world, content):
    world = make_world(dict(SCHEDULE_META))
    world.end({}, content=content)
    finish = ("finish", PROJECT_ID, RUN_ID, "success", None, CONVERSATION_ID, AUTHOR_ID)
    assert finish in world.events, world.events


@pytest.fixture
def schedule_timing(monkeypatch):
    """The sibling module the real writer imports; an earlier suite can leave it out of sys.modules.

    The module is removed again after the test: it binds the `tools` stub of this test, and a later suite loads its own.
    """
    name = "plugins.elitea_core.utils.schedule_timing"
    monkeypatch.setitem(sys.modules, name, sys.modules.get(name))
    return load("utils/schedule_timing.py", fresh=True)


@pytest.fixture
def sent(monkeypatch):
    """The owner notices the real writer sends, as (owner, event, run id)."""
    name = "plugins.elitea_core.utils.schedule_notifications"
    notices = []
    stand_in = _module(
        name,
        notify_schedule_event=lambda project_id, owner_id, event_type, schedule, run=None, extra=None: (
            notices.append((owner_id, str(event_type), run.id))),
        emit_schedule_run_finished=lambda *args: None)
    monkeypatch.setitem(sys.modules, name, stand_in)
    return types.SimpleNamespace(notices=notices)


def test_stream_end_with_a_provider_400_answer_stores_a_failed_run_and_notifies_the_owner(
        schedule_timing, make_world, runs, session, sent):
    schedule = _schedule(runs, session, created_by=AUTHOR_ID)
    run = _run(runs, session, schedule)
    world = make_world({**SCHEDULE_META, "schedule_run_id": run.id})
    world.writer, world.reason = runs.finish_schedule_run, runs.run_failure_reason

    world.end({}, content=PROVIDER_400)

    session.refresh(run)
    session.refresh(schedule)
    assert (run.status, schedule.last_run_status) == ("failed", "failed")
    assert run.events[-1]["text"] == f"Run finished: failed (The run failed: {PROVIDER_400_REASON})"
    assert sent.notices == [(AUTHOR_ID, "schedule_run_failed", run.id)]


def test_stream_end_of_a_chat_without_a_schedule_run_ignores_a_provider_400_answer(make_world):
    world = make_world({})
    world.end({}, content=PROVIDER_400)
    assert world.events == ["commit", ("emit", "sync")]


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
def test_a_pause_inside_a_fan_out_child_fails_the_schedule_run_with_a_reason(make_world, metadata):
    world = make_world(dict(SCHEDULE_META))
    world.pause_event(metadata)
    assert world.msg_group.is_streaming is True
    finish = ("finish", PROJECT_ID, RUN_ID, "failed", CHILD_PAUSE_REASON, CONVERSATION_ID, AUTHOR_ID)
    assert world.events == ["commit", finish]


def test_a_top_level_sign_in_pause_still_parks_the_schedule_run(make_world):
    world = make_world(dict(SCHEDULE_META))
    interrupt = {"guardrail_type": "mcp_auth", "toolkit_name": "github"}
    world.pause_event(interrupt)
    pause = ("pause", PROJECT_ID, RUN_ID, {"chat_project_id": PROJECT_ID, **interrupt}, CONVERSATION_ID, AUTHOR_ID)
    assert world.msg_group.is_streaming is False
    assert world.events == ["commit", pause]


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


# --- EL-7385: run status for the chat lists ---

def test_7385_stream_end_announces_finished_after_the_commit(make_world):
    world = make_world({})
    world.end({})
    assert world.run == [("notice", (PROJECT_ID, "finished"))]
    assert world.msg_group.is_streaming is False


def test_7385_stream_end_with_an_error_announces_failed(make_world):
    world = make_world({})
    world.end({"is_error": True, "error": "x"})
    assert world.run[-1] == ("notice", (PROJECT_ID, "failed"))


def test_7385_a_late_end_after_stop_announces_nothing(make_world):
    # Stop already announced "stopped" and owns the terminal state
    world = make_world({})
    world.stopped = True
    world.end({})
    assert world.run == []


def test_7385_a_pause_that_hands_the_turn_to_the_user_is_waiting(make_world):
    world = make_world({})
    world.pause_event({"hitl_interrupt": {"guardrail_type": "sensitive_tool"}})
    assert world.run == [("notice", (PROJECT_ID, "waiting"))]


def test_7385_a_child_pause_keeps_the_run_running(make_world):
    world = make_world({})
    world.pause_event({"hitl_interrupt": {"resume_strategy": "aggregate_child"}})
    assert world.run == [("notice", (PROJECT_ID, "running"))]


def test_7385_task_start_of_a_streaming_group_announces_running(make_world):
    world = make_world({})
    world.task_id()
    assert world.msg_group.task_id == "t-1"
    assert world.run == [("notice", (PROJECT_ID, "running"))]


def test_7385_task_id_for_a_group_that_already_ended_announces_nothing(make_world):
    world = make_world({})
    world.msg_group.is_streaming = False
    world.task_id()
    assert world.run == []
