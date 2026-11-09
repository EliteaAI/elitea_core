"""EL-7385: source-level guards for wiring that needs the whole Pylon runtime to exercise.

Each assertion pins a detail that silently breaks the feature when it regresses:

* chat_enter_room sends the catch-up only after the join succeeded, to the joining sid only, and never
  before the access check (a denied user must not get a private chat's text);
* every response shape of the sidebar list sets has_running_task (a missed shape shows no spinner);
* the stop / timeout paths, which end a run outside chat_message_stream_end, drop the snapshot and
  announce "stopped"; Stop reads the audience before its delete branch removes the row;
* stream_response keeps the text before the empty-room return, the very case it exists for;
* the chat name also reaches the author's own room.
"""
import pathlib

import pytest


@pytest.fixture(scope="module")
def source(plugin_root: pathlib.Path):
    return lambda relative: (plugin_root / relative).read_text()


def _between(text, start, end):
    begin = text.index(start)
    return text[begin:text.index(end, begin)]


class TestEnterRoom:
    @staticmethod
    def test_catch_up_runs_after_the_access_check_and_the_join(source):
        handler = _between(source("sio/all.py"), "def enter_room(", "@web.sio(SioEvents.test_toolkit_enter_room)")
        check = handler.index("if not can_join_room(")
        join = handler.index("self.context.sio.enter_room(sid, get_chat_room(conversation_uuid))")
        catch_up = handler.index("_send_run_snapshots(self, sid, parsed.project_id, conversation.id, conversation_uuid)")
        assert check < join < catch_up

    @staticmethod
    def test_catch_up_goes_to_the_joining_sid_only(source):
        sender = _between(source("sio/all.py"), "def _send_run_snapshots(", "class SIO:")
        assert "to=sid" in sender
        assert "room=" not in sender


class TestListField:
    @staticmethod
    def test_every_sidebar_shape_sets_has_running_task(source):
        folder = source("api/v2/folder.py")
        get = _between(folder, "def get(self, project_id: int, **kwargs):", "def post(self, project_id: int, **kwargs):")
        # folder page, date-group page, folders in the sidebar, grouped pinned/date groups, legacy list
        assert get.count('"has_running_task"') == 5
        assert get.count('"has_shared_links"') == 5
        assert get.count("running_conversation_ids(session, ") == 5

    @staticmethod
    def test_list_rpc_rows_set_has_running_task(source):
        rpc = _between(source("rpc/chat_conversation.py"), "def list_conversations_rpc(", "def get_conversation_by_uuid_rpc(")
        assert "running_ids = running_conversation_ids(session, conv_ids)" in rpc
        assert '"has_running_task": conv.id in running_ids,' in rpc
        assert "has_running_task: bool = False" in source("models/pd/conversation.py")


class TestRunEnds:
    @staticmethod
    def test_stop_drops_the_snapshot_and_announces_stopped(source):
        stop = _between(source("rpc/chat_all.py"), "def chat_stop_task(", 'return {"success": True}')
        read = stop.index("run_notice = build_notice(session, project_id, msg_group, RunStatus.stopped)")
        assert read < stop.index("session.delete(msg_group)")
        assert "drop_snapshot(self.get_redis_client(), str(message_group_uuid))" in stop
        assert "send_notice(self.context.sio, run_notice)" in stop
        # Announced after the session closes, so no emit runs under the row lock
        assert stop.index("send_notice(") > stop.index("session.commit()")

    @staticmethod
    def test_timeout_ends_the_run_the_same_way(source):
        chat_all = source("rpc/chat_all.py")
        timeout = _between(chat_all, "def finalize_timed_out_response(", "def _end_chat_run(")
        assert "self._end_chat_run(session, project_id, response_msg, RunStatus.stopped)" in timeout
        assert chat_all.count("project_id=parsed.project_id,\n") >= 2
        end = _between(chat_all, "def _end_chat_run(", "def _refuse_schedule_decision(")
        assert "drop_snapshot(" in end and "send_notice(" in end

    @staticmethod
    def test_end_helper_is_on_the_module(source):
        # Pylon puts only @web.method / proxy-named @web.rpc methods on the Module that `self` is
        chat_all = source("rpc/chat_all.py")
        assert "    @web.method()\n    def _end_chat_run(" in chat_all


class TestStreamResponse:
    @staticmethod
    def test_text_is_kept_before_the_empty_room_return(source):
        stream = _between(source("methods/stream.py"), "def stream_response(", "def _keep_run_snapshot(")
        assert stream.index("self._keep_run_snapshot(payload)") < stream.index("get_participants('/', room)")

    @staticmethod
    def test_keep_helper_is_on_the_module(source):
        assert "    @web.method()\n    def _keep_run_snapshot(" in source("methods/stream.py")


class TestNaming:
    @staticmethod
    def test_name_also_goes_to_the_authors_room(source):
        naming = source("methods/conversation_naming.py")
        assert "room=get_user_room(conversation.author_id)" in naming
        assert "{**name_update, 'project_id': project_id}" in naming


class TestNeverStartedRuns:
    @staticmethod
    def test_every_chat_run_start_records_its_task_id(source):
        # A streaming row without a task id past the grace counts as never started; a live LLM run
        # started without a socket (REST, API) used to get none and would have been ended mid-run.
        application = source("rpc/application.py")
        llm = _between(application, "def predict_sio_llm(", '"type": "start_task"')
        fire = llm.index("'applications_predict_task_id'")
        assert fire < llm.index("if sid:\n            self.stream_response(")
        agent = _between(application, "    def predict_sio(self,", "def predict_sio_llm(")
        assert "'applications_predict_task_id'" in agent

    @staticmethod
    def test_room_entry_ends_never_started_runs_before_the_catch_up(source):
        sender = _between(source("sio/all.py"), "def _send_run_snapshots(", "class SIO:")
        assert sender.index("close_never_started_runs(session, conversation_id)") < sender.index(
            "streaming_groups(session, conversation_id)")
