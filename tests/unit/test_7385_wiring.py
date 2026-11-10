"""EL-7385: source-level guards for wiring that needs the whole Pylon runtime to exercise.

Each assertion pins a detail that silently breaks the feature when it regresses:

* chat_enter_room stays read-only: it joins the room and writes nothing (two tabs join at once);
* stream_response does no per-chunk work for a live-text catch-up (dropped after review);
* every response shape of the sidebar list sets has_running_task (a missed shape shows no spinner),
  and the partial index it reads is declared;
* the stop / timeout paths, which end a run outside chat_message_stream_end, announce "stopped";
  Stop reads the audience before its delete branch removes the row;
* the chat name also reaches the author's own room;
* rows whose run never started are ended by an admin task, registered and unregistered.
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
    def test_room_entry_is_read_only(source):
        handler = _between(source("sio/all.py"), "def enter_room(", "@web.sio(SioEvents.test_toolkit_enter_room)")
        assert handler.index("if not can_join_room(") < handler.index(
            "self.context.sio.enter_room(sid, get_chat_room(conversation_uuid))")
        for write in ("commit(", "close_never_started_runs", "emit("):
            assert write not in handler

    @staticmethod
    def test_no_live_text_catch_up(source):
        assert "run_snapshot" not in source("sio/all.py")
        stream = _between(source("methods/stream.py"), "def stream_response(", "def process_index_data_status_event(")
        assert "snapshot" not in stream


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

    @staticmethod
    def test_the_list_query_has_its_partial_index(source):
        model = source("models/message_group.py")
        assert "Index('ix_chat_message_group_streaming', 'conversation_id', postgresql_where=text('is_streaming'))" in model
        assert ("CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_chat_message_group_streaming ON {pid}.chat_message_group "
                "(conversation_id) WHERE is_streaming;") in source("db_migrations.txt")


class TestRunEnds:
    @staticmethod
    def test_stop_announces_stopped(source):
        stop = _between(source("rpc/chat_all.py"), "def chat_stop_task(", 'return {"success": True}')
        read = stop.index("run_notice = build_notice(session, project_id, msg_group, RunStatus.stopped)")
        assert read < stop.index("session.delete(msg_group)")
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
        assert "send_notice(" in end

    @staticmethod
    def test_end_helper_is_on_the_module(source):
        # Pylon puts only @web.method / proxy-named @web.rpc methods on the Module that `self` is
        chat_all = source("rpc/chat_all.py")
        assert "    @web.method()\n    def _end_chat_run(" in chat_all


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
    def test_cleanup_is_an_admin_task(source):
        assert "    @web.method()\n    def close_never_started_chat_runs(" in source("methods/admin_tasks.py")
        module = source("module.py")
        assert '"close_never_started_chat_runs", self.close_never_started_chat_runs,\n                group="R-2.0.9",' in module
        assert module.count('"close_never_started_chat_runs", self.close_never_started_chat_runs') == 2
