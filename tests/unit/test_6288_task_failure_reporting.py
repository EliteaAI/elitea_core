"""Source-level guards for the #6288 task-failure reporting path.

These are structural assertions rather than behavioural ones: the reporting code lives
inside a Pylon ``Method`` class and reaches SIO, the event manager and the DB, so exercising
it needs the whole runtime. What can and must be pinned cheaply is the shape that made the
fix work at all, because each of these details silently no-ops the fix if it regresses:

* the task meta carries ``stream_id`` — ``stream_response`` raises ``KeyError`` without it,
  and on a raised task there is no result dict to read it from;
* the task meta carries ``execution_generation`` — ``chat_message_stream_end`` is gated by
  ``is_current_execution``, which drops the event when the row already has a generation and
  the payload's is falsy, leaving ``is_streaming`` stuck true forever;
* the ``get_task_result`` failure is reported instead of swallowed — swallowing it is the
  original bug;
* both terminal events are still emitted, because they do different jobs: ``agent_exception``
  clears the live spinner, ``chat_message_stream_end`` persists the terminal state.
"""

import pathlib

import pytest


@pytest.fixture(scope="module")
def application_source(plugin_root: pathlib.Path) -> str:
    return (plugin_root / "rpc" / "application.py").read_text()


@pytest.fixture(scope="module")
def callbacks_source(plugin_root: pathlib.Path) -> str:
    return (plugin_root / "methods" / "task_callbacks.py").read_text()


def meta_slice(source: str, task_name: str) -> str:
    """The meta= dict of a start_task call, located by its own task_name entry."""
    start = source.index(f'"task_name": "{task_name}"')
    return source[start:source.index("},", start)]


class TestDispatchMeta:
    @staticmethod
    @pytest.mark.parametrize("task_name", ["indexer_agent", "indexer_predict_agent"])
    def test_agent_dispatches_put_stream_id_and_generation_in_meta(application_source, task_name):
        meta = meta_slice(application_source, task_name)
        assert '"stream_id": parsed.stream_id,' in meta
        assert '"execution_generation": payload.get("execution_generation"),' in meta

    @staticmethod
    @pytest.mark.parametrize("task_name", [
        "indexer_test_toolkit_tool",
        "indexer_test_mcp_connection",
        "indexer_mcp_sync_tools",
    ])
    def test_non_agent_dispatches_put_stream_id_in_meta(application_source, task_name):
        assert '"stream_id": data[\'stream_id\'],' in meta_slice(application_source, task_name)


class TestFailureReporting:
    @staticmethod
    def test_raised_task_is_reported_not_swallowed(callbacks_source):
        dispatch = callbacks_source[
            callbacks_source.index("def _maybe_handle_parallel_dispatch"):
        ]
        assert "self._report_task_failure(task_id, meta)" in dispatch
        assert "REPORTABLE_TASK_NAMES" in dispatch

    @staticmethod
    def test_the_legacy_in_worker_abort_dict_is_no_longer_read(callbacks_source):
        # The in-worker guard is gone (#6318), so nothing returns that dict any more and the
        # reporter has one input. Pinned rather than just deleted: reintroducing a result-dict
        # branch here would quietly re-create the reader that has to match a worker payload.
        assert "fork_dns_probe_failed" not in callbacks_source
        assert "def _report_task_failure(self, task_id, meta):" in callbacks_source

    @staticmethod
    def test_reporter_reads_meta_only(callbacks_source):
        report = callbacks_source[
            callbacks_source.index("def _report_task_failure"):
            callbacks_source.index("def reconcile_stopped_index_metas")
        ]
        assert 'stream_id = meta.get("stream_id")' in report
        assert '"execution_generation": meta.get("execution_generation")' in report
        assert 'if not stream_id:' in report

    @staticmethod
    def test_reporter_emits_both_terminal_events(callbacks_source):
        report = callbacks_source[
            callbacks_source.index("def _report_task_failure"):
            callbacks_source.index("def reconcile_stopped_index_metas")
        ]
        assert '"type": "agent_exception"' in report
        assert '"chat_message_stream_end", {**base_payload, "type": "full_message"}' in report

    @staticmethod
    def test_reporter_skips_runs_that_already_ended(callbacks_source):
        # Widening the trigger means any raise now reports, so a run that already wrote its
        # own specific error - or that the user stopped - must not be painted over.
        report = callbacks_source[
            callbacks_source.index("def _report_task_failure"):
            callbacks_source.index("def reconcile_stopped_index_metas")
        ]
        assert "self.is_chat_run_stopped(message_id)" in report
        assert "self._chat_stream_already_closed(chat_project_id, message_id)" in report

    @staticmethod
    def test_terminal_check_defers_to_supervised_hitl_recovery(callbacks_source):
        # _maybe_handle_parallel_dispatch (where the reporting lives) runs BEFORE
        # _maybe_recover_supervised_hitl, and recovery only clears is_streaming afterwards.
        # So on a raise that still has replayable decisions, is_streaming is stuck true and the
        # check must look at the decisions themselves or we paint an error over a resuming run.
        check = callbacks_source[
            callbacks_source.index("def _chat_stream_already_closed"):
            callbacks_source.index("def _report_task_failure")
        ]
        assert "pending_supervisor_decisions(msg_group.meta)" in check
        assert "RECOVERABLE_SUPERVISOR_PHASES" in check
        # One source of truth for the phase list: recovery and this check must not drift.
        assert callbacks_source.count("RECOVERABLE_SUPERVISOR_PHASES") == 3
        assert (
            callbacks_source.index("self._maybe_handle_parallel_dispatch(task_id)")
            < callbacks_source.index("self._maybe_recover_supervised_hitl(task_id)")
        )

    @staticmethod
    def test_terminal_check_is_a_web_method(callbacks_source):
        # An undecorated helper in a Pylon Method class is not bound onto self.
        marker = "@web.method()\n    def _chat_stream_already_closed"
        assert marker in callbacks_source
        assert "@web.method()\n    def _report_task_failure" in callbacks_source
