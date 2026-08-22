#!/usr/bin/python3
# coding=utf-8
# pylint: disable=W0201

#   Copyright 2025 EPAM Systems
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

""" Method """

import requests  # pylint: disable=E0401

from pylon.core.tools import web, log  # pylint: disable=E0401,E0611,W0611
from sqlalchemy.orm.attributes import flag_modified
from tools import db

from ..models.message_group import ConversationMessageGroup
from ..utils.application_tools import cancel_toolkit_index_meta, resolve_toolkit_index_connection
from ..utils.parallel_hitl import (
    INTERNAL_CONTINUE_TOKEN, merge_interrupts, pending_supervisor_decisions,
    update_supervisor_decision_phase,
)
from ..utils.sio_utils import SioEvents
from ..utils.toolkit_authorization import merge_authorization_request


# Fallback only; an aborting worker that can still talk supplies its own text (#6245).
FORK_PROBE_USER_MESSAGE = "Temporary server error, please try again"

# Supervised HITL phases that _maybe_recover_supervised_hitl can still replay.
RECOVERABLE_SUPERVISOR_PHASES = frozenset({
    "queued", "offered", "committed", "resuming", "fallback_pending",
})

# Task names whose failure must reach the UI: everything dispatched into the agents pool
# that opens a stream the user is watching.
REPORTABLE_TASK_NAMES = (
    "indexer_agent",
    "indexer_predict_agent",
    "indexer_test_toolkit_tool",
    "indexer_test_mcp_connection",
    "indexer_mcp_sync_tools",
)


class Method:
    """ Method """

    @web.method()
    def task_status_changed(self, _, payload):
        """ Handler """
        task_id = payload.get("task_id", None)
        status = payload.get("status", None)
        #
        if status != "stopped":
            return
        #
        # Parallel sub-agent dispatch (#4993 Track 2) runs BEFORE the callback
        # pop: SIO agent tasks (parked parents and their children) register no
        # callback, so they would otherwise hit the early-return below. Best-effort
        # and self-contained — a failure here must never block the callback path.
        try:
            self._maybe_handle_parallel_dispatch(task_id)
        except Exception:  # pylint: disable=W0702,W0703
            log.exception("Parallel dispatch handling failed (task_id=%s)", task_id)
        try:
            self._maybe_recover_supervised_hitl(task_id)
        except Exception:  # pylint: disable=W0702,W0703
            log.exception("Parallel HITL recovery failed (task_id=%s)", task_id)
        #
        # Reconcile any index_data run that was hard-killed by this Stop: an inline
        # index_data run in the agent worker never writes its terminal state when the
        # worker is SIGTERM/os._exit'd, so its index_meta row sticks at 'in_progress'.
        # This runs BEFORE the callback_tasks early-return so it also covers SIO agent
        # runs that register no callback. Best-effort (exceptions are swallowed so it can
        # never break the callback path). It runs inline, so when the stopped task had an
        # active index it adds a small, bounded latency (a toolkit-config resolve + cancel)
        # ahead of the callback POST — rare (only for index-bearing stops) and acceptable.
        try:
            self.reconcile_stopped_index_metas(task_id)
        except Exception:  # pylint: disable=W0702,W0703
            log.exception("Stopped-index reconcile failed (task_id=%s)", task_id)
        #
        callback_data = self.callback_tasks.pop(task_id, None)
        #
        if not callback_data and not self.not_starting_task_event.is_set():
            self.not_starting_task_event.wait(self.task_node.start_max_wait)  # pylint: disable=E1101
            callback_data = self.callback_tasks.pop(task_id, None)
        #
        if not callback_data:
            return
        #
        try:
            task_result = self.task_node.get_task_result(task_id)  # pylint: disable=E1101
            #
            callback_payload = {
                "task_id": task_id,
                "task_result": task_result,
            }
        except:  # pylint: disable=W0702
            callback_payload = {
                "task_id": task_id,
                "task_error": "Exception",
            }
        #
        try:
            requests_result = requests.post(
                callback_data.get("callback_url"),
                headers=callback_data.get("callback_headers", None),
                json=callback_payload,
                timeout=120.0,
                verify=False,
            )
            #
            log.info("Callback POST result: %s", requests_result)
        except:  # pylint: disable=W0702
            log.exception("Error in callback sender (task_id=%s)", task_id)

    @web.method()
    def _maybe_recover_supervised_hitl(self, task_id):
        """Replay durable live decisions if their owning root worker died."""
        try:
            task_meta = self.task_node.get_task_meta(task_id)  # pylint: disable=E1101
        except Exception:  # pylint: disable=W0703
            return
        if not isinstance(task_meta, dict):
            return
        project_id = task_meta.get('chat_project_id')
        message_id = task_meta.get('message_id')
        if not project_id or not message_id or self.is_chat_run_stopped(message_id):
            return

        with db.get_session(project_id) as session:
            response_msg = session.query(ConversationMessageGroup).filter(
                ConversationMessageGroup.uuid == message_id,
                ConversationMessageGroup.task_id == task_id,
            ).with_for_update(of=ConversationMessageGroup).first()
            if response_msg is None:
                return
            decisions = [
                item for item in pending_supervisor_decisions(response_msg.meta)
                if item.get('phase') in RECOVERABLE_SUPERVISOR_PHASES
                and isinstance(item.get('pending_interrupt'), dict)
            ]
            if not decisions:
                return

            meta = dict(response_msg.meta or {})
            for decision in decisions:
                pending = dict(decision['pending_interrupt'])
                interrupt_id = decision.get('interrupt_id')
                pending['resume_strategy'] = 'root'
                meta['resolved_hitl_interrupt_ids'] = [
                    value for value in meta.get('resolved_hitl_interrupt_ids', [])
                    if value != interrupt_id
                ]
                meta['resolved_authorization_request_ids'] = [
                    value for value in meta.get(
                        'resolved_authorization_request_ids', []
                    ) if value != interrupt_id
                ]
                if pending.get('guardrail_type') == 'mcp_auth':
                    meta = merge_authorization_request(meta, pending)
                else:
                    merged = merge_interrupts(meta, {
                        'resume_strategy': 'root',
                        'hitl_interrupt': pending,
                    })
                    if merged:
                        meta['hitl_interrupt'] = merged[0]
                        meta['hitl_interrupts'] = merged
                meta = update_supervisor_decision_phase(
                    meta, decision['decision_id'], 'recovering',
                )
            response_msg.meta = meta
            response_msg.is_streaming = False
            flag_modified(response_msg, 'meta')
            session.commit()

            conversation_uuid = str(response_msg.conversation.uuid)
            author_id = response_msg.conversation.author_id
            root_thread_id = (
                decisions[0].get('root_thread_id')
                or meta.get('thread_id')
                or conversation_uuid
            )

        resume_decisions = [
            {
                'interrupt_id': item.get('interrupt_id'),
                'tool_call_id': item.get('tool_call_id'),
                'action': item.get('action'),
                'value': item.get('value', ''),
                'guardrail_type': item.get('guardrail_type'),
            }
            for item in decisions
        ]
        log.warning(
            '[PARALLEL] recovering %d supervised HITL decision(s) after task %s',
            len(resume_decisions), task_id,
        )
        # ``continue_predict_sio`` is registered on the plugin module under
        # its RPC name (``chat_continue_predict_sio``).  Method mixins cannot
        # call the undecorated Python function name through ``self``.
        self.chat_continue_predict_sio(
            None,
            {
                'project_id': project_id,
                'conversation_uuid': conversation_uuid,
                'message_id': str(message_id),
                'thread_id': root_thread_id,
                'hitl_resume': True,
                'hitl_action': resume_decisions[0].get('action') or 'approve',
                'hitl_decisions': resume_decisions,
                'should_continue': True,
            },
            -1,
            _internal_token=INTERNAL_CONTINUE_TOKEN,
            _internal_user_id=author_id,
        )

    @web.method()
    def _chat_stream_already_closed(self, chat_project_id, message_id):
        """Whether the run's terminal state belongs to someone else, so we must stay quiet."""
        # Two owners: a child that already reported its own specific error (is_streaming
        # went False on the stream-end/pause handlers), or the supervised-HITL recovery that
        # runs right after us and will resume this run - painting an error over either is wrong.
        if not chat_project_id or not message_id:
            return False
        try:
            with db.get_session(chat_project_id) as session:
                msg_group = session.query(ConversationMessageGroup).filter(
                    ConversationMessageGroup.uuid == message_id
                ).first()
                if not msg_group:
                    return False
                if not msg_group.is_streaming:
                    return True
                return any(
                    item.get("phase") in RECOVERABLE_SUPERVISOR_PHASES
                    and isinstance(item.get("pending_interrupt"), dict)
                    for item in pending_supervisor_decisions(msg_group.meta)
                )
        except Exception:  # pylint: disable=W0703
            # Fails open: double-reporting is recoverable, an endless spinner is not.
            log.exception("Terminal-state check failed (message_id=%s)", message_id)
            return False

    @web.method()
    def _report_task_failure(self, task_id, meta, result=None):
        """End the chat stream for a worker task that died without reporting itself (#6288)."""
        # A wedged or hard-killed child never reaches Redis, so its agent_exception and
        # full_message never arrive. Meta is the only source that always survives; a
        # result dict is a bonus the older in-worker guard still supplies.
        result = result if isinstance(result, dict) else {}
        stream_id = meta.get("stream_id") or result.get("stream_id")
        message_id = meta.get("message_id") or result.get("message_id")
        sio_event = meta.get("sio_event") or SioEvents.application_predict.value
        content = result.get("human_readable") or FORK_PROBE_USER_MESSAGE
        chat_project_id = meta.get("chat_project_id")
        #
        if not stream_id:
            log.warning("Cannot report failure of task %s: no stream_id in meta", task_id)
            return
        #
        # A user Stop normally leaves no result at all, but a task that raced the kill and
        # raised must not paint an error over the text the stopper already wrote.
        if message_id and self.is_chat_run_stopped(message_id):
            log.info("Task %s failed after a user Stop; not reporting", task_id)
            return
        #
        if self._chat_stream_already_closed(chat_project_id, message_id):
            log.info(
                "Task %s failed after its message was already closed; not reporting "
                "(message_id=%s)", task_id, message_id,
            )
            return
        #
        log.warning(
            "Task %s died without reporting itself; reporting to the UI (message_id=%s)",
            task_id, message_id,
        )
        #
        response_metadata = {
            "project_id": meta.get("project_id"),
            "chat_project_id": chat_project_id,
            "is_error": True,
            "error": result.get("error") or "task_failed",
        }
        base_payload = {
            "stream_id": stream_id,
            "message_id": message_id,
            "question_id": meta.get("question_id"),
            "sio_event": sio_event,
            "content": content,
            "response_metadata": response_metadata,
            "execution_generation": (
                meta.get("execution_generation") or result.get("execution_generation")
            ),
        }
        #
        # Live UI: clears the spinner and shows the error box in the running chat.
        try:
            self.stream_response(sio_event, {**base_payload, "type": "agent_exception"})
        except Exception:  # pylint: disable=W0703
            log.exception("Task-failure agent_exception emit failed (task_id=%s)", task_id)
        #
        # Persistence: same event the child's full_message would have triggered, so the
        # row stops streaming and the error survives a reload.
        if sio_event == SioEvents.chat_predict.value and response_metadata["chat_project_id"]:
            try:
                self.context.event_manager.fire_event(
                    "chat_message_stream_end", {**base_payload, "type": "full_message"},
                )
            except Exception:  # pylint: disable=W0703
                log.exception("Task-failure stream_end fire failed (task_id=%s)", task_id)

    @web.method()
    def reconcile_stopped_index_metas(self, task_id):
        """Cancel any in_progress index_meta rows a stopped task left orphaned.

        A Stop hard-kills the forked worker before the SDK writes a terminal state, so
        pylon_main reconciles here from the active_index_tasks registry. Also records the
        task as recently-stopped so a late in_progress event can self-cancel (see stream).
        """
        # Mark + drain atomically vs. the in_progress register (see stream); cancel the
        # drained entries after releasing the lock (cancel does DB/vault I/O).
        with self.active_index_tasks_lock:
            self._mark_task_recently_stopped(task_id)
            entries = self.active_index_tasks.pop(str(task_id), {})
        if not entries:
            return
        for (project_id, toolkit_id, index_name), info in entries.items():
            info = info or {}
            self._cancel_stopped_index(
                project_id, toolkit_id, index_name, task_id,
                info.get('user_id'), info.get('created_on'),
            )

    @web.method()
    def _cancel_stopped_index(self, project_id, toolkit_id, index_name, task_id,
                              user_id=None, created_on=None):
        """Resolve a stopped index's connection and cancel its in_progress row (best-effort).

        Shared by reconcile_stopped_index_metas and the fast-Stop race path in stream.
        """
        try:
            connection_string, toolkit_name_id = resolve_toolkit_index_connection(
                project_id, toolkit_id, user_id
            )
            if not connection_string or not toolkit_name_id:
                log.warning(
                    "Cannot resolve connection to cancel stopped index_meta "
                    "(task_id=%s, project_id=%s, toolkit_id=%s, index_name=%s)",
                    task_id, project_id, toolkit_id, index_name,
                )
                return
            cancel_toolkit_index_meta(
                connection_string,
                toolkit_name_id,
                index_name,
                expected_task_id=str(task_id),
                delete_embeddings=False,
                expected_created_on=created_on,
            )
        except Exception:  # pylint: disable=W0702,W0703
            log.exception(
                "Failed to cancel stopped index_meta (task_id=%s, index_name=%s)",
                task_id, index_name,
            )

    @web.method()
    def _mark_task_recently_stopped(self, task_id):
        """Record a stopped task id in the bounded recently-stopped set (FIFO cap)."""
        try:
            store = self.recently_stopped_index_tasks
            key = str(task_id)
            store[key] = True
            store.move_to_end(key)
            while len(store) > self.recently_stopped_index_tasks_max:
                store.popitem(last=False)
        except Exception:  # pylint: disable=W0702,W0703
            log.exception("Failed to record recently-stopped task %s", task_id)

    @web.method()
    def _maybe_handle_parallel_dispatch(self, task_id):
        """Route a stopped task into parked-parent launch, child reconcile, or failure report.

        Reads meta first (cheap) to branch:
          * child  — meta carries reconcile_epoch → advance the reconcile gate.
          * parent — task_name is a reportable runner; its result is parked (launch one
                     durable child per spec), an abort dict, or a raise (report to the UI).
        Anything else (ordinary agent run, index task, unknown) is ignored. The
        result is only deserialized when the cheap meta check already matched, so
        the common no-op path stays O(meta lookup).
        """
        try:
            meta = self.task_node.get_task_meta(task_id)  # pylint: disable=E1101
        except Exception:  # pylint: disable=W0703
            return
        if not isinstance(meta, dict):
            return

        # Child terminal: presence of reconcile_epoch is the marker.
        if meta.get("reconcile_epoch"):
            try:
                child_result = self.task_node.get_task_result(task_id)  # pylint: disable=E1101
            except Exception:  # pylint: disable=W0703
                child_result = None
            if child_result is ...:  # stopped via stop_task / invalid — treat as terminal, no HITL
                child_result = None
            self.parallel_dispatch_on_child_terminal(meta, child_result)
            return

        # Parent candidate: agent runners can park; every name here can fail visibly.
        if meta.get("task_name") not in REPORTABLE_TASK_NAMES:
            return
        try:
            result = self.task_node.get_task_result(task_id)  # pylint: disable=E1101
        except Exception:  # pylint: disable=W0703
            # A raised task, e.g. the arbiter fork-DNS guard. Swallowing it here is what
            # left the UI spinning forever (#6288).
            log.exception(
                "Task %s failed (task_name=%s)", task_id, meta.get("task_name"),
            )
            self._report_task_failure(task_id, meta)
            return
        if not isinstance(result, dict):
            return
        if result.get("fork_dns_probe_failed"):
            # Legacy in-worker guard, still present on images whose arbiter predates #6288.
            self._report_task_failure(task_id, meta, result)
            return
        if not result.get("parallel_parked"):
            return
        self.parallel_dispatch_launch_children(task_id, meta, result)
