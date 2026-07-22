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

from ..utils.application_tools import cancel_toolkit_index_meta, resolve_toolkit_index_connection


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
        """Route a stopped task into parked-parent launch or child reconcile.

        Reads meta first (cheap) to branch:
          * child  — meta carries reconcile_epoch → advance the reconcile gate.
          * parent — task_name is an agent runner AND its result is parked →
                     launch one durable child per spec.
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

        # Parent candidate: only the two agent runners can park.
        if meta.get("task_name") not in ("indexer_agent", "indexer_predict_agent"):
            return
        try:
            result = self.task_node.get_task_result(task_id)  # pylint: disable=E1101
        except Exception:  # pylint: disable=W0703
            return
        if not isinstance(result, dict) or not result.get("parallel_parked"):
            return
        self.parallel_dispatch_launch_children(task_id, meta, result)
