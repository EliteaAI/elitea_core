#!/usr/bin/python3
# coding=utf-8

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

""" Parallel sub-agent dispatch coordination (#4993 Track 2).

When a parent agent fans out to 2+ Application sub-agents, the SDK parks (writes
child specs + returns) instead of running them in-process. The parked parent
task goes terminal (``stopped``); this module — driven from task_status_changed
— launches one durable ``indexer_agent`` child per spec, then reconciles once
every child settles by re-invoking the parent with ``parallel_reconcile``.

Hot-path discipline (the gevent hub runs this): NO LangGraph checkpoint
deserialize here, NO ORM. The gate is a single indexed raw-SQL existence check;
heavy assembly happens in the re-invoked parent inside the fork pool. See
.claude/rules/arbiter.md (ephemeral results) and the two-track plan.
"""

import json
from uuid import uuid4

from pylon.core.tools import web, log  # pylint: disable=E0401,E0611
from sqlalchemy import text  # pylint: disable=E0401

from tools import db, config as c  # pylint: disable=E0401


# Shared (non-tenant) schema: the reconcile handler fires outside project-request
# scope, so a single GLOBAL table avoids a per-event search_path round-trip.
# (parent_thread_id, reconcile_epoch) is globally unique (thread_ids are UUIDs),
# so no tenant scoping is needed for correctness — project_id is a column only
# for cleanup/observability.
_TABLE = f'{c.POSTGRES_SCHEMA}.parallel_agent_runs'

# Child statuses. A HITL-paused child also fires `stopped` but is NOT terminal;
# only completed/error rows open the reconcile gate.
_STATUS_RUNNING = 'running'
_STATUS_TERMINAL = 'terminal'
_STATUS_ERROR = 'error'

# Redis lease TTL (seconds): guards double-wake of the parent across concurrent
# child-terminal events. Generous — the lease only needs to outlive the gate +
# re-invoke, then it is irrelevant (rows are deleted on reconcile).
_LEASE_TTL = 120

# Redis stash TTL (seconds) for the parent's self-contained reconcile re-invoke
# payload. Must outlive human-think-time on a child's HITL pause; a few hours is
# safe because the parent's carried token is a long-lived user/system API token.
_RECONCILE_PAYLOAD_TTL = 6 * 60 * 60


class Method:  # pylint: disable=E1101,R0903,W0201
    """ Parallel dispatch coordination methods (same-pylon @web.method). """

    @web.method()
    def parallel_dispatch_ensure_table(self):
        """Create the global side-table once (idempotent). Called from ready().

        Small, fixed-width columns; rows are DELETED after an epoch reconciles —
        this is transient coordination state, not durable history. PK gives the
        per-child upsert target; the covering index serves the gate query.
        """
        ddl = text(
            f"""
            CREATE TABLE IF NOT EXISTS {_TABLE} (
                parent_thread_id TEXT NOT NULL,
                reconcile_epoch  TEXT NOT NULL,
                child_thread_id  TEXT NOT NULL,
                child_index      INTEGER NOT NULL,
                status           TEXT NOT NULL,
                project_id       INTEGER,
                created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
                PRIMARY KEY (parent_thread_id, reconcile_epoch, child_thread_id)
            )
            """
        )
        idx = text(
            f"""
            CREATE INDEX IF NOT EXISTS ix_parallel_agent_runs_gate
            ON {_TABLE} (parent_thread_id, reconcile_epoch)
            """
        )
        try:
            with db.get_session(None) as session:
                session.execute(ddl)
                session.execute(idx)
                session.commit()
            log.info("[PARALLEL] ensured side-table %s", _TABLE)
        except Exception:  # pylint: disable=W0703
            log.exception("[PARALLEL] failed to ensure side-table %s", _TABLE)

    @web.method()
    def parallel_dispatch_launch_children(self, parent_task_id, parent_meta, parent_result):
        """Launch one durable indexer_agent per parked child spec.

        Reads the parked parent's result (specs + per-child launch payloads +
        the parent's own reconcile payload), stashes the reconcile payload in
        Redis, inserts one side-table row per child (status=running), then
        start_task's each child with parent-linkage meta carrying the
        reconcile_epoch. Dispatch-then-compensate: a child that fails to launch
        is written straight to the side-table as an error row so the gate never
        hangs on it.
        """
        specs = parent_result.get('parallel_dispatch') or []
        if not specs:
            return
        parent_thread_id = parent_result.get('thread_id')
        reconcile_payload = parent_result.get('reconcile_payload') or {}
        project_id = parent_meta.get('project_id')
        parent_stream_id = parent_result.get('parent_stream_id')
        parent_message_id = parent_result.get('parent_message_id')
        # The parent's original task type (indexer_agent | indexer_predict_agent)
        # so the reconcile re-invoke targets the same runner.
        parent_task_name = parent_meta.get('task_name', 'indexer_agent')

        epoch = uuid4().hex

        # Stash the self-contained reconcile re-invoke payload in Redis, keyed by
        # (parent_thread_id, epoch). Kept OUT of the side-table (which stays tiny)
        # and out of the ephemeral arbiter result (deleted on read).
        self._parallel_reconcile_stash(
            parent_thread_id, epoch,
            {
                'reconcile_payload': reconcile_payload,
                'parent_task_name': parent_task_name,
                'parent_stream_id': parent_stream_id,
                'parent_message_id': parent_message_id,
                'project_id': project_id,
            },
        )

        # Pre-insert every child row as running so the gate has a complete roster
        # before any child can fire its terminal event.
        rows = [
            {
                'parent_thread_id': parent_thread_id,
                'reconcile_epoch': epoch,
                'child_thread_id': spec.get('child_thread_id'),
                'child_index': spec.get('index', i),
                'status': _STATUS_RUNNING,
                'project_id': project_id,
            }
            for i, spec in enumerate(specs)
        ]
        self._parallel_rows_insert(rows)

        log.info(
            "[PARALLEL] launching %d child(ren) for parent_thread_id=%s epoch=%s",
            len(specs), parent_thread_id, epoch,
        )

        for i, spec in enumerate(specs):
            child_thread_id = spec.get('child_thread_id')
            child_payload = spec.get('child_payload')
            if not child_payload:
                log.warning("[PARALLEL] spec %s missing child_payload; marking error", child_thread_id)
                self._parallel_mark_child(parent_thread_id, epoch, child_thread_id, _STATUS_ERROR)
                continue
            child_meta = {
                'task_name': 'indexer_agent',
                'project_id': project_id,
                'user_context': parent_meta.get('user_context'),
                'chat_project_id': parent_meta.get('chat_project_id'),
                # Parent linkage — presence of reconcile_epoch is how
                # task_status_changed recognizes a child terminal event.
                'parent_task_id': parent_task_id,
                'parent_thread_id': parent_thread_id,
                'child_thread_id': child_thread_id,
                'child_index': spec.get('index', i),
                'reconcile_epoch': epoch,
            }
            # Stash the child's launch payload + linkage meta so a HITL pause on
            # this child can be resumed (replayed with hitl_resume) without
            # re-resolving the sub-agent. Keyed by child_thread_id; TTL covers
            # human-think-time. Cleared when the epoch reconciles. The
            # stream/message ids are the parent's — a resumed child emits on the
            # same stream so its card renders in the parent conversation.
            self._parallel_child_stash(
                child_thread_id, child_payload, child_meta,
                parent_stream_id, parent_message_id,
            )
            try:
                child_task_id = self.task_node.start_task(  # pylint: disable=E1101
                    "indexer_agent",
                    args=[parent_stream_id, parent_message_id],
                    kwargs=child_payload,
                    pool="agents",
                    meta=child_meta,
                )
            except Exception:  # pylint: disable=W0703
                log.exception("[PARALLEL] start_task raised for child %s", child_thread_id)
                child_task_id = None
            # Pool saturation (start_task returns None) or a raise: compensate so
            # the gate does not wait forever on an undispatched child.
            if child_task_id is None:
                log.warning("[PARALLEL] child %s not dispatched (saturation?); marking error", child_thread_id)
                self._parallel_mark_child(parent_thread_id, epoch, child_thread_id, _STATUS_ERROR)

    @web.method()
    def parallel_dispatch_on_child_terminal(self, child_meta, child_result):
        """Run the cheap reconcile gate when a parked-child task stops.

        A child that PAUSED for HITL also fires `stopped` but carries
        hitl_interrupt in its result — it is still open, so do NOT mark it
        terminal and do NOT open the gate (the user will approve, the child
        resumes, and fires `stopped` again as completed). Only a completed/errored
        child advances the gate.
        """
        parent_thread_id = child_meta.get('parent_thread_id')
        epoch = child_meta.get('reconcile_epoch')
        child_thread_id = child_meta.get('child_thread_id')
        if not (parent_thread_id and epoch and child_thread_id):
            return

        # HITL-paused child: still open, not terminal.
        if isinstance(child_result, dict) and child_result.get('hitl_interrupt'):
            log.info("[PARALLEL] child %s paused for HITL; gate not advanced", child_thread_id)
            return

        self._parallel_mark_child(parent_thread_id, epoch, child_thread_id, _STATUS_TERMINAL)

        # Gate: are any children for this epoch still non-terminal? Existence
        # check is cheaper than COUNT. If still open, nothing to do.
        if self._parallel_epoch_pending(parent_thread_id, epoch):
            return

        # All children settled. Acquire the single-winner lease so only one
        # terminal event re-invokes the parent.
        if not self._parallel_acquire_lease(parent_thread_id, epoch):
            log.info("[PARALLEL] epoch %s already reconciled by another worker", epoch)
            return

        self._parallel_reinvoke_parent(parent_thread_id, epoch)

    # -- Re-invoke ------------------------------------------------------------

    def _parallel_reinvoke_parent(self, parent_thread_id, epoch):
        """Re-invoke the parked parent as a FRESH task with parallel_reconcile.

        Reads the stashed self-contained payload, stamps the epoch, and
        start_task's the same parent task type on the same stream/message. Heavy
        assembly (reading each child's checkpoint, building ToolMessages) happens
        inside that fork-pool invocation — never here on the hub. Rows are then
        deleted; the epoch is done.
        """
        stash = self._parallel_reconcile_unstash(parent_thread_id, epoch)
        if not stash:
            log.warning("[PARALLEL] no reconcile stash for parent=%s epoch=%s", parent_thread_id, epoch)
            self._parallel_rows_delete(parent_thread_id, epoch)
            return

        payload = dict(stash.get('reconcile_payload') or {})
        payload['parallel_reconcile'] = epoch
        payload['thread_id'] = parent_thread_id
        parent_task_name = stash.get('parent_task_name', 'indexer_agent')
        project_id = stash.get('project_id')

        meta = {
            'task_name': parent_task_name,
            'project_id': project_id,
            # Reconcile re-invoke is a normal parent run again — if it parks
            # AGAIN (nested fan-out) the parked-parent branch handles it; it is
            # NOT itself a child, so no reconcile_epoch in meta.
        }
        try:
            task_id = self.task_node.start_task(  # pylint: disable=E1101
                parent_task_name,
                args=[stash.get('parent_stream_id'), stash.get('parent_message_id')],
                kwargs=payload,
                pool="agents",
                meta=meta,
            )
            log.info(
                "[PARALLEL] reconcile re-invoke parent_thread_id=%s epoch=%s task_id=%s",
                parent_thread_id, epoch, task_id,
            )
        except Exception:  # pylint: disable=W0703
            log.exception("[PARALLEL] failed to re-invoke parent for epoch %s", epoch)
        finally:
            # Coordination state is consumed regardless: a failed re-invoke must
            # not leave a stuck epoch that blocks future fan-outs on this thread.
            self._parallel_rows_delete(parent_thread_id, epoch)

    # -- Side-table (raw SQL, global schema, no ORM) --------------------------

    def _parallel_rows_insert(self, rows):
        if not rows:
            return
        stmt = text(
            f"""
            INSERT INTO {_TABLE}
                (parent_thread_id, reconcile_epoch, child_thread_id, child_index, status, project_id)
            VALUES
                (:parent_thread_id, :reconcile_epoch, :child_thread_id, :child_index, :status, :project_id)
            ON CONFLICT (parent_thread_id, reconcile_epoch, child_thread_id) DO NOTHING
            """
        )
        try:
            with db.get_session(None) as session:
                session.execute(stmt, rows)
                session.commit()
        except Exception:  # pylint: disable=W0703
            log.exception("[PARALLEL] side-table insert failed")

    def _parallel_mark_child(self, parent_thread_id, epoch, child_thread_id, status):
        stmt = text(
            f"""
            UPDATE {_TABLE}
            SET status = :status
            WHERE parent_thread_id = :parent_thread_id
              AND reconcile_epoch = :reconcile_epoch
              AND child_thread_id = :child_thread_id
            """
        )
        try:
            with db.get_session(None) as session:
                session.execute(stmt, {
                    'status': status,
                    'parent_thread_id': parent_thread_id,
                    'reconcile_epoch': epoch,
                    'child_thread_id': child_thread_id,
                })
                session.commit()
        except Exception:  # pylint: disable=W0703
            log.exception("[PARALLEL] side-table mark child failed")

    def _parallel_epoch_pending(self, parent_thread_id, epoch):
        """True if any child for this epoch is still non-terminal (gate closed)."""
        stmt = text(
            f"""
            SELECT 1 FROM {_TABLE}
            WHERE parent_thread_id = :parent_thread_id
              AND reconcile_epoch = :reconcile_epoch
              AND status = :running
            LIMIT 1
            """
        )
        try:
            with db.get_session(None) as session:
                row = session.execute(stmt, {
                    'parent_thread_id': parent_thread_id,
                    'reconcile_epoch': epoch,
                    'running': _STATUS_RUNNING,
                }).first()
            return row is not None
        except Exception:  # pylint: disable=W0703
            log.exception("[PARALLEL] gate query failed; treating epoch as pending")
            # Fail closed: better to wait than to reconcile with a missing child.
            return True

    def _parallel_rows_delete(self, parent_thread_id, epoch):
        stmt = text(
            f"""
            DELETE FROM {_TABLE}
            WHERE parent_thread_id = :parent_thread_id
              AND reconcile_epoch = :reconcile_epoch
            """
        )
        try:
            with db.get_session(None) as session:
                session.execute(stmt, {'parent_thread_id': parent_thread_id, 'reconcile_epoch': epoch})
                session.commit()
        except Exception:  # pylint: disable=W0703
            log.exception("[PARALLEL] side-table delete failed")

    # -- Redis (lease + reconcile-payload stash) ------------------------------

    def _parallel_acquire_lease(self, parent_thread_id, epoch):
        """SETNX single-winner lease keyed by (parent_thread_id, epoch)."""
        key = f"parallel_reconcile_lease:{parent_thread_id}:{epoch}"
        try:
            client = self.get_redis_client()  # pylint: disable=E1101
            return bool(client.set(key, '1', nx=True, ex=_LEASE_TTL))
        except Exception:  # pylint: disable=W0703
            log.exception("[PARALLEL] lease acquire failed; allowing reconcile")
            # Fail open: a missed reconcile is worse than a rare double-wake, and
            # the re-invoke is idempotent enough (parent checkpoint is the source).
            return True

    def _parallel_reconcile_stash(self, parent_thread_id, epoch, value):
        key = f"parallel_reconcile_payload:{parent_thread_id}:{epoch}"
        try:
            client = self.get_redis_client()  # pylint: disable=E1101
            client.set(key, json.dumps(value), ex=_RECONCILE_PAYLOAD_TTL)
        except Exception:  # pylint: disable=W0703
            log.exception("[PARALLEL] reconcile stash failed")

    def _parallel_reconcile_unstash(self, parent_thread_id, epoch):
        key = f"parallel_reconcile_payload:{parent_thread_id}:{epoch}"
        try:
            client = self.get_redis_client()  # pylint: disable=E1101
            raw = client.get(key)
            if raw is None:
                return None
            client.delete(key)
            return json.loads(raw)
        except Exception:  # pylint: disable=W0703
            log.exception("[PARALLEL] reconcile unstash failed")
            return None

    def _parallel_child_stash(
        self, child_thread_id, child_payload, child_meta,
        parent_stream_id=None, parent_message_id=None,
    ):
        """Stash a child's launch payload + linkage for HITL resume."""
        key = f"parallel_child_launch:{child_thread_id}"
        try:
            client = self.get_redis_client()  # pylint: disable=E1101
            client.set(
                key,
                json.dumps({
                    'child_payload': child_payload,
                    'child_meta': child_meta,
                    'parent_stream_id': parent_stream_id,
                    'parent_message_id': parent_message_id,
                }),
                ex=_RECONCILE_PAYLOAD_TTL,
            )
        except Exception:  # pylint: disable=W0703
            log.exception("[PARALLEL] child stash failed (%s)", child_thread_id)

    @web.method()
    def parallel_dispatch_lookup_child(self, child_thread_id):
        """Return the stashed launch payload + meta for a child, or None.

        Used by the continue/HITL-resume path to detect that an incoming
        thread_id belongs to a parked-fan-out child and to replay that child
        (with hitl_resume) instead of regenerating the parent's payload. The
        stash is left in place — the child resumes on the SAME thread and may
        pause again before it finally completes; the epoch reconcile clears it.
        """
        if not child_thread_id:
            return None
        key = f"parallel_child_launch:{child_thread_id}"
        try:
            client = self.get_redis_client()  # pylint: disable=E1101
            raw = client.get(key)
            if raw is None:
                return None
            return json.loads(raw)
        except Exception:  # pylint: disable=W0703
            log.exception("[PARALLEL] child lookup failed (%s)", child_thread_id)
            return None
