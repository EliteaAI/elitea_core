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
deserialize here, NO ORM, NO Postgres. All coordination state is Redis — the
reconcile gate is a single atomic Lua op per child terminal (SADD-done +
DECR-remaining), so the moment all children settle is detected in O(1) round
trips with no DB and no lease. See .claude/rules/arbiter.md (ephemeral results)
and the two-track plan.

Coordination state (all Redis, all TTL'd; this is transient state, not durable
history — it is deleted the instant an epoch reconciles):

  parallel_run_remaining:{parent_thread_id}:{epoch}  INT   gate counter (N→0)
  parallel_run_done:{parent_thread_id}:{epoch}       SET   settled child ids (idempotency)
  parallel_run_tasks:{parent_thread_id}:{epoch}      HASH  child_thread_id → child_task_id (stop)
  parallel_parent_task:{parent_task_id}              SET   epoch refs owned by a parked parent (stop)

plus the pre-existing Redis keys (cancel flag, reconcile-payload stash, per-child
launch stash, chat-run-stopped flag) — unchanged.
"""

import json
from uuid import uuid4

from pylon.core.tools import web, log  # pylint: disable=E0401,E0611


# Redis stash TTL (seconds) for ALL coordination keys. Must outlive
# human-think-time on a child's HITL pause; a few hours is safe because the
# parent's carried token is a long-lived user/system API token. The keys are
# normally deleted on reconcile — the TTL is only a backstop against a crash
# between launch and reconcile.
_RECONCILE_PAYLOAD_TTL = 6 * 60 * 60

# Atomic reconcile-gate settle. KEYS[1]=done set, KEYS[2]=remaining counter,
# ARGV[1]=child_thread_id, ARGV[2]=TTL. SADD is the per-child idempotency guard:
# a duplicate terminal for the SAME child (HITL-pause `stopped` then completion
# `stopped`, or a stop_task kill) returns 0 and we bail with the -1 sentinel
# BEFORE decrementing. Two DIFFERENT children both add (1) and both DECR; DECR is
# atomic, so exactly one greenlet observes 0 and reconciles — a stronger
# single-winner than the old SETNX lease, with no separate lease round-trip.
# Returning -1 is unambiguous: remaining legitimately walks N→0 (never negative)
# because exactly N distinct children each decrement once.
_SETTLE_LUA = """
local added = redis.call('SADD', KEYS[1], ARGV[1])
redis.call('EXPIRE', KEYS[1], ARGV[2])
if added == 0 then return -1 end
return redis.call('DECR', KEYS[2])
"""


# -- Redis key builders (module-level: pure string helpers, NOT @web.method —
#    only @web.method-decorated fns bind onto the Module, and these need no
#    instance state). ------------------------------------------------------

def _k_remaining(parent_thread_id, epoch):
    return f"parallel_run_remaining:{parent_thread_id}:{epoch}"


def _k_done(parent_thread_id, epoch):
    return f"parallel_run_done:{parent_thread_id}:{epoch}"


def _k_tasks(parent_thread_id, epoch):
    return f"parallel_run_tasks:{parent_thread_id}:{epoch}"


def _k_parent_task(parent_task_id):
    return f"parallel_parent_task:{parent_task_id}"


def _epoch_ref(parent_thread_id, epoch):
    """A self-describing (parent_thread_id, epoch) set member.

    JSON (not a delimited string) so a thread_id containing ':' can never be
    mis-split — child/parent thread ids are not guaranteed colon-free.
    """
    return json.dumps({'p': parent_thread_id, 'e': epoch}, sort_keys=True)


def _parse_epoch_ref(ref):
    try:
        d = json.loads(ref)
        return d.get('p'), d.get('e')
    except Exception:  # pylint: disable=W0703
        return None, None


class Method:  # pylint: disable=E1101,R0903,W0201
    """ Parallel dispatch coordination methods (same-pylon @web.method). """

    @web.method()
    def parallel_dispatch_launch_children(self, parent_task_id, parent_meta, parent_result):
        """Launch one durable indexer_agent per parked child spec.

        Reads the parked parent's result (specs + per-child launch payloads +
        the parent's own reconcile payload), stashes the reconcile payload in
        Redis, primes the gate counter to N BEFORE any child can fire its
        terminal event, then start_task's each child with parent-linkage meta
        carrying the reconcile_epoch. Dispatch-then-compensate: a child that
        fails to launch is settled immediately (its gate slot released) so the
        gate never hangs on it.
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
        # The sio_event/question_id the live UI subscribed to. The browser joined
        # room_{sio_event}_{stream_id} (chat_predict) via chat_enter_room; the
        # indexer routes each emitted event to that room from task_meta['sio_event']
        # (defaulting to application_predict when absent). Children inherit BOTH so
        # their live chunks, tool chips, and HITL cards land in the SAME room the
        # parent's events do — without this a child emits into an unsubscribed
        # application_predict room and the UI sees nothing (#4993 Track 2 stall).
        parent_sio_event = parent_meta.get('sio_event')
        parent_question_id = parent_meta.get('question_id')
        # The tenant the chat lives in. chat_message_stream_end opens its DB
        # session with response_metadata['chat_project_id'] (stamped from
        # task_meta['chat_project_id']); without it the session resolves the
        # literal 'tenant' placeholder schema and the finalize INSERT throws
        # UndefinedTable, so the reconciled parent's message is never persisted
        # (stays is_streaming=true, no content/items) — the final answer + child
        # attribution vanish on reload (#4993 Track 2). user_context is carried
        # for parity with the child meta so the re-invoked run authenticates the
        # same way.
        parent_chat_project_id = parent_meta.get('chat_project_id')
        parent_user_context = parent_meta.get('user_context')

        epoch = uuid4().hex

        # Stash the self-contained reconcile re-invoke payload in Redis, keyed by
        # (parent_thread_id, epoch). parent_task_id is carried so the reconcile
        # cleanup can SREM this epoch from the parent_task set.
        self._parallel_reconcile_stash(
            parent_thread_id, epoch,
            {
                'reconcile_payload': reconcile_payload,
                'parent_task_name': parent_task_name,
                'parent_task_id': parent_task_id,
                'parent_stream_id': parent_stream_id,
                'parent_message_id': parent_message_id,
                'project_id': project_id,
                # The reconcile re-invoke synthesizes the FINAL answer and streams
                # it on the parent stream — it must emit into the SAME sio room the
                # browser joined, exactly like the children. Without these the
                # re-invoked parent defaults to application_predict (unsubscribed)
                # and the orchestrator's final answer never reaches the UI (#4993).
                'sio_event': parent_sio_event,
                'question_id': parent_question_id,
                # Tenant + auth for the reconcile run's DB finalize (see above).
                'chat_project_id': parent_chat_project_id,
                'user_context': parent_user_context,
            },
        )

        # Prime the gate to the full roster BEFORE launching any child, so a
        # fast child's terminal event can never see a partial count. One
        # round-trip: SET remaining=N, register this epoch under the parent's
        # task_id (for stop fan-out), and TTL both.
        self._parallel_prime_gate(parent_thread_id, epoch, len(specs), parent_task_id)

        log.info(
            "[PARALLEL] launching %d child(ren) for parent_thread_id=%s epoch=%s",
            len(specs), parent_thread_id, epoch,
        )

        task_id_map = {}
        for i, spec in enumerate(specs):
            child_thread_id = spec.get('child_thread_id')
            child_payload = spec.get('child_payload')
            if not child_payload:
                log.warning("[PARALLEL] spec %s missing child_payload; settling as error", child_thread_id)
                self._parallel_settle_child(parent_thread_id, epoch, child_thread_id)
                continue
            child_meta = {
                'task_name': 'indexer_agent',
                'project_id': project_id,
                'user_context': parent_meta.get('user_context'),
                'chat_project_id': parent_meta.get('chat_project_id'),
                # Route the child's live events to the SAME sio room the browser
                # joined for the parent (chat_predict / parent stream). Without
                # these the indexer defaults to application_predict and the
                # child's chunks + HITL card emit into an unsubscribed room.
                'sio_event': parent_sio_event,
                'question_id': parent_question_id,
                # Parent linkage — presence of reconcile_epoch is how
                # task_status_changed recognizes a child terminal event.
                'parent_task_id': parent_task_id,
                'parent_thread_id': parent_thread_id,
                'child_thread_id': child_thread_id,
                'child_index': spec.get('index', i),
                'reconcile_epoch': epoch,
                # Child identity for the indexer to stamp onto every event this
                # child emits (so the UI attributes the child's live chips +
                # HITL card to its own sub-agent accordion) and for HITL-resume
                # decision matching by tool_call_id. The display name mirrors
                # the chip parenthetical the in-process path stamps as
                # parent_agent_name (#4993 Track 2).
                'tool_call_id': spec.get('tool_call_id'),
                'subagent_name': spec.get('display_name') or spec.get('name'),
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
            # Pool saturation (start_task returns None) or a raise: settle so the
            # gate does not wait forever on an undispatched child.
            if child_task_id is None:
                log.warning("[PARALLEL] child %s not dispatched (saturation?); settling as error", child_thread_id)
                self._parallel_settle_child(parent_thread_id, epoch, child_thread_id)
            else:
                # Record the child's own arbiter task_id so a chat stop can
                # stop_task() each live child, not just the parent.
                if child_thread_id:
                    task_id_map[child_thread_id] = child_task_id

        # One batched HSET for every launched child's task_id (replaces the old
        # N separate per-child UPDATEs).
        if task_id_map:
            self._parallel_set_child_task_ids(parent_thread_id, epoch, task_id_map)

    @web.method()
    def parallel_dispatch_on_child_terminal(self, child_meta, child_result):
        """Run the cheap reconcile gate when a parked-child task stops.

        A child that PAUSED for HITL also fires `stopped` but carries
        hitl_interrupt in its result — it is still open, so do NOT settle it and
        do NOT open the gate (the user will approve, the child resumes, and fires
        `stopped` again as completed). Only a completed/errored child settles.
        """
        parent_thread_id = child_meta.get('parent_thread_id')
        epoch = child_meta.get('reconcile_epoch')
        child_thread_id = child_meta.get('child_thread_id')
        if not (parent_thread_id and epoch and child_thread_id):
            return

        # Chat stopped mid fan-out: the stop_task kill makes each child fire a
        # terminal `stopped`. Refuse to advance the gate so the parent is never
        # re-invoked with a final answer for a chat the user cancelled (#4993).
        if self._parallel_epoch_cancelled(parent_thread_id, epoch):
            log.info("[PARALLEL] child %s terminal but epoch cancelled; gate not advanced", child_thread_id)
            return

        # HITL-paused child: still open, not terminal.
        if isinstance(child_result, dict) and child_result.get('hitl_interrupt'):
            log.info("[PARALLEL] child %s paused for HITL; gate not advanced", child_thread_id)
            return

        self._parallel_settle_child(parent_thread_id, epoch, child_thread_id)

    # -- Reconcile gate (atomic Redis, no DB, no lease) -----------------------

    @web.method()
    def _parallel_prime_gate(self, parent_thread_id, epoch, n_children, parent_task_id):
        """Prime the gate counter to N and register the epoch for stop fan-out.

        One round-trip via a pipeline: SET remaining=N (TTL), add this epoch to
        the parent_task set (TTL) so a chat stop can find it. Called once per
        fan-out before any child launches, so the roster is complete before any
        terminal event can fire.
        """
        try:
            client = self.get_redis_client()  # pylint: disable=E1101
            pipe = client.pipeline(transaction=False)
            pipe.set(_k_remaining(parent_thread_id, epoch), int(n_children), ex=_RECONCILE_PAYLOAD_TTL)
            if parent_task_id:
                pipe.sadd(_k_parent_task(parent_task_id), _epoch_ref(parent_thread_id, epoch))
                pipe.expire(_k_parent_task(parent_task_id), _RECONCILE_PAYLOAD_TTL)
            pipe.execute()
        except Exception:  # pylint: disable=W0703
            log.exception("[PARALLEL] prime gate failed (parent=%s epoch=%s)", parent_thread_id, epoch)

    @web.method()
    def _parallel_settle_child(self, parent_thread_id, epoch, child_thread_id):
        """Atomically settle one child; reconcile iff it was the last.

        Single Lua round-trip (SADD-done idempotency + DECR-remaining). The
        greenlet whose DECR returns 0 is the unique winner and re-invokes the
        parent. A duplicate terminal for the same child returns -1 and is a
        no-op. No lease, no DB.
        """
        if not (parent_thread_id and epoch and child_thread_id):
            return
        try:
            client = self.get_redis_client()  # pylint: disable=E1101
            remaining = client.eval(
                _SETTLE_LUA, 2,
                _k_done(parent_thread_id, epoch),
                _k_remaining(parent_thread_id, epoch),
                child_thread_id, _RECONCILE_PAYLOAD_TTL,
            )
        except Exception:  # pylint: disable=W0703
            log.exception("[PARALLEL] settle child failed (%s)", child_thread_id)
            return
        try:
            remaining = int(remaining)
        except (TypeError, ValueError):
            return
        if remaining == -1:
            # Duplicate terminal for an already-settled child — ignore.
            return
        if remaining <= 0:
            self._parallel_reinvoke_parent(parent_thread_id, epoch)

    @web.method()
    def _parallel_set_child_task_ids(self, parent_thread_id, epoch, task_id_map):
        """Record launched children's arbiter task_ids in one HSET (for stop)."""
        if not task_id_map:
            return
        try:
            client = self.get_redis_client()  # pylint: disable=E1101
            pipe = client.pipeline(transaction=False)
            pipe.hset(_k_tasks(parent_thread_id, epoch), mapping=dict(task_id_map))
            pipe.expire(_k_tasks(parent_thread_id, epoch), _RECONCILE_PAYLOAD_TTL)
            pipe.execute()
        except Exception:  # pylint: disable=W0703
            log.exception("[PARALLEL] set child task ids failed (parent=%s epoch=%s)", parent_thread_id, epoch)

    @web.method()
    def _parallel_epoch_cleanup(self, parent_thread_id, epoch, parent_task_id=None):
        """Delete an epoch's coordination keys (consumed on reconcile)."""
        try:
            client = self.get_redis_client()  # pylint: disable=E1101
            pipe = client.pipeline(transaction=False)
            pipe.delete(
                _k_remaining(parent_thread_id, epoch),
                _k_done(parent_thread_id, epoch),
                _k_tasks(parent_thread_id, epoch),
            )
            if parent_task_id:
                pipe.srem(_k_parent_task(parent_task_id), _epoch_ref(parent_thread_id, epoch))
            pipe.execute()
        except Exception:  # pylint: disable=W0703
            log.exception("[PARALLEL] epoch cleanup failed (parent=%s epoch=%s)", parent_thread_id, epoch)

    # -- Re-invoke ------------------------------------------------------------

    @web.method()
    def _parallel_reinvoke_parent(self, parent_thread_id, epoch):
        """Re-invoke the parked parent as a FRESH task with parallel_reconcile.

        Reads the stashed self-contained payload, stamps the epoch, and
        start_task's the same parent task type on the same stream/message. Heavy
        assembly (reading each child's checkpoint, building ToolMessages) happens
        inside that fork-pool invocation — never here on the hub. Coordination
        keys are then deleted; the epoch is done.
        """
        stash = self._parallel_reconcile_unstash(parent_thread_id, epoch)
        if not stash:
            log.warning("[PARALLEL] no reconcile stash for parent=%s epoch=%s", parent_thread_id, epoch)
            self._parallel_epoch_cleanup(parent_thread_id, epoch)
            return

        parent_task_id = stash.get('parent_task_id')
        payload = dict(stash.get('reconcile_payload') or {})
        payload['parallel_reconcile'] = epoch
        payload['thread_id'] = parent_thread_id
        parent_task_name = stash.get('parent_task_name', 'indexer_agent')
        project_id = stash.get('project_id')

        meta = {
            'task_name': parent_task_name,
            'project_id': project_id,
            # Route the re-invoked parent's live events (the synthesized final
            # answer chunks + agent_response) to the SAME sio room the browser
            # joined. create_node_interface derives the room from these; absent
            # them the indexer defaults to application_predict (unsubscribed) and
            # the orchestrator's final answer never reaches the UI (#4993).
            'sio_event': stash.get('sio_event'),
            'question_id': stash.get('question_id'),
            # Tenant + auth so the reconcile run's chat_message_stream_end opens
            # the project schema (not the 'tenant' placeholder) and persists the
            # finalized parent message — without chat_project_id the finalize
            # INSERT throws UndefinedTable and the answer is never stored (#4993).
            'chat_project_id': stash.get('chat_project_id'),
            'user_context': stash.get('user_context'),
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
            self._parallel_epoch_cleanup(parent_thread_id, epoch, parent_task_id)

    # -- Stop fan-out (chat stop button) --------------------------------------

    @web.method()
    def parallel_dispatch_stop_children(self, parent_task_id):
        """Stop all live children spawned by a parked parent (chat stop button).

        The chat stop button reaches stop_task with ONLY the parent's task_id
        (msg_group.task_id). In park+spawn the parent has already gone terminal,
        but its N children run as independent durable indexer_agent tasks that no
        single stop_task reaches. This enumerates this run's children by
        parent_task_id, arbiter-stops each one, flags every owned epoch cancelled
        so the reconcile gate never re-invokes the parent with a final answer for
        a chat the user stopped, drops the per-child HITL stashes, and clears the
        epoch coordination keys (#4993 Track 2). Zero DB.

        Returns the number of children stopped (0 = not a fan-out run, the
        common case — ordinary single-agent chats register no epochs).
        """
        if not parent_task_id:
            return 0
        try:
            client = self.get_redis_client()  # pylint: disable=E1101
            epoch_refs = client.smembers(_k_parent_task(parent_task_id))
        except Exception:  # pylint: disable=W0703
            log.exception("[PARALLEL] stop children: parent_task lookup failed (%s)", parent_task_id)
            return 0
        if not epoch_refs:
            return 0

        stopped = 0
        epoch_count = 0
        for ref in epoch_refs:
            parent_thread_id, epoch = _parse_epoch_ref(ref)
            if not (parent_thread_id and epoch):
                continue
            epoch_count += 1

            # Flag cancelled BEFORE stopping, so a child's terminal `stopped`
            # event (fired by the stop_task kill) cannot win the race into the
            # reconcile gate. The cancel flag is a separate key with its own TTL,
            # so it outlives the epoch keys we delete below.
            self._parallel_mark_epoch_cancelled(parent_thread_id, epoch)

            try:
                tasks = client.hgetall(_k_tasks(parent_thread_id, epoch)) or {}
            except Exception:  # pylint: disable=W0703
                log.exception("[PARALLEL] stop children: tasks lookup failed (epoch=%s)", epoch)
                tasks = {}

            for child_thread_id, child_task_id in tasks.items():
                if child_task_id:
                    try:
                        self.task_node.stop_task(child_task_id)  # pylint: disable=E1101
                        stopped += 1
                    except Exception:  # pylint: disable=W0703
                        log.exception("[PARALLEL] failed to stop child task %s", child_task_id)
                # Drop the child's HITL launch stash so a stopped child cannot be
                # resumed from a stale paused card.
                self._parallel_child_unstash(child_thread_id)

            # Drop the epoch coordination keys (the cancel flag persists).
            self._parallel_epoch_cleanup(parent_thread_id, epoch, parent_task_id)

        log.info(
            "[PARALLEL] stop fan-out: parent_task_id=%s stopped %d child task(s) across %d epoch(s)",
            parent_task_id, stopped, epoch_count,
        )
        return stopped

    # -- Redis (cancel flag, stashes, chat-run-stopped) -----------------------

    @web.method()
    def _parallel_mark_epoch_cancelled(self, parent_thread_id, epoch):
        """Set the per-epoch cancel flag (chat stopped mid fan-out).

        Decoupled from the gate counter (which gets deleted) so a child's
        terminal `stopped` event — fired by the stop_task kill, possibly after
        the gate keys are gone — still finds the flag and refuses to open the
        reconcile gate. TTL matches the reconcile payload's human-think-time
        window.
        """
        key = f"parallel_reconcile_cancelled:{parent_thread_id}:{epoch}"
        try:
            client = self.get_redis_client()  # pylint: disable=E1101
            client.set(key, '1', ex=_RECONCILE_PAYLOAD_TTL)
        except Exception:  # pylint: disable=W0703
            log.exception("[PARALLEL] mark epoch cancelled failed")

    @web.method()
    def _parallel_epoch_cancelled(self, parent_thread_id, epoch):
        """True if this epoch was cancelled by a chat stop."""
        key = f"parallel_reconcile_cancelled:{parent_thread_id}:{epoch}"
        try:
            client = self.get_redis_client()  # pylint: disable=E1101
            return client.get(key) is not None
        except Exception:  # pylint: disable=W0703
            log.exception("[PARALLEL] epoch-cancelled check failed")
            # Fail open: if Redis is unreachable, do not silently swallow a
            # legitimate reconcile — the gate's own guards still apply.
            return False

    @web.method()
    def mark_chat_run_stopped(self, message_uuid):
        """Flag a chat response message's run as stopped (frozen).

        Set by the stop button. Any later HITL resume / continue on this message
        is refused so a stale approval card cannot re-invoke the parent and
        re-fan-out the children (#4993 Track 2). Keyed by the response message
        uuid — the same id the stop API and the continue resume both carry, so
        it guards every resume variant (fan-out child OR parent continue).
        """
        if not message_uuid:
            return
        key = f"chat_run_stopped:{message_uuid}"
        try:
            client = self.get_redis_client()  # pylint: disable=E1101
            client.set(key, '1', ex=_RECONCILE_PAYLOAD_TTL)
        except Exception:  # pylint: disable=W0703
            log.exception("[PARALLEL] mark chat run stopped failed (%s)", message_uuid)

    @web.method()
    def is_chat_run_stopped(self, message_uuid):
        """True if this chat response message's run was stopped by the user."""
        if not message_uuid:
            return False
        key = f"chat_run_stopped:{message_uuid}"
        try:
            client = self.get_redis_client()  # pylint: disable=E1101
            return client.get(key) is not None
        except Exception:  # pylint: disable=W0703
            log.exception("[PARALLEL] chat-run-stopped check failed (%s)", message_uuid)
            # Fail open: do not block a legitimate resume on a Redis blip.
            return False

    @web.method()
    def clear_chat_run_stopped(self, message_uuid):
        """Clear the stopped flag (e.g. when the user sends a fresh message)."""
        if not message_uuid:
            return
        key = f"chat_run_stopped:{message_uuid}"
        try:
            client = self.get_redis_client()  # pylint: disable=E1101
            client.delete(key)
        except Exception:  # pylint: disable=W0703
            log.exception("[PARALLEL] clear chat run stopped failed (%s)", message_uuid)

    @web.method()
    def _parallel_child_unstash(self, child_thread_id):
        """Drop a child's HITL-resume launch stash (used on cancel)."""
        if not child_thread_id:
            return
        key = f"parallel_child_launch:{child_thread_id}"
        try:
            client = self.get_redis_client()  # pylint: disable=E1101
            client.delete(key)
        except Exception:  # pylint: disable=W0703
            log.exception("[PARALLEL] child unstash failed (%s)", child_thread_id)

    @web.method()
    def _parallel_reconcile_stash(self, parent_thread_id, epoch, value):
        key = f"parallel_reconcile_payload:{parent_thread_id}:{epoch}"
        try:
            client = self.get_redis_client()  # pylint: disable=E1101
            # default=str so a stray non-serialisable leaf degrades that leaf to
            # its string form rather than raising — a dropped reconcile payload
            # means the parked parent never reconciles (the run hangs forever).
            client.set(key, json.dumps(value, default=str), ex=_RECONCILE_PAYLOAD_TTL)
        except Exception:  # pylint: disable=W0703
            log.exception("[PARALLEL] reconcile stash failed")

    @web.method()
    def _parallel_reconcile_unstash(self, parent_thread_id, epoch):
        key = f"parallel_reconcile_payload:{parent_thread_id}:{epoch}"
        try:
            client = self.get_redis_client()  # pylint: disable=E1101
            raw = client.get(key)
            if raw is None:
                return None
            # Parse BEFORE delete: if decoding fails (corrupt/stale/partial
            # write) the key survives under its TTL for inspection instead of
            # being lost. The Redis-counter gate guarantees a single winner
            # reaches this point, so reordering opens no double-reconcile window.
            payload = json.loads(raw)
            client.delete(key)
            return payload
        except Exception:  # pylint: disable=W0703
            log.exception("[PARALLEL] reconcile unstash failed")
            return None

    @web.method()
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
                # default=str: a non-serialisable leaf must not blow up the
                # stash — a missing child stash silently breaks HITL resume for
                # that child (it can't be replayed on its own thread).
                json.dumps({
                    'child_payload': child_payload,
                    'child_meta': child_meta,
                    'parent_stream_id': parent_stream_id,
                    'parent_message_id': parent_message_id,
                }, default=str),
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
