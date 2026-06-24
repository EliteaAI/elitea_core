"""Pure, dependency-free helpers for collapsing HITL-replay duplicate tool_calls.

Extracted from ``message_stream.py`` so the dedup logic can be unit-tested in
isolation (it imports only ``json``). ``message_stream`` re-imports these names,
so the public behaviour and call sites are unchanged.

Two concerns are handled here:

* **Identity** (``_tool_call_identity``) — what makes two tool_call entries "the
  same logical call" across HITL-replay re-fires (#4993).
* **Dedup** (``_dedupe_replayed_tool_calls``) — collapsing the re-fires while
  keeping genuinely separate *invocations* of the same sub-agent distinct
  (#5386). Separation is by *completion epoch*: a real completion (truthy
  ``tool_output``) closes an epoch, so the next same-identity entry opens a new
  one. HITL pauses never carry a truthy ``tool_output`` (a deferred pause sets it
  to ``""``; an error/interrupt sets it to ``None``), so replays of a single
  invocation stay collapsed while a second, genuinely re-issued invocation lands
  in its own epoch.
"""

import json


# Input keys that grow/echo across HITL replays and must NOT contribute to a
# tool_call's identity. A sub-agent invocation re-fired on each resume carries
# the same logical args (its "task") plus a swelling transient payload
# (accumulated hitl_decisions, echoed state channels). Hashing the whole dict
# makes each replay look unique; dropping these leaves only the stable args.
_TRANSIENT_INPUT_KEYS = {
    'hitl_decisions', 'state_types', 'parallel_tasks',
    'messages', 'chat_history', '_pipeline_blocked', 'hitl_resume',
    'hitl_action', 'hitl_value',
}


# Private key under which a tool_call's computed identity is memoized ON the
# entry itself. It persists through the meta-JSON DB round-trip, so a reloaded
# entry skips re-serialization on the next save (see _tool_call_identity).
# The UI reads named fields off each entry and ignores unknown keys, so this is
# inert to consumers.
_IDENTITY_CACHE_KEY = '_dedup_identity'


def _tool_call_identity(tc: dict) -> tuple:
    """Stable identity for a tool_call across HITL-replay re-fires (#4993).

    A pipeline sub-agent with N sensitive tool calls pauses for HITL N times.
    Each resume makes LangGraph replay the graph from its checkpoint and re-fire
    on_tool_start for the SAME embedded-agent node — a fresh run_id and a fresh
    checkpoint_ns uuid every time (e.g. "Agent1:<uuid-A>" then "Agent1:<uuid-B>").
    Only the final replay ever completes; the rest are empty placeholders. Two
    entries are "the same call" when tool name, sub-agent attribution, bare node
    name, and stable inputs match. Genuinely different calls (read_file on
    another path) keep distinct inputs and are never collapsed.

    Identity is memoized on the entry under `_IDENTITY_CACHE_KEY`. dedup runs on
    EVERY partial save over the whole accumulated tool_calls dict (fan-out
    children persist only via partial_message, never full_message), so without a
    cache each of K entries is json.dumps'd on each of S saves — O(K*S) ≈ O(K^2)
    CPU + transient-dict churn. The stamp persists through the meta-JSON DB
    round-trip; reloaded entries return their cached identity without
    re-serializing, making total work O(K) (one serialize per distinct entry).
    A replacement entry for the same run_id (e.g. on_tool_end adding output /
    parent_agent_name) arrives WITHOUT the stamp, so its identity is recomputed
    from current state — never served stale.
    """
    cached = tc.get(_IDENTITY_CACHE_KEY)
    if cached is not None:
        # Reloaded from JSON: list, not tuple. Coerce so it stays hashable for
        # use as a dict key in _dedupe_replayed_tool_calls.
        return tuple(cached) if isinstance(cached, list) else cached

    meta = tc.get('metadata') or {}
    tool_meta = tc.get('tool_meta') or {}
    tm_meta = tool_meta.get('metadata') if isinstance(tool_meta, dict) else {}
    tm_meta = tm_meta or {}
    name = tool_meta.get('name') or tc.get('tool_name') or ''
    parent = meta.get('parent_agent_name') or tm_meta.get('parent_agent_name') or ''
    raw_ns = meta.get('checkpoint_ns') or ''
    node = raw_ns.split(':', 1)[0] if raw_ns else (meta.get('langgraph_node') or '')
    raw_inputs = tc.get('tool_inputs')
    if isinstance(raw_inputs, dict):
        stable_inputs = {k: v for k, v in raw_inputs.items() if k not in _TRANSIENT_INPUT_KEYS}
    else:
        stable_inputs = raw_inputs
    try:
        inputs = json.dumps(stable_inputs, sort_keys=True, default=str)
    except (TypeError, ValueError):
        inputs = str(stable_inputs)
    identity = (name, parent, node, inputs)
    # Stamp the entry so subsequent saves reuse this without re-serializing.
    # All four components are strings → JSON-safe; stored as a list and coerced
    # back to a tuple on the cached-read path above.
    if isinstance(tc, dict):
        tc[_IDENTITY_CACHE_KEY] = list(identity)
    return identity


def _completeness(tc: dict) -> int:
    """Rank how "finished" a tool_call entry is. Higher wins within an epoch.

    A real result (truthy ``tool_output``) outranks a merely-finished entry
    (``timestamp_finish`` set, e.g. an errored HITL pause), which outranks a bare
    placeholder. Used to pick the survivor among same-identity, same-epoch
    re-fires.
    """
    if not isinstance(tc, dict):
        return 0
    if tc.get('tool_output'):
        return 2
    if tc.get('timestamp_finish'):
        return 1
    return 0


def _is_real_completion(tc: dict) -> bool:
    """True when this entry is a genuine tool completion that closes an epoch.

    Only a real completion carries a truthy ``tool_output``. A HITL *deferred*
    pause (parallel sentinel) sets ``tool_output=""`` and an error/interrupt
    (sequential pause) sets it to ``None`` — both falsy — so neither closes an
    epoch. This is the single signal that separates two genuine invocations of
    the same sub-agent from the many re-fires of one invocation.
    """
    return bool(isinstance(tc, dict) and tc.get('tool_output'))


def _dedupe_replayed_tool_calls(tool_calls: dict) -> dict:
    """Collapse HITL-replay re-fires while keeping separate invocations distinct.

    Two failure modes pull in opposite directions and both must hold:

    * **#4993 (collapse):** within ONE invocation, a pipeline sub-agent pauses
      for HITL N times; each resume replays the graph and re-fires the embedded
      node with a fresh run_id, so the meta accumulates N near-empty placeholder
      chips and only the last completes. These must collapse to the single
      completed chip.
    * **#5386 (separate):** invoking the SAME sub-agent twice in sequence (same
      args) produces two genuine invocations that share one identity but must
      NOT collapse into one chip.

    The discriminator is the *completion epoch*. Walking entries in first-seen
    (≈ chronological) order, each entry is bucketed under ``(identity, epoch)``.
    A real completion — a truthy ``tool_output`` (see ``_is_real_completion``) —
    CLOSES the current epoch, so the next same-identity entry opens a new one.
    HITL pauses never carry a truthy ``tool_output`` (deferred → ``""``, error /
    interrupt → ``None``), so all the re-fires of one invocation stay in a single
    epoch and collapse (#4993), while a genuinely re-issued invocation lands in
    the next epoch and survives as its own chip (#5386). Within each epoch the
    most-complete entry wins (``_completeness``). First-seen order is preserved.
    Returns a dict still keyed by run_id; genuinely distinct calls keep separate
    identities and are never merged.
    """
    if not isinstance(tool_calls, dict) or len(tool_calls) < 2:
        return tool_calls

    epoch_counter: dict = {}  # identity -> index of the currently-open epoch
    best: dict = {}           # (identity, epoch) -> (run_id, tc)
    order: list = []          # (identity, epoch) keys in first-seen order
    for run_id, tc in tool_calls.items():
        if not isinstance(tc, dict):
            key = ('__nondict__', run_id)
            best[key] = (run_id, tc)
            order.append(key)
            continue
        identity = _tool_call_identity(tc)
        epoch = epoch_counter.get(identity, 0)
        key = (identity, epoch)
        if key not in best:
            best[key] = (run_id, tc)
            order.append(key)
        else:
            # Same invocation replayed within this epoch: keep the more complete.
            _, kept = best[key]
            if _completeness(tc) > _completeness(kept):
                best[key] = (run_id, tc)
        # A real completion closes the epoch so a later same-identity entry opens
        # a fresh one (a separate invocation), not merges into this one.
        if _is_real_completion(tc):
            epoch_counter[identity] = epoch + 1

    deduped: dict = {}
    for key in order:
        run_id, tc = best[key]
        deduped[run_id] = tc
    return deduped
