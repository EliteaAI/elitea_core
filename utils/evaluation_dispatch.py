"""Wait for a dispatched sub-agent fan-out in a batch eval case (#7015 P2).

With ``parallel_subagent_dispatch`` on, an agent that calls several sub-agents parks: the
indexer returns ``build_parked_result`` and the children run as their own tasks. Once every child
is terminal, ``parallel_dispatch`` re-invokes the parent as a fresh *reconcile* task, which writes
the final answer. A chat sees that answer arrive over the socket; a batch run has only the parked
result, so before #7015 the case failed as ``parked``.

:func:`await_dispatch` instead waits for the answer inside the case's own time limit:

1. poll ``parallel_dispatch_reconcile_pointer(thread_id, epoch)`` until the epoch is reconciled;
2. join the reconcile task for the time left;
3. if the reconcile parks again (a second wave of sub-agents), repeat, up to :data:`MAX_WAVES`.

The case's evidence is the parent's steps before each park, every child's steps (read from the
task results before their retention expires) and the reconcile's steps, merged into one envelope
(:func:`merge_envelopes`), so the trajectory and the usage cover the whole fan-out. Child steps
already carry ``parent_agent_name`` from the indexer, so they render under the sub-agent.

On the deadline the reconcile and any live children are stopped and the merged partial evidence is
returned, so a timed-out case still shows how far the fan-out got.

The I/O is passed in (:class:`DispatchIO`), so this module has no pylon import and is tested with
stubs."""
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

#: How many times one case may park and reconcile before it is given up as runaway.
MAX_WAVES = 8
POLL_INTERVAL_S = 1.0

#: ``state`` values of :func:`await_dispatch`
RECONCILED = 'reconciled'
TIMED_OUT = 'timeout'
NOT_RECONCILED = 'dispatch_not_reconciled'
TOO_MANY_WAVES = 'dispatch_too_many_waves'

_TOKEN_TOTALS = ('chat_history_tokens_input', 'llm_response_tokens_output')
# Keys that describe the park itself, not the run: dropped from the merged envelope.
_PARK_KEYS = ('parallel_parked', 'dispatch_epoch', 'parallel_dispatch', 'reconcile_payload',
              'parent_stream_id', 'parent_message_id')


@dataclass
class DispatchIO:
    """What :func:`await_dispatch` needs from the platform.

    ``pointer(thread_id, epoch)`` → ``{task_id, children}`` once reconciled, else None;
    ``epoch_children(thread_id, epoch)`` → child_thread_id → task_id while the epoch is live;
    ``join(task_id, timeout)`` → the task's result, or ``...`` on timeout;
    ``task_result(task_id)`` → a finished task's result, or ``...``/None;
    ``stop_task(task_id)`` / ``stop_epoch(thread_id, epoch)`` cancel what is left on the deadline."""
    pointer: Callable[[str, str], Optional[dict]]
    epoch_children: Callable[[str, str], Dict[str, str]]
    join: Callable[[str, float], Any]
    task_result: Callable[[str], Any]
    stop_task: Callable[[str], Any]
    stop_epoch: Callable[[str, str], Any]
    sleep: Callable[[float], Any] = time.sleep
    clock: Callable[[], float] = time.monotonic


def _inner(result) -> Optional[dict]:
    if not isinstance(result, dict):
        return None
    inner = result.get('result', result)
    return inner if isinstance(inner, dict) else None


def _parked(result) -> Optional[dict]:
    inner = _inner(result)
    return inner if inner and inner.get('parallel_parked') else None


def merge_envelopes(parts: List[Any]) -> Optional[dict]:
    """One ``{'result': envelope}`` holding every part's steps, in the order given.

    The last part is the run's outcome: its answer, chat history and errors are kept. Steps and
    tool calls are concatenated (a tool call id seen twice keeps the later entry, which is the
    reconcile's finished one), token totals are summed, and the trajectory ``source`` says the
    evidence spans a dispatch. Returns None when no part has an envelope."""
    inners = [inner for inner in (_inner(part) for part in parts) if inner is not None]
    if not inners:
        return None
    merged = {k: v for k, v in inners[-1].items() if k not in _PARK_KEYS}
    steps, tool_calls, sources = [], {}, set()
    totals = dict.fromkeys(_TOKEN_TOTALS)
    for inner in inners:
        steps.extend(s for s in inner.get('thinking_steps') or [] if isinstance(s, dict))
        if isinstance(inner.get('tool_calls_dict'), dict):
            tool_calls.update(inner['tool_calls_dict'])
        for key in _TOKEN_TOTALS:
            if isinstance(inner.get(key), (int, float)):
                totals[key] = (totals[key] or 0) + inner[key]
        if inner.get('token_source'):
            sources.add(inner['token_source'])
    merged['thinking_steps'] = steps
    merged['tool_calls_dict'] = tool_calls
    merged.update({k: v for k, v in totals.items() if v is not None})
    if sources:
        if len(sources) > 1:
            # 'none' (no callback) on one part must not hide the counts of the others
            sources.discard('none')
        merged['token_source'] = sources.pop() if len(sources) == 1 else 'mixed'
    if inners[0].get('pipeline_trace') and 'pipeline_trace' not in merged:
        merged['pipeline_trace'] = inners[0]['pipeline_trace']
    merged['trajectory_source'] = 'predict_result+dispatch'
    return {'result': merged}


def _child_results(io: DispatchIO, children: Dict[str, str]) -> List[Any]:
    results = []
    for task_id in children.values():
        try:
            result = io.task_result(task_id)
        except Exception:  # noqa: BLE001 - a lost child result only thins the evidence
            continue
        if isinstance(result, dict):
            results.append(result)
    return results


def await_dispatch(parked_result: Any, *, deadline: float, io: DispatchIO,
                   max_waves: int = MAX_WAVES) -> dict:
    """Wait for a parked case's fan-out to reconcile, until ``deadline`` (``io.clock`` time).

    Returns ``{'state', 'result', 'waves', 'children'}``. ``result`` is the merged envelope:
    the final answer when ``state`` is :data:`RECONCILED`, else the partial evidence."""
    parts: List[Any] = [parked_result]
    waves = children_seen = 0
    current = _parked(parked_result)

    def _done(state):
        return {'state': state, 'result': merge_envelopes(parts), 'waves': waves,
                'children': children_seen}

    while current is not None:
        if waves >= max_waves:
            return _done(TOO_MANY_WAVES)
        waves += 1
        thread_id, epoch = current.get('thread_id'), current.get('dispatch_epoch')
        if not (thread_id and epoch):
            return _done(NOT_RECONCILED)

        pointer = None
        while pointer is None:
            try:
                pointer = io.pointer(thread_id, epoch)
            except Exception:  # noqa: BLE001 - a Redis blip is retried until the deadline
                pointer = None
            if pointer is not None or io.clock() >= deadline:
                break
            io.sleep(min(POLL_INTERVAL_S, max(deadline - io.clock(), 0)))

        if pointer is None:
            # The children are still running (or the reconcile never started): keep what the
            # finished ones did, then stop the epoch so nothing runs on after the case.
            try:
                live = io.epoch_children(thread_id, epoch) or {}
            except Exception:  # noqa: BLE001
                live = {}
            children_seen += len(live)
            parts.extend(_child_results(io, live))
            try:
                io.stop_epoch(thread_id, epoch)
            except Exception:  # noqa: BLE001
                pass
            return _done(NOT_RECONCILED)

        children = pointer.get('children') or {}
        children_seen += len(children)
        parts.extend(_child_results(io, children))
        reconcile_id = pointer.get('task_id')
        remaining = deadline - io.clock()
        result = io.join(reconcile_id, remaining) if reconcile_id and remaining > 0 else ...
        if result is ... or result is None:
            if reconcile_id:
                try:
                    io.stop_task(reconcile_id)
                except Exception:  # noqa: BLE001
                    pass
            return _done(TIMED_OUT)
        parts.append(result)
        current = _parked(result)

    return _done(RECONCILED)
