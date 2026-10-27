"""Wait for a dispatched sub-agent fan-out in a batch eval case (#7015 P2).

With ``parallel_subagent_dispatch`` on, an agent that calls several sub-agents parks: the
indexer returns ``build_parked_result`` and the children run as their own tasks. Once every child
is terminal, ``parallel_dispatch`` re-invokes the parent as a fresh *reconcile* task, which writes
the final answer. A chat sees that answer arrive over the socket; a batch run has only the parked
result, so before #7015 the case failed as ``parked``.

:func:`await_dispatch` instead waits for the answer inside the case's own time limit:

1. poll ``parallel_dispatch_reconcile_pointer(thread_id, epoch)`` until the epoch is reconciled;
2. join the reconcile task for the time left;
3. if the reconcile parks again (a second wave of sub-agents), repeat, up to :data:`MAX_WAVES`
   (past it, the last wave's children are stopped).

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
RECONCILE_FAILED = 'dispatch_reconcile_failed'
CANCELLED = 'dispatch_cancelled'

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
    trace = _merge_traces(inners)
    if trace is not None:
        merged['pipeline_trace'] = trace
    sub_agents = _dispatched_agents(inners)
    if sub_agents:
        merged['dispatched_agents'] = sub_agents
    merged['trajectory_source'] = 'predict_result+dispatch'
    return {'result': merged}


def _dispatched_agents(inners: List[dict]) -> List[dict]:
    """``[{name, display_name, application_id}]`` of every sub-agent the parts dispatched, once each.

    The ledger labels a child's calls with its application's id and cleaned ``name``, its steps
    carry the ``display_name``; settlement needs the id to tie the two (#7015 P3)."""
    seen, out = set(), []
    for inner in inners:
        for spec in inner.get('parallel_dispatch') or []:
            if not isinstance(spec, dict) or spec.get('application_id') is None:
                continue
            entry = {'name': spec.get('name'), 'display_name': spec.get('display_name'),
                     'application_id': spec['application_id']}
            key = tuple(entry.values())
            if key not in seen:
                seen.add(key)
                out.append(entry)
    return out


def _merge_traces(inners: List[dict]) -> Optional[dict]:
    """Every part's node spans, in order, so the nodes that ran before the park stay in the view.

    A part that parked inside a node reports it ``running``; the reconcile that resumes there
    reports it again, finished, so the open copy is dropped. ``truncated`` is set if any part's
    recorder hit its cap."""
    traces = [inner['pipeline_trace'] for inner in inners if isinstance(inner.get('pipeline_trace'), dict)]
    if not traces:
        return None
    spans: List[dict] = []
    for trace in traces:
        nxt = [span for span in trace.get('spans') or [] if isinstance(span, dict)]
        if spans and nxt and spans[-1].get('status') == 'running' and spans[-1].get('node') == nxt[0].get('node'):
            spans.pop()
        spans.extend(nxt)
    return {'spans': spans, 'truncated': any(bool(trace.get('truncated')) for trace in traces)}


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


def _cancel_requested(should_cancel) -> bool:
    try:
        return bool(should_cancel and should_cancel())
    except Exception:  # noqa: BLE001 - a failed cancel check is not a cancel
        return False


def await_dispatch(parked_result: Any, *, deadline: float, io: DispatchIO,
                   max_waves: int = MAX_WAVES, should_cancel: Optional[Callable[[], bool]] = None) -> dict:
    """Wait for a parked case's fan-out to reconcile, until ``deadline`` (``io.clock`` time).

    Returns ``{'state', 'result', 'waves', 'children'}``. ``result`` is the merged envelope:
    the final answer when ``state`` is :data:`RECONCILED`, else the partial evidence. Never raises:
    every platform call is guarded, so a failure becomes a state with the evidence gathered so far.

    ``should_cancel`` is polled while the children run: a stopped run stops the epoch and ends the
    case as :data:`CANCELLED` instead of waiting out its time limit. A reconcile that fails ends as
    :data:`RECONCILE_FAILED`; only the deadline gives :data:`TIMED_OUT`."""
    parts: List[Any] = [parked_result]
    waves = children_seen = 0
    current = _parked(parked_result)

    def _done(state):
        return {'state': state, 'result': merge_envelopes(parts), 'waves': waves,
                'children': children_seen}

    def _stop(thread_id, epoch):
        try:
            io.stop_epoch(thread_id, epoch)
        except Exception:  # noqa: BLE001
            pass

    while current is not None:
        thread_id, epoch = current.get('thread_id'), current.get('dispatch_epoch')
        if not (thread_id and epoch):
            return _done(NOT_RECONCILED)
        if waves >= max_waves:
            # This wave's children were launched with the park: stop them, or they run on (and
            # bill) after the case has ended.
            _stop(thread_id, epoch)
            return _done(TOO_MANY_WAVES)
        waves += 1

        pointer, cancelled = None, False
        while pointer is None:
            try:
                pointer = io.pointer(thread_id, epoch)
            except Exception:  # noqa: BLE001 - a Redis blip is retried until the deadline
                pointer = None
            if pointer is not None or io.clock() >= deadline:
                break
            cancelled = _cancel_requested(should_cancel)
            if cancelled:
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
            _stop(thread_id, epoch)
            return _done(CANCELLED if cancelled else TIMED_OUT)

        children = pointer.get('children') or {}
        children_seen += len(children)
        parts.extend(_child_results(io, children))
        reconcile_id = pointer.get('task_id')
        remaining = deadline - io.clock()
        failed = not reconcile_id
        try:
            result = io.join(reconcile_id, remaining) if reconcile_id and remaining > 0 else ...
        except Exception:  # noqa: BLE001 - a failed or cancelled reconcile is a state, never a raise
            result, failed = None, True
        if result is ... or result is None:
            if reconcile_id:
                try:
                    io.stop_task(reconcile_id)
                except Exception:  # noqa: BLE001
                    pass
            return _done(RECONCILE_FAILED if failed or result is None else TIMED_OUT)
        parts.append(result)
        current = _parked(result)

    return _done(RECONCILED)
