"""What an evaluated agent actually did on one case: its trajectory and counters (#6809, P1 item 1).

``run_agent`` used to read only the last assistant text out of the ``predict_sio`` envelope. The
same envelope already carries the run's evidence (indexer ``build_execution_evidence``, every
result builder since #6809 G1):

  * ``thinking_steps`` — one entry per LLM call, with ``message.usage_metadata`` (provider
    tokens), ``message.tool_calls`` (what the model asked for), a per-step ``token_source``,
    timestamps and, for a sub-agent's steps, ``parent_agent_name`` / ``parent_agent_call_id``;
  * ``tool_calls_dict`` — one ``ToolCallPayload`` per tool run (``tool_name``, ``tool_inputs``,
    ``tool_output``, ``finish_reason``, ``error``, timestamps, ``metadata``). A sub-agent call is a
    tool entry too. The ``tool_calls`` *list* is not used: its filter is provider-dependent.
  * ``pipeline_trace`` — a pipeline run only (#7015): one span per top-level node that ran, with
    its type, timing, status and the edge it took. Each becomes a ``kind: 'node'`` step, and the
    LLM and tool steps inside a node carry its name in ``node``. Pipeline state is never in it.

This module turns that envelope into the normalized, bounded shape stored in
``eval_case_execution`` (design §3.1, §3.3). It is pure — no I/O, no ORM — so the shape is
locked by unit tests against recorded envelopes.

``trajectory_state`` (design §13) never comes from an empty ``steps`` list: ``recorded`` with zero
steps is a real "the agent answered without tools", while a missing envelope is ``not_recorded``
with a reason, and a case where no agent ran is ``not_applicable``.
"""
import hashlib
import json
import math
import re
from datetime import datetime
from typing import Any, List, Optional

from .evaluation_masking import mask_payload, mask_text

TRAJECTORY_RECORDED = 'recorded'
TRAJECTORY_NOT_RECORDED = 'not_recorded'
TRAJECTORY_NOT_APPLICABLE = 'not_applicable'

# What a batch run does with an approval nobody can answer (design §4.5, Q-S10). P1 has one policy:
# the case fails. It is written on the case's pause and frozen into the run snapshot, so a later
# opt-in (deny-and-continue, P3) cannot change how an older run is read.
PAUSE_POLICY_FAIL_CASE = 'fail_case'

# Field caps. A tool output can be a 50 KB essay (a sub-agent's whole reply), and a case may run
# dozens of tools, so both the step and the whole trajectory are bounded. The trajectory cap
# mirrors ``MAX_ENVELOPE_BYTES``.
MAX_STEP_TEXT = 4_000
MAX_TRAJECTORY_BYTES = 256_000
_TRUNCATED_MARK = '… [truncated]'
# Plain text is cut to this before masking, so a multi-MB tool output is not scanned in full only
# to keep its first MAX_STEP_TEXT characters. The margin keeps the masking order safe: a secret cut
# in half here could only reach the stored text if masking removed over 3/4 of what precedes it.
# JSON text is parsed whole, since key masking needs the structure.
_PRE_MASK_TEXT = 4 * MAX_STEP_TEXT

_ENVELOPE_KEYS = ('thinking_steps', 'tool_calls_dict')
# The tool result the SDK's sensitive-tool guard returns when the user declined a call
# (``runtime/tools/llm.py`` SENSITIVE_TOOL_BLOCKED_RESULT_TYPE). The agent keeps going after it.
_BLOCKED_RESULT_TYPE = 'sensitive_tool_blocked'
# A tool that pauses the graph (``ask_user``, a HITL guard) surfaces LangGraph's interrupt as its error.
_INTERRUPT_MARKER = 'GraphInterrupt'
TOOL_STEP_PAUSED = 'paused'
# How a run that ran out of steps shows up: the SDK tool loop appends a fixed warning as the last
# AI message (``runtime/tools/llm.py``), and the LangGraph recursion limit raises an error.
_STEP_LIMIT_MARKERS = ('maximum tool execution iterations', 'recursion limit', 'graphrecursionerror')
# The SDK catches LangGraph's GraphRecursionError and answers with this sentence instead, so a run
# stopped by the graph-level limit looks like an ordinary reply (langraph_agent.py
# `_handle_graph_recursion_error`). Anchored so a reply that merely discusses limits does not match.
_GRAPH_STEP_LIMIT_REPLY = re.compile(r'^tool step limit \d+ reached for this run\.')
_ASSISTANT_ROLES = ('assistant', 'ai')
# Outcomes where the indexer never returned an envelope to read.
_NO_ENVELOPE_REASONS = {
    'timeout': 'timeout',
    'predict_exception': 'no_envelope',
    'budget_blocked': 'budget_blocked',
    'guardrail_blocked': 'guardrail_blocked',
}


def _inner(predict_result) -> Optional[dict]:
    """The indexer envelope: ``predict_sio`` wraps it in ``{'result': ...}``."""
    if not isinstance(predict_result, dict):
        return None
    inner = predict_result.get('result', predict_result)
    return inner if isinstance(inner, dict) else None


def has_envelope(predict_result) -> bool:
    inner = _inner(predict_result)
    return inner is not None and any(key in inner for key in _ENVELOPE_KEYS)


def _parse_ts(value) -> Optional[datetime]:
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value.replace('Z', '+00:00'))
    except ValueError:
        return None


def _duration_ms(start, finish) -> Optional[int]:
    started, finished = _parse_ts(start), _parse_ts(finish)
    if started is None or finished is None:
        return None
    try:
        return max(0, int((finished - started).total_seconds() * 1000))
    except TypeError:  # one naive, one aware
        return None


def _cap_text(value, limit: int = MAX_STEP_TEXT):
    if value is None:
        return None
    if not isinstance(value, str):
        try:
            value = json.dumps(value, ensure_ascii=False, default=str)
        except (TypeError, ValueError):
            value = str(value)
    if len(value) > limit:
        return value[:limit] + _TRUNCATED_MARK
    return value


def _masked(value):
    """:func:`mask_payload` then :func:`_cap_text`, reading at most ``_PRE_MASK_TEXT`` of plain text."""
    if isinstance(value, str) and len(value) > _PRE_MASK_TEXT and value.lstrip()[:1] not in ('{', '['):
        masked = mask_text(value[:_PRE_MASK_TEXT])
        # Masking may shorten the text below the cap; it was still cut.
        return masked[:MAX_STEP_TEXT] + _TRUNCATED_MARK
    return _cap_text(mask_payload(value))


def _cap_inputs(value):
    """Keep ``tool_inputs`` structured when small (P2 matches on args), else a capped string."""
    if value is None:
        return None
    try:
        encoded = json.dumps(value, ensure_ascii=False, default=str)
    except (TypeError, ValueError):
        return _cap_text(str(value))
    if len(encoded) <= MAX_STEP_TEXT:
        return value
    return encoded[:MAX_STEP_TEXT] + _TRUNCATED_MARK


def _llm_step(step: dict) -> dict:
    message = step.get('message') if isinstance(step.get('message'), dict) else {}
    generation_info = step.get('generation_info') if isinstance(step.get('generation_info'), dict) else {}
    response_metadata = message.get('response_metadata') if isinstance(message.get('response_metadata'), dict) else {}
    usage = message.get('usage_metadata') if isinstance(message.get('usage_metadata'), dict) else None
    tool_calls = message.get('tool_calls') if isinstance(message.get('tool_calls'), list) else []
    return {
        'kind': 'llm',
        'model': generation_info.get('model_name') or response_metadata.get('model_name'),
        # None, not 0, when the provider sent no usage: the step's token_source says it was estimated.
        'tokens': ({'in': usage.get('input_tokens'), 'out': usage.get('output_tokens')}
                   if usage is not None else None),
        'token_source': step.get('token_source'),
        'planned_tools': [c.get('name') for c in tool_calls if isinstance(c, dict) and c.get('name')],
        'finish_reason': generation_info.get('finish_reason'),
        'text': _masked(step['text']) if isinstance(step.get('text'), str) and step['text'] else None,
        'parent_agent': step.get('parent_agent_name'),
        'parent_call_id': step.get('parent_agent_call_id'),
        'node': step.get('langgraph_node') or None,
        'started_at': step.get('timestamp_start'),
        'duration_ms': _duration_ms(step.get('timestamp_start'), step.get('timestamp_finish')),
    }


def _is_blocked(tool_output) -> bool:
    if isinstance(tool_output, dict):
        return tool_output.get('type') == _BLOCKED_RESULT_TYPE
    if isinstance(tool_output, str) and _BLOCKED_RESULT_TYPE in tool_output:
        try:
            payload = json.loads(tool_output.strip())
        except ValueError:
            return False
        return isinstance(payload, dict) and payload.get('type') == _BLOCKED_RESULT_TYPE
    return False


def tool_step_status(entry: dict) -> str:
    """``ok`` | ``error`` | ``action_required`` | ``blocked`` | ``paused`` for one ``tool_calls_dict`` entry.

    ``action_required`` is the MCP-auth pause the indexer writes on the call; ``blocked`` is a
    sensitive tool the guard refused (design §4.5); ``paused`` is the call that raised the run's HITL
    pause. None of them is a tool failure. The pause itself is recorded once, on the trajectory
    (:func:`pause_details`).
    """
    finish_reason = entry.get('finish_reason')
    if finish_reason == 'action_required':
        return 'action_required'
    if _is_blocked(entry.get('tool_output')):
        return 'blocked'
    if _INTERRUPT_MARKER in str(entry.get('error') or ''):
        return TOOL_STEP_PAUSED
    if finish_reason == 'error' or entry.get('error'):
        return 'error'
    return 'ok'


def _identity(tool_inputs) -> str:
    """A digest of the unmasked inputs, so ``redundant_calls`` can tell apart two calls whose
    inputs differ only under a masked key. Dropped before the trajectory is stored."""
    return hashlib.sha256(_canonical(tool_inputs).encode('utf-8')).hexdigest()[:16]


def _tool_step(entry: dict, store_tool_outputs: bool = True) -> dict:
    """One tool step. Inputs, output and error are masked before they are capped (Q-S5), so a cap
    cannot cut a secret in half and leave the visible half unmasked. ``store_tool_outputs=False``
    (the suite opt-out) drops the output and flags the step ``output_omitted``."""
    metadata = entry.get('metadata') if isinstance(entry.get('metadata'), dict) else {}
    status = tool_step_status(entry)
    # The orchestrator's own sub-agent call carries ``parent_agent_call_id`` too — its *own* call
    # id, which the sub-agent's steps then point at — but no ``parent_agent_name``. Only a name
    # makes the step a child; without one the id identifies this call.
    parent_agent = metadata.get('parent_agent_name') or None
    call_id = metadata.get('parent_agent_call_id') or None
    step = {
        'kind': 'tool',
        'tool_name': entry.get('tool_name'),
        'toolkit': metadata.get('toolkit_name') or None,
        'toolkit_type': metadata.get('toolkit_type') or None,
        'tool_inputs': _cap_inputs(mask_payload(entry.get('tool_inputs'))),
        'tool_output': _masked(entry.get('tool_output')) if store_tool_outputs else None,
        'status': status,
        'is_error': status == 'error',
        # A paused call's "error" is the interrupt traceback; the pause keeps the identities.
        'error': None if status == TOOL_STEP_PAUSED else _masked(entry.get('error')),
        'finish_reason': entry.get('finish_reason'),
        'call_id': None if parent_agent else call_id,
        'parent_agent': parent_agent,
        'parent_call_id': call_id if parent_agent else None,
        'node': metadata.get('langgraph_node') or None,
        'started_at': entry.get('timestamp_start'),
        'duration_ms': _duration_ms(entry.get('timestamp_start'), entry.get('timestamp_finish')),
        '_identity': _identity(entry.get('tool_inputs')),
    }
    if not store_tool_outputs:
        step['output_omitted'] = True
    return step


def _node_step(span: dict) -> dict:
    """One pipeline node span (indexer ``pipeline_trace``). ``edge`` is how the node chose what
    runs next (``transitional``/``conditional``/``decision``, with ``next`` when known), ``hitl`` a
    human-in-the-loop interrupt or resume on it."""
    edge = span.get('edge') if isinstance(span.get('edge'), dict) else None
    hitl = span.get('hitl') if isinstance(span.get('hitl'), dict) else None
    return {
        'kind': 'node',
        'node': span.get('node'),
        'node_type': span.get('node_type'),
        'graph_step': span.get('step'),
        'status': span.get('status'),
        'edge': {'type': edge.get('type'), 'next': edge.get('next')} if edge else None,
        'hitl': {'action': hitl.get('action'), 'next': hitl.get('next')} if hitl else None,
        'error': _masked(span.get('error')),
        'started_at': span.get('timestamp_start'),
        'duration_ms': _duration_ms(span.get('timestamp_start'), span.get('timestamp_finish')),
    }


def _fit(trajectory: dict) -> dict:
    """Shrink a trajectory over ``MAX_TRAJECTORY_BYTES``: drop tool outputs and LLM text from the
    last step backwards (the order and counts matter more than late payloads), then drop trailing
    steps. Either way ``truncated`` is set, so a reader knows the view is partial.

    Each step is serialized once, and again only when it is shrunk: re-serializing the whole
    trajectory per step would be quadratic in the step count. The sum equals the full dump's
    length (``json.dumps`` joins list items with ``', '``)."""
    def _dump(value) -> int:
        return len(json.dumps(value, ensure_ascii=False, default=str))

    steps = trajectory['steps']
    sizes = [_dump(step) for step in steps]

    def _size() -> int:
        return (_dump({**trajectory, 'steps': []}) + sum(sizes) + 2 * max(len(sizes) - 1, 0))

    if _size() <= MAX_TRAJECTORY_BYTES:
        return trajectory
    trajectory['truncated'] = True
    for index in range(len(steps) - 1, -1, -1):
        step = steps[index]
        for key in ('tool_output', 'text', 'tool_inputs'):
            if step.get(key) is not None:
                step[key] = None
                step['output_omitted'] = True
        sizes[index] = _dump(step)
        if _size() <= MAX_TRAJECTORY_BYTES:
            return trajectory
    while steps and _size() > MAX_TRAJECTORY_BYTES:
        steps.pop()
        sizes.pop()
    return trajectory


def build_trajectory(predict_result, *, store_tool_outputs: bool = True) -> Optional[dict]:
    """The normalized trajectory (§3.3) of one ``predict_sio`` envelope, or None without one.

    Tool steps carry a private ``_identity`` for :func:`trajectory_metrics`;
    :func:`extract_execution` drops it before the trajectory is stored.

    Steps are ordered by ``started_at``; on a tie a node comes before the LLM steps it ran and
    those before tool steps, and insertion order breaks the rest. A step with no timestamp keeps
    its insertion position at the end."""
    inner = _inner(predict_result)
    if inner is None or not any(key in inner for key in _ENVELOPE_KEYS):
        return None

    raw: List[dict] = []
    trace = inner.get('pipeline_trace') if isinstance(inner.get('pipeline_trace'), dict) else None
    for span in (trace or {}).get('spans') or []:
        if isinstance(span, dict) and span.get('node'):
            raw.append(_node_step(span))
    for step in inner.get('thinking_steps') or []:
        if isinstance(step, dict):
            raw.append(_llm_step(step))
    tool_calls = inner.get('tool_calls_dict')
    for entry in (tool_calls.values() if isinstance(tool_calls, dict) else []):
        if isinstance(entry, dict):
            raw.append(_tool_step(entry, store_tool_outputs))

    def _order(item):
        seq, step = item
        started = _parse_ts(step.get('started_at'))
        return (started is None, started.timestamp() if started else 0.0, seq)

    steps = [step for _, step in sorted(enumerate(raw), key=_order)]
    for index, step in enumerate(steps):
        step['i'] = index
    trajectory = {
        'steps': steps,
        'tool_sequence': [s['tool_name'] for s in steps if s['kind'] == 'tool'],
        # The indexer stops recording spans past its cap: the node view is partial.
        'truncated': bool((trace or {}).get('truncated')),
        # A dispatched fan-out merges the parent, its children and the reconcile (#7015)
        'source': inner.get('trajectory_source') or 'predict_result',
        'tool_outputs_stored': store_tool_outputs,
    }
    if trace is not None:
        trajectory['node_sequence'] = [s['node'] for s in steps if s['kind'] == 'node']
    return _fit(trajectory)


def is_parked(predict_result) -> bool:
    """The run parked on a sub-agent fan-out (indexer ``build_parked_result``). A batch run
    cannot resume a parked parent, so the case has no answer."""
    inner = _inner(predict_result)
    return bool(inner and inner.get('parallel_parked'))


def pause_details(predict_result) -> Optional[dict]:
    """What the run paused on, or None when it did not pause (indexer ``build_success_result``).

    Only identities are kept; the interrupt's tool arguments and message stay out of storage.
    A parallel aggregate carries its paused calls in ``pending``; the first one is reported."""
    inner = _inner(predict_result)
    if not inner or not (inner.get('paused') or inner.get('hitl_interrupt') or inner.get('hitl_interrupts')):
        return None
    interrupts = [i for i in (inner.get('hitl_interrupts') or [inner.get('hitl_interrupt')])
                  if isinstance(i, dict)]
    first = interrupts[0] if interrupts else {}
    pending = first.get('pending') if isinstance(first.get('pending'), list) else []
    leaf = pending[0] if pending and isinstance(pending[0], dict) else first
    return {
        'pause_type': inner.get('pause_type') or 'hitl',
        'interaction_type': first.get('interaction_type'),
        'guardrail_type': first.get('guardrail_type'),
        'node_name': leaf.get('node_name') or first.get('node_name'),
        'tool_name': leaf.get('tool_name'),
        'toolkit_name': leaf.get('toolkit_name'),
        'interrupts': max(len(interrupts), len(pending)),
    }


def _mark_paused_steps(trajectory: dict) -> None:
    """When no call carried the interrupt marker, relabel the paused tool's last errored call.

    A guard pause can leave the error on the paused tool's own entry without a traceback. It is not
    a tool failure, so it must not count in ``tool_errors`` or ``retries``."""
    steps = [s for s in trajectory['steps'] if s.get('kind') == 'tool']
    tool_name = (trajectory.get('pause') or {}).get('tool_name')
    if not tool_name or any(s.get('status') == TOOL_STEP_PAUSED for s in steps):
        return
    errored = [s for s in steps if s.get('status') == 'error' and s.get('tool_name') == tool_name]
    if errored:
        errored[-1].update(status=TOOL_STEP_PAUSED, is_error=False)


def _final_assistant_text(predict_result) -> Optional[str]:
    inner = _inner(predict_result) or {}
    history = inner.get('chat_history')
    for msg in reversed(history if isinstance(history, list) else []):
        if isinstance(msg, dict) and (msg.get('role') in _ASSISTANT_ROLES or msg.get('type') == 'ai'):
            content = msg.get('content')
            if isinstance(content, str) and content.strip():
                return content
    return None


def _canonical(value) -> str:
    try:
        return json.dumps(value, sort_keys=True, ensure_ascii=False, default=str)
    except (TypeError, ValueError):
        return str(value)


def final_answer_ms(trajectory: Optional[dict]) -> Optional[float]:
    """``duration_ms`` of the last top-level LLM step: the same step ``performance.final_answer_time``
    scores, so the run diff and the dimension never disagree on which step is the answer.

    In a pipeline this is the last LLM node's call, even when a printer or state-modifier node
    runs after it: those make no model call, so the time to the answer is the LLM's."""
    steps = (trajectory or {}).get('steps') or []
    answers = [s for s in steps if isinstance(s, dict) and s.get('kind') == 'llm' and not s.get('parent_agent')]
    duration = answers[-1].get('duration_ms') if answers else None
    return duration if isinstance(duration, (int, float)) else None


#: Where an ``agents`` entry's figures come from: the recorded steps, or the usage ledger.
AGENT_SOURCE_TRAJECTORY = 'trajectory'
AGENT_SOURCE_LEDGER = 'ledger'


def agent_breakdown(trajectory: Optional[dict]) -> Optional[List[dict]]:
    """Per-agent split of one case (#7015 P3): the agent under test first (``agent`` None), then
    each sub-agent in the order it first ran, with its LLM and tool calls and the tokens its LLM
    steps report. A step is a sub-agent's when it carries ``parent_agent``.

    ``None`` when no step ran in a sub-agent, so a plain agent's metrics are unchanged. Tokens are
    ``None`` for an agent with an LLM step whose provider sent no usage: a partial sum would read as
    a measurement. Settlement replaces them with the ledger's figures where it has them."""
    steps = [s for s in (trajectory or {}).get('steps') or [] if isinstance(s, dict)]
    if not any(s.get('parent_agent') for s in steps):
        return None
    agents: dict = {None: None}
    for step in steps:
        kind = step.get('kind')
        if kind not in ('llm', 'tool'):
            continue
        name = step.get('parent_agent') or None
        entry = agents.get(name)
        if entry is None:
            entry = agents[name] = {'agent': name, 'llm_calls': 0, 'tool_calls': 0,
                                    'input_tokens': 0, 'output_tokens': 0, 'cost': None,
                                    'source': AGENT_SOURCE_TRAJECTORY}
        if kind == 'tool':
            entry['tool_calls'] += 1
            continue
        entry['llm_calls'] += 1
        tokens = step.get('tokens')
        if not isinstance(tokens, dict) or entry['input_tokens'] is None:
            entry['input_tokens'] = entry['output_tokens'] = None
            continue
        entry['input_tokens'] += tokens.get('in') or 0
        entry['output_tokens'] += tokens.get('out') or 0
    return [entry for entry in agents.values() if entry is not None]


def trajectory_metrics(trajectory: Optional[dict], *, latency_ms: Optional[int] = None,
                       error: Optional[str] = None, final_text: Optional[str] = None) -> dict:
    """Per-case counters (G8), computed once here so the UI and P2 dimensions read, not recompute.

    * ``redundant_calls`` — a repeat of an identical (tool, inputs) call that already succeeded
      (design §5.2). A repeat after a failure is a retry, not redundant.
    * ``retries`` — a call to the tool that has just failed, made right after the failure.
    * ``step_limit_hit`` — the run ended on the agent's step limit: the SDK's warning is the final
      message, or the recursion limit raised (§4.5).
    * ``guardrail_events`` — blocked and auth-paused tool calls, plus the HITL pause that ended the
      run, if any.
    * ``node_count`` / ``node_path`` — pipelines only (#7015): how many nodes ran and their names in
      order. Absent for agents, so their stored metrics are unchanged.
    * ``agents`` — the per-agent split (:func:`agent_breakdown`, #7015 P3), only when a sub-agent
      ran. The other counters still cover every agent's steps.
    """
    steps = (trajectory or {}).get('steps') or []
    tools = [s for s in steps if s.get('kind') == 'tool']
    succeeded, redundant, retries = set(), 0, 0
    previous = None
    for step in tools:
        key = (step.get('tool_name'), step.get('_identity') or _canonical(step.get('tool_inputs')))
        if key in succeeded:
            redundant += 1
        if step.get('status') == 'ok':
            succeeded.add(key)
        if previous is not None and previous.get('is_error') and previous.get('tool_name') == step.get('tool_name'):
            retries += 1
        previous = step
    lowered = f"{error or ''}\n{final_text or ''}".lower()
    paused = 1 if (trajectory or {}).get('pause') else 0
    agents = agent_breakdown(trajectory)
    nodes = {}
    if 'node_sequence' in (trajectory or {}):
        path = [s.get('node') for s in steps if s.get('kind') == 'node']
        nodes = {'node_count': len(path), 'node_path': path}
    return {
        'llm_calls': sum(1 for s in steps if s.get('kind') == 'llm'),
        'tool_calls': len(tools),
        'distinct_tools': len({s.get('tool_name') for s in tools}),
        'tool_errors': sum(1 for s in tools if s.get('is_error')),
        'retries': retries,
        'redundant_calls': redundant,
        'step_limit_hit': (any(marker in lowered for marker in _STEP_LIMIT_MARKERS)
                           or bool(_GRAPH_STEP_LIMIT_REPLY.match((final_text or '').strip().lower()))),
        'guardrail_events': sum(1 for s in tools if s.get('status') in ('blocked', 'action_required')) + paused,
        'latency_ms': latency_ms,
        # Stored so the run diff reads one number instead of loading every trajectory (§3.4).
        'final_answer_ms': final_answer_ms(trajectory),
        **nodes,
        **({'agents': agents} if agents else {}),
    }


def extract_execution(predict_result: Any, *, status: str, latency_ms: Optional[int] = None,
                      error: Optional[str] = None, store_tool_outputs: bool = True) -> dict:
    """``{trajectory_state, trajectory_state_reason, trajectory, metrics}`` for one agent outcome.

    The one place ``run_agent`` reads evidence from the envelope. ``status`` is the outcome
    ``run_agent`` already decided; it only matters when there is no envelope to read.
    ``store_tool_outputs`` is the suite's frozen opt-out (Q-S5)."""
    if status == 'unsupported':
        return not_applicable_execution('unsupported')
    trajectory = (build_trajectory(predict_result, store_tool_outputs=store_tool_outputs)
                  if status not in _NO_ENVELOPE_REASONS else None)
    if trajectory is None:
        return {
            'trajectory_state': TRAJECTORY_NOT_RECORDED,
            'trajectory_state_reason': _NO_ENVELOPE_REASONS.get(status, 'no_envelope'),
            'trajectory': None,
            'metrics': {'latency_ms': latency_ms},
        }
    pause = pause_details(predict_result)
    if pause is not None:
        trajectory['pause'] = {**pause, 'policy': PAUSE_POLICY_FAIL_CASE}
        _mark_paused_steps(trajectory)
    metrics = trajectory_metrics(trajectory, latency_ms=latency_ms, error=error,
                                 final_text=_final_assistant_text(predict_result))
    for step in trajectory['steps']:
        step.pop('_identity', None)
    return {
        'trajectory_state': TRAJECTORY_RECORDED,
        'trajectory_state_reason': None,
        'trajectory': trajectory,
        'metrics': metrics,
    }


def not_applicable_execution(reason: str) -> dict:
    """No agent ran for this case (structure-only run, unsupported agent type)."""
    return {
        'trajectory_state': TRAJECTORY_NOT_APPLICABLE,
        'trajectory_state_reason': reason,
        'trajectory': None,
        'metrics': {},
    }


#: Counters summed across recorded cases in ``trajectory_rollup``.
ROLLUP_COUNTERS = ('llm_calls', 'tool_calls', 'tool_errors', 'retries', 'redundant_calls',
                   'guardrail_events')


def _percentile(values: List[float], percent: int) -> Optional[float]:
    """Nearest-rank percentile: always a latency some case actually had, never an interpolation."""
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(math.ceil(percent / 100 * len(ordered)) - 1, 0)]


def trajectory_rollup(rows: List[dict]) -> Optional[dict]:
    """Run-level trajectory counters (``EvalRun.meta.trajectory_rollup``) from execution rows.

    Totals and averages come from recorded cases only. A case with no trajectory to read is
    counted in ``excluded_cases``, split by why, so an average never silently covers fewer cases
    than the run had (G7). ``timed_out_cases`` counts the cases stopped at the suite's time limit:
    their scores are skipped, so this is what keeps them visible next to the headline
    (performance §3.2). ``None`` when the run has no execution rows."""
    if not rows:
        return None
    recorded = [r for r in rows if r.get('trajectory_state') == TRAJECTORY_RECORDED]
    excluded = {'count': 0, 'budget_blocked': 0, TRAJECTORY_NOT_APPLICABLE: 0, TRAJECTORY_NOT_RECORDED: 0}
    for row in rows:
        if row.get('trajectory_state') == TRAJECTORY_RECORDED:
            continue
        excluded['count'] += 1
        if row.get('status') == 'budget_blocked' or row.get('trajectory_state_reason') == 'budget_blocked':
            excluded['budget_blocked'] += 1
        elif row.get('trajectory_state') == TRAJECTORY_NOT_APPLICABLE:
            excluded[TRAJECTORY_NOT_APPLICABLE] += 1
        else:
            excluded[TRAJECTORY_NOT_RECORDED] += 1

    metrics = [r.get('metrics') or {} for r in recorded]
    n = len(recorded)
    totals = {key: sum(m.get(key) or 0 for m in metrics) for key in ROLLUP_COUNTERS}
    latencies = [m['latency_ms'] for m in metrics if isinstance(m.get('latency_ms'), (int, float))]
    return {
        'cases': len(rows),
        'recorded_cases': n,
        'totals': totals,
        'averages': {key: (totals[key] / n if n else None) for key in ROLLUP_COUNTERS},
        'step_limit_hits': sum(1 for m in metrics if m.get('step_limit_hit')),
        'timed_out_cases': sum(1 for r in rows if r.get('status') == 'timeout'),
        'average_latency_ms': sum(latencies) / len(latencies) if latencies else None,
        'p50_latency_ms': _percentile(latencies, 50),
        'p95_latency_ms': _percentile(latencies, 95),
        'excluded_cases': excluded,
    }
