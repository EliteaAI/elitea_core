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

This module turns that envelope into the normalized, bounded shape stored in
``eval_case_execution`` (design §3.1, §3.3). It is pure — no I/O, no ORM — so the shape is
locked by unit tests against recorded envelopes.

``trajectory_state`` (design §13) never comes from an empty ``steps`` list: ``recorded`` with zero
steps is a real "the agent answered without tools", while a missing envelope is ``not_recorded``
with a reason, and a case where no agent ran is ``not_applicable``.
"""
import json
from datetime import datetime
from typing import Any, List, Optional

TRAJECTORY_RECORDED = 'recorded'
TRAJECTORY_NOT_RECORDED = 'not_recorded'
TRAJECTORY_NOT_APPLICABLE = 'not_applicable'

# Field caps. A tool output can be a 50 KB essay (a sub-agent's whole reply), and a case may run
# dozens of tools, so both the step and the whole trajectory are bounded. The trajectory cap
# mirrors ``MAX_ENVELOPE_BYTES``.
MAX_STEP_TEXT = 4_000
MAX_TRAJECTORY_BYTES = 256_000
_TRUNCATED_MARK = '… [truncated]'

_ENVELOPE_KEYS = ('thinking_steps', 'tool_calls_dict')
# The tool result the SDK's sensitive-tool guard returns when the user declined a call
# (``runtime/tools/llm.py`` SENSITIVE_TOOL_BLOCKED_RESULT_TYPE). The agent keeps going after it.
_BLOCKED_RESULT_TYPE = 'sensitive_tool_blocked'
# How a run that ran out of steps shows up: the SDK tool loop appends a fixed warning as the last
# AI message (``runtime/tools/llm.py``), and the LangGraph recursion limit raises an error.
_STEP_LIMIT_MARKERS = ('maximum tool execution iterations', 'recursion limit', 'graphrecursionerror')
_ASSISTANT_ROLES = ('assistant', 'ai')
# Outcomes where the indexer never returned an envelope to read.
_NO_ENVELOPE_REASONS = {
    'timeout': 'timeout',
    'predict_exception': 'no_envelope',
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
        'text': _cap_text(step.get('text') or None),
        'parent_agent': step.get('parent_agent_name'),
        'parent_call_id': step.get('parent_agent_call_id'),
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
    """``ok`` | ``error`` | ``action_required`` | ``blocked`` for one ``tool_calls_dict`` entry.

    ``action_required`` is the MCP-auth pause the indexer writes on the call; ``blocked`` is a
    sensitive tool the guard refused (design §4.5). Neither is a tool failure. A HITL pause is not a
    step status: it ends the run and is recorded once, on the trajectory (:func:`pause_details`).
    """
    finish_reason = entry.get('finish_reason')
    if finish_reason == 'action_required':
        return 'action_required'
    if _is_blocked(entry.get('tool_output')):
        return 'blocked'
    if finish_reason == 'error' or entry.get('error'):
        return 'error'
    return 'ok'


def _tool_step(entry: dict) -> dict:
    metadata = entry.get('metadata') if isinstance(entry.get('metadata'), dict) else {}
    status = tool_step_status(entry)
    # The orchestrator's own sub-agent call carries ``parent_agent_call_id`` too — its *own* call
    # id, which the sub-agent's steps then point at — but no ``parent_agent_name``. Only a name
    # makes the step a child; without one the id identifies this call.
    parent_agent = metadata.get('parent_agent_name') or None
    call_id = metadata.get('parent_agent_call_id') or None
    return {
        'kind': 'tool',
        'tool_name': entry.get('tool_name'),
        'toolkit': metadata.get('toolkit_name') or None,
        'toolkit_type': metadata.get('toolkit_type') or None,
        'tool_inputs': _cap_inputs(entry.get('tool_inputs')),
        'tool_output': _cap_text(entry.get('tool_output')),
        'status': status,
        'is_error': status == 'error',
        'error': _cap_text(entry.get('error')),
        'finish_reason': entry.get('finish_reason'),
        'call_id': None if parent_agent else call_id,
        'parent_agent': parent_agent,
        'parent_call_id': call_id if parent_agent else None,
        'started_at': entry.get('timestamp_start'),
        'duration_ms': _duration_ms(entry.get('timestamp_start'), entry.get('timestamp_finish')),
    }


def _fit(trajectory: dict) -> dict:
    """Shrink a trajectory over ``MAX_TRAJECTORY_BYTES``: drop tool outputs and LLM text from the
    last step backwards (the order and counts matter more than late payloads), then drop trailing
    steps. Either way ``truncated`` is set, so a reader knows the view is partial."""
    def _size() -> int:
        return len(json.dumps(trajectory, ensure_ascii=False, default=str))

    if _size() <= MAX_TRAJECTORY_BYTES:
        return trajectory
    trajectory['truncated'] = True
    for step in reversed(trajectory['steps']):
        for key in ('tool_output', 'text', 'tool_inputs'):
            if step.get(key) is not None:
                step[key] = None
                step['output_omitted'] = True
        if _size() <= MAX_TRAJECTORY_BYTES:
            return trajectory
    while trajectory['steps'] and _size() > MAX_TRAJECTORY_BYTES:
        trajectory['steps'].pop()
    return trajectory


def build_trajectory(predict_result) -> Optional[dict]:
    """The normalized trajectory (§3.3) of one ``predict_sio`` envelope, or None without one.

    Steps are ordered by ``started_at``; LLM steps come before tool steps on a tie, and insertion
    order breaks the rest. A step with no timestamp keeps its insertion position at the end."""
    inner = _inner(predict_result)
    if inner is None or not any(key in inner for key in _ENVELOPE_KEYS):
        return None

    raw: List[dict] = []
    for step in inner.get('thinking_steps') or []:
        if isinstance(step, dict):
            raw.append(_llm_step(step))
    tool_calls = inner.get('tool_calls_dict')
    for entry in (tool_calls.values() if isinstance(tool_calls, dict) else []):
        if isinstance(entry, dict):
            raw.append(_tool_step(entry))

    def _order(item):
        seq, step = item
        started = _parse_ts(step.get('started_at'))
        return (started is None, started.timestamp() if started else 0.0, seq)

    steps = [step for _, step in sorted(enumerate(raw), key=_order)]
    for index, step in enumerate(steps):
        step['i'] = index
    return _fit({
        'steps': steps,
        'tool_sequence': [s['tool_name'] for s in steps if s['kind'] == 'tool'],
        'truncated': False,
        'source': 'predict_result',
    })


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
    """
    steps = (trajectory or {}).get('steps') or []
    tools = [s for s in steps if s.get('kind') == 'tool']
    succeeded, redundant, retries = set(), 0, 0
    previous = None
    for step in tools:
        key = (step.get('tool_name'), _canonical(step.get('tool_inputs')))
        if key in succeeded:
            redundant += 1
        if step.get('status') == 'ok':
            succeeded.add(key)
        if previous is not None and previous.get('is_error') and previous.get('tool_name') == step.get('tool_name'):
            retries += 1
        previous = step
    lowered = f"{error or ''}\n{final_text or ''}".lower()
    paused = 1 if (trajectory or {}).get('pause') else 0
    return {
        'llm_calls': sum(1 for s in steps if s.get('kind') == 'llm'),
        'tool_calls': len(tools),
        'distinct_tools': len({s.get('tool_name') for s in tools}),
        'tool_errors': sum(1 for s in tools if s.get('is_error')),
        'retries': retries,
        'redundant_calls': redundant,
        'step_limit_hit': any(marker in lowered for marker in _STEP_LIMIT_MARKERS),
        'guardrail_events': sum(1 for s in tools if s.get('status') in ('blocked', 'action_required')) + paused,
        'latency_ms': latency_ms,
    }


def extract_execution(predict_result: Any, *, status: str, latency_ms: Optional[int] = None,
                      error: Optional[str] = None) -> dict:
    """``{trajectory_state, trajectory_state_reason, trajectory, metrics}`` for one agent outcome.

    The one place ``run_agent`` reads evidence from the envelope. ``status`` is the outcome
    ``run_agent`` already decided; it only matters when there is no envelope to read."""
    if status == 'unsupported':
        return not_applicable_execution('unsupported')
    trajectory = build_trajectory(predict_result) if status not in _NO_ENVELOPE_REASONS else None
    if trajectory is None:
        return {
            'trajectory_state': TRAJECTORY_NOT_RECORDED,
            'trajectory_state_reason': _NO_ENVELOPE_REASONS.get(status, 'no_envelope'),
            'trajectory': None,
            'metrics': {'latency_ms': latency_ms},
        }
    pause = pause_details(predict_result)
    if pause is not None:
        trajectory['pause'] = pause
    return {
        'trajectory_state': TRAJECTORY_RECORDED,
        'trajectory_state_reason': None,
        'trajectory': trajectory,
        'metrics': trajectory_metrics(trajectory, latency_ms=latency_ms, error=error,
                                      final_text=_final_assistant_text(predict_result)),
    }


def not_applicable_execution(reason: str) -> dict:
    """No agent ran for this case (structure-only run, unsupported agent type)."""
    return {
        'trajectory_state': TRAJECTORY_NOT_APPLICABLE,
        'trajectory_state_reason': reason,
        'trajectory': None,
        'metrics': {},
    }
