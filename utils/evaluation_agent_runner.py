"""Live agent execution for offline-batch runs — EVAL-H4 (design §14.2, §17.1, §8.1).

Closes the §14.2 measurement gap: an offline-batch case stores only an ``input`` (and optional
``expected_output``) — there is no recorded ``output`` to score. This module runs the run's
**pinned** ``ApplicationVersion`` over each case's input to produce that output, so the AI/code
engines score a real agent response instead of an empty string.

Per the H4 scope decision (recommend: single-turn agents only, pipelines deferred):
  * **Supported** — every conversational ``agent_type`` runnable through the tool-full single-turn
    ``predict_sio`` path the chat surface already uses (``openai``/``react``/``elitea``/… — the same
    primitive the judge rides, but with the *real* version_details, so the agent's own tools and
    instructions drive the response).
  * **Deferred** — ``pipeline`` (multi-node, possible HITL pauses) does not fit a single input→output
    turn (§8.1). An unsupported agent yields a clear per-case *unsupported* outcome, never a crash,
    and the run's machine bindings become error rows (E4 fail-closed, handled by the orchestrator).

Split of responsibility (mirrors :mod:`llm_judge`):
  * pure builders (:func:`build_agent_predict_data`, :func:`merge_case_variables`,
    :func:`extract_agent_output`, :func:`agent_type_supported`) carry no I/O and are unit-tested
    without a live model;
  * :func:`run_agent` binds ``this.module.predict_sio`` (injectable for tests) and returns a
    **structured outcome dict** — it NEVER raises for an execution-level failure, so one bad case
    cannot sink a batch run (the E4 contract H5 relies on).

Outcome dict shape::

    {'status': 'ok'|'unsupported'|'timeout'|'predict_exception'|'predict_error'|'empty'
               |'guardrail_paused'|'parked'|'budget_blocked',
     'output': <assistant text> | None,   # set only when status == 'ok'
     'error':  <str> | None,              # set when status != 'ok'
     'execution': {...},                  # trajectory + counters, see :mod:`evaluation_execution`
     'usage': {...}}                      # tokens per model, see :mod:`evaluation_usage`

``status='budget_blocked'`` (with ``budget_scope``: ``project`` | ``member``) is a call refused by
the project/member budget gate at dispatch. The run stops on it (``stop_reason=gate_closed``).
"""
import copy
import time
from typing import Callable, List, Optional
from uuid import uuid4

# agent_type values that DO NOT fit the single-turn input->output contract (§8.1). Kept as a literal
# set so the pure core needs no ORM/enum import; mirrors models.enums.all.AgentTypes.pipeline.
UNSUPPORTED_AGENT_TYPES = frozenset({'pipeline'})

DEFAULT_AGENT_TIMEOUT = 120  # agents run tools + may chain steps, so more headroom than the judge
_ASSISTANT_ROLES = ('assistant', 'ai')
_ERROR_TRUNCATE = 500
# The SDK's fixed text for a run that finished without an answer (langraph_agent.py). It is
# not agent output, so it must score ``empty`` rather than pass as a non-blank reply.
_SDK_NO_OUTPUT_SENTINEL = 'Assistant run has been completed, but output is None.'


def agent_type_supported(version_details: dict) -> bool:
    """True when the version's ``agent_type`` can run as a single input→output turn (§8.1). Unknown
    /missing agent_type is treated as supported (defaults to the conversational path); only the
    explicitly multi-node types (``pipeline``) are deferred for P1."""
    agent_type = (version_details or {}).get('agent_type')
    return agent_type not in UNSUPPORTED_AGENT_TYPES


def agent_structure_snapshot(version_details: dict) -> dict:
    """The "agent structure" evidence (§19.4 evidence_scope.structure): the pinned version's
    configuration, not its output — ``agent_type``, ``instructions``, ``llm_settings``, ``tools``,
    ``skills``, ``meta``. Computed once per run in :func:`evaluation_run_orchestration._make_agent_runner`
    and frozen onto every case so a ``structure``-scoped binding sees exactly what the agent was
    configured with when it produced the case's output."""
    vd = version_details or {}
    return {
        'agent_type': vd.get('agent_type'),
        'instructions': vd.get('instructions'),
        'llm_settings': vd.get('llm_settings'),
        'tools': vd.get('tools'),
        'skills': vd.get('skills'),
        'meta': vd.get('meta'),
    }


def merge_case_variables(version_variables, case_variables: Optional[dict]) -> List[dict]:
    """Overlay a case's ``variables`` dict onto the version's variable list (§17.1).

    Version variables are a list of ``{'name', 'value'}`` dicts; a case supplies a flat
    ``{name: value}`` map. A case value overrides the matching version variable; unmatched case
    keys are appended. Returns a fresh list (never mutates the version's own definitions)."""
    merged: List[dict] = []
    seen = set()
    for var in (version_variables or []):
        if not isinstance(var, dict):
            continue
        name = var.get('name')
        new_var = dict(var)
        if case_variables and name in case_variables:
            new_var['value'] = case_variables[name]
        merged.append(new_var)
        seen.add(name)
    for name, value in (case_variables or {}).items():
        if name not in seen:
            merged.append({'name': name, 'value': value})
    return merged


def build_agent_predict_data(
    project_id: int,
    version_details: dict,
    user_input,
    case_variables: Optional[dict] = None,
    *,
    stream_key: str = 'eval_agent',
    step_limit: Optional[int] = None,
) -> dict:
    """Assemble the ``predict_sio`` payload for one case (pure; no I/O).

    Uses the run's *real* frozen ``version_details`` (agent_type, instructions, llm_settings,
    tools, skills, meta) so the agent responds exactly as it would in chat — unlike the judge,
    which injects a synthetic tool-less prompt. Case variables are overlaid (§17.1). ``user_input``
    falls back to ``'continue'`` when empty, matching the chat path's guard.

    ``step_limit`` is the suite's frozen ``steps_limit`` (design §4.5). It goes into
    ``version_details['meta']['step_limit']``, which ``indexer_agent`` reads as the run's recursion
    limit; None leaves the agent's own setting (SDK default 25)."""
    vd = copy.deepcopy(version_details or {})
    if case_variables:
        vd['variables'] = merge_case_variables(vd.get('variables'), case_variables)
    if step_limit is not None:
        vd['meta'] = {**(vd.get('meta') or {}), 'step_limit': step_limit}

    uid = uuid4().hex[:12]
    text = user_input if (isinstance(user_input, str) and user_input.strip()) else 'continue'
    return {
        'project_id': project_id,
        'user_input': text,
        'llm_settings': vd.get('llm_settings') or {},
        'version_details': vd,
        'chat_history': [],
        'tools': vd.get('tools') or [],
        'internal_tools': (vd.get('meta') or {}).get('internal_tools') or [],
        'stream_id': f'{stream_key}_{uid}',
        'message_id': f'{stream_key}_{uid}',
    }


def extract_agent_output(predict_result) -> Optional[str]:
    """Last non-empty assistant/ai message text from a ``predict_sio`` result, else None.

    Mirrors :func:`llm_judge._extract_chat_response` but unwraps the outer ``result`` envelope the
    RPC returns, so the same extraction serves the agent path. Kept local to stay import-light."""
    inner = predict_result.get('result', predict_result) if isinstance(predict_result, dict) else {}
    if not isinstance(inner, dict):
        return None
    chat_history = inner.get('chat_history')
    if not isinstance(chat_history, list) or not chat_history:
        return None
    for msg in reversed(chat_history):
        if not isinstance(msg, dict):
            continue
        if msg.get('role', '') in _ASSISTANT_ROLES or msg.get('type', '') == 'ai':
            content = msg.get('content', '')
            if isinstance(content, str) and content.strip() and not content.startswith(_SDK_NO_OUTPUT_SENTINEL):
                return content
    return None


def _predict_error_text(result) -> Optional[str]:
    """Truncated error string if predict_sio surfaced an error envelope, else None (mirrors
    :func:`llm_judge._predict_error_text`)."""
    if not isinstance(result, dict):
        return None
    for container in (result, result.get('result')):
        if isinstance(container, dict) and container.get('error'):
            text = str(container['error'])
            if len(text) > _ERROR_TRUNCATE:
                text = text[:_ERROR_TRUNCATE] + '…'
            return text
    return None


def run_agent(
    project_id: int,
    version_details: dict,
    case: dict,
    *,
    user_id: Optional[int] = None,
    timeout: int = DEFAULT_AGENT_TIMEOUT,
    predict: Optional[Callable[..., dict]] = None,
    platform_run_id: Optional[str] = None,
    usage_entity: Optional[dict] = None,
    step_limit: Optional[int] = None,
    case_index: Optional[int] = None,
) -> dict:
    """Run the pinned agent over one case's input and return a structured outcome (never raises).

    ``predict`` defaults to ``this.module.predict_sio`` (the same RPC the judge uses) and is
    injectable so tests exercise extraction/error-mapping against a stub. An unsupported
    ``agent_type`` short-circuits to ``status='unsupported'`` without dispatching.

    ``user_id`` must be passed explicitly: a batch run executes on the ``eval_runs`` pool, so
    ``predict_sio`` has neither a sid nor a live request to recover the acting user from, and
    without it every case fails with 'User token not found'.

    A run that paused for human review (``guardrail_paused``) or parked on a sub-agent fan-out
    (``parked``) fails the case even when text preceded the pause: nobody can answer the prompt in
    a batch run, so that text is not the agent's answer (design §4.5).

    ``case_index`` with ``platform_run_id`` makes the stream id, which the ledger keeps as the
    row's ``conversation_id``, name this case, so settlement can find its ledger rows."""
    # Lazy: keeps this module importable with no package context (sibling preloaded in tests).
    from .evaluation_execution import extract_execution, is_parked, pause_details
    from .evaluation_usage import ROLE_AGENT, case_stream_key, envelope_usage, not_applicable_usage

    def _outcome(status, output, error, result=None, latency_ms=None):
        return {'status': status, 'output': output, 'error': error,
                'execution': extract_execution(result, status=status, latency_ms=latency_ms,
                                               error=error or _predict_error_text(result)),
                'usage': (not_applicable_usage('unsupported') if status == 'unsupported'
                          else envelope_usage(result, status=status))}

    if not agent_type_supported(version_details):
        agent_type = (version_details or {}).get('agent_type')
        return _outcome('unsupported', None,
                        f"agent_type '{agent_type}' is not supported for live batch execution "
                        '(P1 scope: single-turn agents only, pipelines deferred)')

    if predict is None:
        from tools import this
        predict = this.module.predict_sio

    data = build_agent_predict_data(project_id, version_details, case.get('input'),
                                    case.get('variables'), step_limit=step_limit,
                                    stream_key=case_stream_key(ROLE_AGENT, platform_run_id, case_index))
    started = time.monotonic()
    try:
        result = predict(sid=None, data=data, await_task_timeout=timeout,
                         user_id=user_id, skip_expansion=True, return_chat_history=True,
                         platform_run_id=platform_run_id, usage_entity=usage_entity)
    except Exception as exc:  # noqa: BLE001 - execution-level failure is a value, not a raise
        latency_ms = int((time.monotonic() - started) * 1000)
        if getattr(exc, 'type', None) == 'budget_exceeded':
            # BudgetDoorClosedError: the gate refused the call before any model ran (§5.4).
            outcome = _outcome('budget_blocked', None, str(exc), latency_ms=latency_ms)
            outcome['budget_scope'] = getattr(exc, 'scope', None)
            outcome['execution']['metrics']['budget_scope'] = outcome['budget_scope']
            return outcome
        return _outcome('predict_exception', None, str(exc), latency_ms=latency_ms)
    latency_ms = int((time.monotonic() - started) * 1000)

    # Task timeout — predict_sio returns {"task_id": ...} without a "result" (matches run_llm_judge).
    if isinstance(result, dict) and 'task_id' in result and 'result' not in result:
        try:
            from tools import this
            this.module.stop_task(result['task_id'])
        except Exception:
            pass
        return _outcome('timeout', None, f'agent timed out after {timeout}s', latency_ms=latency_ms)

    if is_parked(result):
        return _outcome('parked', None,
                        'agent parked on a sub-agent fan-out, which a batch run cannot resume',
                        result, latency_ms)
    pause = pause_details(result)
    if pause is not None:
        what = pause.get('tool_name') or pause.get('node_name') or pause.get('guardrail_type') or 'a guardrail'
        return _outcome('guardrail_paused', None,
                        f'agent paused for human review at {what}; batch runs fail the case', result,
                        latency_ms)

    output = extract_agent_output(result)
    if output is not None:
        return _outcome('ok', output, None, result, latency_ms)

    err = _predict_error_text(result)
    if err:
        return _outcome('predict_error', None, err, result, latency_ms)
    return _outcome('empty', None, 'agent produced no assistant output', result, latency_ms)
