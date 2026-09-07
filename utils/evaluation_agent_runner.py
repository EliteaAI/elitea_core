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

    {'status': 'ok'|'unsupported'|'timeout'|'predict_exception'|'predict_error'|'empty',
     'output': <assistant text> | None,   # set only when status == 'ok'
     'error':  <str> | None}              # set when status != 'ok'
"""
import copy
from typing import Callable, List, Optional
from uuid import uuid4

# agent_type values that DO NOT fit the single-turn input->output contract (§8.1). Kept as a literal
# set so the pure core needs no ORM/enum import; mirrors models.enums.all.AgentTypes.pipeline.
UNSUPPORTED_AGENT_TYPES = frozenset({'pipeline'})

DEFAULT_AGENT_TIMEOUT = 120  # agents run tools + may chain steps, so more headroom than the judge
_ASSISTANT_ROLES = ('assistant', 'ai')
_ERROR_TRUNCATE = 500


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
) -> dict:
    """Assemble the ``predict_sio`` payload for one case (pure; no I/O).

    Uses the run's *real* frozen ``version_details`` (agent_type, instructions, llm_settings,
    tools, skills, meta) so the agent responds exactly as it would in chat — unlike the judge,
    which injects a synthetic tool-less prompt. Case variables are overlaid (§17.1). ``user_input``
    falls back to ``'continue'`` when empty, matching the chat path's guard."""
    vd = copy.deepcopy(version_details or {})
    if case_variables:
        vd['variables'] = merge_case_variables(vd.get('variables'), case_variables)

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
            if isinstance(content, str) and content.strip():
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
) -> dict:
    """Run the pinned agent over one case's input and return a structured outcome (never raises).

    ``predict`` defaults to ``this.module.predict_sio`` (the same RPC the judge uses) and is
    injectable so tests exercise extraction/error-mapping against a stub. An unsupported
    ``agent_type`` short-circuits to ``status='unsupported'`` without dispatching.

    ``user_id`` must be passed explicitly: a batch run executes on the ``eval_runs`` pool, so
    ``predict_sio`` has neither a sid nor a live request to recover the acting user from, and
    without it every case fails with 'User token not found'."""
    if not agent_type_supported(version_details):
        agent_type = (version_details or {}).get('agent_type')
        return {'status': 'unsupported', 'output': None,
                'error': f"agent_type '{agent_type}' is not supported for live batch execution "
                         '(P1 scope: single-turn agents only, pipelines deferred)'}

    if predict is None:
        from tools import this
        predict = this.module.predict_sio

    data = build_agent_predict_data(project_id, version_details, case.get('input'),
                                    case.get('variables'))
    try:
        result = predict(sid=None, data=data, await_task_timeout=timeout,
                         user_id=user_id, skip_expansion=True, return_chat_history=True)
    except Exception as exc:  # noqa: BLE001 - execution-level failure is a value, not a raise
        return {'status': 'predict_exception', 'output': None, 'error': str(exc)}

    # Task timeout — predict_sio returns {"task_id": ...} without a "result" (matches run_llm_judge).
    if isinstance(result, dict) and 'task_id' in result and 'result' not in result:
        try:
            from tools import this
            this.module.stop_task(result['task_id'])
        except Exception:
            pass
        return {'status': 'timeout', 'output': None,
                'error': f'agent timed out after {timeout}s'}

    output = extract_agent_output(result)
    if output is not None:
        return {'status': 'ok', 'output': output, 'error': None}

    err = _predict_error_text(result)
    if err:
        return {'status': 'predict_error', 'output': None, 'error': err}
    return {'status': 'empty', 'output': None,
            'error': 'agent produced no assistant output'}
