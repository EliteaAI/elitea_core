"""Schema-agnostic single-shot LLM judge (EVAL-H1, design §18/§18.9).

Extracted from ``run_ai_validation()`` (publish_utils.py) so the publish pre-check and the
agent-evaluation AI engine share one judge-invocation path. This primitive carries **zero**
publish assumptions: the caller resolves the judge model, supplies the system prompt + user
payload (already serialized), and interprets the returned JSON against its own schema.

Split of responsibility (behavior-preservation for the publish path depends on this):
  * ``run_llm_judge`` builds the tool-less ``predict_sio`` invocation, pins temperature for
    deterministic JSON, handles the task-timeout ``stop_task`` path, and returns a **structured
    outcome dict** — it NEVER raises for a judge-level failure.
  * The publish caller re-parses ``outcome['raw']`` with ``PublishAIResult`` (unchanged), so its
    field validation / error mapping stays byte-for-byte identical.
  * The eval caller consumes the schema-agnostic ``outcome['data']`` (parsed JSON dict). Because
    a judge failure is a value, not an exception, one bad case cannot sink a batch run (H5).

Outcome dict shape::

    {'status': 'ok'|'timeout'|'predict_exception'|'predict_error'|'unparseable',
     'data':   <parsed JSON dict> | None,   # set only when status == 'ok'
     'error':  <str> | None,                # set when status != 'ok'
     'raw':    <predict_sio result> | None}  # None only on predict_exception
"""
import json
import re
from typing import Optional
from uuid import uuid4

from pylon.core.tools import log
from tools import this


DEFAULT_JUDGE_TEMPERATURE = 0.1
DEFAULT_JUDGE_STEP_LIMIT = 5
_ASSISTANT_ROLES = ('assistant', 'ai')
_JSON_FENCE_RE = re.compile(r'```(?:json)?\s*\n(.*?)\n\s*```', re.DOTALL)
_ERROR_TRUNCATE = 500


def _extract_chat_response(inner):
    """Last non-empty assistant/ai message text from a predict_sio inner dict, else None.

    Mirrors models/pd/publish.py::_extract_chat_response so the eval path extracts identically
    to the publish path, but kept local to keep this primitive import-light + unit-testable.
    """
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


def _extract_json(text):
    """Parse a JSON object from LLM text: bare, then markdown-fenced, then first ``{...}`` span."""
    if not isinstance(text, str):
        return None
    text = text.strip()
    try:
        obj = json.loads(text)
        if isinstance(obj, dict):
            return obj
    except (json.JSONDecodeError, TypeError):
        pass
    m = _JSON_FENCE_RE.search(text)
    if m:
        try:
            obj = json.loads(m.group(1))
            if isinstance(obj, dict):
                return obj
        except (json.JSONDecodeError, TypeError):
            pass
    start, end = text.find('{'), text.rfind('}')
    if start != -1 and end > start:
        try:
            obj = json.loads(text[start:end + 1])
            if isinstance(obj, dict):
                return obj
        except (json.JSONDecodeError, TypeError):
            pass
    return None


def _predict_error_text(result):
    """Return the truncated error string if predict_sio surfaced an error envelope, else None.

    Mirrors publish_utils._check_predict_error's containers (top level + ``result`` wrapper) but
    returns the text instead of raising, so the caller decides how to signal it.
    """
    if not isinstance(result, dict):
        return None
    for container in (result, result.get('result')):
        if isinstance(container, dict) and container.get('error'):
            text = str(container['error'])
            if len(text) > _ERROR_TRUNCATE:
                text = text[:_ERROR_TRUNCATE] + '…'
            return text
    return None


def run_llm_judge(
    project_id: int,
    judge_llm_settings: dict,
    system_prompt: str,
    user_payload_json: str,
    timeout: int,
    *,
    temperature: float = DEFAULT_JUDGE_TEMPERATURE,
    step_limit: int = DEFAULT_JUDGE_STEP_LIMIT,
    stream_key: str = 'llm_judge',
    user_id: Optional[int] = None,
) -> dict:
    """Run one tool-less LLM judge call and return a structured outcome (never raises).

    ``judge_llm_settings`` is resolved by the caller (publish uses
    ``get_validation_llm_settings``; eval uses the suite judge model). ``reasoning_effort`` and
    ``max_tokens`` are stripped and ``temperature`` pinned so JSON output is deterministic.

    ``user_id`` names the acting user for token resolution. Callers running inside a request or SIO
    session may omit it, but a background task pool has neither, so an eval run must pass the run's
    owner or every judge call fails with 'User token not found'.
    """
    resolved = dict(judge_llm_settings or {})
    resolved.pop('reasoning_effort', None)
    resolved.pop('max_tokens', None)  # avoid the version's potentially small limit
    resolved['temperature'] = temperature  # deterministic output for reliable JSON

    version_details = {
        'agent_type': 'openai',
        'instructions': system_prompt,
        'llm_settings': resolved,
        'tools': [],
        'meta': {'internal_tools': [], 'step_limit': step_limit},
    }
    uid = uuid4().hex[:12]
    data = {
        'project_id': project_id,
        'user_input': user_payload_json,
        'llm_settings': resolved,
        'version_details': version_details,
        'chat_history': [],
        'tools': [],
        'internal_tools': [],
        'stream_id': f'{stream_key}_{uid}',
        'message_id': f'{stream_key}_{uid}',
    }

    try:
        result = this.module.predict_sio(
            sid=None,
            data=data,
            await_task_timeout=timeout,
            user_id=user_id,
            skip_expansion=True,
            return_chat_history=True,
        )
    except Exception as exc:
        return {'status': 'predict_exception', 'data': None, 'error': str(exc), 'raw': None}

    # Task timeout — predict_sio returns {"task_id": ...} without a "result" when join_task
    # does not complete in time. Best-effort cancel, then report timeout.
    if isinstance(result, dict) and 'task_id' in result and 'result' not in result:
        try:
            this.module.stop_task(result['task_id'])
        except Exception:
            pass
        return {
            'status': 'timeout', 'data': None,
            'error': f'judge timed out after {timeout}s', 'raw': result,
        }

    # Parse-then-error ordering (matches the original run_ai_validation): a valid assistant JSON
    # response wins even if an 'error' key is also present in the envelope.
    inner = result.get('result', result) if isinstance(result, dict) else {}
    if not isinstance(inner, dict):
        inner = {}
    text = _extract_chat_response(inner)
    parsed = _extract_json(text) if text is not None else None
    if parsed is not None:
        return {'status': 'ok', 'data': parsed, 'error': None, 'raw': result}

    err = _predict_error_text(result)
    if err:
        log.error(f'llm_judge predict error: {err}')
        return {'status': 'predict_error', 'data': None, 'error': err, 'raw': result}
    return {
        'status': 'unparseable', 'data': None,
        'error': 'judge returned unparseable output', 'raw': result,
    }
