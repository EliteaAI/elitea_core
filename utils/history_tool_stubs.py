"""Compact tool_call/tool_result stubs for recent assistant turns in replayed chat history (#7460)."""
import json
from typing import Any, Dict, Iterable, List

STUB_ID_PREFIX = 'hist_'
MAX_ARG_CHARS = 200
MAX_RESULT_CHARS = 200
MAX_CALLS_PER_TURN = 10
TRUNCATED = '…[truncated]'


def stub_settings(config: dict) -> Dict[str, Any]:
    raw = (config or {}).get('chat_history') or {}
    raw = raw.get('tool_call_stubs') or {}
    try:
        last_n = max(0, int(raw.get('last_n', 3)))
    except (TypeError, ValueError):
        last_n = 3
    return {'enabled': bool(raw.get('enabled', True)), 'last_n': last_n}


def _short(value: str, limit: int) -> str:
    return value if len(value) <= limit else value[:limit] + TRUNCATED


def _short_args(args: Any) -> Dict[str, Any]:
    if not isinstance(args, dict):
        return {'input': _short(json.dumps(args, default=str), MAX_ARG_CHARS)} if args is not None else {}
    result = {}
    for key, value in args.items():
        if isinstance(value, str):
            result[key] = _short(value, MAX_ARG_CHARS)
        elif isinstance(value, (int, float, bool)) or value is None:
            result[key] = value
        else:
            result[key] = _short(json.dumps(value, default=str), MAX_ARG_CHARS)
    return result


def _result_text(step: dict) -> str:
    output = (step.get('tool_output') or '').strip()
    if step.get('is_error'):
        return 'error: ' + _short(output or 'tool call failed', MAX_RESULT_CHARS)
    return _short(output, MAX_RESULT_CHARS) if output else 'ok'


def build_tool_stub_messages(steps: Iterable[dict]) -> List[dict]:
    """One assistant tool_calls message, then a tool message per call; [] when there are no steps."""
    steps = list(steps)
    if not steps:
        return []
    kept, omitted = steps[:MAX_CALLS_PER_TURN], len(steps) - MAX_CALLS_PER_TURN
    calls, results = [], []
    for step in kept:
        call_id = f"{STUB_ID_PREFIX}{step['id']}"
        calls.append({'id': call_id, 'name': step.get('tool_name') or 'unknown_tool',
                      'args': _short_args(step.get('tool_inputs')), 'type': 'tool_call'})
        results.append({'role': 'tool', 'content': _result_text(step), 'tool_call_id': call_id})
    if omitted > 0:
        results[-1]['content'] += f'\n(+{omitted} more tool calls this turn omitted from history)'
    return [{'role': 'assistant', 'content': '', 'tool_calls': calls}, *results]


def select_stub_group_ids(groups: List[Any], is_assistant, last_n: int) -> List[int]:
    if last_n <= 0:
        return []
    return [g.id for g in groups if is_assistant(g)][-last_n:]


def summarized_group_ids_from_messages(message_group_ids: List[int], summarized_count: int) -> List[int]:
    """Groups wholly inside the summarized message prefix; a group split by the cutoff stays in history."""
    summarized_count = max(0, summarized_count or 0)
    prefix = message_group_ids[:summarized_count]
    if prefix and summarized_count < len(message_group_ids) and message_group_ids[summarized_count] == prefix[-1]:
        prefix = [gid for gid in prefix if gid != prefix[-1]]
    return list(dict.fromkeys(prefix))


def payload_has_tools(payload: dict) -> bool:
    # Agents carry toolkits in version_details; plain LLM chats carry them at the top level
    version_details = payload.get('version_details') or {}
    return bool(payload.get('tools') or version_details.get('tools') or payload.get('internal_tools'))
