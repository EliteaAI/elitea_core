"""The per-case ``expected_trajectory`` reference (#6809 item 4, design §3.2).

Optional and opportunistic like ``expected_output``: a case without one is never skipped; a check
that needs it returns ``na`` instead. Stdlib only, so the import parser, the pydantic models and the
tests share one validator without the ORM.

Shape (normalized; keys whose value is unset are omitted)::

    {
      "match": "superset",                 # exact | in_order | any_order | subset | superset
      "tools": [{"name": "jira_search", "args": {"project": "EL"}, "args_match": "subset"}],
      "forbidden": ["github_delete_branch"],
      "max_tool_calls": 6,
      "allow_repeat": ["get_job_status"]
    }

``match`` compares the expected ``tools`` with the case's recorded ``tool_sequence``:

* ``exact`` — the same calls in the same order, nothing else;
* ``in_order`` — the expected calls appear in that order; other calls may sit between them;
* ``any_order`` — the same calls in any order, nothing else;
* ``subset`` — every actual call is one of the expected ones (some may be skipped);
* ``superset`` — every expected call happens; extra calls are fine. The default, because agents
  vary their tool order and extra lookups between runs, and a strict default would flap.

``args_match`` is ``subset`` (the expected args are a subset of the actual ones, the default) or
``exact``. The platform trajectory dimensions (item 5) implement these semantics.
"""

import json
from typing import List, Optional, Tuple

MATCH_MODES = ('exact', 'in_order', 'any_order', 'subset', 'superset')
ARGS_MATCH_MODES = ('subset', 'exact')
DEFAULT_MATCH = 'superset'
DEFAULT_ARGS_MATCH = 'subset'

_KEYS = {'match', 'tools', 'forbidden', 'max_tool_calls', 'allow_repeat'}
_TOOL_KEYS = {'name', 'args', 'args_match'}

MAX_TOOLS = 200
MAX_NAME_CHARS = 256
# The value rides on the run snapshot and into every trajectory-scoped judge prompt.
MAX_SERIALIZED_CHARS = 64_000


def _name(value, where: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f'{where} must be a non-empty string')
    value = value.strip()
    if len(value) > MAX_NAME_CHARS:
        raise ValueError(f'{where} exceeds {MAX_NAME_CHARS} characters')
    return value


def _names(value, field: str) -> List[str]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError(f'expected_trajectory.{field} must be a list of tool names')
    if len(value) > MAX_TOOLS:
        raise ValueError(f'expected_trajectory.{field} is limited to {MAX_TOOLS} entries')
    return [_name(v, f'expected_trajectory.{field}[{i}]') for i, v in enumerate(value)]


def _tool(value, i: int) -> dict:
    where = f'expected_trajectory.tools[{i}]'
    if isinstance(value, str):
        value = {'name': value}
    if not isinstance(value, dict):
        raise ValueError(f'{where} must be an object or a tool name')
    unknown = set(value) - _TOOL_KEYS
    if unknown:
        raise ValueError(f'{where} has unknown keys: {sorted(unknown)}')
    tool = {'name': _name(value.get('name'), f'{where}.name')}
    args = value.get('args')
    if args is not None:
        if not isinstance(args, dict):
            raise ValueError(f'{where}.args must be an object')
        tool['args'] = args
        args_match = value.get('args_match') or DEFAULT_ARGS_MATCH
        if args_match not in ARGS_MATCH_MODES:
            raise ValueError(f'{where}.args_match must be one of {list(ARGS_MATCH_MODES)}')
        tool['args_match'] = args_match
    elif value.get('args_match') is not None:
        raise ValueError(f'{where}.args_match needs args')
    return tool


def normalize_expected_trajectory(value) -> Optional[dict]:
    """Validate ``value`` and return its normalized dict, or ``None`` for an absent reference.

    ``None``, ``{}`` and an empty string mean "no expectation". A JSON string is parsed (CSV
    cells). Raises ``ValueError`` with a field-level message on anything malformed.
    """
    if value is None:
        return None
    if isinstance(value, str):
        if not value.strip():
            return None
        try:
            value = json.loads(value)
        except ValueError as exc:
            raise ValueError(f'expected_trajectory is not valid JSON: {exc}') from exc
    if not isinstance(value, dict):
        raise ValueError('expected_trajectory must be an object')
    if not value:
        return None
    unknown = set(value) - _KEYS
    if unknown:
        raise ValueError(f'expected_trajectory has unknown keys: {sorted(unknown)}')

    match = value.get('match') or DEFAULT_MATCH
    if match not in MATCH_MODES:
        raise ValueError(f'expected_trajectory.match must be one of {list(MATCH_MODES)}')

    tools = value.get('tools')
    if tools is None:
        tools = []
    if not isinstance(tools, list):
        raise ValueError('expected_trajectory.tools must be a list')
    if len(tools) > MAX_TOOLS:
        raise ValueError(f'expected_trajectory.tools is limited to {MAX_TOOLS} entries')

    normalized = {
        'match': match,
        'tools': [_tool(t, i) for i, t in enumerate(tools)],
        'forbidden': _names(value.get('forbidden'), 'forbidden'),
        'allow_repeat': _names(value.get('allow_repeat'), 'allow_repeat'),
    }
    max_tool_calls = value.get('max_tool_calls')
    if max_tool_calls is not None:
        if isinstance(max_tool_calls, bool) or not isinstance(max_tool_calls, int) or max_tool_calls < 0:
            raise ValueError('expected_trajectory.max_tool_calls must be a non-negative integer')
        normalized['max_tool_calls'] = max_tool_calls

    try:
        size = len(json.dumps(normalized))
    except (TypeError, ValueError) as exc:
        raise ValueError(f'expected_trajectory must be JSON-serializable: {exc}') from exc
    if size > MAX_SERIALIZED_CHARS:
        raise ValueError(f'expected_trajectory exceeds {MAX_SERIALIZED_CHARS} characters')
    return normalized


def from_tool_calls(tool_names: List[str]) -> Optional[dict]:
    """Pre-fill a reference from a recorded run's tool calls, in call order (promote).

    Names only: recorded args are specific to that one run, so the user adds the ones that
    matter. ``None`` when the run called no tools, since "no tools" is not something the user
    asserted.
    """
    names = [n.strip() for n in tool_names if isinstance(n, str) and n.strip()][:MAX_TOOLS]
    if not names:
        return None
    return {'match': DEFAULT_MATCH, 'tools': [{'name': n} for n in names],
            'forbidden': [], 'allow_repeat': []}


def validate_or_error(value, row_no: int) -> Tuple[Optional[dict], Optional[dict]]:
    """Import helper: ``(normalized, None)`` or ``(None, {'row': n, 'error': msg})``."""
    try:
        return normalize_expected_trajectory(value), None
    except ValueError as exc:
        return None, {'row': row_no, 'error': str(exc)}
