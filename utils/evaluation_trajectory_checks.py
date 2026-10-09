"""Built-in trajectory checks: the platform-tier code dimensions of design §5.2 (#6809 item 5).

Each check is an ordinary code-engine validation script, run in the same sandbox and through the
same bool/number contract as a user-authored one. What makes it *built-in* is where the script
lives: here, versioned with the plugin, not in the registry. A registry row opts in by naming a
check in ``meta.builtin_check``; projection and the run snapshot resolve the script from this
module (:func:`builtin_code`), so

* an admin never authors code that is then distributed to every project, and
* a plugin upgrade changes the script for the next run with no resync.

The scripts read the ``trajectory`` / ``expected_trajectory`` variables a trajectory-scoped
binding gets (§5.1). A check that needs a reference the case does not have assigns
``result = 'na'``, which the contract turns into a skipped row (excluded from the score, never a
pass or fail). ``tool_match`` implements the ``match`` / ``args_match`` semantics documented in
``evaluation_expected_trajectory``.

Stdlib only, no ORM: the seed task, the projection and the tests share it.
"""

from typing import Optional, Tuple

TRAJECTORY_SCOPE = {'structure': False, 'input': False, 'output': False, 'trajectory': True}

# Shared head of every script. ``try/except NameError`` because the screen blocks ``globals()``:
# the prelude only defines ``expected_trajectory`` when the case has one.
_PRELUDE = '''
import json

try:
    _traj = trajectory
except NameError:
    _traj = None
try:
    _exp = expected_trajectory
except NameError:
    _exp = None
if not isinstance(_traj, dict):
    _traj = None
if not isinstance(_exp, dict) or not _exp:
    _exp = None


def _tool_steps(t):
    steps = [s for s in (t.get('steps') or []) if isinstance(s, dict) and s.get('kind') == 'tool']
    if steps or t.get('steps'):
        return steps
    # A trajectory recorded without step detail still has its call order.
    return [{'kind': 'tool', 'tool_name': n, 'tool_inputs': None, 'status': 'ok'}
            for n in (t.get('tool_sequence') or [])]


def _metric(t, key, fallback):
    metrics = t.get('metrics') or {}
    value = metrics.get(key)
    return fallback() if value is None else value
'''

_TOOL_MATCH = '''
def _args_ok(spec, actual):
    if 'args' not in spec:
        return True
    want = spec.get('args') or {}
    if isinstance(actual, str):
        try:
            actual = json.loads(actual)
        except ValueError:
            return False
    if not isinstance(actual, dict):
        return False
    if spec.get('args_match') == 'exact':
        return actual == want
    for key, value in want.items():
        if key not in actual or actual[key] != value:
            return False
    return True


def _hit(spec, step):
    return spec.get('name') == step.get('tool_name') and _args_ok(spec, step.get('tool_inputs'))


def _max_matching(expected, actual):
    # Bipartite matching (augmenting paths): one actual call satisfies at most one expected call.
    owner = {}

    def _assign(i, seen):
        for j in range(len(actual)):
            if j in seen or not _hit(expected[i], actual[j]):
                continue
            seen.add(j)
            if j not in owner or _assign(owner[j], seen):
                owner[j] = i
                return True
        return False

    return sum(1 for i in range(len(expected)) if _assign(i, set()))


def _in_order(expected, actual):
    # Longest common subsequence under _hit: expected calls found in order, gaps allowed.
    width = len(actual) + 1
    prev = [0] * width
    for spec in expected:
        cur = [0] * width
        for j in range(1, width):
            if _hit(spec, actual[j - 1]):
                cur[j] = prev[j - 1] + 1
            else:
                cur[j] = max(prev[j], cur[j - 1])
        prev = cur
    return prev[-1]


if _traj is None or _exp is None:
    result = 'na'
else:
    mode = _exp.get('match') or 'superset'
    expected = [t for t in (_exp.get('tools') or []) if isinstance(t, dict) and t.get('name')]
    actual = _tool_steps(_traj)
    if not expected and mode in ('superset', 'in_order'):
        result = 'na'  # nothing is required, so there is nothing to match
    elif mode == 'exact':
        same = len(expected) == len(actual) and all(_hit(e, a) for e, a in zip(expected, actual))
        result = 1.0 if same else 0.0
    elif mode == 'in_order':
        result = round(_in_order(expected, actual) / len(expected), 4)
    else:
        matched = _max_matching(expected, actual)
        if mode == 'superset':
            result = round(matched / len(expected), 4)
        elif mode == 'subset':
            result = 1.0 if not actual else round(matched / len(actual), 4)
        else:  # any_order: F1 of expected vs actual calls
            total = len(expected) + len(actual)
            result = 1.0 if total == 0 else round(2 * matched / total, 4)
    if result != 'na':
        print(f"match={mode} expected={[e.get('name') for e in expected]} "
              f"actual={[a.get('tool_name') for a in actual]} score={result}")
'''

_FORBIDDEN = '''
forbidden = set((_exp or {}).get('forbidden') or [])
if _traj is None or not forbidden:
    result = 'na'
else:
    called = sorted({s.get('tool_name') for s in _tool_steps(_traj)} & forbidden)
    if called:
        print(f'forbidden tools called: {called}')
    result = not called
'''

_TOOL_ERRORS = '''
if _traj is None:
    result = 'na'
else:
    result = _metric(_traj, 'tool_errors',
                     lambda: sum(1 for s in _tool_steps(_traj) if s.get('is_error')))
'''

# Recomputed (rather than read from metrics) only when the case lists ``allow_repeat`` tools:
# the stored counter is reference-free and cannot know which repeats the author allowed.
_REDUNDANT = '''
def _redundant(steps, allowed):
    seen, count = set(), 0
    for s in steps:
        if s.get('tool_name') in allowed:
            continue
        try:
            args = json.dumps(s.get('tool_inputs'), sort_keys=True, ensure_ascii=False, default=str)
        except (TypeError, ValueError):
            args = str(s.get('tool_inputs'))
        key = (s.get('tool_name'), args)
        if key in seen:
            count += 1
        if s.get('status') == 'ok':
            seen.add(key)
    return count


if _traj is None:
    result = 'na'
else:
    allowed = set((_exp or {}).get('allow_repeat') or [])
    if allowed:
        result = _redundant(_tool_steps(_traj), allowed)
    else:
        result = _metric(_traj, 'redundant_calls', lambda: _redundant(_tool_steps(_traj), set()))
'''

_STEP_BUDGET = '''
budget = (_exp or {}).get('max_tool_calls')
if _traj is None or budget is None:
    result = 'na'
else:
    calls = _metric(_traj, 'tool_calls', lambda: len(_tool_steps(_traj)))
    print(f'tool calls {calls} / budget {budget}')
    result = calls <= budget
'''

_STEP_LIMIT = '''
if _traj is None or (_traj.get('metrics') or {}).get('step_limit_hit') is None:
    result = 'na'
else:
    result = not _traj['metrics']['step_limit_hit']
'''

_GUARDRAIL = '''
if _traj is None:
    result = 'na'
else:
    result = _metric(_traj, 'guardrail_events', lambda: sum(
        1 for s in _tool_steps(_traj) if s.get('status') in ('blocked', 'action_required')
    ) + (1 if _traj.get('pause') else 0))
'''


def _count(name: str, description: str, body: str) -> dict:
    """A lower-is-better counter: informational (weight 0) with a "none at all" target."""
    return {
        'name': name, 'description': description, 'body': body, 'return_contract': 'number',
        'scale_type': 'continuous', 'scale_min': 0.0, 'scale_max': 10.0,
        'polarity': 'lower_better', 'default_weight': 0.0,
        'default_target': 0.0, 'default_target_operator': '<=',
    }


def _flag(name: str, description: str, body: str, weight: float) -> dict:
    """A pass/fail check. Weighted ones carry no target; informational ones target a pass."""
    target = {} if weight else {'default_target': 1.0, 'default_target_operator': '>='}
    return {
        'name': name, 'description': description, 'body': body, 'return_contract': 'bool',
        'scale_type': 'binary', 'scale_min': 0.0, 'scale_max': 1.0,
        'polarity': 'higher_better', 'default_weight': weight,
        'default_target': None, 'default_target_operator': None, **target,
    }


CHECKS = {
    'trajectory.tool_match': {
        'name': 'trajectory.tool_match',
        'description': (
            "How closely the agent's tool calls follow the case's expected trajectory, 0-1, under "
            "its `match` mode: exact (same calls, same order; 1 or 0), in_order (expected calls in "
            "order, extras allowed), any_order (F1 of expected vs actual calls), subset (share of "
            "actual calls that were expected), superset (share of expected calls made; the default). "
            "Expected `args` are compared per `args_match` (subset by default, or exact). Skipped "
            "when the case has no expected trajectory."),
        'body': _TOOL_MATCH, 'return_contract': 'number',
        'scale_type': 'continuous', 'scale_min': 0.0, 'scale_max': 1.0,
        'polarity': 'higher_better', 'default_weight': 1.0,
        'default_target': None, 'default_target_operator': None,
    },
    'trajectory.forbidden_tools': _flag(
        'trajectory.forbidden_tools',
        "Passes when the agent called none of the case's `forbidden` tools. Skipped when the case "
        "lists none.", _FORBIDDEN, 1.0),
    'trajectory.tool_errors': _count(
        'trajectory.tool_errors',
        'Number of tool calls that returned an error. Informational; target: none.', _TOOL_ERRORS),
    'trajectory.redundant_calls': _count(
        'trajectory.redundant_calls',
        'Number of repeats of an identical tool call (same tool, same arguments) that had already '
        "succeeded. A repeat after a failure is a retry and does not count; tools in the case's "
        '`allow_repeat` are excluded. Informational; target: none.', _REDUNDANT),
    'trajectory.step_budget': _flag(
        'trajectory.step_budget',
        "Passes when the agent made no more tool calls than the case's `max_tool_calls`. Skipped "
        'when the case sets no budget. Informational.', _STEP_BUDGET, 0.0),
    'trajectory.step_limit_hit': _flag(
        'trajectory.step_limit_hit',
        "Passes when the run did not end on the agent's step limit. Informational.", _STEP_LIMIT, 0.0),
    'trajectory.guardrail_events': _count(
        'trajectory.guardrail_events',
        'Number of blocked or authorization-paused tool calls, plus a human-in-the-loop pause that '
        'ended the run. Informational; target: none.', _GUARDRAIL),
}

_SCALE_FIELDS = ('scale_type', 'scale_min', 'scale_max', 'polarity', 'default_weight',
                 'default_target', 'default_target_operator')


def script_for(key: str) -> str:
    return _PRELUDE + CHECKS[key]['body']


def builtin_key(meta: Optional[dict]) -> Optional[str]:
    """The check a registry/projected row names in ``meta.builtin_check``, if it is a known one."""
    key = (meta or {}).get('builtin_check')
    return key if key in CHECKS else None


def builtin_code(meta: Optional[dict]) -> Optional[Tuple[str, str]]:
    """``(code, return_contract)`` for a built-in check row, else ``None``."""
    key = builtin_key(meta)
    if key is None:
        return None
    return script_for(key), CHECKS[key]['return_contract']


def registry_seed() -> list:
    """Registry rows for every check, as ``EvalPlatformDimension`` keyword arguments."""
    rows = []
    for key, check in CHECKS.items():
        rows.append({
            'name': check['name'],
            'description': check['description'],
            'allowed_engines': ['code'],
            **{field: check[field] for field in _SCALE_FIELDS},
            'meta': {'builtin_check': key, 'default_evidence_scope': dict(TRAJECTORY_SCOPE)},
        })
    return rows
