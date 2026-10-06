"""Per-case token and cost figures for an evaluation run (#6716 tokenomics, design §3.4, §5.4).

Pure: no ORM, no RPC. ``execute_run`` hands in the pricer and writes the rows. Every figure says
where it came from, and a missing figure stays missing. A case with no usage is
``not_recorded``, never ``0``, and an unpriced model gives ``cost=None``, never ``$0.00``.

* :func:`envelope_usage` reads one indexer envelope (agent or judge call) into a usage dict.
* :func:`merge_usage` sums the judge calls of one case.
* :func:`price_usage` prices a usage dict per model through ``costs_compute_llm_cost``.
* :func:`usage_rollup` builds ``EvalRun.meta.agent_usage`` / ``judge_usage`` from the rows.
* :func:`budget_verdict` checks the agent rows against the suite's ``consumption_budget``.

Token semantics follow LangChain's ``usage_metadata``. ``input_tokens`` includes cached
tokens, as the provider reported them. Pricing subtracts the cache reads and writes, as
``usage.sources.base.billable_input_tokens`` does for inclusive readings, so each is billed at its
own rate once.
"""
from collections import Counter
from decimal import Decimal
from typing import Callable, Iterable, List, Optional

ROLE_AGENT = 'agent'
ROLE_JUDGE = 'judge'

USAGE_RECORDED = 'recorded'
USAGE_NOT_RECORDED = 'not_recorded'
USAGE_NOT_APPLICABLE = 'not_applicable'

COST_PENDING = 'pending'
COST_RUNTIME = 'runtime:costs-catalog'
COST_LEDGER = 'usage_event'
COST_UNPRICED = 'unpriced'

#: The case status ``run_agent`` gives a call the project/member budget gate refused.
STATUS_BUDGET_BLOCKED = 'budget_blocked'

VERDICT_PASS = 'pass'
VERDICT_BREACHED = 'breached'
VERDICT_UNKNOWN = 'unknown'

TOKEN_FIELDS = ('input_tokens', 'output_tokens', 'cache_read_tokens', 'cache_creation_tokens',
                'reasoning_tokens')

# Outcomes with no envelope to read, and the usage_state_reason each one gets.
_NO_ENVELOPE_REASONS = {
    'timeout': 'timeout',
    'predict_exception': 'no_envelope',
    STATUS_BUDGET_BLOCKED: STATUS_BUDGET_BLOCKED,
}

# Validated on the API side by ``EvalSuiteBaseModel`` (models/pd/evaluation.py).
_BUDGET_SCOPES = ('per_case', 'per_run')
_BUDGET_LIMITS = ('tokens', 'cost')


def _count(value) -> int:
    """A provider count, or 0 for anything that is not a non-negative number."""
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value < 0:
        return 0
    return int(value)


def _inner(predict_result) -> Optional[dict]:
    if not isinstance(predict_result, dict):
        return None
    inner = predict_result.get('result', predict_result)
    return inner if isinstance(inner, dict) else None


def _empty_bucket() -> dict:
    return {field: 0 for field in TOKEN_FIELDS}


def _add(into: dict, other: dict) -> None:
    for field in TOKEN_FIELDS:
        into[field] += _count(other.get(field))


def _step_model(step: dict) -> Optional[str]:
    message = step.get('message') if isinstance(step.get('message'), dict) else {}
    generation_info = step.get('generation_info') if isinstance(step.get('generation_info'), dict) else {}
    response_metadata = message.get('response_metadata') if isinstance(message.get('response_metadata'), dict) else {}
    return generation_info.get('model_name') or response_metadata.get('model_name') or None


def _step_usage(step: dict) -> Optional[dict]:
    message = step.get('message') if isinstance(step.get('message'), dict) else {}
    usage = message.get('usage_metadata')
    if not isinstance(usage, dict):
        return None
    inputs = usage.get('input_token_details') if isinstance(usage.get('input_token_details'), dict) else {}
    outputs = usage.get('output_token_details') if isinstance(usage.get('output_token_details'), dict) else {}
    return {
        'input_tokens': _count(usage.get('input_tokens')),
        'output_tokens': _count(usage.get('output_tokens')),
        'cache_read_tokens': _count(inputs.get('cache_read')),
        'cache_creation_tokens': _count(inputs.get('cache_creation')),
        'reasoning_tokens': _count(outputs.get('reasoning')),
    }


def _not_recorded(reason: str) -> dict:
    return {'usage_state': USAGE_NOT_RECORDED, 'usage_state_reason': reason, 'token_source': None,
            'model_name': None, 'models': {}, **_empty_bucket()}


def not_applicable_usage(reason: str) -> dict:
    """No model call was made for this case (structure-only run, unsupported agent type)."""
    return {'usage_state': USAGE_NOT_APPLICABLE, 'usage_state_reason': reason, 'token_source': None,
            'model_name': None, 'models': {}, **_empty_bucket()}


def envelope_usage(predict_result, *, status: Optional[str] = None) -> dict:
    """The usage of one ``predict_sio`` call (agent or judge), per model and in total.

    Steps that carry ``usage_metadata`` are split by model. The envelope totals
    (``chat_history_tokens_input`` / ``llm_response_tokens_output``) come from the indexer's
    callback. They are authoritative and also count estimated calls, which have no per-step
    usage. Whatever the steps do not explain is attributed to the run's most-used model. With
    no model at all it goes to a ``None`` bucket, which cannot be priced.

    Sub-agent calls appear as ordinary thinking steps, so they are included (Q-S1)."""
    if status in _NO_ENVELOPE_REASONS:
        return _not_recorded(_NO_ENVELOPE_REASONS[status])
    inner = _inner(predict_result)
    if inner is None or 'thinking_steps' not in inner and 'chat_history_tokens_input' not in inner:
        return _not_recorded('no_envelope')
    token_source = inner.get('token_source')
    if token_source == 'none':
        # The indexer had no callback, so nothing was counted. That is not a measured zero.
        return _not_recorded('no_callback')

    models: dict = {}
    seen_models: Counter = Counter()
    for step in inner.get('thinking_steps') or []:
        if not isinstance(step, dict):
            continue
        model = _step_model(step)
        if model:
            seen_models[model] += 1
        usage = _step_usage(step)
        if usage is not None:
            _add(models.setdefault(model, _empty_bucket()), usage)

    totals = _empty_bucket()
    for bucket in models.values():
        _add(totals, bucket)
    primary = seen_models.most_common(1)[0][0] if seen_models else None

    # The callback's counters cover calls the provider sent no usage for (tiktoken estimates).
    remainder = {
        'input_tokens': _count(inner.get('chat_history_tokens_input')) - totals['input_tokens'],
        'output_tokens': _count(inner.get('llm_response_tokens_output')) - totals['output_tokens'],
    }
    if any(v > 0 for v in remainder.values()):
        bucket = models.setdefault(primary, _empty_bucket())
        for field, value in remainder.items():
            if value > 0:
                bucket[field] += value
                totals[field] += value

    return {'usage_state': USAGE_RECORDED, 'usage_state_reason': None,
            'token_source': token_source or ('provider' if models else None),
            'model_name': primary, 'models': models, **totals}


def merge_usage(usages: Iterable[dict]) -> Optional[dict]:
    """The sum of several calls' usage (a case's judge groups), or None when there were none.

    One recorded call makes the case ``recorded``. If other calls were not recorded,
    ``usage_state_reason='partial'`` says the sum is a lower bound."""
    usages = [u for u in usages if u]
    if not usages:
        return None
    recorded = [u for u in usages if u.get('usage_state') == USAGE_RECORDED]
    if not recorded:
        return {**usages[0], 'models': {}}
    totals, models = _empty_bucket(), {}
    sources, primaries = set(), Counter()
    for usage in recorded:
        _add(totals, usage)
        for model, bucket in (usage.get('models') or {}).items():
            _add(models.setdefault(model, _empty_bucket()), bucket)
        if usage.get('token_source'):
            sources.add(usage['token_source'])
        if usage.get('model_name'):
            primaries[usage['model_name']] += 1
    return {'usage_state': USAGE_RECORDED,
            'usage_state_reason': 'partial' if len(recorded) < len(usages) else None,
            'token_source': sources.pop() if len(sources) == 1 else ('mixed' if sources else None),
            'model_name': primaries.most_common(1)[0][0] if primaries else None,
            'models': models, **totals}


def _billable_input(bucket: dict) -> int:
    return max(0, bucket['input_tokens'] - bucket['cache_read_tokens'] - bucket['cache_creation_tokens'])


def price_usage(usage: Optional[dict], pricer: Optional[Callable[..., dict]]) -> dict:
    """``{cost, cost_source}`` for one usage dict, priced per model and summed.

    * An unrecorded usage, or no pricer, gives ``cost=None, cost_source=pending``. Settlement may
      still price it from the ledger.
    * A model the catalog does not price, or tokens with no model, gives ``unpriced``. A partial
      sum would read as the whole cost.
    * A pricer that raises leaves the cost ``pending``."""
    if not usage or usage.get('usage_state') != USAGE_RECORDED or pricer is None:
        return {'cost': None, 'cost_source': COST_PENDING}
    total = Decimal(0)
    for model, bucket in (usage.get('models') or {}).items():
        bucket = {**_empty_bucket(), **bucket}
        if not any(bucket[f] for f in TOKEN_FIELDS):
            continue
        if not model:
            return {'cost': None, 'cost_source': COST_UNPRICED}
        try:
            priced = pricer(model_name=model, input_tokens=_billable_input(bucket),
                            output_tokens=bucket['output_tokens'],
                            cache_read_input_tokens=bucket['cache_read_tokens'],
                            cache_creation_input_tokens=bucket['cache_creation_tokens']) or {}
        except Exception:  # noqa: BLE001 - pricing must never fail the run; settlement retries
            return {'cost': None, 'cost_source': COST_PENDING}
        if priced.get('cost') is None:
            return {'cost': None, 'cost_source': COST_UNPRICED}
        total += Decimal(str(priced['cost']))
    return {'cost': total, 'cost_source': COST_RUNTIME}


def usage_row(*, dataset_case_id, case_index: int, role: str, usage: dict, priced: dict,
              case_status: Optional[str]) -> dict:
    """One ``eval_case_usage`` row (without ``run_id``)."""
    return {
        'dataset_case_id': dataset_case_id,
        'case_index': case_index,
        'role': role,
        **{field: _count(usage.get(field)) for field in TOKEN_FIELDS},
        'cost': priced.get('cost'),
        'model_name': usage.get('model_name'),
        'usage_state': usage.get('usage_state'),
        'usage_state_reason': usage.get('usage_state_reason'),
        'token_source': usage.get('token_source'),
        'cost_source': priced.get('cost_source'),
        'case_status': case_status,
        'settled': False,
    }


def _number(value):
    return float(value) if isinstance(value, Decimal) else value


def usage_rollup(rows: List[dict]) -> dict:
    """Totals and per-case averages for one role's rows (``EvalRun.meta.agent_usage``).

    Averages are taken over recorded cases only, and ``excluded_cases`` counts the rest, so no
    case is dropped silently (G7). The cost averages only over priced cases, and
    ``unpriced_cases`` counts the cases left out of that total."""
    recorded = [r for r in rows if r.get('usage_state') == USAGE_RECORDED]
    excluded = {'count': 0, STATUS_BUDGET_BLOCKED: 0, USAGE_NOT_APPLICABLE: 0, USAGE_NOT_RECORDED: 0}
    for row in rows:
        if row.get('usage_state') == USAGE_RECORDED:
            continue
        excluded['count'] += 1
        if row.get('case_status') == STATUS_BUDGET_BLOCKED:
            excluded[STATUS_BUDGET_BLOCKED] += 1
        elif row.get('usage_state') == USAGE_NOT_APPLICABLE:
            excluded[USAGE_NOT_APPLICABLE] += 1
        else:
            excluded[USAGE_NOT_RECORDED] += 1

    totals = {field: sum(_count(r.get(field)) for r in recorded) for field in TOKEN_FIELDS}
    priced = [r for r in recorded if r.get('cost') is not None]
    cost = sum((Decimal(str(r['cost'])) for r in priced), Decimal(0)) if priced else None
    sources = {r.get('cost_source') for r in recorded if r.get('cost_source')}
    token_sources = {r.get('token_source') for r in recorded if r.get('token_source')}
    n = len(recorded)
    return {
        'cases': len(rows),
        'recorded_cases': n,
        'totals': totals,
        'averages': {field: (totals[field] / n if n else None) for field in TOKEN_FIELDS},
        'cost': _number(cost),
        'average_cost': _number(cost / len(priced)) if priced else None,
        'unpriced_cases': n - len(priced),
        'cost_source': sources.pop() if len(sources) == 1 else ('mixed' if sources else None),
        'token_source': token_sources.pop() if len(token_sources) == 1 else ('mixed' if token_sources else None),
        'excluded_cases': excluded,
    }


def _limit_check(values: List[Optional[float]], limit) -> str:
    """``breached`` once the known part already exceeds the limit, ``unknown`` while any figure is
    missing, else ``pass``."""
    known = sum(v for v in values if v is not None)
    if known > limit:
        return VERDICT_BREACHED
    if any(v is None for v in values):
        return VERDICT_UNKNOWN
    return VERDICT_PASS


def _worst(verdicts: Iterable[str]) -> str:
    verdicts = list(verdicts)
    if VERDICT_BREACHED in verdicts:
        return VERDICT_BREACHED
    if VERDICT_UNKNOWN in verdicts:
        return VERDICT_UNKNOWN
    return VERDICT_PASS


def _case_tokens(row: dict) -> Optional[int]:
    if row.get('usage_state') != USAGE_RECORDED:
        return None
    return _count(row.get('input_tokens')) + _count(row.get('output_tokens'))


def _case_cost(row: dict) -> Optional[float]:
    return None if row.get('cost') is None else float(row['cost'])


def budget_verdict(agent_rows: List[dict], budget: Optional[dict]) -> Optional[dict]:
    """Check the agent rows against ``consumption_budget`` → ``EvalRun.meta.budget_verdict``.

    Only the agent role is checked (§5.4). Judge spend is recorded but not gated. Tokens are input
    plus output, because cache tokens are already part of input. Cases refused by the
    project/member gate, and cases where no agent ran, spent nothing and are left out. Any other
    case without a figure makes its limit ``unknown``, never ``pass``. Returns None when the suite
    sets no limit."""
    budget = budget or {}
    limits = [(scope, kind, (budget.get(scope) or {}).get(kind))
              for scope in _BUDGET_SCOPES for kind in _BUDGET_LIMITS]
    limits = [(scope, kind, value) for scope, kind, value in limits if value is not None]
    if not limits:
        return None
    rows = [r for r in agent_rows
            if r.get('case_status') != STATUS_BUDGET_BLOCKED
            and r.get('usage_state') != USAGE_NOT_APPLICABLE]
    read = {'tokens': _case_tokens, 'cost': _case_cost}
    checks = {}
    for scope, kind, limit in limits:
        values = [read[kind](r) for r in rows]
        if scope == 'per_run':
            check = {'limit': limit, 'value': sum(v for v in values if v is not None),
                     'verdict': _limit_check(values, limit)}
            if kind == 'tokens':
                # Whether reaching the limit stopped the run or was only reported.
                check['on_breach'] = budget['per_run'].get('on_breach') or 'stop'
        else:
            per_case = [_limit_check([v], limit) for v in values]
            check = {'limit': limit, 'cases': len(per_case),
                     'breached_cases': per_case.count(VERDICT_BREACHED),
                     'unknown_cases': per_case.count(VERDICT_UNKNOWN),
                     'verdict': _worst(per_case)}
        if kind == 'cost':
            sources = {r.get('cost_source') for r in rows if r.get('cost_source')}
            check['cost_source'] = sources.pop() if len(sources) == 1 else ('mixed' if sources else None)
        checks.setdefault(scope, {})[kind] = check
    return {'verdict': _worst(c['verdict'] for s in checks.values() for c in s.values()), **checks}


def run_tokens(usage: Optional[dict]) -> int:
    """The tokens one agent outcome counts against ``per_run.tokens`` (input + output)."""
    if not usage or usage.get('usage_state') != USAGE_RECORDED:
        return 0
    return _count(usage.get('input_tokens')) + _count(usage.get('output_tokens'))



# --- settlement against the usage ledger (design §4.3) ------------------------------------------

#: ``stream_key`` per role. The predict path stores the stream id as ``usage_event.conversation_id``.
STREAM_KEYS = {ROLE_AGENT: 'eval_agent', ROLE_JUDGE: 'eval_judge'}

SETTLEMENT_PENDING = 'pending'
SETTLEMENT_SETTLED = 'settled'
SETTLEMENT_PARTIAL = 'partial'
SETTLEMENT_UNAVAILABLE = 'unavailable'

_NANO = Decimal(10) ** 9


def case_stream_key(role: str, platform_run_id: Optional[str], case_index: Optional[int]) -> str:
    """``stream_key`` for one case's call, so its ledger rows can be told apart from the others.

    The stream id is ``<key>_<uid>``. The uid keeps each call's checkpointer thread its own. Without
    a run id or index the plain key is returned, and settlement leaves that row on its runtime
    figure."""
    base = STREAM_KEYS[role]
    if not platform_run_id or case_index is None:
        return base
    return f'{base}_{platform_run_id}_{case_index}'


def parse_case_stream(conversation_id, platform_run_id: str) -> Optional[tuple]:
    """``(case_index, role)`` for a ledger row's ``conversation_id``, or None when it is not one of
    this run's case calls."""
    if not isinstance(conversation_id, str) or not platform_run_id:
        return None
    for role, base in STREAM_KEYS.items():
        prefix = f'{base}_{platform_run_id}_'
        if conversation_id.startswith(prefix):
            index = conversation_id[len(prefix):].split('_', 1)[0]
            return (int(index), role) if index.isdigit() else None
    return None


def _expects_ledger(row: dict) -> bool:
    """Rows where a model call was attempted. A refused or skipped case has nothing to settle."""
    return (row.get('case_status') != STATUS_BUDGET_BLOCKED
            and row.get('usage_state') != USAGE_NOT_APPLICABLE)


def settle_usage_rows(rows: List[dict], breakdown: Iterable[dict], platform_run_id: str) -> tuple:
    """``(rows, settlement)``: the usage rows with the ledger's figures where it has them.

    ``breakdown`` is ``usage_eval_run_breakdown``: llm rows summed per ``conversation_id``. A
    row the ledger has calls for takes the ledger's tokens and cost and is marked ``settled``. An
    unpriced call keeps the cost unknown rather than summing the priced part. A row the ledger has
    nothing for keeps its runtime figure, unsettled. This is the degraded mode when the calls did
    not go through a metered interface. ``settlement`` says how far that got."""
    ledger: dict = {}
    for entry in breakdown or []:
        key = parse_case_stream(entry.get('conversation_id'), platform_run_id)
        if key is None or not _count(entry.get('llm_calls')):
            continue
        bucket = ledger.setdefault(key, {'llm_calls': 0, 'cost_nano_usd': 0, 'unpriced_calls': 0,
                                         'model_name': None, **_empty_bucket()})
        _add(bucket, entry)
        for field in ('llm_calls', 'cost_nano_usd', 'unpriced_calls'):
            bucket[field] += _count(entry.get(field))
        bucket['model_name'] = bucket['model_name'] or entry.get('model_name')

    settled_rows, expected, matched = [], 0, set()
    for row in rows:
        expected += _expects_ledger(row)
        key = (row.get('case_index'), row.get('role'))
        bucket = ledger.get(key)
        if bucket is None:
            settled_rows.append(row)
            continue
        matched.add(key)
        unpriced = bucket['unpriced_calls'] > 0
        settled_rows.append({
            **row,
            **{field: bucket[field] for field in TOKEN_FIELDS},
            'model_name': row.get('model_name') or bucket['model_name'],
            'usage_state': USAGE_RECORDED,
            'usage_state_reason': None if row.get('usage_state') == USAGE_RECORDED else 'ledger',
            'token_source': COST_LEDGER,
            'cost': None if unpriced else Decimal(bucket['cost_nano_usd']) / _NANO,
            'cost_source': COST_UNPRICED if unpriced else COST_LEDGER,
            'settled': True,
        })

    count = len(matched)
    if expected and count >= expected:
        state = SETTLEMENT_SETTLED
    elif count:
        state = SETTLEMENT_PARTIAL
    else:
        state = SETTLEMENT_UNAVAILABLE
    return settled_rows, {'state': state, 'settled_rows': count, 'expected_rows': expected,
                          'ledger_calls': sum(b['llm_calls'] for b in ledger.values()),
                          'unmatched_ledger_rows': len(set(ledger) - matched)}


def ledger_calls(breakdown: Iterable[dict]) -> int:
    """How many llm rows the breakdown counts; settlement polls until this stops growing."""
    return sum(_count(entry.get('llm_calls')) for entry in breakdown or [])


# --- pre-run estimate (design Q-S6) -------------------------------------------------------------

def _case_totals(rows: List[dict]) -> List[dict]:
    """One ``{tokens, cost}`` per case, the agent and judge rows of that case added together. Only
    recorded rows count; a case's cost is None when any of its recorded rows is unpriced."""
    cases = {}
    for row in rows:
        if row.get('usage_state') != USAGE_RECORDED:
            continue
        case = cases.setdefault(row.get('case_index'), {'tokens': 0, 'cost': Decimal(0), 'roles': set()})
        case['tokens'] += _count(row.get('input_tokens')) + _count(row.get('output_tokens'))
        case['roles'].add(row.get('role'))
        if row.get('cost') is None or case['cost'] is None:
            case['cost'] = None
        else:
            case['cost'] += Decimal(str(row['cost']))
    return list(cases.values())


def _spread(values: List, cases: int) -> Optional[dict]:
    if not values:
        return None
    mean = sum(values) / len(values)
    return {'low': _number(min(values) * cases), 'expected': _number(mean * cases),
            'high': _number(max(values) * cases)}


def estimate_run(history_rows: List[dict], cases: int) -> Optional[dict]:
    """``cases`` × what one case cost on the suite's last finished run (agent + judge), as a range.

    ``expected`` uses the per-case mean, ``low`` / ``high`` the cheapest and dearest case seen.
    Tokens are input plus output, as in the budget check. The cost range is None when no case of
    that run was fully priced, so the caller can still show tokens. Returns None when the run
    recorded no usage at all: there is nothing to estimate from."""
    per_case = _case_totals(history_rows)
    if not per_case or cases <= 0:
        return None
    priced = [c['cost'] for c in per_case if c['cost'] is not None]
    return {
        'cases': cases,
        'based_on_cases': len(per_case),
        'includes_judge': any(ROLE_JUDGE in c['roles'] for c in per_case),
        'tokens': _spread([c['tokens'] for c in per_case], cases),
        'cost': _spread(priced, cases),
        'unpriced_cases': len(per_case) - len(priced),
    }


def binding_budget(project: Optional[dict], member: Optional[dict]) -> Optional[dict]:
    """The budget that runs out first, from the project and member budget states (USD).

    Each state carries ``remaining`` (None = unlimited). Returns None when neither scope limits
    the caller."""
    scopes = [(scope, state) for scope, state in (('project', project), ('member', member))
              if state and state.get('remaining') is not None]
    if not scopes:
        return None
    scope, state = min(scopes, key=lambda item: item[1]['remaining'])
    return {'scope': scope, 'remaining': float(state['remaining']),
            'limit': state.get('effective_limit'), 'spend_available': state.get('spend_available')}


def estimate_exceeds_budget(estimate: Optional[dict], budget: Optional[dict]) -> Optional[bool]:
    """Whether the expected cost is over the remaining budget. None when either side is unknown."""
    cost = (estimate or {}).get('cost')
    if not cost or not budget:
        return None
    return cost['expected'] > budget['remaining']


def case_usage_view(row: dict) -> dict:
    """One ``eval_case_usage`` row as the case drill-down reads it (#6716): the token columns,
    their total (input + output, as the per-case token limit counts them), ``cost`` as a float
    (null until priced) and where the figures came from."""
    tokens = {field: _count(row.get(field)) for field in TOKEN_FIELDS}
    return {
        'role': row.get('role'),
        'case_index': row.get('case_index'),
        'dataset_case_id': row.get('dataset_case_id'),
        **tokens,
        'total_tokens': tokens['input_tokens'] + tokens['output_tokens'],
        'cost': _number(row.get('cost')),
        'model_name': row.get('model_name'),
        'usage_state': row.get('usage_state'),
        'usage_state_reason': row.get('usage_state_reason'),
        'token_source': row.get('token_source'),
        'cost_source': row.get('cost_source'),
        'settled': bool(row.get('settled')),
    }
