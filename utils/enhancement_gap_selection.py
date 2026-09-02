"""Gap selection + ranking for "Enhance with AI" (ENH-1, §3).

Turns a finished run's snapshot + result rows into a small, ranked payload the LLM can actually
fit in one prompt. A 50-case run with 6 bindings produces 300 result rows; sending them all is
neither affordable nor useful, so this module answers one question deterministically on the
server: *which gaps are worth asking about, and which cases best illustrate each one*.

Pure functions (no DB, no ORM) so the ranking is unit-testable and reproducible — the same run
always yields the same prompt, which is what makes a prompt regression visible.

Two rules worth stating because they are easy to get wrong:

* **Targets are compared on the native scale** with the author's operator (§20.5), exactly as
  the scorecard does client-side. Normalization exists only to make shortfalls from different
  scales comparable to each other during ranking.
* **Shortfall is a magnitude, not a direction.** A ``<=`` target overshot by 10 and a ``>=``
  target undershot by 10 are equally bad, and polarity never enters into it. Deriving shortfall
  from the 0-100 quality axis instead would report 0 for every overshoot.
"""

from typing import List, Optional

# Engine / status / scale vocabulary mirrors models.evaluation, kept as literals so this module
# imports nothing (same convention as evaluation_scoring).
ENGINE_AI = 'ai'
ENGINE_HUMAN = 'human'
ENGINE_CODE = 'code'

STATUS_OK = 'ok'
STATUS_ERROR = 'error'
STATUS_PENDING_HUMAN = 'pending_human'

SCALE_BINARY = 'binary'
SCALE_ORDINAL = 'ordinal'

# §3 caps. The prompt has to hold the agent's full instructions plus this payload, so the case
# budget is deliberately tight: three well-chosen failures explain a dimension better than ten.
MAX_GAP_DIMENSIONS = 5
MAX_CASES_PER_DIMENSION = 3

# Per-field evidence caps. Long agent outputs are the usual reason a payload blows the budget.
MAX_CASE_INPUT_CHARS = 2000
MAX_CASE_OUTPUT_CHARS = 3000
MAX_REASONING_CHARS = 1500
_TRUNCATED_MARK = '... [truncated]'

_OPERATORS = {
    '>=': lambda score, goal: score >= goal,
    '>': lambda score, goal: score > goal,
    '<=': lambda score, goal: score <= goal,
    '<': lambda score, goal: score < goal,
    '==': lambda score, goal: score == goal,
}


def evaluate_target_met(
    native_score: Optional[float],
    operator: Optional[str],
    target: Optional[float],
) -> Optional[bool]:
    """Whether ``native_score`` meets ``target`` under ``operator`` on the native scale (§20.5).

    ``None`` means "not applicable" — no target, no operator, no score, or an operator we do not
    recognise. A missing target is not a pass and not a failure, and collapsing it to either would
    invent gaps or hide them.

    Deliberately mirrors ``evaluateTargetMet`` in ``scorecard.helpers.js``. The two must agree:
    the user decides to click "Enhance with AI" based on the misses the scorecard shows them, so a
    server that counts misses differently would analyse gaps the user cannot see.
    """
    if native_score is None or operator is None or target is None or target == '':
        return None
    try:
        score = float(native_score)
        goal = float(target)
    except (TypeError, ValueError):
        return None
    comparator = _OPERATORS.get(operator)
    return comparator(score, goal) if comparator else None


def native_scale_span(dimension_spec: dict, engine: str = ENGINE_AI) -> Optional[float]:
    """Width of the dimension's native scale, used to express a shortfall as a fraction of what
    the scale can express. ``None`` for a degenerate scale, which makes the shortfall unusable
    rather than infinite.

    Binary scales span 1 (0 -> 1). A code dimension with a ``bool`` return contract is binary
    regardless of its stored scale fields, matching ``_normalize_code`` in the run orchestration.
    """
    spec = dimension_spec or {}
    scale_type = spec.get('scale_type', 'continuous')
    if engine == ENGINE_CODE and spec.get('return_contract', 'bool') == 'bool':
        return 1.0
    if scale_type == SCALE_BINARY:
        return 1.0

    if scale_type == SCALE_ORDINAL:
        low = 1.0 if spec.get('scale_min') is None else float(spec['scale_min'])
        high = spec.get('scale_max')
        if high is None:
            return None
        high = float(high)
    else:
        low = 0.0 if spec.get('scale_min') is None else float(spec['scale_min'])
        high = 100.0 if spec.get('scale_max') is None else float(spec['scale_max'])

    span = high - low
    return span if span > 0 else None


def target_shortfall(
    native_score: Optional[float],
    target: Optional[float],
    span: Optional[float],
) -> Optional[float]:
    """How badly a target was missed, as a 0..1 fraction of the native scale span.

    Absolute distance, not signed: overshooting a ``<=`` target by 10 points on a 0..100 scale
    is a 0.1 shortfall, same as undershooting a ``>=`` target by 10. Callers only ask for this
    once a miss is established, so direction carries no information here.
    """
    if native_score is None or target is None or not span:
        return None
    try:
        distance = abs(float(native_score) - float(target))
    except (TypeError, ValueError):
        return None
    return min(1.0, distance / span)


def _truncate(value: Optional[str], limit: int) -> Optional[str]:
    if value is None:
        return None
    text = value if isinstance(value, str) else str(value)
    return text if len(text) <= limit else text[:limit] + _TRUNCATED_MARK


def _binding_key(binding: dict) -> tuple:
    """Identity of the validated item, matching ``binding_item_key`` in evaluation_scoring so a
    binding, a result row and a human score all land on the same bucket."""
    return (binding.get('dimension_id'), binding.get('platform_key'))


def _result_key(row: dict) -> tuple:
    return (row.get('dimension_id'), row.get('platform_key'))


def _binding_name(binding: dict, dimension_spec: dict) -> str:
    if binding.get('dimension_id') is not None:
        return (dimension_spec or {}).get('name') or f"Dimension #{binding['dimension_id']}"
    return binding.get('platform_key') or 'Platform validation'


def _extract_reasoning(row: dict) -> Optional[str]:
    """The most explanatory text a result row carries, by engine.

    For a code dimension the stderr traceback is the single highest-value piece of evidence in
    the whole payload — it names the exact assertion that failed — so it is preferred over the
    generic pass/fail. AI rows carry the judge's rationale.
    """
    verdict = row.get('verdict') or {}
    if row.get('engine') == ENGINE_CODE:
        parts = [verdict.get('stderr'), verdict.get('stdout')]
        joined = '\n'.join(part for part in parts if part)
        return _truncate(joined, MAX_REASONING_CHARS) or None
    return _truncate(verdict.get('rationale'), MAX_REASONING_CHARS)


def index_latest_human_scores(human_scores) -> dict:
    """``(case_id, dimension_id) -> row`` for the latest human annotation per key.

    Human scores are append-only (§3.4), so rows flagged ``is_latest=False`` are superseded
    history and must not be read as a second opinion on the same case.
    """
    latest = {}
    for row in human_scores or []:
        if row.get('is_latest') is False:
            continue
        latest[(row.get('dataset_case_id'), row.get('dimension_id'))] = row
    return latest


def collect_binding_gaps(snapshot: dict, results, human_scores=None) -> dict:
    """Per-binding miss statistics + the failing cases behind them.

    Returns ``{'gaps': [...], 'coverage': {...}}`` where every gap is a binding that has at least
    one case whose native score missed the binding's target. Bindings with no target configured
    produce no gap: without a target there is no defined notion of failure, and guessing one from
    a low score would make the AI argue against a standard the author never set.

    Excluded from the statistics entirely (§3):
      * ``status='error'`` rows — the judge or script failed, so the agent was never measured;
        treating these as misses would have the AI rewrite instructions to fix infrastructure.
      * ``status='pending_human'`` rows with no human annotation yet — unscored, not failed.
    """
    snapshot = snapshot or {}
    dimensions = snapshot.get('dimensions') or {}
    bindings = snapshot.get('bindings') or []
    cases_by_id = {case.get('id'): case for case in (snapshot.get('cases') or [])}

    results_by_key = {}
    excluded_error = 0
    for row in results or []:
        status = row.get('status')
        if status == STATUS_ERROR:
            excluded_error += 1
            continue
        results_by_key[(row.get('dataset_case_id'), _result_key(row))] = row

    human_index = index_latest_human_scores(human_scores)

    gaps: List[dict] = []
    excluded_pending_human = 0
    targeted_bindings = 0

    for binding in bindings:
        target = binding.get('target')
        operator = binding.get('target_operator')
        if target is None or not operator:
            continue
        targeted_bindings += 1

        key = _binding_key(binding)
        dimension_id = binding.get('dimension_id')
        engine = binding.get('engine', ENGINE_AI)
        spec = dimensions.get(str(dimension_id)) if dimension_id is not None else {}
        span = native_scale_span(spec, engine)

        scored = 0
        missed_cases: List[dict] = []

        for case_id, case in cases_by_id.items():
            row = results_by_key.get((case_id, key))

            if engine == ENGINE_HUMAN:
                human = human_index.get((case_id, dimension_id))
                if not human:
                    excluded_pending_human += 1
                    continue
                native = human.get('native_score')
                reasoning = _truncate(human.get('note'), MAX_REASONING_CHARS)
            else:
                if row is None:
                    continue
                if row.get('status') == STATUS_PENDING_HUMAN:
                    excluded_pending_human += 1
                    continue
                native = row.get('native_score')
                reasoning = _extract_reasoning(row)

            if native is None:
                continue
            scored += 1

            if evaluate_target_met(native, operator, target) is not False:
                continue

            missed_cases.append({
                'case_id': case_id,
                'input': _truncate(case.get('input'), MAX_CASE_INPUT_CHARS),
                'output': _truncate(case.get('output'), MAX_CASE_OUTPUT_CHARS),
                'expected_output': _truncate(case.get('expected_output'), MAX_CASE_OUTPUT_CHARS),
                'native_score': native,
                'shortfall': target_shortfall(native, target, span),
                'reasoning': reasoning,
            })

        if not missed_cases:
            continue

        shortfalls = [case['shortfall'] for case in missed_cases if case['shortfall'] is not None]
        mean_shortfall = sum(shortfalls) / len(shortfalls) if shortfalls else None

        gaps.append({
            'dimension_id': dimension_id,
            'platform_key': binding.get('platform_key'),
            'name': _binding_name(binding, spec),
            'engine': engine,
            # Verbatim from the run snapshot, never from the live dimension row: the rubric may
            # have been edited since, and the AI must judge the rubric that produced these scores.
            'rubric': (spec or {}).get('description'),
            'scale': {
                'type': (spec or {}).get('scale_type'),
                'min': (spec or {}).get('scale_min'),
                'max': (spec or {}).get('scale_max'),
                'polarity': (spec or {}).get('polarity'),
                'return_contract': (spec or {}).get('return_contract'),
            },
            'target': target,
            'target_operator': operator,
            'weight': binding.get('weight', 1.0),
            'scored_count': scored,
            'missed_count': len(missed_cases),
            'miss_rate': len(missed_cases) / scored if scored else None,
            'mean_shortfall': mean_shortfall,
            'cases': missed_cases,
        })

    coverage = {
        'total_cases': len(cases_by_id),
        'total_bindings': len(bindings),
        'targeted_bindings': targeted_bindings,
        'missed_bindings': len(gaps),
        'excluded_error_results': excluded_error,
        'excluded_pending_human': excluded_pending_human,
    }
    return {'gaps': gaps, 'coverage': coverage}


def gap_impact(gap: dict) -> float:
    """``weight × mean_shortfall × miss_rate`` (§3).

    All three factors matter and none substitutes for another: a heavily weighted dimension that
    fails once by a hair is not the run's biggest problem, and neither is an unweighted one that
    fails everywhere. A gap with no measurable shortfall (degenerate scale) still ranks on weight
    and frequency rather than dropping to zero and disappearing.
    """
    weight = gap.get('weight')
    weight = 1.0 if weight is None else float(weight)
    shortfall = gap.get('mean_shortfall')
    miss_rate = gap.get('miss_rate') or 0.0
    if shortfall is None:
        return weight * miss_rate
    return weight * shortfall * miss_rate


def rank_gaps(
    gaps: List[dict],
    max_dimensions: int = MAX_GAP_DIMENSIONS,
    max_cases: int = MAX_CASES_PER_DIMENSION,
) -> List[dict]:
    """Highest-impact gaps first, each trimmed to its worst cases.

    Ties break on ``missed_count`` then name, so the ordering is total and the prompt for a given
    run is byte-stable across calls — otherwise an unchanged run could produce a different
    proposal on every click and there would be no way to tell that from a prompt regression.
    """
    ranked = sorted(
        gaps,
        key=lambda gap: (-gap_impact(gap), -(gap.get('missed_count') or 0), gap.get('name') or ''),
    )[:max_dimensions]

    trimmed = []
    for gap in ranked:
        gap = dict(gap)
        gap['impact'] = round(gap_impact(gap), 6)
        gap['cases'] = sorted(
            gap.get('cases') or [],
            key=lambda case: (-(case.get('shortfall') or 0.0), case.get('case_id') or 0),
        )[:max_cases]
        trimmed.append(gap)
    return trimmed


def select_gaps(
    snapshot: dict,
    results,
    human_scores=None,
    max_dimensions: int = MAX_GAP_DIMENSIONS,
    max_cases: int = MAX_CASES_PER_DIMENSION,
) -> dict:
    """Collect + rank in one call — the shape the prompt builder and the endpoint consume.

    ``coverage`` reports what was left out (capped dimensions, capped cases, excluded rows) so the
    dialog can tell the user the proposal is based on a sample. A truncated analysis presented as
    a complete one is the failure mode this field exists to prevent.
    """
    collected = collect_binding_gaps(snapshot, results, human_scores)
    all_gaps = collected['gaps']
    ranked = rank_gaps(all_gaps, max_dimensions, max_cases)

    coverage = dict(collected['coverage'])
    coverage.update({
        'gap_dimensions_total': len(all_gaps),
        'gap_dimensions_returned': len(ranked),
        'missed_cases_total': sum(gap.get('missed_count') or 0 for gap in all_gaps),
        'missed_cases_returned': sum(len(gap.get('cases') or []) for gap in ranked),
        'max_gap_dimensions': max_dimensions,
        'max_cases_per_dimension': max_cases,
    })
    return {'gaps': ranked, 'coverage': coverage}
