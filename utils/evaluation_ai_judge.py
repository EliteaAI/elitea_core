"""AI-engine case scorer — EVAL-H1 prototype (design §18).

``evaluate_case`` scores one case against N AI dimensions in a **single batched judge call**
(one ``run_llm_judge`` invocation, not one-per-dimension — the §18.5 default this ticket
measures). It is a thin, deterministic layer over the schema-agnostic judge primitive:

  * pure prompt/payload builders (no I/O) → unit-testable without a live model;
  * the judge is injected (``judge=`` param) so tests exercise scoring/clamping/error-mapping
    against a stub, and production defaults to ``run_llm_judge``;
  * a judge failure (timeout / unparseable / predict error) becomes a **per-dimension error
    result**, never an exception — so one bad case cannot sink a batch run (feeds H5).

Scope note: this module batches whatever dimension group ``evaluation_run_orchestration``
hands it into one prompt; the Axis-C **evidence-scope grouping** (D1 — one judge call per
identical evidence-scope group) is enforced by that caller. This module additionally bounds a
group's own token cost against the judge model's context window
(:func:`estimate_group_tokens`, :func:`split_dimensions_for_budget`,
:func:`_truncate_evidence_for_budget`) so an oversized group is split into multiple smaller
calls rather than overflowing the judge's context window.
"""
import json
from typing import Callable, List, Optional

DEFAULT_JUDGE_TIMEOUT = 60

_BINARY = 'binary'
_ORDINAL = 'ordinal'
_CONTINUOUS = 'continuous'
_NO_RATIONALE = 'No rationale provided by judge.'


def build_judge_system_prompt(dimensions: List[dict]) -> str:
    """System prompt instructing the judge to score each dimension on its own native scale and
    return a strict JSON object ``{"scores": [{"dimension_id", "score", "rationale"}, ...]}``.

    The payload below may carry only a subset of ``input``/``output``/``structure``/
    ``expected_output`` — whichever the binding's evidence_scope selected (§19.4). A key's total
    absence from the payload means it was deliberately excluded from scope, not that it is empty;
    the judge must be told this explicitly, or it penalizes e.g. a structure-only binding for
    having "no output" when output was never meant to be part of the evidence at all."""
    lines = [
        'You are an impartial evaluator. The payload below carries only the evidence fields that '
        'were selected in scope for this evaluation, which may be any subset of: `input` (the '
        "task/user input), `output` (the assistant's response), `structure` (the agent's "
        'configured role/instructions/output-format definition), and `expected_output` (a '
        'reference answer). A field missing from the payload was deliberately excluded from scope '
        '— it is NOT empty or unavailable, so never penalize a dimension for the absence of a '
        'field that simply was not included. Score each dimension below strictly using only the '
        "field(s) actually present in the payload, on that dimension's own native scale. Judge "
        'each dimension independently.',
        '',
        'For every dimension, the rationale must justify the score by explaining what specifically '
        'kept it from the highest end of the scale (or, if it scored at or near the lowest end, why '
        "it wasn't even lower) — name the concrete gap, error, or shortfall that cost points rather "
        'than only describing what went well. A rationale that just restates the score or gives no '
        'reason for the distance from the ceiling/floor is not acceptable.',
        '',
        'Dimensions:',
    ]
    for dim in dimensions:
        lines.append(f"  - id={dim['id']} name={dim.get('name', '')}: "
                     f"{dim.get('definition', '')} [{_scale_hint(dim)}]")
    lines += [
        '',
        'Respond with a SINGLE JSON object and nothing else:',
        '{"scores": [{"dimension_id": <id>, "score": <number>, "rationale": "<why>"}]}',
        'Every dimension must appear exactly once. rationale must be a non-empty sentence that '
        'explains why the score is not the maximum (or, near the minimum, why it is not lower).',
    ]
    return '\n'.join(lines)


def _scale_hint(dim: dict) -> str:
    st = dim.get('scale_type')
    if st == _BINARY:
        return 'binary: 0 or 1'
    if st == _ORDINAL:
        return f"ordinal: integer 1..{dim.get('scale_max', 5)}"
    return (f"continuous: {dim.get('scale_min', 0)}..{dim.get('scale_max', 100)}")


def build_case_payload(evidence: dict, dimensions: List[dict]) -> str:
    """Serialize whatever evidence-scope keys are actually present (+ the dimension ids being
    scored) as the judge user payload JSON. A key entirely absent from ``evidence`` (rather than
    present-but-empty) must stay absent from the payload too — forcing `input`/`output` in with a
    `''` default made a structure-only binding's payload look like "empty output" to the judge,
    even though output was never in scope at all."""
    payload = {}
    if 'input' in evidence:
        payload['input'] = evidence.get('input') or ''
    if 'output' in evidence:
        payload['output'] = evidence.get('output') or ''
    if 'structure' in evidence:
        payload['structure'] = evidence.get('structure')
    if 'expected_output' in evidence:
        payload['expected_output'] = evidence.get('expected_output')
    payload['dimension_ids'] = [d['id'] for d in dimensions]
    return json.dumps(payload, ensure_ascii=False)


def _coerce_and_clamp(raw_score, dim: dict):
    """Return ``(native_score, error)``: bounded float on success, or ``(None, reason)``."""
    st = dim.get('scale_type')
    if st == _BINARY:
        if isinstance(raw_score, bool):
            return (1.0 if raw_score else 0.0), None
        if isinstance(raw_score, (int, float)):
            return (1.0 if raw_score else 0.0), None
        return None, 'non-numeric score'
    if isinstance(raw_score, bool) or not isinstance(raw_score, (int, float)):
        return None, 'non-numeric score'
    value = float(raw_score)
    if st == _ORDINAL:
        lo, hi = 1.0, float(dim.get('scale_max') or 5)
    else:  # continuous
        lo = 0.0 if dim.get('scale_min') is None else float(dim['scale_min'])
        hi = 100.0 if dim.get('scale_max') is None else float(dim['scale_max'])
    return round(max(lo, min(hi, value)), 2), None


def _error_results(dimensions: List[dict], rationale: str, error: str) -> List[dict]:
    return [
        {
            'dimension_id': d['id'], 'dimension_name': d.get('name', ''),
            'native_score': None, 'rationale': rationale, 'status': 'error', 'error': error,
        }
        for d in dimensions
    ]


def _parse_dimension_scores(data: dict, dimensions: List[dict]) -> List[dict]:
    scores = data.get('scores') if isinstance(data, dict) else None
    by_id, by_name = {}, {}
    if isinstance(scores, list):
        for entry in scores:
            if not isinstance(entry, dict):
                continue
            if 'dimension_id' in entry:
                by_id[str(entry['dimension_id'])] = entry
            name = entry.get('dimension_name') or entry.get('name')
            if name:
                by_name[str(name).strip().lower()] = entry

    results = []
    for dim in dimensions:
        entry = by_id.get(str(dim['id'])) or by_name.get(str(dim.get('name', '')).strip().lower())
        if entry is None:
            results.append({
                'dimension_id': dim['id'], 'dimension_name': dim.get('name', ''),
                'native_score': None, 'rationale': 'Judge returned no score for this dimension.',
                'status': 'error', 'error': 'missing',
            })
            continue
        native, err = _coerce_and_clamp(entry.get('score'), dim)
        if err:
            results.append({
                'dimension_id': dim['id'], 'dimension_name': dim.get('name', ''),
                'native_score': None, 'rationale': str(entry.get('rationale') or '').strip()
                or 'Judge returned an unusable score.',
                'status': 'error', 'error': err,
            })
            continue
        rationale = str(entry.get('rationale') or '').strip() or _NO_RATIONALE
        results.append({
            'dimension_id': dim['id'], 'dimension_name': dim.get('name', ''),
            'native_score': native, 'rationale': rationale, 'status': 'scored', 'error': None,
        })
    return results


def estimate_group_tokens(evidence: dict, dims: List[dict], model: Optional[str] = None) -> int:
    """Estimate the token cost of one judge call for this evidence + dimension group.

    Builds the exact prompt/payload :func:`evaluate_case` would send and measures it with the
    shared :mod:`context_manager` estimator (model-aware; falls back to a ``len // 4`` heuristic
    when that plugin/RPC is unavailable, e.g. in this module's unit tests)."""
    text = build_judge_system_prompt(dims) + build_case_payload(evidence, dims)
    try:
        from context_manager.utils.token_estimation import estimate_tokens
        return estimate_tokens(text, model)
    except Exception:  # noqa: BLE001 - estimator unavailable: fall back rather than block scoring
        return len(text) // 4


def split_dimensions_for_budget(
    evidence: dict, dims: List[dict], budget_tokens: int, model: Optional[str] = None,
) -> List[List[dict]]:
    """Greedily bin-pack ``dims`` into the fewest judge-call batches that each fit ``budget_tokens``.

    Recomputes the estimate on the real candidate batch at each step (not a running sum) since
    prompt/payload overhead is not linear per dimension. A single dimension that still overflows
    the budget on its own is returned as its own one-item batch — the caller
    (``_score_ai_group_with_budget``) is responsible for shrinking its evidence via
    :func:`_truncate_evidence_for_budget` before dispatching it."""
    if not dims:
        return []
    batches: List[List[dict]] = []
    current: List[dict] = []
    for dim in dims:
        candidate = current + [dim]
        if current and estimate_group_tokens(evidence, candidate, model) > budget_tokens:
            batches.append(current)
            current = [dim]
        else:
            current = candidate
    if current:
        batches.append(current)
    return batches


_TRUNCATE_ORDER = ('output', 'input', 'structure', 'expected_output')


def _shrink_longest_definition(dims: List[dict], mark: str) -> bool:
    """Halve the longest dimension ``definition`` (rubric) text in place. Returns ``False`` when
    none is worth shrinking further, so the caller knows to stop trying.

    ``EvalDimensionBaseModel.description`` is unbounded author-controlled text and is embedded
    verbatim in the judge system prompt (:func:`build_judge_system_prompt`) — evidence alone is
    not always what pushes a single-dimension call over budget."""
    longest = max(
        (d for d in dims if isinstance(d.get('definition'), str) and len(d['definition']) > 200),
        key=lambda d: len(d['definition']),
        default=None,
    )
    if longest is None:
        return False
    value = longest['definition']
    longest['definition'] = value[:max(100, len(value) // 2)] + mark
    return True


def _truncate_evidence_for_budget(
    evidence: dict, dims: List[dict], budget_tokens: int, model: Optional[str] = None,
) -> tuple:
    """Shrink ``evidence`` and, if that alone isn't enough, the dimensions' own rubric text,
    until a single-dimension judge call fits ``budget_tokens``. Returns ``(evidence, dims)``.

    Only reached when a batch of exactly one dimension still overflows the budget on its own
    (:func:`split_dimensions_for_budget` cannot split further). Trims the largest evidence text
    fields in ``_TRUNCATE_ORDER`` by half repeatedly first; once evidence has nothing left worth
    trimming, falls back to shrinking the dimension's own ``definition`` (a large rubric alone
    can keep a call oversized even with empty evidence). Marks the evidence with
    ``_truncated_for_budget`` (same ``_TRUNCATED_MARK`` convention as the snapshot) and gives up
    after a bounded number of rounds rather than looping forever on a budget that can never be
    met (e.g. prompt overhead alone already exceeds it)."""
    from .evaluation_run_orchestration import _TRUNCATED_MARK

    shrunk_evidence = dict(evidence)
    shrunk_dims = [dict(d) for d in dims]
    for _round in range(20):
        if estimate_group_tokens(shrunk_evidence, shrunk_dims, model) <= budget_tokens:
            break
        field = next((f for f in _TRUNCATE_ORDER
                      if isinstance(shrunk_evidence.get(f), str) and len(shrunk_evidence[f]) > 200), None)
        if field is not None:
            value = shrunk_evidence[field]
            shrunk_evidence[field] = value[:max(100, len(value) // 2)] + _TRUNCATED_MARK
            continue
        if _shrink_longest_definition(shrunk_dims, _TRUNCATED_MARK):
            continue
        break
    shrunk_evidence['_truncated_for_budget'] = True

    try:
        from pylon.core.tools import log
        log.warning(
            'Eval AI judge: evidence/definition truncated to fit token budget %s for dimensions %s',
            budget_tokens, [d.get('id') for d in dims],
        )
    except Exception:  # noqa: BLE001 - logging must never block scoring
        pass

    return shrunk_evidence, shrunk_dims


def evaluate_case(
    project_id: int,
    judge_llm_settings: dict,
    case: dict,
    dimensions: List[dict],
    *,
    timeout: int = DEFAULT_JUDGE_TIMEOUT,
    judge: Optional[Callable[..., dict]] = None,
    user_id: Optional[int] = None,
) -> List[dict]:
    """Score ``case`` against ``dimensions`` with one batched judge call.

    Returns one result dict per dimension (order preserved):
    ``{dimension_id, dimension_name, native_score: float|None, rationale: str,
       status: 'scored'|'error', error: str|None}``. Never raises for a judge-level failure.
    """
    if not dimensions:
        return []
    if judge is None:
        from .llm_judge import run_llm_judge
        judge = run_llm_judge

    system_prompt = build_judge_system_prompt(dimensions)
    payload = build_case_payload(case, dimensions)
    outcome = judge(project_id, judge_llm_settings, system_prompt, payload, timeout,
                    stream_key='eval_judge', user_id=user_id)

    if outcome.get('status') != 'ok':
        return _error_results(
            dimensions,
            rationale=f"Judge unavailable ({outcome.get('status')}).",
            error=outcome.get('error') or outcome.get('status') or 'judge_failed',
        )
    return _parse_dimension_scores(outcome.get('data') or {}, dimensions)
