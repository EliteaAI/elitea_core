"""Server-side score normalization + weighted aggregation (§20).

One normalization code path (§20.10): the judge/human/code emit a **native** score on the
dimension's author-chosen scale; we map it onto a common 0-100 "quality" axis where 100 always
means best. Polarity flips lower-is-better dims so the aggregate never special-cases direction
(§20.4). Targets are checked on the native scale elsewhere (§20.5) — normalization only feeds
the weighted aggregate (§20.6).

Pure functions (no DB, no ORM): the human-score write (B6) and, later, the run runner (H5) /
results re-aggregation (B5) all call these so the numbers stay reproducible.
"""

import math
from typing import Iterable, List, Optional, Tuple

# scale vocabulary mirrors models.evaluation.EvalScaleType / EvalPolarity, kept as literals
# here so this module stays import-light (unit-testable without the ORM).
_BINARY = 'binary'
_ORDINAL = 'ordinal'
_CONTINUOUS = 'continuous'
_LOWER_BETTER = 'lower_better'


def normalize_score(
    native: Optional[float],
    scale_type: str,
    scale_min: Optional[float] = None,
    scale_max: Optional[float] = None,
    polarity: str = 'higher_better',
) -> Optional[float]:
    """Map ``native`` onto the 0-100 quality axis (§20.3). Returns ``None`` for a ``None``
    input (unscored). Raises ``ValueError`` on a degenerate scale (min == max, ordinal N<=1).

    binary:      truthy -> 100, falsy -> 0
    ordinal:     (v - min) / (max - min) * 100  with the author-chosen [min, max] (min defaults to 1)
    continuous:  (v - min) / (max - min) * 100, clamped to [0, 100]
    polarity:    lower_better flips the result (100 - x) LAST.

    A non-finite ``native`` (NaN/inf — e.g. a divide-by-zero in a number-contract validation
    script) raises: the clamp below would turn NaN into a perfect 100, because
    ``min(100.0, float('nan'))`` is ``100.0`` in Python.
    """
    if native is None:
        return None
    if not math.isfinite(native):
        raise ValueError('native score must be a finite number')

    if scale_type == _BINARY:
        norm = 100.0 if native else 0.0
    elif scale_type == _ORDINAL:
        lo = 1.0 if scale_min is None else float(scale_min)
        hi = scale_max
        if hi is None or hi <= lo:
            raise ValueError('ordinal scale requires scale_max > scale_min')
        norm = (native - lo) / (hi - lo) * 100.0
    else:  # continuous (and code-numeric, which uses the same path with declared [min,max])
        lo = 0.0 if scale_min is None else scale_min
        hi = 100.0 if scale_max is None else scale_max
        if hi == lo:
            raise ValueError('continuous scale requires scale_max != scale_min')
        norm = (native - lo) / (hi - lo) * 100.0

    norm = max(0.0, min(100.0, norm))
    if polarity == _LOWER_BETTER:
        norm = 100.0 - norm
    return round(norm, 2)


def case_weighted_score(scored: Iterable[Tuple[Optional[float], float]]) -> Optional[float]:
    """Weighted mean of normalized scores for one case (§20.6):
    ``Sigma(normalized * weight) / Sigma(weight)`` over scored validations only.

    ``scored`` is an iterable of ``(normalized_score, weight)``. Entries with a ``None``
    score (pending/skipped, §20.6) or weight 0 (informational) are excluded from both
    numerator and denominator. Returns ``None`` when nothing contributes (case provisional).
    """
    num = 0.0
    den = 0.0
    for normalized, weight in scored:
        if normalized is None or not weight:
            continue
        num += normalized * weight
        den += weight
    return num / den if den else None


def run_headline(case_scores: Iterable[Optional[float]]) -> Optional[float]:
    """Run headline = mean of per-case weighted scores, equal weight per case (§15.4/§20.6).
    ``None`` (provisional/empty) cases are excluded. Returns ``None`` when no case scored."""
    vals: List[float] = [c for c in case_scores if c is not None]
    return round(sum(vals) / len(vals), 2) if vals else None


# ---------------------------------------------------------------------------
# One aggregation path shared by the run runner (H5) and re-aggregation (B5/B6)
# ---------------------------------------------------------------------------
# A run's headline is computed in exactly one place so the number the runner
# produces at finish time equals the number B5 re-derives when results are read
# and the number B6 recomputes after a human override (§20.6/§20.10). Both call
# ``aggregate_run_score`` with items keyed by ``binding_item_key`` and weighted by
# ``snapshot_weight_map`` — a code-engine dimension and a platform validation on the
# same case no longer collapse onto each other, and each carries its own binding
# weight instead of silently defaulting.


def binding_item_key(
    dimension_id: Optional[int] = None,
    platform_key: Optional[str] = None,
) -> tuple:
    """Identity of the validated item within a case (§16.2 — exactly one is set). Used both to
    bucket scores and to look up a binding weight, so a human dimension override lands on the same
    key as the machine dimension result it supersedes ``(dimension_id, None)``."""
    return (dimension_id, platform_key)


def snapshot_weight_map(snapshot: dict) -> dict:
    """Map ``binding_item_key -> weight`` from the run's frozen snapshot bindings (§3.4). A binding
    with no explicit weight is omitted so the caller's ``default 1.0`` applies uniformly."""
    weights: dict = {}
    for binding in (snapshot or {}).get('bindings') or []:
        if binding.get('weight') is None:
            continue
        key = binding_item_key(
            binding.get('dimension_id'),
            binding.get('platform_key'),
        )
        weights[key] = binding['weight']
    return weights


def fold_latest_normalized(machine_items, human_items) -> List[tuple]:
    """Collapse machine + human normalized scores to the latest one per ``(case, item)`` (§15.3).

    Machine results are laid down first, then human dimension annotations overwrite the matching
    dimension key ``(dimension_id, None, None)`` — human is the reconciled layer, so a latest human
    score supersedes the machine verdict for that dimension while code/platform items keep their own
    keys and are never collapsed onto the dimension.

    ``machine_items``: iterable of ``(case_id, dimension_id, platform_key, normalized)``.
    ``human_items``:   iterable of ``(case_id, dimension_id, normalized)`` (latest per key).

    Returns a list of ``(case_id, item_key, normalized)`` ready for :func:`aggregate_run_score`.
    Shared by the run runner (H5), results read (B5) and human re-aggregation (B6) so the headline
    is derived in exactly one way regardless of who asks (§20.6/§20.10).
    """
    latest: dict = {}
    for case_id, dimension_id, platform_key, normalized in machine_items:
        latest[(case_id, binding_item_key(dimension_id, platform_key))] = normalized
    for case_id, dimension_id, normalized in human_items:
        latest[(case_id, binding_item_key(dimension_id))] = normalized
    return [(case_id, item_key, normalized) for (case_id, item_key), normalized in latest.items()]


def aggregate_run_score(scored_items, weight_map: dict) -> Optional[float]:
    """Fold per-item normalized scores into a run headline (§20.6) via the one shared path.

    ``scored_items`` is an iterable of ``(case_id, item_key, normalized_or_None)``. Items are
    bucketed by case, each weighted by ``weight_map.get(item_key, 1.0)``; a case reduces to its
    ``case_weighted_score`` (``None``-score/weight-0 items excluded) and the run reduces to the
    ``run_headline`` over cases. Returns ``None`` when nothing contributes.
    """
    by_case: dict = {}
    for case_id, item_key, normalized in scored_items:
        weight = weight_map.get(item_key, 1.0)
        by_case.setdefault(case_id, []).append((normalized, weight))
    return run_headline([case_weighted_score(scored) for scored in by_case.values()])
