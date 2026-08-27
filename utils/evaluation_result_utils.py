"""Eval **results** read + server-side re-aggregation (EVAL-P1-B5, §15.5, §20.6, §20.10).

The runner (H5) persists one :class:`EvalResult` per case × validation with both the native score
(author scale) and the normalized 0-100 score (§20.10), and stamps ``run.headline_score`` at finish
time; the human layer (B6) refreshes that headline on every annotation. This module is the **read**
side: it returns the per-item verdicts a UI needs (screen #7) alongside a headline it re-derives on
the fly from the *same* normalized items it returns, through the one shared aggregation path
(:func:`fold_latest_normalized` → :func:`aggregate_run_score`). Because that is the identical path
the runner and B6 use, a client recomputing the weighted headline over the returned per-item scores
lands on the same number the server reports (EVAL-E2E-09).

Read-only: unlike B6's write path this never mutates ``run.headline_score`` — it recomputes for
display so a stale persisted value (e.g. a run still finishing) is not shown as authoritative.
Errored items (E4 fail-closed) are returned so the UI can surface them, but carry no normalized
score and never move the headline (they are excluded by the aggregation, §20.6).
"""

from typing import Optional

from .evaluation_library_utils import _session
from .evaluation_human_score_utils import EvalRunNotFoundError
from .evaluation_scoring import (
    snapshot_weight_map,
    fold_latest_normalized,
    aggregate_run_score,
)


DEFAULT_RESULT_LIMIT = 500
MAX_RESULT_LIMIT = 2000


def get_run_results(
    project_id: int,
    run_id: int,
    session=None,
    limit: Optional[int] = None,
    offset: int = 0,
) -> dict:
    """Read a run's results + a server re-derived weighted headline (§20.6).

    Returns a dict ``{run, results, human_scores, headline_score, total, limit, offset}`` where
    ``results`` is one page of :class:`EvalResult` rows (all statuses, newest-alignment order,
    capped at :data:`MAX_RESULT_LIMIT`), ``human_scores`` is the latest
    annotation per (case, dimension), and ``headline_score`` is aggregated from the same normalized
    items — machine ``ok`` results overlaid by latest human overrides — via the shared path so it
    matches the runner's finish-time value and any client recompute. The headline always spans the
    whole run, never just the requested page. Raises
    :class:`EvalRunNotFoundError` when the run is absent.
    """
    from ..models.evaluation import (
        EvalRun,
        EvalResult,
        EvalHumanScore,
        EvalResultStatus,
    )

    with _session(session, project_id) as s:
        run = s.query(EvalRun).filter(EvalRun.id == run_id).first()
        if not run:
            raise EvalRunNotFoundError(run_id)

        # A run has one result per case × validation, each carrying an unbounded evidence/verdict
        # envelope, so the rows are paginated. The headline below is deliberately *not* — it is
        # folded from a columns-only read of every scored row, or a page boundary would silently
        # change the reported score.
        page_size = min(limit or DEFAULT_RESULT_LIMIT, MAX_RESULT_LIMIT)
        ordered = (
            s.query(EvalResult)
            .filter(EvalResult.run_id == run_id)
            .order_by(EvalResult.dataset_case_id.asc(), EvalResult.id.asc())
        )
        total = ordered.count()
        results = ordered.offset(max(offset, 0)).limit(page_size).all()
        human = (
            s.query(EvalHumanScore)
            .filter(
                EvalHumanScore.run_id == run_id,
                EvalHumanScore.is_latest.is_(True),
            )
            .order_by(EvalHumanScore.created_at.desc(), EvalHumanScore.id.desc())
            .all()
        )

        # Re-derive the headline (read-only) over every `ok` row, not just the returned page.
        # Only the five aggregation columns are read, so this stays cheap even for a large run;
        # errored/pending rows are returned to the UI but excluded from the fold.
        scored_rows = (
            s.query(
                EvalResult.dataset_case_id, EvalResult.dimension_id,
                EvalResult.platform_key,
                EvalResult.normalized_score,
            )
            .filter(
                EvalResult.run_id == run_id,
                EvalResult.status == EvalResultStatus.ok,
            )
            .all()
        )
        scored_items = fold_latest_normalized(
            scored_rows,
            ((h.dataset_case_id, h.dimension_id, h.normalized_score) for h in human),
        )
        headline = aggregate_run_score(scored_items, snapshot_weight_map(run.snapshot))

        return {
            'run': run,
            'results': results,
            'human_scores': human,
            'headline_score': headline,
            'total': total,
            'limit': page_size,
            'offset': max(offset, 0),
        }
