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


def get_run_results(project_id: int, run_id: int, session=None) -> dict:
    """Read a run's results + a server re-derived weighted headline (§20.6).

    Returns a dict ``{run, results, human_scores, headline_score}`` where ``results`` is every
    :class:`EvalResult` row (all statuses, newest-alignment order), ``human_scores`` is the latest
    annotation per (case, dimension), and ``headline_score`` is aggregated from the same normalized
    items — machine ``ok`` results overlaid by latest human overrides — via the shared path so it
    matches the runner's finish-time value and any client recompute. Raises
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

        results = (
            s.query(EvalResult)
            .filter(EvalResult.run_id == run_id)
            .order_by(EvalResult.dataset_case_id.asc(), EvalResult.id.asc())
            .all()
        )
        human = (
            s.query(EvalHumanScore)
            .filter(
                EvalHumanScore.run_id == run_id,
                EvalHumanScore.is_latest.is_(True),
            )
            .order_by(EvalHumanScore.created_at.desc(), EvalHumanScore.id.desc())
            .all()
        )

        # Re-derive the headline from the exact normalized items being returned (read-only). Only
        # `ok` machine results contribute; errored/pending rows are shown but excluded from the fold.
        scored_items = fold_latest_normalized(
            ((r.dataset_case_id, r.dimension_id, r.code_validation_id, r.platform_key,
              r.normalized_score)
             for r in results if r.status == EvalResultStatus.ok),
            ((h.dataset_case_id, h.dimension_id, h.normalized_score) for h in human),
        )
        headline = aggregate_run_score(scored_items, snapshot_weight_map(run.snapshot))

        return {
            'run': run,
            'results': results,
            'human_scores': human,
            'headline_score': headline,
        }
