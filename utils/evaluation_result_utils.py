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


# A page of executions without step lists is states and counters only, so it may hold a whole run
# (the dataset import cap) — the list view reads every case. Each trajectory can be up to
# MAX_TRAJECTORY_BYTES, so a page that carries them is kept small.
MAX_EXECUTION_LIMIT = 5000
MAX_TRAJECTORY_PAGE = 10


def _execution_row(row, include_trajectory: bool) -> dict:
    return {
        'id': row.id,
        'run_id': row.run_id,
        'dataset_case_id': row.dataset_case_id,
        'case_index': row.case_index,
        'status': row.status,
        'trajectory_state': row.trajectory_state,
        'trajectory_state_reason': row.trajectory_state_reason,
        # Not touched when deferred: reading it would lazy-load the column row by row.
        'trajectory': row.trajectory if include_trajectory else None,
        'metrics': row.metrics or {},
        'created_at': row.created_at.isoformat() if row.created_at else None,
    }


def get_case_executions(
    project_id: int,
    run_id: int,
    session=None,
    case_index: Optional[int] = None,
    include_trajectory: Optional[bool] = None,
    dataset_case_id: Optional[int] = None,
    limit: Optional[int] = None,
    offset: int = 0,
) -> dict:
    """A run's per-case executions (#6809 P1): trajectory state, counters and the trajectory
    itself, which can be hundreds of KB per case.

    ``include_trajectory`` defaults to true only when one case is asked for, by ``dataset_case_id``
    (the scorecard's case key) or ``case_index`` — the drill-down; a list view gets states and
    counters, and the column is not even read. Rows are paged by ``case_index``: up to
    :data:`MAX_EXECUTION_LIMIT` per page without trajectories, :data:`MAX_TRAJECTORY_PAGE` with.

    ``usage`` lists the same cases' ``eval_case_usage`` rows, agent and judge (#6716). An on-demand
    run (output supplied, no agent executed) has no execution rows; ``executions`` is then empty,
    not an error, and ``usage`` covers the page's ``case_index`` window instead. Raises
    :class:`EvalRunNotFoundError` when the run is absent."""
    from sqlalchemy.orm import defer
    from ..models.evaluation import EvalRun, EvalCaseExecution, EvalCaseUsage
    from .evaluation_usage import case_usage_view

    one_case = case_index is not None or dataset_case_id is not None
    if include_trajectory is None:
        include_trajectory = one_case
    page_cap = MAX_TRAJECTORY_PAGE if include_trajectory else MAX_EXECUTION_LIMIT
    page_size = min(limit or page_cap, page_cap)
    offset = max(offset or 0, 0)

    with _session(session, project_id) as s:
        if not s.query(EvalRun.id).filter(EvalRun.id == run_id).first():
            raise EvalRunNotFoundError(run_id)
        query = s.query(EvalCaseExecution).filter(EvalCaseExecution.run_id == run_id)
        usage_query = s.query(EvalCaseUsage).filter(EvalCaseUsage.run_id == run_id)
        if case_index is not None:
            query = query.filter(EvalCaseExecution.case_index == case_index)
            usage_query = usage_query.filter(EvalCaseUsage.case_index == case_index)
        if dataset_case_id is not None:
            query = query.filter(EvalCaseExecution.dataset_case_id == dataset_case_id)
            usage_query = usage_query.filter(EvalCaseUsage.dataset_case_id == dataset_case_id)
        if not include_trajectory:
            query = query.options(defer(EvalCaseExecution.trajectory))
        total = query.count()
        page = query.order_by(EvalCaseExecution.case_index).offset(offset).limit(page_size).all()
        rows = [_execution_row(r, include_trajectory) for r in page]
        # Agent and judge usage of the same cases (#6716). An on-demand run has judge rows and no
        # executions, so these are listed beside the executions rather than inside them.
        if rows:
            usage_query = usage_query.filter(EvalCaseUsage.case_index.in_({r['case_index'] for r in rows}))
        elif not one_case:
            usage_query = usage_query.filter(EvalCaseUsage.case_index >= offset,
                                             EvalCaseUsage.case_index < offset + page_size)
        usage = [
            case_usage_view(r.to_json())
            for r in usage_query.order_by(EvalCaseUsage.case_index, EvalCaseUsage.role).all()
        ]
    return {'run_id': run_id, 'executions': rows, 'usage': usage,
            'total': total, 'limit': page_size, 'offset': offset}
