"""Human-score write + read for Agent-Evaluation runs (EVAL-P1-B6, §15.5 / §15.6, D2).

Human scores are the one mutable layer, and they are **append-only** (D2, §3.4): every write
is a new row, and reads take the latest by ``created_at`` per (run, case, dimension). No row is
ever mutated — the full sequence is the audit trail (§15.6). A ``is_latest`` flag is maintained
for fast "latest" reads; the previous latest row for the same key is flipped off on each write.

Each write re-aggregates the run headline (§15.5 → §20.6). Run/result orchestration is H5/B5;
this module owns the human layer and the re-aggregation entry point those tickets plug into.
"""

from typing import List, Optional

from tools import db

from ..models.evaluation import (
    EvalRun,
    EvalResult,
    EvalHumanScore,
    EvalDimension,
    EvalResultStatus,
)
from ..models.pd.evaluation import EvalHumanScoreCreateModel
from .evaluation_library_utils import EvalLibraryError, _session
from .evaluation_scoring import (
    normalize_score,
    snapshot_weight_map,
    fold_latest_normalized,
    aggregate_run_score,
)


class EvalRunNotFoundError(EvalLibraryError):
    http_status = 404

    def __init__(self, run_id: int):
        super().__init__(f'Eval run with id {run_id} not found')
        self.run_id = run_id


def _dimension_scale(snapshot: dict, dimension_id: int, live: Optional[EvalDimension]):
    """Resolve (scale_type, scale_min, scale_max, polarity) for a dimension. Prefer the run's
    frozen snapshot (§3.4 — history must not shift when a definition is later edited); fall back
    to the live definition when the snapshot lacks it. Returns ``None`` if neither is available."""
    dims = (snapshot or {}).get('dimensions') or {}
    spec = dims.get(str(dimension_id)) or dims.get(dimension_id)
    if spec:
        return (
            spec.get('scale_type', 'continuous'),
            spec.get('scale_min'),
            spec.get('scale_max'),
            spec.get('polarity', 'higher_better'),
        )
    if live is not None:
        return (live.scale_type, live.scale_min, live.scale_max, live.polarity)
    return None


def write_human_score(
    project_id: int,
    run_id: int,
    data: EvalHumanScoreCreateModel,
    reviewer_id: int,
    session=None,
) -> EvalHumanScore:
    """Append a new human-score row for a (run, case, dimension). Flips the previous latest row
    for the same key off, computes ``normalized_score`` server-side (§20.3), inserts the new row
    as latest, then re-aggregates the run headline (§15.5)."""
    with _session(session, project_id) as s:
        run = s.query(EvalRun).filter(EvalRun.id == run_id).first()
        if not run:
            raise EvalRunNotFoundError(run_id)

        # supersede the current latest for this exact key (append-only, no overwrite — D2)
        (
            s.query(EvalHumanScore)
            .filter(
                EvalHumanScore.run_id == run_id,
                EvalHumanScore.dataset_case_id == data.dataset_case_id,
                EvalHumanScore.dimension_id == data.dimension_id,
                EvalHumanScore.is_latest.is_(True),
            )
            .update({EvalHumanScore.is_latest: False}, synchronize_session=False)
        )

        live_dim = s.query(EvalDimension).filter(EvalDimension.id == data.dimension_id).first()
        scale = _dimension_scale(run.snapshot, data.dimension_id, live_dim)
        normalized = (
            normalize_score(data.native_score, *scale) if scale is not None else None
        )

        row = EvalHumanScore(
            run_id=run_id,
            dataset_case_id=data.dataset_case_id,
            dimension_id=data.dimension_id,
            reviewer_id=reviewer_id,
            native_score=data.native_score,
            normalized_score=normalized,
            note=data.note,
            is_latest=True,
        )
        s.add(row)
        s.flush()

        _reaggregate_run(s, run)
        s.refresh(row)
        return row


def list_human_scores(
    project_id: int,
    run_id: int,
    dataset_case_id: Optional[int] = None,
    dimension_id: Optional[int] = None,
    latest_only: bool = False,
    session=None,
) -> List[EvalHumanScore]:
    """Audit trail (§15.6): all human-score rows for a run, newest first, optionally filtered to
    one case/dimension. ``latest_only`` returns just the current annotation per key."""
    with _session(session, project_id) as s:
        query = s.query(EvalHumanScore).filter(EvalHumanScore.run_id == run_id)
        if dataset_case_id is not None:
            query = query.filter(EvalHumanScore.dataset_case_id == dataset_case_id)
        if dimension_id is not None:
            query = query.filter(EvalHumanScore.dimension_id == dimension_id)
        if latest_only:
            query = query.filter(EvalHumanScore.is_latest.is_(True))
        return query.order_by(EvalHumanScore.created_at.desc(), EvalHumanScore.id.desc()).all()


def reaggregate_run(project_id: int, run_id: int, session=None) -> Optional[float]:
    """Recompute + persist a run's headline from its latest scores. Public entry point B5 owns;
    B6 calls the internal form inline after each write."""
    with _session(session, project_id) as s:
        run = s.query(EvalRun).filter(EvalRun.id == run_id).first()
        if not run:
            raise EvalRunNotFoundError(run_id)
        return _reaggregate_run(s, run)


def _reaggregate_run(session, run: EvalRun) -> Optional[float]:
    """Headline = mean of per-case weighted scores (§20.6), computed from the latest normalized
    score per (case, validated item): a latest human score overrides the machine result for that
    dimension key (§15.3 — human is the reconciled layer). Weights come from the frozen snapshot
    bindings (default 1.0). Runs through the one shared aggregation path (H5/B5 parity), so a code
    or platform validation keeps its own binding weight instead of collapsing onto the dimension
    key. Sets ``run.headline_score`` and returns it."""
    weights = snapshot_weight_map(run.snapshot)

    results = (
        session.query(EvalResult)
        .filter(
            EvalResult.run_id == run.id,
            EvalResult.status == EvalResultStatus.ok,
        )
        .all()
    )
    human = (
        session.query(EvalHumanScore)
        .filter(
            EvalHumanScore.run_id == run.id,
            EvalHumanScore.is_latest.is_(True),
        )
        .all()
    )

    # latest normalized score per (case, item) via the one shared fold: machine results first,
    # then human dimension overrides (§15.3). Same path B5 read uses → identical headline.
    scored_items = fold_latest_normalized(
        ((r.dataset_case_id, r.dimension_id, r.platform_key, r.normalized_score)
         for r in results),
        ((h.dataset_case_id, h.dimension_id, h.normalized_score) for h in human),
    )
    headline = aggregate_run_score(scored_items, weights)
    run.headline_score = headline
    session.flush()
    return headline
