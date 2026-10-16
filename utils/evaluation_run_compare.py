"""Run-over-run **performance diff** (performance design §3.4, main design §21.7 / §25.6).

Compares one run with a reference run, case by case, and is computed on read, never stored. The
reference is, in order: the one asked for, the suite's ``baseline_run_id`` (a dangling id means no
baseline, §21.6), or the previous finished run of the same agent on the same dataset.

Cases align by ``dataset_case_id``. A case whose frozen input or variables changed between the two
snapshots is reported as ``changed`` and left out, because the two figures measure different tasks.
Each metric then covers only the cases that both runs measured:

- latency p50 / p95 and the final-answer time p50 (the ``duration_ms`` of the last top-level LLM
  step) use cases with a recorded trajectory on both sides, so timed-out cases stay out of the
  percentiles (performance Q-P1) and are listed under ``timeouts`` instead;
- agent tokens use cases with recorded usage on both sides, and agent cost uses cases priced on
  both sides. A pending or unpriced case is counted in ``excluded`` rather than read as zero.

A latency change within :data:`LATENCY_NOISE_RELATIVE` or :data:`LATENCY_NOISE_ABSOLUTE_MS`,
whichever is larger, reads ``within_noise``. The time limit decides which cases finish at all, so a
case whose effective limit (its own ``time_limit_seconds``, else the suite's, performance P-5)
differs between the two runs is listed under ``coverage.limit_changed`` and left out of the latency
figures and the timeout lists; its tokens, cost and tool calls are still compared. When that is
every matched case, latency and timeout verdicts are withheld (``not_comparable``).

The pure part (:func:`performance_diff`) takes plain dicts so it is unit-testable without a DB;
:func:`compare_run_performance` loads the rows and resolves the reference.
"""

import json
from typing import Dict, List, Optional

from .evaluation_execution import TRAJECTORY_RECORDED, _percentile
from .evaluation_usage import COST_PENDING, COST_UNPRICED, ROLE_AGENT, USAGE_RECORDED, _count


#: Latency noise tolerance: a change smaller than both of these is not reported as a change.
LATENCY_NOISE_RELATIVE = 0.15
LATENCY_NOISE_ABSOLUTE_MS = 2000

VERDICT_BETTER = 'better'
VERDICT_WORSE = 'worse'
VERDICT_NOISE = 'within_noise'
VERDICT_UNCHANGED = 'unchanged'

REASON_TIME_LIMIT_CHANGED = 'time_limit_changed'
REASON_DIFFERENT_DATASET = 'different_dataset'
REASON_NO_CASES = 'no_comparable_cases'

SOURCE_EXPLICIT = 'explicit'
SOURCE_BASELINE = 'baseline'
SOURCE_PREVIOUS = 'previous'

_LATENCY_METRICS = ('p50_latency_ms', 'p95_latency_ms', 'p50_final_answer_ms')


def final_answer_ms(trajectory: Optional[dict]) -> Optional[float]:
    """``duration_ms`` of the last top-level LLM step: the same step ``performance.final_answer_time``
    scores, so the diff and the dimension never disagree on which step is the answer."""
    steps = (trajectory or {}).get('steps') or []
    answers = [s for s in steps if isinstance(s, dict) and s.get('kind') == 'llm' and not s.get('parent_agent')]
    duration = answers[-1].get('duration_ms') if answers else None
    return duration if isinstance(duration, (int, float)) else None


def _case_fingerprint(case: dict) -> str:
    return json.dumps([case.get('input'), case.get('variables') or {}], sort_keys=True, default=str)


def case_fingerprints(snapshot: dict) -> Dict[int, str]:
    """``dataset_case_id`` → fingerprint of what the agent was asked, from a frozen snapshot."""
    return {c['id']: _case_fingerprint(c) for c in (snapshot or {}).get('cases') or []
            if isinstance(c, dict) and c.get('id') is not None}


def case_limits(side: dict) -> Dict[int, int]:
    """``dataset_case_id`` → the seconds the agent had on it: the case's own frozen
    ``time_limit_seconds``, else the side's suite limit."""
    default = side.get('case_time_limit_seconds')
    return {c['id']: c.get('time_limit_seconds') or default
            for c in (side.get('snapshot') or {}).get('cases') or []
            if isinstance(c, dict) and c.get('id') is not None}


def case_view(execution: Optional[dict], usage: Optional[dict]) -> dict:
    """One case of one run as the diff reads it, from its execution row and its agent usage row."""
    execution = execution or {}
    usage = usage or {}
    metrics = execution.get('metrics') or {}
    recorded = execution.get('trajectory_state') == TRAJECTORY_RECORDED
    usage_recorded = usage.get('usage_state') == USAGE_RECORDED
    priced = (usage_recorded and usage.get('cost') is not None
              and usage.get('cost_source') not in (COST_PENDING, COST_UNPRICED))
    latency = metrics.get('latency_ms')
    return {
        'case_index': execution.get('case_index', usage.get('case_index')),
        'status': execution.get('status') or usage.get('case_status'),
        'recorded': recorded,
        'latency_ms': latency if recorded and isinstance(latency, (int, float)) else None,
        'final_answer_ms': final_answer_ms(execution.get('trajectory')) if recorded else None,
        'tool_calls': _count(metrics.get('tool_calls')) if recorded else None,
        'tokens': (_count(usage.get('input_tokens')) + _count(usage.get('output_tokens'))
                   if usage_recorded else None),
        'cost': float(usage['cost']) if priced else None,
    }


def _noise_ms(reference: float) -> float:
    return max(abs(reference) * LATENCY_NOISE_RELATIVE, LATENCY_NOISE_ABSOLUTE_MS)


def _verdict(run_value, reference_value, *, noisy: bool) -> Optional[str]:
    """Lower is better for every metric here."""
    if run_value is None or reference_value is None:
        return None
    delta = run_value - reference_value
    if delta == 0:
        return VERDICT_UNCHANGED
    if noisy and abs(delta) <= _noise_ms(reference_value):
        return VERDICT_NOISE
    return VERDICT_BETTER if delta < 0 else VERDICT_WORSE


def _metric(run_value, reference_value, cases: int, excluded: int = 0, *, noisy: bool = False,
            not_comparable: Optional[str] = None) -> dict:
    delta = None if run_value is None or reference_value is None else run_value - reference_value
    return {
        'run': run_value,
        'reference': reference_value,
        'delta': delta,
        'delta_pct': (delta / reference_value * 100) if delta is not None and reference_value else None,
        'cases': cases,
        'excluded': excluded,
        'verdict': None if not_comparable else _verdict(run_value, reference_value, noisy=noisy),
        'not_comparable': not_comparable or (REASON_NO_CASES if not cases else None),
    }


def _paired(pairs: List[tuple], field: str) -> tuple:
    both = [(a[field], b[field]) for a, b in pairs if a[field] is not None and b[field] is not None]
    return [a for a, _ in both], [b for _, b in both], len(pairs) - len(both)


def performance_diff(run: dict, reference: dict) -> dict:
    """The diff of two runs. Each argument is ``{snapshot, dataset_id, case_time_limit_seconds,
    cases: {dataset_case_id: case_view(...)}}``, where ``case_time_limit_seconds`` is the suite
    limit and a snapshot case may carry its own ``time_limit_seconds``."""
    run_cases, ref_cases = run['cases'], reference['cases']
    run_prints, ref_prints = case_fingerprints(run.get('snapshot')), case_fingerprints(reference.get('snapshot'))
    common = sorted(set(run_cases) & set(ref_cases))
    changed = [cid for cid in common if run_prints.get(cid) != ref_prints.get(cid)]
    matched = [cid for cid in common if cid not in changed]
    pairs = [(run_cases[cid], ref_cases[cid]) for cid in matched]
    run_limits, ref_limits = case_limits(run), case_limits(reference)

    def limit_of(limits, side, cid):
        return limits.get(cid, side.get('case_time_limit_seconds'))

    limit_changed = [cid for cid in matched
                     if limit_of(run_limits, run, cid) != limit_of(ref_limits, reference, cid)]
    timed = [cid for cid in matched if cid not in limit_changed]
    latency_pairs = [(run_cases[cid], ref_cases[cid]) for cid in timed]

    reasons = []
    if run.get('dataset_id') != reference.get('dataset_id'):
        reasons.append(REASON_DIFFERENT_DATASET)
    cap_changed = bool(matched) and not timed
    if cap_changed:
        reasons.append(REASON_TIME_LIMIT_CHANGED)
    latency_block = REASON_TIME_LIMIT_CHANGED if cap_changed else None

    metrics = {}
    for key, field, percent in (('p50_latency_ms', 'latency_ms', 50), ('p95_latency_ms', 'latency_ms', 95),
                                ('p50_final_answer_ms', 'final_answer_ms', 50)):
        a, b, excluded = _paired(latency_pairs, field)
        metrics[key] = _metric(_percentile(a, percent), _percentile(b, percent), len(a),
                               excluded + len(limit_changed), noisy=True, not_comparable=latency_block)
    for key, field in (('tool_calls', 'tool_calls'), ('agent_tokens', 'tokens'), ('agent_cost', 'cost')):
        a, b, excluded = _paired(pairs, field)
        metrics[key] = _metric(sum(a) if a else None, sum(b) if b else None, len(a), excluded)

    timed_out = {cid for cid, c in run_cases.items() if c['status'] == 'timeout'}
    ref_timed_out = {cid for cid, c in ref_cases.items() if c['status'] == 'timeout'}
    cases = []
    for cid in matched:
        a, b = run_cases[cid], ref_cases[cid]
        same_limit = cid not in limit_changed
        cases.append({
            'dataset_case_id': cid,
            'case_index': a['case_index'],
            'reference_case_index': b['case_index'],
            'status': a['status'],
            'reference_status': b['status'],
            'latency_ms': a['latency_ms'],
            'reference_latency_ms': b['latency_ms'],
            'time_limit_seconds': limit_of(run_limits, run, cid),
            'reference_time_limit_seconds': limit_of(ref_limits, reference, cid),
            'latency_verdict': (_verdict(a['latency_ms'], b['latency_ms'], noisy=True)
                                if same_limit else None),
            'tokens': a['tokens'],
            'reference_tokens': b['tokens'],
        })
    slower = [m for m in _LATENCY_METRICS[:2] if metrics[m]['verdict'] == VERDICT_WORSE]
    return {
        'not_comparable': reasons,
        'noise': {'relative': LATENCY_NOISE_RELATIVE, 'absolute_ms': LATENCY_NOISE_ABSOLUTE_MS},
        'coverage': {
            'run_cases': len(run_cases),
            'reference_cases': len(ref_cases),
            'matched': len(matched),
            'changed': changed,
            'only_in_run': sorted(set(run_cases) - set(ref_cases)),
            'only_in_reference': sorted(set(ref_cases) - set(run_cases)),
            'limit_changed': limit_changed,
        },
        'metrics': metrics,
        'timeouts': {
            'not_comparable': latency_block,
            'run': len(timed_out),
            'reference': len(ref_timed_out),
            'newly_timed_out': [cid for cid in timed if cid in timed_out and cid not in ref_timed_out],
            'no_longer_timed_out': [cid for cid in timed if cid in ref_timed_out and cid not in timed_out],
        },
        'slower_than_reference': bool(slower),
        'cases': cases,
    }


# ----------------------------------------------------------------------------
# DB wrapper
# ----------------------------------------------------------------------------

def _run_header(run, source: Optional[str] = None) -> dict:
    from .evaluation_run_orchestration import suite_time_limit

    header = {
        'id': run.id,
        'status': run.status,
        'suite_id': run.suite_id,
        'dataset_id': run.dataset_id,
        'application_version_id': run.application_version_id,
        'finished_at': run.finished_at.isoformat() if run.finished_at else None,
        'headline_score': run.headline_score,
        'case_time_limit_seconds': suite_time_limit(run.snapshot or {}),
    }
    if source:
        header['source'] = source
    return header


def resolve_reference(s, run, reference_run_id: Optional[int] = None):
    """``(reference_run, source)``, or ``(None, None)`` when the run has nothing to compare with.

    An explicit ``reference_run_id`` must exist (:class:`EvalRunNotFoundError` otherwise). The
    suite baseline is skipped when it no longer exists, is still running, or is the run itself."""
    from ..models.evaluation import EvalRun, EvalRunStatus, EvalSuite
    from .evaluation_human_score_utils import EvalRunNotFoundError

    if reference_run_id is not None:
        reference = s.query(EvalRun).filter(EvalRun.id == reference_run_id).first()
        if reference is None:
            raise EvalRunNotFoundError(reference_run_id)
        return reference, SOURCE_EXPLICIT

    if run.suite_id is not None:
        baseline_id = s.query(EvalSuite.baseline_run_id).filter(EvalSuite.id == run.suite_id).scalar()
        if baseline_id and baseline_id != run.id:
            baseline = s.query(EvalRun).filter(EvalRun.id == baseline_id,
                                               EvalRun.status == EvalRunStatus.finished).first()
            if baseline is not None:
                return baseline, SOURCE_BASELINE

    if run.dataset_id is None:
        return None, None
    previous = (
        s.query(EvalRun)
        .filter(EvalRun.application_id == run.application_id,
                EvalRun.dataset_id == run.dataset_id,
                EvalRun.status == EvalRunStatus.finished,
                EvalRun.id < run.id)
        .order_by(EvalRun.id.desc())
        .first()
    )
    return (previous, SOURCE_PREVIOUS) if previous is not None else (None, None)


def _load_cases(s, run_id: int) -> Dict[int, dict]:
    from ..models.evaluation import EvalCaseExecution, EvalCaseUsage

    executions = {
        r.dataset_case_id: {'case_index': r.case_index, 'status': r.status, 'metrics': r.metrics,
                            'trajectory_state': r.trajectory_state, 'trajectory': r.trajectory}
        for r in s.query(EvalCaseExecution).filter(EvalCaseExecution.run_id == run_id).all()
        if r.dataset_case_id is not None
    }
    usage = {
        r.dataset_case_id: {'case_index': r.case_index, 'usage_state': r.usage_state, 'cost': r.cost,
                            'cost_source': r.cost_source, 'case_status': r.case_status,
                            'input_tokens': r.input_tokens, 'output_tokens': r.output_tokens}
        for r in s.query(EvalCaseUsage).filter(EvalCaseUsage.run_id == run_id,
                                               EvalCaseUsage.role == ROLE_AGENT).all()
        if r.dataset_case_id is not None
    }
    return {cid: case_view(executions.get(cid), usage.get(cid)) for cid in set(executions) | set(usage)}


def compare_run_performance(project_id: int, run_id: int, reference_run_id: Optional[int] = None,
                            session=None) -> dict:
    """``{run, reference, diff}`` for one run. ``reference`` and ``diff`` are ``None`` when there is
    no reference run. Raises :class:`EvalRunNotFoundError` when the run, or an explicitly asked
    reference, is absent."""
    from ..models.evaluation import EvalRun
    from .evaluation_human_score_utils import EvalRunNotFoundError
    from .evaluation_library_utils import _session

    with _session(session, project_id) as s:
        run = s.query(EvalRun).filter(EvalRun.id == run_id).first()
        if run is None:
            raise EvalRunNotFoundError(run_id)
        reference, source = resolve_reference(s, run, reference_run_id)
        if reference is None:
            return {'run': _run_header(run), 'reference': None, 'diff': None}

        run_header, reference_header = _run_header(run), _run_header(reference, source)

        def side(r, header):
            return {'snapshot': r.snapshot or {}, 'dataset_id': r.dataset_id,
                    'case_time_limit_seconds': header['case_time_limit_seconds'],
                    'cases': _load_cases(s, r.id)}

        return {
            'run': run_header,
            'reference': reference_header,
            'diff': performance_diff(side(run, run_header), side(reference, reference_header)),
        }
