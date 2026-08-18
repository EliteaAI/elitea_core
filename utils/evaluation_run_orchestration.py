"""Async eval-run orchestration — EVAL-H5 (design §7#6, §14.2, §19.5, §20).

One eval run = score every dataset case against every binding in a suite, freeze the whole
thing into an immutable snapshot, and fold the per-item verdicts into a single headline. This
module is the **pure, dependency-free core** of that job (the H1/H2 precedent): grouping,
evidence assembly, verdict → result folding, and aggregation are all plain functions over
dicts, with the two things that touch a model or the SDK injected as callables:

  * ``ai_scorer(evidence, dimensions) -> [per-dim result]`` — production binds
    :func:`evaluation_ai_judge.evaluate_case`; a stub in tests.
  * ``code_scorer(binding, evidence) -> verdict`` — production binds
    :func:`code_validation.run_code_validation`; a stub in tests.

Locked acceptance criteria:
  * **D1 (batched evidence-scope grouping):** AI dimensions that share an identical evidence
    scope are scored in ONE judge call (the Axis-C grouping deferred from H1). Distinct scopes
    get distinct calls so a dimension never sees evidence its scope excluded.
  * **D3 (on-demand version pin):** the snapshot freezes a concrete ``application_version_id``
    (§21.6). :func:`build_run_snapshot` requires it; both entry points thread it through.
  * **E4 (judge fail-closed):** a judge/code failure becomes a **per-item error result**, never
    a raise — the run survives and sibling cases still complete (§19.5 fail-safe).

Aggregation reuses :mod:`evaluation_scoring` through the single shared path, so the headline
this runner writes at finish time is byte-for-byte what B5 re-derives and what B6 recomputes
after a human override.

The DB/dispatch wrapper (:func:`execute_run`) keeps every ORM/SDK import lazy inside the
function so this module loads by source with no ``tools``/SDK present — the pure core is
unit-tested here; the wrapper's live behavior is exercised end-to-end (E2E-09 / E2E-11).
"""
from typing import Any, Callable, List, Optional

from .evaluation_scoring import (
    normalize_score,
    binding_item_key,
    snapshot_weight_map,
    aggregate_run_score,
)

# engine / result-status vocab mirrors models.evaluation (kept as literals so the pure core
# stays import-light — no ORM needed to run or test it).
ENGINE_AI = 'ai'
ENGINE_HUMAN = 'human'
ENGINE_CODE = 'code'

STATUS_OK = 'ok'
STATUS_ERROR = 'error'
STATUS_PENDING_HUMAN = 'pending_human'
STATUS_SKIPPED = 'skipped'          # a code validation's own script returned 'na' (not scope-driven)

TRIGGER_OFFLINE_BATCH = 'offline_batch'
TRIGGER_ON_DEMAND = 'on_demand'


def effective_engine(binding: dict) -> str:
    """Engine a binding actually runs on, independent of a possibly-stale stored value.

    Only dimension bindings honor the editable ``engine`` column (ai/human); code
    validations and platform validations always run on the code engine (§12 — engine
    is overridable for a dimension only). Mirrors the UI ``getBindingEngineLabel`` so a
    binding stored with the wrong engine (e.g. a code validation left at ``'ai'``) still
    dispatches to the code path instead of the AI judge."""
    if binding.get('code_validation_id') is not None or binding.get('platform_key') is not None:
        return ENGINE_CODE
    return binding.get('engine') or ENGINE_AI


# ---------------------------------------------------------------------------
# Immutable snapshot (§3.4 / EvalRun.snapshot)
# ---------------------------------------------------------------------------

def build_run_snapshot(
    *,
    suite: dict,
    dimensions: List[dict],
    code_validations: List[dict],
    bindings: List[dict],
    cases: List[dict],
    application_id: int,
    application_version_id: int,
    dataset_id: Optional[int] = None,
    trigger_type: str = TRIGGER_OFFLINE_BATCH,
) -> dict:
    """Freeze suite config + definitions + bindings + case set + scale specs into one snapshot
    (§3.4 — later edits must never mutate history). Shape is consumed both here and by the B5/B6
    re-aggregation path: ``dimensions`` is keyed by ``str(id)`` (scale lookups), ``bindings`` is a
    flat list (weight map + engine split). ``application_version_id`` is mandatory (D3, §21.6)."""
    if application_version_id is None:
        raise ValueError('application_version_id is required (D3, §21.6 version pin)')

    return {
        'suite': {
            'id': suite.get('id'),
            'name': suite.get('name'),
            'judge_model': suite.get('judge_model'),
        },
        'application_id': application_id,
        'application_version_id': application_version_id,
        'dataset_id': dataset_id,
        'trigger_type': trigger_type,
        'dimensions': {
            str(d['id']): {
                'name': d.get('name'),
                'description': d.get('description'),
                'scale_type': d.get('scale_type', 'continuous'),
                'scale_min': d.get('scale_min'),
                'scale_max': d.get('scale_max'),
                'polarity': d.get('polarity', 'higher_better'),
            }
            for d in dimensions
        },
        'code_validations': {
            str(cv['id']): {
                'name': cv.get('name'),
                'code': cv.get('code'),
                'return_contract': cv.get('return_contract', 'bool'),
                'scale_min': cv.get('scale_min'),
                'scale_max': cv.get('scale_max'),
                'polarity': cv.get('polarity', 'higher_better'),
            }
            for cv in code_validations
        },
        'bindings': [
            {
                'engine': b.get('engine', ENGINE_AI),
                'dimension_id': b.get('dimension_id'),
                'code_validation_id': b.get('code_validation_id'),
                'platform_key': b.get('platform_key'),
                'evidence_scope': b.get('evidence_scope') or {},
                'weight': b.get('weight', 1.0),
                'target': b.get('target'),
                'target_operator': b.get('target_operator'),
                'order_index': b.get('order_index', 0),
            }
            for b in bindings
        ],
        'cases': [
            {
                'id': c['id'],
                'input': c.get('input'),
                'output': c.get('output'),
                'expected_output': c.get('expected_output'),
                'structure': c.get('structure'),
                'variables': c.get('variables') or {},
                'order_index': c.get('order_index', 0),
            }
            for c in cases
        ],
    }


# ---------------------------------------------------------------------------
# Run-assembly helpers (B4) — on-demand cases, reference-free filter, version pin
# ---------------------------------------------------------------------------
# Pure inputs to build_run_snapshot the B4 wrapper computes before it touches the ORM: the
# on-demand case set (from H7 turns), the §14.4 reference-free binding split, and the D3 version
# pin. Kept here with the rest of the dependency-free core so they are unit-tested alongside it.

def cases_from_turns(turns) -> List[dict]:
    """On-demand case set (§14.4): ``(input, output)`` turn pairs → snapshot case dicts. Ids are
    synthetic 1-based (no ``EvalDatasetCase`` row backs an on-demand case) and there is never an
    ``expected_output`` — the on-demand path is reference-free only (§17.5)."""
    cases: List[dict] = []
    for i, pair in enumerate(turns):
        inp, out = pair
        cases.append({
            'id': i + 1,
            'input': inp,
            'output': out,
            'expected_output': None,
            'structure': None,
            'order_index': i,
        })
    return cases


def structure_only_case() -> dict:
    """The single synthetic case a dataset-less structure-only run iterates over (see
    :func:`all_bindings_structure_only`). No dataset row backs it — ``input``/``output`` stay
    ``None`` and are never sent to a scorer because every binding's scope excludes them;
    ``structure`` is filled in by :func:`execute_run` without invoking the live agent."""
    return {'id': None, 'input': None, 'output': None, 'expected_output': None,
            'structure': None, 'variables': {}, 'order_index': 0}


def is_structure_only_binding(binding: dict) -> bool:
    """A binding whose evidence scope needs neither ``input`` nor ``output`` — it can be scored
    purely from the agent's structure/instructions with no per-case data at all. Used to allow an
    offline-batch run with no dataset (§19.4 follow-up): if every binding in the suite is
    structure-only, there is nothing case-shaped left to iterate over, so the run needs neither a
    dataset nor a live agent call to produce evidence."""
    scope = binding.get('evidence_scope') or {}
    return not scope.get('input', True) and not scope.get('output', True)


def all_bindings_structure_only(bindings: List[dict]) -> bool:
    """True only if ``bindings`` is non-empty and every binding is structure-only (see
    :func:`is_structure_only_binding`) — an empty suite is not a structure-only run, it's just an
    empty suite, so callers should keep requiring a dataset for that case."""
    return bool(bindings) and all(is_structure_only_binding(b) for b in bindings)


def resolve_version_id(bindings: List[dict], override: Optional[int] = None) -> Optional[int]:
    """Resolve the run's frozen ``application_version_id`` (D3, §16.3/§21.6). An explicit override
    wins; otherwise the single distinct version pinned across the suite's bindings. Returns ``None``
    when no version is resolvable (caller raises a config error); raises ``ValueError`` when bindings
    disagree (ambiguous pin — the caller must pass an override)."""
    if override is not None:
        return override
    pinned = {
        b.get('application_version_id') for b in bindings
        if b.get('application_version_id') is not None
    }
    if not pinned:
        return None
    if len(pinned) > 1:
        raise ValueError(
            f'bindings pin multiple application versions {sorted(pinned)}; '
            'pass an explicit application_version_id override'
        )
    return pinned.pop()


# ---------------------------------------------------------------------------
# Evidence scope (Axis-C) — D1 grouping + §17.5 reference gating
# ---------------------------------------------------------------------------

def evidence_scope_key(scope: dict) -> tuple:
    """Canonical, hashable key for an evidence scope so identical scopes batch into one judge
    call (D1). Order is fixed; unknown/missing flags default to their DB defaults. ``expected_output``
    is not part of the key — it is attached opportunistically per case (see :func:`select_evidence`),
    not a binding-level scope choice, so it cannot split a group."""
    scope = scope or {}
    return (
        bool(scope.get('structure', False)),
        bool(scope.get('input', True)),
        bool(scope.get('output', True)),
    )


def select_evidence(case: dict, scope: dict) -> dict:
    """The evidence a binding's scope actually exposes to the judge/code (§19.4, simplified follow-up).
    ``output`` / ``input`` default to shown (opt-out); ``structure`` is opt-in (hidden by default).
    ``expected_output`` is not a separate scope toggle: whenever ``output`` is in scope and the case
    happens to carry a non-empty ``expected_output``, it is silently included alongside it as extra
    reference context for the judge/rubric — if the case has none, scoring proceeds on the rubric
    description alone, never skipped. Frozen onto each result row so a reader sees exactly what was
    scored."""
    scope = scope or {}
    evidence: dict = {}
    if scope.get('output', True):
        evidence['output'] = case.get('output')
        expected = case.get('expected_output')
        if expected:
            evidence['expected_output'] = expected
    if scope.get('input', True):
        evidence['input'] = case.get('input')
    if scope.get('structure', False):
        evidence['structure'] = case.get('structure')
    return evidence


# ---------------------------------------------------------------------------
# Verdict → EvalResult-shaped dict
# ---------------------------------------------------------------------------

def _result_row(
    case_id, *, engine, status, evidence,
    dimension_id=None, code_validation_id=None, platform_key=None,
    native_score=None, normalized_score=None, verdict=None,
) -> dict:
    """One EvalResult-shaped dict (persistence layer splats it onto a row). Keys mirror the
    model columns so the aggregate + B5 read see a uniform shape regardless of engine."""
    return {
        'dataset_case_id': case_id,
        'dimension_id': dimension_id,
        'code_validation_id': code_validation_id,
        'platform_key': platform_key,
        'engine': engine,
        'status': status,
        'native_score': native_score,
        'normalized_score': normalized_score,
        'verdict': verdict or {},
        'evidence': evidence,
    }


def _normalize_dimension(native, snapshot: dict, dimension_id) -> Optional[float]:
    spec = (snapshot.get('dimensions') or {}).get(str(dimension_id)) or {}
    try:
        return normalize_score(
            native, spec.get('scale_type', 'continuous'),
            spec.get('scale_min'), spec.get('scale_max'),
            spec.get('polarity', 'higher_better'),
        )
    except ValueError:
        return None  # degenerate scale never sinks the run — item just contributes no score


def _normalize_code(native, snapshot: dict, code_validation_id) -> Optional[float]:
    spec = (snapshot.get('code_validations') or {}).get(str(code_validation_id)) or {}
    scale_type = 'binary' if spec.get('return_contract', 'bool') == 'bool' else 'continuous'
    try:
        return normalize_score(
            native, scale_type, spec.get('scale_min'), spec.get('scale_max'),
            spec.get('polarity', 'higher_better'),
        )
    except ValueError:
        return None


def _map_ai_result(case_id, dim_result: dict, snapshot: dict, evidence: dict) -> dict:
    """AI per-dimension judge result → EvalResult dict. A judge 'error' (E4 fail-closed) becomes an
    error row with no score; a 'scored' result carries native + normalized on the dimension scale."""
    dim_id = dim_result.get('dimension_id')
    if dim_result.get('status') == 'scored':
        native = dim_result.get('native_score')
        return _result_row(
            case_id, engine=ENGINE_AI, status=STATUS_OK, evidence=evidence,
            dimension_id=dim_id, native_score=native,
            normalized_score=_normalize_dimension(native, snapshot, dim_id),
            verdict={'rationale': dim_result.get('rationale'),
                     'dimension_name': dim_result.get('dimension_name')},
        )
    return _result_row(
        case_id, engine=ENGINE_AI, status=STATUS_ERROR, evidence=evidence,
        dimension_id=dim_id,
        verdict={'rationale': dim_result.get('rationale'),
                 'dimension_name': dim_result.get('dimension_name'),
                 'error': dim_result.get('error')},
    )


def _map_code_verdict(case_id, binding: dict, verdict: dict, snapshot: dict, evidence: dict) -> dict:
    """code_validation verdict → EvalResult dict. 'scored' → ok with normalized native; 'error'
    (timeout/OOM/exception) and 'unavailable' (Deno absent, §19.7) both degrade to an error row so
    the run survives; 'na' maps to skipped."""
    cv_id = binding.get('code_validation_id')
    vstatus = verdict.get('status')
    base_verdict = {
        'passed': verdict.get('passed'),
        'stdout': verdict.get('stdout'),
        'stderr': verdict.get('error'),
        'status': vstatus,
        'execution_time': verdict.get('execution_time'),
    }
    if vstatus == 'scored':
        native = verdict.get('native_score')
        return _result_row(
            case_id, engine=ENGINE_CODE, status=STATUS_OK, evidence=evidence,
            code_validation_id=cv_id, native_score=native,
            normalized_score=_normalize_code(native, snapshot, cv_id),
            verdict=base_verdict,
        )
    status = STATUS_SKIPPED if vstatus == 'na' else STATUS_ERROR
    return _result_row(
        case_id, engine=ENGINE_CODE, status=status, evidence=evidence,
        code_validation_id=cv_id, verdict=base_verdict,
    )


# ---------------------------------------------------------------------------
# Per-case orchestration
# ---------------------------------------------------------------------------

def _dimension_specs(bindings: List[dict], snapshot: dict) -> List[dict]:
    """Build the judge dimension payloads for a group of AI bindings from the frozen snapshot."""
    dims = snapshot.get('dimensions') or {}
    specs = []
    for b in bindings:
        dim_id = b.get('dimension_id')
        spec = dims.get(str(dim_id)) or {}
        specs.append({
            'id': dim_id,
            'name': spec.get('name', ''),
            'definition': spec.get('description', ''),
            'scale_type': spec.get('scale_type', 'continuous'),
            'scale_min': spec.get('scale_min'),
            'scale_max': spec.get('scale_max'),
        })
    return specs


def assemble_case_results(
    case: dict,
    snapshot: dict,
    *,
    ai_scorer: Optional[Callable[[dict, List[dict]], List[dict]]] = None,
    code_scorer: Optional[Callable[[dict, dict], dict]] = None,
) -> List[dict]:
    """Score one case against every binding → a list of EvalResult dicts.

    AI dimensions are grouped by evidence scope and scored in one judge call per group (**D1**);
    code bindings dispatch through ``code_scorer``; human bindings emit a ``pending_human`` row for the
    B6 annotation layer to fill. Every failure is a row, never a raise (**E4**).

    When live agent execution failed for this case (``_agent_error`` set by :func:`orchestrate_run`,
    H4), there is no output to score: every machine binding becomes an error row carrying that
    reason and no judge/code call is dispatched; human bindings still emit their pending row."""
    bindings = sorted(snapshot.get('bindings') or [], key=lambda b: b.get('order_index', 0))
    results: List[dict] = []

    agent_error = case.get('_agent_error')
    if agent_error:
        for b in bindings:
            engine = effective_engine(b)
            evidence = select_evidence(case, b.get('evidence_scope') or {})
            if engine == ENGINE_HUMAN:
                results.append(_result_row(
                    case['id'], engine=ENGINE_HUMAN, status=STATUS_PENDING_HUMAN, evidence=evidence,
                    dimension_id=b.get('dimension_id'), verdict={'note': 'Awaiting human review.'}))
            else:
                results.append(_result_row(
                    case['id'], engine=engine, status=STATUS_ERROR, evidence=evidence,
                    dimension_id=b.get('dimension_id'), code_validation_id=b.get('code_validation_id'),
                    platform_key=b.get('platform_key'), verdict={'error': agent_error}))
        return results

    # --- AI: batch dimensions that share an evidence scope (D1) ---
    ai_groups: dict = {}
    for b in bindings:
        if effective_engine(b) != ENGINE_AI:
            continue
        ai_groups.setdefault(evidence_scope_key(b.get('evidence_scope')), []).append(b)

    for _key, group in ai_groups.items():
        scope = group[0].get('evidence_scope') or {}
        evidence = select_evidence(case, scope)
        dims = _dimension_specs(group, snapshot)
        try:
            scored = ai_scorer(evidence, dims) if ai_scorer else []
        except Exception as exc:  # noqa: BLE001 - fail-closed (E4): a scorer crash is per-group error
            scored = [{'dimension_id': d['id'], 'dimension_name': d.get('name'),
                       'native_score': None, 'rationale': None,
                       'status': 'error', 'error': f'AI scorer failed: {exc}'} for d in dims]
        by_id = {str(r.get('dimension_id')): r for r in scored}
        for b in group:
            dim_id = b.get('dimension_id')
            dim_result = by_id.get(str(dim_id)) or {
                'dimension_id': dim_id, 'dimension_name': None, 'native_score': None,
                'rationale': None, 'status': 'error', 'error': 'Judge returned no score.'}
            results.append(_map_ai_result(case['id'], dim_result, snapshot, evidence))

    # --- Code: one dispatch per binding ---
    for b in bindings:
        if effective_engine(b) != ENGINE_CODE:
            continue
        scope = b.get('evidence_scope') or {}
        evidence = select_evidence(case, scope)
        try:
            verdict = code_scorer(b, evidence) if code_scorer else {
                'status': 'error', 'error': 'No code executor configured.'}
        except Exception as exc:  # noqa: BLE001 - fail-closed (E4)
            verdict = {'status': 'error', 'error': f'Code scorer failed: {exc}'}
        results.append(_map_code_verdict(case['id'], b, verdict, snapshot, evidence))

    # --- Human: pending annotation row (B6 fills it, then re-aggregates) ---
    for b in bindings:
        if effective_engine(b) != ENGINE_HUMAN:
            continue
        evidence = select_evidence(case, b.get('evidence_scope') or {})
        results.append(_result_row(
            case['id'], engine=ENGINE_HUMAN, status=STATUS_PENDING_HUMAN, evidence=evidence,
            dimension_id=b.get('dimension_id'),
            verdict={'note': 'Awaiting human review.'},
        ))

    return results


def orchestrate_run(
    snapshot: dict,
    *,
    ai_scorer: Optional[Callable[[dict, List[dict]], List[dict]]] = None,
    code_scorer: Optional[Callable[[dict, dict], dict]] = None,
    agent_runner: Optional[Callable[[dict], dict]] = None,
    on_case_done: Optional[Callable[[int, int], None]] = None,
) -> dict:
    """Run every case in the snapshot → ``{results, headline_score, progress}``.

    When ``agent_runner`` is supplied (offline-batch, H4) and a case has no recorded ``output``,
    the pinned agent is run over the case first to produce it (§14.2). A ``status='ok'`` outcome
    fills ``output``; any other outcome (unsupported/timeout/error) tags the case with
    ``_agent_error`` so its machine bindings become error rows (E4). On-demand cases already carry
    their output and pass ``agent_runner=None``, so that path is unchanged.

    The headline goes through the one shared aggregation path (``aggregate_run_score``) over the
    ``ok`` machine results only — ``pending_human`` / ``error`` / ``skipped`` rows contribute
    nothing, so the number equals what B5 re-derives and what B6 recomputes after a human write.
    ``progress`` counts cases (the machine pass is synchronous; the human layer is appended later).

    ``on_case_done(done, total)`` fires after each case so the caller can publish intermediate
    progress; without it the loop is synchronous end-to-end and a poller would only ever observe
    ``0/N`` then ``N/N``.
    """
    cases = snapshot.get('cases') or []
    total = len(cases)
    resolved_cases: List[dict] = []
    all_results: List[dict] = []
    for case in cases:
        if agent_runner is not None and case.get('output') is None:
            outcome = agent_runner(case)
            if outcome.get('status') == 'ok':
                case = {**case, 'output': outcome.get('output'), 'structure': outcome.get('structure')}
            else:
                case = {**case, 'structure': outcome.get('structure'),
                        '_agent_error':
                        outcome.get('error') or f"agent execution {outcome.get('status')}"}
        resolved_cases.append(case)
        all_results.extend(assemble_case_results(
            case, snapshot, ai_scorer=ai_scorer, code_scorer=code_scorer,
        ))
        if on_case_done is not None:
            # Progress reporting must never take the run down with it.
            try:
                on_case_done(len(resolved_cases), total)
            except Exception:  # noqa: BLE001
                from pylon.core.tools import log  # local: this module loads without pylon present
                log.exception('Eval run progress callback failed')

    weight_map = snapshot_weight_map(snapshot)
    scored_items = [
        (r['dataset_case_id'],
         binding_item_key(r.get('dimension_id'), r.get('code_validation_id'), r.get('platform_key')),
         r['normalized_score'])
        for r in all_results if r['status'] == STATUS_OK
    ]
    headline = aggregate_run_score(scored_items, weight_map)

    return {
        'results': all_results,
        'headline_score': headline,
        'progress': {'done': total, 'total': total},
        # Resolved cases (agent output filled in where the runner produced one) — the caller
        # writes this back onto the persisted snapshot so the drill-down shows the real output
        # instead of the pre-execution `None` (§3 lifecycle: "captures the output").
        'cases': resolved_cases,
    }


# ---------------------------------------------------------------------------
# DB / dispatch wrapper — offline-batch (E2E-09) + on-demand (E2E-11) entry points
# ---------------------------------------------------------------------------
# Everything below touches the ORM / SDK and is exercised live, not in this unit suite. Imports
# stay lazy so the pure core above loads by source with no `tools` / SDK present.

def _make_ai_scorer(project_id: int, judge_settings: dict, *, timeout: int = 60, judge=None):
    """Bind :func:`evaluation_ai_judge.evaluate_case` into the ``ai_scorer`` contract."""
    from .evaluation_ai_judge import evaluate_case

    def _score(evidence: dict, dimensions: List[dict]) -> List[dict]:
        return evaluate_case(project_id, judge_settings, evidence, dimensions,
                             timeout=timeout, judge=judge)

    return _score


def _make_agent_runner(project_id: int, snapshot: dict, *, user_id: int, timeout: int = 120):
    """Bind live agent execution (H4) into the ``agent_runner`` contract for an offline-batch run.

    Loads the run's frozen ``application_version_id`` expanded ``version_details`` **once**, checks
    the P1 agent_type scope, then runs each case's input through the real agent
    (:func:`evaluation_agent_runner.run_agent`). An unsupported agent_type or a failed detail load
    degrades every case to an ``unsupported``/``error`` outcome (E4) rather than raising, so the run
    still finishes with error rows the UI can show."""
    from .evaluation_agent_runner import run_agent, agent_type_supported, agent_structure_snapshot

    application_id = snapshot.get('application_id')
    version_id = snapshot.get('application_version_id')

    try:
        from .application_utils import get_application_version_details_expanded
        version_details = get_application_version_details_expanded(
            project_id, application_id, version_id, user_id)
    except Exception as exc:  # noqa: BLE001 - detail load failure → every case errors, run survives
        message = f'could not load agent version {version_id}: {exc}'

        def _unavailable(_case: dict) -> dict:
            return {'status': 'error', 'output': None, 'error': message, 'structure': None}

        return _unavailable

    supported = agent_type_supported(version_details)
    # computed once per run (§19.4 evidence_scope.structure) and stamped onto every outcome below.
    structure = agent_structure_snapshot(version_details)
    # A dataset-less run only exists because every binding is structure-only (§19.4 follow-up,
    # `create_batch_run`) — there is no real case input to run the agent against, and `structure`
    # is already fully known from `version_details`, so skip the live LLM call entirely.
    structure_only_run = snapshot.get('dataset_id') is None

    def _run(case: dict) -> dict:
        if structure_only_run:
            return {'status': 'ok', 'output': None, 'structure': structure}
        if not supported:
            agent_type = version_details.get('agent_type')
            return {'status': 'unsupported', 'output': None,
                    'error': f"agent_type '{agent_type}' is not supported for live batch execution "
                             '(P1 scope: single-turn agents only, pipelines deferred)',
                    'structure': structure}
        outcome = run_agent(project_id, version_details, case, timeout=timeout)
        return {**outcome, 'structure': structure}

    return _run


def _make_code_scorer(snapshot: dict, executor):
    """Bind :func:`code_validation.run_code_validation` into the ``code_scorer`` contract, pulling
    each script from the frozen snapshot and threading the sandbox ``executor`` (dispatches to the
    indexer's ``indexer_code_validation`` task — the concurrency budget that protects interactive
    traffic lives in that light-task pool; saturation degrades to an error verdict, §19.5)."""
    from .code_validation import run_code_validation, _RESULT_SENTINEL

    def _score(binding: dict, evidence: dict) -> dict:
        cv_id = binding.get('code_validation_id')
        spec = (snapshot.get('code_validations') or {}).get(str(cv_id)) or {}
        return run_code_validation(
            spec.get('code', ''),
            code_validation_id=cv_id, name=spec.get('name', ''),
            output=evidence.get('output'),
            expected=evidence.get('expected_output', _RESULT_SENTINEL)
            if 'expected_output' in evidence else _RESULT_SENTINEL,
            input=evidence.get('input', _RESULT_SENTINEL)
            if 'input' in evidence else _RESULT_SENTINEL,
            structure=evidence.get('structure', _RESULT_SENTINEL)
            if 'structure' in evidence else _RESULT_SENTINEL,
            return_contract=spec.get('return_contract', 'bool'),
            executor=executor,
        )

    return _score


def execute_run(
    project_id: int,
    run_id: int,
    *,
    task_node,
    judge_llm_settings: Optional[dict] = None,
    session=None,
    judge=None,
    executor=None,
) -> dict:
    """Execute a persisted ``created`` run to completion and persist its results + headline.

    Loads the run's frozen snapshot, wires the AI/code scorers, runs :func:`orchestrate_run`,
    writes one ``EvalResult`` per item, and marks the run ``finished`` (or ``errored`` only on an
    orchestration-level failure — per-item failures are already error rows, §19.5).

    Dependencies are injected by the API/RPC layer (B4), matching the H1/H2 precedent — this keeps
    the resolution of the judge model and the arbiter node out of this module:
      * ``task_node`` — the plugin's arbiter node (``self.task_node``); the code executor is built
        from it, dispatching to the ``indexer`` light-task pool (the concurrency budget that
        protects interactive traffic; pool saturation degrades to an error verdict, §19.5).
      * ``judge_llm_settings`` — resolved by the caller (suite override §18.7, else project
        default); falls back to the snapshot's frozen ``suite.judge_model``.
      * ``judge`` / ``executor`` — optional overrides for tests. Live path (E2E-09/E2E-11)."""
    from ..models.evaluation import EvalRun, EvalResult, EvalRunStatus
    from .code_validation import make_task_node_executor
    from .evaluation_library_utils import _session
    from datetime import datetime

    with _session(session, project_id) as s:
        run = s.query(EvalRun).filter(EvalRun.id == run_id).first()
        if not run:
            raise ValueError(f'Eval run {run_id} not found')
        snapshot = run.snapshot or {}
        run.status = EvalRunStatus.running
        run.started_at = datetime.utcnow()
        s.commit()

        def _publish_progress(done: int, total: int) -> None:
            """Publish intermediate progress to pollers.

            The status endpoint reads through a *different* session, so a flush would be invisible —
            only a commit makes the count visible. Safe mid-loop: results accumulate in a plain list
            and nothing is added to the session until the loop finishes, so no partial EvalResult
            rows can leak.
            """
            row = s.query(EvalRun).filter(EvalRun.id == run_id).first()
            if row is not None:
                row.progress = {'done': done, 'total': total}
                s.commit()

        try:
            if executor is None:
                executor = make_task_node_executor(task_node)
            settings = judge_llm_settings or (snapshot.get('suite') or {}).get('judge_model') or {}
            ai_scorer = _make_ai_scorer(project_id, settings, judge=judge)
            code_scorer = _make_code_scorer(snapshot, executor)

            # Live agent execution (H4) only for offline-batch: on-demand cases already carry the
            # stored conversation's output. ``owner_id`` is the acting user for detail resolution.
            agent_runner = None
            if snapshot.get('trigger_type') == TRIGGER_OFFLINE_BATCH:
                agent_runner = _make_agent_runner(project_id, snapshot, user_id=run.owner_id)

            outcome = orchestrate_run(snapshot, ai_scorer=ai_scorer, code_scorer=code_scorer,
                                      agent_runner=agent_runner,
                                      on_case_done=_publish_progress)

            for row in outcome['results']:
                s.add(EvalResult(run_id=run.id, **row))
            run.headline_score = outcome['headline_score']
            run.progress = outcome['progress']
            # Reassign (not mutate in place) so SQLAlchemy detects the JSONB column changed —
            # the drill-down reads `snapshot.cases[i].output`, which is only known post-execution.
            run.snapshot = {**snapshot, 'cases': outcome['cases']}
            run.status = EvalRunStatus.finished
            run.finished_at = datetime.utcnow()
            s.flush()
        except Exception as exc:  # noqa: BLE001 - orchestration-level failure marks the run errored
            # Commit the terminal state before re-raising: the caller's session context rolls back
            # on exception, which would discard it and leave the run stuck at `created` forever.
            # Roll back first so a failed DB transaction can't block the marker write.
            s.rollback()
            run = s.query(EvalRun).filter(EvalRun.id == run_id).first()
            if run is not None:
                run.status = EvalRunStatus.errored
                run.error = str(exc)
                run.finished_at = datetime.utcnow()
                s.commit()
            raise

        return {'run_id': run.id, 'status': run.status,
                'headline_score': run.headline_score, 'progress': run.progress}
