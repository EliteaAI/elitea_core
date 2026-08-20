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
import json
import time
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

# Why a run stopped short of its last case. Both keep the partial scorecard, but the user needs to
# know which one happened: one is something they asked for, the other is the platform giving up.
STOP_CANCEL_REQUESTED = 'cancel_requested'
STOP_TIME_BUDGET = 'time_budget_exceeded'

#: Wall-clock ceiling on a whole run, checked at case boundaries. Distinct from the reaper's
#: ``RUN_STALE_AFTER_SECONDS``, which bounds the *quiet gap between two cases* — a run that keeps
#: finishing cases is never stale, so without this cap a large enough dataset (or a pathologically
#: slow agent) can hold a pool slot and heartbeat indefinitely. 6h is far longer than any sane
#: dataset needs and short enough that a stuck run frees its slot within a working day.
RUN_TIME_BUDGET_SECONDS = 6 * 60 * 60
STATUS_SKIPPED = 'skipped'          # a code validation's own script returned 'na' (not scope-driven)

TRIGGER_OFFLINE_BATCH = 'offline_batch'
TRIGGER_ON_DEMAND = 'on_demand'


# Plain exceptions (not EvalLibraryError subclasses) so this module keeps loading without ``tools``.
# Both are raised only by the DB wrapper below, which runs on a daemon thread where the launcher
# logs them — they never need an ``http_status``.

class EvalRunAlreadyStartedError(Exception):
    """Another worker already moved this run out of ``created``; this one must not execute it."""


class EvalRunJudgeUnconfiguredError(Exception):
    """The suite has AI-engine bindings but no judge model resolved (E4 — fail closed)."""


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

#: Per-field cap on the frozen case text, and the total budget the whole case list may spend.
#: A dataset may hold ``MAX_CASES`` rows whose cells the import path caps at 100k each, so the
#: product is what makes an uncapped snapshot dangerous rather than any single case.
MAX_CASE_TEXT = 20_000
MAX_CASES_BYTES = 4_000_000

_CASE_TEXT_FIELDS = ('input', 'output', 'expected_output', 'structure')


def _snapshot_cases(cases: List[dict]) -> List[dict]:
    """Freeze the case set with its text bounded.

    The snapshot is JSONB on one row and is read back whole — by ``execute_run``, by the results
    API, and by the frontend scorecard — so an unbounded case list is both a storage and a
    transfer hazard. Two bounds, because either alone leaves a hole: each text field is clipped at
    ``MAX_CASE_TEXT``, and the list stops spending once it passes ``MAX_CASES_BYTES``, after which
    later cases keep their identity but carry no text. Both mark themselves, so a reader (and the
    scorecard) can tell clipped evidence from a genuinely empty case rather than silently scoring
    against a shortened input.
    """
    frozen: List[dict] = []
    spent = 0
    for c in cases:
        case = {
            'id': c['id'],
            'variables': c.get('variables') or {},
            'order_index': c.get('order_index', 0),
        }
        dropped = spent > MAX_CASES_BYTES
        truncated = False
        for field in _CASE_TEXT_FIELDS:
            value = c.get(field)
            if dropped:
                case[field] = None
                continue
            if isinstance(value, str) and len(value) > MAX_CASE_TEXT:
                value = value[:MAX_CASE_TEXT] + _TRUNCATED_MARK
                truncated = True
            case[field] = value
            spent += len(value) if isinstance(value, str) else 0
        if dropped:
            case['truncated'] = True
            case['dropped'] = True
        elif truncated:
            case['truncated'] = True
        frozen.append(case)
    return frozen


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
        'cases': _snapshot_cases(cases),
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


def snapshot_needs_judge(snapshot: dict) -> bool:
    """True when at least one binding in the snapshot runs on the AI engine, i.e. the run cannot
    proceed without a resolved judge model (E4)."""
    return any(
        effective_engine(b) == ENGINE_AI
        for b in ((snapshot or {}).get('bindings') or [])
    )


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

MAX_ENVELOPE_TEXT = 20_000
MAX_ENVELOPE_BYTES = 256_000

_TRUNCATED_MARK = '… [truncated]'


def cap_envelope(value, max_text: int = MAX_ENVELOPE_TEXT, max_bytes: int = MAX_ENVELOPE_BYTES):
    """Bound an ``evidence``/``verdict`` envelope before it is persisted as JSONB.

    Live agent output and judge rationales flow in unbounded while the import path caps cell
    text, and this platform has already been bitten by oversized JSONB starving the gevent hub.
    Every truncation is marked so a reader can tell a short value from a clipped one:
    strings gain a ``… [truncated]`` suffix, and an envelope still over ``max_bytes`` after that
    collapses to ``{'truncated': True, ...}`` rather than being written whole.
    """
    truncated = False

    def _walk(node):
        nonlocal truncated
        if isinstance(node, str):
            if len(node) > max_text:
                truncated = True
                return node[:max_text] + _TRUNCATED_MARK
            return node
        if isinstance(node, dict):
            return {k: _walk(v) for k, v in node.items()}
        if isinstance(node, (list, tuple)):
            return [_walk(v) for v in node]
        return node

    capped = _walk(value)
    if truncated and isinstance(capped, dict):
        capped['truncated'] = True

    try:
        size = len(json.dumps(capped, default=str))
    except (TypeError, ValueError):
        return {'truncated': True, 'error': 'envelope is not serializable'}
    if size <= max_bytes:
        return capped
    return {
        'truncated': True,
        'reason': f'envelope exceeded {max_bytes} bytes ({size})',
        'keys': sorted(capped.keys()) if isinstance(capped, dict) else None,
    }


def _result_row(
    case_id, *, engine, status, evidence,
    dimension_id=None, code_validation_id=None, platform_key=None,
    native_score=None, normalized_score=None, verdict=None,
) -> dict:
    """One EvalResult-shaped dict (persistence layer splats it onto a row). Keys mirror the
    model columns so the aggregate + B5 read see a uniform shape regardless of engine.

    The two JSONB envelopes are capped here because this is the single point every engine's
    result passes through."""
    return {
        'dataset_case_id': case_id,
        'dimension_id': dimension_id,
        'code_validation_id': code_validation_id,
        'platform_key': platform_key,
        'engine': engine,
        'status': status,
        'native_score': native_score,
        'normalized_score': normalized_score,
        'verdict': cap_envelope(verdict or {}),
        'evidence': cap_envelope(evidence),
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


def run_one_case(
    case: dict,
    snapshot: dict,
    *,
    agent_runner: Optional[Callable[[dict], dict]] = None,
    ai_scorer: Optional[Callable[[dict, List[dict]], List[dict]]] = None,
    code_scorer: Optional[Callable[[dict, dict], dict]] = None,
) -> tuple:
    """Resolve one case's output (H4) then score it → ``(resolved_case, result_rows)``.

    Everything a single case needs, touching no shared state, so :func:`orchestrate_run` can call
    it either in-line or on a worker thread without the two paths diverging.
    """
    if agent_runner is not None and case.get('output') is None:
        outcome = agent_runner(case)
        if outcome.get('status') == 'ok':
            case = {**case, 'output': outcome.get('output'), 'structure': outcome.get('structure')}
        else:
            case = {**case, 'structure': outcome.get('structure'),
                    '_agent_error':
                    outcome.get('error') or f"agent execution {outcome.get('status')}"}
    return case, assemble_case_results(
        case, snapshot, ai_scorer=ai_scorer, code_scorer=code_scorer,
    )


def orchestrate_run(
    snapshot: dict,
    *,
    ai_scorer: Optional[Callable[[dict, List[dict]], List[dict]]] = None,
    code_scorer: Optional[Callable[[dict, dict], dict]] = None,
    agent_runner: Optional[Callable[[dict], dict]] = None,
    on_case_done: Optional[Callable[[int, int], None]] = None,
    should_cancel: Optional[Callable[[], bool]] = None,
    case_concurrency: int = 1,
    time_budget_seconds: Optional[int] = RUN_TIME_BUDGET_SECONDS,
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

    ``should_cancel()`` is polled *before each case is started*: a case's agent call and judge
    dispatches are blocking and already carry their own timeouts, so the run stops at a case
    boundary rather than interrupting work in flight. The cases already scored are kept and
    returned with ``cancelled=True`` — a partial scorecard is more useful than discarding the work,
    and the headline is aggregated over what actually scored.

    ``case_concurrency`` bounds how many cases run at once. **It defaults to 1, i.e. exactly the
    sequential loop**, and that default is deliberate rather than conservative-by-omission: a case
    is a burst of judge predicts plus one code dispatch per code binding, and both degrade to
    *error verdicts* rather than backpressure when saturated (§19.5). Raising it therefore trades
    wall-clock time for a real risk of scoring a run wrongly — judge rate-limit rejections and a
    full indexer light-pool both surface as error rows that are indistinguishable from a genuinely
    failed validation. Raise it only for a deployment whose judge model and code pool have the
    headroom.

    Cases are always submitted in index order and every submitted case is awaited, so the scored
    set is a prefix no matter the concurrency: ``cases`` comes back index-aligned with the frozen
    snapshot (which the caller writes back, and the drill-down indexes into), and ``results`` stays
    in case-then-binding order so persisted row order does not depend on completion timing.

    ``time_budget_seconds`` caps the whole run's wall clock, enforced at the same case boundaries
    as the cancel check (``None`` disables it). It closes the gap the reaper cannot: the reaper
    measures the *quiet interval between two cases*, so a run that keeps finishing cases keeps
    heartbeating and never looks stale no matter how long it has been going. Both stops produce the
    same partial scorecard, but ``stop_reason`` distinguishes them — one is what the user asked for,
    the other is the platform giving up, and a scorecard that cannot tell you which is misleading.
    """
    cases = snapshot.get('cases') or []
    total = len(cases)
    workers = max(1, int(case_concurrency or 1))
    resolved: dict = {}
    results_by_index: dict = {}
    stop_reason: Optional[str] = None
    deadline = (
        time.monotonic() + max(1, int(time_budget_seconds))
        if time_budget_seconds else None
    )

    def _report(done: int) -> None:
        """Progress reporting must never take the run down with it."""
        if on_case_done is None:
            return
        try:
            on_case_done(done, total)
        except Exception:  # noqa: BLE001
            from pylon.core.tools import log  # local: this module loads without pylon present
            log.exception('Eval run progress callback failed')

    def _should_stop() -> bool:
        """Should the run stop before starting another case, and why?

        The budget is checked first so an over-budget run does not spend a DB round trip asking
        about a cancel it is about to stop for anyway. Called from the submitting thread only, so
        the ``stop_reason`` write needs no lock.
        """
        nonlocal stop_reason
        if stop_reason is not None:
            return True
        if deadline is not None and time.monotonic() >= deadline:
            stop_reason = STOP_TIME_BUDGET
        elif should_cancel is not None and should_cancel():
            stop_reason = STOP_CANCEL_REQUESTED
        return stop_reason is not None

    if workers == 1:
        # Kept as a real in-line loop, not a one-worker pool: this is the default path, and running
        # it on the calling thread means the common case never inherits thread-affinity surprises
        # from the agent / judge / code dispatch stack.
        for index, case in enumerate(cases):
            if _should_stop():
                break
            resolved[index], results_by_index[index] = run_one_case(
                cases[index], snapshot, agent_runner=agent_runner,
                ai_scorer=ai_scorer, code_scorer=code_scorer)
            _report(len(resolved))
    else:
        import threading
        from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait

        counter_lock = threading.Lock()
        done_count = 0

        def _count_done() -> int:
            nonlocal done_count
            with counter_lock:
                done_count += 1
                return done_count

        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix='eval_case') as pool:
            pending: dict = {}
            next_index = 0
            while True:
                while stop_reason is None and len(pending) < workers and next_index < total:
                    if _should_stop():
                        break
                    pending[pool.submit(
                        run_one_case, cases[next_index], snapshot, agent_runner=agent_runner,
                        ai_scorer=ai_scorer, code_scorer=code_scorer)] = next_index
                    next_index += 1
                if not pending:
                    break
                # Await whatever finishes first and immediately top the pool back up, so a slow
                # case cannot idle the other slots. Every submitted case is awaited even after a
                # cancel — its agent call is already paid for, so discarding it would throw away
                # work the user is billed for and leave `cases` misaligned.
                completed, _ = wait(list(pending), return_when=FIRST_COMPLETED)
                for future in completed:
                    index = pending.pop(future)
                    # A raise here propagates once the `with` drains the rest, marking the run
                    # errored exactly as the sequential path would.
                    resolved[index], results_by_index[index] = future.result()
                    _report(_count_done())

    scored_case_count = len(resolved)
    # The caller writes `cases` back onto the frozen snapshot, so any case never reached has to be
    # carried through verbatim — dropping it would rewrite history and shrink the run's case set to
    # whatever happened to be scored before the stop.
    resolved_cases: List[dict] = [resolved.get(i, cases[i]) for i in range(total)]
    all_results: List[dict] = [
        row for i in range(total) for row in results_by_index.get(i, [])
    ]

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
        # True for either kind of early stop — both leave a partial scorecard, which is the only
        # thing the terminal status has to express. `stop_reason` carries the distinction.
        'cancelled': stop_reason is not None,
        'stop_reason': stop_reason,
        # On an early stop the count is the cases actually scored, so the progress bar keeps telling
        # the truth about how far the run got instead of jumping to N/N.
        'progress': {'done': scored_case_count, 'total': total},
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
    case_concurrency: int = 1,
    time_budget_seconds: Optional[int] = RUN_TIME_BUDGET_SECONDS,
    judge=None,
    executor=None,
    progress_publisher: Optional[Callable[[dict], None]] = None,
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
      * ``judge`` / ``executor`` — optional overrides for tests. Live path (E2E-09/E2E-11).

    **Sessions are per checkpoint, never held across the run.** A run lasts as long as its dataset
    takes — minutes to hours of blocking agent and judge calls — so a single session spanning it
    would pin one pooled connection (times ``task_limit``, times the gunicorn worker count) for
    that whole time while doing nothing, and an idle connection long enough for a server or pooler
    timeout to reap leaves the terminal write to fail against a dead handle. Each phase below
    therefore opens its own short session and closes it: claim, one per progress heartbeat, one per
    cancel poll, and one for the terminal write. Nothing ORM-mapped is carried between phases —
    only the plain snapshot dict and ``owner_id`` — so there are no detached instances."""
    from ..models.evaluation import EvalRun, EvalResult, EvalRunStatus
    from .code_validation import make_task_node_executor
    from tools import db  # pylint: disable=E0401
    from datetime import datetime

    # --- claim -----------------------------------------------------------------------------
    with db.get_session(project_id) as s:
        run = s.query(EvalRun).filter(EvalRun.id == run_id).first()
        if not run:
            raise ValueError(f'Eval run {run_id} not found')
        snapshot = run.snapshot or {}
        owner_id = run.owner_id

        # Claim the run with a conditional UPDATE: two concurrent launches would both pass a plain
        # read of `status`, both execute, and both write EvalResult rows — duplicated results and a
        # raced headline. Whoever loses the claim exits without touching anything.
        claimed = (
            s.query(EvalRun)
            .filter(EvalRun.id == run_id, EvalRun.status == EvalRunStatus.created)
            .update(
                {EvalRun.status: EvalRunStatus.running, EvalRun.started_at: datetime.utcnow()},
                synchronize_session=False,
            )
        )
        s.commit()
    if not claimed:
        raise EvalRunAlreadyStartedError(run_id)

    def _push(payload: dict) -> None:
        """Hand one progress frame to the injected transport, never letting it break the run.

        The publisher is supplied by the pylon-aware caller (``execute_run_task``) precisely so
        this module keeps loading without pylon. It is a best-effort side channel on top of the
        committed ``progress`` column — a broken socket or event bus must not abort an
        evaluation that is otherwise fine, so failures are logged and swallowed (same posture as
        ``on_case_done`` in :func:`orchestrate_run`).
        """
        if progress_publisher is None:
            return
        try:
            progress_publisher(payload)
        except Exception:  # noqa: BLE001 - a side channel must never fail the run
            try:
                from pylon.core.tools import log  # local: this module loads without pylon present
                log.warning('Eval run %s: progress publish failed', run_id)
            except Exception:  # noqa: BLE001 - not even the logging may re-raise here
                pass

    def _publish_progress(done: int, total: int) -> None:
        """Publish intermediate progress to pollers, and heartbeat for the reaper.

        The status endpoint reads through a different session, so only a commit makes the count
        visible. The row is loaded and assigned rather than bulk-updated so ``updated_at``'s
        ``onupdate`` fires unambiguously — that timestamp is what
        :mod:`evaluation_run_reaper` measures staleness from, so a missed bump would eventually
        get a healthy run failed out from under itself.
        """
        with db.get_session(project_id) as s:
            row = s.query(EvalRun).filter(EvalRun.id == run_id).first()
            if row is not None:
                row.progress = {'done': done, 'total': total}
                s.commit()
        # Push after the commit, so a client that reacts by re-reading the row sees the same count.
        _push({'run_id': run_id, 'project_id': project_id, 'status': EvalRunStatus.running,
               'progress': {'done': done, 'total': total}})

    def _cancel_requested() -> bool:
        """Has someone asked this run to stop? (§14.2 cancel)

        A fresh session per poll, so the flag — written by a request handler in another
        transaction — is always read outside any transaction this run holds open.
        """
        with db.get_session(project_id) as s:
            meta = s.query(EvalRun.meta).filter(EvalRun.id == run_id).scalar() or {}
        return bool(meta.get('cancel_requested'))

    def _mark_errored(message: str) -> None:
        """Record an orchestration-level failure on its own connection.

        The failure may itself be a dead or poisoned connection, so this must not reuse whatever
        session was in play — otherwise the run is left in ``running`` and only the reaper closes
        it out, half an hour later.
        """
        try:
            with db.get_session(project_id) as s:
                row = s.query(EvalRun).filter(EvalRun.id == run_id).first()
                if row is not None:
                    row.status = EvalRunStatus.errored
                    row.error = message
                    row.finished_at = datetime.utcnow()
                    s.commit()
        except Exception:  # noqa: BLE001 - never mask the original failure
            from pylon.core.tools import log  # local: this module loads without pylon present
            log.exception('Could not mark eval run %s (project %s) errored', run_id, project_id)
        # Terminal frame regardless: a client watching this run must not be left on `running`
        # by a failure, whether or not the row itself could be written.
        _push({'run_id': run_id, 'project_id': project_id, 'status': EvalRunStatus.errored,
               'error': message})

    # --- execute (no session held) ---------------------------------------------------------
    try:
        if executor is None:
            executor = make_task_node_executor(task_node)
        settings = judge_llm_settings or (snapshot.get('suite') or {}).get('judge_model') or {}
        # E4 fail-closed: with no judge resolved, every AI dimension would come back as a
        # `predict_error` row and the run would still publish a partial headline that looks
        # legitimate. Refuse the run instead.
        if not settings and snapshot_needs_judge(snapshot):
            raise EvalRunJudgeUnconfiguredError(
                'Suite has AI-scored validations but no judge model is configured '
                '(set one on the suite or as the project default).'
            )
        # Freeze what was actually used to judge. The snapshot's `suite.judge_model` is only the
        # suite's configured *reference*, and `judge_llm_settings` may override it per run, so
        # without this the one input that silently drifts between two runs of the same frozen
        # suite — the model — is the one input the snapshot does not record.
        snapshot['resolved_judge_model'] = settings
        with db.get_session(project_id) as s:
            row = s.query(EvalRun).filter(EvalRun.id == run_id).first()
            if row is not None:
                row.snapshot = {**(row.snapshot or {}), 'resolved_judge_model': settings}
                s.commit()

        ai_scorer = _make_ai_scorer(project_id, settings, judge=judge)
        code_scorer = _make_code_scorer(snapshot, executor)

        # Live agent execution (H4) only for offline-batch: on-demand cases already carry the
        # stored conversation's output. ``owner_id`` is the acting user for detail resolution.
        agent_runner = None
        if snapshot.get('trigger_type') == TRIGGER_OFFLINE_BATCH:
            agent_runner = _make_agent_runner(project_id, snapshot, user_id=owner_id)

        outcome = orchestrate_run(snapshot, ai_scorer=ai_scorer, code_scorer=code_scorer,
                                  agent_runner=agent_runner,
                                  on_case_done=_publish_progress,
                                  should_cancel=_cancel_requested,
                                  case_concurrency=case_concurrency,
                                  time_budget_seconds=time_budget_seconds)
    except Exception as exc:  # noqa: BLE001 - orchestration-level failure marks the run errored
        _mark_errored(str(exc))
        raise

    # --- terminal write --------------------------------------------------------------------
    try:
        with db.get_session(project_id) as s:
            run = s.query(EvalRun).filter(EvalRun.id == run_id).first()
            if run is None:
                raise ValueError(f'Eval run {run_id} disappeared while executing')
            for row in outcome['results']:
                s.add(EvalResult(run_id=run.id, **row))
            run.headline_score = outcome['headline_score']
            run.progress = outcome['progress']
            # Reassign (not mutate in place) so SQLAlchemy detects the JSONB column changed —
            # the drill-down reads `snapshot.cases[i].output`, which is only known post-execution.
            run.snapshot = {**snapshot, 'cases': outcome['cases']}
            # A run stopped early keeps the cases it did score (partial scorecard) but must not be
            # read as a completed evaluation of the whole dataset.
            run.status = (
                EvalRunStatus.cancelled if outcome.get('cancelled') else EvalRunStatus.finished
            )
            if outcome.get('stop_reason') == STOP_TIME_BUDGET:
                # Nobody asked for this stop, so without a reason on the row the run reads as a
                # user cancellation and the missing cases look like someone's choice.
                hours = (time_budget_seconds or 0) / 3600
                run.error = (
                    f'Run stopped after its {hours:g}h time limit with '
                    f"{outcome['progress']['done']} of {outcome['progress']['total']} cases scored. "
                    'The scores below cover only those cases. Split the dataset into smaller runs.'
                )
            run.finished_at = datetime.utcnow()
            s.commit()
            result = {'run_id': run.id, 'status': run.status,
                      'headline_score': run.headline_score, 'progress': run.progress}
            terminal_error = run.error
        _push({**result, 'project_id': project_id, 'error': terminal_error})
        return result
    except Exception as exc:  # noqa: BLE001 - results are lost, but the row must not stay `running`
        _mark_errored(f'Run completed but its results could not be saved: {exc}')
        raise
