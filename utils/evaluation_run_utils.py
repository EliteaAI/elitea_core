"""DB/dispatch wrapper for eval **runs** — EVAL-P1-B4 (§7#6, §14.2, §14.4).

Thin persistence + launch surface over the H5 pure core (:mod:`evaluation_run_orchestration`).
Two entry points create a ``created`` :class:`EvalRun` with a frozen snapshot — one from a stored
dataset (offline-batch, E2E-09), one from a stored conversation (on-demand, E2E-11) — plus list /
get readers and :func:`launch_run`, which hands the run to H5's :func:`execute_run` on a daemon
thread (the API returns ``202`` immediately; the client polls status/progress).

No orchestration logic lives here (that is H5): this module only turns ORM rows into the pure dicts
:func:`build_run_snapshot` consumes, resolves the D3 version pin, and persists the result. Errors
subclass ``EvalLibraryError`` so the v2 boundary returns ``exc.http_status`` uniformly.

ORM models are imported lazily inside functions (H1/H2 precedent); this wrapper is exercised live,
not in the unit suite — its pure inputs are unit-tested in :mod:`evaluation_run_orchestration`.
"""
import threading
from typing import List, Optional

from pylon.core.tools import log

from .evaluation_library_utils import EvalLibraryError, _session
from .evaluation_suite_utils import EvalSuiteNotFoundError
from .evaluation_human_score_utils import EvalRunNotFoundError
from .evaluation_run_orchestration import (
    build_run_snapshot,
    execute_run,
    cases_from_turns,
    resolve_version_id,
    all_bindings_structure_only,
    structure_only_case,
    TRIGGER_OFFLINE_BATCH,
    TRIGGER_ON_DEMAND,
)


class EvalRunConfigError(EvalLibraryError):
    """Run cannot be assembled: no dataset, no resolvable version pin, ambiguous pin, etc."""
    http_status = 400


# ----------------------------------------------------------------------------
# ORM row -> pure dict extractors (feed build_run_snapshot)
# ----------------------------------------------------------------------------

def _binding_dict(b) -> dict:
    """A binding row as the flat dict the snapshot expects. ``application_version_id`` is carried so
    :func:`resolve_version_id` can find the D3 pin."""
    return {
        'engine': b.engine,
        'dimension_id': b.dimension_id,
        'code_validation_id': b.code_validation_id,
        'platform_key': b.platform_key,
        'application_version_id': b.application_version_id,
        'evidence_scope': b.evidence_scope or {},
        'weight': b.weight,
        'target': b.target,
        'target_operator': b.target_operator,
        'order_index': b.order_index,
    }


def _dimension_dict(d) -> dict:
    return {
        'id': d.id, 'name': d.name, 'description': d.description,
        'scale_type': d.scale_type, 'scale_min': d.scale_min, 'scale_max': d.scale_max,
        'polarity': d.polarity,
    }


def _code_validation_dict(cv) -> dict:
    return {
        'id': cv.id, 'name': cv.name, 'code': cv.code,
        'return_contract': cv.return_contract, 'scale_min': cv.scale_min,
        'scale_max': cv.scale_max, 'polarity': cv.polarity,
    }


def _suite_dict(suite, judge_model_override: Optional[dict] = None) -> dict:
    """Suite header for the snapshot; a run-time ``judge_model`` override (§18.7) wins over the
    suite's own default so :func:`execute_run` picks it up from the frozen snapshot."""
    return {
        'id': suite.id, 'name': suite.name,
        'judge_model': judge_model_override or suite.judge_model,
    }


def _case_dict(c) -> dict:
    """A dataset case as a snapshot case. ``output`` starts ``None``: the offline-batch path has no
    stored agent response, so H5 runs the pinned agent live over ``input`` (+ the case ``variables``,
    §17.1) at execute time to populate it (EVAL-H4, §14.2). ``variables`` is carried so the agent
    run can overlay them onto the version's own variables."""
    return {
        'id': c.id, 'input': c.input, 'output': None,
        'expected_output': c.expected_output, 'structure': None,
        'variables': c.variables or {},
        'order_index': c.order_index,
    }


def _load_suite_config(s, suite_id: int, *, judge_model_override: Optional[dict] = None):
    """Load a suite + its ordered bindings + the dimensions / code-validations those bindings
    reference, all as pure dicts. Raises :class:`EvalSuiteNotFoundError` if the suite is gone."""
    from ..models.evaluation import EvalSuite, EvalBinding, EvalDimension, EvalCodeValidation

    suite = s.query(EvalSuite).filter(EvalSuite.id == suite_id).first()
    if not suite:
        raise EvalSuiteNotFoundError(suite_id)

    binding_rows = (
        s.query(EvalBinding)
        .filter(EvalBinding.suite_id == suite_id)
        .order_by(EvalBinding.order_index.asc(), EvalBinding.id.asc())
        .all()
    )
    bindings = [_binding_dict(b) for b in binding_rows]

    dim_ids = {b['dimension_id'] for b in bindings if b['dimension_id'] is not None}
    cv_ids = {b['code_validation_id'] for b in bindings if b['code_validation_id'] is not None}
    dimensions = (
        [_dimension_dict(d) for d in s.query(EvalDimension).filter(EvalDimension.id.in_(dim_ids)).all()]
        if dim_ids else []
    )
    code_validations = (
        [_code_validation_dict(cv)
         for cv in s.query(EvalCodeValidation).filter(EvalCodeValidation.id.in_(cv_ids)).all()]
        if cv_ids else []
    )
    return suite, bindings, dimensions, code_validations


def _resolve_version(bindings: List[dict], override: Optional[int]) -> int:
    """D3 pin resolution with config-error framing: ambiguous pins and an unresolvable version both
    become a 400 rather than an orchestration-time failure."""
    try:
        version_id = resolve_version_id(bindings, override)
    except ValueError as exc:
        raise EvalRunConfigError(str(exc))
    if version_id is None:
        raise EvalRunConfigError(
            'no application_version_id could be resolved; pin a version on the suite bindings '
            'or pass an explicit application_version_id'
        )
    return version_id


def _assert_version_in_application(s, version_id: int, application_id: int) -> None:
    """The pinned version must belong to the suite's application (§21.6, D3). A cross-app version id
    is a caller error, so reject it at create time with a 400 — otherwise the agent loader
    (EVAL-H4) can only discover it per-case inside the background run, turning a knowable bad
    request into a finished-but-all-errored run."""
    from ..models.all import ApplicationVersion

    version = s.query(ApplicationVersion).filter(ApplicationVersion.id == version_id).first()
    if version is None:
        raise EvalRunConfigError(f'application_version_id {version_id} not found')
    if version.application_id != application_id:
        raise EvalRunConfigError(
            f'application_version_id {version_id} belongs to application '
            f'{version.application_id}, not the suite\'s application {application_id}')


# ----------------------------------------------------------------------------
# create — offline-batch (E2E-09) + on-demand (E2E-11)
# ----------------------------------------------------------------------------

def create_batch_run(
    project_id: int,
    suite_id: int,
    *,
    dataset_id: Optional[int] = None,
    application_version_id: Optional[int] = None,
    judge_model: Optional[dict] = None,
    owner_id: int,
    session=None,
):
    """Freeze a full offline-batch run over a stored dataset and persist it ``created`` (§14.2).
    ``dataset_id`` overrides the suite's own dataset; the D3 version pin is resolved from the
    bindings unless ``application_version_id`` overrides it. Every binding runs (batch is not
    reference-gated at assembly time — §17.5 gating is per-case inside H5)."""
    from ..models.evaluation import EvalRun, EvalDataset, EvalRunStatus, EvalRunTrigger

    with _session(session, project_id) as s:
        suite, bindings, dimensions, code_validations = _load_suite_config(
            s, suite_id, judge_model_override=judge_model)

        ds_id = dataset_id if dataset_id is not None else suite.dataset_id
        if ds_id is None:
            if not all_bindings_structure_only(bindings):
                raise EvalRunConfigError(
                    'offline_batch run requires a dataset (suite has no dataset_id; pass dataset_id)')
            cases = [structure_only_case()]
        else:
            dataset = s.query(EvalDataset).filter(EvalDataset.id == ds_id).first()
            if not dataset:
                raise EvalRunConfigError(f'dataset {ds_id} not found')
            cases = [_case_dict(c) for c in dataset.cases]

        version_id = _resolve_version(bindings, application_version_id)
        _assert_version_in_application(s, version_id, suite.application_id)
        snapshot = build_run_snapshot(
            suite=_suite_dict(suite, judge_model),
            dimensions=dimensions, code_validations=code_validations,
            bindings=bindings, cases=cases,
            application_id=suite.application_id, application_version_id=version_id,
            dataset_id=ds_id, trigger_type=TRIGGER_OFFLINE_BATCH,
        )

        run = EvalRun(
            suite_id=suite_id, application_id=suite.application_id,
            application_version_id=version_id, dataset_id=ds_id,
            trigger_type=EvalRunTrigger.offline_batch, status=EvalRunStatus.created,
            snapshot=snapshot, progress={'done': 0, 'total': len(cases)},
            owner_id=owner_id, meta={},
        )
        s.add(run)
        s.flush()
        s.refresh(run)
        return run


def create_on_demand_run(
    project_id: int,
    suite_id: int,
    conversation_id: int,
    *,
    application_version_id: Optional[int] = None,
    judge_model: Optional[dict] = None,
    owner_id: int,
    session=None,
):
    """Freeze an on-demand run over a stored conversation's turns and persist it ``created``
    (§14.4). The case set comes from H7 turn extraction (synthetic reference-free cases). Every
    binding runs — ``expected_output`` is opportunistic (attached only when a case happens to carry
    one; see :func:`select_evidence`), and on-demand turns simply never do, so no binding is ever
    dropped for lacking a reference."""
    from ..models.evaluation import EvalRun, EvalRunStatus, EvalRunTrigger
    from .evaluation_turn_extraction import extract_conversation_turns

    with _session(session, project_id) as s:
        suite, bindings, dimensions, code_validations = _load_suite_config(
            s, suite_id, judge_model_override=judge_model)

        version_id = _resolve_version(bindings, application_version_id)
        _assert_version_in_application(s, version_id, suite.application_id)

        turns = extract_conversation_turns(project_id, conversation_id, session=s)
        cases = cases_from_turns(turns)

        snapshot = build_run_snapshot(
            suite=_suite_dict(suite, judge_model),
            dimensions=dimensions, code_validations=code_validations,
            bindings=bindings, cases=cases,
            application_id=suite.application_id, application_version_id=version_id,
            dataset_id=None, trigger_type=TRIGGER_ON_DEMAND,
        )

        run = EvalRun(
            suite_id=suite_id, application_id=suite.application_id,
            application_version_id=version_id, dataset_id=None,
            trigger_type=EvalRunTrigger.on_demand, status=EvalRunStatus.created,
            snapshot=snapshot, progress={'done': 0, 'total': len(cases)},
            owner_id=owner_id,
            meta={'conversation_id': conversation_id},
        )
        s.add(run)
        s.flush()
        s.refresh(run)
        return run


# ----------------------------------------------------------------------------
# read
# ----------------------------------------------------------------------------

def list_runs(
    project_id: int,
    application_id: Optional[int] = None,
    suite_id: Optional[int] = None,
    session=None,
):
    """Runs for a project, newest first, optionally filtered by agent and/or suite (screen #6)."""
    from ..models.evaluation import EvalRun

    with _session(session, project_id) as s:
        query = s.query(EvalRun)
        if application_id is not None:
            query = query.filter(EvalRun.application_id == application_id)
        if suite_id is not None:
            query = query.filter(EvalRun.suite_id == suite_id)
        return query.order_by(EvalRun.id.desc()).all()


def get_run(project_id: int, run_id: int, session=None):
    """A single run (detail + progress feed). Raises :class:`EvalRunNotFoundError` if absent."""
    from ..models.evaluation import EvalRun

    with _session(session, project_id) as s:
        run = s.query(EvalRun).filter(EvalRun.id == run_id).first()
        if not run:
            raise EvalRunNotFoundError(run_id)
        return run


# ----------------------------------------------------------------------------
# launch — background execution (202)
# ----------------------------------------------------------------------------

def launch_run(project_id: int, run_id: int, *, task_node, judge_llm_settings: Optional[dict] = None):
    """Spawn a daemon thread that runs H5's :func:`execute_run` to completion with its own DB
    session. The POST handler returns ``202`` right after this call; the client polls status /
    progress. The thread owns its session (``execute_run`` opens one when ``session=None``), so it
    never shares the request's session across the thread boundary. H5 already fail-closes per item;
    an orchestration-level failure marks the run ``errored`` inside ``execute_run``, and we swallow
    the re-raise here so the daemon thread exits cleanly."""
    def _worker():
        try:
            execute_run(project_id, run_id, task_node=task_node, judge_llm_settings=judge_llm_settings)
        except Exception:  # noqa: BLE001 - run already marked errored in execute_run; thread must not crash the process
            log.exception('Eval run %s (project %s) failed', run_id, project_id)

    thread = threading.Thread(target=_worker, name=f'eval-run-{run_id}', daemon=True)
    thread.start()
    return thread
