"""DB/dispatch wrapper for eval **runs** — EVAL-P1-B4 (§7#6, §14.2, §14.4).

Thin persistence + launch surface over the H5 pure core (:mod:`evaluation_run_orchestration`).
Two entry points create a ``created`` :class:`EvalRun` with a frozen snapshot — one from a stored
dataset (offline-batch, E2E-09), one from a stored conversation (on-demand, E2E-11) — plus list /
get readers and :func:`launch_run`, which submits the run to the local ``eval_runs`` task pool
where :func:`execute_run_task` drives H5's :func:`execute_run` (the API returns ``202``
immediately; the client polls status/progress).

No orchestration logic lives here (that is H5): this module only turns ORM rows into the pure dicts
:func:`build_run_snapshot` consumes, resolves the D3 version pin, and persists the result. Errors
subclass ``EvalLibraryError`` so the v2 boundary returns ``exc.http_status`` uniformly.

ORM models are imported lazily inside functions (H1/H2 precedent); this wrapper is exercised live,
not in the unit suite — its pure inputs are unit-tested in :mod:`evaluation_run_orchestration`.
"""
from typing import List, Optional

from pylon.core.tools import log

from .evaluation_library_utils import EvalLibraryError, _session
from .evaluation_suite_utils import (
    EvalSuiteNotFoundError,
    effective_cases,
    excluded_case_ids,
)
from .evaluation_human_score_utils import EvalRunNotFoundError
from .evaluation_run_orchestration import (
    build_run_snapshot,
    execute_run,
    cases_from_turns,
    resolve_version_id,
    all_bindings_structure_only,
    structure_only_case,
    RUN_TIME_BUDGET_SECONDS,
    TRIGGER_OFFLINE_BATCH,
    TRIGGER_ON_DEMAND,
)


class EvalRunConfigError(EvalLibraryError):
    """Run cannot be assembled: no dataset, no resolvable version pin, ambiguous pin, etc."""
    http_status = 400


class EvalRunNotCancellableError(EvalLibraryError):
    """The run already reached a terminal state, so there is nothing to stop."""
    http_status = 409

    def __init__(self, status: str):
        super().__init__(f'Run is already {status} and cannot be cancelled')
        self.status = status


# ----------------------------------------------------------------------------
# ORM row -> pure dict extractors (feed build_run_snapshot)
# ----------------------------------------------------------------------------

def _binding_dict(b) -> dict:
    """A binding row as the flat dict the snapshot expects. ``application_version_id`` is carried so
    :func:`resolve_version_id` can find the D3 pin."""
    return {
        'engine': b.engine,
        'dimension_id': b.dimension_id,
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
        'polarity': d.polarity, 'code': d.code, 'return_contract': d.return_contract,
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
    """Load a suite + its ordered bindings + the dimensions those bindings reference, all as pure
    dicts. Raises :class:`EvalSuiteNotFoundError` if the suite is gone."""
    from ..models.evaluation import EvalSuite, EvalBinding, EvalDimension

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
    dimensions = (
        [_dimension_dict(d) for d in s.query(EvalDimension).filter(EvalDimension.id.in_(dim_ids)).all()]
        if dim_ids else []
    )
    return suite, bindings, dimensions


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
        suite, bindings, dimensions = _load_suite_config(
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
            # Exclusions (#6350) are authored against the suite's *own* dataset, so an explicit
            # dataset_id override runs that other dataset whole rather than applying a filter
            # written for a different case set.
            excluded = excluded_case_ids(s, suite_id) if ds_id == suite.dataset_id else set()
            selected = effective_cases(dataset.cases, excluded)
            if not selected and dataset.cases:
                raise EvalRunConfigError(
                    'every case of this suite\'s dataset is excluded; nothing to run')
            cases = [_case_dict(c) for c in selected]

        version_id = _resolve_version(bindings, application_version_id)
        _assert_version_in_application(s, version_id, suite.application_id)
        snapshot = build_run_snapshot(
            suite=_suite_dict(suite, judge_model),
            dimensions=dimensions,
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
        suite, bindings, dimensions = _load_suite_config(
            s, suite_id, judge_model_override=judge_model)

        version_id = _resolve_version(bindings, application_version_id)
        _assert_version_in_application(s, version_id, suite.application_id)

        turns = extract_conversation_turns(project_id, conversation_id, session=s)
        cases = cases_from_turns(turns)

        snapshot = build_run_snapshot(
            suite=_suite_dict(suite, judge_model),
            dimensions=dimensions,
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


def delete_run(project_id: int, run_id: int, session=None) -> None:
    """Hard-deletes a run and its results/human-scores (#6348). Does not touch the dataset, suite,
    or dimension definitions the run referenced — those FKs are ``SET NULL``/independent, while
    :class:`EvalResult` and :class:`EvalHumanScore` cascade on ``run_id`` at the DB level.

    Raises :class:`EvalRunNotFoundError` if absent.
    """
    from ..models.evaluation import EvalRun

    with _session(session, project_id) as s:
        run = s.query(EvalRun).filter(EvalRun.id == run_id).first()
        if not run:
            raise EvalRunNotFoundError(run_id)
        s.delete(run)
        s.flush()


def run_in_project(project_id: int, run_id: int) -> bool:
    """Whether ``run_id`` exists in *this* project's schema.

    Run ids are only unique within a project schema, so a caller that receives both ids from the
    client — the SIO progress room join — cannot treat membership of the claimed project as proof
    that the run belongs to it. Existence here is that proof, and it is an id-only read so the
    check costs one indexed hit.
    """
    from ..models.evaluation import EvalRun

    with _session(None, project_id) as s:
        return s.query(EvalRun.id).filter(EvalRun.id == run_id).first() is not None


def request_cancel(project_id: int, run_id: int, session=None):
    """Ask a run to stop (§14.2 cancel). Returns the updated run.

    A ``created`` run is stopped outright: it has no executing thread yet, and ``execute_run``
    claims only from ``created``, so flipping the status here also prevents a queued launch from
    ever starting. A ``running`` run is stopped *cooperatively* — the flag is committed and the
    orchestration loop picks it up at the next case boundary, because the agent call and judge
    dispatches in flight are blocking and carry their own timeouts. The status therefore stays
    ``running`` until the worker itself writes the terminal row, so a poller sees the truth rather
    than a run marked cancelled while it is demonstrably still working.
    """
    from ..models.evaluation import EvalRun, EvalRunStatus
    from datetime import datetime

    with _session(session, project_id) as s:
        run = s.query(EvalRun).filter(EvalRun.id == run_id).first()
        if not run:
            raise EvalRunNotFoundError(run_id)
        if run.status not in (EvalRunStatus.created, EvalRunStatus.running):
            raise EvalRunNotCancellableError(run.status)

        run.meta = {**(run.meta or {}), 'cancel_requested': True,
                    'cancel_requested_at': datetime.utcnow().isoformat()}
        if run.status == EvalRunStatus.created:
            run.status = EvalRunStatus.cancelled
            run.finished_at = datetime.utcnow()
        s.commit()
        s.refresh(run)
        return run


# ----------------------------------------------------------------------------
# launch — background execution on the eval task pool (202)
# ----------------------------------------------------------------------------

#: Task registered on ``module.eval_task_node`` in ``module.init()``.
EVAL_RUN_TASK_NAME = 'elitea_core_eval_run'

EVAL_RUN_POOL = 'eval_runs'


def execute_run_task(module, project_id: int, run_id: int,
                     judge_llm_settings: Optional[dict] = None) -> dict:
    """Task body: run H5's :func:`execute_run` to completion.

    Bound to the module with ``functools.partial`` at registration so the task's own arguments stay
    plain JSON — arbiter ships them through the event node, so a live session or ORM row could not
    be passed even if we wanted to.

    ``execute_run`` opens its own DB session (``session=None``) rather than inheriting the
    request's, and dispatches sandboxed code validations through ``module.task_node``, the
    indexer-pool dispatcher. That node is a stateless client handle on the module instance, not
    something bound to the Flask request, so using it from a pool worker is safe.

    H5 fail-closes per item and marks the run ``errored`` on an orchestration-level failure, so the
    re-raise is swallowed: letting it escape would only have arbiter log a task crash for a run
    whose outcome is already recorded in the row the client is polling.
    """
    try:
        # Read off the descriptor rather than shipped in the task kwargs: it is a deployment
        # capacity setting (judge rate limit + code pool headroom), not a property of the run, so
        # the value in force must be the one the executing pylon is configured for.
        concurrency = module.descriptor.config.get('eval_case_concurrency', 1)
        budget = module.descriptor.config.get(
            'eval_run_time_budget_seconds', RUN_TIME_BUDGET_SECONDS)
        # Progress push. This is the layer that has `module`, so it owns the transport and
        # `execute_run` stays pylon-free. The frame goes over the event node (Redis pub/sub)
        # rather than `module.context.sio` directly: the socket server lives only in pylon_main
        # and the watching browser may be attached to a different replica than this worker.
        def _publish(payload: dict) -> None:
            module.event_node.emit('elitea_core_eval_run_progress', payload)

        execute_run(project_id, run_id, task_node=module.task_node,
                    judge_llm_settings=judge_llm_settings,
                    case_concurrency=concurrency,
                    time_budget_seconds=budget,
                    progress_publisher=_publish)
        return {'ok': True, 'run_id': run_id}
    except Exception as exc:  # noqa: BLE001 - outcome already persisted by execute_run
        log.exception('Eval run %s (project %s) failed', run_id, project_id)
        return {'ok': False, 'run_id': run_id, 'error': str(exc)}


def launch_run(project_id: int, run_id: int, *, eval_task_node,
               judge_llm_settings: Optional[dict] = None) -> Optional[str]:
    """Submit the run to the eval pool. Returns the task id, or ``None`` if it was not accepted.

    Replaces a bare ``threading.Thread``: the pool's ``task_limit`` bounds how many runs execute
    concurrently, and the node is drained on graceful shutdown instead of being killed mid-case.

    ``None`` means **rejected, not queued.** ``start_task`` broadcasts a start query and returns
    ``None`` when no node answers with a free slot, so a saturated pool drops the request rather
    than holding it — the caller must therefore resolve the run itself instead of leaving a row in
    ``created`` that nothing will ever pick up. Maintenance mode is folded into the same signal by
    the gate installed in ``module.init()``.
    """
    from .exceptions import MaintenanceInProgressError

    try:
        task_id = eval_task_node.start_task(
            EVAL_RUN_TASK_NAME,
            kwargs={
                'project_id': project_id,
                'run_id': run_id,
                'judge_llm_settings': judge_llm_settings,
            },
            pool=EVAL_RUN_POOL,
            meta={'task': EVAL_RUN_TASK_NAME, 'project_id': project_id, 'run_id': run_id},
        )
    except MaintenanceInProgressError:
        log.info('Eval run %s (project %s) not started: maintenance mode', run_id, project_id)
        return None
    if task_id is None:
        log.warning('Eval run %s (project %s) not accepted: eval pool has no free slot',
                    run_id, project_id)
    return task_id


def mark_run_unstarted(project_id: int, run_id: int, reason: str, session=None):
    """Resolve a run that was created but never accepted by the pool.

    Without this the row sits in ``created`` forever: the reaper only considers ``running`` rows
    (staleness is measured from the per-case progress heartbeat, which a run that never started
    does not have), so nothing else would ever close it out.
    """
    from ..models.evaluation import EvalRun, EvalRunStatus
    from datetime import datetime

    with _session(session, project_id) as s:
        run = s.query(EvalRun).filter(EvalRun.id == run_id).first()
        if not run:
            raise EvalRunNotFoundError(run_id)
        run.status = EvalRunStatus.errored
        run.error = reason
        run.finished_at = datetime.utcnow()
        s.commit()
        s.refresh(run)
        return run
