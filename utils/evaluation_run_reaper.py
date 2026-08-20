"""Reap eval runs whose executing thread no longer exists (§14.2 durability).

``launch_run`` submits a run to the local ``eval_runs`` task pool, which executes it on a
worker thread inside the API process. Graceful shutdown drains that pool, but nothing
survives the process dying outright — arbiter keeps task state in memory, so a hard restart,
an OOM kill, or a drain that exceeds ``task_wait_timeout`` still kills the run mid-case and
leaves the ``eval_run`` row in ``running`` forever. The row then reads as "in progress" to the
scorecard, which polls status, so the user sees a run that never finishes and never errors.

**Staleness, not startup state.** The obvious reaper — on boot, mark every ``running`` row
``errored`` — is wrong here, because several gunicorn workers share the database. Worker B
starting up (or being recycled) would kill a run that worker A is actively executing. So a
run is only reaped once it has gone quiet: ``execute_run`` commits progress after every case,
which bumps ``updated_at``, and that commit is the heartbeat. A live run is never silent for
longer than one case.

:data:`RUN_STALE_AFTER_SECONDS` is therefore a *ceiling on one case*, not on a whole run.
Worst case per case today is roughly the agent timeout (120s) plus a few 60s judge/code
dispatches, so 30 minutes leaves a wide margin; a run of any length keeps itself alive by
finishing cases.

Imports of pylon / ORM are local to the functions so the staleness decision itself stays
importable and unit-testable without the platform present.
"""

from datetime import datetime, timedelta
from typing import List, Optional

# A ceiling on the quiet gap between two per-case progress commits, not on total run time.
RUN_STALE_AFTER_SECONDS = 30 * 60

_REAPED_ERROR = (
    'Run was interrupted: the process executing it stopped (restart, deploy, or worker '
    'recycle) and it made no progress for over {minutes} minutes. Start a new run.'
)


def is_stale_run(
    started_at: Optional[datetime],
    updated_at: Optional[datetime],
    now: datetime,
    stale_after_seconds: int = RUN_STALE_AFTER_SECONDS,
) -> bool:
    """Whether a ``running`` row has gone quiet long enough that its thread must be gone.

    Callers pass only rows already filtered to ``running``, so this decides staleness alone.
    The heartbeat is the later of ``updated_at`` (progress commits) and ``started_at``. A row
    with neither is treated as stale: a run cannot be claimed without ``started_at`` being
    set, so such a row predates the claim guard or was written by hand.
    """
    heartbeat = max((ts for ts in (updated_at, started_at) if ts is not None), default=None)
    if heartbeat is None:
        return True
    return now - heartbeat > timedelta(seconds=stale_after_seconds)


def reap_project(
    project_id: int,
    now: Optional[datetime] = None,
    stale_after_seconds: int = RUN_STALE_AFTER_SECONDS,
) -> List[int]:
    """Mark this project's stale ``running`` runs ``errored``. Returns the reaped run ids."""
    from tools import db  # pylint: disable=E0401
    from ..models.evaluation import EvalRun, EvalRunStatus

    now = now or datetime.utcnow()
    reaped = []
    with db.get_session(project_id) as session:
        rows = session.query(EvalRun).filter(EvalRun.status == EvalRunStatus.running).all()
        for run in rows:
            if not is_stale_run(run.started_at, run.updated_at, now, stale_after_seconds):
                continue
            run.status = EvalRunStatus.errored
            run.error = _REAPED_ERROR.format(minutes=stale_after_seconds // 60)
            run.finished_at = now
            reaped.append(run.id)
        if reaped:
            session.commit()
    return reaped


def reap_orphaned_runs(
    now: Optional[datetime] = None,
    stale_after_seconds: int = RUN_STALE_AFTER_SECONDS,
) -> dict:
    """Reap across every active project. One unreadable schema must not stop the rest."""
    from pylon.core.tools import log  # pylint: disable=E0611,E0401
    from .eval_platform_dimension_utils import _active_project_ids

    reaped, failures = {}, []
    for project_id in _active_project_ids():
        try:
            run_ids = reap_project(project_id, now, stale_after_seconds)
        except Exception as exc:  # pylint: disable=broad-except
            log.exception('Failed to reap orphaned eval runs in project %s', project_id)
            failures.append({'project_id': project_id, 'error': str(exc)})
            continue
        if run_ids:
            log.warning('Reaped orphaned eval runs %s in project %s', run_ids, project_id)
            reaped[project_id] = run_ids
    return {
        'reaped': reaped,
        'reaped_runs': sum(len(ids) for ids in reaped.values()),
        'failures': failures,
    }
