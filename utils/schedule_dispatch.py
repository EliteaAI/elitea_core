"""Run starters of entity schedules, started without waiting (EL-7011)."""
import threading
from collections.abc import Callable

from pylon.core.tools import log

START_FAILED = 'the run could not start'
# At least the default dispatch_cap: a tick that dispatches a full batch must find a slot for each run
DEFAULT_MAX_PARALLEL_RUNS = 100
MAX_PARALLEL_RUNS_LIMIT = 1000
_slots: threading.BoundedSemaphore | None = None
_slots_lock = threading.Lock()


def _slot_pool() -> threading.BoundedSemaphore:
    """The process-wide pool of starter threads, sized once from the config; bounded to 1-1000 as the admin schema does."""
    global _slots  # pylint: disable=W0603
    with _slots_lock:
        if _slots is None:
            # Imported here: the starter map loads without the plugin handle
            from tools import this

            from .schedule_timing import at_least_one
            section = this.module.descriptor.config.get('scheduler', {}).get('schedules', {})
            _slots = threading.BoundedSemaphore(
                at_least_one(section, 'max_parallel_runs', DEFAULT_MAX_PARALLEL_RUNS, MAX_PARALLEL_RUNS_LIMIT)
            )
        return _slots


def _start_owner_run(project_id: int, schedule_id: int, run_id: int, owner_request=None) -> None:
    # Imported here, so the wrapper is needed: the starter map loads without the database layer
    from .schedule_run_starter import run_owner_schedule
    run_owner_schedule(project_id, schedule_id, run_id, *_extra(owner_request))


def _extra(owner_request) -> tuple:
    """The owner's run request as an extra argument, or none: a cron firing (and every run with governance off) is
    started with exactly the arguments it had before #7254."""
    return () if owner_request is None else (owner_request,)


# entity type -> starter(project_id, schedule_id, run_id); one starter runs both types
RUN_STARTERS: dict[str, Callable[[int, int, int], None]] = {
    'agent': _start_owner_run,
    'pipeline': _start_owner_run,
}


def _close_unstarted_run(project_id: int, schedule_id: int, run_id: int) -> None:
    """Fail the run a starter dropped; a running run would block the schedule for good (R2.1)."""
    # Imported here: the starter map loads without the database layer
    from tools import db

    from .schedule_claim import fail_run
    try:
        with db.get_session(project_id) as session:
            fail_run(session, run_id, START_FAILED)
            session.commit()
    except Exception:  # noqa: BLE001 -- the stale-run sweep (US-13) closes the run later
        log.exception("Schedule %s run %s: cannot mark the run failed", schedule_id, run_id)


def _run_starter(starter: Callable[..., None], project_id: int, schedule_id: int, run_id: int,
                 owner_request=None) -> None:
    try:
        starter(project_id, schedule_id, run_id, *_extra(owner_request))
    except Exception:  # noqa: BLE001 -- a failed starter must not stop the tick thread (R3.5)
        log.exception("Schedule %s run %s of project %s: the starter failed", schedule_id, run_id, project_id)
        _close_unstarted_run(project_id, schedule_id, run_id)
    finally:
        _slot_pool().release()


def start_schedule_run(entity_type: str, project_id: int, schedule_id: int, run_id: int,
                       owner_request=None) -> bool:
    """Start the run in a daemon thread and return at once; False when the type has no starter or no thread starts.

    ``owner_request``: the admission request of the owner's Run now or Test run (#7254), built by the run endpoint; it
    stays in this process."""
    starter = RUN_STARTERS.get(entity_type)
    if starter is None:
        return False
    if not _slot_pool().acquire(blocking=False):  # the cap of the tick alone does not bound threads across ticks
        log.warning("Schedule %s run %s: no free run slot, the run is refused; raise max_parallel_runs", schedule_id, run_id)
        return False
    try:
        threading.Thread(target=_run_starter, args=(starter, project_id, schedule_id, run_id, *_extra(owner_request)),
                         daemon=True).start()
    except RuntimeError:  # no thread left in the process; the tick closes the run
        _slot_pool().release()
        log.exception("Schedule %s run %s: cannot start the run thread", schedule_id, run_id)
        return False
    return True
