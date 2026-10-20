"""Run starters of entity schedules, started without waiting (EL-7011)."""
import threading
from collections.abc import Callable

from pylon.core.tools import log

START_FAILED = 'the run could not start'


def _start_agent_run(project_id: int, schedule_id: int, run_id: int) -> None:
    # Imported here, so the wrapper is needed: the starter map loads without the database layer
    from .schedule_run_starter import run_owner_schedule
    run_owner_schedule(project_id, schedule_id, run_id)


# entity type -> starter(project_id, schedule_id, run_id); US-12 adds the pipeline
RUN_STARTERS: dict[str, Callable[[int, int, int], None]] = {'agent': _start_agent_run}


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


def _run_starter(starter: Callable[[int, int, int], None], project_id: int, schedule_id: int, run_id: int) -> None:
    try:
        starter(project_id, schedule_id, run_id)
    except Exception:  # noqa: BLE001 -- a failed starter must not stop the tick thread (R3.5)
        log.exception("Schedule %s run %s of project %s: the starter failed", schedule_id, run_id, project_id)
        _close_unstarted_run(project_id, schedule_id, run_id)


def start_schedule_run(entity_type: str, project_id: int, schedule_id: int, run_id: int) -> bool:
    """Start the run in a daemon thread and return at once; False when the type has no starter or no thread starts."""
    starter = RUN_STARTERS.get(entity_type)
    if starter is None:
        return False
    try:
        threading.Thread(target=_run_starter, args=(starter, project_id, schedule_id, run_id), daemon=True).start()
    except RuntimeError:  # no thread left in the process; the tick closes the run
        log.exception("Schedule %s run %s: cannot start the run thread", schedule_id, run_id)
        return False
    return True
