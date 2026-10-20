"""Run starters of entity schedules, started without waiting (EL-7011)."""
import threading
from collections.abc import Callable

from pylon.core.tools import log

# entity type -> starter(project_id, schedule_id, run_id); US-11 and US-12 fill it
RUN_STARTERS: dict[str, Callable[[int, int, int], None]] = {}


def _run_starter(starter: Callable[[int, int, int], None], project_id: int, schedule_id: int, run_id: int) -> None:
    try:
        starter(project_id, schedule_id, run_id)
    except Exception:  # noqa: BLE001 -- a failed starter must not stop the tick thread (R3.5)
        log.exception("Schedule %s run %s: the starter failed", schedule_id, run_id)


def start_schedule_run(entity_type: str, project_id: int, schedule_id: int, run_id: int) -> bool:
    """Start the run in a daemon thread and return at once; False when the type has no starter."""
    starter = RUN_STARTERS.get(entity_type)
    if starter is None:
        return False
    threading.Thread(target=_run_starter, args=(starter, project_id, schedule_id, run_id), daemon=True).start()
    return True
