"""The generic schedule tick: claim every due schedule once and start its run (EL-7011)."""
import time
from datetime import UTC, datetime

from pylon.core.tools import log, web
from tools import db, rpc_tools, this

from ..utils.maintenance_gate import is_maintenance_active
from ..utils.schedule_claim import claim_schedule, insert_cron_run, select_due_schedules
from ..utils.schedule_dispatch import RUN_STARTERS, start_schedule_run
from ..utils.schedule_timing import compute_next_run
from ..utils.utils import end_ambient_transaction, make_yield_to_hub

DEFAULT_DISPATCH_CAP = 100


def _dispatch_project(project_id: int, entity_types: list[str], room: int) -> int:
    """Claim and start due rows of one project, at most `room`; return how many runs started."""
    started = 0
    with db.get_session(project_id) as session:
        for schedule in select_due_schedules(session, entity_types, room):
            try:
                next_run = compute_next_run(schedule.cron, schedule.timezone, datetime.now(UTC))
                if not claim_schedule(session, schedule.id, schedule.next_run, next_run):
                    session.rollback()
                    continue  # another replica took this due time
                run_id = insert_cron_run(session, schedule.id)
                session.commit()  # the claim stays even when an active run blocks this one
                if run_id is not None and start_schedule_run(schedule.entity_type, project_id, schedule.id, run_id):
                    started += 1
            except Exception as exc:  # noqa: BLE001 -- one bad row must not stop the tick (R1.4)
                session.rollback()
                log.exception(
                    "entity_schedules_check: skipped schedule: project_id=%s schedule_id=%s exc=%r",
                    project_id, schedule.id, exc,
                )
    return started


class RPC:
    """RPC of the schedule tick."""

    @web.rpc("entity_schedules_check")
    def entity_schedules_check(self, **kwargs):  # pylint: disable=W0613
        """Called by the scheduling plugin every minute."""
        if is_maintenance_active():
            log.info("entity_schedules_check: maintenance mode active, skipping tick")
            return
        settings = this.module.descriptor.config.get("scheduler", {}).get("schedules", {})
        cap = settings.get("dispatch_cap", DEFAULT_DISPATCH_CAP)
        # Only types with a starter are claimed; agent rows wait while their flag is off
        entity_types = [
            kind for kind in RUN_STARTERS if kind != "agent" or settings.get("agent_scheduling_enabled", True)
        ]
        tick_started, dispatched = time.monotonic(), 0
        yield_to_hub = make_yield_to_hub(self.context.web_runtime)
        try:
            project_ids = [
                project["id"] for project in rpc_tools.RpcMixin().rpc.timeout(3).project_list(
                    filter_={"create_success": True}
                )
            ]
        except Exception as exc:  # noqa: BLE001 -- a failed project list ends this tick quietly (R1.4)
            log.exception("entity_schedules_check: failed to enumerate projects: exc=%r", exc)
            return
        for project_id in project_ids:
            if not entity_types or dispatched >= cap:
                break
            yield_to_hub()
            end_ambient_transaction()  # a long tick must not hold the scheduler thread's transaction open
            try:
                dispatched += _dispatch_project(project_id, entity_types, cap - dispatched)
            except Exception as exc:  # noqa: BLE001 -- one bad project must not stop the tick (R1.4)
                log.exception("entity_schedules_check: skipped project: project_id=%s exc=%r", project_id, exc)
        log.info("entity_schedules_check: started=%s duration=%.3fs", dispatched, time.monotonic() - tick_started)
