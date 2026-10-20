"""The generic schedule tick: claim every due schedule once and start its run (EL-7011)."""
import time
from datetime import UTC, datetime

from pylon.core.tools import log, web
from sqlalchemy import text
from tools import db, rpc_tools, this

from ..utils.maintenance_gate import is_maintenance_active
from ..utils.schedule_claim import (
    claim_schedule,
    fail_run,
    insert_cron_run,
    legacy_lock_key,
    select_due_schedules,
)
from ..utils.schedule_dispatch import RUN_STARTERS, START_FAILED, start_schedule_run
from ..utils.schedule_timing import compute_next_run
from ..utils.utils import end_ambient_transaction, make_yield_to_hub

DEFAULT_DISPATCH_CAP = 100
LEGACY_LOCK = text("SELECT pg_try_advisory_lock(:key)")
LEGACY_UNLOCK = text("SELECT pg_advisory_unlock(:key)")


def _run_legacy_step(project_ids: list[int], yield_to_hub) -> None:
    """R4.1, R4.2: the legacy pipeline check per project; a project the migration holds waits a minute."""
    # Imported at call time: the tick loads without the legacy module's model graph
    from .pipeline_scheduling import (
        _check_pipeline_scheduling_lock,
        _check_project_pipelines,
    )
    if not _check_pipeline_scheduling_lock.acquire(blocking=False):
        log.warning("entity_schedules_check: previous legacy step still running, skipping it this minute")
        return
    try:
        # A session lock is freed only by the connection that took it; autocommit keeps no transaction open
        with db.engine.connect().execution_options(isolation_level="AUTOCOMMIT") as connection:
            for project_id in project_ids:
                yield_to_hub()
                end_ambient_transaction()
                key = {"key": legacy_lock_key(project_id)}
                if not connection.execute(LEGACY_LOCK, key).scalar():
                    log.info("entity_schedules_check: migration holds the project, legacy step skipped: "
                             "project_id=%s", project_id)
                    continue
                try:
                    _check_project_pipelines(project_id, yield_to_hub=yield_to_hub)
                except Exception as exc:  # noqa: BLE001 -- one bad project must not stop the step
                    log.exception("entity_schedules_check: legacy step failed: project_id=%s exc=%r",
                                  project_id, exc)
                finally:
                    try:
                        connection.execute(LEGACY_UNLOCK, key)
                    except Exception:
                        connection.invalidate()  # closing the server session frees the lock for the migration
                        raise
    finally:
        _check_pipeline_scheduling_lock.release()


def _dispatch_project(project_id: int, entity_types: list[str], room: int) -> int:
    """Claim and start due rows of one project, at most `room`; return how many runs started.

    The cap counts started runs; a row whose claim is lost or whose active run blocks it uses no room.
    """
    started = 0
    with db.get_session(project_id) as session:
        # Plain values, read once: a commit expires ORM rows, and a reloaded next_run that another
        # replica already moved would let this claim match and start the schedule twice (R1.1)
        due = [
            (row.id, row.entity_type, row.cron, row.timezone, row.next_run)
            for row in select_due_schedules(session, entity_types, room)
        ]
        for schedule_id, entity_type, cron, timezone, seen_next_run in due:
            try:
                next_run = compute_next_run(cron, timezone, datetime.now(UTC))
                if not claim_schedule(session, schedule_id, seen_next_run, next_run):
                    session.rollback()
                    continue  # another replica took this due time
                run_id = insert_cron_run(session, schedule_id)
                session.commit()  # the claim stays even when an active run blocks this one
                if run_id is None:
                    continue
                if start_schedule_run(entity_type, project_id, schedule_id, run_id):
                    started += 1
                else:  # a running run that never starts would block the schedule for good (R2.1)
                    fail_run(session, run_id, START_FAILED)
                    session.commit()
            except Exception as exc:  # noqa: BLE001 -- one bad row must not stop the tick (R1.4)
                session.rollback()
                log.exception(
                    "entity_schedules_check: skipped schedule: project_id=%s schedule_id=%s exc=%r",
                    project_id, schedule_id, exc,
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
        try:
            _run_legacy_step(project_ids, yield_to_hub)  # R4.1: last, after every schedule step
        except Exception as exc:  # noqa: BLE001 -- a failed legacy step ends this tick quietly
            log.exception("entity_schedules_check: legacy step stopped: exc=%r", exc)
        log.info("entity_schedules_check: started=%s duration=%.3fs", dispatched, time.monotonic() - tick_started)
