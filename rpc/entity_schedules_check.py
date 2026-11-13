"""The generic schedule tick: claim every due schedule once and start its run (EL-7011)."""
import threading
import time
import types
from datetime import datetime
from zoneinfo import ZoneInfoNotFoundError

from pylon.core.tools import log, web
from sqlalchemy import func, select, text, update
from tools import db, rpc_tools, this

from ..utils.constants import LEGACY_INDEX_STEP_DEFAULT
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
DEFAULT_STALE_RUN_MINUTES = 120
SKIPPED_REASON = "the previous run is still running"
INVALID_CRON_REASON = "invalid_cron"
INVALID_CRON_NOTICE = "its cron or time zone is not valid"
LEGACY_RECHECK_SECONDS = 3600
MIGRATION_RETRY_SECONDS = 3600
# No project has id 0, so its legacy lock key is free to guard the whole index step
INDEX_STEP_LOCK_ID = 0
INDEX_STEP_PROJECTS_PER_PASS = 20
# One tick at a time in this process; other replicas stay apart through the claim compare-and-set
_tick_lock = threading.Lock()
# lean: in-process flag, so each replica re-checks once an hour; share it through the database if that cost matters
_legacy_idle_until = 0.0
# lean: in-process, like the pipeline flag; the index step re-checks once an hour on each replica
_legacy_index_idle = {"until": 0.0}
# lean: in-process; a project that held or failed a version is not migrated again for an hour
_migration_retry_at: dict[int, float] = {}
# lean: in-process; a project whose index schedules were held or failed is not migrated again for an hour.
# Each replica keeps its own map, so a replica that takes the step lock later tries such a project once more
_index_migration_retry_at: dict[int, float] = {}
# lean: in-process counter; each replica rotates on its own, which is enough to stop one project starving
_tick_counter = 0
LEGACY_LOCK = text("SELECT pg_try_advisory_lock(:key)")
LEGACY_UNLOCK = text("SELECT pg_advisory_unlock(:key)")


def _migrate_legacy_triggers(project_id: int, migrate=None, retry_at: dict | None = None) -> bool:
    """Move the legacy schedule triggers of one project into schedule rows; True when it found none and none failed.

    A version that fails to move, is held back by a schedule limit or waits for a retry keeps its trigger,
    so the legacy check below still fires it. A project with such a version is retried once an hour.
    `migrate` and `retry_at` default to the pipeline migration and its retry map; the index step passes its own.
    """
    if migrate is None:
        # Imported at call time, as in _dispatch_project
        from ..utils.legacy_schedule_migration import migrate_project as migrate
    if retry_at is None:
        retry_at = _migration_retry_at
    if time.monotonic() < retry_at.get(project_id, 0.0):
        return False
    try:
        report = migrate(project_id, dry_run=False, enforce_limits=True)
    except Exception as exc:  # noqa: BLE001 -- one bad project must not stop the step
        log.warning("entity_schedules_check: legacy migration failed: project_id=%s exc=%r", project_id, exc)
        retry_at[project_id] = time.monotonic() + MIGRATION_RETRY_SECONDS
        return False
    if report["errors"]:
        log.warning("entity_schedules_check: versions kept their legacy trigger: project_id=%s errors=%s",
                    project_id, report["errors"])
    if report.get("held"):
        log.warning("entity_schedules_check: versions kept their legacy trigger over a schedule limit: "
                    "project_id=%s versions=%s", project_id, report["held"])
    if report["errors"] or report.get("held"):
        retry_at[project_id] = time.monotonic() + MIGRATION_RETRY_SECONDS
    return report["found"] == 0


def _run_legacy_step(project_ids: list[int], yield_to_hub, settings: dict) -> None:
    """The legacy pipeline check per project; a project the migration holds waits a minute.

    A version the one-time migration missed becomes a schedule row first, so it shows in the UI. After a
    pass that finds nothing the step sleeps for `LEGACY_RECHECK_SECONDS`; `legacy_step_enabled` false stops it.
    """
    global _legacy_idle_until  # pylint: disable=W0603
    if not settings.get("legacy_step_enabled", True) or time.monotonic() < _legacy_idle_until:
        return
    # Imported at call time: the tick loads without the legacy module's model graph
    from .pipeline_scheduling import (
        _check_pipeline_scheduling_lock,
        _check_project_pipelines,
    )
    if not _check_pipeline_scheduling_lock.acquire(blocking=False):
        log.warning("entity_schedules_check: previous legacy step still running, skipping it this minute")
        return
    try:
        empty_pass = True
        # A session lock is freed only by the connection that took it; autocommit keeps no transaction open
        with db.engine.connect().execution_options(isolation_level="AUTOCOMMIT") as connection:
            for project_id in project_ids:
                yield_to_hub()
                end_ambient_transaction()
                # Before the project lock: the migration takes the same lock on its own connection
                empty_pass &= _migrate_legacy_triggers(project_id)
                key = {"key": legacy_lock_key(project_id)}
                if not connection.execute(LEGACY_LOCK, key).scalar():
                    empty_pass = False
                    log.info("entity_schedules_check: migration holds the project, legacy step skipped: "
                             "project_id=%s", project_id)
                    continue
                try:
                    _check_project_pipelines(project_id, yield_to_hub=yield_to_hub)
                except Exception as exc:  # noqa: BLE001 -- one bad project must not stop the step
                    empty_pass = False
                    log.exception("entity_schedules_check: legacy step failed: project_id=%s exc=%r",
                                  project_id, exc)
                finally:
                    try:
                        connection.execute(LEGACY_UNLOCK, key)
                    except Exception:
                        connection.invalidate()  # closing the server session frees the lock for the migration
                        raise
        if empty_pass:
            _legacy_idle_until = time.monotonic() + LEGACY_RECHECK_SECONDS
    finally:
        _check_pipeline_scheduling_lock.release()


def _migrate_index_projects(project_ids: list[int], yield_to_hub) -> bool:
    """One pass of the index migration; True when it found nothing left to move.

    A project in its retry hour is passed over and does not keep the step busy. At most
    `INDEX_STEP_PROJECTS_PER_PASS` projects that held schedules are migrated; the rest wait for the next tick.
    A project that found none is not counted, so a long clean list cannot starve the ones behind it.
    """
    # Imported at call time: the tick loads without the migration's model graph
    from ..utils.legacy_index_schedule_migration import migrate_index_project
    empty_pass, migrated = True, 0
    for project_id in project_ids:
        if time.monotonic() < _index_migration_retry_at.get(project_id, 0.0):
            continue
        if migrated >= INDEX_STEP_PROJECTS_PER_PASS:
            return False
        yield_to_hub()
        end_ambient_transaction()
        if not _migrate_legacy_triggers(project_id, migrate_index_project, _index_migration_retry_at):
            empty_pass, migrated = False, migrated + 1
    return empty_pass


def _run_legacy_index_step(project_ids: list[int], yield_to_hub, settings: dict) -> None:
    """Move the legacy index schedules of each project into schedule rows, once in a while.

    One replica at a time runs the step, under a session advisory lock; the others skip the pass. A schedule the
    migration cannot move (a failed lookup, a limit) is retried after `MIGRATION_RETRY_SECONDS`. After a pass that
    finds nothing the step sleeps for `LEGACY_RECHECK_SECONDS`; `legacy_index_step_enabled` false stops it.
    """
    step_on = settings.get("legacy_index_step_enabled", LEGACY_INDEX_STEP_DEFAULT)
    if not step_on or time.monotonic() < _legacy_index_idle["until"]:
        return
    key = {"key": legacy_lock_key(INDEX_STEP_LOCK_ID)}
    # A session lock is freed only by the connection that took it; autocommit keeps no transaction open
    with db.engine.connect().execution_options(isolation_level="AUTOCOMMIT") as connection:
        if not connection.execute(LEGACY_LOCK, key).scalar():
            log.info("entity_schedules_check: another replica runs the legacy index step, skipping it this minute")
            return
        try:
            if _migrate_index_projects(project_ids, yield_to_hub):
                _legacy_index_idle["until"] = time.monotonic() + LEGACY_RECHECK_SECONDS
        finally:
            try:
                connection.execute(LEGACY_UNLOCK, key)
            except Exception:
                connection.invalidate()  # closing the server session frees the lock for the next replica
                raise


def _database_now(project_id: int) -> datetime:
    """The database clock, the one source the claims and the sweeps share, so replicas agree."""
    with db.get_session(project_id) as session:
        return session.execute(select(func.now())).scalar()


def _sweep_stale_runs(project_ids: list[int], stale_minutes: int, yield_to_hub) -> None:
    """Fail the running runs that went quiet, per project, before the claims.

    The waiting runs whose approval time ran out fail in the same loop, so a freed slot starts its cron run now.
    """
    # Imported at call time, as in _dispatch_project
    from ..utils.schedule_runs import expire_waiting_runs, fail_stale_runs
    for project_id in project_ids:
        yield_to_hub()
        end_ambient_transaction()
        try:
            now = _database_now(project_id)
        except Exception as exc:  # noqa: BLE001 -- one bad project must not stop the sweep
            log.exception("entity_schedules_check: database clock not read: project_id=%s exc=%r", project_id, exc)
            continue
        try:
            expire_waiting_runs(project_id, now)
        except Exception as exc:  # noqa: BLE001 -- a failed expiry must not skip the stale sweep
            log.exception("entity_schedules_check: expiry sweep failed: project_id=%s exc=%r", project_id, exc)
        try:
            fail_stale_runs(project_id, now, stale_minutes)
        except Exception as exc:  # noqa: BLE001 -- one bad project must not stop the sweep
            log.exception("entity_schedules_check: stale sweep failed: project_id=%s exc=%r", project_id, exc)
        try:
            _sweep_previous_secrets(project_id, now)
        except Exception as exc:  # noqa: BLE001 -- an expired previous secret is cleared on a later tick
            log.exception("entity_schedules_check: secret sweep failed: project_id=%s exc=%r", project_id, exc)


def _sweep_previous_secrets(project_id: int, now: datetime) -> None:
    """Remove the previous webhook secrets whose 24 h overlap ended."""
    from ..utils.webhook_secrets import sweep_expired_previous  # late, as the other sweeps
    with db.get_session(project_id) as session:
        sweep_expired_previous(session, project_id, now)


def _stale_minutes(settings: dict) -> int:
    """The configured stale limit; config.yml skips the admin schema, so 0, null or negative is bounded here."""
    return max(1, int(settings.get("stale_run_minutes") or DEFAULT_STALE_RUN_MINUTES))


def _disable_schedule(session, project_id: int, schedule_id: int, owner_id: int, name: str, reason: str) -> None:
    """Switch one due row off with `reason` and tell its owner; no claim and no run follow."""
    # Imported when a row is disabled: only this path needs the model graph
    from ..models.entity_schedule import EntitySchedule
    from ..models.enums.all import NotificationEventTypes
    from ..utils.schedule_notifications import notify_schedule_event
    disabled = session.execute(
        update(EntitySchedule)
        .where(EntitySchedule.id == schedule_id, EntitySchedule.enabled.is_(True))
        .values(enabled=False, disabled_reason=reason, next_run=None)
    ).rowcount == 1  # zero rows means another replica disabled it and sent the notice
    session.commit()
    if not disabled:
        return
    schedule = types.SimpleNamespace(id=schedule_id, name=name)
    if reason == "expired":
        notify_schedule_event(project_id, owner_id, NotificationEventTypes.schedule_expired, schedule)
    else:
        notify_schedule_event(project_id, owner_id, NotificationEventTypes.schedule_disabled, schedule,
                              extra={"reason": INVALID_CRON_NOTICE})


def _dispatch_project(project_id: int, entity_types: list[str], room: int) -> int:
    """Claim and start due rows of one project, at most `room`; return how many runs started.

    The cap counts started runs; a row whose claim is lost or whose active run blocks it uses no room.
    """
    # Imported at call time: the tick loads without the run writers' model graph
    from ..utils.schedule_runs import record_skipped_run
    started = 0
    with db.get_session(project_id) as session:
        # One database clock per pass: the due query, the claim and the sweeps all use it, so replicas agree
        now = session.execute(select(func.now())).scalar()
        # Plain values, read once: a commit expires ORM rows, and a reloaded next_run that another
        # replica already moved would let this claim match and start the schedule twice
        due = [
            (row.id, row.entity_type, row.cron, row.timezone, row.next_run, row.expires_at, row.name, row.created_by)
            for row in select_due_schedules(session, entity_types, room)
        ]
        for schedule_id, entity_type, cron, timezone, seen_next_run, expires_at, name, owner_id in due:
            try:
                if expires_at is not None and expires_at <= now:  # past its stop date
                    _disable_schedule(session, project_id, schedule_id, owner_id, name, "expired")
                    continue
                try:
                    next_run = compute_next_run(cron, timezone, now)
                except (ValueError, KeyError, ZoneInfoNotFoundError):  # a bad row is switched off, never retried
                    _disable_schedule(session, project_id, schedule_id, owner_id, name, INVALID_CRON_REASON)
                    continue
                if not claim_schedule(session, schedule_id, seen_next_run, next_run):
                    session.rollback()
                    continue  # another replica took this due time
                run_id = insert_cron_run(session, schedule_id)
                session.commit()  # the claim stays even when an active run blocks this one
                if run_id is None:  # the overlap is recorded, not dropped
                    try:
                        record_skipped_run(session, schedule_id, SKIPPED_REASON)
                        session.commit()
                    except Exception as exc:  # noqa: BLE001 -- the claim stays; the other due rows go on
                        session.rollback()
                        log.exception("entity_schedules_check: skipped run not recorded: project_id=%s "
                                      "schedule_id=%s exc=%r", project_id, schedule_id, exc)
                    continue
                if start_schedule_run(entity_type, project_id, schedule_id, run_id):
                    started += 1
                else:  # a running run that never starts would block the schedule for good
                    fail_run(session, run_id, START_FAILED)
                    session.commit()
            except Exception as exc:  # noqa: BLE001 -- one bad row must not stop the tick
                session.rollback()
                log.exception(
                    "entity_schedules_check: skipped schedule: project_id=%s schedule_id=%s exc=%r",
                    project_id, schedule_id, exc,
                )
    return started


def _run_tick(self) -> None:
    """One tick of the schedule steps; `self` is the plugin Module, which pylon binds to each RPC method."""
    # Stopping work is never what maintenance mode holds back, so the sweep runs before that check
    # Imported at call time: the tick loads without the governance model graph
    from ..utils.governance import governance_lifecycle_active
    if governance_lifecycle_active(self):
        try:
            from ..utils.admission import enforce_run_deadlines
            enforce_run_deadlines(self)
        except Exception as exc:  # noqa: BLE001 -- a failed sweep must not stop the tick
            log.exception("entity_schedules_check: run deadline sweep failed: exc=%r", exc)
    if is_maintenance_active():
        log.info("entity_schedules_check: maintenance mode active, skipping tick")
        return
    settings = this.module.descriptor.config.get("scheduler", {}).get("schedules", {})
    cap = settings.get("dispatch_cap", DEFAULT_DISPATCH_CAP)
    # Only types with a starter are claimed; rows of a type wait while its flag is off
    flags = {"agent": "agent_scheduling_enabled", "toolkit_index": "toolkit_index_scheduling_enabled"}
    entity_types = [kind for kind in RUN_STARTERS if kind not in flags or settings.get(flags[kind], True)]
    tick_started, dispatched = time.monotonic(), 0
    yield_to_hub = make_yield_to_hub(self.context.web_runtime)
    try:
        project_ids = [
            project["id"] for project in rpc_tools.RpcMixin().rpc.timeout(3).project_list(
                filter_={"create_success": True}
            )
        ]
    except Exception as exc:  # noqa: BLE001 -- a failed project list ends this tick quietly
        log.exception("entity_schedules_check: failed to enumerate projects: exc=%r", exc)
        return
    # Start at another project each tick, so the dispatch cap never starves the same ones
    global _tick_counter  # pylint: disable=W0603
    if project_ids:
        offset = _tick_counter % len(project_ids)
        project_ids = project_ids[offset:] + project_ids[:offset]
    _tick_counter += 1
    # Before the claims, so a schedule blocked by a dead run starts again this minute
    _sweep_stale_runs(project_ids, _stale_minutes(settings), yield_to_hub)
    for project_id in project_ids:
        if not entity_types or dispatched >= cap:
            break
        yield_to_hub()
        end_ambient_transaction()  # a long tick must not hold the scheduler thread's transaction open
        try:
            dispatched += _dispatch_project(project_id, entity_types, cap - dispatched)
        except Exception as exc:  # noqa: BLE001 -- one bad project must not stop the tick
            log.exception("entity_schedules_check: skipped project: project_id=%s exc=%r", project_id, exc)
    try:
        _run_legacy_step(project_ids, yield_to_hub, settings)  # last, after every schedule step
    except Exception as exc:  # noqa: BLE001 -- a failed legacy step ends this tick quietly
        log.exception("entity_schedules_check: legacy step stopped: exc=%r", exc)
    try:
        _run_legacy_index_step(project_ids, yield_to_hub, settings)
    except Exception as exc:  # noqa: BLE001 -- a failed index step must not end the tick with an error
        log.exception("entity_schedules_check: legacy index step stopped: exc=%r", exc)
    log.info("entity_schedules_check: started=%s duration=%.3fs", dispatched, time.monotonic() - tick_started)


def _guarded_tick(module) -> None:
    """One tick unless the previous one still runs; `module` is the plugin Module pylon binds as `self`."""
    if not _tick_lock.acquire(blocking=False):
        log.warning("entity_schedules_check: previous tick still running, skipping this one")
        return
    try:
        _run_tick(module)
    finally:
        _tick_lock.release()


class RPC:
    """RPC of the schedule tick."""

    @web.rpc("entity_schedules_check")
    def entity_schedules_check(self, **kwargs):  # pylint: disable=W0613
        """Called by the scheduling plugin every minute; a tick that finds the previous one running ends at once.

        Governed platforms also stop the runs this process tracks that must not go on (``enforce_run_deadlines``).
        Known gap: the map of tracked runs is in memory, so after a Main restart a running mandate run is no longer
        stopped by Main (#6947).
        """
        _guarded_tick(self)

    # The old name stays: scheduler rows stored before the generic tick still call it. A second method,
    # so each name has its own registration whatever web.rpc stores on a function. Pylon binds `self` to the
    # plugin Module, not to this class, so the method calls the module function and never a sibling method
    @web.rpc("pipelines_check_scheduling", "check_pipeline_scheduling")
    def pipelines_check_scheduling(self, **kwargs):  # pylint: disable=W0613
        """The tick under its old name."""
        _guarded_tick(self)
