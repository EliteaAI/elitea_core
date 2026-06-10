"""
RPC module for Pipeline Scheduling.

Provides the scheduled execution checker for pipelines configured with
schedule triggers. This RPC is called periodically (every minute) by the
scheduling plugin to check if any pipelines need to be executed.
"""
import threading
import time
from datetime import datetime, UTC

from pylon.core.tools import web, log
from sqlalchemy.orm import joinedload
from sqlalchemy.orm.attributes import flag_modified
from tools import db, rpc_tools

try:
    import gevent  # pylint: disable=C0413
except ImportError:  # pragma: no cover - gevent absent in non-gevent deploys
    gevent = None

from ..models.all import ApplicationVersion
from ..models.enums.all import AgentTypes
from ..models.pd.pipeline_trigger import TriggerType, PipelineTriggerSchedule
from ..utils.cron_utils import is_cron_due
from ..utils.pipeline_execution import (
    TriggerType as TriggerTypeConst,
    create_trigger_run_conversation,
    execute_pipeline_via_predict_sio,
)


# Re-entrancy guard: if a previous tick is still running (took >60s) we skip
# this tick instead of letting the work overlap. The scheduler thread fires
# every minute regardless of whether the prior RPC handler has returned.
_check_pipeline_scheduling_lock = threading.Lock()


def _collect_due_pipelines_for_project(project_id: int, yield_to_hub=lambda: None):
    """
    Phase 1 (read-mostly): enumerate schedule-triggered pipeline versions in
    this project, decide which are cron-due right now, and return both the
    candidate count and the due plan.

    Returns:
        (candidates, due) where:
          - candidates: total number of versions examined (incl. invalid /
            not-due / legacy)
          - due: list of plan dicts ready for the launch phase

    Side effect: legacy schedules missing last_run are initialized to current
    time (one DB commit per legacy row) and excluded from `due` so they wait
    for the next cron match — matching the prior single-pass behavior.
    """
    candidates = 0
    due = []
    with db.get_session(project_id) as session:
        # Push the trigger-type filter down to Postgres so we don't haul back
        # every pipeline's JSONB blob just to discard non-schedule triggers in
        # Python. Chained ->/->>: matches the pattern used in
        # utils/llm_migration_utils.py:249.
        pipeline_versions = session.query(ApplicationVersion).filter(
            ApplicationVersion.agent_type == AgentTypes.pipeline.value,
            ApplicationVersion.pipeline_settings.isnot(None),
            ApplicationVersion.pipeline_settings.op('->')('trigger').op('->>')('type')
            == TriggerType.schedule.value,
        ).all()

        for version in pipeline_versions:
            # Cooperative yield per version: parse_obj + local-inline
            # scheduling_time_to_run RPC are pure Python and accumulate
            # CPU between the outer DB I/O yields.
            yield_to_hub()
            candidates += 1
            try:
                pipeline_settings = version.pipeline_settings or {}
                trigger = pipeline_settings.get("trigger") or {}

                if trigger.get("type") != TriggerType.schedule.value:
                    continue

                try:
                    schedule = PipelineTriggerSchedule.parse_obj(trigger)
                except Exception as e:
                    log.warning(
                        f"Invalid pipeline schedule config: "
                        f"project={project_id}, version_id={version.id}: {e}"
                    )
                    continue

                if not schedule.last_run:
                    # Legacy data: initialize last_run and wait for next match.
                    log.warning(
                        f"Pipeline schedule missing last_run (legacy data): "
                        f"project={project_id}, version_id={version.id}. "
                        f"Initializing to current time."
                    )
                    version.pipeline_settings["trigger"]["last_run"] = (
                        datetime.now(UTC).isoformat()
                    )
                    flag_modified(version, "pipeline_settings")
                    session.commit()
                    continue

                # Inline cron evaluation: avoids a cross-plugin RPC round-trip
                # per scheduled pipeline every minute.
                if not is_cron_due(
                    schedule.cron, schedule.last_run, schedule.timezone
                ):
                    log.debug(
                        f"Pipeline not due: project={project_id}, "
                        f"version_id={version.id}, cron={schedule.cron}, "
                        f"last_run={schedule.last_run}"
                    )
                    continue

                due.append({
                    "project_id": project_id,
                    "version_id": version.id,
                    "application_id": version.application_id,
                    "creator_id": schedule.created_by,
                    "cron": schedule.cron,
                    "timezone": schedule.timezone,
                    "last_run": schedule.last_run,
                })
            except Exception as e:
                log.error(
                    f"Error evaluating pipeline schedule: "
                    f"project={project_id}, version_id={version.id}: {e}"
                )

    return candidates, due


def _launch_due_pipeline(item: dict, yield_to_hub=lambda: None):
    """
    Phase 2: execute one planned pipeline launch and update last_run.

    Re-reads the version row in a fresh session to pick up any concurrent
    edits and to keep the write isolated from the collection-phase session.

    Cooperative yields are placed around the two heaviest operations
    (`_execute_pipeline`, which fans out to predict_sio, and the post-run
    DB commit) to keep the gevent hub responsive — same granularity as the
    original single-pass loop.
    """
    project_id = item["project_id"]
    version_id = item["version_id"]

    log.info(
        f"Triggering scheduled pipeline: project={project_id}, "
        f"version_id={version_id}, cron={item['cron']}"
    )

    launch_start = time.monotonic()
    try:
        with db.get_session(project_id) as session:
            # Eager-load `application` so _execute_pipeline can read the name
            # without opening a second session for a one-column lookup.
            version = session.query(ApplicationVersion).options(
                joinedload(ApplicationVersion.application)
            ).get(version_id)
            if not version:
                log.warning(
                    f"Scheduled pipeline disappeared before launch: "
                    f"project={project_id}, version_id={version_id}"
                )
                return

            pipeline_name = (
                version.application.name
                if version.application
                else f"Pipeline {version.application_id}"
            )

            yield_to_hub()
            _execute_pipeline(
                project_id=project_id,
                version=version,
                creator_id=item["creator_id"],
                pipeline_name=pipeline_name,
            )
            yield_to_hub()

            current_time = datetime.now(UTC).isoformat()
            version.pipeline_settings["trigger"]["last_run"] = current_time
            flag_modified(version, "pipeline_settings")
            session.commit()

            launch_elapsed_ms = int((time.monotonic() - launch_start) * 1000)
            log.info(
                f"Successfully triggered scheduled pipeline: "
                f"project={project_id}, version_id={version_id}, "
                f"last_run={current_time}, took={launch_elapsed_ms}ms"
            )
    except Exception as e:
        launch_elapsed_ms = int((time.monotonic() - launch_start) * 1000)
        log.error(
            f"Failed to execute scheduled pipeline: project={project_id}, "
            f"version_id={version_id}, took={launch_elapsed_ms}ms: {e}"
        )


def _execute_pipeline(
    project_id: int,
    version: ApplicationVersion,
    creator_id: int,
    pipeline_name: str,
):
    """
    Execute a scheduled pipeline.

    Creates a conversation with initial message for the run to track it in History,
    then executes the pipeline using the shared execution utility.

    Args:
        project_id: Project ID
        version: ApplicationVersion instance
        creator_id: User ID who created the schedule
        pipeline_name: Display name for the conversation, resolved by the
            caller from the same session that owns `version` so we don't
            open a second session here for a one-column lookup.
    """
    # Create conversation using shared utility
    conversation_uuid, participant_id, response_message_id = create_trigger_run_conversation(
        project_id=project_id,
        version=version,
        user_id=creator_id,
        trigger_type=TriggerTypeConst.SCHEDULED,
        trigger_message="[Scheduled execution triggered]",
        conversation_name=f"Scheduled run: {pipeline_name}",
    )

    # Execute using shared utility
    result = execute_pipeline_via_predict_sio(
        project_id=project_id,
        version=version,
        user_id=creator_id,
        conversation_uuid=conversation_uuid,
        response_message_id=response_message_id,
        user_input="",  # Empty input for scheduled runs
    )

    return result


class RPC:
    """RPC methods for pipeline scheduling."""

    @web.rpc("pipelines_check_scheduling", "check_pipeline_scheduling")
    def check_pipeline_scheduling(self, **kwargs):
        """
        Check all pipelines with schedule triggers and execute those that are due.

        This function is called by the scheduling plugin every minute.
        It iterates through all projects, finds pipelines with schedule triggers,
        checks if they should run based on their cron expression, and executes them.

        Re-entrancy: an in-process lock prevents a slow tick from overlapping
        with the next minute's tick on the same pylon_main instance. For
        multi-replica deployments a Postgres advisory lock would be needed.
        """
        if not _check_pipeline_scheduling_lock.acquire(blocking=False):
            log.warning(
                "check_pipeline_scheduling: previous tick still running, "
                "skipping this minute"
            )
            return None

        try:
            # Cooperative yield only when gevent is the actual web runtime;
            # under flask/waitress/hypercorn this is a no-op.
            yield_to_hub = (
                (lambda: gevent.sleep(0))
                if (gevent is not None and self.context.web_runtime == "gevent")
                else (lambda: None)
            )

            all_project_ids = [
                project_['id'] for project_ in rpc_tools.RpcMixin().rpc.timeout(3).project_list(
                    filter_={'create_success': True}
                )
            ]

            # Phase 1 — collect all cron-due pipeline schedules across projects,
            # timed end-to-end so we can spot scheduler-side slowdowns.
            collection_start = time.monotonic()
            total_candidates = 0
            due_plan: list[dict] = []
            for project_id in all_project_ids:
                # Yield between projects so a long scheduler tick does not starve
                # the gevent hub and stall request greenlets / EventNode.
                yield_to_hub()
                try:
                    candidates, due = _collect_due_pipelines_for_project(
                        project_id, yield_to_hub=yield_to_hub
                    )
                    total_candidates += candidates
                    due_plan.extend(due)
                except Exception as e:
                    log.error(
                        f"Error collecting pipeline schedules for project "
                        f"{project_id}: {e}"
                    )
            collection_elapsed_ms = int((time.monotonic() - collection_start) * 1000)

            # Compact summaries keep the launch plan readable in a single line.
            launch_plan_summary = [
                {
                    "project_id": item["project_id"],
                    "application_id": item["application_id"],
                    "version_id": item["version_id"],
                    "cron": item["cron"],
                    "timezone": item["timezone"],
                    "last_run": item["last_run"],
                }
                for item in due_plan
            ]
            log.info(
                f"check_pipeline_scheduling: collection took "
                f"{collection_elapsed_ms}ms, examined {total_candidates} "
                f"candidate(s) across {len(all_project_ids)} project(s); "
                f"{len(due_plan)} schedule(s) will launch this tick: "
                f"{launch_plan_summary}"
            )

            # Phase 2 — execute each planned launch.
            launch_start = time.monotonic()
            for item in due_plan:
                yield_to_hub()
                _launch_due_pipeline(item, yield_to_hub=yield_to_hub)
            if due_plan:
                launch_elapsed_ms = int((time.monotonic() - launch_start) * 1000)
                log.info(
                    f"check_pipeline_scheduling: launch phase completed in "
                    f"{launch_elapsed_ms}ms for {len(due_plan)} schedule(s)"
                )

            return None
        finally:
            _check_pipeline_scheduling_lock.release()
