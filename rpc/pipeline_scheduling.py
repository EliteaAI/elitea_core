"""
RPC module for Pipeline Scheduling.

Provides the scheduled execution checker for pipelines configured with
schedule triggers. This RPC is called periodically (every minute) by the
scheduling plugin to check if any pipelines need to be executed.
"""
import threading
from datetime import datetime, UTC

from pylon.core.tools import web, log
from sqlalchemy.orm.attributes import flag_modified
from tools import db, rpc_tools

try:
    import gevent  # pylint: disable=C0413
except ImportError:  # pragma: no cover - gevent absent in non-gevent deploys
    gevent = None

from ..models.all import Application, ApplicationVersion
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


def _check_project_pipelines(project_id: int, yield_to_hub=lambda: None):
    """
    Check and execute scheduled pipelines for a specific project.

    Args:
        project_id: The project ID to check
        yield_to_hub: Callable invoked once per version to cooperatively
            hand control back to the gevent hub. No-op when gevent is
            not the active runtime.
    """
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
            try:
                _process_pipeline_version(project_id, version, session)
            except Exception as e:
                log.error(
                    f"Error processing pipeline schedule: project={project_id}, "
                    f"version_id={version.id}: {e}"
                )


def _process_pipeline_version(project_id: int, version: ApplicationVersion, session):
    """
    Process a single pipeline version and execute if scheduled to run.

    Args:
        project_id: Project ID
        version: ApplicationVersion instance
        session: Database session
    """
    pipeline_settings = version.pipeline_settings or {}
    trigger = pipeline_settings.get("trigger")

    # Skip if no trigger configured or not a schedule trigger
    if not trigger or trigger.get("type") != TriggerType.schedule.value:
        return

    # Validate the schedule configuration
    try:
        schedule = PipelineTriggerSchedule.parse_obj(trigger)
    except Exception as e:
        log.warning(
            f"Invalid pipeline schedule config: project={project_id}, "
            f"version_id={version.id}: {e}"
        )
        return

    # Check if it's time to run using the scheduling plugin's time_to_run RPC
    # Note: last_run is set to current time when schedule is created,
    # so new schedules wait for the next cron match (same as index scheduling)
    # For backward compatibility, handle legacy schedules where last_run might be None
    if not schedule.last_run:
        log.warning(
            f"Pipeline schedule missing last_run (legacy data): project={project_id}, "
            f"version_id={version.id}. Initializing to current time."
        )
        current_time = datetime.now(UTC).isoformat()
        version.pipeline_settings["trigger"]["last_run"] = current_time
        flag_modified(version, "pipeline_settings")
        session.commit()
        return  # Wait for next cron match

    # Inline cron evaluation: avoids a cross-plugin RPC round-trip per
    # scheduled pipeline every minute. Same algorithm as the
    # scheduling_time_to_run RPC (kept available for other callers).
    should_run = is_cron_due(schedule.cron, schedule.last_run, schedule.timezone)

    if not should_run:
        log.debug(
            f"Pipeline not due: project={project_id}, version_id={version.id}, "
            f"cron={schedule.cron}, last_run={schedule.last_run}"
        )
        return

    log.info(
        f"Triggering scheduled pipeline: project={project_id}, "
        f"version_id={version.id}, cron={schedule.cron}"
    )

    # Execute the pipeline
    try:
        _execute_pipeline(
            project_id=project_id,
            version=version,
            creator_id=schedule.created_by,
        )

        # Update last_run timestamp
        current_time = datetime.now(UTC).isoformat()
        version.pipeline_settings["trigger"]["last_run"] = current_time
        flag_modified(version, "pipeline_settings")
        session.commit()

        log.info(
            f"Successfully triggered scheduled pipeline: project={project_id}, "
            f"version_id={version.id}, last_run={current_time}"
        )

    except Exception as e:
        log.error(
            f"Failed to execute scheduled pipeline: project={project_id}, "
            f"version_id={version.id}: {e}"
        )
        session.rollback()


def _execute_pipeline(
    project_id: int,
    version: ApplicationVersion,
    creator_id: int,
):
    """
    Execute a scheduled pipeline.

    Creates a conversation with initial message for the run to track it in History,
    then executes the pipeline using the shared execution utility.

    Args:
        project_id: Project ID
        version: ApplicationVersion instance
        creator_id: User ID who created the schedule
    """
    # Get application name for conversation
    with db.get_session(project_id) as session:
        application = session.query(Application).get(version.application_id)
        pipeline_name = application.name if application else f"Pipeline {version.application_id}"

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

            for project_id in all_project_ids:
                # Yield between projects so a long scheduler tick does not starve
                # the gevent hub and stall request greenlets / EventNode.
                yield_to_hub()
                try:
                    _check_project_pipelines(project_id, yield_to_hub=yield_to_hub)
                except Exception as e:
                    log.error(f"Error checking pipeline schedules for project {project_id}: {e}")

            return None
        finally:
            _check_pipeline_scheduling_lock.release()
