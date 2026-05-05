"""
RPC module for Pipeline Scheduling.

Provides the scheduled execution checker for pipelines configured with
schedule triggers. This RPC is called periodically (every minute) by the
scheduling plugin to check if any pipelines need to be executed.
"""
from datetime import datetime, UTC

from pylon.core.tools import web, log
from sqlalchemy.orm.attributes import flag_modified
from tools import db, rpc_tools, serialize, VaultClient

from ..models.all import Application, ApplicationVersion
from ..models.conversation import Conversation
from ..models.enums.all import AgentTypes, ParticipantTypes
from ..models.message_group import ConversationMessageGroup
from ..models.message_items.text import TextMessageItem
from ..models.pd.participant import ParticipantEntityApplication, ParticipantEntityUser
from ..models.pd.pipeline_trigger import TriggerType, PipelineTriggerSchedule
from ..utils.participant_utils import get_or_create_one
from ..utils.predict_utils import get_system_user_token
from ..utils.sio_utils import SioEvents


def _check_project_pipelines(project_id: int):
    """
    Check and execute scheduled pipelines for a specific project.

    Args:
        project_id: The project ID to check
    """
    with db.get_session(project_id) as session:
        # Query all pipeline versions that have pipeline_settings with trigger
        # We need to filter for pipeline type agents
        pipeline_versions = session.query(ApplicationVersion).filter(
            ApplicationVersion.agent_type == AgentTypes.pipeline.value,
            ApplicationVersion.pipeline_settings.isnot(None),
        ).all()

        for version in pipeline_versions:
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

    should_run = rpc_tools.RpcMixin().rpc.timeout(3).scheduling_time_to_run(
        schedule.cron,
        schedule.last_run,
        schedule.timezone,
    )

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

    # Get system user token for execution
    user_token = get_system_user_token(project_id)
    if not user_token:
        log.error(f"Cannot get system user token for project {project_id}")
        return

    # Execute the pipeline
    try:
        _execute_pipeline(
            project_id=project_id,
            version=version,
            creator_id=schedule.created_by,
            user_token=user_token,
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


def _create_scheduled_run_conversation(
    project_id: int,
    version: ApplicationVersion,
    creator_id: int,
) -> tuple[str, int, str]:
    """
    Create a conversation for the scheduled pipeline run with initial message.

    This ensures the run appears in the History tab, matching the pattern
    used by UI-initiated pipeline runs.

    Args:
        project_id: Project ID
        version: ApplicationVersion instance
        creator_id: User ID who created the schedule

    Returns:
        Tuple of (conversation UUID string, pipeline participant ID, response message UUID)
    """
    with db.get_session(project_id) as session:
        # Get application name for the conversation
        application = session.query(Application).get(version.application_id)
        pipeline_name = application.name if application else f"Pipeline {version.application_id}"

        # Build single_participant metadata (same as UI creates)
        single_participant = {
            "entity_name": ParticipantTypes.application.value,
            "entity_meta": {
                "id": version.application_id,
                "project_id": project_id,
            },
            "entity_settings": {
                "version_id": version.id,
            },
        }

        # Create conversation with proper metadata for History tab
        conversation = Conversation(
            name=f"Scheduled run: {pipeline_name}",
            source="pipeline",
            author_id=creator_id,
            is_private=True,
            meta={
                "single_participant": single_participant,
                "scheduled_run": True,
            },
        )
        session.add(conversation)
        session.flush()

        # Get or create user participant
        user_participant, _ = get_or_create_one(
            session=session,
            entity_name=ParticipantTypes.user,
            entity_meta=ParticipantEntityUser(id=creator_id),
        )
        if user_participant not in conversation.participants:
            conversation.participants.append(user_participant)

        # Get or create pipeline participant
        pipeline_participant, _ = get_or_create_one(
            session=session,
            entity_name=ParticipantTypes.application,
            entity_meta=ParticipantEntityApplication(
                id=version.application_id,
                project_id=project_id,
            ),
        )
        if pipeline_participant not in conversation.participants:
            conversation.participants.append(pipeline_participant)

        session.flush()

        # Update entity_settings for pipeline participant mapping (required for conversation details)
        from ..models.participants import ParticipantMapping
        session.query(ParticipantMapping).filter(
            ParticipantMapping.conversation_id == conversation.id,
            ParticipantMapping.participant_id == pipeline_participant.id
        ).update({'entity_settings': {'version_id': version.id}})

        # Create initial user message (trigger notification)
        user_msg_group = ConversationMessageGroup(
            conversation=conversation,
            author_participant=user_participant,
            sent_to_id=pipeline_participant.id,
            meta={"scheduled_trigger": True},
        )
        user_msg = TextMessageItem(
            message_group=user_msg_group,
            item_type=TextMessageItem.__mapper_args__['polymorphic_identity'],
            content="[Scheduled execution triggered]",
            order_index=0,
        )
        session.add(user_msg_group)
        session.add(user_msg)
        session.flush()

        # Create response message placeholder (will be filled by pipeline execution)
        response_msg_group = ConversationMessageGroup(
            conversation=conversation,
            author_participant_id=pipeline_participant.id,
            is_streaming=True,
            reply_to_id=user_msg_group.id,
        )
        session.add(response_msg_group)
        session.flush()

        session.commit()

        log.debug(
            f"Created conversation for scheduled pipeline run: "
            f"conversation_id={conversation.id}, uuid={conversation.uuid}, "
            f"user_msg_id={user_msg_group.uuid}, response_id={response_msg_group.uuid}"
        )
        return str(conversation.uuid), pipeline_participant.id, str(response_msg_group.uuid)


def _execute_pipeline(
    project_id: int,
    version: ApplicationVersion,
    creator_id: int,
    user_token: str,
):
    """
    Execute a pipeline via task node.

    Creates a conversation with initial message for the run to track it in History,
    then executes the pipeline.

    Args:
        project_id: Project ID
        version: ApplicationVersion instance
        creator_id: User ID who created the schedule
        user_token: System user token for API access
    """
    # Create conversation with initial message (appears in History tab)
    # Returns the response message UUID that will be updated with execution metadata
    conversation_uuid, participant_id, response_message_id = _create_scheduled_run_conversation(
        project_id=project_id,
        version=version,
        creator_id=creator_id,
    )

    stream_id = conversation_uuid  # Use conversation UUID as stream_id

    # Use the existing predict_sio RPC to execute the pipeline
    # This reuses all the existing pipeline execution logic
    # sio_event=chat_predict ensures execution metadata (tool_calls, thinking_steps)
    # is saved to ConversationMessageGroup.meta via chat_message_stream_end event
    # chat_project_id is required to route DB operations to correct project schema
    result = rpc_tools.RpcMixin().rpc.timeout(5).applications_predict_sio(
        sid=None,  # No socket connection for scheduled runs
        sio_event=SioEvents.chat_predict.value,  # Required for metadata persistence
        chat_project_id=project_id,  # Required for correct DB schema routing
        data={
            "project_id": project_id,
            "application_id": version.application_id,
            "version_id": version.id,
            "user_input": "",  # Empty input for scheduled runs
            "chat_history": [],
            "message_id": response_message_id,  # Use response message UUID for metadata storage
            "stream_id": stream_id,
            "conversation_id": conversation_uuid,
        },
        await_task_timeout=-1,  # Don't wait for completion
        user_id=creator_id,
        is_system_user=True,
    )

    log.debug(f"Pipeline execution started: task_id={result.get('task_id')}, conversation={conversation_uuid}")
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
        """
        # Get all active projects
        all_project_ids = [
            project_['id'] for project_ in rpc_tools.RpcMixin().rpc.timeout(3).project_list(
                filter_={'create_success': True}
            )
        ]

        for project_id in all_project_ids:
            try:
                _check_project_pipelines(project_id)
            except Exception as e:
                log.error(f"Error checking pipeline schedules for project {project_id}: {e}")

        return None
