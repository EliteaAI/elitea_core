"""
RPC module for Pipeline Webhook execution.

Provides execution of pipelines configured with webhook triggers.
This creates conversations with History tab tracking, similar to scheduled execution.
"""
import json
from uuid import uuid4

from pylon.core.tools import web, log
from tools import db, rpc_tools, serialize

from ..models.all import Application, ApplicationVersion
from ..models.conversation import Conversation
from ..models.enums.all import AgentTypes, ParticipantTypes
from ..models.message_group import ConversationMessageGroup
from ..models.message_items.text import TextMessageItem
from ..models.pd.participant import ParticipantEntityApplication, ParticipantEntityUser
from ..models.pd.pipeline_trigger import TriggerType
from ..utils.participant_utils import get_or_create_one
from ..utils.sio_utils import SioEvents


def _create_webhook_run_conversation(
    project_id: int,
    version: ApplicationVersion,
    user_id: int,
    webhook_type: str,
    payload_preview: str,
) -> tuple[str, int, str]:
    """
    Create a conversation for the webhook-triggered pipeline run.

    This ensures the run appears in the History tab, matching the pattern
    used by scheduled and UI-initiated pipeline runs.

    Args:
        project_id: Project ID
        version: ApplicationVersion instance
        user_id: User ID (system user for webhook execution)
        webhook_type: Type of webhook (github, gitlab, custom)
        payload_preview: Preview of webhook payload for initial message

    Returns:
        Tuple of (conversation UUID string, pipeline participant ID, response message UUID)
    """
    with db.get_session(project_id) as session:
        application = session.query(Application).get(version.application_id)
        pipeline_name = application.name if application else f"Pipeline {version.application_id}"

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

        conversation = Conversation(
            name=f"Webhook run: {pipeline_name}",
            source="pipeline",
            author_id=user_id,
            is_private=True,
            meta={
                "single_participant": single_participant,
                "webhook_trigger": True,
                "webhook_type": webhook_type,
            },
        )
        session.add(conversation)
        session.flush()

        user_participant, _ = get_or_create_one(
            session=session,
            entity_name=ParticipantTypes.user,
            entity_meta=ParticipantEntityUser(id=user_id),
        )
        if user_participant not in conversation.participants:
            conversation.participants.append(user_participant)

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

        from ..models.participants import ParticipantMapping
        session.query(ParticipantMapping).filter(
            ParticipantMapping.conversation_id == conversation.id,
            ParticipantMapping.participant_id == pipeline_participant.id
        ).update({'entity_settings': {'version_id': version.id}})

        trigger_message = f"[Webhook triggered: {webhook_type}]\n\nPayload:\n```json\n{payload_preview}\n```"
        user_msg_group = ConversationMessageGroup(
            conversation=conversation,
            author_participant=user_participant,
            sent_to_id=pipeline_participant.id,
            meta={"webhook_trigger": True, "webhook_type": webhook_type},
        )
        user_msg = TextMessageItem(
            message_group=user_msg_group,
            item_type=TextMessageItem.__mapper_args__['polymorphic_identity'],
            content=trigger_message,
            order_index=0,
        )
        session.add(user_msg_group)
        session.add(user_msg)
        session.flush()

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
            f"Created conversation for webhook pipeline run: "
            f"conversation_id={conversation.id}, uuid={conversation.uuid}, "
            f"webhook_type={webhook_type}, response_id={response_msg_group.uuid}"
        )
        return str(conversation.uuid), pipeline_participant.id, str(response_msg_group.uuid)


def execute_pipeline_webhook(
    project_id: int,
    version: ApplicationVersion,
    user_id: int,
    webhook_type: str,
    payload: dict,
) -> dict:
    """
    Execute a pipeline via webhook trigger.

    Creates a conversation with History tab tracking and executes the pipeline.

    Args:
        project_id: Project ID
        version: ApplicationVersion instance
        user_id: User ID for execution context
        webhook_type: Type of webhook (github, gitlab, custom)
        payload: Parsed JSON payload from webhook

    Returns:
        Dict with task_id, conversation_id, and status
    """
    payload_str = json.dumps(payload, indent=2)
    payload_preview = payload_str[:500] + "..." if len(payload_str) > 500 else payload_str

    conversation_uuid, participant_id, response_message_id = _create_webhook_run_conversation(
        project_id=project_id,
        version=version,
        user_id=user_id,
        webhook_type=webhook_type,
        payload_preview=payload_preview,
    )

    stream_id = conversation_uuid

    result = rpc_tools.RpcMixin().rpc.timeout(5).applications_predict_sio(
        sid=None,
        sio_event=SioEvents.chat_predict.value,
        chat_project_id=project_id,
        data={
            "project_id": project_id,
            "application_id": version.application_id,
            "version_id": version.id,
            "user_input": payload_str,
            "chat_history": [],
            "message_id": response_message_id,
            "stream_id": stream_id,
            "conversation_id": conversation_uuid,
        },
        await_task_timeout=-1,
        user_id=user_id,
        is_system_user=True,
    )

    log.info(
        f"Webhook pipeline execution started: project={project_id}, "
        f"version_id={version.id}, webhook_type={webhook_type}, "
        f"task_id={result.get('task_id')}, conversation={conversation_uuid}"
    )

    return {
        "status": "started",
        "task_id": result.get("task_id"),
        "conversation_id": conversation_uuid,
    }


class RPC:
    """RPC methods for pipeline webhook execution."""

    @web.rpc("pipelines_execute_webhook", "execute_pipeline_webhook")
    def execute_pipeline_webhook_rpc(
        self,
        project_id: int,
        version_id: int,
        user_id: int,
        webhook_type: str,
        payload: dict,
        **kwargs
    ) -> dict:
        """
        Execute a pipeline via webhook trigger (RPC interface).

        Args:
            project_id: Project ID
            version_id: Pipeline version ID
            user_id: User ID for execution context
            webhook_type: Type of webhook (github, gitlab, custom)
            payload: Parsed JSON payload from webhook

        Returns:
            Dict with task_id, conversation_id, and status
        """
        with db.get_session(project_id) as session:
            version = session.query(ApplicationVersion).filter(
                ApplicationVersion.id == version_id,
                ApplicationVersion.agent_type == AgentTypes.pipeline.value,
            ).first()

            if not version:
                return {"error": f"Pipeline version {version_id} not found", "status": "error"}

            pipeline_settings = version.pipeline_settings or {}
            trigger = pipeline_settings.get("trigger", {})

            if trigger.get("type") != TriggerType.webhook.value:
                return {
                    "error": "Pipeline does not have webhook trigger enabled",
                    "status": "error"
                }

            configured_webhook_type = trigger.get("webhook_type")
            if configured_webhook_type != webhook_type:
                return {
                    "error": f"Webhook type mismatch: expected '{configured_webhook_type}', got '{webhook_type}'",
                    "status": "error"
                }

            return execute_pipeline_webhook(
                project_id=project_id,
                version=version,
                user_id=user_id,
                webhook_type=webhook_type,
                payload=payload,
            )
