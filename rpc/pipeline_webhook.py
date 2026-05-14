"""
RPC module for Pipeline Webhook execution.

Provides execution of pipelines configured with webhook triggers.
This creates conversations with History tab tracking, similar to scheduled execution.
"""
import json

from pylon.core.tools import web, log
from tools import db

from ..models.all import Application, ApplicationVersion
from ..models.enums.all import AgentTypes
from ..models.pd.pipeline_trigger import TriggerType
from ..utils.pipeline_execution import (
    TriggerType as TriggerTypeConst,
    create_trigger_run_conversation,
    execute_pipeline_via_predict_sio,
)


def execute_pipeline_webhook(
    project_id: int,
    version: ApplicationVersion,
    user_id: int,
    webhook_type: str,
    payload: dict,
) -> dict:
    """
    Execute a pipeline via webhook trigger.

    Creates a conversation with History tab tracking and executes the pipeline
    using the shared execution utilities.

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

    # Get application name for conversation
    with db.get_session(project_id) as session:
        application = session.query(Application).get(version.application_id)
        pipeline_name = application.name if application else f"Pipeline {version.application_id}"

    # Build trigger message with payload preview
    trigger_message = f"[Webhook triggered: {webhook_type}]\n\nPayload:\n```json\n{payload_preview}\n```"

    # Create conversation using shared utility
    conversation_uuid, participant_id, response_message_id = create_trigger_run_conversation(
        project_id=project_id,
        version=version,
        user_id=user_id,
        trigger_type=TriggerTypeConst.WEBHOOK,
        trigger_message=trigger_message,
        conversation_name=f"Webhook run: {pipeline_name}",
        extra_meta={"webhook_type": webhook_type},
    )

    # Execute using shared utility
    result = execute_pipeline_via_predict_sio(
        project_id=project_id,
        version=version,
        user_id=user_id,
        conversation_uuid=conversation_uuid,
        response_message_id=response_message_id,
        user_input=payload_str,  # Full payload as input for webhook
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
