"""Run History rows for agent and pipeline runs started over REST predict or MCP.

``do_predict`` opens the conversation before the run starts and binds the run to it, so the answer lands
through the same ``chat_predict`` persistence events a scheduled run uses.
"""
from dataclasses import dataclass
from typing import Optional

from pylon.core.tools import log
from tools import db, this

from .input_guardrail import redacted_input
from .pipeline_execution import TriggerType, create_trigger_run_conversation
from .predict_utils import user_input_preview
from ..models.conversation import Conversation


RECORDED_TRIGGERS = (TriggerType.API, TriggerType.MCP)
RUN_NAME_PREFIX = {TriggerType.API: 'API run', TriggerType.MCP: 'MCP run'}


@dataclass(frozen=True)
class ApiRunConversation:
    project_id: int
    conversation_uuid: str
    response_message_id: str
    source: str

    def discard(self) -> None:
        """Drop the row of a run that never started, so History shows no run that did not happen."""
        try:
            with db.get_session(self.project_id) as session:
                conversation = session.query(Conversation).filter(
                    Conversation.uuid == self.conversation_uuid,
                ).first()
                if conversation is not None:
                    session.delete(conversation)
                    session.commit()
        except Exception:  # pylint: disable=W0703
            log.exception('Could not discard the API run conversation %s', self.conversation_uuid)

    def fail(self, error: str) -> None:
        """End the answer placeholder with an error, so the row does not stay streaming."""
        try:
            this.module.context.event_manager.fire_event('chat_message_stream_end', {
                'message_id': self.response_message_id,
                'content': error,
                'response_metadata': {
                    'project_id': self.project_id,
                    'chat_project_id': self.project_id,
                    'is_error': True,
                    'error': error,
                },
            })
        except Exception:  # pylint: disable=W0703
            log.exception('Could not end the API run %s with an error', self.response_message_id)


def should_record_api_run(trigger_source, webhook_signature, payload_in: dict, enabled: bool = True) -> bool:
    """A REST or MCP run gets its own row; a webhook or a caller continuing its own conversation does not."""
    if not enabled or webhook_signature is not None or trigger_source not in RECORDED_TRIGGERS:
        return False
    return not (payload_in.get('conversation_id') or payload_in.get('stream_id'))


def run_source(agent_type: Optional[str]) -> str:
    return 'pipeline' if agent_type == 'pipeline' else 'agent'


def open_api_run_conversation(
    *, project_id: int, version, user_id: int, user_input, trigger_source: str, entity_name: Optional[str],
) -> Optional[ApiRunConversation]:
    """Open the row, or None when it cannot be written: recording never fails the run."""
    source = run_source(getattr(version, 'agent_type', None))
    try:
        conversation_uuid, _, response_message_id = create_trigger_run_conversation(
            project_id=project_id,
            version=version,
            user_id=user_id,
            trigger_type=trigger_source,
            trigger_message=user_input_preview(redacted_input(user_input), limit=None) or '',
            conversation_name=f"{RUN_NAME_PREFIX[trigger_source]}: {entity_name or version.application_id}",
            source=source,
        )
    except Exception:  # pylint: disable=W0703
        log.exception('Could not open the %s run conversation for version %s', trigger_source, version.id)
        return None
    return ApiRunConversation(project_id, conversation_uuid, response_message_id, source)
