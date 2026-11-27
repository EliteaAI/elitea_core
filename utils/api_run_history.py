"""Run History rows for agent and pipeline runs started over REST predict or MCP.

``do_predict`` opens the conversation before the run starts and binds the run to it, so the answer lands
through the same ``chat_predict`` persistence events a scheduled run uses. A run refused before it starts drops
its row. Recording is off unless the plugin config sets ``record_api_runs``.
"""
from dataclasses import dataclass
from typing import Optional

from pylon.core.tools import log
from tools import db, this

from .input_guardrail import redacted_input
from .pipeline_execution import TriggerType, create_trigger_run_conversation, run_label
from .predict_utils import user_input_preview
from .sio_utils import SioEvents
from ..models.conversation import Conversation
from ..models.message_group import ConversationMessageGroup
from ..models.message_items.text import TextMessageItem


RECORDED_TRIGGERS = (TriggerType.API, TriggerType.MCP)
RUN_NAME_PREFIX = {TriggerType.API: 'API run', TriggerType.MCP: 'MCP run'}
# The run's input as History shows it; API callers can send far more than a person reads there
STORED_INPUT_LIMIT = 4000


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

    def task_meta(self) -> dict:
        """The task meta that sends the worker's events to this row, as predict_sio binds a chat run."""
        return {
            'stream_id': self.conversation_uuid,
            'message_id': self.response_message_id,
            'sio_event': SioEvents.chat_predict.value,
            'chat_project_id': self.project_id,
        }

    def time_out(self) -> None:
        """Close the row of a blocking run stopped on timeout, as a chat timeout closes its answer.

        The run is marked stopped first, so a late end from the stopped worker is ignored (chat_message_stream_end).
        """
        try:
            this.module.mark_chat_run_stopped(self.response_message_id)
            with db.get_session(self.project_id) as session:
                response = session.query(ConversationMessageGroup).filter(
                    ConversationMessageGroup.uuid == self.response_message_id,
                ).first()
                if response is None:
                    return
                response.is_streaming = False
                if not response.message_items:
                    session.add(TextMessageItem(
                        message_group=response,
                        item_type=TextMessageItem.__mapper_args__['polymorphic_identity'],
                        content=f"{run_label(self.source)} timed out",
                        order_index=0,
                    ))
                session.commit()
        except Exception:  # pylint: disable=W0703
            log.exception('Could not close the timed out API run %s', self.response_message_id)


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
            trigger_message=user_input_preview(redacted_input(user_input), limit=STORED_INPUT_LIMIT) or '',
            conversation_name=f"{RUN_NAME_PREFIX[trigger_source]}: {entity_name or version.application_id}",
            source=source,
        )
    except Exception:  # pylint: disable=W0703
        log.exception('Could not open the %s run conversation for version %s', trigger_source, version.id)
        return None
    return ApiRunConversation(project_id, conversation_uuid, response_message_id, source)
