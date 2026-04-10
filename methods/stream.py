#!/usr/bin/python3
# coding=utf-8

#   Copyright 2024 EPAM Systems
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

""" Method """
from pylon.core.tools import web, log  # pylint: disable=E0611,E0401
from tools import prettify
from pydantic import BaseModel, ValidationError, conint

from ..models.enums.all import IndexDataStatus
from ..models.pd.index import IndexDataRemovedEvent
from ..sio.all import get_event_room, SioEvents
from ..utils.application_tools import handle_index_data_failure, ensure_index_data_has_task_id, \
    clean_up_schedule_in_toolkit


class Method:
    @web.method()
    def stream_response(self, event, payload):
        log.debug(f"Application stream_response:\n{prettify(payload)}")
        stream_id = payload['stream_id']
        sio_event = payload.get('sio_event', SioEvents.application_predict) or SioEvents.application_predict

        # Handle index data status events separately
        if payload.get('type') == "agent_index_data_status":
            response_metadata = payload.get('response_metadata', {})
            self.process_index_data_status_event(response_metadata)

        # Handle index data status events separately
        if payload.get('type') == "agent_index_data_removed":
            self.process_index_data_removed_event(payload.get('response_metadata', {}))
        # Handle MCP authorization required - pause streaming (set is_streaming = False)
        if payload.get('type') == "mcp_authorization_required":
            self.context.event_manager.fire_event('chat_message_stream_pause', payload)

        # Handle swarm agent response - emit as separate child message for non-parent agents
        if payload.get('type') == "agent_swarm_agent_response":
            response_metadata = payload.get('response_metadata', {})
            is_default_agent = response_metadata.get('is_default_agent', True)
            # Content is in response_metadata (the SDK payload), not at top level
            content = response_metadata.get('content') or ''
            if not is_default_agent:
                # Emit swarm child message event for UI to display as separate message
                self._emit_swarm_child_message(
                    stream_id=stream_id,
                    sio_event=sio_event,
                    agent_name=response_metadata.get('agent_name', 'child_agent'),
                    content=content,
                    parent_message_id=payload.get('message_id'),
                    response_metadata=response_metadata,
                )

        # Handle summarization events
        if payload.get('type') == "summarization_started":
            self._emit_summarization_event(
                stream_id=stream_id,
                sio_event=sio_event,
                event_type='chat_predict_summary_started',
                message_id=payload.get('message_id'),
                content=payload.get('content', {}),
            )

        if payload.get('type') == "summarization_finished":
            self._emit_summarization_event(
                stream_id=stream_id,
                sio_event=sio_event,
                event_type='chat_predict_summary_finished',
                message_id=payload.get('message_id'),
                content=payload.get('content', {}),
            )

        room = get_event_room(sio_event, stream_id)
        self.context.sio.emit(
            event=sio_event,
            data=payload,
            room=room,
        )

    @web.method()
    def process_index_data_status_event(self, response_metadata):
        """
        Process agent_index_data_status events.

        Handles:
        - Notification about index data status
        - Ensuring task_id is set for in_progress state
        - Handling failure events

        Args:
            response_metadata: Event metadata containing state, task_id, index_name, etc.
        """
        # Always notify about index data status
        self.notify_index_data_status(response_metadata)

        # Ensure task_id is set for in_progress state
        if response_metadata.get('state') == IndexDataStatus.in_progress:
            try:
                ensure_index_data_has_task_id(self.context, response_metadata)
            except Exception as e:
                log.error(f"Failed to ensure task_id for index: {e}")

        # Handle failure events with error
        if response_metadata.get('state') == IndexDataStatus.failed and response_metadata.get('error'):
            try:
                handle_index_data_failure(self.context, response_metadata)
            except Exception as e:
                log.error(f"Failed to handle index_data failure event: {e}")

    @web.method()
    def process_index_data_removed_event(self, response_metadata):
        """
        Clean up the schedule in the toolkit for the specified index.
        Any errors or missing/invalid fields are logged comprehensively.

        Args:
            response_metadata (dict): Metadata containing at least 'index_name' (str), 'toolkit_id' (int), and 'project_id' (int).
                Example: {"index_name": "my_index", "toolkit_id": 123, "project_id": 1, ...}
        """
        try:
            validated = IndexDataRemovedEvent(**response_metadata)
        except ValidationError as e:
            log.error(f"Invalid response_metadata for index removal: {e}\nPayload: {prettify(response_metadata)}")
            return

        result, code = clean_up_schedule_in_toolkit(validated.project_id, validated.toolkit_id, validated.index_name)
        if not result.get("ok", True):
            log.error(result.get("error", f"Failed to clean up index meta for index_name={validated.index_name} in toolkit_id={validated.toolkit_id}, project_id={validated.project_id}"))

    @web.method()
    def _emit_swarm_child_message(
            self,
            stream_id: str,
            sio_event: str,
            agent_name: str,
            content: str,
            parent_message_id: str,
            response_metadata: dict,
    ):
        """
        Emit a swarm child message event for the UI to display as a separate chat message,
        and persist it to the database for history replay.

        This transforms the agent_swarm_agent_response event into a swarm_child_message
        that the UI can render as a distinct message with agent attribution.

        Args:
            stream_id: Stream ID for the conversation
            sio_event: Socket.IO event name
            agent_name: Name of the child agent
            content: Response content from the agent
            parent_message_id: ID of the parent message
            response_metadata: Full response metadata (includes chat_project_id, child_message_uuid)
        """
        from datetime import datetime, timezone

        # Use the UUID generated by pylon_indexer for consistency
        child_message_id = response_metadata.get('child_message_uuid')
        if not child_message_id:
            from uuid import uuid4
            child_message_id = str(uuid4())

        room = get_event_room(sio_event, stream_id)

        # 1. Emit real-time Socket.IO event for immediate UI display
        self.context.sio.emit(
            event=sio_event,
            data={
                'type': 'swarm_child_message',
                'stream_id': stream_id,
                'message_id': child_message_id,
                'parent_message_id': parent_message_id,
                'agent_name': agent_name,
                'content': content,
                'role': 'assistant',
                'is_swarm_child': True,
                'response_metadata': response_metadata,
                'created_at': datetime.now(tz=timezone.utc).isoformat(),
            },
            room=room,
        )
        log.debug(f"[SWARM] Emitted swarm_child_message for agent: {agent_name}")

        # 2. Fire event to persist child message to database (if chat context available)
        chat_project_id = response_metadata.get('chat_project_id')
        if chat_project_id and sio_event == SioEvents.chat_predict.value:
            persistence_payload = {
                'message_id': parent_message_id,
                'content': content,
                'sio_event': sio_event,
                'response_metadata': {
                    'chat_project_id': chat_project_id,
                    'child_agent_name': agent_name,
                    'child_message_uuid': child_message_id,
                },
            }
            self.context.event_manager.fire_event('chat_child_message_save', persistence_payload)
            log.debug(f"[SWARM] Fired chat_child_message_save for persistence: {child_message_id}")

    @web.method()
    def _emit_summarization_event(
            self,
            stream_id: str,
            sio_event: str,
            event_type: str,
            message_id: str,
            content: dict,
    ):
        """
        Emit summarization progress events to the UI.

        Args:
            stream_id: Stream ID for the conversation
            sio_event: Socket.IO event name
            event_type: 'chat_predict_summary_started' or 'chat_predict_summary_finished'
            message_id: Message ID
            content: Event content with summarization details
        """
        room = get_event_room(sio_event, stream_id)
        self.context.sio.emit(
            event=sio_event,
            data={
                'type': event_type,
                'stream_id': stream_id,
                'message_id': message_id,
                'content': content,
                'sio_event': sio_event,
            },
            room=room,
        )
        log.debug(f"[SUMMARIZATION] Emitted {event_type}")
