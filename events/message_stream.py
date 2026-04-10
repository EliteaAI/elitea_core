import json

from pylon.core.tools import web, log
from tools import db, serialize, rpc_tools
from sqlalchemy.orm.attributes import flag_modified

from ..models.participants import Participant
from ..models.message_group import ConversationMessageGroup
from ..models.message_items.text import TextMessageItem
from ..models.pd.message import MessageGroupDetail

from ..utils.sio_utils import get_chat_room
from ..utils.message_stream import update_message_group_meta, safe_decode_bytes_in_dict
from ..utils.attachments import (
    process_single_attachment_file,
    is_multimodal_content,
    process_multimodal_content,
    update_attachment_thumbnails,
)

from ..utils.sio_utils import SioEvents


class Event:
    @web.event('chat_message_stream_end')
    def chat_message_stream_end(self, context, event, payload):
        # log.debug(f'chat_message_stream_end {event=}')
        # log.debug(f'chat_message_stream_end {payload=}')

        with db.get_session(payload['response_metadata']['chat_project_id']) as session:
            msg_group: ConversationMessageGroup = session.query(ConversationMessageGroup).filter(
                ConversationMessageGroup.uuid == payload['message_id']
            ).first()
            if msg_group:
                content = safe_decode_bytes_in_dict(payload['content'])

                # Try to parse string content as JSON (may be stringified list from SDK)
                if isinstance(content, str) and content.strip().startswith('['):
                    try:
                        content = json.loads(content)
                    except (json.JSONDecodeError, TypeError):
                        pass  # Keep original string if parsing fails

                # Check if content is multimodal (list of content chunks)
                if is_multimodal_content(content):
                    # Filter out tool_use chunks (swarm child calls) - they're handled separately
                    # Only process text and image content
                    filtered_content = [
                        chunk for chunk in content
                        if chunk.get('type') in ('text', 'image_url', 'image')
                    ]
                    if filtered_content:
                        # Process multimodal content - creates multiple message items
                        message_items = process_multimodal_content(filtered_content, msg_group, session)
                        log.info(f"Created {len(message_items)} message items from multimodal content (filtered {len(content) - len(filtered_content)} tool_use chunks)")
                    else:
                        # Content was only tool_use blocks (swarm calls) - don't create message item
                        log.debug(f"Skipping message item creation - content only contained tool_use blocks")
                else:
                    # Traditional text content - create single TextMessageItem
                    msg: TextMessageItem = TextMessageItem(
                        content=str(content),
                        message_group=msg_group,
                        order_index=0,
                    )
                    session.add(msg)

                # Extract image thumbnails mapping (used by both files_modified and user-upload blocks)
                image_thumbnails = payload.get('response_metadata', {}).get('image_thumbnails', {})

                # start attachment block
                files_modified = payload.get('response_metadata', {}).get('files_modified', [])
                if files_modified:
                    # Deduplicate files by filepath, keeping only the last occurrence
                    filepath_to_filedata = {}
                    for filedata in files_modified:
                        filepath = filedata.get("filepath", "")
                        if filepath:
                            filepath_to_filedata[filepath] = filedata

                    for order_index, (filepath, filedata) in enumerate(filepath_to_filedata.items(), start=1):
                        try:
                            # Forward media_type as kwarg for image attachments
                            extra_kwargs = {}
                            media_type = (
                                filedata.get('media_type')
                                or filedata.get('meta', {}).get('media_type')
                            )
                            if media_type == 'image':
                                # Use pre-resolved thumbnail if available, otherwise fall back to filepath: scheme
                                thumbnail = image_thumbnails.get(filepath.lstrip('/'))
                                extra_kwargs['image_url'] = thumbnail  # base64 data URL or None → filepath: fallback

                            attachment_msg, _ = process_single_attachment_file(
                                session=session,
                                project_id=payload['response_metadata']['project_id'],
                                msg_group=msg_group,
                                filepath=filepath,
                                order_index=order_index,
                                user_id=filedata.get('user_id'),
                                collection_suffix=None,
                                prompt=filedata.get('meta', {}).get('prompt'),
                                **extra_kwargs,
                            )
                            session.add(attachment_msg)
                        except Exception as e:
                            log.error(f"Failed to process file {filepath}: {e}")
                            # Continue processing other files instead of failing completely

                # Update user-uploaded image attachments with thumbnails
                user_msg_group_updated = False
                if image_thumbnails and msg_group.reply_to_id:
                    user_msg_group = session.query(ConversationMessageGroup).filter(
                        ConversationMessageGroup.id == msg_group.reply_to_id
                    ).first()
                    if user_msg_group:
                        count = update_attachment_thumbnails(session, user_msg_group, image_thumbnails)
                        if count:
                            log.debug(f"Updated {count} attachment(s) with image thumbnails")
                            user_msg_group_updated = True
                # end attachment block

                session.refresh(msg_group)
                msg_group = update_message_group_meta(msg_group, payload, session=session)
                # Set is_streaming to False after refresh to ensure it's persisted
                msg_group.is_streaming = False
                flag_modified(msg_group, 'is_streaming')
                flag_modified(msg_group, 'meta')
                session.add(msg_group)

                # Always flag conversation meta (context_analytics is updated with tokens)
                if msg_group.conversation:
                    flag_modified(msg_group.conversation, 'meta')
                    session.add(msg_group.conversation)

                session.commit()
                session.refresh(msg_group)
                if msg_group.conversation:
                    session.refresh(msg_group.conversation)

                room = get_chat_room(msg_group.conversation.uuid)
                response_payload = serialize(MessageGroupDetail.model_validate(msg_group))
                if msg_group.conversation and msg_group.conversation.meta:
                    context_analytics = msg_group.conversation.meta.get('context_analytics')
                    if context_analytics:
                        response_payload['context_analytics'] = context_analytics
                        # log.debug(f"Added context_analytics to response_payload")
                self.context.sio.emit(
                    event=SioEvents.chat_message_sync,
                    data=response_payload,
                    room=room,
                )

                # Sync user message group if its thumbnails were updated
                if user_msg_group_updated:
                    session.refresh(user_msg_group)
                    user_payload = serialize(MessageGroupDetail.model_validate(user_msg_group))
                    self.context.sio.emit(
                        event=SioEvents.chat_message_sync,
                        data=user_payload,
                        room=room,
                    )

    @web.event('chat_message_stream_partial_save')
    def chat_message_stream_partial_save(self, context, event, payload):
        # log.debug(f'chat_message_stream_partial_save {event=}')
        # log.debug(f'chat_message_stream_partial_save {payload=}')

        with db.get_session(payload['response_metadata']['chat_project_id']) as session:
            msg_group: ConversationMessageGroup = session.query(ConversationMessageGroup).filter(
                ConversationMessageGroup.uuid == payload['message_id']
            ).first()
            if msg_group:
                msg_group = update_message_group_meta(msg_group, payload)
                flag_modified(msg_group, 'meta')
                session.add(msg_group)
                session.commit()
                session.refresh(msg_group)

    @web.event('applications_predict_task_id')
    def applications_predict_task_id(self, context, event, payload):
        # log.debug(f'applications_predict_task_id {event=}')
        # log.debug(f'applications_predict_task_id {payload=}')

        with db.get_session(payload['project_id']) as session:
            msg_group: ConversationMessageGroup = session.query(ConversationMessageGroup).filter(
                ConversationMessageGroup.uuid == payload['message_group_id']
            ).first()
            if msg_group:
                msg_group.task_id = payload['task_id']
                session.add(msg_group)
                session.commit()

    @web.event('chat_child_message_save')
    def chat_child_message_save(self, context, event, payload):
        """
        Handle saving child agent messages as separate chat entries in swarm mode.
        Creates a new message group linked to the parent response and saves the content.
        Child messages get timestamps slightly before parent to maintain natural order.
        """
        from datetime import timedelta

        log.debug(f'chat_child_message_save {event=}')

        response_metadata = payload.get('response_metadata', {})
        chat_project_id = response_metadata.get('chat_project_id')
        parent_message_id = payload.get('message_id')  # UUID of parent response message
        child_agent_name = response_metadata.get('child_agent_name')
        child_message_uuid = response_metadata.get('child_message_uuid')
        content = payload.get('content', '')

        if not chat_project_id or not parent_message_id:
            log.warning(f'chat_child_message_save: Missing required fields chat_project_id={chat_project_id}, parent_message_id={parent_message_id}')
            return

        # Process content: parse JSON and extract text from multimodal content
        # Child agent responses from SDK may come as JSON array with text/tool_use blocks
        if isinstance(content, str) and content.strip().startswith('['):
            try:
                content = json.loads(content)
            except (json.JSONDecodeError, TypeError):
                pass  # Keep original string if parsing fails

        # If content is a list (multimodal), extract text blocks and filter out tool_use
        extracted_text = None
        if isinstance(content, list):
            text_parts = []
            for chunk in content:
                if isinstance(chunk, dict):
                    if chunk.get('type') == 'text':
                        text_parts.append(chunk.get('text', ''))
                    elif 'text' in chunk and chunk.get('type') not in ('tool_use', 'tool_result'):
                        # Handle {'text': '...'} format without explicit type
                        text_parts.append(chunk.get('text', ''))
            if text_parts:
                extracted_text = '\n'.join(text_parts)
            else:
                # Content only contained tool_use blocks (handoff) - skip creating message
                log.debug(f'chat_child_message_save: Skipping child message {child_message_uuid} - content only contained tool_use blocks')
                return
        else:
            extracted_text = str(content) if content else None

        if not extracted_text or not extracted_text.strip():
            log.debug(f'chat_child_message_save: Skipping child message {child_message_uuid} - no text content')
            return

        with db.get_session(chat_project_id) as session:
            # Find the parent message group
            parent_msg_group: ConversationMessageGroup = session.query(ConversationMessageGroup).filter(
                ConversationMessageGroup.uuid == parent_message_id
            ).first()

            if not parent_msg_group:
                log.warning(f'chat_child_message_save: Parent message {parent_message_id} not found')
                return

            # Count existing child messages to calculate offset
            # Each child gets progressively earlier timestamp so they maintain arrival order
            existing_children_count = session.query(ConversationMessageGroup).filter(
                ConversationMessageGroup.meta['parent_message_id'].astext == str(parent_message_id)
            ).count()

            # Set child timestamp slightly before parent (offset by child index to maintain order)
            # First child is 100ms before parent, second is 99ms before, etc.
            offset_ms = 100 - existing_children_count
            if offset_ms < 1:
                offset_ms = 1  # Minimum 1ms offset
            child_created_at = parent_msg_group.created_at - timedelta(milliseconds=offset_ms)

            # Create child message group linked to parent's conversation
            child_msg_group = ConversationMessageGroup(
                uuid=child_message_uuid,
                conversation_id=parent_msg_group.conversation_id,
                author_participant_id=parent_msg_group.author_participant_id,  # Same participant (agent)
                is_streaming=False,
                reply_to_id=parent_msg_group.reply_to_id,  # Same question as parent
                created_at=child_created_at,  # Set timestamp before parent
                meta={
                    'is_child_agent': True,
                    'child_agent_name': child_agent_name,
                    'parent_message_id': str(parent_message_id),
                },
            )
            session.add(child_msg_group)
            session.flush()

            # Create text message item with extracted text content
            msg: TextMessageItem = TextMessageItem(
                content=extracted_text,
                message_group=child_msg_group,
                order_index=0,
            )
            session.add(msg)
            session.commit()
            session.refresh(child_msg_group)

            log.info(f'chat_child_message_save: Created child message {child_message_uuid} for agent {child_agent_name}')

            # Emit sync event to frontend
            room = get_chat_room(parent_msg_group.conversation.uuid)
            response_payload = serialize(MessageGroupDetail.model_validate(child_msg_group))
            # Add child agent metadata for frontend rendering
            response_payload['is_child_agent'] = True
            response_payload['child_agent_name'] = child_agent_name
            response_payload['parent_message_id'] = str(parent_message_id)

            self.context.sio.emit(
                event=SioEvents.chat_message_sync,
                data=response_payload,
                room=room,
            )

    @web.event('chat_message_stream_pause')
    def chat_message_stream_pause(self, context, event, payload):
        """
        Handle pausing the message stream when MCP authorization is required.
        Sets is_streaming = False so UI shows the message is paused waiting for user action.
        """
        log.debug(f'chat_message_stream_pause {event=}')
        
        # Get project_id from response_metadata (set by indexer)
        response_metadata = payload.get('response_metadata', {})
        chat_project_id = response_metadata.get('chat_project_id')
        
        if not chat_project_id:
            log.warning('chat_message_stream_pause: No chat_project_id in payload')
            return
            
        message_id = payload.get('message_id')
        if not message_id:
            log.warning('chat_message_stream_pause: No message_id in payload')
            return

        with db.get_session(chat_project_id) as session:
            msg_group: ConversationMessageGroup = session.query(ConversationMessageGroup).filter(
                ConversationMessageGroup.uuid == message_id
            ).first()
            if msg_group:
                msg_group.is_streaming = False
                session.add(msg_group)
                session.commit()
                log.debug(f'chat_message_stream_pause: Set is_streaming=False for message {message_id}')
