from typing import List, Dict, Any, Union

from pylon.core.tools import log

from ..models.enums.all import ParticipantTypes
from ..models.message_group import ConversationMessageGroup
from ..models.message_items.attachment import AttachmentMessageItem
from ..models.message_items.text import TextMessageItem
from ..models.message_items.canvas import CanvasMessageItem, CanvasVersionItem
from ..models.enums.all import ChatHistoryRole
from ..models.pd.chat import ChatHistory


def get_role(message: ConversationMessageGroup) -> str:
    if message.author_participant.entity_name == ParticipantTypes.user:
        return ChatHistoryRole.user.value
    return ChatHistoryRole.assistant.value


def generate_chat_history_from_summaries(summaries: List['dict'] = None) -> dict:
    """
    Prepend summaries as a user message to chat history.
    Uses the standard "Here is a summary of the conversation to date:" prefix
    so the SDK summarization middleware can detect and skip re-summarizing it.
    """
    if not summaries:
        return ChatHistory(
            role=ChatHistoryRole.user.value,
            content=[{"type": "text", "text": ""}]
        ).dict()

    summary_parts = []
    for summary in summaries:
        summary_text = str(summary['summary_content']).strip()
        if summary_text:
            summary_parts.append(summary_text)

    combined = "\n\n".join(summary_parts)
    return ChatHistory(
        role=ChatHistoryRole.user.value,
        content=[{"type": "text", "text": f"Here is a summary of the conversation to date:\n\n{combined}"}],
        additional_kwargs={"lc_source": "summarization"},
    ).dict(exclude_none=False)


def generate_chat_history_from_message_items(role, message_items) -> dict:
    assert all(m.message_group_id == message_items[0].message_group_id for m in message_items)

    chat_history_chunk = []
    for message in message_items:
        if message.item_type == TextMessageItem.__mapper_args__['polymorphic_identity']:
            if not message.content:
                continue
            chat_history_chunk.append({"type": "text", "text": message.content})
        elif message.item_type == CanvasMessageItem.__mapper_args__['polymorphic_identity']:
            latest_version: CanvasVersionItem = message.latest_version
            if latest_version:
                if latest_version.code_language:
                    canvas_content = f'```{latest_version.code_language}\n\n{latest_version.canvas_content}\n\n```'
                else:
                    canvas_content = latest_version.canvas_content
                chat_history_chunk.append(
                    {"type": "text", "text": canvas_content}
                )
        elif message.item_type == AttachmentMessageItem.__mapper_args__['polymorphic_identity']:
            # message.content is now a list, so extend instead of append to flatten
            if isinstance(message.content, list):
                chat_history_chunk.extend(message.content)
            else:
                # Fallback for any legacy data that might still be dict
                chat_history_chunk.append(message.content)
    
    return ChatHistory(
        role=role,
        content=chat_history_chunk
    ).dict()


def generate_chat_history(message_groups: List[ConversationMessageGroup], summaries: List['dict'] = None) -> List[dict]:
    chat_history = []

    if summaries:
        chat_history.append(
            generate_chat_history_from_summaries(summaries)
        )

    for msg_group in message_groups:
        role = get_role(msg_group)
        chat_history_item = generate_chat_history_from_message_items(role, msg_group.message_items)
        if chat_history_item['content']:
            chat_history.append(
                chat_history_item
            )
    return chat_history


def generate_user_input(message_group: ConversationMessageGroup) -> list:
    role = get_role(message_group)
    user_input = generate_chat_history_from_message_items(
            role=role, message_items=message_group.message_items
    ).get('content', [])
    return user_input


# --- Multimodal Content Filtering ---

def exclude_image_base64_content(content: Union[str, List[Dict[str, Any]]]) -> Union[str, List[Dict[str, Any]]]:
    """Filter image_url items from content. Returns filtered content."""
    if isinstance(content, str):
        return content
    
    if not isinstance(content, list):
        return content
    
    filtered = [
        item for item in content
        if not (isinstance(item, dict) and item.get('type') == 'image_url')
    ]
    
    # Placeholder if all content was filtered
    if not filtered:
        return '[Image content removed - model does not support vision]'
    
    return filtered


def exclude_image_base64_content_from_chat_history(
    chat_history: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """Exclude image_url items from all messages in chat history."""
    log.info("Filtering image content from chat history (model doesn't support vision)")
    
    result = []
    for msg in chat_history:
        if not isinstance(msg, dict):
            result.append(msg)
            continue
        
        content = msg.get('content')
        filtered_content = exclude_image_base64_content(content)
        result.append({**msg, 'content': filtered_content})
    
    return result
