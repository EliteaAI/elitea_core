import json
import time

from flask import request

from pydantic import ValidationError
from tools import api_tools, auth, db, config as c, register_openapi
from tools import serialize
from pylon.core.tools import log

from sqlalchemy import desc, asc
from sqlalchemy.orm import selectinload

from ...models.conversation import Conversation
from ...models.message_group import ConversationMessageGroup
from ...models.message_items.base import MessageItem
from ...models.message_items.text import TextMessageItem
from ...models.pd.message import MessageGroupDetail, MessagePostPayload
from ...utils.conversation_utils import _message_group_columns, fetch_guarded_message_groups
from ...models.participants import Participant, ParticipantMapping
from ...models.enums.all import ParticipantTypes
from ...utils.sio_utils import get_chat_room

from ...utils.constants import PROMPT_LIB_MODE
from ...utils.context_analytics import update_conversation_meta
from ...utils.sio_utils import SioEvents, SioValidationError
from ...utils.exceptions import PoolSaturationError


def _serialize_guarded_groups(group_dicts: list) -> list:
    """Validate guarded message-group dicts through MessageGroupDetail to the JSON output shape."""
    return [MessageGroupDetail.model_validate(g).model_dump(mode='json') for g in group_dicts]


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="Get Messages",
        description="Retrieve a paginated list of message groups from a conversation with optional full-text search",
        mcp_description="""
        USE to retrieve the message history of a conversation for display, analysis, or context-building.

        DO NOT USE to get a single known message by UUID → use get_message.
        DO NOT USE to get conversation metadata alongside messages → use get_conversation for a combined response.

        Pagination guidance: use sort_order=asc + paginate with offset to read messages chronologically. Use
        sort_order=desc to get the most recent messages first.

        Examples:
        1. Get latest 10 messages: GET .../messages/prompt_lib/42/7 (defaults apply)
        2. Get chronological history page 2: GET ...?sort_order=asc&limit=20&offset=20
        3. Search for a term: GET ...?query=authentication+error
        4. Check for streaming messages: filter response rows where is_streaming == true.
        """,
        parameters=[
            {"name": "query", "in": "query", "required": False, "schema": {"type": "string"}, "description": "Search query for filtering messages"},
            {"name": "limit", "in": "query", "required": False, "schema": {"type": "integer", "default": 10}},
            {"name": "offset", "in": "query", "required": False, "schema": {"type": "integer", "default": 0}},
            {"name": "sort_by", "in": "query", "required": False, "schema": {"type": "string", "default": "created_at"}},
            {"name": "sort_order", "in": "query", "required": False, "schema": {"type": "string", "enum": ["asc", "desc"], "default": "desc"}},
        ],
        tags=["elitea_core/chat"],
        available_to_users=True,
    )
    @auth.decorators.check_api({
        "permissions": ["models.chat.messages.list"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        },
    })
    @api_tools.endpoint_metrics
    def get(self, project_id: int, conversation_id: int, **kwargs):
        with db.get_session(project_id) as session:
            q = request.args.get('query')
            limit = request.args.get('limit', default=10, type=int)
            offset = request.args.get('offset', default=0, type=int)
            sort_by = request.args.get('sort_by', default='created_at')
            sorting_by = getattr(ConversationMessageGroup, sort_by)
            sort_order = request.args.get('sort_order', default='desc')
            sorting = desc if sort_order == 'desc' else asc

            # Select safe (server-side stripped) meta columns so an oversized blob never crosses the
            # wire / hits the gevent hub (same pattern as get_conversation_details).
            query = session.query(*_message_group_columns()).filter(
                ConversationMessageGroup.conversation_id == conversation_id
            )

            if q:
                # todo: search in different message types?
                query = query.join(
                    TextMessageItem,
                    ConversationMessageGroup.message_items
                ).filter(TextMessageItem.content.ilike(f'%{q}%'))

            total = query.count()
            result = query.order_by(sorting(sorting_by)).limit(limit).offset(offset).all()

            group_dicts = fetch_guarded_message_groups(
                session, result, log_label=f'messages_list conv {conversation_id}')
            rows = _serialize_guarded_groups(group_dicts)

            return {
                'total': total,
                'rows': rows
            }, 200

    @register_openapi(
        name="Delete All Messages",
        description="Delete all messages from a conversation.",
        tags=["elitea_core/chat"],
        available_to_users=True,
    )
    @auth.decorators.check_api({
        "permissions": ["models.chat.messages.delete"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        }
    })
    @api_tools.endpoint_metrics
    def delete(self, project_id: int, conversation_id: int, **kwargs):
        with db.get_session(project_id) as session:
            current_user = auth.current_user()
            user_id = current_user.get('id')
            conversation: Conversation = session.query(Conversation).filter(
                Conversation.id == conversation_id
            ).first()

            if not conversation:
                return f'No such conversation with id {conversation_id}', 400

            if not conversation.author_id == user_id:
                return (f'You can not delete all messages from '
                        f'conversation with id {conversation_id}'), 400

            # Collect thread_ids from agent message meta BEFORE deleting messages.
            # These are needed to clean up LangGraph checkpoints that persist
            # independently from conversation messages.
            thread_ids = set()
            agent_messages = session.query(ConversationMessageGroup.meta).filter(
                ConversationMessageGroup.conversation_id == conversation_id,
                ConversationMessageGroup.meta.isnot(None)
            ).all()
            for (meta,) in agent_messages:
                if isinstance(meta, dict) and meta.get('thread_id'):
                    thread_ids.add(meta['thread_id'])
            # Also include conversation UUID as fallback thread_id
            # (used by ensure_thread_id when no stored thread_id exists)
            thread_ids.add(str(conversation.uuid))

            session.query(MessageItem).filter(
                MessageItem.message_group_id.in_(
                    session.query(ConversationMessageGroup.id).filter(
                        ConversationMessageGroup.conversation_id == conversation_id
                    )
                )
            ).delete()

            session.query(ConversationMessageGroup).filter(
                ConversationMessageGroup.conversation_id == conversation_id
            ).delete()

            session.commit()

            try:
                update_conversation_meta(project_id, conversation_id, {'context_analytics': None})
            except Exception as e:
                log.error(f"Failed to reset context analytics for conversation {conversation_id}: {e}")

            # Delete LangGraph checkpoints for this conversation's thread_ids
            if thread_ids:
                try:
                    from ...utils.vectorstore import get_pgvector_connection_string
                    pgvector_connstr = get_pgvector_connection_string(project_id)
                    self.module.event_node.emit('indexer_delete_checkpoint', {
                        'thread_ids': list(thread_ids),
                        'pgvector_connstr': pgvector_connstr,
                    })
                except Exception as e:
                    log.error(f"Failed to delete checkpoints for conversation {conversation_id}: {str(e)}")

            room = get_chat_room(conversation.uuid)

            self.module.context.sio.emit(
                event=SioEvents.chat_message_delete_all,
                data={'conversation_id': conversation_id},
                room=room,
            )
            return None, 204

    @register_openapi(
        name="Send Message",
        description="Send a message to a conversation and get AI response.",
        mcp_description="""
        Send a message to a conversation and get AI response.

        CRITICAL: `conversation_uuid` is a required string UUID (e.g. "550e8400-e29b-41d4-a716-446655440000"),
        NOT the integer conversation_id. Get it from list_conversations or get_conversation response.

        `await_task_timeout` controls blocking behaviour:
        - 30 (default) — wait up to 30 s for the AI response; returns completed message_groups.
        - -1 — async: returns immediately with task_id, use get_message to poll.
        - 0 to 300 — custom timeout in seconds.

        Examples:
        1. Simple message: { 'conversation_uuid': '550e...', 'user_input': 'Hello' }
        2. Direct to a specific participant: { 'conversation_uuid': '550e...', 'user_input': 'Hello', 'participant_id': 15 }
        3. Async: { 'conversation_uuid': '550e...', 'user_input': 'Write a long essay...', 'await_task_timeout': 0 }
        """,
        request_body=MessagePostPayload,
        mcp_tool=True,
        tags=["elitea_core/chat"],
        available_to_users=True,
    )
    @auth.decorators.check_api(
        {
            "permissions": ["models.chat.messages.create"],
            "recommended_roles": {
                c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
                c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
            },
        }
    )
    @api_tools.endpoint_metrics
    def post(self, project_id: int, conversation_uuid: str):
        raw = dict(request.json)
        raw['conversation_uuid'] = conversation_uuid
        if "llm_settings" not in raw:
            participant_id = raw.get("participant_id")
            llm_settings_resolved = False
            if participant_id:
                try:
                    with db.get_session(project_id) as session:
                        mapping = session.query(ParticipantMapping).join(
                            Participant, Participant.id == ParticipantMapping.participant_id
                        ).join(
                            Conversation, Conversation.id == ParticipantMapping.conversation_id
                        ).filter(
                            ParticipantMapping.participant_id == participant_id,
                            Participant.entity_name == ParticipantTypes.application,
                            Conversation.uuid == conversation_uuid,
                        ).first()
                        if mapping:
                            # Application LLM settings are resolved from version_details at
                            # prediction time (see generate_payload in chat_all.py).
                            # Do NOT inject a default here — it would override the app's model.
                            llm_settings_resolved = True
                except Exception as e:
                    log.warning(f"Failed to resolve participant llm_settings: {e}")
            if not llm_settings_resolved:
                models_data = self.module.context.rpc_manager.timeout(2).configurations_get_default_model(
                    project_id=project_id, section="llm", include_shared=True
                )
                raw['llm_settings'] = {
                    **models_data
                }
        try:
            request_data = MessagePostPayload.model_validate(raw)
        except ValidationError as e:
            return {"detail": "Validation failed", "errors": e.errors()}, 400

        message_payload = {
            "project_id": project_id,
            **serialize(request_data.model_dump(exclude={"await_task_timeout", "return_chat_history"})),
        }

        if request_data.await_task_timeout > 0 and request_data.return_task_id:
            return {
                "error": "Can not return task id and wait for task completion simultaneously",
            }, 400

        # await response
        try:
            result = self.module.chat_predict_sio(
                sid=None,
                data=message_payload,
                await_task_timeout=request_data.await_task_timeout,
                return_message_ids=True,
                return_chat_history=request_data.return_chat_history,
            )
        except SioValidationError as e:
            return {
                "detail": "SioValidationError",
                "error": f"Wrong input data: {e.error}",
            }, 400
        except PoolSaturationError as e:
            return {
                "error": "temporarily_unavailable",
                "message": "The service is busy processing other requests. Please try again in a few seconds.",
                "retry_after": e.retry_after,
            }, 503
        except Exception as ex:
            import traceback
            log.error(
                f"{ex}\n{traceback.format_exc()}"
            )
            return {
                "error": "Can not create message",
            }, 400

        # Ensure result is a dict (not an exception)
        if not isinstance(result, dict):
            log.error(f"chat_predict_sio returned non-dict: {type(result)} = {result}")
            return {
                "error": f"Unexpected result type: {str(result)}",
            }, 400

        if "error" in result:
            # Ensure error is a string (fix for non-serializable exception objects)
            error_value = result["error"]
            if not isinstance(error_value, str):
                error_value = str(error_value)
            return {
                "error": error_value,
            }, 400

        if request_data.await_task_timeout <= 0 and request_data.return_task_id:
            # Sanitize result to ensure all values are JSON-serializable
            sanitized_result = {}
            for k, v in result.items():
                if isinstance(v, Exception):
                    sanitized_result[k] = str(v)
                else:
                    sanitized_result[k] = v
            return sanitized_result, 200

        status_code = 201
        with db.get_session(project_id) as session:
            message_groups = session.query(ConversationMessageGroup).options(
                selectinload(ConversationMessageGroup.message_items)
            ).filter(
                ConversationMessageGroup.id.in_(result.values())
            ).order_by(
                ConversationMessageGroup.created_at.asc()
            ).all()
            if len(message_groups) != 2:
                return {
                    "error": "Invalid number of message groups: expected to be 2",
                }, 400
            reply_message = message_groups[-1]
            if not reply_message.message_items:
                for poll_timeout in range(1, 4):
                    session.refresh(reply_message)
                    if not reply_message.is_streaming:
                        break
                    time.sleep(poll_timeout)
                else:
                    status_code = 202
            # Re-select via safe (server-side stripped) meta columns so an oversized reply blob
            # never crosses the wire / hits the gevent hub.
            ordered_ids = [mg.id for mg in message_groups]
            safe_rows = session.query(*_message_group_columns()).filter(
                ConversationMessageGroup.id.in_(ordered_ids)
            ).order_by(ConversationMessageGroup.created_at.asc()).all()
            group_dicts = fetch_guarded_message_groups(
                session, safe_rows, log_label=f'send_message conv {conversation_uuid}')
            result = _serialize_guarded_groups(group_dicts)

        return {"message_groups": result}, status_code


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>/<int:conversation_id>',
        '<int:project_id>/<string:conversation_uuid>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI
    }
