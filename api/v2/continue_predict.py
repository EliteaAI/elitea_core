from flask import request
from pydantic import ValidationError
from tools import api_tools, auth, db, config as c, register_openapi
from pylon.core.tools import log

from ...models.conversation import Conversation
from ...models.message_group import ConversationMessageGroup
from ...models.pd.message import MessageGroupDetail
from ...models.pd.continue_predict import ContinuePredictPayload
from ...utils.conversation_utils import _message_group_columns, fetch_guarded_message_groups
from ...utils.constants import PROMPT_LIB_MODE
from ...utils.sio_utils import SioValidationError
from ...utils.exceptions import PoolSaturationError


def _serialize_guarded_groups(group_dicts: list) -> list:
    """Validate guarded message-group dicts through MessageGroupDetail to the JSON output shape."""
    return [MessageGroupDetail.model_validate(g).model_dump(mode='json') for g in group_dicts]


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="Continue / Resume Conversation",
        description="Resume a HITL-paused conversation with an approve/reject/edit/block decision.",
        mcp_description="""
        USE to resume a conversation that is paused at a human-in-the-loop (HITL) node, so a run can
        advance past the interrupt. Sending a plain message to a paused conversation just re-fires the
        interrupt — use this tool instead.

        CRITICAL: `conversation_uuid` is the string UUID (not the integer id). `message_id` is the UUID
        of the paused response message that raised the HITL interrupt (from the message/interrupt payload).

        `hitl_action`:
        - approve — accept and continue past the node.
        - reject — reject; the run terminates.
        - edit — continue with `hitl_value` as the edited text.
        - block_with_comment — block and continue with `hitl_value` as the note.

        `await_task_timeout`: 30 (default) waits up to 30 s and returns the post-resume message_groups;
        -1 returns immediately (poll with get_message); 0..300 is a custom timeout.

        Examples:
        1. Approve: { 'conversation_uuid': '550e...', 'message_id': 'a1b2...', 'hitl_action': 'approve' }
        2. Edit:    { 'conversation_uuid': '550e...', 'message_id': 'a1b2...', 'hitl_action': 'edit', 'hitl_value': 'use v2 endpoint' }
        3. Reject:  { 'conversation_uuid': '550e...', 'message_id': 'a1b2...', 'hitl_action': 'reject' }
        """,
        request_body=ContinuePredictPayload,
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
        try:
            parsed = ContinuePredictPayload.model_validate(raw)
        except ValidationError as e:
            return {"detail": "Validation failed", "errors": e.errors()}, 400

        data = {
            "project_id": project_id,
            "conversation_uuid": str(parsed.conversation_uuid),
            "message_id": parsed.message_id,
            "should_continue": True,
            "hitl_resume": parsed.hitl_resume,
            "hitl_action": parsed.hitl_action,
            "hitl_value": parsed.hitl_value,
            "hitl_decisions": parsed.hitl_decisions,
            "user_input": parsed.user_input,
            "thread_id": parsed.thread_id,
        }

        try:
            result = self.module.chat_continue_predict_sio(
                sid=None,
                data=data,
                await_task_timeout=parsed.await_task_timeout,
            )
        except SioValidationError as e:
            return {"detail": "SioValidationError", "error": f"Wrong input data: {e.error}"}, 400
        except PoolSaturationError as e:
            return {
                "error": "temporarily_unavailable",
                "message": "The service is busy processing other requests. Please try again in a few seconds.",
                "retry_after": e.retry_after,
            }, 503
        except Exception as ex:
            import traceback
            log.error(f"{ex}\n{traceback.format_exc()}")
            return {"error": "Can not continue conversation"}, 400

        # Non-message control results pass through unchanged (run stopped, or async child resume).
        if isinstance(result, dict) and ("error" in result or "stopped" in result or "task_id" in result):
            if "error" in result:
                error_value = result["error"]
                return {"error": error_value if isinstance(error_value, str) else str(error_value)}, 400
            return result, 200

        # Blocking path: the resumed response message (message_id) is complete. Return it plus its
        # question in the same {"message_groups": [...]} shape as Send Message.
        with db.get_session(project_id) as session:
            response_group = session.query(ConversationMessageGroup).filter(
                ConversationMessageGroup.uuid == parsed.message_id
            ).first()
            if response_group is None:
                return {"error": f"No message found with id {parsed.message_id}"}, 400

            group_ids = []
            if response_group.reply_to_id:
                group_ids.append(response_group.reply_to_id)
            group_ids.append(response_group.id)

            safe_rows = session.query(*_message_group_columns()).filter(
                ConversationMessageGroup.id.in_(group_ids)
            ).order_by(
                ConversationMessageGroup.created_at.asc(),
                ConversationMessageGroup.id.asc(),
            ).all()
            group_dicts = fetch_guarded_message_groups(
                session, safe_rows, log_label=f'continue conv {conversation_uuid}')
            message_groups = _serialize_guarded_groups(group_dicts)

        return {"message_groups": message_groups}, 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>/<string:conversation_uuid>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI
    }
