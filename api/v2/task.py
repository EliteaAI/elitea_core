from tools import api_tools, auth, config as c, register_openapi

from ...utils.constants import PROMPT_LIB_MODE

from pylon.core.tools import log


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="Stop Task",
        description="Stop a running chat task for a message group.",
        tags=["elitea_core/runtime"],
        available_to_users=False,
    )
    @auth.decorators.check_api({
        "permissions": ["models.chat.task.delete"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        },
    })
    @api_tools.endpoint_metrics
    def delete(self, project_id: int, message_group_uuid: str, **kwargs):
        """
        Stop a running chat task for a message group.

        Delegates to chat_stop_task RPC which handles all stop logic:
        1. Stop task via Arbiter
        2. Mark chat run as stopped in Redis
        3. Set is_streaming = False in database
        4. Retire all HITL interrupts
        5. Emit chat_message_sync or chat_message_delete event
        """
        user_id = auth.current_user().get('id')

        result = self.module.context.rpc_manager.call.chat_stop_task(
            project_id=project_id,
            message_group_uuid=message_group_uuid,
            user_id=user_id,
        )

        if result.get('error'):
            code = result.get('code', 'ERROR')
            status_map = {
                'NOT_FOUND': 404,
                'FORBIDDEN': 403,
                'NO_TASK': 400,
            }
            return {"error": result['error']}, status_map.get(code, 500)

        return None, 204


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>/<string:message_group_uuid>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI
    }
