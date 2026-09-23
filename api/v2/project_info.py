from flask import request
from tools import api_tools, auth, config as c, rpc_tools

from plugins.configurations.models.pd.project_icon import IconMeta
from ...models.pd.chat_config import ProjectChatConfig
from ...utils.constants import PROMPT_LIB_MODE


class PromptLibAPI(api_tools.APIModeHandler):
    @auth.decorators.check_api(
        {
            "permissions": ["models.project_context.view"],
            "recommended_roles": {
                c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
            },
        }
    )
    @api_tools.endpoint_metrics
    def get(self, project_id: int, **kwargs):
        rpc = rpc_tools.RpcMixin().rpc.timeout(5)
        fields = request.args.get('fields', '')
        requested = set(f.strip() for f in fields.split(',')) if fields else None

        result = {}

        if not requested or 'teammates_count' in requested:
            try:
                user_ids = rpc.admin_get_users_ids_in_project(
                    project_id, filter_system_user=True
                )
                result['teammates_count'] = len(user_ids) if user_ids else 0
            except Exception:
                result['teammates_count'] = 0

        if not requested or 'icon_meta' in requested:
            config = rpc.configurations_get_first_filtered_project(
                project_id=project_id,
                filter_fields={"type": "project_icon", "elitea_title": f"project_icon_{project_id}"},
            )
            icon_meta = None
            if config and config.get("data"):
                icon_meta = config["data"].get("icon_meta")
            result['icon_meta'] = icon_meta

        if not requested or 'chat_config' in requested:
            chat_cfg = rpc.configurations_get_first_filtered_project(
                project_id=project_id,
                filter_fields={
                    "type": "project_chat_config",
                    "elitea_title": f"project_chat_config_{project_id}",
                },
            )
            chat_config = None
            if chat_cfg and chat_cfg.get("data"):
                chat_config = chat_cfg["data"].get("chat_config")
            result['chat_config'] = chat_config or ProjectChatConfig().model_dump()

        return result, 200

    @auth.decorators.check_api(
        {
            "permissions": ["models.project_context.edit"],
            "recommended_roles": {
                c.DEFAULT_MODE: {"admin": True, "editor": False, "viewer": False},
            },
        }
    )
    @api_tools.endpoint_metrics
    def put(self, project_id: int, **kwargs):
        """Update project icon_meta and/or chat_config."""
        raw = dict(request.json)
        rpc = rpc_tools.RpcMixin().rpc.timeout(5)

        response = {}

        # --- icon_meta ---
        if 'icon_meta' in raw:
            raw_icon_meta = raw.get("icon_meta")
            icon_meta = IconMeta.model_validate(raw_icon_meta).model_dump() if raw_icon_meta else None

            config = rpc.configurations_get_first_filtered_project(
                project_id=project_id,
                filter_fields={"type": "project_icon", "elitea_title": f"project_icon_{project_id}"},
            )

            if config is None:
                result, _ = rpc.configurations_create_if_not_exists(
                    payload={
                        "project_id": project_id,
                        "elitea_title": f"project_icon_{project_id}",
                        "label": "Project Icon",
                        "type": "project_icon",
                        "data": {"icon_meta": icon_meta},
                    }
                )
            else:
                result = rpc.configurations_update(
                    project_id=project_id,
                    config_id=config["id"],
                    payload={"data": {"icon_meta": icon_meta}},
                )

            updated_icon_meta = None
            if result and result.get("data"):
                updated_icon_meta = result["data"].get("icon_meta")
            response["icon_meta"] = updated_icon_meta

        # --- chat_config ---
        if 'chat_config' in raw:
            validated_chat_config = ProjectChatConfig.model_validate(raw["chat_config"]).model_dump()

            chat_cfg = rpc.configurations_get_first_filtered_project(
                project_id=project_id,
                filter_fields={
                    "type": "project_chat_config",
                    "elitea_title": f"project_chat_config_{project_id}",
                },
            )

            if chat_cfg is None:
                rpc.configurations_create_if_not_exists(
                    payload={
                        "project_id": project_id,
                        "elitea_title": f"project_chat_config_{project_id}",
                        "label": "Chat Default Configuration",
                        "type": "project_chat_config",
                        "data": {"chat_config": validated_chat_config},
                    }
                )
            else:
                rpc.configurations_update(
                    project_id=project_id,
                    config_id=chat_cfg["id"],
                    payload={"data": {"chat_config": validated_chat_config}},
                )

            response["chat_config"] = validated_chat_config

        return response, 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes(
        [
            "<int:project_id>/project-info",
        ]
    )

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
