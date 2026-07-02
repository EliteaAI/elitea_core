from flask import request
from tools import api_tools, auth, config as c, rpc_tools

from ...configurations.models.pd.project_icon import IconMeta
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

        # Get teammates count
        try:
            user_ids = rpc.admin_get_users_ids_in_project(
                project_id, filter_system_user=True
            )
            teammates_count = len(user_ids) if user_ids else 0
        except Exception:
            teammates_count = 0

        # Get project icon_meta from configurations
        config = rpc.configurations_get_first_filtered_project(
            project_id=project_id,
            filter_fields={"type": "project_icon", "elitea_title": f"project_icon_{project_id}"},
        )
        icon_meta = None
        if config and config.get("data"):
            icon_meta = config["data"].get("icon_meta")

        return {
            "teammates_count": teammates_count,
            "icon_meta": icon_meta,
        }, 200

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
        """Update project icon_meta selection."""
        raw = dict(request.json)
        raw_icon_meta = raw.get("icon_meta")
        icon_meta = IconMeta.model_validate(raw_icon_meta).model_dump() if raw_icon_meta else None

        rpc = rpc_tools.RpcMixin().rpc.timeout(5)
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

        return {
            "icon_meta": updated_icon_meta,
        }, 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes(
        [
            "<int:project_id>/project-info",
        ]
    )

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
