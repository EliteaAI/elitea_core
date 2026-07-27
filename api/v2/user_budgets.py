from tools import api_tools, auth, config as c, rpc_tools

from .project_budgets import _limit_source


class AdminAPI(api_tools.APIModeHandler):
    """Platform-admin listing of per-user budgets and spend within one project."""

    @auth.decorators.check_api(
        {
            "permissions": ["models.admin.project_budgets.view"],
            "recommended_roles": {
                c.ADMINISTRATION_MODE: {"admin": True, "editor": False, "viewer": False},
            },
        }
    )
    @api_tools.endpoint_metrics
    def get(self, project_id: int, **kwargs):
        rpc = rpc_tools.RpcMixin().rpc
        #
        # Returns {user_id: [roles]}, so ids come from the keys
        user_roles = rpc.timeout(15).admin_get_users_roles_in_project(
            project_id, filter_system_user=True,
        ) or {}
        #
        user_ids = [int(user_id) for user_id in user_roles]
        #
        users = rpc.timeout(15).auth_list_users(user_ids=user_ids) or [] if user_ids else []
        by_id = {int(user["id"]): user for user in users}
        #
        stored = rpc.timeout(10).elitea_core_list_user_budgets(project_id=project_id) or []
        by_user = {row["user_id"]: row for row in stored}
        #
        # One LiteLLM call for all members rather than one per row
        try:
            spend_map = rpc.timeout(30).litellm_get_users_spend(
                project_id=project_id, user_ids=user_ids,
            ) or {}
        except Exception:  # pylint: disable=W0703
            spend_map = {}
        #
        try:
            limit_map = rpc.timeout(20).litellm_get_effective_user_limits(
                project_id=project_id, user_ids=user_ids,
            ) or {}
        except Exception:  # pylint: disable=W0703
            limit_map = {}
        #
        rows = []
        #
        for user_id in user_ids:
            user = by_id.get(user_id) or {}
            row = by_user.get(user_id) or {}
            effective = limit_map.get(user_id, limit_map.get(str(user_id)))
            spend = float(spend_map.get(user_id, spend_map.get(str(user_id), 0)) or 0)
            #
            rows.append({
                "project_id": project_id,
                "user_id": user_id,
                "name": user.get("name") or user.get("email"),
                "email": user.get("email"),
                "roles": user_roles.get(user_id) or user_roles.get(str(user_id)) or [],
                "monthly_limit": row.get("monthly_limit"),
                "effective_limit": effective,
                "limit_source": _limit_source(row, effective),
                "currency": row.get("currency", "USD"),
                "enabled": row.get("enabled", False),
                "spend": spend,
                "remaining": None if effective is None else max(0.0, effective - spend),
                "percent_used": None if not effective else round(spend / effective * 100, 2),
            })
        #
        return {
            "rows": rows,
            "total": len(rows),
        }, 200


class API(api_tools.APIBase):
    url_params = [
        "<string:mode>/<int:project_id>",
    ]

    mode_handlers = {
        c.ADMINISTRATION_MODE: AdminAPI,
    }
