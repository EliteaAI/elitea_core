from tools import api_tools, auth, config as c, register_openapi, rpc_tools

from ...utils.constants import PROMPT_LIB_MODE
from .project_budgets import _limit_source

OPENAPI_TAG = "elitea_core/usage"

PROJECT_ID_PARAM = {
    "name": "project_id",
    "in": "path",
    "required": True,
    "schema": {"type": "integer"},
    "description": "Project whose members to report on.",
    "example": 1,
}


def _member_budget_rows(project_id: int):
    """Per-user budgets and spend for every member of a project."""
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
    # One LiteLLM call for all members rather than one per row. Safe to batch because
    # different users' tags cover disjoint requests, unlike a project tag and its users'.
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
    try:
        warning_pct = rpc.timeout(5).litellm_get_warning_threshold(scope="user")
    except Exception:  # pylint: disable=W0703
        warning_pct = 80
    #
    return {
        "rows": rows,
        "total": len(rows),
        "warning_pct": warning_pct,
    }


class PromptLibAPI(api_tools.APIModeHandler):
    """Project admins list their own project's per-member usage."""

    @register_openapi(
        name="List Project Member Budgets",
        description=(
            "Per-member spend and budget for every member of the project, for the current "
            "month. Backs the Members section of Settings -> Usage.\n\n"
            "A member's own limit applies in addition to the project limit: a call is "
            "blocked when either is exceeded.\n\n"
            "In prompt_lib mode this is restricted to admins of that project; ordinary "
            "members receive 403, since they may see only their own usage. In "
            "administration mode it is available to platform administrators regardless of "
            "membership."
        ),
        tags=[OPENAPI_TAG],
        parameters=[PROJECT_ID_PARAM],
        responses={
            "200": {"description": "One row per member, plus the configured warning threshold."},
            "403": {"description": "Caller is not an admin of this project."},
        },
    )
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
        # Members see only their own usage, so the member list is admin-only
        try:
            is_admin = rpc_tools.RpcMixin().rpc.timeout(5).admin_check_user_is_admin(
                project_id, auth.current_user().get("id"),
            )
        except Exception:  # pylint: disable=W0703
            is_admin = False
        #
        if not is_admin:
            return {"error": "Forbidden"}, 403
        #
        return _member_budget_rows(project_id), 200


class AdminAPI(api_tools.APIModeHandler):
    """Platform-admin listing of per-user budgets and spend within one project."""

    # Documented on PromptLibAPI: both handlers share one path, so only one registration
    # survives. Same convention as analytics.py.
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
        return _member_budget_rows(project_id), 200


class API(api_tools.APIBase):
    url_params = [
        "<string:mode>/<int:project_id>",
    ]

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
        c.ADMINISTRATION_MODE: AdminAPI,
    }
