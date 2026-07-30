from flask import request
from tools import api_tools, auth, config as c, register_openapi, rpc_tools

from ...utils.constants import PROMPT_LIB_MODE
from .project_budget import BUDGET_WRITE_BODY

OPENAPI_TAG = "elitea_core/usage"

MEMBER_PARAMS = [
    {
        "name": "project_id",
        "in": "path",
        "required": True,
        "schema": {"type": "integer"},
        "description": "Project the member belongs to.",
        "example": 1,
    },
    {
        "name": "user_id",
        "in": "path",
        "required": True,
        "schema": {"type": "integer"},
        "description": "Member whose budget to read or write.",
        "example": 1,
    },
]


def _user_budget_state(project_id: int, user_id: int):
    """Merge the stored per-user limit with LiteLLM's current-month spend."""
    rpc = rpc_tools.RpcMixin().rpc
    #
    budget = rpc.timeout(5).elitea_core_get_user_budget(
        project_id=project_id, user_id=user_id,
    ) or {}
    #
    try:
        spend = rpc.timeout(15).litellm_get_user_spend(
            project_id=project_id, user_id=user_id,
        ) or {}
    except Exception:  # pylint: disable=W0703
        spend = {}
    #
    limit = budget.get("monthly_limit") if budget.get("enabled", True) else None
    spent = float(spend.get("spend", 0) or 0)
    #
    return {
        "project_id": project_id,
        "user_id": user_id,
        "monthly_limit": budget.get("monthly_limit"),
        "currency": budget.get("currency", "USD"),
        "enabled": budget.get("enabled", False),
        "spend": spent,
        "remaining": None if limit is None else max(0.0, limit - spent),
        "percent_used": None if not limit else round(spent / limit * 100, 2),
        "total_tokens": spend.get("total_tokens", 0),
        "period": spend.get("period"),
        "spend_available": spend.get("available", False),
    }


def _parse_payload(raw: dict):
    """Validate a per-user budget write payload."""
    monthly_limit = raw.get("monthly_limit")
    #
    if monthly_limit is not None:
        monthly_limit = float(monthly_limit)
        if monthly_limit < 0:
            raise ValueError("monthly_limit must be >= 0")
    #
    return {
        "monthly_limit": monthly_limit,
        "enabled": bool(raw.get("enabled", True)),
        "currency": raw.get("currency", "USD"),
    }


class PromptLibAPI(api_tools.APIModeHandler):
    """A project member reads their own per-user budget and spend."""

    @register_openapi(
        name="Get Member Budget",
        description=(
            "One member's own budget and current-month spend within a project. This limit "
            "applies in addition to the project limit, so a call is blocked when either is "
            "exceeded.\n\n"
            "In prompt_lib mode a member may read only their own row: requesting another "
            "member's user_id returns 403 unless the caller is an admin of the project. In "
            "administration mode platform administrators may read any row."
        ),
        tags=[OPENAPI_TAG],
        parameters=MEMBER_PARAMS,
        responses={
            "200": {"description": "The member's budget state and current-month spend."},
            "403": {"description": "Caller asked for another member's budget and is not a project admin."},
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
    def get(self, project_id: int, user_id: int, **kwargs):
        # Without this, any member could read another member's spend by editing the URL
        current_user_id = auth.current_user().get("id")
        #
        if int(user_id) != int(current_user_id):
            try:
                is_admin = rpc_tools.RpcMixin().rpc.timeout(5).admin_check_user_is_admin(
                    project_id, current_user_id,
                )
            except Exception:  # pylint: disable=W0703
                is_admin = False
            #
            if not is_admin:
                return {"error": "Forbidden"}, 403
        #
        return _user_budget_state(project_id, user_id), 200


class AdminAPI(api_tools.APIModeHandler):
    """Platform-admin read/write of any user's budget within a project."""

    # Documented on PromptLibAPI: both handlers share one path, so only one GET
    # registration survives. Same convention as analytics.py. The PUT below has no
    # project-scoped twin, so it is documented here.
    @auth.decorators.check_api(
        {
            "permissions": ["models.admin.project_budgets.view"],
            "recommended_roles": {
                c.ADMINISTRATION_MODE: {"admin": True, "editor": False, "viewer": False},
            },
        }
    )
    @api_tools.endpoint_metrics
    def get(self, project_id: int, user_id: int, **kwargs):
        return _user_budget_state(project_id, user_id), 200

    @register_openapi(
        name="Set Member Budget (Admin)",
        description=(
            "Create or replace one member's limit within a project and push it to the LLM "
            "proxy, so it applies on their next call.\n\n"
            "Caps a single member's share without changing the project's own limit; the "
            "stricter of the two decides whether a call is allowed."
        ),
        tags=[OPENAPI_TAG],
        parameters=MEMBER_PARAMS,
        request_body=BUDGET_WRITE_BODY,
        responses={
            "200": {"description": "The member's budget state after the write."},
            "400": {"description": "monthly_limit was negative or not a number."},
        },
    )
    @auth.decorators.check_api(
        {
            "permissions": ["models.admin.project_budgets.edit"],
            "recommended_roles": {
                c.ADMINISTRATION_MODE: {"admin": True, "editor": False, "viewer": False},
            },
        }
    )
    @api_tools.endpoint_metrics
    def put(self, project_id: int, user_id: int, **kwargs):
        try:
            payload = _parse_payload(dict(request.json or {}))
        except (TypeError, ValueError) as error:
            return {"error": str(error)}, 400
        #
        rpc_tools.RpcMixin().rpc.timeout(20).elitea_core_set_user_budget(
            project_id=project_id,
            user_id=user_id,
            **payload,
        )
        #
        return _user_budget_state(project_id, user_id), 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes(
        [
            "<int:project_id>/user_budget/<int:user_id>",
        ]
    )

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
        c.ADMINISTRATION_MODE: AdminAPI,
    }
