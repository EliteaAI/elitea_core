from flask import request
from tools import api_tools, auth, config as c, rpc_tools

from ...utils.constants import PROMPT_LIB_MODE


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
        return _user_budget_state(project_id, user_id), 200


class AdminAPI(api_tools.APIModeHandler):
    """Platform-admin read/write of any user's budget within a project."""

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
