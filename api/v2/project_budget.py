from flask import request
from tools import api_tools, auth, config as c, register_openapi, rpc_tools

from ...utils.constants import PROMPT_LIB_MODE

OPENAPI_TAG = "elitea_core/usage"

PROJECT_ID_PARAM = {
    "name": "project_id",
    "in": "path",
    "required": True,
    "schema": {"type": "integer"},
    "description": "Project whose budget to read or write.",
    "example": 1,
}

_BUDGET_WRITE_PROPERTIES = {
    "monthly_limit": {
        "type": "number",
        "nullable": True,
        "minimum": 0,
        "description": "Limit in USD. Null means no limit for this scope.",
    },
    "enabled": {
        "type": "boolean",
        "default": True,
        "description": (
            "False marks the scope deliberately exempt, so it stays "
            "unlimited even where a platform default would otherwise apply."
        ),
    },
    "currency": {"type": "string", "default": "USD"},
}

# Shared with the member-budget endpoint, which has no project-scoped fields
BUDGET_WRITE_BODY = {
    "required": True,
    "content": {
        "application/json": {
            "schema": {"type": "object", "properties": _BUDGET_WRITE_PROPERTIES},
            "example": {"monthly_limit": 100, "enabled": True, "currency": "USD"},
        },
    },
}

PROJECT_BUDGET_WRITE_BODY = {
    "required": True,
    "content": {
        "application/json": {
            "schema": {
                "type": "object",
                "properties": {
                    **_BUDGET_WRITE_PROPERTIES,
                    "member_default_limit": {
                        "type": "number",
                        "nullable": True,
                        "minimum": 0,
                        "description": (
                            "Default in USD applied to every member of this project who has "
                            "no limit of their own, ahead of any platform default. Null "
                            "clears it; omit the field to leave it unchanged."
                        ),
                    },
                },
            },
            "example": {
                "monthly_limit": 100,
                "enabled": True,
                "currency": "USD",
                "member_default_limit": 20,
            },
        },
    },
}


def _budget_state(project_id: int):
    """Merge the stored limit with LiteLLM's current-month spend."""
    rpc = rpc_tools.RpcMixin().rpc
    #
    budget = rpc.timeout(5).elitea_core_get_project_budget(project_id=project_id) or {}
    #
    try:
        spend = rpc.timeout(15).litellm_get_project_spend(project_id=project_id) or {}
    except Exception:  # pylint: disable=W0703
        spend = {}
    #
    # The effective limit may come from a platform default when no row exists
    try:
        limit = rpc.timeout(5).litellm_get_effective_project_limit(project_id=project_id)
    except Exception:  # pylint: disable=W0703
        limit = budget.get("monthly_limit") if budget.get("enabled", True) else None
    #
    spent = float(spend.get("spend", 0) or 0)
    #
    return {
        "project_id": project_id,
        "monthly_limit": budget.get("monthly_limit"),
        "member_default_limit": budget.get("member_default_limit"),
        "effective_limit": limit,
        "limit_source": "explicit" if budget.get("monthly_limit") is not None else (
            "default" if limit is not None else "unlimited"
        ),
        "currency": budget.get("currency", "USD"),
        "enabled": budget.get("enabled", False),
        "spend": spent,
        "remaining": None if limit is None else max(0.0, limit - spent),
        "percent_used": None if not limit else round(spent / limit * 100, 2),
        "prompt_tokens": spend.get("prompt_tokens", 0),
        "completion_tokens": spend.get("completion_tokens", 0),
        "total_tokens": spend.get("total_tokens", 0),
        "period": spend.get("period"),
        "spend_available": spend.get("available", False),
    }


def _parse_limit(raw: dict, field: str):
    """Read one optional limit field, rejecting negatives."""
    value = raw.get(field)
    #
    if value is None:
        return None
    #
    value = float(value)
    #
    if value < 0:
        raise ValueError(f"{field} must be >= 0")
    #
    return value


def _parse_payload(raw: dict):
    """Validate a budget write payload."""
    payload = {
        "monthly_limit": _parse_limit(raw, "monthly_limit"),
        "enabled": bool(raw.get("enabled", True)),
        "currency": raw.get("currency", "USD"),
    }
    #
    # Only forwarded when sent: an absent key must leave every member's default alone
    if "member_default_limit" in raw:
        payload["member_default_limit"] = _parse_limit(raw, "member_default_limit")
    #
    return payload


class PromptLibAPI(api_tools.APIModeHandler):
    """Project-scoped read of a project's own budget and spend."""

    @register_openapi(
        name="Get Project Budget",
        description=(
            "The project's own budget and current-month spend. limit_source explains where "
            "the enforced limit came from: explicit when set for this project, default when "
            "inherited from a platform default, unlimited when nothing applies.\n\n"
            "Available to project members in prompt_lib mode and to platform "
            "administrators, regardless of membership, in administration mode."
        ),
        tags=[OPENAPI_TAG],
        parameters=[PROJECT_ID_PARAM],
        responses={"200": {"description": "Budget state and current-month spend."}},
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
        return _budget_state(project_id), 200


class AdminAPI(api_tools.APIModeHandler):
    """Platform-admin read/write of any project's budget."""

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
    def get(self, project_id: int, **kwargs):
        return _budget_state(project_id), 200

    @register_openapi(
        name="Set Project Budget (Admin)",
        description=(
            "Create or replace a project's monthly limit and push it to the LLM proxy, so "
            "it takes effect on the next call rather than at the next period.\n\n"
            "Returns the resulting budget state, which may differ from what was sent: a "
            "null limit or enabled=false leaves the project unlimited, and clearing an "
            "explicit limit can let a platform default apply instead.\n\n"
            "member_default_limit is a separate value applied to members of this project "
            "who have no limit of their own; it is honoured even when enabled=false."
        ),
        tags=[OPENAPI_TAG],
        parameters=[PROJECT_ID_PARAM],
        request_body=PROJECT_BUDGET_WRITE_BODY,
        responses={
            "200": {"description": "Budget state after the write."},
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
    def put(self, project_id: int, **kwargs):
        try:
            payload = _parse_payload(dict(request.json or {}))
        except (TypeError, ValueError) as error:
            return {"error": str(error)}, 400
        #
        rpc_tools.RpcMixin().rpc.timeout(20).elitea_core_set_project_budget(
            project_id=project_id,
            **payload,
        )
        #
        return _budget_state(project_id), 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes(
        [
            "<int:project_id>/budget",
        ]
    )

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
        c.ADMINISTRATION_MODE: AdminAPI,
    }
