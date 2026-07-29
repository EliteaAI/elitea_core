import calendar
import datetime
import re

from flask import request
from tools import api_tools, auth, config as c, rpc_tools

from ...utils.constants import PROMPT_LIB_MODE

SCOPE_PROJECT = "project"
SCOPE_USER = "user"

# Amount fields stripped for members who may not see platform cost figures
AMOUNT_FIELDS = ("spend", "monthly_limit", "effective_limit", "remaining", "currency")

# Used when the runtime plugin cannot be reached; matches its own default
DEFAULT_WARNING_PCT = 80


def _strip_model_prefixes(model: str):
    """Model id as the configuration registry stores it.

    LiteLLM reports the resolved name, which carries a provider prefix and, for a
    shared model, the owning project id. Neither is part of the configured name.
    """
    without_provider = model.rsplit("/", 1)[-1]
    #
    return re.sub(r"^\d+_", "", without_provider)


def _model_display_names(project_id: int):
    """Map of configured model name -> display name, for the usage-by-model table.

    Same source of truth as the analytics pages, so one model reads identically in both.
    An empty map is a safe outcome: callers keep the raw model name.
    """
    try:
        response = rpc_tools.RpcMixin().rpc.timeout(5).configurations_get_models(
            project_id=project_id, section="llm", include_shared=True,
        ) or {}
    except Exception:  # pylint: disable=W0703
        return {}
    #
    result = {}
    #
    for item in response.get("items") or []:
        if isinstance(item, dict) and item.get("name"):
            result[item["name"]] = item.get("display_name") or item["name"]
    #
    return result


def _attach_display_names(models: list, display_names: dict):
    """Add display_name to each usage row that resolves to a configured model.

    Tries the reported name first so an exact registration always wins, then the
    normalised form. Rows that resolve to nothing are left alone rather than given
    the raw id, so the client can fall back to its own formatting.
    """
    if not display_names:
        return models
    #
    for row in models:
        raw = row.get("model") or ""
        display = display_names.get(raw) or display_names.get(_strip_model_prefixes(raw))
        #
        if display:
            row["display_name"] = display
    #
    return models


def _period_reset(period: str):
    """Start of the next monthly period, when the budget tag rolls over."""
    try:
        year, month = int(period[:4]), int(period[4:6])
    except (TypeError, ValueError, IndexError):
        now = datetime.datetime.now(datetime.timezone.utc)
        year, month = now.year, now.month
    #
    year, month = (year + 1, 1) if month == 12 else (year, month + 1)
    #
    return datetime.datetime(year, month, 1, tzinfo=datetime.timezone.utc).isoformat()


def _period_bounds(period: str):
    """First and last day of the period, so the UI can plot a full month."""
    try:
        year, month = int(period[:4]), int(period[4:6])
    except (TypeError, ValueError, IndexError):
        now = datetime.datetime.now(datetime.timezone.utc)
        year, month = now.year, now.month
    #
    last_day = calendar.monthrange(year, month)[1]
    #
    return f"{year:04d}-{month:02d}-01", f"{year:04d}-{month:02d}-{last_day:02d}"


def _is_personal_project(project_id: int, user_id: int):
    """True when this project is the caller's own personal project."""
    try:
        personal_id = rpc_tools.RpcMixin().rpc.timeout(5).projects_get_personal_project_id(
            user_id=user_id,
        )
    except Exception:  # pylint: disable=W0703
        return False
    #
    return personal_id is not None and int(personal_id) == int(project_id)


def _can_see_amounts(project_id: int, user_id: int, is_personal: bool):
    """Members see percentages only; project admins and personal-project owners see cost.

    A personal project's spend is the owner's own, and it is where token-based
    integrations land, so amounts are always shown there.
    """
    if is_personal:
        return True
    #
    try:
        return bool(rpc_tools.RpcMixin().rpc.timeout(5).admin_check_user_is_admin(
            project_id, user_id,
        ))
    except Exception:  # pylint: disable=W0703
        return False


def _redact(payload: dict):
    """Drop cost figures, keeping percentages and token counts."""
    for field in AMOUNT_FIELDS:
        payload.pop(field, None)
    #
    for row in payload.get("models") or []:
        row.pop("spend", None)
    #
    for row in payload.get("daily") or []:
        row.pop("spend", None)
    #
    return payload


def _warning_pct(scope: str, is_personal: bool):
    """Configured percent-of-limit at which this scope warns."""
    threshold_scope = "user" if scope == SCOPE_USER else (
        "personal_project" if is_personal else "project"
    )
    #
    try:
        return rpc_tools.RpcMixin().rpc.timeout(5).litellm_get_warning_threshold(
            scope=threshold_scope,
        )
    except Exception:  # pylint: disable=W0703
        return DEFAULT_WARNING_PCT


def _usage_state(project_id: int, user_id: int, scope: str, is_personal: bool):
    """Budget state plus the per-model and per-day breakdown for one scope."""
    rpc = rpc_tools.RpcMixin().rpc
    #
    if scope == SCOPE_USER:
        budget = rpc.timeout(5).elitea_core_get_user_budget(
            project_id=project_id, user_id=user_id,
        ) or {}
        #
        try:
            limit = (rpc.timeout(10).litellm_get_effective_user_limits(
                project_id=project_id, user_ids=[user_id],
            ) or {}).get(user_id)
        except Exception:  # pylint: disable=W0703
            limit = budget.get("monthly_limit") if budget.get("enabled", True) else None
        #
        try:
            detail = rpc.timeout(30).litellm_get_user_usage_detail(
                project_id=project_id, user_id=user_id,
            ) or {}
        except Exception:  # pylint: disable=W0703
            detail = {}
    else:
        budget = rpc.timeout(5).elitea_core_get_project_budget(project_id=project_id) or {}
        #
        try:
            limit = rpc.timeout(5).litellm_get_effective_project_limit(project_id=project_id)
        except Exception:  # pylint: disable=W0703
            limit = budget.get("monthly_limit") if budget.get("enabled", True) else None
        #
        try:
            detail = rpc.timeout(30).litellm_get_project_usage_detail(project_id=project_id) or {}
        except Exception:  # pylint: disable=W0703
            detail = {}
    #
    spent = float(detail.get("spend", 0) or 0)
    period = detail.get("period") or f"{datetime.datetime.now(datetime.timezone.utc):%Y%m}"
    period_start, period_end = _period_bounds(period)
    #
    return {
        "project_id": project_id,
        "user_id": user_id if scope == SCOPE_USER else None,
        "scope": scope,
        "monthly_limit": budget.get("monthly_limit"),
        "effective_limit": limit,
        "limit_source": "explicit" if budget.get("monthly_limit") is not None else (
            "default" if limit is not None else "unlimited"
        ),
        "currency": budget.get("currency", "USD"),
        "spend": spent,
        "remaining": None if limit is None else max(0.0, limit - spent),
        "percent_used": None if not limit else round(spent / limit * 100, 2),
        "warning_pct": _warning_pct(scope, is_personal),
        "total_tokens": detail.get("total_tokens", 0),
        "api_requests": detail.get("api_requests", 0),
        "models": _attach_display_names(
            detail.get("models") or [], _model_display_names(project_id),
        ),
        "daily": detail.get("daily") or [],
        "period": period,
        "period_start": period_start,
        "period_end": period_end,
        "resets_at": _period_reset(period),
        "spend_available": detail.get("available", False),
    }


class PromptLibAPI(api_tools.APIModeHandler):
    """A project member reads usage for the project or for themselves."""

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
        user_id = auth.current_user().get("id")
        #
        scope = request.args.get("scope", SCOPE_PROJECT)
        #
        if scope not in (SCOPE_PROJECT, SCOPE_USER):
            return {"error": "scope must be 'project' or 'user'"}, 400
        #
        is_personal = _is_personal_project(project_id, user_id)
        #
        payload = _usage_state(project_id, user_id, scope, is_personal)
        #
        visible = _can_see_amounts(project_id, user_id, is_personal)
        payload["can_see_amounts"] = visible
        #
        return (payload if visible else _redact(payload)), 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes(
        [
            "<int:project_id>/usage",
        ]
    )

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
