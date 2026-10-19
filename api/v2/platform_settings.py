"""
Platform Settings API

Exposes deployment-level feature flags and settings to the UI.
These settings are configured in elitea_core.yml and cached at startup.
"""

from tools import api_tools, auth, config as c, rpc_tools, this

from ...utils.constants import PROMPT_LIB_MODE
from ...utils.mcp_config import get_mcp_category_name, is_mcp_exposure_enabled, is_mcp_in_menu_enabled
from ...utils.midturn_injection_utils import (
    get_midturn_injection_blocked,
    get_midturn_injection_whitelist,
)
from ...utils.skill_publish_utils import get_skill_publish_blocked, get_skill_publish_whitelist


_DEFAULT_NEW_ITEM_DAYS = 14
_MIN_NEW_ITEM_DAYS = 7
_MAX_NEW_ITEM_DAYS = 60


def _get_catalog_new_item_days(guardrail_key: str) -> int:
    try:
        raw = this.module.descriptor.config.get(guardrail_key, {}).get(
            'catalog_new_item_days', _DEFAULT_NEW_ITEM_DAYS
        )
        return max(_MIN_NEW_ITEM_DAYS, min(_MAX_NEW_ITEM_DAYS, int(raw)))
    except Exception:
        pass
    return _DEFAULT_NEW_ITEM_DAYS


def _is_analytics_enabled():
    """Check if Analytics tab is enabled via elitea_core config."""
    try:
        analytics_config = this.module.descriptor.config.get('analytics', {})
        return analytics_config.get('enabled', True)
    except Exception:
        pass
    return True


def _usage_mode_settings():
    """Mode plus warning-dismissibility in a single RPC hop; both live in the usage config."""
    try:
        settings = rpc_tools.RpcMixin().rpc.timeout(5).usage_mode_settings()
    except Exception:  # pylint: disable=W0703
        return None, True
    #
    mode = settings.get("mode")
    return (mode if mode in ("enforce", "observe") else None), settings.get(
        "warnings_dismissible", True
    ) is not False


def _tool_spend_availability():
    """Per-feature availability; project-level resolution needs ?project_id."""
    from flask import request  # pylint: disable=C0415
    from ...utils.tool_spend_features import feature_defaults, feature_map  # pylint: disable=C0415
    from ...utils.web_search import WEB_SEARCH_FEATURE, web_search_availability  # pylint: disable=C0415
    #
    try:
        features = feature_map(this.module.toolkit_schemas)
        project_id = request.args.get("project_id", type=int)
        user = auth.current_user() or {}
        result = {}
        for feature in features:
            if feature == WEB_SEARCH_FEATURE:
                result[feature] = web_search_availability(project_id, user.get("id"))
                continue
            if not feature_defaults(this.module.descriptor.config, feature).get("enabled", False):
                result[feature] = {"available": False, "reason": "disabled_platform"}
                continue
            if project_id is not None:
                limits = this.module.get_effective_feature_limits(project_id, user.get("id"), feature)
                if not limits.get("feature_enabled"):
                    result[feature] = {"available": False, "reason": "disabled_project"}
                    continue
            result[feature] = {"available": True, "reason": None}
        return result
    except Exception:  # pylint: disable=W0703
        return {}


class PromptLibAPI(api_tools.APIModeHandler):
    @api_tools.endpoint_metrics
    def get(self, **kwargs):
        """
        Get platform-level feature settings.

        Returns deployment-configured feature flags that control
        UI visibility and functionality availability.
        """
        # One RPC hop for both: mode and dismissibility live in the same usage config
        budgets_mode, warning_dismissible = _usage_mode_settings()
        #
        return {
            "mcp_exposure_enabled": is_mcp_exposure_enabled(),
            "mcp_in_menu_enabled": is_mcp_in_menu_enabled(),
            "mcp_category_name": get_mcp_category_name(),
            "analytics_enabled": _is_analytics_enabled(),
            # Tracking is on, so the Usage tab has data to show
            "cost_budgets_enabled": budgets_mode in ("observe", "enforce"),
            # Limits actually block. Observe mode tracks without blocking, so a warning
            # that requests are about to become unavailable would not be true there.
            "cost_budgets_enforcing": budgets_mode == "enforce",
            "cost_budgets_warning_dismissible": warning_dismissible,
            "is_publish_blocked": getattr(this.module, 'is_publish_blocked', False),
            "publish_whitelist_project_ids": list(
                getattr(this.module, 'publish_whitelist_project_ids', set())
            ),
            "is_skill_publish_blocked": get_skill_publish_blocked(),
            "skill_publish_whitelist_project_ids": list(get_skill_publish_whitelist()),
            "is_midturn_injection_blocked": get_midturn_injection_blocked(),
            "midturn_injection_whitelist_project_ids": list(
                get_midturn_injection_whitelist()
            ),
            "agent_catalog_new_item_days": _get_catalog_new_item_days(
                'publishing_guardrail'
            ),
            "skill_catalog_new_item_days": _get_catalog_new_item_days(
                'skill_publishing_guardrail'
            ),
            "tool_spend": _tool_spend_availability(),
        }, 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI
    }
