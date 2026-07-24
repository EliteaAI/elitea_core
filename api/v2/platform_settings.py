"""
Platform Settings API

Exposes deployment-level feature flags and settings to the UI.
These settings are configured in elitea_core.yml and cached at startup.
"""

from tools import api_tools, auth, config as c, this

from ...utils.constants import PROMPT_LIB_MODE
from ...utils.mcp_config import get_mcp_category_name, is_mcp_exposure_enabled, is_mcp_in_menu_enabled
from ...utils.skill_publish_utils import get_skill_publish_blocked, get_skill_publish_whitelist


def _is_analytics_enabled():
    """Check if Analytics tab is enabled via elitea_core config."""
    try:
        analytics_config = this.module.descriptor.config.get('analytics', {})
        return analytics_config.get('enabled', True)
    except Exception:
        pass
    return True


class PromptLibAPI(api_tools.APIModeHandler):
    @api_tools.endpoint_metrics
    def get(self, **kwargs):
        """
        Get platform-level feature settings.

        Returns deployment-configured feature flags that control
        UI visibility and functionality availability.
        """
        return {
            "mcp_exposure_enabled": is_mcp_exposure_enabled(),
            "mcp_in_menu_enabled": is_mcp_in_menu_enabled(),
            "mcp_category_name": get_mcp_category_name(),
            "analytics_enabled": _is_analytics_enabled(),
            "is_publish_blocked": getattr(this.module, 'is_publish_blocked', False),
            "publish_whitelist_project_ids": list(
                getattr(this.module, 'publish_whitelist_project_ids', set())
            ),
            "is_skill_publish_blocked": get_skill_publish_blocked(),
            "skill_publish_whitelist_project_ids": list(get_skill_publish_whitelist()),
        }, 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI
    }
