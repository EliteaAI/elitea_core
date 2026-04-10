"""
Platform Settings API

Exposes deployment-level feature flags and settings to the UI.
These settings are configured in elitea_core.yml and cached at startup.
"""

from tools import api_tools, auth, config as c, this

from ...utils.constants import PROMPT_LIB_MODE
from ...utils.mcp_config import is_mcp_exposure_enabled, is_mcp_in_menu_enabled


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
            "analytics_enabled": _is_analytics_enabled(),
        }, 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI
    }
