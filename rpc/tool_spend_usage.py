""" Member-facing Tool Spend usage, for Settings -> Usage (#6983) """

from pylon.core.tools import web, log

from ..utils.feature_budget_admin import feature_state
from ..utils.tool_spend_features import feature_map


def feature_usage(state):
    """One feature's Usage-page row, shaped like the LLM usage payload."""
    limit = state.get("effective_limit")
    return {
        "effective_limit": limit,
        # The resolver says "default" even when no default exists; that reads as unlimited here
        "limit_source": "unlimited" if limit is None else (state.get("limit_source") or "default"),
        "currency": "USD",
        "spend": state.get("spend", 0.0),
        "remaining": state.get("remaining"),
        "percent_used": state.get("percent_used"),
        "calls": state.get("calls", 0),
        "enabled": state.get("feature_enabled_effective") is not False,
        "spend_available": state.get("spend_available", False),
    }


class RPC:  # pylint: disable=E1101,R0903
    @web.rpc("elitea_core_get_tool_spend_usage", "get_tool_spend_usage")
    def get_tool_spend_usage(self, project_id, user_id=None, **kwargs):
        """[{feature, label, ...usage}] per Tool Spend feature; user_id scopes it to one member."""
        result = []
        for name, info in sorted(feature_map(self.toolkit_schemas).items()):
            try:
                state = feature_state(project_id, name, user_id)
            except Exception:  # pylint: disable=W0703
                log.exception("Tool spend usage failed for project %s feature %s", project_id, name)
                continue
            result.append({"feature": name, "label": info["label"], **feature_usage(state)})
        return result
