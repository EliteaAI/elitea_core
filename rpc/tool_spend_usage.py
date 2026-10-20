""" Tool Spend limits for Settings -> Usage (#6983); spend is read by usage itself """

from pylon.core.tools import web, log

from ..utils.tool_spend_features import feature_map


class RPC:  # pylint: disable=E1101,R0903
    @web.rpc("elitea_core_get_tool_spend_limits", "get_tool_spend_limits")
    def get_tool_spend_limits(self, project_id, user_id=None, **kwargs):
        """[{feature, label, limit_usd, limit_source, enabled}] per feature; no call back into usage."""
        result = []
        for name, info in sorted(feature_map(self.toolkit_schemas).items()):
            try:
                limits = self.get_effective_feature_limits(project_id, user_id, name)
            except Exception:  # pylint: disable=W0703
                log.exception("Tool spend limits failed for project %s feature %s", project_id, name)
                continue
            limit = limits.get("project_limit_usd" if user_id is None else "member_limit_usd")
            source = limits.get("project_source" if user_id is None else "member_source")
            result.append({
                "feature": name,
                "label": info["label"],
                "limit_usd": limit,
                # The resolver says "default" even when no default exists; that reads as unlimited
                "limit_source": "unlimited" if limit is None else (source or "default"),
                "enabled": limits.get("feature_enabled") is not False,
            })
        return result

    @web.rpc("elitea_core_get_tool_spend_labels", "get_tool_spend_labels")
    def get_tool_spend_labels(self, **kwargs):
        """{feature: label} from toolkit metadata, for analytics rows."""
        return {name: info["label"] for name, info in feature_map(self.toolkit_schemas).items()}
