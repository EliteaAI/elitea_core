""" Tool Spend limits for Settings -> Usage (#6983); spend is read by usage itself """

from pylon.core.tools import web, log

from ..utils.tool_spend_features import feature_map


def limit_source(limit, project_row, member_row, member=False):
    """Where the limit came from, judged by the resolved value like the admin listing."""
    if limit is None:
        return "unlimited"
    project_row = project_row or {}
    if not member:
        return "explicit" if project_row.get("enabled", True) and project_row.get("limit") is not None else "default"
    member_row = member_row or {}
    # An exempt member still inherits the project's member default, so the flag alone is not "explicit"
    if member_row.get("enabled", True) and member_row.get("limit") is not None:
        return "explicit"
    if project_row.get("member_default_limit") is not None and limit == project_row["member_default_limit"]:
        return "project_default"
    return "default"


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
            result.append({
                "feature": name,
                "label": info["label"],
                "limit_usd": limit,
                "limit_source": limit_source(
                    limit,
                    self.get_project_feature_budget(project_id, name) if limit is not None else None,
                    self.get_user_feature_budget(project_id, user_id, name) if limit is not None and user_id else None,
                    member=user_id is not None,
                ),
                "enabled": limits.get("feature_enabled") is not False,
            })
        return result

    @web.rpc("elitea_core_get_tool_spend_labels", "get_tool_spend_labels")
    def get_tool_spend_labels(self, **kwargs):
        """{feature: label} from toolkit metadata, for analytics rows."""
        return {name: info["label"] for name, info in feature_map(self.toolkit_schemas).items()}
